// Command dlockss runs a D-LOCKSS node: file ingestion, sharding, replication, and observability.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"dlockss/internal/api"
	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/discovery"
	"dlockss/internal/fileops"
	"dlockss/internal/identity"
	"dlockss/internal/managers/clusters"
	"dlockss/internal/managers/shard"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/internal/telemetry"
	"dlockss/internal/trust"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/pbnjay/memory"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var logLevel slog.LevelVar
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: &logLevel})))

	cfg := config.LoadFromEnv()
	cfg.Validate()
	if cfg.VerboseLogging {
		logLevel.Set(slog.LevelDebug)
	}
	cfg.Log()

	if msg := cfg.ValidatePathSafetyCheck(); msg != "" {
		log.Fatalf("[Fatal] Unsafe path configuration: %s", msg)
	}

	badBitsFilter, err := badbits.NewFilter(cfg.BadBitsPath)
	if err != nil {
		slog.Warn("failed to load bad bits list", "error", err)
	}

	nodeName := identity.ResolveNodeName(cfg)
	if nodeName != "" {
		slog.Info("node name resolved", "name", nodeName)
	}

	// IPFS client and DHT (required)
	ipfsClient, err := ipfs.NewClient(cfg.IPFSNodeAddress)
	if err != nil {
		log.Fatalf("[Fatal] IPFS not available: %v (start IPFS with: ipfs daemon)", err)
	}
	dht := ipfs.NewIPFSDHTAdapterFromClient(ipfsClient)

	privKey, err := identity.LoadKey(cfg)
	if err != nil {
		log.Fatalf("[Fatal] Failed to load identity: %v", err)
	}

	// Resource manager with a minimum connection floor so shard peers can always connect
	// (default AutoScale on low-memory nodes can hit "resource limit exceeded" and block shard coordination).
	limits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&limits)
	mem := memory.TotalMemory() / 8
	const minMemForScale = 384 << 20 // 384 MiB floor so limits allow enough conns for shard peers
	if mem < minMemForScale {
		mem = minMemForScale
	}
	scaled := limits.Scale(int64(mem), 512)
	limiter := rcmgr.NewFixedLimiter(scaled)
	rcm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create resource manager: %v", err)
	}
	defer rcm.Close()

	h, err := libp2p.New(
		libp2p.ResourceManager(rcm),
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoRelayWithStaticRelays(kaddht.GetDefaultBootstrapPeerAddrInfos()),
		libp2p.EnableNATService(),
		libp2p.ChainOptions(
			libp2p.Security(noise.ID, noise.New),
		),
	)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	slog.Info("libp2p host created", "peer_id", h.ID().String())

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create pubsub: %v", err)
	}

	// DHT for discovery (separate from IPFS daemon; used for dlockss-prod rendezvous).
	kademliaDHT, err := kaddht.New(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create DHT: %v", err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("[Fatal] Failed to bootstrap DHT: %v", err)
	}

	// Connect to default bootstrap peers (non-blocking: proceed after timeout if some fail)
	var wg sync.WaitGroup
	for _, peerAddr := range kaddht.DefaultBootstrapPeers {
		peerinfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Connect(ctx, *peerinfo)
		}()
	}
	// Proceed after BootstrapTimeout or when all connects finish (whichever first)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(cfg.BootstrapTimeout):
		slog.Warn("bootstrap timeout, proceeding with partial connectivity", "timeout", cfg.BootstrapTimeout)
	}

	// Setup Routing Discovery
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, cfg.DiscoveryServiceTag)
	slog.Info("advertising service on DHT", "tag", cfg.DiscoveryServiceTag)

	// mDNS discovery so nodes and monitor find each other on the same LAN (same tag as monitor)
	notifee := &discovery.DiscoveryNotifee{H: h, Ctx: ctx}
	mdnsSvc := mdns.NewMdnsService(h, cfg.DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		slog.Warn("mDNS start failed, peer/monitor discovery limited", "error", err)
	}

	go discovery.RunPeerFinder(ctx, h, routingDiscovery, cfg.DiscoveryServiceTag)

	// Trust (optional: load peers if file exists)
	trustMgr := trust.NewTrustManager(cfg.TrustMode)
	if err := trustMgr.LoadTrustedPeers(cfg.TrustStorePath); err != nil && !os.IsNotExist(err) {
		slog.Warn("trust store load failed", "error", err)
	}

	// Initialize persistent datastore for cluster state
	// We place this OUTSIDE the FileWatchFolder (ingest dir) to avoid the node trying to ingest its own database files.
	// Path is configurable via DLOCKSS_CLUSTER_STORE; otherwise derived from FileWatchFolder (instance-specific when data dirs differ).
	dstore, err := leveldb.NewDatastore(cfg.ClusterStorePath, nil)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create datastore at %s: %v", cfg.ClusterStorePath, err)
	}
	defer dstore.Close()

	rateLimiter := common.NewRateLimiter(cfg.RateLimitWindow, cfg.MaxMessagesPerWindow)
	metrics := telemetry.NewMetricsManager(cfg)
	storageMgr := storage.NewStorageManager(cfg, dht, metrics, badBitsFilter)
	signer := signing.NewSigner(signing.SignerConfig{
		Cfg:      cfg,
		Host:     h,
		PrivKey:  privKey,
		PeerID:   h.ID(),
		TrustMgr: trustMgr,
		DHT:      dht,
	})

	// Shard manager (replication set later to break cycle).
	// onPinSynced: when PinTracker syncs a pin from CRDT, register with storage and announce PINNED immediately so monitor sees replication right away (not only on next heartbeat batch).
	// Also advertise manifest and payload to the DHT so retrieval checkers (e.g. check.ipfs.network) and gateways see all replicas as providers.
	var announcePinned func(string)
	onPinSynced := func(manifestCIDStr string) {
		storageMgr.PinFile(manifestCIDStr)
		if announcePinned != nil {
			announcePinned(manifestCIDStr)
		}
		// Provide manifest in its own goroutine with its own timeout.
		go func() {
			pctx, pcancel := context.WithTimeout(ctx, cfg.DHTProvideTimeout)
			defer pcancel()
			storageMgr.ProvideFile(pctx, manifestCIDStr)
		}()
		// Resolve payload from manifest, pin it as its own root (so Kubo's
		// reprovider with "pinned" strategy re-announces it), and provide it
		// to the DHT.  On the ingesting node the payload is already a pin
		// root from ImportFile; on replicas only ManifestCID is pinned so
		// this call adds the missing pin entry.  Blocks are already local
		// from the manifest's recursive pin so this returns quickly.
		go func() {
			pctx, pcancel := context.WithTimeout(ctx, cfg.DHTProvideTimeout)
			defer pcancel()
			manifestCID, err := cid.Decode(manifestCIDStr)
			if err != nil {
				return
			}
			block, err := ipfsClient.GetBlock(pctx, manifestCID)
			if err != nil {
				slog.Warn("failed to resolve payload from manifest", "manifest", manifestCIDStr, "error", err)
				return
			}
			var ro schema.ResearchObject
			if err := ro.UnmarshalCBOR(block); err != nil {
				return
			}
			if ro.HasLegacyTimestamp {
				return
			}
			payloadCID := ro.Payload
			if !payloadCID.Defined() {
				return
			}
			if err := ipfsClient.PinRecursive(pctx, payloadCID); err != nil {
				slog.Warn("failed to pin payload", "payload", payloadCID, "error", err)
			}
			storageMgr.ProvideFile(pctx, payloadCID.String())
		}()
	}
	onPinRemoved := func(cid string) {
		storageMgr.UnpinFile(cid)
	}
	clusterMgr := clusters.NewClusterManager(clusters.ClusterManagerConfig{
		Cfg:          cfg,
		Host:         h,
		PubSub:       ps,
		DHT:          dht,
		Datastore:    dstore,
		IPFSClient:   ipfsClient,
		TrustedPeers: trustMgr.GetTrustedPeers(),
		BadBits:      badBitsFilter,
		OnPinSynced:  onPinSynced,
		OnPinRemoved: onPinRemoved,
	})
	shardMgr, err := shard.NewShardManager(shard.ShardManagerConfig{
		Cfg:         cfg,
		Ctx:         ctx,
		Host:        h,
		PubSub:      ps,
		IPFSClient:  ipfsClient,
		Storage:     storageMgr,
		Metrics:     metrics,
		Signer:      signer,
		RateLimiter: rateLimiter,
		Cluster:     clusterMgr,
		NodeName:    nodeName,
	})
	if err != nil {
		log.Fatalf("Failed to initialize shard manager: %v", err)
	}
	clusterMgr.SetShardPeerProvider(shardMgr) // CRDT Peers() and allocations use real shard membership
	announcePinned = shardMgr.AnnouncePinned

	metrics.RegisterProviders(shardMgr, storageMgr, rateLimiter)
	metrics.RegisterClusterProvider(clusterMgr) // cluster-style metrics: pins/peers/allocations per shard
	metrics.SetPeerID(h.ID().String())

	// Telemetry and API
	tc := telemetry.NewTelemetryClient(cfg, h, ps, metrics)
	if tc != nil {
		tc.SetShardPublisher(shardMgr, shardMgr)
		tc.Start(ctx)
	}
	apiServer := api.NewAPIServer(cfg.APIPort, metrics)
	apiServer.Start()

	// File processor and watcher
	fp := fileops.NewFileProcessor(fileops.FileProcessorConfig{
		Cfg:        cfg,
		IPFSClient: ipfsClient,
		Shard:      shardMgr,
		Storage:    storageMgr,
		PrivKey:    privKey,
		Signer:     signer,
		BadBits:    badBitsFilter,
	})
	go fp.WatchFolder(ctx)

	// Run managers — must start before scanning existing files so the node
	// can join its shard and discover splits before re-ingesting.
	shardMgr.Run()

	// Scan existing files after a short delay to let the node settle into
	// its shard (discovery, split rebroadcast). This re-creates manifests
	// for any files in the data directory, replacing legacy manifests
	// (with timestamps) with deterministic ones.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
		fp.ScanExistingFiles()
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	if err := shardMgr.Close(); err != nil {
		slog.Error("shard manager close error", "error", err)
	}
	_ = apiServer.Shutdown(context.Background())
}
