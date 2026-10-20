// Command dlockss runs a D-LOCKSS node: file ingestion, sharding, replication, and observability.
package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
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
	"dlockss/internal/trust"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pmetrics "github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
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

	ipfsClient, err := ipfs.NewClient(cfg.IPFSNodeAddress)
	if err != nil {
		log.Fatalf("[Fatal] IPFS not available: %v (start IPFS with: ipfs daemon)", err)
	}
	dht := ipfs.NewIPFSDHTAdapterFromClient(ipfsClient)

	privKey, err := identity.LoadKey(cfg)
	if err != nil {
		log.Fatalf("[Fatal] Failed to load identity: %v", err)
	}

	rcm, err := newResourceManager()
	if err != nil {
		log.Fatalf("[Fatal] Failed to create resource manager: %v", err)
	}
	defer rcm.Close()

	bandwidthCounter := libp2pmetrics.NewBandwidthCounter()
	h, err := newHost(cfg, privKey, rcm, bandwidthCounter)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	slog.Info("libp2p host created", "peer_id", h.ID().String(), "private_network", cfg.PrivateNetwork)
	if path := os.Getenv("DLOCKSS_BANDWIDTH_PATH"); path != "" {
		go writeBandwidthSnapshots(ctx, bandwidthCounter, path)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create pubsub: %v", err)
	}

	// This DHT is the node's own, separate from the IPFS daemon's, and is
	// used for rendezvous.
	kademliaDHT, err := kaddht.New(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create DHT: %v", err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("[Fatal] Failed to bootstrap DHT: %v", err)
	}

	if !cfg.PrivateNetwork {
		connectBootstrapPeers(ctx, h, cfg.BootstrapTimeout)
	} else {
		slog.Info("private network mode: skipping public DHT bootstrap and AutoRelay")
	}

	startDiscovery(ctx, h, kademliaDHT, cfg.DiscoveryServiceTag)

	trustMgr, err := loadTrustManager(cfg)
	if err != nil {
		log.Fatalf("[Fatal] Failed to load trust store %s: %v", cfg.Security.TrustStorePath, err)
	}

	// The cluster store must live outside FileWatchFolder, or the node would
	// ingest its own database files (see Config.ValidatePathSafetyCheck).
	dstore, err := leveldb.NewDatastore(cfg.ClusterStorePath, nil)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create datastore at %s: %v", cfg.ClusterStorePath, err)
	}
	defer dstore.Close()

	rateLimiter := common.NewRateLimiter(cfg.RateLimitWindow, cfg.MaxMessagesPerWindow)
	storageMgr := storage.NewStorageManager(cfg, dht, badBitsFilter)
	signer := signing.NewSigner(signing.SignerConfig{
		Cfg:      cfg,
		Host:     h,
		PrivKey:  privKey,
		PeerID:   h.ID(),
		TrustMgr: trustMgr,
		DHT:      dht,
	})

	// announcePinned is set once the shard manager exists, which itself
	// depends on the cluster manager.
	var announcePinned func(string)
	// onPinSynced runs when the PinTracker syncs a pin from the CRDT.
	// Announcing PINNED right away lets the monitor see the replica before the
	// next heartbeat batch, and providing manifest and payload lets retrieval
	// checkers and gateways see every replica as a provider.
	onPinSynced := func(manifestCIDStr string) {
		storageMgr.PinFile(manifestCIDStr)
		if announcePinned != nil {
			announcePinned(manifestCIDStr)
		}
		go func() {
			pctx, pcancel := context.WithTimeout(ctx, cfg.Files.DHTProvideTimeout)
			defer pcancel()
			storageMgr.ProvideFile(pctx, manifestCIDStr)
		}()
		go func() {
			pctx, pcancel := context.WithTimeout(ctx, cfg.Files.DHTProvideTimeout)
			defer pcancel()
			pinAndProvidePayload(pctx, ipfsClient, storageMgr, manifestCIDStr)
		}()
	}
	clusterMgr := clusters.NewClusterManager(clusters.ClusterManagerConfig{
		Cfg:          cfg,
		Host:         h,
		PubSub:       ps,
		DHT:          dht,
		Datastore:    dstore,
		IPFSClient:   ipfsClient,
		TrustedPeers: trustMgr.GetTrustedPeers(),
		Origin:       trustMgr,
		BadBits:      badBitsFilter,
		OnPinSynced:  onPinSynced,
		OnPinRemoved: storageMgr.UnpinFile,
	})
	shardMgr, err := shard.NewShardManager(shard.ShardManagerConfig{
		Cfg:         cfg,
		Ctx:         ctx,
		Host:        h,
		PubSub:      ps,
		IPFSClient:  ipfsClient,
		Storage:     storageMgr,
		Signer:      signer,
		RateLimiter: rateLimiter,
		Cluster:     clusterMgr,
		Origin:      trustMgr,
		NodeName:    nodeName,
	})
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize shard manager: %v", err)
	}
	clusterMgr.SetShardPeerProvider(shardMgr) // CRDT Peers() and allocations use real shard membership
	announcePinned = shardMgr.AnnouncePinned

	apiServer := api.NewAPIServer(cfg.APIPort)
	apiServer.Start()

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

	// The shard manager must run before existing files are scanned, so the
	// node joins its shard and learns of splits before re-ingesting.
	shardMgr.Run()

	// The scan waits for the node to settle into its shard (discovery, split
	// rebroadcast). Re-ingesting replaces legacy manifests that carry
	// timestamps with deterministic ones.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
		fp.ScanExistingFiles()
		if cfg.Files.PinIngestEnabled {
			go fp.WatchPins(ctx)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	fp.Stop()
	if err := shardMgr.Close(); err != nil {
		slog.Error("shard manager close error", "error", err)
	}
	_ = apiServer.Shutdown(context.Background())
}

// newResourceManager scales libp2p's default limits to an eighth of system
// memory, but never below 384 MiB, so shard peers can always connect. Plain
// AutoScale on low-memory nodes can hit "resource limit exceeded" and block
// shard coordination.
func newResourceManager() (network.ResourceManager, error) {
	limits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&limits)
	const minMemForScale = 384 << 20
	mem := max(memory.TotalMemory()/8, minMemForScale)
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Scale(int64(mem), 512)))
}

func newHost(cfg *config.Config, privKey crypto.PrivKey, rcm network.ResourceManager, bandwidth *libp2pmetrics.BandwidthCounter) (host.Host, error) {
	opts := []libp2p.Option{
		libp2p.ResourceManager(rcm),
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.Security(noise.ID, noise.New),
		libp2p.BandwidthReporter(bandwidth),
	}
	if !cfg.PrivateNetwork {
		opts = append(opts,
			libp2p.NATPortMap(),
			libp2p.EnableHolePunching(),
			libp2p.EnableAutoRelayWithStaticRelays(kaddht.GetDefaultBootstrapPeerAddrInfos()),
			libp2p.EnableNATService(),
		)
	}
	return libp2p.New(opts...)
}

// connectBootstrapPeers dials the public bootstrap peers in parallel and
// returns once every dial has finished or timeout has elapsed.
func connectBootstrapPeers(ctx context.Context, h host.Host, timeout time.Duration) {
	var wg sync.WaitGroup
	for _, addr := range kaddht.DefaultBootstrapPeers {
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			continue
		}
		wg.Go(func() {
			h.Connect(ctx, *info)
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		slog.Warn("bootstrap timeout, proceeding with partial connectivity", "timeout", timeout)
	}
}

// startDiscovery advertises tag on the DHT and on mDNS, so nodes and the
// monitor (which uses the same tag) find each other, and starts the peer
// finder.
func startDiscovery(ctx context.Context, h host.Host, kademliaDHT *kaddht.IpfsDHT, tag string) {
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, tag)
	slog.Info("advertising service on DHT", "tag", tag)

	mdnsSvc := mdns.NewMdnsService(h, tag, &discovery.DiscoveryNotifee{H: h, Ctx: ctx})
	if err := mdnsSvc.Start(); err != nil {
		slog.Warn("mDNS start failed, peer/monitor discovery limited", "error", err)
	}

	go discovery.RunPeerFinder(ctx, h, routingDiscovery, tag)
}

// loadTrustManager loads the trust store, which decides whose data this node
// stores and, in allowlist mode, who may send signed protocol messages or
// write CRDT pins. A missing trust store is not an error.
func loadTrustManager(cfg *config.Config) (*trust.TrustManager, error) {
	trustMgr := trust.NewTrustManager(cfg.Security.TrustMode)
	if err := trustMgr.LoadTrustedPeers(cfg.Security.TrustStorePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if cfg.Security.TrustMode == "allowlist" {
			slog.Info("trust mode is allowlist but trust store is missing; accepting all origins", "path", cfg.Security.TrustStorePath)
		}
	}
	if added, invalid := trustMgr.AddOrigins(cfg.IngestAllowlist); len(invalid) > 0 {
		slog.Warn("ignoring invalid peer IDs in DLOCKSS_INGEST_ALLOWLIST", "ids", invalid, "accepted", added)
	}
	slog.Info("data origin policy",
		"restrict", trustMgr.RestrictsOrigins(),
		"trusted_origins", len(trustMgr.GetTrustedPeers()),
		"trust_mode", cfg.Security.TrustMode,
		"trust_store", cfg.Security.TrustStorePath,
	)
	return trustMgr, nil
}

// pinAndProvidePayload pins the payload of a manifest as its own root, so
// Kubo's "pinned" reprovider strategy re-announces it, and provides it to the
// DHT. Replicas pin only the manifest, so this adds the missing payload pin;
// the blocks are already local through the manifest's recursive pin.
func pinAndProvidePayload(ctx context.Context, ipfsClient *ipfs.Client, storageMgr *storage.StorageManager, manifestCIDStr string) {
	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		return
	}
	block, err := ipfsClient.GetBlock(ctx, manifestCID)
	if err != nil {
		slog.Warn("failed to resolve payload from manifest", "manifest", manifestCIDStr, "error", err)
		return
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil || ro.HasLegacyTimestamp || !ro.Payload.Defined() {
		return
	}
	if err := ipfsClient.PinRecursive(ctx, ro.Payload); err != nil {
		slog.Warn("failed to pin payload", "payload", ro.Payload, "error", err)
	}
	storageMgr.ProvideFile(ctx, ro.Payload.String())
}

type bandwidthSnapshot struct {
	Total     libp2pmetrics.Stats            `json:"total"`
	Protocols map[string]libp2pmetrics.Stats `json:"protocols"`
}

func writeBandwidthSnapshots(ctx context.Context, counter *libp2pmetrics.BandwidthCounter, path string) {
	write := func() {
		protocols := make(map[string]libp2pmetrics.Stats)
		for id, stats := range counter.GetBandwidthByProtocol() {
			protocols[string(id)] = stats
		}
		data, err := json.Marshal(bandwidthSnapshot{
			Total:     counter.GetBandwidthTotals(),
			Protocols: protocols,
		})
		if err != nil {
			slog.Warn("failed to marshal bandwidth snapshot", "error", err)
			return
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			slog.Warn("failed to create bandwidth snapshot directory", "error", err)
			return
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			slog.Warn("failed to write bandwidth snapshot", "error", err)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			slog.Warn("failed to publish bandwidth snapshot", "error", err)
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			write()
		case <-ctx.Done():
			write()
			return
		}
	}
}
