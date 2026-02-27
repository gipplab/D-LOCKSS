// Command dlockss runs a D-LOCKSS node: file ingestion, sharding, replication, and observability.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"dlockss/internal/api"
	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/discovery"
	"dlockss/internal/fileops"
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
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/pbnjay/memory"
)

// resolveNodeName determines the node's human-readable name. Priority:
// 1. DLOCKSS_NODE_NAME env var  2. Persisted file  3. Interactive prompt
func resolveNodeName() string {
	if config.NodeName != "" {
		persistNodeName(config.NodeName)
		return config.NodeName
	}
	nameFile := config.NodeNamePath
	if data, err := os.ReadFile(nameFile); err == nil {
		if name := strings.TrimSpace(string(data)); name != "" {
			log.Printf("[Config] Loaded node name from %s: %s", nameFile, name)
			return name
		}
	}
	fmt.Print("Enter a name for this node (or press Enter to skip): ")
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		if name := strings.TrimSpace(scanner.Text()); name != "" {
			persistNodeName(name)
			return name
		}
	}
	return ""
}

func persistNodeName(name string) {
	nameFile := config.NodeNamePath
	if dir := filepath.Dir(nameFile); dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	if err := os.WriteFile(nameFile, []byte(name+"\n"), 0644); err != nil {
		log.Printf("[Config] Warning: could not persist node name to %s: %v", nameFile, err)
	} else {
		log.Printf("[Config] Persisted node name to %s: %s", nameFile, name)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Config and BadBits
	_ = badbits.LoadBadBits(config.BadBitsPath)
	config.LogConfiguration()

	if msg := config.ValidatePathSafety(); msg != "" {
		log.Fatalf("[Fatal] Unsafe path configuration: %s", msg)
	}

	nodeName := resolveNodeName()
	if nodeName != "" {
		log.Printf("--- Node Name: %s ---", nodeName)
	}

	// IPFS client and DHT (required)
	ipfsClient, err := ipfs.NewClient(config.IPFSNodeAddress)
	if err != nil {
		log.Fatalf("[Fatal] IPFS not available: %v (start IPFS with: ipfs daemon)", err)
	}
	shell := ipfsClient.GetShell()
	dht := ipfs.NewIPFSDHTAdapter(shell)

	// Libp2p identity: use IPFS repo identity when IPFS_PATH is set so D-LOCKSS and IPFS share one peer ID per node.
	privKey, err := loadIdentity()
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

	log.Printf("--- Node ID: %s ---", h.ID().String())

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
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Connect(ctx, *peerinfo); err != nil {
				// log.Printf("Bootstrap warning: %s", err)
			}
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
	case <-time.After(config.BootstrapTimeout):
		log.Printf("[Config] Bootstrap timeout after %v, proceeding (some peers may not be connected)", config.BootstrapTimeout)
	}

	// Setup Routing Discovery
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, config.DiscoveryServiceTag)
	log.Printf("[Config] Advertising service on DHT: %s", config.DiscoveryServiceTag)

	// mDNS discovery so nodes and monitor find each other on the same LAN (same tag as monitor)
	notifee := &discovery.DiscoveryNotifee{H: h, Ctx: ctx}
	mdnsSvc := mdns.NewMdnsService(h, config.DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		log.Printf("[Config] mDNS start failed: %v (peer/monitor discovery limited)", err)
	}

	// Find peers (e.g. monitor)
	go func() {
		for {
			peerChan, err := routingDiscovery.FindPeers(ctx, config.DiscoveryServiceTag)
			if err != nil {
				log.Printf("[Discovery] FindPeers error: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}
			for peer := range peerChan {
				if peer.ID == h.ID() {
					continue
				}
				if h.Network().Connectedness(peer.ID) != network.Connected {
					h.Connect(ctx, peer)
				}
			}
			time.Sleep(30 * time.Second)
		}
	}()

	// Trust (optional: load peers if file exists)
	trustMgr := trust.NewTrustManager(config.TrustMode)
	if err := trustMgr.LoadTrustedPeers(config.TrustStorePath); err != nil && !os.IsNotExist(err) {
		log.Printf("[Config] Trust store load failed: %v", err)
	}

	// Initialize persistent datastore for cluster state
	// We place this OUTSIDE the FileWatchFolder (ingest dir) to avoid the node trying to ingest its own database files.
	// Path is configurable via DLOCKSS_CLUSTER_STORE; otherwise derived from FileWatchFolder (instance-specific when data dirs differ).
	dstore, err := leveldb.NewDatastore(config.ClusterStorePath, nil)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create datastore at %s: %v", config.ClusterStorePath, err)
	}
	defer dstore.Close()

	nonceStore := common.NewNonceStore()
	rateLimiter := common.NewRateLimiter()
	metrics := telemetry.NewMetricsManager()
	storageMgr := storage.NewStorageManager(dht, metrics)
	signer := signing.NewSigner(h, privKey, h.ID(), nonceStore, trustMgr, dht)

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
			pctx, pcancel := context.WithTimeout(context.Background(), config.DHTProvideTimeout)
			defer pcancel()
			storageMgr.ProvideFile(pctx, manifestCIDStr)
		}()
		// Resolve payload from manifest and provide it separately so payload has N providers.
		go func() {
			pctx, pcancel := context.WithTimeout(context.Background(), config.DHTProvideTimeout)
			defer pcancel()
			manifestCID, err := cid.Decode(manifestCIDStr)
			if err != nil {
				return
			}
			block, err := ipfsClient.GetBlock(pctx, manifestCID)
			if err != nil {
				log.Printf("[DHT] Failed to resolve payload from manifest %s: %v", manifestCIDStr, err)
				return
			}
			var ro schema.ResearchObject
			if err := ro.UnmarshalCBOR(block); err != nil {
				return
			}
			if ro.HasLegacyTimestamp {
				return
			}
			payloadStr := ro.Payload.String()
			if payloadStr != "" {
				storageMgr.ProvideFile(pctx, payloadStr)
			}
		}()
	}
	onPinRemoved := func(cid string) {
		storageMgr.UnpinFile(cid)
	}
	clusterMgr := clusters.NewClusterManager(h, ps, dht, dstore, ipfsClient, trustMgr.GetTrustedPeers(), onPinSynced, onPinRemoved)
	shardMgr := shard.NewShardManager(ctx, h, ps, ipfsClient, storageMgr, metrics, signer, rateLimiter, clusterMgr, "", nodeName)
	clusterMgr.SetShardPeerProvider(shardMgr) // CRDT Peers() and allocations use real shard membership
	announcePinned = shardMgr.AnnouncePinned

	metrics.RegisterProviders(shardMgr, storageMgr, rateLimiter)
	metrics.RegisterClusterProvider(clusterMgr) // cluster-style metrics: pins/peers/allocations per shard
	metrics.SetPeerID(h.ID().String())

	// Telemetry and API
	tc := telemetry.NewTelemetryClient(h, ps, metrics)
	if tc != nil {
		tc.SetShardPublisher(shardMgr, shardMgr)
		tc.Start(ctx)
	}
	apiServer := api.NewAPIServer(config.APIPort, metrics)
	apiServer.Start()

	// File processor and watcher
	fp := fileops.NewFileProcessor(ipfsClient, shardMgr, storageMgr, privKey, signer)
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

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[System] Shutting down...")
	cancel()
	shardMgr.Close()
	_ = apiServer.Shutdown(context.Background())
}

// ipfsConfigIdentity is the Identity section of IPFS config (Identity.PrivKey is base64-encoded libp2p key).
type ipfsConfigIdentity struct {
	PrivKey string `json:"PrivKey"`
}

type ipfsConfig struct {
	Identity ipfsConfigIdentity `json:"Identity"`
}

// loadIdentityFromIPFSRepo reads the private key from the IPFS repo config ($IPFS_PATH/config).
// Kubo stores Identity.PrivKey as base64-encoded libp2p protobuf. Returns the key or an error.
func loadIdentityFromIPFSRepo() (crypto.PrivKey, error) {
	ipfsPath := os.Getenv("IPFS_PATH")
	if ipfsPath == "" {
		return nil, fmt.Errorf("IPFS_PATH not set")
	}
	configPath := filepath.Join(ipfsPath, "config")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read IPFS config: %w", err)
	}
	var cfg ipfsConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse IPFS config: %w", err)
	}
	if cfg.Identity.PrivKey == "" {
		return nil, fmt.Errorf("IPFS config has no Identity.PrivKey")
	}
	raw, err := base64.StdEncoding.DecodeString(cfg.Identity.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("decode Identity.PrivKey: %w", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("unmarshal IPFS private key: %w", err)
	}
	return priv, nil
}

// loadIdentity returns the node identity: from IPFS repo when IPFS_PATH is set (one peer ID per node), otherwise from dlockss.key or new key.
func loadIdentity() (crypto.PrivKey, error) {
	priv, err := loadIdentityFromIPFSRepo()
	if err == nil {
		log.Printf("[Config] Using IPFS repo identity (single peer ID per node)")
		return priv, nil
	}
	// Fallback: dlockss.key or generate (e.g. remote IPFS, or IPFS_PATH not set)
	return loadOrCreateIdentity()
}

func loadOrCreateIdentity() (crypto.PrivKey, error) {
	identityPath := config.IdentityPath

	if _, err := os.Stat(identityPath); err == nil {
		data, err := os.ReadFile(identityPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read identity file: %w", err)
		}
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal identity: %w", err)
		}
		log.Printf("[Config] Loaded persistent identity from %s", identityPath)
		return priv, nil
	}

	// Migrate legacy key from CWD if it exists there but not at the configured path.
	if legacyPath := "dlockss.key"; legacyPath != identityPath {
		if _, err := os.Stat(legacyPath); err == nil {
			data, err := os.ReadFile(legacyPath)
			if err == nil {
				if dir := filepath.Dir(identityPath); dir != "." {
					_ = os.MkdirAll(dir, 0755)
				}
				if err := os.WriteFile(identityPath, data, 0600); err == nil {
					log.Printf("[Config] Migrated legacy identity from %s to %s", legacyPath, identityPath)
					priv, err := crypto.UnmarshalPrivateKey(data)
					if err != nil {
						return nil, fmt.Errorf("failed to unmarshal migrated identity: %w", err)
					}
					return priv, nil
				}
			}
		}
	}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal identity: %w", err)
	}

	if dir := filepath.Dir(identityPath); dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	if err := os.WriteFile(identityPath, data, 0600); err != nil {
		log.Printf("[Config] Warning: Failed to save identity to %s: %v", identityPath, err)
	} else {
		log.Printf("[Config] Saved new persistent identity to %s", identityPath)
	}

	return privKey, nil
}
