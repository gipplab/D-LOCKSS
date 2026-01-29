package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	"dlockss/pkg/ipfs"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			globalDHT, err = dht.New(ctx, h, dht.Mode(dht.ModeServer))
			return globalDHT, err
		}),
	)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize libp2p host: %v", err)
	}

	fmt.Printf("--- Node ID: %s ---\n", h.ID().String())
	fmt.Printf("--- Addresses: %v ---\n", h.Addrs())

	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h, ctx: ctx})
	if err := mdnsSvc.Start(); err != nil {
		log.Fatalf("[Fatal] Failed to start mDNS service: %v", err)
	}

	if err = globalDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("[Fatal] Failed to bootstrap DHT: %v", err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize GossipSub: %v", err)
	}

	selfPeerID = h.ID()
	selfPrivKey = h.Peerstore().PrivKey(h.ID())
	if selfPrivKey == nil {
		log.Printf("[Warning] Missing private key for self peer ID; message signing will be unavailable")
	}

	logConfiguration()

	// Initialize optional trust store (default trust mode is "open").
	// In allowlist mode, only peers listed in TrustStorePath may send protocol messages.
	if TrustStorePath != "" {
		if err := loadTrustedPeers(TrustStorePath); err != nil {
			log.Printf("[Trust] Trust store not loaded (%s): %v", TrustStorePath, err)
		} else {
			log.Printf("[Trust] Loaded trust store: %s", TrustStorePath)
		}
	}

	// Initialize IPFS client
	log.Println("Initializing IPFS client...")
	ipfsClient, err = ipfs.NewClient(IPFSNodeAddress)
	if err != nil {
		log.Printf("[Warning] Failed to initialize IPFS client: %v", err)
		log.Printf("[Warning] File operations will be disabled. Ensure IPFS node is running at %s", IPFSNodeAddress)
		// Continue without IPFS - file operations will fail gracefully
	} else {
		log.Printf("[System] IPFS client initialized successfully")
	}

	// Create managers
	storageMgr = NewStorageManager(globalDHT)

	initialShard := getBinaryPrefix(h.ID().String(), 1)
	shardMgr = NewShardManager(ctx, h, ps, ipfsClient, storageMgr, initialShard)

	replicationMgr = NewReplicationManager(ipfsClient, shardMgr, storageMgr, globalDHT)
	shardMgr.SetReplicationManager(replicationMgr)

	go shardMgr.Run()
	// Input loop removed in favor of API server
	
	// Start API Server
	apiPort := getEnvInt("DLOCKSS_API_PORT", 5050)
	apiServer := NewAPIServer(apiPort)
	apiServer.Start()

	// Start Telemetry Client
	if MonitorPeerID != "" {
		telemetryClient := NewTelemetryClient(h, globalDHT, MonitorPeerID)
		if telemetryClient != nil {
			telemetryClient.Start(ctx)
		}
	}

	fileProcessor := NewFileProcessor(ipfsClient, shardMgr, storageMgr, selfPrivKey)

	if err := os.Mkdir(FileWatchFolder, 0755); err != nil && !os.IsExist(err) {
		log.Printf("Error creating folder: %v", err)
	}

	log.Println("Loading badBits list...")
	if err := loadBadBits(BadBitsPath); err != nil {
		log.Printf("[Warning] Failed to load badBits: %v", err)
	}

	log.Println("Scanning for existing files...")
	fileProcessor.scanExistingFiles()

	go fileProcessor.watchFolder(ctx)

	go replicationMgr.startReplicationPipeline(ctx)
	go replicationMgr.runReplicationCacheCleanup(ctx)
	go runRateLimiterCleanup(ctx)
	go runMetricsReporter(ctx)
	go runDiskUsageMonitor(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Error] API server shutdown error: %v", err)
	}

	if shardMgr != nil {
		shardMgr.Close()
	}
	if globalDHT != nil {
		globalDHT.Close()
	}
	if h != nil {
		if err := h.Close(); err != nil {
			log.Printf("[Error] Error closing host: %v", err)
		}
	}

	<-shutdownCtx.Done()
	if shutdownCtx.Err() == context.DeadlineExceeded {
		log.Println("[Warning] Shutdown timeout exceeded, forcing exit")
	}

	log.Println("[System] Shutdown complete")
}
