package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	"dlockss/pkg/ipfs"
)

// loadIPFSIdentity attempts to load the IPFS node's private key to use as D-LOCKSS identity
func loadIPFSIdentity() (crypto.PrivKey, error) {
	// Try to find IPFS config file
	ipfsPath := os.Getenv("IPFS_PATH")
	
	// If IPFS_PATH is set but is a relative path, convert it to absolute
	if ipfsPath != "" && !filepath.IsAbs(ipfsPath) {
		absPath, err := filepath.Abs(ipfsPath)
		if err == nil {
			ipfsPath = absPath
			log.Printf("[Config] Resolved IPFS_PATH to absolute path: %s", ipfsPath)
		}
	}
	
	if ipfsPath == "" {
		// Try to infer from IPFS node address if available
		// In testnet, IPFS repos are often in a predictable location relative to the node
		// Check common testnet patterns: ./ipfs_repo, ../ipfs_repo, etc.
		cwd, err := os.Getwd()
		if err == nil {
			// Check if we're in a testnet node directory
			testPaths := []string{
				filepath.Join(cwd, "ipfs_repo"),
				filepath.Join(cwd, "../ipfs_repo"),
				filepath.Join(cwd, "..", "..", "ipfs_repo"),
			}
			for _, testPath := range testPaths {
				if _, err := os.Stat(filepath.Join(testPath, "config")); err == nil {
					ipfsPath = testPath
					log.Printf("[Config] Auto-detected IPFS repo at: %s", ipfsPath)
					break
				}
			}
		}
		
		// Fallback to default home directory
		if ipfsPath == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("cannot determine home directory: %w", err)
			}
			ipfsPath = filepath.Join(homeDir, ".ipfs")
		}
	}
	
	configPath := filepath.Join(ipfsPath, "config")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read IPFS config at %s: %w", configPath, err)
	}
	
	var config struct {
		Identity struct {
			PrivKey string `json:"PrivKey"`
		} `json:"Identity"`
	}
	
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("cannot parse IPFS config: %w", err)
	}
	
	if config.Identity.PrivKey == "" {
		return nil, fmt.Errorf("IPFS config has no PrivKey")
	}
	
	// Decode base64 private key
	keyBytes, err := base64.StdEncoding.DecodeString(config.Identity.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("cannot decode IPFS private key: %w", err)
	}
	
	// Unmarshal libp2p private key
	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal IPFS private key: %w", err)
	}
	
	return privKey, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Try to use IPFS identity, fallback to generating new one
	var libp2pOpts []libp2p.Option
	ipfsPrivKey, err := loadIPFSIdentity()
	if err != nil {
		log.Printf("[Config] Could not load IPFS identity (%v), generating new D-LOCKSS identity", err)
		log.Printf("[Config] Note: D-LOCKSS and IPFS will have different peer IDs")
	} else {
		log.Printf("[Config] Using IPFS identity - D-LOCKSS and IPFS will share the same peer ID")
		libp2pOpts = append(libp2pOpts, libp2p.Identity(ipfsPrivKey))
	}
	
	libp2pOpts = append(libp2pOpts,
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		// No DHT routing - we'll use IPFS's DHT instead
	)
	
	h, err := libp2p.New(libp2pOpts...)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize libp2p host: %v", err)
	}

	fmt.Printf("--- Node ID: %s ---\n", h.ID().String())
	fmt.Printf("--- Addresses: %v ---\n", h.Addrs())

	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h, ctx: ctx})
	if err := mdnsSvc.Start(); err != nil {
		log.Fatalf("[Fatal] Failed to start mDNS service: %v", err)
	}

	// DHT bootstrap removed - IPFS DHT doesn't need separate bootstrap

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
		log.Fatalf("[Fatal] Failed to initialize IPFS client: %v", err)
		log.Fatalf("[Fatal] D-LOCKSS now requires IPFS node to be running at %s", IPFSNodeAddress)
	}
	log.Printf("[System] IPFS client initialized successfully")
	
	// Create IPFS DHT adapter (uses IPFS's DHT via HTTP API)
	ipfsDHTAdapter := ipfs.NewIPFSDHTAdapterWithRetry(ipfsClient.GetShell(), DHTProvideRetryAttempts, DHTProvideRetryDelay)
	globalDHT = ipfsDHTAdapter
	log.Printf("[System] Using IPFS DHT for provider lookups and announcements")

	// Create managers
	storageMgr = NewStorageManager(globalDHT)

	// Start all nodes in root shard "" - they will split naturally when threshold is exceeded
	initialShard := ""
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
		telemetryClient := NewTelemetryClient(h, ps, globalDHT, MonitorPeerID)
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
	// IPFS DHT adapter doesn't need explicit close (uses HTTP API)
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
