// Command dlockss runs a D-LOCKSS node: file ingestion, sharding, replication, and observability.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"dlockss/internal/api"
	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/discovery"
	"dlockss/internal/fileops"
	"dlockss/internal/managers/replication"
	"dlockss/internal/managers/shard"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/internal/telemetry"
	"dlockss/internal/trust"
	"dlockss/pkg/ipfs"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Config and BadBits
	_ = badbits.LoadBadBits(config.BadBitsPath)
	config.LogConfiguration()

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
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		// Prioritize Noise over TLS to avoid handshake issues during simultaneous connect
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

	// mDNS discovery so nodes and monitor find each other on the same LAN (same tag as monitor)
	notifee := &discovery.DiscoveryNotifee{H: h, Ctx: ctx}
	mdnsSvc := mdns.NewMdnsService(h, config.DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		log.Printf("[Config] mDNS start failed: %v (peer/monitor discovery limited)", err)
	}

	// Trust (optional: load peers if file exists)
	trustMgr := trust.NewTrustManager(config.TrustMode)
	if err := trustMgr.LoadTrustedPeers(config.TrustStorePath); err != nil && !os.IsNotExist(err) {
		log.Printf("[Config] Trust store load failed: %v", err)
	}

	// Initialize persistent datastore for cluster state
	// We place this OUTSIDE the FileWatchFolder (ingest dir) to avoid the node trying to ingest its own database files.
	dsPath := "cluster_store"
	dstore, err := leveldb.NewDatastore(dsPath, nil)
	if err != nil {
		log.Fatalf("[Fatal] Failed to create datastore at %s: %v", dsPath, err)
	}
	defer dstore.Close()

	nonceStore := common.NewNonceStore()
	rateLimiter := common.NewRateLimiter()
	metrics := telemetry.NewMetricsManager()
	storageMgr := storage.NewStorageManager(dht, metrics)
	signer := signing.NewSigner(h, privKey, h.ID(), nonceStore, trustMgr, dht)

	// Shard manager (replication set later to break cycle)
	shardMgr := shard.NewShardManager(ctx, h, ps, ipfsClient, storageMgr, metrics, signer, rateLimiter, dstore, dht, "")
	repMgr := replication.NewReplicationManager(ipfsClient, shardMgr, storageMgr, metrics, signer, dht)
	shardMgr.SetReplicationManager(repMgr)

	metrics.RegisterProviders(shardMgr, storageMgr, repMgr, rateLimiter)
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
	fp := fileops.NewFileProcessor(ipfsClient, shardMgr, storageMgr, privKey)
	fp.ScanExistingFiles()
	go fp.WatchFolder(ctx)

	// Run managers
	shardMgr.Run()
	repMgr.StartReplicationPipeline(ctx)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[System] Shutting down...")
	cancel()
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
	identityPath := "dlockss.key"
	if envPath := os.Getenv("DLOCKSS_IDENTITY_PATH"); envPath != "" {
		identityPath = envPath
	}

	if _, err := os.Stat(identityPath); err == nil {
		data, err := os.ReadFile(identityPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read identity file: %w", err)
		}
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal identity: %w", err)
		}
		log.Printf("[Config] Loaded persistent identity from %s (IPFS_PATH unset, two peer IDs if using separate IPFS)", identityPath)
		return priv, nil
	}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal identity: %w", err)
	}

	if err := os.WriteFile(identityPath, data, 0600); err != nil {
		log.Printf("[Config] Warning: Failed to save identity to %s: %v", identityPath, err)
	} else {
		log.Printf("[Config] Saved new persistent identity to %s", identityPath)
	}

	return privKey, nil
}
