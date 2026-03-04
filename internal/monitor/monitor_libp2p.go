package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"dlockss/internal/discovery"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

func getMonitorIdentityPath() string {
	if cwd, err := os.Getwd(); err == nil {
		path := filepath.Join(cwd, MonitorIdentityFile)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return MonitorIdentityFile
	}
	monitorDir := filepath.Join(homeDir, ".dlockss-monitor")
	os.MkdirAll(monitorDir, 0700)
	return filepath.Join(monitorDir, MonitorIdentityFile)
}

func loadOrCreateMonitorIdentity() (crypto.PrivKey, error) {
	identityPath := getMonitorIdentityPath()
	if data, err := os.ReadFile(identityPath); err == nil {
		privKey, err := crypto.UnmarshalPrivateKey(data)
		if err == nil {
			slog.Info("loaded persistent identity", "path", identityPath)
			return privKey, nil
		}
	}
	slog.Info("generating new persistent identity")
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err == nil {
		os.WriteFile(identityPath, keyBytes, 0600)
		slog.Info("saved persistent identity", "path", identityPath)
	}
	return privKey, nil
}

func StartLibP2P(ctx context.Context, monitor *Monitor) (host.Host, error) {
	privKey, err := loadOrCreateMonitorIdentity()
	if err != nil {
		return nil, fmt.Errorf("failed to load/create identity: %w", err)
	}
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.EnableNATService(),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		return nil, err
	}
	slog.Info("libp2p started", "peer_id", h.ID().String())
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize PubSub: %w", err)
	}
	monitor.ps = ps
	monitor.host = h
	go monitor.cleanupStaleCIDs(ctx)
	// Bootstrap subscribe to all shards up to depth so we see nodes even when joining late
	// (when all nodes have already moved to deeper shards like 00, 01, 10, 11, etc.).
	for _, shardID := range shardIDsUpToDepth(monitor.cfg.BootstrapShardDepth) {
		monitor.ensureShardSubscription(ctx, shardID)
	}
	slog.Info("bootstrap subscribed", "shards", 1<<(monitor.cfg.BootstrapShardDepth+1)-1, "depth", monitor.cfg.BootstrapShardDepth)
	go monitor.subscribeToActiveShards(ctx)

	kademliaDHT, err := dht.New(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	var wg sync.WaitGroup
	for _, peerAddr := range dht.DefaultBootstrapPeers {
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
	wg.Wait()

	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, DiscoveryServiceTag)
	slog.Info("advertising service", "tag", DiscoveryServiceTag)

	notifee := &discovery.DiscoveryNotifee{H: h, Ctx: ctx}
	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		slog.Warn("mdns start failed", "error", err)
	}

	go discovery.RunPeerFinder(ctx, h, routingDiscovery, DiscoveryServiceTag)

	return h, nil
}
