package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dlockss/internal/discovery"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const (
	meshMaintenanceInterval = 10 * time.Minute
	bootstrapConnectTimeout = 15 * time.Second
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
	monitor.appCtx = ctx
	monitor.subCtx, monitor.subCancel = context.WithCancel(ctx)
	go monitor.cleanupStaleCIDs(ctx)
	monitor.resubscribeBootstrap()
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
	dutil.Advertise(ctx, routingDiscovery, discoveryServiceTag)
	slog.Info("advertising service", "tag", discoveryServiceTag)

	notifee := &discovery.DiscoveryNotifee{H: h, Ctx: ctx}
	mdnsSvc := mdns.NewMdnsService(h, discoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		slog.Warn("mdns start failed", "error", err)
	}

	go discovery.RunPeerFinder(ctx, h, routingDiscovery, discoveryServiceTag)
	go runMeshMaintenance(ctx, h, kademliaDHT, routingDiscovery)

	return h, nil
}

// runMeshMaintenance periodically re-bootstraps the DHT, reconnects to
// bootstrap peers, and re-advertises the service tag. This recovers from
// GossipSub mesh degradation that can accumulate over days of runtime —
// without it the monitor silently stops receiving heartbeats and prunes
// all nodes, requiring a full restart to recover.
func runMeshMaintenance(ctx context.Context, h host.Host, kademliaDHT *dht.IpfsDHT, rd *routing.RoutingDiscovery) {
	ticker := time.NewTicker(meshMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		connected := 0
		for _, pid := range h.Network().Peers() {
			if h.Network().Connectedness(pid) == network.Connected {
				connected++
			}
		}

		if err := kademliaDHT.Bootstrap(ctx); err != nil {
			slog.Warn("mesh maintenance: DHT re-bootstrap failed", "error", err)
		}

		reconnected := 0
		for _, peerAddr := range dht.DefaultBootstrapPeers {
			peerinfo, err := peer.AddrInfoFromP2pAddr(peerAddr)
			if err != nil {
				continue
			}
			if h.Network().Connectedness(peerinfo.ID) == network.Connected {
				continue
			}
			connCtx, cancel := context.WithTimeout(ctx, bootstrapConnectTimeout)
			if err := h.Connect(connCtx, *peerinfo); err == nil {
				reconnected++
			}
			cancel()
		}

		dutil.Advertise(ctx, rd, discoveryServiceTag)

		slog.Info("mesh maintenance complete",
			"connected_peers", connected,
			"bootstrap_reconnected", reconnected,
		)
	}
}
