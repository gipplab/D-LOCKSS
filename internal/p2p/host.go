// Package p2p builds the node's libp2p stack: host, DHT, peer discovery,
// bootstrap, and the optional bandwidth snapshot writer.
package p2p

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/pbnjay/memory"

	"dlockss/internal/config"
)

// Connection-manager bounds and the public-bootstrap dial budget (frozen).
const (
	connLowWater     = 160
	connHighWater    = 192
	bootstrapTimeout = 15 * time.Second
)

// Node bundles the libp2p stack.
type Node struct {
	Host host.Host
	DHT  *kaddht.IpfsDHT
	rcm  interface{ Close() error }
}

// NewNode builds the host and its Kademlia DHT (used for peer rendezvous;
// content provides go through Kubo's DHT instead).
func NewNode(ctx context.Context, cfg *config.Config, key crypto.PrivKey) (*Node, error) {
	// Scale the default resource limits to the machine, with a floor so
	// low-memory nodes do not block shard coordination on limit errors.
	limits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&limits)
	mem := int64(memory.TotalMemory() / 8)
	if floor := int64(384 << 20); mem < floor {
		mem = floor
	}
	limiter := rcmgr.NewFixedLimiter(limits.Scale(mem, 512))
	rcm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, fmt.Errorf("resource manager: %w", err)
	}

	cm, err := connmgr.NewConnManager(connLowWater, connHighWater)
	if err != nil {
		return nil, fmt.Errorf("connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(key),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.Security(noise.ID, noise.New),
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rcm),
	}
	if !cfg.PrivateNetwork {
		opts = append(opts,
			libp2p.NATPortMap(),
			libp2p.EnableHolePunching(),
			libp2p.EnableAutoRelayWithStaticRelays(kaddht.GetDefaultBootstrapPeerAddrInfos()),
			libp2p.EnableNATService(),
		)
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		rcm.Close()
		return nil, fmt.Errorf("libp2p host: %w", err)
	}

	dhtOpts := []kaddht.Option{}
	if cfg.DHTClient {
		dhtOpts = append(dhtOpts, kaddht.Mode(kaddht.ModeClient))
	}
	dht, err := kaddht.New(ctx, h, dhtOpts...)
	if err != nil {
		h.Close()
		rcm.Close()
		return nil, fmt.Errorf("kad-dht: %w", err)
	}
	if err := dht.Bootstrap(ctx); err != nil {
		slog.Warn("DHT bootstrap failed", "err", err)
	}
	return &Node{Host: h, DHT: dht, rcm: rcm}, nil
}

// Close tears the stack down.
func (n *Node) Close() error {
	err := n.Host.Close()
	if cerr := n.rcm.Close(); err == nil {
		err = cerr
	}
	return err
}

// ConnectBootstrapPeers dials the public IPFS bootstrap peers in parallel,
// bounded by the dial budget. Failures are warnings: the DHT and mDNS keep
// working regardless.
func ConnectBootstrapPeers(ctx context.Context, h host.Host) {
	ctx, cancel := context.WithTimeout(ctx, bootstrapTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, info := range kaddht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(info peer.AddrInfo) {
			defer wg.Done()
			if err := h.Connect(ctx, info); err != nil {
				slog.Debug("bootstrap dial failed", "peer", info.ID, "err", err)
			}
		}(info)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("bootstrap dialing timed out; continuing")
	}
}

