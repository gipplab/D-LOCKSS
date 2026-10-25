package monitor

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"dlockss/internal/p2p"
)

const (
	identityFileName    = "monitor_identity.key"
	discoveryTag        = "dlockss-prod"
	meshMaintainEvery   = 10 * time.Minute
	bootstrapDialEach   = 15 * time.Second
	peerstoreGCInterval = 5 * time.Minute
)

// Net is the monitor's libp2p stack.
type Net struct {
	Host   host.Host
	DHT    *kaddht.IpfsDHT
	PubSub *pubsub.PubSub
}

// loadIdentity reads or creates the monitor's persistent key: a
// monitor_identity.key in the working directory wins, else one under the
// data directory.
func loadIdentity(dataDir string) (crypto.PrivKey, error) {
	if raw, err := os.ReadFile(identityFileName); err == nil {
		if key, err := crypto.UnmarshalPrivateKey(raw); err == nil {
			return key, nil
		}
	}
	path := filepath.Join(dataDir, identityFileName)
	if raw, err := os.ReadFile(path); err == nil {
		if key, err := crypto.UnmarshalPrivateKey(raw); err == nil {
			return key, nil
		}
	}
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	raw, err := crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o700); err == nil {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			slog.Warn("could not persist monitor identity", "err", err)
		}
	}
	return key, nil
}

// NewNet builds the monitor's host: TCP+QUIC on random ports, NAT helpers,
// GossipSub, a bootstrapped DHT, and tag discovery. A maintenance loop keeps
// the mesh healthy over long uptimes.
func NewNet(ctx context.Context, dataDir string) (*Net, error) {
	key, err := loadIdentity(dataDir)
	if err != nil {
		return nil, err
	}
	h, err := libp2p.New(
		libp2p.Identity(key),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip6/::/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
		libp2p.EnableNATService(),
	)
	if err != nil {
		return nil, err
	}
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		h.Close()
		return nil, err
	}
	dht, err := kaddht.New(ctx, h)
	if err != nil {
		h.Close()
		return nil, err
	}
	if err := dht.Bootstrap(ctx); err != nil {
		slog.Warn("monitor DHT bootstrap failed", "err", err)
	}
	p2p.ConnectBootstrapPeers(ctx, h)
	p2p.StartDiscovery(ctx, h, dht, discoveryTag)

	n := &Net{Host: h, DHT: dht, PubSub: ps}
	go n.maintain(ctx)
	return n, nil
}

// maintain re-bootstraps and reconnects periodically: without it the
// GossipSub mesh degrades over days and the monitor prunes every node.
func (n *Net) maintain(ctx context.Context) {
	ticker := time.NewTicker(meshMaintainEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := n.DHT.Bootstrap(ctx); err != nil {
				slog.Debug("mesh maintenance: bootstrap", "err", err)
			}
			for _, info := range kaddht.GetDefaultBootstrapPeerAddrInfos() {
				if n.Host.Network().Connectedness(info.ID).String() == "Connected" {
					continue
				}
				cctx, cancel := context.WithTimeout(ctx, bootstrapDialEach)
				_ = n.Host.Connect(cctx, info)
				cancel()
			}
		}
	}
}

// PeerIP returns a display IP for a peer, preferring public addresses over
// private, loopback and link-local ones.
func (n *Net) PeerIP(p peer.ID) string {
	best := ""
	for _, addr := range n.Host.Peerstore().Addrs(p) {
		ip := addrIP(addr)
		if ip == nil {
			continue
		}
		if isPublicIP(ip) {
			return ip.String()
		}
		if best == "" {
			best = ip.String()
		}
	}
	return best
}

func addrIP(addr ma.Multiaddr) net.IP {
	for _, proto := range []int{ma.P_IP4, ma.P_IP6} {
		if v, err := addr.ValueForProtocol(proto); err == nil {
			return net.ParseIP(v)
		}
	}
	return nil
}

func isPublicIP(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified())
}

// RunPeerstoreGC evicts peers that are disconnected and untracked, keeping
// anyone still present in a subscribed topic's peer list.
func (n *Net) RunPeerstoreGC(ctx context.Context, tracked func(peer.ID) bool, topicPeers func() map[peer.ID]struct{}) {
	ticker := time.NewTicker(peerstoreGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			keep := topicPeers()
			for _, p := range n.Host.Peerstore().Peers() {
				if p == n.Host.ID() || tracked(p) {
					continue
				}
				if _, ok := keep[p]; ok {
					continue
				}
				if n.Host.Network().Connectedness(p).String() == "Connected" {
					continue
				}
				n.Host.Peerstore().RemovePeer(p)
			}
		}
	}
}
