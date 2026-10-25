package p2p

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
)

const (
	peerFinderInterval      = 30 * time.Second
	peerFinderErrorInterval = 10 * time.Second
)

// notifee connects to peers found via mDNS.
type notifee struct {
	h host.Host
}

func (n *notifee) HandlePeerFound(info peer.AddrInfo) {
	if info.ID == n.h.ID() || n.h.Network().Connectedness(info.ID).String() == "Connected" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.h.Connect(ctx, info); err != nil {
		slog.Debug("mdns connect failed", "peer", info.ID, "err", err)
	}
}

// StartDiscovery advertises the service tag on the DHT, starts mDNS with the
// same tag, and runs a peer-finder loop that keeps connecting to advertised
// peers.
func StartDiscovery(ctx context.Context, h host.Host, dht *kaddht.IpfsDHT, tag string) {
	rd := drouting.NewRoutingDiscovery(dht)
	dutil.Advertise(ctx, rd, tag)

	svc := mdns.NewMdnsService(h, tag, &notifee{h: h})
	if err := svc.Start(); err != nil {
		slog.Warn("mDNS start failed", "err", err)
	}
	go func() {
		<-ctx.Done()
		_ = svc.Close()
	}()

	go runPeerFinder(ctx, h, rd, tag)
}

func runPeerFinder(ctx context.Context, h host.Host, rd *drouting.RoutingDiscovery, tag string) {
	for {
		wait := peerFinderInterval
		peers, err := rd.FindPeers(ctx, tag)
		if err != nil {
			wait = peerFinderErrorInterval
		} else {
			for info := range peers {
				if info.ID == h.ID() || len(info.Addrs) == 0 ||
					h.Network().Connectedness(info.ID).String() == "Connected" {
					continue
				}
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := h.Connect(cctx, info); err != nil {
					slog.Debug("discovery connect failed", "peer", info.ID, "err", err)
				}
				cancel()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
