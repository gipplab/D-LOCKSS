package discovery

import (
	"context"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
)

type DiscoveryNotifee struct {
	H   host.Host
	Ctx context.Context
}

func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.H.Network().Connectedness(pi.ID) != network.Connected {
		if err := n.H.Connect(n.Ctx, pi); err != nil {
			log.Printf("[Discovery] Failed to connect to peer %s: %v", pi.ID.String(), err)
		}
	}
}

// RunPeerFinder periodically discovers and connects to peers via DHT routing.
// Blocks until ctx is cancelled.
func RunPeerFinder(ctx context.Context, h host.Host, rd *routing.RoutingDiscovery, serviceTag string) {
	for {
		peerChan, err := rd.FindPeers(ctx, serviceTag)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Discovery] FindPeers error: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}
		for p := range peerChan {
			if p.ID == h.ID() {
				continue
			}
			if h.Network().Connectedness(p.ID) != network.Connected {
				_ = h.Connect(ctx, p)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}
