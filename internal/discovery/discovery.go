package discovery

import (
	"context"
	"log"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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
