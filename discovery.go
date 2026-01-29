package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type discoveryNotifee struct {
	h   host.Host
	ctx context.Context
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		if err := n.h.Connect(n.ctx, pi); err != nil {
			log.Printf("[Discovery] Failed to connect to peer %s: %v", pi.ID.String(), err)
		}
	}
}

func inputLoop(sm *ShardManager) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Console helpers:
		// - resolve <ManifestCID>
		// - bibtex <ManifestCID>
		fields := strings.Fields(line)
		cmd := strings.ToLower(fields[0])
		if cmd == "resolve" && len(fields) >= 2 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			c, _, err := ResolveCitation(ctx, fields[1])
			cancel()
			if err != nil {
				log.Printf("[Citation] Resolve failed: %v", err)
				continue
			}
			out, err := FormatCitationJSON(c)
			if err != nil {
				log.Printf("[Citation] JSON format failed: %v", err)
				continue
			}
			fmt.Println(out)
			continue
		}
		if cmd == "bibtex" && len(fields) >= 2 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			c, _, err := ResolveCitation(ctx, fields[1])
			cancel()
			if err != nil {
				log.Printf("[Citation] Resolve failed: %v", err)
				continue
			}
			fmt.Println(FormatBibTeX(c))
			continue
		}

		// Default behavior: publish raw line (useful for manual testing)
		shardID, _ := getShardInfo()
		sm.PublishToShard(shardID, line)
	}
}
