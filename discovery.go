package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
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

		// V2 console helpers (not network messages):
		// - resolve <ManifestCID>
		// - bibtex <ManifestCID>
		// - refs <ManifestCID> [depth]
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
		if cmd == "refs" && len(fields) >= 2 {
			depth := 1
			if len(fields) >= 3 {
				if n, err := strconv.Atoi(fields[2]); err == nil {
					depth = n
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			tree, edges, err := ResolveReferenceTree(ctx, fields[1], depth)
			cancel()
			if err != nil {
				log.Printf("[Citation] Refs failed: %v", err)
				continue
			}
			fmt.Print(FormatRefTree(tree))
			fmt.Println("Edges:")
			for _, e := range edges {
				fmt.Printf("  %s -> %s\n", e[0], e[1])
			}
			continue
		}

		// Default behavior: publish raw line (useful for manual testing)
		sm.PublishToShard(line)
	}
}
