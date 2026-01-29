package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const TelemetryProtocol = protocol.ID("/dlockss/telemetry/1.0.0")
const DiscoveryServiceTag = "dlockss-prod" // Must match config.go default

// Reusing struct definitions (copy-paste for simplicity in separate binary)
type StatusResponse struct {
	PeerID          string  `json:"peer_id"`
	Version         string  `json:"version"`
	CurrentShard    string  `json:"current_shard"`
	PeersInShard    int     `json:"peers_in_shard"`
	Storage         StorageStatus `json:"storage"`
	Replication     ReplicationStatus `json:"replication"`
	UptimeSeconds   float64 `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles int `json:"pinned_files"`
	KnownFiles  int `json:"known_files"`
}

type ReplicationStatus struct {
	QueueDepth      int `json:"queue_depth"`
	ActiveWorkers   int `json:"active_workers"`
}

type NodeState struct {
	Data     *StatusResponse
	LastSeen time.Time
}

type Monitor struct {
	mu    sync.Mutex
	nodes map[string]*NodeState
}

type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// Monitor doesn't strictly need to connect proactively, 
	// but it helps establish the mesh.
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		// Attempt connection (best effort)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		n.h.Connect(ctx, pi)
	}
}

func main() {
	// Create a libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		log.Fatalf("Failed to create host: %v", err)
	}

	// Start mDNS to be discoverable by local nodes
	notifee := &discoveryNotifee{h: h}
	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		log.Fatalf("Failed to start mDNS: %v", err)
	}

	monitor := &Monitor{
		nodes: make(map[string]*NodeState),
	}

	// Set stream handler
	h.SetStreamHandler(TelemetryProtocol, func(s network.Stream) {
		defer s.Close()
		
		// Use GZIP reader
		gr, err := gzip.NewReader(s)
		if err != nil {
			log.Printf("Gzip error: %v", err)
			return
		}
		defer gr.Close()

		var status StatusResponse
		if err := json.NewDecoder(gr).Decode(&status); err != nil {
			log.Printf("Decode error: %v", err)
			return
		}

		monitor.mu.Lock()
		monitor.nodes[status.PeerID] = &NodeState{
			Data:     &status,
			LastSeen: time.Now(),
		}
		monitor.mu.Unlock()
	})

	fmt.Printf("--- D-LOCKSS MONITOR ---\n")
	fmt.Printf("Monitor PeerID: %s\n", h.ID())
	fmt.Printf("Discovery Tag: %s\n", DiscoveryServiceTag)
	fmt.Printf("Run nodes with: export DLOCKSS_MONITOR_PEER_ID=%s\n", h.ID())
	fmt.Printf("------------------------\n\n")

	// TUI Loop
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-sigCh:
			return
		case <-ticker.C:
			refreshDashboard(monitor)
		}
	}
}

func refreshDashboard(m *Monitor) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cleanup stale nodes (> 2 missed reports = 2.5 minutes timeout)
	now := time.Now()
	timeout := 2 * time.Minute + 30*time.Second 
	
	for id, node := range m.nodes {
		if now.Sub(node.LastSeen) > timeout {
			delete(m.nodes, id)
		}
	}

	// Clear screen (ANSI escape)
	fmt.Print("\033[H\033[2J")
	
	fmt.Printf("--- D-LOCKSS NETWORK STATUS (%s) ---\n", now.Format("15:04:05"))
	fmt.Printf("Total Nodes: %d\n\n", len(m.nodes))

	fmt.Printf("%-20s | %-5s | %-5s | %-10s | %-10s | %-8s\n", "PeerID", "Shard", "Peers", "Pinned", "Known", "Last Seen")
	fmt.Println("---------------------+-------+-------+------------+------------+---------")

	for _, nodeState := range m.nodes {
		node := nodeState.Data
		shortID := node.PeerID
		if len(shortID) > 10 {
			shortID = shortID[len(shortID)-10:]
		}
		
		lastSeen := time.Since(nodeState.LastSeen).Round(time.Second)
		
		fmt.Printf("...%-17s | %-5s | %-5d | %-10d | %-10d | %s ago\n",
			shortID,
			node.CurrentShard,
			node.PeersInShard,
			node.Storage.PinnedFiles,
			node.Storage.KnownFiles,
			lastSeen,
		)
	}
}
