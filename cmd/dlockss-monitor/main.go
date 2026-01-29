package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
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
const WebUIPort = 8080

// Reusing struct definitions (copy-paste for simplicity in separate binary)
type StatusResponse struct {
	PeerID        string            `json:"peer_id"`
	Version       string            `json:"version"`
	CurrentShard  string            `json:"current_shard"`
	PeersInShard  int               `json:"peers_in_shard"`
	Storage       StorageStatus     `json:"storage"`
	Replication   ReplicationStatus `json:"replication"`
	UptimeSeconds float64           `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles int `json:"pinned_files"`
	KnownFiles  int `json:"known_files"`
}

type ReplicationStatus struct {
	QueueDepth    int `json:"queue_depth"`
	ActiveWorkers int `json:"active_workers"`
}

type NodeState struct {
	Data         *StatusResponse
	LastSeen     time.Time
	ShardHistory []ShardHistoryEntry
}

type ShardHistoryEntry struct {
	ShardID  string
	FirstSeen time.Time
}

type ShardSplitEvent struct {
	ParentShard string    `json:"parent_shard"`
	ChildShard  string    `json:"child_shard"`
	Timestamp   time.Time `json:"timestamp"`
}

type ShardTreeNode struct {
	ShardID    string            `json:"shard_id"`
	SplitTime  *time.Time        `json:"split_time,omitempty"`
	Children   []*ShardTreeNode  `json:"children,omitempty"`
	NodeCount  int               `json:"node_count"`
}

type Monitor struct {
	mu           sync.Mutex
	nodes        map[string]*NodeState
	splitEvents  []ShardSplitEvent
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

func (m *Monitor) updateNodeShard(peerID string, newShard string, timestamp time.Time) {
	nodeState, exists := m.nodes[peerID]
	if !exists {
		return
	}

	// Check if shard changed
	if len(nodeState.ShardHistory) > 0 {
		lastShard := nodeState.ShardHistory[len(nodeState.ShardHistory)-1].ShardID
		if lastShard != newShard {
			// Shard changed - check if it's a split (new shard is longer and starts with old shard)
			if len(newShard) > len(lastShard) && newShard[:len(lastShard)] == lastShard {
				// This is a split!
				splitEvent := ShardSplitEvent{
					ParentShard: lastShard,
					ChildShard:  newShard,
					Timestamp:   timestamp,
				}
				m.splitEvents = append(m.splitEvents, splitEvent)
			}
			// Add new shard to history
			nodeState.ShardHistory = append(nodeState.ShardHistory, ShardHistoryEntry{
				ShardID:   newShard,
				FirstSeen: timestamp,
			})
		}
	} else {
		// First time seeing this node
		nodeState.ShardHistory = []ShardHistoryEntry{{
			ShardID:   newShard,
			FirstSeen: timestamp,
		}}
	}
}

func (m *Monitor) buildShardTree() *ShardTreeNode {
	// Count nodes per shard
	shardCounts := make(map[string]int)
	for _, nodeState := range m.nodes {
		if len(nodeState.ShardHistory) > 0 {
			currentShard := nodeState.ShardHistory[len(nodeState.ShardHistory)-1].ShardID
			shardCounts[currentShard]++
		}
	}

	// Build tree from split events
	root := &ShardTreeNode{
		ShardID:   "",
		Children:  []*ShardTreeNode{},
		NodeCount: 0,
	}

	// Map of shard ID -> node
	shardNodes := make(map[string]*ShardTreeNode)
	shardNodes[""] = root

	// Sort split events by timestamp
	sortedEvents := make([]ShardSplitEvent, len(m.splitEvents))
	copy(sortedEvents, m.splitEvents)
	sort.Slice(sortedEvents, func(i, j int) bool {
		return sortedEvents[i].Timestamp.Before(sortedEvents[j].Timestamp)
	})

	// Process split events chronologically
	for _, event := range sortedEvents {
		parent, exists := shardNodes[event.ParentShard]
		if !exists {
			// Create parent if it doesn't exist
			parent = &ShardTreeNode{
				ShardID:   event.ParentShard,
				Children:  []*ShardTreeNode{},
				NodeCount: shardCounts[event.ParentShard],
			}
			shardNodes[event.ParentShard] = parent
			// Add to root if parent is depth 1
			if len(event.ParentShard) == 1 {
				root.Children = append(root.Children, parent)
			} else {
				// Find grandparent
				grandparentID := event.ParentShard[:len(event.ParentShard)-1]
				if grandparent, ok := shardNodes[grandparentID]; ok {
					grandparent.Children = append(grandparent.Children, parent)
				}
			}
		}

		// Create child node
		splitTime := event.Timestamp
		child := &ShardTreeNode{
			ShardID:   event.ChildShard,
			SplitTime: &splitTime,
			Children:  []*ShardTreeNode{},
			NodeCount: shardCounts[event.ChildShard],
		}
		shardNodes[event.ChildShard] = child
		parent.Children = append(parent.Children, child)
	}

	// Add any shards that exist but haven't split yet (leaf nodes)
	for shardID, count := range shardCounts {
		if _, exists := shardNodes[shardID]; !exists {
			// This is a leaf shard (no splits recorded)
			parentID := ""
			if len(shardID) > 0 {
				parentID = shardID[:len(shardID)-1]
			}
			if parent, ok := shardNodes[parentID]; ok {
				leaf := &ShardTreeNode{
					ShardID:   shardID,
					Children:  []*ShardTreeNode{},
					NodeCount: count,
				}
				parent.Children = append(parent.Children, leaf)
				shardNodes[shardID] = leaf
			} else if len(shardID) == 1 {
				// Top-level shard
				leaf := &ShardTreeNode{
					ShardID:   shardID,
					Children:  []*ShardTreeNode{},
					NodeCount: count,
				}
				root.Children = append(root.Children, leaf)
				shardNodes[shardID] = leaf
			}
		}
	}

	// Update root node count
	root.NodeCount = len(m.nodes)

	return root
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
		nodes:       make(map[string]*NodeState),
		splitEvents: []ShardSplitEvent{},
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

		now := time.Now()
		monitor.mu.Lock()
		
		// Check if node exists
		nodeState, exists := monitor.nodes[status.PeerID]
		if !exists {
			nodeState = &NodeState{
				Data:         &status,
				LastSeen:     now,
				ShardHistory: []ShardHistoryEntry{},
			}
			monitor.nodes[status.PeerID] = nodeState
		} else {
			nodeState.Data = &status
			nodeState.LastSeen = now
		}

		// Update shard tracking
		monitor.updateNodeShard(status.PeerID, status.CurrentShard, now)
		
		monitor.mu.Unlock()
	})

	// Start HTTP server for Web UI
	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, r *http.Request) {
		monitor.mu.Lock()
		defer monitor.mu.Unlock()

		// Cleanup stale nodes
		now := time.Now()
		timeout := 2*time.Minute + 30*time.Second
		for id, node := range monitor.nodes {
			if now.Sub(node.LastSeen) > timeout {
				delete(monitor.nodes, id)
			}
		}

		// Build response with LastSeen timestamps
		response := make(map[string]interface{})
		for id, nodeState := range monitor.nodes {
			response[id] = map[string]interface{}{
				"data":      nodeState.Data,
				"last_seen": nodeState.LastSeen.Unix(),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
	
	mux.HandleFunc("/api/shard-tree", func(w http.ResponseWriter, r *http.Request) {
		monitor.mu.Lock()
		defer monitor.mu.Unlock()

		tree := monitor.buildShardTree()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tree)
	})
	
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(dashboardHTML))
	})

	go func() {
		log.Printf("[Monitor] Web UI available at http://localhost:%d", WebUIPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", WebUIPort), mux); err != nil {
			log.Printf("[Error] HTTP server failed: %v", err)
		}
	}()

	fmt.Printf("--- D-LOCKSS MONITOR ---\n")
	fmt.Printf("Monitor PeerID: %s\n", h.ID())
	fmt.Printf("Discovery Tag: %s\n", DiscoveryServiceTag)
	fmt.Printf("Web UI: http://localhost:%d\n", WebUIPort)
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
	timeout := 2*time.Minute + 30*time.Second

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

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
    <title>D-LOCKSS Network Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .stat-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat-value { font-size: 2em; font-weight: bold; color: #2196F3; }
        .stat-label { color: #666; margin-top: 5px; }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .node-table { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 20px 0; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f8f9fa; font-weight: bold; }
        .status-indicator { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .status-online { background: #4CAF50; }
        .status-offline { background: #f44336; }
        .shard-tree { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 20px 0; }
        .tree-node { margin: 5px 0; padding-left: 20px; }
        .tree-node-root { padding-left: 0; }
        .tree-toggle { cursor: pointer; user-select: none; font-weight: bold; }
        .tree-toggle:hover { color: #2196F3; }
        .tree-children { margin-left: 20px; display: none; }
        .tree-children.expanded { display: block; }
        .tree-label { display: inline-block; margin-left: 5px; }
        .tree-split-time { color: #666; font-size: 0.9em; margin-left: 10px; }
        .tree-node-count { color: #2196F3; font-weight: bold; margin-left: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>D-LOCKSS Network Monitor</h1>
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-nodes">0</div>
                <div class="stat-label">Total Nodes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-pinned">0</div>
                <div class="stat-label">Total Pinned Files</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-known">0</div>
                <div class="stat-label">Total Known Files</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-shards">0</div>
                <div class="stat-label">Active Shards</div>
            </div>
        </div>

        <div class="charts">
            <div class="chart-container">
                <h3>Files per Node</h3>
                <canvas id="filesChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>Shard Distribution</h3>
                <canvas id="shardChart"></canvas>
            </div>
        </div>

        <div class="shard-tree">
            <h3>Shard Tree</h3>
            <div id="shardTreeContainer"></div>
        </div>

        <div class="node-table">
            <h3>Node Details</h3>
            <table id="nodeTable">
                <thead>
                    <tr>
                        <th>Peer ID</th>
                        <th>Shard</th>
                        <th>Peers in Shard</th>
                        <th>Pinned</th>
                        <th>Known</th>
                        <th>Uptime</th>
                        <th>Last Seen</th>
                    </tr>
                </thead>
                <tbody id="nodeTableBody"></tbody>
            </table>
        </div>
    </div>

    <script>
        let filesChart, shardChart;
        const filesCtx = document.getElementById('filesChart').getContext('2d');
        const shardCtx = document.getElementById('shardChart').getContext('2d');

        function initCharts() {
            filesChart = new Chart(filesCtx, {
                type: 'bar',
                data: { labels: [], datasets: [{ label: 'Pinned Files', data: [], backgroundColor: '#2196F3' }] },
                options: { responsive: true, scales: { y: { beginAtZero: true } } }
            });
            shardChart = new Chart(shardCtx, {
                type: 'doughnut',
                data: { labels: [], datasets: [{ data: [], backgroundColor: [] }] },
                options: { responsive: true }
            });
        }

        function renderShardTree(node, container, depth = 0) {
            const div = document.createElement('div');
            div.className = 'tree-node' + (depth === 0 ? ' tree-node-root' : '');
            
            const label = node.shard_id || '(root)';
            const hasChildren = node.children && node.children.length > 0;
            
            let html = '';
            if (hasChildren) {
                html += '<span class="tree-toggle" onclick="toggleTree(this)">▼</span>';
            } else {
                html += '<span style="display:inline-block; width:15px;"></span>';
            }
            
            html += '<span class="tree-label">' + label + '</span>';
            html += '<span class="tree-node-count">(' + node.node_count + ' nodes)</span>';
            
            if (node.split_time) {
                const splitDate = new Date(node.split_time);
                html += '<span class="tree-split-time">Split: ' + splitDate.toLocaleTimeString() + '</span>';
            }
            
            div.innerHTML = html;
            container.appendChild(div);
            
            if (hasChildren) {
                const childrenDiv = document.createElement('div');
                childrenDiv.className = 'tree-children expanded';
                container.appendChild(childrenDiv);
                
                node.children.forEach(child => {
                    renderShardTree(child, childrenDiv, depth + 1);
                });
            }
        }

        function toggleTree(element) {
            const children = element.parentElement.nextElementSibling;
            if (children && children.classList.contains('tree-children')) {
                children.classList.toggle('expanded');
                element.textContent = children.classList.contains('expanded') ? '▼' : '▶';
            }
        }

        function updateShardTree() {
            fetch('/api/shard-tree')
                .then(r => r.json())
                .then(tree => {
                    const container = document.getElementById('shardTreeContainer');
                    container.innerHTML = '';
                    renderShardTree(tree, container);
                })
                .catch(err => console.error('Tree fetch error:', err));
        }

        function updateDashboard() {
            fetch('/api/nodes')
                .then(r => r.json())
                .then(data => {
                    const nodes = Object.values(data).map(n => n.data);
                    const now = Date.now() / 1000;

                    // Update stats
                    document.getElementById('total-nodes').textContent = nodes.length;
                    document.getElementById('total-pinned').textContent = nodes.reduce((s, n) => s + n.storage.pinned_files, 0);
                    document.getElementById('total-known').textContent = nodes.reduce((s, n) => s + n.storage.known_files, 0);
                    document.getElementById('total-shards').textContent = new Set(nodes.map(n => n.current_shard)).size;

                    // Update files chart
                    const labels = nodes.map(n => n.peer_id.slice(-8));
                    filesChart.data.labels = labels;
                    filesChart.data.datasets[0].data = nodes.map(n => n.storage.pinned_files);
                    filesChart.update();

                    // Update shard chart
                    const shardCounts = {};
                    nodes.forEach(n => {
                        shardCounts[n.current_shard] = (shardCounts[n.current_shard] || 0) + 1;
                    });
                    shardChart.data.labels = Object.keys(shardCounts);
                    shardChart.data.datasets[0].data = Object.values(shardCounts);
                    const colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'];
                    shardChart.data.datasets[0].backgroundColor = Object.keys(shardCounts).map((_, i) => colors[i % colors.length]);
                    shardChart.update();

                    // Update table
                    const tbody = document.getElementById('nodeTableBody');
                    tbody.innerHTML = nodes.map(n => {
                        const lastSeen = Math.floor(now - data[n.peer_id].last_seen);
                        const mins = Math.floor(lastSeen / 60);
                        const secs = lastSeen % 60;
                        const uptime = Math.floor(n.uptime_seconds);
                        const uptimeMins = Math.floor(uptime / 60);
                        return '<tr>' +
                            '<td>' + n.peer_id.slice(-12) + '</td>' +
                            '<td>' + n.current_shard + '</td>' +
                            '<td>' + n.peers_in_shard + '</td>' +
                            '<td>' + n.storage.pinned_files + '</td>' +
                            '<td>' + n.storage.known_files + '</td>' +
                            '<td>' + uptimeMins + 'm</td>' +
                            '<td><span class="status-indicator status-online"></span>' + mins + 'm ' + secs + 's ago</td>' +
                        '</tr>';
                    }).join('');
                })
                .catch(err => console.error('Fetch error:', err));
        }

        initCharts();
        updateDashboard();
        updateShardTree();
        setInterval(() => { updateDashboard(); updateShardTree(); }, 2000);
    </script>
</body>
</html>`
