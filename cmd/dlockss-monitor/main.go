package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ma "github.com/multiformats/go-multiaddr"
)

// --- Configuration Constants ---
const (
	TelemetryProtocol     = protocol.ID("/dlockss/telemetry/1.0.0")
	DiscoveryServiceTag   = "dlockss-prod"
	WebUIPort             = 8080
	NodeCleanupTimeout    = 5*time.Minute // Increased to prevent premature pruning if telemetry is delayed
	GeoIPCacheDuration    = 24 * time.Hour
	MaxGeoQueueSize       = 1000
	MonitorIdentityFile   = "monitor_identity.key"
	MonitorDiscoveryTopic = "dlockss-monitor"
)

// --- Data Models ---

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
	PinnedFiles int      `json:"pinned_files"`
	KnownFiles  int      `json:"known_files"`
	KnownCIDs   []string `json:"known_cids,omitempty"`
}

type ReplicationStatus struct {
	QueueDepth            int     `json:"queue_depth"`
	ActiveWorkers         int     `json:"active_workers"`
	AvgReplicationLevel   float64 `json:"avg_replication_level"`
	FilesAtTarget        int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"` // 0-9, 10+
}

type NodeState struct {
	Data         *StatusResponse     `json:"data"`
	LastSeen     time.Time           `json:"last_seen"`
	ShardHistory []ShardHistoryEntry `json:"-"`
	IPAddress    string              `json:"-"`
	Region       string              `json:"region"`
	LastUptime   float64             `json:"-"` // Track uptime to detect restarts
}

type ShardHistoryEntry struct {
	ShardID   string
	FirstSeen time.Time
}

type ShardSplitEvent struct {
	ParentShard string    `json:"parent_shard"`
	ChildShard  string    `json:"child_shard"`
	Timestamp   time.Time `json:"timestamp"`
}

type ShardTreeNode struct {
	ShardID   string           `json:"shard_id"`
	SplitTime *time.Time       `json:"split_time,omitempty"`
	Children  []*ShardTreeNode `json:"children,omitempty"`
	NodeCount int              `json:"node_count"`
}

type GeoLocation struct {
	Country     string `json:"country"`
	RegionName  string `json:"regionName"`
	City        string `json:"city"`
	CountryCode string `json:"countryCode"`
}

// --- Core State Engine ---

type Monitor struct {
	mu           sync.RWMutex
	nodes        map[string]*NodeState
	splitEvents  []ShardSplitEvent
	geoCache     map[string]*GeoLocation
	geoCacheTime map[string]time.Time

	// Tree Caching
	treeCache     *ShardTreeNode
	treeCacheTime time.Time
	treeDirty     bool

	// Workers
	geoQueue chan geoRequest
}

type geoRequest struct {
	ip     string
	peerID string
}

func NewMonitor() *Monitor {
	m := &Monitor{
		nodes:        make(map[string]*NodeState),
		splitEvents:  make([]ShardSplitEvent, 0, 100),
		geoCache:     make(map[string]*GeoLocation),
		geoCacheTime: make(map[string]time.Time),
		geoQueue:     make(chan geoRequest, MaxGeoQueueSize),
	}
	go m.geoWorker()
	return m
}

// --- Logic: Node Management ---

func (m *Monitor) ProcessTelemetry(ip string, status StatusResponse) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[status.PeerID]
	if !exists {
		// New node discovered
		log.Printf("[Monitor] New node discovered: %s (shard: %s, pinned: %d, known: %d)", 
			status.PeerID[:12]+"...", status.CurrentShard, status.Storage.PinnedFiles, status.Storage.KnownFiles)
		nodeState = &NodeState{
			Data:         &status,
			LastSeen:     now,
			ShardHistory: []ShardHistoryEntry{{ShardID: status.CurrentShard, FirstSeen: now}},
			IPAddress:    ip,
			LastUptime:   status.UptimeSeconds,
		}
		m.nodes[status.PeerID] = nodeState
		m.treeDirty = true
		log.Printf("[Monitor] Total nodes tracked: %d (expected: 30)", len(m.nodes))
		
		// Log if we're missing nodes
		if len(m.nodes) < 30 {
			log.Printf("[Monitor] Warning: Only %d nodes tracked, expected 30. Missing %d nodes.", 
				len(m.nodes), 30-len(m.nodes))
		}
	} else {
		// Detect restart: uptime decreased or reset significantly
		// Allow small decreases due to timing, but large decreases indicate restart
		restarted := status.UptimeSeconds < nodeState.LastUptime-10.0
		
		if restarted {
			log.Printf("[Monitor] Node restarted: %s (uptime: %.1fs -> %.1fs)", 
				status.PeerID[:12]+"...", nodeState.LastUptime, status.UptimeSeconds)
			// Node restarted - clear its history and old split events related to this node
			nodeState.ShardHistory = []ShardHistoryEntry{{ShardID: status.CurrentShard, FirstSeen: now}}
			m.pruneSplitEventsForNode(status.PeerID)
			m.treeDirty = true
		}
		
		// Check for shard change
		oldShard := nodeState.Data.CurrentShard
		if oldShard != status.CurrentShard {
			log.Printf("[Monitor] Node %s changed shard: %s -> %s", 
				status.PeerID[:12]+"...", oldShard, status.CurrentShard)
		}
		
		// Check for pinned files change
		oldPinned := nodeState.Data.Storage.PinnedFiles
		if oldPinned != status.Storage.PinnedFiles {
			log.Printf("[Monitor] Node %s pinned files changed: %d -> %d (known: %d)", 
				status.PeerID[:12]+"...", oldPinned, status.Storage.PinnedFiles, status.Storage.KnownFiles)
		}
		
		nodeState.Data = &status
		nodeState.LastSeen = now
		nodeState.LastUptime = status.UptimeSeconds
		if ip != "" && ip != nodeState.IPAddress {
			nodeState.IPAddress = ip
			nodeState.Region = "" // Invalidate region
		}
	}

	m.updateNodeShardLocked(nodeState, status.CurrentShard, now)

	if ip != "" && nodeState.Region == "" {
		select {
		case m.geoQueue <- geoRequest{ip: ip, peerID: status.PeerID}:
		default:
		}
	}
}

func (m *Monitor) updateNodeShardLocked(node *NodeState, newShard string, timestamp time.Time) {
	if len(node.ShardHistory) == 0 {
		return
	}
	lastShard := node.ShardHistory[len(node.ShardHistory)-1].ShardID

	if lastShard != newShard {
		m.treeDirty = true
		// Detect Split: Child is longer and contains parent prefix
		if len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard) {
			log.Printf("[Monitor] Detected shard split: %s -> %s (node: %s)", 
				lastShard, newShard, node.Data.PeerID[:12]+"...")
			m.splitEvents = append(m.splitEvents, ShardSplitEvent{
				ParentShard: lastShard,
				ChildShard:  newShard,
				Timestamp:   timestamp,
			})
		} else {
			log.Printf("[Monitor] Node shard change (not split): %s -> %s (node: %s)", 
				lastShard, newShard, node.Data.PeerID[:12]+"...")
		}
		node.ShardHistory = append(node.ShardHistory, ShardHistoryEntry{
			ShardID:   newShard,
			FirstSeen: timestamp,
		})
	}
}

func (m *Monitor) PruneStaleNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	changed := false
	prunedCount := 0
	for id, node := range m.nodes {
		if now.Sub(node.LastSeen) > NodeCleanupTimeout {
			log.Printf("[Monitor] Pruning stale node: %s (last seen: %v ago)", 
				id[:12]+"...", now.Sub(node.LastSeen))
			delete(m.nodes, id)
			changed = true
			prunedCount++
		}
	}
	if prunedCount > 0 {
		log.Printf("[Monitor] Pruned %d stale nodes. Remaining: %d", prunedCount, len(m.nodes))
		m.treeDirty = true
	}
	
	// Always prune orphaned split events and very old events
	if changed {
		m.pruneOrphanedSplitEvents()
	}
	m.pruneOldSplitEvents(now)
	
	if changed {
		m.treeDirty = true
	}
}

// pruneOldSplitEvents removes split events older than a threshold
func (m *Monitor) pruneOldSplitEvents(now time.Time) {
	// Remove events older than 10 minutes (long enough for normal operations, short enough to avoid stale data)
	cutoff := now.Add(-10 * time.Minute)
	filtered := make([]ShardSplitEvent, 0, len(m.splitEvents))
	for _, event := range m.splitEvents {
		if event.Timestamp.After(cutoff) {
			filtered = append(filtered, event)
		}
	}
	if len(filtered) != len(m.splitEvents) {
		m.splitEvents = filtered
		m.treeDirty = true
	}
}

// pruneSplitEventsForNode removes split events that are no longer relevant after a node restart
func (m *Monitor) pruneSplitEventsForNode(peerID string) {
	// Remove split events older than a reasonable threshold (5 minutes)
	// This ensures we don't keep stale events from before the restart
	cutoff := time.Now().Add(-5 * time.Minute)
	filtered := make([]ShardSplitEvent, 0, len(m.splitEvents))
	for _, event := range m.splitEvents {
		if event.Timestamp.After(cutoff) {
			filtered = append(filtered, event)
		}
	}
	m.splitEvents = filtered
}

// pruneOrphanedSplitEvents removes split events that don't relate to any current nodes
func (m *Monitor) pruneOrphanedSplitEvents() {
	// Build set of all current shards
	currentShards := make(map[string]bool)
	for _, node := range m.nodes {
		if len(node.ShardHistory) > 0 {
			sid := node.ShardHistory[len(node.ShardHistory)-1].ShardID
			currentShards[sid] = true
			// Also include all prefixes
			for len(sid) > 0 {
				currentShards[sid] = true
				sid = sid[:len(sid)-1]
			}
		}
	}
	
	// Keep only split events where both parent and child are in current shards
	filtered := make([]ShardSplitEvent, 0, len(m.splitEvents))
	for _, event := range m.splitEvents {
		if currentShards[event.ParentShard] && currentShards[event.ChildShard] {
			filtered = append(filtered, event)
		}
	}
	m.splitEvents = filtered
}

// --- Logic: GeoLocation Worker ---

func (m *Monitor) geoWorker() {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for req := range m.geoQueue {
		<-ticker.C
		m.mu.RLock()
		cached, found := m.geoCache[req.ip]
		valid := found && time.Since(m.geoCacheTime[req.ip]) < GeoIPCacheDuration
		m.mu.RUnlock()

		if valid {
			m.updateNodeRegion(req.peerID, cached)
			continue
		}

		geo := m.fetchGeoLocation(client, req.ip)
		if geo != nil {
			m.mu.Lock()
			m.geoCache[req.ip] = geo
			m.geoCacheTime[req.ip] = time.Now()
			m.mu.Unlock()
			m.updateNodeRegion(req.peerID, geo)
		}
	}
}

// Helper to check for private IPs
func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	// Check for loopback, link-local, private networks
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	// Check private blocks (10.x, 172.16.x, 192.168.x)
	privateIPBlocks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	for _, cidr := range privateIPBlocks {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Monitor) fetchGeoLocation(client *http.Client, ip string) *GeoLocation {
	if ip == "" {
		return nil
	}

	// FIX: Handle Local/Private IPs immediately
	if isPrivateIP(ip) {
		return &GeoLocation{
			Country:     "Local Network",
			RegionName:  "LAN",
			City:        "Localhost",
			CountryCode: "LOC",
		}
	}

	// Existing API logic
	url := fmt.Sprintf("http://ip-api.com/json/%s?fields=status,country,countryCode,regionName,city", ip)
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var geo GeoLocation
	if err := json.NewDecoder(resp.Body).Decode(&geo); err != nil {
		return nil
	}

	// Check if API actually returned a valid location (API can return success but no data for reserved IPs)
	if geo.Country == "" {
		return nil
	}

	return &geo
}

func (m *Monitor) updateNodeRegion(peerID string, geo *GeoLocation) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exists := m.nodes[peerID]; exists {
		parts := []string{}
		if geo.CountryCode != "" {
			parts = append(parts, geo.CountryCode)
		}
		if geo.RegionName != "" {
			parts = append(parts, geo.RegionName)
		}
		if len(parts) == 0 {
			node.Region = geo.Country
		} else {
			node.Region = strings.Join(parts, " - ")
		}
	}
}

// --- Logic: Tree Building (Fixed) ---

func (m *Monitor) GetShardTree() *ShardTreeNode {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return cached if clean and fresh
	if !m.treeDirty && m.treeCache != nil && time.Since(m.treeCacheTime) < 5*time.Second {
		return m.treeCache
	}

	// 1. Identify all explicit Shard IDs
	rawShardIDs := make(map[string]bool)
	rawShardIDs[""] = true

	for _, e := range m.splitEvents {
		rawShardIDs[e.ParentShard] = true
		rawShardIDs[e.ChildShard] = true
	}

	shardCounts := make(map[string]int)
	nodesWithoutShard := 0
	for _, n := range m.nodes {
		if len(n.ShardHistory) > 0 {
			sid := n.ShardHistory[len(n.ShardHistory)-1].ShardID
			shardCounts[sid]++
			rawShardIDs[sid] = true
		} else if n.Data != nil {
			// Fallback: use current shard from data if history is empty
			sid := n.Data.CurrentShard
			shardCounts[sid]++
			rawShardIDs[sid] = true
		} else {
			// Node has no shard info at all - this shouldn't happen but handle gracefully
			nodesWithoutShard++
			// Can't log peer ID if Data is nil, just count it
		}
	}
	
	// Log shard distribution for debugging
	totalCounted := 0
	for _, count := range shardCounts {
		totalCounted += count
	}
	if len(shardCounts) > 0 {
		log.Printf("[Monitor] Shard distribution: %v (total counted: %d, nodes tracked: %d, missing: %d)", 
			shardCounts, totalCounted, len(m.nodes), 30-len(m.nodes))
	}
	if nodesWithoutShard > 0 {
		log.Printf("[Monitor] Warning: %d nodes have no shard information", nodesWithoutShard)
	}

	// FIX: Expand IDs to include all intermediate prefixes
	allShardIDs := make(map[string]bool)
	for id := range rawShardIDs {
		current := id
		allShardIDs[current] = true
		for len(current) > 0 {
			// Remove last character to get parent prefix
			current = current[:len(current)-1]
			allShardIDs[current] = true
		}
	}

	// 2. Initialize Node Objects (Intermediate nodes get 0 count)
	nodeMap := make(map[string]*ShardTreeNode)
	for id := range allShardIDs {
		nodeMap[id] = &ShardTreeNode{
			ShardID:   id,
			Children:  make([]*ShardTreeNode, 0),
			NodeCount: shardCounts[id],
		}
	}

	// 3. Link Nodes
	// Rule: Each shard should appear only once in the tree.
	// Shards are linked based on prefix relationships (e.g., "11" is child of "1").
	// If a shard has nodes, it can still have children (transitional state during splits).
	hasParent := make(map[string]bool)

	// A. Apply explicit split events
	for _, e := range m.splitEvents {
		if parent, ok := nodeMap[e.ParentShard]; ok {
			if child, ok := nodeMap[e.ChildShard]; ok {
				// Check duplicates before adding
				exists := false
				for _, c := range parent.Children {
					if c.ShardID == child.ShardID {
						exists = true
						break
					}
				}
				if !exists {
					parent.Children = append(parent.Children, child)
					hasParent[e.ChildShard] = true
					t := e.Timestamp
					child.SplitTime = &t
				}
			}
		}
	}

	// B. Infer parents for shards without explicit parent relationships
	// Sort by length (shorter first) to ensure parents are processed before children
	var sortedIDs []string
	for id := range nodeMap {
		if id != "" && !hasParent[id] {
			sortedIDs = append(sortedIDs, id)
		}
	}
	// Sort by length first, then lexicographically
	sort.Slice(sortedIDs, func(i, j int) bool {
		if len(sortedIDs[i]) != len(sortedIDs[j]) {
			return len(sortedIDs[i]) < len(sortedIDs[j])
		}
		return sortedIDs[i] < sortedIDs[j]
	})

	for _, id := range sortedIDs {
		child := nodeMap[id]
		
		// Skip if this shard is the root
		if id == "" {
			continue
		}

		// Find the immediate parent by removing the last character
		// This ensures "11" is always a child of "1", not of ""
		parentID := id[:len(id)-1]
		if parent, ok := nodeMap[parentID]; ok {
			// Check duplicates
			exists := false
			for _, c := range parent.Children {
				if c.ShardID == child.ShardID {
					exists = true
					break
				}
			}
			if !exists {
				parent.Children = append(parent.Children, child)
				hasParent[id] = true
			}
		}
	}

	// 4. Clean up: Remove intermediate nodes that have no children and no nodes
	// This ensures we don't show unnecessary intermediate nodes in the tree
	nodesToRemove := make([]string, 0)
	for id, node := range nodeMap {
		if id != "" && node.NodeCount == 0 && len(node.Children) == 0 {
			// This is an unnecessary intermediate node, mark for removal
			nodesToRemove = append(nodesToRemove, id)
		}
	}
	// Remove marked nodes from their parents and from nodeMap
	for _, id := range nodesToRemove {
		parentID := id[:len(id)-1]
		if parent, ok := nodeMap[parentID]; ok {
			for i, child := range parent.Children {
				if child.ShardID == id {
					parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
					break
				}
			}
		}
		delete(nodeMap, id)
		delete(hasParent, id)
	}

	// 5. Construct Final Tree
	root := nodeMap[""]
	sortChildren(root)

	// Add unlinked orphans to root (ensuring no duplicates)
	for id, node := range nodeMap {
		if id != "" && !hasParent[id] {
			exists := false
			for _, c := range root.Children {
				if c.ShardID == id {
					exists = true
					break
				}
			}
			if !exists {
				root.Children = append(root.Children, node)
			}
		}
	}

	// Root node count should only include nodes actually in root shard (""), not all nodes
	// The shardCounts map already has the correct count for each shard
	root.NodeCount = shardCounts[""]
	m.treeCache = root
	m.treeCacheTime = time.Now()
	m.treeDirty = false

	return root
}

func sortChildren(node *ShardTreeNode) {
	if len(node.Children) == 0 {
		return
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].ShardID < node.Children[j].ShardID
	})
	for _, child := range node.Children {
		sortChildren(child)
	}
}

// --- Networking & Discovery ---

type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Printf("[Monitor] mDNS discovered peer: %s", pi.ID)
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := n.h.Connect(ctx, pi); err != nil {
			log.Printf("[Monitor] Failed to connect to discovered peer %s: %v", pi.ID, err)
		} else {
			log.Printf("[Monitor] Successfully connected to peer %s", pi.ID)
		}
	}
}

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
			log.Printf("[Monitor] Loaded persistent identity from %s", identityPath)
			return privKey, nil
		}
	}
	log.Printf("[Monitor] Generating new persistent identity...")
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err == nil {
		os.WriteFile(identityPath, keyBytes, 0600)
		log.Printf("[Monitor] Saved persistent identity to %s", identityPath)
	}
	return privKey, nil
}

func startLibP2P(ctx context.Context, monitor *Monitor) (host.Host, error) {
	privKey, err := loadOrCreateMonitorIdentity()
	if err != nil {
		return nil, fmt.Errorf("failed to load/create identity: %w", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.EnableNATService(),
	)
	if err != nil {
		return nil, err
	}

	log.Printf("[Monitor] Peer ID: %s", h.ID())

	// Initialize PubSub for monitor discovery
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize PubSub: %w", err)
	}

	// Join monitor discovery topic so nodes can discover us via PubSub
	monitorTopic, err := ps.Join(MonitorDiscoveryTopic)
	if err != nil {
		log.Printf("[Monitor] Warning: Failed to join monitor discovery topic: %v", err)
	} else {
		log.Printf("[Monitor] Joined monitor discovery topic: %s (nodes will discover monitor via PubSub)", MonitorDiscoveryTopic)
		// Keep topic reference to prevent it from being garbage collected
		_ = monitorTopic
	}

	notifee := &discoveryNotifee{h: h}
	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		return nil, fmt.Errorf("mdns error: %w", err)
	}

	h.SetStreamHandler(TelemetryProtocol, func(s network.Stream) {
		defer s.Close()
		s.SetDeadline(time.Now().Add(15 * time.Second))

		peerID := s.Conn().RemotePeer()
		ip := ""
		if conn := s.Conn(); conn != nil {
			remote := conn.RemoteMultiaddr()
			ip, _ = remote.ValueForProtocol(ma.P_IP4)
			if ip == "" {
				ip, _ = remote.ValueForProtocol(ma.P_IP6)
			}
		}

		gr, err := gzip.NewReader(s)
		if err != nil {
			log.Printf("[Monitor] Failed to create gzip reader from %s: %v", peerID, err)
			return
		}
		defer gr.Close()

		var status StatusResponse
		if err := json.NewDecoder(gr).Decode(&status); err != nil {
			log.Printf("[Monitor] Failed to decode telemetry from %s: %v", peerID, err)
			return
		}

		// Log telemetry receipt for debugging (only log first few to avoid spam)
		// Full logging happens in ProcessTelemetry
		
		monitor.ProcessTelemetry(ip, status)
	})

	return h, nil
}

// --- Main & Web Server ---

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	monitor := NewMonitor()

	h, err := startLibP2P(ctx, monitor)
	if err != nil {
		log.Fatalf("Critical P2P Error: %v", err)
	}
	defer h.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, r *http.Request) {
		monitor.PruneStaleNodes()
		monitor.mu.RLock()
		defer monitor.mu.RUnlock()

		response := make(map[string]interface{}, len(monitor.nodes))
		for id, node := range monitor.nodes {
			response[id] = map[string]interface{}{
				"data":      node.Data,
				"last_seen": node.LastSeen.Unix(),
				"region":    node.Region,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	mux.HandleFunc("/api/shard-tree", func(w http.ResponseWriter, r *http.Request) {
		tree := monitor.GetShardTree()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tree)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(dashboardHTML))
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", WebUIPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start periodic status logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				monitor.mu.RLock()
				nodeCount := len(monitor.nodes)
				shardCounts := make(map[string]int)
				totalPinned := 0
				nodesWithPinnedFiles := 0
				for _, node := range monitor.nodes {
					if node.Data != nil {
						shard := node.Data.CurrentShard
						shardCounts[shard]++
						totalPinned += node.Data.Storage.PinnedFiles
						if node.Data.Storage.PinnedFiles > 0 {
							nodesWithPinnedFiles++
						}
					}
				}
				monitor.mu.RUnlock()
				
				log.Printf("[Monitor] Status: %d nodes tracked (expected: 30), %d shards, %d total pinned files (%d nodes have pinned files)", 
					nodeCount, len(shardCounts), totalPinned, nodesWithPinnedFiles)
				if nodeCount < 30 {
					log.Printf("[Monitor] Warning: Missing %d nodes! Check node discovery logs.", 30-nodeCount)
				}
				if totalPinned == 0 && nodeCount > 0 {
					log.Printf("[Monitor] Warning: No pinned files reported by any node! Check if files are being unpinned or telemetry is incorrect.")
				}
				if len(shardCounts) > 1 {
					log.Printf("[Monitor] Shard breakdown: %v", shardCounts)
				} else if len(shardCounts) == 1 {
					// Only one shard - log which one
					for shard, count := range shardCounts {
						log.Printf("[Monitor] All %d nodes in shard: %s", count, shard)
					}
				}
			}
		}
	}()

	go func() {
		log.Printf("[Monitor] UI: http://localhost:%d | PeerID: %s", WebUIPort, h.ID())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[Error] HTTP server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP Shutdown Error: %v", err)
	}
}

// HTML Dashboard
const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
    <title>D-LOCKSS Network Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body { font-family: "Courier New", Courier, monospace; margin: 20px; background: #f8f9fa; color: #333; }
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { color: #222; text-transform: uppercase; border-bottom: 2px solid #333; padding-bottom: 10px; }
        
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .stat-card { background: white; padding: 15px; border: 1px solid #333; }
        .stat-value { font-size: 2em; font-weight: 700; color: #000; }
        .stat-label { color: #555; font-size: 0.8em; text-transform: uppercase; margin-top: 5px; }
        
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }
        .chart-container { background: white; padding: 20px; border: 1px solid #333; }

        /* --- VISUAL TREE CHART CSS --- */
        .shard-tree-section { background: white; padding: 20px; margin: 20px 0; border: 1px solid #333; overflow-x: auto; }
        
        .tree-chart { display: flex; justify-content: center; padding: 20px 0; min-width: max-content; }
        .tree-node { display: flex; flex-direction: column; align-items: center; position: relative; padding: 20px 5px 0 5px; }
        
        .tree-node::before, .tree-node::after {
            content: ''; position: absolute; top: 0; right: 50%; border-top: 1px solid #333; width: 50%; height: 20px;
        }
        .tree-node::after { right: auto; left: 50%; border-left: 1px solid #333; }
        
        .tree-node:only-child::after, .tree-node:only-child::before { display: none; }
        .tree-node:first-child::before, .tree-node:last-child::after { border: 0 none; }
        .tree-node:last-child::before { border-right: 1px solid #333; border-radius: 0 5px 0 0; }
        .tree-node:first-child::after { border-radius: 5px 0 0 0; }
        
        .node-content { 
            border: 1px solid #333; padding: 5px 10px; text-align: center; background: #fff; z-index: 2; 
            font-size: 0.85em; min-width: 80px; box-shadow: 2px 2px 0px #eee;
        }
        .node-id { font-weight: bold; color: #000; margin-bottom: 2px; }
        .node-count { font-size: 0.8em; color: #555; }
        
        .node-children { display: flex; flex-direction: row; padding-top: 20px; position: relative; }
        .node-children::before {
            content: ''; position: absolute; top: 0; left: 50%; border-left: 1px solid #333; width: 0; height: 20px;
        }

        /* --- TABLE CSS --- */
        .node-table { background: white; padding: 20px; margin: 20px 0; border: 1px solid #333; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { text-align: left; padding: 10px; border-bottom: 2px solid #333; background: #eee; text-transform: uppercase; font-size: 0.8em; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        
        .status-text { font-weight: bold; font-size: 0.8em; }
        .status-online { color: green; }
        
        .btn-text { 
            background: none; border: 1px solid #999; cursor: pointer; font-family: inherit; font-size: 0.8em; padding: 2px 6px; 
            text-transform: uppercase; color: #333;
        }
        .btn-text:hover { background: #eee; color: #000; border-color: #333; }
        .btn-save { border-color: green; color: green; }
        .btn-cancel { border-color: red; color: red; }
        
        .shard-badge { background: #eee; padding: 2px 6px; font-size: 0.9em; border: 1px solid #ccc; }
        .alias-input { font-family: inherit; padding: 4px; border: 1px solid #333; width: 120px; }
    </style>
</head>
<body>
    <div class="container">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 20px;">
            <h1>D-LOCKSS Monitor</h1>
            <div style="font-size: 0.9em;">SYSTEM STATUS: <span class="status-text status-online">[ONLINE]</span></div>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-nodes">--</div>
                <div class="stat-label">Total Nodes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-pinned">--</div>
                <div class="stat-label">Total Pinned</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="unique-files">--</div>
                <div class="stat-label">Unique CIDs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-shards">--</div>
                <div class="stat-label">Active Shards</div>
            </div>
        </div>

        <div class="charts">
            <div class="chart-container">
                <h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Replication Status</h3>
                <canvas id="replicationChart"></canvas>
            </div>
            <div class="chart-container">
                <h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Pinned Files per Node</h3>
                <canvas id="filesChart"></canvas>
            </div>
            <div class="chart-container">
                <h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Shard Distribution</h3>
                <canvas id="shardChart"></canvas>
            </div>
        </div>

        <div class="shard-tree-section">
            <h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Shard Topology Chart</h3>
            <div id="shardTreeContainer" class="tree-chart"></div>
        </div>

        <div class="node-table">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
                <h3 style="margin:0; text-transform:uppercase; font-size:1em;">Network Nodes</h3>
                <input type="text" id="nodeSearch" placeholder="SEARCH ID/ALIAS..." 
                       style="padding: 8px; border: 1px solid #333; width: 300px; font-family:inherit;"
                       onkeyup="filterNodes()">
            </div>
            <table id="nodeTable">
                <thead>
                    <tr>
                        <th style="width: 80px;">Action</th>
                        <th>Node Identity</th>
                        <th>Region</th>
                        <th>Shard</th>
                        <th>Peers</th>
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
        let filesChart, shardChart, replicationChart;
        const ALIASES_STORAGE_KEY = 'dlockss_node_aliases';
        let currentlyEditingPeerID = null;
        const filesCtx = document.getElementById('filesChart').getContext('2d');
        const shardCtx = document.getElementById('shardChart').getContext('2d');
        const replicationCtx = document.getElementById('replicationChart').getContext('2d');
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function escapeJs(text) {
            if (!text) return '';
            return text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
        }
        
        function loadAliases() { try { return JSON.parse(localStorage.getItem(ALIASES_STORAGE_KEY) || '{}'); } catch { return {}; } }
        function saveAliases(aliases) { localStorage.setItem(ALIASES_STORAGE_KEY, JSON.stringify(aliases)); }
        function setAlias(peerID, alias) {
            const aliases = loadAliases();
            if (alias && alias.trim()) aliases[peerID] = alias.trim(); else delete aliases[peerID];
            saveAliases(aliases);
            currentlyEditingPeerID = null;
            updateDashboard();
        }
        function getAliases() { return loadAliases(); }

        function editAlias(peerID, currentAlias, btn) {
            const row = btn.closest('tr');
            const displayDiv = row.querySelector('.alias-display-container');
            if (!displayDiv) return;
            currentlyEditingPeerID = peerID;
            
            displayDiv.innerHTML = '';
            const input = document.createElement('input');
            input.type = 'text';
            input.className = 'alias-input';
            input.value = currentAlias || '';
            input.placeholder = 'LABEL...';
            
            const saveBtn = document.createElement('button');
            saveBtn.className = 'btn-text btn-save';
            saveBtn.textContent = 'SAVE';
            saveBtn.onclick = () => setAlias(peerID, input.value);
            
            const cancelBtn = document.createElement('button');
            cancelBtn.className = 'btn-text btn-cancel';
            cancelBtn.textContent = 'CANCEL';
            cancelBtn.onclick = () => { currentlyEditingPeerID = null; updateDashboard(); };
            
            displayDiv.appendChild(input);
            displayDiv.appendChild(saveBtn);
            displayDiv.appendChild(cancelBtn);
            input.focus();
            
            input.onkeypress = (e) => { 
                if(e.key === 'Enter') setAlias(peerID, input.value);
                else if(e.key === 'Escape') cancelBtn.click();
            };
        }

        function restoreEditingState() {
            if (currentlyEditingPeerID) {
                const row = document.querySelector('tr[data-peer-id="' + currentlyEditingPeerID.toLowerCase() + '"]');
                if (row) {
                    const btn = row.querySelector('.alias-edit-btn');
                    const aliases = getAliases();
                    if (btn) editAlias(currentlyEditingPeerID, aliases[currentlyEditingPeerID] || '', btn);
                }
            }
        }

        function initCharts() {
            Chart.defaults.font.family = '"Courier New", Courier, monospace';
            Chart.defaults.color = '#333';
            
            replicationChart = new Chart(replicationCtx, {
                type: 'bar',
                data: { 
                    labels: ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10+'],
                    datasets: [{ 
                        label: 'Files', 
                        data: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 
                        backgroundColor: ['#D00000', '#F18F01', '#FFC300', '#FFD60A', '#FFE66D', '#06A77D', '#06A77D', '#06A77D', '#06A77D', '#06A77D', '#06A77D'],
                        borderRadius: 0 
                    }] 
                },
                options: { 
                    responsive: true, 
                    plugins: { 
                        legend: { display: false },
                        title: { display: true, text: 'Replication Level Distribution (Network-Wide)' }
                    },
                    scales: {
                        y: { beginAtZero: true, title: { display: true, text: 'Number of Files' } },
                        x: { title: { display: true, text: 'Replication Level' } }
                    }
                }
            });
            
            filesChart = new Chart(filesCtx, {
                type: 'bar',
                data: { labels: [], datasets: [{ label: 'Pinned Files', data: [], backgroundColor: '#333', borderRadius: 0 }] },
                options: { responsive: true, plugins: { legend: { display: false } } }
            });
            shardChart = new Chart(shardCtx, {
                type: 'doughnut',
                data: { labels: [], datasets: [{ data: [], backgroundColor: [] }] },
                options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right' } } }
            });
        }

        function renderShardTreeChart(node, container) {
            const nodeDiv = document.createElement('div');
            nodeDiv.className = 'tree-node';
            
            const contentDiv = document.createElement('div');
            contentDiv.className = 'node-content';
            
            const label = node.shard_id === "" ? "ROOT" : node.shard_id;
            contentDiv.innerHTML = '<div class="node-id">' + label + '</div>' + 
                                   '<div class="node-count">' + node.node_count + ' NODES</div>';
            nodeDiv.appendChild(contentDiv);

            if (node.children && node.children.length > 0) {
                const childrenDiv = document.createElement('div');
                childrenDiv.className = 'node-children';
                node.children.forEach(child => {
                    renderShardTreeChart(child, childrenDiv);
                });
                nodeDiv.appendChild(childrenDiv);
            }
            
            container.appendChild(nodeDiv);
        }

        function updateShardTree() {
            fetch('/api/shard-tree').then(r=>r.json()).then(tree => {
                const c = document.getElementById('shardTreeContainer');
                c.innerHTML = '';
                renderShardTreeChart(tree, c);
            });
        }

        function updateDashboard() {
            fetch('/api/nodes').then(r=>r.json()).then(data => {
                const aliases = getAliases();
                const nodes = Object.values(data).map(n => n.data);
                const meta = data;
                
                document.getElementById('total-nodes').textContent = nodes.length;
                document.getElementById('total-pinned').textContent = nodes.reduce((s,n) => s + n.storage.pinned_files, 0).toLocaleString();
                const uCids = new Set();
                nodes.forEach(n => (n.storage.known_cids||[]).forEach(c => uCids.add(c)));
                document.getElementById('unique-files').textContent = uCids.size.toLocaleString();
                document.getElementById('total-shards').textContent = new Set(nodes.map(n => n.current_shard)).size;

                // Update replication status chart (network-wide aggregation)
                const replicationDist = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
                nodes.forEach(n => {
                    if (n.replication && n.replication.replication_distribution) {
                        for (let i = 0; i < 11; i++) {
                            replicationDist[i] += n.replication.replication_distribution[i] || 0;
                        }
                    }
                });
                replicationChart.data.datasets[0].data = replicationDist;
                replicationChart.update();

                filesChart.data.labels = nodes.map(n => aliases[n.peer_id] || n.peer_id.slice(-6));
                filesChart.data.datasets[0].data = nodes.map(n => n.storage.pinned_files);
                filesChart.update();

                const sCounts = {};
                nodes.forEach(n => sCounts[n.current_shard] = (sCounts[n.current_shard]||0)+1);
                shardChart.data.labels = Object.keys(sCounts);
                shardChart.data.datasets[0].data = Object.values(sCounts);
                // Colorful palette for shard distribution
                const colorPalette = [
                    '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
                    '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739', '#52BE80',
                    '#EC7063', '#5DADE2', '#58D68D', '#F4D03F', '#AF7AC5',
                    '#85C1E9', '#F1948A', '#73C6B6', '#F9E79F', '#A569BD'
                ];
                shardChart.data.datasets[0].backgroundColor = Object.keys(sCounts).map((_,i) => 
                    colorPalette[i % colorPalette.length]
                );
                shardChart.update();

                const tbody = document.getElementById('nodeTableBody');
                tbody.innerHTML = nodes.map(n => {
                    const m = meta[n.peer_id];
                    const alias = aliases[n.peer_id] || '';
                    const lastSeen = Math.floor((Date.now()/1000) - m.last_seen);
                    
                    const peerIdEscaped = escapeJs(n.peer_id);
                    const aliasEscaped = escapeJs(alias);
                    const peerIdHtml = escapeHtml(n.peer_id);
                    const aliasHtml = escapeHtml(alias);
                    const regionHtml = escapeHtml(m.region || 'LOCATING...');
                    const shardHtml = escapeHtml(n.current_shard);
                    
                    return '<tr data-peer-id="' + escapeHtml(n.peer_id.toLowerCase()) + '">' +
                        '<td><button class="btn-text alias-edit-btn" onclick="editAlias(\'' + peerIdEscaped + '\', \'' + aliasEscaped + '\', this)">EDIT</button></td>' +
                        '<td class="peer-id-cell"><div class="alias-display-container">' +
                            '<div style="font-weight:600;">' + (aliasHtml || peerIdHtml.slice(0,12) + '...') + '</div>' +
                            '<div style="font-size:0.8em; color:#666; font-family:monospace;">' + peerIdHtml + '</div>' +
                        '</div></td>' +
                        '<td>' + (m.region ? regionHtml : '<span style="color:#999">LOCATING...</span>') + '</td>' +
                        '<td><span class="shard-badge">' + shardHtml + '</span></td>' +
                        '<td>' + n.peers_in_shard + '</td>' +
                        '<td>' + n.storage.pinned_files + '</td>' +
                        '<td>' + n.storage.known_files + '</td>' +
                        '<td>' + Math.floor(n.uptime_seconds/60) + 'm</td>' +
                        '<td><span class="status-text status-online">[ACTIVE]</span> ' + lastSeen + 's ago</td>' +
                        '</tr>';
                }).join('');
                restoreEditingState();
            });
        }

        function filterNodes() {
            const term = document.getElementById('nodeSearch').value.toLowerCase();
            document.querySelectorAll('#nodeTableBody tr').forEach(row => {
                row.style.display = row.innerText.toLowerCase().includes(term) ? '' : 'none';
            });
        }

        initCharts();
        updateDashboard();
        updateShardTree();
        setInterval(() => { updateDashboard(); updateShardTree(); }, 2000);
    </script>
</body>
</html>
`


