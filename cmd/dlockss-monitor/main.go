// Command dlockss-monitor runs the D-LOCKSS network monitor: listens on all shard
// topics via libp2p/pubsub, discovers nodes via heartbeats and ingest messages,
// and serves a web UI with node list, shard tree, and replication charts.
package main

import (
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

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	ma "github.com/multiformats/go-multiaddr"

	"dlockss/pkg/schema"
)

// --- Configuration Constants ---
const (
	DiscoveryServiceTag       = "dlockss-prod"
	WebUIPort                 = 8080
	DefaultNodeCleanupTimeout = 10 * time.Minute
	// ReplicationAnnounceTTL: how long we count a peer as having a file after their last PINNED/Ingest.
	// After a split, nodes unpin non-responsible files ~5s later (ReshardDelay); they stop announcing
	// those files, so we expire them after this TTL. Shorter TTL = dashboard reflects unpins sooner.
	ReplicationAnnounceTTL  = 2 * time.Minute
	MonitorMinReplication   = 5
	MonitorMaxReplication   = 10
	ReplicationCleanupEvery = 1 * time.Minute
	GeoIPCacheDuration      = 24 * time.Hour
	MaxGeoQueueSize         = 1000
	MonitorIdentityFile     = "monitor_identity.key"
	GeoFailureThreshold     = 5
	GeoCooldownDuration     = 5 * time.Minute
)

// nodeCleanupTimeout: after this duration without any message (heartbeat, Ingest, etc.) a node is pruned.
// Configurable via DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT (e.g. "30m", "1h"). For Pi-only or remote networks
// where connectivity can be intermittent, use a longer value so nodes are not pruned during brief gaps.
var nodeCleanupTimeout = DefaultNodeCleanupTimeout

// --- Data Models ---

type StatusResponse struct {
	PeerID        string            `json:"peer_id"` // Single peer ID per node (D-LOCKSS uses IPFS repo identity when IPFS_PATH set)
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
	QueueDepth              int     `json:"queue_depth"`
	ActiveWorkers           int     `json:"active_workers"`
	AvgReplicationLevel     float64 `json:"avg_replication_level"`
	FilesAtTarget           int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"`
}

type NodeState struct {
	PeerID         string              `json:"peer_id"`
	CurrentShard   string              `json:"current_shard"`
	PinnedFiles    int                 `json:"pinned_files"`
	KnownFiles     int                 `json:"known_files"`
	LastSeen       time.Time           `json:"last_seen"`
	ShardHistory   []ShardHistoryEntry `json:"shard_history"`
	IPAddress      string              `json:"ip_address"`
	Region         string              `json:"region"`
	announcedFiles map[string]time.Time
}

type ShardHistoryEntry struct {
	ShardID   string    `json:"shard_id"`
	FirstSeen time.Time `json:"first_seen"`
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
	mu                  sync.RWMutex
	nodes               map[string]*NodeState
	splitEvents         []ShardSplitEvent
	geoCache            map[string]*GeoLocation
	geoCacheTime        map[string]time.Time
	geoFailures         int
	geoCooldownUntil    time.Time
	treeCache           *ShardTreeNode
	treeCacheTime       time.Time
	treeDirty           bool
	geoQueue            chan geoRequest
	uniqueCIDs          map[string]time.Time
	shardTopics         map[string]*pubsub.Topic
	ps                  *pubsub.PubSub
	host                host.Host
	nodeFiles           map[string]map[string]time.Time
	manifestReplication map[string]map[string]time.Time
}

type geoRequest struct {
	ip     string
	peerID string
}

func NewMonitor() *Monitor {
	m := &Monitor{
		nodes:               make(map[string]*NodeState),
		splitEvents:         make([]ShardSplitEvent, 0, 100),
		geoCache:            make(map[string]*GeoLocation),
		geoCacheTime:        make(map[string]time.Time),
		geoQueue:            make(chan geoRequest, MaxGeoQueueSize),
		uniqueCIDs:          make(map[string]time.Time),
		shardTopics:         make(map[string]*pubsub.Topic),
		nodeFiles:           make(map[string]map[string]time.Time),
		manifestReplication: make(map[string]map[string]time.Time),
	}
	go m.geoWorker()
	go m.runReplicationCleanup()
	return m
}

func (m *Monitor) handleIngestMessage(im *schema.IngestMessage, senderID peer.ID, shardID string, ip string) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via IngestMessage: %s (shard: %s)", peerIDStr[:min(12, len(peerIDStr))]+"...", shardID)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
			CurrentShard:   shardID,
			PinnedFiles:    0,
			KnownFiles:     0,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = nodeState
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
		m.treeDirty = true
	}

	nodeState.LastSeen = now
	manifestCIDStr := im.ManifestCID.String()

	if nodeState.announcedFiles == nil {
		nodeState.announcedFiles = make(map[string]time.Time)
	}

	// PinnedFiles is updated only from heartbeat; do not infer from PINNED announcements to avoid chart jumps.
	nodeState.announcedFiles[manifestCIDStr] = now

	if m.nodeFiles[peerIDStr] == nil {
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
	}
	m.nodeFiles[peerIDStr][manifestCIDStr] = now

	nodeState.KnownFiles = len(nodeState.announcedFiles)
	m.uniqueCIDs[manifestCIDStr] = now

	if m.manifestReplication[manifestCIDStr] == nil {
		m.manifestReplication[manifestCIDStr] = make(map[string]time.Time)
	}
	m.manifestReplication[manifestCIDStr][peerIDStr] = now

	if m.ps != nil {
		m.ensureShardSubscriptionUnlocked(context.Background(), shardID)
	}

	if ip != "" && ip != nodeState.IPAddress {
		nodeState.IPAddress = ip
		nodeState.Region = ""
		select {
		case m.geoQueue <- geoRequest{ip: ip, peerID: peerIDStr}:
		default:
		}
	}
}

func (m *Monitor) handleHeartbeat(senderID peer.ID, shardID string, ip string, pinnedCount int) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via heartbeat: %s (shard: %s, pinned: %d)", peerIDStr[:min(12, len(peerIDStr))]+"...", shardID, pinnedCount)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
			CurrentShard:   shardID,
			PinnedFiles:    pinnedCount,
			KnownFiles:     0,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = nodeState
		m.treeDirty = true
	} else {
		nodeState.LastSeen = now
		// Trust heartbeat pinned count (single source of truth). -1 means "don't update" (e.g. non-HEARTBEAT message).
		if pinnedCount >= 0 {
			nodeState.PinnedFiles = pinnedCount
		}
		if nodeState.CurrentShard == "" {
			nodeState.CurrentShard = shardID
			nodeState.ShardHistory = append(nodeState.ShardHistory, ShardHistoryEntry{ShardID: shardID, FirstSeen: now})
			m.treeDirty = true
		} else {
			m.updateNodeShardLocked(nodeState, shardID, now)
		}
		if ip != "" && ip != nodeState.IPAddress {
			nodeState.IPAddress = ip
			nodeState.Region = ""
		}
	}

	if ip != "" && nodeState.Region == "" {
		select {
		case m.geoQueue <- geoRequest{ip: ip, peerID: peerIDStr}:
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
		if len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard) {
			log.Printf("[Monitor] Detected shard split: %s -> %s (node: %s)", lastShard, newShard, node.PeerID[:min(12, len(node.PeerID))]+"...")
			m.splitEvents = append(m.splitEvents, ShardSplitEvent{
				ParentShard: lastShard,
				ChildShard:  newShard,
				Timestamp:   timestamp,
			})
			if m.ps != nil {
				m.ensureShardSubscriptionUnlocked(context.Background(), newShard)
			}
			if lastShard == "" && (newShard == "0" || newShard == "1") {
				siblingShard := "1"
				if newShard == "1" {
					siblingShard = "0"
				}
				m.ensureShardSubscriptionUnlocked(context.Background(), siblingShard)
			}
		} else {
			if m.ps != nil {
				m.ensureShardSubscriptionUnlocked(context.Background(), newShard)
			}
		}
		node.CurrentShard = newShard
		node.ShardHistory = append(node.ShardHistory, ShardHistoryEntry{ShardID: newShard, FirstSeen: timestamp})
	}
}

func (m *Monitor) runReplicationCleanup() {
	ticker := time.NewTicker(ReplicationCleanupEvery)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		cutoff := time.Now().Add(-ReplicationAnnounceTTL)
		for manifest, peers := range m.manifestReplication {
			for peerID, lastSeen := range peers {
				if lastSeen.Before(cutoff) {
					delete(peers, peerID)
				}
			}
			if len(peers) == 0 {
				delete(m.manifestReplication, manifest)
			}
		}
		m.mu.Unlock()
	}
}

// getReplicationStats returns network-wide replication distribution.
// Replication is counted across all shards: each distinct peer that has announced
// a file (via PINNED or IngestMessage) in the last ReplicationAnnounceTTL is
// counted once per manifest. Nodes unpin files that no longer belong to their
// shard after a split, so replication should reflect per-shard responsibility.
func (m *Monitor) getReplicationStats() (distribution [11]int, avgLevel float64, filesAtTarget int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalReplication int
	var manifestCount int
	for _, peers := range m.manifestReplication {
		count := len(peers)
		if count == 0 {
			continue
		}
		manifestCount++
		totalReplication += count
		if count >= 10 {
			distribution[10]++
		} else {
			distribution[count]++
		}
		if count >= MonitorMinReplication && count <= MonitorMaxReplication {
			filesAtTarget++
		}
	}
	if manifestCount > 0 {
		avgLevel = float64(totalReplication) / float64(manifestCount)
	}
	return distribution, avgLevel, filesAtTarget
}

// getReplicationByShard returns, for each shard, how many manifests have at least
// MonitorMinReplication replicas in that shard. Useful to verify both sides of a
// split have sufficient replication.
func (m *Monitor) getReplicationByShard() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// manifest -> shard -> peer count
	perManifestPerShard := make(map[string]map[string]int)
	for manifest, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		perManifestPerShard[manifest] = make(map[string]int)
		for peerID := range peers {
			node, ok := m.nodes[peerID]
			if !ok {
				continue
			}
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			perManifestPerShard[manifest][shard]++
		}
	}

	// shard -> number of manifests with >= MinReplication in that shard
	filesAtTargetPerShard := make(map[string]int)
	for _, shardCounts := range perManifestPerShard {
		for shard, count := range shardCounts {
			if count >= MonitorMinReplication {
				filesAtTargetPerShard[shard]++
			}
		}
	}
	return filesAtTargetPerShard
}

func (m *Monitor) PruneStaleNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	changed := false
	prunedCount := 0
	for id, node := range m.nodes {
		if now.Sub(node.LastSeen) > nodeCleanupTimeout {
			delete(m.nodes, id)
			changed = true
			prunedCount++
		}
	}
	if prunedCount > 0 {
		log.Printf("[Monitor] Pruned %d stale nodes (no message for > %s). Consider DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT for remote/Pi networks.", prunedCount, nodeCleanupTimeout)
		m.treeDirty = true
	}
	if changed {
		m.pruneOrphanedSplitEvents()
	}
	m.pruneOldSplitEvents(now)
	if changed {
		m.treeDirty = true
	}
}

func (m *Monitor) pruneOldSplitEvents(now time.Time) {
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

func (m *Monitor) pruneOrphanedSplitEvents() {
	currentShards := make(map[string]bool)
	for _, node := range m.nodes {
		if len(node.ShardHistory) > 0 {
			sid := node.ShardHistory[len(node.ShardHistory)-1].ShardID
			currentShards[sid] = true
			for len(sid) > 0 {
				currentShards[sid] = true
				sid = sid[:len(sid)-1]
			}
		}
	}
	filtered := make([]ShardSplitEvent, 0, len(m.splitEvents))
	for _, event := range m.splitEvents {
		if currentShards[event.ParentShard] && currentShards[event.ChildShard] {
			filtered = append(filtered, event)
		}
	}
	m.splitEvents = filtered
}

func (m *Monitor) geoWorker() {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for req := range m.geoQueue {
		<-ticker.C

		m.mu.RLock()
		if !m.geoCooldownUntil.IsZero() && time.Now().Before(m.geoCooldownUntil) {
			m.mu.RUnlock()
			continue
		}
		cached, found := m.geoCache[req.ip]
		valid := found && time.Since(m.geoCacheTime[req.ip]) < GeoIPCacheDuration
		m.mu.RUnlock()

		if valid {
			m.updateNodeRegion(req.peerID, cached)
			continue
		}

		geo, err := m.fetchGeoLocation(client, req.ip)

		m.mu.Lock()
		if err != nil {
			m.geoFailures++
			if m.geoFailures >= GeoFailureThreshold {
				log.Printf("[Monitor] GeoIP Circuit Breaker TRIPPED. Cooldown for %v.", GeoCooldownDuration)
				m.geoCooldownUntil = time.Now().Add(GeoCooldownDuration)
			}
		} else {
			m.geoFailures = 0
			if geo != nil {
				m.geoCache[req.ip] = geo
				m.geoCacheTime[req.ip] = time.Now()
			}
		}
		m.mu.Unlock()

		if geo != nil {
			m.updateNodeRegion(req.peerID, geo)
		}
	}
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	privateIPBlocks := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	for _, cidr := range privateIPBlocks {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Monitor) fetchGeoLocation(client *http.Client, ip string) (*GeoLocation, error) {
	if ip == "" {
		return nil, nil
	}
	if isPrivateIP(ip) {
		return &GeoLocation{Country: "Local Network", RegionName: "LAN", City: "Localhost", CountryCode: "LOC"}, nil
	}
	url := fmt.Sprintf("http://ip-api.com/json/%s?fields=status,country,countryCode,regionName,city", ip)
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		return nil, errors.New("rate limit hit")
	}
	var geo GeoLocation
	if err := json.NewDecoder(resp.Body).Decode(&geo); err != nil {
		return nil, err
	}
	if geo.Country == "" {
		return nil, nil
	}
	return &geo, nil
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

func (m *Monitor) GetShardTree() *ShardTreeNode {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.treeDirty && m.treeCache != nil && time.Since(m.treeCacheTime) < 5*time.Second {
		return m.treeCache
	}

	rawShardIDs := make(map[string]bool)
	rawShardIDs[""] = true
	for _, e := range m.splitEvents {
		rawShardIDs[e.ParentShard] = true
		rawShardIDs[e.ChildShard] = true
	}
	shardCounts := make(map[string]int)
	for _, n := range m.nodes {
		if len(n.ShardHistory) > 0 {
			sid := n.ShardHistory[len(n.ShardHistory)-1].ShardID
			shardCounts[sid]++
			rawShardIDs[sid] = true
		} else if n.CurrentShard != "" {
			sid := n.CurrentShard
			shardCounts[sid]++
			rawShardIDs[sid] = true
		}
	}
	allShardIDs := make(map[string]bool)
	for id := range rawShardIDs {
		current := id
		allShardIDs[current] = true
		for len(current) > 0 {
			current = current[:len(current)-1]
			allShardIDs[current] = true
		}
	}
	nodeMap := make(map[string]*ShardTreeNode)
	for id := range allShardIDs {
		nodeMap[id] = &ShardTreeNode{ShardID: id, Children: make([]*ShardTreeNode, 0), NodeCount: shardCounts[id]}
	}
	// Apply split-event timestamps for display (optional)
	for _, e := range m.splitEvents {
		if child, ok := nodeMap[e.ChildShard]; ok {
			t := e.Timestamp
			child.SplitTime = &t
		}
	}
	// Build tree purely from prefix (depth): parent of id is id[:len(id)-1].
	// Process in order of increasing depth so parents exist when we attach children.
	var orderedIDs []string
	for id := range nodeMap {
		if id != "" {
			orderedIDs = append(orderedIDs, id)
		}
	}
	sort.Slice(orderedIDs, func(i, j int) bool {
		if len(orderedIDs[i]) != len(orderedIDs[j]) {
			return len(orderedIDs[i]) < len(orderedIDs[j])
		}
		return orderedIDs[i] < orderedIDs[j]
	})
	for _, id := range orderedIDs {
		parentID := id[:len(id)-1]
		parent, hasParent := nodeMap[parentID]
		if !hasParent {
			continue
		}
		child := nodeMap[id]
		exists := false
		for _, c := range parent.Children {
			if c.ShardID == id {
				exists = true
				break
			}
		}
		if !exists {
			parent.Children = append(parent.Children, child)
		}
	}
	// Root's children must be only depth-1 shards (e.g. "0", "1"). Clear and set explicitly
	// so deeper shards (10, 11, 100, 101) never appear on the same level as 0/1.
	root := nodeMap[""]
	root.Children = nil
	for i := 1; i <= 1; i++ {
		for id, node := range nodeMap {
			if id != "" && len(id) == i {
				root.Children = append(root.Children, node)
			}
		}
	}
	sort.Slice(root.Children, func(i, j int) bool { return root.Children[i].ShardID < root.Children[j].ShardID })
	nodesToRemove := make([]string, 0)
	for id, node := range nodeMap {
		if id != "" && node.NodeCount == 0 && len(node.Children) == 0 {
			nodesToRemove = append(nodesToRemove, id)
		}
	}
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
	}
	root.NodeCount = shardCounts[""]
	sortChildren(root)
	m.treeCache = root
	m.treeCacheTime = time.Now()
	m.treeDirty = false
	return root
}

func sortChildren(node *ShardTreeNode) {
	if len(node.Children) == 0 {
		return
	}
	sort.Slice(node.Children, func(i, j int) bool { return node.Children[i].ShardID < node.Children[j].ShardID })
	for _, child := range node.Children {
		sortChildren(child)
	}
}

type discoveryNotifee struct{ h host.Host }

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = n.h.Connect(ctx, pi)
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

func (m *Monitor) ensureShardSubscription(ctx context.Context, shardID string) {
	if m.ps == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureShardSubscriptionUnlocked(ctx, shardID)
}

func (m *Monitor) ensureShardSubscriptionUnlocked(ctx context.Context, shardID string) {
	if m.ps == nil {
		return
	}
	if _, exists := m.shardTopics[shardID]; exists {
		return
	}
	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	topic, err := m.ps.Join(topicName)
	if err != nil {
		log.Printf("[Monitor] Failed to join shard topic %s: %v", topicName, err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		log.Printf("[Monitor] Failed to subscribe to shard topic %s: %v", topicName, err)
		return
	}
	m.shardTopics[shardID] = topic
	go m.handleShardMessages(ctx, sub, shardID)
	log.Printf("[Monitor] Subscribed to shard topic: %s", shardID)
}

// handleShardMessages processes messages from a shard topic.
// It detects peers based on ANY message activity, not just Heartbeats.
func (m *Monitor) handleShardMessages(ctx context.Context, sub *pubsub.Subscription, shardID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[Monitor] Error reading from shard %s: %v", shardID, err)
				}
				return
			}
			ip := ""
			senderID := msg.GetFrom()
			if m.host != nil && senderID != "" {
				for _, addr := range m.host.Peerstore().Addrs(senderID) {
					if ipVal, err := addr.ValueForProtocol(ma.P_IP4); err == nil {
						ip = ipVal
						break
					}
					if ipVal, err := addr.ValueForProtocol(ma.P_IP6); err == nil {
						ip = ipVal
						break
					}
				}
			}

			// Implicit Heartbeat: Any message counts as "I am alive in this shard"
			// We pass -1 for pinnedCount to indicate "don't update pinned count, just update timestamp"
			// unless it's an explicit heartbeat.
			m.handleHeartbeat(senderID, shardID, ip, -1)

			if len(msg.Data) > 0 && len(msg.Data) < 500 && string(msg.Data[:min(10, len(msg.Data))]) == "HEARTBEAT:" {
				// Format: HEARTBEAT:<PeerID>:<PinnedCount> (optional 4th field ignored for backward compat)
				parts := strings.SplitN(string(msg.Data), ":", 4)
				pinnedCount := 0
				if len(parts) >= 3 {
					fmt.Sscanf(parts[2], "%d", &pinnedCount)
				}
				// Update with explicit pinned count
				m.handleHeartbeat(senderID, shardID, ip, pinnedCount)
				continue
			}
			if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
				manifestCIDStr := string(msg.Data[7:])
				if manifestCID, err := cid.Decode(manifestCIDStr); err == nil {
					im := schema.IngestMessage{ManifestCID: manifestCID, ShardID: shardID}
					m.handleIngestMessage(&im, senderID, shardID, ip)
				}
				continue
			}
			var im schema.IngestMessage
			if err := im.UnmarshalCBOR(msg.Data); err == nil {
				targetShard := im.ShardID
				if m.ps != nil {
					m.mu.RLock()
					_, alreadySubscribed := m.shardTopics[targetShard]
					m.mu.RUnlock()
					if !alreadySubscribed {
						m.ensureShardSubscription(context.Background(), targetShard)
					}
				}
				m.handleIngestMessage(&im, senderID, targetShard, ip)
				continue
			}
			// Other messages (CRDT syncs, etc.) are already handled by the implicit heartbeat above.
		}
	}
}

func (m *Monitor) subscribeToActiveShards(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			activeShards := make(map[string]bool)
			shardCounts := make(map[string]int)
			for _, node := range m.nodes {
				if node.CurrentShard != "" {
					activeShards[node.CurrentShard] = true
					shardCounts[node.CurrentShard]++
				} else {
					activeShards[""] = true
					shardCounts[""]++
				}
				for _, entry := range node.ShardHistory {
					if time.Since(entry.FirstSeen) < 5*time.Minute {
						activeShards[entry.ShardID] = true
					}
				}
			}
			splitShards := make(map[string]bool)
			for shardID, count := range shardCounts {
				if count >= 4 {
					child0, child1 := shardID+"0", shardID+"1"
					if shardID == "" {
						child0, child1 = "0", "1"
					}
					splitShards[child0] = true
					splitShards[child1] = true
				}
			}
			for _, event := range m.splitEvents {
				splitShards[event.ParentShard] = true
				splitShards[event.ChildShard] = true
				if event.ParentShard == "" {
					splitShards["0"] = true
					splitShards["1"] = true
				}
			}
			m.mu.RUnlock()
			for shardID := range activeShards {
				if m.ps != nil {
					m.ensureShardSubscription(ctx, shardID)
				}
			}
			for shardID := range splitShards {
				if m.ps != nil {
					m.ensureShardSubscription(ctx, shardID)
				}
			}
		}
	}
}

func (m *Monitor) cleanupStaleCIDs(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-30 * time.Minute)
			m.mu.Lock()
			for cid, lastSeen := range m.uniqueCIDs {
				if lastSeen.Before(cutoff) {
					delete(m.uniqueCIDs, cid)
				}
			}
			for nodeID, files := range m.nodeFiles {
				for fileCID, lastSeen := range files {
					if lastSeen.Before(cutoff) {
						delete(files, fileCID)
						if nodeState, exists := m.nodes[nodeID]; exists && nodeState.announcedFiles != nil {
							delete(nodeState.announcedFiles, fileCID)
							// Keep heartbeat as single source of truth for PinnedFiles; only update KnownFiles from our view of announcements.
							nodeState.KnownFiles = len(nodeState.announcedFiles)
						}
					}
				}
				if len(files) == 0 {
					delete(m.nodeFiles, nodeID)
				}
			}
			m.mu.Unlock()
		}
	}
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
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize PubSub: %w", err)
	}
	monitor.ps = ps
	monitor.host = h
	go monitor.cleanupStaleCIDs(ctx)
	monitor.ensureShardSubscription(ctx, "")
	go monitor.subscribeToActiveShards(ctx)

	// Initialize DHT for discovery (replacing mDNS)
	kademliaDHT, err := dht.New(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	// Connect to default bootstrap peers
	var wg sync.WaitGroup
	for _, peerAddr := range dht.DefaultBootstrapPeers {
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Connect(ctx, *peerinfo); err != nil {
				// log.Printf("Bootstrap warning: %s", err)
			}
		}()
	}
	wg.Wait()

	// Setup Routing Discovery
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, DiscoveryServiceTag)
	log.Printf("[Monitor] Advertising service: %s", DiscoveryServiceTag)

	// Find peers
	go func() {
		for {
			peerChan, err := routingDiscovery.FindPeers(ctx, DiscoveryServiceTag)
			if err != nil {
				log.Printf("[Monitor] FindPeers error: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}
			for peer := range peerChan {
				if peer.ID == h.ID() {
					continue
				}
				if h.Network().Connectedness(peer.ID) != network.Connected {
					h.Connect(ctx, peer)
				}
			}
			time.Sleep(30 * time.Second)
		}
	}()

	return h, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if v := os.Getenv("DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			nodeCleanupTimeout = d
			log.Printf("[Monitor] Node cleanup timeout: %s (from env)", nodeCleanupTimeout)
		}
	}

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
		// Count nodes per shard for PeersInShard
		shardCounts := make(map[string]int)
		for _, node := range monitor.nodes {
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			shardCounts[shard]++
		}
		query := strings.ToLower(r.URL.Query().Get("q"))
		response := make(map[string]interface{})
		for id, node := range monitor.nodes {
			if query != "" {
				match := strings.Contains(strings.ToLower(id), query) ||
					strings.Contains(strings.ToLower(node.Region), query) ||
					strings.Contains(strings.ToLower(node.CurrentShard), query)
				if !match {
					continue
				}
			}
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			peersInShard := shardCounts[shard]
			if peersInShard < 1 {
				peersInShard = 1
			}
			// Uptime = time since we first saw this node (not time since last heartbeat).
			firstSeen := node.LastSeen
			if len(node.ShardHistory) > 0 {
				firstSeen = node.ShardHistory[0].FirstSeen
			}
			uptimeSeconds := time.Since(firstSeen).Seconds()
			status := StatusResponse{
				PeerID:        node.PeerID,
				Version:       "1.0.0",
				CurrentShard:  node.CurrentShard,
				PeersInShard:  peersInShard,
				Storage:       StorageStatus{PinnedFiles: node.PinnedFiles, KnownFiles: node.KnownFiles, KnownCIDs: []string{}},
				Replication:   ReplicationStatus{},
				UptimeSeconds: uptimeSeconds,
			}
			response[id] = map[string]interface{}{
				"data":      status,
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

	mux.HandleFunc("/api/unique-cids", func(w http.ResponseWriter, r *http.Request) {
		monitor.mu.RLock()
		count := len(monitor.uniqueCIDs)
		monitor.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"count": count})
	})

	mux.HandleFunc("/api/replication", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		dist, avg, atTarget := monitor.getReplicationStats()
		byShard := monitor.getReplicationByShard()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"replication_distribution":  dist,
			"avg_replication_level":     avg,
			"files_at_target":           atTarget,
			"files_at_target_per_shard": byShard,
			"replication_note":          "Counts are network-wide (all shards). Nodes unpin files that no longer belong to their shard after a split.",
		})
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
				for _, node := range monitor.nodes {
					var shard string
					if len(node.ShardHistory) > 0 {
						shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
					} else {
						shard = node.CurrentShard
					}
					shardCounts[shard]++
					totalPinned += node.PinnedFiles
				}
				monitor.mu.RUnlock()
				log.Printf("[Monitor] Status: %d nodes, %d shards, %d pinned", nodeCount, len(shardCounts), totalPinned)
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
