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
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	ma "github.com/multiformats/go-multiaddr"

	"dlockss/pkg/schema"
)

// --- Configuration Constants ---
const (
	DiscoveryServiceTag     = "dlockss-prod"
	WebUIPort               = 8080
	NodeCleanupTimeout      = 10 * time.Minute
	ReplicationAnnounceTTL  = 10 * time.Minute // Peer announcement expiry for inferred replication (same as node cleanup)
	MonitorMinReplication   = 5                 // Min copies for "at target" (align with node config)
	MonitorMaxReplication   = 10                // Max copies for "at target"
	ReplicationCleanupEvery = 2 * time.Minute   // How often to prune stale manifestReplication entries
	GeoIPCacheDuration      = 24 * time.Hour
	MaxGeoQueueSize         = 1000
	MonitorIdentityFile     = "monitor_identity.key"
	GeoFailureThreshold     = 5               // Max failures before cooldown
	GeoCooldownDuration     = 5 * time.Minute // Cooldown time
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
	QueueDepth              int     `json:"queue_depth"`
	ActiveWorkers           int     `json:"active_workers"`
	AvgReplicationLevel     float64 `json:"avg_replication_level"`
	FilesAtTarget           int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"` // 0-9, 10+
}

type NodeState struct {
	PeerID       string              `json:"peer_id"`
	CurrentShard string              `json:"current_shard"`
	PinnedFiles  int                 `json:"pinned_files"`
	KnownFiles   int                 `json:"known_files"`
	LastSeen     time.Time           `json:"last_seen"`
	ShardHistory []ShardHistoryEntry `json:"shard_history"`
	IPAddress    string              `json:"ip_address"`
	Region       string              `json:"region"`

	// Internal tracking only
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
	mu           sync.RWMutex
	nodes        map[string]*NodeState
	splitEvents  []ShardSplitEvent
	geoCache     map[string]*GeoLocation
	geoCacheTime map[string]time.Time

	// Geo Circuit Breaker
	geoFailures      int
	geoCooldownUntil time.Time

	// Tree Caching
	treeCache     *ShardTreeNode
	treeCacheTime time.Time
	treeDirty     bool

	// Workers
	geoQueue chan geoRequest

	// PubSub / Network
	uniqueCIDs  map[string]time.Time     // CID -> last seen time
	shardTopics map[string]*pubsub.Topic // shardID -> topic
	ps          *pubsub.PubSub
	host        host.Host

	// Track which nodes have which files
	nodeFiles map[string]map[string]time.Time // nodeID -> manifestCID -> last seen

	// Inferred replication: manifest CID -> peer ID -> last announce time (eavesdrop on IngestMessage)
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

// --- Logic: Node Management ---

func (m *Monitor) handleIngestMessage(im *schema.IngestMessage, senderID peer.ID, shardID string, ip string) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via IngestMessage: %s (shard: %s)",
			peerIDStr[:12]+"...", shardID)
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

	_, alreadyExists := nodeState.announcedFiles[manifestCIDStr]
	if !alreadyExists {
		nodeState.PinnedFiles++
	}
	nodeState.announcedFiles[manifestCIDStr] = now

	if m.nodeFiles[peerIDStr] == nil {
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
	}
	m.nodeFiles[peerIDStr][manifestCIDStr] = now

	nodeState.KnownFiles = len(nodeState.announcedFiles)
	m.uniqueCIDs[manifestCIDStr] = now

	// Record for inferred replication (eavesdrop: who announced this manifest)
	if m.manifestReplication[manifestCIDStr] == nil {
		m.manifestReplication[manifestCIDStr] = make(map[string]time.Time)
	}
	m.manifestReplication[manifestCIDStr][peerIDStr] = now

	if m.ps != nil {
		m.ensureShardSubscriptionUnlocked(context.Background(), shardID)
	}

	if ip != "" && ip != nodeState.IPAddress {
		nodeState.IPAddress = ip
		nodeState.Region = "" // Invalidate region
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
		log.Printf("[Monitor] New node discovered via heartbeat: %s (shard: %s, pinned: %d)",
			peerIDStr[:12]+"...", shardID, pinnedCount)
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
		if pinnedCount > nodeState.PinnedFiles {
			nodeState.PinnedFiles = pinnedCount
		}

		if nodeState.CurrentShard == "" {
			nodeState.CurrentShard = shardID
			nodeState.ShardHistory = append(nodeState.ShardHistory, ShardHistoryEntry{
				ShardID:   shardID,
				FirstSeen: now,
			})
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
		// Detect Split
		if len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard) {
			log.Printf("[Monitor] Detected shard split: %s -> %s (node: %s)",
				lastShard, newShard, node.PeerID[:12]+"...")
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
		node.ShardHistory = append(node.ShardHistory, ShardHistoryEntry{
			ShardID:   newShard,
			FirstSeen: timestamp,
		})
	}
}

// runReplicationCleanup prunes manifestReplication entries older than ReplicationAnnounceTTL.
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

// getReplicationStats returns network-wide replication distribution, average level, and files at target.
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

func (m *Monitor) PruneStaleNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	changed := false
	prunedCount := 0
	for id, node := range m.nodes {
		if now.Sub(node.LastSeen) > NodeCleanupTimeout {
			delete(m.nodes, id)
			changed = true
			prunedCount++
		}
	}
	if prunedCount > 0 {
		log.Printf("[Monitor] Pruned %d stale nodes.", prunedCount)
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

// --- Logic: GeoLocation Worker (Circuit Breaker) ---

func (m *Monitor) geoWorker() {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for req := range m.geoQueue {
		<-ticker.C

		m.mu.RLock()
		// Circuit Breaker Check
		if !m.geoCooldownUntil.IsZero() && time.Now().Before(m.geoCooldownUntil) {
			m.mu.RUnlock()
			continue // Skip processing during cooldown
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
			// Record Failure
			m.geoFailures++
			if m.geoFailures >= GeoFailureThreshold {
				log.Printf("[Monitor] GeoIP Circuit Breaker TRIPPED. Cooldown for %v.", GeoCooldownDuration)
				m.geoCooldownUntil = time.Now().Add(GeoCooldownDuration)
			}
		} else {
			// Success - Reset breaker
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

// --- Logic: Tree Building ---

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
		nodeMap[id] = &ShardTreeNode{
			ShardID:   id,
			Children:  make([]*ShardTreeNode, 0),
			NodeCount: shardCounts[id],
		}
	}

	hasParent := make(map[string]bool)
	for _, e := range m.splitEvents {
		if parent, ok := nodeMap[e.ParentShard]; ok {
			if child, ok := nodeMap[e.ChildShard]; ok {
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

	var sortedIDs []string
	for id := range nodeMap {
		if id != "" && !hasParent[id] {
			sortedIDs = append(sortedIDs, id)
		}
	}
	sort.Slice(sortedIDs, func(i, j int) bool {
		if len(sortedIDs[i]) != len(sortedIDs[j]) {
			return len(sortedIDs[i]) < len(sortedIDs[j])
		}
		return sortedIDs[i] < sortedIDs[j]
	})

	for _, id := range sortedIDs {
		child := nodeMap[id]
		if id == "" {
			continue
		}
		parentID := id[:len(id)-1]
		if parent, ok := nodeMap[parentID]; ok {
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
		delete(hasParent, id)
	}

	root := nodeMap[""]
	sortChildren(root)

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
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := n.h.Connect(ctx, pi); err != nil {
			// log.Printf("[Monitor] Failed to connect to discovered peer %s: %v", pi.ID, err)
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

	log.Printf("[Monitor] Subscribed to shard topic: %s (to track nodes)", shardID)
}

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
				addrs := m.host.Peerstore().Addrs(senderID)
				for _, addr := range addrs {
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

			if len(msg.Data) > 0 && len(msg.Data) < 200 {
				if string(msg.Data[:min(10, len(msg.Data))]) == "HEARTBEAT:" {
					content := string(msg.Data)
					parts := strings.Split(content, ":")
					pinnedCount := 0
					if len(parts) >= 3 {
						fmt.Sscanf(parts[2], "%d", &pinnedCount)
					}
					m.handleHeartbeat(senderID, shardID, ip, pinnedCount)
					continue
				}
			}

			// Nodes re-announce pinned files in heartbeat batch as "PINNED:<ManifestCID>" (plain text)
			if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
				manifestCIDStr := string(msg.Data[7:])
				if manifestCID, err := cid.Decode(manifestCIDStr); err == nil {
					im := schema.IngestMessage{ManifestCID: manifestCID, ShardID: shardID}
					m.handleIngestMessage(&im, senderID, shardID, ip)
					log.Printf("[Monitor] Replication: received PINNED manifest from %s (shard %s)", senderID.String()[:12], shardID)
				} else {
					log.Printf("[Monitor] Replication: PINNED decode failed (cid=%s) from %s: %v", manifestCIDStr[:min(20, len(manifestCIDStr))], senderID.String()[:12], err)
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
				log.Printf("[Monitor] Replication: received CBOR IngestMessage from %s (shard %s)", senderID.String()[:12], targetShard)
				continue
			}

			var rr schema.ReplicationRequest
			if err := rr.UnmarshalCBOR(msg.Data); err == nil {
				m.handleHeartbeat(senderID, shardID, ip, 0)
				continue
			}

			var ur schema.UnreplicateRequest
			if err := ur.UnmarshalCBOR(msg.Data); err == nil {
				m.handleHeartbeat(senderID, shardID, ip, 0)
				continue
			}
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
					child0 := shardID + "0"
					child1 := shardID + "1"
					if shardID == "" {
						child0 = "0"
						child1 = "1"
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

			removedCIDs := 0
			for cid, lastSeen := range m.uniqueCIDs {
				if lastSeen.Before(cutoff) {
					delete(m.uniqueCIDs, cid)
					removedCIDs++
				}
			}

			removedFiles := 0
			for nodeID, files := range m.nodeFiles {
				for fileCID, lastSeen := range files {
					if lastSeen.Before(cutoff) {
						delete(files, fileCID)
						removedFiles++
						if nodeState, exists := m.nodes[nodeID]; exists && nodeState.announcedFiles != nil {
							delete(nodeState.announcedFiles, fileCID)
							nodeState.PinnedFiles = len(nodeState.announcedFiles)
							nodeState.KnownFiles = len(nodeState.announcedFiles)
						}
					}
				}
				if len(files) == 0 {
					delete(m.nodeFiles, nodeID)
				}
			}

			m.mu.Unlock()
			if removedCIDs > 0 || removedFiles > 0 {
				log.Printf("[Monitor] Cleaned up %d stale CIDs and %d stale node files", removedCIDs, removedFiles)
			}
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

	log.Printf("[Monitor] Monitor will listen on all shard topics for telemetry (silent listener)")
	go monitor.cleanupStaleCIDs(ctx)
	monitor.ensureShardSubscription(ctx, "")
	go monitor.subscribeToActiveShards(ctx)

	notifee := &discoveryNotifee{h: h}
	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, notifee)
	if err := mdnsSvc.Start(); err != nil {
		return nil, fmt.Errorf("mdns error: %w", err)
	}

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

		// Backend Search/Filtering Logic
		query := strings.ToLower(r.URL.Query().Get("q"))

		response := make(map[string]interface{})
		for id, node := range monitor.nodes {
			// Filter if query is present
			if query != "" {
				match := strings.Contains(strings.ToLower(id), query) ||
					strings.Contains(strings.ToLower(node.Region), query) ||
					strings.Contains(strings.ToLower(node.CurrentShard), query)
				if !match {
					continue
				}
			}

			status := StatusResponse{
				PeerID:       node.PeerID,
				Version:      "1.0.0",
				CurrentShard: node.CurrentShard,
				PeersInShard: 0,
				Storage: StorageStatus{
					PinnedFiles: node.PinnedFiles,
					KnownFiles:  node.KnownFiles,
					KnownCIDs:   []string{},
				},
				Replication: ReplicationStatus{
					QueueDepth:              0,
					ActiveWorkers:           0,
					AvgReplicationLevel:     0,
					FilesAtTarget:           0,
					ReplicationDistribution: [11]int{},
				},
				UptimeSeconds: time.Since(node.LastSeen).Seconds(),
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
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count": count,
		})
	})

	mux.HandleFunc("/api/replication", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		dist, avg, atTarget := monitor.getReplicationStats()
		totalManifests := 0
		for _, n := range dist {
			totalManifests += n
		}
		log.Printf("[Monitor] Replication: API request -> manifests=%d avg=%.2f atTarget=%d distribution=%v", totalManifests, avg, atTarget, dist)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"replication_distribution": dist,
			"avg_replication_level":    avg,
			"files_at_target":          atTarget,
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
                <input type="text" id="nodeSearch" placeholder="SEARCH ID/REGION/SHARD..." 
                       style="padding: 8px; border: 1px solid #333; width: 300px; font-family:inherit;"
                       onkeyup="debouncedFilter()">
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
        let searchTimeout;
        
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
            // Use backend search query
            const q = document.getElementById('nodeSearch').value;
            const url = '/api/nodes' + (q ? '?q=' + encodeURIComponent(q) : '');

            fetch(url).then(r=>r.json()).then(data => {
                const aliases = getAliases();
                const nodes = Object.values(data).map(n => n.data);
                const meta = data;
                
                document.getElementById('total-nodes').textContent = nodes.length;
                document.getElementById('total-pinned').textContent = nodes.reduce((s,n) => s + n.storage.pinned_files, 0).toLocaleString();
                
                fetch('/api/unique-cids').then(r=>r.json()).then(data => {
                    document.getElementById('unique-files').textContent = (data.count || 0).toLocaleString();
                }).catch(() => {
                    const uCids = new Set();
                    nodes.forEach(n => (n.storage.known_cids||[]).forEach(c => uCids.add(c)));
                    document.getElementById('unique-files').textContent = uCids.size.toLocaleString();
                });
                document.getElementById('total-shards').textContent = new Set(nodes.map(n => n.current_shard)).size;

                // Replication chart: use inferred network-wide stats from /api/replication (eavesdrop on IngestMessage)
                // Cache-bust so browser does not serve stale empty response
                fetch('/api/replication?t=' + Date.now()).then(r=>r.json()).then(data => {
                    const dist = data.replication_distribution || [0,0,0,0,0,0,0,0,0,0,0];
                    if (replicationChart && replicationChart.data && replicationChart.data.datasets && replicationChart.data.datasets[0]) {
                        replicationChart.data.datasets[0].data = Array.isArray(dist) ? dist : [0,0,0,0,0,0,0,0,0,0,0];
                        replicationChart.update();
                    }
                }).catch(() => {
                    if (replicationChart && replicationChart.data && replicationChart.data.datasets && replicationChart.data.datasets[0]) {
                        replicationChart.data.datasets[0].data = [0,0,0,0,0,0,0,0,0,0,0];
                        replicationChart.update();
                    }
                });

                filesChart.data.labels = nodes.map(n => aliases[n.peer_id] || n.peer_id.slice(-6));
                filesChart.data.datasets[0].data = nodes.map(n => n.storage.pinned_files);
                filesChart.update();

                const sCounts = {};
                nodes.forEach(n => sCounts[n.current_shard] = (sCounts[n.current_shard]||0)+1);
                shardChart.data.labels = Object.keys(sCounts);
                shardChart.data.datasets[0].data = Object.values(sCounts);
                
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

        function debouncedFilter() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                updateDashboard();
            }, 300);
        }

        initCharts();
        updateDashboard();
        updateShardTree();
        setInterval(() => { 
            // Only auto-refresh if not searching AND not editing, to avoid killing the input
            if(!document.getElementById('nodeSearch').value && currentlyEditingPeerID === null) {
                updateDashboard(); 
                updateShardTree(); 
            }
        }, 2000);
    </script>
</body>
</html>
`
