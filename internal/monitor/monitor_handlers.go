package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (m *Monitor) handleIngestMessage(ctx context.Context, im *schema.IngestMessage, senderID peer.ID, shardID string, ip string) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return
	}

	ns, exists := m.nodes[peerIDStr]
	if !exists {
		slog.Info("new node discovered via ingest message", "peer", peerIDStr, "shard", shardID)
		ns = &nodeState{
			PeerID:         peerIDStr,
			CurrentShard:   shardID,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = ns
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
		m.treeDirty = true
	}

	ns.LastSeen = now
	manifestCIDStr := im.ManifestCID.String()

	if ns.announcedFiles == nil {
		ns.announcedFiles = make(map[string]time.Time)
	}
	ns.announcedFiles[manifestCIDStr] = now

	if m.nodeFiles[peerIDStr] == nil {
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
	}
	m.nodeFiles[peerIDStr][manifestCIDStr] = now

	ns.KnownFiles = len(ns.announcedFiles)
	ns.PinnedFiles = max(ns.PinnedFiles, len(ns.announcedFiles))
	m.uniqueCIDs[manifestCIDStr] = now

	if m.manifestReplication[manifestCIDStr] == nil {
		m.manifestReplication[manifestCIDStr] = make(map[string]time.Time)
	}
	m.manifestReplication[manifestCIDStr][peerIDStr] = now
	if existing, ok := m.manifestShard[manifestCIDStr]; !ok || (len(shardID) > len(existing) && strings.HasPrefix(shardID, existing)) {
		m.manifestShard[manifestCIDStr] = shardID
	}
	m.setPeerShardLastSeenLocked(peerIDStr, shardID, now)
	m.ensureShardSubscriptionLocked(context.Background(), shardID)

	if ip != "" {
		ns.IPAddress = ip
	}
}

func isSiblingShard(a, b string) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	return a != b && a[:len(a)-1] == b[:len(b)-1]
}

func (m *Monitor) setPeerShardLastSeenLocked(peerIDStr, shardID string, t time.Time) {
	if m.peerShardLastSeen[peerIDStr] == nil {
		m.peerShardLastSeen[peerIDStr] = make(map[string]time.Time)
	}
	m.peerShardLastSeen[peerIDStr][shardID] = t
}

// dropPeerReplicasLocked removes peerIDStr from every manifest for which drop
// returns true and reports how many manifests it was removed from.
func (m *Monitor) dropPeerReplicasLocked(peerIDStr string, drop func(manifest string) bool) int {
	removed := 0
	for manifest, peers := range m.manifestReplication {
		if _, ok := peers[peerIDStr]; !ok || !drop(manifest) {
			continue
		}
		delete(peers, peerIDStr)
		removed++
		if len(peers) == 0 {
			delete(m.manifestReplication, manifest)
		}
	}
	return removed
}

// handleHeartbeat records that a peer is alive in shardID. A negative
// pinnedCount means the message carried no pin count.
func (m *Monitor) handleHeartbeat(ctx context.Context, senderID peer.ID, shardID string, ip string, pinnedCount int, role string, nodeName string) (shardUpdated bool) {
	now := time.Now()
	peerIDStr := senderID.String()
	if role == "" {
		role = "ACTIVE"
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}

	m.setPeerShardLastSeenLocked(peerIDStr, shardID, now)

	ns, exists := m.nodes[peerIDStr]
	if !exists {
		slog.Info("new node discovered via heartbeat", "peer", peerLabel(peerIDStr, nodeName), "shard", shardLabel(shardID), "pinned", pinnedCount, "role", role)
		m.nodes[peerIDStr] = &nodeState{
			PeerID:         peerIDStr,
			NodeName:       nodeName,
			CurrentShard:   shardID,
			Role:           role,
			PinnedFiles:    pinnedCount,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.treeDirty = true
		return true
	}
	ns.LastSeen = now
	ns.Role = role
	if nodeName != "" {
		ns.NodeName = nodeName
	}
	switch {
	case pinnedCount == 0:
		ns.PinnedFiles = 0
		firstSeen := now
		if len(ns.ShardHistory) > 0 {
			firstSeen = ns.ShardHistory[0].FirstSeen
		}
		if now.Sub(firstSeen) >= unpinGracePeriod {
			removed := m.dropPeerReplicasLocked(peerIDStr, func(string) bool { return true })
			if removed > 0 {
				slog.Info("unpin all", "peer", peerIDStr, "shard", shardLabel(shardID), "removed_manifests", removed)
			}
		}
	case pinnedCount > 0:
		ns.PinnedFiles = pinnedCount
		for _, peers := range m.manifestReplication {
			if _, ok := peers[peerIDStr]; ok {
				peers[peerIDStr] = now
			}
		}
	}
	if ns.CurrentShard == "" {
		ns.CurrentShard = shardID
		ns.ShardHistory = append(ns.ShardHistory, ShardHistoryEntry{ShardID: shardID, FirstSeen: now})
		m.treeDirty = true
		shardUpdated = true
	} else {
		shardUpdated = m.updateNodeShardLocked(ns, shardID, now)
	}
	if ip != "" {
		ns.IPAddress = ip
	}
	return shardUpdated
}

func (m *Monitor) handleLeaveShard(ctx context.Context, peerID peer.ID, shardID string) {
	peerIDStr := peerID.String()
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	node, exists := m.nodes[peerIDStr]
	if !exists {
		return
	}
	now := time.Now()
	m.setPeerShardLastSeenLocked(peerIDStr, shardID, now)
	if node.CurrentShard == shardID {
		node.CurrentShard = ""
		// The node is alive and moving; refreshing LastSeen gives it time to
		// JOIN its new shard before it is pruned.
		node.LastSeen = now
		m.treeDirty = true
	}
}

func (m *Monitor) updateNodeShardLocked(node *nodeState, newShard string, timestamp time.Time) bool {
	if len(node.ShardHistory) == 0 {
		return false
	}
	lastShard := node.ShardHistory[len(node.ShardHistory)-1].ShardID
	if lastShard == newShard {
		return false
	}
	// A node only moves down into a child shard (a split) or across to a
	// sibling. A parent shard or another branch (such as 10 to 0) comes from a
	// delayed message about an earlier position.
	isSplit := strings.HasPrefix(newShard, lastShard)
	if !isSplit && !isSiblingShard(lastShard, newShard) {
		return false
	}
	if !isSplit {
		if last, ok := m.peerLastSiblingMove[node.PeerID]; ok && timestamp.Sub(last) < siblingMoveCooldown {
			return false
		}
	}

	m.treeDirty = true
	slog.Info("shard move", "peer", node.PeerID, "from", shardLabel(lastShard), "to", shardLabel(newShard))

	if isSplit {
		if !m.hasSplitEvent(lastShard, newShard) {
			slog.Info("detected shard split", "parent", shardLabel(lastShard), "child", newShard, "peer", node.PeerID)
		}
		m.lastSplitTime = timestamp
		m.splitEvents = append(m.splitEvents, ShardSplitEvent{
			ParentShard: lastShard,
			ChildShard:  newShard,
			Timestamp:   timestamp,
		})
	}
	m.ensureShardSubscriptionLocked(context.Background(), newShard)
	if lastShard == "" && (newShard == "0" || newShard == "1") {
		sibling := "1"
		if newShard == "1" {
			sibling = "0"
		}
		m.ensureShardSubscriptionLocked(context.Background(), sibling)
	}
	node.CurrentShard = newShard
	node.ShardHistory = append(node.ShardHistory, ShardHistoryEntry{ShardID: newShard, FirstSeen: timestamp})

	// The peer no longer replicates manifests whose shard is neither an
	// ancestor nor a descendant of its new shard.
	removed := m.dropPeerReplicasLocked(node.PeerID, func(manifest string) bool {
		observed := m.manifestShard[manifest]
		return !strings.HasPrefix(newShard, observed) && !strings.HasPrefix(observed, newShard)
	})
	if removed > 0 {
		slog.Info("shard move removed peer from manifests", "peer", node.PeerID, "removed_manifests", removed, "shard", shardLabel(newShard))
	}
	if !isSplit {
		m.peerLastSiblingMove[node.PeerID] = timestamp
	}
	return true
}

func (m *Monitor) hasSplitEvent(parent, child string) bool {
	for _, ev := range m.splitEvents {
		if ev.ParentShard == parent && ev.ChildShard == child {
			return true
		}
	}
	return false
}

func (m *Monitor) getPinnedInShardForNode(peerIDStr, nodeShard string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cutoff := time.Now().Add(-replicationAnnounceTTL)
	if m.peerShardLastSeen[peerIDStr] != nil {
		if last := m.peerShardLastSeen[peerIDStr][nodeShard]; last.Before(cutoff) {
			return 0
		}
	}
	count := 0
	for manifest, peers := range m.manifestReplication {
		if _, ok := peers[peerIDStr]; ok && m.manifestShard[manifest] == nodeShard {
			count++
		}
	}
	return count
}

func (m *Monitor) ensureMinPinnedForPeer(ctx context.Context, peerIDStr string, minPinned int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	if node, ok := m.nodes[peerIDStr]; ok {
		node.PinnedFiles = max(node.PinnedFiles, minPinned)
	}
}

func (m *Monitor) getShardMembership() map[string][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shardToPeers := make(map[string][]string)
	for peerIDStr, node := range m.nodes {
		if !m.isDisplayableNodeLocked(peerIDStr, node) {
			continue
		}
		shard := node.EffectiveShard()
		shardToPeers[shard] = append(shardToPeers[shard], peerIDStr)
	}
	for _, peers := range shardToPeers {
		sort.Strings(peers)
	}
	return shardToPeers
}
