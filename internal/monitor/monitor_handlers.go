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

// writeJSONError writes a JSON {"error":"..."} response with the given status code.
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

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		slog.Info("new node discovered via ingest message", "peer", peerIDStr, "shard", shardID)
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
	nodeState.announcedFiles[manifestCIDStr] = now

	if m.nodeFiles[peerIDStr] == nil {
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
	}
	m.nodeFiles[peerIDStr][manifestCIDStr] = now

	nodeState.KnownFiles = len(nodeState.announcedFiles)
	if n := len(nodeState.announcedFiles); n > nodeState.PinnedFiles {
		nodeState.PinnedFiles = n
	}
	m.uniqueCIDs[manifestCIDStr] = now

	if m.manifestReplication[manifestCIDStr] == nil {
		m.manifestReplication[manifestCIDStr] = make(map[string]time.Time)
	}
	m.manifestReplication[manifestCIDStr][peerIDStr] = now
	// Prefer deeper shard in same subtree; ignore sibling-shard announcements.
	if existing, ok := m.manifestShard[manifestCIDStr]; !ok || (len(shardID) > len(existing) && strings.HasPrefix(shardID, existing)) {
		m.manifestShard[manifestCIDStr] = shardID
	}
	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)

	if m.ps != nil {
		m.ensureShardSubscriptionUnlocked(context.Background(), shardID)
	}

	if ip != "" && ip != nodeState.IPAddress {
		nodeState.IPAddress = ip
	}
}

func isSiblingShard(a, b string) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	parent := a[:len(a)-1]
	return parent == b[:len(b)-1] && a != b
}

func (m *Monitor) setPeerShardLastSeenUnlocked(peerIDStr, shardID string, t time.Time) {
	if m.peerShardLastSeen[peerIDStr] == nil {
		m.peerShardLastSeen[peerIDStr] = make(map[string]time.Time)
	}
	m.peerShardLastSeen[peerIDStr][shardID] = t
}

func (m *Monitor) handleHeartbeat(ctx context.Context, senderID peer.ID, shardID string, ip string, pinnedCount int) {
	m.handleHeartbeatWithRole(ctx, senderID, shardID, ip, pinnedCount, "", "")
}

func (m *Monitor) handleHeartbeatWithRole(ctx context.Context, senderID peer.ID, shardID string, ip string, pinnedCount int, role string, nodeName string) (shardUpdated bool) {
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

	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		logName := peerIDStr
		if nodeName != "" {
			logName = nodeName + " (" + peerIDStr + ")"
		}
		slog.Info("new node discovered via heartbeat", "peer", logName, "shard", shardLogLabel(shardID), "pinned", pinnedCount, "role", role)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
			NodeName:       nodeName,
			CurrentShard:   shardID,
			Role:           role,
			PinnedFiles:    pinnedCount,
			KnownFiles:     0,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = nodeState
		m.treeDirty = true
		return true
	}
	nodeState.LastSeen = now
	nodeState.Role = role
	if nodeName != "" {
		nodeState.NodeName = nodeName
	}
	if pinnedCount >= 0 {
		nodeState.PinnedFiles = pinnedCount
		if pinnedCount == 0 {
			firstSeen := now
			if len(nodeState.ShardHistory) > 0 {
				firstSeen = nodeState.ShardHistory[0].FirstSeen
			}
			// Ignore pinned=0 during grace period: stale heartbeats can arrive
			// before the node finishes its first pin cycle.
			if now.Sub(firstSeen) < unpinGracePeriod {
				// nop
			} else {
				removedFromManifests := 0
				for manifest, peers := range m.manifestReplication {
					if _, had := peers[peerIDStr]; had {
						delete(peers, peerIDStr)
						removedFromManifests++
						if len(peers) == 0 {
							delete(m.manifestReplication, manifest)
						}
					}
				}
				if removedFromManifests > 0 {
					slog.Info("unpin all", "peer", peerIDStr, "shard", shardLogLabel(shardID), "removed_manifests", removedFromManifests)
				}
			}
		} else {
			// Peer is alive and pinning: refresh manifestReplication timestamps
			// so entries don't expire between PINNED re-announcements.
			for _, peers := range m.manifestReplication {
				if _, ok := peers[peerIDStr]; ok {
					peers[peerIDStr] = now
				}
			}
		}
	}
	if nodeState.CurrentShard == "" {
		nodeState.CurrentShard = shardID
		nodeState.ShardHistory = append(nodeState.ShardHistory, ShardHistoryEntry{ShardID: shardID, FirstSeen: now})
		m.treeDirty = true
		shardUpdated = true
	} else {
		shardUpdated = m.updateNodeShardLocked(nodeState, shardID, now)
	}
	if ip != "" && ip != nodeState.IPAddress {
		nodeState.IPAddress = ip
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
	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)
	if node.CurrentShard == shardID {
		node.CurrentShard = ""
		node.LastSeen = now // Refresh TTL: node is alive and transitioning; gives time to JOIN new shard
		m.treeDirty = true
	}
}

func (m *Monitor) updateNodeShardLocked(node *NodeState, newShard string, timestamp time.Time) bool {
	if len(node.ShardHistory) == 0 {
		return false
	}
	lastShard := node.ShardHistory[len(node.ShardHistory)-1].ShardID

	if lastShard == newShard {
		return false
	}
	// Stale parent heartbeat: node already moved to a child shard.
	if len(newShard) < len(lastShard) && strings.HasPrefix(lastShard, newShard) {
		return false
	}
	// Cross-branch moves (e.g. 10→0) are always stale; only ancestor, descendant
	// and sibling transitions are valid.
	if !isSiblingShard(lastShard, newShard) &&
		!strings.HasPrefix(lastShard, newShard) && !strings.HasPrefix(newShard, lastShard) {
		return false
	}
	// Throttle sibling moves (e.g. 0↔1) to suppress oscillation from delayed heartbeats.
	if isSiblingShard(lastShard, newShard) {
		if r, ok := m.peerLastSiblingMove[node.PeerID]; ok && timestamp.Sub(r.when) < siblingMoveCooldown {
			return false
		}
	}

	m.treeDirty = true
	slog.Info("shard move", "peer", node.PeerID, "from", shardLogLabel(lastShard), "to", shardLogLabel(newShard))

	isSplit := len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard)
	if isSplit {
		if !m.hasSplitEvent(lastShard, newShard) {
			slog.Info("detected shard split", "parent", shardLogLabel(lastShard), "child", newShard, "peer", node.PeerID)
		}
		m.lastSplitTime = timestamp
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

	peerIDStr := node.PeerID
	removed := 0
	for manifest, peers := range m.manifestReplication {
		if _, had := peers[peerIDStr]; !had {
			continue
		}
		// Remove peer from manifests whose shard is incompatible with the
		// peer's new shard (neither is an ancestor of the other).
		observedShard := m.manifestShard[manifest]
		compatible := observedShard == newShard ||
			strings.HasPrefix(newShard, observedShard) ||
			strings.HasPrefix(observedShard, newShard)
		if !compatible {
			delete(peers, peerIDStr)
			removed++
			if len(peers) == 0 {
				delete(m.manifestReplication, manifest)
			}
		}
	}
	if removed > 0 {
		slog.Info("shard move removed peer from manifests", "peer", peerIDStr, "removed_manifests", removed, "shard", shardLogLabel(newShard))
	}
	if isSiblingShard(lastShard, newShard) {
		m.peerLastSiblingMove[peerIDStr] = siblingMoveRecord{from: lastShard, to: newShard, when: timestamp}
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
	cutoff := time.Now().Add(-ReplicationAnnounceTTL)
	if m.peerShardLastSeen[peerIDStr] != nil {
		if last := m.peerShardLastSeen[peerIDStr][nodeShard]; last.Before(cutoff) {
			return 0
		}
	}
	count := 0
	for manifest, peers := range m.manifestReplication {
		if _, ok := peers[peerIDStr]; !ok {
			continue
		}
		if m.manifestShard[manifest] != nodeShard {
			continue
		}
		count++
	}
	return count
}

func (m *Monitor) ensureMinPinnedForPeer(ctx context.Context, peerIDStr string, min int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	node, ok := m.nodes[peerIDStr]
	if !ok {
		return
	}
	if node.PinnedFiles < min {
		node.PinnedFiles = min
	}
}

func (m *Monitor) getShardMembership() map[string][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shardToPeers := make(map[string][]string)
	for peerIDStr, node := range m.nodes {
		if !m.isDisplayableNodeUnlocked(peerIDStr, node) {
			continue
		}
		shardToPeers[node.EffectiveShard()] = append(shardToPeers[node.EffectiveShard()], peerIDStr)
	}
	for shard := range shardToPeers {
		sort.Strings(shardToPeers[shard])
	}
	return shardToPeers
}
