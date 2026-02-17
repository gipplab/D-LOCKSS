package main

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

func (m *Monitor) handleIngestMessage(im *schema.IngestMessage, senderID peer.ID, shardID string, ip string) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via IngestMessage: %s (shard: %s)", peerIDStr, shardID)
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
		if region := m.lookupGeoIP(ip); region != "" {
			nodeState.Region = region
		}
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

func (m *Monitor) handleHeartbeat(senderID peer.ID, shardID string, ip string, pinnedCount int) {
	m.handleHeartbeatWithRole(senderID, shardID, ip, pinnedCount, "")
}

func (m *Monitor) handleHeartbeatWithRole(senderID peer.ID, shardID string, ip string, pinnedCount int, role string) (shardUpdated bool) {
	now := time.Now()
	peerIDStr := senderID.String()
	if role == "" {
		role = "ACTIVE"
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via heartbeat: %s (shard: %s, pinned: %d, role: %s)", peerIDStr, shardLogLabel(shardID), pinnedCount, role)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
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
	if pinnedCount >= 0 {
		nodeState.PinnedFiles = pinnedCount
		if pinnedCount == 0 {
			// Skip UNPIN_ALL during grace period after first discovery; pinned=0 may be a stale
			// heartbeat from before the node finished pinning (gossip-sub can reorder/delay).
			firstSeen := now
			if len(nodeState.ShardHistory) > 0 {
				firstSeen = nodeState.ShardHistory[0].FirstSeen
			}
			if now.Sub(firstSeen) < unpinGracePeriod {
				// Within grace period: ignore pinned=0 to avoid removing nodes that are still pinning
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
					log.Printf("[Monitor] UNPIN_ALL peer=%s shard=%s (pinned=0, removed from %d manifests)",
						peerIDStr, shardLogLabel(shardID), removedFromManifests)
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
		if region := m.lookupGeoIP(ip); region != "" {
			nodeState.Region = region
		}
	} else if ip != "" && nodeState.Region == "" {
		if region := m.lookupGeoIP(ip); region != "" {
			nodeState.Region = region
		}
	}
	return shardUpdated
}

func (m *Monitor) handleLeaveShard(peerID peer.ID, shardID string) {
	peerIDStr := peerID.String()
	m.mu.Lock()
	defer m.mu.Unlock()
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
	// Reject stale "parent" updates: if newShard is a prefix of lastShard, the node has
	// already moved to a child; the heartbeat on the parent topic is delayed/stale.
	if len(newShard) < len(lastShard) && strings.HasPrefix(lastShard, newShard) {
		return false
	}
	// Reject cross-branch moves: neither shard is ancestor of the other (e.g. 10→0, 0→11).
	// Valid moves are split (to child), merge (to parent), or sibling; cross-branch is stale.
	// Sibling moves (0↔1, 00↔01) are allowed; they are handled by cooldown below.
	if !isSiblingShard(lastShard, newShard) &&
		!strings.HasPrefix(lastShard, newShard) && !strings.HasPrefix(newShard, lastShard) {
		return false
	}
	// Reject stale "sibling" updates: if lastShard and newShard are siblings (e.g. 0 and 1, 00 and 01),
	// block any sibling move for this peer within cooldown. This prevents both A→B and B→A oscillation
	// from delayed heartbeats (we can't tell which message is stale, so we require a settling period).
	if isSiblingShard(lastShard, newShard) {
		if r, ok := m.peerLastSiblingMove[node.PeerID]; ok && timestamp.Sub(r.when) < siblingMoveCooldown {
			return false
		}
		// Will record after accepting
	}

	m.treeDirty = true
	log.Printf("[Monitor] SHARD_MOVE peer=%s from=%s to=%s",
		node.PeerID, shardLogLabel(lastShard), shardLogLabel(newShard))
	if len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard) {
		// Log split only once per parent->child pair to avoid redundant logs
		alreadyLogged := false
		for _, ev := range m.splitEvents {
			if ev.ParentShard == lastShard && ev.ChildShard == newShard {
				alreadyLogged = true
				break
			}
		}
		if !alreadyLogged {
			log.Printf("[Monitor] Detected shard split: %s -> %s (node: %s)", shardLogLabel(lastShard), newShard, node.PeerID)
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
		// Use the observed shard from PINNED announcements (set by nodes using PayloadCID-based
		// shard assignment). Only remove if the manifest's observed shard is incompatible with
		// the peer's new shard (neither is a prefix of the other).
		// Merge moves (e.g. 1→0): node in shard 0 is incompatible with manifests in branch 1
		// (01, 1, 10, 11); removal is correct—nodes unpin when merging per shard design.
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
		log.Printf("[Monitor] Shard move: removed peer %s from %d manifests (now only in shard %s)",
			peerIDStr, removed, shardLogLabel(newShard))
	}
	if isSiblingShard(lastShard, newShard) {
		m.peerLastSiblingMove[peerIDStr] = siblingMoveRecord{from: lastShard, to: newShard, when: timestamp}
	}
	return true
}

func (m *Monitor) getPinnedInShardForNode(peerIDStr string, nodeShard string) int {
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
		// Use observed shard from PINNED announcements.
		targetShard := m.manifestShard[manifest]
		if targetShard != nodeShard {
			continue
		}
		count++
	}
	return count
}

func (m *Monitor) ensureMinPinnedForPeer(peerIDStr string, min int) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
		shard := node.CurrentShard
		if shard == "" && len(node.ShardHistory) > 0 {
			shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
		}
		short := peerIDStr
		shardToPeers[shard] = append(shardToPeers[shard], short)
	}
	for shard := range shardToPeers {
		sort.Strings(shardToPeers[shard])
	}
	return shardToPeers
}
