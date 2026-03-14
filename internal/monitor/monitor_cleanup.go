package monitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SplitGracePeriod: after a split, don't prune nodes for this duration to allow
// gossip-sub mesh formation on new shards (avoids dropping nodes during active splits).
const splitGracePeriod = 5 * time.Minute

func (m *Monitor) PruneStaleNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	// Skip pruning during grace period after a split (mesh may still be forming on new shards).
	if !m.lastSplitTime.IsZero() && now.Sub(m.lastSplitTime) < splitGracePeriod {
		return
	}
	changed := false
	prunedCount := 0
	for id, node := range m.nodes {
		timeout := m.cfg.NodeCleanupTimeout
		// Nodes in transition (CurrentShard == "" after LEAVE) get 2x grace: JOIN on new shard
		// can be delayed by discovery, gossip-sub mesh formation, or slow networks.
		if node.CurrentShard == "" {
			timeout = m.cfg.NodeCleanupTimeout * 2
		}
		if now.Sub(node.LastSeen) > timeout {
			delete(m.nodes, id)
			delete(m.nodeFiles, id)
			delete(m.peerShardLastSeen, id)
			delete(m.peerLastSiblingMove, id)
			// Remove this peer from manifestReplication maps.
			for manifest, peers := range m.manifestReplication {
				delete(peers, id)
				if len(peers) == 0 {
					delete(m.manifestReplication, manifest)
					delete(m.manifestShard, manifest)
				}
			}
			changed = true
			prunedCount++
		}
	}
	if prunedCount > 0 {
		slog.Info("pruned stale nodes", "count", prunedCount, "timeout", m.cfg.NodeCleanupTimeout)
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

// evictStalePeerstoreEntries removes peers from the libp2p peerstore that are
// disconnected and not tracked in the monitor's nodes map. Without this, the
// peerstore grows unbounded from DHT crawls and transient connections.
//
// To avoid a death spiral (prune nodes → evict their peerstore entries →
// GossipSub can't re-graft → no heartbeats → prune more nodes), we also
// protect peers that GossipSub is actively using across any subscribed topic.
func (m *Monitor) evictStalePeerstoreEntries() {
	if m.host == nil {
		return
	}
	ps := m.host.Peerstore()
	selfID := m.host.ID()

	m.mu.RLock()
	activeNodes := make(map[string]bool, len(m.nodes))
	for id := range m.nodes {
		activeNodes[id] = true
	}
	// Protect peers in any GossipSub topic so mesh recovery remains possible
	// even after all tracked nodes have been pruned.
	topicPeers := make(map[peer.ID]bool)
	for _, ss := range m.shardTopics {
		for _, pid := range ss.topic.ListPeers() {
			topicPeers[pid] = true
		}
	}
	m.mu.RUnlock()

	var evicted int
	for _, pid := range ps.PeersWithAddrs() {
		if pid == selfID {
			continue
		}
		if m.host.Network().Connectedness(pid) == network.Connected {
			continue
		}
		if activeNodes[pid.String()] {
			continue
		}
		if topicPeers[pid] {
			continue
		}
		ps.RemovePeer(pid)
		evicted++
	}
	if evicted > 0 {
		slog.Info("peerstore gc evicted stale peers", "count", evicted, "protected_topic_peers", len(topicPeers))
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
			m.geo.evictStaleCache()
			m.evictStalePeerstoreEntries()
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
