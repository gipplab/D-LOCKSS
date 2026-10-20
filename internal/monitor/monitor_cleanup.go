package monitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// splitGracePeriod suspends node pruning after a split so the gossip-sub mesh
// can form on the new shards without nodes being dropped mid-split.
const splitGracePeriod = 5 * time.Minute

func (m *Monitor) PruneStaleNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if !m.lastSplitTime.IsZero() && now.Sub(m.lastSplitTime) < splitGracePeriod {
		return
	}
	pruned := 0
	for id, node := range m.nodes {
		timeout := m.cfg.NodeCleanupTimeout
		// A node between LEAVE and JOIN has no current shard. Its JOIN can be
		// delayed by discovery, mesh formation or slow networks, so it gets
		// twice the timeout.
		if node.CurrentShard == "" {
			timeout *= 2
		}
		if now.Sub(node.LastSeen) <= timeout {
			continue
		}
		delete(m.nodes, id)
		delete(m.nodeFiles, id)
		delete(m.peerShardLastSeen, id)
		delete(m.peerLastSiblingMove, id)
		for manifest, peers := range m.manifestReplication {
			delete(peers, id)
			if len(peers) == 0 {
				delete(m.manifestReplication, manifest)
				delete(m.manifestShard, manifest)
			}
		}
		pruned++
	}
	if pruned > 0 {
		slog.Info("pruned stale nodes", "count", pruned, "timeout", m.cfg.NodeCleanupTimeout)
		m.treeDirty = true
		m.pruneOrphanedSplitEvents()
	}
	m.pruneOldSplitEvents(now)
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
		if len(node.ShardHistory) == 0 {
			continue
		}
		sid := node.ShardHistory[len(node.ShardHistory)-1].ShardID
		currentShards[sid] = true
		for ; sid != ""; sid = sid[:len(sid)-1] {
			currentShards[sid] = true
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
// Peers that GossipSub uses on any subscribed topic are kept even when they are
// not tracked. Evicting them would stop the mesh from re-grafting, so no
// heartbeats would arrive, more nodes would be pruned and the monitor would
// never recover.
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
	topicPeers := make(map[peer.ID]bool)
	for _, ss := range m.shardTopics {
		for _, pid := range ss.topic.ListPeers() {
			topicPeers[pid] = true
		}
	}
	m.mu.RUnlock()

	var evicted int
	for _, pid := range ps.PeersWithAddrs() {
		if pid == selfID || topicPeers[pid] || activeNodes[pid.String()] ||
			m.host.Network().Connectedness(pid) == network.Connected {
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
			m.evictStalePeerstoreEntries()
			m.pruneStaleAnnouncements(time.Now().Add(-30 * time.Minute))
		}
	}
}

// pruneStaleAnnouncements forgets manifest announcements last seen before cutoff.
func (m *Monitor) pruneStaleAnnouncements(cutoff time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for c, lastSeen := range m.uniqueCIDs {
		if lastSeen.Before(cutoff) {
			delete(m.uniqueCIDs, c)
		}
	}
	for nodeID, files := range m.nodeFiles {
		for fileCID, lastSeen := range files {
			if !lastSeen.Before(cutoff) {
				continue
			}
			delete(files, fileCID)
			if node, ok := m.nodes[nodeID]; ok && node.announcedFiles != nil {
				delete(node.announcedFiles, fileCID)
				node.KnownFiles = len(node.announcedFiles)
			}
		}
		if len(files) == 0 {
			delete(m.nodeFiles, nodeID)
		}
	}
}
