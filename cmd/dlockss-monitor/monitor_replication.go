package main

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"dlockss/internal/common"
)

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

		dist, avgLevel, filesAtTarget := m.getReplicationStats()
		byShard := m.getReplicationByShard()
		membership := m.getShardMembership()
		var totalFiles int
		for _, c := range dist {
			totalFiles += c
		}
		shardLabels := make([]string, 0, len(membership))
		totalNodes := 0
		for s, peers := range membership {
			shardLabels = append(shardLabels, s)
			totalNodes += len(peers)
		}
		sort.Strings(shardLabels)
		var b strings.Builder
		for _, shard := range shardLabels {
			peers := membership[shard]
			atTarget := byShard[shard]
			shardLabel := shard
			if shardLabel == "" {
				shardLabel = "(root)"
			}
			fmt.Fprintf(&b, " %s: %d nodes [%s] %d files at target;", shardLabel, len(peers), strings.Join(peers, ","), atTarget)
		}
		log.Printf("[Monitor] SNAPSHOT total_nodes=%d total_manifests=%d total_at_target=%d avg_replication=%.2f |%s",
			totalNodes, totalFiles, filesAtTarget, avgLevel, strings.TrimSpace(b.String()))
		if totalFiles == 0 && totalNodes > 0 {
			m.mu.RLock()
			knownManifests := len(m.manifestReplication)
			m.mu.RUnlock()
			if knownManifests == 0 {
				log.Printf("[Monitor] Hint: total_manifests=0 — monitor may have started after nodes pinned; replication stats need PINNED/IngestMessage. Start monitor before ingestion or have nodes re-announce pins.")
			}
		}
	}
}

func targetShardForManifest(manifestCIDStr string, depth int) string {
	if depth <= 0 {
		return ""
	}
	hexStr := common.KeyToStableHex(manifestCIDStr)
	return common.GetHexBinaryPrefix(hexStr, depth)
}

func effectiveTargetShardForManifest(manifestCIDStr string, depth int, shardPeerCount map[string]int) (targetShard string, maxRep int) {
	for d := depth; d >= 0; d-- {
		shard := targetShardForManifest(manifestCIDStr, d)
		n := shardPeerCount[shard]
		if n > 0 {
			return shard, n
		}
	}
	return "", 0
}

func (m *Monitor) replicationNetworkDepth() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.replicationNetworkDepthUnlocked()
}

// replicationNetworkDepthUnlocked returns max shard depth; caller must hold m.mu (at least RLock).
func (m *Monitor) replicationNetworkDepthUnlocked() int {
	maxLen := 0
	for _, node := range m.nodes {
		shard := node.CurrentShard
		if shard == "" && len(node.ShardHistory) > 0 {
			shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
		}
		if len(shard) > maxLen {
			maxLen = len(shard)
		}
	}
	return maxLen
}

func (m *Monitor) getReplicationStats() (distribution [11]int, avgLevel float64, filesAtTarget int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shardPeerCount := make(map[string]int)
	for _, node := range m.nodes {
		shard := node.CurrentShard
		if shard == "" && len(node.ShardHistory) > 0 {
			shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
		}
		shardPeerCount[shard]++
	}

	depth := 0
	for _, n := range m.nodes {
		shard := n.CurrentShard
		if shard == "" && len(n.ShardHistory) > 0 {
			shard = n.ShardHistory[len(n.ShardHistory)-1].ShardID
		}
		if len(shard) > depth {
			depth = len(shard)
		}
	}
	cutoff := time.Now().Add(-ReplicationAnnounceTTL)

	var totalReplication int
	var manifestCount int
	for manifest, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		targetShard, maxRep := effectiveTargetShardForManifest(manifest, depth, shardPeerCount)
		if maxRep == 0 {
			maxRep = len(peers)
		}
		count := 0
		for peerID := range peers {
			node, ok := m.nodes[peerID]
			if !ok {
				continue
			}
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			if shard != targetShard {
				continue
			}
			if m.peerShardLastSeen[peerID] != nil {
				if last := m.peerShardLastSeen[peerID][targetShard]; last.Before(cutoff) {
					continue
				}
			}
			count++
		}
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
		minRep := MonitorMinReplication
		if maxRep > 0 && minRep > maxRep {
			minRep = maxRep
		}
		if count >= minRep && count <= maxRep {
			filesAtTarget++
		}
	}
	if manifestCount > 0 {
		avgLevel = float64(totalReplication) / float64(manifestCount)
	}
	return distribution, avgLevel, filesAtTarget
}

func (m *Monitor) getReplicationByShard() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shardPeerCount := make(map[string]int)
	for _, node := range m.nodes {
		shard := node.CurrentShard
		if shard == "" && len(node.ShardHistory) > 0 {
			shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
		}
		shardPeerCount[shard]++
	}

	depth := 0
	for _, n := range m.nodes {
		shard := n.CurrentShard
		if shard == "" && len(n.ShardHistory) > 0 {
			shard = n.ShardHistory[len(n.ShardHistory)-1].ShardID
		}
		if len(shard) > depth {
			depth = len(shard)
		}
	}
	cutoff := time.Now().Add(-ReplicationAnnounceTTL)

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
			if m.peerShardLastSeen[peerID] != nil {
				if last := m.peerShardLastSeen[peerID][shard]; last.Before(cutoff) {
					continue
				}
			}
			perManifestPerShard[manifest][shard]++
		}
	}

	filesAtTargetPerShard := make(map[string]int)
	for manifest, shardCounts := range perManifestPerShard {
		targetShard, maxRep := effectiveTargetShardForManifest(manifest, depth, shardPeerCount)
		if maxRep == 0 {
			continue
		}
		count := shardCounts[targetShard]
		minRep := MonitorMinReplication
		if minRep > maxRep {
			minRep = maxRep
		}
		if count >= minRep && count <= maxRep {
			filesAtTargetPerShard[targetShard]++
		}
	}
	return filesAtTargetPerShard
}
