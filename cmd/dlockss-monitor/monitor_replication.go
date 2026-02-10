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
				delete(m.manifestShard, manifest)
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

// shardWithMostReplicas returns the shard that has the most replicas for a manifest (from shardCounts),
// or "" if none. Used when the observed/hash-based target shard has 0 replicas (e.g. nodes moved to children).
func shardWithMostReplicas(shardCounts map[string]int, shardPeerCount map[string]int) string {
	var best string
	maxCount := 0
	for shard, c := range shardCounts {
		if c <= 0 {
			continue
		}
		if shardPeerCount[shard] == 0 {
			continue
		}
		if c > maxCount {
			maxCount = c
			best = shard
		}
	}
	return best
}

// sumDescendantReplicasAndNodes returns total replicas and total nodes over all shards that are
// strict descendants of parentShard (same prefix, longer). Used when the logical target shard is
// a parent that has 0 nodes after a split; replicas may be split across child shards (e.g. 10 and 11).
func sumDescendantReplicasAndNodes(manifestShardCounts map[string]int, shardPeerCount map[string]int, parentShard string) (totalReplicas int, totalNodes int) {
	for shard, c := range manifestShardCounts {
		if strings.HasPrefix(shard, parentShard) && len(shard) > len(parentShard) {
			totalReplicas += c
			totalNodes += shardPeerCount[shard]
		}
	}
	return totalReplicas, totalNodes
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
		// Use observed shard from PINNED announcements (nodes use PayloadCID for shard assignment).
		// Fall back to ManifestCID-based computation if never observed OR if the observed
		// shard is now empty (e.g. parent shard after a split).
		targetShard := m.manifestShard[manifest]
		if targetShard == "" || shardPeerCount[targetShard] == 0 {
			targetShard, _ = effectiveTargetShardForManifest(manifest, depth, shardPeerCount)
		}
		// Build per-shard replica counts for this manifest (for fallback when target has 0).
		manifestShardCounts := make(map[string]int)
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
			manifestShardCounts[shard]++
		}
		count := manifestShardCounts[targetShard]
		maxRep := shardPeerCount[targetShard]
		// When logical target is a parent with 0 nodes (after split), replicas may be split across
		// child shards (e.g. 10 and 11). Aggregate count and maxRep over all descendant shards
		// so the manifest can be counted as at target when total replicas are sufficient.
		if maxRep == 0 && len(targetShard) > 0 {
			descReplicas, descNodes := sumDescendantReplicasAndNodes(manifestShardCounts, shardPeerCount, targetShard)
			if descReplicas > 0 {
				count = descReplicas
				maxRep = descNodes
			}
		}
		// When target is one child (e.g. "00") but replicas are split across siblings (00+01),
		// manifestShard may have been set to "00" by the first announcement. If we're still
		// under-replicated, try aggregating over the parent's descendants (parent has 0 nodes after split).
		if count > 0 && len(targetShard) >= 1 {
			minRep := MonitorMinReplication
			if maxRep > 0 && minRep > maxRep {
				minRep = maxRep
			}
			if count < minRep {
				parent := targetShard[:len(targetShard)-1]
				if shardPeerCount[parent] == 0 {
					descReplicas, descNodes := sumDescendantReplicasAndNodes(manifestShardCounts, shardPeerCount, parent)
					if descReplicas > count {
						count = descReplicas
						maxRep = descNodes
					}
				}
			}
		}
		// If observed/hash-based target has no replicas (e.g. nodes moved to child shards),
		// use the shard where this manifest actually has replicas.
		if count == 0 {
			targetShard = shardWithMostReplicas(manifestShardCounts, shardPeerCount)
			if targetShard == "" {
				continue
			}
			count = manifestShardCounts[targetShard]
			maxRep = shardPeerCount[targetShard]
		}
		if maxRep == 0 {
			maxRep = len(peers)
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
		// Use observed shard from PINNED announcements; fall back to ManifestCID-based computation
		// if never observed or if the observed shard is now empty.
		targetShard := m.manifestShard[manifest]
		if targetShard == "" || shardPeerCount[targetShard] == 0 {
			targetShard, _ = effectiveTargetShardForManifest(manifest, depth, shardPeerCount)
		}
		count := shardCounts[targetShard]
		maxRep := shardPeerCount[targetShard]
		// When logical target is a parent with 0 nodes (after split), aggregate over descendant shards.
		attributeShard := targetShard // shard we credit for "files at target" (for per-shard display)
		if maxRep == 0 && len(targetShard) > 0 {
			descReplicas, descNodes := sumDescendantReplicasAndNodes(shardCounts, shardPeerCount, targetShard)
			if descReplicas > 0 {
				count = descReplicas
				maxRep = descNodes
				attributeShard = shardWithMostReplicas(shardCounts, shardPeerCount) // credit child with most replicas
			}
		}
		// When target is one child but replicas are split across siblings, try parent's descendants.
		minRep := MonitorMinReplication
		if maxRep > 0 && minRep > maxRep {
			minRep = maxRep
		}
		if count > 0 && count < minRep && len(targetShard) >= 1 {
			parent := targetShard[:len(targetShard)-1]
			if shardPeerCount[parent] == 0 {
				descReplicas, descNodes := sumDescendantReplicasAndNodes(shardCounts, shardPeerCount, parent)
				if descReplicas > count {
					count = descReplicas
					maxRep = descNodes
					attributeShard = shardWithMostReplicas(shardCounts, shardPeerCount)
				}
			}
		}
		// If observed/hash-based target has no replicas (e.g. nodes moved to child shards after
		// first PINNED from parent), assign this manifest to the shard where it actually has replicas.
		if count == 0 {
			targetShard = shardWithMostReplicas(shardCounts, shardPeerCount)
			if targetShard == "" {
				continue
			}
			attributeShard = targetShard
			count = shardCounts[targetShard]
			maxRep = shardPeerCount[targetShard]
		}
		if maxRep == 0 {
			continue
		}
		minRep = MonitorMinReplication
		if minRep > maxRep {
			minRep = maxRep
		}
		if count >= minRep && count <= maxRep {
			filesAtTargetPerShard[attributeShard]++
		}
	}
	return filesAtTargetPerShard
}
