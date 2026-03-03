package main

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"dlockss/internal/common"
)

// replicationSnapshot holds pre-computed state shared across replication computations.
// Build once with newReplicationSnapshot(), then call resolveManifest() per manifest.
// Caller must hold m.mu at least as RLock.
type replicationSnapshot struct {
	shardPeerCount map[string]int
	depth          int
	cutoff         time.Time
	m              *Monitor
}

func (m *Monitor) newReplicationSnapshotUnlocked() replicationSnapshot {
	spc := make(map[string]int)
	depth := 0
	for id, node := range m.nodes {
		if !m.isDisplayableNodeUnlocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		spc[shard]++
		if len(shard) > depth {
			depth = len(shard)
		}
	}
	return replicationSnapshot{
		shardPeerCount: spc,
		depth:          depth,
		cutoff:         time.Now().Add(-ReplicationAnnounceTTL),
		m:              m,
	}
}

// manifestResult holds the resolved replication state for a single manifest.
type manifestResult struct {
	count       int
	maxRep      int
	targetShard string
}

// buildShardCounts returns per-shard replica counts for a manifest,
// filtering out stale peers. Caller must hold m.mu.
func (rs *replicationSnapshot) buildShardCounts(peers map[string]time.Time) map[string]int {
	counts := make(map[string]int)
	for peerID := range peers {
		node, ok := rs.m.nodes[peerID]
		if !ok || !rs.m.isDisplayableNodeUnlocked(peerID, node) {
			continue
		}
		shard := node.EffectiveShard()
		if rs.m.peerShardLastSeen[peerID] != nil {
			if last := rs.m.peerShardLastSeen[peerID][shard]; last.Before(rs.cutoff) {
				continue
			}
		}
		counts[shard]++
	}
	return counts
}

// resolveManifest computes the effective replica count and max replication for
// a manifest. Returns zero count if the manifest should be skipped.
func (rs *replicationSnapshot) resolveManifest(manifest string, peers map[string]time.Time, shardCounts map[string]int) manifestResult {
	targetShard := rs.m.manifestShard[manifest]
	if targetShard == "" || rs.shardPeerCount[targetShard] == 0 {
		targetShard, _ = effectiveTargetShardForManifest(manifest, rs.depth, rs.shardPeerCount)
	}

	count := shardCounts[targetShard]
	maxRep := rs.shardPeerCount[targetShard]

	// Parent with 0 nodes after split: aggregate descendant shards.
	if maxRep == 0 && len(targetShard) > 0 {
		descReplicas, descNodes := sumDescendantReplicasAndNodes(shardCounts, rs.shardPeerCount, targetShard)
		if descReplicas > 0 {
			count = descReplicas
			maxRep = descNodes
		}
	}

	// Sibling aggregation: replicas split across children of the same parent.
	if count > 0 && len(targetShard) >= 1 {
		minRep := MonitorMinReplication
		if maxRep > 0 && minRep > maxRep {
			minRep = maxRep
		}
		if count < minRep {
			parent := targetShard[:len(targetShard)-1]
			if rs.shardPeerCount[parent] == 0 {
				descReplicas, descNodes := sumDescendantReplicasAndNodes(shardCounts, rs.shardPeerCount, parent)
				if descReplicas > count {
					count = descReplicas
					maxRep = descNodes
				}
			}
		}
	}

	// Fallback: use shard with most replicas if target has none.
	if count == 0 {
		targetShard = shardWithMostReplicas(shardCounts, rs.shardPeerCount)
		if targetShard == "" {
			return manifestResult{}
		}
		count = shardCounts[targetShard]
		maxRep = rs.shardPeerCount[targetShard]
	}

	if maxRep == 0 {
		maxRep = len(peers)
	}
	if count == 0 {
		return manifestResult{}
	}
	return manifestResult{count: count, maxRep: maxRep, targetShard: targetShard}
}

// isAtTarget returns true if count is within the replication target range.
func (mr manifestResult) isAtTarget() bool {
	minRep := MonitorMinReplication
	if mr.maxRep > 0 && minRep > mr.maxRep {
		minRep = mr.maxRep
	}
	return mr.count >= minRep && mr.count <= mr.maxRep
}

// --- Public methods using the shared helpers ---

func (m *Monitor) runReplicationCleanup() {
	ticker := time.NewTicker(ReplicationCleanupEvery)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
		}
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
		if filesAtTarget == 0 && totalFiles > 0 && totalNodes > 0 {
			log.Printf("[Monitor] Hint: total_at_target=0 with manifests — may be transient during shard churn, cutoff filtering (ReplicationAnnounceTTL=%v), or target/replica mismatch", ReplicationAnnounceTTL)
		}
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

func (m *Monitor) replicationNetworkDepthUnlocked() int {
	maxLen := 0
	for id, node := range m.nodes {
		if !m.isDisplayableNodeUnlocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		if len(shard) > maxLen {
			maxLen = len(shard)
		}
	}
	return maxLen
}

func (m *Monitor) getReplicationStats() (distribution [11]int, avgLevel float64, filesAtTarget int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotUnlocked()
	var totalReplication, manifestCount int

	for manifest, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		shardCounts := rs.buildShardCounts(peers)
		mr := rs.resolveManifest(manifest, peers, shardCounts)
		if mr.count == 0 {
			continue
		}
		manifestCount++
		totalReplication += mr.count
		if mr.count >= 10 {
			distribution[10]++
		} else {
			distribution[mr.count]++
		}
		if mr.isAtTarget() {
			filesAtTarget++
		}
	}
	if manifestCount > 0 {
		avgLevel = float64(totalReplication) / float64(manifestCount)
	}
	return distribution, avgLevel, filesAtTarget
}

func (m *Monitor) getReplicationCIDsByLevel(level int) []CIDEntry {
	if level < 0 || level > 10 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotUnlocked()
	var result []CIDEntry

	for manifest, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		shardCounts := rs.buildShardCounts(peers)
		mr := rs.resolveManifest(manifest, peers, shardCounts)
		if mr.count == 0 {
			continue
		}
		matches := (level == 10 && mr.count >= 10) || (level < 10 && mr.count == level)
		if matches {
			shardLabel := m.manifestShard[manifest]
			if shardLabel == "" {
				shardLabel = "root"
			}
			result = append(result, CIDEntry{CID: manifest, Shard: shardLabel, Replicas: mr.count})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CID < result[j].CID })
	return result
}

func (m *Monitor) getReplicationByShard() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotUnlocked()

	// Build per-manifest shard counts for attribution.
	type manifestInfo struct {
		shardCounts map[string]int
		peers       map[string]time.Time
	}
	manifests := make(map[string]manifestInfo, len(m.manifestReplication))
	for manifest, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		manifests[manifest] = manifestInfo{
			shardCounts: rs.buildShardCounts(peers),
			peers:       peers,
		}
	}

	filesAtTargetPerShard := make(map[string]int)
	for manifest, info := range manifests {
		mr := rs.resolveManifest(manifest, info.peers, info.shardCounts)
		if mr.count == 0 || mr.maxRep == 0 {
			continue
		}
		if mr.isAtTarget() {
			// Attribute to the resolved target shard (or child with most replicas if parent split).
			attributeShard := mr.targetShard
			if rs.shardPeerCount[attributeShard] == 0 {
				if best := shardWithMostReplicas(info.shardCounts, rs.shardPeerCount); best != "" {
					attributeShard = best
				}
			}
			filesAtTargetPerShard[attributeShard]++
		}
	}
	return filesAtTargetPerShard
}
