package monitor

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"dlockss/internal/common"
)

// maxReplicationLevel is the top bucket of the replication distribution; it
// also counts manifests with more replicas.
const maxReplicationLevel = 10

// replicationSnapshot holds pre-computed state shared across replication computations.
// Build once with newReplicationSnapshotLocked, then call resolveManifest per manifest.
// Caller must hold m.mu at least as RLock.
type replicationSnapshot struct {
	shardPeerCount map[string]int
	depth          int
	cutoff         time.Time
	m              *Monitor
}

func (m *Monitor) newReplicationSnapshotLocked() replicationSnapshot {
	spc := make(map[string]int)
	depth := 0
	for id, node := range m.nodes {
		if !m.isDisplayableNodeLocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		spc[shard]++
		depth = max(depth, len(shard))
	}
	return replicationSnapshot{
		shardPeerCount: spc,
		depth:          depth,
		cutoff:         time.Now().Add(-replicationAnnounceTTL),
		m:              m,
	}
}

// manifestResult holds the resolved replication state for a single manifest.
type manifestResult struct {
	count       int
	maxRep      int
	targetShard string
}

// minReplicas is the replication target for a shard group of maxRep nodes:
// replicationTarget, capped by the number of nodes when that is known.
func minReplicas(maxRep int) int {
	if maxRep > 0 {
		return min(replicationTarget, maxRep)
	}
	return replicationTarget
}

// buildShardCounts returns per-shard replica counts for a manifest,
// filtering out stale peers. Caller must hold m.mu.
func (rs *replicationSnapshot) buildShardCounts(peers map[string]time.Time) map[string]int {
	counts := make(map[string]int)
	for peerID := range peers {
		node, ok := rs.m.nodes[peerID]
		if !ok || !rs.m.isDisplayableNodeLocked(peerID, node) {
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
		targetShard = effectiveTargetShardForManifest(manifest, rs.depth, rs.shardPeerCount)
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
	if count > 0 && len(targetShard) >= 1 && count < minReplicas(maxRep) {
		parent := targetShard[:len(targetShard)-1]
		if rs.shardPeerCount[parent] == 0 {
			descReplicas, descNodes := sumDescendantReplicasAndNodes(shardCounts, rs.shardPeerCount, parent)
			if descReplicas > count {
				count = descReplicas
				maxRep = descNodes
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
	return manifestResult{count: count, maxRep: maxRep, targetShard: targetShard}
}

// isAtTarget returns true if count is within the replication target range.
func (mr manifestResult) isAtTarget() bool {
	return mr.count >= minReplicas(mr.maxRep) && mr.count <= mr.maxRep
}

func (m *Monitor) runReplicationCleanup() {
	ticker := time.NewTicker(replicationCleanupEvery)
	defer ticker.Stop()
	for range ticker.C {
		m.pruneExpiredReplicas()
		m.logReplicationSnapshot()
	}
}

func (m *Monitor) pruneExpiredReplicas() {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Now().Add(-replicationAnnounceTTL)
	for c, peers := range m.manifestReplication {
		for peerID, lastSeen := range peers {
			if lastSeen.Before(cutoff) {
				delete(peers, peerID)
			}
		}
		if len(peers) == 0 {
			delete(m.manifestReplication, c)
			delete(m.manifestShard, c)
		}
	}
}

func (m *Monitor) logReplicationSnapshot() {
	dist, avgLevel, filesAtTarget := m.getReplicationStats()
	byShard := m.getReplicationByShard()
	membership := m.getShardMembership()
	var totalFiles int
	for _, c := range dist {
		totalFiles += c
	}
	shardIDs := make([]string, 0, len(membership))
	totalNodes := 0
	for s, peers := range membership {
		shardIDs = append(shardIDs, s)
		totalNodes += len(peers)
	}
	sort.Strings(shardIDs)
	var b strings.Builder
	for _, shard := range shardIDs {
		peers := membership[shard]
		fmt.Fprintf(&b, " %s: %d nodes [%s] %d files at target;", shardLabel(shard), len(peers), strings.Join(peers, ","), byShard[shard])
	}
	slog.Info("replication snapshot", "total_nodes", totalNodes, "total_manifests", totalFiles, "total_at_target", filesAtTarget, "avg_replication", fmt.Sprintf("%.2f", avgLevel), "shards", strings.TrimSpace(b.String()))
	if filesAtTarget == 0 && totalFiles > 0 && totalNodes > 0 {
		slog.Warn("total at target is zero", "replication_announce_ttl", replicationAnnounceTTL)
	}
	if totalFiles == 0 && totalNodes > 0 {
		m.mu.RLock()
		knownManifests := len(m.manifestReplication)
		m.mu.RUnlock()
		if knownManifests == 0 {
			slog.Warn("no manifests tracked", "hint", "monitor may have started after nodes pinned; replication stats need PINNED/IngestMessage")
		}
	}
}

func effectiveTargetShardForManifest(manifestCIDStr string, depth int, shardPeerCount map[string]int) string {
	for d := depth; d >= 0; d-- {
		shard, err := common.TargetShardForPayload(manifestCIDStr, d)
		if err == nil && shardPeerCount[shard] > 0 {
			return shard
		}
	}
	return ""
}

func shardWithMostReplicas(shardCounts map[string]int, shardPeerCount map[string]int) string {
	var best string
	maxCount := 0
	for shard, c := range shardCounts {
		if c > maxCount && shardPeerCount[shard] > 0 {
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

func (m *Monitor) getReplicationStats() (distribution [maxReplicationLevel + 1]int, avgLevel float64, filesAtTarget int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotLocked()
	var totalReplication, manifestCount int

	for c, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		mr := rs.resolveManifest(c, peers, rs.buildShardCounts(peers))
		if mr.count == 0 {
			continue
		}
		manifestCount++
		totalReplication += mr.count
		distribution[min(mr.count, maxReplicationLevel)]++
		if mr.isAtTarget() {
			filesAtTarget++
		}
	}
	if manifestCount > 0 {
		avgLevel = float64(totalReplication) / float64(manifestCount)
	}
	return distribution, avgLevel, filesAtTarget
}

func (m *Monitor) getReplicationCIDsByLevel(level int) []cidEntry {
	if level < 0 || level > maxReplicationLevel {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotLocked()
	var result []cidEntry

	for c, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		mr := rs.resolveManifest(c, peers, rs.buildShardCounts(peers))
		if mr.count > 0 && min(mr.count, maxReplicationLevel) == level {
			result = append(result, cidEntry{CID: c, Shard: shardLabel(m.manifestShard[c]), Replicas: mr.count})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CID < result[j].CID })
	return result
}

func (m *Monitor) getReplicationByShard() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rs := m.newReplicationSnapshotLocked()
	filesAtTargetPerShard := make(map[string]int)
	for c, peers := range m.manifestReplication {
		if len(peers) == 0 {
			continue
		}
		shardCounts := rs.buildShardCounts(peers)
		mr := rs.resolveManifest(c, peers, shardCounts)
		if mr.count == 0 || mr.maxRep == 0 || !mr.isAtTarget() {
			continue
		}
		// A target shard emptied by a split is credited to the child holding
		// the most replicas.
		attributeShard := mr.targetShard
		if rs.shardPeerCount[attributeShard] == 0 {
			if best := shardWithMostReplicas(shardCounts, rs.shardPeerCount); best != "" {
				attributeShard = best
			}
		}
		filesAtTargetPerShard[attributeShard]++
	}
	return filesAtTargetPerShard
}
