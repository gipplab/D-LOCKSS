package monitor

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"dlockss/internal/common"
)

// maxReplicationLevel is the top bucket of the copies distribution; it also
// counts files with more copies.
const maxReplicationLevel = 10

// replicationSnapshot holds the node state that the copy counts of all
// files depend on. Build it with newReplicationSnapshotLocked.
type replicationSnapshot struct {
	shardPeerCount map[string]int
	// live maps each node that still counts as a holder to its shard: a node
	// the monitor shows, heard from in its shard within the announce TTL.
	live  map[string]string
	depth int
}

func (m *Monitor) newReplicationSnapshotLocked() replicationSnapshot {
	rs := replicationSnapshot{shardPeerCount: make(map[string]int), live: make(map[string]string)}
	cutoff := time.Now().Add(-replicationAnnounceTTL)
	for id, node := range m.nodes {
		if !m.isDisplayableNodeLocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		rs.shardPeerCount[shard]++
		rs.depth = max(rs.depth, len(shard))
		if seen := m.peerShardLastSeen[id]; seen == nil || !seen[shard].Before(cutoff) {
			rs.live[id] = shard
		}
	}
	return rs
}

// fileResult holds the resolved replication state of a file.
type fileResult struct {
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

// payloadShard returns the deepest shard with nodes on the payload's path,
// the shard whose nodes are responsible for it, as nodes decide it
// (AmIResponsibleFor).
func (rs *replicationSnapshot) payloadShard(payload string) (string, bool) {
	// KeyToStableHex always returns valid hex.
	bits, _ := common.GetHexBinaryPrefix(common.KeyToStableHex(payload), rs.depth)
	for d := len(bits); d >= 0; d-- {
		if rs.shardPeerCount[bits[:d]] > 0 {
			return bits[:d], true
		}
	}
	return "", false
}

// resolveFile computes the copies and the copy target of a file from the
// live holders per shard (shardCounts) and the number of all its holders.
// The target shard follows from the payload when it is known, otherwise from
// the shard the manifests were announced in (observed). Holders in other
// shards count only while a split moves nodes between parent and children.
func (rs *replicationSnapshot) resolveFile(payload, observed string, shardCounts map[string]int, holders int) fileResult {
	targetShard, fromPayload := "", false
	if payload != "" {
		targetShard, fromPayload = rs.payloadShard(payload)
	}
	if !fromPayload {
		targetShard = observed
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

	// Without a payload the observed shard is a guess: fall back to the
	// shard holding most copies.
	if count == 0 && !fromPayload {
		if best := shardWithMostReplicas(shardCounts, rs.shardPeerCount); best != "" {
			targetShard = best
			count = shardCounts[best]
			maxRep = rs.shardPeerCount[best]
		}
	}

	if maxRep == 0 {
		maxRep = holders
	}
	return fileResult{count: count, maxRep: maxRep, targetShard: targetShard}
}

func (m *Monitor) runReplicationCleanup() {
	ticker := time.NewTicker(replicationCleanupEvery)
	defer ticker.Stop()
	for range ticker.C {
		m.PruneStaleNodes()
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
	s := m.snapshot()
	nodesPerShard := make(map[string][]string)
	behind := 0
	for _, n := range s.nodes {
		if n.Status == nodeOffline {
			continue
		}
		nodesPerShard[n.Shard] = append(nodesPerShard[n.Shard], peerLabel(n.ID, n.Name))
		if n.Status == nodeBehind {
			behind++
		}
	}
	var b strings.Builder
	for _, id := range slices.Sorted(maps.Keys(nodesPerShard)) {
		fmt.Fprintf(&b, " %s: %d nodes [%s];", shardLabel(id), len(nodesPerShard[id]), strings.Join(nodesPerShard[id], ","))
	}
	slog.Info("replication snapshot", "files", s.health.Total, "at_target", s.health.AtTarget, "below", s.health.Below,
		"critical", s.health.Critical, "target", s.target, "nodes_behind", behind, "shards", strings.TrimSpace(b.String()))
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
