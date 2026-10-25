package monitor

import (
	"sort"
	"time"
)

const maxReplicationLevel = 10 // last distribution bucket means "10 or more"

// ReplicationStats summarizes the network's replication health.
type ReplicationStats struct {
	Distribution   [maxReplicationLevel + 1]int `json:"replication_distribution"`
	AvgReplication float64                      `json:"avg_replication_level"`
	FilesAtTarget  int                          `json:"files_at_target"`
	PerShard       map[string]int               `json:"files_at_target_per_shard"`
	TotalManifests int                          `json:"-"`
}

// replicaView is one manifest's raw placement: its fresh replica count, the
// shard it was observed in, and the copy target there.
type replicaView struct {
	shard  string
	count  int
	target int
}

func (v replicaView) atTarget() bool { return v.count > 0 && v.count >= v.target }

// replicaViews computes raw counts, with no placement heuristics: a
// manifest's replicas are its fresh (TTL-bounded) entries from displayable
// nodes, and its target is min(6, nodes in its observed shard), falling back
// to min(6, all displayable nodes) when that shard is empty. Mid-split the
// numbers may dip briefly and correct themselves as announcements arrive.
func (s *State) replicaViews() map[string]replicaView {
	cutoff := time.Now().Add(-replicaTTL)
	s.mu.Lock()
	defer s.mu.Unlock()

	shardNodes := map[string]int{}
	total := 0
	for _, n := range s.nodes {
		if displayable(n) {
			shardNodes[effectiveShard(n)]++
			total++
		}
	}

	views := make(map[string]replicaView, len(s.replicas))
	for m, set := range s.replicas {
		count := 0
		for p, seen := range set {
			if seen.Before(cutoff) || !displayable(s.nodes[p]) {
				continue
			}
			count++
		}
		sh := s.manifestShard[m]
		nodes := shardNodes[sh]
		if nodes == 0 {
			nodes = total
		}
		views[m] = replicaView{shard: sh, count: count, target: min(replicationTarget, nodes)}
	}
	return views
}

// ReplicationStats computes the distribution, average and at-target counts.
// Manifests with zero fresh replicas are skipped, so bucket 0 stays empty.
func (s *State) ReplicationStats() ReplicationStats {
	stats := ReplicationStats{PerShard: map[string]int{}}
	sum := 0
	for _, v := range s.replicaViews() {
		if v.count == 0 {
			continue
		}
		stats.TotalManifests++
		sum += v.count
		bucket := v.count
		if bucket > maxReplicationLevel {
			bucket = maxReplicationLevel
		}
		stats.Distribution[bucket]++
		if v.atTarget() {
			stats.FilesAtTarget++
			stats.PerShard[shardLabel(v.shard)]++
		}
	}
	if stats.TotalManifests > 0 {
		stats.AvgReplication = float64(sum) / float64(stats.TotalManifests)
	}
	return stats
}

// CIDAtLevel is one manifest with its observed placement.
type CIDAtLevel struct {
	CID      string `json:"cid"`
	Shard    string `json:"shard"`
	Replicas int    `json:"replicas"`
}

// CIDsAtLevel lists manifests whose replica count maps to the bucket.
func (s *State) CIDsAtLevel(level int) []CIDAtLevel {
	var out []CIDAtLevel
	for m, v := range s.replicaViews() {
		if v.count == 0 {
			continue
		}
		bucket := v.count
		if bucket > maxReplicationLevel {
			bucket = maxReplicationLevel
		}
		if bucket == level {
			out = append(out, CIDAtLevel{CID: m, Shard: shardLabel(v.shard), Replicas: v.count})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CID < out[j].CID })
	return out
}

// PinnedInShard counts manifests observed in the node's shard that the node
// replicates (0 when its sighting there is stale).
func (s *State) PinnedInShard(n *NodeState) int {
	sh := effectiveShard(n)
	cutoff := time.Now().Add(-replicaTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	if seen, ok := s.peerShardLastSeen[n.PeerID][sh]; !ok || seen.Before(cutoff) {
		return 0
	}
	count := 0
	for m, set := range s.replicas {
		if s.manifestShard[m] != sh {
			continue
		}
		if _, ok := set[n.PeerID]; ok {
			count++
		}
	}
	return count
}

func shardLabel(sh string) string {
	if sh == "" {
		return "ROOT"
	}
	return sh
}
