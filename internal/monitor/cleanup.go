package monitor

import (
	"context"
	"strings"
	"time"
)

// Prune drops stale nodes and their traces. It is called lazily by node API
// handlers and from a 1-minute timer. Pruning is suspended for a grace
// period after a split, when nodes legitimately go quiet while they migrate.
func (s *State) Prune() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.lastSplitTime) < splitGracePeriod {
		return
	}
	for p, n := range s.nodes {
		timeout := s.nodeTimeout
		if n.CurrentShard == "" {
			timeout *= 2 // between LEAVE and JOIN
		}
		if now.Sub(n.LastSeen) <= timeout {
			continue
		}
		delete(s.nodes, p)
		delete(s.nodeFiles, p)
		delete(s.peerShardLastSeen, p)
		delete(s.lastSiblingMove, p)
		for m, set := range s.replicas {
			delete(set, p)
			if len(set) == 0 {
				delete(s.replicas, m)
			}
		}
	}
	s.pruneSplitEventsLocked(now)
}

// pruneSplitEventsLocked drops expired split events, and events that no
// longer have a live node on both the parent and child paths.
func (s *State) pruneSplitEventsLocked(now time.Time) {
	kept := s.splitEvents[:0]
	for _, ev := range s.splitEvents {
		if now.Sub(ev.At) > splitEventTTL {
			continue
		}
		parentAlive, childAlive := false, false
		for _, n := range s.nodes {
			sh := effectiveShard(n)
			if strings.HasPrefix(sh, ev.Parent) {
				parentAlive = true
			}
			if strings.HasPrefix(sh, ev.Child) {
				childAlive = true
			}
			if parentAlive && childAlive {
				break
			}
		}
		if parentAlive && childAlive {
			kept = append(kept, ev)
		}
	}
	s.splitEvents = kept
}

// RunCleanup drives the periodic sweeps until ctx ends: replica expiry every
// minute, announcement pruning every five, plus the timer-driven node prune.
func (s *State) RunCleanup(ctx context.Context) {
	replicaT := time.NewTicker(replicaSweepEvery)
	announceT := time.NewTicker(announcementSweep)
	pruneT := time.NewTicker(pruneTimerInterval)
	defer replicaT.Stop()
	defer announceT.Stop()
	defer pruneT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-replicaT.C:
			s.sweepReplicas()
			stats := s.ReplicationStats()
			s.slogDiag(stats.FilesAtTarget, stats.TotalManifests, stats.AvgReplication)
		case <-announceT.C:
			s.sweepAnnouncements()
		case <-pruneT.C:
			s.Prune()
		}
	}
}

// sweepReplicas expires replica entries older than the TTL.
func (s *State) sweepReplicas() {
	cutoff := time.Now().Add(-replicaTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for m, set := range s.replicas {
		for p, seen := range set {
			if seen.Before(cutoff) {
				delete(set, p)
			}
		}
		if len(set) == 0 {
			delete(s.replicas, m)
		}
	}
	s.lastReplicaSweep = time.Now()
}

// sweepAnnouncements drops announcements not refreshed within the TTL.
func (s *State) sweepAnnouncements() {
	cutoff := time.Now().Add(-announcementTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for c, seen := range s.announced {
		if seen.Before(cutoff) {
			delete(s.announced, c)
			delete(s.manifestShard, c)
		}
	}
	for p, files := range s.nodeFiles {
		for c, seen := range files {
			if seen.Before(cutoff) {
				delete(files, c)
			}
		}
		if len(files) == 0 {
			delete(s.nodeFiles, p)
		}
	}
}
