package monitor

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/protocol"
)

// Fabricated but distinct peer IDs: handlers only compare them.
func pid(n int) peer.ID { return peer.ID(fmt.Sprintf("test-peer-%03d", n)) }

func newTestState() *State { return NewState(peer.ID("monitor-self"), time.Hour) }

func nodeOf(s *State, p peer.ID) *NodeState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[p]
}

func TestShardMoveDescendantAccepted(t *testing.T) {
	s := newTestState()
	p := pid(1)
	s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
	s.HandleHeartbeat("01", p, 1, protocol.RoleActive, "", "")
	n := nodeOf(s, p)
	if n.CurrentShard != "01" {
		t.Fatalf("descendant move refused: %q", n.CurrentShard)
	}
	if len(s.SplitEvents()) == 0 {
		t.Fatal("a descendant move must record a split event")
	}
}

func TestShardMoveStaleRejected(t *testing.T) {
	s := newTestState()
	p := pid(2)
	s.HandleHeartbeat("01", p, 1, protocol.RoleActive, "", "")
	// Move to the parent: stale gossip, ignored.
	s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
	if got := nodeOf(s, p).CurrentShard; got != "01" {
		t.Fatalf("parent move must be ignored, got %q", got)
	}
	// Cross-branch move: ignored.
	s.HandleHeartbeat("10", p, 1, protocol.RoleActive, "", "")
	if got := nodeOf(s, p).CurrentShard; got != "01" {
		t.Fatalf("cross-branch move must be ignored, got %q", got)
	}
}

func TestShardMoveSiblingCooldown(t *testing.T) {
	s := newTestState()
	p := pid(3)
	s.HandleHeartbeat("00", p, 1, protocol.RoleActive, "", "")
	s.HandleHeartbeat("01", p, 1, protocol.RoleActive, "", "")
	if got := nodeOf(s, p).CurrentShard; got != "01" {
		t.Fatalf("first sibling move refused: %q", got)
	}
	// An immediate sibling flap is suppressed by the cooldown.
	s.HandleHeartbeat("00", p, 1, protocol.RoleActive, "", "")
	if got := nodeOf(s, p).CurrentShard; got != "01" {
		t.Fatalf("sibling move within cooldown must be ignored, got %q", got)
	}
}

func TestProbeNodesNeverShown(t *testing.T) {
	s := newTestState()
	s.HandleHeartbeat("", pid(4), 1, protocol.RoleProbe, "", "")
	s.HandleHeartbeat("", pid(5), 1, protocol.RoleActive, "", "")
	if s.NodeCount() != 1 {
		t.Fatalf("probe nodes must not count, got %d", s.NodeCount())
	}
	// The monitor's own messages are ignored entirely.
	s.HandleHeartbeat("", s.self, 1, protocol.RoleActive, "", "")
	if s.NodeCount() != 1 {
		t.Fatal("the monitor itself must never be tracked")
	}
}

func TestReplicationRawCounts(t *testing.T) {
	s := newTestState()
	manifest := "bafytestmanifest1"
	// Six active nodes announcing the manifest in shard "0".
	for i := 0; i < 6; i++ {
		p := pid(10 + i)
		s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
		s.HandlePinned("0", p, manifest)
	}
	v := s.replicaViews()[manifest]
	if v.count != 6 || v.shard != "0" || v.target != 6 {
		t.Fatalf("view: %+v (want 6/6 in \"0\")", v)
	}
	if !v.atTarget() {
		t.Fatal("6 copies against a 6-node shard must be at target")
	}
	stats := s.ReplicationStats()
	if stats.FilesAtTarget != 1 || stats.Distribution[6] != 1 || stats.PerShard["0"] != 1 {
		t.Fatalf("stats: %+v", stats)
	}
	cids := s.CIDsAtLevel(6)
	if len(cids) != 1 || cids[0].CID != manifest || cids[0].Replicas != 6 {
		t.Fatalf("CIDsAtLevel: %+v", cids)
	}
}

func TestReplicationEmptyShardFallsBackToNetworkSize(t *testing.T) {
	s := newTestState()
	manifest := "bafytestmanifest2"
	// Three nodes, all in shard "0"; the manifest was observed in shard "1",
	// which has no displayable nodes -> target falls back to min(6, total).
	for i := 0; i < 3; i++ {
		p := pid(30 + i)
		s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
		s.HandlePinned("0", p, manifest)
	}
	s.mu.Lock()
	s.manifestShard[manifest] = "1"
	s.mu.Unlock()

	v := s.replicaViews()[manifest]
	if v.shard != "1" || v.count != 3 || v.target != 3 {
		t.Fatalf("view: %+v (want 3 replicas, target 3 via network size)", v)
	}
	if !v.atTarget() {
		t.Fatal("3 copies against a 3-node network must be at target")
	}
}

func TestReplicationBelowTarget(t *testing.T) {
	s := newTestState()
	manifest := "bafytestmanifest3"
	// Six nodes in the shard but only two hold the manifest.
	for i := 0; i < 6; i++ {
		p := pid(50 + i)
		s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
		if i < 2 {
			s.HandlePinned("0", p, manifest)
		}
	}
	v := s.replicaViews()[manifest]
	if v.count != 2 || v.target != 6 || v.atTarget() {
		t.Fatalf("view: %+v (want 2/6, below target)", v)
	}
	stats := s.ReplicationStats()
	if stats.FilesAtTarget != 0 || stats.Distribution[2] != 1 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestReplicationBucketCapAndProbeExclusion(t *testing.T) {
	s := newTestState()
	manifest := "bafytestmanifest4"
	// Twelve replicas land in the 10+ bucket; a PROBE's announcement and a
	// stale set entry never count.
	for i := 0; i < 12; i++ {
		p := pid(70 + i)
		s.HandleHeartbeat("0", p, 1, protocol.RoleActive, "", "")
		s.HandlePinned("0", p, manifest)
	}
	// A replica entry from a node that then turns out to be a PROBE observer
	// must not count (PINNED itself marks senders active, so flip the role
	// afterwards).
	probe := pid(90)
	s.HandlePinned("0", probe, manifest)
	s.HandleHeartbeat("0", probe, 1, protocol.RoleProbe, "", "")
	stale := pid(91)
	s.HandleHeartbeat("0", stale, 1, protocol.RoleActive, "", "")
	s.HandlePinned("0", stale, manifest)
	s.mu.Lock()
	s.replicas[manifest][stale] = time.Now().Add(-replicaTTL - time.Minute)
	s.mu.Unlock()

	v := s.replicaViews()[manifest]
	if v.count != 12 {
		t.Fatalf("count = %d, want 12 (probe and stale entries excluded)", v.count)
	}
	stats := s.ReplicationStats()
	if stats.Distribution[maxReplicationLevel] != 1 {
		t.Fatalf("12 replicas must land in the 10+ bucket: %+v", stats.Distribution)
	}
}
