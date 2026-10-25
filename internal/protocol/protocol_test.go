package protocol

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestParseTextHeartbeatWithRoleAndName(t *testing.T) {
	m, ok := ParseText([]byte("HEARTBEAT:12D3KooWabc:42:PASSIVE:my-node"))
	if !ok || m.Kind != TextHeartbeat {
		t.Fatalf("parse failed: %+v ok=%v", m, ok)
	}
	if m.Peer != "12D3KooWabc" || m.Pinned != 42 || m.Role != RolePassive || m.Name != "my-node" {
		t.Fatalf("fields wrong: %+v", m)
	}
}

func TestParseTextHeartbeatBackwardCompatible(t *testing.T) {
	// Older nodes omit trailing fields.
	m, ok := ParseText([]byte("HEARTBEAT:peerX:7"))
	if !ok || m.Pinned != 7 || m.Role != RoleActive || m.Name != "" {
		t.Fatalf("short heartbeat: %+v ok=%v", m, ok)
	}
	// Unparseable pinned count means unknown (-1).
	m, _ = ParseText([]byte("HEARTBEAT:peerX:notanumber:ACTIVE:n"))
	if m.Pinned != -1 {
		t.Fatalf("pinned should be -1, got %d", m.Pinned)
	}
}

func TestParseTextJoinLeavePinnedProbeSplit(t *testing.T) {
	m, ok := ParseText([]byte("JOIN:peerY:REPLICATOR:name:with:colons"))
	if !ok || m.Kind != TextJoin || m.Role != RoleReplicator || m.Name != "name:with:colons" {
		t.Fatalf("join: %+v", m)
	}
	m, ok = ParseText([]byte("LEAVE:peerZ"))
	if !ok || m.Kind != TextLeave || m.Peer != "peerZ" {
		t.Fatalf("leave: %+v", m)
	}
	m, ok = ParseText([]byte("PINNED:bafyabc"))
	if !ok || m.Kind != TextPinned || m.CID != "bafyabc" {
		t.Fatalf("pinned: %+v", m)
	}
	m, ok = ParseText([]byte("PROBE:peerW"))
	if !ok || m.Kind != TextProbe || m.Peer != "peerW" {
		t.Fatalf("probe: %+v", m)
	}
	m, ok = ParseText([]byte("SPLIT:00:01"))
	if !ok || m.Kind != TextSplit || m.Child0 != "00" || m.Child1 != "01" {
		t.Fatalf("split: %+v", m)
	}
}

func TestParseTextNonText(t *testing.T) {
	if _, ok := ParseText([]byte{0xA5, 0x01, 0x02}); ok {
		t.Fatal("CBOR bytes must not parse as text")
	}
	if m, ok := ParseText([]byte(`{"legacy":"json"}`)); !ok || m.Kind != "" {
		t.Fatal("legacy JSON must be consumed and ignored")
	}
	if _, ok := ParseText(nil); ok {
		t.Fatal("empty payload must not parse")
	}
}

func TestFormatParseRoundtrip(t *testing.T) {
	p := peer.ID("test-peer")
	line := FormatHeartbeat(p, 3, RoleReplicator, "n1")
	m, ok := ParseText([]byte(line))
	if !ok || m.Peer != p.String() || m.Pinned != 3 || m.Role != RoleReplicator || m.Name != "n1" {
		t.Fatalf("roundtrip: %+v", m)
	}
}

func TestParseRole(t *testing.T) {
	cases := map[string]Role{
		"ACTIVE": RoleActive, "passive": RolePassive, "Probe": RoleProbe,
		"REPLICATOR": RoleReplicator, "garbage": RoleActive, "": RoleActive,
	}
	for in, want := range cases {
		if got := ParseRole(in); got != want {
			t.Fatalf("ParseRole(%q) = %v, want %v", in, got, want)
		}
	}
	if !RoleActive.Counted() || !RoleReplicator.Counted() {
		t.Fatal("active/replicator must count")
	}
	if RolePassive.Counted() || RoleProbe.Counted() {
		t.Fatal("passive/probe must not count")
	}
}

func TestNonceStoreReplay(t *testing.T) {
	s := newNonceStore(50 * time.Millisecond)
	p := peer.ID("p1")
	n := []byte{1, 2, 3}
	if s.seenBefore(p, n) {
		t.Fatal("first sighting must not be a replay")
	}
	if !s.seenBefore(p, n) {
		t.Fatal("second sighting must be a replay")
	}
	if s.seenBefore(peer.ID("p2"), n) {
		t.Fatal("same nonce from another peer is not a replay")
	}
	time.Sleep(60 * time.Millisecond)
	if s.seenBefore(p, n) {
		t.Fatal("expired nonce must not be a replay")
	}
}

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(100*time.Millisecond, 3)
	p := peer.ID("p")
	for i := 0; i < 3; i++ {
		if !rl.Allow(p) {
			t.Fatalf("message %d within budget was rejected", i)
		}
	}
	if rl.Allow(p) {
		t.Fatal("over-budget message was allowed")
	}
	if !rl.Allow(peer.ID("other")) {
		t.Fatal("limits must be per peer")
	}
	time.Sleep(110 * time.Millisecond)
	if !rl.Allow(p) {
		t.Fatal("budget must refresh after the window")
	}
}
