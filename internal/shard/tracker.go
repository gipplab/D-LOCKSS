package shard

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/protocol"
)

// tracker records which peers were seen in which shards, with what role and
// when. Membership counts come exclusively from here (never from the pubsub
// mesh), so observers and monitors are never miscounted as members.
type tracker struct {
	mu    sync.Mutex
	seen  map[string]map[peer.ID]peerInfo // shardID -> peer -> info
}

type peerInfo struct {
	role     protocol.Role
	lastSeen time.Time
}

func newTracker() *tracker {
	return &tracker{seen: map[string]map[peer.ID]peerInfo{}}
}

// setRole records a peer with an explicit role (HEARTBEAT/JOIN/PROBE).
func (t *tracker) setRole(shardID string, p peer.ID, role protocol.Role) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.seen[shardID]
	if m == nil {
		m = map[peer.ID]peerInfo{}
		t.seen[shardID] = m
	}
	m[p] = peerInfo{role: role, lastSeen: time.Now()}
}

// touch refreshes a peer's last-seen without changing its role; unknown
// peers are recorded as ACTIVE.
func (t *tracker) touch(shardID string, p peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.seen[shardID]
	if m == nil {
		m = map[peer.ID]peerInfo{}
		t.seen[shardID] = m
	}
	info, ok := m[p]
	if !ok {
		info.role = protocol.RoleActive
	}
	info.lastSeen = time.Now()
	m[p] = info
}

// remove drops a peer from a shard (LEAVE).
func (t *tracker) remove(shardID string, p peer.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if m := t.seen[shardID]; m != nil {
		delete(m, p)
		if len(m) == 0 {
			delete(t.seen, shardID)
		}
	}
}

// activePeers returns the counted (ACTIVE/REPLICATOR) peers of a shard seen
// within the window, excluding self.
func (t *tracker) activePeers(shardID string, window time.Duration, self peer.ID) []peer.ID {
	cutoff := time.Now().Add(-window)
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []peer.ID
	for p, info := range t.seen[shardID] {
		if p == self || !info.role.Counted() || info.lastSeen.Before(cutoff) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// countActive counts a shard's active members. includeSelf adds one for this
// node (used when counting the node's own current shard).
func (t *tracker) countActive(shardID string, window time.Duration, self peer.ID, includeSelf bool) int {
	n := len(t.activePeers(shardID, window, self))
	if includeSelf {
		n++
	}
	return n
}

// prune drops entries older than maxAge.
func (t *tracker) prune(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	t.mu.Lock()
	defer t.mu.Unlock()
	for shardID, m := range t.seen {
		for p, info := range m {
			if info.lastSeen.Before(cutoff) {
				delete(m, p)
			}
		}
		if len(m) == 0 {
			delete(t.seen, shardID)
		}
	}
}
