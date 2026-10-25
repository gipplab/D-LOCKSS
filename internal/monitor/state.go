// Package monitor implements the read-only network observer: it subscribes
// to shard topics as a PROBE, tracks nodes, shards, splits and per-manifest
// replication from the gossip, and serves the dashboard and its API.
package monitor

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/protocol"
)

const (
	replicationTarget     = 6 // min copies considered healthy (MinPeersPerShard default)
	defaultNodeTimeout    = 350 * time.Second
	replicaTTL            = 350 * time.Second
	replicaSweepEvery     = time.Minute
	announcementTTL       = 30 * time.Minute
	announcementSweep     = 5 * time.Minute
	splitEventTTL         = 10 * time.Minute
	splitGracePeriod      = 5 * time.Minute
	siblingMoveCooldown   = 90 * time.Second
	unpinGracePeriod      = 30 * time.Second
	shardHistoryRecent    = 5 * time.Minute
	pruneTimerInterval    = time.Minute
)

// ShardVisit is one entry of a node's shard history.
type ShardVisit struct {
	Shard string
	At    time.Time
}

// NodeState is everything tracked about one network node.
type NodeState struct {
	PeerID       peer.ID
	CurrentShard string
	Role         protocol.Role
	PinnedFiles  int // -1 = unknown
	NodeName     string
	IP           string
	FirstSeen    time.Time
	LastSeen     time.Time
	History      []ShardVisit
}

// SplitEvent records an observed shard split.
type SplitEvent struct {
	Parent string
	Child  string
	At     time.Time
}

// State is the monitor's view of the network. All access goes through its
// mutex; handlers are called from the pubsub pumps.
type State struct {
	self        peer.ID
	nodeTimeout time.Duration

	mu sync.Mutex

	nodes             map[peer.ID]*NodeState
	peerShardLastSeen map[peer.ID]map[string]time.Time
	lastSiblingMove   map[peer.ID]time.Time

	splitEvents   []SplitEvent
	lastSplitTime time.Time

	announced   map[string]time.Time            // manifest CID -> last announcement
	nodeFiles   map[peer.ID]map[string]time.Time // peer -> manifest -> last seen
	replicas    map[string]map[peer.ID]time.Time // manifest -> replica set
	manifestShard map[string]string              // deepest observed shard per manifest

	lastReplicaSweep time.Time

	tree treeCache

	// SubscribeHint is invoked (outside the lock) when a message names a
	// shard worth subscribing to.
	SubscribeHint func(shardID string)
}

// NewState builds the tracker. self is the monitor's own peer (never shown).
func NewState(self peer.ID, nodeTimeout time.Duration) *State {
	if nodeTimeout <= 0 {
		nodeTimeout = defaultNodeTimeout
	}
	return &State{
		self:              self,
		nodeTimeout:       nodeTimeout,
		nodes:             map[peer.ID]*NodeState{},
		peerShardLastSeen: map[peer.ID]map[string]time.Time{},
		lastSiblingMove:   map[peer.ID]time.Time{},
		announced:         map[string]time.Time{},
		nodeFiles:         map[peer.ID]map[string]time.Time{},
		replicas:          map[string]map[peer.ID]time.Time{},
		manifestShard:     map[string]string{},
	}
}

func (s *State) hint(shard string) {
	if s.SubscribeHint != nil {
		s.SubscribeHint(shard)
	}
}

// displayable reports whether a node is shown and counted: not the monitor,
// not a PROBE.
func displayable(n *NodeState) bool {
	return n != nil && n.Role != protocol.RoleProbe
}

// HandleHeartbeat processes HEARTBEAT (and JOIN as pinned=-1).
func (s *State) HandleHeartbeat(shard string, p peer.ID, pinned int, role protocol.Role, name, ip string) {
	if p == s.self {
		return
	}
	now := time.Now()
	var moveHint string

	s.mu.Lock()
	s.touchShardLocked(p, shard, now)
	n := s.nodes[p]
	if n == nil {
		n = &NodeState{
			PeerID: p, CurrentShard: shard, Role: role, PinnedFiles: pinned,
			NodeName: name, IP: ip, FirstSeen: now, LastSeen: now,
			History: []ShardVisit{{Shard: shard, At: now}},
		}
		s.nodes[p] = n
	} else {
		n.LastSeen = now
		n.Role = role
		if name != "" {
			n.NodeName = name
		}
		if ip != "" {
			n.IP = ip
		}
		switch {
		case pinned == 0:
			n.PinnedFiles = 0
			if now.Sub(n.FirstSeen) > unpinGracePeriod {
				s.dropPeerReplicasLocked(p)
			}
		case pinned > 0:
			n.PinnedFiles = pinned
			for _, set := range s.replicas {
				if _, ok := set[p]; ok {
					set[p] = now
				}
			}
		}
		if n.CurrentShard == "" {
			n.CurrentShard = shard
			n.History = append(n.History, ShardVisit{Shard: shard, At: now})
		} else if n.CurrentShard != shard {
			moveHint = s.updateNodeShardLocked(n, shard, now)
		}
	}
	s.mu.Unlock()

	if moveHint != "" {
		s.hint(moveHint)
	}
}

// updateNodeShardLocked accepts only descendant moves (splits) and sibling
// moves (with a per-peer cooldown); anything else is stale gossip. It
// returns a shard to subscribe to, or "".
func (s *State) updateNodeShardLocked(n *NodeState, newShard string, now time.Time) string {
	old := n.CurrentShard
	isDescendant := len(newShard) > len(old) && strings.HasPrefix(newShard, old)
	isSibling := len(newShard) == len(old) && len(old) > 0 &&
		newShard[:len(old)-1] == old[:len(old)-1] && newShard != old

	switch {
	case isDescendant:
		s.splitEvents = append(s.splitEvents, SplitEvent{Parent: old, Child: newShard, At: now})
		s.lastSplitTime = now
	case isSibling:
		if now.Sub(s.lastSiblingMove[n.PeerID]) < siblingMoveCooldown {
			return ""
		}
		s.lastSiblingMove[n.PeerID] = now
	default:
		return "" // parent or cross-branch moves are stale
	}

	n.CurrentShard = newShard
	n.History = append(n.History, ShardVisit{Shard: newShard, At: now})

	// Drop the peer from manifests observed in unrelated branches.
	for m, set := range s.replicas {
		if _, ok := set[n.PeerID]; !ok {
			continue
		}
		ms := s.manifestShard[m]
		if ms == "" {
			continue
		}
		if !strings.HasPrefix(ms, newShard) && !strings.HasPrefix(newShard, ms) {
			delete(set, n.PeerID)
		}
	}
	return newShard
}

// HandleLeave processes LEAVE: the node goes shardless until its next JOIN.
func (s *State) HandleLeave(shard string, p peer.ID) {
	if p == s.self {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchShardLocked(p, shard, now)
	if n := s.nodes[p]; n != nil && n.CurrentShard == shard {
		n.CurrentShard = ""
		n.LastSeen = now
	}
}

// HandlePinned processes a PINNED announcement: a replica sighting.
func (s *State) HandlePinned(shard string, p peer.ID, manifestCID string) {
	s.HandleHeartbeat(shard, p, -1, protocol.RoleActive, "", "")
	if p == s.self {
		return
	}
	s.recordAnnouncement(shard, p, manifestCID, false)
}

// HandleIngest processes a decoded IngestMessage.
func (s *State) HandleIngest(shard string, author peer.ID, manifestCID, targetShard string) {
	if author == s.self {
		return
	}
	s.HandleHeartbeat(shard, author, -1, protocol.RoleActive, "", "")
	s.recordAnnouncement(targetShard, author, manifestCID, true)
	s.hint(targetShard)
}

// HandleSplit records both children of an announced split.
func (s *State) HandleSplit(parent, child0, child1 string) {
	now := time.Now()
	s.mu.Lock()
	for _, c := range []string{child0, child1} {
		if c != "" {
			s.splitEvents = append(s.splitEvents, SplitEvent{Parent: parent, Child: c, At: now})
		}
	}
	s.lastSplitTime = now
	s.mu.Unlock()
	s.hint(child0)
	s.hint(child1)
}

// recordAnnouncement tracks a manifest sighting from a peer in a shard.
func (s *State) recordAnnouncement(shard string, p peer.ID, manifestCID string, fromIngest bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.announced[manifestCID] = now
	nf := s.nodeFiles[p]
	if nf == nil {
		nf = map[string]time.Time{}
		s.nodeFiles[p] = nf
	}
	nf[manifestCID] = now
	set := s.replicas[manifestCID]
	if set == nil {
		set = map[peer.ID]time.Time{}
		s.replicas[manifestCID] = set
	}
	set[p] = now

	// Deepest observed shard wins.
	if cur, ok := s.manifestShard[manifestCID]; !ok ||
		(len(shard) > len(cur) && strings.HasPrefix(shard, cur)) {
		s.manifestShard[manifestCID] = shard
	}

	if n := s.nodes[p]; n != nil {
		if fromIngest && n.PinnedFiles < 1 {
			n.PinnedFiles = 1
		}
	}
}

func (s *State) touchShardLocked(p peer.ID, shard string, now time.Time) {
	m := s.peerShardLastSeen[p]
	if m == nil {
		m = map[string]time.Time{}
		s.peerShardLastSeen[p] = m
	}
	m[shard] = now
}

func (s *State) dropPeerReplicasLocked(p peer.ID) {
	for m, set := range s.replicas {
		delete(set, p)
		if len(set) == 0 {
			delete(s.replicas, m)
		}
	}
}

// effectiveShard is the node's shard for display: current, else the last
// history entry (LEAVE/JOIN gap).
func effectiveShard(n *NodeState) string {
	if n.CurrentShard != "" {
		return n.CurrentShard
	}
	if len(n.History) > 0 {
		return n.History[len(n.History)-1].Shard
	}
	return ""
}

// Nodes returns a snapshot of displayable nodes (pruning first).
func (s *State) Nodes() []*NodeState {
	s.Prune()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*NodeState, 0, len(s.nodes))
	for _, n := range s.nodes {
		if displayable(n) {
			c := *n
			c.History = append([]ShardVisit{}, n.History...)
			out = append(out, &c)
		}
	}
	return out
}

// UniqueCIDs returns all announced manifest CIDs.
func (s *State) UniqueCIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.announced))
	for c := range s.announced {
		out = append(out, c)
	}
	return out
}

// NodeFiles returns the manifests last announced by one peer.
func (s *State) NodeFiles(p peer.ID) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.nodeFiles[p]))
	for c := range s.nodeFiles[p] {
		out = append(out, c)
	}
	return out
}

// ReplicaCount returns the raw replica-set size of a manifest.
func (s *State) ReplicaCount(manifestCID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.replicas[manifestCID])
}

// ManifestShard returns the deepest observed shard of a manifest.
func (s *State) ManifestShard(manifestCID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifestShard[manifestCID]
}

// RecentShardHints lists shards worth following: every node's shard, shard
// history younger than 5m, and split parents/children.
func (s *State) RecentShardHints() []string {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]struct{}{}
	for _, n := range s.nodes {
		set[effectiveShard(n)] = struct{}{}
		for _, v := range n.History {
			if now.Sub(v.At) < shardHistoryRecent {
				set[v.Shard] = struct{}{}
			}
		}
	}
	for _, ev := range s.splitEvents {
		set[ev.Parent] = struct{}{}
		set[ev.Child] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for sh := range set {
		out = append(out, sh)
	}
	return out
}

// SplitEvents returns the live split events.
func (s *State) SplitEvents() []SplitEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SplitEvent{}, s.splitEvents...)
}

// NodeCount returns how many displayable nodes are tracked.
func (s *State) NodeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, node := range s.nodes {
		if displayable(node) {
			n++
		}
	}
	return n
}

// slogDiag logs a one-line replication snapshot (called by the sweeps).
func (s *State) slogDiag(atTarget, total int, avg float64) {
	switch {
	case total == 0:
		slog.Warn("replication snapshot: no manifests tracked")
	case atTarget == 0:
		slog.Warn("replication snapshot: zero files at target", "total_manifests", total, "avg_replication", avg)
	default:
		slog.Info("replication snapshot", "total_manifests", total, "total_at_target", atTarget, "avg_replication", avg)
	}
}
