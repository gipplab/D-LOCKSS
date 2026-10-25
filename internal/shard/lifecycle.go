package shard

import (
	"log/slog"
	"time"

	"dlockss/internal/protocol"
)

// Lifecycle timings (frozen; the only operator knobs are the shard size
// bounds in the config).
const (
	shardPeerCheckInterval = 2 * time.Minute  // split-check gate off the root
	seenPeersWindow        = 350 * time.Second
	pruneStalePeersMaxAge  = 10 * time.Minute
	shardOverlapDuration   = 2 * time.Minute
	shardMoveCooldown      = 30 * time.Second
	mergeUpCooldown        = 2 * time.Minute
	probeTimeoutMerge      = 6 * time.Second
	shardDiscoveryInterval = 2 * time.Minute
	splitRebroadcastInterval = time.Minute
)

// minAcrossSiblings is the distinct-peer total a sibling pair needs before a
// descent is worthwhile: enough for two viable children with a little slack.
func (m *Manager) minAcrossSiblings() int {
	return 2*m.cfg.MinPeersPerShard - 2
}

// checkAndSplitIfNeeded evaluates the split conditions and, when they hold,
// announces SPLIT and moves this node into its target child. Only the
// deciding node moves; the rest of the shard follows through discovery.
func (m *Manager) checkAndSplitIfNeeded() {
	m.mu.Lock()
	current := m.current
	if time.Since(m.lastShardMove) < shardMoveCooldown {
		m.mu.Unlock()
		return
	}
	gate := shardPeerCheckInterval
	if current == "" {
		gate = rootPeerCheckInterval
	}
	if time.Since(m.lastSplitCheck) < gate {
		m.mu.Unlock()
		return
	}
	m.lastSplitCheck = time.Now()
	m.mu.Unlock()

	peers := m.tracker.activePeers(current, seenPeersWindow, m.self)
	count := len(peers) + 1 // self
	maxPeers := m.cfg.MaxPeersPerShard
	minPeers := m.cfg.MinPeersPerShard

	m.mu.Lock()
	if count < maxPeers {
		m.aboveMaxStreak = 0
		m.mu.Unlock()
		return
	}
	m.aboveMaxStreak++
	streak := m.aboveMaxStreak
	m.mu.Unlock()
	if streak < 2 {
		return // hysteresis: two consecutive checks above the limit
	}

	// Project the membership into the two children; refuse unbalanced splits.
	child0, child1 := Children(current)
	n0, n1 := 0, 0
	if BitPrefix(m.self.String(), len(current)+1) == child0 {
		n0++
	} else {
		n1++
	}
	for _, p := range peers {
		if BitPrefix(p.String(), len(current)+1) == child0 {
			n0++
		} else {
			n1++
		}
	}
	if n0 < minPeers || n1 < minPeers {
		slog.Debug("split refused: unbalanced membership", "shard", shardLabel(current), "child0", n0, "child1", n1)
		return
	}

	target := BitPrefix(m.self.String(), len(current)+1)
	sibling := child0
	if target == child0 {
		sibling = child1
	}
	// Join an existing child, or create one only when the shard clearly
	// overflows (enough members for two viable children plus slack).
	if m.probeShard(target, splitProbeTimeout) < 1 && count < 2*minPeers+2 {
		slog.Debug("split refused: target child empty and shard not large enough to create it",
			"shard", shardLabel(current), "count", count)
		return
	}

	slog.Info("splitting shard", "from", shardLabel(current), "to", target, "peers", count)
	m.publishText(current, protocol.FormatSplit(target, sibling))
	m.recordChildren(target, sibling)
	m.moveToShard(current, target, false)
}

// discoverAndMoveToDeeperShard follows previously announced SPLITs down the
// tree once enough distinct peers exist to keep both children viable.
func (m *Manager) discoverAndMoveToDeeperShard() {
	m.mu.Lock()
	current := m.current
	if time.Since(m.lastShardMove) < shardMoveCooldown ||
		time.Since(m.lastMergeUp) < mergeUpCooldown {
		m.mu.Unlock()
		return
	}
	target := BitPrefix(m.self.String(), len(current)+1)
	sibling := Sibling(target)
	_, targetKnown := m.knownChildren[target]
	_, siblingKnown := m.knownChildren[sibling]
	m.mu.Unlock()

	if !targetKnown && !siblingKnown {
		return // descend only into shards a SPLIT has announced
	}

	minPeers := m.cfg.MinPeersPerShard

	childCount := m.probeShard(target, discoveryProbeTimeout)
	siblingCount := m.probeShard(sibling, discoveryProbeTimeout)

	if childCount == 0 {
		// Complete a half-finished split: move only when the sibling is
		// viable and our own child would become viable with us and the
		// current-shard peers that hash into our half.
		if siblingCount < minPeers {
			return
		}
		projected := 1
		for _, p := range m.tracker.activePeers(current, seenPeersWindow, m.self) {
			if BitPrefix(p.String(), len(target)) == target {
				projected++
			}
		}
		if projected < minPeers {
			return
		}
		slog.Info("completing half-finished split", "to", target, "projected", projected)
		m.moveToShard(current, target, false)
		return
	}

	// Count each peer once across parent, child and sibling: peers that just
	// merged up can still look active in their old child.
	union := map[string]string{} // peer -> child-depth prefix
	add := func(shardID string) {
		for _, p := range m.tracker.activePeers(shardID, seenPeersWindow, m.self) {
			union[p.String()] = BitPrefix(p.String(), len(target))
		}
	}
	add(current)
	add(target)
	add(sibling)

	n0, n1 := 1, 0 // self goes to target's side
	if BitPrefix(m.self.String(), len(target)) != target {
		n0, n1 = 0, 1
	}
	for _, half := range union {
		if half == target {
			n0++
		} else {
			n1++
		}
	}
	if n0 < minPeers || n1 < minPeers {
		return
	}
	pairTotal := childCount + 1 + siblingCount
	parentCount := m.countActive(current)
	if pairTotal < m.minAcrossSiblings() &&
		pairTotal+parentCount-1 < m.minAcrossSiblings() {
		return
	}
	slog.Info("descending to child shard", "from", shardLabel(current), "to", target,
		"child_peers", childCount, "sibling_peers", siblingCount)
	m.moveToShard(current, target, false)
}

// checkAndMergeUpIfAlone merges under-populated shards back toward the root.
// The ladder is deliberately small: merge when this shard cannot hold enough
// copies, stay when the sibling pair is healthy or this is the larger (or
// only) side, and otherwise let the smaller side fold after two consecutive
// observations.
func (m *Manager) checkAndMergeUpIfAlone() {
	m.mu.Lock()
	current := m.current
	if current == "" ||
		time.Since(m.lastShardMove) < shardMoveCooldown ||
		time.Since(m.lastMoveDeeper) < mergeUpCooldown {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	minPeers := m.cfg.MinPeersPerShard
	parent := Parent(current)
	sibling := Sibling(current)

	currentCount := m.countActive(current)
	parentCount := m.probeShard(parent, probeTimeoutMerge)
	siblingCount := m.probeShard(sibling, probeTimeoutMerge)

	if parentCount >= m.cfg.MaxPeersPerShard {
		return // the parent is full; merging would immediately re-split
	}
	resetStreak := func() {
		m.mu.Lock()
		m.mergeStreak = 0
		m.mu.Unlock()
	}
	merge := func(reason string) {
		slog.Info("merging up", "from", current, "to", shardLabel(parent), "reason", reason,
			"current", currentCount, "parent", parentCount, "sibling", siblingCount)
		m.moveToShard(current, parent, true)
	}

	if currentCount < minPeers {
		merge("below minimum")
		return
	}
	if currentCount+siblingCount >= m.minAcrossSiblings() {
		resetStreak()
		return
	}
	if currentCount > siblingCount {
		resetStreak() // the larger (or only populated) side holds its ground
		return
	}
	m.mu.Lock()
	m.mergeStreak++
	streak := m.mergeStreak
	m.mu.Unlock()
	if streak >= 2 {
		merge("under-populated sibling pair")
	}
}
