package shard

import (
	"context"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"

	"github.com/libp2p/go-libp2p/core/peer"
)

const probeTimeoutForSplitChild = 6 * time.Second

// lifecycleOps is the narrow interface that lifecycleManager uses to query
// shard state and execute transitions.  ShardManager implements it.
type lifecycleOps interface {
	getCurrentShard() string
	getLastShardMove() time.Time
	getLastMergeUpTime() time.Time
	getLastMoveToDeeperShard() time.Time
	getLastMessageTime() time.Time
	localPeerID() peer.ID

	getShardPeerCount() int
	getShardPeerCountForSplit() int
	probeShard(shardID string, timeout time.Duration) int

	moveToShard(from, to string, isMergeUp bool)
	announceSplit(parentShard, targetChild string)
	rebroadcastSplitToAncestors()
	incrementShardSplits()

	pruneStaleSeenPeers()
}

// lifecycleManager encapsulates shard lifecycle decisions: split, merge-up,
// and discovery.  It owns the state needed for those decisions and delegates
// execution (topic operations, cluster joins, pin migration) back to the
// ShardManager through the lifecycleOps interface.
type lifecycleManager struct {
	ops lifecycleOps
	cfg *config.Config
	ctx func() context.Context // thunk to avoid storing stale ctx

	mu                       sync.Mutex
	splitAboveThresholdCount int
	knownChildShards         map[string]time.Time
	lastPeerCheck            time.Time
}

func newLifecycleManager(ctxFn func() context.Context, cfg *config.Config, ops lifecycleOps) *lifecycleManager {
	return &lifecycleManager{
		ops:              ops,
		cfg:              cfg,
		ctx:              ctxFn,
		knownChildShards: make(map[string]time.Time),
	}
}

// onShardTransition resets lifecycle state after any shard move.
func (lm *lifecycleManager) onShardTransition() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.knownChildShards = make(map[string]time.Time)
	lm.splitAboveThresholdCount = 0
	lm.lastPeerCheck = time.Now()
}

// recordSplitAnnouncement records child shard IDs from a SPLIT:child0:child1 message.
func (lm *lifecycleManager) recordSplitAnnouncement(payload string) {
	parts := strings.SplitN(payload, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return
	}
	child0, child1 := parts[0], parts[1]
	now := time.Now()
	lm.mu.Lock()
	lm.knownChildShards[child0] = now
	lm.knownChildShards[child1] = now
	lm.mu.Unlock()
	slog.Info("received split announcement", "child0", child0, "child1", child1)
}

// hasKnownChildren reports whether children of the given shard have been
// announced via SPLIT (needed by discovery to decide whether to move deeper).
func (lm *lifecycleManager) hasKnownChildren(currentShard string) bool {
	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(lm.ops.localPeerID().String(), nextDepth)
	siblingShard := getSiblingShard(targetChild)
	lm.mu.Lock()
	_, targetKnown := lm.knownChildShards[targetChild]
	_, siblingKnown := lm.knownChildShards[siblingShard]
	lm.mu.Unlock()
	return targetKnown || siblingKnown
}

// --- Split ---

func (lm *lifecycleManager) checkAndSplitIfNeeded() {
	now := time.Now()
	currentShard := lm.ops.getCurrentShard()

	lastShardMove := lm.ops.getLastShardMove()
	if !lastShardMove.IsZero() && now.Sub(lastShardMove) < lm.cfg.ShardMoveCooldown {
		return
	}

	interval := lm.cfg.ShardPeerCheckInterval
	if currentShard == "" {
		interval = rootPeerCheckInterval
	}
	lm.mu.Lock()
	if now.Sub(lm.lastPeerCheck) < interval {
		lm.mu.Unlock()
		return
	}
	lm.lastPeerCheck = now
	lm.mu.Unlock()

	peerCount := lm.ops.getShardPeerCountForSplit()
	if peerCount < lm.cfg.MaxPeersPerShard {
		lm.mu.Lock()
		lm.splitAboveThresholdCount = 0
		lm.mu.Unlock()
		return
	}

	lm.mu.Lock()
	lm.splitAboveThresholdCount++
	count := lm.splitAboveThresholdCount
	lm.mu.Unlock()
	if count < 2 {
		slog.Debug("waiting for 2nd consecutive check before split", "shard", currentShard, "peers", peerCount, "max_peers", lm.cfg.MaxPeersPerShard)
		return
	}

	estimatedPerChild := peerCount / 2
	if estimatedPerChild < lm.cfg.MinPeersPerShard {
		slog.Debug("split would leave too few peers per child", "shard", currentShard, "peers", peerCount, "max_peers", lm.cfg.MaxPeersPerShard, "estimated_per_child", estimatedPerChild, "min_peers", lm.cfg.MinPeersPerShard)
		return
	}

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(lm.ops.localPeerID().String(), nextDepth)
	childPeerCount := lm.ops.probeShard(targetChild, probeTimeoutForSplitChild)

	canJoinExisting := childPeerCount >= 1
	minParentToCreate := 2 * lm.cfg.MinPeersPerShard
	minParentToCreateNew := minParentToCreate + 2
	canCreateChild := childPeerCount == 0 && peerCount >= minParentToCreateNew
	if !canJoinExisting && !canCreateChild {
		slog.Debug("shard at limit but not splitting", "shard", currentShard, "peers", peerCount, "child", targetChild, "child_peers", childPeerCount, "min_parent_to_create", minParentToCreateNew)
		return
	}

	slog.Info("shard at limit, splitting", "shard", currentShard, "peers", peerCount, "child", targetChild, "child_peers", childPeerCount)
	lm.ops.incrementShardSplits()
	lm.ops.announceSplit(currentShard, targetChild)
	lm.ops.moveToShard(currentShard, targetChild, false)
}

// --- Merge ---

func (lm *lifecycleManager) checkAndMergeUpIfAlone() {
	currentShard := lm.ops.getCurrentShard()
	if currentShard == "" {
		return
	}

	lastAnyMove := lm.ops.getLastShardMove()
	if !lastAnyMove.IsZero() && time.Since(lastAnyMove) < lm.cfg.ShardMoveCooldown {
		return
	}

	lastMove := lm.ops.getLastMoveToDeeperShard()
	parentShard := currentShard[:len(currentShard)-1]
	if !lastMove.IsZero() && time.Since(lastMove) < lm.cfg.MergeUpCooldown {
		slog.Debug("merge-up skipped, moved to deeper shard recently", "cooldown_elapsed", time.Since(lastMove).Round(time.Second), "cooldown", lm.cfg.MergeUpCooldown)
		return
	}

	currentPeerCount := lm.ops.getShardPeerCount()
	parentPeerCount := lm.ops.probeShard(parentShard, lm.cfg.ProbeTimeoutMerge)
	if parentPeerCount >= lm.cfg.MaxPeersPerShard {
		return
	}

	siblingShard := getSiblingShard(currentShard)
	siblingPeerCount := lm.ops.probeShard(siblingShard, lm.cfg.ProbeTimeoutMerge)
	siblingsTotal := currentPeerCount + siblingPeerCount

	if siblingPeerCount == 0 {
		if lastMove.IsZero() || time.Since(lastMove) < lm.cfg.SiblingEmptyMergeAfter {
			slog.Debug("sibling empty, possible split in progress", "shard", currentShard, "peers", currentPeerCount, "sibling", siblingShard)
			return
		}
		if currentPeerCount >= lm.cfg.MinPeersPerShard {
			slog.Debug("sibling empty but we are healthy, not merging", "shard", currentShard, "peers", currentPeerCount, "min_peers", lm.cfg.MinPeersPerShard, "sibling", siblingShard)
			return
		}
		slog.Info("merging up, sibling empty too long", "shard", currentShard, "peers", currentPeerCount, "min_peers", lm.cfg.MinPeersPerShard, "sibling", siblingShard, "empty_after", lm.cfg.SiblingEmptyMergeAfter, "target", parentShard)
		lm.ops.moveToShard(currentShard, parentShard, true)
		return
	}

	if siblingsTotal >= lm.cfg.MinPeersAcrossSiblings {
		slog.Debug("siblings have enough peers, not merging", "shard", currentShard, "peers", currentPeerCount, "sibling", siblingShard, "sibling_peers", siblingPeerCount, "total", siblingsTotal, "min_across_siblings", lm.cfg.MinPeersAcrossSiblings)
		return
	}
	if siblingPeerCount > 0 && currentPeerCount > siblingPeerCount {
		slog.Debug("we are larger shard, not merging up", "shard", currentShard, "peers", currentPeerCount, "sibling", siblingShard, "sibling_peers", siblingPeerCount)
		return
	}

	slog.Info("siblings below threshold, merging up", "total", siblingsTotal, "min_across_siblings", lm.cfg.MinPeersAcrossSiblings, "shard", currentShard, "peers", currentPeerCount, "sibling", siblingShard, "sibling_peers", siblingPeerCount, "target", parentShard)

	lm.ops.moveToShard(currentShard, parentShard, true)
}

// --- Discovery ---

func (lm *lifecycleManager) discoverAndMoveToDeeperShard() {
	currentShard := lm.ops.getCurrentShard()

	lastAnyMove := lm.ops.getLastShardMove()
	if !lastAnyMove.IsZero() && time.Since(lastAnyMove) < lm.cfg.ShardMoveCooldown {
		return
	}

	lastMerge := lm.ops.getLastMergeUpTime()
	if !lastMerge.IsZero() && time.Since(lastMerge) < lm.cfg.MergeUpCooldown {
		slog.Debug("skipped discovery, merged recently", "shard", currentShard, "cooldown_elapsed", time.Since(lastMerge).Round(time.Second), "cooldown", lm.cfg.MergeUpCooldown)
		return
	}

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(lm.ops.localPeerID().String(), nextDepth)
	siblingShard := getSiblingShard(targetChild)

	lm.mu.Lock()
	_, targetKnown := lm.knownChildShards[targetChild]
	_, siblingKnown := lm.knownChildShards[siblingShard]
	lm.mu.Unlock()
	splitAnnounced := targetKnown || siblingKnown

	if !splitAnnounced {
		if currentShard != "" {
			slog.Debug("no split announcement, skipping discovery", "shard", currentShard, "child", targetChild, "sibling", siblingShard)
		}
		return
	}

	childPeerCount := lm.ops.probeShard(targetChild, probeTimeoutDiscovery)
	if childPeerCount < 1 {
		if currentShard != "" {
			slog.Debug("child has too few peers for discovery", "shard", currentShard, "child", targetChild, "child_peers", childPeerCount)
		}
		return
	}

	siblingPeerCount := lm.ops.probeShard(siblingShard, probeTimeoutDiscovery)
	ourChildAfter := childPeerCount + 1
	pairTotalAfter := ourChildAfter + siblingPeerCount
	if pairTotalAfter < lm.cfg.MinPeersAcrossSiblings {
		parentPeerCount := lm.ops.getShardPeerCount()
		projectedPairTotal := pairTotalAfter + (parentPeerCount - 1)
		if projectedPairTotal >= lm.cfg.MinPeersAcrossSiblings {
			slog.Info("pair total below threshold but projected allows join", "shard", currentShard, "pair_total", pairTotalAfter, "min_across_siblings", lm.cfg.MinPeersAcrossSiblings, "projected_total", projectedPairTotal, "parent_peers", parentPeerCount)
		} else {
			slog.Debug("pair total below threshold, not joining", "shard", currentShard, "child", targetChild, "child_after_join", ourChildAfter, "sibling", siblingShard, "sibling_peers", siblingPeerCount, "pair_total", pairTotalAfter, "projected_total", projectedPairTotal, "parent_peers", parentPeerCount, "min_across_siblings", lm.cfg.MinPeersAcrossSiblings)
			return
		}
	}

	slog.Info("discovered child shard, joining", "shard", currentShard, "child", targetChild, "child_peers", childPeerCount, "sibling", siblingShard, "sibling_peers", siblingPeerCount)
	lm.ops.moveToShard(currentShard, targetChild, false)
}

// --- Loops ---

func (lm *lifecycleManager) runPeerCountChecker() {
	ticker := time.NewTicker(rootPeerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lm.ctx().Done():
			return
		case <-ticker.C:
			lm.checkAndSplitIfNeeded()
			lm.ops.pruneStaleSeenPeers()
		}
	}
}

func (lm *lifecycleManager) runShardDiscovery() {
	for {
		currentShard := lm.ops.getCurrentShard()

		interval := lm.cfg.ShardDiscoveryInterval
		if currentShard == "" {
			interval = discoveryIntervalOnRoot
		} else if lm.hasKnownChildren(currentShard) {
			interval = discoveryIntervalWithChildren
		}

		jitter := time.Duration(rand.Int63n(int64(interval / 4)))
		t := time.NewTimer(interval + jitter)
		select {
		case <-lm.ctx().Done():
			t.Stop()
			return
		case <-t.C:
		}

		currentShard = lm.ops.getCurrentShard()
		isIdle := func() bool {
			lt := lm.ops.getLastMessageTime()
			return lt.IsZero() || time.Since(lt) > 1*time.Minute
		}()
		peerCount := lm.ops.getShardPeerCountForSplit()
		fewPeersInShard := peerCount <= lm.cfg.MaxPeersPerShard
		onRoot := currentShard == ""
		hasChildren := lm.hasKnownChildren(currentShard)
		if !hasChildren && !isIdle && !fewPeersInShard && !onRoot {
			continue
		}

		slog.Debug("running discovery then merge-up", "shard", currentShard, "peers", peerCount)
		lm.discoverAndMoveToDeeperShard()
		lm.checkAndMergeUpIfAlone()
	}
}

func (lm *lifecycleManager) runSplitRebroadcast() {
	jitterRange := lm.cfg.ShardSplitRebroadcastInterval / 2
	if jitterRange < time.Second {
		jitterRange = time.Second
	}
	for {
		delay := lm.cfg.ShardSplitRebroadcastInterval + time.Duration(rand.Int63n(int64(jitterRange)))
		t := time.NewTimer(delay)
		select {
		case <-lm.ctx().Done():
			t.Stop()
			return
		case <-t.C:
			lm.ops.rebroadcastSplitToAncestors()
		}
	}
}

// splitShard is a test helper that forces a split to the target child.
func (lm *lifecycleManager) splitShard() {
	currentShard := lm.ops.getCurrentShard()
	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(lm.ops.localPeerID().String(), nextDepth)
	lm.ops.incrementShardSplits()
	lm.ops.moveToShard(currentShard, targetChild, false)
}
