package shard

import (
	"log"
	"math/rand"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"
)

const (
	discoveryIntervalOnRoot       = 10 * time.Second
	probeTimeoutDiscovery         = 12 * time.Second // allow mesh formation before counting peers
	discoveryIntervalWithChildren = 45 * time.Second // when SPLIT known, retry more often
)

// discoverAndMoveToDeeperShard joins an existing child shard that matches this node's hash (children created only by split).
// Join only if (child + sibling) would be >= MinPeersAcrossSiblings. Use projected pair total only when SPLIT was announced.
// Never join without a SPLIT announcement: probe counts can be inflated by the monitor or other probers, leading to
// premature joins (e.g. shard 0 has 7 nodes, no split, but one node joins 00).
// Skip for a cooldown after merging up to avoid oscillation: merge to root → immediately re-join child → merge again.
func (sm *ShardManager) discoverAndMoveToDeeperShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	lastMerge := sm.lastMergeUpTime
	lastAnyMove := sm.lastShardMove
	sm.mu.RUnlock()

	// General move cooldown: don't re-descend right after any shard transition.
	if !lastAnyMove.IsZero() && time.Since(lastAnyMove) < config.ShardMoveCooldown {
		return
	}

	if !lastMerge.IsZero() && time.Since(lastMerge) < config.MergeUpCooldown {
		if config.VerboseLogging {
			label := currentShard
			if label == "" {
				label = "root"
			}
			log.Printf("[ShardDiscovery] Shard %s: skipped discovery (merged %v ago, cooldown %v)",
				label, time.Since(lastMerge).Round(time.Second), config.MergeUpCooldown)
		}
		return
	}

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	siblingShard := getSiblingShard(targetChild)

	sm.mu.RLock()
	_, targetKnown := sm.knownChildShards[targetChild]
	_, siblingKnown := sm.knownChildShards[siblingShard]
	sm.mu.RUnlock()
	splitAnnounced := targetKnown || siblingKnown

	if !splitAnnounced {
		if config.VerboseLogging && currentShard != "" {
			log.Printf("[ShardDiscovery] Shard %s: no SPLIT announcement for %s/%s, skipping discovery (avoid phantom-join)", currentShard, targetChild, siblingShard)
		}
		return
	}

	childPeerCount := sm.probeShard(targetChild, probeTimeoutDiscovery)

	if childPeerCount < 1 {
		if config.VerboseLogging && currentShard != "" {
			log.Printf("[ShardDiscovery] Shard %s: child %s has %d peers (need ≥1 to join), not moving", currentShard, targetChild, childPeerCount)
		}
		return
	}

	siblingPeerCount := sm.probeShard(siblingShard, probeTimeoutDiscovery)
	ourChildAfter := childPeerCount + 1
	pairTotalAfter := ourChildAfter + siblingPeerCount
	if pairTotalAfter < config.MinPeersAcrossSiblings {
		// splitAnnounced is true (we returned early otherwise). Project parent peers that will migrate.
		parentPeerCount := sm.getShardPeerCount()
		projectedPairTotal := pairTotalAfter + (parentPeerCount - 1) // -1 because we already counted ourselves
		if projectedPairTotal >= config.MinPeersAcrossSiblings {
			log.Printf("[ShardDiscovery] Shard %s: pair total %d < %d but projected %d (%d parent peers) >= threshold, allowing join",
				currentShard, pairTotalAfter, config.MinPeersAcrossSiblings, projectedPairTotal, parentPeerCount)
		} else {
			if config.VerboseLogging {
				log.Printf("[ShardDiscovery] Shard %s: child %s→%d + sibling %s→%d = %d (projected %d with %d parent peers) < %d (would merge), not joining",
					currentShard, targetChild, ourChildAfter, siblingShard, siblingPeerCount, pairTotalAfter, projectedPairTotal, parentPeerCount, config.MinPeersAcrossSiblings)
			}
			return
		}
	}

	log.Printf("[ShardDiscovery] Shard %s: discovered child %s with %d peers, sibling %s has %d → joining", currentShard, targetChild, childPeerCount, siblingShard, siblingPeerCount)
	sm.moveToShard(currentShard, targetChild, false)
}

// runShardDiscovery runs periodically: merge-up when below limit, then discover existing deeper shards to join.
func (sm *ShardManager) runShardDiscovery() {
	for {
		sm.mu.RLock()
		currentShard := sm.currentShard
		sm.mu.RUnlock()

		interval := config.ShardDiscoveryInterval
		if currentShard == "" {
			interval = discoveryIntervalOnRoot
		} else {
			// When we know children exist (SPLIT received), retry discovery more often so we migrate promptly
			nextDepth := len(currentShard) + 1
			targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
			siblingShard := getSiblingShard(targetChild)
			sm.mu.RLock()
			_, targetKnown := sm.knownChildShards[targetChild]
			_, siblingKnown := sm.knownChildShards[siblingShard]
			sm.mu.RUnlock()
			if targetKnown || siblingKnown {
				interval = discoveryIntervalWithChildren
			}
		}

		// Add 0-25% random jitter to desynchronize discovery timers across
		// nodes.  Without jitter, nodes that start at similar times fire
		// discovery simultaneously, leading to "thundering herd" probe storms
		// and synchronized merge/re-join cascades.
		jitter := time.Duration(rand.Int63n(int64(interval / 4)))
		t := time.NewTimer(interval + jitter)
		select {
		case <-sm.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}

		sm.mu.RLock()
		currentShard = sm.currentShard
		isIdle := sm.lastMessageTime.IsZero() || time.Since(sm.lastMessageTime) > 1*time.Minute
		sm.mu.RUnlock()

		peerCount := sm.getShardPeerCountForSplit() // mesh-only: stale seenPeers mustn't block discovery
		fewPeersInShard := peerCount <= config.MaxPeersPerShard
		onRoot := currentShard == ""
		// When we know children exist (SPLIT received), always run discovery so we migrate promptly
		nextDepth := len(currentShard) + 1
		targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
		siblingShard := getSiblingShard(targetChild)
		sm.mu.RLock()
		_, targetKnown := sm.knownChildShards[targetChild]
		_, siblingKnown := sm.knownChildShards[siblingShard]
		sm.mu.RUnlock()
		hasKnownChildren := targetKnown || siblingKnown
		if !hasKnownChildren && !isIdle && !fewPeersInShard && !onRoot {
			continue
		}

		if config.VerboseLogging {
			log.Printf("[ShardDiscovery] Shard %s (%d peers), discovery then merge-up...", currentShard, peerCount)
		}
		// Discovery first: join existing child if it has nodes. Then merge-up if below limit.
		sm.discoverAndMoveToDeeperShard()
		sm.checkAndMergeUpIfAlone()
	}
}
