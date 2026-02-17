package shard

import (
	"log"
	"time"

	"dlockss/internal/config"
)

// mergeUpCooldown, probeTimeoutMerge, and siblingEmptyMergeAfter are now
// configurable via config.MergeUpCooldown, config.ProbeTimeoutMerge,
// and config.SiblingEmptyMergeAfter respectively.

// checkAndMergeUpIfAlone merges to parent when (my shard + sibling) < MinPeersAcrossSiblings.
func (sm *ShardManager) checkAndMergeUpIfAlone() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	lastMove := sm.lastMoveToDeeperShard
	lastAnyMove := sm.lastShardMove
	sm.mu.RUnlock()

	if currentShard == "" {
		return
	}

	// General move cooldown: don't evaluate merge right after any shard transition.
	if !lastAnyMove.IsZero() && time.Since(lastAnyMove) < config.ShardMoveCooldown {
		return
	}

	parentShard := currentShard[:len(currentShard)-1]
	if !lastMove.IsZero() && time.Since(lastMove) < config.MergeUpCooldown {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Skipping: moved to deeper shard %s ago (cooldown %v)", time.Since(lastMove).Round(time.Second), config.MergeUpCooldown)
		}
		return
	}

	currentPeerCount := sm.getShardPeerCount()
	parentPeerCount := sm.probeShard(parentShard, config.ProbeTimeoutMerge)
	if parentPeerCount >= config.MaxPeersPerShard {
		return
	}

	siblingShard := getSiblingShard(currentShard)
	siblingPeerCount := sm.probeShard(siblingShard, config.ProbeTimeoutMerge)
	siblingsTotal := currentPeerCount + siblingPeerCount

	if siblingPeerCount == 0 {
		// Sibling is empty.  This can mean a split is still in progress (nodes
		// haven't joined yet) OR that hash distribution is skewed and no peers
		// naturally belong to the sibling.
		//
		// Only merge up when BOTH conditions are true:
		//   1. The sibling has been empty long enough that it's not just slow
		//      mesh formation (SiblingEmptyMergeAfter).
		//   2. Our own shard is understaffed (below MinPeersPerShard).
		//
		// If we have plenty of peers ourselves (e.g. 14 nodes in shard 00,
		// sibling 01 empty), the empty sibling is just natural hash skew —
		// merging up would create pointless oscillation.
		if lastMove.IsZero() || time.Since(lastMove) < config.SiblingEmptyMergeAfter {
			if config.VerboseLogging {
				log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has 0 (split in progress?), not merging",
					currentShard, currentPeerCount, siblingShard)
			}
			return
		}
		if currentPeerCount >= config.MinPeersPerShard {
			if config.VerboseLogging {
				log.Printf("[ShardMergeUp] Shard %s has %d peers (>= %d), sibling %s has 0 but we're healthy, not merging",
					currentShard, currentPeerCount, config.MinPeersPerShard, siblingShard)
			}
			return
		}
		log.Printf("[ShardMergeUp] Shard %s has %d peers (< %d), sibling %s has 0 for >%v → merging to %s",
			currentShard, currentPeerCount, config.MinPeersPerShard, siblingShard, config.SiblingEmptyMergeAfter, parentShard)
		sm.moveToShard(currentShard, parentShard, true)
		return
	}

	if siblingsTotal >= config.MinPeersAcrossSiblings {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has %d (total %d >= %d), not merging",
				currentShard, currentPeerCount, siblingShard, siblingPeerCount, siblingsTotal, config.MinPeersAcrossSiblings)
		}
		return
	}
	if siblingPeerCount > 0 && currentPeerCount > siblingPeerCount {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has %d (we're larger), not merging up",
				currentShard, currentPeerCount, siblingShard, siblingPeerCount)
		}
		return
	}

	log.Printf("[ShardMergeUp] Siblings total %d < %d in %s (%d) + %s (%d) → merging to %s",
		siblingsTotal, config.MinPeersAcrossSiblings, currentShard, currentPeerCount, siblingShard, siblingPeerCount, parentShard)

	sm.moveToShard(currentShard, parentShard, true)
}
