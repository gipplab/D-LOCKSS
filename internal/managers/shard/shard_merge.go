package shard

import (
	"log"
	"time"

	"dlockss/internal/config"
)

const (
	// mergeUpCooldown: after moving deeper (split or discovery), wait this long before considering merge.
	// Must be long enough for other nodes in the parent to split (e.g. ShardPeerCheckInterval * (MaxPeersPerShard/2)).
	mergeUpCooldown   = 15 * time.Minute
	probeTimeoutMerge = 3 * time.Second
)

// checkAndMergeUpIfAlone implements "merge only at the limit" (see docs/SHARDING_ALGORITHM.md).
// When (my shard + sibling) < MinPeersAcrossSiblings, merge up to the parent. Applies at any
// depth including "0"/"1" → root (root is a normal parent).
func (sm *ShardManager) checkAndMergeUpIfAlone() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	lastMove := sm.lastMoveToDeeperShard
	sm.mu.RUnlock()

	// At root there is no parent to merge into.
	if currentShard == "" {
		return
	}

	parentShard := currentShard[:len(currentShard)-1]
	if !lastMove.IsZero() && time.Since(lastMove) < mergeUpCooldown {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Skipping: moved to deeper shard %s ago (cooldown %v)", time.Since(lastMove).Round(time.Second), mergeUpCooldown)
		}
		return
	}

	currentPeerCount := sm.getShardPeerCount()
	parentPeerCount := sm.probeShard(parentShard, probeTimeoutMerge)
	if parentPeerCount >= config.MaxPeersPerShard {
		return
	}

	lastBit := currentShard[len(currentShard)-1]
	siblingShard := parentShard + string([]byte{'0' + (1 - (lastBit - '0'))})
	siblingPeerCount := sm.probeShard(siblingShard, probeTimeoutMerge)
	siblingsTotal := currentPeerCount + siblingPeerCount

	// Never merge when sibling has 0 nodes: split is in progress; more nodes will move to the sibling.
	if siblingPeerCount == 0 {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has 0 (split in progress), not merging",
				currentShard, currentPeerCount, siblingShard)
		}
		return
	}

	// Merge only at the limit: when sibling pair is below threshold.
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
