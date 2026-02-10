package shard

import (
	"log"
	"time"

	"dlockss/internal/config"
)

const (
	mergeUpCooldown   = 15 * time.Minute
	probeTimeoutMerge = 3 * time.Second
)

// checkAndMergeUpIfAlone merges to parent when (my shard + sibling) < MinPeersAcrossSiblings.
func (sm *ShardManager) checkAndMergeUpIfAlone() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	lastMove := sm.lastMoveToDeeperShard
	sm.mu.RUnlock()

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

	siblingShard := getSiblingShard(currentShard)
	siblingPeerCount := sm.probeShard(siblingShard, probeTimeoutMerge)
	siblingsTotal := currentPeerCount + siblingPeerCount

	if siblingPeerCount == 0 {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has 0 (split in progress), not merging",
				currentShard, currentPeerCount, siblingShard)
		}
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
