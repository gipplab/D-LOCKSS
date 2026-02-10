package shard

import (
	"log"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"
)

const (
	discoveryIntervalOnRoot = 10 * time.Second
	probeTimeoutDiscovery   = 6 * time.Second
)

// discoverAndMoveToDeeperShard discovers existing child shards that have nodes and joins the one
// that matches this node's hash. New child shards are never created here—only by splitting.
// We only join if after we move the sibling pair (child + sibling) would be >= MinPeersAcrossSiblings,
// so we never trigger an immediate merge (no faulty split / faulty shard creation).
//
// To prevent phantom peers (multiple nodes simultaneously probing a child topic see each other
// as real members), we only use the projected pair total (including parent peers) when children
// were announced via a SPLIT message. Without SPLIT knowledge, we require the strict pair total
// to meet the threshold, which prevents phantom-peer false positives.
func (sm *ShardManager) discoverAndMoveToDeeperShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	siblingShard := getSiblingShard(targetChild)

	// Check if children were announced via a SPLIT message (proof that a real split happened).
	// This determines whether we can use the optimistic projected pair total or must rely
	// on strict observed counts only.
	sm.mu.RLock()
	_, targetKnown := sm.knownChildShards[targetChild]
	_, siblingKnown := sm.knownChildShards[siblingShard]
	sm.mu.RUnlock()
	splitAnnounced := targetKnown || siblingKnown

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
		if splitAnnounced {
			// Children are known from SPLIT announcement — safe to project parent peers
			// that will also migrate, since we know the split is real (not phantom probers).
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
		} else {
			// No SPLIT announcement — children discovered via probing only.
			// Don't use projected pair total: probers create phantom peers that inflate
			// counts. Require strict observed pair total to prevent faulty shard creation.
			if config.VerboseLogging {
				log.Printf("[ShardDiscovery] Shard %s: child %s→%d + sibling %s→%d = %d < %d (no SPLIT announced, strict check), not joining",
					currentShard, targetChild, ourChildAfter, siblingShard, siblingPeerCount, pairTotalAfter, config.MinPeersAcrossSiblings)
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
		}

		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(interval):
		}

		sm.mu.RLock()
		currentShard = sm.currentShard
		isIdle := sm.lastMessageTime.IsZero() || time.Since(sm.lastMessageTime) > 1*time.Minute
		sm.mu.RUnlock()

		peerCount := sm.getShardPeerCountForSplit() // mesh-only: stale seenPeers mustn't block discovery
		fewPeersInShard := peerCount <= config.MaxPeersPerShard
		onRoot := currentShard == ""
		if !isIdle && !fewPeersInShard && !onRoot {
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
