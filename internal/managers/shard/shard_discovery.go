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
func (sm *ShardManager) discoverAndMoveToDeeperShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	childPeerCount := sm.probeShard(targetChild, probeTimeoutDiscovery)

	if childPeerCount < 1 {
		if config.VerboseLogging && currentShard != "" {
			log.Printf("[ShardDiscovery] Shard %s: child %s has %d peers (need ≥1 to join), not moving", currentShard, targetChild, childPeerCount)
		}
		return
	}

	siblingShard := getSiblingShard(targetChild)
	siblingPeerCount := sm.probeShard(siblingShard, probeTimeoutDiscovery)
	ourChildAfter := childPeerCount + 1
	pairTotalAfter := ourChildAfter + siblingPeerCount
	if pairTotalAfter < config.MinPeersAcrossSiblings {
		if config.VerboseLogging {
			log.Printf("[ShardDiscovery] Shard %s: child %s→%d + sibling %s→%d = %d < %d (would merge), not joining",
				currentShard, targetChild, ourChildAfter, siblingShard, siblingPeerCount, pairTotalAfter, config.MinPeersAcrossSiblings)
		}
		return
	}

	log.Printf("[ShardDiscovery] Shard %s: discovered child %s with %d peers, sibling %s has %d → joining", currentShard, targetChild, childPeerCount, siblingShard, siblingPeerCount)
	sm.moveToShard(currentShard, targetChild, false)
}

// runShardDiscovery runs periodically: merge-up when below limit, then discover existing deeper shards to join.
func (sm *ShardManager) runShardDiscovery() {
	ticker := time.NewTicker(discoveryIntervalOnRoot)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.Lock()
			now := time.Now()
			currentShard := sm.currentShard
			interval := config.ShardDiscoveryInterval
			if currentShard == "" {
				interval = discoveryIntervalOnRoot
			}
			if now.Sub(sm.lastDiscoveryCheck) < interval {
				sm.mu.Unlock()
				continue
			}
			sm.lastDiscoveryCheck = now
			isIdle := sm.lastMessageTime.IsZero() || now.Sub(sm.lastMessageTime) > 1*time.Minute
			sm.mu.Unlock()

			peerCount := sm.getShardPeerCount()
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
}
