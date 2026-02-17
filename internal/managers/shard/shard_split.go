package shard

import (
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"dlockss/internal/common"
	"dlockss/internal/config"
)

func calculateXORDistance(peerID peer.ID, contentCID cid.Cid) (*big.Int, error) {
	peerIDBytes := []byte(peerID)
	var peerHash []byte
	peerMh, err := multihash.Decode(peerIDBytes)
	if err != nil {
		peerHash = peerIDBytes
	} else {
		peerHash = peerMh.Digest
	}

	cidMh := contentCID.Hash()
	cidMhDecoded, err := multihash.Decode(cidMh)
	if err != nil {
		return nil, fmt.Errorf("decode CID multihash: %w", err)
	}
	contentHash := cidMhDecoded.Digest

	maxLen := len(peerHash)
	if len(contentHash) > maxLen {
		maxLen = len(contentHash)
	}

	peerPadded := make([]byte, maxLen)
	copy(peerPadded[maxLen-len(peerHash):], peerHash)
	contentPadded := make([]byte, maxLen)
	copy(contentPadded[maxLen-len(contentHash):], contentHash)

	xorResult := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		xorResult[i] = peerPadded[i] ^ contentPadded[i]
	}
	return new(big.Int).SetBytes(xorResult), nil
}

const probeTimeoutForSplitChild = 6 * time.Second

// checkAndSplitIfNeeded splits when peer count >= MaxPeersPerShard; move to child if join existing or parent >= 2*MinPeersPerShard.
func (sm *ShardManager) checkAndSplitIfNeeded() {
	sm.mu.Lock()
	now := time.Now()
	currentShard := sm.currentShard

	// General move cooldown: don't evaluate splits right after any shard transition.
	// This prevents cascading splits during the overlap period when peer counts
	// are inflated by nodes that are still migrating.
	if !sm.lastShardMove.IsZero() && now.Sub(sm.lastShardMove) < config.ShardMoveCooldown {
		sm.mu.Unlock()
		return
	}

	interval := config.ShardPeerCheckInterval
	if currentShard == "" {
		interval = rootPeerCheckInterval
	}
	if now.Sub(sm.lastPeerCheck) < interval {
		sm.mu.Unlock()
		return
	}
	sm.lastPeerCheck = now
	sm.mu.Unlock()

	peerCount := sm.getShardPeerCountForSplit()
	if peerCount < config.MaxPeersPerShard {
		sm.mu.Lock()
		sm.splitAboveThresholdCount = 0
		sm.mu.Unlock()
		return
	}

	// Hysteresis: require 2 consecutive checks above threshold to avoid splitting on transient spikes
	sm.mu.Lock()
	sm.splitAboveThresholdCount++
	count := sm.splitAboveThresholdCount
	sm.mu.Unlock()
	if count < 2 {
		if config.VerboseLogging {
			log.Printf("[Sharding] Shard %s at %d peers (≥%d) but waiting for 2nd consecutive check before split", currentShard, peerCount, config.MaxPeersPerShard)
		}
		return
	}

	estimatedPerChild := peerCount / 2
	if estimatedPerChild < config.MinPeersPerShard {
		if config.VerboseLogging {
			log.Printf("[Sharding] Shard %s has %d peers (at max %d) but won't split: estimated per child (%d) < MinPeersPerShard (%d)",
				currentShard, peerCount, config.MaxPeersPerShard, estimatedPerChild, config.MinPeersPerShard)
		}
		return
	}

	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	childPeerCount := sm.probeShard(targetChild, probeTimeoutForSplitChild)

	canJoinExisting := childPeerCount >= 1
	minParentToCreate := 2 * config.MinPeersPerShard
	// When creating a new child (child empty), require +2 buffer so we have 14+ in parent.
	// First mover creates 13+1; more peers will join before merge timeout, reducing 1-node windows.
	minParentToCreateNew := minParentToCreate + 2
	canCreateChild := childPeerCount == 0 && peerCount >= minParentToCreateNew
	if !canJoinExisting && !canCreateChild {
		if config.VerboseLogging {
			log.Printf("[Sharding] Shard %s at limit (%d peers) but not splitting: child %s has %d (need ≥1 to join or parent ≥%d to create)",
				currentShard, peerCount, targetChild, childPeerCount, minParentToCreateNew)
		}
		return
	}

	log.Printf("[Sharding] Shard %s at limit (%d peers), child %s has %d → splitting", currentShard, peerCount, targetChild, childPeerCount)
	sm.metrics.IncrementShardSplits()
	sm.announceSplit(currentShard, targetChild)
	sm.moveToShard(currentShard, targetChild, false)
}

// announceSplit publishes SPLIT:child0:child1 on the parent topic.
func (sm *ShardManager) announceSplit(parentShard string, targetChild string) {
	sibling := getSiblingShard(targetChild)
	sm.mu.RLock()
	sub, exists := sm.shardSubs[parentShard]
	sm.mu.RUnlock()
	if !exists || sub.topic == nil {
		return
	}
	msg := []byte(fmt.Sprintf("SPLIT:%s:%s", targetChild, sibling))
	_ = sub.topic.Publish(sm.ctx, msg)
	log.Printf("[Sharding] Announced split on shard %s: children %s, %s", parentShard, targetChild, sibling)
}

// publishSplitToAncestor joins the ancestor shard as observer, publishes SPLIT:child0:child1, then leaves.
// This lets late-joining nodes in the ancestor discover existing children.
func (sm *ShardManager) publishSplitToAncestor(ancestorShard string) {
	child0 := ancestorShard + "0"
	child1 := ancestorShard + "1"
	if ancestorShard == "" {
		child0 = "0"
		child1 = "1"
	}
	if !sm.JoinShardAsObserver(ancestorShard) {
		return // already full member (we're in this shard) or failed to join
	}
	defer sm.LeaveShardAsObserver(ancestorShard)

	msg := []byte(fmt.Sprintf("SPLIT:%s:%s", child0, child1))
	sm.mu.RLock()
	sub, exists := sm.shardSubs[ancestorShard]
	sm.mu.RUnlock()
	if exists && sub.topic != nil {
		_ = sub.topic.Publish(sm.ctx, msg)
	} else {
		// Fallback when topic is nil (e.g. "topic already exists" path in JoinShardAsObserver)
		topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, ancestorShard)
		_ = sm.ps.Publish(topicName, msg)
	}
	log.Printf("[Sharding] Re-broadcast SPLIT to ancestor %q: children %s, %s", ancestorShard, child0, child1)
}

// rebroadcastSplitToAncestors publishes SPLIT to each ancestor shard (parent, grandparent, ..., root).
// Late joiners in any ancestor will receive the announcement and can discover existing children.
func (sm *ShardManager) rebroadcastSplitToAncestors() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if currentShard == "" {
		return // root has no ancestors
	}
	for ancestor := currentShard[:len(currentShard)-1]; ; {
		sm.publishSplitToAncestor(ancestor)
		if ancestor == "" {
			break
		}
		ancestor = ancestor[:len(ancestor)-1]
	}
}

// splitShard moves this node to its target child. For tests; normal path uses checkAndSplitIfNeeded.
func (sm *ShardManager) splitShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.metrics.IncrementShardSplits()
	sm.moveToShard(currentShard, targetChild, false)
}
