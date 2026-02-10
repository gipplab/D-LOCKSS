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

// calculateXORDistance computes the XOR distance between a peer ID and a CID.
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

// probeTimeoutForSplitChild is used when probing the target child shard before splitting.
const probeTimeoutForSplitChild = 6 * time.Second

// checkAndSplitIfNeeded implements "split only at the limit" (see docs/SHARDING_ALGORITHM.md).
// Split when current shard peer count >= MaxPeersPerShard. New child shards are created only
// by splitting: we move to the child only if (1) the child already has >= 1 peer (join existing)
// or (2) the child is empty and the parent has >= 2*MinPeersPerShard (create child by splitting).
func (sm *ShardManager) checkAndSplitIfNeeded() {
	sm.mu.Lock()
	now := time.Now()
	currentShard := sm.currentShard
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

	// Use mesh count only for split: getShardPeerCount() can overcount (seenPeers includes
	// nodes that left the shard for up to 350s), which would split e.g. 11 with 3 nodes.
	peerCount := sm.getShardPeerCountForSplit()
	if peerCount < config.MaxPeersPerShard {
		return
	}

	// Only at the limit: consider splitting.
	estimatedPerChild := peerCount / 2
	if estimatedPerChild < config.MinPeersPerShard {
		if config.VerboseLogging {
			log.Printf("[Sharding] Shard %s has %d peers (at max %d) but won't split: estimated per child (%d) < MinPeersPerShard (%d)",
				currentShard, peerCount, config.MaxPeersPerShard, estimatedPerChild, config.MinPeersPerShard)
		}
		return
	}

	// Parent is at limit; after everyone splits, the two children will have total = peerCount >= MinPeersAcrossSiblings.
	// We do NOT require (child+1)+sibling >= MinPeersAcrossSiblings here: that would deadlock (first mover needs sibling to have 9, but sibling needs us to have 9).
	// Merge is prevented when sibling has 0 (split in progress) and by merge cooldown after we move.
	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	childPeerCount := sm.probeShard(targetChild, probeTimeoutForSplitChild)

	// Create new child only through splitting: either join existing branch or parent can spare.
	canJoinExisting := childPeerCount >= 1
	minParentToCreate := 2 * config.MinPeersPerShard
	canCreateChild := childPeerCount == 0 && peerCount >= minParentToCreate
	if !canJoinExisting && !canCreateChild {
		if config.VerboseLogging {
			log.Printf("[Sharding] Shard %s at limit (%d peers) but not splitting: child %s has %d (need ≥1 to join or parent ≥%d to create)",
				currentShard, peerCount, targetChild, childPeerCount, minParentToCreate)
		}
		return
	}

	log.Printf("[Sharding] Shard %s at limit (%d peers), child %s has %d → splitting", currentShard, peerCount, targetChild, childPeerCount)
	sm.metrics.IncrementShardSplits()
	sm.announceSplit(currentShard, targetChild)
	sm.moveToShard(currentShard, targetChild, false)
}

// announceSplit publishes a SPLIT announcement on the parent shard topic so that
// other nodes in the parent know child shards exist. Without this, discovery uses
// probeShardSilently which causes "phantom peer" false positives when multiple nodes
// simultaneously subscribe to a child topic to probe it.
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

// splitShard is used when the current shard is at the split limit; it moves this node to its target child.
// Callers must have already decided to split (checkAndSplitIfNeeded). Exposed for tests.
func (sm *ShardManager) splitShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	nextDepth := len(currentShard) + 1
	targetChild := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.metrics.IncrementShardSplits()
	sm.moveToShard(currentShard, targetChild, false)
}
