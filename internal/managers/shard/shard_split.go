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
