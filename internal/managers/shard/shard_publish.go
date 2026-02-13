package shard

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/internal/config"
)

func (sm *ShardManager) AnnouncePinned(manifestCID string) {
	if manifestCID == "" {
		return
	}
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()
	if !exists || sub.topic == nil {
		return
	}
	msg := []byte(fmt.Sprintf("PINNED:%s", manifestCID))
	_ = sub.topic.Publish(sm.ctx, msg)
}

func (sm *ShardManager) PublishToShard(shardID, msg string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	if sub.topic != nil {
		_ = sub.topic.Publish(sm.ctx, []byte(msg))
	} else {
		topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, shardID)
		_ = sm.ps.Publish(topicName, []byte(msg))
	}
}

func (sm *ShardManager) PublishToShardCBOR(data []byte, shardID string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	if sub.topic != nil {
		_ = sub.topic.Publish(sm.ctx, data)
	} else {
		topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, shardID)
		_ = sm.ps.Publish(topicName, data)
	}
}

// PublishIngestMessageToCurrentAndChildIfSplit publishes to current shard and to target child if split in progress.
func (sm *ShardManager) PublishIngestMessageToCurrentAndChildIfSplit(data []byte, currentShard string, payloadCIDStr string) {
	sm.PublishToShardCBOR(data, currentShard)

	child0 := currentShard + "0"
	child1 := currentShard + "1"
	if currentShard == "" {
		child0 = "0"
		child1 = "1"
	}
	const probeTimeout = 2 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)
	if n0 < 1 && n1 < 1 {
		return
	}
	depth := len(currentShard) + 1
	if depth < 1 {
		depth = 1
	}
	childShard := common.TargetShardForPayload(payloadCIDStr, depth)
	if sm.JoinShardAsObserver(childShard) {
		sm.PublishToShardCBOR(data, childShard)
		sm.LeaveShardAsObserver(childShard)
		log.Printf("[Shard] IngestMessage also published to child %s (split in progress)", childShard)
	}
}

// ResolveTargetShardForCustodial returns child shard if parent has active children, else nominalTargetShard.
func (sm *ShardManager) ResolveTargetShardForCustodial(nominalTargetShard string, payloadCIDStr string) string {
	child0 := nominalTargetShard + "0"
	child1 := nominalTargetShard + "1"
	if nominalTargetShard == "" {
		child0 = "0"
		child1 = "1"
	}
	const probeTimeout = 2 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)
	if n0 < 1 && n1 < 1 {
		return nominalTargetShard
	}
	depth := len(nominalTargetShard) + 1
	if depth < 1 {
		depth = 1
	}
	childShard := common.TargetShardForPayload(payloadCIDStr, depth)
	log.Printf("[Shard] Custodial target resolved: parent %s has active children → using child %s", nominalTargetShard, childShard)
	return childShard
}

func (sm *ShardManager) AmIResponsibleFor(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	prefix := common.GetHexBinaryPrefix(common.KeyToStableHex(key), len(sm.currentShard))
	return prefix == sm.currentShard
}

func (sm *ShardManager) PinToCluster(ctx context.Context, c cid.Cid) error {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return sm.clusterMgr.Pin(ctx, currentShard, c, -1, -1)
}

func (sm *ShardManager) EnsureClusterForShard(ctx context.Context, shardID string) error {
	return sm.clusterMgr.JoinShard(ctx, shardID, nil)
}

func (sm *ShardManager) PinToShard(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.Pin(ctx, shardID, c, -1, -1)
}
