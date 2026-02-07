package shard

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
)

// AnnouncePinned publishes PINNED:<manifestCID> to the current shard topic immediately.
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

// PublishToShard publishes a message to a specific shard topic.
func (sm *ShardManager) PublishToShard(shardID, msg string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		sub.topic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) PublishToShardCBOR(data []byte, shardID string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		sub.topic.Publish(sm.ctx, data)
	}
}

func (sm *ShardManager) AmIResponsibleFor(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	prefix := common.GetHexBinaryPrefix(common.KeyToStableHex(key), len(sm.currentShard))
	return prefix == sm.currentShard
}

// PinToCluster pins a CID to the current shard's cluster state.
func (sm *ShardManager) PinToCluster(ctx context.Context, c cid.Cid) error {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return sm.clusterMgr.Pin(ctx, currentShard, c, -1, -1)
}

// EnsureClusterForShard ensures the embedded cluster for the given shard exists.
func (sm *ShardManager) EnsureClusterForShard(ctx context.Context, shardID string) error {
	return sm.clusterMgr.JoinShard(ctx, shardID, nil)
}

// PinToShard pins a CID to a specific shard's cluster state.
func (sm *ShardManager) PinToShard(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.Pin(ctx, shardID, c, -1, -1)
}
