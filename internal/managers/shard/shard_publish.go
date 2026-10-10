package shard

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
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
	msg := []byte(msgPrefixPinned + manifestCID)
	_ = sub.topic.Publish(sm.ctx, msg)
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
		topicName := sm.shardTopicName(shardID)
		_ = sm.ps.Publish(topicName, data)
	}
}

// PublishIngestMessageToCurrentAndChildIfSplit publishes to current shard and to target child if split in progress.
func (sm *ShardManager) PublishIngestMessageToCurrentAndChildIfSplit(data []byte, currentShard string, payloadCIDStr string) {
	sm.PublishToShardCBOR(data, currentShard)

	child0, child1 := childShards(currentShard)
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
	childShard, err := common.TargetShardForPayload(payloadCIDStr, depth)
	if err != nil {
		slog.Error("failed to compute target shard for child", "payload", payloadCIDStr, "error", err)
		return
	}
	if sm.JoinShardAsObserver(childShard) {
		sm.PublishToShardCBOR(data, childShard)
		sm.LeaveShardAsObserver(childShard)
		slog.Info("ingest message also published to child shard", "child", childShard)
	} else {
		slog.Warn("could not join child as observer to forward ingest message during split", "child", childShard)
	}
}

const (
	resolveProbeTimeout  = 3 * time.Second
	maxResolveExtraDepth = 8
)

// ResolveTargetShardForCustodial finds the shard that currently holds the
// payload's hash range. Shards split and merge independently, so the tree can
// be deeper or shallower along the payload's path than the ingesting node's
// own depth. The resolver descends while a deeper shard on the path has active
// peers; if none does and the nominal shard is empty, it ascends to the
// nearest ancestor on the path that has active peers.
func (sm *ShardManager) ResolveTargetShardForCustodial(nominalTargetShard string, payloadCIDStr string) string {
	shardAt := func(depth int) (string, bool) {
		if depth == 0 {
			return "", true
		}
		s, err := common.TargetShardForPayload(payloadCIDStr, depth)
		if err != nil {
			slog.Error("failed to compute custodial target shard", "payload", payloadCIDStr, "error", err)
			return "", false
		}
		return s, true
	}
	populated := func(s string) bool { return sm.probeShard(s, resolveProbeTimeout) > 0 }

	target := nominalTargetShard
	for depth := len(nominalTargetShard) + 1; depth <= len(nominalTargetShard)+maxResolveExtraDepth; depth++ {
		child, ok := shardAt(depth)
		if !ok || !populated(child) {
			break
		}
		target = child
	}
	if target != nominalTargetShard {
		slog.Info("custodial target resolved to deeper shard", "nominal", nominalTargetShard, "target", target)
		return target
	}
	if populated(nominalTargetShard) {
		return nominalTargetShard
	}
	for depth := len(nominalTargetShard) - 1; depth >= 0; depth-- {
		ancestor, ok := shardAt(depth)
		if ok && populated(ancestor) {
			slog.Info("custodial target resolved to ancestor shard", "nominal", nominalTargetShard, "target", ancestor)
			return ancestor
		}
	}
	return nominalTargetShard
}

func (sm *ShardManager) AmIResponsibleFor(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	prefix, err := common.GetHexBinaryPrefix(common.KeyToStableHex(key), len(sm.currentShard))
	if err != nil {
		slog.Error("failed to compute shard prefix", "key", key, "error", err)
		return false
	}
	return prefix == sm.currentShard
}

func (sm *ShardManager) PinToCluster(ctx context.Context, c cid.Cid) error {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return sm.clusterMgr.Pin(ctx, currentShard, c, -1, -1)
}

func (sm *ShardManager) EnsureClusterForShard(ctx context.Context, shardID string) error {
	return sm.clusterMgr.JoinShard(ctx, shardID)
}

func (sm *ShardManager) PinToShard(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.Pin(ctx, shardID, c, -1, -1)
}
