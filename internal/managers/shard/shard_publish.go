package shard

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
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
	_ = sub.topic.Publish(sm.ctx, []byte(msgPrefixPinned+manifestCID))
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
		_ = sm.ps.Publish(sm.shardTopicName(shardID), data)
	}
}

// publishAsObserver subscribes to shardID as an observer for the duration of
// one publish. It returns false, without publishing, when the observer
// subscription cannot be made.
func (sm *ShardManager) publishAsObserver(data []byte, shardID string) bool {
	if !sm.JoinShardAsObserver(shardID) {
		return false
	}
	sm.PublishToShardCBOR(data, shardID)
	sm.LeaveShardAsObserver(shardID)
	return true
}

// publishIngest signs an IngestMessage for manifestCID and publishes it to shardID.
func (sm *ShardManager) publishIngest(manifestCID cid.Cid, shardID string) {
	im := schema.IngestMessage{
		SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: manifestCID},
		ShardID:        shardID,
	}
	if err := sm.signer.SignProtocolMessage(&im); err != nil {
		return
	}
	b, err := im.MarshalCBOR()
	if err != nil {
		return
	}
	sm.PublishToShardCBOR(b, shardID)
}

func (sm *ShardManager) signedReplicationRequest(manifestCID cid.Cid) ([]byte, error) {
	rr := &schema.ReplicationRequest{
		SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: manifestCID},
	}
	if err := sm.signer.SignProtocolMessage(rr); err != nil {
		return nil, err
	}
	return rr.MarshalCBOR()
}

func (sm *ShardManager) heartbeatMessage(pinnedCount int, role peerRole) []byte {
	return []byte(fmt.Sprintf("%s%s:%d:%s:%s", msgPrefixHeartbeat, sm.h.ID().String(), pinnedCount, role, sm.nodeName))
}

func (sm *ShardManager) announceMembership(topic *pubsub.Topic) {
	role := sm.getOurRole()
	_ = topic.Publish(sm.ctx, []byte(msgPrefixJoin+sm.h.ID().String()+":"+string(role)+":"+sm.nodeName))
	_ = topic.Publish(sm.ctx, sm.heartbeatMessage(sm.storageMgr.GetPinnedCount(), role))
}

// PublishIngestMessageToCurrentAndChildIfSplit publishes to the current shard
// and, while a split is in progress, to the child shard the payload belongs to.
func (sm *ShardManager) PublishIngestMessageToCurrentAndChildIfSplit(data []byte, currentShard string, payloadCIDStr string) {
	sm.PublishToShardCBOR(data, currentShard)

	child0, child1 := childShards(currentShard)
	const probeTimeout = 2 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)
	if n0 < 1 && n1 < 1 {
		return
	}
	childShard, err := common.TargetShardForPayload(payloadCIDStr, len(currentShard)+1)
	if err != nil {
		slog.Error("failed to compute target shard for child", "payload", payloadCIDStr, "error", err)
		return
	}
	if !sm.publishAsObserver(data, childShard) {
		slog.Warn("could not join child as observer to forward ingest message during split", "child", childShard)
		return
	}
	slog.Info("ingest message also published to child shard", "child", childShard)
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
	return sm.clusterMgr.Pin(ctx, sm.getCurrentShard(), c, -1, -1)
}

func (sm *ShardManager) EnsureClusterForShard(ctx context.Context, shardID string) error {
	return sm.clusterMgr.JoinShard(ctx, shardID)
}

func (sm *ShardManager) PinToShard(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.Pin(ctx, shardID, c, -1, -1)
}
