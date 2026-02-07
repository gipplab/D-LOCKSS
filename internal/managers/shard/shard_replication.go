package shard

import (
	"context"
	"fmt"
	"log"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

// targetShardForManifest returns the shard (cluster) that should hold this file by CID.
func (sm *ShardManager) targetShardForManifest(ctx context.Context, manifestCIDStr, topicShardID string) string {
	payloadCIDStr := common.GetPayloadCIDForShardAssignment(ctx, sm.ipfsClient, manifestCIDStr)
	depth := len(topicShardID) + 1
	return common.TargetShardForPayload(payloadCIDStr, depth)
}

// handleReplicationRequest processes a ReplicationRequest (under-replicated file).
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("ReplicationRequest (Shard %s)", shardID)
	if sm.signer.VerifyAndAuthorizeMessage(msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped ReplicationRequest for %s from %s in shard %s due to verification failure", rr.ManifestCID.String(), msg.GetFrom().String(), shardID)
		return
	}
	manifestCIDStr := rr.ManifestCID.String()
	c := rr.ManifestCID

	if sm.storageMgr.IsPinned(manifestCIDStr) {
		targetShard := sm.targetShardForManifest(sm.ctx, manifestCIDStr, shardID)
		sm.JoinShard(targetShard)
		if err := sm.EnsureClusterForShard(sm.ctx, targetShard); err != nil {
			log.Printf("[Shard] ReplicationRequest: failed to ensure cluster for target shard %s: %v", targetShard, err)
			return
		}
		if err := sm.clusterMgr.Pin(sm.ctx, targetShard, c, config.MinReplication, config.MaxReplication); err != nil {
			log.Printf("[Shard] ReplicationRequest: failed to pin %s to target shard %s: %v", manifestCIDStr, targetShard, err)
			return
		}
		sm.clusterMgr.TriggerSync(targetShard)
		return
	}
	if !config.AutoReplicationEnabled {
		return
	}
	go func() {
		fetchCtx, cancelFetch := context.WithTimeout(sm.ctx, config.AutoReplicationTimeout)
		if err := sm.ipfsClient.PinRecursive(fetchCtx, c); err != nil {
			cancelFetch()
			log.Printf("[Shard] Auto-replication: failed to fetch/pin %s: %v", manifestCIDStr, err)
			return
		}
		cancelFetch()
		targetShard := sm.targetShardForManifest(sm.ctx, manifestCIDStr, shardID)
		sm.JoinShard(targetShard)
		pinCtx, cancelPin := context.WithTimeout(sm.ctx, config.CRDTOpTimeout)
		defer cancelPin()
		if err := sm.EnsureClusterForShard(pinCtx, targetShard); err != nil {
			log.Printf("[Shard] Auto-replication: failed to ensure cluster for target shard %s: %v", targetShard, err)
			return
		}
		if err := sm.clusterMgr.Pin(pinCtx, targetShard, c, config.MinReplication, config.MaxReplication); err != nil {
			log.Printf("[Shard] Auto-replication: failed to add %s to cluster %s: %v", manifestCIDStr, targetShard, err)
			return
		}
		log.Printf("[Shard] Auto-replication: fetched and pinned %s to target shard %s", manifestCIDStr, targetShard)
		sm.clusterMgr.TriggerSync(targetShard)
	}()
}

// handleIngestMessage processes an IngestMessage.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if sm.signer.VerifyAndAuthorizeMessage(msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped IngestMessage from %s in shard %s due to verification failure", msg.GetFrom().String(), shardID)
		return
	}
	key := im.ManifestCID.String()
	sm.metrics.IncrementMessagesReceived()

	sm.storageMgr.AddKnownFile(key)

	payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
	if sm.AmIResponsibleFor(payloadCIDStr) {
		if err := sm.clusterMgr.Pin(sm.ctx, shardID, im.ManifestCID, -1, -1); err != nil {
			log.Printf("[Shard] Failed to pin ingested file %s to cluster: %v", key, err)
		} else {
			log.Printf("[Shard] Automatically pinned ingested file %s to cluster %s", key, shardID)
			sm.AnnouncePinned(key)
		}
	}
}

// RunReshardPass re-evaluates responsibility.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.GetKnownFiles().All()
	if len(files) == 0 {
		return
	}

	log.Printf("[Reshard] Starting reshard pass: %s -> %s", oldShard, newShard)
	oldDepth := len(oldShard)
	newDepth := len(newShard)

	for key := range files {
		if sm.reshardedFiles.Has(key) {
			continue
		}

		payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetOld := common.GetHexBinaryPrefix(stableHex, oldDepth)
		targetNew := common.GetHexBinaryPrefix(stableHex, newDepth)

		wasResponsible := (targetOld == oldShard)
		isResponsible := (targetNew == newShard)

		if wasResponsible == isResponsible {
			sm.reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := common.KeyToCID(key)
		if err != nil {
			continue
		}

		if isResponsible && sm.storageMgr.IsPinned(key) {
			im := schema.IngestMessage{
				Type:        schema.MessageTypeIngest,
				ManifestCID: manifestCID,
				ShardID:     newShard,
				HintSize:    0,
			}
			if err := sm.signer.SignProtocolMessage(&im); err == nil {
				if b, err := im.MarshalCBOR(); err == nil {
					sm.PublishToShardCBOR(b, newShard)
				}
			}
		} else if wasResponsible {
			if sm.storageMgr.IsPinned(key) {
				log.Printf("[Reshard] Unpinning file that no longer belongs to shard %s: %s", newShard, key)
				if sm.clusterMgr != nil {
					if err := sm.clusterMgr.Unpin(sm.ctx, oldShard, manifestCID); err != nil {
						log.Printf("[Reshard] Warning: Failed to unpin from old shard cluster: %v", err)
					}
				}
				if sm.ipfsClient != nil {
					_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
				}
				sm.storageMgr.UnpinFile(key)
			}
		}

		sm.reshardedFiles.Add(key)
		time.Sleep(10 * time.Millisecond)
	}
}

const orphanUnpinInterval = 2 * time.Minute
const orphanUnpinGracePeriod = 3 * time.Minute

// RunOrphanUnpinPass unpins files that belong to active child shards when we are still in the parent.
func (sm *ShardManager) RunOrphanUnpinPass() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	files := sm.storageMgr.GetKnownFiles().All()
	if len(files) == 0 {
		return
	}

	child0 := currentShard + "0"
	child1 := currentShard + "1"
	if currentShard == "" {
		child0 = "0"
		child1 = "1"
	}
	probeTimeout := 4 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)
	if n0 < 1 && n1 < 1 {
		return
	}
	activeChildren := make(map[string]struct{})
	if n0 >= 1 {
		activeChildren[child0] = struct{}{}
	}
	if n1 >= 1 {
		activeChildren[child1] = struct{}{}
	}

	depth := len(currentShard) + 1
	if currentShard == "" {
		depth = 1
	}
	unpinned := 0
	for key := range files {
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < orphanUnpinGracePeriod {
			continue
		}
		payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetChild := common.GetHexBinaryPrefix(stableHex, depth)
		if _, active := activeChildren[targetChild]; !active {
			continue
		}
		manifestCID, err := common.KeyToCID(key)
		if err != nil {
			continue
		}
		log.Printf("[Reshard] Orphan unpin: file belongs to child shard %s (we are in %s): %s",
			targetChild, currentShard, key)
		if sm.clusterMgr != nil {
			_ = sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID)
		}
		if sm.ipfsClient != nil {
			_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
		}
		sm.storageMgr.UnpinFile(key)
		unpinned++
		time.Sleep(10 * time.Millisecond)
	}
	if unpinned > 0 {
		log.Printf("[Reshard] Orphan unpin pass: unpinned %d files that belong to child shards", unpinned)
	}
}

func (sm *ShardManager) runOrphanUnpinLoop() {
	ticker := time.NewTicker(orphanUnpinInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.RunOrphanUnpinPass()
		}
	}
}

func (sm *ShardManager) runReannouncePinsLoop() {
	if config.PinReannounceInterval <= 0 {
		return
	}
	ticker := time.NewTicker(config.PinReannounceInterval)
	defer ticker.Stop()
	const delayBetweenPins = 40 * time.Millisecond
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			manifests := sm.storageMgr.GetPinnedManifests()
			if len(manifests) == 0 {
				continue
			}
			announced := 0
			for _, manifestCIDStr := range manifests {
				payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, manifestCIDStr)
				if !sm.AmIResponsibleFor(payloadCIDStr) {
					continue
				}
				sm.AnnouncePinned(manifestCIDStr)
				announced++
				select {
				case <-sm.ctx.Done():
					return
				case <-time.After(delayBetweenPins):
				}
			}
			if config.VerboseLogging && announced > 0 {
				log.Printf("[Shard] Re-announced %d pins on current shard (interval %v)", announced, config.PinReannounceInterval)
			}
		}
	}
}
