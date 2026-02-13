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

// handleReplicationRequest verifies, then fetches and pins if not already pinned.
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("ReplicationRequest (Shard %s)", shardID)
	if sm.signer.ShouldDropMessage(msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] ReplicationRequest rejected %s from %s (shard %s)", rr.ManifestCID.String(), msg.GetFrom().String(), shardID)
		return
	}
	manifestCIDStr := rr.ManifestCID.String()
	c := rr.ManifestCID

	if sm.storageMgr.IsPinned(manifestCIDStr) {
		if err := sm.EnsureClusterForShard(sm.ctx, shardID); err != nil {
			log.Printf("[Shard] ReplicationRequest: cluster for shard %s: %v", shardID, err)
			return
		}
		sm.clusterMgr.TriggerSync(shardID)
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
		if err := sm.EnsureClusterForShard(sm.ctx, shardID); err != nil {
			log.Printf("[Shard] Auto-replication: cluster for shard %s: %v", shardID, err)
			return
		}
		if err := sm.clusterMgr.Pin(sm.ctx, shardID, c, 0, 0); err != nil {
			log.Printf("[Shard] Auto-replication: failed to write CRDT pin for %s: %v", manifestCIDStr, err)
		}
		log.Printf("[Shard] Auto-replication: fetched and pinned %s to shard %s", manifestCIDStr, shardID)
		sm.clusterMgr.TriggerSync(shardID)
	}()
}

// handleIngestMessage verifies IngestMessage and pins if on current shard and responsible.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if sm.signer.ShouldDropMessage(msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] IngestMessage rejected from %s (shard %s)", msg.GetFrom().String(), shardID)
		return
	}
	key := im.ManifestCID.String()
	sm.metrics.IncrementMessagesReceived()

	sm.storageMgr.AddKnownFile(key)

	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if shardID != currentShard {
		return
	}

	resolveCtx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	payloadCIDStr := common.GetPayloadCIDForShardAssignment(resolveCtx, sm.ipfsClient, key)
	cancel()
	if payloadCIDStr != key && !sm.AmIResponsibleFor(payloadCIDStr) {
		return
	}

	if err := sm.clusterMgr.Pin(sm.ctx, shardID, im.ManifestCID, -1, -1); err != nil {
		log.Printf("[Shard] Failed to pin ingested file %s to cluster: %v", key, err)
	} else {
		log.Printf("[Shard] Automatically pinned ingested file %s to cluster %s", key, shardID)
		sm.AnnouncePinned(key)
	}
}

// RunReshardPass migrates or unpins files when moving between shards.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.GetKnownFiles().All()
	if len(files) == 0 {
		return
	}

	log.Printf("[Reshard] Starting reshard pass: %s -> %s", oldShard, newShard)
	oldDepth := len(oldShard)
	newDepth := len(newShard)

	for key := range files {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

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
				if sm.signer != nil && targetNew != newShard {
					rr := &schema.ReplicationRequest{
						Type:        schema.MessageTypeReplicationRequest,
						ManifestCID: manifestCID,
						Priority:    0,
						Deadline:    0,
					}
					if err := sm.signer.SignProtocolMessage(rr); err == nil {
						if b, err := rr.MarshalCBOR(); err == nil && sm.JoinShardAsObserver(targetNew) {
							sm.PublishToShardCBOR(b, targetNew)
							sm.LeaveShardAsObserver(targetNew)
							log.Printf("[Reshard] ReplicationRequest sent to shard %s before unpinning %s", targetNew, key)
							select {
							case <-sm.ctx.Done():
							case <-time.After(config.ReshardHandoffDelay):
							}
						}
					}
				}
				log.Printf("[Reshard] Unpinning file that no longer belongs to shard %s: %s", newShard, key)
				if err := sm.clusterMgr.Unpin(sm.ctx, oldShard, manifestCID); err != nil {
					log.Printf("[Reshard] Unpin from old shard failed: %v", err)
				}
				_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
				sm.storageMgr.UnpinFile(key)
			}
		}

		sm.reshardedFiles.Add(key)
		time.Sleep(10 * time.Millisecond)
	}
}

const orphanUnpinInterval = 2 * time.Minute

// RunOrphanUnpinPass unpins files that belong to active child shards (we are still in parent).
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
	unpinned := 0
	for key := range files {
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < config.OrphanUnpinGracePeriod {
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
		sm.mu.Lock()
		var info *orphanHandoffInfo
		if sm.orphanHandoffSent[key] != nil {
			info = sm.orphanHandoffSent[key][targetChild]
		}
		sm.mu.Unlock()
		if info != nil && time.Since(info.lastSent) < config.OrphanHandoffGrace {
			continue
		}
		minCount := config.OrphanUnpinMinHandoffCount
		if minCount < 1 {
			minCount = 1
		}
		if info != nil && info.count >= minCount && time.Since(info.lastSent) >= config.OrphanHandoffGrace {
			// Proceed to unpin
		} else if info == nil || info.count < minCount {
			if sm.signer != nil {
				rr := &schema.ReplicationRequest{
					Type:        schema.MessageTypeReplicationRequest,
					ManifestCID: manifestCID,
					Priority:    0,
					Deadline:    0,
				}
				if err := sm.signer.SignProtocolMessage(rr); err == nil {
					if b, err := rr.MarshalCBOR(); err == nil && sm.JoinShardAsObserver(targetChild) {
						sm.PublishToShardCBOR(b, targetChild)
						sm.LeaveShardAsObserver(targetChild)
						sm.mu.Lock()
						if sm.orphanHandoffSent[key] == nil {
							sm.orphanHandoffSent[key] = make(map[string]*orphanHandoffInfo)
						}
						if sm.orphanHandoffSent[key][targetChild] == nil {
							sm.orphanHandoffSent[key][targetChild] = &orphanHandoffInfo{}
						}
						ho := sm.orphanHandoffSent[key][targetChild]
						ho.lastSent = time.Now()
						ho.count++
						sm.mu.Unlock()
						log.Printf("[Reshard] Orphan handoff: sent ReplicationRequest to child %s for %s (count=%d)", targetChild, key, ho.count)
						time.Sleep(10 * time.Millisecond)
						continue
					}
				}
			}
			continue
		}

		log.Printf("[Reshard] Orphan unpin: %s (belongs to child %s)", key, targetChild)
		_ = sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID)
		_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
		sm.storageMgr.UnpinFile(key)
		sm.mu.Lock()
		if sm.orphanHandoffSent[key] != nil {
			delete(sm.orphanHandoffSent[key], targetChild)
			if len(sm.orphanHandoffSent[key]) == 0 {
				delete(sm.orphanHandoffSent, key)
			}
		}
		sm.mu.Unlock()
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
