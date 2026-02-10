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
		// Already pinned locally — don't re-pin to CRDT to avoid overwriting
		// full-replication (empty allocations) pins with allocation-specific pins
		// that cause churn when nodes compute slightly different peer lists.
		if err := sm.EnsureClusterForShard(sm.ctx, shardID); err != nil {
			log.Printf("[Shard] ReplicationRequest: failed to ensure cluster for shard %s: %v", shardID, err)
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
			log.Printf("[Shard] Auto-replication: failed to ensure cluster for shard %s: %v", shardID, err)
			return
		}
		// Write to CRDT with empty allocations (full replication mode) so the PinTracker
		// doesn't unpin the file before the migration entry propagates via CRDT rebroadcast.
		// Using (0, 0) preserves full-replication semantics without computing specific
		// allocations that would cause churn when nodes see slightly different peer lists.
		if err := sm.clusterMgr.Pin(sm.ctx, shardID, c, 0, 0); err != nil {
			log.Printf("[Shard] Auto-replication: failed to write CRDT pin for %s: %v", manifestCIDStr, err)
		}
		log.Printf("[Shard] Auto-replication: fetched and pinned %s to shard %s", manifestCIDStr, shardID)
		sm.clusterMgr.TriggerSync(shardID)
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

	// Only pin if this message arrived on our current shard (not an observed shard).
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if shardID != currentShard {
		return
	}

	// Use a short timeout for PayloadCID resolution so we don't block message processing
	// while waiting for the manifest block to propagate via IPFS.
	resolveCtx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	payloadCIDStr := common.GetPayloadCIDForShardAssignment(resolveCtx, sm.ipfsClient, key)
	cancel()

	// If PayloadCID resolved successfully but maps to a different shard, skip pinning.
	// If resolution failed (payloadCIDStr == key, i.e. fallback to ManifestCID because the
	// manifest block hasn't propagated yet), trust the signed IngestMessage sender's shard
	// assignment and pin anyway — the sender already verified responsibility before sending.
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
				// Reshard safety: before dropping our copy, notify the correct child shard so
				// nodes there can fetch/pin before we unpin. Reduces risk of dropping the last replica.
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
							log.Printf("[Reshard] ReplicationRequest sent to shard %s before unpinning %s (file belongs there)", targetNew, key)
							// Brief delay to give nodes in targetNew time to start fetch before we drop our copy.
							select {
							case <-sm.ctx.Done():
							case <-time.After(reshardHandoffDelay):
							}
						}
					}
				}
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
const orphanUnpinGracePeriod = 6 * time.Minute
const orphanHandoffGrace = 6 * time.Minute  // After sending ReplicationRequest to child, wait this long before orphan-unpinning (New 2: child replication sufficient).
const reshardHandoffDelay = 3 * time.Second // Delay before unpinning so target shard can start fetch (reshard safety).

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

		// New 2 (full): only orphan-unpin after we've given the child a chance to replicate.
		// If we've sent a ReplicationRequest to this child for this file within orphanHandoffGrace, skip (extend grace).
		sm.mu.Lock()
		lastSent := time.Time{}
		if sm.orphanHandoffSent[key] != nil {
			lastSent = sm.orphanHandoffSent[key][targetChild]
		}
		sm.mu.Unlock()
		if !lastSent.IsZero() && time.Since(lastSent) < orphanHandoffGrace {
			continue // Child had handoff recently; don't unpin yet.
		}

		// If we haven't sent a handoff yet, send ReplicationRequest to child and skip unpinning this round.
		// After orphanHandoffGrace we'll unpin in a later pass.
		if lastSent.IsZero() && sm.signer != nil {
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
						sm.orphanHandoffSent[key] = make(map[string]time.Time)
					}
					sm.orphanHandoffSent[key][targetChild] = time.Now()
					sm.mu.Unlock()
					log.Printf("[Reshard] Orphan handoff: sent ReplicationRequest to child %s for %s (will unpin after %v)", targetChild, key, orphanHandoffGrace)
					time.Sleep(10 * time.Millisecond)
					continue // Don't unpin this round; give child time to replicate.
				}
			}
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
