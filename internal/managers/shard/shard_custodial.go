package shard

import (
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

const (
	custodialRetryInterval     = 2 * time.Minute
	maxCustodialRetriesPerPass = 10
)

// RunCustodialRetryPass handles files this node holds for another shard: an
// ingest whose cross-shard injection was lost (receivers rate-limit senders,
// a fresh observer may publish before the mesh forms), or a copy left from an
// earlier membership. Once the responsible shard confirms min copies the local
// copy is released; until then the file is injected again, a bounded number
// per pass so the retries themselves stay under the receivers' rate limit.
// Files of the node's own shard get the same check: an ingest or a pin
// migration during shard moves can leave a file in one member's CRDT only,
// so a holder that sees fewer than MinPeersPerShard-1 other providers
// announces the file to its shard again.
func (sm *ShardManager) RunCustodialRetryPass() {
	retried, released, reannounced := 0, 0, 0
	for key := range sm.storageMgr.GetAllKnownFiles() {
		if sm.ctx.Err() != nil {
			return
		}
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < custodialRetryInterval {
			continue
		}
		payloadCIDStr, err := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		if err != nil || payloadCIDStr == key {
			continue
		}
		if sm.AmIResponsibleFor(payloadCIDStr) {
			if reannounced < maxCustodialRetriesPerPass && !sm.otherProvidersAtLeast(key, sm.cfg.Sharding.MinPeersPerShard-1) {
				sm.reannounceToOwnShard(key)
				reannounced++
			}
			continue
		}
		if sm.handoffConfirmed(key) {
			sm.releaseCustodialCopy(key, payloadCIDStr)
			released++
			continue
		}
		if retried >= maxCustodialRetriesPerPass {
			continue
		}
		sm.reinjectCustodial(key, payloadCIDStr)
		retried++
	}
	if retried > 0 || released > 0 || reannounced > 0 {
		slog.Info("custodial retry pass complete", "reinjected", retried, "released", released, "reannounced", reannounced)
	}
}

func (sm *ShardManager) reannounceToOwnShard(key string) {
	manifestCID, err := cid.Decode(key)
	if err != nil {
		return
	}
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if err := sm.clusterMgr.PinIfAbsent(sm.ctx, currentShard, manifestCID, -1, -1); err != nil {
		slog.Error("re-announce: failed to pin to own shard", "shard", currentShard, "error", err)
	}
	if sm.signer != nil {
		im := schema.IngestMessage{
			SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: manifestCID},
			ShardID:        currentShard,
		}
		if err := sm.signer.SignProtocolMessage(&im); err == nil {
			if b, err := im.MarshalCBOR(); err == nil {
				sm.PublishToShardCBOR(b, currentShard)
			}
		}
	}
	slog.Info("re-announced under-replicated file to own shard", "shard", currentShard, "manifest", key)
}

func (sm *ShardManager) releaseCustodialCopy(key, payloadCIDStr string) {
	manifestCID, err := cid.Decode(key)
	if err != nil {
		return
	}
	slog.Info("custodial: releasing copy, responsible shard holds min copies", "manifest", key)
	if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
		slog.Debug("custodial: manifest unpin failed", "manifest", key, "error", err)
	}
	if payloadCID, err := cid.Decode(payloadCIDStr); err == nil {
		if err := sm.ipfsClient.UnpinRecursive(sm.ctx, payloadCID); err != nil {
			slog.Debug("custodial: payload unpin failed", "payload", payloadCIDStr, "error", err)
		}
	}
	sm.storageMgr.UnpinFile(key)
}

func (sm *ShardManager) reinjectCustodial(key, payloadCIDStr string) {
	manifestCID, err := cid.Decode(key)
	if err != nil {
		return
	}
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	nominal, err := common.TargetShardForPayload(payloadCIDStr, max(1, len(currentShard)))
	if err != nil {
		return
	}
	target := sm.ResolveTargetShardForCustodial(nominal, payloadCIDStr)
	if target == currentShard {
		return
	}
	if !sm.JoinShardAsObserver(target) {
		return
	}
	defer sm.LeaveShardAsObserver(target)
	if err := sm.clusterMgr.JoinShard(sm.ctx, target); err != nil {
		slog.Error("custodial retry: failed to join target cluster", "target_shard", target, "error", err)
		return
	}
	if err := sm.clusterMgr.Pin(sm.ctx, target, manifestCID, -1, -1); err != nil {
		slog.Error("custodial retry: failed to pin to target shard", "target_shard", target, "error", err)
		return
	}
	if sm.signer != nil {
		im := schema.IngestMessage{
			SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: manifestCID},
			ShardID:        target,
		}
		if err := sm.signer.SignProtocolMessage(&im); err == nil {
			if b, err := im.MarshalCBOR(); err == nil {
				sm.PublishToShardCBOR(b, target)
			}
		}
	}
	slog.Info("custodial retry: re-injected file", "target_shard", target, "manifest", key)
}

func (sm *ShardManager) runCustodialRetryLoop() {
	ticker := time.NewTicker(custodialRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.RunCustodialRetryPass()
		}
	}
}
