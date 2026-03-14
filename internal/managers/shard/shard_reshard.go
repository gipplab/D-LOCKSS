package shard

import (
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

// RunReshardPass migrates or unpins files when moving between shards.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.GetAllKnownFiles()
	if len(files) == 0 {
		return
	}

	slog.Info("starting reshard pass", "from", oldShard, "to", newShard)
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

		payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetOld, err := common.GetHexBinaryPrefix(stableHex, oldDepth)
		if err != nil {
			continue
		}
		targetNew, err := common.GetHexBinaryPrefix(stableHex, newDepth)
		if err != nil {
			continue
		}

		wasResponsible := (targetOld == oldShard)
		isResponsible := (targetNew == newShard)

		if wasResponsible == isResponsible {
			sm.reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := cid.Decode(key)
		if err != nil {
			continue
		}

		if isResponsible && sm.storageMgr.IsPinned(key) {
			im := schema.IngestMessage{
				SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: manifestCID},
				ShardID:        newShard,
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
						SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: manifestCID},
					}
					if err := sm.signer.SignProtocolMessage(rr); err == nil {
						if b, err := rr.MarshalCBOR(); err == nil && sm.JoinShardAsObserver(targetNew) {
							sm.PublishToShardCBOR(b, targetNew)
							sm.LeaveShardAsObserver(targetNew)
							slog.Info("reshard: ReplicationRequest sent before unpinning", "target_shard", targetNew, "manifest", key)
							select {
							case <-sm.ctx.Done():
							case <-time.After(sm.cfg.Files.ReshardHandoffDelay):
							}
						}
					}
				}
				slog.Info("reshard: unpinning file that no longer belongs to shard", "shard", newShard, "manifest", key)
				if err := sm.clusterMgr.Unpin(sm.ctx, oldShard, manifestCID); err != nil {
					slog.Error("reshard: unpin from old shard failed", "manifest", key, "error", err)
				}
				if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
					slog.Error("reshard: IPFS unpin failed", "manifest", key, "error", err)
				}
				sm.storageMgr.UnpinFile(key)
			}
		}

		sm.reshardedFiles.Add(key)
		time.Sleep(10 * time.Millisecond)
	}
}
