package shard

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
)

const handoffConfirmTimeout = 30 * time.Second

// handoffConfirmed reports whether at least MinPeersPerShard other peers
// advertise the manifest, so dropping the local copy cannot remove the last
// ones. Handoff messages can be lost (rate limits, a mesh not yet formed), so
// sending a request alone is no evidence that the target shard took the file.
func (sm *ShardManager) handoffConfirmed(key string) bool {
	return sm.otherProvidersAtLeast(key, sm.cfg.Sharding.MinPeersPerShard)
}

func (sm *ShardManager) otherProvidersAtLeast(key string, need int) bool {
	var self peer.ID
	if sm.h != nil {
		self = sm.h.ID()
	}
	ctx, cancel := context.WithTimeout(sm.ctx, handoffConfirmTimeout)
	defer cancel()
	return sm.storageMgr.ProviderCount(ctx, key, self, need) >= need
}

// RunReshardPass migrates or unpins files when moving between shards.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.GetAllKnownFiles()
	if len(files) == 0 {
		return
	}

	slog.Info("starting reshard pass", "from", oldShard, "to", newShard)
	for key := range files {
		if sm.ctx.Err() != nil {
			return
		}
		if sm.reshardedFiles.Has(key) {
			continue
		}

		payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetOld, err := common.GetHexBinaryPrefix(stableHex, len(oldShard))
		if err != nil {
			continue
		}
		targetNew, err := common.GetHexBinaryPrefix(stableHex, len(newShard))
		if err != nil {
			continue
		}

		wasResponsible := targetOld == oldShard
		isResponsible := targetNew == newShard
		if wasResponsible == isResponsible {
			sm.reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := cid.Decode(key)
		if err != nil {
			continue
		}

		pinned := sm.storageMgr.IsPinned(key)
		switch {
		case pinned && isResponsible:
			sm.publishIngest(manifestCID, newShard)
		case pinned: // responsible before the move, not after it
			if sm.signer != nil {
				if b, err := sm.signedReplicationRequest(manifestCID); err == nil && sm.publishAsObserver(b, targetNew) {
					slog.Info("reshard: ReplicationRequest sent before unpinning", "target_shard", targetNew, "manifest", key)
					sm.sleep(sm.cfg.Files.ReshardHandoffDelay)
				}
			}
			if !sm.handoffConfirmed(key) {
				slog.Info("reshard: keeping file until its shard holds min copies", "shard", newShard, "target_shard", targetNew, "manifest", key)
				continue
			}
			slog.Info("reshard: unpinning file that no longer belongs to shard", "shard", newShard, "manifest", key)
			if err := sm.clusterMgr.Unpin(sm.ctx, oldShard, manifestCID); err != nil {
				slog.Error("reshard: unpin from old shard failed", "manifest", key, "error", err)
			}
			if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
				slog.Error("reshard: IPFS unpin failed", "manifest", key, "error", err)
			}
			// The reprovide loop pins the payload separately; without this the
			// node keeps a copy of every range it ever belonged to.
			if payloadCID, err := cid.Decode(payloadCIDStr); err == nil && payloadCIDStr != key {
				if err := sm.ipfsClient.UnpinRecursive(sm.ctx, payloadCID); err != nil {
					slog.Debug("reshard: payload unpin failed", "payload", payloadCIDStr, "error", err)
				}
			}
			sm.storageMgr.UnpinFile(key)
		}

		sm.reshardedFiles.Add(key)
		time.Sleep(10 * time.Millisecond)
	}
}
