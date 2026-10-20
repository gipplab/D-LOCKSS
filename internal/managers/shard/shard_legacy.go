package shard

import (
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
)

const legacyCleanupInterval = 5 * time.Minute

func (sm *ShardManager) runLegacyManifestCleanup() {
	if !sm.sleep(30 * time.Second) {
		return
	}
	sm.cleanupLegacyManifests()
	sm.runEvery(legacyCleanupInterval, sm.cleanupLegacyManifests)
}

func (sm *ShardManager) cleanupLegacyManifests() {
	manifests := sm.storageMgr.GetPinnedManifests()
	if len(manifests) == 0 {
		return
	}

	currentShard := sm.getCurrentShard()
	removed := 0
	for _, manifestCIDStr := range manifests {
		if sm.ctx.Err() != nil {
			return
		}
		if !common.IsLegacyManifest(sm.ctx, sm.ipfsClient, manifestCIDStr) {
			continue
		}
		manifestCID, err := cid.Decode(manifestCIDStr)
		if err != nil {
			continue
		}
		slog.Info("removing legacy manifest", "manifest", manifestCIDStr)
		if currentShard != "" {
			if err := sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID); err != nil {
				slog.Error("cluster unpin failed for legacy manifest", "manifest", manifestCIDStr, "error", err)
			}
		}
		if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
			slog.Error("IPFS unpin failed for legacy manifest", "manifest", manifestCIDStr, "error", err)
		}
		sm.storageMgr.UnpinFile(manifestCIDStr)
		removed++
		time.Sleep(50 * time.Millisecond)
	}
	if removed > 0 {
		slog.Info("legacy manifest cleanup complete", "removed", removed)
	}
}
