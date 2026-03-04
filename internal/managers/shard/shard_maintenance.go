package shard

import (
	"log/slog"
	"time"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

const orphanUnpinInterval = 2 * time.Minute

// RunOrphanUnpinPass unpins files that belong to active child shards (we are still in parent).
func (sm *ShardManager) RunOrphanUnpinPass() {
	sm.pruneOrphanHandoffSent()

	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	files := sm.storageMgr.GetAllKnownFiles()
	if len(files) == 0 {
		return
	}

	child0, child1 := childShards(currentShard)
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
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < sm.cfg.OrphanUnpinGracePeriod {
			continue
		}
		payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetChild, err := common.GetHexBinaryPrefix(stableHex, depth)
		if err != nil {
			continue
		}
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
		if info != nil && time.Since(info.lastSent) < sm.cfg.OrphanHandoffGrace {
			continue
		}
		minCount := sm.cfg.OrphanUnpinMinHandoffCount
		if minCount < 1 {
			minCount = 1
		}
		if info != nil && info.count >= minCount && time.Since(info.lastSent) >= sm.cfg.OrphanHandoffGrace {
			// Proceed to unpin
		} else if info == nil || info.count < minCount {
			if sm.signer != nil {
				rr := &schema.ReplicationRequest{
					SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: manifestCID},
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
						slog.Info("orphan handoff: sent ReplicationRequest to child", "child", targetChild, "manifest", key, "count", ho.count)
						time.Sleep(10 * time.Millisecond)
						continue
					}
				}
			}
			continue
		}

		slog.Info("orphan unpin", "manifest", key, "child", targetChild)
		if err := sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID); err != nil {
			slog.Error("orphan unpin: cluster unpin failed", "manifest", key, "error", err)
		}
		if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
			slog.Error("orphan unpin: IPFS unpin failed", "manifest", key, "error", err)
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
		slog.Info("orphan unpin pass complete", "unpinned", unpinned)
	}
}

func (sm *ShardManager) pruneOrphanHandoffSent() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	cutoff := time.Now().Add(-2 * sm.cfg.OrphanHandoffGrace)
	for key, children := range sm.orphanHandoffSent {
		for child, info := range children {
			if info.lastSent.Before(cutoff) {
				delete(children, child)
			}
		}
		if len(children) == 0 {
			delete(sm.orphanHandoffSent, key)
		}
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

const legacyCleanupInterval = 5 * time.Minute

// runLegacyManifestCleanup periodically scans pinned manifests and unpins any
// that contain a legacy timestamp field (non-deterministic CIDs from the old format).
func (sm *ShardManager) runLegacyManifestCleanup() {
	select {
	case <-sm.ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}
	sm.cleanupLegacyManifests()

	ticker := time.NewTicker(legacyCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.cleanupLegacyManifests()
		}
	}
}

func (sm *ShardManager) cleanupLegacyManifests() {
	manifests := sm.storageMgr.GetPinnedManifests()
	if len(manifests) == 0 {
		return
	}

	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	removed := 0
	for _, manifestCIDStr := range manifests {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}
		if !common.IsLegacyManifest(sm.ctx, sm.ipfsClient, manifestCIDStr) {
			continue
		}
		manifestCID, err := common.KeyToCID(manifestCIDStr)
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

func (sm *ShardManager) runReannouncePinsLoop() {
	if sm.cfg.PinReannounceInterval <= 0 {
		return
	}
	ticker := time.NewTicker(sm.cfg.PinReannounceInterval)
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
				if common.IsLegacyManifest(sm.ctx, sm.ipfsClient, manifestCIDStr) {
					continue
				}
				payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, manifestCIDStr)
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
			if announced > 0 {
				slog.Debug("re-announced pins on current shard", "announced", announced, "interval", sm.cfg.PinReannounceInterval)
			}
		}
	}
}
