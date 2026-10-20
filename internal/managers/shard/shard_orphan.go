package shard

import (
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
)

const orphanUnpinInterval = 2 * time.Minute

func (sm *ShardManager) RunOrphanUnpinPass() {
	sm.pruneOrphanHandoffSent()

	currentShard := sm.getCurrentShard()
	files := sm.storageMgr.GetAllKnownFiles()
	if len(files) == 0 {
		return
	}

	activeChildren := sm.collectActiveChildShards(currentShard)
	if len(activeChildren) == 0 {
		return
	}

	depth := len(currentShard) + 1
	unpinned := 0
	for key := range files {
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < sm.cfg.Orphan.UnpinGracePeriod {
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
		manifestCID, err := cid.Decode(key)
		if err != nil {
			continue
		}
		if sm.orphanHandoffOrUnpin(key, targetChild, currentShard, manifestCID) {
			unpinned++
		}
		time.Sleep(10 * time.Millisecond)
	}
	if unpinned > 0 {
		slog.Info("orphan unpin pass complete", "unpinned", unpinned)
	}
}

func (sm *ShardManager) collectActiveChildShards(currentShard string) map[string]struct{} {
	const probeTimeout = 4 * time.Second
	active := make(map[string]struct{})
	child0, child1 := childShards(currentShard)
	if sm.probeShard(child0, probeTimeout) >= 1 {
		active[child0] = struct{}{}
	}
	if sm.probeShard(child1, probeTimeout) >= 1 {
		active[child1] = struct{}{}
	}
	return active
}

func (sm *ShardManager) orphanHandoffOrUnpin(key, targetChild, currentShard string, manifestCID cid.Cid) bool {
	sm.mu.Lock()
	info := sm.orphanHandoffSent[key][targetChild]
	sm.mu.Unlock()

	if info != nil && time.Since(info.lastSent) < sm.cfg.Orphan.HandoffGrace {
		return false
	}

	minCount := max(1, sm.cfg.Orphan.UnpinMinHandoffCnt)
	readyToUnpin := info != nil && info.count >= minCount
	if !readyToUnpin || !sm.handoffConfirmed(key) {
		sm.sendOrphanHandoff(key, targetChild, manifestCID)
		return false
	}

	slog.Info("orphan unpin", "manifest", key, "child", targetChild)
	payloadCIDStr, payloadErr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
	if err := sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID); err != nil {
		slog.Error("orphan unpin: cluster unpin failed", "manifest", key, "error", err)
	}
	if err := sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID); err != nil {
		slog.Error("orphan unpin: IPFS unpin failed", "manifest", key, "error", err)
	}
	// The payload has its own pin; left behind it would look like a user pin
	// to pin ingest.
	if payloadCID, err := cid.Decode(payloadCIDStr); payloadErr == nil && err == nil && payloadCIDStr != key {
		if err := sm.ipfsClient.UnpinRecursive(sm.ctx, payloadCID); err != nil {
			slog.Debug("orphan unpin: payload unpin failed", "payload", payloadCIDStr, "error", err)
		}
	}
	sm.storageMgr.UnpinFile(key)
	sm.mu.Lock()
	delete(sm.orphanHandoffSent[key], targetChild)
	if len(sm.orphanHandoffSent[key]) == 0 {
		delete(sm.orphanHandoffSent, key)
	}
	sm.mu.Unlock()
	return true
}

func (sm *ShardManager) sendOrphanHandoff(key, targetChild string, manifestCID cid.Cid) {
	if sm.signer == nil {
		return
	}
	b, err := sm.signedReplicationRequest(manifestCID)
	if err != nil || !sm.publishAsObserver(b, targetChild) {
		return
	}

	sm.mu.Lock()
	children := sm.orphanHandoffSent[key]
	if children == nil {
		children = make(map[string]*orphanHandoffInfo)
		sm.orphanHandoffSent[key] = children
	}
	info := children[targetChild]
	if info == nil {
		info = &orphanHandoffInfo{}
		children[targetChild] = info
	}
	info.lastSent = time.Now()
	info.count++
	count := info.count
	sm.mu.Unlock()
	slog.Info("orphan handoff: sent ReplicationRequest to child", "child", targetChild, "manifest", key, "count", count)
}

func (sm *ShardManager) pruneOrphanHandoffSent() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	cutoff := time.Now().Add(-2 * sm.cfg.Orphan.HandoffGrace)
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
	sm.runEvery(orphanUnpinInterval, sm.RunOrphanUnpinPass)
}
