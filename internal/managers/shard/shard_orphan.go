package shard

import (
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

const orphanUnpinInterval = 2 * time.Minute

func (sm *ShardManager) RunOrphanUnpinPass() {
	sm.pruneOrphanHandoffSent()

	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

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
	child0, child1 := childShards(currentShard)
	probeTimeout := 4 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)

	active := make(map[string]struct{})
	if n0 >= 1 {
		active[child0] = struct{}{}
	}
	if n1 >= 1 {
		active[child1] = struct{}{}
	}
	return active
}

func (sm *ShardManager) orphanHandoffOrUnpin(key, targetChild, currentShard string, manifestCID cid.Cid) bool {
	sm.mu.Lock()
	var info *orphanHandoffInfo
	if sm.orphanHandoffSent[key] != nil {
		info = sm.orphanHandoffSent[key][targetChild]
	}
	sm.mu.Unlock()

	if info != nil && time.Since(info.lastSent) < sm.cfg.Orphan.HandoffGrace {
		return false
	}

	minCount := sm.cfg.Orphan.UnpinMinHandoffCnt
	if minCount < 1 {
		minCount = 1
	}

	readyToUnpin := info != nil && info.count >= minCount && time.Since(info.lastSent) >= sm.cfg.Orphan.HandoffGrace
	if !readyToUnpin {
		sm.sendOrphanHandoff(key, targetChild, manifestCID)
		return false
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
	return true
}

func (sm *ShardManager) sendOrphanHandoff(key, targetChild string, manifestCID cid.Cid) {
	if sm.signer == nil {
		return
	}
	rr := &schema.ReplicationRequest{
		SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: manifestCID},
	}
	if err := sm.signer.SignProtocolMessage(rr); err != nil {
		return
	}
	b, err := rr.MarshalCBOR()
	if err != nil {
		return
	}
	if !sm.JoinShardAsObserver(targetChild) {
		return
	}
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
