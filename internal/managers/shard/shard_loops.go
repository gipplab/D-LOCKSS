package shard

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"dlockss/internal/common"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

const probeResponseCooldown = 5 * time.Second

// runHeartbeat periodically sends heartbeat messages to the current shard topic.
func (sm *ShardManager) runHeartbeat() {
	var heartbeatInterval time.Duration
	if sm.cfg.HeartbeatInterval > 0 {
		heartbeatInterval = sm.cfg.HeartbeatInterval
	} else {
		heartbeatInterval = sm.cfg.Sharding.ShardPeerCheckInterval / 3
		if heartbeatInterval < 10*time.Second {
			heartbeatInterval = 10 * time.Second
		}
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	sm.sendHeartbeat()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.sendHeartbeat()
		}
	}
}

// sendHeartbeat publishes heartbeat and a batch of PINNED to the current shard.
func (sm *ShardManager) sendHeartbeat() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return
	}

	pinnedCount := sm.storageMgr.GetPinnedCount()
	role := sm.getOurRole()
	heartbeatMsg := []byte(fmt.Sprintf("%s%s:%d:%s:%s", msgPrefixHeartbeat, sm.h.ID().String(), pinnedCount, role, sm.nodeName))
	if err := sub.topic.Publish(sm.ctx, heartbeatMsg); err != nil {
		return
	}
	slog.Debug("heartbeat sent", "shard", currentShard, "pinned", pinnedCount)

	sm.announcePinnedFilesBatch(sub.topic, 20)

	sm.reprovideNextPinnedFile()
}

// reprovideNextPinnedFile re-pins one manifest each heartbeat (fetching any
// missing blocks), then provides both manifest and payload CIDs to the DHT.
// This gradually completes incomplete DAGs on resource-constrained nodes
// and keeps DHT provider records fresh (~24h expiry).
// A CAS guard prevents concurrent iterations from piling up.
func (sm *ShardManager) reprovideNextPinnedFile() {
	if !sm.reprovideInFlight.CompareAndSwap(false, true) {
		return
	}
	manifestCIDStr := sm.storageMgr.GetNextFileToAnnounce()
	if manifestCIDStr == "" {
		sm.reprovideInFlight.Store(false)
		return
	}
	go func() {
		defer sm.reprovideInFlight.Store(false)

		manifestCID, err := cid.Decode(manifestCIDStr)
		if err != nil {
			return
		}

		// Re-pin to fetch any blocks missed by the initial PinRecursive
		// (e.g. OOM/timeout on low-memory Pis). Idempotent: returns
		// quickly when the DAG is already complete locally.
		pinCtx, pinCancel := context.WithTimeout(sm.ctx, 2*time.Minute)
		if err := sm.ipfsClient.PinRecursive(pinCtx, manifestCID); err != nil {
			pinCancel()
			slog.Debug("reprovide pin failed, will retry", "manifest", manifestCIDStr, "error", err)
			return
		}
		pinCancel()

		pctx, pcancel := context.WithTimeout(sm.ctx, sm.cfg.Files.DHTProvideTimeout)
		defer pcancel()
		sm.storageMgr.ProvideFile(pctx, manifestCIDStr)

		block, err := sm.ipfsClient.GetBlock(pctx, manifestCID)
		if err != nil {
			return
		}
		var ro schema.ResearchObject
		if err := ro.UnmarshalCBOR(block); err != nil {
			return
		}
		if ro.HasLegacyTimestamp {
			return
		}
		payloadCID := ro.Payload
		if !payloadCID.Defined() {
			return
		}
		if err := sm.ipfsClient.PinRecursive(sm.ctx, payloadCID); err != nil {
			slog.Debug("reprovide pin payload failed", "payload", payloadCID, "error", err)
		}
		pctx2, pcancel2 := context.WithTimeout(sm.ctx, sm.cfg.Files.DHTProvideTimeout)
		defer pcancel2()
		sm.storageMgr.ProvideFile(pctx2, payloadCID.String())
	}()
}

func (sm *ShardManager) announcePinnedFilesBatch(topic *pubsub.Topic, batchSize int) {
	for i := 0; i < batchSize; i++ {
		key := sm.storageMgr.GetNextFileToAnnounce()
		if key == "" {
			return
		}
		msg := []byte(msgPrefixPinned + key)
		_ = topic.Publish(sm.ctx, msg)
	}
}

func (sm *ShardManager) processMessage(msg *pubsub.Message, shardID string) {
	if msg.GetFrom() == sm.h.ID() {
		return
	}

	from := msg.GetFrom()
	now := time.Now()
	sm.peers.RecordSeen(shardID, from)
	sm.mu.Lock()
	sm.lastMessageTime = now
	sm.mu.Unlock()

	if sm.processTextProtocol(msg, shardID, from, now) {
		return
	}

	if sm.rateLimiter != nil && !sm.rateLimiter.Check(from) {
		return
	}
	sm.processCBORMessage(msg, shardID)
}

func (sm *ShardManager) processTextProtocol(msg *pubsub.Message, shardID string, from peer.ID, now time.Time) bool {
	if len(msg.Data) == 0 {
		return false
	}
	if msg.Data[0] == '{' {
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixHeartbeat)) {
		sm.peers.RecordRole(shardID, from, parseHeartbeatRole(msg.Data))
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixPinned)) {
		key := string(msg.Data[len(msgPrefixPinned):])
		sm.storageMgr.AddKnownFile(key)
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixJoin)) {
		sm.peers.RecordRole(shardID, from, parseJoinRole(msg.Data))
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixLeave)) {
		sm.peers.RemoveRole(shardID, from)
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixProbe)) {
		sm.handleProbeMessage(shardID, from, now)
		return true
	}
	if bytes.HasPrefix(msg.Data, []byte(msgPrefixSplit)) {
		sm.lifecycle.recordSplitAnnouncement(string(msg.Data[len(msgPrefixSplit):]))
		return true
	}
	return false
}

func (sm *ShardManager) processCBORMessage(msg *pubsub.Message, shardID string) {
	msgType, err := decodeCBORMessageType(msg.Data)
	if err != nil {
		slog.Error("failed to decode message type", "from", msg.GetFrom().String(), "shard", shardID, "error", err)
		return
	}

	switch msgType {
	case schema.MessageTypeIngest:
		var im schema.IngestMessage
		if err := im.UnmarshalCBOR(msg.Data); err != nil {
			slog.Error("failed to unmarshal IngestMessage", "from", msg.GetFrom().String(), "shard", shardID, "error", err)
			return
		}
		sm.handleIngestMessage(msg, &im, shardID)
	case schema.MessageTypeReplicationRequest:
		var rr schema.ReplicationRequest
		if err := rr.UnmarshalCBOR(msg.Data); err != nil {
			slog.Error("failed to unmarshal ReplicationRequest", "from", msg.GetFrom().String(), "shard", shardID, "error", err)
			return
		}
		sm.repl.handleRequest(msg, &rr, shardID)
	}
}

func (sm *ShardManager) handleProbeMessage(shardID string, from peer.ID, now time.Time) {
	sm.peers.RecordRole(shardID, from, roleProbe)

	sm.mu.Lock()
	probeRateLimited := !sm.lastProbeResponseTime.IsZero() && now.Sub(sm.lastProbeResponseTime) < probeResponseCooldown
	if !probeRateLimited {
		sm.lastProbeResponseTime = now
	}
	sm.mu.Unlock()

	if probeRateLimited {
		return
	}

	sm.mu.RLock()
	cs := sm.currentShard
	probeSub, probeSubExists := sm.shardSubs[shardID]
	sm.mu.RUnlock()
	if shardID == cs && probeSubExists && probeSub.topic != nil && !probeSub.observerOnly {
		pinnedCount := 0
		if sm.storageMgr != nil {
			pinnedCount = sm.storageMgr.GetPinnedCount()
		}
		role := sm.getOurRole()
		hb := []byte(fmt.Sprintf("HEARTBEAT:%s:%d:%s:%s", sm.h.ID().String(), pinnedCount, role, sm.nodeName))
		_ = probeSub.topic.Publish(sm.ctx, hb)
	}
}

func decodeCBORMessageType(data []byte) (schema.MessageType, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return 0, err
	}
	node := nb.Build()
	tn, err := node.LookupByString("type")
	if err != nil {
		return 0, err
	}
	ti, err := tn.AsInt()
	if err != nil {
		return 0, err
	}
	return schema.MessageType(ti), nil
}

func (sm *ShardManager) runReannouncePinsLoop() {
	if sm.cfg.Replication.PinReannounceInterval <= 0 {
		return
	}
	ticker := time.NewTicker(sm.cfg.Replication.PinReannounceInterval)
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
				slog.Debug("re-announced pins on current shard", "announced", announced, "interval", sm.cfg.Replication.PinReannounceInterval)
			}
		}
	}
}
