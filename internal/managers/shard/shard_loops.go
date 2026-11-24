package shard

import (
	"bytes"
	"context"
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
	interval := sm.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = max(sm.cfg.Sharding.ShardPeerCheckInterval/3, 10*time.Second)
	}
	sm.sendHeartbeat()
	sm.runEvery(interval, sm.sendHeartbeat)
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
	if err := sub.topic.Publish(sm.ctx, sm.heartbeatMessage(pinnedCount, sm.getOurRole())); err != nil {
		return
	}
	slog.Debug("heartbeat sent", "shard", currentShard, "pinned", pinnedCount)

	sm.announcePinnedFilesBatch(sub.topic, 20)

	sm.completeNextPinnedFile()
}

// completeNextPinnedFile re-pins the next pinned manifest that is not yet
// known to be complete, which fetches blocks that a failed first pin left
// out (e.g. OOM or a timeout on a Pi), and pins its payload as its own root.
// Kubo re-pins by walking the whole DAG, so complete files are skipped.
// Kubo's reprovider announces pin roots. A CAS guard keeps one run at a time.
func (sm *ShardManager) completeNextPinnedFile() {
	if !sm.repinInFlight.CompareAndSwap(false, true) {
		return
	}
	manifestCIDStr := sm.storageMgr.GetNextFileToAnnounce()
	if manifestCIDStr == "" || sm.storageMgr.IsComplete(manifestCIDStr) {
		sm.repinInFlight.Store(false)
		return
	}
	go func() {
		defer sm.repinInFlight.Store(false)

		manifestCID, err := cid.Decode(manifestCIDStr)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(sm.ctx, 2*time.Minute)
		defer cancel()
		if err := sm.ipfsClient.PinManifest(ctx, manifestCID); err != nil {
			slog.Debug("re-pin failed, will retry", "manifest", manifestCIDStr, "error", err)
			return
		}
		block, err := sm.ipfsClient.GetBlock(ctx, manifestCID)
		if err != nil {
			return
		}
		var ro schema.ResearchObject
		if err := ro.UnmarshalCBOR(block); err == nil && !ro.HasLegacyTimestamp && ro.Payload.Defined() {
			if err := sm.ipfsClient.PinRecursive(ctx, ro.Payload); err != nil {
				slog.Debug("re-pin payload failed, will retry", "payload", ro.Payload, "error", err)
				return
			}
		}
		sm.storageMgr.MarkComplete(manifestCIDStr)
	}()
}

func (sm *ShardManager) announcePinnedFilesBatch(topic *pubsub.Topic, batchSize int) {
	for i := 0; i < batchSize; i++ {
		key := sm.storageMgr.GetNextFileToAnnounce()
		if key == "" {
			return
		}
		_ = topic.Publish(sm.ctx, []byte(msgPrefixPinned+key))
	}
}

func (sm *ShardManager) processMessage(msg *pubsub.Message, shardID string) {
	from := msg.GetFrom()
	if from == sm.h.ID() {
		return
	}

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

// processTextProtocol handles plain-text protocol messages and reports whether
// msg was consumed.
func (sm *ShardManager) processTextProtocol(msg *pubsub.Message, shardID string, from peer.ID, now time.Time) bool {
	data := msg.Data
	switch {
	case len(data) == 0:
		return false
	case data[0] == '{':
		// JSON messages are consumed and ignored.
	case bytes.HasPrefix(data, []byte(msgPrefixHeartbeat)):
		sm.peers.RecordRole(shardID, from, parseHeartbeatRole(data))
	case bytes.HasPrefix(data, []byte(msgPrefixPinned)):
		sm.storageMgr.AddKnownFile(string(data[len(msgPrefixPinned):]))
	case bytes.HasPrefix(data, []byte(msgPrefixJoin)):
		sm.peers.RecordRole(shardID, from, parseJoinRole(data))
	case bytes.HasPrefix(data, []byte(msgPrefixLeave)):
		sm.peers.RemoveRole(shardID, from)
	case bytes.HasPrefix(data, []byte(msgPrefixProbe)):
		sm.handleProbeMessage(shardID, from, now)
	case bytes.HasPrefix(data, []byte(msgPrefixSplit)):
		sm.lifecycle.recordSplitAnnouncement(string(data[len(msgPrefixSplit):]))
	default:
		return false
	}
	return true
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
	if !sm.lastProbeResponseTime.IsZero() && now.Sub(sm.lastProbeResponseTime) < probeResponseCooldown {
		sm.mu.Unlock()
		return
	}
	sm.lastProbeResponseTime = now
	sm.mu.Unlock()

	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()
	if shardID != currentShard || !exists || sub.topic == nil || sub.observerOnly {
		return
	}
	_ = sub.topic.Publish(sm.ctx, sm.heartbeatMessage(sm.storageMgr.GetPinnedCount(), sm.getOurRole()))
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
	sm.runEvery(sm.cfg.Replication.PinReannounceInterval, sm.reannouncePins)
}

// reannouncePins announces every pinned manifest the current shard is
// responsible for.
func (sm *ShardManager) reannouncePins() {
	const delayBetweenPins = 40 * time.Millisecond
	announced := 0
	for _, manifestCIDStr := range sm.storageMgr.GetPinnedManifests() {
		if common.IsLegacyManifest(sm.ctx, sm.ipfsClient, manifestCIDStr) {
			continue
		}
		payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, manifestCIDStr)
		if !sm.AmIResponsibleFor(payloadCIDStr) {
			continue
		}
		sm.AnnouncePinned(manifestCIDStr)
		announced++
		if !sm.sleep(delayBetweenPins) {
			return
		}
	}
	if announced > 0 {
		slog.Debug("re-announced pins on current shard", "announced", announced, "interval", sm.cfg.Replication.PinReannounceInterval)
	}
}
