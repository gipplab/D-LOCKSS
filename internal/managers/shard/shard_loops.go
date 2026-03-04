package shard

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

const probeResponseCooldown = 5 * time.Second

// pruneReplicationRequestCooldown removes stale entries from the cooldown map.
func (sm *ShardManager) pruneReplicationRequestCooldown() {
	sm.replicationRequestMu.Lock()
	defer sm.replicationRequestMu.Unlock()
	cutoff := time.Now().Add(-2 * replicationRequestCooldownDuration)
	for cidStr, lastSent := range sm.replicationRequestLastSent {
		if lastSent.Before(cutoff) {
			delete(sm.replicationRequestLastSent, cidStr)
		}
	}
}

// runReplicationChecker sends ReplicationRequest for pinned files below target replication.
func (sm *ShardManager) runReplicationChecker() {
	if sm.cfg.CheckInterval <= 0 {
		return
	}
	ticker := time.NewTicker(rootReplicationCheckInterval)
	defer ticker.Stop()

	var lastReplicationCheck time.Time
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.RLock()
			currentShard := sm.currentShard
			sm.mu.RUnlock()

			interval := sm.cfg.CheckInterval
			if currentShard == "" {
				interval = rootReplicationCheckInterval
			}
			if time.Since(lastReplicationCheck) < interval {
				continue
			}
			lastReplicationCheck = time.Now()

			manifests := sm.storageMgr.GetPinnedManifests()
			if len(manifests) == 0 {
				continue
			}

			sm.pruneReplicationRequestCooldown()

			maxConc := sm.cfg.MaxConcurrentReplicationChecks
			if maxConc < 1 {
				maxConc = 1
			}
			sem := make(chan struct{}, maxConc)
			var wg sync.WaitGroup
			var sentThisCycle int32
			for _, manifestCIDStr := range manifests {
				select {
				case <-sm.ctx.Done():
					wg.Wait()
					return
				case sem <- struct{}{}:
				}
				if atomic.LoadInt32(&sentThisCycle) >= maxReplicationRequestsPerCycle {
					<-sem
					continue
				}
				wg.Add(1)
				go func(manifestCIDStr string) {
					defer wg.Done()
					defer func() { <-sem }()
					c, err := cid.Decode(manifestCIDStr)
					if err != nil {
						return
					}
					allocations, err := sm.clusterMgr.GetAllocations(sm.ctx, currentShard, c)
					if err != nil {
						_ = sm.clusterMgr.Pin(sm.ctx, currentShard, c, -1, -1)
						allocations = nil
					}
					peerCount := sm.getShardPeerCount()
					targetRep := sm.cfg.MaxReplication
					if peerCount > 0 && targetRep > peerCount {
						targetRep = peerCount
					}
					currentPeers := sm.GetPeersForShard(currentShard)
					currentSet := make(map[peer.ID]struct{}, len(currentPeers)+1)
					currentSet[sm.h.ID()] = struct{}{}
					for _, p := range currentPeers {
						currentSet[p] = struct{}{}
					}
					activeAllocations := 0
					for _, a := range allocations {
						if _, ok := currentSet[a]; ok {
							activeAllocations++
						}
					}
					if activeAllocations >= targetRep {
						return
					}
					if atomic.LoadInt32(&sentThisCycle) >= maxReplicationRequestsPerCycle {
						return
					}
					sm.replicationRequestMu.Lock()
					lastSent := sm.replicationRequestLastSent[manifestCIDStr]
					if time.Since(lastSent) < replicationRequestCooldownDuration {
						sm.replicationRequestMu.Unlock()
						return
					}
					sm.replicationRequestLastSent[manifestCIDStr] = time.Now()
					sm.replicationRequestMu.Unlock()
					if sm.signer == nil {
						return
					}
					rr := &schema.ReplicationRequest{
						SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: c},
					}
					if err := sm.signer.SignProtocolMessage(rr); err != nil {
						slog.Error("failed to sign ReplicationRequest", "manifest", manifestCIDStr, "error", err)
						return
					}
					b, err := rr.MarshalCBOR()
					if err != nil {
						return
					}
					sm.PublishToShardCBOR(b, currentShard)
					atomic.AddInt32(&sentThisCycle, 1)
					slog.Debug("ReplicationRequest sent", "manifest", manifestCIDStr, "shard", currentShard, "active_alloc", activeAllocations, "total_alloc", len(allocations), "target", targetRep, "peers", peerCount)
				}(manifestCIDStr)
			}
			wg.Wait()
		}
	}
}

// runHeartbeat periodically sends heartbeat messages to the current shard topic.
func (sm *ShardManager) runHeartbeat() {
	var heartbeatInterval time.Duration
	if sm.cfg.HeartbeatInterval > 0 {
		heartbeatInterval = sm.cfg.HeartbeatInterval
	} else {
		heartbeatInterval = sm.cfg.ShardPeerCheckInterval / 3
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

		pctx, pcancel := context.WithTimeout(sm.ctx, sm.cfg.DHTProvideTimeout)
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
		pctx2, pcancel2 := context.WithTimeout(sm.ctx, sm.cfg.DHTProvideTimeout)
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

// processMessage decodes CBOR and dispatches to Ingest or ReplicationRequest handler.
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

	if len(msg.Data) > 0 {
		if msg.Data[0] == '{' {
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixHeartbeat)) {
			sm.peers.RecordRole(shardID, from, parseHeartbeatRole(msg.Data))
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixPinned)) {
			key := string(msg.Data[len(msgPrefixPinned):])
			sm.storageMgr.AddKnownFile(key)
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixJoin)) {
			sm.peers.RecordRole(shardID, from, parseJoinRole(msg.Data))
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixLeave)) {
			sm.peers.RemoveRole(shardID, from)
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixProbe)) {
			sm.peers.RecordRole(shardID, from, RoleProbe)

			// Rate-limit heartbeat responses to PROBEs to avoid "heartbeat storms".
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
			return
		}
		if bytes.HasPrefix(msg.Data, []byte(msgPrefixSplit)) {
			sm.lifecycle.recordSplitAnnouncement(string(msg.Data[len(msgPrefixSplit):]))
			return
		}
	}

	if sm.rateLimiter != nil && !sm.rateLimiter.Check(msg.GetFrom()) {
		sm.metrics.IncrementMessagesDropped()
		return
	}

	sm.mu.Lock()
	sm.msgCounter++
	sm.mu.Unlock()

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
		sm.handleReplicationRequest(msg, &rr, shardID)
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
