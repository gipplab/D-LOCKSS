package shard

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

// runPeerCountChecker checks peer count in the current shard and triggers splits when at limit.
func (sm *ShardManager) runPeerCountChecker() {
	ticker := time.NewTicker(rootPeerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.checkAndSplitIfNeeded()
			sm.pruneStaleSeenPeers()
		}
	}
}

// pruneStaleSeenPeers drops peers not seen in 10 minutes.
func (sm *ShardManager) pruneStaleSeenPeers() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	cutoff := time.Now().Add(-600 * time.Second)
	for shardID, peers := range sm.seenPeers {
		for peerID, lastSeen := range peers {
			if lastSeen.Before(cutoff) {
				delete(peers, peerID)
			}
		}
		if len(peers) == 0 {
			delete(sm.seenPeers, shardID)
		}
	}
}

// runReplicationChecker sends ReplicationRequest for pinned files below target replication.
func (sm *ShardManager) runReplicationChecker() {
	if config.CheckInterval <= 0 {
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

			interval := config.CheckInterval
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

			for _, manifestCIDStr := range manifests {
				c, err := cid.Decode(manifestCIDStr)
				if err != nil {
					continue
				}
				allocations, err := sm.clusterMgr.GetAllocations(sm.ctx, currentShard, c)
				if err != nil {
					_ = sm.clusterMgr.Pin(sm.ctx, currentShard, c, 0, 0)
					allocations = nil
				}
				peerCount := sm.getShardPeerCount()
				targetRep := config.MaxReplication
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
					const replicationGracePeriod = 3 * time.Minute
					pinTime := sm.storageMgr.GetPinTime(manifestCIDStr)
					if pinTime.IsZero() || time.Since(pinTime) >= replicationGracePeriod {
						continue
					}
				}
				if sm.signer == nil {
					continue
				}
				rr := &schema.ReplicationRequest{
					Type:        schema.MessageTypeReplicationRequest,
					ManifestCID: c,
					Priority:    0,
					Deadline:    0,
				}
				if err := sm.signer.SignProtocolMessage(rr); err != nil {
					log.Printf("[Shard] Failed to sign ReplicationRequest for %s: %v", manifestCIDStr, err)
					continue
				}
				b, err := rr.MarshalCBOR()
				if err != nil {
					continue
				}
				sm.PublishToShardCBOR(b, currentShard)
				log.Printf("[Shard] ReplicationRequest sent for %s (shard %s, active_alloc=%d, total_alloc=%d, target=%d, peers=%d)",
					manifestCIDStr, currentShard, activeAllocations, len(allocations), targetRep, peerCount)
			}
		}
	}
}

// runHeartbeat periodically sends heartbeat messages to the current shard topic.
func (sm *ShardManager) runHeartbeat() {
	var heartbeatInterval time.Duration
	if config.HeartbeatInterval > 0 {
		heartbeatInterval = config.HeartbeatInterval
	} else {
		heartbeatInterval = config.ShardPeerCheckInterval / 3
		if heartbeatInterval < 10*time.Second {
			heartbeatInterval = 10 * time.Second
		}
	}
	const backoffWhenPeers = 2
	const backoffInterval = 5 * time.Minute
	baseInterval := heartbeatInterval

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	sm.sendHeartbeat()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.sendHeartbeat()
			n := sm.getShardPeerCount()
			if n >= backoffWhenPeers && heartbeatInterval < backoffInterval {
				ticker.Reset(backoffInterval)
				heartbeatInterval = backoffInterval
			} else if n < backoffWhenPeers && heartbeatInterval > baseInterval {
				ticker.Reset(baseInterval)
				heartbeatInterval = baseInterval
			}
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
	heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d", sm.h.ID().String(), pinnedCount))
	if err := sub.topic.Publish(sm.ctx, heartbeatMsg); err != nil {
		return
	}
	if config.VerboseLogging {
		log.Printf("[Heartbeat] sent to shard %s (pinned: %d)", currentShard, pinnedCount)
	}

	sm.announcePinnedFilesBatch(sub.topic, 20)
}

func (sm *ShardManager) announcePinnedFilesBatch(topic *pubsub.Topic, batchSize int) {
	for i := 0; i < batchSize; i++ {
		key := sm.storageMgr.GetNextFileToAnnounce()
		if key == "" {
			return
		}
		msg := []byte(fmt.Sprintf("PINNED:%s", key))
		_ = topic.Publish(sm.ctx, msg)
	}
}

// processMessage decodes CBOR and dispatches to Ingest or ReplicationRequest handler.
func (sm *ShardManager) processMessage(msg *pubsub.Message, shardID string) {
	if msg.GetFrom() == sm.h.ID() {
		return
	}

	sm.mu.Lock()
	if sm.seenPeers[shardID] == nil {
		sm.seenPeers[shardID] = make(map[peer.ID]time.Time)
	}
	sm.seenPeers[shardID][msg.GetFrom()] = time.Now()
	sm.lastMessageTime = time.Now()
	sm.mu.Unlock()

	if len(msg.Data) > 0 {
		if msg.Data[0] == '{' {
			return
		}
		if string(msg.Data[:min(10, len(msg.Data))]) == "HEARTBEAT:" {
			return
		}
		if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
			key := string(msg.Data[7:])
			sm.storageMgr.AddKnownFile(key)
			return
		}
		if len(msg.Data) >= 5 && (string(msg.Data[:5]) == "JOIN:" || (len(msg.Data) >= 6 && string(msg.Data[:6]) == "LEAVE:")) {
			return
		}
		if len(msg.Data) > 6 && string(msg.Data[:6]) == "SPLIT:" {
			sm.handleSplitAnnouncement(string(msg.Data[6:]))
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
		log.Printf("[Shard] Failed to decode message type from %s in shard %s: %v", msg.GetFrom().String(), shardID, err)
		return
	}

	switch msgType {
	case schema.MessageTypeIngest:
		var im schema.IngestMessage
		if err := im.UnmarshalCBOR(msg.Data); err != nil {
			log.Printf("[Shard] Failed to unmarshal IngestMessage from %s in shard %s: %v", msg.GetFrom().String(), shardID, err)
			return
		}
		sm.handleIngestMessage(msg, &im, shardID)
	case schema.MessageTypeReplicationRequest:
		var rr schema.ReplicationRequest
		if err := rr.UnmarshalCBOR(msg.Data); err != nil {
			log.Printf("[Shard] Failed to unmarshal ReplicationRequest from %s in shard %s: %v", msg.GetFrom().String(), shardID, err)
			return
		}
		sm.handleReplicationRequest(msg, &rr, shardID)
	}
}

// handleSplitAnnouncement parses SPLIT:child0:child1 and records child shards.
func (sm *ShardManager) handleSplitAnnouncement(payload string) {
	sep := -1
	for i := 0; i < len(payload); i++ {
		if payload[i] == ':' {
			sep = i
			break
		}
	}
	if sep < 1 || sep >= len(payload)-1 {
		return
	}
	child0 := payload[:sep]
	child1 := payload[sep+1:]
	now := time.Now()
	sm.mu.Lock()
	sm.knownChildShards[child0] = now
	sm.knownChildShards[child1] = now
	sm.mu.Unlock()
	log.Printf("[Shard] Received split announcement: children %s and %s", child0, child1)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
