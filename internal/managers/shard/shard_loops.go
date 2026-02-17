package shard

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

const probeResponseCooldown = 5 * time.Second

// runSplitRebroadcast periodically re-broadcasts SPLIT to all ancestor shards so late-joining
// nodes (e.g. in root or parent) can discover existing children. Each node in a child shard
// publishes to its ancestors; no central coordinator needed.
func (sm *ShardManager) runSplitRebroadcast() {
	jitterRange := config.ShardSplitRebroadcastInterval / 2
	if jitterRange < time.Second {
		jitterRange = time.Second
	}
	for {
		delay := config.ShardSplitRebroadcastInterval + time.Duration(rand.Int63n(int64(jitterRange)))
		t := time.NewTimer(delay)
		select {
		case <-sm.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			sm.rebroadcastSplitToAncestors()
		}
	}
}

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

// pruneStaleSeenPeers drops peers not seen within PruneStalePeersInterval.
func (sm *ShardManager) pruneStaleSeenPeers() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	cutoff := time.Now().Add(-config.PruneStalePeersInterval)
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
	for shardID, roles := range sm.seenPeerRoles {
		for peerID, info := range roles {
			if info.LastSeen.Before(cutoff) {
				delete(roles, peerID)
			}
		}
		if len(roles) == 0 {
			delete(sm.seenPeerRoles, shardID)
		}
	}
}

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

			sm.pruneReplicationRequestCooldown()

			maxConc := config.MaxConcurrentReplicationChecks
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
						Type:        schema.MessageTypeReplicationRequest,
						ManifestCID: c,
						Priority:    0,
						Deadline:    0,
					}
					if err := sm.signer.SignProtocolMessage(rr); err != nil {
						log.Printf("[Shard] Failed to sign ReplicationRequest for %s: %v", manifestCIDStr, err)
						return
					}
					b, err := rr.MarshalCBOR()
					if err != nil {
						return
					}
					sm.PublishToShardCBOR(b, currentShard)
					atomic.AddInt32(&sentThisCycle, 1)
					if config.VerboseLogging {
						log.Printf("[Shard] ReplicationRequest sent for %s (shard %s, active_alloc=%d, total_alloc=%d, target=%d, peers=%d)",
							manifestCIDStr, currentShard, activeAllocations, len(allocations), targetRep, peerCount)
					}
				}(manifestCIDStr)
			}
			wg.Wait()
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
	heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d:%s", sm.h.ID().String(), pinnedCount, role))
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

	from := msg.GetFrom()
	now := time.Now()
	sm.mu.Lock()
	if sm.seenPeers[shardID] == nil {
		sm.seenPeers[shardID] = make(map[peer.ID]time.Time)
	}
	sm.seenPeers[shardID][from] = now
	sm.lastMessageTime = now
	sm.mu.Unlock()

	if len(msg.Data) > 0 {
		if msg.Data[0] == '{' {
			return
		}
		if len(msg.Data) >= 10 && string(msg.Data[:10]) == "HEARTBEAT:" {
			role := parseHeartbeatRole(msg.Data)
			sm.mu.Lock()
			if sm.seenPeerRoles[shardID] == nil {
				sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
			}
			sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: role, LastSeen: now}
			sm.mu.Unlock()
			return
		}
		if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
			key := string(msg.Data[7:])
			sm.storageMgr.AddKnownFile(key)
			return
		}
		if len(msg.Data) >= 5 && string(msg.Data[:5]) == "JOIN:" {
			role := parseJoinRole(msg.Data)
			sm.mu.Lock()
			if sm.seenPeerRoles[shardID] == nil {
				sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
			}
			sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: role, LastSeen: now}
			sm.mu.Unlock()
			return
		}
		if len(msg.Data) >= 6 && string(msg.Data[:6]) == "LEAVE:" {
			sm.mu.Lock()
			if sm.seenPeerRoles[shardID] != nil {
				delete(sm.seenPeerRoles[shardID], from)
			}
			sm.mu.Unlock()
			return
		}
		if len(msg.Data) >= 6 && string(msg.Data[:6]) == "PROBE:" {
			sm.mu.Lock()
			if sm.seenPeerRoles[shardID] == nil {
				sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
			}
			sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: RoleProbe, LastSeen: now}

			// Rate-limit heartbeat responses to PROBEs.  When multiple nodes
			// probe at once (e.g. after synchronized MergeUpCooldown expiry),
			// every probe triggers an immediate heartbeat from every peer.
			// With N probers and M responders this creates N*M messages in a
			// short window — a "heartbeat storm".  Limit responses to at most
			// one per probeResponseCooldown (5 s) to keep traffic bounded.
			probeRateLimited := !sm.lastProbeResponseTime.IsZero() && now.Sub(sm.lastProbeResponseTime) < probeResponseCooldown
			if !probeRateLimited {
				sm.lastProbeResponseTime = now
			}
			sm.mu.Unlock()

			if probeRateLimited {
				return
			}

			// Respond with an immediate heartbeat if this is our current shard.
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
				hb := []byte(fmt.Sprintf("HEARTBEAT:%s:%d:%s", sm.h.ID().String(), pinnedCount, role))
				_ = probeSub.topic.Publish(sm.ctx, hb)
			}
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
