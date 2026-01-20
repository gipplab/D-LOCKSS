package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"

	"dlockss/pkg/schema"
)

type ShardManager struct {
	ctx context.Context
	h   host.Host
	ps  *pubsub.PubSub

	mu           sync.RWMutex
	currentShard string
	shardTopic   *pubsub.Topic
	shardSub     *pubsub.Subscription

	oldShardName    string
	oldShardTopic   *pubsub.Topic
	oldShardSub     *pubsub.Subscription
	oldShardEndTime time.Time
	inOverlap       bool

	controlTopic *pubsub.Topic
	controlSub   *pubsub.Subscription

	msgCounter    int
	lastPeerCheck time.Time

	shardDone chan struct{}
}

func NewShardManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, startShard string) *ShardManager {
	sm := &ShardManager{
		ctx:          ctx,
		h:            h,
		ps:           ps,
		currentShard: startShard,
		shardDone:    make(chan struct{}, 1),
	}
	sm.joinChannels()
	return sm
}

func (sm *ShardManager) joinChannels() {
	sm.mu.Lock()

	if sm.controlTopic == nil {
		t, err := sm.ps.Join(ControlTopicName)
		if err != nil {
			log.Printf("[Error] Failed to join control topic %s: %v", ControlTopicName, err)
			sm.mu.Unlock()
			return
		}
		sm.controlTopic = t

		sub, err := t.Subscribe()
		if err != nil {
			log.Printf("[Error] Failed to subscribe to control topic: %v", err)
			sm.mu.Unlock()
			return
		}
		sm.controlSub = sub
		log.Printf("[System] Joined Control Channel: %s", ControlTopicName)
	}

	oldShardSub := sm.shardSub
	oldShardTopic := sm.shardTopic
	oldShardName := sm.oldShardName
	oldShardTopicName := ""
	if oldShardSub != nil && oldShardName != "" {
		oldShardTopicName = fmt.Sprintf("dlockss-v2-creative-commons-shard-%s", oldShardName)
	}

	topicName := fmt.Sprintf("dlockss-v2-creative-commons-shard-%s", sm.currentShard)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic %s: %v", topicName, err)
		sm.mu.Unlock()
		return
	}
	sm.shardTopic = t

	sub, err := t.Subscribe()
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic: %v", err)
		sm.mu.Unlock()
		return
	}
	sm.shardSub = sub
	sm.msgCounter = 0

	if oldShardSub != nil && oldShardTopicName != topicName && oldShardTopicName != "" {
		sm.oldShardSub = oldShardSub
		sm.oldShardTopic = oldShardTopic
		sm.oldShardEndTime = time.Now().Add(ShardOverlapDuration)
		sm.inOverlap = true
		sm.mu.Unlock()
		log.Printf("[Sharding] Entering overlap state: keeping old shard %s active until %v", oldShardTopicName, sm.oldShardEndTime)
		go sm.readOldShard()
	} else {
		if oldShardSub != nil {
			oldShardSub.Cancel()
			sm.mu.Unlock()
			select {
			case <-sm.shardDone:
			case <-time.After(2 * time.Second):
				log.Printf("[Warning] Timeout waiting for old shard subscription to close")
			}
		} else {
			sm.mu.Unlock()
		}
		sm.oldShardName = ""
	}

	log.Printf("[Sharding] Active Data Shard: %s (Topic: %s)", sm.currentShard, topicName)
}

func (sm *ShardManager) Run() {
	go sm.readControl()
	go sm.readShard()
	go sm.manageOverlap()
	go sm.runPeerCountChecker()
}

// runPeerCountChecker periodically checks the number of peers in the shard
// and triggers splits when the threshold is exceeded.
func (sm *ShardManager) runPeerCountChecker() {
	ticker := time.NewTicker(ShardPeerCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.checkAndSplitIfNeeded()
		}
	}
}

func (sm *ShardManager) readControl() {
	sub := sm.controlSub
	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(sm.ctx)
		if err != nil {
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		// Control channel uses CBOR DelegateMessage in V2.
		msgType, err := decodeCBORMessageType(msg.Data)
		if err != nil || msgType != schema.MessageTypeDelegate {
			continue
		}

		if !checkRateLimitForMessage(msg.GetFrom(), "DELEGATE") {
			incrementMetric(&metrics.messagesDropped)
			if !canAcceptCustodialFile() {
				usage := checkDiskUsage()
				log.Printf("[Storage] Dropped DELEGATE message from %s (disk usage high: %.1f%%)", msg.GetFrom().String(), usage)
			} else {
				log.Printf("[RateLimit] Dropped DELEGATE message from %s (rate limit exceeded)", msg.GetFrom().String())
			}
			continue
		}

		var dm schema.DelegateMessage
		if err := dm.UnmarshalCBOR(msg.Data); err != nil {
			log.Printf("[Warning] Invalid DelegateMessage CBOR: %v", err)
			continue
		}
		if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), dm.SenderID, dm.Timestamp, dm.Nonce, dm.Sig, dm.MarshalCBORForSigning, "DelegateMessage") {
			continue
		}

		sm.mu.RLock()
		myShard := sm.currentShard
		sm.mu.RUnlock()

		if dm.TargetShard == myShard {
			incrementMetric(&metrics.messagesReceived)
			manifestKey := dm.ManifestCID.String()
			log.Printf("[Delegate] Accepted delegation for %s (I am %s)", manifestKey[:min(16, len(manifestKey))]+"...", myShard)
			addKnownFile(manifestKey)
			if !withReplicationWorker(sm.ctx, func() {
				checkReplication(sm.ctx, manifestKey)
			}) {
				return
			}
		}
	}
}

// handleIngestMessage processes an IngestMessage after CBOR unmarshaling.
// It performs authorization, signature verification, and enqueues replication checking.
// The logPrefix parameter allows customizing log messages (e.g., "IngestMessage" vs "IngestMessage (old shard overlap)").
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, logPrefix string) bool {
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		return false
	}
	key := im.ManifestCID.String()
	incrementMetric(&metrics.messagesReceived)
	if logPrefix != "IngestMessage" {
		log.Printf("[Sharding] Received Ingest message from old shard during overlap: %s", key[:min(16, len(key))]+"...")
	}
	addKnownFile(key)
	if !withReplicationWorker(sm.ctx, func() {
		checkReplication(sm.ctx, key)
	}) {
		return false
	}
	return true
}

// handleReplicationRequest processes a ReplicationRequest after CBOR unmarshaling.
// It performs authorization, signature verification, and enqueues replication checking.
// The logPrefix parameter allows customizing log messages (e.g., "ReplicationRequest" vs "ReplicationRequest (old shard overlap)").
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, logPrefix string) bool {
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		return false
	}
	key := rr.ManifestCID.String()
	incrementMetric(&metrics.messagesReceived)
	if logPrefix != "ReplicationRequest" {
		log.Printf("[Sharding] Received ReplicationRequest from old shard during overlap: %s", key[:min(16, len(key))]+"...")
	}
	addKnownFile(key)
	if !withReplicationWorker(sm.ctx, func() {
		checkReplication(sm.ctx, key)
	}) {
		return false
	}
	return true
}

func (sm *ShardManager) readShard() {
	defer func() {
		select {
		case sm.shardDone <- struct{}{}:
		default:
		}
	}()

	sub := sm.shardSub
	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(sm.ctx)
		if err != nil {
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		if !checkRateLimit(msg.GetFrom()) {
			incrementMetric(&metrics.messagesDropped)
			continue
		}

		sm.mu.Lock()
		sm.msgCounter++
		sm.mu.Unlock()

		msgType, err := decodeCBORMessageType(msg.Data)
		if err != nil {
			continue
		}

		switch msgType {
		case schema.MessageTypeIngest:
			var im schema.IngestMessage
			if err := im.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleIngestMessage(msg, &im, "IngestMessage") {
				return
			}

		case schema.MessageTypeReplicationRequest:
			var rr schema.ReplicationRequest
			if err := rr.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleReplicationRequest(msg, &rr, "ReplicationRequest") {
				return
			}
		}
	}
}

// getShardPeerCount returns the estimated number of peers in the current shard topic.
// This is an estimate because GossipSub only reports peers it knows about.
func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	topic := sm.shardTopic
	sm.mu.RUnlock()

	if topic == nil {
		return 0
	}

	// ListPeers returns peers subscribed to this topic (excluding self)
	peers := topic.ListPeers()
	count := len(peers)
	// Add 1 for self (we're subscribed but not in the list)
	return count + 1
}

func (sm *ShardManager) checkAndSplitIfNeeded() {
	// Rate-limit how often we check to avoid hammering GossipSub APIs.
	sm.mu.Lock()
	now := time.Now()
	if now.Sub(sm.lastPeerCheck) < ShardPeerCheckInterval {
		sm.mu.Unlock()
		return
	}
	sm.lastPeerCheck = now
	currentShard := sm.currentShard
	sm.mu.Unlock()

	peerCount := sm.getShardPeerCount()

	// Estimate peers per shard after split (roughly half)
	estimatedPeersAfterSplit := peerCount / 2

	// Check if we should split:
	// 1. Current shard exceeds max peers
	// 2. After split, each new shard would have at least MinPeersPerShard
	shouldSplit := peerCount > MaxPeersPerShard && estimatedPeersAfterSplit >= MinPeersPerShard

	if shouldSplit {
		log.Printf("[Sharding] Shard %s has %d peers (threshold: %d). Splitting...", currentShard, peerCount, MaxPeersPerShard)
		sm.splitShard()
	} else if peerCount > MaxPeersPerShard {
		log.Printf("[Sharding] Shard %s has %d peers but cannot split (would create shards with < %d peers each)", currentShard, peerCount, MinPeersPerShard)
	} else if peerCount > 0 {
		// Debug logging: show peer count occasionally
		log.Printf("[Sharding] Shard %s peer count: %d/%d (checking every %v)", currentShard, peerCount, MaxPeersPerShard, ShardPeerCheckInterval)
	}
}

func (sm *ShardManager) splitShard() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentDepth := len(sm.currentShard)
	nextDepth := currentDepth + 1

	oldShard := sm.currentShard
	peerIDHash := getBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.currentShard = peerIDHash
	sm.oldShardName = oldShard
	sm.msgCounter = 0

	incrementMetric(&metrics.shardSplits)
	log.Printf("[Sharding] Split shard to depth %d: %s -> %s (entering overlap state)", nextDepth, oldShard, sm.currentShard)

	// Release the lock before rejoining topics to avoid deadlocks; joinChannels
	// will acquire its own locks as needed.
	sm.mu.Unlock()
	sm.joinChannels()
	sm.mu.Lock()
}

func (sm *ShardManager) readOldShard() {
	sm.mu.RLock()
	sub := sm.oldShardSub
	endTime := sm.oldShardEndTime
	sm.mu.RUnlock()

	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		if time.Now().After(endTime) {
			sm.mu.Lock()
			if sm.oldShardSub != nil {
				log.Printf("[Sharding] Overlap period ended, closing old shard subscription")
				sm.oldShardSub.Cancel()
				sm.oldShardSub = nil
				sm.oldShardTopic = nil
				sm.inOverlap = false
			}
			sm.mu.Unlock()
			return
		}

		ctx, cancel := context.WithDeadline(sm.ctx, endTime)
		msg, err := sub.Next(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		if !checkRateLimit(msg.GetFrom()) {
			incrementMetric(&metrics.messagesDropped)
			continue
		}

		msgType, err := decodeCBORMessageType(msg.Data)
		if err != nil {
			continue
		}
		switch msgType {
		case schema.MessageTypeIngest:
			var im schema.IngestMessage
			if err := im.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleIngestMessage(msg, &im, "IngestMessage (old shard overlap)") {
				return
			}
		case schema.MessageTypeReplicationRequest:
			var rr schema.ReplicationRequest
			if err := rr.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleReplicationRequest(msg, &rr, "ReplicationRequest (old shard overlap)") {
				return
			}
		}
	}
}

func (sm *ShardManager) manageOverlap() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.RLock()
			inOverlap := sm.inOverlap
			endTime := sm.oldShardEndTime
			sm.mu.RUnlock()

			if inOverlap && time.Now().After(endTime) {
				sm.mu.Lock()
				if sm.oldShardSub != nil {
					log.Printf("[Sharding] Overlap period expired, closing old shard subscription")
					sm.oldShardSub.Cancel()
					sm.oldShardSub = nil
					sm.oldShardTopic = nil
					sm.oldShardName = ""
					sm.inOverlap = false
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardManager) PublishToShard(msg string) {
	sm.mu.RLock()
	topic := sm.shardTopic
	inOverlap := sm.inOverlap
	oldTopic := sm.oldShardTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, []byte(msg))
	}

	if inOverlap && oldTopic != nil {
		_ = oldTopic.Publish(sm.ctx, []byte(msg))
		log.Printf("[Sharding] Published to both shards during overlap: %s", msg[:min(20, len(msg))])
	}
}

func (sm *ShardManager) PublishToShardCBOR(data []byte) {
	sm.mu.RLock()
	topic := sm.shardTopic
	inOverlap := sm.inOverlap
	oldTopic := sm.oldShardTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, data)
	}
	if inOverlap && oldTopic != nil {
		_ = oldTopic.Publish(sm.ctx, data)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sm *ShardManager) PublishToControl(msg string) {
	sm.mu.RLock()
	topic := sm.controlTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) PublishToControlCBOR(data []byte) {
	sm.mu.RLock()
	topic := sm.controlTopic
	sm.mu.RUnlock()
	if topic != nil {
		_ = topic.Publish(sm.ctx, data)
	}
}

func (sm *ShardManager) AmIResponsibleFor(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	// V2 keys are ManifestCID strings (not 64-hex). Use stable hashing for sharding.
	prefix := getHexBinaryPrefix(keyToStableHex(key), len(sm.currentShard))
	return prefix == sm.currentShard
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

func (sm *ShardManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.shardSub != nil {
		sm.shardSub.Cancel()
	}
	if sm.oldShardSub != nil {
		sm.oldShardSub.Cancel()
	}
	if sm.controlSub != nil {
		sm.controlSub.Cancel()
	}
}
