package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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
			case <-time.After(ShardSubscriptionTimeout):
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
			// Trigger immediate check (high priority for custodial)
			// Note: In async pipeline, we don't have direct access to jobQueue here.
			// We rely on the fact that addKnownFile adds it to the set, and the scheduler will pick it up.
			// To make it faster, we could expose a way to trigger the scheduler or inject a job.
			// For now, we rely on the scheduler loop (every 1m or faster).
			// Ideally, we should inject into the job queue.
			// TODO: Inject into job queue directly for lower latency.
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
	// Scheduler will pick this up.
	return true
}

// handleReplicationRequest processes a ReplicationRequest after CBOR unmarshaling.
// It performs authorization, signature verification, and enqueues replication checking.
// If AutoReplicationEnabled is true and the file is not pinned, it attempts to fetch and pin the file.
// The logPrefix parameter allows customizing log messages (e.g., "ReplicationRequest" vs "ReplicationRequest (old shard overlap)").
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, logPrefix string) bool {
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		return false
	}
	key := rr.ManifestCID.String()
	manifestCID := rr.ManifestCID
	incrementMetric(&metrics.messagesReceived)
	if logPrefix != "ReplicationRequest" {
		log.Printf("[Sharding] Received ReplicationRequest from old shard during overlap: %s", key[:min(16, len(key))]+"...")
	}

	// Attempt automatic replication if enabled and file is not pinned
	if AutoReplicationEnabled && !isPinned(key) {
		log.Printf("[Replication] Received replication request for %s, attempting to replicate...", key[:min(16, len(key))]+"...")

		// Directly replicate (bypass pipeline for immediate action on replication requests)
		go func() {
			success, err := replicateFileFromRequest(sm.ctx, manifestCID, msg.GetFrom())
			if err != nil {
				log.Printf("[Replication] Failed to replicate %s: %v", key[:min(16, len(key))]+"...", err)
				recordFailedOperation(key)
			} else if success {
				log.Printf("[Replication] Successfully replicated %s", key[:min(16, len(key))]+"...")
				clearBackoff(key)
				// File is now replicated, add to known files and let pipeline handle ongoing monitoring
				addKnownFile(key)
			}
		}()
	} else {
		// File already pinned or auto-replication disabled, just ensure it's tracked
		addKnownFile(key)
		// Pipeline will pick up the new file on next scan
	}

	return true
}

// handleUnreplicateRequest processes an UnreplicateRequest message.
// Peers use deterministic selection (hash of ManifestCID + PeerID) to decide
// if they should drop the file, ensuring distributed consensus without coordination.
func (sm *ShardManager) handleUnreplicateRequest(msg *pubsub.Message, ur *schema.UnreplicateRequest, logPrefix string) bool {
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), ur.SenderID, ur.Timestamp, ur.Nonce, ur.Sig, ur.MarshalCBORForSigning, logPrefix) {
		return false
	}
	key := ur.ManifestCID.String()
	manifestCID := ur.ManifestCID
	incrementMetric(&metrics.messagesReceived)

	log.Printf("[Replication] Received UnreplicateRequest for %s (excess: %d, current: %d)", key[:min(16, len(key))]+"...", ur.ExcessCount, ur.CurrentCount)

	// Only act if we're pinning this file
	if !isPinned(key) {
		log.Printf("[Replication] Not pinning %s, ignoring UnreplicateRequest", key[:min(16, len(key))]+"...")
		return true
	}

	// Deterministic selection: hash(ManifestCID + PeerID) to decide if this peer should drop
	// This ensures distributed consensus - all peers independently make the same decision
	selectionKey := key + sm.h.ID().String()
	hash := sha256.Sum256([]byte(selectionKey))
	
	// Use first 8 bytes of hash as a 64-bit integer for selection
	// Select peers where (hash % currentCount) < excessCount
	// This gives us approximately excessCount / currentCount probability per peer
	var hashInt uint64
	for i := 0; i < 8 && i < len(hash); i++ {
		hashInt = (hashInt << 8) | uint64(hash[i])
	}
	
	selected := (hashInt % uint64(ur.CurrentCount)) < uint64(ur.ExcessCount)
	
	if selected {
		log.Printf("[Replication] Selected to drop over-replicated file %s (hash selection)", key[:min(16, len(key))]+"...")
		unpinKeyV2(sm.ctx, key, manifestCID)
		// Keep in knownFiles for monitoring, but unpinned
	} else {
		log.Printf("[Replication] Not selected to drop %s (hash selection)", key[:min(16, len(key))]+"...")
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

		case schema.MessageTypeUnreplicateRequest:
			var ur schema.UnreplicateRequest
			if err := ur.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleUnreplicateRequest(msg, &ur, "UnreplicateRequest") {
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

// getShardInfo returns the current shard ID and the estimated number of peers
// subscribed to the shard topic (including self). It is safe to call from
// other packages without manually locking shardMgr.
func getShardInfo() (string, int) {
	if shardMgr == nil {
		return "", 0
	}

	shardMgr.mu.RLock()
	topic := shardMgr.shardTopic
	currentShard := shardMgr.currentShard
	shardMgr.mu.RUnlock()

	if topic == nil {
		return currentShard, 0
	}

	peers := topic.ListPeers()
	return currentShard, len(peers) + 1
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
	// 1. Current shard exceeds max peers (tied to replication requirements)
	// 2. After split, each new shard would have at least MinPeersPerShard nodes
	// This ensures replication targets remain achievable after splits
	shouldSplit := peerCount > MaxPeersPerShard && estimatedPeersAfterSplit >= MinPeersPerShard

	if shouldSplit {
		log.Printf("[Sharding] Shard %s has %d peers (threshold: %d). Splitting to ensure replication targets remain achievable...", currentShard, peerCount, MaxPeersPerShard)
		sm.splitShard()
	} else if peerCount > MaxPeersPerShard {
		log.Printf("[Sharding] Shard %s has %d peers but cannot split (would create shards with < %d peers each, risking replication failures)", currentShard, peerCount, MinPeersPerShard)
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

	// Schedule a reshard pass to re-evaluate responsibility for existing files
	// at the new shard depth. Delay slightly to allow overlap state to settle.
	go func() {
		time.Sleep(ReshardDelay)
		sm.RunReshardPass()
	}()
}

// RunReshardPass walks knownFiles and re-evaluates responsibility for each
// ManifestCID at the new shard depth after a split. It emits DelegateMessages
// or IngestMessages as needed so that content is gradually redistributed to
// the correct shards.
func (sm *ShardManager) RunReshardPass() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	oldShard := sm.oldShardName
	ctx := sm.ctx
	sm.mu.RUnlock()

	if ctx == nil {
		return
	}
	if oldShard == "" {
		// No previous shard recorded; nothing to do.
		return
	}

	newDepth := len(currentShard)
	oldDepth := len(oldShard)
	if newDepth <= oldDepth {
		// Depth did not actually increase; nothing to do.
		return
	}

	files := knownFiles.All()
	if len(files) == 0 {
		return
	}

	log.Printf("[Reshard] Starting reshard pass for shard %s (old shard %s), files=%d", currentShard, oldShard, len(files))

	batchCount := 0

	for key := range files {
		if ctx.Err() != nil {
			log.Printf("[Reshard] Context canceled, stopping reshard pass")
			return
		}

		// Skip files we've already processed in a previous reshard pass.
		if reshardedFiles.Has(key) {
			continue
		}

		// Use PayloadCID for shard assignment (stable, content-based)
		// ManifestCID includes timestamp/metadata and changes on every ingestion
		payloadCIDStr := getPayloadCIDForShardAssignment(sm.ctx, key)
		stableHex := keyToStableHex(payloadCIDStr)
		targetOld := getHexBinaryPrefix(stableHex, oldDepth)
		targetNew := getHexBinaryPrefix(stableHex, newDepth)

		wasResponsible := (targetOld == oldShard)
		isResponsible := (targetNew == currentShard)

		// If responsibility didn't change with the new depth, just mark as processed.
		if wasResponsible == isResponsible {
			reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := keyToCID(key)
		if err != nil {
			log.Printf("[Reshard] Invalid key format during reshard: %s, error: %v", key, err)
			reshardedFiles.Add(key)
			continue
		}

		shortKey := key
		if len(shortKey) > 16 {
			shortKey = shortKey[:16] + "..."
		}

		switch {
		// Case B: Node was responsible, now not responsible -> become custodial and delegate.
		case wasResponsible && !isResponsible:
			log.Printf("[Reshard] %s moved off my shard (%s -> %s). Delegating to shard %s", shortKey, oldShard, currentShard, targetNew)
			dm := schema.DelegateMessage{
				Type:        schema.MessageTypeDelegate,
				ManifestCID: manifestCID,
				TargetShard: targetNew,
			}
			if err := signProtocolMessage(&dm); err != nil {
				log.Printf("[Reshard] Failed to sign DelegateMessage for %s: %v", shortKey, err)
			} else if b, err := dm.MarshalCBOR(); err != nil {
				log.Printf("[Reshard] Failed to marshal DelegateMessage for %s: %v", shortKey, err)
			} else {
				sm.PublishToControlCBOR(b)
			}

		// Case C: Node was custodial (or uninvolved), now responsible -> announce ingest.
		case !wasResponsible && isResponsible:
			log.Printf("[Reshard] %s moved onto my shard (%s -> %s). Announcing ingest.", shortKey, oldShard, currentShard)
			im := schema.IngestMessage{
				Type:        schema.MessageTypeIngest,
				ManifestCID: manifestCID,
				ShardID:     currentShard,
				// We don't know the original size hint here; use 0 as a neutral value.
				HintSize: 0,
			}
			if err := signProtocolMessage(&im); err != nil {
				log.Printf("[Reshard] Failed to sign IngestMessage for %s: %v", shortKey, err)
			} else if b, err := im.MarshalCBOR(); err != nil {
				log.Printf("[Reshard] Failed to marshal IngestMessage for %s: %v", shortKey, err)
			} else {
				sm.PublishToShardCBOR(b)
			}
		}

		reshardedFiles.Add(key)
		batchCount++

		if batchCount >= ReshardBatchSize {
			// Yield to avoid overwhelming the network and CPU.
			time.Sleep(50 * time.Millisecond)
			batchCount = 0
		}
	}

	log.Printf("[Reshard] Completed reshard pass for shard %s", currentShard)
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

		case schema.MessageTypeUnreplicateRequest:
			var ur schema.UnreplicateRequest
			if err := ur.UnmarshalCBOR(msg.Data); err != nil {
				continue
			}
			if !sm.handleUnreplicateRequest(msg, &ur, "UnreplicateRequest (old shard overlap)") {
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
		if err := topic.Publish(sm.ctx, []byte(msg)); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish to shard topic: %v", err)
		}
	}

	if inOverlap && oldTopic != nil {
		if err := oldTopic.Publish(sm.ctx, []byte(msg)); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish to old shard topic during overlap: %v", err)
		} else {
			log.Printf("[Sharding] Published to both shards during overlap: %s", msg[:min(20, len(msg))])
		}
	}
}

func (sm *ShardManager) PublishToShardCBOR(data []byte) {
	sm.mu.RLock()
	topic := sm.shardTopic
	inOverlap := sm.inOverlap
	oldTopic := sm.oldShardTopic
	sm.mu.RUnlock()

	if topic != nil {
		if err := topic.Publish(sm.ctx, data); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish CBOR to shard topic: %v", err)
		}
	}
	if inOverlap && oldTopic != nil {
		if err := oldTopic.Publish(sm.ctx, data); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish CBOR to old shard topic during overlap: %v", err)
		}
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
		if err := topic.Publish(sm.ctx, []byte(msg)); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish to control topic: %v", err)
		}
	}
}

func (sm *ShardManager) PublishToControlCBOR(data []byte) {
	sm.mu.RLock()
	topic := sm.controlTopic
	sm.mu.RUnlock()
	if topic != nil {
		if err := topic.Publish(sm.ctx, data); err != nil {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[PubSub] Failed to publish CBOR to control topic: %v", err)
		}
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
