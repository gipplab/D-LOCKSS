package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

type shardSubscription struct {
	topic    *pubsub.Topic
	sub      *pubsub.Subscription
	refCount int
	cancel   context.CancelFunc
	shardID  string
}

type ShardManager struct {
	ctx            context.Context
	h              host.Host
	ps             *pubsub.PubSub
	ipfsClient     ipfs.IPFSClient
	storageMgr     *StorageManager
	replicationMgr *ReplicationManager
	reshardedFiles *KnownFiles

	mu           sync.RWMutex
	currentShard string
	
	// Map of shardID -> subscription
	shardSubs map[string]*shardSubscription

	// Old shard overlap management (still useful for split transitions)
	// We handle this by simply 'holding' a reference to the old shard for a duration.
	// No special fields needed, just logic in splitShard.

	msgCounter    int
	lastPeerCheck time.Time
}

func NewShardManager(
	ctx context.Context, 
	h host.Host, 
	ps *pubsub.PubSub, 
	ipfsClient ipfs.IPFSClient,
	stm *StorageManager,
	startShard string,
) *ShardManager {
	sm := &ShardManager{
		ctx:            ctx,
		h:              h,
		ps:             ps,
		ipfsClient:     ipfsClient,
		storageMgr:     stm,
		reshardedFiles: NewKnownFiles(),
		currentShard:   startShard,
		shardSubs:      make(map[string]*shardSubscription),
	}
	
	// Join initial shard
	sm.JoinShard(startShard)
	
	return sm
}

func (sm *ShardManager) SetReplicationManager(rm *ReplicationManager) {
	sm.replicationMgr = rm
}

func (sm *ShardManager) Run() {
	go sm.runPeerCountChecker()
}

// JoinShard increments the reference count for a shard topic.
// If the topic is not currently subscribed, it joins and starts a read loop.
func (sm *ShardManager) JoinShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if exists {
		sub.refCount++
		log.Printf("[Sharding] Retained shard %s (refCount: %d)", shardID, sub.refCount)
		return
	}

	// New subscription
	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		log.Printf("[Error] Failed to join shard topic %s: %v", topicName, err)
		return
	}

	psSub, err := t.Subscribe()
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic %s: %v", topicName, err)
		return
	}

	ctx, cancel := context.WithCancel(sm.ctx)
	newSub := &shardSubscription{
		topic:    t,
		sub:      psSub,
		refCount: 1,
		cancel:   cancel,
		shardID:  shardID,
	}
	sm.shardSubs[shardID] = newSub

	log.Printf("[Sharding] Joined shard %s (Topic: %s)", shardID, topicName)

	// Start read loop
	go sm.readLoop(ctx, newSub)
}

// LeaveShard decrements the reference count for a shard topic.
// If the count reaches zero, it unsubscribes and closes the topic.
func (sm *ShardManager) LeaveShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if !exists {
		return
	}

	sub.refCount--
	log.Printf("[Sharding] Released shard %s (refCount: %d)", shardID, sub.refCount)

	if sub.refCount <= 0 {
		sub.cancel() // Stop read loop
		sub.sub.Cancel()
		sub.topic.Close()
		delete(sm.shardSubs, shardID)
		log.Printf("[Sharding] Left shard %s", shardID)
	}
}

func (sm *ShardManager) readLoop(ctx context.Context, sub *shardSubscription) {
	defer sub.sub.Cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := sub.sub.Next(ctx)
		if err != nil {
			return
		}

		sm.processMessage(msg, sub.shardID)
	}
}

// runPeerCountChecker periodically checks the number of peers in the CURRENT shard
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

// processMessage decodes and dispatches a message to the appropriate handler.
func (sm *ShardManager) processMessage(msg *pubsub.Message, shardID string) {
	// Self-check
	if msg.GetFrom() == sm.h.ID() {
		return
	}

	// Rate limit check
	if !checkRateLimit(msg.GetFrom()) {
		incrementMetric(&metrics.messagesDropped)
		log.Printf("[Shard] Dropped message from %s due to rate limiting (shard %s)", msg.GetFrom().String()[:12], shardID)
		return
	}

	sm.mu.Lock()
	sm.msgCounter++
	sm.mu.Unlock()

	msgType, err := decodeCBORMessageType(msg.Data)
	if err != nil {
		log.Printf("[Shard] Failed to decode message type from %s in shard %s: %v", msg.GetFrom().String()[:12], shardID, err)
		return
	}

	switch msgType {
	case schema.MessageTypeIngest:
		var im schema.IngestMessage
		if err := im.UnmarshalCBOR(msg.Data); err != nil {
			log.Printf("[Shard] Failed to unmarshal IngestMessage from %s in shard %s: %v", msg.GetFrom().String()[:12], shardID, err)
			return
		}
		sm.handleIngestMessage(msg, &im, shardID)

	case schema.MessageTypeReplicationRequest:
		var rr schema.ReplicationRequest
		if err := rr.UnmarshalCBOR(msg.Data); err != nil {
			return
		}
		sm.handleReplicationRequest(msg, &rr, shardID)

	case schema.MessageTypeUnreplicateRequest:
		var ur schema.UnreplicateRequest
		if err := ur.UnmarshalCBOR(msg.Data); err != nil {
			return
		}
		sm.handleUnreplicateRequest(msg, &ur, shardID)
	}
}

// handleIngestMessage processes an IngestMessage.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped IngestMessage from %s in shard %s due to verification failure", msg.GetFrom().String()[:12], shardID)
		return
	}
	key := im.ManifestCID.String()
	incrementMetric(&metrics.messagesReceived)
	
	// Add to known files. If we are just a tourist, we still "know" about it,
	// but the replication manager will eventually decide if we stick around.
	sm.storageMgr.addKnownFile(key)
	log.Printf("[Shard] Added file to known files: %s (from %s, shard %s)", truncateCID(key, 16), msg.GetFrom().String()[:12], shardID)
}

// handleReplicationRequest processes a ReplicationRequest.
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, shardID string) {
	logPrefix := fmt.Sprintf("ReplicationRequest (Shard %s)", shardID)
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		return
	}
	key := rr.ManifestCID.String()
	manifestCID := rr.ManifestCID
	incrementMetric(&metrics.messagesReceived)

	// Check responsibility
	payloadCIDStr := getPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
	responsible := sm.AmIResponsibleFor(payloadCIDStr)
	
	if !responsible {
		// If we are a "Tourist" (Custodial Node watching its upload), we might see this.
		// But tourists don't replicate other people's files.
		// However, if WE sent the request (unlikely as we filter self), or if another node is requesting,
		// we ignore it unless we are responsible.
		return
	}

	// Attempt automatic replication if enabled and file is not pinned
	if AutoReplicationEnabled && !sm.storageMgr.isPinned(key) {
		// XOR distance-based selection (IPFS/Kademlia style)
		// Select the MinReplication nodes with smallest XOR distance to the content
		xorDistance, err := calculateXORDistance(sm.h.ID(), manifestCID)
		if err != nil {
			log.Printf("[Replication] Failed to calculate XOR distance for %s: %v", key[:min(16, len(key))]+"...", err)
			sm.storageMgr.addKnownFile(key)
			return
		}
		
		peersInShard := sm.getShardPeerCount()
		if peersInShard == 0 {
			peersInShard = MinReplication // Fallback
		}
		
		// Simple approach: Use XOR distance modulo to select nodes
		// Nodes with smaller distance mod values are "closer" and should replicate
		// We select nodes where (distance % peersInShard) < MinReplication
		// This ensures roughly MinReplication nodes participate per file
		distanceMod := new(big.Int).Mod(xorDistance, big.NewInt(int64(peersInShard)))
		modValue := int(distanceMod.Int64())
		
		selected := modValue < MinReplication
		
		if !selected {
			log.Printf("[Replication] Not selected to replicate %s (XOR distance mod %d/%d, need < %d, distance: %s)", 
				key[:min(16, len(key))]+"...", modValue, peersInShard, MinReplication, truncateBigInt(xorDistance, 16))
			sm.storageMgr.addKnownFile(key)
			return
		}
		
		log.Printf("[Replication] Selected to replicate %s (XOR distance mod %d/%d, distance: %s)", 
			key[:min(16, len(key))]+"...", modValue, peersInShard, truncateBigInt(xorDistance, 16))
		

		go func() {
			if sm.replicationMgr == nil {
				return
			}
			success, err := sm.replicationMgr.replicateFileFromRequest(sm.ctx, manifestCID, msg.GetFrom(), true) // responsible=true
			if err != nil {
				log.Printf("[Replication] Failed to replicate %s: %v", key[:min(16, len(key))]+"...", err)
				sm.storageMgr.recordFailedOperation(key)
			} else if success {
				log.Printf("[Replication] Successfully replicated %s", key[:min(16, len(key))]+"...")
				sm.storageMgr.failedOperations.Clear(key)
				sm.storageMgr.addKnownFile(key)
			}
		}()
	} else {
		sm.storageMgr.addKnownFile(key)
	}
}

// handleUnreplicateRequest processes an UnreplicateRequest.
func (sm *ShardManager) handleUnreplicateRequest(msg *pubsub.Message, ur *schema.UnreplicateRequest, shardID string) {
	logPrefix := fmt.Sprintf("UnreplicateRequest (Shard %s)", shardID)
	if verifyAndAuthorizeMessage(sm.h, msg.GetFrom(), ur.SenderID, ur.Timestamp, ur.Nonce, ur.Sig, ur.MarshalCBORForSigning, logPrefix) {
		return
	}
	key := ur.ManifestCID.String()
	manifestCID := ur.ManifestCID
	incrementMetric(&metrics.messagesReceived)

	if !sm.storageMgr.isPinned(key) {
		return
	}

	// Deterministic selection to drop
	selectionKey := key + sm.h.ID().String()
	hash := sha256.Sum256([]byte(selectionKey))
	var hashInt uint64
	for i := 0; i < 8 && i < len(hash); i++ {
		hashInt = (hashInt << 8) | uint64(hash[i])
	}
	
	selected := (hashInt % uint64(ur.CurrentCount)) < uint64(ur.ExcessCount)
	
	if selected {
		log.Printf("[Replication] Selected to drop over-replicated file %s", key[:min(16, len(key))]+"...")
		if sm.replicationMgr != nil {
			sm.replicationMgr.UnpinFile(sm.ctx, key, manifestCID)
		}
	}
}

// getShardPeerCount returns peer count for the CURRENT shard.
func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}

	peers := sub.topic.ListPeers()
	return len(peers) + 1
}

// getShardInfo returns current shard ID and peer count.
func getShardInfo() (string, int) {
	if shardMgr == nil {
		return "", 0
	}
	// Note: Avoiding global lock for peek
	return shardMgr.currentShard, shardMgr.getShardPeerCount()
}

// calculateXORDistance computes the XOR distance between a peer ID and a CID.
// This follows IPFS/Kademlia DHT conventions for determining content locality.
// Returns the XOR distance as a big.Int for comparison.
func calculateXORDistance(peerID peer.ID, contentCID cid.Cid) (*big.Int, error) {
	// Extract raw hash bytes from peer ID
	// Peer IDs in libp2p are multihash-encoded, so we need to decode
	peerIDBytes := []byte(peerID)
	peerMh, err := multihash.Decode(peerIDBytes)
	if err != nil {
		// If decoding fails, try using the bytes directly (might be identity hash)
		peerHash := peerIDBytes
		// Try to extract hash from CID
		cidMh := contentCID.Hash()
		cidMhDecoded, err := multihash.Decode(cidMh)
		if err != nil {
			return nil, fmt.Errorf("decode CID multihash: %w", err)
		}
		contentHash := cidMhDecoded.Digest
		
		// XOR with minimum length
		minLen := len(peerHash)
		if len(contentHash) < minLen {
			minLen = len(contentHash)
		}
		xorResult := make([]byte, minLen)
		for i := 0; i < minLen; i++ {
			xorResult[i] = peerHash[i] ^ contentHash[i]
		}
		return new(big.Int).SetBytes(xorResult), nil
	}
	peerHash := peerMh.Digest
	
	// Extract raw hash bytes from CID (multihash)
	cidMh := contentCID.Hash()
	cidMhDecoded, err := multihash.Decode(cidMh)
	if err != nil {
		return nil, fmt.Errorf("decode CID multihash: %w", err)
	}
	contentHash := cidMhDecoded.Digest
	
	// Ensure both hashes are the same length (pad with zeros if needed)
	maxLen := len(peerHash)
	if len(contentHash) > maxLen {
		maxLen = len(contentHash)
	}
	
	peerPadded := make([]byte, maxLen)
	copy(peerPadded[maxLen-len(peerHash):], peerHash)
	
	contentPadded := make([]byte, maxLen)
	copy(contentPadded[maxLen-len(contentHash):], contentHash)
	
	// Calculate XOR distance byte by byte
	xorResult := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		xorResult[i] = peerPadded[i] ^ contentPadded[i]
	}
	
	// Convert to big.Int for comparison
	return new(big.Int).SetBytes(xorResult), nil
}

// truncateBigInt truncates a big.Int's string representation for logging.
func truncateBigInt(n *big.Int, maxLen int) string {
	str := n.String()
	if len(str) <= maxLen {
		return str
	}
	return str[:maxLen] + "..."
}

func (sm *ShardManager) checkAndSplitIfNeeded() {
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
	estimatedPeersAfterSplit := peerCount / 2
	shouldSplit := peerCount > MaxPeersPerShard && estimatedPeersAfterSplit >= MinPeersPerShard

	if shouldSplit {
		log.Printf("[Sharding] Shard %s has %d peers. Splitting...", currentShard, peerCount)
		sm.splitShard()
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
	sm.msgCounter = 0

	incrementMetric(&metrics.shardSplits)
	log.Printf("[Sharding] Split shard to depth %d: %s -> %s", nextDepth, oldShard, sm.currentShard)

	// Release lock to join/leave (avoid deadlock)
	sm.mu.Unlock()
	
	// Join new shard
	sm.JoinShard(peerIDHash)
	
	// Keep old shard for overlap duration, then leave
	go func() {
		time.Sleep(ShardOverlapDuration)
		sm.LeaveShard(oldShard)
	}()
	
	sm.mu.Lock()

	go func() {
		time.Sleep(ReshardDelay)
		sm.RunReshardPass(oldShard, peerIDHash)
	}()
}

// RunReshardPass re-evaluates responsibility.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.knownFiles.All()
	if len(files) == 0 {
		return
	}

	log.Printf("[Reshard] Starting reshard pass: %s -> %s", oldShard, newShard)
	oldDepth := len(oldShard)
	newDepth := len(newShard)

	for key := range files {
		if sm.reshardedFiles.Has(key) {
			continue
		}

		payloadCIDStr := getPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := keyToStableHex(payloadCIDStr)
		targetOld := getHexBinaryPrefix(stableHex, oldDepth)
		targetNew := getHexBinaryPrefix(stableHex, newDepth)

		wasResponsible := (targetOld == oldShard)
		isResponsible := (targetNew == newShard)

		if wasResponsible == isResponsible {
			sm.reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := keyToCID(key)
		if err != nil {
			continue
		}

		// Logic simplified: We just announce to the new shard if we are now responsible.
		// If we are NO LONGER responsible, we are "Custodial". We should check if we can hand off.
		// But in the new "Tourist" model, we rely on the network to pick it up.
		// If we are responsible now, we should announce Ingest to our new shard so others know.
		
		if isResponsible {
			im := schema.IngestMessage{
				Type:        schema.MessageTypeIngest,
				ManifestCID: manifestCID,
				ShardID:     newShard,
				HintSize:    0,
			}
			if err := signProtocolMessage(&im); err == nil {
				if b, err := im.MarshalCBOR(); err == nil {
					sm.PublishToShardCBOR(b, newShard)
				}
			}
		} else if wasResponsible {
			// We lost responsibility. We effectively become custodial.
			// We should "Join" the target shard as a tourist to ensure handoff?
			// Or just let the new responsible nodes find it via DHT?
			// Providing to DHT is enough if nodes query.
			// But for proactive handoff, we could Join(targetNew) -> Announce -> Wait -> Leave.
			// For simplicity in this pass, we skip proactive handoff logic for now.
		}

		sm.reshardedFiles.Add(key)
		time.Sleep(10 * time.Millisecond)
	}
}

// PublishToShard publishes a message to a specific shard topic.
func (sm *ShardManager) PublishToShard(shardID, msg string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		sub.topic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) PublishToShardCBOR(data []byte, shardID string) {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		sub.topic.Publish(sm.ctx, data)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sm *ShardManager) AmIResponsibleFor(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
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

	for _, sub := range sm.shardSubs {
		sub.cancel()
		sub.sub.Cancel()
	}
}
