package shard

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/clusters"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/internal/telemetry"
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
	storageMgr     *storage.StorageManager
	clusterMgr     *clusters.ClusterManager // NEW: Cluster Manager
	metrics        *telemetry.MetricsManager
	signer         *signing.Signer
	reshardedFiles *common.KnownFiles
	rateLimiter    *common.RateLimiter

	mu           sync.RWMutex
	currentShard string

	// Map of shardID -> subscription
	shardSubs map[string]*shardSubscription

	// Old shard overlap management (still useful for split transitions)
	// We handle this by simply 'holding' a reference to the old shard for a duration.
	// No special fields needed, just logic in splitShard.

	msgCounter         int
	lastPeerCheck      time.Time
	lastDiscoveryCheck time.Time
	lastMessageTime    time.Time // Track when we last received a message to detect idle state

	// Track unique peers seen via messages (for more accurate counting)
	seenPeers map[string]map[peer.ID]time.Time // shardID -> peerID -> last seen
}

func NewShardManager(
	ctx context.Context,
	h host.Host,
	ps *pubsub.PubSub,
	ipfsClient ipfs.IPFSClient,
	stm *storage.StorageManager,
	metrics *telemetry.MetricsManager,
	signer *signing.Signer,
	rateLimiter *common.RateLimiter,
	ds datastore.Datastore,
	dht routing.Routing,
	startShard string,
) *ShardManager {
	sm := &ShardManager{
		ctx:            ctx,
		h:              h,
		ps:             ps,
		ipfsClient:     ipfsClient,
		storageMgr:     stm,
		clusterMgr:     clusters.NewClusterManager(h, ps, dht, ds, ipfsClient), // Initialize Cluster Manager with proper args
		metrics:        metrics,
		signer:         signer,
		rateLimiter:    rateLimiter,
		reshardedFiles: common.NewKnownFiles(),
		currentShard:   startShard,
		shardSubs:      make(map[string]*shardSubscription),
		seenPeers:      make(map[string]map[peer.ID]time.Time),
	}

	// Initialize cluster for start shard
	if err := sm.clusterMgr.JoinShard(ctx, startShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for start shard %s: %v", startShard, err)
	}

	// Join initial shard
	sm.JoinShard(startShard)

	return sm
}

func (sm *ShardManager) Run() {
	go sm.runPeerCountChecker()
	go sm.runHeartbeat()
	go sm.runShardDiscovery()
}

// JoinShard increments the reference count for a shard topic.
// If the topic is not currently subscribed, it joins and starts a read loop.
func (sm *ShardManager) JoinShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if exists {
		sub.refCount++
		// log.Printf("[Sharding] Retained shard %s (refCount: %d)", shardID, sub.refCount)
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
	// log.Printf("[Sharding] Released shard %s (refCount: %d)", shardID, sub.refCount)

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
	ticker := time.NewTicker(config.ShardPeerCheckInterval)
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

// runHeartbeat periodically sends heartbeat messages to the current shard topic
// to help peers discover each other for accurate peer counting.
func (sm *ShardManager) runHeartbeat() {
	// Heartbeat interval: use configured value, or auto-calculate from peer check interval
	var heartbeatInterval time.Duration
	if config.HeartbeatInterval > 0 {
		heartbeatInterval = config.HeartbeatInterval
	} else {
		// Auto-calculate: 1/3 of the peer check interval, but no less than 10 seconds
		heartbeatInterval = config.ShardPeerCheckInterval / 3
		if heartbeatInterval < 10*time.Second {
			heartbeatInterval = 10 * time.Second
		}
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a heartbeat message to the current shard topic
func (sm *ShardManager) sendHeartbeat() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return
	}

	// Send lightweight heartbeat message for peer discovery.
	// Format: HEARTBEAT:<PeerID>:<PinnedCount>
	// When IPFS_PATH is set, D-LOCKSS uses the IPFS repo identity so there is one peer ID per node.
	pinnedCount := 0
	if sm.storageMgr != nil {
		pinnedCount = sm.storageMgr.GetPinnedCount()
	}
	heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d", sm.h.ID().String(), pinnedCount))
	if err := sub.topic.Publish(sm.ctx, heartbeatMsg); err != nil {
		// Don't log every failure to avoid spam, but log occasionally
		// Heartbeat failures are not critical
		return
	}
	if config.VerboseLogging {
		log.Printf("[Heartbeat] sent to shard %s (pinned: %d)", currentShard, pinnedCount)
	}

	// Also rotate through pinned files and announce a BATCH per heartbeat interval
	// This ensures new peers eventually learn about old files without full graph traversal.
	// We announce up to 20 files per heartbeat.
	sm.announcePinnedFilesBatch(sub.topic, 20)
}

func (sm *ShardManager) announcePinnedFilesBatch(topic *pubsub.Topic, batchSize int) {
	if sm.storageMgr == nil {
		return
	}

	for i := 0; i < batchSize; i++ {
		// Get next file to announce from storage manager
		key := sm.storageMgr.GetNextFileToAnnounce()
		if key == "" {
			return
		}

		// Lightweight announcement format: PINNED:<ManifestCID>
		msg := []byte(fmt.Sprintf("PINNED:%s", key))
		_ = topic.Publish(sm.ctx, msg)
	}
}

// processMessage decodes and dispatches a message to the appropriate handler.
func (sm *ShardManager) processMessage(msg *pubsub.Message, shardID string) {
	// Self-check
	if msg.GetFrom() == sm.h.ID() {
		return
	}

	// Track peer in current shard (do this early for all messages, including heartbeats)
	sm.mu.Lock()
	if sm.seenPeers[shardID] == nil {
		sm.seenPeers[shardID] = make(map[peer.ID]time.Time)
	}
	sm.seenPeers[shardID][msg.GetFrom()] = time.Now()
	sm.lastMessageTime = time.Now() // Track activity for idle detection
	sm.mu.Unlock()

	// Check for heartbeat message (lightweight text message for peer discovery)
	if len(msg.Data) > 0 {
		// Ignore JSON telemetry messages (start with '{') which cause CBOR decoding errors
		if msg.Data[0] == '{' {
			return
		}

		if string(msg.Data[:min(10, len(msg.Data))]) == "HEARTBEAT:" {
			// Heartbeat message - just track the peer, no further processing needed
			// Format: HEARTBEAT:<PeerID>:<PinnedCount>
			return
		}

		// Format: PINNED:<ManifestCID>
		if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
			key := string(msg.Data[7:])
			// Legacy PINNED message handling removed (replicationMgr was nil check before anyway)
			sm.storageMgr.AddKnownFile(key)
			// No longer trigger replication from here; Cluster handles it.
			return
		}
	}

	// Rate limit check (skip for heartbeats, but apply to protocol messages)
	if sm.rateLimiter != nil && !sm.rateLimiter.Check(msg.GetFrom()) {
		sm.metrics.IncrementMessagesDropped()
		// log.Printf("[Shard] Dropped message from %s due to rate limiting (shard %s)", msg.GetFrom().String()[:12], shardID)
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
	}
}

// handleIngestMessage processes an IngestMessage.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if sm.signer.VerifyAndAuthorizeMessage(msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped IngestMessage from %s in shard %s due to verification failure", msg.GetFrom().String()[:12], shardID)
		return
	}
	key := im.ManifestCID.String()
	sm.metrics.IncrementMessagesReceived()

	// Add to known files. If we are just a tourist, we still "know" about it.
	sm.storageMgr.AddKnownFile(key)

	// Check if we are responsible for this file
	payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
	if sm.AmIResponsibleFor(payloadCIDStr) {
		// Pin to cluster
		// This replaces the old tryReplicateFromAnnouncement logic
		// We use -1 for replication factor to mean "Cluster Default" (which is effectively All for CRDT mode without allocator)
		if err := sm.clusterMgr.Pin(sm.ctx, shardID, im.ManifestCID, -1, -1); err != nil {
			log.Printf("[Shard] Failed to pin ingested file %s to cluster: %v", key, err)
		} else {
			log.Printf("[Shard] Automatically pinned ingested file %s to cluster %s", key, shardID)
		}
	}
}

// getShardPeerCount returns peer count for the CURRENT shard.
// Counts only peers that have sent a message (HEARTBEAT or protocol) in the last 2 minutes,
// so monitor-only subscribers (which do not publish) are not counted and cannot trigger
// premature splits or migration.
func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}

	meshPeers := sub.topic.ListPeers()
	meshCount := len(meshPeers) + 1 // +1 for self

	sm.mu.RLock()
	seenCount := 0
	if seenMap, exists := sm.seenPeers[currentShard]; exists {
		cutoff := time.Now().Add(-2 * time.Minute)
		for _, lastSeen := range seenMap {
			if lastSeen.After(cutoff) {
				seenCount++
			}
		}
		seenCount++ // Include self
	}
	sm.mu.RUnlock()

	// Prefer seen count so we don't count silent mesh peers (e.g. monitor).
	// Use mesh only when we have no seen data yet (fresh shard / mesh forming).
	if seenCount > 0 {
		return seenCount
	}
	return meshCount
}

// GetShardInfo returns current shard ID and peer count.
func (sm *ShardManager) GetShardInfo() (string, int) {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return currentShard, sm.getShardPeerCount()
}

func (sm *ShardManager) GetHost() host.Host {
	return sm.h
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

// checkAndSplitIfNeeded periodically checks the number of peers in the CURRENT shard
func (sm *ShardManager) checkAndSplitIfNeeded() {
	sm.mu.Lock()
	now := time.Now()
	if now.Sub(sm.lastPeerCheck) < config.ShardPeerCheckInterval {
		sm.mu.Unlock()
		return
	}
	sm.lastPeerCheck = now
	currentShard := sm.currentShard
	sm.mu.Unlock()

	peerCount := sm.getShardPeerCount()
	estimatedPeersAfterSplit := peerCount / 2
	shouldSplit := peerCount > config.MaxPeersPerShard && estimatedPeersAfterSplit >= config.MinPeersPerShard

	// Log peer count for debugging
	if currentShard == "" && config.VerboseLogging {
		log.Printf("[Sharding] Root shard peer count: %d (threshold: %d, min after split: %d)",
			peerCount, config.MaxPeersPerShard, config.MinPeersPerShard)
	}

	if shouldSplit {
		log.Printf("[Sharding] Shard %s has %d peers. Splitting...", currentShard, peerCount)
		sm.splitShard()
	} else if peerCount > config.MaxPeersPerShard && config.VerboseLogging {
		// Log why split didn't happen
		log.Printf("[Sharding] Shard %s has %d peers (exceeds threshold %d) but won't split: estimated after split (%d) < minimum (%d)",
			currentShard, peerCount, config.MaxPeersPerShard, estimatedPeersAfterSplit, config.MinPeersPerShard)
	}
}

func (sm *ShardManager) splitShard() {
	sm.mu.Lock()
	// No defer, handle manually to avoid deadlock with metrics and confusion with mid-function unlocks

	currentDepth := len(sm.currentShard)
	nextDepth := currentDepth + 1

	oldShard := sm.currentShard
	peerIDHash := common.GetBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.currentShard = peerIDHash
	sm.msgCounter = 0

	// Update local state is done, unlock before calling external systems
	sm.mu.Unlock()

	// Increment metrics outside the lock to avoid deadlock (MetricsManager might try to acquire ShardManager lock)
	sm.metrics.IncrementShardSplits()
	log.Printf("[Sharding] Split shard to depth %d: %s -> %s", nextDepth, oldShard, peerIDHash)

	// Join new shard
	sm.JoinShard(peerIDHash)

	// Initialize new cluster for the new shard (Dual-Homing)
	if err := sm.clusterMgr.JoinShard(sm.ctx, peerIDHash, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for new shard %s: %v", peerIDHash, err)
	}

	// Trigger migration from old cluster to new cluster
	go func() {
		if err := sm.clusterMgr.MigratePins(sm.ctx, oldShard, peerIDHash); err != nil {
			log.Printf("[Sharding] Migration failed: %v", err)
		}
	}()

	// Keep old shard for overlap duration, then leave
	go func() {
		time.Sleep(config.ShardOverlapDuration)
		sm.LeaveShard(oldShard)

		// Leave the old cluster as well
		if err := sm.clusterMgr.LeaveShard(oldShard); err != nil {
			log.Printf("[Sharding] Failed to leave old cluster %s: %v", oldShard, err)
		}
	}()

	// Start reshard pass after a short delay
	go func() {
		time.Sleep(config.ReshardDelay)
		sm.RunReshardPass(oldShard, peerIDHash)
	}()
}

// RunReshardPass re-evaluates responsibility.
func (sm *ShardManager) RunReshardPass(oldShard, newShard string) {
	files := sm.storageMgr.GetKnownFiles().All()
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

		payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetOld := common.GetHexBinaryPrefix(stableHex, oldDepth)
		targetNew := common.GetHexBinaryPrefix(stableHex, newDepth)

		wasResponsible := (targetOld == oldShard)
		isResponsible := (targetNew == newShard)

		if wasResponsible == isResponsible {
			sm.reshardedFiles.Add(key)
			continue
		}

		manifestCID, err := common.KeyToCID(key)
		if err != nil {
			continue
		}

		// Logic simplified: We just announce to the new shard if we are now responsible.
		// If we are NO LONGER responsible, we are "Custodial". We should check if we can hand off.
		// But in the new "Tourist" model, we rely on the network to pick it up.
		// If we are responsible now, we should announce Ingest to our new shard so others know.

		if isResponsible && sm.storageMgr.IsPinned(key) {
			im := schema.IngestMessage{
				Type:        schema.MessageTypeIngest,
				ManifestCID: manifestCID,
				ShardID:     newShard,
				HintSize:    0,
			}
			if err := sm.signer.SignProtocolMessage(&im); err == nil {
				if b, err := im.MarshalCBOR(); err == nil {
					sm.PublishToShardCBOR(b, newShard)
				}
			}
		} else if wasResponsible {
			// We lost responsibility: file belongs to the other branch after the split.
			// Unpin so the responsible shard holds the replicas instead of early nodes.
			if sm.storageMgr.IsPinned(key) {
				log.Printf("[Reshard] Unpinning file that no longer belongs to shard %s: %s", newShard, common.TruncateCID(key, 16))

				// Use Cluster Manager to unpin from state
				if sm.clusterMgr != nil {
					if err := sm.clusterMgr.Unpin(sm.ctx, oldShard, manifestCID); err != nil {
						log.Printf("[Reshard] Warning: Failed to unpin from old shard cluster: %v", err)
					}
				}

				// Also explicit local cleanup if needed, but Cluster Unpin should trigger tracker.
				// However, if we LEFT the old shard, the tracker might be stopped.
				// But ReshardPass runs during overlap/after split.
				// If we left the shard, we can't unpin via consensus.
				// We should unpin locally if we are no longer tracking that shard.
				if sm.ipfsClient != nil {
					_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
				}
				sm.storageMgr.UnpinFile(key)
			}
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
	prefix := common.GetHexBinaryPrefix(common.KeyToStableHex(key), len(sm.currentShard))
	return prefix == sm.currentShard
}

// PinToCluster pins a CID to the current shard's cluster state.
func (sm *ShardManager) PinToCluster(ctx context.Context, c cid.Cid) error {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	// Default to -1 (recursive) and no specific replication factor (use cluster default/all)
	return sm.clusterMgr.Pin(ctx, currentShard, c, -1, -1)
}

// GetShardPeers returns the list of peers in the current shard (mesh peers from pubsub)
// This is more efficient than DHT queries since nodes already know each other via pubsub.
func (sm *ShardManager) GetShardPeers() []peer.ID {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}

	// Get mesh peers (connected and in mesh) - these are peers we already know via pubsub
	return sub.topic.ListPeers()
}

// GetShardPeerCount returns the peer count for a specific shard.
// Returns 0 if we're not subscribed to that shard.
func (sm *ShardManager) GetShardPeerCount(shardID string) int {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}

	return len(sub.topic.ListPeers())
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

// generateDeeperShards generates all possible deeper shards in the branch starting from the current shard.
// For example, if current shard is "1", it generates ["10", "11", "100", "101", "110", "111", ...]
// up to maxDepth levels deeper.
func (sm *ShardManager) generateDeeperShards(currentShard string, maxDepth int) []string {
	if maxDepth <= 0 {
		return nil
	}

	var shards []string
	queue := []string{currentShard}
	maxShardLength := len(currentShard) + maxDepth

	for len(queue) > 0 {
		shard := queue[0]
		queue = queue[1:]

		// Generate children: append "0" and "1"
		child0 := shard + "0"
		child1 := shard + "1"

		// Only add if within max depth
		if len(child0) <= maxShardLength {
			shards = append(shards, child0, child1)

			// Add to queue for next level if we haven't reached max depth
			if len(child0) < maxShardLength {
				queue = append(queue, child0, child1)
			}
		}
	}

	return shards
}

// probeShard temporarily joins a shard to check if it exists and has peers.
// Returns the peer count if the shard exists and has activity, 0 otherwise.
func (sm *ShardManager) probeShard(shardID string, probeTimeout time.Duration) int {
	// Check if we're already in this shard
	sm.mu.RLock()
	sub, alreadyJoined := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if alreadyJoined && sub.topic != nil {
		return sm.getProbePeerCount(shardID, sub.topic, 2*time.Minute)
	}

	// Temporarily join the shard
	sm.JoinShard(shardID)
	defer sm.LeaveShard(shardID)

	time.Sleep(probeTimeout)

	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}

	return sm.getProbePeerCount(shardID, sub.topic, 1*time.Minute)
}

// getProbePeerCount returns peer count for a shard. When we have little or no "seen"
// data (e.g. we just joined the topic and waited only 3s), we use the mesh count so
// ROOT nodes can see that shard "0" or "1" has peers and migrate. We return the
// maximum of seen and mesh so probing does not undercount; probe may include
// passive subscribers (e.g. monitor) but that is acceptable for migration decisions.
func (sm *ShardManager) getProbePeerCount(shardID string, topic interface{ ListPeers() []peer.ID }, activeWindow time.Duration) int {
	meshPeers := topic.ListPeers()
	meshCount := len(meshPeers) + 1

	sm.mu.RLock()
	seenCount := 0
	if seenMap, exists := sm.seenPeers[shardID]; exists {
		cutoff := time.Now().Add(-activeWindow)
		for _, lastSeen := range seenMap {
			if lastSeen.After(cutoff) {
				seenCount++
			}
		}
		seenCount++
	}
	sm.mu.RUnlock()

	// Use max so we don't undercount when we've just joined and have few heartbeats (fixes ROOT stuck)
	if seenCount > meshCount {
		return seenCount
	}
	return meshCount
}

// checkAndMergeUpIfAlone moves to the parent shard when alone in a leaf, parent has room, sibling empty.
func (sm *ShardManager) checkAndMergeUpIfAlone() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	if len(currentShard) < 2 {
		return
	}

	currentPeerCount := sm.getShardPeerCount()
	if currentPeerCount > 1 {
		return
	}

	parentShard := currentShard[:len(currentShard)-1]
	probeTimeout := 3 * time.Second
	parentPeerCount := sm.probeShard(parentShard, probeTimeout)
	if parentPeerCount >= config.MaxPeersPerShard {
		return
	}

	lastBit := currentShard[len(currentShard)-1]
	siblingShard := parentShard + string([]byte{'0' + (1 - (lastBit - '0'))})
	siblingPeerCount := sm.probeShard(siblingShard, probeTimeout)
	if siblingPeerCount > 0 {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers but sibling %s has %d peers, not merging up",
				currentShard, currentPeerCount, siblingShard, siblingPeerCount)
		}
		return
	}

	log.Printf("[ShardMergeUp] Alone in %s (%d peers), parent %s has %d, sibling %s empty -> moving to %s",
		currentShard, currentPeerCount, parentShard, parentPeerCount, siblingShard, parentShard)

	sm.mu.Lock()
	oldShard := sm.currentShard
	sm.currentShard = parentShard
	sm.msgCounter = 0
	sm.mu.Unlock()

	sm.JoinShard(parentShard)
	go func() {
		time.Sleep(config.ShardOverlapDuration)
		sm.LeaveShard(oldShard)
	}()
	go func() {
		time.Sleep(config.ReshardDelay)
		sm.RunReshardPass(oldShard, parentShard)
	}()
}

// minPeersToJoinDeeperShard is the minimum peer count required to move into a deeper shard.
// Must be at least 2 so we don't move into a shard where we'd be alone (probe counts self as 1).
const minPeersToJoinDeeperShard = 2

// discoverAndMoveToDeeperShard probes deeper shards in our branch and moves only when:
// - current shard is over the limit (MaxPeersPerShard), or
// - a deeper shard has strictly more peers than current.
// We do not move when current is under the limit and the deeper shard has fewer peers
// (e.g. 7 nodes in current must not move to a deeper shard with 2–3 nodes).
func (sm *ShardManager) discoverAndMoveToDeeperShard() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	peerIDHash := common.GetBinaryPrefix(sm.h.ID().String(), 256)
	sm.mu.RUnlock()

	currentPeerCount := sm.getShardPeerCount()
	currentOverLimit := currentPeerCount > config.MaxPeersPerShard

	deeperShards := sm.generateDeeperShards(currentShard, 3)
	matchingShards := make([]string, 0)
	for _, shard := range deeperShards {
		if len(shard) <= len(peerIDHash) && peerIDHash[:len(shard)] == shard {
			matchingShards = append(matchingShards, shard)
		}
	}

	if len(matchingShards) == 0 {
		return
	}

	sort.Slice(matchingShards, func(i, j int) bool {
		return len(matchingShards[i]) > len(matchingShards[j])
	})

	probeTimeout := 3 * time.Second
	deepestActiveShard := ""
	for _, shard := range matchingShards {
		// Do not move to a shard more than one level deeper unless its parent has at least
		// MinPeersPerShard peers (e.g. don't move to 110 when 11 only has 4 nodes).
		if len(shard) > len(currentShard)+1 {
			parentShard := shard[:len(shard)-1]
			parentPeerCount := sm.probeShard(parentShard, probeTimeout)
			if parentPeerCount < config.MinPeersPerShard {
				if config.VerboseLogging {
					log.Printf("[ShardDiscovery] Skipping deeper shard %s: parent %s has %d peers (need >= %d)",
						shard, parentShard, parentPeerCount, config.MinPeersPerShard)
				}
				continue
			}
		}

		peerCount := sm.probeShard(shard, probeTimeout)
		if peerCount < minPeersToJoinDeeperShard {
			continue
		}
		// Move only when over limit or deeper has strictly more peers (never drain when current is under limit)
		moveBecauseDeeperHasMore := peerCount > currentPeerCount
		if currentOverLimit || moveBecauseDeeperHasMore {
			deepestActiveShard = shard
			reason := "deeper has more peers"
			if currentOverLimit {
				reason = "current over limit"
			}
			log.Printf("[ShardDiscovery] Found active deeper shard %s with %d peers (current: %s has %d, %s)",
				shard, peerCount, currentShard, currentPeerCount, reason)
			break
		}
	}

	if deepestActiveShard == "" || deepestActiveShard == currentShard {
		return
	}

	log.Printf("[ShardDiscovery] Moving from shard %s to deeper shard %s",
		currentShard, deepestActiveShard)

	sm.mu.Lock()
	oldShard := sm.currentShard
	sm.currentShard = deepestActiveShard
	sm.msgCounter = 0
	sm.mu.Unlock()

	sm.JoinShard(deepestActiveShard)
	go func() {
		time.Sleep(config.ShardOverlapDuration)
		sm.LeaveShard(oldShard)
	}()
	go func() {
		time.Sleep(config.ReshardDelay)
		sm.RunReshardPass(oldShard, deepestActiveShard)
	}()
}

// runShardDiscovery runs when idle or when current shard has few peers; then merge-up if alone, else discover deeper.
func (sm *ShardManager) runShardDiscovery() {
	ticker := time.NewTicker(config.ShardDiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.Lock()
			now := time.Now()
			if now.Sub(sm.lastDiscoveryCheck) < config.ShardDiscoveryInterval {
				sm.mu.Unlock()
				continue
			}
			sm.lastDiscoveryCheck = now
			isIdle := sm.lastMessageTime.IsZero() || now.Sub(sm.lastMessageTime) > 1*time.Minute
			currentShard := sm.currentShard
			sm.mu.Unlock()

			peerCount := sm.getShardPeerCount()
			fewPeersInShard := peerCount <= config.MaxPeersPerShard
			if !isIdle && !fewPeersInShard {
				continue
			}

			if config.VerboseLogging {
				log.Printf("[ShardDiscovery] Shard %s (%d peers), checking merge-up / deeper...", currentShard, peerCount)
			}
			sm.checkAndMergeUpIfAlone()
			sm.discoverAndMoveToDeeperShard()
		}
	}
}

func (sm *ShardManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, sub := range sm.shardSubs {
		sub.cancel()
		sub.sub.Cancel()
	}
}
