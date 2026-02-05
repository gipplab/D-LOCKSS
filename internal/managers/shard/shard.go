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
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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

// migratePinsFlushDelay: wait before MigratePins after a split so the CRDT batch (MaxBatchAge 200ms in clusters) can flush; State() then includes recently logged pins and single-file replication is not lost.
const migratePinsFlushDelay = 250 * time.Millisecond

// rootPeerCheckInterval: when at root shard, check peer count this often so multiple nodes can split in quick succession instead of waiting for the full ShardPeerCheckInterval.
const rootPeerCheckInterval = 30 * time.Second

// rootReplicationCheckInterval: when at root shard, check replication this often so under-replicated files get ReplicationRequest sooner and replication fills faster.
const rootReplicationCheckInterval = 20 * time.Second

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
	clusterMgr     clusters.ClusterManagerInterface // NEW: Cluster Manager Interface
	metrics        *telemetry.MetricsManager
	signer         *signing.Signer
	reshardedFiles *common.KnownFiles
	rateLimiter    *common.RateLimiter

	mu           sync.RWMutex
	currentShard string

	// Map of shardID -> subscription
	shardSubs map[string]*shardSubscription

	// probeTopicCache: topic from a recent silent probe (we don't Close it so Join() would fail with "topic already exists").
	// JoinShard reuses it when joining that shard so we only have one Topic handle per topic name.
	probeTopicCache map[string]*pubsub.Topic

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
	clusterMgr clusters.ClusterManagerInterface,
	startShard string,
) *ShardManager {
	sm := &ShardManager{
		ctx:             ctx,
		h:               h,
		ps:              ps,
		ipfsClient:      ipfsClient,
		storageMgr:      stm,
		clusterMgr:      clusterMgr,
		metrics:         metrics,
		signer:          signer,
		rateLimiter:     rateLimiter,
		reshardedFiles:  common.NewKnownFiles(),
		currentShard:    startShard,
		shardSubs:       make(map[string]*shardSubscription),
		probeTopicCache: make(map[string]*pubsub.Topic),
		seenPeers:       make(map[string]map[peer.ID]time.Time),
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
	go sm.runOrphanUnpinLoop()
	go sm.runReplicationChecker()
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

	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	var t *pubsub.Topic
	if cached := sm.probeTopicCache[shardID]; cached != nil {
		// Reuse topic from a recent silent probe (go-libp2p Join errors with "topic already exists" if we closed it).
		t = cached
		delete(sm.probeTopicCache, shardID)
	} else {
		var err error
		t, err = sm.ps.Join(topicName)
		if err != nil {
			log.Printf("[Error] Failed to join shard topic %s: %v", topicName, err)
			return
		}
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

	// Always announce join so monitor and peers see us on this topic immediately (no wait for periodic heartbeat).
	// 1) Dedicated JOIN message so monitor can log "Node X joined shard Y".
	joinMsg := []byte("JOIN:" + sm.h.ID().String())
	_ = newSub.topic.Publish(sm.ctx, joinMsg)
	// 2) Immediate heartbeat with pinned count (same format as periodic heartbeat).
	pinnedCount := 0
	if sm.storageMgr != nil {
		pinnedCount = sm.storageMgr.GetPinnedCount()
	}
	heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d", sm.h.ID().String(), pinnedCount))
	_ = newSub.topic.Publish(sm.ctx, heartbeatMsg)

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
		// Notify monitor and peers that we are leaving before closing the topic.
		leaveMsg := []byte("LEAVE:" + sm.h.ID().String())
		_ = sub.topic.Publish(sm.ctx, leaveMsg)
		// Give pubsub time to flush the LEAVE message before we close the topic.
		sm.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		sm.mu.Lock()
		// Re-fetch in case refCount was bumped (e.g. JoinShard during sleep)
		sub, exists = sm.shardSubs[shardID]
		if !exists || sub.refCount > 0 {
			return
		}
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
// and triggers splits when the threshold is exceeded. Uses a short tick so that when
// at root we can run the check every rootPeerCheckInterval; when in a child shard
// checkAndSplitIfNeeded still enforces ShardPeerCheckInterval.
func (sm *ShardManager) runPeerCountChecker() {
	ticker := time.NewTicker(rootPeerCheckInterval)
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

// runReplicationChecker periodically checks replication for our pinned files and broadcasts ReplicationRequest when under target.
// When at root we use a shorter interval so replication fills faster.
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
				// Only request replication for files that belong to our shard.
				// Files belonging to other shards must not be pinned here (shard 1 must not pin shard 0 files).
				payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, manifestCIDStr)
				if !sm.AmIResponsibleFor(payloadCIDStr) {
					continue
				}
				c, err := cid.Decode(manifestCIDStr)
				if err != nil {
					continue
				}
				allocations, err := sm.clusterMgr.GetAllocations(sm.ctx, currentShard, c)
				if err != nil {
					continue
				}
				// Cap "at target" at shard size: 4-node shard can only have 4 replicas.
				peerCount := sm.getShardPeerCount()
				minRep := config.MinReplication
				if peerCount > 0 && minRep > peerCount {
					minRep = peerCount
				}
				if len(allocations) >= minRep {
					continue
				}
				// Under-replicated: broadcast ReplicationRequest so peers can sync/re-announce.
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
				log.Printf("[Shard] ReplicationRequest sent for %s (shard %s, allocations=%d, min=%d)",
					manifestCIDStr, currentShard, len(allocations), minRep)
			}
		}
	}
}

// runHeartbeat periodically sends heartbeat messages to the current shard topic
// to help peers and the monitor discover each other. Uses a short interval so
// nodes appear without requiring file ingest traffic.
func (sm *ShardManager) runHeartbeat() {
	// Heartbeat interval: use configured value, or auto-calculate from peer check interval
	var heartbeatInterval time.Duration
	if config.HeartbeatInterval > 0 {
		heartbeatInterval = config.HeartbeatInterval
	} else {
		// Auto: 1/3 of peer check interval, min 10s, so monitor and peers discover within ~10–40s
		heartbeatInterval = config.ShardPeerCheckInterval / 3
		if heartbeatInterval < 10*time.Second {
			heartbeatInterval = 10 * time.Second
		}
	}
	// Back off when shard has multiple peers to save bandwidth (cluster state sync is separate)
	const backoffWhenPeers = 2
	const backoffInterval = 5 * time.Minute

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	// Send first heartbeat soon so nodes and monitor discover this node without waiting for file activity
	sm.sendHeartbeat()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.sendHeartbeat()
			// If shard has enough peers, slow down next heartbeats
			if n := sm.getShardPeerCount(); n >= backoffWhenPeers && heartbeatInterval < backoffInterval {
				ticker.Reset(backoffInterval)
				heartbeatInterval = backoffInterval
			}
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
		// JOIN: and LEAVE: are plain text; skip CBOR decode to avoid "unexpected content after end of cbor object" log
		if len(msg.Data) >= 5 && (string(msg.Data[:5]) == "JOIN:" || (len(msg.Data) >= 6 && string(msg.Data[:6]) == "LEAVE:")) {
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

// handleReplicationRequest processes a ReplicationRequest (under-replicated file).
// If we already have the file, trigger sync to re-announce. If not and auto-replication is enabled, fetch from network and pin to cluster.
func (sm *ShardManager) handleReplicationRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("ReplicationRequest (Shard %s)", shardID)
	if sm.signer.VerifyAndAuthorizeMessage(msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped ReplicationRequest for %s from %s in shard %s due to verification failure", rr.ManifestCID.String(), msg.GetFrom().String(), shardID)
		return
	}
	manifestCIDStr := rr.ManifestCID.String()
	if sm.storageMgr.IsPinned(manifestCIDStr) {
		sm.clusterMgr.TriggerSync(shardID)
		return
	}
	if !config.AutoReplicationEnabled {
		return
	}
	// Fetch from network and add ourselves as a replica (run in goroutine to avoid blocking message handler).
	go func() {
		fetchCtx, cancel := context.WithTimeout(sm.ctx, config.AutoReplicationTimeout)
		defer cancel()
		c := rr.ManifestCID
		if err := sm.ipfsClient.PinRecursive(fetchCtx, c); err != nil {
			log.Printf("[Shard] Auto-replication: failed to fetch/pin %s: %v", manifestCIDStr, err)
			return
		}
		if err := sm.clusterMgr.Pin(fetchCtx, shardID, c, config.MinReplication, config.MaxReplication); err != nil {
			log.Printf("[Shard] Auto-replication: failed to add to cluster %s: %v", manifestCIDStr, err)
			return
		}
		log.Printf("[Shard] Auto-replication: fetched and pinned %s to shard %s", manifestCIDStr, shardID)
		sm.clusterMgr.TriggerSync(shardID)
	}()
}

// handleIngestMessage processes an IngestMessage.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if sm.signer.VerifyAndAuthorizeMessage(msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		log.Printf("[Shard] Dropped IngestMessage from %s in shard %s due to verification failure", msg.GetFrom().String(), shardID)
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
			sm.AnnouncePinned(key)
		}
	}
}

// getShardPeerCount returns peer count for the CURRENT shard.
// When we have a subscription we use PubSub mesh+seen so split/merge sees real shard membership;
// CRDT Peers() can lag or return a subset and would block splits (e.g. 15 nodes at root never splitting).
func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	// 1. When we have a subscription, use PubSub so split/merge sees real count.
	// Use max(mesh, seen) so we don't undercount: mesh can be slow to form (GossipSub degree),
	// and seen can be incomplete (not all peers sent a message yet). Undercounting blocks splits.
	if exists && sub.topic != nil {
		meshPeers := sub.topic.ListPeers()
		meshCount := len(meshPeers) + 1 // +1 for self

		sm.mu.RLock()
		seenCount := 0
		if seenMap, ok := sm.seenPeers[currentShard]; ok {
			cutoff := time.Now().Add(-350 * time.Second)
			for _, lastSeen := range seenMap {
				if lastSeen.After(cutoff) {
					seenCount++
				}
			}
			seenCount++ // Include self
		}
		sm.mu.RUnlock()

		if seenCount > meshCount {
			return seenCount
		}
		return meshCount
	}

	// 2. No subscription: use cluster if available (e.g. for metrics).
	if sm.clusterMgr != nil {
		count, err := sm.clusterMgr.GetPeerCount(sm.ctx, currentShard)
		if err == nil {
			return count
		}
	}
	return 0
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

// checkAndSplitIfNeeded periodically checks the number of peers in the CURRENT shard
func (sm *ShardManager) checkAndSplitIfNeeded() {
	sm.mu.Lock()
	now := time.Now()
	currentShard := sm.currentShard
	interval := config.ShardPeerCheckInterval
	if currentShard == "" {
		interval = rootPeerCheckInterval
	}
	if now.Sub(sm.lastPeerCheck) < interval {
		sm.mu.Unlock()
		return
	}
	sm.lastPeerCheck = now
	sm.mu.Unlock()

	peerCount := sm.getShardPeerCount()
	estimatedPeersAfterSplit := peerCount / 2
	// Require both children to have enough peers: at least MinPeersPerShard and at least minPeersPerChildAfterSplit
	// so we never create tiny shards (e.g. 1–3 nodes) that fragment the network.
	minPerChild := config.MinPeersPerShard
	if minPeersPerChildAfterSplit > minPerChild {
		minPerChild = minPeersPerChildAfterSplit
	}
	shouldSplit := peerCount > config.MaxPeersPerShard && estimatedPeersAfterSplit >= minPerChild

	// Log peer count for debugging
	if currentShard == "" && config.VerboseLogging {
		log.Printf("[Sharding] Root shard peer count: %d (threshold: %d, min per child after split: %d)",
			peerCount, config.MaxPeersPerShard, minPerChild)
	}

	if shouldSplit {
		log.Printf("[Sharding] Shard %s has %d peers. Splitting...", currentShard, peerCount)
		sm.splitShard()
	} else if peerCount > config.MaxPeersPerShard && config.VerboseLogging {
		// Log why split didn't happen
		log.Printf("[Sharding] Shard %s has %d peers (exceeds threshold %d) but won't split: estimated per child (%d) < minimum (%d)",
			currentShard, peerCount, config.MaxPeersPerShard, estimatedPeersAfterSplit, minPerChild)
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

	// Trigger migration from old cluster to new cluster (see migratePinsFlushDelay).
	go func() {
		time.Sleep(migratePinsFlushDelay)
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
				log.Printf("[Reshard] Unpinning file that no longer belongs to shard %s: %s", newShard, key)

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

// orphanUnpinInterval is how often to check for pins that belong to active child shards.
// Nodes that stay in a parent shard (e.g. "0") never run RunReshardPass, so they can keep
// pins for files that belong to children ("00"/"01"); this pass unpins those so replication
// reflects only nodes in each shard.
const orphanUnpinInterval = 2 * time.Minute
const orphanUnpinGracePeriod = 3 * time.Minute // do not unpin files pinned this recently so child nodes can sync

// RunOrphanUnpinPass unpins files that belong to active child shards when we are still in the parent.
// E.g. we're in "", child shards "0" and "1" have peers → unpin all files (they belong to "0" or "1").
// E.g. we're in "0", child shards "00" and "01" have peers → unpin files whose prefix at depth 2 is "00" or "01".
func (sm *ShardManager) RunOrphanUnpinPass() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	files := sm.storageMgr.GetKnownFiles().All()
	if len(files) == 0 {
		return
	}

	// When on root, children are "0" and "1"; otherwise currentShard+"0" and currentShard+"1".
	child0 := currentShard + "0"
	child1 := currentShard + "1"
	if currentShard == "" {
		child0 = "0"
		child1 = "1"
	}
	probeTimeout := 4 * time.Second
	n0 := sm.probeShard(child0, probeTimeout)
	n1 := sm.probeShard(child1, probeTimeout)
	if n0 < 1 && n1 < 1 {
		return
	}
	activeChildren := make(map[string]struct{})
	if n0 >= 1 {
		activeChildren[child0] = struct{}{}
	}
	if n1 >= 1 {
		activeChildren[child1] = struct{}{}
	}

	depth := len(currentShard) + 1
	if currentShard == "" {
		depth = 1
	}
	unpinned := 0
	for key := range files {
		if !sm.storageMgr.IsPinned(key) {
			continue
		}
		// Do not unpin recently pinned files: give child shard nodes time to sync from CRDT before we drop our replica.
		if pinTime := sm.storageMgr.GetPinTime(key); !pinTime.IsZero() && time.Since(pinTime) < orphanUnpinGracePeriod {
			continue
		}
		payloadCIDStr := common.GetPayloadCIDForShardAssignment(sm.ctx, sm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetChild := common.GetHexBinaryPrefix(stableHex, depth)
		if _, active := activeChildren[targetChild]; !active {
			continue
		}
		manifestCID, err := common.KeyToCID(key)
		if err != nil {
			continue
		}
		log.Printf("[Reshard] Orphan unpin: file belongs to child shard %s (we are in %s): %s",
			targetChild, currentShard, key)
		if sm.clusterMgr != nil {
			_ = sm.clusterMgr.Unpin(sm.ctx, currentShard, manifestCID)
		}
		if sm.ipfsClient != nil {
			_ = sm.ipfsClient.UnpinRecursive(sm.ctx, manifestCID)
		}
		sm.storageMgr.UnpinFile(key)
		unpinned++
		time.Sleep(10 * time.Millisecond)
	}
	if unpinned > 0 {
		log.Printf("[Reshard] Orphan unpin pass: unpinned %d files that belong to child shards", unpinned)
	}
}

// runOrphanUnpinLoop periodically runs RunOrphanUnpinPass so nodes that stayed in a parent
// shard drop pins that belong to active child shards (fixing replication > nodes per shard).
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

// AnnouncePinned publishes PINNED:<manifestCID> to the current shard topic immediately,
// so the monitor (and peers) see replication as soon as a file is pinned instead of waiting for the next heartbeat batch.
func (sm *ShardManager) AnnouncePinned(manifestCID string) {
	if manifestCID == "" {
		return
	}
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()
	if !exists || sub.topic == nil {
		return
	}
	msg := []byte(fmt.Sprintf("PINNED:%s", manifestCID))
	_ = sub.topic.Publish(sm.ctx, msg)
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

// EnsureClusterForShard ensures the embedded cluster for the given shard exists (e.g. when joining as tourist to pin there).
// Idempotent; safe to call after JoinShard(shardID).
func (sm *ShardManager) EnsureClusterForShard(ctx context.Context, shardID string) error {
	return sm.clusterMgr.JoinShard(ctx, shardID, nil)
}

// PinToShard pins a CID to a specific shard's cluster state (e.g. target shard when custodial).
// Call EnsureClusterForShard(ctx, shardID) first if this node is a tourist (not a member of shardID).
func (sm *ShardManager) PinToShard(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.Pin(ctx, shardID, c, -1, -1)
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

// GetPeersForShard returns the list of peers in the given shard (mesh peers from pubsub).
// Used by ClusterManager's PeerMonitor so CRDT Peers() and allocations use real shard membership.
// Returns nil if we are not subscribed to that shard.
func (sm *ShardManager) GetPeersForShard(shardID string) []peer.ID {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}
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

// probeShard checks how many peers are in a shard without becoming a member.
// If we're already in the shard, we use our subscription. Otherwise we subscribe
// silently (like the monitor): no JOIN/HEARTBEAT/LEAVE, just observe mesh size and unsubscribe.
// Returns the peer count if the shard exists and has activity, 0 otherwise.
func (sm *ShardManager) probeShard(shardID string, probeTimeout time.Duration) int {
	// Check if we're already in this shard
	sm.mu.RLock()
	sub, alreadyJoined := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if alreadyJoined && sub.topic != nil {
		return sm.getProbePeerCount(shardID, sub.topic, 350*time.Second)
	}

	// Silent probe: subscribe to the topic without publishing JOIN/HEARTBEAT/LEAVE (like the monitor)
	return sm.probeShardSilently(shardID, probeTimeout)
}

// probeShardSilently subscribes to a shard topic to observe the mesh, then unsubscribes.
// No JOIN, HEARTBEAT, or LEAVE is published; we are a silent listener and do not count as a member.
// We do not Close the topic: go-libp2p Join() errors with "topic already exists" if the topic name
// is still registered. We cache the topic so JoinShard can reuse it when we move to that shard.
func (sm *ShardManager) probeShardSilently(shardID string, probeTimeout time.Duration) int {
	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		return 0
	}
	psSub, err := t.Subscribe()
	if err != nil {
		_ = t.Close()
		return 0
	}
	defer psSub.Cancel()
	// Do not Close(t): cache for JoinShard to reuse (avoids "topic already exists" on real join)

	time.Sleep(probeTimeout)

	// Mesh peers = other peers in the topic. We do NOT add 1 for self; we're a silent observer, not a member.
	meshPeers := t.ListPeers()
	n := len(meshPeers)
	// If mesh was slow to populate, retry once after a short extra wait so root nodes don't stay stuck.
	if n == 0 {
		time.Sleep(2 * time.Second)
		meshPeers = t.ListPeers()
		if len(meshPeers) > n {
			n = len(meshPeers)
		}
	}

	// Cache topic for JoinShard so we reuse it (avoids "topic already exists"). Evict one if cache full and we're adding new.
	sm.mu.Lock()
	if old := sm.probeTopicCache[shardID]; old != nil {
		_ = old.Close()
	}
	const maxProbeCache = 4
	if len(sm.probeTopicCache) >= maxProbeCache && sm.probeTopicCache[shardID] == nil {
		for k, v := range sm.probeTopicCache {
			_ = v.Close()
			delete(sm.probeTopicCache, k)
			break
		}
	}
	sm.probeTopicCache[shardID] = t
	sm.mu.Unlock()
	return n
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

// checkAndMergeUpIfAlone moves to the parent shard when understaffed in a leaf and (sibling empty or we're the smaller side), parent has room.
// This consolidates tiny shards (e.g. 1 node in 00) back into the parent so we don't end up with many understaffed shards.
func (sm *ShardManager) checkAndMergeUpIfAlone() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()

	if len(currentShard) < 2 {
		return
	}

	currentPeerCount := sm.getShardPeerCount()
	// Merge when understaffed (not only when alone), so small shards consolidate into the parent.
	if currentPeerCount >= config.MinPeersPerShard {
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
	// Merge when sibling is empty, or when we're the smaller/equal side (so we consolidate into parent).
	if siblingPeerCount > 0 && currentPeerCount > siblingPeerCount {
		if config.VerboseLogging {
			log.Printf("[ShardMergeUp] Shard %s has %d peers, sibling %s has %d peers (we're larger), not merging up",
				currentShard, currentPeerCount, siblingShard, siblingPeerCount)
		}
		return
	}

	log.Printf("[ShardMergeUp] Understaffed in %s (%d peers), parent %s has %d, sibling %s has %d -> moving to %s",
		currentShard, currentPeerCount, parentShard, parentPeerCount, siblingShard, siblingPeerCount, parentShard)

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

// minPeersPerChildAfterSplit is the minimum peers each child must have after a split.
// Prevents creating tiny shards (1–3 nodes) that fragment the network; use with small testnets.
const minPeersPerChildAfterSplit = 4

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

	// When on root, also consider the other depth-1 shard so we can move to whichever child
	// already has nodes (e.g. 14 prefix-"0" nodes at root can discover and join shard "1"
	// instead of staying stuck until someone with prefix "1" splits into "1").
	if currentShard == "" {
		has0, has1 := false, false
		for _, s := range matchingShards {
			if s == "0" {
				has0 = true
			}
			if s == "1" {
				has1 = true
			}
		}
		if !has0 {
			matchingShards = append(matchingShards, "0")
		}
		if !has1 {
			matchingShards = append(matchingShards, "1")
		}
	}

	if len(matchingShards) == 0 {
		return
	}

	// When on root, try depth-1 shards ("0"/"1") first so we move quickly; otherwise we'd probe empty "00","01","000"...
	// first (6s each) and take 24–30s before seeing "0"/"1", leaving nodes stuck on root.
	// When not on root, try deepest first so we prefer the most specific active shard.
	if currentShard == "" {
		sort.Slice(matchingShards, func(i, j int) bool {
			return len(matchingShards[i]) < len(matchingShards[j])
		})
	} else {
		sort.Slice(matchingShards, func(i, j int) bool {
			return len(matchingShards[i]) > len(matchingShards[j])
		})
	}

	// Silent probe needs time for GossipSub mesh to populate after Subscribe(). When on root probing
	// depth-1 ("0"/"1"), use a longer timeout and allow one retry so root nodes reliably see existing
	// shards and migrate (avoids 9 nodes stuck at root while 0/1 already have nodes).
	probeTimeout := 6 * time.Second
	if currentShard == "" {
		probeTimeout = 12 * time.Second
	}
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
		// When on root probing depth-1, retry once if mesh was empty so we don't stay stuck.
		if currentShard == "" && len(shard) == 1 && peerCount == 0 {
			peerCount = sm.probeShard(shard, probeTimeout)
		}
		minRequired := minPeersToJoinDeeperShard
		// When on root, allow moving to depth-1 shard ("0" or "1") with >= 1 peer so we follow the first splitter.
		if currentShard == "" && len(shard) == 1 {
			minRequired = 1
		}
		// When not over limit, require deeper shard to have at least MinPeersPerShard so we don't fragment
		// (e.g. don't move from "0" with 7 nodes to "00" with 2 nodes). Exception: when on root, allow
		// moving to depth-1 ("0"/"1") with any count >= minRequired so root can drain and nodes don't get stuck.
		if !currentOverLimit && peerCount < config.MinPeersPerShard {
			if currentShard == "" && len(shard) == 1 {
				// On root: take "0"/"1" if it has >= 1 peer so we drain root
			} else if peerCount >= minRequired && config.VerboseLogging {
				log.Printf("[ShardDiscovery] Skipping deeper shard %s: has %d peers (need >= %d when current not over limit)",
					shard, peerCount, config.MinPeersPerShard)
			} else {
				continue
			}
		}
		if peerCount < minRequired {
			continue
		}
		// When on root, always allow moving if deeper shard has >= minRequired so root can drain (avoid stuck nodes).
		// Otherwise move only when over limit or deeper has strictly more peers.
		moveBecauseOnRoot := currentShard == "" && peerCount >= minRequired
		moveBecauseDeeperHasMore := peerCount > currentPeerCount
		if currentOverLimit || moveBecauseDeeperHasMore || moveBecauseOnRoot {
			deepestActiveShard = shard
			reason := "deeper has more peers"
			if currentOverLimit {
				reason = "current over limit"
			} else if moveBecauseOnRoot {
				reason = "draining root"
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
	if err := sm.clusterMgr.JoinShard(sm.ctx, deepestActiveShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for discovered shard %s: %v", deepestActiveShard, err)
	}

	// Migrate pins from old cluster to new so the new shard's CRDT has our pins and peers can replicate (same as split).
	go func() {
		time.Sleep(migratePinsFlushDelay)
		if err := sm.clusterMgr.MigratePins(sm.ctx, oldShard, deepestActiveShard); err != nil {
			log.Printf("[Sharding] Migration on discovery move failed: %v", err)
		}
	}()

	// Keep old shard for overlap, then leave pubsub and cluster (match split behavior).
	go func() {
		time.Sleep(config.ShardOverlapDuration)
		sm.LeaveShard(oldShard)
		if err := sm.clusterMgr.LeaveShard(oldShard); err != nil {
			log.Printf("[Sharding] Failed to leave old cluster %s after discovery move: %v", oldShard, err)
		}
	}()
	go func() {
		time.Sleep(config.ReshardDelay)
		sm.RunReshardPass(oldShard, deepestActiveShard)
	}()
}

// discoveryIntervalOnRoot is how often to check for deeper shards when on root,
// so nodes follow to "0"/"1" quickly after the first node splits (instead of waiting 5 min).
// Short interval (10s) so first discovery runs soon after ShardPeerCheckInterval (10s in testnet) and root drains.
const discoveryIntervalOnRoot = 10 * time.Second

// runShardDiscovery runs when idle or when current shard has few peers; then merge-up if alone, else discover deeper.
func (sm *ShardManager) runShardDiscovery() {
	ticker := time.NewTicker(discoveryIntervalOnRoot)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.Lock()
			now := time.Now()
			currentShard := sm.currentShard
			// When on root, check every discoveryIntervalOnRoot so nodes follow to 0/1 quickly.
			// When on a deeper shard, use full ShardDiscoveryInterval to avoid churn.
			interval := config.ShardDiscoveryInterval
			if currentShard == "" {
				interval = discoveryIntervalOnRoot
			}
			if now.Sub(sm.lastDiscoveryCheck) < interval {
				sm.mu.Unlock()
				continue
			}
			sm.lastDiscoveryCheck = now
			isIdle := sm.lastMessageTime.IsZero() || now.Sub(sm.lastMessageTime) > 1*time.Minute
			sm.mu.Unlock()

			peerCount := sm.getShardPeerCount()
			fewPeersInShard := peerCount <= config.MaxPeersPerShard
			onRoot := currentShard == ""
			// Run discovery when idle, when shard has few peers, or when on root so nodes can follow to "0"/"1" after a split.
			if !isIdle && !fewPeersInShard && !onRoot {
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
