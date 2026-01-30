package main

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// ReplicationManager handles the replication lifecycle and pipeline.
type ReplicationManager struct {
	ipfsClient           ipfs.IPFSClient
	shardMgr             *ShardManager
	storageMgr           *StorageManager
	dht                  DHTProvider
	
	// State
	checkingFiles        *CheckingFiles
	lastCheckTime        *LastCheckTime
	pendingVerifications *PendingVerifications
	replicationCache     *ReplicationCache
}

// NewReplicationManager creates a new ReplicationManager.
func NewReplicationManager(
	client ipfs.IPFSClient, 
	sm *ShardManager, 
	stm *StorageManager, 
	dht DHTProvider,
) *ReplicationManager {
	return &ReplicationManager{
		ipfsClient:           client,
		shardMgr:             sm,
		storageMgr:           stm,
		dht:                  dht,
		checkingFiles:        NewCheckingFiles(),
		lastCheckTime:        NewLastCheckTime(),
		pendingVerifications: NewPendingVerifications(),
		replicationCache:     NewReplicationCache(),
	}
}

// CheckJob represents a unit of work for the replication pipeline
type CheckJob struct {
	Key         string
	ManifestCID cid.Cid
	Priority    int       // 1=High, 2=Medium, 3=Low
	Responsible bool      // Am I responsible?
	Pinned      bool      // Is it locally pinned?
	Timestamp   time.Time // When was this job scheduled?
}

// CheckResult represents the outcome of a replication check
type CheckResult struct {
	Job       CheckJob
	Count     int
	Err       error
	FromCache bool
	Duration  time.Duration
}

// startReplicationPipeline initializes and starts the async replication pipeline
func (rm *ReplicationManager) startReplicationPipeline(ctx context.Context) {
	jobQueue := make(chan CheckJob, ReplicationQueueSize)
	resultQueue := make(chan CheckResult, ReplicationQueueSize)

	// Start Scheduler (Stage 1)
	go rm.runScheduler(ctx, jobQueue)

	// Start Network Probers (Stage 2)
	var wg sync.WaitGroup
	for i := 0; i < ReplicationWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rm.runNetworkProber(ctx, jobQueue, resultQueue)
		}()
	}

	// Start Reconciler (Stage 3)
	go rm.runReconciler(ctx, resultQueue)

	// Wait for context cancellation to clean up
	go func() {
		<-ctx.Done()
		// Scheduler stops on ctx.Done()
		// Probers stop on ctx.Done()
		wg.Wait()
		close(resultQueue)
		// Reconciler stops when resultQueue is closed
	}()
}

// Stage 1: Scheduler
func (rm *ReplicationManager) runScheduler(ctx context.Context, jobQueue chan<- CheckJob) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.scheduleJobs(ctx, jobQueue)
		}
	}
}

func (rm *ReplicationManager) scheduleJobs(ctx context.Context, jobQueue chan<- CheckJob) {
	files := rm.storageMgr.knownFiles.All()
	for key := range files {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Filter: Skip if recently checked or removed
		if !rm.shouldCheckFile(key) {
			continue
		}

		// Use PayloadCID for shard assignment (stable, content-based)
		// ManifestCID includes timestamp/metadata and changes on every ingestion
		payloadCIDStr := getPayloadCIDForShardAssignment(ctx, rm.ipfsClient, key)
		responsible := rm.shardMgr.AmIResponsibleFor(payloadCIDStr)
		pinned := rm.storageMgr.isPinned(key)

		// Filter: Cleanup if not responsible and not pinned
		if !responsible && !pinned {
			rm.storageMgr.removeKnownFile(key)
			continue
		}

		// Filter: TryLock to prevent duplicate scheduling
		if !rm.checkingFiles.TryLock(key) {
			continue
		}

		manifestCID, err := keyToCID(key)
		if err != nil {
			logError("Replication", "convert key to CID in scheduler", key, err)
			rm.checkingFiles.Unlock(key)
			continue
		}

		// Determine Priority
		priority := 3 // Low
		if responsible {
			priority = 1 // High
		} else if !responsible && pinned {
			priority = 2 // Medium (Custodial)
		}

		job := CheckJob{
			Key:         key,
			ManifestCID: manifestCID,
			Priority:    priority,
			Responsible: responsible,
			Pinned:      pinned,
			Timestamp:   time.Now(),
		}

		// Non-blocking send to job queue
		select {
		case jobQueue <- job:
		default:
			// If queue is full, we skip scheduling low priority jobs
			if priority == 1 {
				// Try harder for high priority? For now, just log and skip to avoid blocking scheduler
				log.Printf("[Scheduler] Job queue full, skipping HIGH priority check for %s", key[:min(16, len(key))]+"...")
			}
			rm.checkingFiles.Unlock(key)
		}
	}
}

func (rm *ReplicationManager) shouldCheckFile(key string) bool {
	lastCheck, exists := rm.lastCheckTime.Get(key)
	if exists && time.Since(lastCheck) < ReplicationCheckCooldown {
		return false
	}

	removedTime, wasRemoved := rm.storageMgr.recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return false
	}
	return true
}

// Stage 2: Network Prober
func (rm *ReplicationManager) runNetworkProber(ctx context.Context, jobQueue <-chan CheckJob, resultQueue chan<- CheckResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobQueue:
			rm.processJob(ctx, job, resultQueue)
		}
	}
}

func (rm *ReplicationManager) processJob(ctx context.Context, job CheckJob, resultQueue chan<- CheckResult) {
	start := time.Now()

	// Local Verification (Liar Detection)
	// Skip verification for recently pinned files to allow DAG to be fully available
	if job.Pinned {
		pinTime := rm.storageMgr.pinnedFiles.GetPinTime(job.Key)
		if !pinTime.IsZero() {
			timeSincePin := time.Since(pinTime)
			if timeSincePin < ReplicationVerificationDelay {
				// File was pinned recently, skip verification to allow DAG to be fully available
				// This prevents false negatives where the DAG isn't ready yet
				log.Printf("[Replication] Skipping verification for recently pinned file %s (pinned %v ago, grace period: %v)", 
					truncateCID(job.Key, 16), timeSincePin, ReplicationVerificationDelay)
			} else {
				// File has been pinned long enough, perform verification
				ok, err := rm.verifyResearchObjectLocal(ctx, job.ManifestCID)
				if err != nil {
					log.Printf("[Replication] Local verification error for %s: %v", job.Key[:min(16, len(job.Key))]+"...", err)
				}
				if !ok {
					log.Printf("[Replication] Local DAG incomplete or invalid for %s; unpinning local state", job.Key[:min(16, len(job.Key))]+"...")
					rm.UnpinFile(ctx, job.Key, job.ManifestCID)
					job.Pinned = false // Update job state
				}
			}
		} else {
			// No pin time recorded (shouldn't happen, but handle gracefully)
			// Perform verification anyway
			ok, err := rm.verifyResearchObjectLocal(ctx, job.ManifestCID)
			if err != nil {
				log.Printf("[Replication] Local verification error for %s: %v", job.Key[:min(16, len(job.Key))]+"...", err)
			}
			if !ok {
				log.Printf("[Replication] Local DAG incomplete or invalid for %s; unpinning local state", job.Key[:min(16, len(job.Key))]+"...")
				rm.UnpinFile(ctx, job.Key, job.ManifestCID)
				job.Pinned = false // Update job state
			}
		}
	}

	// Check Cache
	cachedCount, cachedAt, hasCache := rm.replicationCache.GetWithAge(job.Key)
	if hasCache && time.Since(cachedAt) < ReplicationCacheTTL {
		resultQueue <- CheckResult{
			Job:       job,
			Count:     cachedCount,
			FromCache: true,
			Duration:  time.Since(start),
		}
		return
	}

	var count int

	// Replication is managed entirely within shards via pubsub, not DHT
	// DHT is only for external IPFS users to access DLOCKSS replications
	if rm.shardMgr != nil {
		if job.Responsible {
			// For files we're responsible for: use current shard peer count
			shardPeers := rm.shardMgr.GetShardPeers()
			count = len(shardPeers)
			log.Printf("[Replication] Using shard peer count for responsible file %s: %d peers in shard", 
				truncateCID(job.Key, 16), count)
		} else {
			// For custodial files: use target shard peer count
			// Calculate target shard based on payload CID
			payloadCIDStr := getPayloadCIDForShardAssignment(ctx, rm.ipfsClient, job.Key)
			stableHex := keyToStableHex(payloadCIDStr)
			
			// Determine target shard depth (use current shard depth as reference)
			rm.shardMgr.mu.RLock()
			currentDepth := len(rm.shardMgr.currentShard)
			rm.shardMgr.mu.RUnlock()
			
			if currentDepth == 0 {
				currentDepth = 1
			}
			targetShard := getHexBinaryPrefix(stableHex, currentDepth)
			
			// Get peer count from target shard (we may already be in it via Tourist Mode)
			count = rm.shardMgr.GetShardPeerCount(targetShard)
			
			if count == 0 {
				// Not in target shard yet - this shouldn't happen for custodial files
				// as they should have joined the target shard. Use current shard as fallback.
				shardPeers := rm.shardMgr.GetShardPeers()
				count = len(shardPeers)
				log.Printf("[Replication] Warning: Not in target shard %s for custodial file %s, using current shard: %d peers", 
					targetShard, truncateCID(job.Key, 16), count)
			} else {
				log.Printf("[Replication] Using target shard %s peer count for custodial file %s: %d peers", 
					targetShard, truncateCID(job.Key, 16), count)
			}
		}
	} else {
		// Shard manager not available - this shouldn't happen in normal operation
		log.Printf("[Replication] Warning: ShardManager not available for file %s, replication count unknown", 
			truncateCID(job.Key, 16))
		count = 0
	}

	resultQueue <- CheckResult{
		Job:       job,
		Count:     count,
		Err:       nil, // No DHT errors - replication is managed via pubsub
		FromCache: false,
		Duration:  time.Since(start),
	}
}

// Stage 3: Reconciler
func (rm *ReplicationManager) runReconciler(ctx context.Context, resultQueue <-chan CheckResult) {
	for result := range resultQueue {
		rm.reconcile(ctx, result)
	}
}

func (rm *ReplicationManager) reconcile(ctx context.Context, result CheckResult) {
	job := result.Job
	key := job.Key
	count := result.Count

	// Always unlock at the end of reconciliation
	defer rm.checkingFiles.Unlock(key)
	rm.lastCheckTime.Set(key, time.Now())

	incrementMetric(&metrics.replicationChecks)

	// Handle DHT query failures
	if result.Err == context.DeadlineExceeded {
		rm.handleDHTQueryFailure(key)
		return
	}

	// Process successful replication check
	rm.processReplicationResult(ctx, job, count, result.FromCache)
}

// handleDHTQueryFailure handles DHT query timeout errors by applying backoff.
func (rm *ReplicationManager) handleDHTQueryFailure(key string) {
	incrementMetric(&metrics.replicationFailures)
	rm.storageMgr.recordFailedOperation(key)
	log.Printf("[Backoff] DHT query timeout for %s, applying backoff", truncateCID(key, 16))
}

// processReplicationResult processes a successful replication check result and handles
// under-replication, over-replication, and custodial handoff scenarios.
func (rm *ReplicationManager) processReplicationResult(ctx context.Context, job CheckJob, count int, fromCache bool) {
	key := job.Key

	// Log zero providers for responsible files (may be new/unreplicated)
	if count == 0 && job.Responsible && !fromCache {
		log.Printf("[Replication] Zero providers found for responsible file %s (may be new/unreplicated)", truncateCID(key, 16))
	}

	// Update cache and metrics
	if !fromCache {
		rm.replicationCache.Set(key, count)
	}

	incrementMetric(&metrics.replicationSuccess)
	rm.storageMgr.failedOperations.Clear(key)
	rm.storageMgr.fileReplicationLevels.Set(key, count)
	rm.updateReplicationMetrics(count)

	// Handle replication scenarios
	if count < MinReplication {
		rm.handleUnderReplication(ctx, job, count)
	} else {
		// Clear pending verifications if healthy
		rm.pendingVerifications.Remove(key)
	}

	// Handle over-replication: responsible nodes keep files pinned, custodial nodes unpin
	if count > MaxReplication && job.Pinned {
		rm.handleOverReplication(ctx, job, count)
	}

	// Handle custodial handoff: unpin when replication is sufficient (but not if already handled by over-replication)
	if !job.Responsible && count >= MinReplication && count <= MaxReplication && job.Pinned {
		rm.handleCustodialHandoff(ctx, job, count)
	}
}

func (rm *ReplicationManager) updateReplicationMetrics(count int) {
	if count >= MinReplication && count <= MaxReplication {
		updateMetrics(func() {
			metrics.filesAtTargetReplication++
			// Track convergence
			metrics.filesConvergedTotal++
			metrics.filesConvergedThisPeriod++
		})
	} else if count < MinReplication {
		updateMetrics(func() {
			metrics.lowReplicationFiles++
		})
	} else {
		updateMetrics(func() {
			metrics.highReplicationFiles++
		})
	}
}

func (rm *ReplicationManager) handleUnderReplication(ctx context.Context, job CheckJob, count int) {
	key := job.Key
	pending, hasPending := rm.pendingVerifications.Get(key)

	if hasPending {
		if time.Now().After(pending.verifyTime) {
			log.Printf("[Replication] Verified under-replication (%d/%d). Triggering NEED for %s", count, MinReplication, key[:min(16, len(key))]+"...")

			// If responsible and missing, re-pin and provide
			if pending.responsible && !pending.pinned {
				if err := rm.pinKeyV2(ctx, key, job.ManifestCID); err != nil {
					logError("Replication", "pin file during under-replication handling", key, err)
					rm.storageMgr.recordFailedOperation(key)
					// Continue to broadcast NEED even if pinning fails
				} else {
					rm.storageMgr.provideFile(ctx, key)
				}
			}

			// Broadcast NEED
			rr := schema.ReplicationRequest{
				Type:        schema.MessageTypeReplicationRequest,
				ManifestCID: job.ManifestCID,
				Priority:    1,
				Deadline:    0,
			}
			if err := signProtocolMessage(&rr); err != nil {
				log.Printf("[Sig] Failed to sign ReplicationRequest: %v", err)
			}
			b, err := rr.MarshalCBOR()
			if err != nil {
				logError("Replication", "marshal ReplicationRequest", key, err)
				return
			}
			rm.shardMgr.PublishToShardCBOR(b, rm.shardMgr.currentShard) // Uses current shard for responsible nodes
			rm.pendingVerifications.Remove(key)
		} else {
			log.Printf("[Replication] Under-replication detected (%d/%d) but verification pending for %s", count, MinReplication, key[:min(16, len(key))]+"...")
		}
	} else {
		// Schedule verification (Hysteresis)
		delay := getRandomVerificationDelay()
		verifyTime := time.Now().Add(delay)
		log.Printf("[Replication] Under-replication detected (%d/%d). Scheduling verification for %s in %v", count, MinReplication, key[:min(16, len(key))]+"...", delay)

		rm.pendingVerifications.Add(key, &verificationPending{
			firstCount:     count,
			firstCheckTime: time.Now(),
			verifyTime:     verifyTime,
			responsible:    job.Responsible,
			pinned:         job.Pinned,
		})

		// Note: The scheduler will pick this up again naturally.
		// We rely on the fact that the scheduler runs every minute.
	}
}

func (rm *ReplicationManager) handleOverReplication(ctx context.Context, job CheckJob, count int) {
	// Responsible nodes should NEVER unpin their files, even if over-replicated.
	// They are responsible for maintaining the file. Only custodial nodes should unpin.
	if !job.Responsible {
		// Custodial node: unpin if over-replicated
		log.Printf("[Replication] High Redundancy (%d/%d). Unpinning custodial file %s", count, MaxReplication, truncateCID(job.Key, 16))
		rm.UnpinFile(ctx, job.Key, job.ManifestCID)
		return
	}

	// Responsible node: keep file pinned, but ask others to reduce replication
	if count > MaxReplication+3 {
		excessCount := count - MaxReplication
		log.Printf("[Replication] Excessive replication (%d/%d, excess: %d). Broadcasting UnreplicateRequest for %s (keeping responsible copy pinned)", count, MaxReplication, excessCount, truncateCID(job.Key, 16))

		// Broadcast UnreplicateRequest to coordinate peers dropping excess replicas
		ur := schema.UnreplicateRequest{
			Type:         schema.MessageTypeUnreplicateRequest,
			ManifestCID:  job.ManifestCID,
			ExcessCount:  excessCount,
			CurrentCount: count,
		}
		if err := signProtocolMessage(&ur); err != nil {
			log.Printf("[Sig] Failed to sign UnreplicateRequest: %v", err)
		}
		b, err := ur.MarshalCBOR()
		if err != nil {
			logError("Replication", "marshal UnreplicateRequest", job.Key, err)
			return
		}
		rm.shardMgr.PublishToShardCBOR(b, rm.shardMgr.currentShard) // Responsible node broadcasts to its shard

		// Keep file in tracking to monitor if replication comes down
		log.Printf("[Replication] Replication excessive (%d), keeping responsible copy pinned and monitoring reduction", count)
	} else {
		log.Printf("[Replication] High Redundancy (%d/%d) for responsible file %s. Keeping pinned (responsible node must maintain copy)", count, MaxReplication, truncateCID(job.Key, 16))
	}
}

func (rm *ReplicationManager) handleCustodialHandoff(ctx context.Context, job CheckJob, count int) {
	log.Printf("[Replication] Handoff Complete (%d/%d copies found). Unpinning custodial file %s", count, MinReplication, job.Key)
	rm.UnpinFile(ctx, job.Key, job.ManifestCID)
	
	// Tourist Mode Exit: Leave the target shard
	// Calculate target shard based on file
	payloadCIDStr := getPayloadCIDForShardAssignment(ctx, rm.ipfsClient, job.Key)
	stableHex := keyToStableHex(payloadCIDStr)
	
	// Determine depth: we assume the depth matches our current shard depth
	// This is an approximation, but typically nodes operate at similar depths.
	rm.shardMgr.mu.RLock()
	currentDepth := len(rm.shardMgr.currentShard)
	rm.shardMgr.mu.RUnlock()
	
	if currentDepth == 0 {
		currentDepth = 1
	}
	targetShard := getHexBinaryPrefix(stableHex, currentDepth)
	
	log.Printf("[Replication] Tourist Mode: Leaving target shard %s for file %s", targetShard, truncateCID(job.Key, 16))
	rm.shardMgr.LeaveShard(targetShard)
}

// Helpers

func getRandomVerificationDelay() time.Duration {
	n, _ := cryptorand.Int(cryptorand.Reader, big.NewInt(10000))
	// 0.5 * Delay + Random(Delay) -> Range [0.5*Delay, 1.5*Delay]
	base := int64(ReplicationVerificationDelay) / 2
	jitter := n.Int64() % int64(ReplicationVerificationDelay)
	return time.Duration(base + jitter)
}

func (rm *ReplicationManager) pinKeyV2(ctx context.Context, key string, manifestCID cid.Cid) error {
	if rm.ipfsClient != nil {
		if err := rm.ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
			log.Printf("[IPFS] PinRecursive failed for %s: %v", key[:min(16, len(key))]+"...", err)
			rm.storageMgr.recordFailedOperation(key)
			return err
		}
	}
	if !rm.storageMgr.pinFile(key) {
		logWarning("Replication", "Failed to track pinned file in local state", key)
		// Continue despite tracking failure - file is pinned in IPFS
	}
	return nil
}

func (rm *ReplicationManager) UnpinFile(ctx context.Context, key string, manifestCID cid.Cid) {
	if rm.ipfsClient != nil {
		if err := rm.ipfsClient.UnpinRecursive(ctx, manifestCID); err != nil {
			log.Printf("[IPFS] UnpinRecursive failed for %s: %v", key[:min(16, len(key))]+"...", err)
		}
	}
	rm.storageMgr.unpinFile(key)
}

// replicateFileFromRequest attempts to fetch and pin a ResearchObject when
// receiving a ReplicationRequest.
func (rm *ReplicationManager) replicateFileFromRequest(ctx context.Context, manifestCID cid.Cid, sender peer.ID, responsible bool) (bool, error) {
	if rm.ipfsClient == nil {
		return false, fmt.Errorf("IPFS client not initialized")
	}

	manifestCIDStr := manifestCID.String()

	// Step 1: Check if already pinned
	if rm.storageMgr.isPinned(manifestCIDStr) {
		log.Printf("[Replication] File %s already pinned, skipping fetch", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
		return true, nil
	}

	// Step 2: Check disk usage (respect storage limits)
	if !canAcceptCustodialFile() {
		usage := checkDiskUsage()
		log.Printf("[Replication] Cannot replicate %s: disk usage high (%.1f%%)",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", usage)
		return false, fmt.Errorf("disk usage too high: %.1f%%", usage)
	}

	// Step 3: Create timeout context for replication
	replCtx, replCancel := context.WithTimeout(ctx, AutoReplicationTimeout)
	defer replCancel()

	if sender != "" {
		log.Printf("[Replication] Using sender %s as provider hint for %s", sender.String(), manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	}

	// Step 4: Fetch manifest block
	manifestBytes, err := rm.ipfsClient.GetBlock(replCtx, manifestCID)
	if err != nil {
		log.Printf("[Replication] Failed to fetch manifest %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		return false, fmt.Errorf("fetch manifest: %w", err)
	}

	// Step 5: Decode and verify ResearchObject
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		log.Printf("[Replication] Invalid manifest %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		return false, fmt.Errorf("decode manifest: %w", err)
	}

	// Step 6: Check file size limit
	if AutoReplicationMaxSize > 0 && ro.TotalSize > AutoReplicationMaxSize {
		log.Printf("[Replication] File %s too large (%d bytes, max: %d), skipping replication",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", ro.TotalSize, AutoReplicationMaxSize)
		return false, fmt.Errorf("file too large: %d bytes (max: %d)", ro.TotalSize, AutoReplicationMaxSize)
	}

	// Step 7: Verify signature and authorization
	if !signaturesDisabled() {
		if err := authorizePeer(ro.IngestedBy); err != nil {
			if handleSignatureError("ResearchObject trust", err) {
				return false, fmt.Errorf("unauthorized peer: %w", err)
			}
		} else if rm.shardMgr != nil && rm.shardMgr.h != nil {
			unsigned, err := ro.MarshalCBORForSigning()
			if err != nil {
				if handleSignatureError("ResearchObject marshal", err) {
					return false, fmt.Errorf("marshal error: %w", err)
				}
			} else if handleSignatureError("ResearchObject signature",
				verifySignedObject(rm.shardMgr.h, ro.IngestedBy, ro.Timestamp, ro.Signature, unsigned)) {
				return false, fmt.Errorf("signature verification failed")
			}
		}
	}

	// Step 8: Check BadBits
	if isCIDBlocked(manifestCIDStr, NodeCountry) {
		log.Printf("[Replication] Refused to replicate blocked CID: %s", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
		return false, fmt.Errorf("CID blocked in country %s", NodeCountry)
	}

	// Step 9: Fetch payload (IPFS will fetch recursively via Bitswap)
	log.Printf("[Replication] Fetching and pinning payload %s for manifest %s",
		ro.Payload.String()[:min(16, len(ro.Payload.String()))]+"...", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	if err := rm.ipfsClient.PinRecursive(replCtx, ro.Payload); err != nil {
		log.Printf("[Replication] Failed to pin payload %s: %v",
			ro.Payload.String()[:min(16, len(ro.Payload.String()))]+"...", err)
		return false, fmt.Errorf("pin payload: %w", err)
	}

	// Step 10: Pin manifest recursively (ensures entire DAG is pinned)
	if err := rm.ipfsClient.PinRecursive(replCtx, manifestCID); err != nil {
		log.Printf("[Replication] Failed to pin manifest %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		// Unpin payload on failure
		if unpinErr := rm.ipfsClient.UnpinRecursive(ctx, ro.Payload); unpinErr != nil {
			logWarningWithContext("Replication", "Failed to cleanup payload after manifest pin failure", ro.Payload.String(), "cleanup")
		}
		return false, fmt.Errorf("pin manifest: %w", err)
	}

	// Step 11: Verify payload size (liar detection)
	actualSize, err := rm.ipfsClient.GetFileSize(replCtx, ro.Payload)
	if err != nil {
		log.Printf("[Replication] Warning: failed to verify payload size for %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		// Continue despite error - size verification is best-effort
	} else if ro.TotalSize != 0 && actualSize != ro.TotalSize {
		log.Printf("[Security] Liar detection: payload size mismatch for %s (manifest=%d, actual=%d). Unpinning.",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", ro.TotalSize, actualSize)
		if err := rm.ipfsClient.UnpinRecursive(ctx, manifestCID); err != nil {
			logWarningWithContext("Security", "Failed to unpin manifest after liar detection", manifestCIDStr, "cleanup")
		}
		if err := rm.ipfsClient.UnpinRecursive(ctx, ro.Payload); err != nil {
			logWarningWithContext("Security", "Failed to unpin payload after liar detection", ro.Payload.String(), "cleanup")
		}
		return false, fmt.Errorf("payload size mismatch: manifest=%d, actual=%d", ro.TotalSize, actualSize)
	}

	// Step 12: Track in local state
	if !rm.storageMgr.pinFile(manifestCIDStr) {
		log.Printf("[Replication] Failed to track ManifestCID: %s", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	}

	// Step 13: Provide to DHT (only if responsible)
	if responsible {
		provideCtx, provideCancel := context.WithTimeout(context.Background(), DHTProvideTimeout)
		go func() {
			defer provideCancel()
			rm.storageMgr.provideFile(provideCtx, manifestCIDStr)
		}()
	} else {
		log.Printf("[Replication] Not responsible for %s, skipping DHT announcement (pinned for local use only)", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	}

	// Step 14: Add to known files
	rm.storageMgr.addKnownFile(manifestCIDStr)

	log.Printf("[Replication] Successfully replicated file %s (payload: %s, size: %d bytes)",
		manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", ro.Payload.String()[:min(16, len(ro.Payload.String()))]+"...", ro.TotalSize)

	return true, nil
}

// runReplicationCacheCleanup periodically cleans up expired cache entries
func (rm *ReplicationManager) runReplicationCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-ReplicationCacheTTL)
			removed := rm.replicationCache.Cleanup(cutoff)
			if removed > 0 {
				log.Printf("[Cache] Cleaned up %d expired replication cache entries", removed)
			}
		}
	}
}

func (rm *ReplicationManager) verifyResearchObjectLocal(ctx context.Context, manifestCID cid.Cid) (bool, error) {
	if rm.ipfsClient == nil {
		return false, fmt.Errorf("IPFS client not initialized")
	}

	// 1. Verify manifest block exists and is a valid ResearchObject
	manifestBytes, err := rm.ipfsClient.GetBlock(ctx, manifestCID)
	if err != nil {
		return false, fmt.Errorf("manifest block missing: %w", err)
	}

	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		return false, fmt.Errorf("invalid manifest CBOR: %w", err)
	}

	// 2. Verify payload is pinned
	isPinned, err := rm.ipfsClient.IsPinned(ctx, ro.Payload)
	if err != nil {
		return false, fmt.Errorf("check payload pin: %w", err)
	}
	if !isPinned {
		return false, nil
	}

	// 3. Verify payload size (Liar Detection)
	// Only check if we have a size hint
	if ro.TotalSize > 0 {
		size, err := rm.ipfsClient.GetFileSize(ctx, ro.Payload)
		if err != nil {
			// If we can't get size, assume it's okay but warn?
			// Or fail safe? Let's fail safe for integrity.
			return false, fmt.Errorf("check payload size: %w", err)
		}
		if size != ro.TotalSize {
			return false, fmt.Errorf("size mismatch: expected %d, got %d", ro.TotalSize, size)
		}
	}

	return true, nil
}
