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

	"dlockss/pkg/schema"
)

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
func startReplicationPipeline(ctx context.Context) {
	jobQueue := make(chan CheckJob, ReplicationQueueSize)
	resultQueue := make(chan CheckResult, ReplicationQueueSize)

	// Start Scheduler (Stage 1)
	go runScheduler(ctx, jobQueue)

	// Start Network Probers (Stage 2)
	var wg sync.WaitGroup
	for i := 0; i < ReplicationWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runNetworkProber(ctx, jobQueue, resultQueue)
		}()
	}

	// Start Reconciler (Stage 3)
	go runReconciler(ctx, resultQueue)

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
func runScheduler(ctx context.Context, jobQueue chan<- CheckJob) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduleJobs(ctx, jobQueue)
		}
	}
}

func scheduleJobs(ctx context.Context, jobQueue chan<- CheckJob) {
	files := knownFiles.All()
	for key := range files {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Filter: Skip if recently checked or removed
		if !shouldCheckFile(key) {
			continue
		}

		// Use PayloadCID for shard assignment (stable, content-based)
		// ManifestCID includes timestamp/metadata and changes on every ingestion
		payloadCIDStr := getPayloadCIDForShardAssignment(ctx, key)
		responsible := shardMgr.AmIResponsibleFor(payloadCIDStr)
		pinned := isPinned(key)

		// Filter: Cleanup if not responsible and not pinned
		if !responsible && !pinned {
			removeKnownFile(key)
			continue
		}

		// Filter: TryLock to prevent duplicate scheduling
		if !checkingFiles.TryLock(key) {
			continue
		}

		manifestCID, err := keyToCID(key)
		if err != nil {
			log.Printf("[Error] Invalid key format in scheduler: %s, error: %v", key, err)
			checkingFiles.Unlock(key)
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
			checkingFiles.Unlock(key)
		}
	}
}

func shouldCheckFile(key string) bool {
	lastCheck, exists := lastCheckTime.Get(key)
	if exists && time.Since(lastCheck) < ReplicationCheckCooldown {
		return false
	}

	removedTime, wasRemoved := recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return false
	}
	return true
}

// Stage 2: Network Prober
func runNetworkProber(ctx context.Context, jobQueue <-chan CheckJob, resultQueue chan<- CheckResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobQueue:
			processJob(ctx, job, resultQueue)
		}
	}
}

func processJob(ctx context.Context, job CheckJob, resultQueue chan<- CheckResult) {
	start := time.Now()

	// Local Verification (Liar Detection)
	if job.Pinned {
		ok, err := verifyResearchObjectLocal(ctx, job.ManifestCID)
		if err != nil {
			log.Printf("[Replication] Local verification error for %s: %v", job.Key[:min(16, len(job.Key))]+"...", err)
		}
		if !ok {
			log.Printf("[Replication] Local DAG incomplete or invalid for %s; unpinning local state", job.Key[:min(16, len(job.Key))]+"...")
			unpinKeyV2(ctx, job.Key, job.ManifestCID)
			job.Pinned = false // Update job state
		}
	}

	// Check Cache
	cachedCount, cachedAt, hasCache := replicationCache.GetWithAge(job.Key)
	if hasCache && time.Since(cachedAt) < ReplicationCacheTTL {
		resultQueue <- CheckResult{
			Job:       job,
			Count:     cachedCount,
			FromCache: true,
			Duration:  time.Since(start),
		}
		return
	}

	// Query DHT
	dhtCtx, dhtCancel := context.WithTimeout(ctx, DHTQueryTimeout)
	defer dhtCancel()

	incrementMetric(&metrics.dhtQueries)
	provs := globalDHT.FindProvidersAsync(dhtCtx, job.ManifestCID, 0)
	count := 0
	maxCount := DHTMaxSampleSize

	for range provs {
		count++
		if count >= maxCount {
			break
		}
	}

	if dhtCtx.Err() == context.DeadlineExceeded {
		incrementMetric(&metrics.dhtQueryTimeouts)
	}

	resultQueue <- CheckResult{
		Job:       job,
		Count:     count,
		Err:       dhtCtx.Err(),
		FromCache: false,
		Duration:  time.Since(start),
	}
}

// Stage 3: Reconciler
func runReconciler(ctx context.Context, resultQueue <-chan CheckResult) {
	for result := range resultQueue {
		reconcile(ctx, result)
	}
}

func reconcile(ctx context.Context, result CheckResult) {
	job := result.Job
	key := job.Key
	count := result.Count

	// Always unlock at the end of reconciliation
	defer checkingFiles.Unlock(key)
	lastCheckTime.Set(key, time.Now())

	incrementMetric(&metrics.replicationChecks)

	// Only apply backoff for actual DHT query failures (timeouts), not for legitimate zero-count results
	// Zero count with no cache might mean the file is new/unreplicated, which should trigger replication
	// requests rather than backoff. Backoff should be reserved for network/DHT failures.
	if result.Err == context.DeadlineExceeded {
		incrementMetric(&metrics.replicationFailures)
		recordFailedOperation(key)
		log.Printf("[Backoff] DHT query timeout for %s, applying backoff", key[:min(16, len(key))]+"...")
		return
	}

	// If count is 0 and we're responsible, this might be under-replication, not a DHT failure
	// Let the under-replication handler deal with it (it will trigger replication requests)
	if count == 0 && job.Responsible && !result.FromCache {
		// Don't apply backoff - this is likely legitimate under-replication
		// The under-replication handler will deal with it below
		log.Printf("[Replication] Zero providers found for responsible file %s (may be new/unreplicated)", key[:min(16, len(key))]+"...")
	}

	if !result.FromCache {
		replicationCache.Set(key, count)
	}

	incrementMetric(&metrics.replicationSuccess)
	clearBackoff(key)
	fileReplicationLevels.Set(key, count)

	// Metrics updates
	updateReplicationMetrics(count)

	// Logic: Under-replication
	if count < MinReplication {
		handleUnderReplication(ctx, job, count)
	} else {
		// Clear pending verifications if healthy
		pendingVerifications.Remove(key)
	}

	// Logic: Over-replication
	if job.Responsible && count > MaxReplication && job.Pinned {
		handleOverReplication(ctx, job, count)
	}

	// Logic: Custodial Handoff
	if !job.Responsible && count >= MinReplication && job.Pinned {
		handleCustodialHandoff(ctx, job, count)
	}
}

func updateReplicationMetrics(count int) {
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

func handleUnderReplication(ctx context.Context, job CheckJob, count int) {
	key := job.Key
	pending, hasPending := pendingVerifications.Get(key)

	if hasPending {
		if time.Now().After(pending.verifyTime) {
			log.Printf("[Replication] Verified under-replication (%d/%d). Triggering NEED for %s", count, MinReplication, key[:min(16, len(key))]+"...")

			// If responsible and missing, re-pin and provide
			if pending.responsible && !pending.pinned {
				_ = pinKeyV2(ctx, key, job.ManifestCID)
				provideFile(ctx, key)
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
				log.Printf("[Error] Failed to marshal ReplicationRequest: %v", err)
				return
			}
			shardMgr.PublishToShardCBOR(b)
			pendingVerifications.Remove(key)
		} else {
			log.Printf("[Replication] Under-replication detected (%d/%d) but verification pending for %s", count, MinReplication, key[:min(16, len(key))]+"...")
		}
	} else {
		// Schedule verification (Hysteresis)
		delay := getRandomVerificationDelay()
		verifyTime := time.Now().Add(delay)
		log.Printf("[Replication] Under-replication detected (%d/%d). Scheduling verification for %s in %v", count, MinReplication, key[:min(16, len(key))]+"...", delay)

		pendingVerifications.Add(key, &verificationPending{
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

func handleOverReplication(ctx context.Context, job CheckJob, count int) {
	log.Printf("[Replication] High Redundancy (%d/%d). Unpinning %s (Monitoring only)", count, MaxReplication, job.Key)
	unpinKeyV2(ctx, job.Key, job.ManifestCID)
	if count > MaxReplication+3 {
		excessCount := count - MaxReplication
		log.Printf("[Replication] Excessive replication (%d/%d, excess: %d). Broadcasting UnreplicateRequest for %s", count, MaxReplication, excessCount, job.Key[:min(16, len(job.Key))]+"...")

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
			log.Printf("[Error] Failed to marshal UnreplicateRequest: %v", err)
			return
		}
		shardMgr.PublishToShardCBOR(b)

		// Keep file in tracking to monitor if replication comes down
		log.Printf("[Replication] Replication excessive (%d), keeping in tracking to monitor reduction", count)
	}
}

func handleCustodialHandoff(ctx context.Context, job CheckJob, count int) {
	log.Printf("[Replication] Handoff Complete (%d/%d copies found). Unpinning custodial file %s", count, MinReplication, job.Key)
	unpinKeyV2(ctx, job.Key, job.ManifestCID)
}

// Helpers

func getRandomVerificationDelay() time.Duration {
	n, _ := cryptorand.Int(cryptorand.Reader, big.NewInt(10000))
	// 0.5 * Delay + Random(Delay) -> Range [0.5*Delay, 1.5*Delay]
	base := int64(ReplicationVerificationDelay) / 2
	jitter := n.Int64() % int64(ReplicationVerificationDelay)
	return time.Duration(base + jitter)
}

func pinKeyV2(ctx context.Context, key string, manifestCID cid.Cid) error {
	if ipfsClient != nil {
		if err := ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
			log.Printf("[IPFS] PinRecursive failed for %s: %v", key[:min(16, len(key))]+"...", err)
			recordFailedOperation(key)
			return err
		}
	}
	_ = pinFileV2(key)
	return nil
}

func unpinKeyV2(ctx context.Context, key string, manifestCID cid.Cid) {
	if ipfsClient != nil {
		if err := ipfsClient.UnpinRecursive(ctx, manifestCID); err != nil {
			log.Printf("[IPFS] UnpinRecursive failed for %s: %v", key[:min(16, len(key))]+"...", err)
		}
	}
	unpinFile(key)
}

// replicateFileFromRequest attempts to fetch and pin a ResearchObject when
// receiving a ReplicationRequest. If a non-empty sender peer ID is provided,
// it can be used as a provider hint by higher-level components or future
// IPFS client enhancements to prioritize fetching from that peer.
func replicateFileFromRequest(ctx context.Context, manifestCID cid.Cid, sender peer.ID) (bool, error) {
	if ipfsClient == nil {
		return false, fmt.Errorf("IPFS client not initialized")
	}

	manifestCIDStr := manifestCID.String()

	// Step 1: Check if already pinned
	if isPinned(manifestCIDStr) {
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

	// Note: At this layer we only log the sender as a potential provider hint.
	// Future work can extend the IPFS client to take a peer.ID and use IPFS's
	// provider system or direct connections to prefer fetching from that peer.
	if sender != "" {
		log.Printf("[Replication] Using sender %s as provider hint for %s", sender.String(), manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	}

	// Step 4: Fetch manifest block
	manifestBytes, err := ipfsClient.GetBlock(replCtx, manifestCID)
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
		} else if shardMgr != nil && shardMgr.h != nil {
			unsigned, err := ro.MarshalCBORForSigning()
			if err != nil {
				if handleSignatureError("ResearchObject marshal", err) {
					return false, fmt.Errorf("marshal error: %w", err)
				}
			} else if handleSignatureError("ResearchObject signature",
				verifySignedObject(shardMgr.h, ro.IngestedBy, ro.Timestamp, ro.Signature, unsigned)) {
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
	if err := ipfsClient.PinRecursive(replCtx, ro.Payload); err != nil {
		log.Printf("[Replication] Failed to pin payload %s: %v",
			ro.Payload.String()[:min(16, len(ro.Payload.String()))]+"...", err)
		return false, fmt.Errorf("pin payload: %w", err)
	}

	// Step 10: Pin manifest recursively (ensures entire DAG is pinned)
	if err := ipfsClient.PinRecursive(replCtx, manifestCID); err != nil {
		log.Printf("[Replication] Failed to pin manifest %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		// Unpin payload on failure
		_ = ipfsClient.UnpinRecursive(ctx, ro.Payload)
		return false, fmt.Errorf("pin manifest: %w", err)
	}

	// Step 11: Verify payload size (liar detection)
	actualSize, err := ipfsClient.GetFileSize(replCtx, ro.Payload)
	if err != nil {
		log.Printf("[Replication] Warning: failed to verify payload size for %s: %v",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		// Continue despite error - size verification is best-effort
	} else if ro.TotalSize != 0 && actualSize != ro.TotalSize {
		log.Printf("[Security] Liar detection: payload size mismatch for %s (manifest=%d, actual=%d). Unpinning.",
			manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", ro.TotalSize, actualSize)
		_ = ipfsClient.UnpinRecursive(ctx, manifestCID)
		_ = ipfsClient.UnpinRecursive(ctx, ro.Payload)
		return false, fmt.Errorf("payload size mismatch: manifest=%d, actual=%d", ro.TotalSize, actualSize)
	}

	// Step 12: Track in local state
	if !pinFileV2(manifestCIDStr) {
		log.Printf("[Replication] Failed to track ManifestCID: %s", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...")
	}

	// Step 13: Provide to DHT
	provideCtx, provideCancel := context.WithTimeout(context.Background(), DHTProvideTimeout)
	go func() {
		defer provideCancel()
		provideFile(provideCtx, manifestCIDStr)
	}()

	// Step 14: Add to known files
	addKnownFile(manifestCIDStr)

	log.Printf("[Replication] Successfully replicated file %s (payload: %s, size: %d bytes)",
		manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", ro.Payload.String()[:min(16, len(ro.Payload.String()))]+"...", ro.TotalSize)

	return true, nil
}

// runReplicationCacheCleanup periodically cleans up expired cache entries
func runReplicationCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-ReplicationCacheTTL)
			removed := replicationCache.Cleanup(cutoff)
			if removed > 0 {
				log.Printf("[Cache] Cleaned up %d expired replication cache entries", removed)
			}
		}
	}
}

func verifyResearchObjectLocal(ctx context.Context, manifestCID cid.Cid) (bool, error) {
	if ipfsClient == nil {
		return false, fmt.Errorf("IPFS client not initialized")
	}

	// 1. Verify manifest block exists and is a valid ResearchObject
	manifestBytes, err := ipfsClient.GetBlock(ctx, manifestCID)
	if err != nil {
		return false, fmt.Errorf("manifest block missing: %w", err)
	}

	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		return false, fmt.Errorf("invalid manifest CBOR: %w", err)
	}

	// 2. Verify payload is pinned
	isPinned, err := ipfsClient.IsPinned(ctx, ro.Payload)
	if err != nil {
		return false, fmt.Errorf("check payload pin: %w", err)
	}
	if !isPinned {
		return false, nil
	}

	// 3. Verify payload size (Liar Detection)
	// Only check if we have a size hint
	if ro.TotalSize > 0 {
		size, err := ipfsClient.GetFileSize(ctx, ro.Payload)
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
