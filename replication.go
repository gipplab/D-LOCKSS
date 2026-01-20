package main

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/pkg/schema"
)

// withReplicationWorker executes a function within the replication worker pool.
// Returns true if the function was scheduled, false if context was cancelled.
func withReplicationWorker(ctx context.Context, fn func()) bool {
	select {
	case replicationWorkers <- struct{}{}:
		go func() {
			defer func() { <-replicationWorkers }()
			fn()
		}()
		return true
	case <-ctx.Done():
		return false
	}
}

// checkReplication checks the replication level for a file/manifest.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func checkReplication(ctx context.Context, key string) {
	if !checkingFiles.TryLock(key) {
		return
	}

	defer func() {
		checkingFiles.Unlock(key)
		lastCheckTime.Set(key, time.Now())
	}()

	lastCheck, exists := lastCheckTime.Get(key)
	if exists {
		timeSinceLastCheck := time.Since(lastCheck)
		if timeSinceLastCheck < ReplicationCheckCooldown {
			return
		}
	}

	removedTime, wasRemoved := recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return
	}

	responsible := shardMgr.AmIResponsibleFor(key)
	pinned := isPinned(key)

	if !responsible && !pinned {
		removeKnownFile(key)
		return
	}

	manifestCID, err := keyToCID(key)
	if err != nil {
		log.Printf("[Error] Invalid key format in checkReplication: %s, error: %v", key, err)
		incrementMetric(&metrics.replicationChecks)
		incrementMetric(&metrics.replicationFailures)
		return
	}

	// Only count as a check if we're actually going to verify replication
	incrementMetric(&metrics.replicationChecks)

	// Phase 3: Verify DAG completeness and perform liar detection.
	// If our local state says it's pinned but the manifest/payload isn't actually present,
	// treat it as unpinned (and clean up local pin state).
	if pinned {
		ok, err := verifyResearchObjectLocal(ctx, manifestCID)
		if err != nil {
			log.Printf("[Replication] Local verification error for %s: %v", key[:min(16, len(key))]+"...", err)
		}
		if !ok {
			log.Printf("[Replication] Local DAG incomplete or invalid for %s; unpinning local state", key[:min(16, len(key))]+"...")
			unpinKeyV2(ctx, key, manifestCID)
			pinned = false
		}
	}

	// Check cache first to avoid unnecessary DHT queries
	var count int
	var fromCache bool
	cachedCount, cachedAt, hasCache := replicationCache.GetWithAge(key)

	if hasCache {
		age := time.Since(cachedAt)
		if age < ReplicationCacheTTL {
			count = cachedCount
			fromCache = true
			log.Printf("[Replication] Using cached replication count for %s: %d (cached %v ago)", key[:min(16, len(key))]+"...", count, age.Round(time.Second))
		}
	}

	// Query DHT if cache miss or expired
	if !fromCache {
		dhtCtx, dhtCancel := context.WithTimeout(ctx, 2*time.Minute)
		defer dhtCancel()

		incrementMetric(&metrics.dhtQueries)
		log.Printf("[Replication] Checking replication for %s (responsible: %v, pinned: %v)", key[:min(16, len(key))]+"...", responsible, pinned)

		provs := globalDHT.FindProvidersAsync(dhtCtx, manifestCID, 0)
		count = 0
		maxCount := DHTMaxSampleSize
	providerLoop:
		for range provs {
			count++
			if count >= maxCount {
				break
			}
			select {
			case <-dhtCtx.Done():
				incrementMetric(&metrics.dhtQueryTimeouts)
				log.Printf("[Warning] DHT query timeout for %s, found %d providers", key[:min(16, len(key))]+"...", count)
				break providerLoop
			default:
			}
		}

		log.Printf("[Replication] Found %d providers for %s (target: %d-%d, sampled up to %d)", count, key[:min(16, len(key))]+"...", MinReplication, MaxReplication, maxCount)

		if dhtCtx.Err() == context.DeadlineExceeded || (count == 0 && responsible) {
			incrementMetric(&metrics.replicationFailures)
			recordFailedOperation(key)
			log.Printf("[Backoff] DHT query failed for %s, applying backoff", key[:min(16, len(key))]+"...")
			return
		}

		// Cache successful result
		replicationCache.Set(key, count)
	}

	incrementMetric(&metrics.replicationSuccess)
	clearBackoff(key)

	fileReplicationLevels.Set(key, count)

	if count >= MinReplication && count <= MaxReplication {
		if fileConvergenceTime.SetIfNotExists(key, time.Now()) {
			updateMetrics(func() {
				metrics.filesConvergedTotal++
				metrics.filesConvergedThisPeriod++
			})
		}
	}

	if count < MinReplication {
		updateMetrics(func() {
			metrics.lowReplicationFiles++
		})
	} else if count > MaxReplication {
		updateMetrics(func() {
			metrics.highReplicationFiles++
		})
	} else if count >= MinReplication && count <= MaxReplication {
		updateMetrics(func() {
			metrics.filesAtTargetReplication++
		})
	}

	if count < MinReplication {
		pending, hasPending := pendingVerifications.Get(key)

		if hasPending {
			if time.Now().After(pending.verifyTime) {
				log.Printf("[Replication] Verification check for %s (first count: %d, current count: %d)", key[:min(16, len(key))]+"...", pending.firstCount, count)
				if count < MinReplication {
					log.Printf("[Replication] Verified under-replication (%d/%d). Triggering NEED for %s", count, MinReplication, key[:min(16, len(key))]+"...")
					if pending.responsible && !pending.pinned {
						_ = pinKeyV2(ctx, key, manifestCID)
						provideFile(ctx, key)
					}
					rr := schema.ReplicationRequest{
						Type:        schema.MessageTypeReplicationRequest,
						ManifestCID: manifestCID,
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
				} else {
					log.Printf("[Replication] Verification shows adequate replication (%d/%d). Canceling NEED for %s", count, MinReplication, key[:min(16, len(key))]+"...")
				}
				pendingVerifications.Remove(key)
			} else {
				log.Printf("[Replication] Under-replication detected (%d/%d) but verification pending for %s", count, MinReplication, key[:min(16, len(key))]+"...")
			}
		} else {
			delay := getRandomVerificationDelay()
			verifyTime := time.Now().Add(delay)
			log.Printf("[Replication] Under-replication detected (%d/%d). Scheduling verification for %s in %v", count, MinReplication, key[:min(16, len(key))]+"...", delay)

			pendingVerifications.Add(key, &verificationPending{
				firstCount:     count,
				firstCheckTime: time.Now(),
				verifyTime:     verifyTime,
				responsible:    responsible,
				pinned:         pinned,
			})

			go func(k string, verifyAt time.Time) {
				time.Sleep(time.Until(verifyAt))
				withReplicationWorker(ctx, func() {
					checkReplication(ctx, k)
				})
			}(key, verifyTime)
		}
	} else {
		pendingVerifications.Remove(key)
	}

	if responsible && count > MaxReplication && pinned {
		log.Printf("[Replication] High Redundancy (%d/%d). Unpinning %s (Monitoring only)", count, MaxReplication, key)
		unpinKeyV2(ctx, key, manifestCID)
		if count > MaxReplication+3 {
			log.Printf("[Replication] Replication stable (%d), removing from tracking", count)
			removeKnownFile(key)
		}
	}

	if !responsible && count >= MinReplication && pinned {
		log.Printf("[Replication] Handoff Complete (%d/%d copies found). Unpinning custodial file %s", count, MinReplication, key)
		unpinKeyV2(ctx, key, manifestCID)
	}
}

func pinKeyV2(ctx context.Context, key string, manifestCID cid.Cid) error {
	if ipfsClient != nil {
		_ = ipfsClient.PinRecursive(ctx, manifestCID)
	}
	_ = pinFileV2(key)
	return nil
}

func unpinKeyV2(ctx context.Context, key string, manifestCID cid.Cid) {
	if ipfsClient != nil {
		_ = ipfsClient.UnpinRecursive(ctx, manifestCID)
	}
	unpinFile(key)
}

// verifyResearchObjectLocal verifies:
// - manifest block exists and decodes as ResearchObject
// - payload is pinned
// - payload size matches TotalSize (liar detection)
// Returns false if incomplete/invalid.
func verifyResearchObjectLocal(ctx context.Context, manifestCID cid.Cid) (bool, error) {
	if ipfsClient == nil {
		return true, nil
	}

	manifestBytes, err := ipfsClient.GetBlock(ctx, manifestCID)
	if err != nil {
		return false, err
	}

	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		return false, fmt.Errorf("decode ResearchObject: %w", err)
	}

	// Verify ResearchObject signature (integrity + authorization policy).
	// In open networks you can run SignatureMode=warn to observe without enforcing.
	if !signaturesDisabled() {
		if err := authorizePeer(ro.IngestedBy); err != nil {
			if handleSignatureError("ResearchObject trust", err) {
				return false, nil
			}
		} else if shardMgr != nil && shardMgr.h != nil {
			unsigned, err := ro.MarshalCBORForSigning()
			if err != nil {
				if handleSignatureError("ResearchObject marshal for signing", err) {
					return false, nil
				}
			} else if handleSignatureError("ResearchObject signature", verifySignedObject(shardMgr.h, ro.IngestedBy, ro.Timestamp, ro.Signature, unsigned)) {
				return false, nil
			}
		}
	}

	manifestPinned, err := ipfsClient.IsPinned(ctx, manifestCID)
	if err != nil {
		return false, err
	}
	if !manifestPinned {
		return false, nil
	}

	payloadPinned, err := ipfsClient.IsPinned(ctx, ro.Payload)
	if err != nil {
		return false, err
	}
	if !payloadPinned {
		return false, nil
	}

	actualSize, err := ipfsClient.GetFileSize(ctx, ro.Payload)
	if err != nil {
		return false, err
	}
	if ro.TotalSize != 0 && actualSize != ro.TotalSize {
		log.Printf("[Security] Liar detection: payload size mismatch for %s (manifest=%d, actual=%d). Unpinning.",
			manifestCID.String()[:min(16, len(manifestCID.String()))]+"...", ro.TotalSize, actualSize)
		_ = ipfsClient.UnpinRecursive(ctx, manifestCID)
		return false, nil
	}

	return true, nil
}

func getRandomVerificationDelay() time.Duration {
	baseDelay := ReplicationVerificationDelay
	jitterRange := int64(baseDelay.Seconds() * 0.2)
	if jitterRange < 1 {
		jitterRange = 1
	}
	jitter, err := cryptorand.Int(cryptorand.Reader, big.NewInt(jitterRange*2))
	if err != nil {
		jitter = big.NewInt(0)
	}
	jitterSeconds := jitter.Int64() - jitterRange
	return baseDelay + time.Duration(jitterSeconds)*time.Second
}

func runReplicationChecker(ctx context.Context) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			knownFilesMap := knownFiles.All()
			for key := range knownFilesMap {
				if shouldSkipDueToBackoff(key) {
					continue
				}

				pending, hasPending := pendingVerifications.Get(key)
				if hasPending && time.Now().Before(pending.verifyTime) {
					continue
				}

				if !withReplicationWorker(ctx, func() {
					checkReplication(ctx, key)
				}) {
					return
				}
			}
		}
	}
}

// runReplicationCacheCleanup periodically removes expired entries from the replication cache
// to prevent unbounded memory growth.
func runReplicationCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(ReplicationCacheTTL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			cutoff := now.Add(-ReplicationCacheTTL)
			removed := replicationCache.Cleanup(cutoff)
			if removed > 0 {
				log.Printf("[Cache] Cleaned up %d expired replication cache entries", removed)
			}
		}
	}
}
