package main

import (
	"context"
	"log"
	"time"
)

func checkReplication(ctx context.Context, hash string) {
	checkingFiles.Lock()
	if checkingFiles.hashes[hash] {
		checkingFiles.Unlock()
		return
	}
	checkingFiles.hashes[hash] = true
	checkingFiles.Unlock()

	defer func() {
		checkingFiles.Lock()
		delete(checkingFiles.hashes, hash)
		checkingFiles.Unlock()

		lastCheckTime.Lock()
		lastCheckTime.times[hash] = time.Now()
		lastCheckTime.Unlock()
	}()

	lastCheckTime.RLock()
	lastCheck, exists := lastCheckTime.times[hash]
	lastCheckTime.RUnlock()
	if exists {
		timeSinceLastCheck := time.Since(lastCheck)
		if timeSinceLastCheck < ReplicationCheckCooldown {
			return
		}
	}

	recentlyRemoved.RLock()
	removedTime, wasRemoved := recentlyRemoved.hashes[hash]
	recentlyRemoved.RUnlock()
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return
	}

	incrementMetric(&metrics.replicationChecks)

	responsible := shardMgr.AmIResponsibleFor(hash)
	pinned := isPinned(hash)

	if !responsible && !pinned {
		removeKnownFile(hash)
		return
	}

	c, err := hashToCid(hash)
	if err != nil {
		log.Printf("[Error] Invalid hash format in checkReplication: %s, error: %v", hash, err)
		return
	}

	dhtCtx, dhtCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer dhtCancel()

	incrementMetric(&metrics.dhtQueries)
	log.Printf("[Replication] Checking replication for %s (responsible: %v, pinned: %v)", hash[:16]+"...", responsible, pinned)

	provs := globalDHT.FindProvidersAsync(dhtCtx, c, 0)
	count := 0
	maxCount := MaxReplication + 5
providerLoop:
	for range provs {
		count++
		if count >= maxCount {
			break
		}
		select {
		case <-dhtCtx.Done():
			incrementMetric(&metrics.dhtQueryTimeouts)
			log.Printf("[Warning] DHT query timeout for %s, found %d providers", hash[:16]+"...", count)
			break providerLoop
		default:
		}
	}

	log.Printf("[Replication] Found %d providers for %s (target: %d-%d)", count, hash[:16]+"...", MinReplication, MaxReplication)

	if dhtCtx.Err() == context.DeadlineExceeded || (count == 0 && responsible) {
		incrementMetric(&metrics.replicationFailures)
		recordFailedOperation(hash)
		log.Printf("[Backoff] DHT query failed for %s, applying backoff", hash[:16]+"...")
		return
	}

	incrementMetric(&metrics.replicationSuccess)
	clearBackoff(hash)

	fileReplicationLevels.Lock()
	fileReplicationLevels.levels[hash] = count
	fileReplicationLevels.Unlock()

	if count >= MinReplication && count <= MaxReplication {
		fileConvergenceTime.Lock()
		if _, exists := fileConvergenceTime.times[hash]; !exists {
			fileConvergenceTime.times[hash] = time.Now()
			updateMetrics(func() {
				metrics.filesConvergedTotal++
				metrics.filesConvergedThisPeriod++
			})
		}
		fileConvergenceTime.Unlock()
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
		if responsible && !pinned {
			log.Printf("[Replication] Low Redundancy (%d/%d). RE-PINNING %s", count, MinReplication, hash)
			pinFile(hash)
			provideFile(ctx, hash)
		}
		shardMgr.PublishToShard("NEED:" + hash)
	}

	if responsible && count > MaxReplication && pinned {
		log.Printf("[Replication] High Redundancy (%d/%d). Unpinning %s (Monitoring only)", count, MaxReplication, hash)
		unpinFile(hash)
		if count > MaxReplication+3 {
			log.Printf("[Replication] Replication stable (%d), removing from tracking", count)
			removeKnownFile(hash)
		}
	}

	if !responsible && count >= MinReplication && pinned {
		log.Printf("[Replication] Handoff Complete (%d/%d copies found). Unpinning custodial file %s", count, MinReplication, hash)
		unpinFile(hash)
	}
}

func runReplicationChecker(ctx context.Context) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			knownFiles.RLock()
			for hash := range knownFiles.hashes {
				if shouldSkipDueToBackoff(hash) {
					continue
				}

				select {
				case replicationWorkers <- struct{}{}:
					go func(h string) {
						defer func() { <-replicationWorkers }()
						checkReplication(ctx, h)
					}(hash)
				case <-ctx.Done():
					knownFiles.RUnlock()
					return
				}
			}
			knownFiles.RUnlock()
		}
	}
}
