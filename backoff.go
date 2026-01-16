package main

import (
	"log"
	"time"
)

func shouldSkipDueToBackoff(hash string) bool {
	failedOperations.RLock()
	defer failedOperations.RUnlock()

	backoff, exists := failedOperations.hashes[hash]
	if !exists {
		return false
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	if time.Now().Before(backoff.nextRetry) {
		return true
	}

	return false
}

func recordFailedOperation(hash string) {
	failedOperations.Lock()
	defer failedOperations.Unlock()

	backoff, exists := failedOperations.hashes[hash]
	if !exists {
		backoff = &operationBackoff{
			delay: InitialBackoffDelay,
		}
		failedOperations.hashes[hash] = backoff
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	backoff.nextRetry = time.Now().Add(backoff.delay)
	backoff.delay = time.Duration(float64(backoff.delay) * BackoffMultiplier)

	if backoff.delay > MaxBackoffDelay {
		backoff.delay = MaxBackoffDelay
	}

	log.Printf("[Backoff] Operation for %s will retry after %v (current delay: %v)", hash[:16]+"...", backoff.nextRetry.Format(time.RFC3339), backoff.delay)
}

func clearBackoff(hash string) {
	failedOperations.Lock()
	defer failedOperations.Unlock()

	if backoff, exists := failedOperations.hashes[hash]; exists {
		backoff.mu.Lock()
		backoff.delay = InitialBackoffDelay
		backoff.nextRetry = time.Time{}
		backoff.mu.Unlock()
	}
}
