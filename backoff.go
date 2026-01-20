package main

import (
	"log"
	"time"
)

// shouldSkipDueToBackoff checks if an operation should be skipped due to backoff.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func shouldSkipDueToBackoff(key string) bool {
	return failedOperations.ShouldSkip(key)
}

// recordFailedOperation records a failed operation and applies exponential backoff.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func recordFailedOperation(key string) {
	failedOperations.RecordFailure(key)
	nextRetry, delay, exists := failedOperations.GetNextRetry(key)
	if exists {
		log.Printf("[Backoff] Operation for %s will retry after %v (current delay: %v)", key[:16]+"...", nextRetry.Format(time.RFC3339), delay)
	}
}

// clearBackoff clears the backoff state for a successful operation.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func clearBackoff(key string) {
	failedOperations.Clear(key)
}
