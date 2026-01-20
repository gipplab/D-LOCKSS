package main

import (
	"context"
	"log"
	"time"
)

// pinFile is the legacy V1 function that takes a SHA-256 hash and converts it to CID.
// For V2, use pinFileV2 which takes a ManifestCID string directly.
func pinFile(hash string) bool {
	fileCid, err := hashToCid(hash)
	if err != nil {
		log.Printf("[Storage] Failed to convert hash to CID: %v", err)
		return false
	}

	cidStr := fileCid.String()
	if isCIDBlocked(cidStr, NodeCountry) {
		log.Printf("[Storage] Refused to pin blocked CID: %s (blocked in %s)", cidStr[:16]+"...", NodeCountry)
		return false
	}

	if pinnedFiles.Add(hash) {
		updateMetrics(func() {
			metrics.pinnedFilesCount = pinnedFiles.Size()
		})
		log.Printf("[Storage] Pinned file: %s (total pinned: %d)", hash[:16]+"...", pinnedFiles.Size())
		return true
	}
	return true
}

// unpinFile removes a file/manifest from the pinned set.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func unpinFile(key string) {
	if pinnedFiles.Has(key) {
		pinnedFiles.Remove(key)
		updateMetrics(func() {
			metrics.pinnedFilesCount = pinnedFiles.Size()
		})
		log.Printf("[Storage] Unpinned file: %s (remaining pinned: %d)", key[:16]+"...", pinnedFiles.Size())
	}
}

// isPinned checks if a file/manifest is pinned.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func isPinned(key string) bool {
	return pinnedFiles.Has(key)
}

// addKnownFile adds a file/manifest to the known files set.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func addKnownFile(key string) {
	removedTime, wasRemoved := recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return
	}

	knownFiles.Add(key)
	updateMetrics(func() {
		metrics.knownFilesCount = knownFiles.Size()
	})
}

// removeKnownFile removes a file/manifest from the known files set.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func removeKnownFile(key string) {
	knownFiles.Remove(key)
	updateMetrics(func() {
		metrics.knownFilesCount = knownFiles.Size()
	})

	fileReplicationLevels.Remove(key)

	recentlyRemoved.Record(key)
}

// provideFile announces a file/manifest to the DHT.
// The key parameter can be either a legacy SHA-256 hash (V1) or a ManifestCID string (V2).
func provideFile(ctx context.Context, key string) {
	c, err := keyToCID(key)
	if err != nil {
		log.Printf("[Error] Failed to convert key to CID: %v", err)
		return
	}
	if globalDHT != nil {
		_ = globalDHT.Provide(ctx, c, true)
	}
}

// pinFileV2 is the V2 version that works with ManifestCID strings
// It tracks the ManifestCID in our internal state (for now, still using hash map)
func pinFileV2(manifestCIDStr string) bool {
	if pinnedFiles.Add(manifestCIDStr) {
		updateMetrics(func() {
			metrics.pinnedFilesCount = pinnedFiles.Size()
		})
		log.Printf("[Storage] Pinned ManifestCID: %s (total pinned: %d)", manifestCIDStr[:16]+"...", pinnedFiles.Size())
		return true
	}
	return true
}
