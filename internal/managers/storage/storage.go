package storage

import (
	"context"
	"log"
	"time"

	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/telemetry"
)

// StorageManager handles local file state and DHT announcements.
type StorageManager struct {
	dht                   common.DHTProvider
	pinnedFiles           *common.PinnedSet
	knownFiles            *common.KnownFiles
	recentlyRemoved       *common.RecentlyRemoved
	fileReplicationLevels *common.FileReplicationLevels
	failedOperations      *common.BackoffTable
	metrics               *telemetry.MetricsManager
}

// NewStorageManager creates a new StorageManager.
func NewStorageManager(dht common.DHTProvider, metrics *telemetry.MetricsManager) *StorageManager {
	// Handle nil metrics gracefully (for tests)
	return &StorageManager{
		dht:                   dht,
		pinnedFiles:           common.NewPinnedSet(),
		knownFiles:            common.NewKnownFiles(),
		recentlyRemoved:       common.NewRecentlyRemoved(),
		fileReplicationLevels: common.NewFileReplicationLevels(),
		failedOperations:      common.NewBackoffTable(),
		metrics:               metrics,
	}
}

// GetNextFileToAnnounce returns the next file key to announce in a round-robin fashion.
// Returns empty string if no files are pinned.
func (sm *StorageManager) GetNextFileToAnnounce() string {
	files := sm.pinnedFiles.All() // Returns map[string]time.Time
	if len(files) == 0 {
		return ""
	}

	// Just pick the first one since we removed the index
	for k := range files {
		return k
	}
	return ""
}

// GetPinnedManifests returns all manifest CID strings currently pinned (for replication check).
func (sm *StorageManager) GetPinnedManifests() []string {
	files := sm.pinnedFiles.All()
	out := make([]string, 0, len(files))
	for k := range files {
		out = append(out, k)
	}
	return out
}

// PinFile pins a file using its ManifestCID string.
// It tracks the ManifestCID in our internal state and announces to DHT.
func (sm *StorageManager) PinFile(manifestCIDStr string) bool {
	// Check BadBits
	if badbits.IsCIDBlocked(manifestCIDStr) {
		log.Printf("[Storage] Refused to pin blocked CID: %s", manifestCIDStr)
		return false
	}

	wasNew := sm.pinnedFiles.Add(manifestCIDStr)
	if wasNew {
		if sm.metrics != nil {
			sm.metrics.SetPinnedFilesCount(sm.pinnedFiles.Size())
		}
		log.Printf("[Storage] Pinned ManifestCID: %s (total pinned: %d)", manifestCIDStr, sm.pinnedFiles.Size())
	} else if config.VerboseLogging {
		// File was already pinned - timestamp was updated by Add() to reflect latest pin time
		log.Printf("[Storage] ManifestCID already pinned (timestamp updated): %s (total pinned: %d)", manifestCIDStr, sm.pinnedFiles.Size())
	}

	return true
}

// UnpinFile removes a file/manifest from the pinned set.
func (sm *StorageManager) UnpinFile(key string) {
	if sm.pinnedFiles.Has(key) {
		// Get pin time before removing to log how long it was pinned
		pinTime := sm.pinnedFiles.GetPinTime(key)
		sm.pinnedFiles.Remove(key)
		if sm.metrics != nil {
			sm.metrics.SetPinnedFilesCount(sm.pinnedFiles.Size())
		}
		timeSincePin := time.Since(pinTime)
		log.Printf("[Storage] Unpinned file: %s (was pinned for %v, remaining pinned: %d)",
			key, timeSincePin, sm.pinnedFiles.Size())
	} else {
		log.Printf("[Storage] Attempted to unpin file that wasn't pinned: %s", key)
	}
}

// IsPinned checks if a file/manifest is pinned.
func (sm *StorageManager) IsPinned(key string) bool {
	return sm.pinnedFiles.Has(key)
}

// AddKnownFile adds a file/manifest to the known files set.
func (sm *StorageManager) AddKnownFile(key string) {
	removedTime, wasRemoved := sm.recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < config.RemovedFileCooldown {
		return
	}

	if sm.knownFiles.Add(key) {
		if sm.metrics != nil {
			sm.metrics.SetKnownFilesCount(sm.knownFiles.Size())
		}
	}

	// Ensure we track replication level for new files, starting at 0 (or 1 if pinned)
	if sm.pinnedFiles.Has(key) {
		sm.fileReplicationLevels.Set(key, 1)
	} else {
		sm.fileReplicationLevels.Set(key, 0)
	}
}

// RemoveKnownFile removes a file/manifest from the known files set.
func (sm *StorageManager) RemoveKnownFile(key string) {
	sm.knownFiles.Remove(key)
	if sm.metrics != nil {
		sm.metrics.SetKnownFilesCount(sm.knownFiles.Size())
	}

	sm.fileReplicationLevels.Remove(key)

	sm.recentlyRemoved.Record(key)
}

// ProvideFile announces a file/manifest to the DHT.
func (sm *StorageManager) ProvideFile(ctx context.Context, key string) {
	c, err := common.KeyToCID(key)
	if err != nil {
		common.LogError("Storage", "convert key to CID", key, err)
		return
	}
	if sm.dht != nil {
		if err := sm.dht.Provide(ctx, c, true); err != nil {
			common.LogError("DHT", "provide file", key, err)
			sm.RecordFailedOperation(key)
			return
		}
	}
}

// RecordFailedOperation records a failure for exponential backoff
func (sm *StorageManager) RecordFailedOperation(key string) {
	sm.failedOperations.RecordFailure(key)
}

func (sm *StorageManager) ClearFailedOperation(key string) {
	sm.failedOperations.Clear(key)
}

func (sm *StorageManager) SetReplicationLevel(key string, count int) {
	sm.fileReplicationLevels.Set(key, count)
}

func (sm *StorageManager) GetReplicationLevels() map[string]int {
	return sm.fileReplicationLevels.All()
}

func (sm *StorageManager) GetKnownFiles() *common.KnownFiles {
	return sm.knownFiles
}

func (sm *StorageManager) GetPinTime(key string) time.Time {
	return sm.pinnedFiles.GetPinTime(key)
}

func (sm *StorageManager) GetPinnedCount() int {
	return sm.pinnedFiles.Size()
}

// Methods for metrics interface
func (sm *StorageManager) GetStorageStatus() (int, int, []string, int) {
	pinned := sm.pinnedFiles.Size()
	known := sm.knownFiles.Size()
	backoff := sm.failedOperations.Size()

	// Convert known files to list
	allKnown := sm.knownFiles.All()
	knownCIDs := make([]string, 0, len(allKnown))
	for k := range allKnown {
		knownCIDs = append(knownCIDs, k)
	}

	return pinned, known, knownCIDs, backoff
}
