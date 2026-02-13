package storage

import (
	"context"
	"log"
	"sort"
	"sync"
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

	// Round-robin pin reannouncement: cached sorted list, rebuilt when pins change
	announceMu        sync.Mutex
	announceIndex     int
	announceKeys      []string
	announceKeysDirty bool
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

// GetNextFileToAnnounce returns next file key for round-robin PINNED announcements.
func (sm *StorageManager) GetNextFileToAnnounce() string {
	sm.announceMu.Lock()
	defer sm.announceMu.Unlock()

	if sm.announceKeysDirty {
		sm.rebuildAnnounceKeys()
	}
	if len(sm.announceKeys) == 0 {
		return ""
	}
	idx := sm.announceIndex % len(sm.announceKeys)
	key := sm.announceKeys[idx]
	sm.announceIndex++
	if sm.announceIndex < 0 {
		sm.announceIndex = 0
	}
	return key
}

func (sm *StorageManager) rebuildAnnounceKeys() {
	files := sm.pinnedFiles.All()
	if len(files) == 0 {
		sm.announceKeys = nil
		sm.announceKeysDirty = false
		return
	}
	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sm.announceKeys = keys
	sm.announceKeysDirty = false
	sm.announceIndex = sm.announceIndex % len(keys)
	if sm.announceIndex < 0 {
		sm.announceIndex = 0
	}
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
		sm.announceMu.Lock()
		sm.announceKeysDirty = true
		sm.announceMu.Unlock()
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
		sm.announceMu.Lock()
		sm.announceKeysDirty = true
		sm.announceMu.Unlock()
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
