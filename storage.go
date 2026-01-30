package main

import (
	"context"
	"log"
	"time"
)

// StorageManager handles local file state and DHT announcements.
type StorageManager struct {
	dht                   DHTProvider
	pinnedFiles           *PinnedSet
	knownFiles            *KnownFiles
	recentlyRemoved       *RecentlyRemoved
	fileReplicationLevels *FileReplicationLevels
	failedOperations      *BackoffTable
}

// NewStorageManager creates a new StorageManager.
func NewStorageManager(dht DHTProvider) *StorageManager {
	return &StorageManager{
		dht:                   dht,
		pinnedFiles:           NewPinnedSet(),
		knownFiles:            NewKnownFiles(),
		recentlyRemoved:       NewRecentlyRemoved(),
		fileReplicationLevels: NewFileReplicationLevels(),
		failedOperations:      NewBackoffTable(),
	}
}

// pinFile pins a file using its ManifestCID string.
// It tracks the ManifestCID in our internal state and announces to DHT.
func (sm *StorageManager) pinFile(manifestCIDStr string) bool {
	// Only pin if allowed
	if isCIDBlocked(manifestCIDStr, NodeCountry) {
		log.Printf("[Storage] Refused to pin blocked CID: %s (blocked in %s)", truncateCID(manifestCIDStr, 16), NodeCountry)
		return false
	}

	wasNew := sm.pinnedFiles.Add(manifestCIDStr)
	if wasNew {
		updateMetrics(func() {
			metrics.pinnedFilesCount = sm.pinnedFiles.Size()
		})
		log.Printf("[Storage] Pinned ManifestCID: %s (total pinned: %d)", truncateCID(manifestCIDStr, 16), sm.pinnedFiles.Size())
		log.Printf("[Storage] Metrics updated: pinnedFilesCount=%d", metrics.pinnedFilesCount)
	} else {
		// File was already pinned - timestamp was updated by Add() to reflect latest pin time
		log.Printf("[Storage] ManifestCID already pinned (timestamp updated): %s (total pinned: %d)", truncateCID(manifestCIDStr, 16), sm.pinnedFiles.Size())
	}
	
	// Also announce to DHT (provide)
	// We do this here to ensure anything we pin is also provided
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), DHTProvideTimeout)
		defer cancel()
		sm.provideFile(ctx, manifestCIDStr)
	}()
	
	return true
}

// unpinFile removes a file/manifest from the pinned set.
func (sm *StorageManager) unpinFile(key string) {
	if sm.pinnedFiles.Has(key) {
		sm.pinnedFiles.Remove(key)
		updateMetrics(func() {
			metrics.pinnedFilesCount = sm.pinnedFiles.Size()
		})
		log.Printf("[Storage] Unpinned file: %s (remaining pinned: %d)", truncateCID(key, 16), sm.pinnedFiles.Size())
	}
}

// isPinned checks if a file/manifest is pinned.
func (sm *StorageManager) isPinned(key string) bool {
	return sm.pinnedFiles.Has(key)
}

// addKnownFile adds a file/manifest to the known files set.
func (sm *StorageManager) addKnownFile(key string) {
	removedTime, wasRemoved := sm.recentlyRemoved.WasRemoved(key)
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return
	}

	sm.knownFiles.Add(key)
	updateMetrics(func() {
		metrics.knownFilesCount = sm.knownFiles.Size()
	})
}

// removeKnownFile removes a file/manifest from the known files set.
func (sm *StorageManager) removeKnownFile(key string) {
	sm.knownFiles.Remove(key)
	updateMetrics(func() {
		metrics.knownFilesCount = sm.knownFiles.Size()
	})

	sm.fileReplicationLevels.Remove(key)

	sm.recentlyRemoved.Record(key)
}

// provideFile announces a file/manifest to the DHT.
func (sm *StorageManager) provideFile(ctx context.Context, key string) {
	c, err := keyToCID(key)
	if err != nil {
		logError("Storage", "convert key to CID", key, err)
		return
	}
	if sm.dht != nil {
		if err := sm.dht.Provide(ctx, c, true); err != nil {
			logError("DHT", "provide file", key, err)
			sm.recordFailedOperation(key)
			return
		}
	}
}

// recordFailedOperation records a failure for exponential backoff
func (sm *StorageManager) recordFailedOperation(key string) {
	sm.failedOperations.RecordFailure(key)
}
