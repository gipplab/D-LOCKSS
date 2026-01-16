package main

import (
	"context"
	"log"
	"time"
)

func pinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()
	if !pinnedFiles.hashes[hash] {
		pinnedFiles.hashes[hash] = true
		updateMetrics(func() {
			metrics.pinnedFilesCount = len(pinnedFiles.hashes)
		})
		log.Printf("[Storage] Pinned file: %s (total pinned: %d)", hash[:16]+"...", len(pinnedFiles.hashes))
	}
}

func unpinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()
	if pinnedFiles.hashes[hash] {
		delete(pinnedFiles.hashes, hash)
		updateMetrics(func() {
			metrics.pinnedFilesCount = len(pinnedFiles.hashes)
		})
		log.Printf("[Storage] Unpinned file: %s (remaining pinned: %d)", hash[:16]+"...", len(pinnedFiles.hashes))
	}
}

func isPinned(hash string) bool {
	pinnedFiles.RLock()
	defer pinnedFiles.RUnlock()
	return pinnedFiles.hashes[hash]
}

func addKnownFile(hash string) {
	recentlyRemoved.RLock()
	removedTime, wasRemoved := recentlyRemoved.hashes[hash]
	recentlyRemoved.RUnlock()
	if wasRemoved && time.Since(removedTime) < RemovedFileCooldown {
		return
	}

	knownFiles.Lock()
	defer knownFiles.Unlock()
	knownFiles.hashes[hash] = true
	updateMetrics(func() {
		metrics.knownFilesCount = len(knownFiles.hashes)
	})
}

func removeKnownFile(hash string) {
	knownFiles.Lock()
	defer knownFiles.Unlock()
	delete(knownFiles.hashes, hash)
	updateMetrics(func() {
		metrics.knownFilesCount = len(knownFiles.hashes)
	})

	fileReplicationLevels.Lock()
	delete(fileReplicationLevels.levels, hash)
	fileReplicationLevels.Unlock()

	recentlyRemoved.Lock()
	recentlyRemoved.hashes[hash] = time.Now()
	recentlyRemoved.Unlock()
}

func provideFile(ctx context.Context, hash string) {
	c, err := hashToCid(hash)
	if err != nil {
		log.Printf("[Error] Failed to convert hash to CID: %v", err)
		return
	}
	if globalDHT != nil {
		_ = globalDHT.Provide(ctx, c, true)
	}
}
