package storage

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
)

// StorageManager handles local file state and DHT announcements.
type StorageManager struct {
	cfg         *config.Config
	dht         common.DHTProvider
	badBits     *badbits.Filter
	disk        *DiskMonitor
	pinnedFiles *common.PinnedSet
	knownFiles  *common.KnownFiles
	provideSem  chan struct{}

	announceMu        sync.Mutex
	announceIndex     int
	announceKeys      []string
	announceKeysDirty bool
}

// NewStorageManager creates a new StorageManager.
func NewStorageManager(cfg *config.Config, dht common.DHTProvider, badBits *badbits.Filter) *StorageManager {
	maxProvides := cfg.Files.MaxConcurrentDHTProvides
	if maxProvides < 1 {
		maxProvides = 8
	}
	return &StorageManager{
		cfg:         cfg,
		dht:         dht,
		badBits:     badBits,
		disk:        NewDiskMonitor(cfg.FileWatchFolder, cfg.DiskUsageHighWaterMark),
		pinnedFiles: common.NewPinnedSet(),
		knownFiles:  common.NewKnownFiles(),
		provideSem:  make(chan struct{}, maxProvides),
	}
}

// CanAcceptCustodialFile delegates to the DiskMonitor.
func (sm *StorageManager) CanAcceptCustodialFile() bool {
	return sm.disk.CanAcceptCustodialFile()
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
	keys := sm.pinnedFiles.Keys()
	if len(keys) == 0 {
		sm.announceKeys = nil
		sm.announceKeysDirty = false
		return
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
	return sm.pinnedFiles.Keys()
}

// PinFile pins a file using its ManifestCID string.
// It tracks the ManifestCID in our internal state and announces to DHT.
func (sm *StorageManager) PinFile(manifestCIDStr string) bool {
	// Check BadBits
	if sm.badBits.IsBlocked(manifestCIDStr) {
		slog.Warn("refused to pin blocked cid", "manifest", manifestCIDStr)
		return false
	}

	wasNew := sm.pinnedFiles.Add(manifestCIDStr)
	if wasNew {
		sm.announceMu.Lock()
		sm.announceKeysDirty = true
		sm.announceMu.Unlock()
		slog.Info("pinned manifest", "manifest", manifestCIDStr, "total", sm.pinnedFiles.Size())
	} else {
		slog.Debug("manifest already pinned, timestamp updated", "manifest", manifestCIDStr, "total", sm.pinnedFiles.Size())
	}

	return true
}

// UnpinFile removes a file/manifest from the pinned set.
func (sm *StorageManager) UnpinFile(key string) {
	pinTime, was := sm.pinnedFiles.RemoveIfPresent(key)
	if was {
		sm.announceMu.Lock()
		sm.announceKeysDirty = true
		sm.announceMu.Unlock()
		slog.Info("unpinned file", "key", key, "pinned_for", time.Since(pinTime), "remaining", sm.pinnedFiles.Size())
	} else {
		slog.Warn("attempted to unpin file that was not pinned", "key", key)
	}
}

// IsPinned checks if a file/manifest is pinned.
func (sm *StorageManager) IsPinned(key string) bool {
	return sm.pinnedFiles.Has(key)
}

// AddKnownFile adds a file/manifest to the known files set.
func (sm *StorageManager) AddKnownFile(key string) {
	sm.knownFiles.Add(key)
}

// ProvideFile announces a file/manifest to the DHT.
// Concurrency is bounded by MaxConcurrentDHTProvides. If all slots are busy
// the call returns immediately — the heartbeat re-provide loop will pick up
// any files that were skipped during bulk syncs.
func (sm *StorageManager) ProvideFile(ctx context.Context, key string) {
	if sm.dht == nil {
		return
	}
	c, err := cid.Decode(key)
	if err != nil {
		slog.Error("failed to convert key to CID", "key", key, "error", err)
		return
	}

	select {
	case sm.provideSem <- struct{}{}:
	default:
		slog.Debug("DHT provide skipped, all slots busy", "key", key)
		return
	}
	defer func() { <-sm.provideSem }()

	if err := sm.dht.Provide(ctx, c, true); err != nil {
		slog.Warn("failed to provide file to DHT", "key", key, "error", err)
		return
	}
}

// GetAllKnownFiles returns a snapshot of all known file keys.
func (sm *StorageManager) GetAllKnownFiles() map[string]bool {
	return sm.knownFiles.All()
}

func (sm *StorageManager) GetPinTime(key string) time.Time {
	return sm.pinnedFiles.GetPinTime(key)
}

func (sm *StorageManager) GetPinnedCount() int {
	return sm.pinnedFiles.Size()
}
