package common

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ReplicationTracker tracks which nodes have which files based on D-LOCKSS announcements.
type ReplicationTracker struct {
	mu           sync.RWMutex
	fileReplicas map[string]map[peer.ID]time.Time
	entryTTL     time.Duration
}

func NewReplicationTracker(entryTTL time.Duration) *ReplicationTracker {
	if entryTTL == 0 {
		entryTTL = 5 * time.Minute
	}
	return &ReplicationTracker{
		fileReplicas: make(map[string]map[peer.ID]time.Time),
		entryTTL:     entryTTL,
	}
}

func (rt *ReplicationTracker) RecordAnnouncement(fileKey string, peerID peer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.fileReplicas[fileKey] == nil {
		rt.fileReplicas[fileKey] = make(map[peer.ID]time.Time)
	}
	rt.fileReplicas[fileKey][peerID] = time.Now()
}

func (rt *ReplicationTracker) GetReplicationCount(fileKey string) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	replicas, exists := rt.fileReplicas[fileKey]
	if !exists {
		return 0
	}

	now := time.Now()
	count := 0
	for _, lastSeen := range replicas {
		if now.Sub(lastSeen) < rt.entryTTL {
			count++
		}
	}
	return count
}

func (rt *ReplicationTracker) GetReplicas(fileKey string) []peer.ID {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	replicas, exists := rt.fileReplicas[fileKey]
	if !exists {
		return nil
	}

	now := time.Now()
	var result []peer.ID
	for peerID, lastSeen := range replicas {
		if now.Sub(lastSeen) < rt.entryTTL {
			result = append(result, peerID)
		}
	}
	return result
}

func (rt *ReplicationTracker) Cleanup() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	for fileKey, replicas := range rt.fileReplicas {
		for peerID, lastSeen := range replicas {
			if now.Sub(lastSeen) >= rt.entryTTL {
				delete(replicas, peerID)
			}
		}
		if len(replicas) == 0 {
			delete(rt.fileReplicas, fileKey)
		}
	}
}

func (rt *ReplicationTracker) RemovePeer(peerID peer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	for fileKey, replicas := range rt.fileReplicas {
		delete(replicas, peerID)
		if len(replicas) == 0 {
			delete(rt.fileReplicas, fileKey)
		}
	}
}

// FileReplicationLevels tracks replication counts for files.
type FileReplicationLevels struct {
	mu sync.RWMutex
	m  map[string]int
}

func NewFileReplicationLevels() *FileReplicationLevels {
	return &FileReplicationLevels{m: make(map[string]int)}
}

func (frl *FileReplicationLevels) Set(key string, count int) {
	frl.mu.Lock()
	defer frl.mu.Unlock()
	frl.m[key] = count
}

func (frl *FileReplicationLevels) Get(key string) (int, bool) {
	frl.mu.RLock()
	defer frl.mu.RUnlock()
	count, ok := frl.m[key]
	return count, ok
}

func (frl *FileReplicationLevels) Remove(key string) {
	frl.mu.Lock()
	defer frl.mu.Unlock()
	delete(frl.m, key)
}

func (frl *FileReplicationLevels) All() map[string]int {
	frl.mu.RLock()
	defer frl.mu.RUnlock()
	result := make(map[string]int, len(frl.m))
	for k, v := range frl.m {
		result[k] = v
	}
	return result
}

// FileConvergenceTime tracks when files reached target replication.
type FileConvergenceTime struct {
	mu sync.RWMutex
	m  map[string]time.Time
}

func NewFileConvergenceTime() *FileConvergenceTime {
	return &FileConvergenceTime{m: make(map[string]time.Time)}
}

func (fct *FileConvergenceTime) SetIfNotExists(key string, t time.Time) bool {
	fct.mu.Lock()
	defer fct.mu.Unlock()
	if _, exists := fct.m[key]; !exists {
		fct.m[key] = t
		return true
	}
	return false
}

func (fct *FileConvergenceTime) Get(key string) (time.Time, bool) {
	fct.mu.RLock()
	defer fct.mu.RUnlock()
	t, ok := fct.m[key]
	return t, ok
}

// PendingVerifications tracks files awaiting replication verification.
type PendingVerifications struct {
	mu sync.RWMutex
	m  map[string]*VerificationPending
}

func NewPendingVerifications() *PendingVerifications {
	return &PendingVerifications{m: make(map[string]*VerificationPending)}
}

func (pv *PendingVerifications) Add(key string, pending *VerificationPending) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.m[key] = pending
}

func (pv *PendingVerifications) Get(key string) (*VerificationPending, bool) {
	pv.mu.RLock()
	defer pv.mu.RUnlock()
	pending, ok := pv.m[key]
	return pending, ok
}

func (pv *PendingVerifications) Remove(key string) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	delete(pv.m, key)
}

// ReplicationCache caches replication check results.
type ReplicationCache struct {
	mu      sync.RWMutex
	entries map[string]*cachedReplication
}

func NewReplicationCache() *ReplicationCache {
	return &ReplicationCache{entries: make(map[string]*cachedReplication)}
}

func (rc *ReplicationCache) Get(key string) (int, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	cached, hasCache := rc.entries[key]
	if !hasCache {
		return 0, false
	}
	return cached.count, true
}

func (rc *ReplicationCache) GetWithAge(key string) (int, time.Time, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	cached, hasCache := rc.entries[key]
	if !hasCache {
		return 0, time.Time{}, false
	}
	return cached.count, cached.cachedAt, true
}

func (rc *ReplicationCache) Set(key string, count int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.entries[key] = &cachedReplication{
		count:    count,
		cachedAt: time.Now(),
	}
}

func (rc *ReplicationCache) Cleanup(cutoff time.Time) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	removed := 0
	for key, cached := range rc.entries {
		if cached.cachedAt.Before(cutoff) {
			delete(rc.entries, key)
			removed++
		}
	}
	return removed
}

type VerificationPending struct {
	FirstCount     int
	FirstCheckTime time.Time
	VerifyTime     time.Time
	Responsible    bool
	Pinned         bool
}

type cachedReplication struct {
	count    int
	cachedAt time.Time
}
