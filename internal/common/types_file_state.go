package common

import (
	"sync"
	"time"
)

// CheckingFiles tracks files currently being checked for replication.
type CheckingFiles struct {
	mu sync.RWMutex
	m  map[string]bool
}

func NewCheckingFiles() *CheckingFiles {
	return &CheckingFiles{m: make(map[string]bool)}
}

func (cf *CheckingFiles) TryLock(key string) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	if cf.m[key] {
		return false
	}
	cf.m[key] = true
	return true
}

func (cf *CheckingFiles) Unlock(key string) {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	delete(cf.m, key)
}

func (cf *CheckingFiles) Size() int {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return len(cf.m)
}

// LastCheckTime tracks when files were last checked for replication.
type LastCheckTime struct {
	mu sync.RWMutex
	m  map[string]time.Time
}

func NewLastCheckTime() *LastCheckTime {
	return &LastCheckTime{m: make(map[string]time.Time)}
}

func (lct *LastCheckTime) Get(key string) (time.Time, bool) {
	lct.mu.RLock()
	defer lct.mu.RUnlock()
	t, ok := lct.m[key]
	return t, ok
}

func (lct *LastCheckTime) Set(key string, t time.Time) {
	lct.mu.Lock()
	defer lct.mu.Unlock()
	lct.m[key] = t
}

// RecentlyRemoved tracks files that were recently removed (for cooldown).
type RecentlyRemoved struct {
	mu sync.RWMutex
	m  map[string]time.Time
}

func NewRecentlyRemoved() *RecentlyRemoved {
	return &RecentlyRemoved{m: make(map[string]time.Time)}
}

func (rr *RecentlyRemoved) WasRemoved(key string) (time.Time, bool) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	t, ok := rr.m[key]
	return t, ok
}

func (rr *RecentlyRemoved) Record(key string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	now := time.Now()
	rr.m[key] = now
	// Prune entries older than 10 minutes every 64 records
	if len(rr.m)&0x3F == 0 {
		cutoff := now.Add(-10 * time.Minute)
		for k, t := range rr.m {
			if t.Before(cutoff) {
				delete(rr.m, k)
			}
		}
	}
}

func (rr *RecentlyRemoved) Remove(key string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	delete(rr.m, key)
}
