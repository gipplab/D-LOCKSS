package common

import (
	"time"

	"dlockss/internal/syncmap"
)

// CheckingFiles tracks files currently being checked for replication.
type CheckingFiles struct {
	m *syncmap.Map[string, bool]
}

func NewCheckingFiles() *CheckingFiles {
	return &CheckingFiles{m: syncmap.New[string, bool]()}
}

func (cf *CheckingFiles) TryLock(key string) bool { return cf.m.SetIfAbsent(key, true) }
func (cf *CheckingFiles) Unlock(key string)       { cf.m.Delete(key) }
func (cf *CheckingFiles) Size() int               { return cf.m.Len() }

// LastCheckTime tracks when files were last checked for replication.
type LastCheckTime = syncmap.Map[string, time.Time]

func NewLastCheckTime() *LastCheckTime {
	return syncmap.New[string, time.Time]()
}

// RecentlyRemoved tracks files that were recently removed (for cooldown).
type RecentlyRemoved struct {
	m     *syncmap.Map[string, time.Time]
	count int
}

func NewRecentlyRemoved() *RecentlyRemoved {
	return &RecentlyRemoved{m: syncmap.New[string, time.Time]()}
}

func (rr *RecentlyRemoved) WasRemoved(key string) (time.Time, bool) { return rr.m.Get(key) }
func (rr *RecentlyRemoved) Remove(key string)                       { rr.m.Delete(key) }

func (rr *RecentlyRemoved) Record(key string) {
	rr.m.Set(key, time.Now())
	rr.count++
	const pruneEveryN = 64
	const recentlyRemovedTTL = 10 * time.Minute
	if rr.count%pruneEveryN == 0 {
		cutoff := time.Now().Add(-recentlyRemovedTTL)
		rr.m.Prune(func(_ string, t time.Time) bool { return t.Before(cutoff) })
	}
}
