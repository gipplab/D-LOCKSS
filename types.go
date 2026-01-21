package main

import (
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/ipfs"
)

// PinnedSet tracks which files/manifests are currently pinned
type PinnedSet struct {
	mu sync.RWMutex
	m  map[string]bool
}

func NewPinnedSet() *PinnedSet {
	return &PinnedSet{m: make(map[string]bool)}
}

func (ps *PinnedSet) Add(key string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.m[key] {
		ps.m[key] = true
		return true
	}
	return false
}

func (ps *PinnedSet) Remove(key string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.m, key)
}

func (ps *PinnedSet) Has(key string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.m[key]
}

func (ps *PinnedSet) Size() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.m)
}

func (ps *PinnedSet) All() map[string]bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	result := make(map[string]bool, len(ps.m))
	for k, v := range ps.m {
		result[k] = v
	}
	return result
}

// KnownFiles tracks which files/manifests are known to the system
type KnownFiles struct {
	mu sync.RWMutex
	m  map[string]bool
}

func NewKnownFiles() *KnownFiles {
	return &KnownFiles{m: make(map[string]bool)}
}

func (kf *KnownFiles) Add(key string) {
	kf.mu.Lock()
	defer kf.mu.Unlock()
	kf.m[key] = true
}

func (kf *KnownFiles) Remove(key string) {
	kf.mu.Lock()
	defer kf.mu.Unlock()
	delete(kf.m, key)
}

func (kf *KnownFiles) Has(key string) bool {
	kf.mu.RLock()
	defer kf.mu.RUnlock()
	return kf.m[key]
}

func (kf *KnownFiles) Size() int {
	kf.mu.RLock()
	defer kf.mu.RUnlock()
	return len(kf.m)
}

func (kf *KnownFiles) All() map[string]bool {
	kf.mu.RLock()
	defer kf.mu.RUnlock()
	result := make(map[string]bool, len(kf.m))
	for k, v := range kf.m {
		result[k] = v
	}
	return result
}

// TrustedPeers tracks which peers are trusted (for allowlist mode)
type TrustedPeers struct {
	mu sync.RWMutex
	m  map[peer.ID]bool
}

func NewTrustedPeers() *TrustedPeers {
	return &TrustedPeers{m: make(map[peer.ID]bool)}
}

func (tp *TrustedPeers) Add(pid peer.ID) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.m[pid] = true
}

func (tp *TrustedPeers) Remove(pid peer.ID) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	delete(tp.m, pid)
}

func (tp *TrustedPeers) Has(pid peer.ID) bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.m[pid]
}

func (tp *TrustedPeers) SetAll(peers map[peer.ID]bool) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.m = peers
}

// NonceStore tracks seen nonces for replay protection
type NonceStore struct {
	mu      sync.RWMutex
	entries map[string]time.Time
}

func NewNonceStore() *NonceStore {
	return &NonceStore{entries: make(map[string]time.Time)}
}

func (ns *NonceStore) SeenBefore(sender peer.ID, nonce []byte) bool {
	key := nonceKey(sender, nonce)
	now := time.Now()

	ns.mu.Lock()
	defer ns.mu.Unlock()

	// Lazy cleanup
	for k, exp := range ns.entries {
		if now.After(exp) {
			delete(ns.entries, k)
		}
	}

	if exp, ok := ns.entries[key]; ok && now.Before(exp) {
		return true
	}
	ns.entries[key] = now.Add(SignatureMaxAge)
	return false
}

func (ns *NonceStore) Record(sender peer.ID, nonce []byte) {
	key := nonceKey(sender, nonce)
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.entries[key] = time.Now().Add(SignatureMaxAge)
}

// RateLimiter tracks message rates per peer
type RateLimiter struct {
	mu    sync.RWMutex
	peers map[peer.ID]*peerRateLimit
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{peers: make(map[peer.ID]*peerRateLimit)}
}

func (rl *RateLimiter) GetOrCreate(peerID peer.ID) *peerRateLimit {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	prl, exists := rl.peers[peerID]
	if !exists {
		prl = &peerRateLimit{
			messages: make([]time.Time, 0),
		}
		rl.peers[peerID] = prl
	}
	return prl
}

func (rl *RateLimiter) Remove(peerID peer.ID) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.peers, peerID)
}

func (rl *RateLimiter) Size() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.peers)
}

func (rl *RateLimiter) Cleanup(cutoff time.Time) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	removed := 0
	for peerID, peerLimit := range rl.peers {
		peerLimit.mu.Lock()
		hasRecent := false
		for _, msgTime := range peerLimit.messages {
			if msgTime.After(cutoff) {
				hasRecent = true
				break
			}
		}
		peerLimit.mu.Unlock()

		if !hasRecent {
			delete(rl.peers, peerID)
			removed++
		}
	}
	return removed
}

// BackoffTable tracks backoff delays for failed operations
type BackoffTable struct {
	mu sync.RWMutex
	m  map[string]*operationBackoff
}

func NewBackoffTable() *BackoffTable {
	return &BackoffTable{m: make(map[string]*operationBackoff)}
}

func (bt *BackoffTable) ShouldSkip(key string) bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	backoff, exists := bt.m[key]
	if !exists {
		return false
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	return time.Now().Before(backoff.nextRetry)
}

func (bt *BackoffTable) RecordFailure(key string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	backoff, exists := bt.m[key]
	if !exists {
		backoff = &operationBackoff{
			delay: InitialBackoffDelay,
		}
		bt.m[key] = backoff
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	backoff.nextRetry = time.Now().Add(backoff.delay)
	backoff.delay = time.Duration(float64(backoff.delay) * BackoffMultiplier)

	if backoff.delay > MaxBackoffDelay {
		backoff.delay = MaxBackoffDelay
	}
}

func (bt *BackoffTable) Clear(key string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if backoff, exists := bt.m[key]; exists {
		backoff.mu.Lock()
		backoff.delay = InitialBackoffDelay
		backoff.nextRetry = time.Time{}
		backoff.mu.Unlock()
	}
}

func (bt *BackoffTable) GetNextRetry(key string) (time.Time, time.Duration, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	backoff, exists := bt.m[key]
	if !exists {
		return time.Time{}, 0, false
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()
	return backoff.nextRetry, backoff.delay, true
}

func (bt *BackoffTable) Size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.m)
}

// CheckingFiles tracks files currently being checked for replication
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

// LastCheckTime tracks when files were last checked for replication
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

// RecentlyRemoved tracks files that were recently removed (for cooldown)
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
	rr.m[key] = time.Now()
}

func (rr *RecentlyRemoved) Remove(key string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	delete(rr.m, key)
}

// FileReplicationLevels tracks replication counts for files
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

// FileConvergenceTime tracks when files reached target replication
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

// PendingVerifications tracks files awaiting replication verification
type PendingVerifications struct {
	mu sync.RWMutex
	m  map[string]*verificationPending
}

func NewPendingVerifications() *PendingVerifications {
	return &PendingVerifications{m: make(map[string]*verificationPending)}
}

func (pv *PendingVerifications) Add(key string, pending *verificationPending) {
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.m[key] = pending
}

func (pv *PendingVerifications) Get(key string) (*verificationPending, bool) {
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

// ReplicationCache caches replication check results
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

var (
	pinnedFiles = NewPinnedSet()
	knownFiles  = NewKnownFiles()

	// reshardedFiles tracks which files have already been processed during
	// a reshard pass after a shard split. This prevents duplicate work and
	// duplicate announcements for the same ManifestCID.
	reshardedFiles = NewKnownFiles()

	globalDHT   *dht.IpfsDHT
	shardMgr    *ShardManager
	ipfsClient  *ipfs.Client
	selfPeerID  peer.ID
	selfPrivKey crypto.PrivKey

	trustedPeers = NewTrustedPeers()

	seenNonces = NewNonceStore()

	// replicationWorkers chan struct{} // Deprecated by Async Pipeline

	rateLimiter = NewRateLimiter()

	failedOperations = NewBackoffTable()

	checkingFiles = NewCheckingFiles()

	lastCheckTime = NewLastCheckTime()

	recentlyRemoved = NewRecentlyRemoved()

	fileReplicationLevels = NewFileReplicationLevels()

	fileConvergenceTime = NewFileConvergenceTime()

	pendingVerifications = NewPendingVerifications()

	replicationCache = NewReplicationCache()

	metrics = struct {
		sync.RWMutex
		pinnedFilesCount              int
		knownFilesCount               int
		messagesReceived              int64
		messagesDropped               int64
		replicationChecks             int64
		replicationSuccess            int64
		replicationFailures           int64
		shardSplits                   int64
		workerPoolActive              int
		rateLimitedPeers              int
		filesInBackoff                int
		lowReplicationFiles           int
		highReplicationFiles          int
		dhtQueries                    int64
		dhtQueryTimeouts              int64
		lastReportTime                time.Time
		startTime                     time.Time
		replicationDistribution       [11]int
		filesAtTargetReplication      int
		avgReplicationLevel           float64
		filesConvergedTotal           int64
		filesConvergedThisPeriod      int64
		cumulativeMessagesReceived    int64
		cumulativeMessagesDropped     int64
		cumulativeReplicationChecks   int64
		cumulativeReplicationSuccess  int64
		cumulativeReplicationFailures int64
		cumulativeDhtQueries          int64
		cumulativeDhtQueryTimeouts    int64
		cumulativeShardSplits         int64
	}{
		lastReportTime: time.Now(),
		startTime:      time.Now(),
	}
)

type operationBackoff struct {
	nextRetry time.Time
	delay     time.Duration
	mu        sync.Mutex
}

type peerRateLimit struct {
	messages []time.Time
	mu       sync.Mutex
}

type verificationPending struct {
	firstCount     int
	firstCheckTime time.Time
	verifyTime     time.Time
	responsible    bool
	pinned         bool
}

type cachedReplication struct {
	count    int
	cachedAt time.Time
}
