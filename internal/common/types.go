package common

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
)

func NonceKey(sender peer.ID, nonce []byte) string {
	return sender.String() + ":" + hex.EncodeToString(nonce)
}

func NewNonce(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

// DHTProvider abstracts the DHT operations for testing
type DHTProvider interface {
	FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo
	Provide(ctx context.Context, key cid.Cid, broadcast bool) error
	FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error)
}

// PinnedSet tracks which files/manifests are currently pinned
type PinnedSet struct {
	mu sync.RWMutex
	m  map[string]time.Time // Map of key -> pin timestamp
}

func NewPinnedSet() *PinnedSet {
	return &PinnedSet{m: make(map[string]time.Time)}
}

func (ps *PinnedSet) Add(key string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if _, exists := ps.m[key]; !exists {
		ps.m[key] = time.Now()
		return true
	}
	// File already exists - update timestamp to reflect latest pin time
	ps.m[key] = time.Now()
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
	_, exists := ps.m[key]
	return exists
}

// GetPinTime returns when a file was pinned, or zero time if not pinned
func (ps *PinnedSet) GetPinTime(key string) time.Time {
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
	for k := range ps.m {
		result[k] = true
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

func (kf *KnownFiles) Add(key string) bool {
	kf.mu.Lock()
	defer kf.mu.Unlock()
	if kf.m[key] {
		return false
	}
	kf.m[key] = true
	return true
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

// ReplicationTracker tracks which nodes have which files based on D-LOCKSS announcements
// This provides accurate replication counts independent of IPFS DHT
type ReplicationTracker struct {
	mu sync.RWMutex
	// Map of ManifestCID -> set of peer IDs that have announced this file
	fileReplicas map[string]map[peer.ID]time.Time // file -> peer -> last seen
	// TTL for replication entries (entries older than this are considered stale)
	entryTTL time.Duration
}

func NewReplicationTracker(entryTTL time.Duration) *ReplicationTracker {
	if entryTTL == 0 {
		entryTTL = 5 * time.Minute // Default: 5 minutes
	}
	return &ReplicationTracker{
		fileReplicas: make(map[string]map[peer.ID]time.Time),
		entryTTL:     entryTTL,
	}
}

// RecordAnnouncement records that a peer has announced they have a file
func (rt *ReplicationTracker) RecordAnnouncement(fileKey string, peerID peer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	if rt.fileReplicas[fileKey] == nil {
		rt.fileReplicas[fileKey] = make(map[peer.ID]time.Time)
	}
	rt.fileReplicas[fileKey][peerID] = time.Now()
}

// GetReplicationCount returns the number of unique peers that have announced a file
func (rt *ReplicationTracker) GetReplicationCount(fileKey string) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	replicas, exists := rt.fileReplicas[fileKey]
	if !exists {
		return 0
	}
	
	// Count only non-stale entries
	now := time.Now()
	count := 0
	for _, lastSeen := range replicas {
		if now.Sub(lastSeen) < rt.entryTTL {
			count++
		}
	}
	return count
}

// GetReplicas returns the set of peer IDs that have announced a file (non-stale)
func (rt *ReplicationTracker) GetReplicas(fileKey string) []peer.ID {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	replicas, exists := rt.fileReplicas[fileKey]
	if !exists {
		return nil
	}
	
	// Return only non-stale entries
	now := time.Now()
	var result []peer.ID
	for peerID, lastSeen := range replicas {
		if now.Sub(lastSeen) < rt.entryTTL {
			result = append(result, peerID)
		}
	}
	return result
}

// Cleanup removes stale entries (older than entryTTL)
func (rt *ReplicationTracker) Cleanup() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	now := time.Now()
	for fileKey, replicas := range rt.fileReplicas {
		// Remove stale peer entries
		for peerID, lastSeen := range replicas {
			if now.Sub(lastSeen) >= rt.entryTTL {
				delete(replicas, peerID)
			}
		}
		// Remove file entry if no replicas remain
		if len(replicas) == 0 {
			delete(rt.fileReplicas, fileKey)
		}
	}
}

// RemovePeer removes all entries for a peer (e.g., when peer disconnects)
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
	key := NonceKey(sender, nonce)
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
	ns.entries[key] = now.Add(config.SignatureMaxAge)
	return false
}

func (ns *NonceStore) Record(sender peer.ID, nonce []byte) {
	key := NonceKey(sender, nonce)
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.entries[key] = time.Now().Add(config.SignatureMaxAge)
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

func (rl *RateLimiter) Check(peerID peer.ID) bool {
	prl := rl.GetOrCreate(peerID)

	prl.mu.Lock()
	defer prl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-config.RateLimitWindow)

	validMessages := make([]time.Time, 0, len(prl.messages))
	for _, msgTime := range prl.messages {
		if msgTime.After(cutoff) {
			validMessages = append(validMessages, msgTime)
		}
	}

	if len(validMessages) >= config.MaxMessagesPerWindow {
		return false
	}

	validMessages = append(validMessages, now)
	prl.messages = validMessages
	return true
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
			delay: config.InitialBackoffDelay,
		}
		bt.m[key] = backoff
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	// Add exponential backoff with jitter to prevent thundering herd
	backoff.delay = time.Duration(float64(backoff.delay) * config.BackoffMultiplier)
	if backoff.delay > config.MaxBackoffDelay {
		backoff.delay = config.MaxBackoffDelay
	}
	
	// Add jitter: random variation between -25% and +25% of delay
	// This spreads out retries when many operations fail simultaneously,
	// preventing thundering herd problems where all retries happen at once
	jitterRange := float64(backoff.delay) * 0.25
	jitterRangeInt := int64(jitterRange * 2)
	if jitterRangeInt > 0 {
		// Generate random jitter between -jitterRange and +jitterRange
		jitterVal, err := rand.Int(rand.Reader, big.NewInt(jitterRangeInt))
		if err == nil {
			jitter := time.Duration(jitterVal.Int64()) - time.Duration(jitterRange)
			jitteredDelay := backoff.delay + jitter
			if jitteredDelay < config.InitialBackoffDelay {
				jitteredDelay = config.InitialBackoffDelay
			}
			backoff.nextRetry = time.Now().Add(jitteredDelay)
		} else {
			// Fallback if random generation fails
			backoff.nextRetry = time.Now().Add(backoff.delay)
		}
	} else {
		backoff.nextRetry = time.Now().Add(backoff.delay)
	}
}

func (bt *BackoffTable) Clear(key string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if backoff, exists := bt.m[key]; exists {
		backoff.mu.Lock()
		backoff.delay = config.InitialBackoffDelay
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

func (cf *CheckingFiles) Size() int {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return len(cf.m)
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

type operationBackoff struct {
	nextRetry time.Time
	delay     time.Duration
	mu        sync.Mutex
}

type peerRateLimit struct {
	messages []time.Time
	mu       sync.Mutex
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

// StatusResponse defines the JSON structure for /status
type StatusResponse struct {
	PeerID        string            `json:"peer_id"`
	Version       string            `json:"version"`
	CurrentShard  string            `json:"current_shard"`
	PeersInShard  int               `json:"peers_in_shard"`
	Storage       StorageStatus     `json:"storage"`
	Replication   ReplicationStatus `json:"replication"`
	UptimeSeconds float64           `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles int      `json:"pinned_files"`
	KnownFiles  int      `json:"known_files"`
	KnownCIDs   []string `json:"known_cids,omitempty"`
}

type ReplicationStatus struct {
	QueueDepth              int     `json:"queue_depth"`
	ActiveWorkers           int     `json:"active_workers"`
	AvgReplicationLevel     float64 `json:"avg_replication_level"`
	FilesAtTarget           int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"`
}
