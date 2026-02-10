package common

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
)

// TrustedPeers tracks which peers are trusted (for allowlist mode).
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

func (tp *TrustedPeers) All() []peer.ID {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	peers := make([]peer.ID, 0, len(tp.m))
	for pid := range tp.m {
		peers = append(peers, pid)
	}
	return peers
}

// NonceStore tracks seen nonces for replay protection.
type NonceStore struct {
	mu             sync.RWMutex
	entries        map[string]time.Time
	cleanupCounter uint64
}

func NewNonceStore() *NonceStore {
	return &NonceStore{entries: make(map[string]time.Time)}
}

func nonceTTL() time.Duration {
	ttl := config.SignatureMaxAge
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return ttl
}

func (ns *NonceStore) SeenBefore(sender peer.ID, nonce []byte) bool {
	key := NonceKey(sender, nonce)
	now := time.Now()

	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.cleanupCounter++
	if ns.cleanupCounter&0xFF == 0 {
		for k, exp := range ns.entries {
			if now.After(exp) {
				delete(ns.entries, k)
			}
		}
	}

	if _, exists := ns.entries[key]; exists {
		return true
	}
	ns.entries[key] = now.Add(nonceTTL())
	return false
}

func (ns *NonceStore) Record(sender peer.ID, nonce []byte) {
	key := NonceKey(sender, nonce)
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.entries[key] = time.Now().Add(nonceTTL())
}

// RateLimiter tracks message rates per peer.
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

// BackoffTable tracks backoff delays for failed operations.
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

	backoff.delay = time.Duration(float64(backoff.delay) * config.BackoffMultiplier)
	if backoff.delay > config.MaxBackoffDelay {
		backoff.delay = config.MaxBackoffDelay
	}

	jitterRange := float64(backoff.delay) * 0.25
	jitterRangeInt := int64(jitterRange * 2)
	if jitterRangeInt > 0 {
		jitterVal, err := rand.Int(rand.Reader, big.NewInt(jitterRangeInt))
		if err == nil {
			jitter := time.Duration(jitterVal.Int64()) - time.Duration(jitterRange)
			jitteredDelay := backoff.delay + jitter
			if jitteredDelay < config.InitialBackoffDelay {
				jitteredDelay = config.InitialBackoffDelay
			}
			backoff.nextRetry = time.Now().Add(jitteredDelay)
		} else {
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

type operationBackoff struct {
	nextRetry time.Time
	delay     time.Duration
	mu        sync.Mutex
}

type peerRateLimit struct {
	messages []time.Time
	mu       sync.Mutex
}
