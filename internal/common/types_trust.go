package common

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/syncmap"
)

// TrustedPeers tracks which peers are trusted (for allowlist mode).
type TrustedPeers struct {
	m *syncmap.Map[peer.ID, bool]
}

func NewTrustedPeers() *TrustedPeers {
	return &TrustedPeers{m: syncmap.New[peer.ID, bool]()}
}

func (tp *TrustedPeers) Add(pid peer.ID)               { tp.m.Set(pid, true) }
func (tp *TrustedPeers) Remove(pid peer.ID)            { tp.m.Delete(pid) }
func (tp *TrustedPeers) Has(pid peer.ID) bool          { return tp.m.Has(pid) }
func (tp *TrustedPeers) SetAll(peers map[peer.ID]bool) { tp.m.ReplaceAll(peers) }
func (tp *TrustedPeers) All() []peer.ID                { return tp.m.Keys() }

// RateLimiter tracks message rates per peer.
type RateLimiter struct {
	mu                   sync.RWMutex
	peers                map[peer.ID]*peerRateLimit
	rateLimitWindow      time.Duration
	maxMessagesPerWindow int
}

func NewRateLimiter(window time.Duration, maxMessages int) *RateLimiter {
	return &RateLimiter{
		peers:                make(map[peer.ID]*peerRateLimit),
		rateLimitWindow:      window,
		maxMessagesPerWindow: maxMessages,
	}
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
	cutoff := now.Add(-rl.rateLimitWindow)

	validMessages := make([]time.Time, 0, len(prl.messages))
	for _, msgTime := range prl.messages {
		if msgTime.After(cutoff) {
			validMessages = append(validMessages, msgTime)
		}
	}

	if len(validMessages) >= rl.maxMessagesPerWindow {
		return false
	}

	validMessages = append(validMessages, now)
	prl.messages = validMessages
	return true
}

type peerRateLimit struct {
	messages []time.Time
	mu       sync.Mutex
}
