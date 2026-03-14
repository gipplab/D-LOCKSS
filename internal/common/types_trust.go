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

func (rl *RateLimiter) getOrCreate(peerID peer.ID) *peerRateLimit {
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

func (rl *RateLimiter) Check(peerID peer.ID) bool {
	prl := rl.getOrCreate(peerID)

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
