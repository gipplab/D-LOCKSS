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
func (tp *TrustedPeers) Add(pid peer.ID)               { tp.m.Upsert(pid, true) }
func (tp *TrustedPeers) SetAll(peers map[peer.ID]bool) { tp.m.ReplaceAll(peers) }
func (tp *TrustedPeers) All() []peer.ID                { return tp.m.Keys() }
func (tp *TrustedPeers) Len() int                      { return tp.m.Len() }

// RateLimiter enforces a sliding-window message limit per peer.
type RateLimiter struct {
	mu          sync.RWMutex
	peers       map[peer.ID]*peerRateLimit
	window      time.Duration
	maxMessages int
}

func NewRateLimiter(window time.Duration, maxMessages int) *RateLimiter {
	return &RateLimiter{
		peers:       make(map[peer.ID]*peerRateLimit),
		window:      window,
		maxMessages: maxMessages,
	}
}

func (rl *RateLimiter) getOrCreate(peerID peer.ID) *peerRateLimit {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	prl, exists := rl.peers[peerID]
	if !exists {
		prl = &peerRateLimit{}
		rl.peers[peerID] = prl
	}
	return prl
}

// Check reports whether a message from peerID is within the limit and, if
// so, counts it. Rejected messages do not count against the window.
func (rl *RateLimiter) Check(peerID peer.ID) bool {
	prl := rl.getOrCreate(peerID)

	prl.mu.Lock()
	defer prl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	recent := make([]time.Time, 0, len(prl.messages))
	for _, t := range prl.messages {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}

	if len(recent) >= rl.maxMessages {
		return false
	}
	prl.messages = append(recent, now)
	return true
}

type peerRateLimit struct {
	mu       sync.Mutex
	messages []time.Time
}
