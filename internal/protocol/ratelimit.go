package protocol

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// RateLimiter enforces a per-peer sliding-window message budget. It is
// applied to CBOR protocol messages only; text shard messages are exempt.
type RateLimiter struct {
	window time.Duration
	max    int

	mu    sync.Mutex
	hits  map[peer.ID][]time.Time
	sweep int
}

// NewRateLimiter allows max messages per window per peer.
func NewRateLimiter(window time.Duration, max int) *RateLimiter {
	return &RateLimiter{window: window, max: max, hits: map[peer.ID][]time.Time{}}
}

// Allow reports whether the peer may send another message now; rejected
// messages do not consume budget.
func (r *RateLimiter) Allow(p peer.ID) bool {
	now := time.Now()
	cutoff := now.Add(-r.window)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Periodically drop peers whose whole window has expired, so the map
	// does not grow without bound.
	if r.sweep++; r.sweep%1024 == 0 {
		for id, ts := range r.hits {
			if len(ts) == 0 || ts[len(ts)-1].Before(cutoff) {
				delete(r.hits, id)
			}
		}
	}

	ts := r.hits[p]
	keep := ts[:0]
	for _, t := range ts {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	if len(keep) >= r.max {
		r.hits[p] = keep
		return false
	}
	r.hits[p] = append(keep, now)
	return true
}
