package ratelimit

import (
	"context"
	"log"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"

	"github.com/libp2p/go-libp2p/core/peer"
)

type RateLimiter struct {
	rl                 *common.RateLimiter
	canAcceptCustodial func() bool
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		rl:                 common.NewRateLimiter(),
		canAcceptCustodial: func() bool { return true },
	}
}

// SetCustodialCheck registers the function used to decide whether the node can
// accept delegated (custodial) files. Typically wired to StorageManager.CanAcceptCustodialFile.
func (r *RateLimiter) SetCustodialCheck(fn func() bool) {
	r.canAcceptCustodial = fn
}

func (r *RateLimiter) Check(peerID peer.ID) bool {
	return r.CheckForMessage(peerID, "")
}

func (r *RateLimiter) CheckForMessage(peerID peer.ID, messageType string) bool {
	if messageType == "DELEGATE" && !r.canAcceptCustodial() {
		return false
	}

	return r.rl.Check(peerID)
}

func (r *RateLimiter) RunCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			cutoff := now.Add(-config.RateLimitWindow * 2)
			removed := r.rl.Cleanup(cutoff)
			if removed > 0 {
				log.Printf("[RateLimit] Cleaned up %d inactive peer rate limiters", removed)
			}
		}
	}
}

func (r *RateLimiter) Size() int {
	return r.rl.Size()
}

func (r *RateLimiter) GetCommon() *common.RateLimiter {
	return r.rl
}
