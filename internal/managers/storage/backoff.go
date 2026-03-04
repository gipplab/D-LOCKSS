package storage

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

// BackoffTable tracks exponential backoff delays for failed operations.
type BackoffTable struct {
	mu                sync.RWMutex
	m                 map[string]*operationBackoff
	initialDelay      time.Duration
	maxDelay          time.Duration
	backoffMultiplier float64
}

func newBackoffTable(initialDelay, maxDelay time.Duration, multiplier float64) *BackoffTable {
	return &BackoffTable{
		m:                 make(map[string]*operationBackoff),
		initialDelay:      initialDelay,
		maxDelay:          maxDelay,
		backoffMultiplier: multiplier,
	}
}

func (bt *BackoffTable) shouldSkip(key string) bool {
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

func (bt *BackoffTable) recordFailure(key string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	backoff, exists := bt.m[key]
	if !exists {
		backoff = &operationBackoff{
			delay: bt.initialDelay,
		}
		bt.m[key] = backoff
	}

	backoff.mu.Lock()
	defer backoff.mu.Unlock()

	backoff.delay = time.Duration(float64(backoff.delay) * bt.backoffMultiplier)
	if backoff.delay > bt.maxDelay {
		backoff.delay = bt.maxDelay
	}

	const backoffJitterFraction = 0.25
	jitterRange := float64(backoff.delay) * backoffJitterFraction
	jitterRangeInt := int64(jitterRange * 2)
	if jitterRangeInt > 0 {
		jitterVal, err := rand.Int(rand.Reader, big.NewInt(jitterRangeInt))
		if err == nil {
			jitter := time.Duration(jitterVal.Int64()) - time.Duration(jitterRange)
			jitteredDelay := backoff.delay + jitter
			if jitteredDelay < bt.initialDelay {
				jitteredDelay = bt.initialDelay
			}
			backoff.nextRetry = time.Now().Add(jitteredDelay)
		} else {
			backoff.nextRetry = time.Now().Add(backoff.delay)
		}
	} else {
		backoff.nextRetry = time.Now().Add(backoff.delay)
	}
}

func (bt *BackoffTable) clear(key string) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if backoff, exists := bt.m[key]; exists {
		backoff.mu.Lock()
		backoff.delay = bt.initialDelay
		backoff.nextRetry = time.Time{}
		backoff.mu.Unlock()
	}
}

func (bt *BackoffTable) size() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.m)
}

// purgeExpired removes entries whose nextRetry has passed.
func (bt *BackoffTable) purgeExpired() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	now := time.Now()
	for key, backoff := range bt.m {
		backoff.mu.Lock()
		expired := !backoff.nextRetry.IsZero() && now.After(backoff.nextRetry)
		backoff.mu.Unlock()
		if expired {
			delete(bt.m, key)
		}
	}
}

type operationBackoff struct {
	nextRetry time.Time
	delay     time.Duration
	mu        sync.Mutex
}
