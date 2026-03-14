package signing

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func newNonce(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

func nonceKey(sender peer.ID, nonce []byte) string {
	return sender.String() + ":" + hex.EncodeToString(nonce)
}

// nonceStore tracks seen nonces for replay protection.
type nonceStore struct {
	mu             sync.RWMutex
	entries        map[string]time.Time
	cleanupCounter uint64
}

func newNonceStore() *nonceStore {
	return &nonceStore{entries: make(map[string]time.Time)}
}

func (ns *nonceStore) seenBefore(sender peer.ID, nonce []byte, ttl time.Duration) bool {
	key := nonceKey(sender, nonce)
	now := time.Now()

	ns.mu.Lock()
	defer ns.mu.Unlock()

	const cleanupEveryN = 256
	ns.cleanupCounter++
	if ns.cleanupCounter%cleanupEveryN == 0 {
		for k, exp := range ns.entries {
			if now.After(exp) {
				delete(ns.entries, k)
			}
		}
	}

	if _, exists := ns.entries[key]; exists {
		return true
	}
	ns.entries[key] = now.Add(ttl)
	return false
}
