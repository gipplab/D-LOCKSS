package protocol

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// sweepEvery bounds how often expired nonces are garbage collected: once per
// this many lookups.
const sweepEvery = 256

// nonceStore remembers (peer, nonce) pairs for the signature max-age window,
// so a message cannot be replayed while it would still verify.
type nonceStore struct {
	ttl time.Duration

	mu    sync.Mutex
	seen  map[string]time.Time // "peerID:hexnonce" -> expiry
	calls int
}

func newNonceStore(ttl time.Duration) *nonceStore {
	return &nonceStore{ttl: ttl, seen: map[string]time.Time{}}
}

// seenBefore records the nonce and reports whether it was already present
// and unexpired.
func (s *nonceStore) seenBefore(p peer.ID, nonce []byte) bool {
	key := p.String() + ":" + hex.EncodeToString(nonce)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.calls++; s.calls%sweepEvery == 0 {
		for k, exp := range s.seen {
			if now.After(exp) {
				delete(s.seen, k)
			}
		}
	}
	if exp, ok := s.seen[key]; ok && now.Before(exp) {
		return true
	}
	s.seen[key] = now.Add(s.ttl)
	return false
}
