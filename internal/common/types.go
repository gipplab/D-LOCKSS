package common

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func NonceKey(sender peer.ID, nonce []byte) string {
	return sender.String() + ":" + hex.EncodeToString(nonce)
}

func NewNonce(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

// DHTProvider abstracts the DHT operations for testing.
type DHTProvider interface {
	FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo
	Provide(ctx context.Context, key cid.Cid, broadcast bool) error
	FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error)
}

// PinnedSet tracks which files/manifests are currently pinned.
type PinnedSet struct {
	mu sync.RWMutex
	m  map[string]time.Time
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

// KnownFiles tracks which files/manifests are known to the system.
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
