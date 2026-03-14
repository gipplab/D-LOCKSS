package common

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/syncmap"
)

// DHTProvider abstracts the DHT operations for testing.
type DHTProvider interface {
	Provide(ctx context.Context, key cid.Cid, broadcast bool) error
	FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error)
}

// PinnedSet tracks which files/manifests are currently pinned.
type PinnedSet struct {
	m *syncmap.Map[string, time.Time]
}

func NewPinnedSet() *PinnedSet {
	return &PinnedSet{m: syncmap.New[string, time.Time]()}
}

// Add pins a key (always refreshes timestamp). Returns true if key was new.
func (ps *PinnedSet) Add(key string) bool { return ps.m.Upsert(key, time.Now()) }
func (ps *PinnedSet) Has(key string) bool { return ps.m.Has(key) }
func (ps *PinnedSet) Size() int           { return ps.m.Len() }
func (ps *PinnedSet) Keys() []string      { return ps.m.Keys() }

func (ps *PinnedSet) GetPinTime(key string) time.Time {
	v, _ := ps.m.Get(key)
	return v
}

// RemoveIfPresent atomically checks, removes, and returns the pin time.
func (ps *PinnedSet) RemoveIfPresent(key string) (time.Time, bool) {
	return ps.m.DeleteAndGet(key)
}

// KnownFiles tracks which files/manifests are known to the system.
type KnownFiles struct {
	m *syncmap.Map[string, bool]
}

func NewKnownFiles() *KnownFiles {
	return &KnownFiles{m: syncmap.New[string, bool]()}
}

func (kf *KnownFiles) Add(key string)       { kf.m.SetIfAbsent(key, true) }
func (kf *KnownFiles) Has(key string) bool  { return kf.m.Has(key) }
func (kf *KnownFiles) All() map[string]bool { return kf.m.Snapshot() }
