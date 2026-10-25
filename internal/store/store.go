// Package store tracks the node's local view: pinned manifests, known files,
// DHT provide gating, and disk headroom. All of it is in-memory; after a
// restart it is rebuilt by the startup scan and pinset sync.
package store

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/badbits"
	"dlockss/internal/kubo"
)

// Provide-gate bounds (frozen): concurrent announces and the per-announce
// timeout.
const (
	maxConcurrentProvides = 8
	provideTimeout        = time.Minute
)

// Store is the node's in-memory file registry.
type Store struct {
	badbits *badbits.Filter
	client  *kubo.Client

	provideSem chan struct{}

	mu       sync.Mutex
	pinned   map[cid.Cid]time.Time
	known    map[cid.Cid]struct{}
	sorted   []cid.Cid // round-robin order over pinned; rebuilt lazily
	dirty    bool
	cursor   int
}

// New builds a Store.
func New(client *kubo.Client, bb *badbits.Filter) *Store {
	return &Store{
		badbits:    bb,
		client:     client,
		provideSem: make(chan struct{}, maxConcurrentProvides),
		pinned:     map[cid.Cid]time.Time{},
		known:      map[cid.Cid]struct{}{},
	}
}

// Pin registers a pinned manifest (refreshing its time when already present).
// It refuses denylisted CIDs and returns whether the CID is registered.
func (s *Store) Pin(k cid.Cid) bool {
	if s.badbits.IsBlocked(k.String()) {
		slog.Warn("refusing to register denylisted CID", "cid", k)
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pinned[k]; !ok {
		s.dirty = true
	}
	s.pinned[k] = time.Now()
	return true
}

// Unpin removes a manifest from the pinned registry.
func (s *Store) Unpin(k cid.Cid) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pinned[k]; !ok {
		slog.Debug("unpin of untracked CID", "cid", k)
		return
	}
	delete(s.pinned, k)
	s.dirty = true
}

// IsPinned reports whether the manifest is registered.
func (s *Store) IsPinned(k cid.Cid) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pinned[k]
	return ok
}

// PinTime returns when the manifest was registered.
func (s *Store) PinTime(k cid.Cid) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.pinned[k]
	return t, ok
}

// PinnedCount returns the number of registered manifests.
func (s *Store) PinnedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pinned)
}

// Pinned returns a snapshot of the pinned manifests.
func (s *Store) Pinned() []cid.Cid {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cid.Cid, 0, len(s.pinned))
	for k := range s.pinned {
		out = append(out, k)
	}
	return out
}

// NextToAnnounce returns the next pinned manifest in round-robin order over
// the sorted key set, or false when nothing is pinned.
func (s *Store) NextToAnnounce() (cid.Cid, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirty {
		s.sorted = s.sorted[:0]
		for k := range s.pinned {
			s.sorted = append(s.sorted, k)
		}
		sort.Slice(s.sorted, func(i, j int) bool { return s.sorted[i].KeyString() < s.sorted[j].KeyString() })
		s.dirty = false
	}
	if len(s.sorted) == 0 {
		return cid.Undef, false
	}
	s.cursor %= len(s.sorted)
	k := s.sorted[s.cursor]
	s.cursor++
	return k, true
}

// AddKnown records a file announced to this shard (not necessarily pinned).
func (s *Store) AddKnown(k cid.Cid) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.known[k] = struct{}{}
}

// Known returns a snapshot of known files.
func (s *Store) Known() []cid.Cid {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cid.Cid, 0, len(s.known))
	for k := range s.known {
		out = append(out, k)
	}
	return out
}

// Provide announces the CID to the DHT through the bounded provide gate.
// When every slot is busy the provide is skipped; the heartbeat reprovide
// loop re-announces everything eventually.
func (s *Store) Provide(ctx context.Context, k cid.Cid) {
	select {
	case s.provideSem <- struct{}{}:
	default:
		slog.Debug("provide skipped: all slots busy", "cid", k)
		return
	}
	defer func() { <-s.provideSem }()

	pctx, cancel := context.WithTimeout(ctx, provideTimeout)
	defer cancel()
	if err := s.client.Provide(pctx, k); err != nil {
		slog.Warn("DHT provide failed", "cid", k, "err", err)
	}
}

// ProviderCount counts distinct providers of the key, excluding self, up to
// limit. It stops as soon as limit providers are seen.
func (s *Store) ProviderCount(ctx context.Context, k cid.Cid, self peer.ID, limit int) int {
	ch, err := s.client.FindProviders(ctx, k, limit+1)
	if err != nil {
		slog.Debug("findprovs failed", "cid", k, "err", err)
		return 0
	}
	seen := map[peer.ID]struct{}{}
	for info := range ch {
		if info.ID == self {
			continue
		}
		seen[info.ID] = struct{}{}
		if len(seen) >= limit {
			break
		}
	}
	return len(seen)
}
