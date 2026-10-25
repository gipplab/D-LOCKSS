// Package pinset maintains the per-shard replicated pin sets.
//
// Every member of a shard must hold every manifest in that shard's pin set.
// Convergence between members rides on the protocol's own gossip — Ingest
// messages, ReplicationRequests (periodic anti-entropy), and PINNED
// announcements — so no consensus machinery is needed. This replaces the
// embedded IPFS Cluster + CRDT stack of the original implementation.
//
// Each joined shard runs a tracker: a loop that makes the local Kubo state
// match the set (pinning missing manifests in bounded background fetches)
// and fires callbacks when a manifest is first locally synced or leaves the
// set. Removal from the set never unpins in IPFS; the reshard and orphan
// passes own unpinning.
package pinset

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/badbits"
	"dlockss/internal/kubo"
	"dlockss/internal/schema"
	"dlockss/internal/trust"
)

const (
	syncInterval    = 10 * time.Second
	pinCheckTimeout = 30 * time.Second
	fetchTimeout    = 5 * time.Minute
	maxFetches      = 3 // concurrent background fetches per shard
)

// Manager owns all per-shard pin sets of this node.
type Manager struct {
	ctx     context.Context
	dir     string // persistence directory (DLOCKSS_CLUSTER_STORE)
	client  *kubo.Client
	badbits *badbits.Filter
	trust   *trust.Manager

	// OnSynced fires once per manifest when it is first locally pinned and
	// accepted; OnRemoved fires when a manifest leaves a set.
	OnSynced  func(c cid.Cid)
	OnRemoved func(c cid.Cid)

	mu     sync.Mutex
	shards map[string]*shardSet
}

type shardSet struct {
	id      string
	cancel  context.CancelFunc
	trigger chan struct{}

	mu       sync.Mutex
	members  map[cid.Cid]struct{}
	tracked  map[cid.Cid]struct{}
	fetching map[cid.Cid]struct{}
	slots    chan struct{}
}

// NewManager creates the pinset manager; trackers live until ctx ends.
func NewManager(ctx context.Context, dir string, client *kubo.Client, bb *badbits.Filter, tm *trust.Manager) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating pinset store %s: %w", dir, err)
	}
	return &Manager{
		ctx:     ctx,
		dir:     dir,
		client:  client,
		badbits: bb,
		trust:   tm,
		shards:  map[string]*shardSet{},
	}, nil
}

func (m *Manager) path(shardID string) string {
	name := shardID
	if name == "" {
		name = "root"
	}
	return filepath.Join(m.dir, "pinset-"+name+".json")
}

// Join loads (or creates) the shard's pin set and starts its tracker.
// Joining an already-joined shard is a no-op.
func (m *Manager) Join(shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.shards[shardID]; ok {
		return nil
	}
	members, err := m.load(shardID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(m.ctx)
	ss := &shardSet{
		id:       shardID,
		cancel:   cancel,
		trigger:  make(chan struct{}, 1),
		members:  members,
		tracked:  map[cid.Cid]struct{}{},
		fetching: map[cid.Cid]struct{}{},
		slots:    make(chan struct{}, maxFetches),
	}
	m.shards[shardID] = ss
	go m.run(ctx, ss)
	slog.Info("pinset: joined shard", "shard", shardID, "pins", len(members))
	return nil
}

// Leave stops the shard's tracker. The set stays on disk and local IPFS pins
// stay in place (reshard/orphan passes remove what should go).
func (m *Manager) Leave(shardID string) {
	m.mu.Lock()
	ss, ok := m.shards[shardID]
	if ok {
		delete(m.shards, shardID)
	}
	m.mu.Unlock()
	if ok {
		ss.cancel()
		slog.Info("pinset: left shard", "shard", shardID)
	}
}

// Joined reports whether the shard's tracker is running.
func (m *Manager) Joined(shardID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.shards[shardID]
	return ok
}

func (m *Manager) get(shardID string) (*shardSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ss, ok := m.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("not a member of shard %q", shardID)
	}
	return ss, nil
}

// Add puts a manifest into the shard's set, persists, and triggers a sync.
// The save runs under the set's lock so concurrent mutations cannot persist
// an older snapshot last.
func (m *Manager) Add(shardID string, k cid.Cid) error {
	ss, err := m.get(shardID)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	_, present := ss.members[k]
	ss.members[k] = struct{}{}
	if !present {
		if err := m.save(shardID, ss.snapshotLocked()); err != nil {
			slog.Warn("pinset: persist failed", "shard", shardID, "err", err)
		}
	}
	ss.mu.Unlock()
	m.TriggerSync(shardID)
	return nil
}

// AddIfAbsent adds only when missing; it reports whether it added.
func (m *Manager) AddIfAbsent(shardID string, k cid.Cid) (bool, error) {
	ss, err := m.get(shardID)
	if err != nil {
		return false, err
	}
	ss.mu.Lock()
	if _, ok := ss.members[k]; ok {
		ss.mu.Unlock()
		return false, nil
	}
	ss.mu.Unlock()
	return true, m.Add(shardID, k)
}

// Remove deletes a manifest from the shard's set. The local IPFS pin stays.
func (m *Manager) Remove(shardID string, k cid.Cid) error {
	ss, err := m.get(shardID)
	if err != nil {
		return err
	}
	ss.mu.Lock()
	delete(ss.members, k)
	if err := m.save(shardID, ss.snapshotLocked()); err != nil {
		slog.Warn("pinset: persist failed", "shard", shardID, "err", err)
	}
	ss.mu.Unlock()
	m.TriggerSync(shardID)
	return nil
}

// RemoveFromAll deletes a manifest from every joined shard's set. Used when
// a local copy is released: a leftover membership (e.g. in a pinset joined
// for custodial injection) would otherwise make the tracker re-pin the
// manifest and keep advertising a copy this node no longer holds.
func (m *Manager) RemoveFromAll(k cid.Cid) {
	m.mu.Lock()
	shards := make([]string, 0, len(m.shards))
	for id := range m.shards {
		shards = append(shards, id)
	}
	m.mu.Unlock()
	for _, id := range shards {
		_ = m.Remove(id, k) // a shard left concurrently is fine
	}
}

// List returns the shard's pins.
func (m *Manager) List(shardID string) ([]cid.Cid, error) {
	ss, err := m.get(shardID)
	if err != nil {
		return nil, err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.snapshotLocked(), nil
}

// TriggerSync coalesces an immediate tracker pass for the shard.
func (m *Manager) TriggerSync(shardID string) {
	m.mu.Lock()
	ss, ok := m.shards[shardID]
	m.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ss.trigger <- struct{}{}:
	default:
	}
}

func (ss *shardSet) snapshotLocked() []cid.Cid {
	out := make([]cid.Cid, 0, len(ss.members))
	for k := range ss.members {
		out = append(out, k)
	}
	return out
}

// MigratePins moves every pin of the source shard whose payload key matches
// belongsToDest into the destination shard. Returns the number of failures.
func (m *Manager) MigratePins(ctx context.Context, from, to string, belongsToDest func(payloadKey string) bool) int {
	pins, err := m.List(from)
	if err != nil {
		return 0
	}
	failures := 0
	for _, k := range pins {
		// Bound each resolution: one unfetchable manifest must not hang the
		// whole migration.
		rctx, cancel := context.WithTimeout(ctx, pinCheckTimeout)
		payload := ResolvePayloadKey(rctx, m.client, k)
		cancel()
		if !belongsToDest(payload) {
			continue
		}
		if err := m.Add(to, k); err != nil {
			failures++
			continue
		}
		if err := m.Remove(from, k); err != nil {
			failures++
		}
	}
	if failures > 0 {
		slog.Warn("pinset: migration had failures", "from", from, "to", to, "failures", failures)
	}
	return failures
}

// ---- persistence ----

type fileFormat struct {
	Pins []string `json:"pins"`
}

func (m *Manager) load(shardID string) (map[cid.Cid]struct{}, error) {
	members := map[cid.Cid]struct{}{}
	raw, err := os.ReadFile(m.path(shardID))
	if err != nil {
		if os.IsNotExist(err) {
			return members, nil
		}
		return nil, err
	}
	var f fileFormat
	if err := json.Unmarshal(raw, &f); err != nil {
		slog.Warn("pinset: corrupt state file, starting empty", "shard", shardID, "err", err)
		return members, nil
	}
	for _, s := range f.Pins {
		if k, err := cid.Decode(s); err == nil {
			members[k] = struct{}{}
		}
	}
	return members, nil
}

func (m *Manager) save(shardID string, pins []cid.Cid) error {
	f := fileFormat{Pins: make([]string, 0, len(pins))}
	for _, k := range pins {
		f.Pins = append(f.Pins, k.String())
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	path := m.path(shardID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---- tracker ----

func (m *Manager) run(ctx context.Context, ss *shardSet) {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	m.syncOnce(ctx, ss)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-ss.trigger:
		}
		m.syncOnce(ctx, ss)
	}
}

// syncOnce reconciles local Kubo pins with the set. Local checks are quick
// (30 s cap); downloads run in bounded background fetches so a slow file can
// never stall tracking of files that are already local.
func (m *Manager) syncOnce(ctx context.Context, ss *shardSet) {
	ss.mu.Lock()
	members := ss.snapshotLocked()
	ss.mu.Unlock()

	inSet := make(map[cid.Cid]struct{}, len(members))
	for _, k := range members {
		inSet[k] = struct{}{}
		if m.badbits.IsBlocked(k.String()) {
			slog.Debug("pinset: skipping denylisted pin", "cid", k)
			continue
		}
		checkCtx, cancel := context.WithTimeout(ctx, pinCheckTimeout)
		pinned, err := m.client.IsPinned(checkCtx, k)
		cancel()
		if err != nil {
			slog.Debug("pinset: pin check failed", "cid", k, "err", err)
			continue
		}
		if !pinned {
			m.fetchAsync(ctx, ss, k)
			continue
		}
		acceptCtx, cancel := context.WithTimeout(ctx, pinCheckTimeout)
		ok := m.accepts(acceptCtx, k)
		cancel()
		if ok {
			m.markTracked(ss, k)
		}
	}

	// Release tracking for pins that left the set. IPFS pins stay: during a
	// split the same node may hold the file for a child shard, and the
	// reshard/orphan passes remove what really should go.
	ss.mu.Lock()
	var removed []cid.Cid
	for k := range ss.tracked {
		if _, ok := inSet[k]; !ok {
			delete(ss.tracked, k)
			removed = append(removed, k)
		}
	}
	ss.mu.Unlock()
	for _, k := range removed {
		if m.OnRemoved != nil {
			m.OnRemoved(k)
		}
	}
}

// fetchAsync pins a missing manifest in the background. Busy slots or an
// in-flight fetch simply defer to a later sync pass.
func (m *Manager) fetchAsync(ctx context.Context, ss *shardSet, k cid.Cid) {
	ss.mu.Lock()
	if _, busy := ss.fetching[k]; busy {
		ss.mu.Unlock()
		return
	}
	select {
	case ss.slots <- struct{}{}:
	default:
		ss.mu.Unlock()
		return
	}
	ss.fetching[k] = struct{}{}
	ss.mu.Unlock()

	go func() {
		defer func() {
			ss.mu.Lock()
			delete(ss.fetching, k)
			ss.mu.Unlock()
			<-ss.slots
		}()
		fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
		defer cancel()
		if !m.accepts(fctx, k) {
			return
		}
		if err := m.client.PinManifest(fctx, k); err != nil {
			slog.Debug("pinset: fetch failed", "cid", k, "err", err)
			return
		}
		m.markTracked(ss, k)
	}()
}

// accepts enforces the origin gate: when origins are restricted, the
// manifest must be readable and its ingester trusted (failing closed on
// unreadable blocks). Without restriction everything passes.
func (m *Manager) accepts(ctx context.Context, k cid.Cid) bool {
	if m.trust == nil || !m.trust.RestrictsOrigins() {
		return true
	}
	block, err := m.client.GetBlock(ctx, k)
	if err != nil {
		return false
	}
	return m.trust.CheckManifestOrigin(block) == nil
}

func (m *Manager) markTracked(ss *shardSet, k cid.Cid) {
	ss.mu.Lock()
	_, already := ss.tracked[k]
	ss.tracked[k] = struct{}{}
	ss.mu.Unlock()
	if !already && m.OnSynced != nil {
		m.OnSynced(k)
	}
}

// ---- shared helpers ----

// ResolvePayloadKey returns the payload CID string for shard placement,
// falling back to the manifest CID string when the manifest is unreadable.
func ResolvePayloadKey(ctx context.Context, client *kubo.Client, manifest cid.Cid) string {
	if ro, err := ReadManifest(ctx, client, manifest); err == nil && ro.Payload.Defined() {
		return ro.Payload.String()
	}
	return manifest.String()
}

// ReadManifest fetches and decodes a ResearchObject.
func ReadManifest(ctx context.Context, client *kubo.Client, manifest cid.Cid) (*schema.ResearchObject, error) {
	block, err := client.GetBlock(ctx, manifest)
	if err != nil {
		return nil, err
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return nil, err
	}
	return &ro, nil
}

