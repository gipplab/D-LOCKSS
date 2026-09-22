package clusters

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"dlockss/internal/testutil"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/state"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// mockState implements state.ReadOnly for tests. List sends pins then closes the channel.
type mockState struct {
	pins []api.Pin
}

func (m *mockState) List(ctx context.Context, out chan<- api.Pin) error {
	for _, p := range m.pins {
		select {
		case out <- p:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(out)
	return nil
}

func (m *mockState) Has(ctx context.Context, c api.Cid) (bool, error) {
	return false, nil
}

func (m *mockState) Get(ctx context.Context, c api.Cid) (api.Pin, error) {
	return api.Pin{}, state.ErrNotFound
}

// mockConsensus implements ConsensusClient for tests.
type mockConsensus struct {
	st state.ReadOnly
}

func (m *mockConsensus) LogPin(ctx context.Context, pin api.Pin) error   { return nil }
func (m *mockConsensus) LogUnpin(ctx context.Context, pin api.Pin) error { return nil }
func (m *mockConsensus) State(ctx context.Context) (state.ReadOnly, error) {
	return m.st, nil
}
func (m *mockConsensus) Peers(ctx context.Context) ([]peer.ID, error) {
	return nil, nil
}
func (m *mockConsensus) Shutdown(ctx context.Context) error { return nil }

// mockIPFSForTracker implements ipfsPinner and records Pin calls.
type mockIPFSForTracker struct {
	mu       sync.Mutex
	pinCalls []cid.Cid
	blocks   map[string][]byte
	blockErr error
}

func (m *mockIPFSForTracker) IsPinned(ctx context.Context, c cid.Cid) (bool, error) {
	return false, nil
}
func (m *mockIPFSForTracker) PinRecursive(ctx context.Context, c cid.Cid) error {
	m.mu.Lock()
	m.pinCalls = append(m.pinCalls, c)
	m.mu.Unlock()
	return nil
}
func (m *mockIPFSForTracker) GetBlock(ctx context.Context, c cid.Cid) ([]byte, error) {
	if m.blockErr != nil {
		return nil, m.blockErr
	}
	if m.blocks != nil {
		if b, ok := m.blocks[c.String()]; ok {
			return b, nil
		}
	}
	return nil, nil
}

func manifestBlock(t *testing.T, ingester peer.ID) (cid.Cid, []byte) {
	t.Helper()
	payload, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err != nil {
		t.Fatal(err)
	}
	ro := schema.NewResearchObject("file://test", ingester, payload, 12)
	b, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	return payload, b
}
func TestPinTracker_allocation_pin(t *testing.T) {
	ourPeer := testutil.MustPeerID(t, "our")
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, nil)

	state := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{ourPeer}},
		},
	}
	consensus := &mockConsensus{st: state}

	pt.syncState(consensus)

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 PinRecursive call, got %d", n)
	}
	if n >= 1 && ipfs.pinCalls[0] != c1 {
		t.Errorf("PinRecursive called with %s, want %s", ipfs.pinCalls[0], c1)
	}
}

func TestPinTracker_allocation_skip(t *testing.T) {
	// Since v0.0.3, allocations are ignored — all nodes on a shard pin
	// everything in the shard's CRDT. This test verifies that a pin
	// allocated to another peer is still synced locally.
	otherPeer := testutil.MustPeerID(t, "other")
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, nil)

	state := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{otherPeer}},
		},
	}
	consensus := &mockConsensus{st: state}

	pt.syncState(consensus)

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 PinRecursive call (allocations ignored, pin everything), got %d", n)
	}
}

func TestPinTracker_empty_allocations_full_replication(t *testing.T) {
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, nil)

	// Empty Allocations means "pin everywhere" (full replication)
	state := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: nil},
		},
	}
	consensus := &mockConsensus{st: state}

	pt.syncState(consensus)

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 PinRecursive call for empty Allocations (full replication), got %d", n)
	}
}

func TestPinTracker_tracking_released_when_removed_from_CRDT(t *testing.T) {
	// Since v0.0.3, PinTracker does NOT call UnpinRecursive — it only
	// releases tracking. Actual IPFS unpins are handled by the reshard
	// pass which is migration-aware.
	ourPeer := testutil.MustPeerID(t, "our")
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	removed := make([]string, 0)
	onRemoved := func(cidStr string) { removed = append(removed, cidStr) }
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, onRemoved, nil, nil)

	stateWithPin := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{ourPeer}},
		},
	}
	consensusWithPin := &mockConsensus{st: stateWithPin}
	pt.syncState(consensusWithPin)

	ipfs.mu.Lock()
	pinCalls := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if pinCalls != 1 {
		t.Fatalf("after first sync: expected 1 PinRecursive, got %d", pinCalls)
	}

	// Second sync: state has no pins (removed from CRDT)
	stateEmpty := &mockState{pins: nil}
	consensusEmpty := &mockConsensus{st: stateEmpty}
	pt.syncState(consensusEmpty)

	// PinTracker should NOT call UnpinRecursive (migration-safe);
	// the ipfsPinner interface doesn't even include UnpinRecursive.
	// But onPinRemoved callback should have been called.
	if len(removed) != 1 {
		t.Errorf("expected onPinRemoved called once, got %d", len(removed))
	}
	if len(removed) >= 1 && removed[0] != c1.String() {
		t.Errorf("onPinRemoved called with %s, want %s", removed[0], c1)
	}
}

func TestPinTracker_refusesUntrustedOrigin(t *testing.T) {
	trusted := testutil.MustPeerID(t, "trusted")
	untrusted := testutil.MustPeerID(t, "untrusted")
	c1, block := manifestBlock(t, untrusted)

	tm := trust.NewTrustManager("open")
	if _, invalid := tm.AddOrigins([]string{trusted.String()}); len(invalid) > 0 {
		t.Fatalf("trusted peer id not encodable: %v", invalid)
	}

	ipfs := &mockIPFSForTracker{blocks: map[string][]byte{c1.String(): block}}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, tm)
	pt.syncState(&mockConsensus{st: &mockState{pins: []api.Pin{{Cid: api.NewCid(c1)}}}})

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 0 {
		t.Errorf("expected no PinRecursive for untrusted origin, got %d", n)
	}
}

func TestPinTracker_pinsTrustedOrigin(t *testing.T) {
	trusted := testutil.MustPeerID(t, "trusted")
	c1, block := manifestBlock(t, trusted)

	tm := trust.NewTrustManager("open")
	if _, invalid := tm.AddOrigins([]string{trusted.String()}); len(invalid) > 0 {
		t.Fatalf("trusted peer id not encodable: %v", invalid)
	}

	ipfs := &mockIPFSForTracker{blocks: map[string][]byte{c1.String(): block}}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, tm)
	pt.syncState(&mockConsensus{st: &mockState{pins: []api.Pin{{Cid: api.NewCid(c1)}}}})

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 PinRecursive for trusted origin, got %d", n)
	}
}

func TestPinTracker_refusesWhenManifestUnreadable(t *testing.T) {
	tm := trust.NewTrustManager("open")
	tm.AddOrigins([]string{testutil.MustPeerID(t, "trusted").String()})
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{blockErr: fmt.Errorf("offline")}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, tm)
	pt.syncState(&mockConsensus{st: &mockState{pins: []api.Pin{{Cid: api.NewCid(c1)}}}})

	ipfs.mu.Lock()
	n := len(ipfs.pinCalls)
	ipfs.mu.Unlock()
	if n != 0 {
		t.Errorf("expected no PinRecursive when origin check cannot read manifest, got %d", n)
	}
}
