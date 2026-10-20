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

// mockConsensus implements consensusClient for tests.
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
func (m *mockIPFSForTracker) PinManifest(ctx context.Context, c cid.Cid) error {
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

func (m *mockIPFSForTracker) pinCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pinCalls)
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

	st := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{ourPeer}},
		},
	}
	pt.syncState(&mockConsensus{st: st})

	n := ipfs.pinCount()
	if n != 1 {
		t.Errorf("expected 1 PinManifest call, got %d", n)
	}
	if n >= 1 && ipfs.pinCalls[0] != c1 {
		t.Errorf("PinManifest called with %s, want %s", ipfs.pinCalls[0], c1)
	}
}

// Allocations are informational: a pin allocated to another peer is still
// synced locally.
func TestPinTracker_allocation_to_other_peer_still_pinned(t *testing.T) {
	otherPeer := testutil.MustPeerID(t, "other")
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, nil)

	st := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{otherPeer}},
		},
	}
	pt.syncState(&mockConsensus{st: st})

	if n := ipfs.pinCount(); n != 1 {
		t.Errorf("expected 1 PinManifest call (allocations ignored, pin everything), got %d", n)
	}
}

func TestPinTracker_empty_allocations_full_replication(t *testing.T) {
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, nil)

	st := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: nil},
		},
	}
	pt.syncState(&mockConsensus{st: st})

	if n := ipfs.pinCount(); n != 1 {
		t.Errorf("expected 1 PinManifest call for empty Allocations (full replication), got %d", n)
	}
}

// A pin removed from the CRDT only releases tracking. ipfsPinner has no unpin
// method, so the IPFS pin is left for the migration-aware reshard pass.
func TestPinTracker_tracking_released_when_removed_from_CRDT(t *testing.T) {
	ourPeer := testutil.MustPeerID(t, "our")
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	var removed []string
	onRemoved := func(cidStr string) { removed = append(removed, cidStr) }
	ipfs := &mockIPFSForTracker{}
	pt := newLocalPinTracker(ipfs, "1", nil, onRemoved, nil, nil)

	withPin := &mockState{
		pins: []api.Pin{
			{Cid: api.NewCid(c1), Allocations: []peer.ID{ourPeer}},
		},
	}
	pt.syncState(&mockConsensus{st: withPin})

	if n := ipfs.pinCount(); n != 1 {
		t.Fatalf("after first sync: expected 1 PinManifest, got %d", n)
	}

	pt.syncState(&mockConsensus{st: &mockState{}})

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

	if n := ipfs.pinCount(); n != 0 {
		t.Errorf("expected no PinManifest for untrusted origin, got %d", n)
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

	if n := ipfs.pinCount(); n != 1 {
		t.Errorf("expected 1 PinManifest for trusted origin, got %d", n)
	}
}

func TestPinTracker_refusesWhenManifestUnreadable(t *testing.T) {
	tm := trust.NewTrustManager("open")
	tm.AddOrigins([]string{testutil.MustPeerID(t, "trusted").String()})
	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	ipfs := &mockIPFSForTracker{blockErr: fmt.Errorf("offline")}
	pt := newLocalPinTracker(ipfs, "1", nil, nil, nil, tm)
	pt.syncState(&mockConsensus{st: &mockState{pins: []api.Pin{{Cid: api.NewCid(c1)}}}})

	if n := ipfs.pinCount(); n != 0 {
		t.Errorf("expected no PinManifest when origin check cannot read manifest, got %d", n)
	}
}
