package clusters_test

import (
	"context"
	"testing"
	"time"

	"dlockss/internal/config"
	"dlockss/internal/managers/clusters"
	"dlockss/internal/testutil"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestClusterManager_Lifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a dummy libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	// Setup PubSub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatalf("Failed to create pubsub: %v", err)
	}

	// Setup Datastore
	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// Setup Routing
	dht := &testutil.MockDHTProvider{}

	// 2. Initialize ClusterManager
	cm := clusters.NewClusterManager(clusters.ClusterManagerConfig{
		Cfg:        config.DefaultConfig(),
		Host:       h,
		PubSub:     ps,
		DHT:        dht,
		Datastore:  ds,
		IPFSClient: &testutil.MockIPFSClient{},
	})

	// 3. Test JoinShard (Primary Shard "1")
	shard1 := "1"
	err = cm.JoinShard(ctx, shard1, nil)
	if err != nil {
		t.Fatalf("JoinShard failed for %s: %v", shard1, err)
	}
	t.Logf("Joined shard %s successfully", shard1)

	// 4. Test JoinShard (Secondary Shard "10" - Dual Homing)
	shard10 := "10"
	err = cm.JoinShard(ctx, shard10, nil)
	if err != nil {
		t.Fatalf("JoinShard failed for %s: %v", shard10, err)
	}
	t.Logf("Joined shard %s successfully (Dual Homing)", shard10)

	// 5. Test Pinning (Simulate ingest)
	testCid, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	err = cm.Pin(ctx, shard1, testCid, 5, 10)
	if err != nil {
		t.Errorf("Pin failed: %v", err)
	}

	// 6. Test Migration (Stubbed)
	err = cm.MigratePins(ctx, shard1, shard10)
	if err != nil {
		t.Errorf("MigratePins failed: %v", err)
	}

	// Allow the CRDT batch worker to finish processing before we shut down
	// the shard. The ipfs-cluster library's batch worker panics on nil
	// context if a batch item is still in-flight when Consensus.Shutdown
	// races with context cancellation.
	time.Sleep(500 * time.Millisecond)

	// 7. Test LeaveShard (Cleanup old shard)
	err = cm.LeaveShard(shard1)
	if err != nil {
		t.Errorf("LeaveShard failed: %v", err)
	}
	t.Logf("Left shard %s", shard1)

	// Verify we can't pin to the left shard
	err = cm.Pin(ctx, shard1, testCid, 5, 10)
	if err == nil {
		t.Error("Pin should fail for left shard")
	}
}

func TestDeterministicSecrets(t *testing.T) {
	shardA := "10"
	shardB := "10"
	shardC := "11"

	// Secrets should be deterministic
	secA, _ := clusters.GenerateClusterSecretHex(shardA)
	secB, _ := clusters.GenerateClusterSecretHex(shardB)
	secC, _ := clusters.GenerateClusterSecretHex(shardC)

	if secA != secB {
		t.Errorf("Secrets for same shard ID should match: %s != %s", secA, secB)
	}
	if secA == secC {
		t.Errorf("Secrets for different shard IDs should differ")
	}
}

func TestSelectAllocations(t *testing.T) {
	// Build deterministic peer list (order will be sorted by peer ID string).
	pA := testutil.MustPeerID(t, "peerA")
	pB := testutil.MustPeerID(t, "peerB")
	pC := testutil.MustPeerID(t, "peerC")
	pD := testutil.MustPeerID(t, "peerD")
	peers := []peer.ID{pD, pA, pC, pB} // unsorted

	c1, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	c2, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

	// n <= 0 → nil
	if got := clusters.SelectAllocations(peers, c1, 0); got != nil {
		t.Errorf("SelectAllocations(peers, c1, 0) = %v, want nil", got)
	}
	if got := clusters.SelectAllocations(peers, c1, -1); got != nil {
		t.Errorf("SelectAllocations(peers, c1, -1) = %v, want nil", got)
	}

	// empty peers → nil
	if got := clusters.SelectAllocations(nil, c1, 2); got != nil {
		t.Errorf("SelectAllocations(nil, c1, 2) = %v, want nil", got)
	}
	if got := clusters.SelectAllocations([]peer.ID{}, c1, 2); got != nil {
		t.Errorf("SelectAllocations([], c1, 2) = %v, want nil", got)
	}

	// n >= len(peers) → full sorted list (no truncation)
	all := clusters.SelectAllocations(peers, c1, 10)
	if len(all) != 4 {
		t.Errorf("SelectAllocations(peers, c1, 10) len = %d, want 4", len(all))
	}
	// Should be sorted by peer ID string
	for i := 1; i < len(all); i++ {
		if all[i].String() < all[i-1].String() {
			t.Errorf("SelectAllocations(peers, c1, 10) not sorted: %s before %s", all[i-1], all[i])
		}
	}

	// Same CID → same allocation (determinism)
	a1 := clusters.SelectAllocations(peers, c1, 2)
	a2 := clusters.SelectAllocations(peers, c1, 2)
	if len(a1) != 2 || len(a2) != 2 {
		t.Fatalf("expected 2 allocations each, got %d and %d", len(a1), len(a2))
	}
	if a1[0] != a2[0] || a1[1] != a2[1] {
		t.Errorf("same CID must yield same allocations: %v vs %v", a1, a2)
	}

	// Exactly n when n < len(peers)
	for n := 1; n <= 4; n++ {
		got := clusters.SelectAllocations(peers, c1, n)
		if len(got) != n {
			t.Errorf("SelectAllocations(peers, c1, %d) len = %d, want %d", n, len(got), n)
		}
	}

	// Different CIDs can yield different start indices (hash-based)
	alloc1 := clusters.SelectAllocations(peers, c1, 2)
	alloc2 := clusters.SelectAllocations(peers, c2, 2)
	// They might be equal by chance; we only check we get 2 each and no panic
	if len(alloc1) != 2 || len(alloc2) != 2 {
		t.Errorf("expected 2 allocations for c1 and c2, got %d and %d", len(alloc1), len(alloc2))
	}
}
