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
)

func newTestClusterManager(ctx context.Context, t *testing.T) *clusters.ClusterManager {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create libp2p host: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatalf("Failed to create pubsub: %v", err)
	}
	return clusters.NewClusterManager(clusters.ClusterManagerConfig{
		Cfg:        config.DefaultConfig(),
		Host:       h,
		PubSub:     ps,
		DHT:        &testutil.MockDHTProvider{},
		Datastore:  dssync.MutexWrap(datastore.NewMapDatastore()),
		IPFSClient: &testutil.MockIPFSClient{},
	})
}

func TestClusterManager_Lifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm := newTestClusterManager(ctx, t)

	shard1 := "1"
	if err := cm.JoinShard(ctx, shard1); err != nil {
		t.Fatalf("JoinShard failed for %s: %v", shard1, err)
	}

	// A node is a member of both shards while a split is in progress.
	shard10 := "10"
	if err := cm.JoinShard(ctx, shard10); err != nil {
		t.Fatalf("JoinShard failed for %s: %v", shard10, err)
	}

	testCid, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err := cm.Pin(ctx, shard1, testCid); err != nil {
		t.Errorf("Pin failed: %v", err)
	}

	if err := cm.MigratePins(ctx, shard1, shard10); err != nil {
		t.Errorf("MigratePins failed: %v", err)
	}

	// Let the CRDT batch worker drain before shutdown: ipfs-cluster's batch
	// worker panics on a nil context if an item is still in flight when
	// Consensus.Shutdown races with context cancellation.
	time.Sleep(500 * time.Millisecond)

	if err := cm.LeaveShard(shard1); err != nil {
		t.Errorf("LeaveShard failed: %v", err)
	}

	if err := cm.Pin(ctx, shard1, testCid); err == nil {
		t.Error("Pin should fail for left shard")
	}
}

// A merge-up returns a node to a parent shard it left at the split, so a
// shard must be joinable again after LeaveShard.
func TestClusterManager_RejoinAfterLeave(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm := newTestClusterManager(ctx, t)

	if err := cm.JoinShard(ctx, ""); err != nil {
		t.Fatalf("initial JoinShard failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := cm.LeaveShard(""); err != nil {
		t.Fatalf("LeaveShard failed: %v", err)
	}
	if err := cm.JoinShard(ctx, ""); err != nil {
		t.Fatalf("rejoin after leave failed: %v", err)
	}
	testCid, _ := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err := cm.Pin(ctx, "", testCid); err != nil {
		t.Errorf("Pin into rejoined shard failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
}
