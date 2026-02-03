package clusters_test

import (
	"context"
	"testing"

	"dlockss/internal/managers/clusters"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
)

// MockIPFSClient is a placeholder for the IPFS client interface
type MockIPFSClient struct{}

func TestClusterManager_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a dummy libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	// 2. Initialize ClusterManager
	cm := clusters.NewClusterManager(h, &MockIPFSClient{})

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
