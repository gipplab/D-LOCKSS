package clusters_test

import (
	"context"
	"io"
	"testing"
	"time"

	"dlockss/internal/managers/clusters"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// MockIPFSClient is a placeholder for the IPFS client interface
type MockIPFSClient struct{}

func (m *MockIPFSClient) PinRecursive(ctx context.Context, c cid.Cid) error          { return nil }
func (m *MockIPFSClient) UnpinRecursive(ctx context.Context, c cid.Cid) error        { return nil }
func (m *MockIPFSClient) IsPinned(ctx context.Context, c cid.Cid) (bool, error)      { return false, nil }
func (m *MockIPFSClient) GetBlock(ctx context.Context, c cid.Cid) ([]byte, error)    { return nil, nil }
func (m *MockIPFSClient) GetFileSize(ctx context.Context, c cid.Cid) (uint64, error) { return 0, nil }
func (m *MockIPFSClient) GetPeerID(ctx context.Context) (string, error)              { return "mock-peer", nil }
func (m *MockIPFSClient) ImportFile(ctx context.Context, path string) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) ImportReader(ctx context.Context, r io.Reader) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) PutDagCBOR(ctx context.Context, data []byte) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) SwarmConnect(ctx context.Context, addrs []string) error { return nil }
func (m *MockIPFSClient) VerifyDAGCompleteness(ctx context.Context, c cid.Cid) (bool, error) {
	return true, nil
}

// MockRouting implements routing.Routing
type MockRouting struct{}

func (m *MockRouting) Provide(context.Context, cid.Cid, bool) error { return nil }
func (m *MockRouting) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	return nil
}
func (m *MockRouting) FindPeer(context.Context, peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, nil
}
func (m *MockRouting) PutValue(context.Context, string, []byte, ...routing.Option) error { return nil }
func (m *MockRouting) GetValue(context.Context, string, ...routing.Option) ([]byte, error) {
	return nil, nil
}
func (m *MockRouting) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	return nil, nil
}
func (m *MockRouting) Bootstrap(context.Context) error { return nil }

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
	dht := &MockRouting{}

	// 2. Initialize ClusterManager
	cm := clusters.NewClusterManager(h, ps, dht, ds, &MockIPFSClient{}, nil)

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
