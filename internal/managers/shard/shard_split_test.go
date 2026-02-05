package shard

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"

	"dlockss/internal/common"
	"dlockss/internal/managers/storage"
	"dlockss/internal/telemetry"
	"dlockss/pkg/ipfs"
)

// MockIPFSClient implements ipfs.IPFSClient
type MockIPFSClient struct{}

func (m *MockIPFSClient) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) ImportReader(ctx context.Context, reader io.Reader) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	return nil, nil
}
func (m *MockIPFSClient) PinRecursive(ctx context.Context, cid cid.Cid) error          { return nil }
func (m *MockIPFSClient) UnpinRecursive(ctx context.Context, cid cid.Cid) error        { return nil }
func (m *MockIPFSClient) IsPinned(ctx context.Context, cid cid.Cid) (bool, error)      { return false, nil }
func (m *MockIPFSClient) GetFileSize(ctx context.Context, cid cid.Cid) (uint64, error) { return 0, nil }
func (m *MockIPFSClient) VerifyDAGCompleteness(ctx context.Context, rootCID cid.Cid) (bool, error) {
	return true, nil
}
func (m *MockIPFSClient) GetPeerID(ctx context.Context) (string, error)          { return "mock-peer-id", nil }
func (m *MockIPFSClient) SwarmConnect(ctx context.Context, addrs []string) error { return nil }

// MockDHTProvider implements common.DHTProvider and routing.Routing
type MockDHTProvider struct{}

func (m *MockDHTProvider) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	return nil
}
func (m *MockDHTProvider) Provide(ctx context.Context, key cid.Cid, broadcast bool) error { return nil }
func (m *MockDHTProvider) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, nil
}
func (m *MockDHTProvider) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return nil
}
func (m *MockDHTProvider) GetValue(context.Context, string, ...routing.Option) ([]byte, error) {
	return nil, nil
}
func (m *MockDHTProvider) SearchValue(context.Context, string, ...routing.Option) (<-chan []byte, error) {
	return nil, nil
}
func (m *MockDHTProvider) Bootstrap(context.Context) error { return nil }

var _ ipfs.IPFSClient = (*MockIPFSClient)(nil)
var _ routing.Routing = (*MockDHTProvider)(nil)

// MockClusterManager implements clusters.ClusterManagerInterface
type MockClusterManager struct{}

func (m *MockClusterManager) JoinShard(ctx context.Context, shardID string, bootstrapPeers []multiaddr.Multiaddr) error {
	return nil
}
func (m *MockClusterManager) LeaveShard(shardID string) error { return nil }
func (m *MockClusterManager) Pin(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
	return nil
}
func (m *MockClusterManager) Unpin(ctx context.Context, shardID string, c cid.Cid) error { return nil }
func (m *MockClusterManager) GetAllocations(ctx context.Context, shardID string, c cid.Cid) ([]peer.ID, error) {
	return nil, nil
}
func (m *MockClusterManager) GetPeerCount(ctx context.Context, shardID string) (int, error) {
	return 0, nil
}
func (m *MockClusterManager) MigratePins(ctx context.Context, fromShard, toShard string) error {
	return nil
}
func (m *MockClusterManager) TriggerSync(shardID string) {}

func TestSplitShard_NoDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup Host
	h, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Setup PubSub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatal(err)
	}

	// Setup Dependencies
	metrics := telemetry.NewMetricsManager()
	dht := &MockDHTProvider{}
	storageMgr := storage.NewStorageManager(dht, metrics)
	ipfsClient := &MockIPFSClient{}

	// Create ShardManager with nil signer (safe because storage is empty, so RunReshardPass returns early)
	// ds := dssync.MutexWrap(datastore.NewMapDatastore())
	clusterMgr := &MockClusterManager{}
	sm := NewShardManager(ctx, h, ps, ipfsClient, storageMgr, metrics, nil, nil, clusterMgr, "")

	// Register shard info with metrics to simulate production setup
	metrics.RegisterProviders(sm, storageMgr, nil)

	// Trigger splitShard
	done := make(chan struct{})
	go func() {
		sm.splitShard()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Success - no deadlock
	case <-time.After(5 * time.Second):
		t.Fatal("splitShard timed out - likely deadlock")
	}

	// Verify state changed
	currentShard, _ := sm.GetShardInfo()
	expectedShard := common.GetBinaryPrefix(h.ID().String(), 1)
	if currentShard != expectedShard {
		t.Errorf("expected shard %s, got %s", expectedShard, currentShard)
	}
}
