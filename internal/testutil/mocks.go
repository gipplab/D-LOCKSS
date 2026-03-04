// Package testutil provides shared mocks and helpers for tests across the
// D-LOCKSS codebase. Import only from *_test.go files.
package testutil

import (
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

// MockIPFSClient is a no-op implementation of ipfs.IPFSClient.
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
func (m *MockIPFSClient) PinRecursive(ctx context.Context, c cid.Cid) error          { return nil }
func (m *MockIPFSClient) UnpinRecursive(ctx context.Context, c cid.Cid) error        { return nil }
func (m *MockIPFSClient) IsPinned(ctx context.Context, c cid.Cid) (bool, error)      { return false, nil }
func (m *MockIPFSClient) GetFileSize(ctx context.Context, c cid.Cid) (uint64, error) { return 0, nil }
func (m *MockIPFSClient) GetPeerID(ctx context.Context) (string, error)              { return "mock-peer-id", nil }
func (m *MockIPFSClient) SwarmConnect(ctx context.Context, addrs []string) error     { return nil }

// MockDHTProvider is a no-op implementation of common.DHTProvider that also
// satisfies routing.Routing.
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

var _ routing.Routing = (*MockDHTProvider)(nil)

// MockClusterManager is a no-op implementation of clusters.ClusterManagerInterface.
type MockClusterManager struct{}

func (m *MockClusterManager) JoinShard(ctx context.Context, shardID string, bootstrapPeers []multiaddr.Multiaddr) error {
	return nil
}
func (m *MockClusterManager) LeaveShard(shardID string) error { return nil }
func (m *MockClusterManager) Pin(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
	return nil
}
func (m *MockClusterManager) PinIfAbsent(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
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

// MustPeerID creates a deterministic peer.ID from a seed string.
func MustPeerID(t *testing.T, seed string) peer.ID {
	t.Helper()
	mh, err := multihash.Sum([]byte(seed), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("multihash.Sum: %v", err)
	}
	pid, err := peer.IDFromBytes(mh)
	if err != nil {
		t.Fatalf("peer.IDFromBytes: %v", err)
	}
	return pid
}
