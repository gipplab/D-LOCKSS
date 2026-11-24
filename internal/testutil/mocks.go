// Package testutil provides shared mocks and helpers for tests across the
// D-LOCKSS codebase. Import only from *_test.go files.
package testutil

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"

	"dlockss/pkg/schema"
)

// MockIPFSClient is a no-op implementation of ipfs.IPFSClient.
type MockIPFSClient struct{}

func (m *MockIPFSClient) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	return cid.Cid{}, nil
}
func (m *MockIPFSClient) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	return nil, nil
}
func (m *MockIPFSClient) PinRecursive(ctx context.Context, c cid.Cid) error     { return nil }
func (m *MockIPFSClient) PinManifest(ctx context.Context, c cid.Cid) error      { return nil }
func (m *MockIPFSClient) UnpinRecursive(ctx context.Context, c cid.Cid) error   { return nil }
func (m *MockIPFSClient) IsPinned(ctx context.Context, c cid.Cid) (bool, error) { return false, nil }
func (m *MockIPFSClient) RecursivePins(ctx context.Context) (map[cid.Cid]string, error) {
	return nil, nil
}
func (m *MockIPFSClient) DAGSize(ctx context.Context, c cid.Cid) (uint64, error) { return 0, nil }
func (m *MockIPFSClient) NodeID(context.Context) (peer.ID, error)                { return "", nil }
func (m *MockIPFSClient) SignWithNodeKey(ctx context.Context, data []byte) ([]byte, error) {
	return nil, nil
}

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

func (m *MockClusterManager) JoinShard(ctx context.Context, shardID string) error {
	return nil
}
func (m *MockClusterManager) LeaveShard(shardID string) error { return nil }
func (m *MockClusterManager) Pin(ctx context.Context, shardID string, c cid.Cid) error {
	return nil
}
func (m *MockClusterManager) PinIfAbsent(ctx context.Context, shardID string, c cid.Cid) error {
	return nil
}
func (m *MockClusterManager) Unpin(ctx context.Context, shardID string, c cid.Cid) error { return nil }
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

// Key is an Ed25519 key and its peer ID, for a Kubo node or a D-LOCKSS helper.
type Key struct {
	Priv crypto.PrivKey
	ID   peer.ID
}

// NewKey generates a Key.
func NewKey(t *testing.T) Key {
	t.Helper()
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return Key{Priv: priv, ID: id}
}

// SignWithNodeKey signs data as 'ipfs key sign' does, so a Key can stand in
// for a Kubo node.
func (k Key) SignWithNodeKey(_ context.Context, data []byte) ([]byte, error) {
	return k.Priv.Sign(append([]byte(schema.KuboSignPrefix), data...))
}

// Bind returns k's binding for helper.
func (k Key) Bind(t *testing.T, helper peer.ID) *schema.Binding {
	t.Helper()
	sig, err := k.SignWithNodeKey(context.Background(), schema.BindingMessage(helper))
	if err != nil {
		t.Fatal(err)
	}
	return &schema.Binding{Node: k.ID, Sig: sig}
}
