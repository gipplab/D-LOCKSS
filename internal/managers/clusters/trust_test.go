package clusters

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/consensus/crdt"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/internal/testutil"
)

func newTestManager(t *testing.T, ctx context.Context, mode string, trusted []peer.ID) *ClusterManager {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.Security.TrustMode = mode
	cm := NewClusterManager(ClusterManagerConfig{
		Cfg:          cfg,
		Host:         h,
		PubSub:       ps,
		DHT:          &testutil.MockDHTProvider{},
		Datastore:    dssync.MutexWrap(datastore.NewMapDatastore()),
		IPFSClient:   &testutil.MockIPFSClient{},
		TrustedPeers: trusted,
	})
	t.Cleanup(func() {
		for _, shard := range []string{"0", "1"} {
			_ = cm.LeaveShard(shard)
		}
	})
	return cm
}

// joinReady joins shard and waits until its CRDT is set up, so that the
// trusted peers from its config are loaded before the test checks them.
func joinReady(t *testing.T, ctx context.Context, cm *ClusterManager, shard string) {
	t.Helper()
	if err := cm.JoinShard(ctx, shard); err != nil {
		t.Fatal(err)
	}
	c, err := cm.shardCluster(shard)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.consensus.(*crdt.Consensus).Ready(ctx):
	case <-ctx.Done():
		t.Fatalf("shard %q CRDT not ready: %v", shard, ctx.Err())
	}
}

func isTrustedWriter(t *testing.T, ctx context.Context, cm *ClusterManager, shard string, pid peer.ID) bool {
	t.Helper()
	c, err := cm.shardCluster(shard)
	if err != nil {
		t.Fatal(err)
	}
	return c.consensus.(*crdt.Consensus).IsTrustedPeer(ctx, pid)
}

// A helper writes the shard CRDTs only once its binding to an allowlisted
// node has verified, on clusters joined before and after that.
func TestTrustHelper_AllowlistMode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	node := testutil.MustPeerID(t, "node")
	helper := testutil.MustPeerID(t, "helper")
	cm := newTestManager(t, ctx, "allowlist", []peer.ID{node})

	joinReady(t, ctx, cm, "0")
	if !isTrustedWriter(t, ctx, cm, "0", node) {
		t.Fatal("allowlisted node ID should be trusted (TrustedPeers)")
	}
	if isTrustedWriter(t, ctx, cm, "0", helper) {
		t.Fatal("helper without a verified binding must not be trusted")
	}

	cm.TrustHelper(ctx, helper)
	if !isTrustedWriter(t, ctx, cm, "0", helper) {
		t.Fatal("helper should be trusted on a live cluster after TrustHelper")
	}

	joinReady(t, ctx, cm, "1")
	if !isTrustedWriter(t, ctx, cm, "1", helper) {
		t.Fatal("helper should be trusted on clusters joined after TrustHelper")
	}
}

func TestTrustHelper_OpenModeIsNoop(t *testing.T) {
	ctx := context.Background()
	cm := newTestManager(t, ctx, "open", nil)
	helper := testutil.MustPeerID(t, "helper")
	cm.TrustHelper(ctx, helper)
	if _, ok := cm.trustedHelpers[helper]; ok {
		t.Fatal("open mode trusts every writer; TrustHelper should not record helpers")
	}
}
