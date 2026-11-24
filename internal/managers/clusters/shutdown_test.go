package clusters

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/state"
	"github.com/multiformats/go-multihash"
)

// leaveShard runs LeaveShard and fails the test if it does not return.
func leaveShard(t *testing.T, cm *ClusterManager, shard string) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cm.LeaveShard(shard) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatalf("LeaveShard(%q) did not return", shard)
		return nil
	}
}

// shortSetupTimeout lowers crdtSetupTimeout for one test.
func shortSetupTimeout(t *testing.T) {
	t.Helper()
	old := crdtSetupTimeout
	crdtSetupTimeout = 300 * time.Millisecond
	t.Cleanup(func() { crdtSetupTimeout = old })
}

// Leaving a shard right after JoinShard, before its CRDT has finished its
// setup, must neither hang nor crash.
func TestLeaveShard_RightAfterJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm := newTestManager(t, ctx, "open", nil)
	if err := cm.JoinShard(ctx, "1"); err != nil {
		t.Fatal(err)
	}
	if err := leaveShard(t, cm, "1"); err != nil {
		t.Errorf("LeaveShard: %v", err)
	}
}

// A real failed CRDT setup: the CRDT's broadcast topic, the base58 MD5
// multihash of its cluster name, is already joined.
func TestLeaveShard_SetupFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	shortSetupTimeout(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm := newTestManager(t, ctx, "open", nil)
	mh, err := multihash.Sum([]byte(cm.cfg.PubsubTopicPrefix+"-"+cm.cfg.TopicName+"-crdt-1"), multihash.MD5, -1)
	if err != nil {
		t.Fatal(err)
	}
	held, err := cm.pubsub.Join(mh.B58String())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	if err := cm.JoinShard(ctx, "1"); err != nil {
		t.Fatal(err)
	}
	if err := leaveShard(t, cm, "1"); err == nil {
		t.Error("LeaveShard should report the failed setup")
	}
}

// unreadyConsensus is a CRDT whose setup never finishes.
type unreadyConsensus struct {
	mockConsensus
	shutdown bool
}

func (c *unreadyConsensus) State(ctx context.Context) (state.ReadOnly, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *unreadyConsensus) Shutdown(context.Context) error {
	c.shutdown = true
	return nil
}

// A CRDT whose setup has not finished is not shut down, since ipfs-cluster's
// Shutdown would block, but the shard is still dropped.
func TestLeaveShard_SetupUnfinished(t *testing.T) {
	shortSetupTimeout(t)
	c := &unreadyConsensus{}
	cancelled := false
	cm := &ClusterManager{clusters: map[string]*embeddedCluster{
		"1": {consensus: c, cancel: func() { cancelled = true }},
	}}
	if err := leaveShard(t, cm, "1"); err == nil {
		t.Error("LeaveShard should report the unfinished setup")
	}
	if c.shutdown {
		t.Error("Shutdown must not be called before the setup has finished")
	}
	if !cancelled {
		t.Error("LeaveShard should still stop the signal subscription")
	}
	if _, err := cm.shardCluster("1"); err == nil {
		t.Error("shard should be gone after LeaveShard")
	}
}
