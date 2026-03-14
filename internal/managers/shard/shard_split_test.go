package shard

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/storage"
	"dlockss/internal/testutil"
)

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
	dht := &testutil.MockDHTProvider{}
	storageMgr := storage.NewStorageManager(config.DefaultConfig(), dht, nil)
	ipfsClient := &testutil.MockIPFSClient{}

	clusterMgr := &testutil.MockClusterManager{}
	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        config.DefaultConfig(),
		Ctx:        ctx,
		Host:       h,
		PubSub:     ps,
		IPFSClient: ipfsClient,
		Storage:    storageMgr,
		Cluster:    clusterMgr,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Trigger split: compute target child and move
	done := make(chan struct{})
	go func() {
		currentShard := sm.getCurrentShard()
		targetChild := common.GetBinaryPrefix(sm.h.ID().String(), len(currentShard)+1)
		sm.moveToShard(currentShard, targetChild, false)
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
	currentShard := sm.GetShardInfo()
	expectedShard := common.GetBinaryPrefix(h.ID().String(), 1)
	if currentShard != expectedShard {
		t.Errorf("expected shard %s, got %s", expectedShard, currentShard)
	}
}
