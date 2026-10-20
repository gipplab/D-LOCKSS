package shard

import (
	"context"
	"testing"
	"time"

	"dlockss/internal/common"
)

func TestSplitShard_NoDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "")

	done := make(chan struct{})
	go func() {
		currentShard := sm.getCurrentShard()
		targetChild := common.GetBinaryPrefix(sm.h.ID().String(), len(currentShard)+1)
		sm.moveToShard(currentShard, targetChild, false)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("splitShard timed out - likely deadlock")
	}

	expectedShard := common.GetBinaryPrefix(sm.h.ID().String(), 1)
	if currentShard := sm.GetShardInfo(); currentShard != expectedShard {
		t.Errorf("expected shard %s, got %s", expectedShard, currentShard)
	}
}
