package shard

import (
	"context"
	"testing"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"
)

func TestPruneOrphanHandoffSent(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)

	cfg := config.DefaultConfig()
	sm.cfg = cfg
	cfg.OrphanHandoffGrace = 100 * time.Millisecond

	now := time.Now()
	old := now.Add(-5 * cfg.OrphanHandoffGrace)
	recent := now.Add(-cfg.OrphanHandoffGrace / 2)

	sm.orphanHandoffSent = map[string]map[string]*orphanHandoffInfo{
		"old-manifest": {
			"00": {lastSent: old, count: 3},
		},
		"recent-manifest": {
			"01": {lastSent: recent, count: 1},
		},
		"mixed": {
			"00": {lastSent: old, count: 2},
			"01": {lastSent: recent, count: 1},
		},
	}

	sm.pruneOrphanHandoffSent()

	if _, exists := sm.orphanHandoffSent["old-manifest"]; exists {
		t.Error("old-manifest should have been pruned entirely")
	}
	if _, exists := sm.orphanHandoffSent["recent-manifest"]; !exists {
		t.Error("recent-manifest should still exist")
	}
	if mixed, ok := sm.orphanHandoffSent["mixed"]; ok {
		if _, has00 := mixed["00"]; has00 {
			t.Error("mixed[00] should have been pruned (old)")
		}
		if _, has01 := mixed["01"]; !has01 {
			t.Error("mixed[01] should still exist (recent)")
		}
	} else {
		t.Error("mixed should still exist (has one recent entry)")
	}
}

func TestPruneOrphanHandoffSent_Empty(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)

	sm.pruneOrphanHandoffSent()

	if len(sm.orphanHandoffSent) != 0 {
		t.Error("should remain empty")
	}
}

func TestCleanupLegacyManifests_NoManifests(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)
	sm.currentShard = "0"

	sm.cleanupLegacyManifests()

	if len(storage.unpinnedKeys) != 0 {
		t.Error("no manifests means no unpins")
	}
}

func TestCleanupLegacyManifests_SkipsNonLegacy(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)
	sm.currentShard = "0"

	key := "bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storage.PinFile(key)
	storage.pinnedFiles[key] = true

	sm.cleanupLegacyManifests()

	if len(cluster.unpinned) != 0 {
		t.Error("non-legacy manifest should not be unpinned (GetBlock returns nil → not legacy)")
	}
}

func TestCleanupLegacyManifests_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)
	sm.currentShard = "0"

	key := "bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storage.PinFile(key)
	storage.pinnedFiles[key] = true

	sm.cleanupLegacyManifests()

	if len(cluster.unpinned) != 0 {
		t.Error("cancelled context should prevent cleanup")
	}
}

func TestReshardedFilesMarking(t *testing.T) {
	kf := common.NewKnownFiles()

	if kf.Has("test") {
		t.Error("should not have key before add")
	}
	kf.Add("test")
	if !kf.Has("test") {
		t.Error("should have key after add")
	}
	if kf.Size() != 1 {
		t.Errorf("expected size 1, got %d", kf.Size())
	}
}
