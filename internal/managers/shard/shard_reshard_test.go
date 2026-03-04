package shard

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/testutil"
)

// --- test doubles ---

type recordingStorage struct {
	mu           sync.Mutex
	knownFiles   map[string]bool
	pinnedFiles  map[string]bool
	pinTimes     map[string]time.Time
	unpinnedKeys []string
}

func newRecordingStorage() *recordingStorage {
	return &recordingStorage{
		knownFiles:  make(map[string]bool),
		pinnedFiles: make(map[string]bool),
		pinTimes:    make(map[string]time.Time),
	}
}

func (s *recordingStorage) CanAcceptCustodialFile() bool  { return true }
func (s *recordingStorage) GetNextFileToAnnounce() string { return "" }
func (s *recordingStorage) GetPinnedCount() int           { return len(s.pinnedFiles) }
func (s *recordingStorage) GetPinnedManifests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.pinnedFiles))
	for k := range s.pinnedFiles {
		out = append(out, k)
	}
	return out
}
func (s *recordingStorage) IsPinned(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pinnedFiles[key]
}
func (s *recordingStorage) GetAllKnownFiles() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]bool, len(s.knownFiles))
	for k, v := range s.knownFiles {
		cp[k] = v
	}
	return cp
}
func (s *recordingStorage) GetPinTime(key string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pinTimes[key]
}
func (s *recordingStorage) PinFile(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinnedFiles[key] = true
	s.pinTimes[key] = time.Now()
	return true
}
func (s *recordingStorage) UnpinFile(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pinnedFiles, key)
	s.unpinnedKeys = append(s.unpinnedKeys, key)
}
func (s *recordingStorage) AddKnownFile(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.knownFiles[key] = true
}
func (s *recordingStorage) ProvideFile(_ context.Context, _ string) {}

type recordingCluster struct {
	mu       sync.Mutex
	unpinned []string
	pinned   []string
	joinedSh []string
	synced   []string
}

func (c *recordingCluster) JoinShard(_ context.Context, shardID string, _ []multiaddr.Multiaddr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.joinedSh = append(c.joinedSh, shardID)
	return nil
}
func (c *recordingCluster) LeaveShard(_ string) error { return nil }
func (c *recordingCluster) Pin(_ context.Context, shardID string, cc cid.Cid, _, _ int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinned = append(c.pinned, cc.String())
	return nil
}
func (c *recordingCluster) PinIfAbsent(_ context.Context, _ string, cc cid.Cid, _, _ int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinned = append(c.pinned, cc.String())
	return nil
}
func (c *recordingCluster) Unpin(_ context.Context, _ string, cc cid.Cid) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unpinned = append(c.unpinned, cc.String())
	return nil
}
func (c *recordingCluster) GetAllocations(_ context.Context, _ string, _ cid.Cid) ([]peer.ID, error) {
	return nil, nil
}
func (c *recordingCluster) GetPeerCount(_ context.Context, _ string) (int, error) { return 0, nil }
func (c *recordingCluster) MigratePins(_ context.Context, _, _ string) error      { return nil }
func (c *recordingCluster) TriggerSync(shardID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.synced = append(c.synced, shardID)
}

// buildTestSM creates a minimal ShardManager for testing without requiring pubsub.
func buildTestSM(ctx context.Context, storage *recordingStorage, cluster *recordingCluster) *ShardManager {
	cfg := config.DefaultConfig()
	cfg.ReshardHandoffDelay = 0
	return &ShardManager{
		ctx:                        ctx,
		cfg:                        cfg,
		ipfsClient:                 &testutil.MockIPFSClient{},
		storageMgr:                 storage,
		clusterMgr:                 cluster,
		reshardedFiles:             common.NewKnownFiles(),
		shardSubs:                  make(map[string]*shardSubscription),
		probeTopicCache:            make(map[string]*pubsub.Topic),
		observerOnlyShards:         make(map[string]struct{}),
		orphanHandoffSent:          make(map[string]map[string]*orphanHandoffInfo),
		replicationRequestLastSent: make(map[string]time.Time),
		autoReplicationSem:         make(chan struct{}, 1),
	}
}

// --- RunReshardPass tests ---

func TestRunReshardPass_NoFiles(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)
	sm.currentShard = "0"

	sm.RunReshardPass("", "0")

	if len(cluster.unpinned) != 0 {
		t.Errorf("expected no unpins, got %d", len(cluster.unpinned))
	}
}

func TestRunReshardPass_FileStaysOnSameShard(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)

	key := "bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storage.AddKnownFile(key)
	storage.PinFile(key)

	stableHex := common.KeyToStableHex(key)
	oldPrefix, _ := common.GetHexBinaryPrefix(stableHex, 1)
	newPrefix, _ := common.GetHexBinaryPrefix(stableHex, 2)

	sm.currentShard = newPrefix
	sm.RunReshardPass(oldPrefix, newPrefix)

	if len(cluster.unpinned) != 0 {
		t.Errorf("file should not be unpinned when it stays on the same shard")
	}
	if !sm.reshardedFiles.Has(key) {
		t.Error("file should be marked as resharded")
	}
}

func TestRunReshardPass_FileUnpinnedWhenLeavingShard(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)
	sm.signer = nil // no replication request will be sent

	// Generate two CID-like keys. We need keys where one maps to "0" and
	// the other maps to "1" at depth 1, so that a split from "" -> "0"
	// causes one to leave.
	keys := generateKeysForBothChildShards(t)

	for _, key := range keys {
		storage.AddKnownFile(key)
		storage.PinFile(key)
	}

	sm.currentShard = "0"
	sm.RunReshardPass("", "0")

	if len(cluster.unpinned) == 0 {
		t.Fatal("expected at least one file to be unpinned during reshard")
	}

	for _, unpinnedCID := range cluster.unpinned {
		found := false
		for _, k := range keys {
			if k == unpinnedCID {
				found = true
				break
			}
		}
		if found {
			continue
		}
		stableHex := common.KeyToStableHex(unpinnedCID)
		prefix, _ := common.GetHexBinaryPrefix(stableHex, 1)
		if prefix == "0" {
			t.Errorf("unpinned CID %s maps to shard 0, should not be unpinned", unpinnedCID)
		}
	}
}

func TestRunReshardPass_AlreadyReshardedSkipped(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)

	key := "bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storage.AddKnownFile(key)
	storage.PinFile(key)
	sm.reshardedFiles.Add(key)

	sm.currentShard = "0"
	sm.RunReshardPass("", "0")

	if len(cluster.unpinned) != 0 {
		t.Error("already-resharded file should be skipped")
	}
}

func TestRunReshardPass_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	storage := newRecordingStorage()
	cluster := &recordingCluster{}
	sm := buildTestSM(ctx, storage, cluster)

	key := "bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storage.AddKnownFile(key)
	storage.PinFile(key)

	sm.currentShard = "0"
	sm.RunReshardPass("", "0")

	if sm.reshardedFiles.Has(key) {
		t.Error("cancelled context should prevent processing")
	}
}

// generateKeysForBothChildShards returns two CID strings that map to different
// child shards ("0" and "1") at depth 1.
func generateKeysForBothChildShards(t *testing.T) []string {
	t.Helper()
	candidates := []string{
		"bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bafkreibbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"bafkreiccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"bafkreiddddddddddddddddddddddddddddddddddddddddddddddddddd",
		"bafkreieeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"bafkreifffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"bafkreiggggggggggggggggggggggggggggggggggggggggggggggggggggg",
		"bafkreihhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
	}
	var zero, one string
	for _, k := range candidates {
		stableHex := common.KeyToStableHex(k)
		prefix, err := common.GetHexBinaryPrefix(stableHex, 1)
		if err != nil {
			continue
		}
		if prefix == "0" && zero == "" {
			zero = k
		} else if prefix == "1" && one == "" {
			one = k
		}
		if zero != "" && one != "" {
			break
		}
	}
	if zero == "" || one == "" {
		t.Skip("could not find candidate keys for both child shards")
	}
	return []string{zero, one}
}
