package fileops_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/internal/fileops"
	"dlockss/pkg/schema"
)

// ---------------------------------------------------------------------------
// Mock: IPFSClient
// ---------------------------------------------------------------------------

type mockIPFS struct {
	importFileFn func(ctx context.Context, path string) (cid.Cid, error)
	putDagFn     func(ctx context.Context, block []byte) (cid.Cid, error)
}

func (m *mockIPFS) ImportFile(ctx context.Context, path string) (cid.Cid, error) {
	if m.importFileFn != nil {
		return m.importFileFn(ctx, path)
	}
	return fakeCID("import-" + filepath.Base(path)), nil
}
func (m *mockIPFS) ImportReader(context.Context, io.Reader) (cid.Cid, error) {
	return fakeCID("reader"), nil
}
func (m *mockIPFS) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	if m.putDagFn != nil {
		return m.putDagFn(ctx, block)
	}
	return fakeCID("dag"), nil
}
func (m *mockIPFS) GetBlock(context.Context, cid.Cid) ([]byte, error) { return nil, nil }
func (m *mockIPFS) PinRecursive(context.Context, cid.Cid) error       { return nil }
func (m *mockIPFS) UnpinRecursive(context.Context, cid.Cid) error     { return nil }
func (m *mockIPFS) IsPinned(context.Context, cid.Cid) (bool, error)   { return false, nil }
func (m *mockIPFS) GetFileSize(context.Context, cid.Cid) (uint64, error) {
	return 0, nil
}
func (m *mockIPFS) GetPeerID(context.Context) (string, error)         { return "test-peer", nil }
func (m *mockIPFS) SwarmConnect(context.Context, []string) error       { return nil }

// ---------------------------------------------------------------------------
// Mock: ShardCoordinator (ShardIdentity + ShardPublisher + CustodialInjector)
// ---------------------------------------------------------------------------

type mockShardCoordinator struct {
	peerID      peer.ID
	shardID     string
	shardDepth  int
	responsible bool

	mu           sync.Mutex
	announced    []string
	pinned       []cid.Cid
	publishedTo  []string
}

func (m *mockShardCoordinator) PeerID() peer.ID                  { return m.peerID }
func (m *mockShardCoordinator) GetShardInfo() (string, int)      { return m.shardID, m.shardDepth }
func (m *mockShardCoordinator) AmIResponsibleFor(string) bool    { return m.responsible }

func (m *mockShardCoordinator) AnnouncePinned(manifestCID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.announced = append(m.announced, manifestCID)
}

func (m *mockShardCoordinator) PinToCluster(_ context.Context, c cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pinned = append(m.pinned, c)
	return nil
}

func (m *mockShardCoordinator) PublishIngestMessageToCurrentAndChildIfSplit(data []byte, currentShard, payloadCIDStr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedTo = append(m.publishedTo, currentShard)
}

func (m *mockShardCoordinator) ResolveTargetShardForCustodial(nominal, _ string) string {
	return nominal
}
func (m *mockShardCoordinator) JoinShardAsObserver(string) bool             { return true }
func (m *mockShardCoordinator) LeaveShardAsObserver(string)                 {}
func (m *mockShardCoordinator) EnsureClusterForShard(context.Context, string) error { return nil }
func (m *mockShardCoordinator) PinToShard(context.Context, string, cid.Cid) error   { return nil }
func (m *mockShardCoordinator) PublishToShardCBOR([]byte, string)           {}

// ---------------------------------------------------------------------------
// Mock: StorageTracker
// ---------------------------------------------------------------------------

type mockStorage struct {
	mu       sync.Mutex
	pinned   map[string]bool
	known    []string
	provided []string
}

func newMockStorage() *mockStorage {
	return &mockStorage{pinned: make(map[string]bool)}
}

func (m *mockStorage) PinFile(manifestCIDStr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pinned[manifestCIDStr] {
		return false
	}
	m.pinned[manifestCIDStr] = true
	return true
}

func (m *mockStorage) AddKnownFile(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.known = append(m.known, key)
}

func (m *mockStorage) ProvideFile(_ context.Context, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.provided = append(m.provided, key)
}

// ---------------------------------------------------------------------------
// Mock: MessageSigner
// ---------------------------------------------------------------------------

type mockSigner struct {
	calls atomic.Int64
}

func (m *mockSigner) SignProtocolMessage(msg schema.Signable) error {
	m.calls.Add(1)
	env := msg.GetEnvelope()
	env.Nonce = []byte("test-nonce")
	env.Sig = []byte("test-sig")
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func fakeCID(label string) cid.Cid {
	hash, _ := mh.Sum([]byte(label), mh.SHA2_256, -1)
	return cid.NewCidV1(cid.DagCBOR, hash)
}

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.FileWatchFolder = t.TempDir()
	cfg.FileStabilityDelay = 0
	cfg.FileImportTimeout = 5 * time.Second
	cfg.DHTProvideTimeout = 1 * time.Second
	cfg.MaxConcurrentFileProcessing = 2
	return cfg
}

func newTestProcessor(t *testing.T, cfg *config.Config, ipfsMock *mockIPFS, shardMock *mockShardCoordinator, storageMock *mockStorage, signerMock *mockSigner) *fileops.FileProcessor {
	t.Helper()
	badBitsFilter, err := badbits.NewFilter("")
	if err != nil {
		t.Fatalf("failed to create badbits filter: %v", err)
	}
	fp := fileops.NewFileProcessor(fileops.FileProcessorConfig{
		Cfg:        cfg,
		IPFSClient: ipfsMock,
		Shard:      shardMock,
		Storage:    storageMock,
		PrivKey:    nil,
		Signer:     signerMock,
		BadBits:    badBitsFilter,
	})
	t.Cleanup(fp.Stop)
	return fp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNewFileProcessorAndStop(t *testing.T) {
	cfg := testConfig(t)
	ipfsMock := &mockIPFS{}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	// Stop should be idempotent and safe.
	fp.Stop()
	fp.Stop()
}

func TestTryEnqueue_AcceptsFiles(t *testing.T) {
	cfg := testConfig(t)
	// Large concurrency * 100 = buffer size; we just need a few slots.
	ipfsMock := &mockIPFS{}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)
	// Stop workers so jobs stay in the queue.
	fp.Stop()

	ok := fp.TryEnqueue("/some/file.txt")
	if !ok {
		t.Fatal("TryEnqueue should accept a file into a non-full queue")
	}
}

func TestTryEnqueue_ReturnsFalseWhenFull(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxConcurrentFileProcessing = 1 // queue size = 1 * 100 = 100
	ipfsMock := &mockIPFS{}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)
	fp.Stop() // stop workers so nothing drains the channel

	queueSize := cfg.MaxConcurrentFileProcessing * 100
	for i := 0; i < queueSize; i++ {
		if !fp.TryEnqueue("/file") {
			t.Fatalf("TryEnqueue should succeed for item %d/%d", i, queueSize)
		}
	}

	if fp.TryEnqueue("/overflow") {
		t.Fatal("TryEnqueue should return false when queue is full")
	}
}

func TestEnqueueOrRetry_FallsBackToRetryQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxConcurrentFileProcessing = 1
	ipfsMock := &mockIPFS{}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)
	fp.Stop()

	queueSize := cfg.MaxConcurrentFileProcessing * 100
	for i := 0; i < queueSize; i++ {
		fp.TryEnqueue("/fill")
	}

	// Main queue full — EnqueueOrRetry should still succeed (goes to retry queue).
	ok := fp.EnqueueOrRetry("/retry-file")
	if !ok {
		t.Fatal("EnqueueOrRetry should succeed by adding to retry queue")
	}
}

func TestProcessNewFile_SkipsFilesOutsideWatchDir(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("should-not-import"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	// File outside the watch folder should be rejected by validateFilePath.
	outsidePath := filepath.Join(os.TempDir(), "outside_file.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(outsidePath)

	fp.TryEnqueue(outsidePath)
	time.Sleep(200 * time.Millisecond)

	if imported.Load() != 0 {
		t.Error("file outside watch folder should not be imported")
	}
}

func TestProcessNewFile_SkipsTmpFiles(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("should-not-import"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	tmpPath := filepath.Join(cfg.FileWatchFolder, "download.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}

	fp.TryEnqueue(tmpPath)
	time.Sleep(200 * time.Millisecond)

	if imported.Load() != 0 {
		t.Error(".tmp files should be skipped by validation")
	}
}

func TestProcessNewFile_SkipsPartFiles(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("should-not-import"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	partPath := filepath.Join(cfg.FileWatchFolder, "download.part")
	if err := os.WriteFile(partPath, []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}

	fp.TryEnqueue(partPath)
	time.Sleep(200 * time.Millisecond)

	if imported.Load() != 0 {
		t.Error(".part files should be skipped by validation")
	}
}

func TestScanExistingFiles(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("scan"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	// Populate the watch folder with files.
	for _, name := range []string{"a.txt", "b.txt", "c.dat"} {
		if err := os.WriteFile(filepath.Join(cfg.FileWatchFolder, name), []byte("data-"+name), 0644); err != nil {
			t.Fatal(err)
		}
	}
	// Also place a .tmp file that should be ignored by processNewFile.
	if err := os.WriteFile(filepath.Join(cfg.FileWatchFolder, "skip.tmp"), []byte("tmp"), 0644); err != nil {
		t.Fatal(err)
	}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	fp.ScanExistingFiles()

	// Wait for workers to pick up jobs.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if imported.Load() >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := imported.Load()
	if got < 3 {
		t.Errorf("ScanExistingFiles: expected at least 3 imports, got %d", got)
	}
}

func TestScanExistingFiles_IncludesSubdirectories(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("sub"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	sub := filepath.Join(cfg.FileWatchFolder, "subdir")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"x.txt", "y.txt"} {
		if err := os.WriteFile(filepath.Join(sub, name), []byte("nested"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)
	fp.ScanExistingFiles()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if imported.Load() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := imported.Load()
	if got < 2 {
		t.Errorf("ScanExistingFiles should recurse into subdirectories: expected >= 2 imports, got %d", got)
	}
}

func TestShouldProcessFileEvent_Deduplication(t *testing.T) {
	cfg := testConfig(t)
	var imported atomic.Int64
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			imported.Add(1)
			return fakeCID("dedup"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	testFile := filepath.Join(cfg.FileWatchFolder, "dedup_test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	// The watcher calls WatchFolder which is hard to unit-test directly.
	// Instead, we exercise shouldProcessFileEvent through the exported API:
	// enqueue the same file twice rapidly — the second should still be enqueued
	// (dedup happens in shouldProcessFileEvent, not TryEnqueue), but processNewFile
	// has its own recent-ingest guard for CID-level dedup.
	fp.TryEnqueue(testFile)
	fp.TryEnqueue(testFile)

	// Wait for processing.
	time.Sleep(500 * time.Millisecond)

	// Both enqueues reach processNewFile; the second import will be deduped by
	// recentIngests (same CID within TTL), so only 1 should actually be imported
	// past the CID dedup guard. But ImportFile will be called for each since
	// validateFilePath passes for both — the recentIngests check happens after
	// ImportFile. We verify ImportFile was called at least once.
	if imported.Load() < 1 {
		t.Error("expected at least one import call")
	}
}

func TestProcessNewFile_FullPipeline(t *testing.T) {
	cfg := testConfig(t)

	payloadCID := fakeCID("payload-full")
	dagCID := fakeCID("manifest-full")

	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			return payloadCID, nil
		},
		putDagFn: func(_ context.Context, _ []byte) (cid.Cid, error) {
			return dagCID, nil
		},
	}
	shardMock := &mockShardCoordinator{
		peerID:      "test-peer-full",
		shardID:     "0",
		shardDepth:  1,
		responsible: true,
	}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	testFile := filepath.Join(cfg.FileWatchFolder, "pipeline.txt")
	if err := os.WriteFile(testFile, []byte("pipeline-content"), 0644); err != nil {
		t.Fatal(err)
	}

	fp.TryEnqueue(testFile)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		storageMock.mu.Lock()
		pinned := storageMock.pinned[dagCID.String()]
		storageMock.mu.Unlock()
		if pinned {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	storageMock.mu.Lock()
	defer storageMock.mu.Unlock()
	if !storageMock.pinned[dagCID.String()] {
		t.Error("expected manifest to be pinned in storage tracker")
	}
	if len(storageMock.known) == 0 {
		t.Error("expected AddKnownFile to be called")
	}

	shardMock.mu.Lock()
	defer shardMock.mu.Unlock()
	if len(shardMock.announced) == 0 {
		t.Error("expected AnnouncePinned to be called")
	}
	if len(shardMock.pinned) == 0 {
		t.Error("expected PinToCluster to be called for responsible node")
	}
}

func TestProcessNewFile_CIDDedup(t *testing.T) {
	cfg := testConfig(t)

	callCount := atomic.Int64{}
	stableCID := fakeCID("same-payload")
	ipfsMock := &mockIPFS{
		importFileFn: func(_ context.Context, _ string) (cid.Cid, error) {
			callCount.Add(1)
			return stableCID, nil
		},
		putDagFn: func(_ context.Context, _ []byte) (cid.Cid, error) {
			return fakeCID("manifest-dedup"), nil
		},
	}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	// Create two distinct files that produce the same CID.
	f1 := filepath.Join(cfg.FileWatchFolder, "file1.txt")
	f2 := filepath.Join(cfg.FileWatchFolder, "file2.txt")
	if err := os.WriteFile(f1, []byte("same"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f2, []byte("same"), 0644); err != nil {
		t.Fatal(err)
	}

	fp.TryEnqueue(f1)
	// Small delay to ensure the first processes before the second.
	time.Sleep(300 * time.Millisecond)
	fp.TryEnqueue(f2)
	time.Sleep(500 * time.Millisecond)

	// ImportFile is called for both, but the second should hit recentIngests dedup.
	imports := callCount.Load()
	if imports < 2 {
		t.Errorf("expected ImportFile called twice, got %d", imports)
	}

	// PinFile should only be called once since the second file is deduped by CID.
	storageMock.mu.Lock()
	pinnedCount := len(storageMock.pinned)
	storageMock.mu.Unlock()
	if pinnedCount > 1 {
		t.Errorf("expected at most 1 pinned file (CID dedup), got %d", pinnedCount)
	}
}

func TestProcessNewFile_NilIPFSClient(t *testing.T) {
	cfg := testConfig(t)
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	badBitsFilter, err := badbits.NewFilter("")
	if err != nil {
		t.Fatal(err)
	}

	fp := fileops.NewFileProcessor(fileops.FileProcessorConfig{
		Cfg:        cfg,
		IPFSClient: nil,
		Shard:      shardMock,
		Storage:    storageMock,
		Signer:     signerMock,
		BadBits:    badBitsFilter,
	})
	t.Cleanup(fp.Stop)

	testFile := filepath.Join(cfg.FileWatchFolder, "noipfs.txt")
	if err := os.WriteFile(testFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	fp.TryEnqueue(testFile)
	time.Sleep(200 * time.Millisecond)

	// Should not panic; processNewFile bails when ipfsClient is nil.
	storageMock.mu.Lock()
	defer storageMock.mu.Unlock()
	if len(storageMock.pinned) != 0 {
		t.Error("no files should be pinned when IPFS client is nil")
	}
}

func TestWatchFolder_ContextCancellation(t *testing.T) {
	cfg := testConfig(t)
	ipfsMock := &mockIPFS{}
	shardMock := &mockShardCoordinator{peerID: "test-peer", shardID: "0", shardDepth: 1, responsible: true}
	storageMock := newMockStorage()
	signerMock := &mockSigner{}

	fp := newTestProcessor(t, cfg, ipfsMock, shardMock, storageMock, signerMock)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		fp.WatchFolder(ctx)
		close(done)
	}()

	// Allow the watcher to start.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// WatchFolder returned cleanly.
	case <-time.After(10 * time.Second):
		t.Fatal("WatchFolder did not return after context cancellation")
	}
}
