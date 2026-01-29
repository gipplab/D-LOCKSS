package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dlockss/pkg/ipfs"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func TestProcessNewFilePathContainment(t *testing.T) {
	watch := filepath.Join(string(filepath.Separator), "tmp", "data")

	inside := filepath.Join(watch, "subdir", "file.pdf")
	if rel, err := filepath.Rel(watch, inside); err != nil || rel == ".." || (len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)) {
		t.Fatalf("expected inside path to be within watch dir: rel=%q err=%v", rel, err)
	}

	// Prefix confusion: /tmp/dataX/... must be rejected even though it shares a string prefix.
	outsidePrefix := filepath.Join(string(filepath.Separator), "tmp", "dataX", "file.pdf")
	rel, err := filepath.Rel(watch, outsidePrefix)
	if err == nil && rel != ".." && !(len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)) {
		t.Fatalf("expected outside prefix path to be outside watch dir: rel=%q", rel)
	}
}

func TestShouldProcessFileEvent(t *testing.T) {
	// Create a temporary file for testing
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Create initial file
	if err := os.WriteFile(testFile, []byte("initial content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// First event should be processed
	if !shouldProcessFileEvent(testFile) {
		t.Error("first event should be processed")
	}

	// Immediate duplicate event (same content) should be skipped
	if shouldProcessFileEvent(testFile) {
		t.Error("duplicate event with same content should be skipped")
	}

	// Wait a moment, then modify the file (different size)
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("modified content - longer"), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Modified file should be processed (different size)
	if !shouldProcessFileEvent(testFile) {
		t.Error("modified file (different size) should be processed")
	}

	// Another modification (same size, different content - detected via mtime change)
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(testFile, []byte("different content same len"), 0644); err != nil {
		t.Fatalf("failed to modify test file: %v", err)
	}

	// Modified file should be processed (different mtime)
	if !shouldProcessFileEvent(testFile) {
		t.Error("modified file (different mtime) should be processed")
	}

	// Duplicate event for this version should be skipped
	if shouldProcessFileEvent(testFile) {
		t.Error("duplicate event for same version should be skipped")
	}
}

func TestProcessNewFile(t *testing.T) {
	// Setup mocks
	mockIPFS := ipfs.NewMockClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mock ShardManager
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create mock host: %v", err)
	}
	defer h.Close()

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatalf("Failed to create pubsub: %v", err)
	}

	stm := NewStorageManager(nil) // No DHT
	sm := NewShardManager(ctx, h, ps, mockIPFS, stm, "1")
	// Start message loop to avoid channel blocking? 
	// ShardManager doesn't block on publish unless channel is full?
	// NewGossipSub starts it.
	
	// Create FileProcessor
	fp := NewFileProcessor(mockIPFS, sm, stm, nil) // No private key for now

	// Setup Watch Dir override
	tmpDir := t.TempDir()
	originalWatchFolder := FileWatchFolder
	FileWatchFolder = tmpDir // Set global config
	defer func() { FileWatchFolder = originalWatchFolder }()

	// Create a test file
	testFilePath := filepath.Join(tmpDir, "test.pdf")
	if err := os.WriteFile(testFilePath, []byte("fake pdf content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Run processNewFile
	fp.processNewFile(testFilePath)

	// Verify IPFS interactions
	// 1. Check if Manifest is pinned
	// Manifest CID from mock: QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N
	expectedManifest, _ := cid.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	pinned, _ := mockIPFS.IsPinned(ctx, expectedManifest)
	
	if !pinned {
		t.Errorf("Expected manifest CID to be pinned: %s", expectedManifest.String())
	}
	
	// 2. Check if Payload was unpinned (cleanup)
	// Payload CID from mock: bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
	expectedPayload, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	payloadPinned, _ := mockIPFS.IsPinned(ctx, expectedPayload)
	
	if payloadPinned {
		t.Error("Expected payload CID to be unpinned by cleanup (since mock doesn't link DAG)")
	}
}

func TestProcessNewFile_Errors(t *testing.T) {
	// Constants from MockClient
	// Payload: bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
	// Manifest: QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N
	mockPayloadCID, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	mockManifestCID, _ := cid.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")

	tests := []struct {
		name           string
		failImport     bool
		failPin        bool
		blockManifest  bool
		expectManifest bool // Should PutDagCBOR be called?
		expectPin      bool // Should PinRecursive be called successfully?
		expectCleanup  bool // Should payload be unpinned?
	}{
		{
			name:           "Import Failure",
			failImport:     true,
			expectManifest: false,
			expectPin:      false,
			expectCleanup:  false, // Import fails, so nothing to cleanup
		},
		{
			name:           "Pin Failure (Cleanup)",
			failPin:        true,
			expectManifest: true,
			expectPin:      false, // It's called but fails
			expectCleanup:  true,  // Should trigger cleanup
		},
		{
			name:           "BadBits Blocked",
			blockManifest:  true,
			expectManifest: true,
			expectPin:      false, // Blocked before pin
			expectCleanup:  true,  // Should trigger cleanup
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup Mocks
			mockIPFS := ipfs.NewMockClient()
			mockIPFS.FailImport = tt.failImport
			mockIPFS.FailPin = tt.failPin

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			defer h.Close()
			ps, _ := pubsub.NewGossipSub(ctx, h)
			stm := NewStorageManager(nil)
			sm := NewShardManager(ctx, h, ps, mockIPFS, stm, "1")
			fp := NewFileProcessor(mockIPFS, sm, stm, nil)

			// Setup BadBits if needed
			if tt.blockManifest {
				badBitsFile := filepath.Join(t.TempDir(), "badbits.csv")
				content := "CID,Country\n" + mockManifestCID.String() + ",US\n"
				if err := os.WriteFile(badBitsFile, []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
				loadBadBits(badBitsFile)
				NodeCountry = "US" // Set global config
			} else {
				// Reset BadBits
				badBits.mu.Lock()
				badBits.cids = make(map[string]map[string]bool)
				badBits.loaded = false
				badBits.mu.Unlock()
			}

			// Create dummy file
			tmpDir := t.TempDir()
			// Set global watch folder temporarily
			originalWatchFolder := FileWatchFolder
			FileWatchFolder = tmpDir
			defer func() { FileWatchFolder = originalWatchFolder }()
			
			testPath := filepath.Join(tmpDir, "test.pdf")
			os.WriteFile(testPath, []byte("data"), 0644)

			// Run
			fp.processNewFile(testPath)

			// Checks
			
			// 1. Manifest Creation
			manifestCreated := len(mockIPFS.Blocks) > 0
			if manifestCreated != tt.expectManifest {
				t.Errorf("Manifest creation: expected %v, got %v", tt.expectManifest, manifestCreated)
			}

			// 2. Pinning
			isPinned, _ := mockIPFS.IsPinned(ctx, mockManifestCID)
			if isPinned != tt.expectPin {
				// Note: If FailPin is true, IsPinned returns false (correct)
				t.Errorf("Manifest pinned: expected %v, got %v", tt.expectPin, isPinned)
			}

			// 3. Cleanup (Payload Unpinning)
			// In all these error cases (and success), payload should be unpinned by cleanupPayload
			// because MockClient doesn't track DAG links. 
			// If payload is still pinned, it means cleanup didn't run.
			payloadPinned, _ := mockIPFS.IsPinned(ctx, mockPayloadCID)
			if payloadPinned {
				t.Error("Payload should be unpinned (cleanup did not run?)")
			}
		})
	}
}
