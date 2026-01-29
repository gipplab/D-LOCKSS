package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// FileProcessor handles file ingestion and processing.
type FileProcessor struct {
	ipfsClient ipfs.IPFSClient
	shardMgr   *ShardManager
	storageMgr *StorageManager
	privKey    crypto.PrivKey
}

// NewFileProcessor creates a new FileProcessor with dependencies.
func NewFileProcessor(
	client ipfs.IPFSClient, 
	sm *ShardManager, 
	stm *StorageManager,
	key crypto.PrivKey,
) *FileProcessor {
	return &FileProcessor{
		ipfsClient: client,
		shardMgr:   sm,
		storageMgr: stm,
		privKey:    key,
	}
}

// scanExistingFiles walks the data directory and processes any existing files.
// New ingests go through ResearchObject manifests in processNewFile below.
func (fp *FileProcessor) scanExistingFiles() {
	var fileCount int
	err := filepath.Walk(FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("[Warning] Error accessing %s: %v", path, err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		// File ingestion goes through the ResearchObject path in processNewFile.
		fp.processNewFile(path)
		fileCount++
		return nil
	})
	if err != nil {
		log.Printf("[Error] Error scanning existing files: %v", err)
		return
	}
	log.Printf("[System] Found %d existing files (recursive scan).", fileCount)
}

// fileEventInfo tracks file metadata to detect actual content changes.
type fileEventInfo struct {
	size    int64
	modTime time.Time
	lastSeen time.Time
}

// shouldProcessFileEvent dedupes noisy fsnotify sequences (Create/Write/Rename bursts)
// while allowing legitimate file modifications. Returns true if the file should be processed.
// It checks file size + modification time to distinguish duplicate events from real changes.
var fileEventDeduper = struct {
	mu   sync.Mutex
	info map[string]fileEventInfo
}{
	info: make(map[string]fileEventInfo),
}

func shouldProcessFileEvent(path string) bool {
	const window = 2 * time.Second
	now := time.Now()
	
	// Get current file metadata
	info, err := os.Stat(path)
	if err != nil {
		// File doesn't exist or can't be stat'd - process anyway (might be deletion/rename)
		return true
	}
	if info.IsDir() {
		return false // Directories are handled separately
	}
	
	currentSize := info.Size()
	currentModTime := info.ModTime()
	
	fileEventDeduper.mu.Lock()
	defer fileEventDeduper.mu.Unlock()
	
	// Check if we've seen this exact file content recently
	if last, ok := fileEventDeduper.info[path]; ok {
		// Same size + mtime within window = duplicate event, skip
		if last.size == currentSize && 
		   last.modTime.Equal(currentModTime) && 
		   now.Sub(last.lastSeen) < window {
			return false
		}
		// Different size or mtime = legitimate modification, process
	}
	
	// Record this file state
	fileEventDeduper.info[path] = fileEventInfo{
		size:     currentSize,
		modTime:   currentModTime,
		lastSeen:  now,
	}
	
	// Opportunistic cleanup to avoid unbounded growth
	cutoff := now.Add(-10 * window)
	for k, v := range fileEventDeduper.info {
		if v.lastSeen.Before(cutoff) {
			delete(fileEventDeduper.info, k)
		}
	}
	
	return true
}

// processNewFile imports a newly detected file into IPFS, builds a ResearchObject
// manifest, pins it, and announces it to the D-LOCKSS network.
func (fp *FileProcessor) processNewFile(path string) {
	// Validate path and check prerequisites
	if !validateFilePath(path) {
		return
	}

	if fp.ipfsClient == nil {
		logError("FileOps", "process file", path, fmt.Errorf("IPFS client not initialized"))
		return
	}

	// Import file and create manifest
	ctx, cancel := context.WithTimeout(context.Background(), FileImportTimeout)
	defer cancel()

	payloadCID, cleanupPayload, err := fp.importFileToIPFS(ctx, path)
	if err != nil {
		return
	}
	defer cleanupPayload()

	// Build and store ResearchObject
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, path, payloadCID, cleanupPayload)
	if err != nil {
		return
	}

	// Check BadBits and pin
	if !fp.checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, path, cleanupPayload) {
		return
	}

	// Track and announce
	fp.trackAndAnnounceFile(manifestCID, manifestCIDStr, payloadCID)
}

// validateFilePath validates that the file path is within the watch folder and returns true if valid.
func validateFilePath(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		logError("FileOps", "resolve absolute path", path, err)
		return false
	}

	absWatch, err := filepath.Abs(FileWatchFolder)
	if err != nil {
		logError("FileOps", "resolve watch folder path", FileWatchFolder, err)
		return false
	}

	// Prevent path traversal / prefix confusion
	rel, err := filepath.Rel(absWatch, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		log.Printf("[Security] Rejected path outside watch folder: %s", path)
		return false
	}

	// Ignore temp/partial files (common from downloads/edits)
	if strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, ".part") || strings.HasSuffix(path, ".crdownload") {
		return false
	}

	return true
}

// importFileToIPFS imports the file to IPFS and returns the payload CID.
func (fp *FileProcessor) importFileToIPFS(ctx context.Context, path string) (cid.Cid, func(), error) {
	payloadCID, err := fp.ipfsClient.ImportFile(ctx, path)
	if err != nil {
		logError("FileOps", "import file to IPFS", path, err)
		return cid.Cid{}, func() {}, err
	}

	cleanupPayload := func() {
		// Best effort cleanup if something fails later
		_ = fp.ipfsClient.UnpinRecursive(context.Background(), payloadCID)
	}

	return payloadCID, cleanupPayload, nil
}

// buildAndStoreManifest creates and stores the ResearchObject manifest.
func (fp *FileProcessor) buildAndStoreManifest(ctx context.Context, path string, payloadCID cid.Cid, cleanupPayload func()) (cid.Cid, string, error) {
	// Get file size
	info, err := os.Stat(path)
	if err != nil {
		logError("FileOps", "stat file", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Build ResearchObject
	// TODO: Get external metadata reference (DOI/URL) if available.
	// For now, we use the filename as a placeholder, or leave it empty.
	metaRef := "file://" + filepath.Base(path)
	ro := schema.NewResearchObject(
		metaRef,
		fp.shardMgr.h.ID(),
		payloadCID,
		uint64(info.Size()),
	)

	// Sign ResearchObject
	if err := fp.signResearchObject(ro); err != nil {
		logError("FileOps", "sign ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Marshal to CBOR
	roBytes, err := ro.MarshalCBOR()
	if err != nil {
		logError("FileOps", "marshal ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Store manifest in IPFS (dag-cbor)
	manifestCID, err := fp.ipfsClient.PutDagCBOR(ctx, roBytes)
	if err != nil {
		logError("FileOps", "put manifest to IPFS", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	return manifestCID, manifestCID.String(), nil
}

// signResearchObject signs the ResearchObject with the node's private key.
func (fp *FileProcessor) signResearchObject(ro *schema.ResearchObject) error {
	if fp.privKey == nil {
		log.Printf("[Sig] Warning: missing private key; ResearchObject manifest will not be signed")
		return nil
	}

	unsignedBytes, err := ro.MarshalCBORForSigning()
	if err != nil {
		return fmt.Errorf("failed to marshal for signing: %w", err)
	}

	sig, err := fp.privKey.Sign(unsignedBytes)
	if err != nil {
		return fmt.Errorf("failed to sign: %w", err)
	}

	ro.Signature = sig
	return nil
}

// checkBadBitsAndPin checks BadBits and pins the manifest if allowed.
func (fp *FileProcessor) checkBadBitsAndPin(ctx context.Context, manifestCID cid.Cid, manifestCIDStr, path string, cleanupPayload func()) bool {
	// Check BadBits
	if isCIDBlocked(manifestCIDStr, NodeCountry) {
		log.Printf("[FileOps] Refused to process file %s (blocked ManifestCID: %s)", path, manifestCIDStr)
		cleanupPayload()
		// Also ensure manifest is not pinned (PutDagCBOR might pin it?)
		// IPFS usually pins indirectly or blocks are just in repo.
		// Explicit unpin just in case.
		_ = fp.ipfsClient.UnpinRecursive(ctx, manifestCID)
		return false
	}

	// Pin recursively
	if err := fp.ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
		logError("FileOps", "pin ManifestCID recursively", manifestCIDStr, err)
		cleanupPayload()
		return false
	}

	return true
}

// trackAndAnnounceFile tracks the file and announces it to the appropriate shard.
func (fp *FileProcessor) trackAndAnnounceFile(manifestCID cid.Cid, manifestCIDStr string, payloadCID cid.Cid) {
	// Track in local state
	if !fp.storageMgr.pinFile(manifestCIDStr) {
		logWarning("FileOps", "Failed to track ManifestCID", manifestCIDStr)
		return
	}

	fp.storageMgr.addKnownFile(manifestCIDStr)

	// Determine responsibility and announce
	payloadCIDStr := payloadCID.String()
	if fp.shardMgr.AmIResponsibleFor(payloadCIDStr) {
		fp.announceResponsibleFile(manifestCID, manifestCIDStr, payloadCIDStr)
	} else {
		fp.announceCustodialFile(manifestCID, manifestCIDStr, payloadCIDStr)
	}
}

// announceResponsibleFile announces a file when this node is responsible for it.
func (fp *FileProcessor) announceResponsibleFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] I am responsible for PayloadCID %s (ManifestCID %s). Announcing to Shard.",
		truncateCID(payloadCIDStr, 16), truncateCID(manifestCIDStr, 16))

	// Get file size for hint (we'd need to pass this, but for now use 0)
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     fp.shardMgr.currentShard,
		HintSize:    0, // TODO: Pass actual size
	}

	if err := signProtocolMessage(&im); err != nil {
		logError("FileOps", "sign IngestMessage", manifestCIDStr, err)
	}

	b, err := im.MarshalCBOR()
	if err != nil {
		logError("FileOps", "marshal IngestMessage", manifestCIDStr, err)
		return
	}

	fp.shardMgr.PublishToShardCBOR(b, fp.shardMgr.currentShard)

	// Announce to DHT
	provideCtx, provideCancel := context.WithTimeout(context.Background(), DHTProvideTimeout)
	go func() {
		defer provideCancel()
		fp.storageMgr.provideFile(provideCtx, manifestCIDStr)
	}()
}

// announceCustodialFile announces a custodial file delegation using the "Tourist" pattern.
// It joins the target shard temporarily to announce the file.
func (fp *FileProcessor) announceCustodialFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] Custodial Mode: I am NOT responsible for PayloadCID %s (ManifestCID %s). Visiting target shard.",
		truncateCID(payloadCIDStr, 16), truncateCID(manifestCIDStr, 16))

	// Calculate target shard
	stableHex := keyToStableHex(payloadCIDStr)
	targetDepth := len(fp.shardMgr.currentShard)
	if targetDepth == 0 {
		targetDepth = 1
	}
	targetShard := getHexBinaryPrefix(stableHex, targetDepth)

	// Join target shard (Tourist Mode)
	fp.shardMgr.JoinShard(targetShard)

	// Announce IngestMessage directly to the target shard
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     targetShard,
		HintSize:    0,
	}

	if err := signProtocolMessage(&im); err != nil {
		logError("FileOps", "sign IngestMessage", manifestCIDStr, err)
	} else if b, err := im.MarshalCBOR(); err != nil {
		logError("FileOps", "marshal IngestMessage", manifestCIDStr, err)
	} else {
		fp.shardMgr.PublishToShardCBOR(b, targetShard)
		log.Printf("[Core] Published IngestMessage to target shard %s", targetShard)
	}
	
	// We DO NOT LeaveShard here immediately. We must remain in the shard to listen for
	// status updates or simply hold the reference until the file is safe.
	// Actually, we don't *need* to listen to the shard to check replication (we use DHT).
	// But keeping the subscription open allows receiving "UnreplicateRequest" or other signals?
	// The requirement is: "until replication is at target. Then drop from the pubsub channel".
	// The ReplicationManager will eventually verify replication is at target.
	// We need to link that event to LeaveShard.
	// For now, we rely on the fact that we incremented the refcount via JoinShard.
	// We need to ensure it gets decremented.
	
	// Register this file as a "Tourist File" in ReplicationManager?
	// Or we can just calculate the shard again when we unpin.
}

// watchFolder watches the data directory for new files.
func (fp *FileProcessor) watchFolder(ctx context.Context) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[Error] Failed to create file watcher: %v", err)
		return
	}
	defer watcher.Close()

	if err := os.MkdirAll(FileWatchFolder, 0755); err != nil {
		log.Printf("[Error] Failed to create data directory: %v", err)
		return
	}

	if err := watcher.Add(FileWatchFolder); err != nil {
		log.Printf("[Error] Failed to watch data directory: %v", err)
		return
	}

	log.Printf("[FileWatcher] Watching %s for new files...", FileWatchFolder)

	// Debounce timer (not strictly used with goroutine approach, but good practice concept)
	// const debounceDuration = 100 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Handle Create and Write events
			if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
				path := event.Name
				
				// Validate path before processing (security)
				if !validateFilePath(path) {
					continue
				}

				if shouldProcessFileEvent(path) {
					// Use a goroutine to not block the watcher loop
					// And verify file stability (done writing)
					go func(p string) {
						// Wait for file to stabilize
						time.Sleep(FileProcessingDelay)
						fp.processNewFile(p)
					}(path)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[Error] Watcher error: %v", err)
		}
	}
}
