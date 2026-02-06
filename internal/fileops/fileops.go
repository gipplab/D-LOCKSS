package fileops

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

	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/shard"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// FileProcessor handles file ingestion and processing.
type FileProcessor struct {
	ipfsClient ipfs.IPFSClient
	shardMgr   *shard.ShardManager
	storageMgr *storage.StorageManager
	privKey    crypto.PrivKey
	semaphore  chan struct{}   // Semaphore to limit concurrent file processing
	jobQueue   chan string     // Queue for file paths to process
	signer     *signing.Signer // Add signer for manual signing if needed, or use FileProcessor methods
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewFileProcessor creates a new FileProcessor with dependencies.
// signer should be the same Signer used by the shard manager so protocol messages use consistent nonce bounds and replay store.
func NewFileProcessor(
	client ipfs.IPFSClient,
	sm *shard.ShardManager,
	stm *storage.StorageManager,
	key crypto.PrivKey,
	signer *signing.Signer,
) *FileProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &FileProcessor{
		ipfsClient: client,
		shardMgr:   sm,
		storageMgr: stm,
		privKey:    key,
		signer:     signer,
		semaphore:  make(chan struct{}, config.MaxConcurrentFileProcessing),
		jobQueue:   make(chan string, config.MaxConcurrentFileProcessing*100), // Buffer size
		ctx:        ctx,
		cancel:     cancel,
	}
	fp.startWorkers()
	return fp
}

func (fp *FileProcessor) startWorkers() {
	for i := 0; i < config.MaxConcurrentFileProcessing; i++ {
		go fp.workerLoop()
	}
}

func (fp *FileProcessor) workerLoop() {
	for {
		select {
		case <-fp.ctx.Done():
			return
		case path := <-fp.jobQueue:
			fp.processNewFile(path)
		}
	}
}

// Stop stops the file processor workers.
func (fp *FileProcessor) Stop() {
	fp.cancel()
}

// ScanExistingFiles walks the data directory and processes any existing files.
// New ingests go through ResearchObject manifests in processNewFile below.
func (fp *FileProcessor) ScanExistingFiles() {
	var fileCount int
	err := filepath.Walk(config.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("[Warning] Error accessing %s: %v", path, err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		// File ingestion goes through the ResearchObject path in processNewFile.
		// Use TryEnqueue to avoid blocking scan if queue is full
		fp.TryEnqueue(path)
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
	size     int64
	modTime  time.Time
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
		modTime:  currentModTime,
		lastSeen: now,
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

// TryEnqueue attempts to add a file to the processing queue.
// Returns false if the queue is full (backpressure).
func (fp *FileProcessor) TryEnqueue(path string) bool {
	select {
	case fp.jobQueue <- path:
		return true
	default:
		log.Printf("[FileOps] Queue full, dropping file: %s", path)
		return false
	}
}

// processNewFile imports a newly detected file into IPFS, builds a ResearchObject
// manifest, pins it, and announces it to the D-LOCKSS network.
func (fp *FileProcessor) processNewFile(path string) {
	// Acquire semaphore to limit concurrent processing
	// Note: We already have worker pool limit, but semaphore is double safety or for other uses.
	// Actually, with worker pool, semaphore inside workerLoop is redundant if pool size == semaphore size.
	// But let's keep it if we want to limit specifically the heavy lifting part.
	fp.semaphore <- struct{}{}
	defer func() { <-fp.semaphore }()

	log.Printf("[FileOps] Starting processing: %s", path)

	// Validate path and check prerequisites
	if !validateFilePath(path) {
		log.Printf("[FileOps] File validation failed: %s", path)
		return
	}

	if fp.ipfsClient == nil {
		common.LogError("FileOps", "process file", path, fmt.Errorf("IPFS client not initialized"))
		return
	}

	// Import file and create manifest
	ctx, cancel := context.WithTimeout(context.Background(), config.FileImportTimeout)
	defer cancel()

	log.Printf("[FileOps] Importing file to IPFS: %s", path)
	payloadCID, cleanupPayload, err := fp.importFileToIPFS(ctx, path)
	if err != nil {
		log.Printf("[FileOps] Failed to import file: %s, error: %v", path, err)
		return
	}
	// Do NOT defer cleanupPayload() here unconditionally.
	// We only want to cleanup if subsequent steps fail.
	// If we succeed, we keep the payload pinned.

	log.Printf("[FileOps] File imported, PayloadCID: %s", payloadCID.String())

	// Build and store ResearchObject
	log.Printf("[FileOps] Building manifest for: %s", path)
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, path, payloadCID, cleanupPayload)
	if err != nil {
		log.Printf("[FileOps] Failed to build manifest: %s, error: %v", path, err)
		cleanupPayload() // Cleanup on failure
		return
	}
	log.Printf("[FileOps] Manifest created, ManifestCID: %s", manifestCIDStr)

	// Check BadBits and pin
	log.Printf("[FileOps] Checking BadBits and pinning: %s", path)
	if !fp.checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, path, cleanupPayload) {
		log.Printf("[FileOps] BadBits check failed or pinning failed: %s", path)
		// checkBadBitsAndPin calls cleanupPayload internally on failure
		return
	}
	log.Printf("[FileOps] File pinned successfully: %s", path)

	// Track and announce
	log.Printf("[FileOps] Tracking and announcing: %s", path)
	fp.trackAndAnnounceFile(manifestCID, manifestCIDStr, payloadCID)
	log.Printf("[FileOps] File processing completed: %s", path)
}

// validateFilePath validates that the file path is within the watch folder and returns true if valid.
func validateFilePath(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		common.LogError("FileOps", "resolve absolute path", path, err)
		return false
	}

	absWatch, err := filepath.Abs(config.FileWatchFolder)
	if err != nil {
		common.LogError("FileOps", "resolve watch folder path", config.FileWatchFolder, err)
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
		common.LogError("FileOps", "import file to IPFS", path, err)
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
		common.LogError("FileOps", "stat file", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Build ResearchObject
	// TODO: Get external metadata reference (DOI/URL) if available.
	// For now, we use the filename as a placeholder, or leave it empty.
	metaRef := "file://" + filepath.Base(path)
	ro := schema.NewResearchObject(
		metaRef,
		fp.shardMgr.GetHost().ID(), // Need GetHost in ShardManager or pass host
		payloadCID,
		uint64(info.Size()),
	)

	// Sign ResearchObject
	if err := fp.signResearchObject(ro); err != nil {
		common.LogError("FileOps", "sign ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Marshal to CBOR
	roBytes, err := ro.MarshalCBOR()
	if err != nil {
		common.LogError("FileOps", "marshal ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Store manifest in IPFS (dag-cbor)
	manifestCID, err := fp.ipfsClient.PutDagCBOR(ctx, roBytes)
	if err != nil {
		common.LogError("FileOps", "put manifest to IPFS", path, err)
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
	if badbits.IsCIDBlocked(manifestCIDStr) {
		log.Printf("[FileOps] Refused to process file %s (blocked ManifestCID: %s)", path, manifestCIDStr)
		cleanupPayload()
		// Also ensure manifest is not pinned (PutDagCBOR might pin it?)
		// IPFS usually pins indirectly or blocks are just in repo.
		// Explicit unpin just in case.
		_ = fp.ipfsClient.UnpinRecursive(ctx, manifestCID)
		return false
	}

	// Pin recursively
	log.Printf("[FileOps] Pinning manifest recursively: %s", manifestCIDStr)
	if err := fp.ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
		log.Printf("[FileOps] Failed to pin ManifestCID %s: %v", manifestCIDStr, err)
		common.LogError("FileOps", "pin ManifestCID recursively", manifestCIDStr, err)
		cleanupPayload()
		return false
	}
	log.Printf("[FileOps] Successfully pinned manifest: %s", manifestCIDStr)

	return true
}

// trackAndAnnounceFile tracks the file and announces it to the cluster that owns it by CID.
// Invariant: each file lives in exactly one cluster (target by payload CID). We pin only there:
// responsible => pin in current shard (current == target); custodial => inject into target shard only, never in ours.
func (fp *FileProcessor) trackAndAnnounceFile(manifestCID cid.Cid, manifestCIDStr string, payloadCID cid.Cid) {
	// Track in local state
	log.Printf("[FileOps] Calling pinFile for: %s", manifestCIDStr)
	if !fp.storageMgr.PinFile(manifestCIDStr) {
		log.Printf("[FileOps] Warning: pinFile returned false for %s (file may be blocked or already tracked)", manifestCIDStr)
		common.LogWarning("FileOps", "Failed to track ManifestCID", manifestCIDStr)
		return
	}
	log.Printf("[FileOps] pinFile succeeded for: %s", manifestCIDStr)
	fp.shardMgr.AnnouncePinned(manifestCIDStr)

	// Invariant: each file lives in exactly one cluster (the target cluster by payload CID).
	// We never pin the same file in the uploader's cluster and another cluster — only in the target.
	payloadCIDStr := payloadCID.String()
	log.Printf("[FileOps] Checking responsibility for PayloadCID: %s", payloadCIDStr)
	isResponsible := fp.shardMgr.AmIResponsibleFor(payloadCIDStr)
	log.Printf("[FileOps] Responsibility check for %s: responsible=%v", payloadCIDStr, isResponsible)

	// Pin only in the cluster that owns this file (target cluster). Never pin in our cluster when we're not responsible.
	if isResponsible {
		log.Printf("[FileOps] Pinning to Cluster (current shard) for: %s", manifestCIDStr)
		if err := fp.shardMgr.PinToCluster(context.Background(), manifestCID); err != nil {
			log.Printf("[FileOps] Error pinning to cluster: %v", err)
		} else {
			log.Printf("[FileOps] Successfully pinned to Cluster state")
		}
	} else {
		// Defensive: we must never PinToCluster when not responsible; file will be injected into target cluster only.
		log.Printf("[FileOps] Not pinning to current cluster (custodial); file will be injected into target cluster only")
	}

	// Check if shardMgr is nil (shouldn't happen, but safety check)
	if fp.shardMgr == nil {
		log.Printf("[FileOps] ERROR: shardMgr is nil! Cannot announce file %s", manifestCIDStr)
		return
	}
	log.Printf("[FileOps] shardMgr check passed, proceeding to addKnownFile for %s", manifestCIDStr)

	// Add panic recovery to catch any issues
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[FileOps] PANIC in trackAndAnnounceFile after addKnownFile for %s: %v", manifestCIDStr, r)
		}
	}()

	log.Printf("[FileOps] About to call addKnownFile for %s", manifestCIDStr)
	fp.storageMgr.AddKnownFile(manifestCIDStr)
	log.Printf("[FileOps] addKnownFile completed, about to log 'Added to known files' for %s", manifestCIDStr)
	log.Printf("[FileOps] Added to known files: %s", manifestCIDStr)

	if isResponsible {
		log.Printf("[FileOps] Calling announceResponsibleFile for %s", manifestCIDStr)
		fp.announceResponsibleFile(manifestCID, manifestCIDStr, payloadCIDStr)
	} else {
		log.Printf("[FileOps] Calling announceCustodialFile for %s", manifestCIDStr)
		fp.announceCustodialFile(manifestCID, manifestCIDStr, payloadCIDStr)
	}
	log.Printf("[FileOps] Announcement completed for %s", manifestCIDStr)
}

// announceResponsibleFile announces a file when this node is responsible for it.
func (fp *FileProcessor) announceResponsibleFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] I am responsible for PayloadCID %s (ManifestCID %s). Announcing to Shard.",
		payloadCIDStr, manifestCIDStr)

	// Get file size for hint (we'd need to pass this, but for now use 0)
	currentShard, _ := fp.shardMgr.GetShardInfo()
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     currentShard,
		HintSize:    0, // TODO: Pass actual size
	}

	// We need signer or sign manually. Signer handles Message Envelope.
	// But FileProcessor doesn't have Signer instance in struct, only privKey.
	// I should probably inject Signer or duplicate signing logic?
	// The original code used a helper `signProtocolMessage` which used globals.
	// I'll assume I need to implement signing here or use a helper.
	// Since I have privKey, I can sign.

	// Helper to sign
	if err := fp.SignProtocolMessage(&im); err != nil {
		common.LogError("FileOps", "sign IngestMessage", manifestCIDStr, err)
	}

	b, err := im.MarshalCBOR()
	if err != nil {
		common.LogError("FileOps", "marshal IngestMessage", manifestCIDStr, err)
		return
	}

	fp.shardMgr.PublishToShardCBOR(b, currentShard)

	// Record our own announcement in internal replication tracker
	// Need to access replication manager via shard manager or inject it
	// Access via ShardManager (it has SetReplicationManager but maybe no getter? I'll check)
	// I'll skip this if I can't easily access it, or use public method on ShardManager if I add one.
	// ShardManager has `GetReplicationManager`? No.
	// But `trackAndAnnounceFile` is called within the node context where everything is wired.

	// Announce to DHT
	provideCtx, provideCancel := context.WithTimeout(context.Background(), config.DHTProvideTimeout)
	go func() {
		defer provideCancel()
		fp.storageMgr.ProvideFile(provideCtx, manifestCIDStr)
	}()
}

// announceCustodialFile injects a file into the cluster that owns it by CID (target shard).
// Invariant: we never pin this file in our own cluster; we only pin and announce in the target cluster.
// The uploader is "aware" of the target cluster by deriving it from the file's payload CID (TargetShardForPayload).
func (fp *FileProcessor) announceCustodialFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] Custodial Mode: I am NOT responsible for PayloadCID %s (ManifestCID %s). Injecting into target shard only.",
		payloadCIDStr, manifestCIDStr)

	currentShard, _ := fp.shardMgr.GetShardInfo()
	targetDepth := len(currentShard)
	if targetDepth == 0 {
		targetDepth = 1
	}
	targetShard := common.TargetShardForPayload(payloadCIDStr, targetDepth)

	// Defensive: custodial implies target != our shard (we are not responsible).
	if targetShard == currentShard {
		log.Printf("[Core] ERROR: Custodial path but target shard %s == current shard (invariant violation); skipping inject", targetShard)
		return
	}

	// Join target shard (Tourist Mode): pubsub + cluster so we can inject there. We never pin in our own cluster.
	fp.shardMgr.JoinShard(targetShard)
	if err := fp.shardMgr.EnsureClusterForShard(context.Background(), targetShard); err != nil {
		log.Printf("[Core] Failed to ensure cluster for target shard %s: %v", targetShard, err)
		return
	}
	// Pin only in target shard's cluster. Never in pinning node's cluster.
	if err := fp.shardMgr.PinToShard(context.Background(), targetShard, manifestCID); err != nil {
		log.Printf("[Core] Failed to pin to target shard %s: %v", targetShard, err)
		return
	}
	log.Printf("[Core] Injected file into target shard %s (not in our shard %s)", targetShard, currentShard)

	// Announce IngestMessage directly to the target shard
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     targetShard,
		HintSize:    0,
	}

	if err := fp.SignProtocolMessage(&im); err != nil {
		common.LogError("FileOps", "sign IngestMessage", manifestCIDStr, err)
	} else if b, err := im.MarshalCBOR(); err != nil {
		common.LogError("FileOps", "marshal IngestMessage", manifestCIDStr, err)
	} else {
		fp.shardMgr.PublishToShardCBOR(b, targetShard)
		log.Printf("[Core] Published IngestMessage to target shard %s", targetShard)
	}

	// We DO NOT LeaveShard here immediately. We must remain in the shard to listen for
	// status updates or simply hold the reference until the file is safe.
}

// WatchFolder watches the data directory for new files.
// It will restart automatically if the watcher fails.
func (fp *FileProcessor) WatchFolder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[FileWatcher] Context cancelled, stopping file watcher")
			return
		default:
			if err := fp.runWatcher(ctx); err != nil {
				log.Printf("[FileWatcher] Watcher exited with error: %v. Restarting in 5 seconds...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					log.Printf("[FileWatcher] Restarting file watcher...")
					// Continue loop to restart
				}
			} else {
				log.Printf("[FileWatcher] Watcher exited normally. Restarting in 5 seconds...")
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					log.Printf("[FileWatcher] Restarting file watcher...")
					// Continue loop to restart
				}
			}
		}
	}
}

// runWatcher runs a single instance of the file watcher.
func (fp *FileProcessor) runWatcher(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	defer watcher.Close()

	if err := os.MkdirAll(config.FileWatchFolder, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := watcher.Add(config.FileWatchFolder); err != nil {
		return fmt.Errorf("failed to watch data directory: %w", err)
	}

	// Track watched directories to avoid duplicates
	watchedDirs := make(map[string]bool)
	watchedDirs[config.FileWatchFolder] = true

	// Recursive watch: Add all existing subdirectories
	err = filepath.Walk(config.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if !watchedDirs[path] {
				if err := watcher.Add(path); err != nil {
					log.Printf("[Error] Failed to watch subdirectory %s: %v", path, err)
				} else {
					watchedDirs[path] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("[Error] Failed to walk directory for watching: %v", err)
	}

	log.Printf("[FileWatcher] Watching %s (and subdirectories) for new files...", config.FileWatchFolder)

	var watchedDirsMu sync.RWMutex

	for {
		select {
		case <-ctx.Done():
			log.Printf("[FileWatcher] Context cancelled, stopping file watcher")
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("events channel closed unexpectedly")
			}

			// Handle Create events for directories (to add new watches)
			if event.Op&fsnotify.Create == fsnotify.Create {
				info, err := os.Stat(event.Name)
				if err == nil && info.IsDir() {
					// It's a directory, watch it (if not already watched)
					watchedDirsMu.RLock()
					alreadyWatched := watchedDirs[event.Name]
					watchedDirsMu.RUnlock()

					if !alreadyWatched {
						if err := watcher.Add(event.Name); err != nil {
							log.Printf("[Error] Failed to watch new directory %s: %v", event.Name, err)
						} else {
							watchedDirsMu.Lock()
							watchedDirs[event.Name] = true
							watchedDirsMu.Unlock()
							log.Printf("[FileWatcher] Added watch for new directory: %s", event.Name)
						}
					}
					// Scan the new directory for existing files and nested subdirectories
					go func(dirPath string) {
						time.Sleep(config.FileProcessingDelay)

						fileCount := 0
						dirCount := 0

						err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								log.Printf("[Warning] Error accessing %s during directory scan: %v", path, err)
								return nil
							}
							if info.IsDir() {
								if path != dirPath {
									watchedDirsMu.RLock()
									alreadyWatched := watchedDirs[path]
									watchedDirsMu.RUnlock()

									if !alreadyWatched {
										if err := watcher.Add(path); err != nil {
											log.Printf("[Error] Failed to watch nested directory %s: %v", path, err)
										} else {
											watchedDirsMu.Lock()
											watchedDirs[path] = true
											watchedDirsMu.Unlock()
											log.Printf("[FileWatcher] Added watch for nested directory: %s", path)
											dirCount++
										}
									}
								}
								return nil
							}
							if !validateFilePath(path) {
								log.Printf("[FileWatcher] File filtered by validation: %s", path)
								return nil
							}
							fileCount++

							// Enqueue instead of spawning goroutine
							if !fp.TryEnqueue(path) {
								log.Printf("[FileWatcher] Dropped file %s due to backpressure", path)
							}
							return nil
						})
						if err != nil {
							log.Printf("[Error] Failed to scan new directory %s: %v", dirPath, err)
						} else {
							log.Printf("[FileWatcher] Scanned directory %s: found %d files, %d nested directories", dirPath, fileCount, dirCount)
						}
					}(event.Name)
					continue
				}
			}

			// Handle Create and Write events for files
			if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
				path := event.Name

				if !validateFilePath(path) {
					continue
				}

				info, err := os.Stat(path)
				if err != nil || info.IsDir() {
					continue
				}

				if shouldProcessFileEvent(path) {
					// Enqueue instead of spawning goroutine
					if !fp.TryEnqueue(path) {
						log.Printf("[FileWatcher] Dropped file %s due to backpressure", path)
					}
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("errors channel closed unexpectedly")
			}
			log.Printf("[FileWatcher] ERROR: Watcher error: %v", err)
		}
	}
}

// SignProtocolMessage signs a message with the node's private key.
// Prefer using the injected Signer (same nonce bounds and semantics as verifier). Fallback uses EffectiveNonceSizeForSigning so verifier never rejects nonce length.
func (fp *FileProcessor) SignProtocolMessage(msg interface{}) error {
	if fp.signer != nil {
		return fp.signer.SignProtocolMessage(msg)
	}
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	nonceSize := signing.EffectiveNonceSizeForSigning()
	nonce, err := common.NewNonce(nonceSize)
	if err != nil {
		return err
	}

	ts := time.Now().Unix()

	switch m := msg.(type) {
	case *schema.IngestMessage:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	case *schema.ReplicationRequest:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	case *schema.UnreplicateRequest:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	default:
		return fmt.Errorf("unsupported message type for signing: %T", msg)
	}
}
