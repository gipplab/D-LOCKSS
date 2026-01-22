package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"dlockss/pkg/schema"
)

// scanExistingFiles walks the data directory and processes any existing files.
// This is the legacy V1 path that ingests files by computing a SHA-256 hash.
// In V2, new ingests go through ResearchObject manifests in processNewFile below.
func scanExistingFiles() {
	var fileCount int
	err := filepath.Walk(FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("[Warning] Error accessing %s: %v", path, err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		// In V2, file ingestion goes through the ResearchObject path in processNewFile
		// which lives in the main package. Keep this call as-is for now; the symbol
		// will be provided by the V2 implementation.
		processNewFile(path)
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
// manifest, pins it, and announces it to the D-LOCKSS network (V2 ingestion path).
func processNewFile(path string) {
	// Validate path and check prerequisites
	if !validateFilePath(path) {
		return
	}

	if ipfsClient == nil {
		logError("FileOps", "process file", path, fmt.Errorf("IPFS client not initialized"))
		return
	}

	// Import file and create manifest
	ctx, cancel := context.WithTimeout(context.Background(), FileImportTimeout)
	defer cancel()

	payloadCID, cleanupPayload, err := importFileToIPFS(ctx, path)
	if err != nil {
		return
	}
	defer cleanupPayload()

	// Build and store ResearchObject
	manifestCID, manifestCIDStr, err := buildAndStoreManifest(ctx, path, payloadCID, cleanupPayload)
	if err != nil {
		return
	}

	// Check BadBits and pin
	if !checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, path, cleanupPayload) {
		return
	}

	// Track and announce
	trackAndAnnounceFile(ctx, manifestCID, manifestCIDStr, payloadCID)
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

	return true
}

// importFileToIPFS imports a file to IPFS and returns the PayloadCID and a cleanup function.
func importFileToIPFS(ctx context.Context, path string) (cid.Cid, func(), error) {
	payloadCID, err := ipfsClient.ImportFile(ctx, path)
	if err != nil {
		logError("FileOps", "import file to IPFS", path, err)
		return cid.Cid{}, nil, err
	}

	cleanupPayload := func() {
		if err := ipfsClient.UnpinRecursive(ctx, payloadCID); err != nil {
			logWarningWithContext("FileOps", "Failed to cleanup payload CID during error recovery", payloadCID.String(), "cleanup")
		}
	}

	return payloadCID, cleanupPayload, nil
}

// buildAndStoreManifest creates a ResearchObject, signs it, and stores it in IPFS.
func buildAndStoreManifest(ctx context.Context, path string, payloadCID cid.Cid, cleanupPayload func()) (cid.Cid, string, error) {
	// Get file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		logError("FileOps", "stat file", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}
	totalSize := uint64(fileInfo.Size())

	// Extract metadata
	title := filepath.Base(path)
	authors := []string{}     // TODO: Extract from file metadata or user input
	references := []cid.Cid{} // TODO: Allow user to specify citations

	// Get ingester ID
	var ingesterID peer.ID
	if shardMgr != nil && shardMgr.h != nil {
		ingesterID = shardMgr.h.ID()
	} else {
		logError("FileOps", "get ingester ID", path, fmt.Errorf("shard manager not initialized"))
		cleanupPayload()
		return cid.Cid{}, "", fmt.Errorf("shard manager not initialized")
	}

	// Create and sign ResearchObject
	ro := schema.NewResearchObject(title, authors, ingesterID, payloadCID, references, totalSize)
	if err := signResearchObject(ro); err != nil {
		logError("FileOps", "sign ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	// Marshal and store in IPFS
	manifestBytes, err := ro.MarshalCBOR()
	if err != nil {
		logError("FileOps", "marshal ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	manifestCID, err := ipfsClient.PutDagCBOR(ctx, manifestBytes)
	if err != nil {
		logError("FileOps", "store manifest block in IPFS", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	return manifestCID, manifestCID.String(), nil
}

// signResearchObject signs a ResearchObject if a private key is available.
func signResearchObject(ro *schema.ResearchObject) error {
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		return err
	}

	if selfPrivKey == nil {
		log.Printf("[Sig] Warning: missing private key; ResearchObject manifest will not be signed")
		return nil
	}

	sig, err := selfPrivKey.Sign(unsigned)
	if err != nil {
		return err
	}

	ro.Signature = sig
	return nil
}

// checkBadBitsAndPin checks BadBits and pins the manifest if allowed.
func checkBadBitsAndPin(ctx context.Context, manifestCID cid.Cid, manifestCIDStr, path string, cleanupPayload func()) bool {
	// Check BadBits
	if isCIDBlocked(manifestCIDStr, NodeCountry) {
		log.Printf("[FileOps] Refused to process file %s (blocked ManifestCID: %s)", path, truncateCID(manifestCIDStr, 16))
		cleanupPayload()
		return false
	}

	// Pin recursively
	if err := ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
		logError("FileOps", "pin ManifestCID recursively", manifestCIDStr, err)
		cleanupPayload()
		return false
	}

	return true
}

// trackAndAnnounceFile tracks the file and announces it to the appropriate shard.
func trackAndAnnounceFile(ctx context.Context, manifestCID cid.Cid, manifestCIDStr string, payloadCID cid.Cid) {
	// Track in local state
	if !pinFileV2(manifestCIDStr) {
		logWarning("FileOps", "Failed to track ManifestCID", manifestCIDStr)
		return
	}

	addKnownFile(manifestCIDStr)

	// Determine responsibility and announce
	payloadCIDStr := payloadCID.String()
	if shardMgr.AmIResponsibleFor(payloadCIDStr) {
		announceResponsibleFile(ctx, manifestCID, manifestCIDStr, payloadCIDStr)
	} else {
		announceCustodialFile(manifestCID, manifestCIDStr, payloadCIDStr)
	}
}

// announceResponsibleFile announces a file when this node is responsible for it.
func announceResponsibleFile(ctx context.Context, manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] I am responsible for PayloadCID %s (ManifestCID %s). Announcing to Shard.",
		truncateCID(payloadCIDStr, 16), truncateCID(manifestCIDStr, 16))

	// Get file size for hint (we'd need to pass this, but for now use 0)
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     shardMgr.currentShard,
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

	shardMgr.PublishToShardCBOR(b)

	// Announce to DHT
	provideCtx, provideCancel := context.WithTimeout(context.Background(), DHTProvideTimeout)
	go func() {
		defer provideCancel()
		provideFile(provideCtx, manifestCIDStr)
	}()
}

// announceCustodialFile announces a file when this node is in custodial mode.
func announceCustodialFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	targetPrefix := getHexBinaryPrefix(keyToStableHex(payloadCIDStr), len(shardMgr.currentShard))
	log.Printf("[Core] Custodial Mode: PayloadCID %s (ManifestCID %s) belongs to shard %s. Delegating but holding...",
		truncateCID(payloadCIDStr, 16), truncateCID(manifestCIDStr, 16), targetPrefix)

	dm := schema.DelegateMessage{
		Type:        schema.MessageTypeDelegate,
		ManifestCID: manifestCID,
		TargetShard: targetPrefix,
	}

	if err := signProtocolMessage(&dm); err != nil {
		logError("FileOps", "sign DelegateMessage", manifestCIDStr, err)
	}

	b, err := dm.MarshalCBOR()
	if err != nil {
		logError("FileOps", "marshal DelegateMessage", manifestCIDStr, err)
		return
	}

	shardMgr.PublishToControlCBOR(b)
}

// watchFolder monitors the data directory recursively for new or updated files
// and ingests them as ResearchObjects (V2 path).
func watchFolder(ctx context.Context) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[Error] Failed to create file watcher: %v", err)
		return
	}
	defer watcher.Close()

	addWatchDir := func(path string) error {
		if err := watcher.Add(path); err != nil {
			return err
		}
		return nil
	}

	absWatch, err := filepath.Abs(FileWatchFolder)
	if err != nil {
		log.Printf("[Error] Failed to get absolute path for watch folder: %v", err)
		return
	}

	if err := filepath.Walk(absWatch, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if err := addWatchDir(path); err != nil {
				log.Printf("[Warning] Failed to watch directory %s: %v", path, err)
			}
		}
		return nil
	}); err != nil {
		log.Printf("[Error] Failed to initialize recursive watch: %v", err)
		return
	}

	log.Printf("[FileWatcher] Watching %s recursively", FileWatchFolder)

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Treat Create, Write, and Rename as signals that new content is present.
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				time.Sleep(FileProcessingDelay)
				info, err := os.Stat(event.Name)
				if err != nil {
					continue
				}
				if info.IsDir() && event.Op&fsnotify.Create == fsnotify.Create {
					// New directory: start watching it and scan existing files inside once.
					if err := addWatchDir(event.Name); err != nil {
						log.Printf("[Warning] Failed to watch new directory %s: %v", event.Name, err)
					} else {
						log.Printf("[FileWatcher] Added watch for new directory: %s", event.Name)
						go func(dirPath string) {
							time.Sleep(FileRetryDelay)
							filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
								if err != nil {
									return nil
								}
								if !info.IsDir() {
									processNewFile(path)
								}
								return nil
							})
						}(event.Name)
					}
				} else if !info.IsDir() {
					// New or updated file inside watched tree.
					if shouldProcessFileEvent(event.Name) {
						processNewFile(event.Name)
					}
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[Error] File watcher error: %v", err)
		}
	}
}

// Legacy V1 helpers below are kept for backward compatibility with hash-based keys.

// calculateFileHash computes a SHA-256 hash of a file on disk.
// NOTE: This is only used for legacy V1 hash-based ingests; V2 uses ManifestCIDs.
func calculateFileHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashToCid converts a legacy V1 SHA-256 hex hash into a CID.
// This is only used for backward compatibility with V1 hash-based keys.
// V2 uses ManifestCID strings directly, which can be decoded with cid.Decode().
func hashToCid(hash string) (cid.Cid, error) {
	b, err := hex.DecodeString(hash)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("invalid hash format: %w", err)
	}
	mh, err := multihash.Sum(b, 0x12, 32)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create multihash: %w", err)
	}
	return cid.NewCidV1(cid.Raw, mh), nil
}

