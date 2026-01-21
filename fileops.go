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
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Printf("[Error] Invalid path: %v", err)
		return
	}

	absWatch, err := filepath.Abs(FileWatchFolder)
	if err != nil {
		log.Printf("[Error] Invalid watch folder: %v", err)
		return
	}

	// Prevent path traversal / prefix confusion:
	// - filepath.Abs cleans .. segments
	// - filepath.Rel ensures absPath is within absWatch (not just string-prefix)
	rel, err := filepath.Rel(absWatch, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		log.Printf("[Security] Rejected path outside watch folder: %s", path)
		return
	}

	// Check if IPFS client is available
	if ipfsClient == nil {
		log.Printf("[Error] IPFS client not initialized. Cannot process file: %s", path)
		return
	}

	// Step 1: Import file to IPFS (get PayloadCID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	payloadCID, err := ipfsClient.ImportFile(ctx, path)
	if err != nil {
		log.Printf("[Error] Failed to import file to IPFS: %s: %v", path, err)
		return
	}
	// If ingestion fails later, ensure we don't leak a pinned payload.
	cleanupPayload := func() {
		_ = ipfsClient.UnpinRecursive(ctx, payloadCID)
	}

	// Step 2: Get file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Printf("[Error] Failed to stat file: %s: %v", path, err)
		return
	}
	totalSize := uint64(fileInfo.Size())

	// Step 3: Extract metadata (simple: filename as title, empty authors for now)
	title := filepath.Base(path)
	authors := []string{}     // TODO: Extract from file metadata or user input
	references := []cid.Cid{} // TODO: Allow user to specify citations

	// Step 4: Get ingester ID (current node's peer ID)
	var ingesterID peer.ID
	if shardMgr != nil && shardMgr.h != nil {
		ingesterID = shardMgr.h.ID()
	} else {
		log.Printf("[Error] Cannot get ingester ID: shard manager not initialized")
		return
	}

	// Step 5: Create ResearchObject
	ro := schema.NewResearchObject(title, authors, ingesterID, payloadCID, references, totalSize)

	// Step 6: Sign manifest (ResearchObject) and store in IPFS (dag-cbor) to obtain ManifestCID
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		log.Printf("[Error] Failed to marshal ResearchObject for signing: %v", err)
		return
	}
	if selfPrivKey == nil {
		log.Printf("[Sig] Warning: missing private key; ResearchObject manifest will not be signed")
	} else {
		sig, err := selfPrivKey.Sign(unsigned)
		if err != nil {
			log.Printf("[Sig] Failed to sign ResearchObject: %v", err)
		} else {
			ro.Signature = sig
		}
	}

	manifestBytes, err := ro.MarshalCBOR()
	if err != nil {
		log.Printf("[Error] Failed to marshal ResearchObject: %v", err)
		cleanupPayload()
		return
	}

	manifestCID, err := ipfsClient.PutDagCBOR(ctx, manifestBytes)
	if err != nil {
		log.Printf("[Error] Failed to store manifest block in IPFS: %v", err)
		cleanupPayload()
		return
	}

	manifestCIDStr := manifestCID.String()

	// Step 7: Check BadBits (on ManifestCID)
	if isCIDBlocked(manifestCIDStr, NodeCountry) {
		log.Printf("[FileOps] Refused to process file %s (blocked ManifestCID: %s)", path, manifestCIDStr[:16]+"...")
		// Unpin the payload since we're rejecting it
		cleanupPayload()
		return
	}

	// Step 8: Pin recursively (ManifestCID will pin PayloadCID)
	if err := ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
		log.Printf("[Error] Failed to pin ManifestCID recursively: %v", err)
		// Best-effort cleanup: payload may be pinned from import.
		cleanupPayload()
		return
	}

	// Step 9: Store in our tracking (using ManifestCID string as key)
	manifestKey := manifestCIDStr // Use ManifestCID string as the key for tracking
	if !pinFileV2(manifestKey) {
		log.Printf("[FileOps] Failed to track ManifestCID: %s", manifestCIDStr[:16]+"...")
		return
	}

	// Step 10: Provide to DHT
	provideCtx, provideCancel := context.WithTimeout(context.Background(), 30*time.Second)
	go func() {
		defer provideCancel()
		provideFile(provideCtx, manifestKey)
	}()

	addKnownFile(manifestKey)

	// Step 11: Determine responsibility and announce
	if shardMgr.AmIResponsibleFor(manifestKey) {
		log.Printf("[Core] I am responsible for ManifestCID %s. Announcing to Shard.", manifestCIDStr[:16]+"...")
		im := schema.IngestMessage{
			Type:        schema.MessageTypeIngest,
			ManifestCID: manifestCID,
			ShardID:     shardMgr.currentShard,
			HintSize:    totalSize,
		}
		if err := signProtocolMessage(&im); err != nil {
			log.Printf("[Sig] Failed to sign IngestMessage: %v", err)
		}
		b, err := im.MarshalCBOR()
		if err != nil {
			log.Printf("[Error] Failed to marshal IngestMessage: %v", err)
			return
		}
		shardMgr.PublishToShardCBOR(b)
	} else {
		targetPrefix := getHexBinaryPrefix(keyToStableHex(manifestKey), len(shardMgr.currentShard))
		log.Printf("[Core] Custodial Mode: %s belongs to shard %s. Delegating but holding...", manifestCIDStr[:16]+"...", targetPrefix)
		dm := schema.DelegateMessage{
			Type:        schema.MessageTypeDelegate,
			ManifestCID: manifestCID,
			TargetShard: targetPrefix,
		}
		if err := signProtocolMessage(&dm); err != nil {
			log.Printf("[Sig] Failed to sign DelegateMessage: %v", err)
		}
		b, err := dm.MarshalCBOR()
		if err != nil {
			log.Printf("[Error] Failed to marshal DelegateMessage: %v", err)
			return
		}
		shardMgr.PublishToControlCBOR(b)
	}
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
				time.Sleep(100 * time.Millisecond)
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
							time.Sleep(200 * time.Millisecond)
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

