package fileops

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

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

	rel, err := filepath.Rel(absWatch, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		log.Printf("[Security] Rejected path outside watch folder: %s", path)
		return false
	}

	if strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, ".part") || strings.HasSuffix(path, ".crdownload") {
		return false
	}

	return true
}

// processNewFile imports a newly detected file into IPFS, builds a ResearchObject manifest, pins it, and announces it.
func (fp *FileProcessor) processNewFile(path string) {
	log.Printf("[FileOps] Processing: %s", path)

	if !validateFilePath(path) {
		log.Printf("[FileOps] File validation failed: %s", path)
		return
	}

	if fp.ipfsClient == nil {
		common.LogError("FileOps", "process file", path, fmt.Errorf("IPFS client not initialized"))
		return
	}

	ctx, cancel := context.WithTimeout(fp.ctx, config.FileImportTimeout)
	defer cancel()

	if config.VerboseLogging {
		log.Printf("[FileOps] Importing file to IPFS: %s", path)
	}
	payloadCID, cleanupPayload, err := fp.importFileToIPFS(ctx, path)
	if err != nil {
		log.Printf("[FileOps] Failed to import file: %s, error: %v", path, err)
		return
	}

	payloadCIDStr := payloadCID.String()
	fp.recentIngestMu.Lock()
	if lastIngest, seen := fp.recentIngests[payloadCIDStr]; seen && time.Since(lastIngest) < recentIngestTTL {
		fp.recentIngestMu.Unlock()
		log.Printf("[FileOps] Skipping duplicate PayloadCID %s (path: %s, last ingested %v ago)",
			payloadCIDStr, path, time.Since(lastIngest).Round(time.Second))
		cleanupPayload()
		return
	}
	fp.recentIngests[payloadCIDStr] = time.Now()
	now := time.Now()
	for k, t := range fp.recentIngests {
		if now.Sub(t) > 2*recentIngestTTL {
			delete(fp.recentIngests, k)
		}
	}
	fp.recentIngestMu.Unlock()

	if config.VerboseLogging {
		log.Printf("[FileOps] Building manifest for: %s (PayloadCID: %s)", path, payloadCIDStr)
	}
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, path, payloadCID, cleanupPayload)
	if err != nil {
		log.Printf("[FileOps] Failed to build manifest: %s, error: %v", path, err)
		return
	}

	if !fp.checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, path, cleanupPayload) {
		log.Printf("[FileOps] BadBits check or pinning failed: %s", path)
		return
	}

	fp.trackAndAnnounceFile(manifestCID, manifestCIDStr, payloadCID)
	log.Printf("[FileOps] Completed: %s -> ManifestCID %s (PayloadCID %s)", path, manifestCIDStr, payloadCIDStr)
}

func (fp *FileProcessor) importFileToIPFS(ctx context.Context, path string) (cid.Cid, func(), error) {
	payloadCID, err := fp.ipfsClient.ImportFile(ctx, path)
	if err != nil {
		common.LogError("FileOps", "import file to IPFS", path, err)
		return cid.Cid{}, func() {}, err
	}

	cleanupPayload := func() {
		_ = fp.ipfsClient.UnpinRecursive(context.Background(), payloadCID)
	}

	return payloadCID, cleanupPayload, nil
}

func (fp *FileProcessor) buildAndStoreManifest(ctx context.Context, path string, payloadCID cid.Cid, cleanupPayload func()) (cid.Cid, string, error) {
	info, err := os.Stat(path)
	if err != nil {
		common.LogError("FileOps", "stat file", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	metaRef := "file://" + filepath.Base(path)
	ro := schema.NewResearchObject(
		metaRef,
		fp.shardMgr.GetHost().ID(),
		payloadCID,
		uint64(info.Size()),
	)

	if err := fp.signResearchObject(ro); err != nil {
		common.LogError("FileOps", "sign ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	roBytes, err := ro.MarshalCBOR()
	if err != nil {
		common.LogError("FileOps", "marshal ResearchObject", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	manifestCID, err := fp.ipfsClient.PutDagCBOR(ctx, roBytes)
	if err != nil {
		common.LogError("FileOps", "put manifest to IPFS", path, err)
		cleanupPayload()
		return cid.Cid{}, "", err
	}

	return manifestCID, manifestCID.String(), nil
}

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

func (fp *FileProcessor) checkBadBitsAndPin(ctx context.Context, manifestCID cid.Cid, manifestCIDStr, path string, cleanupPayload func()) bool {
	if badbits.IsCIDBlocked(manifestCIDStr) {
		log.Printf("[FileOps] Refused to process file %s (blocked ManifestCID: %s)", path, manifestCIDStr)
		cleanupPayload()
		_ = fp.ipfsClient.UnpinRecursive(ctx, manifestCID)
		return false
	}

	if config.VerboseLogging {
		log.Printf("[FileOps] Pinning manifest recursively: %s", manifestCIDStr)
	}
	if err := fp.ipfsClient.PinRecursive(ctx, manifestCID); err != nil {
		log.Printf("[FileOps] Failed to pin ManifestCID %s: %v", manifestCIDStr, err)
		common.LogError("FileOps", "pin ManifestCID recursively", manifestCIDStr, err)
		cleanupPayload()
		return false
	}

	return true
}

func (fp *FileProcessor) trackAndAnnounceFile(manifestCID cid.Cid, manifestCIDStr string, payloadCID cid.Cid) {
	if !fp.storageMgr.PinFile(manifestCIDStr) {
		if config.VerboseLogging {
			log.Printf("[FileOps] Skipping %s (already tracked or blocked)", manifestCIDStr)
		}
		return
	}
	fp.shardMgr.AnnouncePinned(manifestCIDStr)

	payloadCIDStr := payloadCID.String()
	isResponsible := fp.shardMgr.AmIResponsibleFor(payloadCIDStr)

	if isResponsible {
		if err := fp.shardMgr.PinToCluster(context.Background(), manifestCID); err != nil {
			log.Printf("[FileOps] Error pinning to cluster: %v", err)
		} else if config.VerboseLogging {
			log.Printf("[FileOps] Pinned to cluster state: %s", manifestCIDStr)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[FileOps] PANIC in trackAndAnnounceFile for %s: %v", manifestCIDStr, r)
		}
	}()

	fp.storageMgr.AddKnownFile(manifestCIDStr)

	if isResponsible {
		fp.announceResponsibleFile(manifestCID, manifestCIDStr, payloadCIDStr)
	} else {
		fp.announceCustodialFile(manifestCID, manifestCIDStr, payloadCIDStr)
	}
}

func (fp *FileProcessor) announceResponsibleFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] I am responsible for PayloadCID %s (ManifestCID %s). Announcing to Shard.",
		payloadCIDStr, manifestCIDStr)

	currentShard, _ := fp.shardMgr.GetShardInfo()
	im := schema.IngestMessage{
		Type:        schema.MessageTypeIngest,
		ManifestCID: manifestCID,
		ShardID:     currentShard,
		HintSize:    0,
	}

	if err := fp.SignProtocolMessage(&im); err != nil {
		common.LogError("FileOps", "sign IngestMessage", manifestCIDStr, err)
		return
	}

	b, err := im.MarshalCBOR()
	if err != nil {
		common.LogError("FileOps", "marshal IngestMessage", manifestCIDStr, err)
		return
	}

	fp.shardMgr.PublishIngestMessageToCurrentAndChildIfSplit(b, currentShard, payloadCIDStr)

	provideCtx, provideCancel := context.WithTimeout(context.Background(), config.DHTProvideTimeout)
	go func() {
		defer provideCancel()
		fp.storageMgr.ProvideFile(provideCtx, manifestCIDStr)
	}()
}

func (fp *FileProcessor) announceCustodialFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	log.Printf("[Core] Custodial Mode: I am NOT responsible for PayloadCID %s (ManifestCID %s). Injecting into target shard only.",
		payloadCIDStr, manifestCIDStr)

	currentShard, _ := fp.shardMgr.GetShardInfo()
	targetDepth := len(currentShard)
	if targetDepth == 0 {
		targetDepth = 1
	}
	nominalTarget := common.TargetShardForPayload(payloadCIDStr, targetDepth)
	targetShard := fp.shardMgr.ResolveTargetShardForCustodial(nominalTarget, payloadCIDStr)

	if targetShard == currentShard {
		log.Printf("[Core] ERROR: Custodial path but target shard %s == current shard (invariant violation); skipping inject", targetShard)
		return
	}

	if !fp.shardMgr.JoinShardAsObserver(targetShard) {
		log.Printf("[Core] Failed to join target shard %s as observer for custodial inject", targetShard)
		return
	}
	defer fp.shardMgr.LeaveShardAsObserver(targetShard)

	if err := fp.shardMgr.EnsureClusterForShard(context.Background(), targetShard); err != nil {
		log.Printf("[Core] Failed to ensure cluster for target shard %s: %v", targetShard, err)
		return
	}
	if err := fp.shardMgr.PinToShard(context.Background(), targetShard, manifestCID); err != nil {
		log.Printf("[Core] Failed to pin to target shard %s: %v", targetShard, err)
		return
	}
	log.Printf("[Core] Injected file into target shard %s (not in our shard %s)", targetShard, currentShard)

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
}
