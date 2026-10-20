package fileops

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

func (fp *FileProcessor) validateFilePath(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		slog.Error("failed to resolve absolute path", "path", path, "error", err)
		return false
	}

	absWatch, err := filepath.Abs(fp.cfg.FileWatchFolder)
	if err != nil {
		slog.Error("failed to resolve watch folder path", "path", fp.cfg.FileWatchFolder, "error", err)
		return false
	}

	rel, err := filepath.Rel(absWatch, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		slog.Warn("rejected path outside watch folder", "path", path)
		return false
	}

	if strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, ".part") || strings.HasSuffix(path, ".crdownload") {
		return false
	}

	return true
}

// processNewFile imports a newly detected file into IPFS, builds a ResearchObject manifest, pins it, and announces it.
func (fp *FileProcessor) processNewFile(path string) {
	slog.Info("processing file", "path", path)

	if !fp.shardMgr.IsLocalNodeIngestor() {
		slog.Warn("node is not an authorized ingestor, skipping file", "path", path)
		return
	}

	if !fp.validateFilePath(path) {
		slog.Warn("file validation failed", "path", path)
		return
	}

	if fp.ipfsClient == nil {
		slog.Error("IPFS client not initialized", "path", path)
		return
	}

	ctx, cancel := context.WithTimeout(fp.ctx, fp.cfg.Files.FileImportTimeout)
	defer cancel()

	slog.Debug("importing file to IPFS", "path", path)
	payloadCID, cleanupPayload, err := fp.importFileToIPFS(ctx, path)
	if err != nil {
		slog.Error("failed to import file", "path", path, "error", err)
		return
	}

	payloadCIDStr := payloadCID.String()
	if !fp.claimRecentIngest(payloadCIDStr) {
		slog.Info("skipping duplicate payload", "payload", payloadCIDStr, "path", path)
		cleanupPayload()
		return
	}

	slog.Debug("building manifest", "path", path, "payload", payloadCIDStr)
	info, err := os.Stat(path)
	if err != nil {
		cleanupPayload()
		slog.Error("failed to stat file", "path", path, "error", err)
		return
	}
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, "file://"+filepath.Base(path), uint64(info.Size()), payloadCID, cleanupPayload)
	if err != nil {
		slog.Error("failed to build manifest", "path", path, "error", err)
		return
	}

	if !fp.checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, path, cleanupPayload) {
		slog.Warn("badbits check or pinning failed", "path", path)
		return
	}

	fp.trackAndAnnounceFile(manifestCID, manifestCIDStr, payloadCID)
	slog.Info("file processing completed", "path", path, "manifest", manifestCIDStr, "payload", payloadCIDStr)
}

// claimRecentIngest records payload as ingested now. It reports false when the
// payload was already ingested within recentIngestTTL.
func (fp *FileProcessor) claimRecentIngest(payload string) bool {
	fp.recentIngestMu.Lock()
	defer fp.recentIngestMu.Unlock()
	now := time.Now()
	if last, seen := fp.recentIngests[payload]; seen && now.Sub(last) < recentIngestTTL {
		return false
	}
	fp.recentIngests[payload] = now
	for k, t := range fp.recentIngests {
		if now.Sub(t) > 2*recentIngestTTL {
			delete(fp.recentIngests, k)
		}
	}
	return true
}

func (fp *FileProcessor) importFileToIPFS(ctx context.Context, path string) (cid.Cid, func(), error) {
	payloadCID, err := fp.ipfsClient.ImportFile(ctx, path)
	if err != nil {
		return cid.Cid{}, func() {}, err
	}

	cleanupPayload := func() {
		_ = fp.ipfsClient.UnpinRecursive(context.Background(), payloadCID)
	}

	return payloadCID, cleanupPayload, nil
}

// buildAndStoreManifest signs and stores the manifest for payloadCID. On
// failure it calls cleanupPayload.
func (fp *FileProcessor) buildAndStoreManifest(ctx context.Context, metaRef string, size uint64, payloadCID cid.Cid, cleanupPayload func()) (cid.Cid, string, error) {
	ro := schema.NewResearchObject(metaRef, fp.shardMgr.PeerID(), payloadCID, size)
	manifestCID, err := fp.storeManifest(ctx, ro)
	if err != nil {
		cleanupPayload()
		return cid.Cid{}, "", err
	}
	return manifestCID, manifestCID.String(), nil
}

func (fp *FileProcessor) storeManifest(ctx context.Context, ro *schema.ResearchObject) (cid.Cid, error) {
	if err := fp.signResearchObject(ro); err != nil {
		return cid.Cid{}, err
	}
	roBytes, err := ro.MarshalCBOR()
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	manifestCID, err := fp.ipfsClient.PutDagCBOR(ctx, roBytes)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to store manifest: %w", err)
	}
	return manifestCID, nil
}

func (fp *FileProcessor) signResearchObject(ro *schema.ResearchObject) error {
	if fp.privKey == nil {
		slog.Warn("missing private key, manifest will not be signed")
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
	if fp.badBits.IsBlocked(manifestCIDStr) {
		slog.Warn("refused to process blocked file", "path", path, "manifest", manifestCIDStr)
		cleanupPayload()
		_ = fp.ipfsClient.UnpinRecursive(ctx, manifestCID)
		return false
	}

	slog.Debug("pinning manifest recursively", "manifest", manifestCIDStr)
	if err := fp.ipfsClient.PinManifest(ctx, manifestCID); err != nil {
		slog.Error("failed to pin manifest", "manifest", manifestCIDStr, "error", err)
		cleanupPayload()
		return false
	}

	return true
}

func (fp *FileProcessor) trackAndAnnounceFile(manifestCID cid.Cid, manifestCIDStr string, payloadCID cid.Cid) {
	if !fp.storageMgr.PinFile(manifestCIDStr) {
		slog.Debug("skipping already tracked or blocked manifest", "manifest", manifestCIDStr)
		return
	}
	fp.shardMgr.AnnouncePinned(manifestCIDStr)

	payloadCIDStr := payloadCID.String()
	isResponsible := fp.shardMgr.AmIResponsibleFor(payloadCIDStr)

	if isResponsible {
		if err := fp.shardMgr.PinToCluster(fp.ctx, manifestCID); err != nil {
			slog.Error("error pinning to cluster", "error", err)
		} else {
			slog.Debug("pinned to cluster state", "manifest", manifestCIDStr)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in trackAndAnnounceFile", "manifest", manifestCIDStr, "recover", r)
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
	slog.Info("responsible for file, announcing to shard", "payload", payloadCIDStr, "manifest", manifestCIDStr)

	currentShard := fp.shardMgr.GetShardInfo()
	b, err := fp.encodeIngestMessage(manifestCID, currentShard)
	if err != nil {
		slog.Error("failed to encode ingest message", "manifest", manifestCIDStr, "error", err)
		return
	}
	fp.shardMgr.PublishIngestMessageToCurrentAndChildIfSplit(b, currentShard, payloadCIDStr)

	// Both manifest and payload are provided so gateways can find this node.
	// Separate timeouts keep a slow manifest provide from starving the payload.
	fp.provideAsync(manifestCIDStr)
	fp.provideAsync(payloadCIDStr)
}

func (fp *FileProcessor) provideAsync(key string) {
	go func() {
		ctx, cancel := context.WithTimeout(fp.ctx, fp.cfg.Files.DHTProvideTimeout)
		defer cancel()
		fp.storageMgr.ProvideFile(ctx, key)
	}()
}

// encodeIngestMessage returns a signed, CBOR-encoded IngestMessage that
// announces manifestCID to shardID.
func (fp *FileProcessor) encodeIngestMessage(manifestCID cid.Cid, shardID string) ([]byte, error) {
	im := schema.IngestMessage{
		SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: manifestCID},
		ShardID:        shardID,
	}
	if err := fp.signer.SignProtocolMessage(&im); err != nil {
		return nil, fmt.Errorf("failed to sign IngestMessage: %w", err)
	}
	b, err := im.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal IngestMessage: %w", err)
	}
	return b, nil
}

func (fp *FileProcessor) announceCustodialFile(manifestCID cid.Cid, manifestCIDStr, payloadCIDStr string) {
	slog.Info("custodial mode, injecting into target shard", "payload", payloadCIDStr, "manifest", manifestCIDStr)

	currentShard := fp.shardMgr.GetShardInfo()
	targetDepth := len(currentShard)
	if targetDepth == 0 {
		targetDepth = 1
	}
	nominalTarget, err := common.TargetShardForPayload(payloadCIDStr, targetDepth)
	if err != nil {
		slog.Error("failed to compute target shard", "payload", payloadCIDStr, "error", err)
		return
	}
	targetShard := fp.shardMgr.ResolveTargetShardForCustodial(nominalTarget, payloadCIDStr)

	if targetShard == currentShard {
		// The payload's range is held by this node's own shard after all.
		if err := fp.shardMgr.PinToCluster(fp.ctx, manifestCID); err != nil {
			slog.Error("error pinning to cluster", "error", err)
		}
		fp.announceResponsibleFile(manifestCID, manifestCIDStr, payloadCIDStr)
		return
	}

	if !fp.shardMgr.JoinShardAsObserver(targetShard) {
		slog.Error("failed to join target shard as observer", "target_shard", targetShard)
		return
	}
	defer fp.shardMgr.LeaveShardAsObserver(targetShard)

	if err := fp.shardMgr.EnsureClusterForShard(fp.ctx, targetShard); err != nil {
		slog.Error("failed to ensure cluster for target shard", "target_shard", targetShard, "error", err)
		return
	}
	if err := fp.shardMgr.PinToShard(fp.ctx, targetShard, manifestCID); err != nil {
		slog.Error("failed to pin to target shard", "target_shard", targetShard, "error", err)
		return
	}
	slog.Info("injected file into target shard", "target_shard", targetShard, "current_shard", currentShard)

	b, err := fp.encodeIngestMessage(manifestCID, targetShard)
	if err != nil {
		slog.Error("failed to encode ingest message", "manifest", manifestCIDStr, "error", err)
		return
	}
	fp.shardMgr.PublishToShardCBOR(b, targetShard)
	slog.Info("published ingest message to target shard", "target_shard", targetShard)
}
