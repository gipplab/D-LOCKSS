package fileops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ipfs/go-cid"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

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

// errNotPinned reports a manifest that is blocked or could not be pinned.
var errNotPinned = errors.New("blocked or pinning failed")

// ingestManifest makes, pins, and announces the manifest of payloadCID, with
// the user's signature u unless it is the zero value. On failure it calls
// cleanupPayload. Uploads and pin ingest both end here.
func (fp *FileProcessor) ingestManifest(ctx context.Context, metaRef string, size uint64, payloadCID cid.Cid, u schema.UploaderSignature, cleanupPayload func()) (cid.Cid, error) {
	ro := schema.NewResearchObject(metaRef, fp.shardMgr.NodeID(), payloadCID, size)
	ro.Uploader, ro.UploaderSig = u.Key, u.Sig
	manifestCID, err := fp.storeManifest(ctx, ro)
	if err != nil {
		cleanupPayload()
		return cid.Undef, err
	}
	if !fp.checkBadBitsAndPin(ctx, manifestCID, metaRef, cleanupPayload) {
		return cid.Undef, errNotPinned
	}
	fp.trackAndAnnounceFile(manifestCID, payloadCID)
	return manifestCID, nil
}

// storeManifest signs ro and stores it in IPFS, or returns this node's pinned
// manifest with the same content.
func (fp *FileProcessor) storeManifest(ctx context.Context, ro *schema.ResearchObject) (cid.Cid, error) {
	if c, ok := fp.existingManifest(ro); ok {
		slog.Debug("reusing existing manifest", "manifest", c, "payload", ro.Payload)
		return c, nil
	}
	if err := fp.signResearchObject(ctx, ro); err != nil {
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

// signResearchObject has the Kubo node sign ro.
func (fp *FileProcessor) signResearchObject(ctx context.Context, ro *schema.ResearchObject) error {
	if fp.nodeSigner == nil {
		slog.Warn("no node signer, manifest will not be signed")
		return nil
	}

	unsignedBytes, err := ro.MarshalCBORForSigning()
	if err != nil {
		return fmt.Errorf("failed to marshal for signing: %w", err)
	}

	sig, err := fp.nodeSigner.SignWithNodeKey(ctx, unsignedBytes)
	if err != nil {
		return fmt.Errorf("failed to sign: %w", err)
	}

	ro.Signature = sig
	return nil
}

func (fp *FileProcessor) checkBadBitsAndPin(ctx context.Context, manifestCID cid.Cid, metaRef string, cleanupPayload func()) bool {
	manifestCIDStr := manifestCID.String()
	if fp.badBits.IsBlocked(manifestCIDStr) {
		slog.Warn("refused to process blocked file", "meta_ref", metaRef, "manifest", manifestCIDStr)
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

func (fp *FileProcessor) trackAndAnnounceFile(manifestCID, payloadCID cid.Cid) {
	manifestCIDStr := manifestCID.String()
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
