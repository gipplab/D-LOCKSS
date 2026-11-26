package fileops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

// ErrPayloadMismatch reports an uploaded file whose CID is not the signed one.
var ErrPayloadMismatch = errors.New("the file does not have the signed CID")

// IngestUpload ingests the file at path, which uploader signed as payload
// (see schema.VerifyUpload, checked by the caller). metaRef names the file.
func (fp *FileProcessor) IngestUpload(ctx context.Context, path, metaRef string, payload cid.Cid, uploader peer.ID, sig []byte) (cid.Cid, error) {
	if !fp.shardMgr.IsLocalNodeIngestor() {
		return cid.Undef, errors.New("this node is not on the ingest allowlist")
	}
	info, err := os.Stat(path)
	if err != nil {
		return cid.Undef, err
	}
	ctx, cancel := context.WithTimeout(ctx, fp.cfg.Files.FileImportTimeout)
	defer cancel()

	payloadCID, cleanupPayload, err := fp.importFileToIPFS(ctx, path)
	if err != nil {
		return cid.Undef, fmt.Errorf("import: %w", err)
	}
	if !payloadCID.Equals(payload) {
		cleanupPayload()
		return cid.Undef, fmt.Errorf("%w: got %s, signed %s", ErrPayloadMismatch, payloadCID, payload)
	}

	manifestCID, err := fp.ingestManifest(ctx, metaRef, uint64(info.Size()), payloadCID,
		schema.UploaderSignature{Key: uploader, Sig: sig}, cleanupPayload)
	if err != nil {
		return cid.Undef, err
	}
	slog.Info("upload ingested", "uploader", uploader, "payload", payloadCID, "manifest", manifestCID)
	return manifestCID, nil
}
