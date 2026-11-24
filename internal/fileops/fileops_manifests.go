package fileops

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// manifestIndexLoadTimeout bounds reading the pinned manifests at startup.
const manifestIndexLoadTimeout = 10 * time.Minute

// manifestIndex maps the unsigned content of this node's pinned manifests to
// their CIDs, so that re-ingesting a file reuses its manifest even if it was
// signed in the older format, directly with the node key.
type manifestIndex struct {
	mu        sync.Mutex
	byContent map[[32]byte]cid.Cid
}

// loadManifestIndex fills the index from the manifests pinned in Kubo. Until
// it has run, or if it fails, ingests store new manifests.
func (fp *FileProcessor) loadManifestIndex() {
	if fp.ipfsClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(fp.ctx, manifestIndexLoadTimeout)
	defer cancel()
	start := time.Now()
	pins, err := fp.ipfsClient.RecursivePins(ctx)
	if err != nil {
		slog.Warn("manifest index: listing pins failed; re-ingested files get new manifests", "error", err)
		return
	}
	self := fp.shardMgr.NodeID()
	byContent := make(map[[32]byte]cid.Cid)
	for c, name := range pins {
		if c.Type() != cid.DagCBOR && name != ipfs.ManifestPinName {
			continue
		}
		block, err := fp.ipfsClient.GetBlock(ctx, c)
		if err != nil {
			if ctx.Err() != nil {
				slog.Warn("manifest index: reading pins timed out; re-ingested files get new manifests", "error", ctx.Err())
				return
			}
			continue
		}
		var ro schema.ResearchObject
		if ro.UnmarshalCBOR(block) != nil {
			continue
		}
		if key, ok := ownManifestKey(&ro, self); ok {
			byContent[key] = c
		}
	}

	fp.manifests.mu.Lock()
	fp.manifests.byContent = byContent
	fp.manifests.mu.Unlock()
	slog.Info("manifest index loaded", "own_manifests", len(byContent), "pins", len(pins), "took", time.Since(start).Round(time.Millisecond))
}

// ownManifestKey returns the index key of ro if self ingested and signed it.
// Legacy manifests with a timestamp are skipped; the legacy cleanup removes
// them.
func ownManifestKey(ro *schema.ResearchObject, self peer.ID) ([32]byte, bool) {
	if self == "" || ro.IngestedBy != self || ro.HasLegacyTimestamp || ro.VerifySignature() != nil {
		return [32]byte{}, false
	}
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(unsigned), true
}

// existingManifest returns a manifest from the index with the same unsigned
// content as ro.
func (fp *FileProcessor) existingManifest(ro *schema.ResearchObject) (cid.Cid, bool) {
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		return cid.Undef, false
	}
	fp.manifests.mu.Lock()
	defer fp.manifests.mu.Unlock()
	c, ok := fp.manifests.byContent[sha256.Sum256(unsigned)]
	return c, ok
}
