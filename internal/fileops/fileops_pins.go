package fileops

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

const (
	defaultPinIngestInterval = time.Minute
	// pinIngestRetryAfter is how long a pin whose ingest failed waits before
	// the next attempt.
	pinIngestRetryAfter = 30 * time.Minute
)

// emptyDir is pinned by 'ipfs init' and holds no data.
var emptyDir = cid.MustParse("QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")

type pinKind int

const (
	pinUser pinKind = iota
	pinManifest
	// pinUnknown is a dag-cbor pin whose block could not be read, so it is
	// neither ingested nor counted as a manifest until a later poll.
	pinUnknown
)

type pinClass struct {
	kind    pinKind
	payload cid.Cid
}

// pinWatcher ingests recursive pins that users create in Kubo, e.g. with
// 'ipfs pin add' or "Set pinning" in the WebUI. Content in MFS that is not
// pinned is not ingested. D-LOCKSS's own pins are skipped: manifests, named
// ipfs.ManifestPinName or recognised by decoding, and every payload a pinned
// manifest references.
type pinWatcher struct {
	fp *FileProcessor
	// classes caches the classification of each pinned CID. Blocks are
	// immutable, so only pinUnknown is ever looked at again.
	classes map[cid.Cid]pinClass
	// pending holds user pins seen in the previous poll. A pin is ingested on
	// its second sighting, because an ingest from the watch folder pins the
	// payload before its manifest exists.
	pending map[string]struct{}
	// failed holds the time of the last failed ingest per pin.
	failed map[string]time.Time
}

func newPinWatcher(fp *FileProcessor) *pinWatcher {
	return &pinWatcher{
		fp:      fp,
		classes: make(map[cid.Cid]pinClass),
		pending: make(map[string]struct{}),
		failed:  make(map[string]time.Time),
	}
}

// WatchPins polls Kubo's recursive pins and ingests those created by users.
func (fp *FileProcessor) WatchPins(ctx context.Context) {
	interval := fp.cfg.Files.PinIngestInterval
	if interval <= 0 {
		interval = defaultPinIngestInterval
	}
	slog.Info("watching kubo pins for ingest", "interval", interval)
	w := newPinWatcher(fp)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		w.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *pinWatcher) poll(ctx context.Context) {
	if !w.fp.shardMgr.IsLocalNodeIngestor() {
		return
	}
	pins, err := w.fp.ipfsClient.RecursivePins(ctx)
	if err != nil {
		slog.Warn("pin ingest: listing pins failed", "error", err)
		return
	}

	// Keys are multihashes, so a CIDv0 user pin matches the CIDv1 payload
	// of a manifest.
	covered := make(map[string]struct{})
	classes := make(map[cid.Cid]pinClass, len(pins))
	unknown := 0
	for c, name := range pins {
		cl := w.classify(ctx, c, name)
		classes[c] = cl
		switch cl.kind {
		case pinManifest:
			covered[string(c.Hash())] = struct{}{}
			if cl.payload.Defined() {
				covered[string(cl.payload.Hash())] = struct{}{}
			}
		case pinUnknown:
			unknown++
		}
	}
	w.classes = classes
	// An unreadable pin may be a manifest whose payload would then look like
	// a user pin.
	if unknown > 0 {
		slog.Debug("pin ingest: skipping pass, some pins could not be classified", "unknown", unknown)
		return
	}

	now := time.Now()
	pending := make(map[string]struct{})
	failed := make(map[string]time.Time)
	ingested := 0
	for c, cl := range classes {
		key := string(c.Hash())
		if cl.kind != pinUser || key == string(emptyDir.Hash()) {
			continue
		}
		if _, ok := covered[key]; ok {
			continue
		}
		pending[key] = struct{}{}
		if t, ok := w.failed[key]; ok && now.Sub(t) < pinIngestRetryAfter {
			failed[key] = t
			continue
		}
		if _, seen := w.pending[key]; !seen {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if w.fp.ingestPinned(ctx, c, pins[c]) {
			ingested++
		} else {
			failed[key] = now
		}
	}
	w.pending = pending
	w.failed = failed
	if ingested > 0 {
		slog.Info("pin ingest pass complete", "ingested", ingested, "pins", len(pins))
	}
}

func (w *pinWatcher) classify(ctx context.Context, c cid.Cid, name string) pinClass {
	if cl, ok := w.classes[c]; ok && cl.kind != pinUnknown {
		return cl
	}
	cl := pinClass{kind: pinUser}
	if c.Type() == cid.DagCBOR {
		block, err := w.fp.ipfsClient.GetBlock(ctx, c)
		if err != nil {
			slog.Debug("pin ingest: cannot read dag-cbor pin", "cid", c, "error", err)
			cl.kind = pinUnknown
		} else {
			var ro schema.ResearchObject
			if ro.UnmarshalCBOR(block) == nil && ro.Payload.Defined() {
				cl = pinClass{kind: pinManifest, payload: ro.Payload}
			}
		}
	}
	if name == ipfs.ManifestPinName {
		cl.kind = pinManifest
	}
	return cl
}

// ingestPinned creates, pins, and announces a manifest for a payload that a
// user pinned in Kubo. The user's pin is never removed. It reports whether
// the pin is now handled.
func (fp *FileProcessor) ingestPinned(ctx context.Context, payloadCID cid.Cid, name string) bool {
	payloadCIDStr := payloadCID.String()
	if !fp.claimRecentIngest(payloadCIDStr) {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, fp.cfg.Files.FileImportTimeout)
	defer cancel()

	size, err := fp.ipfsClient.DAGSize(ctx, payloadCID)
	if err != nil {
		slog.Warn("pin ingest: size unknown, recording 0", "payload", payloadCIDStr, "error", err)
	}
	metaRef := "ipfs://" + payloadCIDStr
	if name != "" {
		metaRef = name
	}
	keepPin := func() {}

	slog.Info("ingesting pinned content", "payload", payloadCIDStr, "meta_ref", metaRef, "size", size)
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, metaRef, size, payloadCID, keepPin)
	if err != nil {
		slog.Error("pin ingest: failed to build manifest", "payload", payloadCIDStr, "error", err)
		return false
	}
	if !fp.checkBadBitsAndPin(ctx, manifestCID, manifestCIDStr, metaRef, keepPin) {
		return false
	}
	fp.trackAndAnnounceFile(manifestCID, manifestCIDStr, payloadCID)
	slog.Info("pinned content ingested", "payload", payloadCIDStr, "manifest", manifestCIDStr)
	return true
}
