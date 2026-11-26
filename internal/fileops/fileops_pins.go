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
	// pending holds the user pins seen in the previous poll, with the time
	// each was first seen. A pin is ingested on its second sighting, because
	// an ingest from the watch folder pins the payload before its manifest
	// exists, and not before PinIngestDelay has passed, so the wiki can
	// store its users' signatures first.
	pending map[string]time.Time
	now     func() time.Time
	// failed holds the time of the last failed ingest per pin.
	failed map[string]time.Time
	// blocked is the reason the last poll ingested nothing, so each reason
	// is logged once rather than on every poll.
	blocked string
	scanned bool
}

func newPinWatcher(fp *FileProcessor) *pinWatcher {
	return &pinWatcher{
		fp:      fp,
		classes: make(map[cid.Cid]pinClass),
		pending: make(map[string]time.Time),
		failed:  make(map[string]time.Time),
		now:     time.Now,
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

func (w *pinWatcher) setBlocked(reason string, args ...any) {
	if reason == w.blocked {
		return
	}
	w.blocked = reason
	if reason != "" {
		slog.Warn("pin ingest: "+reason, args...)
	}
}

func (w *pinWatcher) poll(ctx context.Context) {
	if !w.fp.shardMgr.IsLocalNodeIngestor() {
		w.setBlocked("this node is not on the ingest allowlist; pins are not ingested", "node", w.fp.shardMgr.NodeID())
		return
	}
	pins, err := w.fp.ipfsClient.RecursivePins(ctx)
	if err != nil {
		w.setBlocked("listing pins failed", "error", err)
		return
	}

	// Keys are multihashes, so a CIDv0 user pin matches the CIDv1 payload
	// of a manifest.
	start := time.Now()
	covered := make(map[string]struct{})
	classes := make(map[cid.Cid]pinClass, len(pins))
	var unknown []cid.Cid
	manifests := 0
	for c, name := range pins {
		cl := w.classify(ctx, c, name)
		classes[c] = cl
		switch cl.kind {
		case pinManifest:
			manifests++
			covered[string(c.Hash())] = struct{}{}
			if cl.payload.Defined() {
				covered[string(cl.payload.Hash())] = struct{}{}
			}
		case pinUnknown:
			unknown = append(unknown, c)
		}
	}
	w.classes = classes
	if !w.scanned {
		w.scanned = true
		slog.Info("pin ingest: scanned kubo pins", "pins", len(pins), "manifests", manifests,
			"unreadable", len(unknown), "took", time.Since(start).Round(time.Millisecond))
	}
	// An unreadable pin may be a manifest whose payload would then look like
	// a user pin.
	if len(unknown) > 0 {
		w.setBlocked("pins that cannot be read block ingest until they can", "count", len(unknown), "example", unknown[0])
		return
	}
	w.setBlocked("")

	now := w.now()
	delay := w.fp.cfg.Files.PinIngestDelay
	pending := make(map[string]time.Time)
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
		firstSeen, seen := w.pending[key]
		if !seen {
			firstSeen = now
		}
		pending[key] = firstSeen
		if t, ok := w.failed[key]; ok && now.Sub(t) < pinIngestRetryAfter {
			failed[key] = t
			continue
		}
		if !seen {
			slog.Info("pin ingest: new pin found", "cid", c, "name", pins[c], "ingest_after", delay)
			continue
		}
		if now.Sub(firstSeen) < delay {
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
	// Before the claim, so a file that may not be ingested yet is retried.
	uploaders, ok := fp.uploadersOf(ctx, payloadCID)
	if !ok {
		return false
	}
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

	slog.Info("ingesting pinned content", "payload", payloadCIDStr, "meta_ref", metaRef, "size", size, "uploaders", len(uploaders))
	manifestCID, manifestCIDStr, err := fp.buildAndStoreManifest(ctx, metaRef, size, payloadCID, uploaders, keepPin)
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

// uploadersOf returns the users' signatures of payload and whether a
// manifest may be made. With a signature source (DLOCKSS_WIKI_API), it may
// only if a signature verifies, unless AllowUnsigned is set; then a file
// whose signatures cannot be fetched is ingested without them.
func (fp *FileProcessor) uploadersOf(ctx context.Context, payload cid.Cid) ([]schema.UploaderSignature, bool) {
	if fp.signatures == nil {
		return nil, true
	}
	uploaders, err := fp.signatures.Signatures(ctx, payload)
	switch {
	case err != nil && fp.cfg.AllowUnsigned:
		slog.Warn("pin ingest: cannot fetch the uploaders' signatures, ingesting unsigned", "payload", payload, "error", err)
		return nil, true
	case err != nil:
		slog.Warn("pin ingest: cannot fetch the uploaders' signatures, retrying later", "payload", payload, "error", err, "retry_after", pinIngestRetryAfter)
		return nil, false
	case len(uploaders) == 0 && !fp.cfg.AllowUnsigned:
		slog.Info("pin ingest: no valid signature in the wiki, not ingested yet", "payload", payload, "retry_after", pinIngestRetryAfter)
		return nil, false
	}
	return uploaders, true
}
