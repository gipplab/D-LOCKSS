package fileops

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

const (
	defaultPinIngestInterval = time.Minute
	// pinIngestRetryAfter is how long a pin waits after an attempt that
	// failed, e.g. because the wiki could not be reached. A failed attempt
	// does not count as one of the signatureChecks.
	pinIngestRetryAfter = 30 * time.Minute
)

// signatureChecks are the times, after a pin was first seen, at which the
// wiki is asked for the pin's signatures (DLOCKSS_WIKI_API). A signature
// found later adds a manifest, so the checks go on after the first ingest.
// After the last one the pin is left alone until the node restarts.
var signatureChecks = []time.Duration{0, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour}

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
	// own and uploader are set for manifests: whether this node made it,
	// and the user whose signature it holds.
	own      bool
	uploader peer.ID
}

// pinState is what the watcher knows about a user pin.
type pinState struct {
	firstSeen time.Time
	// checks counts the signatureChecks done.
	checks int
	// retryAt delays the next attempt after one that failed.
	retryAt time.Time
	// ingested is set once this node made a manifest for the pin since it
	// started; only then is the pin followed after its manifest covers it.
	ingested bool
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
	// states holds the user pins seen in the previous poll, by multihash. A
	// pin is ingested on its second sighting, because an upload pins the
	// payload before its manifest exists.
	states map[string]*pinState
	now    func() time.Time
	// blocked is the reason the last poll ingested nothing, so each reason
	// is logged once rather than on every poll.
	blocked string
	scanned bool
}

func newPinWatcher(fp *FileProcessor) *pinWatcher {
	return &pinWatcher{
		fp:      fp,
		classes: make(map[cid.Cid]pinClass),
		states:  make(map[string]*pinState),
		now:     time.Now,
	}
}

// WatchPins polls Kubo's recursive pins and ingests those created by users.
func (fp *FileProcessor) WatchPins(ctx context.Context) {
	interval := fp.cfg.Files.PinIngestInterval
	if interval <= 0 {
		interval = defaultPinIngestInterval
	}
	slog.Info("watching kubo pins for ingest", "interval", interval, "signed_only", fp.signatures != nil)
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
	// signed holds, per payload, the users whose signatures this node's
	// manifests already hold.
	signed := make(map[string]map[peer.ID]bool)
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
			if !cl.payload.Defined() {
				break
			}
			payload := string(cl.payload.Hash())
			covered[payload] = struct{}{}
			if cl.own && cl.uploader != "" {
				if signed[payload] == nil {
					signed[payload] = make(map[peer.ID]bool)
				}
				signed[payload][cl.uploader] = true
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
	states := make(map[string]*pinState)
	ingested := 0
	for c, cl := range classes {
		key := string(c.Hash())
		if cl.kind != pinUser || key == string(emptyDir.Hash()) {
			continue
		}
		st, seen := w.states[key]
		// A covered pin is followed only while this node adds manifests for
		// it: one ingested before the node started, or the payload of a
		// replica, is left alone.
		if _, ok := covered[key]; ok && (!seen || !st.ingested) {
			continue
		}
		if !seen {
			st = &pinState{firstSeen: now}
			states[key] = st
			slog.Info("pin ingest: new pin found, ingesting on the next poll", "cid", c, "name", pins[c])
			continue
		}
		states[key] = st
		if !st.due(now) {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		made, err := w.fp.ingestPinned(ctx, c, pins[c], signed[key])
		ingested += made
		if made > 0 {
			st.ingested = true
		}
		if err != nil {
			st.retryAt = now.Add(pinIngestRetryAfter)
			continue
		}
		st.checks++
		if w.fp.signatures == nil {
			// Without signatures there is nothing more to learn.
			st.checks = len(signatureChecks)
		} else if st.checks == len(signatureChecks) && !st.ingested {
			slog.Info("pin ingest: nobody signed the file in the wiki; asking again after a restart", "cid", c, "name", pins[c])
		}
	}
	w.states = states
	if ingested > 0 {
		slog.Info("pin ingest pass complete", "manifests", ingested, "pins", len(pins))
	}
}

// due reports whether the pin's next signature check is due.
func (st *pinState) due(now time.Time) bool {
	if st.checks >= len(signatureChecks) || now.Before(st.retryAt) {
		return false
	}
	return !now.Before(st.firstSeen.Add(signatureChecks[st.checks]))
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
				cl = pinClass{kind: pinManifest, payload: ro.Payload,
					own: ro.IngestedBy == w.fp.shardMgr.NodeID(), uploader: ro.Uploader}
			}
		}
	}
	if name == ipfs.ManifestPinName {
		cl.kind = pinManifest
	}
	return cl
}

// ingestPinned creates, pins, and announces manifests for a payload that a
// user pinned in Kubo. With a signature source (DLOCKSS_WIKI_API) it makes
// one per valid signature whose key is not in have, and none for a file
// that nobody signed; without one, a single manifest without a signature.
// The user's pin is never removed. It returns how many manifests it made;
// an error means the attempt should be repeated later.
func (fp *FileProcessor) ingestPinned(ctx context.Context, payloadCID cid.Cid, name string, have map[peer.ID]bool) (int, error) {
	payloadCIDStr := payloadCID.String()
	sigs := []schema.UploaderSignature{{}}
	if fp.signatures != nil {
		all, err := fp.signatures.Signatures(ctx, payloadCID)
		if err != nil {
			slog.Warn("pin ingest: cannot fetch the signatures, retrying later", "payload", payloadCIDStr, "error", err, "retry_after", pinIngestRetryAfter)
			return 0, err
		}
		sigs = sigs[:0]
		for _, s := range all {
			if !have[s.Key] {
				sigs = append(sigs, s)
			}
		}
		if len(all) == 0 {
			slog.Info("pin ingest: nobody signed the file in the wiki yet", "payload", payloadCIDStr, "name", name)
		}
	}
	if len(sigs) == 0 {
		return 0, nil
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

	made := 0
	for _, s := range sigs {
		slog.Info("ingesting pinned content", "payload", payloadCIDStr, "meta_ref", metaRef, "size", size, "uploader", s.Key)
		// The user's pin stays, so there is nothing to clean up.
		manifestCID, err := fp.ingestManifest(ctx, metaRef, size, payloadCID, s, func() {})
		if err != nil {
			slog.Error("pin ingest: failed", "payload", payloadCIDStr, "error", err)
			return made, err
		}
		slog.Info("pinned content ingested", "payload", payloadCIDStr, "manifest", manifestCID, "uploader", s.Key)
		made++
	}
	return made, nil
}
