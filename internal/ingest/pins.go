package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/kubo"
	"dlockss/internal/schema"
)

const (
	pinIngestInterval   = time.Minute
	pinIngestRetryAfter = 30 * time.Minute
	// emptyDirCID is Kubo's canonical empty directory, pinned by default in
	// every fresh repo; never a user intent to preserve.
	emptyDirCID = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn"
)

type pinKind int

const (
	pinUnknown pinKind = iota // could not be read yet; retried next poll
	pinManifest                // a D-LOCKSS manifest (by content or pin name)
	pinUser                    // user content, candidate for ingestion
)

// pinWatcher ingests recursive pins users create in Kubo (CLI, WebUI, IPFS
// Desktop) — the network's only ingest path. A pinned directory becomes one
// research object. D-LOCKSS's own manifest pins and the payloads they cover
// are never ingested, and a pin is only ingested on its second consecutive
// sighting so a pin whose covering manifest is still landing (ingest and
// replication pin payloads moments after their manifests) is not raced.
type pinWatcher struct {
	p *Processor

	kinds    map[cid.Cid]pinKind
	payloads map[cid.Cid]string // manifest -> payload multihash key
	pending  map[cid.Cid]int    // sighting count of user pins
	failed   map[cid.Cid]time.Time
}

// WatchPins polls Kubo's recursive pins until ctx ends. The first poll runs
// immediately.
func (p *Processor) WatchPins(ctx context.Context) {
	w := &pinWatcher{
		p:        p,
		kinds:    map[cid.Cid]pinKind{},
		payloads: map[cid.Cid]string{},
		pending:  map[cid.Cid]int{},
		failed:   map[cid.Cid]time.Time{},
	}
	ticker := time.NewTicker(pinIngestInterval)
	defer ticker.Stop()
	w.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

func (w *pinWatcher) poll(ctx context.Context) {
	if !w.p.network.IsLocalIngestor() {
		slog.Debug("pin ingest: node is not an authorized ingestor")
		return
	}
	lctx, cancel := context.WithTimeout(ctx, time.Minute)
	pins, err := w.p.client.RecursivePins(lctx)
	cancel()
	if err != nil {
		slog.Warn("pin ingest: listing pins failed", "err", err)
		return
	}

	// Classify every pin; covered payloads are compared by multihash so a
	// CIDv0 user pin matches the CIDv1 payload a manifest references.
	covered := map[string]struct{}{}
	anyUnknown := false
	for k, name := range pins {
		kind := w.classify(ctx, k, name)
		switch kind {
		case pinManifest:
			covered[string(k.Hash())] = struct{}{}
			if ph, ok := w.payloads[k]; ok {
				covered[ph] = struct{}{}
			}
		case pinUnknown:
			anyUnknown = true
		}
	}
	if anyUnknown {
		// An unreadable manifest might cover a payload we would otherwise
		// ingest a second time: wait for a clean pass.
		slog.Debug("pin ingest: unreadable pins, deferring the pass")
		return
	}

	// Drop state for pins that disappeared.
	for k := range w.pending {
		if _, ok := pins[k]; !ok {
			delete(w.pending, k)
		}
	}
	for k := range w.failed {
		if _, ok := pins[k]; !ok {
			delete(w.failed, k)
		}
	}

	for k, name := range pins {
		if w.kinds[k] != pinUser || k.String() == emptyDirCID {
			continue
		}
		if _, ok := covered[string(k.Hash())]; ok {
			continue
		}
		if t, ok := w.failed[k]; ok && time.Since(t) < pinIngestRetryAfter {
			continue
		}
		if w.pending[k]++; w.pending[k] < 2 {
			continue // two-sighting rule
		}
		delete(w.pending, k)
		if err := w.ingest(ctx, k, name); err != nil {
			slog.Warn("pin ingest failed", "cid", k, "err", err)
			w.failed[k] = time.Now()
		} else {
			delete(w.failed, k)
		}
	}
}

// classify decides what a pin is, caching results; only unknowns retry.
// Every dag-cbor pin is read — including this node's own dlockss:manifest
// pins — because the payload a manifest covers is exactly what keeps that
// payload's (unnamed) pin from being re-ingested as new user content.
func (w *pinWatcher) classify(ctx context.Context, k cid.Cid, name string) pinKind {
	if kind, ok := w.kinds[k]; ok && kind != pinUnknown {
		return kind
	}
	if k.Prefix().Codec == cid.DagCBOR {
		bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		block, err := w.p.client.GetBlock(bctx, k)
		cancel()
		if err != nil {
			w.kinds[k] = pinUnknown // retried; an unknown blocks the pass
			return pinUnknown
		}
		var ro schema.ResearchObject
		if ro.UnmarshalCBOR(block) == nil && ro.Payload.Defined() {
			w.payloads[k] = string(ro.Payload.Hash())
			w.kinds[k] = pinManifest
			return pinManifest
		}
	}
	kind := pinUser
	if name == kubo.ManifestPinName {
		kind = pinManifest // named but not a readable manifest: still never ingest
	}
	w.kinds[k] = kind
	return kind
}

// ingest wraps one user pin (file or directory) as a research object. The
// user's pin is never removed, even when the ingest aborts.
func (w *pinWatcher) ingest(ctx context.Context, payload cid.Cid, pinName string) error {
	metaRef := pinName
	if metaRef == "" {
		metaRef = "ipfs://" + payload.String()
	}
	var size uint64
	sctx, cancel := context.WithTimeout(ctx, time.Minute)
	if s, err := w.p.client.DAGSize(sctx, payload); err == nil {
		size = s
	} else {
		slog.Debug("pin ingest: size lookup failed, recording 0", "cid", payload, "err", err)
	}
	cancel()

	return w.p.finishIngest(ctx, metaRef, payload, size)
}
