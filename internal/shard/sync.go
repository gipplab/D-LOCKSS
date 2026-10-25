package shard

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/internal/pinset"
)

// payloadFetchTimeout is the budget for pulling a payload DAG on first pin
// sync. The manifest references the payload as a plain string, not an IPLD
// link, so pinning the manifest never fetches the data — this is the main
// path that does, and it must tolerate large files.
const payloadFetchTimeout = 5 * time.Minute

// OnPinSynced handles a manifest the pinset tracker just synced locally:
// register it, announce PINNED immediately (so monitors see the replica
// before the next heartbeat), provide the manifest, then fetch/pin the
// payload as its own root and provide it too. Wired to pinset.Manager.OnSynced.
func (m *Manager) OnPinSynced(k cid.Cid) {
	if !m.store.Pin(k) {
		return // denylisted
	}
	m.AnnouncePinned(k)
	m.track(func() {
		ctx, cancel := context.WithTimeout(m.ctx, dhtProvideTimeout)
		defer cancel()
		m.store.Provide(ctx, k)
	})
	m.track(func() {
		// Bound concurrent payload fetches: a burst of syncs (e.g. a fresh
		// node joining a full shard) must not open unbounded downloads.
		select {
		case m.payloadSem <- struct{}{}:
		case <-m.ctx.Done():
			return
		}
		defer func() { <-m.payloadSem }()
		m.pinAndProvidePayload(k)
	})
}

// pinAndProvidePayload resolves the manifest's payload, pins it recursively
// as its own root (so Kubo's "pinned" reprovide strategy covers it), checks
// the manifest's size claim (liar detection), and provides the payload.
func (m *Manager) pinAndProvidePayload(manifest cid.Cid) {
	rctx, rcancel := context.WithTimeout(m.ctx, manifestReadTimeout)
	ro, err := pinset.ReadManifest(rctx, m.client, manifest)
	rcancel()
	if err != nil || !ro.Payload.Defined() {
		return
	}

	pctx, pcancel := context.WithTimeout(m.ctx, payloadFetchTimeout)
	err = m.client.PinRecursive(pctx, ro.Payload, "")
	pcancel()
	if err != nil {
		slog.Debug("payload pin failed; the reprovide loop retries", "cid", ro.Payload, "err", err)
		return
	}

	if !m.verifyPayloadSize(manifest, ro.Payload, ro.TotalSize) {
		return
	}

	vctx, vcancel := context.WithTimeout(m.ctx, dhtProvideTimeout)
	defer vcancel()
	m.store.Provide(vctx, ro.Payload)
}

// verifyPayloadSize enforces liar detection: a payload whose actual size
// contradicts the manifest's claim is unpinned and dropped. Claimed sizes of
// 0 are exempt — the pin-ingest path records 0 when the size lookup fails.
// It reports whether the file may be kept.
func (m *Manager) verifyPayloadSize(manifest, payload cid.Cid, claimed uint64) bool {
	if claimed == 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(m.ctx, time.Minute)
	defer cancel()
	actual, err := m.client.DAGSize(ctx, payload)
	if err != nil || actual == claimed {
		return true // unverifiable counts as OK; a later pass rechecks
	}
	slog.Warn("liar detection: payload size contradicts the manifest, unpinning",
		"manifest", manifest, "claimed", claimed, "actual", actual)
	m.unpinEverywhere(manifest)
	return false
}
