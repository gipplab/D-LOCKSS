// Package ingest brings new content into the network. Kubo's pin set is the
// single source of truth: a pin watcher wraps every user-pinned CID in a
// signed manifest and announces it; D-LOCKSS then keeps it replicated.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/internal/kubo"
	"dlockss/internal/protocol"
	"dlockss/internal/schema"
	"dlockss/internal/store"
)

const (
	recentIngestTTL    = 30 * time.Second // payload-level dedup window
	recentIngestSweep  = time.Minute
	manifestPinTimeout = 2 * time.Minute
)

// Network is the shard engine surface the ingest pipeline needs.
type Network interface {
	IsLocalIngestor() bool
	AmIResponsibleFor(key string) bool
	AnnouncePinned(k cid.Cid)
	AnnounceResponsibleFile(manifest, payload cid.Cid)
	AnnounceCustodialFile(manifest, payload cid.Cid)
}

// Processor runs the ingest pipeline.
type Processor struct {
	cfg     *config.Config
	client  *kubo.Client
	store   *store.Store
	badbits *badbits.Filter
	signer  *protocol.Signer
	network Network
	self    peer.ID

	recentMu sync.Mutex
	recent   map[string]time.Time // payload CID -> ingest time
}

// NewProcessor builds the ingest pipeline.
func NewProcessor(cfg *config.Config, client *kubo.Client, st *store.Store, bb *badbits.Filter,
	signer *protocol.Signer, network Network, self peer.ID) *Processor {
	return &Processor{
		cfg:     cfg,
		client:  client,
		store:   st,
		badbits: bb,
		signer:  signer,
		network: network,
		self:    self,
		recent:  map[string]time.Time{},
	}
}

// finishIngest wraps a payload in a signed manifest and announces it: dedup,
// manifest build + store, denylist, manifest pin, then track and announce.
// The user's payload pin is never touched. The returned error distinguishes
// a failed ingest (retryable) from a completed or deliberately skipped one.
func (p *Processor) finishIngest(ctx context.Context, metaRef string, payload cid.Cid, size uint64) error {
	if !p.claimRecentIngest(payload) {
		slog.Debug("skipping duplicate ingest", "payload", payload)
		return nil
	}

	ro := &schema.ResearchObject{
		MetadataRef: metaRef,
		IngestedBy:  p.self,
		Payload:     payload,
		TotalSize:   size,
	}
	if err := p.signer.SignManifest(ro); err != nil {
		slog.Warn("manifest signing failed; storing unsigned", "err", err)
	}
	raw, err := ro.MarshalCBOR()
	if err != nil {
		return fmt.Errorf("manifest encode: %w", err)
	}
	bctx, cancel := context.WithTimeout(ctx, time.Minute)
	manifest, err := p.client.PutDagCBOR(bctx, raw)
	cancel()
	if err != nil {
		return fmt.Errorf("manifest store: %w", err)
	}

	if p.badbits.IsBlocked(manifest.String()) {
		slog.Warn("ingest refused: manifest is denylisted", "cid", manifest)
		return nil // deliberate refusal, not a retryable failure
	}

	pctx, cancel := context.WithTimeout(ctx, manifestPinTimeout)
	err = p.client.PinManifest(pctx, manifest)
	cancel()
	if err != nil {
		return fmt.Errorf("manifest pin: %w", err)
	}

	if !p.store.Pin(manifest) {
		return nil // denylisted via the registry
	}
	p.network.AnnouncePinned(manifest)
	p.store.AddKnown(manifest)

	if p.network.AmIResponsibleFor(payload.String()) {
		p.network.AnnounceResponsibleFile(manifest, payload)
	} else {
		p.network.AnnounceCustodialFile(manifest, payload)
	}
	slog.Info("ingested", "meta", metaRef, "manifest", manifest, "payload", payload, "size", size)
	return nil
}

// claimRecentIngest dedupes payloads across the two ingest paths within a
// short window (e.g. the folder watcher and the pin watcher racing).
func (p *Processor) claimRecentIngest(payload cid.Cid) bool {
	now := time.Now()
	p.recentMu.Lock()
	defer p.recentMu.Unlock()
	for k, t := range p.recent {
		if now.Sub(t) > recentIngestSweep {
			delete(p.recent, k)
		}
	}
	key := payload.String()
	if t, ok := p.recent[key]; ok && now.Sub(t) < recentIngestTTL {
		return false
	}
	p.recent[key] = now
	return true
}
