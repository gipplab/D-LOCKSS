// Package ingest brings new content into the network: a recursive folder
// watcher and a Kubo pin watcher feed one shared pipeline that imports,
// wraps, signs, pins, and announces files.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
	recentIngestTTL   = 30 * time.Second // payload-level dedup window
	recentIngestSweep = time.Minute
	fileImportTimeout = 2 * time.Minute
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

// validPath rejects paths outside the watch folder and temp-file suffixes.
func (p *Processor) validPath(path string) error {
	absRoot, err := filepath.Abs(p.cfg.FileWatchFolder)
	if err != nil {
		return err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path %s is outside the watch folder", path)
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".tmp", ".part", ".crdownload":
		return fmt.Errorf("temporary file")
	}
	return nil
}

// processFile imports one local file and runs the shared pipeline.
func (p *Processor) processFile(ctx context.Context, path string) error {
	if !p.network.IsLocalIngestor() {
		slog.Debug("local ingest refused: node is not an authorized ingestor", "path", path)
		return nil
	}
	if err := p.validPath(path); err != nil {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return nil // vanished or replaced by a directory: the next scan decides
	}

	ictx, cancel := context.WithTimeout(ctx, fileImportTimeout)
	payload, err := p.client.ImportFile(ictx, path)
	cancel()
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	cleanup := func() {
		uctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		_ = p.client.Unpin(uctx, payload)
	}
	return p.finishIngest(ctx, "file://"+filepath.Base(path), payload, uint64(info.Size()), cleanup)
}

// finishIngest is the shared tail of both ingest paths: dedup, manifest,
// denylist, manifest pin, then track and announce. cleanup undoes the
// payload pin of THIS path when the ingest aborts (the pin watcher passes a
// no-op: user pins are never removed). The returned error distinguishes a
// failed ingest (retryable) from a completed or deliberately skipped one.
func (p *Processor) finishIngest(ctx context.Context, metaRef string, payload cid.Cid, size uint64, cleanup func()) error {
	if !p.claimRecentIngest(payload) {
		slog.Debug("skipping duplicate ingest", "payload", payload)
		cleanup()
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
		cleanup()
		return fmt.Errorf("manifest encode: %w", err)
	}
	bctx, cancel := context.WithTimeout(ctx, time.Minute)
	manifest, err := p.client.PutDagCBOR(bctx, raw)
	cancel()
	if err != nil {
		cleanup()
		return fmt.Errorf("manifest store: %w", err)
	}

	if p.badbits.IsBlocked(manifest.String()) {
		slog.Warn("ingest refused: manifest is denylisted", "cid", manifest)
		uctx, ucancel := context.WithTimeout(ctx, time.Minute)
		_ = p.client.Unpin(uctx, manifest)
		ucancel()
		cleanup()
		return nil // deliberate refusal, not a retryable failure
	}

	pctx, cancel := context.WithTimeout(ctx, fileImportTimeout)
	err = p.client.PinManifest(pctx, manifest)
	cancel()
	if err != nil {
		cleanup()
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
