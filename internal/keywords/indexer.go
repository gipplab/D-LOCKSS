package keywords

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"
)

const (
	pickInterval      = time.Second
	indexWorkers      = 2
	saiaDailyLimit    = 20000
	googleDailyLimit  = 1000
	llmMaxRetries     = 5
	llmBaseBackoff    = 2 * time.Second
	llmBackoffCap     = time.Minute
	llmWaitCap        = 5 * time.Minute
	maxRateLimitPause = 5 * time.Minute
	gatewayPauseMin   = time.Minute
	networkRetries    = 3
	networkRetryWait  = 20 * time.Second
)

// Indexer schedules keyword extraction over the monitor's known CIDs.
type Indexer struct {
	Store    *Store
	Settings *Settings
	universe func() []string // current manifest CIDs, any order

	mu         sync.Mutex
	client     *llmClient
	keyReady   chan struct{}
	readyOnce  sync.Once
	inflight   map[string]struct{}
	pauseUntil time.Time
	dailyCount int
	dailyDay   string // UTC date of dailyCount
}

// NewIndexer wires the indexer; universe supplies the CID population.
func NewIndexer(store *Store, settings *Settings, universe func() []string) *Indexer {
	ix := &Indexer{
		Store:    store,
		Settings: settings,
		universe: universe,
		keyReady: make(chan struct{}),
		inflight: map[string]struct{}{},
	}
	if settings.HasKey() {
		ix.client = newLLMClient(settings)
		ix.signalReady()
	}
	return ix
}

func (ix *Indexer) signalReady() { ix.readyOnce.Do(func() { close(ix.keyReady) }) }

// ApplySettings validates and saves new settings, clears recorded failures
// and pauses, and (re)builds the LLM client.
func (ix *Indexer) ApplySettings(password, provider, apiKey, apiBase, gateway string) *ApplyError {
	if err := ix.Settings.Apply(password, provider, apiKey, apiBase, gateway); err != nil {
		return err
	}
	ix.Store.ResetFailures()
	ix.mu.Lock()
	ix.pauseUntil = time.Time{}
	ix.client = newLLMClient(ix.Settings)
	ix.mu.Unlock()
	ix.signalReady()
	return nil
}

// DailyQuota returns (used, limit) for today.
func (ix *Indexer) DailyQuota() (int, int) {
	limit := saiaDailyLimit
	if p, _, _, _, _ := ix.Settings.Snapshot(); p == ProviderGoogle {
		limit = googleDailyLimit
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.rollDayLocked()
	return ix.dailyCount, limit
}

func (ix *Indexer) rollDayLocked() {
	today := time.Now().UTC().Format("2006-01-02")
	if ix.dailyDay != today {
		ix.dailyDay = today
		ix.dailyCount = 0
	}
}

// Run blocks until an API key exists, then schedules extraction until ctx
// ends: one pick per second feeding a small worker pool, bounded by the
// provider's daily quota and any active pause.
func (ix *Indexer) Run(ctx context.Context) {
	select {
	case <-ix.keyReady:
	case <-ctx.Done():
		return
	}
	if !PDFSupported() {
		slog.Warn("pdftotext not found on PATH: PDFs will be skipped")
	}
	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < indexWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				ix.processCID(ctx, c)
				ix.mu.Lock()
				delete(ix.inflight, c)
				ix.mu.Unlock()
			}
		}()
	}
	ticker := time.NewTicker(pickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case <-ticker.C:
			if c, ok := ix.pickNext(); ok {
				select {
				case jobs <- c:
				case <-ctx.Done():
				default:
					// workers busy: release the claim, try next tick
					ix.mu.Lock()
					delete(ix.inflight, c)
					ix.mu.Unlock()
				}
			}
		}
	}
}

// pickNext selects the next eligible CID (alphabetical order) and claims it.
func (ix *Indexer) pickNext() (string, bool) {
	ix.mu.Lock()
	ix.rollDayLocked()
	limit := saiaDailyLimit
	if p, _, _, _, _ := ix.Settings.Snapshot(); p == ProviderGoogle {
		limit = googleDailyLimit
	}
	if time.Now().Before(ix.pauseUntil) || ix.dailyCount >= limit {
		ix.mu.Unlock()
		return "", false
	}
	ix.mu.Unlock()

	cids := ix.universe()
	sort.Strings(cids)
	for _, c := range cids {
		ix.mu.Lock()
		if _, busy := ix.inflight[c]; busy {
			ix.mu.Unlock()
			continue
		}
		if !ix.Store.eligible(c) {
			ix.mu.Unlock()
			continue
		}
		ix.inflight[c] = struct{}{}
		ix.mu.Unlock()
		return c, true
	}
	return "", false
}

// pause suspends picking; a longer existing pause is kept.
func (ix *Indexer) pause(d time.Duration) {
	if d < gatewayPauseMin {
		d = gatewayPauseMin
	}
	if d > maxRateLimitPause {
		d = maxRateLimitPause
	}
	until := time.Now().Add(d)
	ix.mu.Lock()
	if until.After(ix.pauseUntil) {
		ix.pauseUntil = until
	}
	ix.mu.Unlock()
	slog.Info("keyword indexer paused", "for", d)
}

func (ix *Indexer) processCID(ctx context.Context, manifestCID string) {
	ix.mu.Lock()
	paused := time.Now().Before(ix.pauseUntil)
	client := ix.client
	ix.mu.Unlock()
	if paused || client == nil {
		return
	}
	gw := ix.Settings.Gateway()

	ro, err := FetchManifest(ctx, gw, manifestCID)
	if err != nil {
		ix.handleFetchError(manifestCID, err)
		return
	}
	payload := ro.Payload
	if !payload.Defined() {
		ix.Store.MarkSkipped(manifestCID)
		return
	}
	doc, err := LoadContent(ctx, gw, payload, ro.MetadataRef)
	if err != nil {
		if IsUnsupported(err) {
			ix.Store.MarkSkipped(manifestCID)
			return
		}
		ix.handleFetchError(manifestCID, err)
		return
	}
	text := doc.Text
	if limit := client.textLimit(); len(text) > limit {
		text = text[:limit]
	}

	ex, err := ix.extractWithRetries(ctx, client, doc.Name, doc.Format, text)
	if err != nil {
		var rl *rateLimitError
		if errors.As(err, &rl) {
			ix.pause(maxRateLimitPause) // stays pending, not a failure
			return
		}
		slog.Debug("keyword extraction failed", "cid", manifestCID, "err", err)
		ix.Store.RecordFailure(manifestCID)
		return
	}

	ix.Store.Put(&Entry{
		ManifestCID:   manifestCID,
		PayloadCID:    payload.String(),
		MetaRef:       ro.MetadataRef,
		Title:         ex.Title,
		BroadField:    ex.BroadField,
		SubTopic:      ex.SubTopic,
		ResearchNiche: ex.ResearchNiche,
		Keywords:      ex.Keywords,
		IndexedAt:     time.Now().Unix(),
	})
	ix.mu.Lock()
	ix.rollDayLocked()
	ix.dailyCount++
	ix.mu.Unlock()
	slog.Info("indexed", "cid", manifestCID, "title", ex.Title, "keywords", len(ex.Keywords))
}

// handleFetchError pauses on gateway 429s and records other errors as
// failures.
func (ix *Indexer) handleFetchError(cid string, err error) {
	var gw *gatewayStatusError
	if errors.As(err, &gw) && gw.status == 429 {
		d := gw.retryAfter
		if d <= 0 {
			d = time.Second
		}
		ix.pause(d) // clamped to [1m, 5m]; the CID stays pending
		return
	}
	slog.Debug("keyword fetch failed", "cid", cid, "err", err)
	ix.Store.RecordFailure(cid)
}

// extractWithRetries retries LLM 429s with exponential backoff and network
// errors with flat waits.
func (ix *Indexer) extractWithRetries(ctx context.Context, client *llmClient, name, format, text string) (*Extraction, error) {
	netTries := 0
	var lastErr error
	for attempt := 0; attempt <= llmMaxRetries; attempt++ {
		ex, err := client.Extract(ctx, name, format, text)
		if err == nil {
			return ex, nil
		}
		lastErr = err
		var rl *rateLimitError
		switch {
		case errors.As(err, &rl):
			if attempt == llmMaxRetries {
				return nil, err
			}
			wait := llmBaseBackoff << attempt
			if wait > llmBackoffCap {
				wait = llmBackoffCap
			}
			if rl.retryAfter > wait {
				wait = rl.retryAfter
			}
			if wait > llmWaitCap {
				wait = llmWaitCap
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case isNetworkError(err) && netTries < networkRetries:
			netTries++
			select {
			case <-time.After(networkRetryWait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			return nil, err
		}
	}
	return nil, lastErr
}

// PayloadCIDOf returns the indexed payload CID for a manifest, if known.
func (ix *Indexer) PayloadCIDOf(manifestCID string) (string, bool) {
	if e, ok := ix.Store.Get(manifestCID); ok && e.PayloadCID != "" {
		return e.PayloadCID, true
	}
	return "", false
}
