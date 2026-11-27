package keywords

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dlockss/internal/common"

	"golang.org/x/time/rate"
)

const (
	indexFileName    = "keyword_index.json"
	failuresFileName = "keyword_failures.json"

	numWorkers      = 2
	requestSpacing  = time.Second
	saiaDailyCap    = 20_000
	googleDailyCap  = 1_000
	maxRetries      = 3
	retryCooldown   = 10 * time.Minute
	batchRetryDelay = time.Minute // shortest indexer pause after a rate limit
)

// CIDSource is the set of known manifest CIDs (implemented by the monitor).
type CIDSource interface {
	UniqueCIDList() []string
	// PayloadOf returns the payload CID of a manifest, if known. A payload
	// is indexed once, whichever of its manifests comes first.
	PayloadOf(manifestCID string) (string, bool)
}

// Store indexes documents and datasets announced on the network and answers
// keyword searches.
type Store struct {
	mu sync.RWMutex

	cidKeywords map[string]*CIDKeywordEntry
	keywordCIDs map[string]map[string]struct{}
	processed   map[string]bool
	inflight    map[string]bool
	failures    map[string]*failureRecord
	// donePayloads holds the payloads that are indexed or cannot be.
	donePayloads map[string]bool

	dailyCount int
	dayStart   time.Time

	cfg        Config
	keyReady   chan struct{}
	keyOnce    sync.Once
	llmLimiter *rate.Limiter
	pauseUntil time.Time
	// pdfToText is set once by Run before the workers start.
	pdfToText bool
}

type CIDKeywordEntry struct {
	ManifestCID   string    `json:"manifest_cid"`
	PayloadCID    string    `json:"payload_cid"`
	MetaRef       string    `json:"meta_ref"`
	Title         string    `json:"title"`
	BroadField    string    `json:"broad_field"`
	SubTopic      string    `json:"sub_topic"`
	ResearchNiche string    `json:"research_niche"`
	Keywords      []string  `json:"keywords"`
	IndexedAt     time.Time `json:"indexed_at"`
}

type failureRecord struct {
	Count   int       `json:"count"`
	LastTry time.Time `json:"last_try"`
}

type KeywordSuggestion struct {
	Keyword  string `json:"keyword"`
	CIDCount int    `json:"cid_count"`
}

// Stats reports the indexer settings and today's request quota.
type Stats struct {
	DailyRemaining int  `json:"daily_remaining"`
	DailyLimit     int  `json:"daily_limit"`
	Enabled        bool `json:"enabled"`
	// SettingsEnabled reports that the dashboard may change the settings
	// (DLOCKSS_MONITOR_PASSWORD is set).
	SettingsEnabled bool   `json:"settings_enabled"`
	Provider        string `json:"provider"`
	Model           string `json:"model"`
	Gateway         string `json:"gateway"`
}

func NewStore(cfg Config) *Store {
	if cfg.DataDir != "" {
		if saved, ok := loadSettings(cfg.DataDir); ok {
			if cfg.APIKey == "" {
				cfg.APIKey = saved.APIKey
			}
			if cfg.Provider == "" {
				cfg.Provider = saved.Provider
			}
			if cfg.APIBase == "" {
				cfg.APIBase = saved.APIBase
			}
			if cfg.Model == "" {
				cfg.Model = saved.Model
			}
			if cfg.Gateway == "" {
				cfg.Gateway = saved.Gateway
			}
		} else if cfg.APIKey == "" {
			cfg.APIKey = LoadAPIKey(cfg.DataDir)
		}
	}
	cfg = normalizeConfig(cfg)
	s := &Store{
		cidKeywords:  make(map[string]*CIDKeywordEntry),
		keywordCIDs:  make(map[string]map[string]struct{}),
		processed:    make(map[string]bool),
		inflight:     make(map[string]bool),
		failures:     make(map[string]*failureRecord),
		donePayloads: make(map[string]bool),
		dayStart:     startOfDay(time.Now()),
		cfg:          cfg,
		keyReady:     make(chan struct{}),
		llmLimiter:   newLLMLimiter(cfg.Provider),
	}
	if cfg.APIKey != "" {
		s.signalKeyReady()
	}
	s.loadIndex()
	s.loadFailures()
	return s
}

func (s *Store) Enabled() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.APIKey != ""
}

func (s *Store) signalKeyReady() {
	s.keyOnce.Do(func() { close(s.keyReady) })
}

// requestCap returns the provider's daily request limit. The caller holds s.mu.
func (s *Store) requestCap() int {
	if s.cfg.Provider == ProviderGoogle {
		return googleDailyCap
	}
	return saiaDailyCap
}

func (s *Store) snapshotConfig() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

func (s *Store) waitLLM() {
	s.mu.RLock()
	lim := s.llmLimiter
	s.mu.RUnlock()
	if lim != nil {
		_ = lim.Wait(context.Background())
	}
}

func (s *Store) indexPath() string {
	return filepath.Join(s.cfg.DataDir, indexFileName)
}

func (s *Store) failuresPath() string {
	return filepath.Join(s.cfg.DataDir, failuresFileName)
}

func (s *Store) loadIndex() {
	if s.cfg.DataDir == "" {
		return
	}
	data, err := os.ReadFile(s.indexPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read keyword index", "path", s.indexPath(), "error", err)
		}
		return
	}
	var entries map[string]*CIDKeywordEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		slog.Warn("failed to parse keyword index", "path", s.indexPath(), "error", err)
		return
	}
	for cid, entry := range entries {
		s.cidKeywords[cid] = entry
		s.processed[cid] = true
		if entry.PayloadCID != "" {
			s.donePayloads[entry.PayloadCID] = true
		}
		s.indexLabels(cid, entry)
	}
	slog.Info("loaded keyword index", "entries", len(entries), "keywords", len(s.keywordCIDs))
}

func (s *Store) saveIndex() {
	if s.cfg.DataDir == "" {
		return
	}
	if err := os.MkdirAll(s.cfg.DataDir, 0o700); err != nil {
		slog.Error("keyword index dir", "error", err)
		return
	}
	s.mu.RLock()
	data, err := json.Marshal(s.cidKeywords)
	s.mu.RUnlock()
	if err != nil {
		slog.Error("marshal keyword index", "error", err)
		return
	}
	atomicWrite(s.indexPath(), data)
}

func (s *Store) loadFailures() {
	if s.cfg.DataDir == "" {
		return
	}
	data, err := os.ReadFile(s.failuresPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read keyword failures", "path", s.failuresPath(), "error", err)
		}
		return
	}
	var entries map[string]*failureRecord
	if err := json.Unmarshal(data, &entries); err != nil {
		slog.Warn("failed to parse keyword failures", "error", err)
		return
	}
	for cid, f := range entries {
		s.failures[cid] = f
	}
}

func (s *Store) saveFailures() {
	if s.cfg.DataDir == "" {
		return
	}
	permanent := make(map[string]*failureRecord)
	for cid, f := range s.failures {
		if f.Count >= maxRetries {
			permanent[cid] = f
		}
	}
	if len(permanent) == 0 {
		return
	}
	if err := os.MkdirAll(s.cfg.DataDir, 0o700); err != nil {
		return
	}
	data, err := json.Marshal(permanent)
	if err != nil {
		return
	}
	atomicWrite(s.failuresPath(), data)
}

// resetFailuresLocked forgets all failures, including those saved as
// permanent, so files that failed under the previous provider or key are
// tried again. The caller holds s.mu.
func (s *Store) resetFailuresLocked() {
	s.failures = make(map[string]*failureRecord)
	if s.cfg.DataDir == "" {
		return
	}
	if err := os.Remove(s.failuresPath()); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove keyword failures", "path", s.failuresPath(), "error", err)
	}
}

func atomicWrite(path string, data []byte) {
	if err := common.WriteFileAtomic(path, data); err != nil {
		slog.Error("write file", "path", path, "error", err)
	}
}

func (s *Store) indexLabels(manifestCID string, entry *CIDKeywordEntry) {
	for _, kw := range entryLabels(entry) {
		if s.keywordCIDs[kw] == nil {
			s.keywordCIDs[kw] = make(map[string]struct{})
		}
		s.keywordCIDs[kw][manifestCID] = struct{}{}
	}
}

func entryLabels(entry *CIDKeywordEntry) []string {
	labels := make([]string, 0, len(entry.Keywords)+3)
	for _, kw := range entry.Keywords {
		if kw = NormalizeLabel(kw); kw != "" {
			labels = append(labels, kw)
		}
	}
	for _, field := range []string{entry.BroadField, entry.SubTopic, entry.ResearchNiche} {
		if kw := NormalizeLabel(field); kw != "" {
			labels = append(labels, kw)
		}
	}
	return labels
}

// NormalizeLabel folds a keyword or classification label for matching.
func NormalizeLabel(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// Run waits for an API key if none is set, then indexes newly announced content.
func (s *Store) Run(ctx context.Context, source CIDSource) {
	if !s.Enabled() {
		slog.Info("no API key yet; set it in the monitor settings or via SAIA_API_KEY / .api_key")
		select {
		case <-ctx.Done():
			return
		case <-s.keyReady:
		}
		if !s.Enabled() {
			return
		}
	}
	if _, err := exec.LookPath("pdftotext"); err == nil {
		s.pdfToText = true
	} else {
		slog.Warn("pdftotext not found; PDFs are not indexed (install poppler-utils)")
	}
	cfg := s.snapshotConfig()
	slog.Info("keyword indexing enabled", "provider", cfg.Provider, "model", cfg.Model, "api_base", cfg.APIBase, "workers", numWorkers)

	work := make(chan string, numWorkers)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cid := range work {
				s.processCID(cid)
			}
		}()
	}

	ticker := time.NewTicker(requestSpacing)
	defer ticker.Stop()
	defer func() { close(work); wg.Wait() }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cid := s.pickNextCID(source)
			if cid == "" {
				continue
			}
			select {
			case work <- cid:
			case <-ctx.Done():
				return
			}
		}
	}
}

// pickNextCID returns the next manifest to index, skipping files that are
// indexed through another manifest. The source's lock is taken while s.mu
// is held, never the other way round.
func (s *Store) pickNextCID(source CIDSource) string {
	candidates := source.UniqueCIDList()
	sort.Strings(candidates)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetDayIfNeeded()
	now := time.Now()
	if s.dailyCount >= s.requestCap() || now.Before(s.pauseUntil) {
		return ""
	}
	for _, c := range candidates {
		if s.processed[c] || s.inflight[c] {
			continue
		}
		if f, ok := s.failures[c]; ok && (f.Count >= maxRetries || now.Sub(f.LastTry) < retryCooldown) {
			continue
		}
		if p, ok := source.PayloadOf(c); ok && s.donePayloads[p] {
			continue
		}
		s.inflight[c] = true
		return c
	}
	return ""
}

func (s *Store) clearInflight(manifestCID string) {
	s.mu.Lock()
	delete(s.inflight, manifestCID)
	s.mu.Unlock()
}

func (s *Store) recordFailure(manifestCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.failures[manifestCID]
	if !ok {
		f = &failureRecord{}
		s.failures[manifestCID] = f
	}
	f.Count++
	f.LastTry = time.Now()
	if f.Count >= maxRetries {
		s.saveFailures()
	}
}

func (s *Store) markSkipped(manifestCID, payloadCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[manifestCID] = true
	s.donePayloads[payloadCID] = true
}

func (s *Store) resetDayIfNeeded() {
	dayStart := startOfDay(time.Now())
	if dayStart.After(s.dayStart) {
		s.dayStart = dayStart
		s.dailyCount = 0
	}
}

func startOfDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
