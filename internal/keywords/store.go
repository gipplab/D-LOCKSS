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

	"golang.org/x/time/rate"
)

const (
	indexFileName    = "keyword_index.json"
	failuresFileName = "keyword_failures.json"

	numWorkers      = 2
	requestSpacing  = time.Second
	dailyLimit      = 20_000
	maxRetries      = 3
	retryCooldown   = 10 * time.Minute
	batchRetryDelay = time.Minute // ipfs-tracker indexer.RetryDelay
	maxRecentItems  = 30
)

// CIDSource is the set of known manifest CIDs (implemented by the monitor).
type CIDSource interface {
	UniqueCIDList() []string
}

// Store indexes PDFs announced on the network and answers keyword searches.
type Store struct {
	mu sync.RWMutex

	cidKeywords map[string]*CIDKeywordEntry
	keywordCIDs map[string]map[string]struct{}
	processed   map[string]bool
	inflight    map[string]bool
	failures    map[string]*failureRecord
	recent      []RecentSearch

	dailyCount   int
	dayStart     time.Time
	totalOK      int
	totalFail    int
	totalSkipped int

	cfg        Config
	keyReady   chan struct{}
	keyOnce    sync.Once
	llmLimiter *rate.Limiter
	pauseUntil time.Time
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

type RecentSearch struct {
	Keyword     string `json:"keyword"`
	ResultCount int    `json:"result_count"`
	Timestamp   int64  `json:"timestamp"`
}

type KeywordSuggestion struct {
	Keyword  string `json:"keyword"`
	CIDCount int    `json:"cid_count"`
}

type Stats struct {
	TotalCIDs      int  `json:"total_cids"`
	Indexed        int  `json:"indexed"`
	Failed         int  `json:"failed"`
	Skipped        int  `json:"skipped"`
	Pending        int  `json:"pending"`
	UniqueKeywords int  `json:"unique_keywords"`
	DailyRemaining int  `json:"daily_remaining"`
	DailyLimit     int  `json:"daily_limit"`
	Enabled        bool `json:"enabled"`
	CanSetKey      bool `json:"can_set_key"`
}

func NewStore(cfg Config) *Store {
	if cfg.APIBase == "" {
		cfg.APIBase = DefaultAPIBase
	}
	if cfg.Model == "" {
		cfg.Model = DefaultModel
	}
	if cfg.Gateway == "" {
		cfg.Gateway = DefaultGateway
	}
	if cfg.APIKey == "" && cfg.DataDir != "" {
		cfg.APIKey = LoadAPIKey(cfg.DataDir)
	}
	s := &Store{
		cidKeywords: make(map[string]*CIDKeywordEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		processed:   make(map[string]bool),
		inflight:    make(map[string]bool),
		failures:    make(map[string]*failureRecord),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		dayStart:    startOfDay(time.Now()),
		cfg:         cfg,
		keyReady:    make(chan struct{}),
		llmLimiter:  rate.NewLimiter(rate.Every(time.Second), 1),
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

// SetAPIKeyOnce persists the SAIA key to {DataDir}/.api_key and enables indexing.
// It fails if a key is already set (file, env, or a previous UI save).
func (s *Store) SetAPIKeyOnce(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrAPIKeyEmpty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.APIKey != "" {
		return ErrAPIKeySet
	}
	dataDir := s.cfg.DataDir
	if dataDir == "" {
		dataDir = DefaultDataDir()
		s.cfg.DataDir = dataDir
	}
	if err := saveAPIKey(dataDir, key); err != nil {
		return err
	}
	s.cfg.APIKey = key
	s.signalKeyReady()
	slog.Info("api key saved once", "path", apiKeyPath(dataDir))
	return nil
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
		s.indexLabels(cid, entry)
	}
	s.totalOK = len(entries)
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

func atomicWrite(path string, data []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		slog.Error("write temp file", "path", tmp, "error", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		slog.Error("rename file", "path", path, "error", err)
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
		if kw = normalizeKeyword(kw); kw != "" {
			labels = append(labels, kw)
		}
	}
	for _, field := range []string{entry.BroadField, entry.SubTopic, entry.ResearchNiche} {
		if kw := normalizeKeyword(field); kw != "" {
			labels = append(labels, kw)
		}
	}
	return labels
}

func normalizeKeyword(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// Run waits for an API key if none is set, then indexes newly announced PDFs.
func (s *Store) Run(ctx context.Context, source CIDSource) {
	if !s.Enabled() {
		slog.Info("no API key yet; set it once in the monitor UI or via SAIA_API_KEY / .api_key")
		select {
		case <-ctx.Done():
			return
		case <-s.keyReady:
		}
		if !s.Enabled() {
			return
		}
	}
	if _, err := exec.LookPath("pdftotext"); err != nil {
		slog.Warn("pdftotext not found; keyword indexing disabled (install poppler-utils)")
		return
	}
	slog.Info("keyword indexing enabled", "model", s.cfg.Model, "api_base", s.cfg.APIBase, "workers", numWorkers)

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

func (s *Store) pickNextCID(source CIDSource) string {
	candidates := source.UniqueCIDList()
	sort.Strings(candidates)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetDayIfNeeded()
	if s.dailyCount >= dailyLimit {
		return ""
	}
	if !s.pauseUntil.IsZero() && time.Now().Before(s.pauseUntil) {
		return ""
	}
	now := time.Now()
	for _, c := range candidates {
		if s.processed[c] || s.inflight[c] {
			continue
		}
		if f, ok := s.failures[c]; ok {
			if f.Count >= maxRetries {
				continue
			}
			if now.Sub(f.LastTry) < retryCooldown {
				continue
			}
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
	s.totalFail++
	if f.Count >= maxRetries {
		s.saveFailures()
	}
}

func (s *Store) markSkipped(manifestCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[manifestCID] = true
	s.totalSkipped++
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

func looksLikePDF(metaRef string) bool {
	return strings.HasSuffix(strings.ToLower(metaRef), ".pdf")
}
