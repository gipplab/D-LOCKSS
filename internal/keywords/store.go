package keywords

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	indexFileName    = "keyword_index.json"
	failuresFileName = "keyword_failures.json"
	maxRecentSearches = 30
	maxFailures       = 3                // then permanent
	failureCooldown   = 10 * time.Minute // between tries of one CID
)

// Entry is one indexed research object.
type Entry struct {
	ManifestCID   string   `json:"manifest_cid"`
	PayloadCID    string   `json:"payload_cid"`
	MetaRef       string   `json:"meta_ref"`
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
	IndexedAt     int64    `json:"indexed_at"`
}

type failRec struct {
	Count   int       `json:"count"`
	LastTry time.Time `json:"last_try"`
}

// RecentSearch is a remembered successful query.
type RecentSearch struct {
	Keyword     string `json:"keyword"`
	ResultCount int    `json:"result_count"`
	Timestamp   int64  `json:"timestamp"`
}

// Store holds the keyword index: entries, an inverted label map, failures,
// and recent searches. Entries and permanent failures persist to disk.
type Store struct {
	dataDir string

	mu       sync.RWMutex
	entries  map[string]*Entry
	inverted map[string]map[string]struct{} // label -> manifest CIDs
	skipped  map[string]struct{}
	failures map[string]*failRec
	recent   []RecentSearch
}

// NewStore loads (or creates) the index under dataDir.
func NewStore(dataDir string) *Store {
	s := &Store{
		dataDir:  dataDir,
		entries:  map[string]*Entry{},
		inverted: map[string]map[string]struct{}{},
		skipped:  map[string]struct{}{},
		failures: map[string]*failRec{},
	}
	s.load()
	return s
}

func (s *Store) load() {
	if raw, err := os.ReadFile(filepath.Join(s.dataDir, indexFileName)); err == nil {
		var m map[string]*Entry
		if json.Unmarshal(raw, &m) == nil {
			s.entries = m
			for _, e := range m {
				s.indexLabelsLocked(e)
			}
		}
	}
	if raw, err := os.ReadFile(filepath.Join(s.dataDir, failuresFileName)); err == nil {
		var m map[string]*failRec
		if json.Unmarshal(raw, &m) == nil {
			s.failures = m
		}
	}
}

func (s *Store) saveIndexLocked() {
	raw, err := json.Marshal(s.entries)
	if err != nil {
		return
	}
	os.MkdirAll(s.dataDir, 0o700)
	_ = writeAtomic(filepath.Join(s.dataDir, indexFileName), raw, 0o600)
}

// saveFailuresLocked persists only permanent failures.
func (s *Store) saveFailuresLocked() {
	perm := map[string]*failRec{}
	for k, f := range s.failures {
		if f.Count >= maxFailures {
			perm[k] = f
		}
	}
	raw, err := json.Marshal(perm)
	if err != nil {
		return
	}
	os.MkdirAll(s.dataDir, 0o700)
	_ = writeAtomic(filepath.Join(s.dataDir, failuresFileName), raw, 0o600)
}

func (s *Store) labelsOf(e *Entry) []string {
	labels := append([]string{}, e.Keywords...)
	for _, extra := range []string{e.BroadField, e.SubTopic, e.ResearchNiche} {
		if v := strings.ToLower(strings.TrimSpace(extra)); v != "" {
			labels = append(labels, v)
		}
	}
	return labels
}

func (s *Store) indexLabelsLocked(e *Entry) {
	for _, label := range s.labelsOf(e) {
		set := s.inverted[label]
		if set == nil {
			set = map[string]struct{}{}
			s.inverted[label] = set
		}
		set[e.ManifestCID] = struct{}{}
	}
}

// Put stores a successful extraction and persists the index.
func (s *Store) Put(e *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[e.ManifestCID] = e
	s.indexLabelsLocked(e)
	delete(s.failures, e.ManifestCID)
	s.saveIndexLocked()
}

// Get returns an indexed entry.
func (s *Store) Get(manifestCID string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[manifestCID]
	return e, ok
}

// MarkSkipped records unsupported content: processed, never retried.
func (s *Store) MarkSkipped(cid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.skipped[cid] = struct{}{}
}

// RecordFailure counts a failed attempt; at maxFailures it becomes permanent
// and is persisted.
func (s *Store) RecordFailure(cid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.failures[cid]
	if f == nil {
		f = &failRec{}
		s.failures[cid] = f
	}
	f.Count++
	f.LastTry = time.Now()
	if f.Count >= maxFailures {
		s.saveFailuresLocked()
	}
}

// ResetFailures clears every failure (settings changed: try again).
func (s *Store) ResetFailures() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = map[string]*failRec{}
	os.Remove(filepath.Join(s.dataDir, failuresFileName))
}

// eligible reports whether a CID should be picked for indexing now.
func (s *Store) eligible(cid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, done := s.entries[cid]; done {
		return false
	}
	if _, skip := s.skipped[cid]; skip {
		return false
	}
	if f := s.failures[cid]; f != nil {
		if f.Count >= maxFailures {
			return false
		}
		if time.Since(f.LastTry) < failureCooldown {
			return false
		}
	}
	return true
}

// Search runs an AND substring query over labels and descriptive fields,
// newest first.
func (s *Store) Search(query string) []*Entry {
	terms := strings.Fields(strings.ToLower(query))
	if len(terms) == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Entry
	for _, e := range s.entries {
		haystack := strings.ToLower(e.Title + " " + e.BroadField + " " + e.SubTopic + " " + e.ResearchNiche)
		match := true
		for _, term := range terms {
			found := strings.Contains(haystack, term)
			if !found {
				for _, label := range s.labelsOf(e) {
					if strings.Contains(label, term) {
						found = true
						break
					}
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IndexedAt > out[j].IndexedAt })
	return out
}

// Suggestion is one autocomplete candidate.
type Suggestion struct {
	Keyword  string `json:"keyword"`
	CIDCount int    `json:"cid_count"`
}

// Suggest returns up to 20 labels containing the query, by count then name.
func (s *Store) Suggest(query string) []Suggestion {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Suggestion
	for label, set := range s.inverted {
		if strings.Contains(label, q) {
			out = append(out, Suggestion{Keyword: label, CIDCount: len(set)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CIDCount != out[j].CIDCount {
			return out[i].CIDCount > out[j].CIDCount
		}
		return out[i].Keyword < out[j].Keyword
	})
	if len(out) > 20 {
		out = out[:20]
	}
	return out
}

// RecordSearch remembers a query that returned results.
func (s *Store) RecordSearch(keyword string, results int) {
	if results == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recent = append([]RecentSearch{{Keyword: keyword, ResultCount: results, Timestamp: time.Now().Unix()}}, s.recent...)
	if len(s.recent) > maxRecentSearches {
		s.recent = s.recent[:maxRecentSearches]
	}
}

// RecentSearches returns remembered queries, newest first.
func (s *Store) RecentSearches() []RecentSearch {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]RecentSearch{}, s.recent...)
}

// Counts returns (indexed, skipped, failed, uniqueLabels). failed counts
// distinct failing CIDs once, permanent or not.
func (s *Store) Counts() (indexed, skipped, failed, labels int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries), len(s.skipped), len(s.failures), len(s.inverted)
}
