package keywords

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAPIKey_FileThenEnv(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".api_key"), []byte("  file-key  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SAIA_API_KEY", "env-key")
	if got := LoadAPIKey(dir); got != "file-key" {
		t.Fatalf("file key = %q, want file-key", got)
	}
}

func TestLoadAPIKey_EnvFallback(t *testing.T) {
	t.Setenv("SAIA_API_KEY", "env-key")
	if got := LoadAPIKey(t.TempDir()); got != "env-key" {
		t.Fatalf("env key = %q, want env-key", got)
	}
}

func TestLoadAPIKey_Empty(t *testing.T) {
	t.Setenv("SAIA_API_KEY", "")
	if got := LoadAPIKey(t.TempDir()); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestAcceptableKeyword(t *testing.T) {
	if !AcceptableKeyword("machine learning") {
		t.Fatal("expected acceptable")
	}
	if AcceptableKeyword("") {
		t.Fatal("empty should be rejected")
	}
	if AcceptableKeyword("one two three four five six") {
		t.Fatal("six words should be rejected")
	}
}

func TestParseExtraction(t *testing.T) {
	raw := "```json\n{\"title\":\"T\",\"broad_field\":\"CS\",\"sub_topic\":\"ML\",\"research_niche\":\"NLP\",\"keywords\":[\"transformer\",\"attention mechanism\"]}\n```"
	got, err := parseExtraction(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "T" || got.BroadField != "CS" || len(got.Keywords) != 2 {
		t.Fatalf("%+v", got)
	}
}

func TestSearchAND(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir()})
	s.storeEntry("cid-a", "pay-a", "file://a.pdf", &extractionResult{
		Title: "Attention Is All You Need", BroadField: "Computer Science", SubTopic: "Machine Learning",
		Keywords: []string{"transformer", "attention"},
	})
	s.storeEntry("cid-b", "pay-b", "file://b.pdf", &extractionResult{
		Title: "A Biology Paper", BroadField: "Biology", SubTopic: "Genomics",
		Keywords: []string{"crispr", "gene"},
	})

	hits := s.Search("transformer")
	if len(hits) != 1 || hits[0].ManifestCID != "cid-a" {
		t.Fatalf("transformer hits = %+v", hits)
	}
	if hits := s.Search("biology transformer"); len(hits) != 0 {
		t.Fatalf("AND should miss, got %+v", hits)
	}
	if hits := s.Search("computer science"); len(hits) != 1 {
		t.Fatalf("field search = %+v", hits)
	}
}

func TestStatsDisabledWithoutKey(t *testing.T) {
	s := NewStore(Config{})
	if s.Enabled() {
		t.Fatal("empty key should disable indexing")
	}
	st := s.GetStats(10)
	if st.Enabled || !st.CanSetKey {
		t.Fatalf("stats = %+v, want enabled=false can_set_key=true", st)
	}
}

func TestSetAPIKeyOnce(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(Config{DataDir: dir})
	if err := s.SetAPIKeyOnce("  secret-key  "); err != nil {
		t.Fatal(err)
	}
	if !s.Enabled() {
		t.Fatal("expected enabled after set")
	}
	if err := s.SetAPIKeyOnce("another"); !errors.Is(err, ErrAPIKeySet) {
		t.Fatalf("second set = %v, want ErrAPIKeySet", err)
	}
	if got := LoadAPIKey(dir); got != "secret-key" {
		t.Fatalf("persisted key = %q", got)
	}
	s2 := NewStore(Config{DataDir: dir})
	if !s2.Enabled() || s2.GetStats(0).CanSetKey {
		t.Fatal("reloaded store should already have the key")
	}
}

func TestSuggestAndRecent(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir()})
	s.storeEntry("cid-a", "pay-a", "file://a.pdf", &extractionResult{
		Title: "T", BroadField: "Computer Science", Keywords: []string{"transformer"},
	})
	sugs := s.Suggest("trans")
	if len(sugs) == 0 || sugs[0].Keyword != "transformer" {
		t.Fatalf("suggest = %+v", sugs)
	}
	s.RecordSearch("transformer", 1)
	recent := s.GetRecentSearches()
	if len(recent) != 1 || recent[0].Keyword != "transformer" {
		t.Fatalf("recent = %+v", recent)
	}
}

func TestRateLimitWaitMatchesTracker(t *testing.T) {
	// attempt 0: max(1s, 2s) = 2s; attempt 2: max(1s, 8s) = 8s
	if got := rateLimitWait(time.Second, 0); got != 2*time.Second {
		t.Fatalf("attempt 0 = %s, want 2s", got)
	}
	if got := rateLimitWait(time.Second, 2); got != 8*time.Second {
		t.Fatalf("attempt 2 = %s, want 8s", got)
	}
	if got := rateLimitWait(3*time.Minute, 0); got != 3*time.Minute {
		t.Fatalf("retry-after wins = %s", got)
	}
}

func TestGatewayRateLimitPausesIndexer(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir(), APIKey: "k"})
	s.noteRateLimit(time.Second) // floored to batchRetryDelay (1m)
	if !s.rateLimited() {
		t.Fatal("expected pause after gateway 429")
	}
	if got := s.pickNextCID(staticCIDs{"cid-a"}); got != "" {
		t.Fatalf("pickNextCID during pause = %q", got)
	}
}

type staticCIDs []string

func (s staticCIDs) UniqueCIDList() []string { return append([]string(nil), s...) }

func TestPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(Config{DataDir: dir, APIKey: "k"})
	s.storeEntry("cid-a", "pay-a", "file://a.pdf", &extractionResult{
		Title: "T", BroadField: "CS", Keywords: []string{"transformer"},
	})
	// give IndexedAt a stable-enough value
	_ = time.Second
	s2 := NewStore(Config{DataDir: dir, APIKey: "k"})
	hits := s2.Search("transformer")
	if len(hits) != 1 || hits[0].Title != "T" {
		t.Fatalf("reloaded search = %+v", hits)
	}
}
