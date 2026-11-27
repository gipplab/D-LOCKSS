package keywords

import (
	"encoding/json"
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
	st := s.GetStats()
	if st.Enabled || st.SettingsEnabled {
		t.Fatalf("stats = %+v, want enabled=false settings_enabled=false", st)
	}
}

func TestApplySettingsNeedsPasswordFromEnv(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir()})
	if err := s.ApplySettings("", "google", "first-key", "", ""); !errors.Is(err, ErrSettingsDisabled) {
		t.Fatalf("without DLOCKSS_MONITOR_PASSWORD settings must be off, got %v", err)
	}
	if s.Enabled() {
		t.Fatal("a refused save must not set a key")
	}
	t.Setenv("DLOCKSS_MONITOR_PASSWORD", "pw")
	if got := ConfigFromEnv().Password; got != "pw" {
		t.Fatalf("ConfigFromEnv password = %q", got)
	}
}

func TestApplySettingsPasswordAndGoogle(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(Config{DataDir: dir, Password: "pw"})
	if !s.GetStats().SettingsEnabled {
		t.Fatal("with a password the settings can be changed")
	}
	if err := s.ApplySettings("", "google", "first-key", "", ""); !errors.Is(err, ErrSettingsAuth) {
		t.Fatalf("missing password = %v", err)
	}
	if err := s.ApplySettings("pw", "google", "first-key", "", ""); err != nil {
		t.Fatal(err)
	}
	if got := s.GetStats().Gateway; got != DefaultGateway {
		t.Fatalf("default gateway = %q", got)
	}
	if err := s.ApplySettings("first-key", "google", "next-key", "", "http://ipfs:8081"); !errors.Is(err, ErrSettingsAuth) {
		t.Fatalf("the api key is no password, got %v", err)
	}
	if err := s.ApplySettings("pw", "google", "next-key", "", "http://ipfs:8081"); err != nil {
		t.Fatal(err)
	}
	s2 := NewStore(Config{DataDir: dir})
	st := s2.GetStats()
	if st.SettingsEnabled || st.Provider != ProviderGoogle || st.Model != GoogleModel || st.DailyLimit != googleDailyCap || st.Gateway != "http://ipfs:8081" {
		t.Fatalf("reloaded stats = %+v", st)
	}
	if got := LoadAPIKey(dir); got != "next-key" {
		t.Fatalf("persisted key = %q", got)
	}
}

func TestApplySettingsRetriesFailedCIDs(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(Config{DataDir: dir, APIKey: "old-key", Password: "pw"})
	for i := 0; i < maxRetries; i++ {
		s.recordFailure("cid-a")
	}
	s.noteRateLimit(time.Minute)

	reloaded := NewStore(Config{DataDir: dir, APIKey: "old-key", Password: "pw"})
	if got := reloaded.pickNextCID(staticCIDs{"cid-a"}); got != "" {
		t.Fatalf("a permanent failure survives a restart, picked %q", got)
	}

	if err := reloaded.ApplySettings("pw", "google", "new-key", "", ""); err != nil {
		t.Fatal(err)
	}
	if got := reloaded.pickNextCID(staticCIDs{"cid-a"}); got != "cid-a" {
		t.Fatalf("after new settings the failed cid must be retried, picked %q", got)
	}
	if v := reloaded.View(); len(v.Failed) != 0 {
		t.Fatalf("failed after reset = %v", v.Failed)
	}
	if got := NewStore(Config{DataDir: dir}).pickNextCID(staticCIDs{"cid-a"}); got != "cid-a" {
		t.Fatalf("the reset must be persisted, picked %q", got)
	}
}

func TestGeminiGenerationConfigDisablesThinking(t *testing.T) {
	cfg := geminiGenerationConfig()
	if cfg["maxOutputTokens"] != googleMaxOutputTokens {
		t.Fatalf("maxOutputTokens = %v", cfg["maxOutputTokens"])
	}
	think, ok := cfg["thinkingConfig"].(map[string]int)
	if !ok || think["thinkingBudget"] != 0 {
		t.Fatalf("thinkingConfig = %#v", cfg["thinkingConfig"])
	}
	if _, ok := cfg["responseSchema"]; !ok {
		t.Fatal("responseSchema missing")
	}
}

func TestTrimUTF8KeepsRuneBoundary(t *testing.T) {
	got := trimUTF8("ab©", 3)
	if got != "ab" {
		t.Fatalf("trim = %q", got)
	}
}

func TestGeminiResponseText(t *testing.T) {
	inner := `{"title":"T","broad_field":"CS","sub_topic":"ML","research_niche":"NLP","keywords":["transformer"]}`
	raw, err := json.Marshal(map[string]interface{}{
		"candidates": []map[string]interface{}{
			{"content": map[string]interface{}{
				"parts": []map[string]string{{"text": inner}},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	text, err := geminiResponseText(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseExtraction(text)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "T" || len(got.Keywords) != 1 || got.Keywords[0] != "transformer" {
		t.Fatalf("parsed = %+v", got)
	}
}

func TestSuggest(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir()})
	s.storeEntry("cid-a", "pay-a", "file://a.pdf", &extractionResult{
		Title: "T", BroadField: "Computer Science", Keywords: []string{"transformer"},
	})
	sugs := s.Suggest("trans")
	if len(sugs) == 0 || sugs[0].Keyword != "transformer" {
		t.Fatalf("suggest = %+v", sugs)
	}
}

func TestView(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir()})
	s.storeEntry("cid-a", "pay-a", "", &extractionResult{Title: "T"})
	s.markSkipped("cid-b", "pay-b")
	for i := 0; i < maxRetries; i++ {
		s.recordFailure("cid-c")
	}
	s.recordFailure("cid-d")
	v := s.View()
	if v.Entries["cid-a"] == nil || v.Entries["cid-a"].Title != "T" || len(v.Entries) != 1 {
		t.Fatalf("entries = %+v", v.Entries)
	}
	if !v.Skipped["cid-b"] || len(v.Skipped) != 1 {
		t.Fatalf("skipped = %+v", v.Skipped)
	}
	if !v.Failed["cid-c"] || len(v.Failed) != 1 {
		t.Fatalf("failed = %+v, want only the file that failed %d times", v.Failed, maxRetries)
	}
}

func TestPickNextCIDIndexesAPayloadOnce(t *testing.T) {
	s := NewStore(Config{DataDir: t.TempDir(), APIKey: "k"})
	s.storeEntry("cid-a", "pay-1", "", &extractionResult{Title: "T"})
	s.markSkipped("cid-b", "pay-2")
	src := payloadCIDs{"cid-a": "pay-1", "cid-a2": "pay-1", "cid-b2": "pay-2", "cid-c": "pay-3"}
	if got := s.pickNextCID(src); got != "cid-c" {
		t.Fatalf("another manifest of an indexed or skipped payload must not be indexed again, picked %q", got)
	}
}

// payloadCIDs maps manifests to their payloads.
type payloadCIDs map[string]string

func (p payloadCIDs) UniqueCIDList() []string {
	out := make([]string, 0, len(p))
	for c := range p {
		out = append(out, c)
	}
	return out
}

func (p payloadCIDs) PayloadOf(c string) (string, bool) {
	v, ok := p[c]
	return v, ok
}

func TestRateLimitWaitBackoff(t *testing.T) {
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

func (staticCIDs) PayloadOf(string) (string, bool) { return "", false }

func TestPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(Config{DataDir: dir, APIKey: "k"})
	s.storeEntry("cid-a", "pay-a", "file://a.pdf", &extractionResult{
		Title: "T", BroadField: "CS", Keywords: []string{"transformer"},
	})
	s2 := NewStore(Config{DataDir: dir, APIKey: "k"})
	hits := s2.Search("transformer")
	if len(hits) != 1 || hits[0].Title != "T" {
		t.Fatalf("reloaded search = %+v", hits)
	}
}
