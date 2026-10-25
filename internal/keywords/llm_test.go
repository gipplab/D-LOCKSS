package keywords

import (
	"strings"
	"testing"
)

func TestParseExtractionFencedJSON(t *testing.T) {
	raw := "```json\n{\"title\":\" T \",\"broad_field\":\"Physics\",\"sub_topic\":\"Optics\"," +
		"\"research_niche\":\"Lasers\",\"keywords\":[\"Laser\",\"photonics\"]}\n```"
	ex, err := parseExtraction(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Title != "T" || ex.BroadField != "Physics" {
		t.Fatalf("fields: %+v", ex)
	}
	if len(ex.Keywords) != 2 || ex.Keywords[0] != "laser" {
		t.Fatalf("keywords must be lowercased: %v", ex.Keywords)
	}
}

func TestParseExtractionBalancedBraces(t *testing.T) {
	// Braces inside strings must not confuse the scanner; trailing junk is
	// ignored.
	raw := `noise {"title":"a {b} c","broad_field":"f","sub_topic":"s","research_niche":"n","keywords":["k"]} trailing {`
	ex, err := parseExtraction(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Title != "a {b} c" {
		t.Fatalf("title: %q", ex.Title)
	}
}

func TestParseExtractionKeywordValidation(t *testing.T) {
	long := strings.Repeat("x", 60)
	raw := `{"title":"t","broad_field":"f","sub_topic":"s","research_niche":"n",
		"keywords":["ok","OK","one two three four five six","` + long + `","a b",
		"k1","k2","k3","k4","k5","k6","k7","k8","k9"]}`
	ex, err := parseExtraction(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, kw := range ex.Keywords {
		if kw == "ok" {
			continue
		}
	}
	// "OK" dedupes onto "ok"; six-word and 60-rune keywords are dropped.
	seen := map[string]bool{}
	for _, kw := range ex.Keywords {
		if seen[kw] {
			t.Fatalf("duplicate keyword %q", kw)
		}
		seen[kw] = true
		if len(strings.Fields(kw)) > 5 {
			t.Fatalf("keyword with too many words kept: %q", kw)
		}
		if len([]rune(kw)) > maxKeywordRunes {
			t.Fatalf("over-long keyword kept: %q", kw)
		}
	}
	if len(ex.Keywords) > maxKeywords {
		t.Fatalf("more than %d keywords kept", maxKeywords)
	}
	if _, err := parseExtraction(`{"title":"t","keywords":[]}`); err == nil {
		t.Fatal("zero keywords must be an error")
	}
	if _, err := parseExtraction("no json here"); err == nil {
		t.Fatal("missing JSON must be an error")
	}
}

func TestSettingsPasswordRule(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DLOCKSS_MONITOR_DATA_DIR", dir)
	s := LoadSettings(dir)
	if s.HasKey() {
		t.Fatal("fresh settings must have no key")
	}
	// First save needs no password.
	if err := s.Apply("", "saia", "secret-key-1", "", ""); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// Wrong password refused.
	if err := s.Apply("wrong", "saia", "key2", "", ""); err == nil || err.Status != 401 {
		t.Fatalf("wrong password must give 401, got %v", err)
	}
	// Correct password accepted; empty key keeps the current one.
	if err := s.Apply("secret-key-1", "google", "", "", ""); err != nil {
		t.Fatalf("correct password: %v", err)
	}
	p, key, base, model, _ := s.Snapshot()
	if p != ProviderGoogle || key != "secret-key-1" {
		t.Fatalf("provider switch lost state: %v %q", p, key)
	}
	if base != googleBase || model != googleModel {
		t.Fatal("google must force its base and model")
	}
	// Invalid gateway refused.
	if err := s.Apply("secret-key-1", "saia", "", "ftp://bad", "ftp://bad"); err == nil {
		t.Fatal("invalid gateway must be refused")
	}
	// Gateway trailing slash is stripped; settings persist across reloads.
	if err := s.Apply("secret-key-1", "saia", "", "", "http://gw:8080/"); err != nil {
		t.Fatalf("gateway save: %v", err)
	}
	if s.Gateway() != "http://gw:8080" {
		t.Fatalf("gateway not normalized: %q", s.Gateway())
	}
	s2 := LoadSettings(dir)
	if !s2.HasKey() || s2.Gateway() != "http://gw:8080" {
		t.Fatal("settings must persist")
	}
	// Unknown provider refused.
	if err := s.Apply("secret-key-1", "bogus", "", "", ""); err == nil || err.Status != 400 {
		t.Fatal("unknown provider must give 400")
	}
}
