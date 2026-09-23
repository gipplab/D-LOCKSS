package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"dlockss/internal/keywords"
)

func TestUniqueCIDList(t *testing.T) {
	m := NewMonitor(DefaultMonitorConfig())
	m.mu.Lock()
	m.uniqueCIDs["aaa"] = m.uniqueCIDs["aaa"]
	m.uniqueCIDs["bbb"] = m.uniqueCIDs["bbb"]
	m.mu.Unlock()
	got := m.UniqueCIDList()
	if len(got) != 2 {
		t.Fatalf("UniqueCIDList = %v, want 2 entries", got)
	}
}

func TestKeywordStatsDisabled(t *testing.T) {
	m := NewMonitor(DefaultMonitorConfig())
	req := httptest.NewRequest(http.MethodGet, "/api/keyword-stats", nil)
	rec := httptest.NewRecorder()
	m.handleKeywordStats(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var st keywords.Stats
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Enabled || !st.CanSetKey {
		t.Fatalf("stats = %+v", st)
	}
}

func TestKeywordAPIKeyOnce(t *testing.T) {
	m := NewMonitor(DefaultMonitorConfig())
	m.SetKeywords(keywords.NewStore(keywords.Config{DataDir: t.TempDir()}))

	post := func(key string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"api_key": key})
		req := httptest.NewRequest(http.MethodPost, "/api/keyword-api-key", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		m.handleKeywordAPIKey(rec, req)
		return rec
	}
	if rec := post("once-only"); rec.Code != http.StatusOK {
		t.Fatalf("first save status %d body %s", rec.Code, rec.Body.String())
	}
	if rec := post("second"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("second save status %d, want 401", rec.Code)
	}
	body, _ := json.Marshal(map[string]string{
		"password": "once-only",
		"provider": "google",
		"api_key":  "google-key",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/keyword-api-key", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	m.handleKeywordAPIKey(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestManifestPayloadUsesLocalIndex(t *testing.T) {
	dir := t.TempDir()
	idx := map[string]*keywords.CIDKeywordEntry{
		"bafy-manifest": {ManifestCID: "bafy-manifest", PayloadCID: "bafy-payload", MetaRef: "paper.pdf"},
	}
	data, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keyword_index.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	m := NewMonitor(DefaultMonitorConfig())
	m.SetKeywords(keywords.NewStore(keywords.Config{DataDir: dir, Gateway: "http://127.0.0.1:9"}))

	req := httptest.NewRequest(http.MethodGet, "/api/manifest-payload?cid=bafy-manifest", nil)
	rec := httptest.NewRecorder()
	m.handleManifestPayload(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var got map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["payload_cid"] != "bafy-payload" {
		t.Fatalf("payload_cid = %v", got["payload_cid"])
	}
}
