package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	if rec := post("second"); rec.Code != http.StatusConflict {
		t.Fatalf("second save status %d, want 409", rec.Code)
	}
}
