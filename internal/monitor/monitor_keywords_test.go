package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"dlockss/internal/keywords"
)

func TestUniqueCIDList(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	now := time.Now()
	m.mu.Lock()
	m.uniqueCIDs["aaa"] = now
	m.uniqueCIDs["bbb"] = now
	m.mu.Unlock()
	got := m.UniqueCIDList()
	sort.Strings(got)
	if want := []string{"aaa", "bbb"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("UniqueCIDList = %v, want %v", got, want)
	}
}

func TestKeywordSettingsNeedPassword(t *testing.T) {
	post := func(m *Monitor, password, key string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"password": password, "provider": "google", "api_key": key})
		req := httptest.NewRequest(http.MethodPost, "/api/keyword-api-key", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		m.handleKeywordAPIKey(rec, req)
		return rec
	}

	off := NewMonitor(DefaultConfig())
	off.SetKeywords(keywords.NewStore(keywords.Config{DataDir: t.TempDir()}))
	if rec := post(off, "", "key"); rec.Code != http.StatusForbidden {
		t.Fatalf("without DLOCKSS_MONITOR_PASSWORD settings are off, got %d %s", rec.Code, rec.Body.String())
	}

	m := NewMonitor(DefaultConfig())
	m.SetKeywords(keywords.NewStore(keywords.Config{DataDir: t.TempDir(), Password: "pw"}))
	if rec := post(m, "wrong", "key"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password status %d, want 401", rec.Code)
	}
	rec := post(m, "pw", "google-key")
	if rec.Code != http.StatusOK {
		t.Fatalf("save status %d body %s", rec.Code, rec.Body.String())
	}
	var got map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil || got["enabled"] != true || got["provider"] != "google" {
		t.Fatalf("answer = %v, %v", got, err)
	}
	if rec := post(m, "google-key", "other"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("the api key is no password, got %d", rec.Code)
	}
}

func TestIPFSGatewayProxiesKubo(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy" {
			t.Errorf("upstream path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("%PDF"))
	}))
	defer upstream.Close()

	m := NewMonitor(DefaultConfig())
	m.SetKeywords(keywords.NewStore(keywords.Config{DataDir: t.TempDir(), Gateway: upstream.URL}))
	req := httptest.NewRequest(http.MethodGet, "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy", nil)
	rec := httptest.NewRecorder()
	m.handleIPFSGateway(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "%PDF" {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
}
