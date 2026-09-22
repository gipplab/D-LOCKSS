package monitor

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"dlockss/internal/keywords"
)

func (m *Monitor) handleKeywordSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, map[string]interface{}{"results": []struct{}{}, "count": 0})
		return
	}
	results := m.keywords.Search(query)
	if results == nil {
		results = []keywords.CIDKeywordEntry{}
	}

	type enrichedResult struct {
		keywords.CIDKeywordEntry
		Shard    string `json:"shard"`
		Replicas int    `json:"replicas"`
	}
	m.mu.RLock()
	enriched := make([]enrichedResult, 0, len(results))
	for _, res := range results {
		replicas := 0
		if peers, ok := m.manifestReplication[res.ManifestCID]; ok {
			replicas = len(peers)
		}
		enriched = append(enriched, enrichedResult{
			CIDKeywordEntry: res,
			Shard:           m.manifestShard[res.ManifestCID],
			Replicas:        replicas,
		})
	}
	m.mu.RUnlock()

	m.keywords.RecordSearch(query, len(enriched))
	writeJSON(w, map[string]interface{}{"query": query, "results": enriched, "count": len(enriched)})
}

func (m *Monitor) handleKeywordSuggest(w http.ResponseWriter, r *http.Request) {
	suggestions := m.keywords.Suggest(strings.TrimSpace(r.URL.Query().Get("q")))
	if suggestions == nil {
		suggestions = []keywords.KeywordSuggestion{}
	}
	writeJSON(w, map[string]interface{}{"suggestions": suggestions})
}

func (m *Monitor) handleRecentSearches(w http.ResponseWriter, r *http.Request) {
	recent := m.keywords.GetRecentSearches()
	if recent == nil {
		recent = []keywords.RecentSearch{}
	}
	writeJSON(w, map[string]interface{}{"searches": recent})
}

func (m *Monitor) handleKeywordStats(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	totalCIDs := len(m.uniqueCIDs)
	m.mu.RUnlock()
	writeJSON(w, m.keywords.GetStats(totalCIDs))
}

func (m *Monitor) handleKeywordAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid json", http.StatusBadRequest)
		return
	}
	err := m.keywords.SetAPIKeyOnce(body.APIKey)
	switch {
	case errors.Is(err, keywords.ErrAPIKeyEmpty):
		writeJSONError(w, "api key is empty", http.StatusBadRequest)
	case errors.Is(err, keywords.ErrAPIKeySet):
		writeJSONError(w, "api key already set", http.StatusConflict)
	case err != nil:
		writeJSONError(w, "failed to save api key", http.StatusInternalServerError)
	default:
		writeJSON(w, map[string]interface{}{"enabled": true, "can_set_key": false})
	}
}
