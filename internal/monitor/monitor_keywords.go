package monitor

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"dlockss/internal/keywords"
)

func (m *Monitor) handleKeywordSuggest(w http.ResponseWriter, r *http.Request) {
	suggestions := m.keywords.Suggest(strings.TrimSpace(r.URL.Query().Get("q")))
	if suggestions == nil {
		suggestions = []keywords.KeywordSuggestion{}
	}
	writeJSON(w, map[string]interface{}{"suggestions": suggestions})
}

// handleKeywordAPIKey changes the indexer settings. It needs the password
// from DLOCKSS_MONITOR_PASSWORD.
func (m *Monitor) handleKeywordAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Password string `json:"password"`
		Provider string `json:"provider"`
		APIKey   string `json:"api_key"`
		APIBase  string `json:"api_base"`
		Gateway  string `json:"gateway"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid json", http.StatusBadRequest)
		return
	}
	switch err := m.keywords.ApplySettings(body.Password, body.Provider, body.APIKey, body.APIBase, body.Gateway); {
	case err == nil:
		writeJSON(w, m.keywords.GetStats())
	case errors.Is(err, keywords.ErrSettingsDisabled):
		writeJSONError(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, keywords.ErrSettingsAuth):
		writeJSONError(w, err.Error(), http.StatusUnauthorized)
	case errors.Is(err, keywords.ErrAPIKeyEmpty), errors.Is(err, keywords.ErrUnknownProvider), errors.Is(err, keywords.ErrBadGateway):
		writeJSONError(w, err.Error(), http.StatusBadRequest)
	default:
		writeJSONError(w, "failed to save settings", http.StatusInternalServerError)
	}
}
