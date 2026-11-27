package keywords

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxWordsPerKeyword = 5
	MaxRunesPerKeyword = 56
)

// Gateway is the IPFS HTTP gateway used for server-side fetches (DLOCKSS_IPFS_GATEWAY).
func (s *Store) Gateway() string {
	if s == nil {
		return DefaultGateway
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cfg.Gateway == "" {
		return DefaultGateway
	}
	return strings.TrimSuffix(s.cfg.Gateway, "/")
}

func (s *Store) Search(query string) []CIDKeywordEntry {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}

	terms := strings.Fields(query)
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matchSet map[string]struct{}
	for _, term := range terms {
		termMatches := make(map[string]struct{})
		for kw, cids := range s.keywordCIDs {
			if strings.Contains(kw, term) {
				for c := range cids {
					termMatches[c] = struct{}{}
				}
			}
		}
		for cidStr, entry := range s.cidKeywords {
			haystack := strings.ToLower(entry.Title + " " + entry.BroadField + " " + entry.SubTopic + " " + entry.ResearchNiche)
			if strings.Contains(haystack, term) {
				termMatches[cidStr] = struct{}{}
			}
		}
		if matchSet == nil {
			matchSet = termMatches
			continue
		}
		for c := range matchSet {
			if _, ok := termMatches[c]; !ok {
				delete(matchSet, c)
			}
		}
	}

	results := make([]CIDKeywordEntry, 0, len(matchSet))
	for c := range matchSet {
		if entry, ok := s.cidKeywords[c]; ok {
			results = append(results, *entry)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].IndexedAt.After(results[j].IndexedAt)
	})
	return results
}

func (s *Store) Suggest(prefix string) []KeywordSuggestion {
	prefix = strings.ToLower(strings.TrimSpace(prefix))

	s.mu.RLock()
	defer s.mu.RUnlock()

	var suggestions []KeywordSuggestion
	for kw, cids := range s.keywordCIDs {
		if prefix == "" || strings.Contains(kw, prefix) {
			suggestions = append(suggestions, KeywordSuggestion{Keyword: kw, CIDCount: len(cids)})
		}
	}
	sort.Slice(suggestions, func(i, j int) bool {
		if suggestions[i].CIDCount != suggestions[j].CIDCount {
			return suggestions[i].CIDCount > suggestions[j].CIDCount
		}
		return suggestions[i].Keyword < suggestions[j].Keyword
	})
	if len(suggestions) > 20 {
		suggestions = suggestions[:20]
	}
	return suggestions
}

// IndexView is what the indexer knows about each manifest.
type IndexView struct {
	// Entries holds the indexed manifests. They are shared with the store,
	// which never changes an entry once stored.
	Entries map[string]*CIDKeywordEntry
	// Skipped holds manifests whose content cannot be indexed.
	Skipped map[string]bool
	// Failed holds manifests that failed too often to be tried again.
	Failed map[string]bool
}

// View returns the indexer's state per manifest.
func (s *Store) View() IndexView {
	v := IndexView{Entries: map[string]*CIDKeywordEntry{}, Skipped: map[string]bool{}, Failed: map[string]bool{}}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for c, e := range s.cidKeywords {
		v.Entries[c] = e
	}
	for c := range s.processed {
		if _, ok := s.cidKeywords[c]; !ok {
			v.Skipped[c] = true
		}
	}
	for c, f := range s.failures {
		if f.Count >= maxRetries {
			v.Failed[c] = true
		}
	}
	return v
}

func (s *Store) GetStats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit := s.requestCap()
	return Stats{
		DailyRemaining:  max(limit-s.dailyCount, 0),
		DailyLimit:      limit,
		Enabled:         s.cfg.APIKey != "",
		SettingsEnabled: s.cfg.Password != "",
		Provider:        s.cfg.Provider,
		Model:           s.cfg.Model,
		Gateway:         s.cfg.Gateway,
	}
}

func AcceptableKeyword(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if utf8.RuneCountInString(s) > MaxRunesPerKeyword {
		return false
	}
	toks := keywordSplit(s)
	return len(toks) > 0 && len(toks) <= MaxWordsPerKeyword
}

func keywordSplit(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ';' || unicode.IsSpace(r)
	})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.Trim(p, "\"'`"); p != "" {
			out = append(out, p)
		}
	}
	return out
}
