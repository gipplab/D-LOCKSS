package keywords

import (
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	MaxWordsPerKeyword = 5
	MaxRunesPerKeyword = 56
)

// Gateway is the IPFS HTTP gateway used for server-side fetches (DLOCKSS_IPFS_GATEWAY).
func (s *Store) Gateway() string {
	if s == nil || s.cfg.Gateway == "" {
		return DefaultGateway
	}
	return strings.TrimSuffix(s.cfg.Gateway, "/")
}

// Lookup returns a locally indexed entry without hitting a gateway.
func (s *Store) Lookup(manifestCID string) (CIDKeywordEntry, bool) {
	if s == nil || manifestCID == "" {
		return CIDKeywordEntry{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.cidKeywords[manifestCID]
	if !ok || e == nil || e.PayloadCID == "" {
		return CIDKeywordEntry{}, false
	}
	return *e, true
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

func (s *Store) RecordSearch(keyword string, resultCount int) {
	if resultCount == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sr := range s.recent {
		if strings.EqualFold(sr.Keyword, keyword) {
			s.recent = append(s.recent[:i], s.recent[i+1:]...)
			break
		}
	}
	s.recent = append(s.recent, RecentSearch{
		Keyword:     strings.ToLower(keyword),
		ResultCount: resultCount,
		Timestamp:   time.Now().Unix(),
	})
	if len(s.recent) > maxRecentItems {
		s.recent = s.recent[len(s.recent)-maxRecentItems:]
	}
}

func (s *Store) GetRecentSearches() []RecentSearch {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]RecentSearch, len(s.recent))
	for i, sr := range s.recent {
		result[len(s.recent)-1-i] = sr
	}
	return result
}

func (s *Store) GetStats(totalUniqueCIDs int) Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pending := totalUniqueCIDs - s.totalOK - s.totalSkipped
	permanentFails := 0
	for _, f := range s.failures {
		if f.Count >= maxRetries {
			permanentFails++
		}
	}
	pending -= permanentFails
	if pending < 0 {
		pending = 0
	}
	cap := s.requestCap()
	remaining := cap - s.dailyCount
	if remaining < 0 {
		remaining = 0
	}
	return Stats{
		TotalCIDs:      totalUniqueCIDs,
		Indexed:        s.totalOK,
		Failed:         s.totalFail + permanentFails,
		Skipped:        s.totalSkipped,
		Pending:        pending,
		UniqueKeywords: len(s.keywordCIDs),
		DailyRemaining: remaining,
		DailyLimit:     cap,
		Enabled:        s.cfg.APIKey != "",
		CanSetKey:      s.cfg.APIKey == "",
		Provider:       s.cfg.Provider,
		Model:          s.cfg.Model,
	}
}

func AcceptableKeyword(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if len([]rune(s)) > MaxRunesPerKeyword {
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
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"'`+"`")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
