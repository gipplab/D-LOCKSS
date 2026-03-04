// Package keywords handles PDF keyword extraction via the Gemini API,
// indexing, and full-text search across ingested CIDs.
package keywords

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"dlockss/pkg/schema"
)

const (
	geminiModel    = "gemini-2.5-flash-lite"
	geminiEndpoint = "https://generativelanguage.googleapis.com/v1beta/models/"

	maxPDFFetchSize = 20 * 1024 * 1024 // 20 MB (Gemini request limit)

	keywordPrompt = `Analyze this PDF document. Extract the following information and return ONLY a valid JSON object with these fields:
- "title": the document title (string)
- "broad_field": the broad academic/research field (string, e.g. "Computer Science", "Biology", "Physics", "Economics")
- "sub_topic": the sub-topic within that field (string, e.g. "Machine Learning", "Genomics", "Quantum Computing")
- "research_niche": the specific research niche (string, e.g. "Transformer Architectures for NLP", "CRISPR Gene Editing in Plants")
- "keywords": the 10 most important keywords or key phrases (array of exactly 10 lowercase strings)
Example: {"title":"Attention Is All You Need","broad_field":"Computer Science","sub_topic":"Machine Learning","research_niche":"Transformer Architectures for Sequence Modeling","keywords":["transformer","attention mechanism","self-attention","neural networks","sequence modeling","encoder-decoder","natural language processing","deep learning","machine translation","positional encoding"]}`

	geminiRPD            = 1000
	geminiRequestSpacing = 4500 * time.Millisecond // ~13.3 RPM, safe margin under 15 RPM

	manifestFetchTimeout = 30 * time.Second
	pdfFetchTimeout      = 90 * time.Second
	geminiFetchTimeout   = 120 * time.Second

	maxRetries     = 3
	retryCooldown  = 10 * time.Minute
	maxRecentItems = 30

	ipfsGateway = "https://ipfs.io"
)

// CIDSource provides the set of known CIDs to index. The monitor implements
// this interface so the keywords package doesn't depend on the monitor.
type CIDSource interface {
	UniqueCIDList() []string
}

// Store manages keyword extraction, indexing, and search for CIDs.
type Store struct {
	mu sync.RWMutex

	cidKeywords map[string]*CIDKeywordEntry
	keywordCIDs map[string]map[string]struct{}
	processed   map[string]bool
	failures    map[string]*failureRecord
	recent      []RecentSearch

	dailyCount int
	dayStart   time.Time

	totalOK      int
	totalFail    int
	totalSkipped int

	apiKey string
}

type CIDKeywordEntry struct {
	ManifestCID   string    `json:"manifest_cid"`
	PayloadCID    string    `json:"payload_cid"`
	MetaRef       string    `json:"meta_ref"`
	Title         string    `json:"title"`
	BroadField    string    `json:"broad_field"`
	SubTopic      string    `json:"sub_topic"`
	ResearchNiche string    `json:"research_niche"`
	Keywords      []string  `json:"keywords"`
	IndexedAt     time.Time `json:"indexed_at"`
}

type failureRecord struct {
	count   int
	lastTry time.Time
}

type RecentSearch struct {
	Keyword     string `json:"keyword"`
	ResultCount int    `json:"result_count"`
	Timestamp   int64  `json:"timestamp"`
}

type KeywordSuggestion struct {
	Keyword  string `json:"keyword"`
	CIDCount int    `json:"cid_count"`
}

type Stats struct {
	TotalCIDs      int  `json:"total_cids"`
	Indexed        int  `json:"indexed"`
	Failed         int  `json:"failed"`
	Skipped        int  `json:"skipped"`
	Pending        int  `json:"pending"`
	UniqueKeywords int  `json:"unique_keywords"`
	DailyRemaining int  `json:"daily_remaining"`
	Enabled        bool `json:"enabled"`
}

type geminiRequest struct {
	Contents         []geminiContent  `json:"contents"`
	GenerationConfig *geminiGenConfig `json:"generationConfig,omitempty"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text       string        `json:"text,omitempty"`
	InlineData *geminiInline `json:"inline_data,omitempty"`
}

type geminiInline struct {
	MimeType string `json:"mime_type"`
	Data     string `json:"data"`
}

type geminiGenConfig struct {
	Temperature     float64 `json:"temperature"`
	MaxOutputTokens int     `json:"maxOutputTokens"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type geminiResult struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}

func NewStore(apiKey string) *Store {
	return &Store{
		cidKeywords: make(map[string]*CIDKeywordEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		processed:   make(map[string]bool),
		failures:    make(map[string]*failureRecord),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		dayStart:    startOfDayPT(time.Now()),
		apiKey:      apiKey,
	}
}

// Run is the background loop that discovers new CIDs and extracts keywords.
func (s *Store) Run(done <-chan struct{}, source CIDSource) {
	if s.apiKey == "" {
		slog.Warn("gemini api key not set, keyword extraction disabled")
		return
	}
	slog.Info("background extraction enabled", "model", geminiModel, "spacing", geminiRequestSpacing)

	ticker := time.NewTicker(geminiRequestSpacing)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			cid := s.pickNextCID(source)
			if cid == "" {
				continue
			}
			s.processCID(cid)
		}
	}
}

func (s *Store) pickNextCID(source CIDSource) string {
	s.mu.Lock()
	s.resetDayIfNeeded()
	if s.dailyCount >= geminiRPD {
		s.mu.Unlock()
		return ""
	}
	s.mu.Unlock()

	candidates := source.UniqueCIDList()
	sort.Strings(candidates)

	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	for _, c := range candidates {
		if s.processed[c] {
			continue
		}
		if f, ok := s.failures[c]; ok {
			if f.count >= maxRetries {
				continue
			}
			if now.Sub(f.lastTry) < retryCooldown {
				continue
			}
		}
		return c
	}
	return ""
}

func (s *Store) processCID(manifestCID string) {
	payloadCID, metaRef, err := s.resolveManifest(manifestCID)
	if err != nil {
		slog.Error("manifest resolve failed", "manifest", manifestCID, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	if !looksLikePDF(metaRef) {
		pdfData, isPDF, fetchErr := s.fetchAndCheckPDF(payloadCID)
		if fetchErr != nil {
			slog.Error("fetch failed for payload", "payload", payloadCID, "error", fetchErr)
			s.recordFailure(manifestCID)
			return
		}
		if !isPDF {
			slog.Debug("skipping non-pdf", "manifest", manifestCID, "meta_ref", metaRef)
			s.markSkipped(manifestCID)
			return
		}
		s.extractAndStore(manifestCID, payloadCID, metaRef, pdfData)
		return
	}

	pdfData, _, fetchErr := s.fetchAndCheckPDF(payloadCID)
	if fetchErr != nil {
		slog.Error("pdf fetch failed", "payload", payloadCID, "error", fetchErr)
		s.recordFailure(manifestCID)
		return
	}

	s.extractAndStore(manifestCID, payloadCID, metaRef, pdfData)
}

func (s *Store) extractAndStore(manifestCID, payloadCID, metaRef string, pdfData []byte) {
	result, err := s.callGemini(pdfData)
	if err != nil {
		slog.Error("gemini call failed", "manifest", manifestCID, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	s.mu.Lock()
	s.dailyCount++
	entry := &CIDKeywordEntry{
		ManifestCID:   manifestCID,
		PayloadCID:    payloadCID,
		MetaRef:       metaRef,
		Title:         result.Title,
		BroadField:    result.BroadField,
		SubTopic:      result.SubTopic,
		ResearchNiche: result.ResearchNiche,
		Keywords:      result.Keywords,
		IndexedAt:     time.Now(),
	}
	s.cidKeywords[manifestCID] = entry

	allLabels := make([]string, 0, len(result.Keywords)+3)
	allLabels = append(allLabels, result.Keywords...)
	for _, label := range []string{result.BroadField, result.SubTopic, result.ResearchNiche} {
		if label != "" {
			allLabels = append(allLabels, label)
		}
	}
	for _, kw := range allLabels {
		kwLower := strings.ToLower(strings.TrimSpace(kw))
		if kwLower == "" {
			continue
		}
		if s.keywordCIDs[kwLower] == nil {
			s.keywordCIDs[kwLower] = make(map[string]struct{})
		}
		s.keywordCIDs[kwLower][manifestCID] = struct{}{}
	}
	s.processed[manifestCID] = true
	delete(s.failures, manifestCID)
	s.totalOK++
	s.mu.Unlock()

	slog.Info("indexed cid",
		"manifest", manifestCID, "title", result.Title,
		"broad_field", result.BroadField, "sub_topic", result.SubTopic,
		"research_niche", result.ResearchNiche, "keywords", result.Keywords)
}

func (s *Store) resolveManifest(manifestCID string) (payloadCID, metaRef string, err error) {
	reqURL := ipfsGateway + "/ipfs/" + url.PathEscape(manifestCID)
	client := &http.Client{Timeout: manifestFetchTimeout}
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", "", fmt.Errorf("gateway fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("gateway status: %s", resp.Status)
	}

	block, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if err != nil {
		return "", "", fmt.Errorf("read body: %w", err)
	}

	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return "", "", fmt.Errorf("unmarshal CBOR: %w", err)
	}

	return ro.Payload.String(), ro.MetadataRef, nil
}

func (s *Store) fetchAndCheckPDF(payloadCID string) ([]byte, bool, error) {
	reqURL := ipfsGateway + "/ipfs/" + url.PathEscape(payloadCID)
	client := &http.Client{Timeout: pdfFetchTimeout}
	resp, err := client.Get(reqURL)
	if err != nil {
		return nil, false, fmt.Errorf("gateway fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("gateway status: %s", resp.Status)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxPDFFetchSize+1))
	if err != nil {
		return nil, false, fmt.Errorf("read body: %w", err)
	}
	if len(data) > maxPDFFetchSize {
		return nil, false, fmt.Errorf("file exceeds %d MB limit", maxPDFFetchSize/(1024*1024))
	}

	isPDF := len(data) >= 4 && string(data[:4]) == "%PDF"
	return data, isPDF, nil
}

func (s *Store) callGemini(pdfData []byte) (*geminiResult, error) {
	b64 := base64.StdEncoding.EncodeToString(pdfData)

	reqBody := geminiRequest{
		Contents: []geminiContent{{
			Parts: []geminiPart{
				{Text: keywordPrompt},
				{InlineData: &geminiInline{MimeType: "application/pdf", Data: b64}},
			},
		}},
		GenerationConfig: &geminiGenConfig{
			Temperature:     0.1,
			MaxOutputTokens: 512,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	apiURL := geminiEndpoint + geminiModel + ":generateContent?key=" + s.apiKey
	client := &http.Client{Timeout: geminiFetchTimeout}
	resp, err := client.Post(apiURL, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}

	var gemResp geminiResponse
	if err := json.Unmarshal(respBody, &gemResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if gemResp.Error != nil {
		return nil, fmt.Errorf("API error %d: %s", gemResp.Error.Code, gemResp.Error.Message)
	}

	if len(gemResp.Candidates) == 0 || len(gemResp.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("empty response from Gemini")
	}

	text := strings.TrimSpace(gemResp.Candidates[0].Content.Parts[0].Text)
	return parseGeminiResponse(text)
}

func parseGeminiResponse(text string) (*geminiResult, error) {
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("no JSON object found in: %s", text)
	}

	var result geminiResult
	if err := json.Unmarshal([]byte(text[start:end+1]), &result); err != nil {
		return nil, fmt.Errorf("parse JSON object: %w (text: %s)", err, text)
	}

	result.Title = strings.TrimSpace(result.Title)
	result.BroadField = strings.TrimSpace(result.BroadField)
	result.SubTopic = strings.TrimSpace(result.SubTopic)
	result.ResearchNiche = strings.TrimSpace(result.ResearchNiche)

	kws := make([]string, 0, 10)
	for _, kw := range result.Keywords {
		kw = strings.TrimSpace(kw)
		if kw == "" {
			continue
		}
		kws = append(kws, strings.ToLower(kw))
		if len(kws) == 10 {
			break
		}
	}
	if len(kws) == 0 {
		return nil, fmt.Errorf("no keywords extracted")
	}
	result.Keywords = kws
	return &result, nil
}

// Search returns CID entries matching the given keyword query. Multiple words use AND logic.
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
		} else {
			for c := range matchSet {
				if _, ok := termMatches[c]; !ok {
					delete(matchSet, c)
				}
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

// Suggest returns keyword suggestions matching the given prefix.
func (s *Store) Suggest(prefix string) []KeywordSuggestion {
	prefix = strings.ToLower(strings.TrimSpace(prefix))

	s.mu.RLock()
	defer s.mu.RUnlock()

	var suggestions []KeywordSuggestion
	for kw, cids := range s.keywordCIDs {
		if prefix == "" || strings.Contains(kw, prefix) {
			suggestions = append(suggestions, KeywordSuggestion{
				Keyword:  kw,
				CIDCount: len(cids),
			})
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

// RecordSearch adds a successful search to the recent searches list.
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

// GetRecentSearches returns recent successful searches, newest first.
func (s *Store) GetRecentSearches() []RecentSearch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]RecentSearch, len(s.recent))
	for i, sr := range s.recent {
		result[len(s.recent)-1-i] = sr
	}
	return result
}

// GetStats returns keyword extraction progress statistics.
func (s *Store) GetStats(totalUniqueCIDs int) Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	indexed := s.totalOK
	failed := s.totalFail
	skipped := s.totalSkipped
	pending := totalUniqueCIDs - indexed - skipped

	permanentFails := 0
	for _, f := range s.failures {
		if f.count >= maxRetries {
			permanentFails++
		}
	}
	pending -= permanentFails
	if pending < 0 {
		pending = 0
	}

	return Stats{
		TotalCIDs:      totalUniqueCIDs,
		Indexed:        indexed,
		Failed:         failed + permanentFails,
		Skipped:        skipped,
		Pending:        pending,
		UniqueKeywords: len(s.keywordCIDs),
		DailyRemaining: geminiRPD - s.dailyCount,
		Enabled:        s.apiKey != "",
	}
}

func (s *Store) recordFailure(manifestCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.failures[manifestCID]
	if !ok {
		f = &failureRecord{}
		s.failures[manifestCID] = f
	}
	f.count++
	f.lastTry = time.Now()
	s.totalFail++
}

func (s *Store) markSkipped(manifestCID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[manifestCID] = true
	s.totalSkipped++
}

func (s *Store) resetDayIfNeeded() {
	now := time.Now()
	dayStart := startOfDayPT(now)
	if dayStart.After(s.dayStart) {
		s.dayStart = dayStart
		s.dailyCount = 0
	}
}

func looksLikePDF(metaRef string) bool {
	lower := strings.ToLower(metaRef)
	return strings.HasSuffix(lower, ".pdf")
}

func startOfDayPT(t time.Time) time.Time {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	pt := t.In(loc)
	return time.Date(pt.Year(), pt.Month(), pt.Day(), 0, 0, 0, 0, loc)
}
