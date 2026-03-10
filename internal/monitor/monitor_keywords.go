package monitor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

	// Gemini 2.5 Flash-Lite free tier limits
	geminiRPM            = 15
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

// KeywordStore manages keyword extraction, indexing, and search for CIDs.
type KeywordStore struct {
	mu sync.RWMutex

	cidKeywords map[string]*CIDKeywordEntry    // manifest CID → entry
	keywordCIDs map[string]map[string]struct{} // lowercase keyword → set of manifest CIDs
	processed   map[string]bool                // CIDs fully processed (or skipped)
	failures    map[string]*failureRecord      // CIDs that failed processing
	recent      []RecentSearch                 // recent successful searches (ring buffer)

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

type KeywordStats struct {
	TotalCIDs      int  `json:"total_cids"`
	Indexed        int  `json:"indexed"`
	Failed         int  `json:"failed"`
	Skipped        int  `json:"skipped"`
	Pending        int  `json:"pending"`
	UniqueKeywords int  `json:"unique_keywords"`
	DailyRemaining int  `json:"daily_remaining"`
	Enabled        bool `json:"enabled"`
}

// geminiRequest / geminiResponse model the Gemini REST API.
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

func NewKeywordStore(apiKey string) *KeywordStore {
	return &KeywordStore{
		cidKeywords: make(map[string]*CIDKeywordEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		processed:   make(map[string]bool),
		failures:    make(map[string]*failureRecord),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		dayStart:    startOfDayPT(time.Now()),
		apiKey:      apiKey,
	}
}

// Run is the background loop that discovers new CIDs from the monitor and extracts keywords.
func (ks *KeywordStore) Run(ctx <-chan struct{}, monitor *Monitor) {
	if ks.apiKey == "" {
		log.Println("[Keywords] GEMINI_API_KEY not set — keyword extraction disabled")
		return
	}
	log.Printf("[Keywords] Background extraction enabled (model: %s, spacing: %s)", geminiModel, geminiRequestSpacing)

	ticker := time.NewTicker(geminiRequestSpacing)
	defer ticker.Stop()

	for {
		select {
		case <-ctx:
			return
		case <-ticker.C:
			cid := ks.pickNextCID(monitor)
			if cid == "" {
				continue
			}
			ks.processCID(cid, monitor)
		}
	}
}

func (ks *KeywordStore) pickNextCID(monitor *Monitor) string {
	ks.mu.Lock()
	ks.resetDayIfNeeded()
	if ks.dailyCount >= geminiRPD {
		ks.mu.Unlock()
		return ""
	}
	ks.mu.Unlock()

	monitor.mu.RLock()
	candidates := make([]string, 0)
	for cidStr := range monitor.uniqueCIDs {
		candidates = append(candidates, cidStr)
	}
	monitor.mu.RUnlock()

	sort.Strings(candidates)

	ks.mu.RLock()
	defer ks.mu.RUnlock()

	now := time.Now()
	for _, c := range candidates {
		if ks.processed[c] {
			continue
		}
		if f, ok := ks.failures[c]; ok {
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

func (ks *KeywordStore) processCID(manifestCID string, monitor *Monitor) {
	payloadCID, metaRef, err := ks.resolveManifest(manifestCID)
	if err != nil {
		log.Printf("[Keywords] Manifest resolve failed for %s: %v", manifestCID, err)
		ks.recordFailure(manifestCID)
		return
	}

	if !looksLikePDF(metaRef) {
		pdfData, isPDF, fetchErr := ks.fetchAndCheckPDF(payloadCID)
		if fetchErr != nil {
			log.Printf("[Keywords] Fetch failed for payload %s: %v", payloadCID, fetchErr)
			ks.recordFailure(manifestCID)
			return
		}
		if !isPDF {
			log.Printf("[Keywords] Skipping non-PDF: %s (meta_ref: %s)", manifestCID, metaRef)
			ks.markSkipped(manifestCID)
			return
		}
		ks.extractAndStore(manifestCID, payloadCID, metaRef, pdfData, monitor)
		return
	}

	pdfData, _, fetchErr := ks.fetchAndCheckPDF(payloadCID)
	if fetchErr != nil {
		log.Printf("[Keywords] PDF fetch failed for %s: %v", payloadCID, fetchErr)
		ks.recordFailure(manifestCID)
		return
	}

	ks.extractAndStore(manifestCID, payloadCID, metaRef, pdfData, monitor)
}

func (ks *KeywordStore) extractAndStore(manifestCID, payloadCID, metaRef string, pdfData []byte, monitor *Monitor) {
	result, err := ks.callGemini(pdfData)
	if err != nil {
		log.Printf("[Keywords] Gemini call failed for %s: %v", manifestCID, err)
		ks.recordFailure(manifestCID)
		return
	}

	ks.mu.Lock()
	ks.dailyCount++
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
	ks.cidKeywords[manifestCID] = entry

	// Index all searchable labels into the unified keyword→CID map
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
		if ks.keywordCIDs[kwLower] == nil {
			ks.keywordCIDs[kwLower] = make(map[string]struct{})
		}
		ks.keywordCIDs[kwLower][manifestCID] = struct{}{}
	}
	ks.processed[manifestCID] = true
	delete(ks.failures, manifestCID)
	ks.totalOK++
	ks.mu.Unlock()

	log.Printf("[Keywords] Indexed %s → %q [%s / %s / %s] %v",
		manifestCID, result.Title, result.BroadField, result.SubTopic, result.ResearchNiche, result.Keywords)
}

func (ks *KeywordStore) resolveManifest(manifestCID string) (payloadCID, metaRef string, err error) {
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

func (ks *KeywordStore) fetchAndCheckPDF(payloadCID string) ([]byte, bool, error) {
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

func (ks *KeywordStore) callGemini(pdfData []byte) (*geminiResult, error) {
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

	apiURL := geminiEndpoint + geminiModel + ":generateContent?key=" + ks.apiKey
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

// geminiResult is the expected JSON structure from the Gemini response text.
type geminiResult struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}

func parseGeminiResponse(text string) (*geminiResult, error) {
	// Strip markdown code fences if present
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	// Find JSON object boundaries
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

	keywords := make([]string, 0, 10)
	for _, kw := range result.Keywords {
		kw = strings.TrimSpace(kw)
		if kw == "" {
			continue
		}
		keywords = append(keywords, strings.ToLower(kw))
		if len(keywords) == 10 {
			break
		}
	}
	if len(keywords) == 0 {
		return nil, fmt.Errorf("no keywords extracted")
	}
	result.Keywords = keywords
	return &result, nil
}

// Search returns CID entries matching the given keyword query. Multiple words use AND logic.
func (ks *KeywordStore) Search(query string) []CIDKeywordEntry {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}

	terms := strings.Fields(query)
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	var matchSet map[string]struct{}
	for _, term := range terms {
		termMatches := make(map[string]struct{})
		for kw, cids := range ks.keywordCIDs {
			if strings.Contains(kw, term) {
				for c := range cids {
					termMatches[c] = struct{}{}
				}
			}
		}
		// Also match against title, broad field, sub-topic, and research niche
		for cid, entry := range ks.cidKeywords {
			haystack := strings.ToLower(entry.Title + " " + entry.BroadField + " " + entry.SubTopic + " " + entry.ResearchNiche)
			if strings.Contains(haystack, term) {
				termMatches[cid] = struct{}{}
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
		if entry, ok := ks.cidKeywords[c]; ok {
			results = append(results, *entry)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].IndexedAt.After(results[j].IndexedAt)
	})
	return results
}

// Suggest returns keyword suggestions matching the given prefix.
func (ks *KeywordStore) Suggest(prefix string) []KeywordSuggestion {
	prefix = strings.ToLower(strings.TrimSpace(prefix))

	ks.mu.RLock()
	defer ks.mu.RUnlock()

	var suggestions []KeywordSuggestion
	for kw, cids := range ks.keywordCIDs {
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
func (ks *KeywordStore) RecordSearch(keyword string, resultCount int) {
	if resultCount == 0 {
		return
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()

	// Deduplicate: remove old entry for same keyword
	for i, s := range ks.recent {
		if strings.EqualFold(s.Keyword, keyword) {
			ks.recent = append(ks.recent[:i], ks.recent[i+1:]...)
			break
		}
	}

	ks.recent = append(ks.recent, RecentSearch{
		Keyword:     strings.ToLower(keyword),
		ResultCount: resultCount,
		Timestamp:   time.Now().Unix(),
	})

	if len(ks.recent) > maxRecentItems {
		ks.recent = ks.recent[len(ks.recent)-maxRecentItems:]
	}
}

// GetRecentSearches returns recent successful searches, newest first.
func (ks *KeywordStore) GetRecentSearches() []RecentSearch {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	result := make([]RecentSearch, len(ks.recent))
	for i, s := range ks.recent {
		result[len(ks.recent)-1-i] = s
	}
	return result
}

// GetStats returns keyword extraction progress statistics.
func (ks *KeywordStore) GetStats(totalUniqueCIDs int) KeywordStats {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	ks.resetDayIfNeededRLocked()

	indexed := ks.totalOK
	failed := ks.totalFail
	skipped := ks.totalSkipped
	pending := totalUniqueCIDs - indexed - skipped

	permanentFails := 0
	for _, f := range ks.failures {
		if f.count >= maxRetries {
			permanentFails++
		}
	}
	pending -= permanentFails
	if pending < 0 {
		pending = 0
	}

	return KeywordStats{
		TotalCIDs:      totalUniqueCIDs,
		Indexed:        indexed,
		Failed:         failed + permanentFails,
		Skipped:        skipped,
		Pending:        pending,
		UniqueKeywords: len(ks.keywordCIDs),
		DailyRemaining: geminiRPD - ks.dailyCount,
		Enabled:        ks.apiKey != "",
	}
}

func (ks *KeywordStore) recordFailure(manifestCID string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	f, ok := ks.failures[manifestCID]
	if !ok {
		f = &failureRecord{}
		ks.failures[manifestCID] = f
	}
	f.count++
	f.lastTry = time.Now()
	ks.totalFail++
}

func (ks *KeywordStore) markSkipped(manifestCID string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.processed[manifestCID] = true
	ks.totalSkipped++
}

func (ks *KeywordStore) resetDayIfNeeded() {
	now := time.Now()
	dayStart := startOfDayPT(now)
	if dayStart.After(ks.dayStart) {
		ks.dayStart = dayStart
		ks.dailyCount = 0
	}
}

func (ks *KeywordStore) resetDayIfNeededRLocked() {
	// read-only check (no mutation); safe under RLock
	// actual reset happens in write-locked paths
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
