// Package keywords handles PDF keyword extraction via the SAIA Chat AI API,
// indexing, and full-text search across ingested CIDs.
//
// Pipeline per CID:
//  1. Fetch manifest from IPFS, resolve payload CID.
//  2. Fetch PDF payload from IPFS.
//  3. Convert PDF → Markdown via SAIA /v1/documents/convert.
//  4. Send Markdown to an LLM via /v1/chat/completions to extract metadata.
package keywords

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dlockss/pkg/schema"
)

const indexFileName = "keyword_index.json"

const (
	defaultModel = "llama-3.3-70b-instruct"
	apiBase      = "https://chat-ai.academiccloud.de/v1"

	maxPDFFetchSize = 20 * 1024 * 1024
	maxTextLen      = 100_000 // truncate converted markdown to stay within context window

	keywordPrompt = `Analyze the following academic document text. Extract the following information and return ONLY a valid JSON object with these fields:
- "title": the document title (string)
- "broad_field": the broad academic/research field (string, e.g. "Computer Science", "Biology", "Physics", "Economics")
- "sub_topic": the sub-topic within that field (string, e.g. "Machine Learning", "Genomics", "Quantum Computing")
- "research_niche": the specific research niche (string, e.g. "Transformer Architectures for NLP", "CRISPR Gene Editing in Plants")
- "keywords": the 10 most important keywords or key phrases (array of exactly 10 lowercase strings)
Example: {"title":"Attention Is All You Need","broad_field":"Computer Science","sub_topic":"Machine Learning","research_niche":"Transformer Architectures for Sequence Modeling","keywords":["transformer","attention mechanism","self-attention","neural networks","sequence modeling","encoder-decoder","natural language processing","deep learning","machine translation","positional encoding"]}

Document text:
`

	dailyLimit     = 20_000
	requestSpacing = 3 * time.Second

	manifestFetchTimeout = 30 * time.Second
	pdfFetchTimeout      = 90 * time.Second
	convertTimeout       = 120 * time.Second
	llmTimeout           = 120 * time.Second

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

	apiKey  string
	dataDir string
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
	DailyLimit     int  `json:"daily_limit"`
	Enabled        bool `json:"enabled"`
}

// OpenAI-compatible request/response types.

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
	Error   *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

type convertResponse struct {
	Markdown string `json:"markdown"`
}

type extractionResult struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}

func NewStore(apiKey, dataDir string) *Store {
	s := &Store{
		cidKeywords: make(map[string]*CIDKeywordEntry),
		keywordCIDs: make(map[string]map[string]struct{}),
		processed:   make(map[string]bool),
		failures:    make(map[string]*failureRecord),
		recent:      make([]RecentSearch, 0, maxRecentItems),
		dayStart:    startOfDay(time.Now()),
		apiKey:      apiKey,
		dataDir:     dataDir,
	}
	s.loadIndex()
	return s
}

func (s *Store) indexPath() string {
	return filepath.Join(s.dataDir, indexFileName)
}

// loadIndex reads the persisted keyword index from disk and rebuilds
// the in-memory maps. Called once at startup.
func (s *Store) loadIndex() {
	if s.dataDir == "" {
		return
	}
	data, err := os.ReadFile(s.indexPath())
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("failed to read keyword index", "path", s.indexPath(), "error", err)
		}
		return
	}

	var entries map[string]*CIDKeywordEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		slog.Warn("failed to parse keyword index", "path", s.indexPath(), "error", err)
		return
	}

	for cid, entry := range entries {
		s.cidKeywords[cid] = entry
		s.processed[cid] = true

		allLabels := make([]string, 0, len(entry.Keywords)+3)
		allLabels = append(allLabels, entry.Keywords...)
		for _, label := range []string{entry.BroadField, entry.SubTopic, entry.ResearchNiche} {
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
			s.keywordCIDs[kwLower][cid] = struct{}{}
		}
	}
	s.totalOK = len(entries)
	slog.Info("loaded keyword index", "entries", len(entries), "keywords", len(s.keywordCIDs))
}

// saveIndex persists the keyword index to disk using atomic write.
func (s *Store) saveIndex() {
	if s.dataDir == "" {
		return
	}

	s.mu.RLock()
	data, err := json.Marshal(s.cidKeywords)
	s.mu.RUnlock()
	if err != nil {
		slog.Error("failed to marshal keyword index", "error", err)
		return
	}

	tmpPath := s.indexPath() + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		slog.Error("failed to write keyword index", "path", tmpPath, "error", err)
		return
	}
	if err := os.Rename(tmpPath, s.indexPath()); err != nil {
		slog.Error("failed to rename keyword index", "error", err)
	}
}

// Run is the background loop that discovers new CIDs and extracts keywords.
func (s *Store) Run(done <-chan struct{}, source CIDSource) {
	if s.apiKey == "" {
		slog.Warn("SAIA API key not set, keyword extraction disabled")
		return
	}
	slog.Info("background extraction enabled", "model", defaultModel, "spacing", requestSpacing)

	ticker := time.NewTicker(requestSpacing)
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
	if s.dailyCount >= dailyLimit {
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
	slog.Info("processing cid", "manifest", manifestCID, "step", "resolve-manifest")
	payloadCID, metaRef, err := s.resolveManifest(manifestCID)
	if err != nil {
		slog.Error("manifest resolve failed", "manifest", manifestCID, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	slog.Info("processing cid", "manifest", manifestCID, "step", "fetch-pdf", "payload", payloadCID, "meta_ref", metaRef)
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
	slog.Info("processing cid", "manifest", manifestCID, "step", "convert-pdf", "pdf_bytes", len(pdfData))
	markdown, err := s.convertPDFToMarkdown(pdfData)
	if err != nil {
		slog.Error("pdf conversion failed", "manifest", manifestCID, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	slog.Info("processing cid", "manifest", manifestCID, "step", "extract-keywords", "markdown_len", len(markdown))
	result, err := s.extractKeywords(markdown)
	if err != nil {
		slog.Error("keyword extraction failed", "manifest", manifestCID, "error", err)
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

	s.saveIndex()
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

// convertPDFToMarkdown sends a PDF to the SAIA document conversion endpoint
// and returns the extracted markdown text.
func (s *Store) convertPDFToMarkdown(pdfData []byte) (string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("document", "paper.pdf")
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(pdfData); err != nil {
		return "", fmt.Errorf("write pdf data: %w", err)
	}
	writer.Close()

	req, err := http.NewRequest("POST", apiBase+"/documents/convert", &buf)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: convertTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("convert error %d: %s", resp.StatusCode, string(body))
	}

	var cr convertResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		return "", fmt.Errorf("parse convert response: %w", err)
	}

	markdown := strings.TrimSpace(cr.Markdown)
	if markdown == "" {
		return "", fmt.Errorf("empty markdown from conversion")
	}

	if len(markdown) > maxTextLen {
		markdown = markdown[:maxTextLen]
	}
	return markdown, nil
}

// extractKeywords sends document text to the LLM and parses the structured response.
func (s *Store) extractKeywords(markdownText string) (*extractionResult, error) {
	temp := 0.1
	reqBody := chatRequest{
		Model: defaultModel,
		Messages: []chatMessage{{
			Role:    "user",
			Content: keywordPrompt + markdownText,
		}},
		Temperature: &temp,
		MaxTokens:   512,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", apiBase+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	client := &http.Client{Timeout: llmTimeout}
	resp, err := client.Do(req)
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

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if chatResp.Error != nil {
		return nil, fmt.Errorf("API error: %s", chatResp.Error.Message)
	}

	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message.Content == "" {
		return nil, fmt.Errorf("empty response from LLM")
	}

	text := strings.TrimSpace(chatResp.Choices[0].Message.Content)
	return parseExtractionResponse(text)
}

func parseExtractionResponse(text string) (*extractionResult, error) {
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("no JSON object found in: %s", text)
	}

	var result extractionResult
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
		DailyRemaining: dailyLimit - s.dailyCount,
		DailyLimit:     dailyLimit,
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
	dayStart := startOfDay(now)
	if dayStart.After(s.dayStart) {
		s.dayStart = dayStart
		s.dailyCount = 0
	}
}

func looksLikePDF(metaRef string) bool {
	lower := strings.ToLower(metaRef)
	return strings.HasSuffix(lower, ".pdf")
}

func startOfDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
