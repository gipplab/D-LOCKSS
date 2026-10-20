package keywords

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"dlockss/pkg/schema"
)

var (
	errRateLimited       = errors.New("rate limited")
	errEmptyChatResponse = errors.New("empty chat response")
)

type rateLimitedError struct {
	after time.Duration
}

func (e rateLimitedError) Error() string {
	return fmt.Sprintf("rate limited (retry after %s)", e.after)
}

func (e rateLimitedError) Unwrap() error { return errRateLimited }

const (
	maxTextLen             = 30_000
	googleMaxTextLen       = 8_000
	googleMaxOutputTokens  = 256
	maxChatResponseBytes   = 2 * 1024 * 1024
	maxRateLimitRetries    = 5
	maxRateLimitPause      = 5 * time.Minute
	maxKeywords            = 10
	manifestFetchTimeout   = 30 * time.Second
	llmTimeout             = 120 * time.Second
	extractionSystemPrompt = `The user message describes an archived item (a document, a dataset, source code, or a directory) and contains its text or an excerpt between ---BEGIN_DOCUMENT--- and ---END_DOCUMENT---. Respond with one JSON object only: {"title","broad_field","sub_topic","research_niche","keywords"} where keywords is an array of 10 lowercase strings describing the item's subject. If the item has no title, write a short descriptive one.`
)

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
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type extractionResult struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}

func (s *Store) processCID(manifestCID string) {
	defer s.clearInflight(manifestCID)
	if s.rateLimited() {
		return
	}
	slog.Info("indexing cid", "manifest", manifestCID, "step", "resolve-manifest")
	payloadCID, metaRef, err := s.resolveManifest(manifestCID)
	if s.handleFetchErr(manifestCID, err) {
		return
	}

	slog.Info("indexing cid", "manifest", manifestCID, "step", "fetch-content", "payload", payloadCID)
	c, err := s.loadContent(payloadCID, itemName(metaRef))
	if errors.Is(err, errUnsupported) {
		slog.Info("skipping content that cannot be indexed", "manifest", manifestCID, "meta_ref", metaRef, "reason", err)
		s.markSkipped(manifestCID)
		return
	}
	if s.handleFetchErr(manifestCID, err) {
		return
	}

	slog.Info("extracting keywords", "manifest", manifestCID, "format", c.format, "text_len", len(c.text))
	result, err := s.extractKeywordsWithRetry(c, manifestCID)
	if errors.Is(err, errRateLimited) {
		// A provider quota is no fault of the file; it stays pending.
		s.noteRateLimit(maxRateLimitPause)
		slog.Warn("llm rate limited; pausing indexer", "manifest", manifestCID, "error", err)
		return
	}
	if err != nil {
		slog.Error("keyword extract failed", "manifest", manifestCID, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	s.storeEntry(manifestCID, payloadCID, metaRef, result)
}

func (s *Store) storeEntry(manifestCID, payloadCID, metaRef string, result *extractionResult) {
	s.mu.Lock()
	s.dailyCount++
	entry := &CIDKeywordEntry{
		ManifestCID:   manifestCID,
		PayloadCID:    payloadCID,
		MetaRef:       metaRef,
		Title:         truncate(result.Title, 2000),
		BroadField:    truncate(result.BroadField, 2000),
		SubTopic:      truncate(result.SubTopic, 2000),
		ResearchNiche: truncate(result.ResearchNiche, 2000),
		Keywords:      result.Keywords,
		IndexedAt:     time.Now(),
	}
	s.cidKeywords[manifestCID] = entry
	s.indexLabels(manifestCID, entry)
	s.processed[manifestCID] = true
	delete(s.failures, manifestCID)
	s.totalOK++
	s.mu.Unlock()
	s.saveIndex()
	slog.Info("indexed", "manifest", manifestCID, "title", entry.Title, "keywords", len(entry.Keywords))
}

// gatewayGet fetches /ipfs/<ipfsPath> from the configured gateway. ipfsPath
// must already be escaped. On success the caller must close the response body.
func (s *Store) gatewayGet(ipfsPath string, timeout time.Duration) (*http.Response, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(s.Gateway() + "/ipfs/" + ipfsPath)
	if err != nil {
		return nil, fmt.Errorf("gateway request: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, rateLimitedError{after: retryAfterFrom(resp)}
	}
	return nil, fmt.Errorf("gateway status: %s", resp.Status)
}

func (s *Store) resolveManifest(manifestCID string) (payloadCID, metaRef string, err error) {
	resp, err := s.gatewayGet(gatewayPath(manifestCID), manifestFetchTimeout)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	block, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", fmt.Errorf("read body: %w", err)
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return "", "", fmt.Errorf("unmarshal CBOR: %w", err)
	}
	if !ro.Payload.Defined() {
		return "", "", errors.New("manifest has no payload CID")
	}
	return ro.Payload.String(), ro.MetadataRef, nil
}

func (s *Store) extractKeywordsWithRetry(c *content, manifestCID string) (*extractionResult, error) {
	for attempt := 0; attempt <= maxRateLimitRetries; attempt++ {
		s.waitLLM()
		result, retryAfter, err := s.extractKeywords(c)
		if err == nil {
			return result, nil
		}
		if retryAfter > 0 {
			wait := rateLimitWait(retryAfter, attempt)
			slog.Warn("rate limited on extract, backing off", "manifest", manifestCID, "attempt", attempt+1, "wait", wait)
			time.Sleep(wait)
			continue
		}
		if isRetryableExtractNetErr(err) && attempt < 3 {
			slog.Warn("network error on extract, retrying", "manifest", manifestCID, "attempt", attempt+1, "wait", 20*time.Second, "error", err)
			time.Sleep(20 * time.Second)
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("after %d retries: %w", maxRateLimitRetries, errRateLimited)
}

func (s *Store) handleFetchErr(manifestCID string, err error) bool {
	if err == nil {
		return false
	}
	var rl rateLimitedError
	if errors.As(err, &rl) {
		s.noteRateLimit(rl.after)
		slog.Warn("gateway rate limited; pausing indexer", "manifest", manifestCID, "error", err)
		return true
	}
	slog.Error("failed to load content", "manifest", manifestCID, "error", err)
	s.recordFailure(manifestCID)
	return true
}

// noteRateLimit pauses the indexer for retryAfter, clamped to
// [batchRetryDelay, maxRateLimitPause]. An existing longer pause is kept.
func (s *Store) noteRateLimit(retryAfter time.Duration) {
	until := time.Now().Add(min(max(retryAfter, batchRetryDelay), maxRateLimitPause))
	s.mu.Lock()
	if until.After(s.pauseUntil) {
		s.pauseUntil = until
	}
	s.mu.Unlock()
}

func (s *Store) rateLimited() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Now().Before(s.pauseUntil)
}

func retryAfterFrom(resp *http.Response) time.Duration {
	d := time.Second
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			d = time.Duration(secs) * time.Second
		}
	}
	return d
}

// rateLimitWait is the larger of retryAfter and an exponential backoff
// (2s doubling per attempt, at most a minute), capped at maxRateLimitPause.
func rateLimitWait(retryAfter time.Duration, attempt int) time.Duration {
	backoff := min(2*time.Second<<attempt, time.Minute)
	return min(max(retryAfter, backoff), maxRateLimitPause)
}

func isRetryableExtractNetErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		return false
	}
	return opErr.Timeout() || opErr.Op == "dial"
}

func (s *Store) extractKeywords(c *content) (*extractionResult, time.Duration, error) {
	cfg := s.snapshotConfig()
	if cfg.Provider == ProviderGoogle {
		return geminiExtract(cfg, documentPrompt(c, googleMaxTextLen))
	}
	return openAIExtract(cfg, documentPrompt(c, maxTextLen))
}

func documentPrompt(c *content, limit int) string {
	var b strings.Builder
	if c.name != "" {
		b.WriteString("Name: " + c.name + "\n")
	}
	b.WriteString("Format: " + c.format + "\n")
	b.WriteString("Content (between delimiters only):\n---BEGIN_DOCUMENT---\n")
	b.WriteString(trimUTF8(c.text, limit))
	b.WriteString("\n---END_DOCUMENT---")
	return b.String()
}

func openAIExtract(cfg Config, userText string) (*extractionResult, time.Duration, error) {
	temp := 0.1
	reqBody := chatRequest{
		Model: cfg.Model,
		Messages: []chatMessage{
			{Role: "system", Content: extractionSystemPrompt},
			{Role: "user", Content: userText},
		},
		Temperature: &temp,
		MaxTokens:   512,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(cfg.APIBase, "/")+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	respBody, retryAfter, err := doLLMRequest(req)
	if err != nil || retryAfter > 0 {
		return nil, retryAfter, err
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, 0, fmt.Errorf("parse response: %w", err)
	}
	if chatResp.Error != nil {
		return nil, 0, fmt.Errorf("API error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message.Content == "" {
		return nil, 0, errEmptyChatResponse
	}
	result, err := parseExtraction(chatResp.Choices[0].Message.Content)
	return result, 0, err
}

func geminiGenerationConfig() map[string]interface{} {
	return map[string]interface{}{
		"temperature":      0.1,
		"maxOutputTokens":  googleMaxOutputTokens,
		"responseMimeType": "application/json",
		"responseSchema": map[string]interface{}{
			"type": "OBJECT",
			"properties": map[string]interface{}{
				"title":          map[string]string{"type": "STRING"},
				"broad_field":    map[string]string{"type": "STRING"},
				"sub_topic":      map[string]string{"type": "STRING"},
				"research_niche": map[string]string{"type": "STRING"},
				"keywords": map[string]interface{}{
					"type":  "ARRAY",
					"items": map[string]string{"type": "STRING"},
				},
			},
			"required": []string{"title", "broad_field", "sub_topic", "research_niche", "keywords"},
		},
		// Flash-Lite can spend output budget on hidden reasoning. This task does not need it.
		"thinkingConfig": map[string]int{"thinkingBudget": 0},
	}
}

// trimUTF8 truncates s to at most n bytes without splitting a UTF-8 sequence.
func trimUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && (s[cut]&0xC0) == 0x80 {
		cut--
	}
	return s[:cut]
}

func geminiExtract(cfg Config, userText string) (*extractionResult, time.Duration, error) {
	reqBody := map[string]interface{}{
		"systemInstruction": map[string]interface{}{
			"parts": []map[string]string{{"text": extractionSystemPrompt}},
		},
		"contents": []map[string]interface{}{
			{"role": "user", "parts": []map[string]string{{"text": userText}}},
		},
		"generationConfig": geminiGenerationConfig(),
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	endpoint := strings.TrimSuffix(cfg.APIBase, "/") + "/models/" + url.PathEscape(cfg.Model) + ":generateContent"
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", cfg.APIKey)

	respBody, retryAfter, err := doLLMRequest(req)
	if err != nil || retryAfter > 0 {
		return nil, retryAfter, err
	}
	text, err := geminiResponseText(respBody)
	if err != nil {
		return nil, 0, err
	}
	result, err := parseExtraction(text)
	return result, 0, err
}

func geminiResponseText(respBody []byte) (string, error) {
	var parsed struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return "", fmt.Errorf("API error: %s", parsed.Error.Message)
	}
	if len(parsed.Candidates) == 0 || len(parsed.Candidates[0].Content.Parts) == 0 {
		return "", errEmptyChatResponse
	}
	text := strings.TrimSpace(parsed.Candidates[0].Content.Parts[0].Text)
	if text == "" {
		return "", errEmptyChatResponse
	}
	return text, nil
}

func doLLMRequest(req *http.Request) ([]byte, time.Duration, error) {
	client := &http.Client{Timeout: llmTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("HTTP: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxChatResponseBytes+1))
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}
	if len(respBody) > maxChatResponseBytes {
		return nil, 0, errors.New("chat response too large")
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, retryAfterFrom(resp), errors.New("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("API status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}
	return respBody, 0, nil
}

func parseExtraction(raw string) (*extractionResult, error) {
	text := strings.TrimSpace(raw)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	obj, err := firstJSONObject(strings.TrimSpace(text))
	if err != nil {
		return nil, err
	}
	var result extractionResult
	if err := json.Unmarshal([]byte(obj), &result); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	result.Title = strings.TrimSpace(result.Title)
	result.BroadField = strings.TrimSpace(result.BroadField)
	result.SubTopic = strings.TrimSpace(result.SubTopic)
	result.ResearchNiche = strings.TrimSpace(result.ResearchNiche)

	cleaned := make([]string, 0, maxKeywords)
	seen := make(map[string]struct{})
	for _, kw := range result.Keywords {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if !AcceptableKeyword(kw) {
			continue
		}
		if _, ok := seen[kw]; ok {
			continue
		}
		seen[kw] = struct{}{}
		cleaned = append(cleaned, kw)
		if len(cleaned) == maxKeywords {
			break
		}
	}
	if len(cleaned) == 0 {
		return nil, errors.New("no keywords extracted")
	}
	result.Keywords = cleaned
	return &result, nil
}

// firstJSONObject returns the first balanced {...} object in s, skipping
// braces inside JSON strings.
func firstJSONObject(s string) (string, error) {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return "", errors.New("no JSON object in response")
	}
	depth := 0
	inString := false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inString {
			switch ch {
			case '\\':
				i++
			case '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start : i+1], nil
			}
		}
	}
	return "", errors.New("no complete JSON object in response")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
