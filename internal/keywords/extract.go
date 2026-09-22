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
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"dlockss/pkg/schema"
)

var errRateLimited = errors.New("rate limited")

type rateLimitedError struct {
	after time.Duration
}

func (e rateLimitedError) Error() string {
	return fmt.Sprintf("rate limited (retry after %s)", e.after)
}

func (e rateLimitedError) Unwrap() error { return errRateLimited }

const (
	maxFetchSize           = 20 * 1024 * 1024
	maxTextLen             = 30_000
	maxChatResponseBytes   = 2 * 1024 * 1024
	maxRateLimitRetries    = 5
	manifestFetchTimeout   = 30 * time.Second
	pdfFetchTimeout        = 90 * time.Second
	llmTimeout             = 120 * time.Second
	extractionSystemPrompt = `The user message contains document text between ---BEGIN_DOCUMENT--- and ---END_DOCUMENT---. Respond with one JSON object only: {"title","broad_field","sub_topic","research_niche","keywords"} where keywords is an array of 10 lowercase strings.`
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

	slog.Info("indexing cid", "manifest", manifestCID, "step", "fetch-pdf", "payload", payloadCID)
	tmpPath, size, isPDF, err := s.fetchToTempFile(payloadCID)
	if tmpPath != "" {
		defer func() { _ = os.Remove(tmpPath) }()
	}
	if s.handleFetchErr(manifestCID, err) {
		return
	}
	if !isPDF && !looksLikePDF(metaRef) {
		slog.Debug("skipping non-pdf", "manifest", manifestCID, "meta_ref", metaRef)
		s.markSkipped(manifestCID)
		return
	}
	if !isPDF {
		slog.Error("meta_ref looks like PDF but payload is not", "manifest", manifestCID)
		s.recordFailure(manifestCID)
		return
	}

	text, err := convertPDFFromFile(tmpPath)
	if err != nil {
		slog.Error("pdftotext failed", "manifest", manifestCID, "bytes", size, "error", err)
		s.recordFailure(manifestCID)
		return
	}

	slog.Info("extracting keywords", "manifest", manifestCID, "text_len", len(text))
	result, err := s.extractKeywordsWithRetry(text, manifestCID)
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

func (s *Store) resolveManifest(manifestCID string) (payloadCID, metaRef string, err error) {
	reqURL := strings.TrimSuffix(s.cfg.Gateway, "/") + "/ipfs/" + url.PathEscape(manifestCID)
	client := &http.Client{Timeout: manifestFetchTimeout}
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", "", fmt.Errorf("gateway fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return "", "", rateLimitedError{after: retryAfterFrom(resp)}
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return "", "", fmt.Errorf("gateway status: %s", resp.Status)
	}
	block, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", fmt.Errorf("read body: %w", err)
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return "", "", fmt.Errorf("unmarshal CBOR: %w", err)
	}
	if !ro.Payload.Defined() {
		return "", "", fmt.Errorf("manifest has no payload CID")
	}
	return ro.Payload.String(), ro.MetadataRef, nil
}

func (s *Store) fetchToTempFile(cid string) (tmpPath string, size int64, isPDF bool, err error) {
	reqURL := strings.TrimSuffix(s.cfg.Gateway, "/") + "/ipfs/" + url.PathEscape(cid)
	client := &http.Client{Timeout: pdfFetchTimeout}
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", 0, false, fmt.Errorf("gateway request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return "", 0, false, rateLimitedError{after: retryAfterFrom(resp)}
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return "", 0, false, fmt.Errorf("gateway returned %s", resp.Status)
	}

	header := make([]byte, 4)
	n, err := io.ReadFull(resp.Body, header)
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", 0, false, fmt.Errorf("read header: %w", err)
	}
	if n < 4 || string(header[:4]) != "%PDF" {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxFetchSize))
		return "", 0, false, nil
	}

	tmpFile, err := os.CreateTemp("", "dlockss-pdf-*.pdf")
	if err != nil {
		return "", 0, false, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath = tmpFile.Name()
	if _, err := tmpFile.Write(header); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", 0, false, fmt.Errorf("write header: %w", err)
	}
	written, err := io.Copy(tmpFile, io.LimitReader(resp.Body, maxFetchSize-3))
	_ = tmpFile.Close()
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, false, fmt.Errorf("stream body: %w", err)
	}
	total := written + 4
	if total > maxFetchSize {
		_ = os.Remove(tmpPath)
		return "", 0, false, fmt.Errorf("file exceeds %d MB limit", maxFetchSize/(1024*1024))
	}
	return tmpPath, total, true, nil
}

func convertPDFFromFile(pdfPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pdftotext", "-layout", "-enc", "UTF-8", pdfPath, "-")
	out := bytes.NewBuffer(make([]byte, 0, maxTextLen+1024))
	var stderr bytes.Buffer
	cmd.Stdout = out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("pdftotext timeout: %w", ctx.Err())
		}
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return "", fmt.Errorf("pdftotext: %w: %s", err, truncate(msg, 500))
		}
		return "", fmt.Errorf("pdftotext: %w", err)
	}
	text := strings.TrimSpace(out.String())
	if text == "" {
		return "", fmt.Errorf("PDF produced no text (possibly image-only)")
	}
	if len(text) > maxTextLen {
		text = text[:maxTextLen]
	}
	return text, nil
}

func (s *Store) extractKeywordsWithRetry(text, cid string) (*extractionResult, error) {
	for attempt := 0; attempt <= maxRateLimitRetries; attempt++ {
		if s.llmLimiter != nil {
			_ = s.llmLimiter.Wait(context.Background())
		}
		result, retryAfter, err := s.extractKeywords(text)
		if err == nil {
			return result, nil
		}
		if retryAfter > 0 {
			wait := rateLimitWait(retryAfter, attempt)
			slog.Warn("rate limited on extract, backing off", "cid", cid, "attempt", attempt+1, "wait", wait)
			time.Sleep(wait)
			continue
		}
		if isRetryableExtractNetErr(err) && attempt < 3 {
			slog.Warn("network error on extract, retrying", "cid", cid, "attempt", attempt+1, "wait", 20*time.Second, "error", err)
			time.Sleep(20 * time.Second)
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("rate limited after %d retries", maxRateLimitRetries)
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
	slog.Error("fetch failed", "manifest", manifestCID, "error", err)
	s.recordFailure(manifestCID)
	return true
}

func (s *Store) noteRateLimit(retryAfter time.Duration) {
	wait := retryAfter
	if wait < batchRetryDelay {
		wait = batchRetryDelay
	}
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	until := time.Now().Add(wait)
	s.mu.Lock()
	if until.After(s.pauseUntil) {
		s.pauseUntil = until
	}
	s.mu.Unlock()
}

func (s *Store) rateLimited() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.pauseUntil.IsZero() && time.Now().Before(s.pauseUntil)
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

func rateLimitWait(retryAfter time.Duration, attempt int) time.Duration {
	exp := 2 * time.Second
	for i := 0; i < attempt; i++ {
		exp *= 2
	}
	if exp > 60*time.Second {
		exp = 60 * time.Second
	}
	wait := retryAfter
	if exp > wait {
		wait = exp
	}
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	return wait
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

func (s *Store) extractKeywords(markdown string) (*extractionResult, time.Duration, error) {
	temp := 0.1
	reqBody := chatRequest{
		Model: s.cfg.Model,
		Messages: []chatMessage{
			{Role: "system", Content: extractionSystemPrompt},
			{Role: "user", Content: "Document text (between delimiters only):\n---BEGIN_DOCUMENT---\n" + markdown + "\n---END_DOCUMENT---"},
		},
		Temperature: &temp,
		MaxTokens:   512,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequest("POST", strings.TrimSuffix(s.cfg.APIBase, "/")+"/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.cfg.APIKey)

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
		return nil, 0, fmt.Errorf("chat response too large")
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := time.Second
		if v := resp.Header.Get("Retry-After"); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, retryAfter, fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("API status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, 0, fmt.Errorf("parse response: %w", err)
	}
	if chatResp.Error != nil {
		return nil, 0, fmt.Errorf("API error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 || chatResp.Choices[0].Message.Content == "" {
		return nil, 0, fmt.Errorf("empty chat response")
	}
	result, err := parseExtraction(chatResp.Choices[0].Message.Content)
	return result, 0, err
}

func parseExtraction(raw string) (*extractionResult, error) {
	text := strings.TrimSpace(raw)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)
	start := strings.Index(text, "{")
	if start == -1 {
		return nil, fmt.Errorf("no JSON object in response")
	}
	depth := 0
	inString := false
	jsonEnd := -1
	for i := start; i < len(text); i++ {
		ch := text[i]
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
				jsonEnd = i + 1
			}
		}
		if jsonEnd >= 0 {
			break
		}
	}
	if jsonEnd < 0 {
		return nil, fmt.Errorf("no complete JSON object in response")
	}
	var result extractionResult
	if err := json.Unmarshal([]byte(text[start:jsonEnd]), &result); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	result.Title = strings.TrimSpace(result.Title)
	result.BroadField = strings.TrimSpace(result.BroadField)
	result.SubTopic = strings.TrimSpace(result.SubTopic)
	result.ResearchNiche = strings.TrimSpace(result.ResearchNiche)

	cleaned := make([]string, 0, 10)
	seen := make(map[string]struct{})
	for _, kw := range result.Keywords {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw == "" || !AcceptableKeyword(kw) {
			continue
		}
		if _, ok := seen[kw]; ok {
			continue
		}
		seen[kw] = struct{}{}
		cleaned = append(cleaned, kw)
		if len(cleaned) == 10 {
			break
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("no keywords extracted")
	}
	result.Keywords = cleaned
	return &result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
