package keywords

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	llmHTTPTimeout   = 120 * time.Second
	llmResponseLimit = 2 << 20 // 2 MiB
	saiaTextLimit    = 30000   // bytes of document text per request
	googleTextLimit  = 8000
	maxKeywords      = 10
	maxKeywordRunes  = 56
	fieldLimit       = 2000 // stored field truncation
)

const systemPrompt = `You are a research-document classifier. Reply with a single JSON object and nothing else, with exactly these fields: "title" (string), "broad_field" (string), "sub_topic" (string), "research_niche" (string), "keywords" (array of 10 lowercase keyword strings).`

// Extraction is the structured result of one LLM call.
type Extraction struct {
	Title         string   `json:"title"`
	BroadField    string   `json:"broad_field"`
	SubTopic      string   `json:"sub_topic"`
	ResearchNiche string   `json:"research_niche"`
	Keywords      []string `json:"keywords"`
}

// rateGate spaces requests at a fixed interval (burst 1).
type rateGate struct {
	mu   sync.Mutex
	next time.Time
	gap  time.Duration
}

func (g *rateGate) wait(ctx context.Context) error {
	g.mu.Lock()
	now := time.Now()
	at := g.next
	if at.Before(now) {
		at = now
	}
	g.next = at.Add(g.gap)
	g.mu.Unlock()
	select {
	case <-time.After(time.Until(at)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// llmClient calls one provider; rebuilt whenever settings change.
type llmClient struct {
	provider Provider
	apiKey   string
	apiBase  string
	model    string
	gate     *rateGate
	http     *http.Client
}

func newLLMClient(s *Settings) *llmClient {
	provider, key, base, model, _ := s.Snapshot()
	gap := time.Second // SAIA: 1 req/s
	if provider == ProviderGoogle {
		gap = 4 * time.Second // Google free tier: 1 req / 4 s
	}
	return &llmClient{
		provider: provider,
		apiKey:   key,
		apiBase:  base,
		model:    model,
		gate:     &rateGate{gap: gap},
		http:     &http.Client{Timeout: llmHTTPTimeout},
	}
}

// textLimit returns the provider's document-text budget in bytes.
func (c *llmClient) textLimit() int {
	if c.provider == ProviderGoogle {
		return googleTextLimit
	}
	return saiaTextLimit
}

// rateLimitError marks an HTTP 429 with its suggested retry delay.
type rateLimitError struct {
	retryAfter time.Duration
}

func (e *rateLimitError) Error() string { return "LLM rate limited (429)" }

func userPrompt(name, format, text string) string {
	if name == "" {
		name = "(unnamed)"
	}
	return fmt.Sprintf("Name: %s\nFormat: %s\nContent (between delimiters only):\n---BEGIN_DOCUMENT---\n%s\n---END_DOCUMENT---", name, format, text)
}

// Extract performs one classification call.
func (c *llmClient) Extract(ctx context.Context, name, format, text string) (*Extraction, error) {
	if err := c.gate.wait(ctx); err != nil {
		return nil, err
	}
	var raw string
	var err error
	if c.provider == ProviderGoogle {
		raw, err = c.callGoogle(ctx, name, format, text)
	} else {
		raw, err = c.callSAIA(ctx, name, format, text)
	}
	if err != nil {
		return nil, err
	}
	return parseExtraction(raw)
}

func (c *llmClient) callSAIA(ctx context.Context, name, format, text string) (string, error) {
	body := map[string]any{
		"model":       c.model,
		"temperature": 0.1,
		"max_tokens":  512,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt(name, format, text)},
		},
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := c.post(ctx, c.apiBase+"/chat/completions", map[string]string{"Authorization": "Bearer " + c.apiKey}, body, &out); err != nil {
		return "", err
	}
	if len(out.Choices) == 0 {
		return "", errors.New("empty LLM response")
	}
	return out.Choices[0].Message.Content, nil
}

func (c *llmClient) callGoogle(ctx context.Context, name, format, text string) (string, error) {
	strType := map[string]any{"type": "STRING"}
	body := map[string]any{
		"systemInstruction": map[string]any{"parts": []map[string]string{{"text": systemPrompt}}},
		"contents": []map[string]any{
			{"role": "user", "parts": []map[string]string{{"text": userPrompt(name, format, text)}}},
		},
		"generationConfig": map[string]any{
			"temperature":      0.1,
			"maxOutputTokens":  256,
			"responseMimeType": "application/json",
			"responseSchema": map[string]any{
				"type": "OBJECT",
				"properties": map[string]any{
					"title": strType, "broad_field": strType, "sub_topic": strType, "research_niche": strType,
					"keywords": map[string]any{"type": "ARRAY", "items": strType},
				},
				"required": []string{"title", "broad_field", "sub_topic", "research_niche", "keywords"},
			},
			"thinkingConfig": map[string]any{"thinkingBudget": 0},
		},
	}
	var out struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	url := fmt.Sprintf("%s/models/%s:generateContent", c.apiBase, c.model)
	if err := c.post(ctx, url, map[string]string{"x-goog-api-key": c.apiKey}, body, &out); err != nil {
		return "", err
	}
	if len(out.Candidates) == 0 || len(out.Candidates[0].Content.Parts) == 0 {
		return "", errors.New("empty LLM response")
	}
	var sb strings.Builder
	for _, p := range out.Candidates[0].Content.Parts {
		sb.WriteString(p.Text)
	}
	return sb.String(), nil
}

func (c *llmClient) post(ctx context.Context, url string, headers map[string]string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, llmResponseLimit))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &rateLimitError{retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("LLM HTTP %d: %s", resp.StatusCode, truncate(string(data), 300))
	}
	return json.Unmarshal(data, out)
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// isNetworkError reports timeouts and dial failures worth a plain retry.
func isNetworkError(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") || strings.Contains(s, "no such host") ||
		strings.Contains(s, "context deadline exceeded")
}

// parseExtraction pulls the first balanced JSON object out of an LLM reply
// (stripping code fences) and validates its fields.
func parseExtraction(raw string) (*Extraction, error) {
	obj := firstJSONObject(stripFences(raw))
	if obj == "" {
		return nil, errors.New("no JSON object in LLM response")
	}
	var ex Extraction
	if err := json.Unmarshal([]byte(obj), &ex); err != nil {
		return nil, fmt.Errorf("invalid LLM JSON: %w", err)
	}
	ex.Title = truncate(strings.TrimSpace(ex.Title), fieldLimit)
	ex.BroadField = truncate(strings.TrimSpace(ex.BroadField), fieldLimit)
	ex.SubTopic = truncate(strings.TrimSpace(ex.SubTopic), fieldLimit)
	ex.ResearchNiche = truncate(strings.TrimSpace(ex.ResearchNiche), fieldLimit)
	ex.Keywords = validKeywords(ex.Keywords)
	if len(ex.Keywords) == 0 {
		return nil, errors.New("LLM returned no usable keywords")
	}
	return &ex, nil
}

func stripFences(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}

// firstJSONObject returns the first balanced {...}, ignoring braces inside
// strings.
func firstJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}

// validKeywords lower-cases, dedupes, and keeps keywords of 1–5 words and at
// most 56 runes, up to 10.
func validKeywords(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, kw := range in {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw == "" || utf8.RuneCountInString(kw) > maxKeywordRunes {
			continue
		}
		words := strings.FieldsFunc(kw, func(r rune) bool {
			return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
		})
		if len(words) < 1 || len(words) > 5 {
			continue
		}
		if _, dup := seen[kw]; dup {
			continue
		}
		seen[kw] = struct{}{}
		out = append(out, kw)
		if len(out) == maxKeywords {
			break
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
