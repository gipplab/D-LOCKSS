// Package keywords indexes preserved content for local search: it fetches
// payloads through an IPFS gateway, extracts text, asks an LLM for a title,
// classification and keywords, and stores the results in a small on-disk
// index with an inverted label map.
package keywords

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Provider identifies an LLM backend.
type Provider string

const (
	ProviderSAIA   Provider = "saia"
	ProviderGoogle Provider = "google"
)

const (
	defaultGateway    = "http://ipfs:8080"
	saiaDefaultBase   = "https://chat-ai.academiccloud.de/v1"
	saiaDefaultModel  = "meta-llama-3.1-8b-instruct"
	googleBase        = "https://generativelanguage.googleapis.com/v1beta"
	googleModel       = "gemini-2.5-flash-lite"
	settingsFileName  = "llm_settings.json"
	apiKeyFileName    = ".api_key"
)

// parseProvider maps the accepted aliases; empty means SAIA.
func parseProvider(s string) (Provider, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "saia", "openai":
		return ProviderSAIA, nil
	case "google", "gemini", "aistudio", "ai-studio":
		return ProviderGoogle, nil
	default:
		return "", fmt.Errorf("unknown provider %q", s)
	}
}

// Settings holds the mutable LLM/gateway configuration. The first API key
// saved becomes the password for later changes.
type Settings struct {
	dataDir string

	mu       sync.RWMutex
	provider Provider
	apiKey   string
	apiBase  string
	model    string
	gateway  string
}

type settingsFile struct {
	Provider string `json:"provider"`
	APIKey   string `json:"api_key"`
	APIBase  string `json:"api_base,omitempty"`
	Model    string `json:"model,omitempty"`
	Gateway  string `json:"gateway,omitempty"`
}

// DataDir resolves the monitor data directory (DLOCKSS_MONITOR_DATA_DIR,
// default ~/.dlockss-monitor).
func DataDir() string {
	if d := os.Getenv("DLOCKSS_MONITOR_DATA_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".dlockss-monitor"
	}
	return filepath.Join(home, ".dlockss-monitor")
}

// LoadSettings builds the settings from the saved settings file, falling
// back to the environment. A valid saved file (non-empty key) wins over env
// vars, except that a saved gateway only replaces the env gateway when
// non-empty.
func LoadSettings(dataDir string) *Settings {
	s := &Settings{
		dataDir:  dataDir,
		provider: ProviderSAIA,
		apiBase:  saiaDefaultBase,
		model:    saiaDefaultModel,
		gateway:  defaultGateway,
	}
	if g := os.Getenv("DLOCKSS_IPFS_GATEWAY"); g != "" {
		s.gateway = strings.TrimRight(g, "/")
	}

	if s.loadFile() {
		return s
	}

	// Environment fallback.
	if p, err := parseProvider(os.Getenv("DLOCKSS_LLM_PROVIDER")); err == nil {
		s.provider = p
	}
	base := firstEnv("DLOCKSS_LLM_API_BASE", "TRACKER_API_BASE")
	model := firstEnv("DLOCKSS_LLM_MODEL", "TRACKER_MODEL")
	if s.provider == ProviderGoogle {
		s.apiBase, s.model = googleBase, googleModel
	} else {
		if base != "" {
			s.apiBase = strings.TrimRight(base, "/")
		}
		if model != "" {
			s.model = model
		}
	}
	s.apiKey = loadKeyFile(dataDir)
	return s
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// loadKeyFile reads <dataDir>/.api_key, then ./.api_key, then SAIA_API_KEY.
func loadKeyFile(dataDir string) string {
	for _, p := range []string{filepath.Join(dataDir, apiKeyFileName), apiKeyFileName} {
		if raw, err := os.ReadFile(p); err == nil {
			if k := strings.TrimSpace(string(raw)); k != "" {
				return k
			}
		}
	}
	return strings.TrimSpace(os.Getenv("SAIA_API_KEY"))
}

// loadFile reads llm_settings.json; it reports whether a usable key was
// loaded.
func (s *Settings) loadFile() bool {
	raw, err := os.ReadFile(filepath.Join(s.dataDir, settingsFileName))
	if err != nil {
		return false
	}
	var f settingsFile
	if json.Unmarshal(raw, &f) != nil || f.APIKey == "" {
		return false
	}
	p, err := parseProvider(f.Provider)
	if err != nil {
		p = ProviderSAIA
	}
	s.provider = p
	s.apiKey = f.APIKey
	if p == ProviderGoogle {
		s.apiBase, s.model = googleBase, googleModel
	} else {
		s.apiBase = saiaDefaultBase
		if f.APIBase != "" {
			s.apiBase = strings.TrimRight(f.APIBase, "/")
		}
		s.model = saiaDefaultModel
		if f.Model != "" {
			s.model = f.Model
		}
	}
	if f.Gateway != "" {
		s.gateway = strings.TrimRight(f.Gateway, "/")
	}
	return true
}

// saveLocked persists the settings; callers hold s.mu.
func (s *Settings) saveLocked() error {
	f := settingsFile{Provider: string(s.provider), APIKey: s.apiKey, APIBase: s.apiBase, Model: s.model, Gateway: s.gateway}
	key := s.apiKey
	if err := os.MkdirAll(s.dataDir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(s.dataDir, settingsFileName), raw, 0o600); err != nil {
		return err
	}
	return writeAtomic(filepath.Join(s.dataDir, apiKeyFileName), []byte(key+"\n"), 0o600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ApplyError carries an HTTP-ish status for the settings endpoint.
type ApplyError struct {
	Status int
	Msg    string
}

func (e *ApplyError) Error() string { return e.Msg }

// Apply validates and persists a settings change. The password must equal
// the API key currently in use (constant-time); the first save needs none.
// An empty new key keeps the current one.
func (s *Settings) Apply(password, providerStr, apiKey, apiBase, gateway string) *ApplyError {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.apiKey != "" && subtle.ConstantTimeCompare([]byte(password), []byte(s.apiKey)) != 1 {
		return &ApplyError{401, "wrong password: enter the API key currently in use"}
	}
	if apiKey == "" {
		apiKey = s.apiKey
	}
	if apiKey == "" {
		return &ApplyError{400, "an API key is required"}
	}
	p, err := parseProvider(providerStr)
	if err != nil {
		return &ApplyError{400, err.Error()}
	}
	gateway = strings.TrimSpace(gateway)
	if gateway != "" {
		u, err := url.Parse(gateway)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return &ApplyError{400, "gateway must be an http(s) URL"}
		}
		gateway = strings.TrimRight(gateway, "/")
	} else {
		gateway = defaultGateway
	}

	s.provider = p
	s.apiKey = apiKey
	s.gateway = gateway
	if p == ProviderGoogle {
		s.apiBase, s.model = googleBase, googleModel
	} else {
		apiBase = strings.TrimSpace(apiBase)
		if apiBase == "" {
			s.apiBase = saiaDefaultBase
		} else {
			s.apiBase = strings.TrimRight(apiBase, "/")
		}
		if s.model == "" || s.model == googleModel {
			s.model = saiaDefaultModel
		}
	}
	if err := s.saveLocked(); err != nil {
		return &ApplyError{500, "could not save settings: " + err.Error()}
	}
	return nil
}

// Snapshot returns the current values.
func (s *Settings) Snapshot() (provider Provider, apiKey, apiBase, model, gateway string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.provider, s.apiKey, s.apiBase, s.model, s.gateway
}

// HasKey reports whether an API key is configured.
func (s *Settings) HasKey() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.apiKey != ""
}

// Gateway returns the gateway base URL.
func (s *Settings) Gateway() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gateway
}
