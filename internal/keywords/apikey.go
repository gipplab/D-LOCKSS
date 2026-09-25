package keywords

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

var (
	ErrAPIKeyEmpty     = errors.New("api key is empty")
	ErrAPIKeySet       = errors.New("api key already set")
	ErrSettingsAuth    = errors.New("settings password does not match the current api key")
	ErrUnknownProvider = errors.New("unknown provider")
	ErrBadGateway      = errors.New("gateway must be an http or https URL")
)

const (
	ProviderSAIA   = "saia"
	ProviderGoogle = "google"

	DefaultModel   = "meta-llama-3.1-8b-instruct"
	DefaultAPIBase = "https://chat-ai.academiccloud.de/v1"
	DefaultGateway = "http://ipfs:8080"

	// gemini-2.5-flash-lite has the highest published free-tier quota among stable
	// Gemini models: about 15 requests/minute and 1,000 requests/day.
	GoogleModel   = "gemini-2.5-flash-lite"
	GoogleAPIBase = "https://generativelanguage.googleapis.com/v1beta"

	settingsFileName = "llm_settings.json"
)

// Config is the LLM + persistence setup for keyword indexing.
type Config struct {
	Provider string
	APIKey   string
	APIBase  string
	Model    string
	Gateway  string
	DataDir  string
}

type savedSettings struct {
	Provider string `json:"provider"`
	APIKey   string `json:"api_key"`
	APIBase  string `json:"api_base,omitempty"`
	Model    string `json:"model,omitempty"`
	Gateway  string `json:"gateway,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		APIBase: DefaultAPIBase,
		Model:   DefaultModel,
		Gateway: DefaultGateway,
	}
}

// DefaultDataDir is ~/.dlockss-monitor (same dir as the monitor identity).
func DefaultDataDir() string {
	if v := os.Getenv("DLOCKSS_MONITOR_DATA_DIR"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".dlockss-monitor")
}

// ConfigFromEnv loads saved monitor settings, then SAIA / tracker-style env.
// A settings file written by the UI wins over the environment so a restart
// keeps the provider and key chosen in the dashboard.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	cfg.DataDir = DefaultDataDir()
	if v := os.Getenv("DLOCKSS_IPFS_GATEWAY"); v != "" {
		cfg.Gateway = v
	}
	if saved, ok := loadSettings(cfg.DataDir); ok {
		cfg.Provider = saved.Provider
		cfg.APIKey = saved.APIKey
		cfg.APIBase = saved.APIBase
		cfg.Model = saved.Model
		if saved.Gateway != "" {
			cfg.Gateway = saved.Gateway
		}
		return normalizeConfig(cfg)
	}
	if v := os.Getenv("DLOCKSS_LLM_PROVIDER"); v != "" {
		cfg.Provider = v
	}
	if v := os.Getenv("DLOCKSS_LLM_API_BASE"); v != "" {
		cfg.APIBase = v
	} else if v := os.Getenv("TRACKER_API_BASE"); v != "" {
		cfg.APIBase = v
	}
	if v := os.Getenv("DLOCKSS_LLM_MODEL"); v != "" {
		cfg.Model = v
	} else if v := os.Getenv("TRACKER_MODEL"); v != "" {
		cfg.Model = v
	}
	cfg.APIKey = LoadAPIKey(cfg.DataDir, ".")
	return normalizeConfig(cfg)
}

func normalizeConfig(cfg Config) Config {
	cfg.Provider = normalizeProvider(cfg.Provider)
	switch cfg.Provider {
	case ProviderGoogle:
		if cfg.APIBase == "" || cfg.APIBase == DefaultAPIBase {
			cfg.APIBase = GoogleAPIBase
		}
		if cfg.Model == "" || cfg.Model == DefaultModel {
			cfg.Model = GoogleModel
		}
	default:
		cfg.Provider = ProviderSAIA
		if cfg.APIBase == "" {
			cfg.APIBase = DefaultAPIBase
		}
		if cfg.Model == "" {
			cfg.Model = DefaultModel
		}
	}
	if cfg.Gateway == "" {
		cfg.Gateway = DefaultGateway
	}
	return cfg
}

func normalizeProvider(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case ProviderGoogle, "gemini", "aistudio", "ai-studio":
		return ProviderGoogle
	case "", ProviderSAIA, "openai":
		return ProviderSAIA
	default:
		return strings.ToLower(strings.TrimSpace(p))
	}
}

func normalizeGateway(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultGateway, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", ErrBadGateway
	}
	return strings.TrimSuffix(raw, "/"), nil
}

func newLLMLimiter(provider string) *rate.Limiter {
	if provider == ProviderGoogle {
		return rate.NewLimiter(rate.Every(4*time.Second), 1)
	}
	return rate.NewLimiter(rate.Every(time.Second), 1)
}

func settingsPath(dataDir string) string {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	return filepath.Join(dataDir, settingsFileName)
}

func loadSettings(dataDir string) (Config, bool) {
	data, err := os.ReadFile(settingsPath(dataDir))
	if err != nil {
		return Config{}, false
	}
	var saved savedSettings
	if err := json.Unmarshal(data, &saved); err != nil {
		return Config{}, false
	}
	saved.APIKey = strings.TrimSpace(saved.APIKey)
	if saved.APIKey == "" {
		return Config{}, false
	}
	return Config{
		Provider: saved.Provider,
		APIKey:   saved.APIKey,
		APIBase:  saved.APIBase,
		Model:    saved.Model,
		Gateway:  saved.Gateway,
	}, true
}

func saveSettings(dataDir string, cfg Config) error {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(savedSettings{
		Provider: cfg.Provider,
		APIKey:   cfg.APIKey,
		APIBase:  cfg.APIBase,
		Model:    cfg.Model,
		Gateway:  cfg.Gateway,
	})
	if err != nil {
		return err
	}
	tmp := settingsPath(dataDir) + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, settingsPath(dataDir)); err != nil {
		return err
	}
	return saveAPIKey(dataDir, cfg.APIKey)
}

// ApplySettings saves the provider and API key. The first save needs no
// password. Later saves must pass the API key that is currently in use.
func (s *Store) ApplySettings(password, provider, apiKey, apiBase, gateway string) error {
	apiKey = strings.TrimSpace(apiKey)
	provider = normalizeProvider(provider)
	if provider != ProviderSAIA && provider != ProviderGoogle {
		return ErrUnknownProvider
	}
	gateway, err := normalizeGateway(gateway)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.APIKey != "" && subtle.ConstantTimeCompare([]byte(password), []byte(s.cfg.APIKey)) != 1 {
		return ErrSettingsAuth
	}
	if apiKey == "" {
		apiKey = s.cfg.APIKey
	}
	if apiKey == "" {
		return ErrAPIKeyEmpty
	}
	cfg := s.cfg
	cfg.Provider = provider
	cfg.APIKey = apiKey
	cfg.Gateway = gateway
	if provider == ProviderGoogle {
		cfg.APIBase = GoogleAPIBase
		cfg.Model = GoogleModel
	} else {
		cfg.APIBase = strings.TrimSpace(apiBase)
		if cfg.Model == "" || cfg.Model == GoogleModel {
			cfg.Model = DefaultModel
		}
	}
	cfg = normalizeConfig(cfg)
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = DefaultDataDir()
		cfg.DataDir = dataDir
	}
	if err := saveSettings(dataDir, cfg); err != nil {
		return err
	}
	s.cfg = cfg
	s.llmLimiter = newLLMLimiter(cfg.Provider)
	s.signalKeyReady()
	return nil
}

// LoadAPIKey matches ipfs-tracker: .api_key files first, then SAIA_API_KEY.
func LoadAPIKey(dirs ...string) string {
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		path := filepath.Join(dir, ".api_key")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if key := strings.TrimSpace(string(data)); key != "" {
			return key
		}
	}
	return strings.TrimSpace(os.Getenv("SAIA_API_KEY"))
}

func apiKeyPath(dataDir string) string {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	return filepath.Join(dataDir, ".api_key")
}

func saveAPIKey(dataDir, key string) error {
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	tmp := apiKeyPath(dataDir) + ".tmp"
	if err := os.WriteFile(tmp, []byte(key+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, apiKeyPath(dataDir))
}
