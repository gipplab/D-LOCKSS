package keywords

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrAPIKeyEmpty = errors.New("api key is empty")
	ErrAPIKeySet   = errors.New("api key already set")
)

const (
	DefaultModel   = "meta-llama-3.1-8b-instruct"
	DefaultAPIBase = "https://chat-ai.academiccloud.de/v1"
	DefaultGateway = "https://ipfs.io"
)

// Config is the LLM + persistence setup for keyword indexing.
type Config struct {
	APIKey  string
	APIBase string
	Model   string
	Gateway string
	DataDir string
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

// ConfigFromEnv loads SAIA / tracker-style settings.
// API key: {DataDir}/.api_key, then ./.api_key, then SAIA_API_KEY.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	cfg.DataDir = DefaultDataDir()
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
	if v := os.Getenv("DLOCKSS_IPFS_GATEWAY"); v != "" {
		cfg.Gateway = v
	}
	cfg.APIKey = LoadAPIKey(cfg.DataDir, ".")
	return cfg
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
