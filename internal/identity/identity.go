// Package identity handles loading, generating, migrating, and persisting
// the node's libp2p identity key and human-readable node name.
package identity

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"dlockss/internal/config"

	"github.com/libp2p/go-libp2p/core/crypto"
)

type ipfsConfigIdentity struct {
	PrivKey string `json:"PrivKey"`
}

type ipfsConfig struct {
	Identity ipfsConfigIdentity `json:"Identity"`
}

// LoadKey returns the node's private key. It tries, in order:
//  1. DLOCKSS_IPFS_CONFIG — read Identity.PrivKey from the Kubo config JSON.
//  2. A persisted key at config.IdentityPath.
//  3. A legacy key at ./dlockss.key (migrated to config.IdentityPath).
//  4. A freshly generated Ed25519 key (persisted to config.IdentityPath).
func LoadKey(cfg *config.Config) (crypto.PrivKey, error) {
	if cfg.IPFSConfigPath != "" {
		priv, err := loadFromIPFSConfig(cfg.IPFSConfigPath)
		if err != nil {
			return nil, fmt.Errorf("DLOCKSS_IPFS_CONFIG set but failed: %w", err)
		}
		slog.Info("using IPFS config identity", "path", cfg.IPFSConfigPath)
		return priv, nil
	}
	return loadOrCreate(cfg)
}

func loadFromIPFSConfig(configPath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read IPFS config: %w", err)
	}
	var cfg ipfsConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse IPFS config: %w", err)
	}
	if cfg.Identity.PrivKey == "" {
		return nil, fmt.Errorf("IPFS config has no Identity.PrivKey")
	}
	raw, err := base64.StdEncoding.DecodeString(cfg.Identity.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("decode Identity.PrivKey: %w", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("unmarshal IPFS private key: %w", err)
	}
	return priv, nil
}

func loadOrCreate(cfg *config.Config) (crypto.PrivKey, error) {
	identityPath := cfg.IdentityPath

	if _, err := os.Stat(identityPath); err == nil {
		data, err := os.ReadFile(identityPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read identity file: %w", err)
		}
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal identity: %w", err)
		}
		slog.Info("loaded persistent identity", "path", identityPath)
		return priv, nil
	}

	// Migrate legacy key from CWD if it exists there but not at the configured path.
	if legacyPath := "dlockss.key"; legacyPath != identityPath {
		if _, err := os.Stat(legacyPath); err == nil {
			data, err := os.ReadFile(legacyPath)
			if err == nil {
				ensureDir(identityPath)
				if err := os.WriteFile(identityPath, data, 0600); err == nil {
					slog.Info("migrated legacy identity", "from", legacyPath, "to", identityPath)
					priv, err := crypto.UnmarshalPrivateKey(data)
					if err != nil {
						return nil, fmt.Errorf("failed to unmarshal migrated identity: %w", err)
					}
					return priv, nil
				}
			}
		}
	}

	privKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal identity: %w", err)
	}

	ensureDir(identityPath)
	if err := os.WriteFile(identityPath, data, 0600); err != nil {
		slog.Warn("failed to save identity", "path", identityPath, "error", err)
	} else {
		slog.Info("saved new persistent identity", "path", identityPath)
	}

	return privKey, nil
}

// ResolveNodeName determines the node's human-readable name. Priority:
//  1. DLOCKSS_NODE_NAME env var
//  2. Persisted file at config.NodeNamePath
//  3. Interactive prompt on stdin
func ResolveNodeName(cfg *config.Config) string {
	if cfg.NodeName != "" {
		if err := persistNodeName(cfg, cfg.NodeName); err != nil {
			slog.Warn("failed to persist node name", "error", err)
		}
		return cfg.NodeName
	}
	nameFile := cfg.NodeNamePath
	if data, err := os.ReadFile(nameFile); err == nil {
		if name := strings.TrimSpace(string(data)); name != "" {
			slog.Info("loaded node name", "path", nameFile, "name", name)
			return name
		}
	}
	fmt.Print("Enter a name for this node (or press Enter to skip): ")
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		if name := strings.TrimSpace(scanner.Text()); name != "" {
			if err := persistNodeName(cfg, name); err != nil {
				slog.Warn("failed to persist node name", "error", err)
			}
			return name
		}
	}
	return ""
}

func persistNodeName(cfg *config.Config, name string) error {
	nameFile := cfg.NodeNamePath
	ensureDir(nameFile)
	if err := os.WriteFile(nameFile, []byte(name+"\n"), 0644); err != nil {
		return fmt.Errorf("persist node name to %s: %w", nameFile, err)
	}
	slog.Info("persisted node name", "path", nameFile, "name", name)
	return nil
}

func ensureDir(filePath string) {
	if dir := filepath.Dir(filePath); dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
}
