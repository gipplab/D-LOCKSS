// Package identity resolves the node's libp2p private key and human-readable
// name. The key determines the Peer ID and therefore the node's shard path.
package identity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"

	"dlockss/internal/config"
)

// LoadKey resolves the private key in priority order:
//  1. The Kubo config at DLOCKSS_IPFS_CONFIG (Identity.PrivKey), so the node
//     shares the IPFS daemon's Peer ID. Any failure here is fatal.
//  2. The key file at IdentityPath (raw libp2p protobuf bytes).
//  3. A newly generated Ed25519 key, saved to IdentityPath.
func LoadKey(cfg *config.Config) (crypto.PrivKey, error) {
	if cfg.IPFSConfigPath != "" {
		key, err := keyFromKuboConfig(cfg.IPFSConfigPath)
		if err != nil {
			return nil, fmt.Errorf("DLOCKSS_IPFS_CONFIG is set but unusable: %w", err)
		}
		slog.Info("identity: using IPFS repo key", "config", cfg.IPFSConfigPath)
		return key, nil
	}

	if key, err := keyFromFile(cfg.IdentityPath); err == nil {
		return key, nil
	} else if !os.IsNotExist(err) {
		slog.Warn("identity: unreadable key file, generating a new key", "path", cfg.IdentityPath, "err", err)
	}

	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, fmt.Errorf("generating identity key: %w", err)
	}
	if err := saveKey(key, cfg.IdentityPath); err != nil {
		slog.Warn("identity: could not persist new key; the Peer ID will change on restart",
			"path", cfg.IdentityPath, "err", err)
	} else {
		slog.Info("identity: generated new key", "path", cfg.IdentityPath)
	}
	return key, nil
}

func keyFromKuboConfig(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var kubo struct {
		Identity struct {
			PrivKey string `json:"PrivKey"`
		} `json:"Identity"`
	}
	if err := json.Unmarshal(raw, &kubo); err != nil {
		return nil, fmt.Errorf("parsing Kubo config: %w", err)
	}
	if kubo.Identity.PrivKey == "" {
		return nil, fmt.Errorf("no Identity.PrivKey in %s", path)
	}
	der, err := base64.StdEncoding.DecodeString(kubo.Identity.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("decoding Identity.PrivKey: %w", err)
	}
	return crypto.UnmarshalPrivateKey(der)
}

func keyFromFile(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalPrivateKey(raw)
}

func saveKey(key crypto.PrivKey, path string) error {
	raw, err := crypto.MarshalPrivateKey(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// ResolveNodeName resolves the display name: DLOCKSS_NODE_NAME (persisted),
// then the node_name file. An empty result means the node is unnamed.
func ResolveNodeName(cfg *config.Config) string {
	if cfg.NodeName != "" {
		persistName(cfg.NodeNamePath, cfg.NodeName)
		return cfg.NodeName
	}
	if raw, err := os.ReadFile(cfg.NodeNamePath); err == nil {
		if name := strings.TrimSpace(string(raw)); name != "" {
			return name
		}
	}
	return ""
}

func persistName(path, name string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
		if err := os.WriteFile(path, []byte(name+"\n"), 0o644); err != nil {
			slog.Warn("could not persist node name", "path", path, "err", err)
		}
	}
}
