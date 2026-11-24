package ipfs

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// AuthSecretFromKuboConfig returns the API.Authorizations AuthSecret in the
// Kubo config at path: the entry named "dlockss", or else the only entry.
// It returns "" if the RPC API has no authorizations.
func AuthSecretFromKuboConfig(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read IPFS config: %w", err)
	}
	var cfg struct {
		API struct {
			Authorizations map[string]struct{ AuthSecret string }
		}
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("parse IPFS config: %w", err)
	}
	auths := cfg.API.Authorizations
	if a, ok := auths["dlockss"]; ok {
		return a.AuthSecret, nil
	}
	if len(auths) == 0 {
		return "", nil
	}
	if len(auths) == 1 {
		for _, a := range auths {
			return a.AuthSecret, nil
		}
	}
	return "", fmt.Errorf("IPFS config %s has %d API.Authorizations, none named \"dlockss\"; set DLOCKSS_IPFS_TOKEN", path, len(auths))
}

// authHeader converts an AuthSecret into an Authorization header value, as
// Kubo's config.ConvertAuthSecret does.
func authHeader(secret string) (string, error) {
	kind, value, found := strings.Cut(secret, ":")
	switch {
	case !found:
		return "Bearer " + secret, nil
	case kind == "bearer":
		return "Bearer " + value, nil
	case kind == "basic" && strings.Contains(value, ":"):
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(value)), nil
	case kind == "basic":
		return "Basic " + value, nil
	}
	return "", fmt.Errorf("unsupported auth secret type %q", kind)
}
