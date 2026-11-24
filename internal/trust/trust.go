package trust

import (
	"encoding/json"
	"fmt"
	"os"

	"dlockss/internal/common"

	"github.com/libp2p/go-libp2p/core/peer"
)

// OriginGate decides whether this node will store data created by a given peer.
type OriginGate interface {
	RestrictsOrigins() bool
	AllowsOrigin(id peer.ID) bool
}

type TrustManager struct {
	trustedPeers *common.TrustedPeers
	trustMode    string
}

func NewTrustManager(mode string) *TrustManager {
	return &TrustManager{
		trustedPeers: common.NewTrustedPeers(),
		trustMode:    mode,
	}
}

type trustedPeersFile struct {
	Peers []string `json:"peers"`
}

// LoadTrustedPeers replaces the trusted set with the peers listed in the JSON
// trust store at path. Read errors are returned unwrapped so callers can test
// them with os.IsNotExist.
func (tm *TrustManager) LoadTrustedPeers(path string) error {
	if path == "" {
		return fmt.Errorf("trust store path is empty")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var f trustedPeersFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("failed to parse trust store JSON: %w", err)
	}

	parsed := make(map[peer.ID]bool, len(f.Peers))
	for _, s := range f.Peers {
		if s == "" {
			continue
		}
		pid, err := peer.Decode(s)
		if err != nil {
			return fmt.Errorf("invalid peer id in trust store: %q: %w", s, err)
		}
		parsed[pid] = true
	}

	tm.trustedPeers.SetAll(parsed)
	return nil
}

// AddOrigins merges extra peer IDs (e.g. DLOCKSS_INGEST_ALLOWLIST) into the
// trusted set. Invalid IDs are skipped. Restriction is on only when at least
// one valid peer is present; an empty list is default/open mode.
func (tm *TrustManager) AddOrigins(raw []string) (added int, invalid []string) {
	for _, s := range raw {
		if s == "" {
			continue
		}
		pid, err := peer.Decode(s)
		if err != nil {
			invalid = append(invalid, s)
			continue
		}
		tm.trustedPeers.Add(pid)
		added++
	}
	return added, invalid
}

func (tm *TrustManager) GetTrustedPeers() []peer.ID {
	return tm.trustedPeers.All()
}

func (tm *TrustManager) RestrictsOrigins() bool {
	return tm.trustedPeers.Len() > 0
}

func (tm *TrustManager) AllowsOrigin(id peer.ID) bool {
	if !tm.RestrictsOrigins() {
		return true
	}
	if id == "" {
		return false
	}
	return tm.trustedPeers.Has(id)
}

// acceptsNode reports whether messages of node count: in allowlist mode with a
// non-empty allowlist, node must be on it.
func (tm *TrustManager) acceptsNode(node peer.ID) bool {
	return tm.trustMode != "allowlist" || !tm.RestrictsOrigins() || tm.trustedPeers.Has(node)
}
