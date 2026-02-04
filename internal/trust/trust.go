package trust

import (
	"encoding/json"
	"fmt"
	"os"

	"dlockss/internal/common"

	"github.com/libp2p/go-libp2p/core/peer"
)

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

func (tm *TrustManager) IsPeerTrusted(peerID peer.ID) bool {
	return tm.trustedPeers.Has(peerID)
}

func (tm *TrustManager) GetTrustedPeers() []peer.ID {
	return tm.trustedPeers.All()
}

// AuthorizeIncomingSender enforces:
// - SenderID must match libp2p's ReceivedFrom (prevents in-message spoofing)
// - If TrustMode == "allowlist", sender must be present in trust store
func (tm *TrustManager) AuthorizeIncomingSender(receivedFrom peer.ID, peerID peer.ID) error {
	if peerID == "" {
		return fmt.Errorf("missing sender_id")
	}
	if receivedFrom != "" && peerID != receivedFrom {
		return fmt.Errorf("sender_id mismatch: sender_id=%s received_from=%s", peerID.String(), receivedFrom.String())
	}
	if tm.trustMode == "allowlist" && !tm.IsPeerTrusted(peerID) {
		return fmt.Errorf("sender not trusted: %s", peerID.String())
	}
	return nil
}

// AuthorizePeer enforces allowlist-only mode for contexts where there is no ReceivedFrom
// (e.g., verifying a stored ResearchObject manifest).
func (tm *TrustManager) AuthorizePeer(peerID peer.ID) error {
	if peerID == "" {
		return fmt.Errorf("missing sender_id")
	}
	if tm.trustMode == "allowlist" && !tm.IsPeerTrusted(peerID) {
		return fmt.Errorf("sender not trusted: %s", peerID.String())
	}
	return nil
}
