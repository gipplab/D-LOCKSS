// Package trust implements the two trust gates of D-LOCKSS:
//
//   - Origin restriction: when the trusted set is non-empty (in ANY mode),
//     the node stores only content whose manifest ingester is on the list.
//   - Sender authorization: in "allowlist" mode with a non-empty set,
//     protocol messages are accepted only from listed peers.
//
// An empty or missing list means open mode: accept everyone.
package trust

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/schema"
)

// Mode selects how far trust is enforced.
type Mode string

const (
	ModeOpen      Mode = "open"
	ModeAllowlist Mode = "allowlist"
)

// Manager holds the trusted peer set and mode.
type Manager struct {
	mode Mode

	mu      sync.RWMutex
	trusted map[peer.ID]struct{}
}

// NewManager creates a Manager; unknown modes fall back to open.
func NewManager(mode string) *Manager {
	m := Mode(mode)
	if m != ModeAllowlist {
		m = ModeOpen
	}
	return &Manager{mode: m, trusted: map[peer.ID]struct{}{}}
}

// storeFormat is the trusted_peers.json schema.
type storeFormat struct {
	Peers []string `json:"peers"`
}

// LoadStore reads trusted_peers.json. A missing file is not an error; an
// invalid peer ID fails the whole load (fail closed on malformed config).
func (m *Manager) LoadStore(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err // callers treat os.IsNotExist as fine
	}
	var s storeFormat
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}
	ids := make([]peer.ID, 0, len(s.Peers))
	for _, v := range s.Peers {
		if v == "" {
			continue
		}
		id, err := peer.Decode(v)
		if err != nil {
			return fmt.Errorf("invalid peer ID %q in %s: %w", v, path, err)
		}
		ids = append(ids, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.trusted[id] = struct{}{}
	}
	return nil
}

// AddOrigins merges peer ID strings (e.g. from DLOCKSS_INGEST_ALLOWLIST)
// into the trusted set; invalid IDs are skipped with a warning.
func (m *Manager) AddOrigins(ids []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range ids {
		id, err := peer.Decode(v)
		if err != nil {
			slog.Warn("ignoring invalid peer ID in ingest allowlist", "id", v, "err", err)
			continue
		}
		m.trusted[id] = struct{}{}
	}
}

// Mode returns the configured trust mode.
func (m *Manager) Mode() Mode { return m.mode }

// Len returns the size of the trusted set.
func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.trusted)
}

func (m *Manager) has(id peer.ID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.trusted[id]
	return ok
}

// RestrictsOrigins reports whether the origin gate is active.
func (m *Manager) RestrictsOrigins() bool { return m.Len() > 0 }

// AllowsOrigin reports whether content ingested by the given peer may be
// stored. An empty ID is never allowed; with no restriction everyone is.
func (m *Manager) AllowsOrigin(id peer.ID) bool {
	if id == "" {
		return false
	}
	if !m.RestrictsOrigins() {
		return true
	}
	return m.has(id)
}

// AuthorizeSender checks a protocol message's sender: it must be present,
// match the pubsub transport peer when known, and in allowlist mode with a
// non-empty set, be listed.
func (m *Manager) AuthorizeSender(sender, receivedFrom peer.ID) error {
	if sender == "" {
		return fmt.Errorf("message has no sender")
	}
	if receivedFrom != "" && sender != receivedFrom {
		return fmt.Errorf("sender %s does not match transport peer %s", sender, receivedFrom)
	}
	if m.mode == ModeAllowlist && m.Len() > 0 && !m.has(sender) {
		return fmt.Errorf("sender %s is not on the allowlist", sender)
	}
	return nil
}

// CheckManifestOrigin verifies a raw manifest block against the origin gate.
// With no restriction it always passes; otherwise it fails closed on empty
// or undecodable blocks and unlisted ingesters.
func (m *Manager) CheckManifestOrigin(block []byte) error {
	if !m.RestrictsOrigins() {
		return nil
	}
	if len(block) == 0 {
		return fmt.Errorf("empty manifest block")
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return fmt.Errorf("undecodable manifest: %w", err)
	}
	if ro.IngestedBy == "" {
		return fmt.Errorf("manifest has no ingester")
	}
	if !m.has(ro.IngestedBy) {
		return fmt.Errorf("ingester %s is not trusted", ro.IngestedBy)
	}
	return nil
}

// TrustedPeers returns a snapshot of the trusted set.
func (m *Manager) TrustedPeers() []peer.ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]peer.ID, 0, len(m.trusted))
	for id := range m.trusted {
		out = append(out, id)
	}
	return out
}
