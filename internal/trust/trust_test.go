package trust

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"dlockss/internal/schema"
)

func newPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestOpenByDefault(t *testing.T) {
	m := NewManager("open")
	if m.RestrictsOrigins() {
		t.Fatal("empty set must be open")
	}
	if !m.AllowsOrigin(newPeer(t)) {
		t.Fatal("open mode must allow any origin")
	}
	if m.AllowsOrigin("") {
		t.Fatal("empty origin must never be allowed")
	}
}

func TestLoadStore(t *testing.T) {
	p := newPeer(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "trusted_peers.json")
	if err := os.WriteFile(path, []byte(`{"peers":["`+p.String()+`",""]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewManager("open")
	if err := m.LoadStore(path); err != nil {
		t.Fatal(err)
	}
	if !m.RestrictsOrigins() || !m.AllowsOrigin(p) {
		t.Fatal("loaded peer must be trusted")
	}
	if m.AllowsOrigin(newPeer(t)) {
		t.Fatal("unlisted peers must be refused once restricted")
	}
	// Invalid IDs fail the whole load.
	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte(`{"peers":["not-a-peer-id"]}`), 0o600)
	if err := NewManager("open").LoadStore(bad); err == nil {
		t.Fatal("invalid peer ID must fail the load")
	}
	// Missing files surface os.IsNotExist.
	if err := NewManager("open").LoadStore(filepath.Join(dir, "missing.json")); !os.IsNotExist(err) {
		t.Fatalf("missing store: %v", err)
	}
}

func TestAllowlistSenderAuthorization(t *testing.T) {
	trusted := newPeer(t)
	other := newPeer(t)
	m := NewManager("allowlist")
	// Empty allowlist behaves as open.
	if err := m.AuthorizeSender(other, other); err != nil {
		t.Fatalf("empty allowlist must authorize: %v", err)
	}
	m.AddOrigins([]string{trusted.String()})
	if err := m.AuthorizeSender(trusted, trusted); err != nil {
		t.Fatalf("listed sender refused: %v", err)
	}
	if err := m.AuthorizeSender(other, other); err == nil {
		t.Fatal("unlisted sender must be refused in allowlist mode")
	}
	if err := m.AuthorizeSender(trusted, other); err == nil {
		t.Fatal("sender/transport mismatch must be refused")
	}
	if err := m.AuthorizeSender("", ""); err == nil {
		t.Fatal("missing sender must be refused")
	}
	// Open mode never restricts senders by list.
	mo := NewManager("open")
	mo.AddOrigins([]string{trusted.String()})
	if err := mo.AuthorizeSender(other, other); err != nil {
		t.Fatalf("open mode must not restrict senders: %v", err)
	}
}

func TestCheckManifestOrigin(t *testing.T) {
	trusted := newPeer(t)
	m := NewManager("open")
	m.AddOrigins([]string{trusted.String()})

	if err := m.CheckManifestOrigin(nil); err == nil {
		t.Fatal("empty block must fail closed")
	}
	if err := m.CheckManifestOrigin([]byte{0xff, 0x00}); err == nil {
		t.Fatal("undecodable block must fail closed")
	}
	mh, err := multihash.Sum([]byte("payload"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	ro := &schema.ResearchObject{MetadataRef: "m", IngestedBy: trusted, Payload: cid.NewCidV1(cid.Raw, mh)}
	raw, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.CheckManifestOrigin(raw); err != nil {
		t.Fatalf("trusted ingester refused: %v", err)
	}
	roBad := &schema.ResearchObject{MetadataRef: "m", IngestedBy: newPeer(t), Payload: ro.Payload}
	rawBad, _ := roBad.MarshalCBOR()
	if err := m.CheckManifestOrigin(rawBad); err == nil {
		t.Fatal("untrusted ingester must be refused")
	}
	// No restriction: everything passes.
	if err := NewManager("open").CheckManifestOrigin([]byte{0x00}); err != nil {
		t.Fatal("unrestricted gate must pass everything")
	}
}
