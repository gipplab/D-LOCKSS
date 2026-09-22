package trust

import (
	"os"
	"path/filepath"
	"testing"

	"dlockss/internal/testutil"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
)

func TestAllowsOrigin_openByDefault(t *testing.T) {
	tm := NewTrustManager("open")
	if tm.RestrictsOrigins() {
		t.Fatal("open mode with no store should not restrict")
	}
	if !tm.AllowsOrigin(testutil.MustPeerID(t, "anyone")) {
		t.Fatal("open mode should accept any origin")
	}
}

func TestAllowsOrigin_fileEnablesRestriction(t *testing.T) {
	trusted := testutil.MustPeerID(t, "trusted")
	other := testutil.MustPeerID(t, "other")

	dir := t.TempDir()
	path := filepath.Join(dir, "trusted_peers.json")
	if err := os.WriteFile(path, []byte(`{"peers":["`+trusted.String()+`"]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	tm := NewTrustManager("open")
	if err := tm.LoadTrustedPeers(path); err != nil {
		t.Fatal(err)
	}
	if !tm.RestrictsOrigins() {
		t.Fatal("non-empty trust store should restrict data origins")
	}
	if !tm.AllowsOrigin(trusted) {
		t.Fatal("listed peer should be allowed")
	}
	if tm.AllowsOrigin(other) {
		t.Fatal("unlisted peer should be refused")
	}
}

func TestAllowsOrigin_emptyListIsOpen(t *testing.T) {
	tm := NewTrustManager("allowlist")
	if tm.RestrictsOrigins() {
		t.Fatal("empty list should stay in default/open mode")
	}
	if !tm.AllowsOrigin(testutil.MustPeerID(t, "anyone")) {
		t.Fatal("empty list must accept every origin")
	}
}

func TestLoadTrustedPeers_emptyFileIsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trusted_peers.json")
	if err := os.WriteFile(path, []byte(`{"peers":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	tm := NewTrustManager("open")
	if err := tm.LoadTrustedPeers(path); err != nil {
		t.Fatal(err)
	}
	if tm.RestrictsOrigins() {
		t.Fatal("empty trusted_peers.json should stay in default/open mode")
	}
}

func TestAddOrigins_invalidIDsStayOpen(t *testing.T) {
	tm := NewTrustManager("open")
	added, invalid := tm.AddOrigins([]string{"not-a-peer-id"})
	if added != 0 || len(invalid) != 1 {
		t.Fatalf("added=%d invalid=%v", added, invalid)
	}
	if tm.RestrictsOrigins() {
		t.Fatal("invalid-only list should stay in default/open mode")
	}
	if !tm.AllowsOrigin(testutil.MustPeerID(t, "anyone")) {
		t.Fatal("invalid-only list must accept every origin")
	}
}

func TestAddOrigins_nilDoesNotRestrict(t *testing.T) {
	tm := NewTrustManager("open")
	added, invalid := tm.AddOrigins(nil)
	if added != 0 || invalid != nil {
		t.Fatalf("added=%d invalid=%v", added, invalid)
	}
	if tm.RestrictsOrigins() {
		t.Fatal("unset ingest allowlist must not restrict")
	}
}

func TestLoadTrustedPeers_missingFile(t *testing.T) {
	tm := NewTrustManager("open")
	err := tm.LoadTrustedPeers(filepath.Join(t.TempDir(), "missing.json"))
	if !os.IsNotExist(err) {
		t.Fatalf("want IsNotExist, got %v", err)
	}
}

func TestCheckManifestOrigin(t *testing.T) {
	trusted := testutil.MustPeerID(t, "trusted")
	other := testutil.MustPeerID(t, "other")
	payload, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err != nil {
		t.Fatal(err)
	}
	ro := schema.NewResearchObject("file://x", trusted, payload, 1)
	block, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}

	tm := NewTrustManager("open")
	tm.AddOrigins([]string{trusted.String()})

	if _, err := CheckManifestOrigin(block, tm); err != nil {
		t.Fatalf("trusted origin: %v", err)
	}

	ro2 := schema.NewResearchObject("file://x", other, payload, 1)
	block2, err := ro2.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CheckManifestOrigin(block2, tm); err == nil {
		t.Fatal("untrusted origin should fail")
	}
	if _, err := CheckManifestOrigin(nil, tm); err == nil {
		t.Fatal("empty block should fail when restricted")
	}
	if _, err := CheckManifestOrigin(block2, nil); err != nil {
		t.Fatalf("nil gate should allow: %v", err)
	}
}

func TestAuthorizeIncomingSender_allowlist(t *testing.T) {
	trusted := testutil.MustPeerID(t, "trusted")
	other := testutil.MustPeerID(t, "other")
	tm := NewTrustManager("allowlist")
	if err := tm.AuthorizeIncomingSender(other, other); err != nil {
		t.Fatalf("empty allowlist is open: %v", err)
	}
	tm.AddOrigins([]string{trusted.String()})

	if err := tm.AuthorizeIncomingSender(trusted, trusted); err != nil {
		t.Fatalf("trusted sender: %v", err)
	}
	if err := tm.AuthorizeIncomingSender(other, other); err == nil {
		t.Fatal("untrusted sender should be rejected in allowlist mode")
	}
}
