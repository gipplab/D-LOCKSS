package shard

import (
	"context"
	"testing"

	"dlockss/internal/testutil"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type originIPFS struct {
	testutil.MockIPFSClient
	block []byte
	err   error
}

func (m *originIPFS) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.block, nil
}

func TestIsAuthorizedIngestor_usesOriginGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	trusted := testutil.MustPeerID(t, "trusted")
	other := testutil.MustPeerID(t, "other")
	tm := trust.NewTrustManager("open")
	tm.AddOrigins([]string{trusted.String()})
	sm.origin = tm

	if !sm.isAuthorizedIngestor(trusted) {
		t.Fatal("trusted peer should be an authorized ingestor")
	}
	if sm.isAuthorizedIngestor(other) {
		t.Fatal("untrusted peer should not be an authorized ingestor")
	}
	if sm.IsLocalNodeIngestor() {
		t.Fatal("local node must not ingest when not on the trusted list")
	}
}

func TestAllowsManifestCID_checksIngestedBy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	trusted := testutil.MustPeerID(t, "trusted")
	other := testutil.MustPeerID(t, "other")
	payload, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err != nil {
		t.Fatal(err)
	}
	ro := schema.NewResearchObject("file://x", other, payload, 1)
	block, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}

	tm := trust.NewTrustManager("open")
	tm.AddOrigins([]string{trusted.String()})
	sm.origin = tm
	sm.ipfsClient = &originIPFS{block: block}

	if sm.allowsManifestCID(ctx, payload) {
		t.Fatal("manifest ingested by untrusted peer must be refused")
	}

	ro2 := schema.NewResearchObject("file://x", trusted, payload, 1)
	block2, err := ro2.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	sm.ipfsClient = &originIPFS{block: block2}
	if !sm.allowsManifestCID(ctx, payload) {
		t.Fatal("manifest ingested by trusted peer must be accepted")
	}
}

func TestAllowsManifestCID_openWhenNoGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")
	c, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	if err != nil {
		t.Fatal(err)
	}
	if !sm.allowsManifestCID(ctx, c) {
		t.Fatal("unrestricted node must accept data")
	}
}

func TestIsAuthorizedIngestor_openWhenNoGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")
	if !sm.isAuthorizedIngestor(peer.ID("anyone")) {
		t.Fatal("unrestricted node must accept ingest announcements")
	}
}
