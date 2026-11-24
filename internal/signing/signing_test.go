package signing

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"

	"dlockss/internal/config"
	"dlockss/internal/testutil"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"
)

func newReceiverHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// signedIngest returns an IngestMessage signed by helper carrying b.
func signedIngest(t *testing.T, helper testutil.Key, b *schema.Binding) *schema.IngestMessage {
	t.Helper()
	sender := NewSigner(SignerConfig{Cfg: config.DefaultConfig(), PrivKey: helper.Priv, PeerID: helper.ID, Binding: b})
	c, err := cid.Decode("bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am")
	if err != nil {
		t.Fatal(err)
	}
	im := &schema.IngestMessage{SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeIngest, ManifestCID: c}, ShardID: "0"}
	if err := sender.SignProtocolMessage(im); err != nil {
		t.Fatal(err)
	}
	return im
}

// newReceiver returns a strict Signer that knows the public keys of senders.
func newReceiver(t *testing.T, tm *trust.TrustManager, senders ...testutil.Key) *Signer {
	t.Helper()
	h := newReceiverHost(t)
	for _, s := range senders {
		if err := h.Peerstore().AddPubKey(s.ID, s.Priv.GetPublic()); err != nil {
			t.Fatal(err)
		}
	}
	return NewSigner(SignerConfig{Cfg: config.DefaultConfig(), Host: h, Nodes: trust.NewNodes(tm)})
}

func TestAuthenticate_BoundHelperResolvesToNode(t *testing.T) {
	node, helper := testutil.NewKey(t), testutil.NewKey(t)
	rx := newReceiver(t, trust.NewTrustManager("open"), helper)

	im := signedIngest(t, helper, node.Bind(t, helper.ID))
	got, ok := rx.Authenticate(helper.ID, im, "test")
	if !ok || got != node.ID {
		t.Fatalf("Authenticate = %v, %v; want node %v", got, ok, node.ID)
	}
}

func TestAuthenticate_CopiedBindingDropped(t *testing.T) {
	node, helper, thief := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)
	rx := newReceiver(t, trust.NewTrustManager("open"), thief)

	im := signedIngest(t, thief, node.Bind(t, helper.ID))
	if got, ok := rx.Authenticate(thief.ID, im, "test"); ok {
		t.Fatalf("a binding for another helper must not verify, got node %v", got)
	}
}

func TestAuthenticate_BindingCoveredBySignature(t *testing.T) {
	node, other, helper := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)
	rx := newReceiver(t, trust.NewTrustManager("open"), helper)

	im := signedIngest(t, helper, node.Bind(t, helper.ID))
	// Swap in a binding that verifies on its own but was not signed.
	b := other.Bind(t, helper.ID)
	im.NodeID, im.NodeSig = b.Node, b.Sig
	if _, ok := rx.Authenticate(helper.ID, im, "test"); ok {
		t.Fatal("changing the binding after signing must break the envelope signature")
	}
	// The rejected message must not have bound the helper to the other node.
	if got, _ := rx.nodes.Resolve(helper.ID, nil); got == other.ID {
		t.Fatal("binding of a rejected message was remembered")
	}
}

func TestAuthenticate_SenderMismatchDropped(t *testing.T) {
	node, helper, relay := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)
	rx := newReceiver(t, trust.NewTrustManager("open"), helper)

	im := signedIngest(t, helper, node.Bind(t, helper.ID))
	if _, ok := rx.Authenticate(relay.ID, im, "test"); ok {
		t.Fatal("sender_id differing from the libp2p sender must be dropped")
	}
}

func TestAuthenticate_Allowlist(t *testing.T) {
	trusted, untrusted := testutil.NewKey(t), testutil.NewKey(t)
	helper, helper2, legacy := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)
	tm := trust.NewTrustManager("allowlist")
	tm.AddOrigins([]string{trusted.ID.String(), legacy.ID.String()})
	rx := newReceiver(t, tm, helper, helper2, legacy)

	if got, ok := rx.Authenticate(helper.ID, signedIngest(t, helper, trusted.Bind(t, helper.ID)), "test"); !ok || got != trusted.ID {
		t.Fatalf("helper of an allowlisted node: got %v, %v", got, ok)
	}
	if _, ok := rx.Authenticate(helper2.ID, signedIngest(t, helper2, untrusted.Bind(t, helper2.ID)), "test"); ok {
		t.Fatal("helper of a node off the allowlist must be dropped")
	}
	// A message without binding is dropped, even from a listed ID.
	if got, ok := rx.Authenticate(legacy.ID, signedIngest(t, legacy, nil), "test"); ok {
		t.Fatalf("unbound sender on the allowlist must be dropped, got %v", got)
	}
	if _, ok := rx.Authenticate(helper2.ID, signedIngest(t, helper2, nil), "test"); ok {
		t.Fatal("unbound sender off the allowlist must be dropped")
	}
}
