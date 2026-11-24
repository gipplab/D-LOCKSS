package schema

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testEd25519(t *testing.T) (crypto.PrivKey, peer.ID) {
	t.Helper()
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return priv, id
}

func kuboSign(t *testing.T, priv crypto.PrivKey, msg []byte) []byte {
	t.Helper()
	sig, err := priv.Sign(append([]byte(KuboSignPrefix), msg...))
	if err != nil {
		t.Fatal(err)
	}
	return sig
}

func TestBinding_Verify(t *testing.T) {
	nodePriv, nodeID := testEd25519(t)
	_, helper := testEd25519(t)
	b := &Binding{Node: nodeID, Sig: kuboSign(t, nodePriv, BindingMessage(helper))}
	if err := b.Verify(helper); err != nil {
		t.Fatalf("valid binding: %v", err)
	}
}

func TestBinding_WrongHelper(t *testing.T) {
	nodePriv, nodeID := testEd25519(t)
	_, helper := testEd25519(t)
	_, other := testEd25519(t)
	b := &Binding{Node: nodeID, Sig: kuboSign(t, nodePriv, BindingMessage(helper))}
	if err := b.Verify(other); err != ErrBindingSignature {
		t.Fatalf("copied binding: got %v, want ErrBindingSignature", err)
	}
}

func TestBinding_WrongNodeKey(t *testing.T) {
	_, nodeID := testEd25519(t)
	otherPriv, _ := testEd25519(t)
	_, helper := testEd25519(t)
	b := &Binding{Node: nodeID, Sig: kuboSign(t, otherPriv, BindingMessage(helper))}
	if err := b.Verify(helper); err != ErrBindingSignature {
		t.Fatalf("wrong key: got %v, want ErrBindingSignature", err)
	}
}

func TestBinding_RSANodeID(t *testing.T) {
	priv, pub, err := crypto.GenerateRSAKeyPair(2048, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	_, helper := testEd25519(t)
	// RSA peer IDs do not embed the public key, so verification cannot run.
	b := &Binding{Node: nodeID, Sig: kuboSign(t, priv, BindingMessage(helper))}
	if err := b.Verify(helper); err != ErrBindingSignature {
		t.Fatalf("RSA node: got %v, want ErrBindingSignature", err)
	}
}

func TestHeartbeat_RoundTripWithBinding(t *testing.T) {
	nodePriv, nodeID := testEd25519(t)
	_, helper := testEd25519(t)
	b := &Binding{Node: nodeID, Sig: kuboSign(t, nodePriv, BindingMessage(helper))}
	raw := (&Heartbeat{Helper: helper, Pinned: 3, Role: "ACTIVE", Binding: b, Name: "pi1:lab"}).Marshal()
	got := ParseHeartbeat(raw)
	if got.Helper != helper || got.Pinned != 3 || got.Role != "ACTIVE" || got.Name != "pi1:lab" {
		t.Fatalf("parse: %+v", got)
	}
	if got.Binding == nil || got.Binding.Node != nodeID {
		t.Fatalf("binding: %+v", got.Binding)
	}
	if err := got.Binding.Verify(helper); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeat_LegacyWithoutBinding(t *testing.T) {
	_, helper := testEd25519(t)
	raw := []byte(HeartbeatPrefix + helper.String() + ":2:ACTIVE:old-node")
	got := ParseHeartbeat(raw)
	if got.Binding != nil || got.Name != "old-node" || got.Pinned != 2 {
		t.Fatalf("legacy: %+v", got)
	}
}

func TestJoinAndProbe_RoundTripWithBinding(t *testing.T) {
	nodePriv, nodeID := testEd25519(t)
	_, helper := testEd25519(t)
	b := &Binding{Node: nodeID, Sig: kuboSign(t, nodePriv, BindingMessage(helper))}

	j := ParseJoin((&Join{Helper: helper, Role: "PASSIVE", Binding: b, Name: "a:b"}).Marshal())
	if j.Helper != helper || j.Role != "PASSIVE" || j.Name != "a:b" || j.Binding == nil || j.Binding.Verify(helper) != nil {
		t.Fatalf("join: %+v", j)
	}
	p := ParseProbe((&Probe{Helper: helper, Binding: b}).Marshal())
	if p.Helper != helper || p.Binding == nil || p.Binding.Verify(helper) != nil {
		t.Fatalf("probe: %+v", p)
	}
	if got := AnnouncementBinding((&Probe{Helper: helper, Binding: b}).Marshal()); got == nil || got.Node != nodeID {
		t.Fatalf("AnnouncementBinding(probe) = %+v", got)
	}
}

func TestJoinAndProbe_Legacy(t *testing.T) {
	_, helper := testEd25519(t)
	j := ParseJoin([]byte(JoinPrefix + helper.String() + ":ACTIVE:old:name"))
	if j.Binding != nil || j.Role != "ACTIVE" || j.Name != "old:name" {
		t.Fatalf("legacy join: %+v", j)
	}
	p := ParseProbe([]byte(ProbePrefix + helper.String()))
	if p.Helper != helper || p.Binding != nil {
		t.Fatalf("legacy probe: %+v", p)
	}
	if AnnouncementBinding([]byte("LEAVE:"+helper.String())) != nil {
		t.Fatal("LEAVE carries no binding")
	}
}

func TestSignedEnvelope_BindingRoundTrip(t *testing.T) {
	nodePriv, nodeID := testEd25519(t)
	_, helper := testEd25519(t)
	sig := kuboSign(t, nodePriv, BindingMessage(helper))
	payload := mustDecode(t, cid.Decode, kuboPayload)

	im := IngestMessage{SignedEnvelope: SignedEnvelope{
		Type: MessageTypeIngest, ManifestCID: payload, SenderID: helper,
		NodeID: nodeID, NodeSig: sig, Timestamp: 1, Nonce: []byte("nonce"), Sig: []byte("sig"),
	}, ShardID: "01"}
	raw, err := im.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	var got IngestMessage
	if err := got.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	b := got.Binding()
	if b == nil || b.Node != nodeID || b.Verify(helper) != nil {
		t.Fatalf("binding lost in round trip: %+v", b)
	}

	// The binding is covered by the envelope signature.
	unsigned, err := im.MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	im.NodeID = helper
	tampered, err := im.MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(unsigned, tampered) {
		t.Fatal("node_id is not part of the signed bytes")
	}
}

func TestSignedEnvelope_LegacyWithoutBinding(t *testing.T) {
	_, helper := testEd25519(t)
	rr := ReplicationRequest{SignedEnvelope: SignedEnvelope{
		Type: MessageTypeReplicationRequest, ManifestCID: mustDecode(t, cid.Decode, kuboPayload),
		SenderID: helper, Timestamp: 1, Nonce: []byte("nonce"), Sig: []byte("sig"),
	}}
	raw, err := rr.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("node_id")) {
		t.Fatal("a message without binding must not carry node_id")
	}
	var got ReplicationRequest
	if err := got.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	if got.Binding() != nil || got.SenderID != helper {
		t.Fatalf("legacy envelope: %+v", got.SignedEnvelope)
	}
}
