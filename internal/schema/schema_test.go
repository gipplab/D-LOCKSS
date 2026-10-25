package schema

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func testCID(t *testing.T, data string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func testPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(bytes.NewReader(make([]byte, 64)))
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestResearchObjectRoundtrip(t *testing.T) {
	ro := &ResearchObject{
		MetadataRef: "file://paper.pdf",
		IngestedBy:  testPeer(t),
		Signature:   []byte{1, 2, 3},
		Payload:     testCID(t, "payload"),
		TotalSize:   12345,
	}
	raw, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	var got ResearchObject
	if err := got.UnmarshalCBOR(raw); err != nil {
		t.Fatal(err)
	}
	if got.MetadataRef != ro.MetadataRef || got.IngestedBy != ro.IngestedBy ||
		!bytes.Equal(got.Signature, ro.Signature) || got.Payload != ro.Payload ||
		got.TotalSize != ro.TotalSize {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", got, ro)
	}
}

func TestResearchObjectDeterministic(t *testing.T) {
	p := testPeer(t)
	a := &ResearchObject{MetadataRef: "x", IngestedBy: p, Payload: testCID(t, "p"), TotalSize: 1}
	b := &ResearchObject{MetadataRef: "x", IngestedBy: p, Payload: testCID(t, "p"), TotalSize: 1}
	ra, _ := a.MarshalCBOR()
	rb, _ := b.MarshalCBOR()
	if !bytes.Equal(ra, rb) {
		t.Fatal("identical manifests must encode identically (deterministic CIDs)")
	}
}

func TestEncodingIsCanonical(t *testing.T) {
	// Wire compatibility: the DAG-CBOR encoder canonicalizes map key order
	// (length first, then bytewise) exactly like the original implementation,
	// so identical manifests yield identical bytes — and identical CIDs —
	// regardless of assembly order.
	fields := []field{
		{"meta_ref", "m"},
		{"ingester_id", "peer"},
		{"sig", []byte{1}},
		{"payload", "cid"},
		{"size", int64(7)},
	}
	reversed := make([]field, len(fields))
	for i, f := range fields {
		reversed[len(fields)-1-i] = f
	}
	a, err := encodeMap(fields)
	if err != nil {
		t.Fatal(err)
	}
	b, err := encodeMap(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("encoding must be canonical: assembly order must not change the bytes")
	}
}

func TestSigningEncodingExcludesSig(t *testing.T) {
	ro := &ResearchObject{MetadataRef: "m", IngestedBy: testPeer(t), Payload: testCID(t, "p"), Signature: []byte("SIGBYTES")}
	forSigning, err := ro.MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(forSigning, []byte("sig")) {
		t.Fatal("for-signing encoding must not contain the sig field")
	}
}

func TestMessageRoundtripAndDispatch(t *testing.T) {
	p := testPeer(t)
	im := &IngestMessage{ShardID: "01"}
	im.Env = Envelope{
		Type:        MessageTypeIngest,
		ManifestCID: testCID(t, "manifest"),
		SenderID:    p,
		Timestamp:   1234567,
		Nonce:       []byte{9, 9},
		Sig:         []byte{7},
	}
	raw, err := im.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := msg.(*IngestMessage)
	if !ok {
		t.Fatalf("decoded wrong type %T", msg)
	}
	if got.ShardID != "01" || got.Env.Type != im.Env.Type ||
		got.Env.ManifestCID != im.Env.ManifestCID || got.Env.SenderID != p ||
		got.Env.Timestamp != im.Env.Timestamp ||
		!bytes.Equal(got.Env.Nonce, im.Env.Nonce) || !bytes.Equal(got.Env.Sig, im.Env.Sig) {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	rr := &ReplicationRequest{}
	rr.Env = im.Env
	rr.Env.Type = MessageTypeReplicationRequest
	raw, err = rr.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mustDecode(t, raw).(*ReplicationRequest); !ok {
		t.Fatal("replication request did not dispatch")
	}

}

func mustDecode(t *testing.T, raw []byte) Message {
	t.Helper()
	msg, err := DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}
