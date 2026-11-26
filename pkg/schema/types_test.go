package schema

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Made with Kubo 0.43.1: printf 'dlockss-upload <payload>' | ipfs key sign -k alice
const (
	kuboKey     = "12D3KooWNEd5gaYNKe9nAesWuQ1HMgyH3sjXrNQYgbBK1H1ixuN9"
	kuboPayload = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
	kuboSig     = "0UeavBivwyduao91YLSf41jA6XUNhOAMvh9JHLi1L4_DwV98Z5155wkOnkpCFAgFvcfe6XRiqhFFQDTM31kMBw"
)

func TestVerifyUpload_KuboSignature(t *testing.T) {
	key := mustDecode(t, peer.Decode, kuboKey)
	payload := mustDecode(t, cid.Decode, kuboPayload)
	sig, err := base64.RawURLEncoding.DecodeString(kuboSig)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyUpload(key, payload, sig); err != nil {
		t.Fatalf("a signature from 'ipfs key sign' must verify: %v", err)
	}
	h, err := multihash.Sum([]byte("other"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyUpload(key, cid.NewCidV1(cid.Raw, h), sig); !errors.Is(err, ErrUploadSignature) {
		t.Fatalf("the signature of another CID must not verify, got %v", err)
	}
}

func TestResearchObject_Uploader(t *testing.T) {
	ingester := mustDecode(t, peer.Decode, kuboKey)
	payload := mustDecode(t, cid.Decode, kuboPayload)

	plain, err := NewResearchObject("paper.pdf", ingester, payload, 6).MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plain, []byte("uploader")) {
		t.Fatal("a manifest without uploader must not change, so its CID stays the same")
	}

	ro := NewResearchObject("paper.pdf", ingester, payload, 6)
	ro.Uploader = ingester
	ro.UploaderSig = []byte{1, 2, 3}
	b, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	var got ResearchObject
	if err := got.UnmarshalCBOR(b); err != nil {
		t.Fatal(err)
	}
	if got.Uploader != ro.Uploader || !bytes.Equal(got.UploaderSig, ro.UploaderSig) {
		t.Fatalf("uploader lost in round trip: %v %v", got.Uploader, got.UploaderSig)
	}
}

func TestResearchObject_VerifySignature(t *testing.T) {
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ingester, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	payload := mustDecode(t, cid.Decode, kuboPayload)
	ro := NewResearchObject("paper.pdf", ingester, payload, 6)
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	ro.Signature, err = priv.Sign(append([]byte(KuboSignPrefix), unsigned...))
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.VerifySignature(); err != nil {
		t.Fatalf("kubo-signed manifest: %v", err)
	}
	ro.Signature[0] ^= 1
	if err := ro.VerifySignature(); !errors.Is(err, ErrManifestSignature) {
		t.Fatalf("tampered sig: got %v", err)
	}

	// Before helpers had their own keys, D-LOCKSS signed with the node key
	// directly, without Kubo's prefix.
	ro.Signature, err = priv.Sign(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.VerifySignature(); err != nil {
		t.Fatalf("manifest signed directly with the node key: %v", err)
	}
	other := NewResearchObject("other.pdf", ingester, payload, 6)
	other.Signature = ro.Signature
	if err := other.VerifySignature(); !errors.Is(err, ErrManifestSignature) {
		t.Fatalf("signature of another manifest: got %v", err)
	}
}

func mustDecode[T any](t *testing.T, decode func(string) (T, error), s string) T {
	t.Helper()
	v, err := decode(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestResearchObject_Uploaders(t *testing.T) {
	ingester := mustDecode(t, peer.Decode, kuboKey)
	payload := mustDecode(t, cid.Decode, kuboPayload)

	ro := NewResearchObject("paper.pdf", ingester, payload, 6)
	ro.Uploaders = []UploaderSignature{{ingester, []byte{1}}, {ingester, []byte{2}}}
	b, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	var got ResearchObject
	if err := got.UnmarshalCBOR(b); err != nil {
		t.Fatal(err)
	}
	if len(got.Uploaders) != 2 || got.Uploaders[1].Key != ingester || !bytes.Equal(got.Uploaders[1].Sig, []byte{2}) || got.Uploader != "" {
		t.Fatalf("uploaders lost in round trip: %+v", got)
	}
	again, err := got.MarshalCBOR()
	if err != nil || !bytes.Equal(again, b) {
		t.Fatal("re-encoding changes the manifest, so its signature would no longer verify")
	}
}

func TestResearchObject_OldUploaderStaysValid(t *testing.T) {
	ingester := mustDecode(t, peer.Decode, kuboKey)
	payload := mustDecode(t, cid.Decode, kuboPayload)

	old := NewResearchObject("paper.pdf", ingester, payload, 6)
	old.Uploader, old.UploaderSig = ingester, []byte{1, 2, 3}
	b, err := old.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	var got ResearchObject
	if err := got.UnmarshalCBOR(b); err != nil {
		t.Fatal(err)
	}
	if again, _ := got.MarshalCBOR(); !bytes.Equal(again, b) {
		t.Fatal("an old manifest re-encodes differently, so its signature would no longer verify")
	}
	if all := got.AllUploaders(); len(all) != 1 || all[0].Key != ingester || !bytes.Equal(all[0].Sig, []byte{1, 2, 3}) {
		t.Fatalf("AllUploaders = %+v", all)
	}
}
