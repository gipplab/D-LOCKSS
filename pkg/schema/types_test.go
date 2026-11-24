package schema

import (
	"bytes"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
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

func mustDecode[T any](t *testing.T, decode func(string) (T, error), s string) T {
	t.Helper()
	v, err := decode(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
