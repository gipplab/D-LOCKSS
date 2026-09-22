package ipfs

import (
	"testing"

	ipfsapi "github.com/ipfs/go-ipfs-api"
)

func TestUnixfsV1AddOpts(t *testing.T) {
	if unixfsV1Chunker != "size-1048576" || unixfsV1Hash != "sha2-256" ||
		unixfsV1CidVersion != 1 || unixfsV1MaxFileLinks != 1024 {
		t.Fatal("unixfs-v1-2025 constants drifted from IPIP-0499")
	}
	opts := unixfsV1AddOpts()
	if len(opts) != 7 {
		t.Fatalf("got %d add options, want 7", len(opts))
	}
	rb := &ipfsapi.RequestBuilder{}
	for _, opt := range opts {
		if err := opt(rb); err != nil {
			t.Fatal(err)
		}
	}
}
