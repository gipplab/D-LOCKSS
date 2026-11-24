package trust

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	trustedKey   = "12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb"
	untrustedKey = "12D3KooWCFyKj1WuxUpsNdCNqzTAcZpSCNxCZ3ybSH5dVssV4YVT"
	unknownKey   = "12D3KooWLb7WFH45GCF4ZKvxvH1ZbMvWDqBvsGYtMpsCACvuLSiC"
)

func TestWikiKeys(t *testing.T) {
	down := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case down:
			w.WriteHeader(http.StatusBadGateway)
		case r.URL.Path == "/w/rest.php/math/v0/key/"+trustedKey:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	w := NewWikiKeys(srv.URL + "/w/rest.php/")
	ctx := context.Background()
	trusted, _ := peer.Decode(trustedKey)
	untrusted, _ := peer.Decode(untrustedKey)

	if ok, err := w.Trusted(ctx, trusted); !ok || err != nil {
		t.Fatalf("204 means trusted, got %v, %v", ok, err)
	}
	if ok, err := w.Trusted(ctx, untrusted); ok || err != nil {
		t.Fatalf("404 means not trusted, got %v, %v", ok, err)
	}

	down = true
	w.answers[trusted] = wikiAnswer{trusted: true}
	if ok, err := w.Trusted(ctx, trusted); !ok || err != nil {
		t.Fatalf("while the wiki is down, its last answer counts, got %v, %v", ok, err)
	}
	unknown, _ := peer.Decode(unknownKey)
	if _, err := w.Trusted(ctx, unknown); err == nil {
		t.Fatal("a key the wiki never answered for cannot be checked while it is down")
	}
}
