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
	if ok, err := w.Trusted(ctx, trusted); ok || err == nil {
		t.Fatalf("answers are not cached: while the wiki is down no key is trusted, got %v, %v", ok, err)
	}
}
