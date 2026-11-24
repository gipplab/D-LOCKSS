package ipfs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewClientToken(t *testing.T) {
	for token, want := range map[string]string{"": "", "s3cret": "Bearer s3cret"} {
		var got string
		var keepAlive bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("Authorization")
			keepAlive = !r.Close
			_, _ = w.Write([]byte(`{"Version":"0.43.1"}`))
		}))
		if _, err := NewClient(srv.URL, token); err != nil {
			t.Fatal(err)
		}
		srv.Close()
		if got != want {
			t.Errorf("token %q: Authorization = %q, want %q", token, got, want)
		}
		if keepAlive {
			t.Errorf("token %q: connection kept alive, want closed like ipfsapi.NewShell", token)
		}
	}
}

func TestNewClientRefused(t *testing.T) {
	const denied = "Kubo RPC Access Denied: Please provide a valid authorization token as defined in the API.Authorizations configuration."
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, denied, http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, "wrong")
	if err == nil || !strings.Contains(err.Error(), denied) {
		t.Fatalf("want Kubo's refusal, got %v", err)
	}
	if strings.Contains(err.Error(), "ipfs daemon") {
		t.Errorf("a node that answered must not be reported as down: %v", err)
	}
}
