package ipfs

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientToken(t *testing.T) {
	for token, want := range map[string]string{"": "", "s3cret": "Bearer s3cret"} {
		var got string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"Version":"0.43.1"}`))
		}))
		if _, err := NewClient(srv.URL, token); err != nil {
			t.Fatal(err)
		}
		srv.Close()
		if got != want {
			t.Errorf("token %q: Authorization = %q, want %q", token, got, want)
		}
	}
}
