package trust

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"

	"dlockss/internal/testutil"
	"dlockss/pkg/schema"
)

// Made with Kubo 0.43.1: printf 'dlockss-upload <payload>' | ipfs key sign -k alice
const (
	kuboKey     = "12D3KooWNEd5gaYNKe9nAesWuQ1HMgyH3sjXrNQYgbBK1H1ixuN9"
	kuboPayload = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
	kuboSig     = "u0UeavBivwyduao91YLSf41jA6XUNhOAMvh9JHLi1L4_DwV98Z5155wkOnkpCFAgFvcfe6XRiqhFFQDTM31kMBw"
)

// kuboSign signs payload like 'ipfs key sign' with a new key.
func kuboSign(t *testing.T, payload cid.Cid) wikiSignature {
	t.Helper()
	key := testutil.NewKey(t)
	sig, err := key.SignWithNodeKey(context.Background(), []byte("dlockss-upload "+payload.String()))
	if err != nil {
		t.Fatal(err)
	}
	return wikiSignature{key.ID.String(), "u" + base64.RawURLEncoding.EncodeToString(sig)}
}

func TestWikiSignatures(t *testing.T) {
	payload := cid.MustParse(kuboPayload)
	answer := []wikiSignature{
		kuboSign(t, payload),
		{kuboKey, kuboSig},
		{kuboKey, kuboSig},      // twice
		{untrustedKey, kuboSig}, // signature of another key
		{kuboKey, "AAAA"},       // not multibase
		{"not-a-key", kuboSig},  // not a peer ID
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/w/rest.php/math/v0/signatures/"+kuboPayload {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}))
	defer srv.Close()

	got, err := NewWiki(srv.URL+"/w/rest.php/").Signatures(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key >= got[1].Key {
		t.Fatalf("got %+v; want the two valid signatures, once each, sorted by key", got)
	}
	for _, u := range got {
		if err := schema.VerifyUpload(u.Key, payload, u.Sig); err != nil {
			t.Fatalf("%s: %v", u.Key, err)
		}
	}
}

func TestWikiSignatures_NoneAndErrors(t *testing.T) {
	payload := cid.MustParse(kuboPayload)
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()
	ws := NewWiki(srv.URL)

	if got, err := ws.Signatures(context.Background(), payload); err != nil || len(got) != 0 {
		t.Fatalf("an empty list means nobody signed, got %v, %v", got, err)
	}
	for _, status = range []int{http.StatusBadRequest, http.StatusNotFound} {
		if got, err := ws.Signatures(context.Background(), payload); err != nil || len(got) != 0 {
			t.Fatalf("%d means nobody signed, got %v, %v", status, got, err)
		}
	}
	status = http.StatusBadGateway
	if _, err := ws.Signatures(context.Background(), payload); err == nil {
		t.Fatal("want an error when the wiki does not answer 200")
	}
}
