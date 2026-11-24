package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/fileops"
)

// Made with Kubo 0.43.1: printf 'dlockss-upload <payload>' | ipfs key sign -k alice
const (
	kuboKey     = "12D3KooWNEd5gaYNKe9nAesWuQ1HMgyH3sjXrNQYgbBK1H1ixuN9"
	kuboPayload = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
	kuboSig     = "u0UeavBivwyduao91YLSf41jA6XUNhOAMvh9JHLi1L4_DwV98Z5155wkOnkpCFAgFvcfe6XRiqhFFQDTM31kMBw"
)

type fakeTrust bool

func (f fakeTrust) Trusted(context.Context, peer.ID) (bool, error) { return bool(f), nil }

type fakeIngester struct {
	err           error
	metaRef, body string
	payload       cid.Cid
	uploader      peer.ID
	called        bool
}

func (f *fakeIngester) IngestUpload(_ context.Context, path, metaRef string, payload cid.Cid, uploader peer.ID, _ []byte) (cid.Cid, error) {
	f.called = true
	b, _ := os.ReadFile(path)
	f.body, f.metaRef, f.payload, f.uploader = string(b), metaRef, payload, uploader
	return payload, f.err
}

// countingReader records whether the handler read the body.
type countingReader struct{ read bool }

func (c *countingReader) Read(p []byte) (int, error) {
	c.read = true
	return copy(p, "hello\n"), io.EOF
}

func upload(t *testing.T, trust KeyTrust, ing Ingester, sig string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	q := url.Values{"cid": {kuboPayload}, "key": {kuboKey}, "sig": {sig}, "name": {"dir/paper.pdf"}}
	req := httptest.NewRequest(http.MethodPost, "/upload?"+q.Encode(), body)
	rec := httptest.NewRecorder()
	UploadHandler(trust, ing).ServeHTTP(rec, req)
	return rec
}

func TestUpload_UntrustedKeyIsRefusedBeforeReading(t *testing.T) {
	body := &countingReader{}
	ing := &fakeIngester{}
	rec := upload(t, fakeTrust(false), ing, kuboSig, body)
	if rec.Code != http.StatusForbidden || body.read || ing.called {
		t.Fatalf("got %d, body read %v, ingested %v; want 403 without reading", rec.Code, body.read, ing.called)
	}
}

func TestUpload_WrongSignature(t *testing.T) {
	wrong := "u" + strings.Repeat("A", 86)
	ing := &fakeIngester{}
	if rec := upload(t, fakeTrust(true), ing, wrong, strings.NewReader("hello\n")); rec.Code != http.StatusForbidden || ing.called {
		t.Fatalf("got %d, ingested %v; want 403", rec.Code, ing.called)
	}
}

func TestUpload_Ingests(t *testing.T) {
	ing := &fakeIngester{}
	rec := upload(t, fakeTrust(true), ing, kuboSig, strings.NewReader("hello\n"))
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, rec.Body)
	}
	if ing.body != "hello\n" || ing.metaRef != "paper.pdf" || ing.payload.String() != kuboPayload || ing.uploader.String() != kuboKey {
		t.Fatalf("ingester got body %q, metaRef %q, payload %s, uploader %s", ing.body, ing.metaRef, ing.payload, ing.uploader)
	}
}

func TestUpload_PayloadMismatch(t *testing.T) {
	ing := &fakeIngester{err: fmt.Errorf("%w: got x", fileops.ErrPayloadMismatch)}
	if rec := upload(t, fakeTrust(true), ing, kuboSig, strings.NewReader("other\n")); rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", rec.Code)
	}
}
