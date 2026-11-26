package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/keywords"
	"dlockss/pkg/schema"
)

func TestUploaderIndex(t *testing.T) {
	ingester, _ := peer.Decode("12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb")
	uploader, _ := peer.Decode("12D3KooWNEd5gaYNKe9nAesWuQ1HMgyH3sjXrNQYgbBK1H1ixuN9")
	payload, _ := cid.Decode("bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am")

	uploaded := schema.NewResearchObject("paper.pdf", ingester, payload, 6)
	uploaded.Uploader, uploaded.UploaderSig = uploader, []byte{1} // before the uploaders list
	signed := schema.NewResearchObject("paper.pdf", ingester, payload, 6)
	signed.Uploaders = []schema.UploaderSignature{{Key: ingester, Sig: []byte{2}}, {Key: uploader, Sig: []byte{3}}}
	blocks := map[string]*schema.ResearchObject{
		"uploaded": uploaded,
		"signed":   signed,
		"plain":    schema.NewResearchObject("file://x", ingester, payload, 6),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ro, ok := blocks[strings.TrimPrefix(r.URL.Path, "/ipfs/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		b, _ := ro.MarshalCBOR()
		w.Write(b)
	}))
	defer srv.Close()

	m := NewMonitor(DefaultConfig())
	m.SetKeywords(keywords.NewStore(keywords.Config{Gateway: srv.URL}))
	now := time.Now()
	for _, c := range []string{"uploaded", "signed", "plain", "missing"} {
		m.uniqueCIDs[c] = now
	}
	m.scanUploaders(context.Background())

	got := m.manifestUploader
	if len(got) != 3 || !slices.Equal(got["uploaded"], []string{uploader.String()}) ||
		!slices.Equal(got["signed"], []string{ingester.String(), uploader.String()}) || len(got["plain"]) != 0 {
		t.Fatalf("index = %v; want the uploaders of 'uploaded' and 'signed', none for 'plain', and 'missing' left for a later pass", got)
	}

	var ups struct{ Uploaders []uploaderEntry }
	get(t, m.handleUploaders, "/api/uploaders", &ups)
	want := []uploaderEntry{{Key: uploader.String(), Files: 2}, {Key: ingester.String(), Files: 1}}
	if !slices.Equal(ups.Uploaders, want) {
		t.Fatalf("uploaders = %+v, want %+v", ups.Uploaders, want)
	}
	var files struct{ CIDs []cidEntry }
	get(t, m.handleUploaderFiles, "/api/uploader-files?key="+uploader.String(), &files)
	if len(files.CIDs) != 2 {
		t.Fatalf("files = %+v", files.CIDs)
	}
}

func get(t *testing.T, h http.HandlerFunc, target string, v any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, target, nil))
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("%s: %v: %s", target, err, rec.Body)
	}
}
