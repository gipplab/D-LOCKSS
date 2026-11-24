package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	uploaded.Uploader, uploaded.UploaderSig = uploader, []byte{1}
	blocks := map[string]*schema.ResearchObject{
		"uploaded": uploaded,
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
	for _, c := range []string{"uploaded", "plain", "missing"} {
		m.uniqueCIDs[c] = now
	}
	m.scanUploaders(context.Background())

	if got := m.manifestUploader; len(got) != 2 || got["uploaded"] != uploader.String() || got["plain"] != "" {
		t.Fatalf("index = %v; want the uploader of 'uploaded', '' for 'plain', and 'missing' left for a later pass", got)
	}

	var ups struct{ Uploaders []uploaderEntry }
	get(t, m.handleUploaders, "/api/uploaders", &ups)
	if len(ups.Uploaders) != 1 || ups.Uploaders[0] != (uploaderEntry{Key: uploader.String(), Files: 1}) {
		t.Fatalf("uploaders = %+v", ups.Uploaders)
	}
	var files struct{ CIDs []cidEntry }
	get(t, m.handleUploaderFiles, "/api/uploader-files?key="+uploader.String(), &files)
	if len(files.CIDs) != 1 || files.CIDs[0].CID != "uploaded" {
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
