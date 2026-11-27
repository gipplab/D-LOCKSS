package monitor

import (
	"context"
	"encoding/base64"
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

func TestManifestCatalog(t *testing.T) {
	ingester, _ := peer.Decode("12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb")
	uploader, _ := peer.Decode("12D3KooWNEd5gaYNKe9nAesWuQ1HMgyH3sjXrNQYgbBK1H1ixuN9")
	payload, _ := cid.Decode("bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am")

	// Made with Kubo 0.43.1: printf 'dlockss-upload <payload>' | ipfs key sign
	sig, _ := base64.RawURLEncoding.DecodeString("0UeavBivwyduao91YLSf41jA6XUNhOAMvh9JHLi1L4_DwV98Z5155wkOnkpCFAgFvcfe6XRiqhFFQDTM31kMBw")
	uploaded := schema.NewResearchObject("paper.pdf", ingester, payload, 6)
	uploaded.Uploader, uploaded.UploaderSig = uploader, sig
	forged := schema.NewResearchObject("forged.pdf", ingester, payload, 6)
	forged.Uploader, forged.UploaderSig = uploader, []byte{1}
	blocks := map[string]*schema.ResearchObject{
		"uploaded": uploaded,
		"forged":   forged,
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

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	m := NewMonitor(cfg)
	m.SetKeywords(keywords.NewStore(keywords.Config{Gateway: srv.URL}))
	now := time.Now()
	for _, c := range []string{"uploaded", "forged", "plain", "missing"} {
		m.uniqueCIDs[c] = now
	}
	m.scanCatalog(context.Background())

	if info, ok := m.catalog.get("uploaded"); !ok || info.Uploader != uploader.String() || info.Payload != payload.String() || info.Ingester != ingester.String() {
		t.Fatalf("uploaded = %+v, %v", info, ok)
	}
	if info, ok := m.catalog.get("forged"); !ok || info.Uploader != "" {
		t.Fatalf("a signature that does not verify must not name an uploader: %+v", info)
	}
	if info, ok := m.catalog.get("plain"); !ok || info.Uploader != "" {
		t.Fatalf("plain = %+v, %v", info, ok)
	}
	if _, ok := m.catalog.get("missing"); ok {
		t.Fatal("an unreadable manifest is not in the catalog")
	}
	if todo := m.catalog.todo([]string{"missing"}, now, 10); len(todo) != 0 {
		t.Fatalf("an unreadable manifest waits %v before the next try, got %v", catalogRetryAfter, todo)
	}
	if p, ok := m.PayloadOf("uploaded"); !ok || p != payload.String() {
		t.Fatalf("PayloadOf = %q, %v", p, ok)
	}

	m.catalog.save()
	reloaded := newManifestCatalog(dir)
	if info, ok := reloaded.get("uploaded"); !ok || info.Uploader != uploader.String() {
		t.Fatalf("the catalog survives a restart, got %+v, %v", info, ok)
	}

	var d struct {
		MetaRef  string `json:"meta_ref"`
		Payload  string `json:"payload"`
		Uploader string `json:"uploader"`
	}
	get(t, m.handleManifest, "/api/manifest?cid=uploaded", &d)
	if d.MetaRef != "paper.pdf" || d.Payload != payload.String() || d.Uploader != uploader.String() {
		t.Fatalf("uploaded as DAG-JSON = %+v", d)
	}
	rec := httptest.NewRecorder()
	m.handleManifest(rec, httptest.NewRequest(http.MethodGet, "/api/manifest?cid="+payload.String(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("a payload CID is no manifest, got %d", rec.Code)
	}
}

func TestManifestInfoMarksLegacy(t *testing.T) {
	ingester, _ := peer.Decode("12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb")
	payload, _ := cid.Decode("bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am")
	ro := schema.NewResearchObject("x", ingester, payload, 1)
	ro.HasLegacyTimestamp = true
	if info := manifestInfoOf("m", ro); !info.Legacy {
		t.Fatalf("info = %+v, want legacy", info)
	}
	m := NewMonitor(DefaultConfig())
	m.catalog.put("m", manifestInfoOf("m", ro))
	if _, ok := m.PayloadOf("m"); ok {
		t.Fatal("nodes ignore legacy manifests, so the indexer must too")
	}
}
