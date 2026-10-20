package keywords

import (
	"archive/zip"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

func TestClassify(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
	cases := []struct {
		name, contentType string
		head              []byte
		want              contentKind
	}{
		{"paper.pdf", "application/pdf", []byte("%PDF-1.7"), kindPDF},
		{"", "", []byte("%PDF-1.4"), kindPDF},
		{"report.docx", "", []byte("PK\x03\x04rest"), kindZip},
		{"data.csv", "text/csv", []byte("a,b\n1,2\n"), kindText},
		{"run.json", "application/json", []byte(`{"x":1}`), kindText},
		{"", "", []byte("plain words without a name"), kindText},
		{"index.html", "text/html", []byte("<html><body>hi"), kindHTML},
		{"", "application/xml", []byte("<?xml version=\"1.0\"?><a/>"), kindXML},
		{"photo.png", "image/png", png, kindUnsupported},
		{"", "", png, kindUnsupported},
		{"clip.mp4", "video/mp4", []byte("\x00\x00\x00\x18ftypmp42"), kindUnsupported},
		{"blob", "", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}, kindUnsupported},
		{"data.csv", "text/csv", []byte{0x00, 0x01, 0x02, 0xff}, kindUnsupported},
	}
	for _, c := range cases {
		if got, _ := classify(c.name, c.contentType, c.head); got != c.want {
			t.Errorf("classify(%q, %q) = %v, want %v", c.name, c.contentType, got, c.want)
		}
	}
}

func TestHTMLTextSkipsScripts(t *testing.T) {
	got := htmlText([]byte(`<html><head><style>p{}</style><script>var x=1</script></head><body><h1>Title</h1><p>Body &amp; text</p></body></html>`))
	if strings.Contains(got, "var x") || strings.Contains(got, "p{}") || !strings.Contains(got, "Title") || !strings.Contains(got, "Body & text") {
		t.Fatalf("htmlText = %q", got)
	}
}

func writeZip(t *testing.T, files map[string]string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "doc.zip")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for name, body := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestZipText(t *testing.T) {
	cases := []struct {
		files      map[string]string
		wantFormat string
		wantText   string
	}{
		{map[string]string{"word/document.xml": `<w:document xmlns:w="w"><w:body><w:p><w:r><w:t>Soil carbon</w:t></w:r></w:p></w:body></w:document>`}, "Word document", "Soil carbon"},
		{map[string]string{"xl/sharedStrings.xml": `<sst><si><t>species</t></si><si><t>count</t></si></sst>`}, "Excel workbook", "species count"},
		{map[string]string{"ppt/slides/slide2.xml": `<p:sld><a:t>Second</a:t></p:sld>`, "ppt/slides/slide10.xml": `<p:sld><a:t>Tenth</a:t></p:sld>`}, "PowerPoint presentation", "Second"},
		{map[string]string{"mimetype": "application/vnd.oasis.opendocument.text", "content.xml": `<office:document-content><text:p>Thesis</text:p></office:document-content>`}, "OpenDocument file", "Thesis"},
		{map[string]string{"META-INF/container.xml": `<container/>`, "OEBPS/ch1.xhtml": `<html><body><p>Chapter one</p></body></html>`}, "EPUB book", "Chapter one"},
	}
	for _, c := range cases {
		text, f, err := zipText(writeZip(t, c.files), "zip container")
		if err != nil {
			t.Fatalf("%s: %v", c.wantFormat, err)
		}
		if f != c.wantFormat || !strings.Contains(text, c.wantText) {
			t.Errorf("zipText = (%q, %q), want format %q containing %q", text, f, c.wantFormat, c.wantText)
		}
	}
	slides, _, _ := zipText(writeZip(t, cases[2].files), "zip container")
	if strings.Index(slides, "Second") > strings.Index(slides, "Tenth") {
		t.Errorf("slides must be read in numeric order, got %q", slides)
	}

	_, _, err := zipText(writeZip(t, map[string]string{"photos/a.jpg": "x"}), "zip container")
	if !errors.Is(err, errUnsupported) {
		t.Fatalf("a plain zip archive must be unsupported, got %v", err)
	}
}

func rawCID(t *testing.T, data string) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte(data), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// fakeGateway serves fixed paths and records which were requested.
type fakeGateway struct {
	mu        sync.Mutex
	requested []string
	routes    map[string]func(w http.ResponseWriter)
}

func (g *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		key += "?" + r.URL.RawQuery
	}
	g.mu.Lock()
	g.requested = append(g.requested, key)
	g.mu.Unlock()
	if h, ok := g.routes[key]; ok {
		h(w)
		return
	}
	http.NotFound(w, r)
}

func serve(contentType, body string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(body))
	}
}

// dirListing answers like Kubo does for a UnixFS directory without index.html.
func dirListing(w http.ResponseWriter) {
	w.Header().Set("Etag", `"DirIndex-abc_CID-x"`)
	serve("text/html", "<html>listing</html>")(w)
}

func newGatewayStore(t *testing.T, g *fakeGateway) *Store {
	t.Helper()
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return NewStore(Config{DataDir: t.TempDir(), Gateway: srv.URL})
}

func TestLoadContentDirectory(t *testing.T) {
	csv := "station,temperature\nA,12.5\n"
	notes := "Field notes from the 2026 survey."
	sub := merkledag.NodeWithData(unixfs.FolderPBData())
	dir := merkledag.NodeWithData(unixfs.FolderPBData())
	for name, c := range map[string]cid.Cid{
		"data.csv":  rawCID(t, csv),
		"photo.jpg": rawCID(t, "jpeg"),
		"NOTES":     rawCID(t, notes),
		"sub":       sub.Cid(),
	} {
		if err := dir.AddRawLink(name, &format.Link{Cid: c, Size: 10}); err != nil {
			t.Fatal(err)
		}
	}
	d := dir.Cid().String()
	g := &fakeGateway{routes: map[string]func(http.ResponseWriter){
		"/ipfs/" + d:                 dirListing,
		"/ipfs/" + d + "?format=raw": serve("application/vnd.ipld.raw", string(dir.RawData())),
		"/ipfs/" + d + "/data.csv":   serve("text/csv", csv),
		"/ipfs/" + d + "/NOTES":      serve("text/plain", notes),
		"/ipfs/" + d + "/photo.jpg":  serve("image/jpeg", "jpeg"),
		"/ipfs/" + d + "/sub":        dirListing,
	}}
	s := newGatewayStore(t, g)

	c, err := s.loadContent(d, "survey")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Directory with 4 entries", "photo.jpg (10 bytes)", "=== data.csv (CSV table) ===", "A,12.5", "=== NOTES (plain text) ===", notes} {
		if !strings.Contains(c.text, want) {
			t.Errorf("directory text lacks %q:\n%s", want, c.text)
		}
	}
	if c.format != "directory" || strings.Contains(c.text, "listing") {
		t.Errorf("format %q, nested directory must not be sampled:\n%s", c.format, c.text)
	}
	for _, p := range g.requested {
		if strings.HasSuffix(p, "photo.jpg") {
			t.Error("media files must not be fetched")
		}
	}
}

func TestLoadContentFile(t *testing.T) {
	csv := "a,b\n1,2\n"
	png := "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
	csvCID, pngCID := rawCID(t, csv), rawCID(t, png)
	g := &fakeGateway{routes: map[string]func(http.ResponseWriter){
		"/ipfs/" + csvCID.String(): serve("text/plain; charset=utf-8", csv),
		"/ipfs/" + pngCID.String(): serve("image/png", png),
	}}
	s := newGatewayStore(t, g)

	c, err := s.loadContent(csvCID.String(), "issuers.csv")
	if err != nil || c.format != "CSV table" || c.text != strings.TrimSpace(csv) {
		t.Fatalf("csv content = %+v, %v", c, err)
	}
	if _, err := s.loadContent(pngCID.String(), "scan.png"); !errors.Is(err, errUnsupported) {
		t.Fatalf("an image must be unsupported, got %v", err)
	}
}

func TestDocumentPromptNamesItem(t *testing.T) {
	p := documentPrompt(&content{name: "issuers.csv", format: "CSV table", text: strings.Repeat("x", 100)}, 10)
	if !strings.Contains(p, "Name: issuers.csv\nFormat: CSV table\n") || strings.Contains(p, strings.Repeat("x", 11)) {
		t.Fatalf("prompt = %q", p)
	}
}
