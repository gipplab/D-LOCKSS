package keywords

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
	"golang.org/x/net/html"

	"dlockss/internal/schema"
)

const (
	manifestFetchLimit  = 1 << 20  // 1 MiB
	manifestFetchTime   = 30 * time.Second
	payloadFetchTime    = 90 * time.Second
	textReadLimit       = 512 << 10 // 512 KiB of plain text
	rootBlockLimit      = 2 << 20   // directory root block
	binaryFetchLimit    = 20 << 20  // PDF / zip containers
	zipPartLimit        = 8 << 20   // each document part inside a zip
	pdftotextTimeout    = 2 * time.Minute
	dirListingMaxEntries = 50
	dirSampleMaxFiles   = 5
	dirSampleMaxTries   = 10
	dirSampleMinBudget  = 500
)

// errUnsupported marks content that is deliberately skipped (media,
// archives, oversized, empty): recorded as skipped, never retried.
var errUnsupported = errors.New("unsupported content")

// IsUnsupported reports whether err marks skippable content.
func IsUnsupported(err error) bool { return errors.Is(err, errUnsupported) }

// gatewayStatusError surfaces upstream HTTP status (429 pauses the indexer).
type gatewayStatusError struct {
	status     int
	retryAfter time.Duration
}

func (e *gatewayStatusError) Error() string { return fmt.Sprintf("gateway HTTP %d", e.status) }

var httpClient = &http.Client{}

func gatewayGet(ctx context.Context, url string, limit int64, timeout time.Duration) ([]byte, http.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, resp.Header, &gatewayStatusError{status: resp.StatusCode, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	return data, resp.Header, err
}

// FetchManifest reads and decodes a ResearchObject block through the gateway.
func FetchManifest(ctx context.Context, gateway string, manifest string) (*schema.ResearchObject, error) {
	data, _, err := gatewayGet(ctx, gateway+"/ipfs/"+manifest+"?format=raw", manifestFetchLimit, manifestFetchTime)
	if err != nil {
		return nil, err
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(data); err != nil {
		return nil, fmt.Errorf("manifest decode: %w", err)
	}
	return &ro, nil
}

// document is extracted text ready for the LLM.
type document struct {
	Name   string
	Format string
	Text   string
}

// nameFromMetaRef derives a display name: file:// gives the basename,
// ipfs:// gives none, anything else is used as-is.
func nameFromMetaRef(metaRef string) string {
	switch {
	case strings.HasPrefix(metaRef, "file://"):
		return path.Base(strings.TrimPrefix(metaRef, "file://"))
	case strings.HasPrefix(metaRef, "ipfs://"):
		return ""
	default:
		return metaRef
	}
}

// LoadContent fetches a payload through the gateway and extracts text,
// handling single files and UnixFS directories.
func LoadContent(ctx context.Context, gateway string, payload cid.Cid, metaRef string) (*document, error) {
	switch payload.Prefix().Codec {
	case cid.Raw, cid.DagProtobuf:
	default:
		return nil, fmt.Errorf("%w: payload codec %d", errUnsupported, payload.Prefix().Codec)
	}
	name := nameFromMetaRef(metaRef)

	data, hdr, err := gatewayGet(ctx, gateway+"/ipfs/"+payload.String(), textReadLimit, payloadFetchTime)
	if err != nil {
		var gw *gatewayStatusError
		if errors.As(err, &gw) {
			return nil, err
		}
		return nil, err
	}
	if strings.HasPrefix(hdr.Get("Etag"), `"DirIndex-`) {
		return loadDirectory(ctx, gateway, payload, name)
	}
	return extract(ctx, gateway, payload.String(), name, data, hdr.Get("Content-Type"), textReadLimit)
}

// extract classifies content and pulls text out of it. bigFetchPath is the
// gateway path used to re-fetch large binary formats in full.
func extract(ctx context.Context, gateway, gwPath, name string, head []byte, contentType string, budget int) (*document, error) {
	kind := classify(name, head, contentType)
	switch kind {
	case kindPDF:
		return extractBinary(ctx, gateway, gwPath, name, "PDF", extractPDF)
	case kindZip:
		return extractBinary(ctx, gateway, gwPath, name, zipFormatName(name), extractZip)
	case kindHTML:
		text := htmlText(head)
		return finishDoc(name, "HTML", text, budget)
	case kindXML:
		return finishDoc(name, "XML", xmlText(head), budget)
	case kindText:
		return finishDoc(name, "text", string(head), budget)
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupported, name)
	}
}

func finishDoc(name, format, text string, budget int) (*document, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("%w: no text", errUnsupported)
	}
	if len(text) > budget {
		text = text[:budget]
	}
	return &document{Name: name, Format: format, Text: text}, nil
}

// extractBinary spools PDF/zip content to a temp file (bounded) and runs the
// given extractor.
func extractBinary(ctx context.Context, gateway, gwPath, name, format string, fn func(ctx context.Context, path string, size int64) (string, error)) (*document, error) {
	ctx, cancel := context.WithTimeout(ctx, payloadFetchTime)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway+"/ipfs/"+gwPath, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &gatewayStatusError{status: resp.StatusCode, retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	tmp, err := os.CreateTemp("", "dlockss-kw-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	n, err := io.Copy(tmp, io.LimitReader(resp.Body, binaryFetchLimit+1))
	if err != nil {
		return nil, err
	}
	if n > binaryFetchLimit {
		return nil, fmt.Errorf("%w: over %d MiB", errUnsupported, binaryFetchLimit>>20)
	}
	text, err := fn(ctx, tmp.Name(), n)
	if err != nil {
		return nil, err
	}
	return finishDoc(name, format, text, saiaTextLimit)
}

// PDFSupported reports whether pdftotext is on PATH; without it PDFs are
// skipped rather than failed.
func PDFSupported() bool {
	_, err := exec.LookPath("pdftotext")
	return err == nil
}

func extractPDF(ctx context.Context, path string, _ int64) (string, error) {
	if !PDFSupported() {
		return "", fmt.Errorf("%w: pdftotext not installed", errUnsupported)
	}
	ctx, cancel := context.WithTimeout(ctx, pdftotextTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pdftotext", "-layout", "-enc", "UTF-8", path, "-").Output()
	if err != nil {
		return "", fmt.Errorf("pdftotext: %w", err)
	}
	return string(out), nil
}

var slideRe = regexp.MustCompile(`^ppt/slides/slide(\d+)\.xml$`)

// extractZip handles OOXML, OpenDocument, and EPUB containers.
func extractZip(ctx context.Context, path string, size int64) (string, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return "", fmt.Errorf("%w: unreadable zip", errUnsupported)
	}
	defer zr.Close()

	readPart := func(f *zip.File) string {
		rc, err := f.Open()
		if err != nil {
			return ""
		}
		defer rc.Close()
		raw, _ := io.ReadAll(io.LimitReader(rc, zipPartLimit))
		return string(raw)
	}

	files := map[string]*zip.File{}
	var slides []*zip.File
	isEpub := false
	for _, f := range zr.File {
		files[f.Name] = f
		if slideRe.MatchString(f.Name) {
			slides = append(slides, f)
		}
		if f.Name == "META-INF/container.xml" {
			isEpub = true
		}
	}

	switch {
	case files["word/document.xml"] != nil:
		return xmlText([]byte(readPart(files["word/document.xml"]))), nil
	case files["xl/sharedStrings.xml"] != nil:
		return xmlText([]byte(readPart(files["xl/sharedStrings.xml"]))), nil
	case len(slides) > 0:
		sort.Slice(slides, func(i, j int) bool {
			a := slideRe.FindStringSubmatch(slides[i].Name)[1]
			b := slideRe.FindStringSubmatch(slides[j].Name)[1]
			return len(a) < len(b) || (len(a) == len(b) && a < b)
		})
		var sb strings.Builder
		for _, s := range slides {
			sb.WriteString(xmlText([]byte(readPart(s))))
			sb.WriteByte('\n')
		}
		return sb.String(), nil
	case files["content.xml"] != nil: // OpenDocument
		return xmlText([]byte(readPart(files["content.xml"]))), nil
	case isEpub:
		var sb strings.Builder
		for _, f := range zr.File {
			ext := strings.ToLower(path2ext(f.Name))
			if ext == ".xhtml" || ext == ".html" || ext == ".htm" {
				sb.WriteString(htmlText([]byte(readPart(f))))
				sb.WriteByte('\n')
				if sb.Len() > saiaTextLimit {
					break
				}
			}
		}
		return sb.String(), nil
	default:
		return "", fmt.Errorf("%w: unrecognized zip container", errUnsupported)
	}
}

func path2ext(p string) string { return path.Ext(p) }

// ---- classification ----

type contentKind int

const (
	kindUnsupported contentKind = iota
	kindText
	kindHTML
	kindXML
	kindPDF
	kindZip
)

var textExts = map[string]bool{
	".txt": true, ".md": true, ".rst": true, ".org": true, ".csv": true, ".tsv": true,
	".json": true, ".jsonl": true, ".ndjson": true, ".geojson": true, ".yaml": true, ".yml": true,
	".toml": true, ".ini": true, ".cfg": true, ".tex": true, ".bib": true, ".ris": true,
	".r": true, ".py": true, ".ipynb": true, ".jl": true, ".m": true, ".go": true, ".java": true,
	".c": true, ".h": true, ".cpp": true, ".rs": true, ".js": true, ".ts": true, ".sh": true,
	".sql": true, ".log": true, ".srt": true, ".vtt": true, ".nt": true, ".ttl": true,
}

var markupExts = map[string]contentKind{
	".html": kindHTML, ".htm": kindHTML, ".xhtml": kindHTML,
	".xml": kindXML, ".rdf": kindXML, ".kml": kindXML, ".gpx": kindXML, ".tei": kindXML, ".jats": kindXML,
}

// skipExts marks media/archives never worth sampling or extracting.
var skipExts = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true, ".svg": true,
	".mp3": true, ".wav": true, ".flac": true, ".ogg": true, ".mp4": true, ".mkv": true,
	".avi": true, ".mov": true, ".webm": true, ".zip": true, ".tar": true, ".gz": true,
	".bz2": true, ".xz": true, ".7z": true, ".rar": true, ".iso": true, ".bin": true,
	".exe": true, ".dll": true, ".so": true, ".woff": true, ".woff2": true, ".ttf": true,
}

var zipDocExts = map[string]bool{
	".docx": true, ".pptx": true, ".xlsx": true, ".odt": true, ".odp": true, ".ods": true, ".epub": true,
}

func zipFormatName(name string) string {
	ext := strings.TrimPrefix(strings.ToLower(path.Ext(name)), ".")
	if ext == "" {
		return "document"
	}
	return strings.ToUpper(ext)
}

func classify(name string, head []byte, contentType string) contentKind {
	if bytes.HasPrefix(head, []byte("%PDF")) {
		return kindPDF
	}
	if bytes.HasPrefix(head, []byte("PK\x03\x04")) {
		if ext := strings.ToLower(path.Ext(name)); ext != "" && !zipDocExts[ext] {
			return kindUnsupported
		}
		return kindZip
	}
	ct := contentType
	if ct == "" || strings.HasPrefix(ct, "application/octet-stream") {
		ct = http.DetectContentType(head)
	}
	switch {
	case strings.HasPrefix(ct, "image/"), strings.HasPrefix(ct, "audio/"),
		strings.HasPrefix(ct, "video/"), strings.HasPrefix(ct, "font/"):
		return kindUnsupported
	}
	ext := strings.ToLower(path.Ext(name))
	if skipExts[ext] {
		return kindUnsupported
	}
	if k, ok := markupExts[ext]; ok {
		return k
	}
	if textExts[ext] && looksLikeText(head) {
		return kindText
	}
	switch {
	case strings.Contains(ct, "html"):
		return kindHTML
	case strings.Contains(ct, "xml"):
		return kindXML
	case strings.HasPrefix(ct, "text/") && looksLikeText(head):
		return kindText
	}
	if looksLikeText(head) && ext == "" {
		return kindText
	}
	return kindUnsupported
}

// looksLikeText: valid UTF-8, no NUL bytes, ≤1% control characters.
func looksLikeText(b []byte) bool {
	if len(b) == 0 || !utf8.Valid(b) {
		return false
	}
	control := 0
	for _, c := range b {
		if c == 0 {
			return false
		}
		if c < 0x20 && c != '\n' && c != '\r' && c != '\t' {
			control++
		}
	}
	return control*100 <= len(b)
}

// ---- markup extraction ----

func htmlText(raw []byte) string {
	tok := html.NewTokenizer(bytes.NewReader(raw))
	var sb strings.Builder
	skipDepth := 0
	for {
		switch tok.Next() {
		case html.ErrorToken:
			return sb.String()
		case html.StartTagToken:
			name, _ := tok.TagName()
			switch string(name) {
			case "script", "style", "noscript", "template":
				skipDepth++
			}
		case html.EndTagToken:
			name, _ := tok.TagName()
			switch string(name) {
			case "script", "style", "noscript", "template":
				if skipDepth > 0 {
					skipDepth--
				}
			}
		case html.TextToken:
			if skipDepth == 0 {
				if t := strings.TrimSpace(string(tok.Text())); t != "" {
					sb.WriteString(t)
					sb.WriteByte(' ')
				}
			}
		}
	}
}

func xmlText(raw []byte) string {
	dec := xml.NewDecoder(bytes.NewReader(raw))
	dec.Strict = false
	var sb strings.Builder
	for {
		tok, err := dec.Token()
		if err != nil {
			return sb.String()
		}
		if cd, ok := tok.(xml.CharData); ok {
			if t := strings.TrimSpace(string(cd)); t != "" {
				sb.WriteString(t)
				sb.WriteByte(' ')
			}
		}
	}
}

// ---- UnixFS directories ----

// loadDirectory indexes a pinned directory: its file listing plus excerpts
// of a few readable files.
func loadDirectory(ctx context.Context, gateway string, root cid.Cid, name string) (*document, error) {
	block, _, err := gatewayGet(ctx, gateway+"/ipfs/"+root.String()+"?format=raw", rootBlockLimit, manifestFetchTime)
	if err != nil {
		return nil, err
	}
	links, err := parseDagPBLinks(block)
	if err != nil {
		return nil, fmt.Errorf("%w: unreadable directory", errUnsupported)
	}

	var listing strings.Builder
	listing.WriteString("Directory listing:\n")
	for i, l := range links {
		if i >= dirListingMaxEntries {
			listing.WriteString(fmt.Sprintf("... and %d more entries\n", len(links)-i))
			break
		}
		listing.WriteString(fmt.Sprintf("%s (%d bytes)\n", l.Name, l.Size))
	}

	// Sample a few extractable files, best formats first.
	ordered := make([]dagLink, len(links))
	copy(ordered, links)
	sort.SliceStable(ordered, func(i, j int) bool {
		return samplePriority(ordered[i].Name) < samplePriority(ordered[j].Name)
	})

	limit := saiaTextLimit
	budget := (limit - listing.Len()) / dirSampleMaxFiles
	if budget < dirSampleMinBudget {
		budget = dirSampleMinBudget
	}
	var sb strings.Builder
	sb.WriteString(listing.String())
	sampled, tries := 0, 0
	for _, l := range ordered {
		if sampled >= dirSampleMaxFiles || tries >= dirSampleMaxTries {
			break
		}
		prio := samplePriority(l.Name)
		if prio >= prioSkip {
			continue
		}
		tries++
		head, hdr, err := gatewayGet(ctx, gateway+"/ipfs/"+root.String()+"/"+l.Name, textReadLimit, payloadFetchTime)
		if err != nil {
			continue
		}
		doc, err := extract(ctx, gateway, root.String()+"/"+l.Name, l.Name, head, hdr.Get("Content-Type"), budget)
		if err != nil {
			continue
		}
		text := doc.Text
		if len(text) > budget {
			text = text[:budget]
		}
		sb.WriteString("\n--- " + l.Name + " ---\n" + text + "\n")
		sampled++
	}
	if name == "" {
		name = "directory " + root.String()
	}
	return finishDoc(name, "directory", sb.String(), limit)
}

const (
	prioDocument = 0 // pdf/office/epub
	prioText     = 1 // text and markup
	prioUnknown  = 2
	prioSkip     = 3 // media/archives
)

func samplePriority(name string) int {
	ext := strings.ToLower(path.Ext(name))
	switch {
	case ext == ".pdf" || zipDocExts[ext]:
		return prioDocument
	case textExts[ext]:
		return prioText
	case markupExts[ext] != kindUnsupported && ext != "":
		if _, ok := markupExts[ext]; ok {
			return prioText
		}
		return prioUnknown
	case skipExts[ext]:
		return prioSkip
	default:
		return prioUnknown
	}
}

// ---- minimal dag-pb decoding (directory links only) ----

type dagLink struct {
	Name string
	Size uint64
}

// unixfs Data types (from the UnixFS protobuf enum).
const unixfsHAMTShard = 5

// parseDagPBLinks reads a dag-pb node's links and strips HAMT shard name
// prefixes (2 hex chars) when the node is a HAMT directory.
func parseDagPBLinks(block []byte) ([]dagLink, error) {
	var links []dagLink
	var nodeData []byte
	if err := walkProto(block, func(field int, wire int, val []byte, num uint64) error {
		switch field {
		case 1: // Data
			nodeData = val
		case 2: // Links
			var l dagLink
			if err := walkProto(val, func(f, w int, v []byte, n uint64) error {
				switch f {
				case 2:
					l.Name = string(v)
				case 3:
					l.Size = n
				}
				return nil
			}); err != nil {
				return err
			}
			links = append(links, l)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// HAMT shards prefix names with 2 hex characters.
	if unixfsType(nodeData) == unixfsHAMTShard {
		for i := range links {
			if len(links[i].Name) > 2 {
				links[i].Name = links[i].Name[2:]
			}
		}
	}
	return links, nil
}

func unixfsType(data []byte) int {
	t := -1
	_ = walkProto(data, func(field, wire int, val []byte, num uint64) error {
		if field == 1 && wire == 0 {
			t = int(num)
		}
		return nil
	})
	return t
}

// walkProto iterates protobuf wire fields: varint (wire 0) passes num,
// length-delimited (wire 2) passes val. Other wire types are skipped.
func walkProto(b []byte, fn func(field, wire int, val []byte, num uint64) error) error {
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return errors.New("bad protobuf tag")
		}
		b = b[n:]
		field := int(tag >> 3)
		wire := int(tag & 7)
		switch wire {
		case 0:
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return errors.New("bad varint")
			}
			b = b[n:]
			if err := fn(field, wire, nil, v); err != nil {
				return err
			}
		case 2:
			l, n := binary.Uvarint(b)
			if n <= 0 || uint64(len(b)-n) < l {
				return errors.New("bad length")
			}
			val := b[n : n+int(l)]
			b = b[n+int(l):]
			if err := fn(field, wire, val, 0); err != nil {
				return err
			}
		case 5:
			if len(b) < 4 {
				return errors.New("truncated fixed32")
			}
			b = b[4:]
		case 1:
			if len(b) < 8 {
				return errors.New("truncated fixed64")
			}
			b = b[8:]
		default:
			return fmt.Errorf("unsupported wire type %d", wire)
		}
	}
	return nil
}
