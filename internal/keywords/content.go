package keywords

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"golang.org/x/net/html"
)

const (
	maxFetchSize        = 20 * 1024 * 1024
	maxTextFetch        = 512 * 1024
	maxRootBlockSize    = 2 * 1024 * 1024
	maxZipPartSize      = 8 * 1024 * 1024
	maxDirEntries       = 50
	maxDirSamples       = 5
	maxDirAttempts      = 2 * maxDirSamples
	minSampleLen        = 500
	sniffLen            = 512
	payloadFetchTimeout = 90 * time.Second
)

// errUnsupported marks content that cannot be indexed, such as media,
// archives, image-only PDFs, or files over maxFetchSize. Retrying it would
// give the same result, so it is skipped rather than counted as a failure.
var errUnsupported = errors.New("unsupported content")

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUnsupported, fmt.Sprintf(format, args...))
}

type contentKind int

const (
	kindUnsupported contentKind = iota
	kindText
	kindHTML
	kindXML
	kindPDF
	// kindZip is a zip container that may be an OOXML, OpenDocument, or EPUB
	// file; the parts inside decide.
	kindZip
)

// content is the text handed to the LLM for one payload.
type content struct {
	name   string
	format string
	text   string
}

var textFormats = map[string]string{
	".txt": "plain text", ".md": "Markdown", ".rst": "reStructuredText", ".org": "Org",
	".csv": "CSV table", ".tsv": "TSV table", ".json": "JSON", ".jsonl": "JSON lines",
	".ndjson": "JSON lines", ".geojson": "GeoJSON", ".yaml": "YAML", ".yml": "YAML",
	".toml": "TOML", ".ini": "INI", ".cfg": "configuration", ".tex": "LaTeX",
	".bib": "BibTeX", ".ris": "RIS citation", ".r": "R source", ".py": "Python source",
	".ipynb": "Jupyter notebook", ".jl": "Julia source", ".m": "MATLAB source",
	".go": "Go source", ".java": "Java source", ".c": "C source", ".h": "C header",
	".cpp": "C++ source", ".rs": "Rust source", ".js": "JavaScript source",
	".ts": "TypeScript source", ".sh": "shell script", ".sql": "SQL", ".log": "log",
	".srt": "subtitles", ".vtt": "subtitles", ".nt": "N-Triples", ".ttl": "Turtle",
}

var markupFormats = map[string]contentKind{
	".html": kindHTML, ".htm": kindHTML, ".xhtml": kindHTML,
	".xml": kindXML, ".rdf": kindXML, ".kml": kindXML,
	".gpx": kindXML, ".tei": kindXML, ".jats": kindXML,
}

// skipExts are media, archive, and binary extensions that directory sampling
// does not fetch.
var skipExts = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".bmp": true, ".tif": true,
	".tiff": true, ".webp": true, ".svg": true, ".heic": true, ".ico": true, ".raw": true,
	".mp3": true, ".wav": true, ".flac": true, ".ogg": true, ".m4a": true, ".aac": true,
	".mp4": true, ".mkv": true, ".mov": true, ".avi": true, ".webm": true, ".m4v": true,
	".zip": true, ".gz": true, ".tgz": true, ".tar": true, ".bz2": true, ".xz": true,
	".zst": true, ".7z": true, ".rar": true, ".iso": true, ".img": true, ".bin": true,
	".exe": true, ".dll": true, ".so": true, ".dylib": true, ".class": true, ".jar": true,
	".pyc": true, ".o": true, ".a": true, ".wasm": true, ".ttf": true, ".otf": true,
	".woff": true, ".woff2": true, ".parquet": true, ".h5": true, ".hdf5": true,
	".nc": true, ".npy": true, ".npz": true, ".sqlite": true, ".db": true,
}

var zipFormats = map[string]string{
	".docx": "Word document", ".pptx": "PowerPoint presentation", ".xlsx": "Excel workbook",
	".odt": "OpenDocument text", ".odp": "OpenDocument presentation",
	".ods": "OpenDocument spreadsheet", ".epub": "EPUB book",
}

// classify decides how to read a payload from its name, the gateway's
// Content-Type, and its first bytes. Media types are never indexed.
func classify(name, contentType string, head []byte) (contentKind, string) {
	ext := strings.ToLower(path.Ext(name))
	switch {
	case bytes.HasPrefix(head, []byte("%PDF")):
		return kindPDF, "PDF"
	case bytes.HasPrefix(head, []byte("PK\x03\x04")):
		if f, ok := zipFormats[ext]; ok {
			return kindZip, f
		}
		return kindZip, "zip container"
	}

	mediaType, _, _ := mime.ParseMediaType(contentType)
	if mediaType == "" || mediaType == "application/octet-stream" {
		mediaType, _, _ = mime.ParseMediaType(http.DetectContentType(head))
	}
	if isMedia(mediaType) {
		return kindUnsupported, mediaType
	}
	if kind, ok := markupFormats[ext]; ok {
		return kind, strings.ToUpper(ext[1:])
	}
	if f, ok := textFormats[ext]; ok && looksLikeText(head) {
		return kindText, f
	}
	switch {
	case mediaType == "text/html" || mediaType == "application/xhtml+xml":
		return kindHTML, "HTML"
	case mediaType == "text/xml" || mediaType == "application/xml" || strings.HasSuffix(mediaType, "+xml"):
		return kindXML, "XML"
	}
	if looksLikeText(head) {
		return kindText, "plain text"
	}
	return kindUnsupported, mediaType
}

func isMedia(mediaType string) bool {
	for _, p := range []string{"image/", "audio/", "video/", "font/"} {
		if strings.HasPrefix(mediaType, p) {
			return true
		}
	}
	return false
}

// looksLikeText reports whether b is UTF-8 with almost no control characters.
// A rune cut off at the end of the sniffed prefix is ignored.
func looksLikeText(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for i := 0; i < utf8.UTFMax && !utf8.Valid(b); i++ {
		b = b[:len(b)-1]
	}
	if !utf8.Valid(b) {
		return false
	}
	control := 0
	for _, r := range string(b) {
		if r == 0 {
			return false
		}
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' && r != '\f' {
			control++
		}
	}
	return control*100 <= len(b)
}

// itemName is the file name recorded in a manifest's metadata reference:
// "file://<name>" for watch-folder ingests, the pin name for pin ingests, and
// nothing for "ipfs://<cid>".
func itemName(metaRef string) string {
	switch {
	case strings.HasPrefix(metaRef, "file://"):
		return strings.TrimPrefix(metaRef, "file://")
	case strings.HasPrefix(metaRef, "ipfs://"):
		return ""
	default:
		return metaRef
	}
}

func gatewayPath(root string, segments ...string) string {
	p := url.PathEscape(root)
	for _, s := range segments {
		p += "/" + url.PathEscape(s)
	}
	return p
}

// errDirectory reports that the gateway answered with a directory listing.
var errDirectory = errors.New("directory")

// loadContent fetches a payload and extracts text from it. A UnixFS
// directory is read as its listing plus excerpts of a few readable files.
func (s *Store) loadContent(payloadCID, name string) (*content, error) {
	c, err := cid.Decode(payloadCID)
	if err != nil {
		return nil, fmt.Errorf("payload cid: %w", err)
	}
	if t := c.Type(); t != cid.Raw && t != cid.DagProtobuf {
		return nil, unsupported("codec %s", multicodec.Code(t))
	}
	limit := s.textLimit()
	text, format, err := s.fileText(gatewayPath(payloadCID), name, limit)
	if errors.Is(err, errDirectory) {
		return s.directoryContent(payloadCID, name, limit)
	}
	if err != nil {
		return nil, err
	}
	return &content{name: name, format: format, text: text}, nil
}

// textLimit is the number of bytes of content the current provider accepts.
func (s *Store) textLimit() int {
	if s.snapshotConfig().Provider == ProviderGoogle {
		return googleMaxTextLen
	}
	return maxTextLen
}

func (s *Store) rootNode(payloadCID string) (*merkledag.ProtoNode, *unixfs.FSNode, error) {
	resp, err := s.gatewayGet(gatewayPath(payloadCID)+"?format=raw", manifestFetchTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRootBlockSize))
	if err != nil {
		return nil, nil, fmt.Errorf("read root block: %w", err)
	}
	node, err := merkledag.DecodeProtobuf(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decode root block: %w", err)
	}
	fsn, err := unixfs.FSNodeFromBytes(node.Data())
	if err != nil {
		return nil, nil, unsupported("dag-pb node without UnixFS data")
	}
	return node, fsn, nil
}

type dirEntry struct {
	name string
	size uint64
}

func (s *Store) directoryContent(dirCID, name string, limit int) (*content, error) {
	node, fsn, err := s.rootNode(dirCID)
	if err != nil {
		return nil, err
	}
	entries := directoryEntries(node, fsn)
	var listing strings.Builder
	fmt.Fprintf(&listing, "Directory with %d entries:\n", len(entries))
	for i, e := range entries {
		if i == maxDirEntries {
			fmt.Fprintf(&listing, "... and %d more\n", len(entries)-maxDirEntries)
			break
		}
		fmt.Fprintf(&listing, "%s (%d bytes)\n", e.name, e.size)
	}

	budget := max((limit-listing.Len())/maxDirSamples, minSampleLen)
	var samples strings.Builder
	sampled, attempts := 0, 0
	for _, e := range sampleOrder(entries) {
		if sampled == maxDirSamples || attempts == maxDirAttempts {
			break
		}
		attempts++
		text, format, err := s.fileText(gatewayPath(dirCID, e.name), e.name, budget)
		var rl rateLimitedError
		if errors.As(err, &rl) {
			return nil, err
		}
		if err != nil {
			continue
		}
		fmt.Fprintf(&samples, "\n=== %s (%s) ===\n%s\n", e.name, format, text)
		sampled++
	}
	if sampled == 0 {
		return nil, unsupported("directory without readable files")
	}
	return &content{name: name, format: "directory", text: listing.String() + samples.String()}, nil
}

// directoryEntries lists a directory node's links. Links of a HAMT shard
// carry a hex bucket prefix; links that are only a prefix point to nested
// shards and are left out.
func directoryEntries(node *merkledag.ProtoNode, fsn *unixfs.FSNode) []dirEntry {
	prefix := 0
	if fsn.Type() == unixfs.THAMTShard && fsn.Fanout() > 1 {
		prefix = len(strconv.FormatUint(fsn.Fanout()-1, 16))
	}
	entries := make([]dirEntry, 0, len(node.Links()))
	for _, l := range node.Links() {
		if len(l.Name) <= prefix {
			continue
		}
		entries = append(entries, dirEntry{name: l.Name[prefix:], size: l.Size})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	return entries
}

// sampleOrder puts documents first, then other text files, then names without
// a known extension; known media and archives are dropped.
func sampleOrder(entries []dirEntry) []dirEntry {
	rank := func(name string) int {
		ext := strings.ToLower(path.Ext(name))
		switch {
		case ext == ".pdf" || zipFormats[ext] != "":
			return 0
		case textFormats[ext] != "" || markupFormats[ext] != kindUnsupported:
			return 1
		case skipExts[ext]:
			return -1
		default:
			return 2
		}
	}
	out := make([]dirEntry, 0, len(entries))
	for _, e := range entries {
		if rank(e.name) >= 0 {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return rank(out[i].name) < rank(out[j].name) })
	return out
}

// fileText fetches one file and returns at most limit bytes of its text and a
// short description of its format.
func (s *Store) fileText(ipfsPath, name string, limit int) (string, string, error) {
	resp, err := s.gatewayGet(ipfsPath, payloadFetchTimeout)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if strings.HasPrefix(resp.Header.Get("Etag"), `"DirIndex-`) {
		return "", "", errDirectory
	}

	head := make([]byte, sniffLen)
	n, err := io.ReadFull(resp.Body, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", "", fmt.Errorf("read: %w", err)
	}
	head = head[:n]
	kind, format := classify(name, resp.Header.Get("Content-Type"), head)
	body := io.MultiReader(bytes.NewReader(head), resp.Body)

	var text string
	switch kind {
	case kindText, kindHTML, kindXML:
		data, err := io.ReadAll(io.LimitReader(body, maxTextFetch))
		if err != nil {
			return "", "", fmt.Errorf("read: %w", err)
		}
		switch kind {
		case kindHTML:
			text = htmlText(data)
		case kindXML:
			text = xmlText(bytes.NewReader(data))
		default:
			text = string(data)
		}
	case kindPDF, kindZip:
		if resp.ContentLength > maxFetchSize {
			return "", "", unsupported("%s of %d bytes exceeds %d MB", format, resp.ContentLength, maxFetchSize>>20)
		}
		tmp, err := spoolToTemp(body)
		if err != nil {
			return "", "", err
		}
		defer func() { _ = os.Remove(tmp) }()
		if kind == kindPDF {
			if !s.pdfToText {
				return "", "", unsupported("PDF (pdftotext not installed)")
			}
			text, err = convertPDFFromFile(tmp)
		} else {
			text, format, err = zipText(tmp, format)
		}
		if err != nil {
			return "", "", err
		}
	default:
		return "", "", unsupported("%s", format)
	}

	text = strings.TrimSpace(text)
	if text == "" {
		return "", "", unsupported("%s without text", format)
	}
	return trimUTF8(text, limit), format, nil
}

func spoolToTemp(r io.Reader) (string, error) {
	f, err := os.CreateTemp("", "dlockss-index-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	n, copyErr := io.Copy(f, io.LimitReader(r, maxFetchSize+1))
	closeErr := f.Close()
	switch {
	case copyErr != nil || closeErr != nil:
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("download: %w", errors.Join(copyErr, closeErr))
	case n > maxFetchSize:
		_ = os.Remove(f.Name())
		return "", unsupported("file exceeds %d MB", maxFetchSize>>20)
	}
	return f.Name(), nil
}

func convertPDFFromFile(pdfPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pdftotext", "-layout", "-enc", "UTF-8", pdfPath, "-")
	out := bytes.NewBuffer(make([]byte, 0, maxTextLen+1024))
	var stderr bytes.Buffer
	cmd.Stdout = out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("pdftotext timeout: %w", ctx.Err())
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("pdftotext: %w: %s", err, truncate(msg, 500))
		}
		return "", fmt.Errorf("pdftotext: %w", err)
	}
	return out.String(), nil
}

// zipText reads the text parts of OOXML, OpenDocument, and EPUB files. Other
// zip files are not indexed.
func zipText(zipPath, format string) (string, string, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", "", unsupported("unreadable zip: %v", err)
	}
	defer zr.Close()

	parts, detected := textParts(zr.File)
	if len(parts) == 0 {
		return "", "", unsupported("%s", format)
	}
	if format == "zip container" {
		format = detected
	}
	var b strings.Builder
	for _, f := range parts {
		if b.Len() >= maxTextLen {
			break
		}
		rc, err := f.Open()
		if err != nil {
			continue
		}
		b.WriteString(xmlText(io.LimitReader(rc, maxZipPartSize)))
		b.WriteByte('\n')
		_ = rc.Close()
	}
	return b.String(), format, nil
}

func textParts(files []*zip.File) ([]*zip.File, string) {
	byName := make(map[string]*zip.File, len(files))
	for _, f := range files {
		byName[f.Name] = f
	}
	if f, ok := byName["word/document.xml"]; ok {
		return []*zip.File{f}, "Word document"
	}
	if f, ok := byName["xl/sharedStrings.xml"]; ok {
		return []*zip.File{f}, "Excel workbook"
	}
	if f, ok := byName["content.xml"]; ok {
		return []*zip.File{f}, "OpenDocument file"
	}
	var slides, chapters []*zip.File
	for _, f := range files {
		switch {
		case strings.HasPrefix(f.Name, "ppt/slides/slide") && strings.HasSuffix(f.Name, ".xml"):
			slides = append(slides, f)
		case byName["META-INF/container.xml"] != nil && (strings.HasSuffix(f.Name, ".xhtml") || strings.HasSuffix(f.Name, ".html") || strings.HasSuffix(f.Name, ".htm")):
			chapters = append(chapters, f)
		}
	}
	if len(slides) > 0 {
		sort.Slice(slides, func(i, j int) bool { return slideNumber(slides[i].Name) < slideNumber(slides[j].Name) })
		return slides, "PowerPoint presentation"
	}
	if len(chapters) > 0 {
		sort.Slice(chapters, func(i, j int) bool { return chapters[i].Name < chapters[j].Name })
		return chapters, "EPUB book"
	}
	return nil, ""
}

func slideNumber(name string) int {
	n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "ppt/slides/slide"), ".xml"))
	return n
}

// xmlText returns the character data of an XML document, one space between
// text nodes. Malformed input yields the text read up to the error.
func xmlText(r io.Reader) string {
	dec := xml.NewDecoder(r)
	dec.Strict = false
	dec.AutoClose = xml.HTMLAutoClose
	dec.Entity = xml.HTMLEntity
	var b strings.Builder
	for b.Len() < maxTextFetch {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if cd, ok := tok.(xml.CharData); ok {
			if t := strings.TrimSpace(string(cd)); t != "" {
				b.WriteString(t)
				b.WriteByte(' ')
			}
		}
	}
	return b.String()
}

// htmlText returns the visible text of an HTML document.
func htmlText(data []byte) string {
	z := html.NewTokenizer(bytes.NewReader(data))
	var b strings.Builder
	skip := 0
	for {
		switch z.Next() {
		case html.ErrorToken:
			return b.String()
		case html.StartTagToken:
			if name, _ := z.TagName(); isHiddenTag(name) {
				skip++
			}
		case html.EndTagToken:
			if name, _ := z.TagName(); isHiddenTag(name) && skip > 0 {
				skip--
			}
		case html.TextToken:
			if skip == 0 {
				if t := strings.TrimSpace(string(z.Text())); t != "" {
					b.WriteString(t)
					b.WriteByte(' ')
				}
			}
		}
	}
}

func isHiddenTag(name []byte) bool {
	switch string(name) {
	case "script", "style", "noscript", "template":
		return true
	}
	return false
}
