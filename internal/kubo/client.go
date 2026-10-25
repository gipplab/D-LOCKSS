// Package kubo is a minimal client for the Kubo (go-ipfs) HTTP RPC API,
// covering exactly the endpoints D-LOCKSS needs. Every call honours its
// context. Requires Kubo v0.43.1+ (provide/once; event-stream routing
// endpoints).
package kubo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// ManifestPinName labels pins created by D-LOCKSS for its own manifests, so
// the pin-ingest watcher never re-ingests them.
const ManifestPinName = "dlockss:manifest"

// Client talks to one Kubo daemon.
type Client struct {
	base string // e.g. http://127.0.0.1:5001/api/v0
	http *http.Client
}

// New connects to the Kubo API at the given multiaddr (e.g.
// /ip4/127.0.0.1/tcp/5001 or /dns4/ipfs/tcp/5001) and verifies it is up.
func New(ctx context.Context, apiAddr string) (*Client, error) {
	addr, err := ma.NewMultiaddr(apiAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid IPFS API address %q: %w", apiAddr, err)
	}
	_, hostPort, err := manet.DialArgs(addr)
	if err != nil {
		return nil, fmt.Errorf("unsupported IPFS API address %q: %w", apiAddr, err)
	}
	c := &Client{
		base: "http://" + hostPort + "/api/v0",
		// No global timeout: per-call contexts bound each request.
		http: &http.Client{},
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := c.Version(pingCtx); err != nil {
		return nil, fmt.Errorf("IPFS daemon not reachable at %s: %w", apiAddr, err)
	}
	return c, nil
}

// apiError is Kubo's JSON error body.
type apiError struct {
	Message string `json:"Message"`
}

func (c *Client) request(ctx context.Context, path string, args url.Values, body io.Reader, contentType string) (*http.Response, error) {
	u := c.base + "/" + path
	if len(args) > 0 {
		u += "?" + args.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var e apiError
		if json.Unmarshal(raw, &e) == nil && e.Message != "" {
			return nil, fmt.Errorf("%s: %s", path, e.Message)
		}
		return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return resp, nil
}

// call runs a request and decodes a single JSON object response into out
// (out may be nil to discard the body).
func (c *Client) call(ctx context.Context, path string, args url.Values, out any) error {
	resp, err := c.request(ctx, path, args, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Version returns the daemon version string (also used as the up check).
func (c *Client) Version(ctx context.Context) (string, error) {
	var v struct {
		Version string `json:"Version"`
	}
	if err := c.call(ctx, "version", nil, &v); err != nil {
		return "", err
	}
	return v.Version, nil
}

// unixfsV1Args returns the IPIP-0499 unixfs-v1-2025 import profile: CIDv1,
// sha2-256, raw leaves, 1 MiB chunks, balanced DAG, 1024 links per node.
func unixfsV1Args() url.Values {
	return url.Values{
		"pin":            {"true"},
		"cid-version":    {"1"},
		"hash":           {"sha2-256"},
		"raw-leaves":     {"true"},
		"chunker":        {"size-1048576"},
		"trickle":        {"false"},
		"max-file-links": {"1024"},
	}
}

// ImportFile adds a local file to Kubo with the unixfs-v1-2025 profile and
// pins it. It returns the root CID.
func (c *Client) ImportFile(ctx context.Context, path string) (cid.Cid, error) {
	f, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	defer f.Close()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		part, err := mw.CreateFormFile("file", filepath.Base(path))
		if err == nil {
			_, err = io.Copy(part, f)
		}
		if cerr := mw.Close(); err == nil {
			err = cerr
		}
		pw.CloseWithError(err)
	}()

	resp, err := c.request(ctx, "add", unixfsV1Args(), pr, mw.FormDataContentType())
	if err != nil {
		return cid.Undef, err
	}
	defer resp.Body.Close()

	// The response is NDJSON; the root entry is the last line.
	var last struct {
		Hash string `json:"Hash"`
	}
	dec := json.NewDecoder(resp.Body)
	seen := false
	for {
		var line struct {
			Hash string `json:"Hash"`
		}
		if err := dec.Decode(&line); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return cid.Undef, err
		}
		if line.Hash != "" {
			last, seen = line, true
		}
	}
	if !seen {
		return cid.Undef, errors.New("add: no root CID in response")
	}
	return cid.Decode(last.Hash)
}

// PutDagCBOR stores a dag-cbor block (sha2-256) and returns its CID.
func (c *Client) PutDagCBOR(ctx context.Context, data []byte) (cid.Cid, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		part, err := mw.CreateFormFile("file", "block")
		if err == nil {
			_, err = part.Write(data)
		}
		if cerr := mw.Close(); err == nil {
			err = cerr
		}
		pw.CloseWithError(err)
	}()
	args := url.Values{"cid-codec": {"dag-cbor"}, "mhtype": {"sha2-256"}}
	resp, err := c.request(ctx, "block/put", args, pr, mw.FormDataContentType())
	if err != nil {
		return cid.Undef, err
	}
	defer resp.Body.Close()
	var out struct {
		Key string `json:"Key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return cid.Undef, err
	}
	return cid.Decode(out.Key)
}

// GetBlock fetches a raw block. If it is not local, Kubo fetches it from the
// network until ctx ends.
func (c *Client) GetBlock(ctx context.Context, k cid.Cid) ([]byte, error) {
	resp, err := c.request(ctx, "block/get", url.Values{"arg": {k.String()}}, nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// PinRecursive pins a CID recursively with an optional pin name.
func (c *Client) PinRecursive(ctx context.Context, k cid.Cid, name string) error {
	args := url.Values{"arg": {k.String()}, "recursive": {"true"}}
	if name != "" {
		args.Set("name", name)
	}
	return c.call(ctx, "pin/add", args, nil)
}

// PinManifest pins a manifest recursively under the D-LOCKSS pin name.
func (c *Client) PinManifest(ctx context.Context, k cid.Cid) error {
	return c.PinRecursive(ctx, k, ManifestPinName)
}

// Unpin removes a recursive pin. Unpinning a CID that is not pinned is not
// an error.
func (c *Client) Unpin(ctx context.Context, k cid.Cid) error {
	err := c.call(ctx, "pin/rm", url.Values{"arg": {k.String()}}, nil)
	if err != nil && strings.Contains(err.Error(), "not pinned") {
		return nil
	}
	return err
}

// IsPinned reports whether a CID has a recursive pin.
func (c *Client) IsPinned(ctx context.Context, k cid.Cid) (bool, error) {
	args := url.Values{"arg": {k.String()}, "type": {"recursive"}}
	var out struct {
		Keys map[string]struct{} `json:"Keys"`
	}
	err := c.call(ctx, "pin/ls", args, &out)
	if err != nil {
		if strings.Contains(err.Error(), "is not pinned") {
			return false, nil
		}
		return false, err
	}
	_, ok := out.Keys[k.String()]
	return ok, nil
}

// RecursivePins lists all recursive pins with their names.
func (c *Client) RecursivePins(ctx context.Context) (map[cid.Cid]string, error) {
	args := url.Values{"type": {"recursive"}, "names": {"true"}}
	var out struct {
		Keys map[string]struct {
			Name string `json:"Name"`
		} `json:"Keys"`
	}
	if err := c.call(ctx, "pin/ls", args, &out); err != nil {
		return nil, err
	}
	pins := make(map[cid.Cid]string, len(out.Keys))
	for s, v := range out.Keys {
		k, err := cid.Decode(s)
		if err != nil {
			continue
		}
		pins[k] = v.Name
	}
	return pins, nil
}

// DAGSize returns the payload size of a CID: the file size for files, the
// cumulative size for directories.
func (c *Client) DAGSize(ctx context.Context, k cid.Cid) (uint64, error) {
	var out struct {
		Size           uint64 `json:"Size"`
		CumulativeSize uint64 `json:"CumulativeSize"`
		Type           string `json:"Type"`
	}
	if err := c.call(ctx, "files/stat", url.Values{"arg": {"/ipfs/" + k.String()}}, &out); err != nil {
		return 0, err
	}
	if out.Type == "file" {
		return out.Size, nil
	}
	return out.CumulativeSize, nil
}

// eventProvider mirrors Kubo's routing.QueryEventType Provider value.
const eventProvider = 4

type queryEvent struct {
	Type      int `json:"Type"`
	Responses []struct {
		ID    string   `json:"ID"`
		Addrs []string `json:"Addrs"`
	} `json:"Responses"`
}

// decodeEvents streams NDJSON query events, invoking fn per event until EOF
// or fn returns false.
func decodeEvents(body io.Reader, fn func(queryEvent) bool) error {
	dec := json.NewDecoder(body)
	for {
		var ev queryEvent
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if !fn(ev) {
			return nil
		}
	}
}

// FindProviders streams provider peer IDs for a CID. count <= 0 uses Kubo's
// default. The channel closes when the stream ends or ctx is done.
func (c *Client) FindProviders(ctx context.Context, k cid.Cid, count int) (<-chan peer.AddrInfo, error) {
	args := url.Values{"arg": {k.String()}}
	if count > 0 {
		args.Set("num-providers", fmt.Sprint(count))
	}
	resp, err := c.request(ctx, "routing/findprovs", args, nil, "")
	if err != nil {
		return nil, err
	}
	out := make(chan peer.AddrInfo, 10)
	go func() {
		defer resp.Body.Close()
		defer close(out)
		_ = decodeEvents(resp.Body, func(ev queryEvent) bool {
			if ev.Type != eventProvider {
				return true
			}
			for _, r := range ev.Responses {
				id, err := peer.Decode(r.ID)
				if err != nil {
					continue
				}
				info := peer.AddrInfo{ID: id}
				for _, a := range r.Addrs {
					if addr, err := ma.NewMultiaddr(a); err == nil {
						info.Addrs = append(info.Addrs, addr)
					}
				}
				select {
				case out <- info:
				case <-ctx.Done():
					return false
				}
			}
			return true
		})
	}()
	return out, nil
}

// provideOnce queues the CID with Kubo's provider (Kubo >= 0.43). Success
// replies have an empty body.
func (c *Client) provideOnce(ctx context.Context, k cid.Cid) error {
	return c.call(ctx, "provide/once", url.Values{"arg": {k.String()}}, nil)
}
