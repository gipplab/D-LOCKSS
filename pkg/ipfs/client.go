package ipfs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// IPFSClient is the interface for IPFS node operations.
type IPFSClient interface {
	ImportFile(ctx context.Context, filePath string) (cid.Cid, error)
	PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error)
	GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error)
	PinRecursive(ctx context.Context, root cid.Cid) error
	PinManifest(ctx context.Context, root cid.Cid) error
	UnpinRecursive(ctx context.Context, root cid.Cid) error
	IsPinned(ctx context.Context, root cid.Cid) (bool, error)
	RecursivePins(ctx context.Context) (map[cid.Cid]string, error)
	DAGSize(ctx context.Context, root cid.Cid) (uint64, error)
	NodeID(ctx context.Context) (peer.ID, error)
	SignWithNodeKey(ctx context.Context, data []byte) ([]byte, error)
}

// ManifestPinName labels the Kubo pins of D-LOCKSS manifests, so pin-based
// ingest can tell them from pins that users create.
const ManifestPinName = "dlockss:manifest"

// Client wraps IPFS API operations for importing files and managing pins.
type Client struct {
	api *ipfsapi.Shell
}

var _ IPFSClient = (*Client)(nil)

// NewClient creates a new IPFS client. An empty ipfsNodeAddr selects the
// local node at /ip4/127.0.0.1/tcp/5001. A non-empty authSecret (a bare
// token, or a Kubo API.Authorizations AuthSecret such as "bearer:<token>")
// authorizes every request.
func NewClient(ipfsNodeAddr, authSecret string) (*Client, error) {
	if ipfsNodeAddr == "" {
		ipfsNodeAddr = "/ip4/127.0.0.1/tcp/5001"
	}

	api := ipfsapi.NewShell(ipfsNodeAddr)
	if authSecret != "" {
		header, err := authHeader(authSecret)
		if err != nil {
			return nil, err
		}
		// Same transport settings as ipfsapi.NewShell.
		base := &http.Transport{Proxy: http.ProxyFromEnvironment, DisableKeepAlives: true}
		api = ipfsapi.NewShellWithClient(ipfsNodeAddr, &http.Client{Transport: authorize{header, base}})
	}
	if _, _, err := api.Version(); err != nil {
		var refused *ipfsapi.Error
		if errors.As(err, &refused) {
			return nil, fmt.Errorf("IPFS node at %s refused the request: %w", ipfsNodeAddr, err)
		}
		return nil, fmt.Errorf("IPFS node is not available at %s (start IPFS with: ipfs daemon): %w", ipfsNodeAddr, err)
	}

	return &Client{api: api}, nil
}

// authorize adds an Authorization header to every request.
type authorize struct {
	header string
	base   http.RoundTripper
}

func (a authorize) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", a.header)
	return a.base.RoundTrip(req)
}

// ImportFile imports a file into IPFS using IPIP-0499 unixfs-v1-2025
// (CIDv1, sha2-256, 1 MiB raw leaves, balanced DAG, 1024 links per file node).
// ctx unused (go-ipfs-api Add has no cancel).
func (c *Client) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	ipfsPath, err := c.api.Add(file, unixfsV1AddOpts()...)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to import file to IPFS: %w", err)
	}

	parsedCID, err := cid.Decode(ipfsPath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to parse CID from IPFS path: %w", err)
	}

	return parsedCID, nil
}

// PutDagCBOR stores a dag-cbor block (e.g. ResearchObject). ctx unused (BlockPut has no cancel).
func (c *Client) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	cidStr, err := c.api.BlockPut(block, "dag-cbor", "sha2-256", -1)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to put dag-cbor block: %w", err)
	}
	parsed, err := cid.Decode(cidStr)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to decode block CID: %w", err)
	}
	return parsed, nil
}

// GetBlock fetches block bytes by CID. A block that is not local is fetched
// from the network until ctx ends.
func (c *Client) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	resp, err := c.api.Request("block/get", blockCID.String()).Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockCID, err)
	}
	defer resp.Close()
	if resp.Error != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockCID, resp.Error)
	}
	b, err := io.ReadAll(resp.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to read block %s: %w", blockCID, err)
	}
	return b, nil
}

// PinRecursive pins the full DAG. ctx unused (Pin has no cancel).
func (c *Client) PinRecursive(ctx context.Context, root cid.Cid) error {
	return c.api.Pin(root.String())
}

// PinManifest pins a manifest's full DAG under ManifestPinName. Pinning a CID
// that is already pinned only updates the pin's name.
func (c *Client) PinManifest(ctx context.Context, root cid.Cid) error {
	return c.api.Request("pin/add", root.String()).
		Option("recursive", true).
		Option("name", ManifestPinName).
		Exec(ctx, nil)
}

// RecursivePins returns the root of every recursive pin with its name.
func (c *Client) RecursivePins(ctx context.Context) (map[cid.Cid]string, error) {
	var raw struct {
		Keys map[string]struct {
			Name string `json:"Name"`
		} `json:"Keys"`
	}
	if err := c.api.Request("pin/ls").Option("type", "recursive").Option("names", true).Exec(ctx, &raw); err != nil {
		return nil, fmt.Errorf("pin/ls failed: %w", err)
	}
	pins := make(map[cid.Cid]string, len(raw.Keys))
	for k, v := range raw.Keys {
		pinned, err := cid.Decode(k)
		if err != nil {
			continue
		}
		pins[pinned] = v.Name
	}
	return pins, nil
}

// DAGSize returns the content size of a UnixFS file, or the cumulative size of
// a directory.
func (c *Client) DAGSize(ctx context.Context, root cid.Cid) (uint64, error) {
	var st struct {
		Type           string `json:"Type"`
		Size           uint64 `json:"Size"`
		CumulativeSize uint64 `json:"CumulativeSize"`
	}
	if err := c.api.Request("files/stat", "/ipfs/"+root.String()).Exec(ctx, &st); err != nil {
		return 0, fmt.Errorf("files/stat failed for %s: %w", root, err)
	}
	if st.Type == "file" {
		return st.Size, nil
	}
	return st.CumulativeSize, nil
}

// UnpinRecursive unpins the full DAG. ctx unused (Unpin has no cancel).
func (c *Client) UnpinRecursive(ctx context.Context, root cid.Cid) error {
	return c.api.Unpin(root.String())
}

// IsPinned reports whether root is pinned.
func (c *Client) IsPinned(ctx context.Context, root cid.Cid) (bool, error) {
	var raw struct{ Keys map[string]ipfsapi.PinInfo }
	// Manifests are always pinned recursively. Without a type, Kubo also
	// checks indirect pins by walking every pinned DAG.
	if err := c.api.Request("pin/ls", root.String()).Option("type", "recursive").Exec(ctx, &raw); err != nil {
		// Kubo reports an unpinned CID as an error.
		if strings.Contains(err.Error(), "is not pinned") {
			return false, nil
		}
		return false, fmt.Errorf("pin/ls check failed for %s: %w", root, err)
	}
	_, ok := raw.Keys[root.String()]
	return ok, nil
}

// NodeID returns the peer ID of the Kubo node.
func (c *Client) NodeID(ctx context.Context) (peer.ID, error) {
	var out struct{ ID string }
	if err := c.api.Request("id").Exec(ctx, &out); err != nil {
		return "", fmt.Errorf("id failed: %w", err)
	}
	return peer.Decode(out.ID)
}

// SignWithNodeKey signs data with the Kubo node's key, as 'ipfs key sign' does.
func (c *Client) SignWithNodeKey(ctx context.Context, data []byte) ([]byte, error) {
	var out struct{ Signature string }
	body := files.NewSliceDirectory([]files.DirEntry{files.FileEntry("", files.NewBytesFile(data))})
	if err := c.api.Request("key/sign").Body(files.NewMultiFileReader(body, true, false)).Exec(ctx, &out); err != nil {
		return nil, fmt.Errorf("key/sign failed: %w", err)
	}
	sig, ok := strings.CutPrefix(out.Signature, "u")
	if !ok {
		return nil, fmt.Errorf("key/sign returned an unexpected signature encoding %q", out.Signature)
	}
	return base64.RawURLEncoding.DecodeString(sig)
}
