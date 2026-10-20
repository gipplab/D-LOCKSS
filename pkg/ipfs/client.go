package ipfs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
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
// local node at /ip4/127.0.0.1/tcp/5001.
func NewClient(ipfsNodeAddr string) (*Client, error) {
	if ipfsNodeAddr == "" {
		ipfsNodeAddr = "/ip4/127.0.0.1/tcp/5001"
	}

	api := ipfsapi.NewShell(ipfsNodeAddr)
	if !api.IsUp() {
		return nil, fmt.Errorf("IPFS node is not available at %s", ipfsNodeAddr)
	}

	return &Client{api: api}, nil
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

// GetBlock fetches block bytes by CID. ctx unused (BlockGet has no cancel).
func (c *Client) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	b, err := c.api.BlockGet(blockCID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockCID, err)
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
	if err := c.api.Request("pin/ls", root.String()).Exec(ctx, &raw); err != nil {
		// Kubo reports an unpinned CID as an error.
		if strings.Contains(err.Error(), "is not pinned") {
			return false, nil
		}
		return false, fmt.Errorf("pin/ls check failed for %s: %w", root, err)
	}
	_, ok := raw.Keys[root.String()]
	return ok, nil
}
