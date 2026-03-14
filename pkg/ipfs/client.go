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
	PinRecursive(ctx context.Context, cid cid.Cid) error
	UnpinRecursive(ctx context.Context, cid cid.Cid) error
	IsPinned(ctx context.Context, cid cid.Cid) (bool, error)
}

// Client wraps IPFS API operations for importing files and managing pins.
type Client struct {
	api *ipfsapi.Shell
}

// shell returns the underlying API handle. Package-internal only; callers
// outside this package should use NewIPFSDHTAdapterFromClient instead of
// reaching for the raw shell.
func (c *Client) shell() *ipfsapi.Shell {
	return c.api
}

var _ IPFSClient = (*Client)(nil)

// NewClient creates a new IPFS client.
// If ipfsNodeAddr is empty, it defaults to local IPFS node at /ip4/127.0.0.1/tcp/5001
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

// ImportFile imports a file into IPFS as UnixFS. ctx unused (go-ipfs-api Add has no cancel).
func (c *Client) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	ipfsPath, err := c.api.Add(file, ipfsapi.Pin(true))
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
	_ = ctx
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
	_ = ctx
	b, err := c.api.BlockGet(blockCID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockCID.String(), err)
	}
	return b, nil
}

// PinRecursive pins the full DAG. ctx unused (Pin has no cancel).
func (c *Client) PinRecursive(ctx context.Context, cid cid.Cid) error {
	return c.api.Pin(cid.String())
}

// UnpinRecursive unpins the full DAG. ctx unused (Unpin has no cancel).
func (c *Client) UnpinRecursive(ctx context.Context, cid cid.Cid) error {
	return c.api.Unpin(cid.String())
}

func (c *Client) IsPinned(ctx context.Context, cid cid.Cid) (bool, error) {
	var raw struct{ Keys map[string]ipfsapi.PinInfo }
	if err := c.api.Request("pin/ls", cid.String()).Exec(ctx, &raw); err != nil {
		// IPFS returns an error when the CID is not pinned — that's a valid "no".
		if strings.Contains(err.Error(), "is not pinned") {
			return false, nil
		}
		return false, fmt.Errorf("pin/ls check failed for %s: %w", cid.String(), err)
	}
	_, ok := raw.Keys[cid.String()]
	return ok, nil
}
