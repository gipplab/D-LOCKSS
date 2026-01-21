package ipfs

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
)

// Client wraps IPFS API operations for importing files and managing pins.
type Client struct {
	api *ipfsapi.Shell
}

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

// ImportFile imports a file from the local filesystem into IPFS as UnixFS.
// Returns the CID of the imported file (PayloadCID).
// NOTE: go-ipfs-api's Add API does not support context cancellation; ctx is
// accepted for future compatibility but is not currently used to abort the call.
func (c *Client) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Import file to IPFS (creates UnixFS DAG)
	ipfsPath, err := c.api.Add(file, ipfsapi.Pin(true))
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to import file to IPFS: %w", err)
	}

	// Parse CID from IPFS path
	parsedCID, err := cid.Decode(ipfsPath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to parse CID from IPFS path: %w", err)
	}

	return parsedCID, nil
}

// ImportReader imports data from an io.Reader into IPFS as UnixFS.
// Returns the CID of the imported data (PayloadCID).
// NOTE: go-ipfs-api's Add API does not support context cancellation; ctx is
// accepted for future compatibility but is not currently used to abort the call.
func (c *Client) ImportReader(ctx context.Context, reader io.Reader) (cid.Cid, error) {
	ipfsPath, err := c.api.Add(reader, ipfsapi.Pin(true))
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to import data to IPFS: %w", err)
	}

	parsedCID, err := cid.Decode(ipfsPath)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to parse CID from IPFS path: %w", err)
	}

	return parsedCID, nil
}

// PutDagCBOR stores a dag-cbor block in IPFS and returns its CID.
// This is used for ResearchObject manifests.
// NOTE: go-ipfs-api's BlockPut API does not support context cancellation;
// ctx is accepted for future compatibility but is not currently used.
func (c *Client) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	// go-ipfs-api doesn't take context for BlockPut; keep ctx for future upgrades.
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

// GetBlock fetches the raw bytes of a block by CID.
// NOTE: go-ipfs-api's BlockGet API does not support context cancellation;
// ctx is accepted for future compatibility but is not currently used.
func (c *Client) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	_ = ctx
	b, err := c.api.BlockGet(blockCID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockCID.String(), err)
	}
	return b, nil
}

// PinRecursive pins a CID recursively (pins the entire DAG).
// NOTE: go-ipfs-api's Pin API does not support context cancellation.
func (c *Client) PinRecursive(ctx context.Context, cid cid.Cid) error {
	return c.api.Pin(cid.String())
}

// UnpinRecursive unpins a CID recursively.
// NOTE: go-ipfs-api's Unpin API does not support context cancellation.
func (c *Client) UnpinRecursive(ctx context.Context, cid cid.Cid) error {
	return c.api.Unpin(cid.String())
}

// IsPinned checks if a CID is pinned.
func (c *Client) IsPinned(ctx context.Context, cid cid.Cid) (bool, error) {
	// Prefer a targeted pin/ls <cid> query to avoid enumerating all pins (which can be huge).
	// go-ipfs-api v0.7.0 doesn't expose PinLs directly, but Shell.Request supports it.
	var raw struct{ Keys map[string]ipfsapi.PinInfo }
	if err := c.api.Request("pin/ls", cid.String()).Exec(ctx, &raw); err == nil {
		_, ok := raw.Keys[cid.String()]
		return ok, nil
	}

	// Fallback to enumerating all pins (may be expensive).
	pins, err := c.api.Pins()
	if err != nil {
		return false, err
	}
	_, isPinned := pins[cid.String()]
	return isPinned, nil
}

// GetFileSize returns the size of a file in the IPFS DAG.
// NOTE: FilesStat currently uses ctx for timeout/cancellation.
func (c *Client) GetFileSize(ctx context.Context, cid cid.Cid) (uint64, error) {
	stat, err := c.api.FilesStat(ctx, "/ipfs/"+cid.String())
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	return uint64(stat.Size), nil
}

// VerifyDAGCompleteness verifies that a CID and all its children are available locally.
// Returns true if the entire DAG is present.
//
// NOTE: This is currently a conservative stub. We treat "root is pinned" as a
// proxy for DAG completeness because go-ipfs-api does not expose a cheap way
// to walk and verify every child block. Callers should treat a true result as
// "locally pinned according to IPFS" rather than a full cryptographic audit
// of all descendants.
func (c *Client) VerifyDAGCompleteness(ctx context.Context, rootCID cid.Cid) (bool, error) {
	// Check if root is pinned
	isPinned, err := c.IsPinned(ctx, rootCID)
	if err != nil {
		return false, err
	}
	if !isPinned {
		return false, nil
	}

	// TODO: Implement full recursive DAG verification.
	// For now, treat "pinned" as the completeness signal.
	return true, nil
}
