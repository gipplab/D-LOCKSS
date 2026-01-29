package ipfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
)

// MockClient is a thread-safe mock implementation of IPFSClient for testing.
type MockClient struct {
	mu          sync.RWMutex
	Pins        map[string]bool
	Blocks      map[string][]byte
	FileSizes   map[string]uint64
	FailPin     bool
	FailUnpin   bool
	FailGet     bool
	FailImport  bool
}

// NewMockClient creates a new initialized MockClient.
func NewMockClient() *MockClient {
	return &MockClient{
		Pins:      make(map[string]bool),
		Blocks:    make(map[string][]byte),
		FileSizes: make(map[string]uint64),
	}
}

func (m *MockClient) ImportFile(ctx context.Context, filePath string) (cid.Cid, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.FailImport {
		return cid.Cid{}, fmt.Errorf("mock import failure")
	}

	// For testing, we just generate a dummy CID based on the path
	// In a real mock we might read the file content
	// Payload CID: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
	c, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	m.Pins[c.String()] = true
	return c, nil
}

func (m *MockClient) ImportReader(ctx context.Context, reader io.Reader) (cid.Cid, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.FailImport {
		return cid.Cid{}, fmt.Errorf("mock import failure")
	}
	c, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	m.Pins[c.String()] = true
	return c, nil
}

func (m *MockClient) PutDagCBOR(ctx context.Context, block []byte) (cid.Cid, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Use a different valid CID for Manifest (using a known valid legacy CID for safety)
	c, _ := cid.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	
	m.Blocks[c.String()] = block
	return c, nil
}

func (m *MockClient) GetBlock(ctx context.Context, blockCID cid.Cid) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.FailGet {
		return nil, fmt.Errorf("mock get block failure")
	}

	block, ok := m.Blocks[blockCID.String()]
	if !ok {
		return nil, fmt.Errorf("block not found: %s", blockCID.String())
	}
	return block, nil
}

func (m *MockClient) PinRecursive(ctx context.Context, cid cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.FailPin {
		return fmt.Errorf("mock pin failure")
	}

	m.Pins[cid.String()] = true
	return nil
}

func (m *MockClient) UnpinRecursive(ctx context.Context, cid cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.FailUnpin {
		return fmt.Errorf("mock unpin failure")
	}

	delete(m.Pins, cid.String())
	return nil
}

func (m *MockClient) IsPinned(ctx context.Context, cid cid.Cid) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Pins[cid.String()], nil
}

func (m *MockClient) GetFileSize(ctx context.Context, cid cid.Cid) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if size, ok := m.FileSizes[cid.String()]; ok {
		return size, nil
	}
	return 0, nil
}

func (m *MockClient) VerifyDAGCompleteness(ctx context.Context, rootCID cid.Cid) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Mock: if root is pinned, we assume DAG is complete
	return m.Pins[rootCID.String()], nil
}

// Ensure MockClient implements IPFSClient
var _ IPFSClient = (*MockClient)(nil)
