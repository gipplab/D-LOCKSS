package clusters

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"dlockss/internal/badbits"
	// "dlockss/internal/config"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/go-cid"
)

// IPFSClient defines the subset of ipfs.IPFSClient needed for pinning
type IPFSClient interface {
	PinRecursive(ctx context.Context, c cid.Cid) error
	UnpinRecursive(ctx context.Context, c cid.Cid) error
	IsPinned(ctx context.Context, c cid.Cid) (bool, error)
	GetBlock(ctx context.Context, c cid.Cid) ([]byte, error)
	GetFileSize(ctx context.Context, c cid.Cid) (uint64, error)
	GetPeerID(ctx context.Context) (string, error)
	ImportFile(ctx context.Context, path string) (cid.Cid, error)
	ImportReader(ctx context.Context, r io.Reader) (cid.Cid, error)
	PutDagCBOR(ctx context.Context, data []byte) (cid.Cid, error)
	// GetShell() interface{} // Removed to avoid interface mismatch if not needed by ClusterManager directly
	SwarmConnect(ctx context.Context, addrs []string) error
	VerifyDAGCompleteness(ctx context.Context, c cid.Cid) (bool, error)
}

// LocalPinTracker monitors the CRDT state and syncs it to the local IPFS node.
// It acts as a bridge between the Cluster Consensus and the actual IPFS Daemon.
type LocalPinTracker struct {
	ipfsClient IPFSClient
	shardID    string

	// State
	mu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Trigger channel for event-driven updates
	trigger chan struct{}
}

func NewLocalPinTracker(ipfsClient IPFSClient, shardID string) *LocalPinTracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalPinTracker{
		ipfsClient: ipfsClient,
		shardID:    shardID,
		ctx:        ctx,
		cancel:     cancel,
		trigger:    make(chan struct{}, 1),
	}
}

// TriggerSync forces an immediate sync check.
func (pt *LocalPinTracker) TriggerSync() {
	select {
	case pt.trigger <- struct{}{}:
	default:
		// Already triggered
	}
}

// Start begins monitoring the consensus state and syncing pins.
// consensusClient is the CRDT component to watch.
func (pt *LocalPinTracker) Start(consensusClient ConsensusClient) {
	go pt.syncLoop(consensusClient)
}

func (pt *LocalPinTracker) Stop() {
	pt.cancel()
}

func (pt *LocalPinTracker) syncLoop(consensus ConsensusClient) {
	ticker := time.NewTicker(30 * time.Second) // Poll state every 30s as backup
	defer ticker.Stop()

	for {
		select {
		case <-pt.ctx.Done():
			return
		case <-ticker.C:
			pt.syncState(consensus)
		case <-pt.trigger:
			pt.syncState(consensus)
		}
	}
}

func (pt *LocalPinTracker) syncState(consensus ConsensusClient) {
	// 1. Get Global State
	state, err := consensus.State(pt.ctx)
	if err != nil {
		log.Printf("[PinTracker:%s] Failed to get consensus state: %v", pt.shardID, err)
		return
	}

	// 2. Iterate pins
	out := make(chan api.Pin)
	go func() {
		defer close(out)
		_ = state.List(pt.ctx, out)
	}()

	for pin := range out {
		// Check if we are allocated
		// Note: CRDT allocates to specific Peer IDs.
		// If we are not in Allocations, we should NOT pin it (unless replication factor says everyone).
		// For now, in CRDT mode, usually everyone pins if they are part of the cluster?
		// No, usually IPFS Cluster assigns allocations.
		// BUT: D-LOCKSS model is "everyone in shard replicates".
		// Does CRDT automatically add everyone to allocations?
		// If we use "ReplicationFactorMax: -1" (all), then everyone should pin.

		// For now, we assume if it's in the state, we pin it (Full Replication per Shard).
		// Optimization: Check allocations later.

		c := pin.Cid.Cid

		// Check BadBits before syncing (Compliance Check)
		if badbits.IsCIDBlocked(c.String()) {
			log.Printf("[PinTracker:%s] Refusing to sync blocked content %s", pt.shardID, c)
			continue
		}

		isPinned, _ := pt.ipfsClient.IsPinned(pt.ctx, c)
		if !isPinned {
			log.Printf("[PinTracker:%s] Syncing pin %s to local IPFS", pt.shardID, c)
			if err := pt.ipfsClient.PinRecursive(pt.ctx, c); err != nil {
				log.Printf("[PinTracker:%s] Failed to pin %s: %v", pt.shardID, c, err)
			}
		}
	}
}
