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
	"github.com/libp2p/go-libp2p/core/peer"
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

// OnPinSynced is called when a pin is present locally (after sync or already pinned).
// Used so the node can register the CID with storage and announce it (e.g. PINNED on pubsub),
// allowing the monitor to count replication per file.
type OnPinSynced func(cid string)

// OnPinRemoved is called when we unpin a CID (no longer allocated). Used so storage/heartbeat count stays correct.
type OnPinRemoved func(cid string)

// LocalPinTracker monitors the CRDT state and syncs it to the local IPFS node.
// It acts as a bridge between the Cluster Consensus and the actual IPFS Daemon.
// Tracks which CIDs we pinned from this shard so we can unpin when no longer allocated.
type LocalPinTracker struct {
	ipfsClient   IPFSClient
	shardID      string
	onPinSynced  OnPinSynced
	onPinRemoved OnPinRemoved

	// State
	mu sync.RWMutex

	// pinnedByUs: CIDs we pinned from this shard's CRDT (so we can unpin when no longer allocated)
	pinnedByUs map[string]struct{}

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Trigger channel for event-driven updates
	trigger chan struct{}
}

func NewLocalPinTracker(ipfsClient IPFSClient, shardID string, onPinSynced OnPinSynced, onPinRemoved OnPinRemoved) *LocalPinTracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalPinTracker{
		ipfsClient:   ipfsClient,
		shardID:      shardID,
		onPinSynced:  onPinSynced,
		onPinRemoved: onPinRemoved,
		pinnedByUs:   make(map[string]struct{}),
		ctx:          ctx,
		cancel:       cancel,
		trigger:      make(chan struct{}, 1),
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
	ticker := time.NewTicker(10 * time.Second) // Poll state every 10s so peers replicate sooner
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

	// 2. Iterate pins (state.List closes out when done; do not close it here)
	out := make(chan api.Pin)
	go func() {
		_ = state.List(pt.ctx, out)
	}()

	// Resolve our peer ID once per sync (for allocation check).
	ourPeerIDStr, err := pt.ipfsClient.GetPeerID(pt.ctx)
	if err != nil {
		log.Printf("[PinTracker:%s] Failed to get our peer ID: %v", pt.shardID, err)
		return
	}
	ourPeerID, err := peer.Decode(ourPeerIDStr)
	if err != nil {
		log.Printf("[PinTracker:%s] Invalid peer ID %s: %v", pt.shardID, ourPeerIDStr, err)
		return
	}

	// CIDs we should have pinned (we are allocated for these, or Allocations is empty).
	shouldHave := make(map[string]struct{})

	for pin := range out {
		// Allocation-aware: only pin if we are in Allocations, or if Allocations is empty (full replication).
		if len(pin.Allocations) > 0 {
			weAreAllocated := false
			for _, p := range pin.Allocations {
				if p == ourPeerID {
					weAreAllocated = true
					break
				}
			}
			if !weAreAllocated {
				continue
			}
		}

		c := pin.Cid.Cid
		cStr := c.String()
		shouldHave[cStr] = struct{}{}

		// Check BadBits before syncing (Compliance Check)
		if badbits.IsCIDBlocked(cStr) {
			log.Printf("[PinTracker:%s] Refusing to sync blocked content %s", pt.shardID, c)
			continue
		}

		isPinned, err := pt.ipfsClient.IsPinned(pt.ctx, c)
		if err != nil {
			log.Printf("[PinTracker:%s] Error checking pin status for %s: %v", pt.shardID, c, err)
			continue
		}
		if !isPinned {
			log.Printf("[PinTracker:%s] Syncing pin %s to local IPFS", pt.shardID, c)
			if err := pt.ipfsClient.PinRecursive(pt.ctx, c); err != nil {
				log.Printf("[PinTracker:%s] Failed to pin %s: %v", pt.shardID, c, err)
				continue
			}
		}
		pt.mu.Lock()
		_, alreadyTracked := pt.pinnedByUs[cStr]
		pt.pinnedByUs[cStr] = struct{}{}
		pt.mu.Unlock()
		// Only notify on first sync so we don't spam announcements every 10s.
		if !alreadyTracked && pt.onPinSynced != nil {
			pt.onPinSynced(cStr)
		}
	}

	// Unpin CIDs we previously pinned from this shard but are no longer allocated for.
	// Collect the list under the lock, then process outside to avoid data race
	// from unlocking/relocking mid-iteration.
	pt.mu.RLock()
	var toUnpin []string
	for cidStr := range pt.pinnedByUs {
		if _, ok := shouldHave[cidStr]; !ok {
			toUnpin = append(toUnpin, cidStr)
		}
	}
	pt.mu.RUnlock()

	for _, cidStr := range toUnpin {
		c, err := cid.Decode(cidStr)
		if err != nil {
			pt.mu.Lock()
			delete(pt.pinnedByUs, cidStr)
			pt.mu.Unlock()
			continue
		}
		log.Printf("[PinTracker:%s] Unpinning %s (no longer allocated)", pt.shardID, cidStr)
		_ = pt.ipfsClient.UnpinRecursive(pt.ctx, c)
		if pt.onPinRemoved != nil {
			pt.onPinRemoved(cidStr)
		}
		pt.mu.Lock()
		delete(pt.pinnedByUs, cidStr)
		pt.mu.Unlock()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
