package clusters

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"dlockss/internal/badbits"
	"dlockss/pkg/schema"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/go-cid"
)

type ipfsPinner interface {
	PinRecursive(ctx context.Context, c cid.Cid) error
	IsPinned(ctx context.Context, c cid.Cid) (bool, error)
	GetBlock(ctx context.Context, c cid.Cid) ([]byte, error)
}

type localPinTracker struct {
	ipfsClient   ipfsPinner
	badBits      *badbits.Filter
	shardID      string
	onPinSynced  func(cid string)
	onPinRemoved func(cid string)

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

// isLegacyManifest fetches the block for a CID and checks if it's a manifest
// with a legacy timestamp field. Returns false if the block can't be fetched
// or decoded (non-manifest CIDs, unavailable blocks).
func (pt *localPinTracker) isLegacyManifest(c cid.Cid) bool {
	ctx, cancel := context.WithTimeout(pt.ctx, 5*time.Second)
	defer cancel()
	data, err := pt.ipfsClient.GetBlock(ctx, c)
	if err != nil {
		return false
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(data); err != nil {
		return false
	}
	return ro.HasLegacyTimestamp
}

func newLocalPinTracker(ipfsClient ipfsPinner, shardID string, onPinSynced func(string), onPinRemoved func(string), badBits *badbits.Filter) *localPinTracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &localPinTracker{
		ipfsClient:   ipfsClient,
		badBits:      badBits,
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
func (pt *localPinTracker) TriggerSync() {
	select {
	case pt.trigger <- struct{}{}:
	default:
		// Already triggered
	}
}

// Start begins monitoring the consensus state and syncing pins.
// consensusClient is the CRDT component to watch.
func (pt *localPinTracker) Start(cc consensusClient) {
	go pt.syncLoop(cc)
}

func (pt *localPinTracker) Stop() {
	pt.cancel()
}

func (pt *localPinTracker) syncLoop(consensus consensusClient) {
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

func (pt *localPinTracker) syncState(consensus consensusClient) {
	// 1. Get Global State
	state, err := consensus.State(pt.ctx)
	if err != nil {
		slog.Error("failed to get consensus state", "shard", pt.shardID, "error", err)
		return
	}

	// 2. Iterate pins (state.List closes out when done; do not close it here)
	out := make(chan api.Pin)
	go func() {
		_ = state.List(pt.ctx, out)
	}()

	// CIDs we should have pinned — all nodes on a shard pin everything in
	// the shard's CRDT.  Allocations are informational only (for monitoring
	// target replication).  We ignore them here so that pins propagated
	// via PinIfAbsent (with -1,-1 "pin everywhere" allocations) or via
	// the ingesting node (with specific allocations) are treated equally.
	shouldHave := make(map[string]struct{})

	for pin := range out {
		c := pin.Cid.Cid
		cStr := c.String()
		shouldHave[cStr] = struct{}{}

		// Check BadBits before syncing (Compliance Check)
		if pt.badBits.IsBlocked(cStr) {
			slog.Warn("refusing to sync blocked content", "shard", pt.shardID, "cid", c)
			continue
		}

		// Skip legacy manifests that contain a timestamp field.
		if pt.isLegacyManifest(c) {
			continue
		}

		isPinned, err := pt.ipfsClient.IsPinned(pt.ctx, c)
		if err != nil {
			slog.Error("failed to check pin status", "shard", pt.shardID, "cid", c, "error", err)
			continue
		}
		if !isPinned {
			slog.Info("syncing pin to local ipfs", "shard", pt.shardID, "cid", c)
			if err := pt.ipfsClient.PinRecursive(pt.ctx, c); err != nil {
				slog.Error("failed to pin", "shard", pt.shardID, "cid", c, "error", err)
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

	// Remove CIDs we previously pinned from this shard but are no longer allocated for.
	// We intentionally do NOT call ipfsClient.UnpinRecursive here because during a
	// shard split/migration, the same CID may be migrated to a child shard on this
	// same node.  If we unpinned from IPFS, the child shard's PinTracker would
	// have to re-fetch the data (unnecessary churn, risk of loss during GC window).
	// Actual IPFS-level unpins are handled by the reshard pass (shard_replication.go)
	// which is migration-aware and calls ipfsClient.UnpinRecursive directly.
	pt.mu.RLock()
	var toUnpin []string
	for cidStr := range pt.pinnedByUs {
		if _, ok := shouldHave[cidStr]; !ok {
			toUnpin = append(toUnpin, cidStr)
		}
	}
	pt.mu.RUnlock()

	for _, cidStr := range toUnpin {
		slog.Info("releasing tracking, no longer in crdt", "shard", pt.shardID, "cid", cidStr)
		if pt.onPinRemoved != nil {
			pt.onPinRemoved(cidStr)
		}
		pt.mu.Lock()
		delete(pt.pinnedByUs, cidStr)
		pt.mu.Unlock()
	}
}
