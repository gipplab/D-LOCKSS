package clusters

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"dlockss/internal/badbits"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs/go-cid"
)

type ipfsPinner interface {
	PinManifest(ctx context.Context, c cid.Cid) error
	IsPinned(ctx context.Context, c cid.Cid) (bool, error)
	GetBlock(ctx context.Context, c cid.Cid) ([]byte, error)
}

type localPinTracker struct {
	ipfsClient   ipfsPinner
	badBits      *badbits.Filter
	shardID      string
	onPinSynced  func(cid string)
	onPinRemoved func(cid string)
	origin       trust.OriginGate

	mu sync.RWMutex
	// tracked holds the CIDs synced from this shard's CRDT, so tracking can
	// be released once they leave it.
	tracked map[string]struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	trigger chan struct{}
}

func isLegacyManifestBlock(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(data); err != nil {
		return false
	}
	return ro.HasLegacyTimestamp
}

func newLocalPinTracker(ipfsClient ipfsPinner, shardID string, onPinSynced func(string), onPinRemoved func(string), badBits *badbits.Filter, origin trust.OriginGate) *localPinTracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &localPinTracker{
		ipfsClient:   ipfsClient,
		badBits:      badBits,
		shardID:      shardID,
		onPinSynced:  onPinSynced,
		onPinRemoved: onPinRemoved,
		origin:       origin,
		tracked:      make(map[string]struct{}),
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
		// A sync is already pending.
	}
}

// Start syncs local pins against the consensus state until Stop is called.
func (pt *localPinTracker) Start(cc consensusClient) {
	go pt.syncLoop(cc)
}

func (pt *localPinTracker) Stop() {
	pt.cancel()
}

func (pt *localPinTracker) syncLoop(consensus consensusClient) {
	ticker := time.NewTicker(10 * time.Second)
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
	st, err := consensus.State(pt.ctx)
	if err != nil {
		slog.Error("failed to get consensus state", "shard", pt.shardID, "error", err)
		return
	}

	// List closes out when it is done.
	out := make(chan api.Pin)
	go func() {
		_ = st.List(pt.ctx, out)
	}()

	// Every shard member pins everything in the shard's CRDT. Allocations are
	// informational (target replication for monitoring) and are ignored here,
	// so "pin everywhere" pins from PinIfAbsent and pins with explicit
	// allocations from the ingesting node are treated alike.
	shouldHave := make(map[string]struct{})

	for pin := range out {
		c := pin.Cid.Cid
		cStr := c.String()
		shouldHave[cStr] = struct{}{}

		if pt.badBits.IsBlocked(cStr) {
			slog.Warn("refusing to sync blocked content", "shard", pt.shardID, "cid", c)
			continue
		}

		block, getErr := pt.ipfsClient.GetBlock(pt.ctx, c)
		if isLegacyManifestBlock(block) {
			continue
		}
		if pt.origin != nil && pt.origin.RestrictsOrigins() {
			if getErr != nil {
				slog.Warn("refusing pin: cannot read manifest for origin check", "shard", pt.shardID, "cid", c, "error", getErr)
				continue
			}
			if _, err := trust.CheckManifestOrigin(block, pt.origin); err != nil {
				slog.Warn("refusing to store content from untrusted origin", "shard", pt.shardID, "cid", c, "error", err)
				continue
			}
		}

		isPinned, err := pt.ipfsClient.IsPinned(pt.ctx, c)
		if err != nil {
			slog.Error("failed to check pin status", "shard", pt.shardID, "cid", c, "error", err)
			continue
		}
		if !isPinned {
			slog.Info("syncing pin to local ipfs", "shard", pt.shardID, "cid", c)
			if err := pt.ipfsClient.PinManifest(pt.ctx, c); err != nil {
				slog.Error("failed to pin", "shard", pt.shardID, "cid", c, "error", err)
				continue
			}
		}
		pt.mu.Lock()
		_, alreadyTracked := pt.tracked[cStr]
		pt.tracked[cStr] = struct{}{}
		pt.mu.Unlock()
		// Notify only on the first sync; later syncs repeat every tick.
		if !alreadyTracked && pt.onPinSynced != nil {
			pt.onPinSynced(cStr)
		}
	}

	// CIDs that left the CRDT only lose tracking; their IPFS pins stay. During
	// a split the same CID may move to a child shard on this node, and
	// unpinning would force a re-fetch and risk loss in the GC window.
	// IPFS-level unpins belong to the migration-aware reshard pass.
	pt.mu.RLock()
	var released []string
	for cidStr := range pt.tracked {
		if _, ok := shouldHave[cidStr]; !ok {
			released = append(released, cidStr)
		}
	}
	pt.mu.RUnlock()

	for _, cidStr := range released {
		slog.Info("releasing tracking, no longer in crdt", "shard", pt.shardID, "cid", cidStr)
		if pt.onPinRemoved != nil {
			pt.onPinRemoved(cidStr)
		}
		pt.mu.Lock()
		delete(pt.tracked, cidStr)
		pt.mu.Unlock()
	}
}
