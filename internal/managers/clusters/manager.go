package clusters

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/pkg/ipfs"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/consensus/crdt"
	"github.com/ipfs-cluster/ipfs-cluster/state"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
)

// ClusterManagerInterface defines the interface for ClusterManager to allow mocking.
type ClusterManagerInterface interface {
	JoinShard(ctx context.Context, shardID string, bootstrapPeers []multiaddr.Multiaddr) error
	LeaveShard(shardID string) error
	Pin(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error
	PinIfAbsent(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error
	Unpin(ctx context.Context, shardID string, c cid.Cid) error
	GetAllocations(ctx context.Context, shardID string, c cid.Cid) ([]peer.ID, error)
	GetPeerCount(ctx context.Context, shardID string) (int, error)
	MigratePins(ctx context.Context, fromShard, toShard string) error
	TriggerSync(shardID string)
}

// ShardPeerProvider supplies peers for a shard (e.g. pubsub mesh). Used by CRDT for allocations.
type ShardPeerProvider interface {
	GetPeersForShard(shardID string) []peer.ID
}

// ClusterManager manages multiple embedded IPFS Cluster instances (Consensus/PinTracker)
// sharing the same underlying IPFS node.
type ClusterManager struct {
	cfg          *config.Config
	host         host.Host
	ipfsClient   ipfs.IPFSClient
	badBits      *badbits.Filter
	pubsub       *pubsub.PubSub
	dht          routing.Routing
	datastore    datastore.Datastore
	trustedPeers []peer.ID
	onPinSynced  func(cid string)
	onPinRemoved func(cid string)
	peerProvider ShardPeerProvider

	mu       sync.RWMutex
	clusters map[string]*EmbeddedCluster
}

// ConsensusClient defines the interface for interacting with the consensus component.
type ConsensusClient interface {
	LogPin(ctx context.Context, pin api.Pin) error
	LogUnpin(ctx context.Context, pin api.Pin) error
	State(ctx context.Context) (state.ReadOnly, error)
	Peers(ctx context.Context) ([]peer.ID, error)
	Shutdown(ctx context.Context) error
}

// EmbeddedCluster represents a single shard's consensus state (CRDT).
type EmbeddedCluster struct {
	ShardID string
	// Consensus holds the CRDT state for this shard
	Consensus ConsensusClient
	// PinTracker syncs consensus to IPFS
	PinTracker *LocalPinTracker

	ctx    context.Context
	cancel context.CancelFunc
}

// ClusterManagerConfig holds all dependencies for a ClusterManager.
type ClusterManagerConfig struct {
	Cfg          *config.Config
	Host         host.Host
	PubSub       *pubsub.PubSub
	DHT          routing.Routing
	Datastore    datastore.Datastore
	IPFSClient   ipfs.IPFSClient
	TrustedPeers []peer.ID
	BadBits      *badbits.Filter
	OnPinSynced  func(cid string)
	OnPinRemoved func(cid string)
}

func NewClusterManager(cfg ClusterManagerConfig) *ClusterManager {
	return &ClusterManager{
		cfg:          cfg.Cfg,
		host:         cfg.Host,
		badBits:      cfg.BadBits,
		pubsub:       cfg.PubSub,
		dht:          cfg.DHT,
		datastore:    cfg.Datastore,
		ipfsClient:   cfg.IPFSClient,
		trustedPeers: cfg.TrustedPeers,
		onPinSynced:  cfg.OnPinSynced,
		onPinRemoved: cfg.OnPinRemoved,
		clusters:     make(map[string]*EmbeddedCluster),
	}
}

// SetShardPeerProvider sets the provider for CRDT Peers(). Set before using clusters.
func (cm *ClusterManager) SetShardPeerProvider(provider ShardPeerProvider) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.peerProvider = provider
}

// JoinShard initializes a new embedded cluster for the given shard.
// secret is the deterministically generated shared key for the cluster.
func (cm *ClusterManager) JoinShard(ctx context.Context, shardID string, bootstrapPeers []multiaddr.Multiaddr) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.clusters[shardID]; exists {
		return nil
	}

	// Namespace datastore for this shard
	shardDS := namespace.Wrap(cm.datastore, datastore.NewKey(shardID))

	// Configure CRDT
	trustAll := true
	if cm.cfg.TrustMode == "allowlist" {
		trustAll = false
	}

	cfg := &crdt.Config{
		ClusterName:         cm.cfg.PubsubTopicPrefix + "-shard-" + shardID,
		PeersetMetric:       "ping",
		RebroadcastInterval: 30 * time.Second,
		DatastoreNamespace:  datastore.NewKey("consensus").String(),
		TrustAll:            trustAll,
		TrustedPeers:        cm.trustedPeers,
		Batching: crdt.BatchingConfig{
			MaxBatchSize: 50,
			MaxBatchAge:  200 * time.Millisecond,
			MaxQueueSize: 100,
		},
	}

	// Initialize CRDT Consensus
	// PinTracker nil for now; state consensus only. storageMgr wired later or custom listener.
	consensus, err := crdt.New(cm.host, cm.dht, cm.pubsub, cfg, shardDS)
	if err != nil {
		return fmt.Errorf("failed to initialize CRDT for shard %s: %w", shardID, err)
	}
	// CRDT uses gorpc for PutHook/DeleteHook (PinTracker) and Peers() (PeerMonitor).
	// Set an embedded RPC client with stub handlers so it never uses a nil client.
	// getPeers reads cm.peerProvider at call time (not creation time) so clusters
	// created before SetShardPeerProvider still get real shard peers for allocations.
	getPeers := func(s string) []peer.ID {
		cm.mu.RLock()
		p := cm.peerProvider
		cm.mu.RUnlock()
		if p == nil {
			return nil
		}
		return p.GetPeersForShard(s)
	}
	// On Track/Untrack (CRDT PutHook/DeleteHook), trigger immediate PinTracker sync for this shard.
	onTrack := func(s string) { cm.TriggerSync(s) }
	setConsensusRPCClient(consensus, cm.host, shardID, getPeers, onTrack)

	// Subscribe to the signal topic before starting the tracker so we can
	// clean up everything on failure.
	topicName := cfg.ClusterName
	topic, err := cm.pubsub.Join(topicName)
	if err != nil {
		_ = consensus.Shutdown(context.Background())
		return fmt.Errorf("failed to join signal topic %s: %w", topicName, err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		_ = topic.Close()
		_ = consensus.Shutdown(context.Background())
		return fmt.Errorf("failed to subscribe to signal topic %s: %w", topicName, err)
	}

	subCtx, cancel := context.WithCancel(context.Background())

	tracker := NewLocalPinTracker(cm.ipfsClient, shardID, cm.onPinSynced, cm.onPinRemoved, cm.badBits)
	tracker.Start(consensus)

	go func() {
		defer sub.Cancel()
		defer topic.Close()
		for {
			select {
			case <-subCtx.Done():
				return
			default:
				_, err := sub.Next(subCtx)
				if err != nil {
					return
				}
				tracker.TriggerSync()
			}
		}
	}()

	cm.clusters[shardID] = &EmbeddedCluster{
		ShardID:    shardID,
		Consensus:  consensus,
		PinTracker: tracker,
		ctx:        subCtx,
		cancel:     cancel,
	}

	return nil
}

// LeaveShard gracefully shuts down the cluster for the given shard.
func (cm *ClusterManager) LeaveShard(shardID string) error {
	cm.mu.Lock()
	cluster, exists := cm.clusters[shardID]
	if !exists {
		cm.mu.Unlock()
		return nil
	}
	delete(cm.clusters, shardID)
	cm.mu.Unlock()

	slog.Info("shutting down embedded cluster", "shard", shardID)
	if cluster.PinTracker != nil {
		cluster.PinTracker.Stop()
	}
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	var shutdownErr error
	if err := cluster.Consensus.Shutdown(shutCtx); err != nil {
		shutdownErr = fmt.Errorf("consensus shutdown for shard %s: %w", shardID, err)
	}
	cluster.cancel()
	return shutdownErr
}

// Shutdown gracefully shuts down all embedded clusters.
func (cm *ClusterManager) Shutdown() error {
	cm.mu.Lock()
	shards := make([]string, 0, len(cm.clusters))
	for shardID := range cm.clusters {
		shards = append(shards, shardID)
	}
	cm.mu.Unlock()

	var firstErr error
	for _, shardID := range shards {
		if err := cm.LeaveShard(shardID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			slog.Error("failed to leave shard during shutdown", "shard", shardID, "error", err)
		}
	}
	return firstErr
}

// SelectAllocations deterministically chooses n peers from sorted list for the given CID (same CID → same set on all nodes).
// Exported for tests.
func SelectAllocations(peers []peer.ID, c cid.Cid, n int) []peer.ID {
	if n <= 0 || len(peers) == 0 {
		return nil
	}
	sorted := make([]peer.ID, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })
	if n >= len(sorted) {
		return sorted
	}
	// Hash CID to get a stable start index so the same CID gets the same replicas everywhere.
	h := sha256.Sum256(c.Bytes())
	start := (int(h[0])<<8 | int(h[1])) % len(sorted)
	out := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, sorted[(start+i)%len(sorted)])
	}
	return out
}

// Pin submits a pin to the shard's cluster. Use context with long timeout (e.g. CRDTOpTimeout).
func (cm *ClusterManager) Pin(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not a member of shard %s", shardID)
	}

	// Use config defaults when -1 (cluster default / "all").
	// Use 0,0 for full replication (no allocations computed - all nodes pin).
	repMin := replicationFactorMin
	repMax := replicationFactorMax
	if repMin < 0 {
		repMin = cm.cfg.MinReplication
	}
	if repMax < 0 {
		repMax = cm.cfg.MaxReplication
	}

	var allocations []peer.ID
	if repMin > 0 || repMax > 0 {
		peers, err := cluster.Consensus.Peers(ctx)
		if err != nil {
			slog.Warn("failed to get peers, using full replication", "shard", shardID, "error", err)
		}
		// Cap replication at shard size: a shard with 4 nodes can only replicate 4x.
		peerCount := len(peers)
		if peerCount > 0 {
			if repMax > peerCount {
				repMax = peerCount
			}
			if repMin > peerCount {
				repMin = peerCount
			}
		}
		allocations = SelectAllocations(peers, c, repMax)
		if len(allocations) == 0 && len(peers) > 0 {
			allocations = SelectAllocations(peers, c, repMin)
		}
	} else {
		// repMin=0 && repMax=0: full replication mode (used during migration).
		// Store config defaults as metadata but leave Allocations empty so all nodes pin.
		repMin = cm.cfg.MinReplication
		repMax = cm.cfg.MaxReplication
	}

	pin := api.Pin{
		Cid:         api.NewCid(c),
		Type:        api.DataType,
		Allocations: allocations,
		MaxDepth:    -1, // Recursive
	}
	pin.ReplicationFactorMin = repMin
	pin.ReplicationFactorMax = repMax

	if err := cluster.Consensus.LogPin(ctx, pin); err != nil {
		return fmt.Errorf("failed to log pin to CRDT: %w", err)
	}

	slog.Info("pinning to shard", "cid", c, "shard", shardID, "rep_min", repMin, "rep_max", repMax, "allocations", len(allocations))
	return nil
}

// PinIfAbsent writes a pin to the CRDT only if the CID is not already tracked.
// This prevents overwriting allocations computed by the ingesting node when
// multiple nodes handle the same IngestMessage or ReplicationRequest.
func (cm *ClusterManager) PinIfAbsent(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
	_, err := cm.GetAllocations(ctx, shardID, c)
	if err == nil {
		return nil // Pin already exists in CRDT — don't overwrite
	}
	return cm.Pin(ctx, shardID, c, replicationFactorMin, replicationFactorMax)
}

// Unpin submits an unpin operation to the specific shard's cluster.
func (cm *ClusterManager) Unpin(ctx context.Context, shardID string, c cid.Cid) error {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not a member of shard %s", shardID)
	}

	pin := api.Pin{
		Cid:  api.NewCid(c),
		Type: api.DataType,
	}

	if err := cluster.Consensus.LogUnpin(ctx, pin); err != nil {
		return fmt.Errorf("failed to log unpin to CRDT: %w", err)
	}

	slog.Info("unpinning from shard", "cid", c, "shard", shardID)
	return nil
}

// GetAllocations returns the list of peers allocated for a CID in the shard.
func (cm *ClusterManager) GetAllocations(ctx context.Context, shardID string, c cid.Cid) ([]peer.ID, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("not a member of shard %s", shardID)
	}

	st, err := cluster.Consensus.State(ctx)
	if err != nil {
		return nil, err
	}

	// List streams pins to a channel. Use a cancellable context so the List
	// goroutine exits promptly when we find our CID and stop reading.
	listCtx, listCancel := context.WithCancel(ctx)
	defer listCancel()

	out := make(chan api.Pin)
	go func() {
		_ = st.List(listCtx, out)
	}()

	for pin := range out {
		if pin.Cid.Equals(api.NewCid(c)) {
			return pin.Allocations, nil // listCancel fires via defer, unblocking the List goroutine
		}
	}
	return nil, fmt.Errorf("pin not found in state")
}

// ListPins returns all pins in the shard's consensus state (CRDT).
// Useful for migration, replication checks, and API/monitor.
func (cm *ClusterManager) ListPins(ctx context.Context, shardID string) ([]api.Pin, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("not a member of shard %s", shardID)
	}

	st, err := cluster.Consensus.State(ctx)
	if err != nil {
		return nil, err
	}

	// state.List closes out when done; do not close it here (double-close causes panic).
	out := make(chan api.Pin)
	go func() {
		_ = st.List(ctx, out)
	}()

	var pins []api.Pin
	for pin := range out {
		pins = append(pins, pin)
	}
	return pins, nil
}

// GetPeerCount returns the number of peers in the shard's consensus cluster.
func (cm *ClusterManager) GetPeerCount(ctx context.Context, shardID string) (int, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("not a member of shard %s", shardID)
	}

	peers, err := cluster.Consensus.Peers(ctx)
	if err != nil {
		return 0, err
	}
	return len(peers), nil
}

// TriggerSync syncs PinTracker for the shard.
func (cm *ClusterManager) TriggerSync(shardID string) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()
	if !exists || cluster.PinTracker == nil {
		return
	}
	cluster.PinTracker.TriggerSync()
}

// GetClusterMetrics returns cluster-style metrics per shard for telemetry.
// Implements telemetry.ClusterInfoProvider.
func (cm *ClusterManager) GetClusterMetrics(ctx context.Context) (pinsPerShard, peersPerShard, allocationsTotalPerShard map[string]int, err error) {
	cm.mu.RLock()
	shardIDs := make([]string, 0, len(cm.clusters))
	for id := range cm.clusters {
		shardIDs = append(shardIDs, id)
	}
	cm.mu.RUnlock()

	pinsPerShard = make(map[string]int)
	peersPerShard = make(map[string]int)
	allocationsTotalPerShard = make(map[string]int)

	for _, shardID := range shardIDs {
		pins, err := cm.ListPins(ctx, shardID)
		if err != nil {
			return nil, nil, nil, err
		}
		pinsPerShard[shardID] = len(pins)
		allocTotal := 0
		for _, pin := range pins {
			allocTotal += len(pin.Allocations)
		}
		allocationsTotalPerShard[shardID] = allocTotal

		peerCount, err := cm.GetPeerCount(ctx, shardID)
		if err != nil {
			return nil, nil, nil, err
		}
		peersPerShard[shardID] = peerCount
	}
	return pinsPerShard, peersPerShard, allocationsTotalPerShard, nil
}
