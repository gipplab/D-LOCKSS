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
)

// ClusterManagerInterface defines the interface for ClusterManager to allow mocking.
type ClusterManagerInterface interface {
	JoinShard(ctx context.Context, shardID string) error
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
	clusters map[string]*embeddedCluster
}

type consensusClient interface {
	LogPin(ctx context.Context, pin api.Pin) error
	LogUnpin(ctx context.Context, pin api.Pin) error
	State(ctx context.Context) (state.ReadOnly, error)
	Peers(ctx context.Context) ([]peer.ID, error)
	Shutdown(ctx context.Context) error
}

type embeddedCluster struct {
	consensus  consensusClient
	pinTracker *localPinTracker
	cancel     context.CancelFunc
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
		clusters:     make(map[string]*embeddedCluster),
	}
}

// SetShardPeerProvider sets the provider for CRDT Peers(). Set before using clusters.
func (cm *ClusterManager) SetShardPeerProvider(provider ShardPeerProvider) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.peerProvider = provider
}

// JoinShard initializes a new embedded cluster for the given shard.
func (cm *ClusterManager) JoinShard(ctx context.Context, shardID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.clusters[shardID]; exists {
		return nil
	}

	// Namespace datastore for this shard
	shardDS := namespace.Wrap(cm.datastore, datastore.NewKey(shardID))

	// Configure CRDT
	trustAll := true
	if cm.cfg.Security.TrustMode == "allowlist" {
		trustAll = false
	}

	cfg := &crdt.Config{
		ClusterName:         cm.cfg.PubsubTopicPrefix + "-" + cm.cfg.TopicName + "-crdt-" + shardID,
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

	tracker := newLocalPinTracker(cm.ipfsClient, shardID, cm.onPinSynced, cm.onPinRemoved, cm.badBits)
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

	cm.clusters[shardID] = &embeddedCluster{
		consensus:  consensus,
		pinTracker: tracker,
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
	if cluster.pinTracker != nil {
		cluster.pinTracker.Stop()
	}
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	var shutdownErr error
	if err := cluster.consensus.Shutdown(shutCtx); err != nil {
		shutdownErr = fmt.Errorf("consensus shutdown for shard %s: %w", shardID, err)
	}
	cluster.cancel()
	return shutdownErr
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

// Pin submits a pin to the shard's cluster.
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
		repMin = cm.cfg.Replication.MinReplication
	}
	if repMax < 0 {
		repMax = cm.cfg.Replication.MaxReplication
	}

	var allocations []peer.ID
	if repMin > 0 || repMax > 0 {
		peers, err := cluster.consensus.Peers(ctx)
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
		repMin = cm.cfg.Replication.MinReplication
		repMax = cm.cfg.Replication.MaxReplication
	}

	pin := api.Pin{
		Cid:         api.NewCid(c),
		Type:        api.DataType,
		Allocations: allocations,
		MaxDepth:    -1, // Recursive
	}
	pin.ReplicationFactorMin = repMin
	pin.ReplicationFactorMax = repMax

	if err := cluster.consensus.LogPin(ctx, pin); err != nil {
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

	if err := cluster.consensus.LogUnpin(ctx, pin); err != nil {
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

	st, err := cluster.consensus.State(ctx)
	if err != nil {
		return nil, err
	}

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
func (cm *ClusterManager) ListPins(ctx context.Context, shardID string) ([]api.Pin, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("not a member of shard %s", shardID)
	}

	st, err := cluster.consensus.State(ctx)
	if err != nil {
		return nil, err
	}

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

func (cm *ClusterManager) GetPeerCount(ctx context.Context, shardID string) (int, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("not a member of shard %s", shardID)
	}

	peers, err := cluster.consensus.Peers(ctx)
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
	if !exists || cluster.pinTracker == nil {
		return
	}
	cluster.pinTracker.TriggerSync()
}
