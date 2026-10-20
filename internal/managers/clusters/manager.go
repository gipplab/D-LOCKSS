// Package clusters runs one embedded ipfs-cluster CRDT consensus per shard,
// all sharing the node's single IPFS daemon.
package clusters

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/internal/trust"
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

// ClusterManagerInterface is the part of ClusterManager used by other
// components, so that it can be mocked.
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

// ShardPeerProvider supplies the peers of a shard (e.g. its pubsub mesh). The
// CRDT uses them for Peers() and pin allocations.
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
	origin       trust.OriginGate
	onPinSynced  func(cid string)
	onPinRemoved func(cid string)
	peerProvider ShardPeerProvider

	mu       sync.RWMutex
	clusters map[string]*embeddedCluster
	// signalTopics keeps one pubsub handle per shard signal topic for the
	// process lifetime. pubsub refuses a second Join of a topic that is still
	// open, and closing races with subscription teardown, so rejoining a shard
	// after a merge reuses the handle instead.
	signalTopics map[string]*pubsub.Topic
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
	Origin       trust.OriginGate
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
		origin:       cfg.Origin,
		onPinSynced:  cfg.OnPinSynced,
		onPinRemoved: cfg.OnPinRemoved,
		clusters:     make(map[string]*embeddedCluster),
		signalTopics: make(map[string]*pubsub.Topic),
	}
}

// SetShardPeerProvider sets the provider behind CRDT Peers(). Clusters joined
// earlier pick it up on their next lookup.
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

	shardDS := namespace.Wrap(cm.datastore, datastore.NewKey(shardID))

	// An empty trusted-peer list means open mode, even under "allowlist".
	trustAll := cm.cfg.Security.TrustMode != "allowlist" || len(cm.trustedPeers) == 0

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

	consensus, err := crdt.New(cm.host, cm.dht, cm.pubsub, cfg, shardDS)
	if err != nil {
		return fmt.Errorf("failed to initialize CRDT for shard %s: %w", shardID, err)
	}
	// The CRDT calls PinTracker and PeerMonitor over gorpc and must never see
	// a nil client. getPeers reads cm.peerProvider per call, so clusters
	// joined before SetShardPeerProvider still allocate across real shard peers.
	getPeers := func(s string) []peer.ID {
		cm.mu.RLock()
		p := cm.peerProvider
		cm.mu.RUnlock()
		if p == nil {
			return nil
		}
		return p.GetPeersForShard(s)
	}
	consensus.SetClient(newEmbeddedRPCClient(cm.host, shardID, getPeers, cm.TriggerSync))

	// Subscribe before starting the tracker so that a failure only has to
	// shut down consensus.
	topicName := cfg.ClusterName
	topic, joined := cm.signalTopics[topicName]
	if !joined {
		topic, err = cm.pubsub.Join(topicName)
		if err != nil {
			_ = consensus.Shutdown(context.Background())
			return fmt.Errorf("failed to join signal topic %s: %w", topicName, err)
		}
		cm.signalTopics[topicName] = topic
	}
	sub, err := topic.Subscribe()
	if err != nil {
		_ = consensus.Shutdown(context.Background())
		return fmt.Errorf("failed to subscribe to signal topic %s: %w", topicName, err)
	}

	subCtx, cancel := context.WithCancel(context.Background())

	tracker := newLocalPinTracker(cm.ipfsClient, shardID, cm.onPinSynced, cm.onPinRemoved, cm.badBits, cm.origin)
	tracker.Start(consensus)

	go func() {
		defer sub.Cancel()
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

func (cm *ClusterManager) shardCluster(shardID string) (*embeddedCluster, error) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("not a member of shard %s", shardID)
	}
	return cluster, nil
}

// SelectAllocations deterministically chooses n of peers for c, so every node
// computes the same replica set for the same CID. When n covers all peers,
// they are returned sorted.
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
	cluster, err := cm.shardCluster(shardID)
	if err != nil {
		return err
	}

	// A negative factor selects the configured default. 0,0 requests full
	// replication: no allocations are computed and every shard member pins.
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
		if n := len(peers); n > 0 {
			repMax = min(repMax, n)
			repMin = min(repMin, n)
		}
		allocations = SelectAllocations(peers, c, repMax)
		if len(allocations) == 0 && len(peers) > 0 {
			allocations = SelectAllocations(peers, c, repMin)
		}
	} else {
		// Full replication (used during migration): record the configured
		// factors as metadata but leave Allocations empty so all nodes pin.
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
	if _, err := cm.GetAllocations(ctx, shardID, c); err == nil {
		return nil
	}
	return cm.Pin(ctx, shardID, c, replicationFactorMin, replicationFactorMax)
}

// Unpin submits an unpin operation to the shard's cluster.
func (cm *ClusterManager) Unpin(ctx context.Context, shardID string, c cid.Cid) error {
	cluster, err := cm.shardCluster(shardID)
	if err != nil {
		return err
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
	cluster, err := cm.shardCluster(shardID)
	if err != nil {
		return nil, err
	}
	st, err := cluster.consensus.State(ctx)
	if err != nil {
		return nil, fmt.Errorf("consensus state for shard %s: %w", shardID, err)
	}

	// Cancelling listCtx on return unblocks the List goroutine after an early match.
	listCtx, listCancel := context.WithCancel(ctx)
	defer listCancel()

	out := make(chan api.Pin)
	go func() {
		_ = st.List(listCtx, out)
	}()

	want := api.NewCid(c)
	for pin := range out {
		if pin.Cid.Equals(want) {
			return pin.Allocations, nil
		}
	}
	return nil, errors.New("pin not found in state")
}

// ListPins returns all pins in the shard's consensus state (CRDT).
func (cm *ClusterManager) ListPins(ctx context.Context, shardID string) ([]api.Pin, error) {
	cluster, err := cm.shardCluster(shardID)
	if err != nil {
		return nil, err
	}
	st, err := cluster.consensus.State(ctx)
	if err != nil {
		return nil, fmt.Errorf("consensus state for shard %s: %w", shardID, err)
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

// GetPeerCount returns the number of peers in the shard's consensus peerset.
func (cm *ClusterManager) GetPeerCount(ctx context.Context, shardID string) (int, error) {
	cluster, err := cm.shardCluster(shardID)
	if err != nil {
		return 0, err
	}
	peers, err := cluster.consensus.Peers(ctx)
	if err != nil {
		return 0, fmt.Errorf("consensus peers for shard %s: %w", shardID, err)
	}
	return len(peers), nil
}

// TriggerSync requests an immediate pin-tracker sync for the shard.
func (cm *ClusterManager) TriggerSync(shardID string) {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()
	if !exists || cluster.pinTracker == nil {
		return
	}
	cluster.pinTracker.TriggerSync()
}
