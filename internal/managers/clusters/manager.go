package clusters

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

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

// ClusterManager manages multiple embedded IPFS Cluster instances (Consensus/PinTracker)
// sharing the same underlying IPFS node.
type ClusterManager struct {
	host       host.Host
	ipfsClient IPFSClient // Used for PinTracker
	pubsub     *pubsub.PubSub
	dht        routing.Routing
	datastore  datastore.Datastore

	mu       sync.RWMutex
	clusters map[string]*EmbeddedCluster
}

// ConsensusClient defines the interface for interacting with the consensus component.
type ConsensusClient interface {
	LogPin(ctx context.Context, pin api.Pin) error
	LogUnpin(ctx context.Context, pin api.Pin) error
	State(ctx context.Context) (state.ReadOnly, error)
	Peers(ctx context.Context) ([]peer.ID, error)
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

func NewClusterManager(h host.Host, ps *pubsub.PubSub, dht routing.Routing, ds datastore.Datastore, ipfsClient IPFSClient) *ClusterManager {
	return &ClusterManager{
		host:       h,
		pubsub:     ps,
		dht:        dht,
		datastore:  ds,
		ipfsClient: ipfsClient,
		clusters:   make(map[string]*EmbeddedCluster),
	}
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
	cfg := &crdt.Config{
		ClusterName:         "dlockss-shard-" + shardID,
		PeersetMetric:       "ping",
		RebroadcastInterval: 5 * time.Minute,
		DatastoreNamespace:  datastore.NewKey("consensus").String(),
		TrustAll:            true,
		Batching: crdt.BatchingConfig{
			MaxBatchSize: 50,
			MaxBatchAge:  500 * time.Millisecond,
			MaxQueueSize: 100,
		},
	}

	// Initialize CRDT Consensus
	// Note: We pass nil for PinTracker for now as we just want state consensus first.
	// We will wire up storageMgr later or use a custom listener.
	consensus, err := crdt.New(cm.host, cm.dht, cm.pubsub, cfg, shardDS)
	if err != nil {
		return fmt.Errorf("failed to initialize CRDT for shard %s: %w", shardID, err)
	}

	subCtx, cancel := context.WithCancel(context.Background())

	// Start PinTracker
	tracker := NewLocalPinTracker(cm.ipfsClient, shardID)
	tracker.Start(consensus)

	// Start Signal Listener (Event-Driven Updates)
	// We subscribe to the same topic that CRDT uses to detect activity.
	// When we see a message, we trigger the tracker to sync immediately.
	topicName := cfg.ClusterName
	topic, err := cm.pubsub.Join(topicName)
	if err == nil {
		sub, err := topic.Subscribe()
		if err == nil {
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
						// Trigger sync on any message
						tracker.TriggerSync()
					}
				}
			}()
		} else {
			log.Printf("[Cluster] Warning: Failed to subscribe to signal topic %s: %v", topicName, err)
		}
	} else {
		log.Printf("[Cluster] Warning: Failed to join signal topic %s: %v", topicName, err)
	}

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
	defer cm.mu.Unlock()

	cluster, exists := cm.clusters[shardID]
	if !exists {
		return nil
	}

	log.Printf("[Cluster] Shutting down embedded cluster for shard %s...", shardID)
	if cluster.PinTracker != nil {
		cluster.PinTracker.Stop()
	}
	cluster.cancel()
	// Wait for shutdown...
	delete(cm.clusters, shardID)
	return nil
}

// Pin submits a pin operation to the specific shard's cluster.
func (cm *ClusterManager) Pin(ctx context.Context, shardID string, c cid.Cid, replicationFactorMin, replicationFactorMax int) error {
	cm.mu.RLock()
	cluster, exists := cm.clusters[shardID]
	cm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not a member of shard %s", shardID)
	}

	pin := api.Pin{
		Cid:         api.NewCid(c),
		Type:        api.DataType,
		Allocations: []peer.ID{}, // CRDT will decide
		MaxDepth:    -1,          // Recursive
	}

	if err := cluster.Consensus.LogPin(ctx, pin); err != nil {
		return fmt.Errorf("failed to log pin to CRDT: %w", err)
	}

	log.Printf("[Cluster] Pinning %s to shard %s (Rep: %d-%d)", c, shardID, replicationFactorMin, replicationFactorMax)
	return nil
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

	log.Printf("[Cluster] Unpinning %s from shard %s", c, shardID)
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

	// List streams pins to a channel
	out := make(chan api.Pin)
	go func() {
		defer close(out)
		_ = st.List(ctx, out)
	}()

	for pin := range out {
		if pin.Cid.Equals(api.NewCid(c)) {
			return pin.Allocations, nil
		}
	}
	return nil, fmt.Errorf("pin not found in state")
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
