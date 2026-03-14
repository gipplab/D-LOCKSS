package shard

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/clusters"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// MessageAuthenticator abstracts protocol message signing and verification.
type MessageAuthenticator interface {
	SignProtocolMessage(msg schema.Signable) error
	ShouldDropMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool
}

// StorageReader provides read-only queries over the local pin/file state.
type StorageReader interface {
	CanAcceptCustodialFile() bool
	IsPinned(key string) bool
	GetAllKnownFiles() map[string]bool
	GetPinTime(key string) time.Time
	GetPinnedManifests() []string
	GetPinnedCount() int
	GetNextFileToAnnounce() string
}

// StorageWriter provides mutating operations on the local pin/file state.
type StorageWriter interface {
	PinFile(manifestCIDStr string) bool
	UnpinFile(key string)
	AddKnownFile(key string)
	ProvideFile(ctx context.Context, key string)
}

// StorageProvider composes all storage operations needed by the shard manager.
type StorageProvider interface {
	StorageReader
	StorageWriter
}

const migratePinsFlushDelay = 250 * time.Millisecond
const rootPeerCheckInterval = 30 * time.Second
const rootReplicationCheckInterval = 20 * time.Second
const replicationRequestCooldownDuration = 5 * time.Minute
const maxReplicationRequestsPerCycle = 50

const (
	msgPrefixHeartbeat = "HEARTBEAT:"
	msgPrefixPinned    = "PINNED:"
	msgPrefixJoin      = "JOIN:"
	msgPrefixLeave     = "LEAVE:"
	msgPrefixProbe     = "PROBE:"
	msgPrefixSplit     = "SPLIT:"
)

func (sm *ShardManager) shardTopicName(shardID string) string {
	return sm.cfg.PubsubTopicPrefix + "-" + sm.cfg.TopicName + "-shard-" + shardID
}

func childShards(parent string) (child0, child1 string) {
	if parent == "" {
		return "0", "1"
	}
	return parent + "0", parent + "1"
}

type shardSubscription struct {
	topic        *pubsub.Topic
	sub          *pubsub.Subscription
	refCount     int
	cancel       context.CancelFunc
	shardID      string
	observerOnly bool
}

type ShardManager struct {
	// Dependencies
	ctx         context.Context
	cfg         *config.Config
	h           host.Host
	ps          *pubsub.PubSub
	ipfsClient  ipfs.IPFSClient
	storageMgr  StorageProvider
	clusterMgr  clusters.ClusterManagerInterface
	signer      MessageAuthenticator
	rateLimiter *common.RateLimiter
	nodeName    string

	// Ingest authorization
	ingestAllowlist map[peer.ID]struct{}

	// Peer tracking
	peers *PeerTracker

	// Shard membership (protected by mu)
	mu           sync.RWMutex
	currentShard string

	// PubSub topic management (protected by mu)
	shardSubs          map[string]*shardSubscription
	probeTopicCache    map[string]*pubsub.Topic
	observerOnlyShards map[string]struct{}

	// Shard transition timestamps (protected by mu)
	lastMoveToDeeperShard time.Time
	lastMergeUpTime       time.Time
	lastShardMove         time.Time // set on ANY shard transition (split, merge, discovery)

	// Message handling (protected by mu)
	msgCounter            int
	lastMessageTime       time.Time
	lastProbeResponseTime time.Time

	// Replication state
	reshardedFiles             *common.KnownFiles
	orphanHandoffSent          map[string]map[string]*orphanHandoffInfo
	replicationRequestMu       sync.Mutex
	replicationRequestLastSent map[string]time.Time
	autoReplicationSem         chan struct{}
	reprovideInFlight          atomic.Bool

	// Lifecycle: split/merge/discovery (delegated)
	lifecycle *lifecycleManager
}

type orphanHandoffInfo struct {
	lastSent time.Time
	count    int
}

// ShardManagerConfig holds all dependencies for a ShardManager.
type ShardManagerConfig struct {
	Cfg         *config.Config
	Ctx         context.Context
	Host        host.Host
	PubSub      *pubsub.PubSub
	IPFSClient  ipfs.IPFSClient
	Storage     StorageProvider
	Signer      MessageAuthenticator
	RateLimiter *common.RateLimiter
	Cluster     clusters.ClusterManagerInterface
	StartShard  string
	NodeName    string
}

func NewShardManager(cfg ShardManagerConfig) (*ShardManager, error) {
	allowlist := make(map[peer.ID]struct{}, len(cfg.Cfg.IngestAllowlist))
	for _, raw := range cfg.Cfg.IngestAllowlist {
		pid, err := peer.Decode(raw)
		if err != nil {
			slog.Warn("ignoring invalid peer ID in ingest allowlist", "peer", raw, "error", err)
			continue
		}
		allowlist[pid] = struct{}{}
	}
	sm := &ShardManager{
		ctx:                        cfg.Ctx,
		cfg:                        cfg.Cfg,
		h:                          cfg.Host,
		ps:                         cfg.PubSub,
		ipfsClient:                 cfg.IPFSClient,
		storageMgr:                 cfg.Storage,
		clusterMgr:                 cfg.Cluster,
		signer:                     cfg.Signer,
		rateLimiter:                cfg.RateLimiter,
		nodeName:                   cfg.NodeName,
		ingestAllowlist:            allowlist,
		peers:                      NewPeerTracker(cfg.Host.ID()),
		reshardedFiles:             common.NewKnownFiles(),
		currentShard:               cfg.StartShard,
		shardSubs:                  make(map[string]*shardSubscription),
		probeTopicCache:            make(map[string]*pubsub.Topic),
		observerOnlyShards:         make(map[string]struct{}),
		orphanHandoffSent:          make(map[string]map[string]*orphanHandoffInfo),
		replicationRequestLastSent: make(map[string]time.Time),
		autoReplicationSem:         make(chan struct{}, cfg.Cfg.MaxConcurrentReplicationChecks),
	}
	sm.lifecycle = newLifecycleManager(func() context.Context { return sm.ctx }, cfg.Cfg, sm)

	if err := sm.clusterMgr.JoinShard(cfg.Ctx, cfg.StartShard, nil); err != nil {
		return nil, fmt.Errorf("join cluster for start shard %s: %w", cfg.StartShard, err)
	}

	if err := sm.JoinShard(cfg.StartShard); err != nil {
		return nil, fmt.Errorf("join shard topic %s: %w", cfg.StartShard, err)
	}
	if err := sm.loadReshardedFiles(); err != nil {
		slog.Warn("failed to load resharded files, starting fresh", "error", err)
	}

	return sm, nil
}

func (sm *ShardManager) Run() {
	go sm.lifecycle.runPeerCountChecker()
	go sm.lifecycle.runShardDiscovery()
	go sm.lifecycle.runSplitRebroadcast()
	go sm.runHeartbeat()
	go sm.runOrphanUnpinLoop()
	go sm.runReplicationChecker()
	go sm.runReannouncePinsLoop()
	go sm.runReshardedFilesSaveLoop()
	go sm.runLegacyManifestCleanup()
}

func (sm *ShardManager) Close() error {
	var firstErr error
	if err := sm.saveReshardedFiles(); err != nil {
		firstErr = err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, sub := range sm.shardSubs {
		sub.cancel()
		sub.sub.Cancel()
		if sub.topic != nil {
			_ = sub.topic.Close()
		}
	}
	sm.shardSubs = make(map[string]*shardSubscription)

	for _, t := range sm.probeTopicCache {
		_ = t.Close()
	}
	sm.probeTopicCache = make(map[string]*pubsub.Topic)
	return firstErr
}

// --- lifecycleOps implementation ---

func (sm *ShardManager) getCurrentShard() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentShard
}

func (sm *ShardManager) getLastShardMove() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastShardMove
}

func (sm *ShardManager) getLastMergeUpTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastMergeUpTime
}

func (sm *ShardManager) getLastMoveToDeeperShard() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastMoveToDeeperShard
}

func (sm *ShardManager) getLastMessageTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastMessageTime
}

func (sm *ShardManager) localPeerID() peer.ID {
	return sm.h.ID()
}

func (sm *ShardManager) incrementShardSplits() {}

func (sm *ShardManager) pruneStaleSeenPeers() {
	sm.peers.PruneStale(sm.cfg.PruneStalePeersInterval)
}

// moveToShard switches shard: join new, migrate pins, leave old. Used by split, discovery, merge.
func (sm *ShardManager) moveToShard(fromShard, toShard string, isMergeUp bool) {
	sm.mu.Lock()
	if sm.currentShard != fromShard {
		sm.mu.Unlock()
		return
	}
	sm.currentShard = toShard
	sm.msgCounter = 0
	sm.reshardedFiles = common.NewKnownFiles()
	sm.lastShardMove = time.Now()
	if isMergeUp {
		sm.lastMergeUpTime = sm.lastShardMove
	} else {
		sm.lastMoveToDeeperShard = sm.lastShardMove
	}
	sm.mu.Unlock()
	sm.lifecycle.onShardTransition()

	// Immediately announce departure from the old shard so other peers stop
	// counting us as ACTIVE.  The actual topic unsubscription happens later
	// (after ShardOverlapDuration) to allow continued message reception for
	// data migration, but other nodes need to drop us from their peer counts
	// now — otherwise stale entries inflate getShardPeerCountForSplit() and
	// can trigger premature splits.
	sm.mu.RLock()
	fromSub, fromSubExists := sm.shardSubs[fromShard]
	sm.mu.RUnlock()
	if fromSubExists && fromSub.topic != nil && !fromSub.observerOnly {
		leaveMsg := []byte(msgPrefixLeave + sm.h.ID().String())
		_ = fromSub.topic.Publish(sm.ctx, leaveMsg)
	}

	if err := sm.JoinShard(toShard); err != nil {
		slog.Error("failed to join shard topic", "shard", toShard, "error", err)
	}
	if err := sm.clusterMgr.JoinShard(sm.ctx, toShard, nil); err != nil {
		slog.Error("failed to join cluster for shard", "shard", toShard, "error", err)
	}
	go func() {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(migratePinsFlushDelay):
		}
		sm.mu.RLock()
		current := sm.currentShard
		sm.mu.RUnlock()
		if current != toShard {
			if strings.HasPrefix(current, toShard) {
				slog.Info("migration redirect", "from", fromShard, "to", current, "intermediate", toShard)
				if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, current); err != nil {
					slog.Error("migration failed", "from", fromShard, "to", current, "error", err)
				}
			}
			return
		}
		if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, toShard); err != nil {
			slog.Error("migration failed", "from", fromShard, "to", toShard, "error", err)
		}
	}()
	go func() {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(sm.cfg.ShardOverlapDuration):
		}
		sm.mu.RLock()
		current := sm.currentShard
		sm.mu.RUnlock()
		if current == fromShard {
			return // we moved back to fromShard, don't leave it
		}
		sm.LeaveShard(fromShard)
		if err := sm.clusterMgr.LeaveShard(fromShard); err != nil {
			slog.Error("failed to leave cluster", "shard", fromShard, "error", err)
		}
	}()
	go func() {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(sm.cfg.ReshardDelay):
		}
		sm.mu.RLock()
		current := sm.currentShard
		sm.mu.RUnlock()
		if current != toShard {
			return // another transition happened, skip stale reshard
		}
		sm.RunReshardPass(fromShard, toShard)
	}()
}
