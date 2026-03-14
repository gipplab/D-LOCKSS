package shard

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
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
	peers *peerTracker

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
	lastMessageTime       time.Time
	lastProbeResponseTime time.Time

	// Replication state
	reshardedFiles    *common.KnownFiles
	orphanHandoffSent map[string]map[string]*orphanHandoffInfo
	reprovideInFlight atomic.Bool

	// Replication: request sending and handling (delegated)
	repl *replicationManager

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
		ctx:                cfg.Ctx,
		cfg:                cfg.Cfg,
		h:                  cfg.Host,
		ps:                 cfg.PubSub,
		ipfsClient:         cfg.IPFSClient,
		storageMgr:         cfg.Storage,
		clusterMgr:         cfg.Cluster,
		signer:             cfg.Signer,
		rateLimiter:        cfg.RateLimiter,
		nodeName:           cfg.NodeName,
		ingestAllowlist:    allowlist,
		peers:              newPeerTracker(cfg.Host.ID()),
		reshardedFiles:     common.NewKnownFiles(),
		currentShard:       cfg.StartShard,
		shardSubs:          make(map[string]*shardSubscription),
		probeTopicCache:    make(map[string]*pubsub.Topic),
		observerOnlyShards: make(map[string]struct{}),
		orphanHandoffSent:  make(map[string]map[string]*orphanHandoffInfo),
	}
	sm.repl = newReplicationManager(sm, cfg.Cfg.Replication.MaxConcurrentReplicationChecks)
	sm.lifecycle = newLifecycleManager(func() context.Context { return sm.ctx }, cfg.Cfg, sm)

	if err := sm.clusterMgr.JoinShard(cfg.Ctx, cfg.StartShard); err != nil {
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
	go sm.repl.runChecker()
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

func (sm *ShardManager) pruneStaleSeenPeers() {
	sm.peers.PruneStale(sm.cfg.Sharding.PruneStalePeersInterval)
}

// --- replicationOps implementation ---

func (sm *ShardManager) replicationContext() context.Context { return sm.ctx }
func (sm *ShardManager) replicationConfig() *config.Config   { return sm.cfg }
func (sm *ShardManager) getPinnedManifests() []string        { return sm.storageMgr.GetPinnedManifests() }
func (sm *ShardManager) isPinned(key string) bool            { return sm.storageMgr.IsPinned(key) }
func (sm *ShardManager) publishCBOR(data []byte, shardID string) {
	sm.PublishToShardCBOR(data, shardID)
}
func (sm *ShardManager) replicationSigner() MessageAuthenticator { return sm.signer }
func (sm *ShardManager) clusterTriggerSync(shardID string)       { sm.clusterMgr.TriggerSync(shardID) }
func (sm *ShardManager) ipfsPinRecursive(ctx context.Context, c cid.Cid) error {
	return sm.ipfsClient.PinRecursive(ctx, c)
}
func (sm *ShardManager) ensureCluster(ctx context.Context, shardID string) error {
	return sm.EnsureClusterForShard(ctx, shardID)
}
func (sm *ShardManager) clusterPinIfAbsent(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.PinIfAbsent(ctx, shardID, c, -1, -1)
}
func (sm *ShardManager) isLegacyManifest(cidStr string) bool {
	ctx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	defer cancel()
	return common.IsLegacyManifest(ctx, sm.ipfsClient, cidStr)
}

func (sm *ShardManager) moveToShard(fromShard, toShard string, isMergeUp bool) {
	sm.mu.Lock()
	if sm.currentShard != fromShard {
		sm.mu.Unlock()
		return
	}
	sm.currentShard = toShard
	sm.reshardedFiles = common.NewKnownFiles()
	sm.lastShardMove = time.Now()
	if isMergeUp {
		sm.lastMergeUpTime = sm.lastShardMove
	} else {
		sm.lastMoveToDeeperShard = sm.lastShardMove
	}
	sm.mu.Unlock()
	sm.lifecycle.onShardTransition()

	sm.publishLeaveFromShard(fromShard)

	if err := sm.JoinShard(toShard); err != nil {
		slog.Error("failed to join shard topic", "shard", toShard, "error", err)
	}
	if err := sm.clusterMgr.JoinShard(sm.ctx, toShard); err != nil {
		slog.Error("failed to join cluster for shard", "shard", toShard, "error", err)
	}

	go sm.schedulePinMigration(fromShard, toShard)
	go sm.scheduleDelayedLeave(fromShard, toShard)
	go sm.scheduleReshardPass(fromShard, toShard)
}

// publishLeaveFromShard announces departure immediately so peers drop us from
// their active counts, even though the topic stays open for ShardOverlapDuration.
func (sm *ShardManager) publishLeaveFromShard(fromShard string) {
	sm.mu.RLock()
	fromSub, exists := sm.shardSubs[fromShard]
	sm.mu.RUnlock()
	if exists && fromSub.topic != nil && !fromSub.observerOnly {
		leaveMsg := []byte(msgPrefixLeave + sm.h.ID().String())
		_ = fromSub.topic.Publish(sm.ctx, leaveMsg)
	}
}

func (sm *ShardManager) schedulePinMigration(fromShard, toShard string) {
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
}

func (sm *ShardManager) scheduleDelayedLeave(fromShard, toShard string) {
	select {
	case <-sm.ctx.Done():
		return
	case <-time.After(sm.cfg.Sharding.ShardOverlapDuration):
	}
	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current == fromShard {
		return
	}
	sm.LeaveShard(fromShard)
	if err := sm.clusterMgr.LeaveShard(fromShard); err != nil {
		slog.Error("failed to leave cluster", "shard", fromShard, "error", err)
	}
}

func (sm *ShardManager) scheduleReshardPass(fromShard, toShard string) {
	select {
	case <-sm.ctx.Done():
		return
	case <-time.After(sm.cfg.Files.ReshardDelay):
	}
	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != toShard {
		return
	}
	sm.RunReshardPass(fromShard, toShard)
}
