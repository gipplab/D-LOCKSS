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
	"dlockss/internal/trust"
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
	ProviderCount(ctx context.Context, key string, self peer.ID, limit int) int
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

const (
	migratePinsFlushDelay              = 250 * time.Millisecond
	rootPeerCheckInterval              = 30 * time.Second
	rootReplicationCheckInterval       = 20 * time.Second
	replicationRequestCooldownDuration = 5 * time.Minute
	maxReplicationRequestsPerCycle     = 50
	maxProbeTopicCache                 = 4
)

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

func (s *shardSubscription) close() {
	s.cancel()
	s.sub.Cancel()
	if s.topic != nil {
		_ = s.topic.Close()
	}
}

type ShardManager struct {
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

	// origin is the data-origin gate (trusted_peers.json / ingest allowlist).
	origin trust.OriginGate

	peers *peerTracker

	// mu guards the fields in this block.
	mu                    sync.RWMutex
	currentShard          string
	shardSubs             map[string]*shardSubscription
	probeTopicCache       map[string]*pubsub.Topic
	observerOnlyShards    map[string]struct{}
	lastMoveToDeeperShard time.Time
	lastMergeUpTime       time.Time
	lastShardMove         time.Time // set on any shard transition (split, merge, discovery)
	lastMessageTime       time.Time
	lastProbeResponseTime time.Time

	reshardedFiles    *common.KnownFiles
	orphanHandoffSent map[string]map[string]*orphanHandoffInfo
	reprovideInFlight atomic.Bool
	// custodialChecked is the time of each pinned file's last custodial
	// provider lookup. Only RunCustodialRetryPass uses it.
	custodialChecked map[string]time.Time

	repl      *replicationManager
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
	Origin      trust.OriginGate
	StartShard  string
	NodeName    string
}

func NewShardManager(cfg ShardManagerConfig) (*ShardManager, error) {
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
		origin:             cfg.Origin,
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
	go sm.runCustodialRetryLoop()
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
		sub.close()
	}
	sm.shardSubs = make(map[string]*shardSubscription)

	for _, t := range sm.probeTopicCache {
		_ = t.Close()
	}
	sm.probeTopicCache = make(map[string]*pubsub.Topic)
	return firstErr
}

// sleep waits for d and reports false if the manager's context ends first.
func (sm *ShardManager) sleep(d time.Duration) bool {
	select {
	case <-sm.ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// runEvery calls fn every interval until the manager's context ends.
func (sm *ShardManager) runEvery(interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

var (
	_ lifecycleOps   = (*ShardManager)(nil)
	_ replicationOps = (*ShardManager)(nil)
)

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

func (sm *ShardManager) replicationContext() context.Context { return sm.ctx }
func (sm *ShardManager) replicationConfig() *config.Config   { return sm.cfg }
func (sm *ShardManager) getPinnedManifests() []string        { return sm.storageMgr.GetPinnedManifests() }
func (sm *ShardManager) isPinned(key string) bool            { return sm.storageMgr.IsPinned(key) }
func (sm *ShardManager) publishCBOR(data []byte, shardID string) {
	sm.PublishToShardCBOR(data, shardID)
}
func (sm *ShardManager) replicationSigner() MessageAuthenticator { return sm.signer }
func (sm *ShardManager) clusterTriggerSync(shardID string)       { sm.clusterMgr.TriggerSync(shardID) }
func (sm *ShardManager) ipfsPinManifest(ctx context.Context, c cid.Cid) error {
	return sm.ipfsClient.PinManifest(ctx, c)
}
func (sm *ShardManager) ensureCluster(ctx context.Context, shardID string) error {
	return sm.EnsureClusterForShard(ctx, shardID)
}
func (sm *ShardManager) clusterPinIfAbsent(ctx context.Context, shardID string, c cid.Cid) error {
	return sm.clusterMgr.PinIfAbsent(ctx, shardID, c)
}
func (sm *ShardManager) isLegacyManifest(cidStr string) bool {
	ctx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	defer cancel()
	return common.IsLegacyManifest(ctx, sm.ipfsClient, cidStr)
}

func (sm *ShardManager) allowsManifestCID(ctx context.Context, c cid.Cid) bool {
	if sm.origin == nil || !sm.origin.RestrictsOrigins() {
		return true
	}
	block, err := sm.ipfsClient.GetBlock(ctx, c)
	if err != nil {
		slog.Warn("refusing data: cannot read manifest for origin check", "manifest", c.String(), "error", err)
		return false
	}
	if _, err := trust.CheckManifestOrigin(block, sm.origin); err != nil {
		slog.Warn("refusing data from untrusted origin", "manifest", c.String(), "error", err)
		return false
	}
	return true
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
	go sm.scheduleDelayedLeave(fromShard)
	go sm.scheduleReshardPass(fromShard, toShard)
}

// publishLeaveFromShard announces departure immediately so peers stop counting
// this node as active, even though the topic stays open for ShardOverlapDuration.
func (sm *ShardManager) publishLeaveFromShard(fromShard string) {
	sm.mu.RLock()
	fromSub, exists := sm.shardSubs[fromShard]
	sm.mu.RUnlock()
	if exists && fromSub.topic != nil && !fromSub.observerOnly {
		_ = fromSub.topic.Publish(sm.ctx, []byte(msgPrefixLeave+sm.h.ID().String()))
	}
}

// schedulePinMigration migrates pins to toShard, or to the shard below it the
// node has moved on to in the meantime.
func (sm *ShardManager) schedulePinMigration(fromShard, toShard string) {
	if !sm.sleep(migratePinsFlushDelay) {
		return
	}
	target := sm.getCurrentShard()
	if target != toShard {
		if !strings.HasPrefix(target, toShard) {
			return
		}
		slog.Info("migration redirect", "from", fromShard, "to", target, "intermediate", toShard)
	}
	if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, target); err != nil {
		slog.Error("migration failed", "from", fromShard, "to", target, "error", err)
	}
}

func (sm *ShardManager) scheduleDelayedLeave(fromShard string) {
	if !sm.sleep(sm.cfg.Sharding.ShardOverlapDuration) {
		return
	}
	if sm.getCurrentShard() == fromShard {
		return
	}
	sm.LeaveShard(fromShard)
	if err := sm.clusterMgr.LeaveShard(fromShard); err != nil {
		slog.Error("failed to leave cluster", "shard", fromShard, "error", err)
	}
}

func (sm *ShardManager) scheduleReshardPass(fromShard, toShard string) {
	if !sm.sleep(sm.cfg.Files.ReshardDelay) {
		return
	}
	if sm.getCurrentShard() != toShard {
		return
	}
	sm.RunReshardPass(fromShard, toShard)
}
