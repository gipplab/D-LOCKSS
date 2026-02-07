package shard

import (
	"context"
	"log"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/clusters"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/internal/telemetry"
	"dlockss/pkg/ipfs"
)

const migratePinsFlushDelay = 250 * time.Millisecond
const rootPeerCheckInterval = 30 * time.Second
const rootReplicationCheckInterval = 20 * time.Second

type shardSubscription struct {
	topic        *pubsub.Topic
	sub          *pubsub.Subscription
	refCount     int
	cancel       context.CancelFunc
	shardID      string
	observerOnly bool
}

type ShardManager struct {
	ctx            context.Context
	h              host.Host
	ps             *pubsub.PubSub
	ipfsClient     ipfs.IPFSClient
	storageMgr     *storage.StorageManager
	clusterMgr     clusters.ClusterManagerInterface
	metrics        *telemetry.MetricsManager
	signer         *signing.Signer
	reshardedFiles *common.KnownFiles
	rateLimiter    *common.RateLimiter

	mu           sync.RWMutex
	currentShard string

	shardSubs       map[string]*shardSubscription
	probeTopicCache map[string]*pubsub.Topic

	msgCounter            int
	lastPeerCheck         time.Time
	lastDiscoveryCheck    time.Time
	lastMessageTime       time.Time
	lastMoveToDeeperShard time.Time
	lastMergeUpTime       time.Time

	seenPeers          map[string]map[peer.ID]time.Time
	observerOnlyShards map[string]struct{}
}

func NewShardManager(
	ctx context.Context,
	h host.Host,
	ps *pubsub.PubSub,
	ipfsClient ipfs.IPFSClient,
	stm *storage.StorageManager,
	metrics *telemetry.MetricsManager,
	signer *signing.Signer,
	rateLimiter *common.RateLimiter,
	clusterMgr clusters.ClusterManagerInterface,
	startShard string,
) *ShardManager {
	sm := &ShardManager{
		ctx:                ctx,
		h:                  h,
		ps:                 ps,
		ipfsClient:         ipfsClient,
		storageMgr:         stm,
		clusterMgr:         clusterMgr,
		metrics:            metrics,
		signer:             signer,
		rateLimiter:        rateLimiter,
		reshardedFiles:     common.NewKnownFiles(),
		currentShard:       startShard,
		shardSubs:          make(map[string]*shardSubscription),
		probeTopicCache:    make(map[string]*pubsub.Topic),
		seenPeers:          make(map[string]map[peer.ID]time.Time),
		observerOnlyShards: make(map[string]struct{}),
	}

	if err := sm.clusterMgr.JoinShard(ctx, startShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for start shard %s: %v", startShard, err)
	}

	sm.JoinShard(startShard)

	return sm
}

func (sm *ShardManager) Run() {
	go sm.runPeerCountChecker()
	go sm.runHeartbeat()
	go sm.runShardDiscovery()
	go sm.runOrphanUnpinLoop()
	go sm.runReplicationChecker()
	go sm.runReannouncePinsLoop()
}

func (sm *ShardManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, sub := range sm.shardSubs {
		sub.cancel()
		sub.sub.Cancel()
	}
}

// moveToShard atomically switches from one shard to another: join new, migrate pins, leave old.
// Used by split (parent→child), discovery (parent→existing child), and merge (child→parent).
func (sm *ShardManager) moveToShard(fromShard, toShard string, isMergeUp bool) {
	sm.mu.Lock()
	if sm.currentShard != fromShard {
		sm.mu.Unlock()
		return
	}
	sm.currentShard = toShard
	sm.msgCounter = 0
	if isMergeUp {
		sm.lastMergeUpTime = time.Now()
	} else {
		sm.lastMoveToDeeperShard = time.Now()
	}
	sm.mu.Unlock()

	sm.JoinShard(toShard)
	if err := sm.clusterMgr.JoinShard(sm.ctx, toShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for shard %s: %v", toShard, err)
	}
	go func() {
		time.Sleep(migratePinsFlushDelay)
		if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, toShard); err != nil {
			log.Printf("[Sharding] Migration failed %s → %s: %v", fromShard, toShard, err)
		}
	}()
	go func() {
		time.Sleep(config.ShardOverlapDuration)
		sm.LeaveShard(fromShard)
		if err := sm.clusterMgr.LeaveShard(fromShard); err != nil {
			log.Printf("[Sharding] Failed to leave cluster %s: %v", fromShard, err)
		}
	}()
	go func() {
		time.Sleep(config.ReshardDelay)
		sm.RunReshardPass(fromShard, toShard)
	}()
}
