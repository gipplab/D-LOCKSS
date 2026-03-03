package shard

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/clusters"
	"dlockss/internal/telemetry"
	"dlockss/pkg/ipfs"
)

// MessageAuthenticator abstracts protocol message signing and verification.
type MessageAuthenticator interface {
	SignProtocolMessage(msg interface{}) error
	ShouldDropMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool
}

// StorageProvider abstracts the storage operations needed by the shard manager.
type StorageProvider interface {
	CanAcceptCustodialFile() bool
	IsPinned(key string) bool
	PinFile(manifestCIDStr string) bool
	UnpinFile(key string)
	AddKnownFile(key string)
	GetAllKnownFiles() map[string]bool
	GetPinTime(key string) time.Time
	GetPinnedManifests() []string
	GetPinnedCount() int
	GetNextFileToAnnounce() string
	ProvideFile(ctx context.Context, key string)
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

func shardTopicName(shardID string) string {
	return config.PubsubTopicPrefix + "-creative-commons-shard-" + shardID
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
	ctx            context.Context
	h              host.Host
	ps             *pubsub.PubSub
	ipfsClient     ipfs.IPFSClient
	storageMgr     StorageProvider
	clusterMgr     clusters.ClusterManagerInterface
	metrics        *telemetry.MetricsManager
	signer         MessageAuthenticator
	reshardedFiles *common.KnownFiles
	rateLimiter    *common.RateLimiter
	nodeName       string

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
	lastShardMove         time.Time // set on ANY shard transition (split, merge, discovery)
	lastProbeResponseTime time.Time // rate-limits heartbeat responses to PROBE messages

	seenPeers                map[string]map[peer.ID]time.Time
	seenPeerRoles            map[string]map[peer.ID]PeerRoleInfo
	observerOnlyShards       map[string]struct{}
	splitAboveThresholdCount int // consecutive checks where peerCount >= MaxPeersPerShard
	knownChildShards         map[string]time.Time
	orphanHandoffSent        map[string]map[string]*orphanHandoffInfo

	replicationRequestMu       sync.Mutex
	replicationRequestLastSent map[string]time.Time

	autoReplicationSem chan struct{} // bounds concurrent auto-replication fetches
}

type orphanHandoffInfo struct {
	lastSent time.Time
	count    int
}

func NewShardManager(
	ctx context.Context,
	h host.Host,
	ps *pubsub.PubSub,
	ipfsClient ipfs.IPFSClient,
	stm StorageProvider,
	metrics *telemetry.MetricsManager,
	signer MessageAuthenticator,
	rateLimiter *common.RateLimiter,
	clusterMgr clusters.ClusterManagerInterface,
	startShard string,
	nodeName string,
) *ShardManager {
	sm := &ShardManager{
		ctx:                        ctx,
		h:                          h,
		ps:                         ps,
		ipfsClient:                 ipfsClient,
		storageMgr:                 stm,
		clusterMgr:                 clusterMgr,
		metrics:                    metrics,
		signer:                     signer,
		rateLimiter:                rateLimiter,
		nodeName:                   nodeName,
		reshardedFiles:             common.NewKnownFiles(),
		currentShard:               startShard,
		shardSubs:                  make(map[string]*shardSubscription),
		probeTopicCache:            make(map[string]*pubsub.Topic),
		seenPeers:                  make(map[string]map[peer.ID]time.Time),
		seenPeerRoles:              make(map[string]map[peer.ID]PeerRoleInfo),
		observerOnlyShards:         make(map[string]struct{}),
		knownChildShards:           make(map[string]time.Time),
		orphanHandoffSent:          make(map[string]map[string]*orphanHandoffInfo),
		replicationRequestLastSent: make(map[string]time.Time),
		autoReplicationSem:         make(chan struct{}, config.MaxConcurrentReplicationChecks),
	}

	if err := sm.clusterMgr.JoinShard(ctx, startShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for start shard %s: %v", startShard, err)
	}

	sm.JoinShard(startShard)
	sm.loadReshardedFiles()

	return sm
}

func (sm *ShardManager) Run() {
	go sm.runPeerCountChecker()
	go sm.runHeartbeat()
	go sm.runSplitRebroadcast()
	go sm.runShardDiscovery()
	go sm.runOrphanUnpinLoop()
	go sm.runReplicationChecker()
	go sm.runReannouncePinsLoop()
	go sm.runReshardedFilesSaveLoop()
	go sm.runLegacyManifestCleanup()
}

func (sm *ShardManager) Close() {
	sm.saveReshardedFiles()
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
	sm.knownChildShards = make(map[string]time.Time)
	sm.lastPeerCheck = time.Now()
	sm.splitAboveThresholdCount = 0
	sm.reshardedFiles = common.NewKnownFiles()
	sm.lastShardMove = time.Now()
	if isMergeUp {
		sm.lastMergeUpTime = sm.lastShardMove
	} else {
		sm.lastMoveToDeeperShard = sm.lastShardMove
	}
	sm.mu.Unlock()

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

	sm.JoinShard(toShard)
	if err := sm.clusterMgr.JoinShard(sm.ctx, toShard, nil); err != nil {
		log.Printf("[Sharding] Failed to join cluster for shard %s: %v", toShard, err)
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
				log.Printf("[Sharding] Migration redirect: %s → %s (current shard moved past %s)", fromShard, current, toShard)
				if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, current); err != nil {
					log.Printf("[Sharding] Migration failed %s → %s: %v", fromShard, current, err)
				}
			}
			return
		}
		if err := sm.clusterMgr.MigratePins(sm.ctx, fromShard, toShard); err != nil {
			log.Printf("[Sharding] Migration failed %s → %s: %v", fromShard, toShard, err)
		}
	}()
	go func() {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(config.ShardOverlapDuration):
		}
		sm.mu.RLock()
		current := sm.currentShard
		sm.mu.RUnlock()
		if current == fromShard {
			return // we moved back to fromShard, don't leave it
		}
		sm.LeaveShard(fromShard)
		if err := sm.clusterMgr.LeaveShard(fromShard); err != nil {
			log.Printf("[Sharding] Failed to leave cluster %s: %v", fromShard, err)
		}
	}()
	go func() {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(config.ReshardDelay):
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
