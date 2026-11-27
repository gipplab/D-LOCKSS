// Package monitor provides the D-LOCKSS network monitor.
package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dlockss/internal/config"
	"dlockss/internal/keywords"
	"dlockss/internal/trust"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

// shardSub bundles a PubSub topic with its subscription.
type shardSub struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

const (
	discoveryServiceTag          = "dlockss-prod"
	WebUIPort                    = 8080
	defaultBootstrapShardDepth   = 6
	maxShardDepthForSubscription = 10
	maxShardDepthForTreeDisplay  = 8
	defaultNodeCleanupTimeout    = 350 * time.Second
	replicationAnnounceTTL       = 350 * time.Second
	replicationCleanupEvery      = 1 * time.Minute
	identityFileName             = "monitor_identity.key"
	// Gossip-sub can deliver heartbeats 20-30s late, so a second sibling move
	// within this window is treated as stale to stop nodes oscillating between
	// sibling shards.
	siblingMoveCooldown = 90 * time.Second
	// pinned=0 is ignored until this long after first discovery, since the
	// first heartbeats seen from a node can be stale.
	unpinGracePeriod = 30 * time.Second
)

// replicationTarget is the number of copies a file should have: nodes keep at
// least MinPeersPerShard members per shard, and every member holds every file.
var replicationTarget = config.DefaultConfig().Sharding.MinPeersPerShard

// Config holds runtime-configurable settings for the monitor,
// overridable via environment variables.
type Config struct {
	NodeCleanupTimeout  time.Duration
	BootstrapShardDepth int
	PubsubTopicPrefix   string
	TopicName           string
	// DataDir keeps the manifest catalog across restarts; "" keeps it in
	// memory only.
	DataDir string
}

func DefaultConfig() Config {
	return Config{
		NodeCleanupTimeout:  defaultNodeCleanupTimeout,
		BootstrapShardDepth: defaultBootstrapShardDepth,
		PubsubTopicPrefix:   config.DefaultPubsubVersion,
		TopicName:           config.DefaultTopicName,
	}
}

type nodeState struct {
	PeerID       string
	HelperID     string
	NodeName     string
	CurrentShard string
	Role         string
	LastSeen     time.Time
	ShardHistory []ShardHistoryEntry
}

type ShardHistoryEntry struct {
	ShardID   string
	FirstSeen time.Time
}

type ShardSplitEvent struct {
	ParentShard string    `json:"parent_shard"`
	ChildShard  string    `json:"child_shard"`
	Timestamp   time.Time `json:"timestamp"`
}

type ShardTreeNode struct {
	ShardID   string           `json:"shard_id"`
	SplitTime *time.Time       `json:"split_time,omitempty"`
	Children  []*ShardTreeNode `json:"children,omitempty"`
	NodeCount int              `json:"node_count"`
}

type Monitor struct {
	mu                  sync.RWMutex
	cfg                 Config
	appCtx              context.Context // set by StartLibP2P
	nodes               map[string]*nodeState
	helpers             *trust.Nodes // D-LOCKSS helper to Kubo node
	splitEvents         []ShardSplitEvent
	treeCache           *ShardTreeNode
	treeCacheTime       time.Time
	treeDirty           bool
	uniqueCIDs          map[string]time.Time
	shardTopics         map[string]*shardSub
	ps                  *pubsub.PubSub
	host                host.Host
	manifestReplication map[string]map[string]time.Time
	peerShardLastSeen   map[string]map[string]time.Time
	manifestShard       map[string]string    // manifest CID to the shard it was announced in, preferring descendants
	lastSplitTime       time.Time            // pruning is suspended for splitGracePeriod after this
	peerLastSiblingMove map[string]time.Time // peer ID to the time of its last sibling move
	lostNodes           map[string]lostNode  // nodes the monitor stopped hearing from, shown as offline
	started             time.Time
	keywords            *keywords.Store
	catalog             *manifestCatalog

	// snap is the network view the API serves, rebuilt at most every
	// snapshotTTL.
	snapMu sync.Mutex
	snap   *netSnapshot
}

func (n *nodeState) EffectiveShard() string {
	if n.CurrentShard != "" {
		return n.CurrentShard
	}
	if len(n.ShardHistory) > 0 {
		return n.ShardHistory[len(n.ShardHistory)-1].ShardID
	}
	return ""
}

func shardLabel(shardID string) string {
	if shardID == "" {
		return "root"
	}
	return shardID
}

// peerLabel names a peer in log output, including its node name when known.
func peerLabel(peerID, nodeName string) string {
	if nodeName == "" {
		return peerID
	}
	return nodeName + " (" + peerID + ")"
}

// isDisplayableNodeLocked returns false for PROBE nodes and the monitor itself.
// ACTIVE, PASSIVE, and REPLICATOR nodes appear in the UI.
func (m *Monitor) isDisplayableNodeLocked(peerID string, node *nodeState) bool {
	if node.Role == "PROBE" {
		return false
	}
	if m.host != nil && peerID == m.host.ID().String() {
		return false
	}
	return true
}

// shardTopic returns the pubsub topic name for shardID. m.cfg is never
// modified after NewMonitor, so no lock is needed.
func (m *Monitor) shardTopic(shardID string) string {
	return fmt.Sprintf("%s-%s-shard-%s", m.cfg.PubsubTopicPrefix, m.cfg.TopicName, shardID)
}

func NewMonitor(cfg Config) *Monitor {
	m := &Monitor{
		cfg:                 cfg,
		nodes:               make(map[string]*nodeState),
		helpers:             trust.NewNodes(trust.NewTrustManager("open")),
		splitEvents:         make([]ShardSplitEvent, 0, 100),
		uniqueCIDs:          make(map[string]time.Time),
		shardTopics:         make(map[string]*shardSub),
		manifestReplication: make(map[string]map[string]time.Time),
		peerShardLastSeen:   make(map[string]map[string]time.Time),
		manifestShard:       make(map[string]string),
		peerLastSiblingMove: make(map[string]time.Time),
		lostNodes:           make(map[string]lostNode),
		started:             time.Now(),
		keywords:            keywords.NewStore(keywords.Config{}),
		catalog:             newManifestCatalog(cfg.DataDir),
	}
	go m.runReplicationCleanup()
	return m
}

// SetKeywords replaces the (disabled) default keyword store. Call before StartKeywordIndexer.
func (m *Monitor) SetKeywords(s *keywords.Store) {
	if s != nil {
		m.keywords = s
	}
}

// StartKeywordIndexer indexes newly announced content in the background when an API key is set.
func (m *Monitor) StartKeywordIndexer(ctx context.Context) {
	if m.keywords == nil {
		return
	}
	go m.keywords.Run(ctx, m)
}

// UniqueCIDList returns manifest CIDs observed on gossip-sub.
func (m *Monitor) UniqueCIDList() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.uniqueCIDs))
	for c := range m.uniqueCIDs {
		out = append(out, c)
	}
	return out
}
