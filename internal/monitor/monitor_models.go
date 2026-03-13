// Package monitor provides the D-LOCKSS network monitor.
package monitor

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/oschwald/geoip2-golang"

	"dlockss/internal/common"
)

// shardSub bundles a PubSub topic with its subscription so that
// SwitchTopic / SwitchTopicPrefix can cancel the subscription
// before closing the topic (Topic.Close fails if subs are active).
type shardSub struct {
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

const (
	DiscoveryServiceTag          = "dlockss-prod"
	WebUIPort                    = 8080
	DefaultBootstrapShardDepth   = 6  // Depth of shard tree to subscribe to on startup (covers late-join case)
	MaxShardDepthForSubscription = 10 // Don't subscribe to shards deeper than this (avoids thousands of topics)
	MaxShardDepthForTreeDisplay  = 8  // Prune tree display at this depth (avoids very deep chart)
	DefaultNodeCleanupTimeout    = 350 * time.Second
	ReplicationAnnounceTTL       = 350 * time.Second
	MonitorMinReplication        = 5
	MonitorMaxReplication        = 10
	ReplicationCleanupEvery      = 1 * time.Minute
	MonitorIdentityFile          = "monitor_identity.key"
	siblingMoveCooldown          = 90 * time.Second // ignore sibling moves within this window (reduces 00↔01, 10↔11 oscillation; gossip-sub can delay 20–30s)
	unpinGracePeriod             = 30 * time.Second // don't act on pinned=0 until this long after first discovery (avoids stale heartbeats)
)

// MonitorConfig holds runtime-configurable settings for the monitor,
// overridable via environment variables.
type MonitorConfig struct {
	NodeCleanupTimeout  time.Duration
	BootstrapShardDepth int
	PubsubTopicPrefix   string
	TopicName           string
}

const DefaultPubsubTopicPrefix = "dlockss-v0.0.3"

const DefaultTopicName = "creative-commons"

func DefaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		NodeCleanupTimeout:  DefaultNodeCleanupTimeout,
		BootstrapShardDepth: DefaultBootstrapShardDepth,
		PubsubTopicPrefix:   DefaultPubsubTopicPrefix,
		TopicName:           DefaultTopicName,
	}
}

type StatusResponse = common.StatusResponse
type StorageStatus = common.StorageStatus
type ReplicationStatus = common.ReplicationStatus

type NodeState struct {
	PeerID         string              `json:"peer_id"`
	NodeName       string              `json:"node_name,omitempty"`
	CurrentShard   string              `json:"current_shard"`
	Role           string              `json:"role,omitempty"` // ACTIVE, PASSIVE, REPLICATOR, or PROBE (empty = ACTIVE)
	PinnedFiles    int                 `json:"pinned_files"`
	KnownFiles     int                 `json:"known_files"`
	LastSeen       time.Time           `json:"last_seen"`
	ShardHistory   []ShardHistoryEntry `json:"shard_history"`
	IPAddress      string              `json:"ip_address"`
	announcedFiles map[string]time.Time
}

type ShardHistoryEntry struct {
	ShardID   string    `json:"shard_id"`
	FirstSeen time.Time `json:"first_seen"`
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
	cfg                 MonitorConfig
	appCtx              context.Context    // long-lived context from StartLibP2P
	subCtx              context.Context    // per-generation context; cancelled on topic switch to kill goroutines immediately
	subCancel           context.CancelFunc // cancels subCtx
	topicPrefixOverride string             // if set, overrides config.PubsubTopicPrefix for subscriptions
	topicNameOverride   string             // if set, overrides config.TopicName for subscriptions
	nodes               map[string]*NodeState
	splitEvents         []ShardSplitEvent
	geoDB               *geoip2.Reader // local GeoIP database; nil if not configured
	geoCache            sync.Map       // IP → region string; cache for on-demand lookups
	treeCache           *ShardTreeNode
	treeCacheTime       time.Time
	treeDirty           bool
	uniqueCIDs          map[string]time.Time
	shardTopics         map[string]*shardSub
	ps                  *pubsub.PubSub
	host                host.Host
	nodeFiles           map[string]map[string]time.Time
	manifestReplication map[string]map[string]time.Time
	peerShardLastSeen   map[string]map[string]time.Time
	manifestShard       map[string]string // manifest CID → observed shard (from PINNED/IngestMessage announcements)
	lastSplitTime       time.Time         // when we last detected a split; used to avoid pruning during mesh formation
	peerLastSiblingMove map[string]siblingMoveRecord
	done                chan struct{} // closed on shutdown to stop background goroutines
}

// siblingMoveRecord tracks the last sibling shard move for cooldown (reduces 0↔1 oscillation from stale messages).
type siblingMoveRecord struct {
	from string
	to   string
	when time.Time
}

func (n *NodeState) EffectiveShard() string {
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

// isDisplayableNode returns false for PROBE nodes and the monitor itself.
// ACTIVE, PASSIVE, and REPLICATOR nodes appear in the UI.
func (m *Monitor) isDisplayableNodeUnlocked(peerID string, node *NodeState) bool {
	if node.Role == "PROBE" {
		return false
	}
	if m.host != nil && peerID == m.host.ID().String() {
		return false
	}
	return true
}

// getTopicPrefix returns the effective topic prefix (override or config).
func (m *Monitor) getTopicPrefix() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getTopicPrefixUnlocked()
}

// getTopicPrefixUnlocked returns the effective topic prefix. Call only when holding m.mu.
func (m *Monitor) getTopicPrefixUnlocked() string {
	if m.topicPrefixOverride != "" {
		return m.topicPrefixOverride
	}
	return m.cfg.PubsubTopicPrefix
}

// getTopicName returns the effective topic name (override or config).
func (m *Monitor) getTopicName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getTopicNameUnlocked()
}

// getTopicNameUnlocked returns the effective topic name. Call only when holding m.mu.
func (m *Monitor) getTopicNameUnlocked() string {
	if m.topicNameOverride != "" {
		return m.topicNameOverride
	}
	return m.cfg.TopicName
}

// CIDEntry is a manifest CID with its observed shard and replica count.
// Used by node-files, unique-cids, and replication-cids API responses.
type CIDEntry struct {
	CID      string `json:"cid"`
	Shard    string `json:"shard"`
	Replicas int    `json:"replicas"`
}

// buildCIDEntries returns sorted CIDEntries for the given CID→time map.
// Caller must hold m.mu at least as RLock.
func (m *Monitor) buildCIDEntriesUnlocked(cids map[string]time.Time) []CIDEntry {
	entries := make([]CIDEntry, 0, len(cids))
	for cidStr := range cids {
		replicas := 0
		if peers, ok := m.manifestReplication[cidStr]; ok {
			replicas = len(peers)
		}
		shard := m.manifestShard[cidStr]
		entries = append(entries, CIDEntry{CID: cidStr, Shard: shard, Replicas: replicas})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].CID < entries[j].CID })
	return entries
}

func monitorDataDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(homeDir, ".dlockss-monitor")
	os.MkdirAll(dir, 0700)
	return dir
}

func NewMonitor(cfg MonitorConfig, geoDBPath string) *Monitor {
	m := &Monitor{
		cfg:                 cfg,
		nodes:               make(map[string]*NodeState),
		splitEvents:         make([]ShardSplitEvent, 0, 100),
		geoDB:               openGeoIPDB(geoDBPath),
		uniqueCIDs:          make(map[string]time.Time),
		shardTopics:         make(map[string]*shardSub),
		nodeFiles:           make(map[string]map[string]time.Time),
		manifestReplication: make(map[string]map[string]time.Time),
		peerShardLastSeen:   make(map[string]map[string]time.Time),
		manifestShard:       make(map[string]string),
		peerLastSiblingMove: make(map[string]siblingMoveRecord),
		done:                make(chan struct{}),
	}
	if m.geoDB != nil {
		slog.Info("geoip mode", "source", "local database")
	} else {
		slog.Info("geoip mode", "source", "ip-api.com")
	}
	go m.runReplicationCleanup()
	return m
}
