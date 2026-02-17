// Package main: data models and core state for the D-LOCKSS monitor.
package main

import (
	"log"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/oschwald/geoip2-golang"

	"dlockss/internal/config"
)

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

var nodeCleanupTimeout = DefaultNodeCleanupTimeout
var bootstrapShardDepth = DefaultBootstrapShardDepth

type StatusResponse struct {
	PeerID        string            `json:"peer_id"`
	Version       string            `json:"version"`
	CurrentShard  string            `json:"current_shard"`
	PeersInShard  int               `json:"peers_in_shard"`
	Storage       StorageStatus     `json:"storage"`
	Replication   ReplicationStatus `json:"replication"`
	UptimeSeconds float64           `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles   int      `json:"pinned_files"`
	PinnedInShard int      `json:"pinned_in_shard,omitempty"`
	KnownFiles    int      `json:"known_files"`
	KnownCIDs     []string `json:"known_cids,omitempty"`
}

type ReplicationStatus struct {
	QueueDepth              int     `json:"queue_depth"`
	ActiveWorkers           int     `json:"active_workers"`
	AvgReplicationLevel     float64 `json:"avg_replication_level"`
	FilesAtTarget           int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"`
}

type NodeState struct {
	PeerID         string              `json:"peer_id"`
	CurrentShard   string              `json:"current_shard"`
	Role           string              `json:"role,omitempty"` // ACTIVE, PASSIVE, or PROBE (empty = ACTIVE)
	PinnedFiles    int                 `json:"pinned_files"`
	KnownFiles     int                 `json:"known_files"`
	LastSeen       time.Time           `json:"last_seen"`
	ShardHistory   []ShardHistoryEntry `json:"shard_history"`
	IPAddress      string              `json:"ip_address"`
	Region         string              `json:"region"`
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
	topicPrefixOverride string // if set, overrides config.PubsubTopicPrefix for subscriptions
	nodes               map[string]*NodeState
	splitEvents         []ShardSplitEvent
	geoDB               *geoip2.Reader // local GeoIP database; nil if not configured
	geoCache            sync.Map       // IP → region string; permanent cache for API lookups
	geoQueue            chan string    // IPs pending API geo lookup
	treeCache           *ShardTreeNode
	treeCacheTime       time.Time
	treeDirty           bool
	uniqueCIDs          map[string]time.Time
	shardTopics         map[string]*pubsub.Topic
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

func shardLogLabel(shardID string) string {
	if shardID == "" {
		return "root"
	}
	return shardID
}

// isDisplayableNode returns false for PROBE nodes and the monitor itself.
// Only ACTIVE and PASSIVE nodes should appear in the UI.
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
	return config.PubsubTopicPrefix
}

func NewMonitor(geoDBPath string) *Monitor {
	m := &Monitor{
		nodes:               make(map[string]*NodeState),
		splitEvents:         make([]ShardSplitEvent, 0, 100),
		geoDB:               openGeoIPDB(geoDBPath),
		geoQueue:            make(chan string, geoMaxQueueSize),
		uniqueCIDs:          make(map[string]time.Time),
		shardTopics:         make(map[string]*pubsub.Topic),
		nodeFiles:           make(map[string]map[string]time.Time),
		manifestReplication: make(map[string]map[string]time.Time),
		peerShardLastSeen:   make(map[string]map[string]time.Time),
		manifestShard:       make(map[string]string),
		peerLastSiblingMove: make(map[string]siblingMoveRecord),
		done:                make(chan struct{}),
	}
	if m.geoDB != nil {
		log.Println("[Monitor] GeoIP: using local database (instant lookups)")
	} else {
		log.Println("[Monitor] GeoIP: using ip-api.com API (async batch lookups)")
		go m.geoAPIWorker()
	}
	go m.runReplicationCleanup()
	return m
}
