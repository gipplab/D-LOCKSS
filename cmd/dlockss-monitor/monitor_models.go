// Package main: data models and core state for the D-LOCKSS monitor.
package main

import (
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

const (
	DiscoveryServiceTag        = "dlockss-prod"
	WebUIPort                  = 8080
	DefaultBootstrapShardDepth = 5 // Depth of shard tree to subscribe to on startup (covers late-join case)
	DefaultNodeCleanupTimeout  = 350 * time.Second
	ReplicationAnnounceTTL     = 350 * time.Second
	MonitorMinReplication      = 5
	MonitorMaxReplication      = 10
	ReplicationCleanupEvery    = 1 * time.Minute
	GeoIPCacheDuration         = 24 * time.Hour
	MaxGeoQueueSize            = 1000
	MonitorIdentityFile        = "monitor_identity.key"
	GeoFailureThreshold        = 5
	GeoCooldownDuration        = 5 * time.Minute
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

type GeoLocation struct {
	Country     string `json:"country"`
	RegionName  string `json:"regionName"`
	City        string `json:"city"`
	CountryCode string `json:"countryCode"`
}

type Monitor struct {
	mu                  sync.RWMutex
	nodes               map[string]*NodeState
	splitEvents         []ShardSplitEvent
	geoCache            map[string]*GeoLocation
	geoCacheTime        map[string]time.Time
	geoFailures         int
	geoCooldownUntil    time.Time
	treeCache           *ShardTreeNode
	treeCacheTime       time.Time
	treeDirty           bool
	geoQueue            chan geoRequest
	uniqueCIDs          map[string]time.Time
	shardTopics         map[string]*pubsub.Topic
	ps                  *pubsub.PubSub
	host                host.Host
	nodeFiles           map[string]map[string]time.Time
	manifestReplication map[string]map[string]time.Time
	peerShardLastSeen   map[string]map[string]time.Time
	manifestShard       map[string]string // manifest CID → observed shard (from PINNED/IngestMessage announcements)
}

type geoRequest struct {
	ip     string
	peerID string
}

func shardLogLabel(shardID string) string {
	if shardID == "" {
		return "root"
	}
	return shardID
}

func NewMonitor() *Monitor {
	m := &Monitor{
		nodes:               make(map[string]*NodeState),
		splitEvents:         make([]ShardSplitEvent, 0, 100),
		geoCache:            make(map[string]*GeoLocation),
		geoCacheTime:        make(map[string]time.Time),
		geoQueue:            make(chan geoRequest, MaxGeoQueueSize),
		uniqueCIDs:          make(map[string]time.Time),
		shardTopics:         make(map[string]*pubsub.Topic),
		nodeFiles:           make(map[string]map[string]time.Time),
		manifestReplication: make(map[string]map[string]time.Time),
		peerShardLastSeen:   make(map[string]map[string]time.Time),
		manifestShard:       make(map[string]string),
	}
	go m.geoWorker()
	go m.runReplicationCleanup()
	return m
}
