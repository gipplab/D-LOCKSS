package config

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// DefaultPubsubVersion is the protocol version used for pubsub topic names.
// Bump when releasing to avoid cross-talk with older nodes. Keep in sync with releases.
const DefaultPubsubVersion = "dlockss-v0.0.3"

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
		log.Printf("[Config] Invalid integer value for %s, using default: %d", key, defaultValue)
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
		log.Printf("[Config] Invalid duration value for %s, using default: %v", key, defaultValue)
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
		log.Printf("[Config] Invalid float value for %s, using default: %f", key, defaultValue)
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
		log.Printf("[Config] Invalid boolean value for %s, using default: %v", key, defaultValue)
	}
	return defaultValue
}

func getClusterStorePath() string {
	if p := os.Getenv("DLOCKSS_CLUSTER_STORE"); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(FileWatchFolder), "cluster_store")
}

func LogConfiguration() {
	log.Printf("[Config] Discovery Tag: %s", DiscoveryServiceTag)
	log.Printf("[Config] Pubsub Topic Prefix: %s", PubsubTopicPrefix)
	log.Printf("[Config] Data Directory: %s", FileWatchFolder)
	log.Printf("[Config] Cluster Store: %s", ClusterStorePath)
	log.Printf("[Config] Replication: %d-%d", MinReplication, MaxReplication)
	log.Printf("[Config] Check Interval: %v", CheckInterval)
	log.Printf("[Config] Max Peers Per Shard: %d (do not split until this many nodes)", MaxPeersPerShard)
	log.Printf("[Config] Min Peers Per Shard: %d (minimum per child after split)", MinPeersPerShard)
	log.Printf("[Config] Min Peers Across Siblings: %d (join/stay only if shard+sibling >= this; else remerge)", MinPeersAcrossSiblings)
	log.Printf("[Config] Shard Peer Check Interval: %v", ShardPeerCheckInterval)
	log.Printf("[Config] Shard Discovery Interval: %v", ShardDiscoveryInterval)
	log.Printf("[Config] Shard Split Rebroadcast Interval: %v", ShardSplitRebroadcastInterval)
	log.Printf("[Config] Bootstrap Timeout: %v", BootstrapTimeout)
	log.Printf("[Config] Seen Peers Window: %v", SeenPeersWindow)
	log.Printf("[Config] Prune Stale Peers Interval: %v", PruneStalePeersInterval)
	log.Printf("[Config] Max Concurrent Checks: %d", MaxConcurrentReplicationChecks)
	log.Printf("[Config] Rate Limit: %d messages per %v", MaxMessagesPerWindow, RateLimitWindow)
	log.Printf("[Config] Backoff: %v - %v (multiplier: %.1f)", InitialBackoffDelay, MaxBackoffDelay, BackoffMultiplier)
	log.Printf("[Config] Metrics: Report interval %v", MetricsReportInterval)
	if MetricsExportPath != "" {
		log.Printf("[Config] Metrics Export: %s", MetricsExportPath)
	}
	log.Printf("[Config] Replication Cooldown: %v", ReplicationCheckCooldown)
	log.Printf("[Config] Removed File Cooldown: %v", RemovedFileCooldown)
	log.Printf("[Config] BadBits Path: %s", BadBitsPath)
	log.Printf("[Config] Shard Overlap Duration: %v", ShardOverlapDuration)
	log.Printf("[Config] Orphan Unpin Grace: %v, Handoff Grace: %v, Min Handoff Count: %d", OrphanUnpinGracePeriod, OrphanHandoffGrace, OrphanUnpinMinHandoffCount)
	log.Printf("[Config] Replication Verification Delay: %v", ReplicationVerificationDelay)
	log.Printf("[Config] Disk Usage High Water Mark: %.1f%%", DiskUsageHighWaterMark)
	log.Printf("[Config] IPFS Node Address: %s", IPFSNodeAddress)
	log.Printf("[Config] Trust Mode: %s", TrustMode)
	log.Printf("[Config] Trust Store Path: %s", TrustStorePath)
	log.Printf("[Config] Signature Mode: %s", SignatureMode)
	if SignatureMode != "off" && SignatureMode != "warn" && SignatureMode != "strict" {
		log.Printf("[Config] Unknown SignatureMode %q; defaulting to strict", SignatureMode)
		SignatureMode = "strict"
	}
	if MaxConcurrentFileProcessing < 1 {
		log.Printf("[Config] MaxConcurrentFileProcessing=%d is invalid; defaulting to 5", MaxConcurrentFileProcessing)
		MaxConcurrentFileProcessing = 5
	}
	if NonceSize < 1 {
		log.Printf("[Config] NonceSize=%d is invalid; defaulting to 16", NonceSize)
		NonceSize = 16
	}
	if MinNonceSize < 1 {
		log.Printf("[Config] MinNonceSize=%d is invalid; defaulting to 8", MinNonceSize)
		MinNonceSize = 8
	}
	if MinReplication > MaxReplication {
		log.Printf("[Config] MinReplication (%d) > MaxReplication (%d); swapping", MinReplication, MaxReplication)
		MinReplication, MaxReplication = MaxReplication, MinReplication
	}
	if MaxConcurrentReplicationChecks < 1 {
		log.Printf("[Config] MaxConcurrentReplicationChecks=%d is invalid; defaulting to 5", MaxConcurrentReplicationChecks)
		MaxConcurrentReplicationChecks = 5
	}
	if DiskUsageHighWaterMark <= 0 || DiskUsageHighWaterMark > 100 {
		log.Printf("[Config] DiskUsageHighWaterMark=%.1f is out of range (0,100]; defaulting to 90.0", DiskUsageHighWaterMark)
		DiskUsageHighWaterMark = 90.0
	}
	log.Printf("[Config] Signature Max Age: %v", SignatureMaxAge)
	log.Printf("[Config] Use PubSub for Replication: %v (min shard peers: %d)", UsePubsubForReplication, MinShardPeersForPubsubOnly)
	log.Printf("[Config] Replication Cache TTL: %v", ReplicationCacheTTL)
	log.Printf("[Config] Auto Replication Enabled: %v", AutoReplicationEnabled)
	log.Printf("[Config] Auto Replication Timeout: %v", AutoReplicationTimeout)
	log.Printf("[Config] CRDT Op Timeout: %v", CRDTOpTimeout)

	// File operation configuration
	log.Printf("[Config] File Import Timeout: %v", FileImportTimeout)
	log.Printf("[Config] DHT Provide Timeout: %v", DHTProvideTimeout)
	log.Printf("[Config] File Processing Delay: %v", FileProcessingDelay)
	log.Printf("[Config] File Stability Delay: %v (wait for file size to settle before ingest)", FileStabilityDelay)
	log.Printf("[Config] Max Concurrent File Processing: %d", MaxConcurrentFileProcessing)

	// Replication timeouts
	log.Printf("[Config] DHT Query Timeout: %v", DHTQueryTimeout)

	// Cryptographic parameters
	log.Printf("[Config] Nonce Size: %d bytes", NonceSize)
	log.Printf("[Config] Min Nonce Size: %d bytes", MinNonceSize)
	log.Printf("[Config] Future Skew Tolerance: %v", FutureSkewTolerance)

	// Reshard configuration
	log.Printf("[Config] Reshard Delay After Split: %v", ReshardDelay)
	log.Printf("[Config] Reshard Handoff Delay: %v", ReshardHandoffDelay)
	log.Printf("[Config] Telemetry Interval: %v", TelemetryInterval)
	log.Printf("[Config] Telemetry Include CIDs: %v", TelemetryIncludeCIDs)
	if HeartbeatInterval > 0 {
		log.Printf("[Config] Heartbeat Interval: %v", HeartbeatInterval)
	} else {
		log.Printf("[Config] Heartbeat Interval: auto (ShardPeerCheckInterval/3, min 10s)")
	}
	log.Printf("[Config] Verbose Logging: %v", VerboseLogging)
	log.Printf("[Config] Merge Up Cooldown: %v", MergeUpCooldown)
	log.Printf("[Config] Probe Timeout Merge: %v", ProbeTimeoutMerge)
	log.Printf("[Config] Sibling Empty Merge After: %v", SiblingEmptyMergeAfter)
	log.Printf("[Config] Shard Move Cooldown: %v", ShardMoveCooldown)
}

var (
	DiscoveryServiceTag            = getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-prod")
	PubsubTopicPrefix              = getEnvString("DLOCKSS_PUBSUB_TOPIC_PREFIX", DefaultPubsubVersion)
	FileWatchFolder                = getEnvString("DLOCKSS_DATA_DIR", "./data")
	ClusterStorePath               = getClusterStorePath()
	MinReplication                 = getEnvInt("DLOCKSS_MIN_REPLICATION", 5)
	MaxReplication                 = getEnvInt("DLOCKSS_MAX_REPLICATION", 10)
	CheckInterval                  = getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute)
	MaxPeersPerShard               = getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", 12)
	MinPeersPerShard               = getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", 6)
	MinPeersAcrossSiblings         = getEnvInt("DLOCKSS_MIN_PEERS_ACROSS_SIBLINGS", 10)
	ShardPeerCheckInterval         = getEnvDuration("DLOCKSS_SHARD_PEER_CHECK_INTERVAL", 2*time.Minute)
	ShardDiscoveryInterval         = getEnvDuration("DLOCKSS_SHARD_DISCOVERY_INTERVAL", 2*time.Minute)
	ShardSplitRebroadcastInterval  = getEnvDuration("DLOCKSS_SHARD_SPLIT_REBROADCAST_INTERVAL", 60*time.Second)
	BootstrapTimeout               = getEnvDuration("DLOCKSS_BOOTSTRAP_TIMEOUT", 15*time.Second)
	SeenPeersWindow                = getEnvDuration("DLOCKSS_SEEN_PEERS_WINDOW", 350*time.Second)
	PruneStalePeersInterval        = getEnvDuration("DLOCKSS_PRUNE_STALE_PEERS_INTERVAL", 10*time.Minute)
	MaxConcurrentReplicationChecks = getEnvInt("DLOCKSS_MAX_CONCURRENT_CHECKS", 5)
	RateLimitWindow                = getEnvDuration("DLOCKSS_RATE_LIMIT_WINDOW", 1*time.Minute)
	MaxMessagesPerWindow           = getEnvInt("DLOCKSS_MAX_MESSAGES_PER_WINDOW", 100)
	InitialBackoffDelay            = getEnvDuration("DLOCKSS_INITIAL_BACKOFF", 5*time.Second)
	MaxBackoffDelay                = getEnvDuration("DLOCKSS_MAX_BACKOFF", 5*time.Minute)
	BackoffMultiplier              = getEnvFloat("DLOCKSS_BACKOFF_MULTIPLIER", 2.0)
	MetricsReportInterval          = getEnvDuration("DLOCKSS_METRICS_INTERVAL", 5*time.Second)
	ReplicationCheckCooldown       = getEnvDuration("DLOCKSS_REPLICATION_COOLDOWN", 1*time.Minute) // Increased to reduce bandwidth
	RemovedFileCooldown            = getEnvDuration("DLOCKSS_REMOVED_COOLDOWN", 2*time.Minute)
	MetricsExportPath              = getEnvString("DLOCKSS_METRICS_EXPORT", "")
	BadBitsPath                    = getEnvString("DLOCKSS_BADBITS_PATH", "badBits.csv")
	ShardOverlapDuration           = getEnvDuration("DLOCKSS_SHARD_OVERLAP_DURATION", 2*time.Minute)
	OrphanUnpinGracePeriod         = getEnvDuration("DLOCKSS_ORPHAN_UNPIN_GRACE", 6*time.Minute)
	OrphanHandoffGrace             = getEnvDuration("DLOCKSS_ORPHAN_HANDOFF_GRACE", 6*time.Minute)
	OrphanUnpinMinHandoffCount     = getEnvInt("DLOCKSS_ORPHAN_MIN_HANDOFF_COUNT", 2)
	ReplicationVerificationDelay   = getEnvDuration("DLOCKSS_REPLICATION_VERIFICATION_DELAY", 2*time.Minute)
	DiskUsageHighWaterMark         = getEnvFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", 90.0)
	IPFSNodeAddress                = getEnvString("DLOCKSS_IPFS_NODE", "/ip4/127.0.0.1/tcp/5001")
	APIPort                        = getEnvInt("DLOCKSS_API_PORT", 5050) // observability /metrics and /status
	TrustMode                      = getEnvString("DLOCKSS_TRUST_MODE", "open")
	TrustStorePath                 = getEnvString("DLOCKSS_TRUST_STORE", "trusted_peers.json")
	SignatureMode                  = getEnvString("DLOCKSS_SIGNATURE_MODE", "strict")
	SignatureMaxAge                = getEnvDuration("DLOCKSS_SIGNATURE_MAX_AGE", 10*time.Minute)
	UsePubsubForReplication        = getEnvBool("DLOCKSS_USE_PUBSUB_FOR_REPLICATION", true)
	MinShardPeersForPubsubOnly     = getEnvInt("DLOCKSS_MIN_SHARD_PEERS_PUBSUB_ONLY", 5)
	ReplicationCacheTTL            = getEnvDuration("DLOCKSS_REPLICATION_CACHE_TTL", 5*time.Minute)
	AutoReplicationEnabled         = getEnvBool("DLOCKSS_AUTO_REPLICATION_ENABLED", true)
	AutoReplicationTimeout         = getEnvDuration("DLOCKSS_AUTO_REPLICATION_TIMEOUT", 5*time.Minute)
	CRDTOpTimeout                  = getEnvDuration("DLOCKSS_CRDT_OP_TIMEOUT", 10*time.Minute)

	FileImportTimeout           = getEnvDuration("DLOCKSS_FILE_IMPORT_TIMEOUT", 2*time.Minute)
	DHTProvideTimeout           = getEnvDuration("DLOCKSS_DHT_PROVIDE_TIMEOUT", 60*time.Second)
	FileProcessingDelay         = getEnvDuration("DLOCKSS_FILE_PROCESSING_DELAY", 100*time.Millisecond)
	FileStabilityDelay          = getEnvDuration("DLOCKSS_FILE_STABILITY_DELAY", 3*time.Second)
	MaxConcurrentFileProcessing = getEnvInt("DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING", 5)

	DHTQueryTimeout = getEnvDuration("DLOCKSS_DHT_QUERY_TIMEOUT", 2*time.Minute)

	ReshardDelay        = getEnvDuration("DLOCKSS_RESHARD_DELAY", 5*time.Second)
	ReshardHandoffDelay = getEnvDuration("DLOCKSS_RESHARD_HANDOFF_DELAY", 3*time.Second)

	PinReannounceInterval = getEnvDuration("DLOCKSS_PIN_REANNOUNCE_INTERVAL", 2*time.Minute)

	NonceSize           = getEnvInt("DLOCKSS_NONCE_SIZE", 16)
	MinNonceSize        = getEnvInt("DLOCKSS_MIN_NONCE_SIZE", 8)
	FutureSkewTolerance = getEnvDuration("DLOCKSS_FUTURE_SKEW_TOLERANCE", 30*time.Second)

	TelemetryInterval    = getEnvDuration("DLOCKSS_TELEMETRY_INTERVAL", 30*time.Second)
	TelemetryIncludeCIDs = getEnvBool("DLOCKSS_TELEMETRY_INCLUDE_CIDS", false)
	HeartbeatInterval    = getEnvDuration("DLOCKSS_HEARTBEAT_INTERVAL", 10*time.Second)
	VerboseLogging       = getEnvBool("DLOCKSS_VERBOSE_LOGGING", false)

	MergeUpCooldown        = getEnvDuration("DLOCKSS_MERGE_UP_COOLDOWN", 2*time.Minute)
	ProbeTimeoutMerge      = getEnvDuration("DLOCKSS_PROBE_TIMEOUT_MERGE", 6*time.Second)
	SiblingEmptyMergeAfter = getEnvDuration("DLOCKSS_SIBLING_EMPTY_MERGE_AFTER", 5*time.Minute)
	ShardMoveCooldown      = getEnvDuration("DLOCKSS_SHARD_MOVE_COOLDOWN", 30*time.Second)
)
