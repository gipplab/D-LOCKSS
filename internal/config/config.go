package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

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

func getEnvUint64(key string, defaultValue uint64) uint64 {
	if value := os.Getenv(key); value != "" {
		if uintValue, err := strconv.ParseUint(value, 10, 64); err == nil {
			return uintValue
		}
		log.Printf("[Config] Invalid uint64 value for %s, using default: %d", key, defaultValue)
	}
	return defaultValue
}

func LogConfiguration() {
	log.Printf("[Config] Discovery Tag: %s", DiscoveryServiceTag)
	log.Printf("[Config] Data Directory: %s", FileWatchFolder)
	log.Printf("[Config] Replication: %d-%d", MinReplication, MaxReplication)
	log.Printf("[Config] Check Interval: %v", CheckInterval)
	log.Printf("[Config] Max Shard Load: %d (deprecated, using peer count)", MaxShardLoad)
	log.Printf("[Config] Max Peers Per Shard: %d (split threshold, ensures 2x replication safety)", MaxPeersPerShard)
	log.Printf("[Config] Min Peers Per Shard: %d (minimum, ensures replication achievable)", MinPeersPerShard)
	log.Printf("[Config] Shard Peer Check Interval: %v", ShardPeerCheckInterval)
	log.Printf("[Config] Max Concurrent Checks: %d", MaxConcurrentReplicationChecks)
	log.Printf("[Config] Rate Limit: %d messages per %v", MaxMessagesPerWindow, RateLimitWindow)
	log.Printf("[Config] Backoff: %v - %v (multiplier: %.1f)", InitialBackoffDelay, MaxBackoffDelay, BackoffMultiplier)
	log.Printf("[Config] Metrics: Report interval %v", MetricsReportInterval)
	if MetricsExportPath != "" {
		log.Printf("[Config] Metrics Export: %s", MetricsExportPath)
	}
	log.Printf("[Config] Replication Cooldown: %v", ReplicationCheckCooldown)
	log.Printf("[Config] Removed File Cooldown: %v", RemovedFileCooldown)
	log.Printf("[Config] Node Country: %s", NodeCountry)
	log.Printf("[Config] BadBits Path: %s", BadBitsPath)
	log.Printf("[Config] Shard Overlap Duration: %v", ShardOverlapDuration)
	log.Printf("[Config] Replication Verification Delay: %v", ReplicationVerificationDelay)
	log.Printf("[Config] Disk Usage High Water Mark: %.1f%%", DiskUsageHighWaterMark)
	log.Printf("[Config] IPFS Node Address: %s", IPFSNodeAddress)
	log.Printf("[Config] Trust Mode: %s", TrustMode)
	log.Printf("[Config] Trust Store Path: %s", TrustStorePath)
	log.Printf("[Config] Signature Mode: %s", SignatureMode)
	log.Printf("[Config] Signature Max Age: %v", SignatureMaxAge)
	log.Printf("[Config] DHT Max Sample Size: %d", DHTMaxSampleSize)
	log.Printf("[Config] Use PubSub for Replication: %v (min shard peers: %d)", UsePubsubForReplication, MinShardPeersForPubsubOnly)
	log.Printf("[Config] Replication Cache TTL: %v", ReplicationCacheTTL)
	log.Printf("[Config] Auto Replication Enabled: %v", AutoReplicationEnabled)
	if AutoReplicationMaxSize > 0 {
		log.Printf("[Config] Auto Replication Max Size: %d bytes", AutoReplicationMaxSize)
	} else {
		log.Printf("[Config] Auto Replication Max Size: unlimited")
	}
	log.Printf("[Config] Auto Replication Timeout: %v", AutoReplicationTimeout)

	// File operation configuration
	log.Printf("[Config] File Import Timeout: %v", FileImportTimeout)
	log.Printf("[Config] DHT Provide Timeout: %v", DHTProvideTimeout)
	log.Printf("[Config] DHT Provide Retry Attempts: %d", DHTProvideRetryAttempts)
	log.Printf("[Config] DHT Provide Retry Delay: %v", DHTProvideRetryDelay)
	log.Printf("[Config] File Processing Delay: %v", FileProcessingDelay)
	log.Printf("[Config] File Retry Delay: %v", FileRetryDelay)
	log.Printf("[Config] Max Concurrent File Processing: %d", MaxConcurrentFileProcessing)

	// Replication timeouts
	log.Printf("[Config] Replication Workers: %d", ReplicationWorkers)
	log.Printf("[Config] Replication Queue Size: %d", ReplicationQueueSize)
	log.Printf("[Config] DHT Query Timeout: %v", DHTQueryTimeout)

	// Shard operation timeouts
	log.Printf("[Config] Shard Subscription Timeout: %v", ShardSubscriptionTimeout)

	// Pipeline configuration
	log.Printf("[Config] Pipeline Prober Workers: %d", PipelineProberWorkers)
	log.Printf("[Config] Pipeline Job Queue Size: %d", PipelineJobQueueSize)
	log.Printf("[Config] Pipeline Result Queue Size: %d", PipelineResultQueueSize)

	// Cryptographic parameters
	log.Printf("[Config] Nonce Size: %d bytes", NonceSize)
	log.Printf("[Config] Min Nonce Size: %d bytes", MinNonceSize)
	log.Printf("[Config] Future Skew Tolerance: %v", FutureSkewTolerance)

	// Reshard configuration
	log.Printf("[Config] Reshard Batch Size: %d", ReshardBatchSize)
	log.Printf("[Config] Reshard Delay After Split: %v", ReshardDelay)
	log.Printf("[Config] Telemetry Interval: %v", TelemetryInterval)
	log.Printf("[Config] Telemetry Include CIDs: %v", TelemetryIncludeCIDs)
	if HeartbeatInterval > 0 {
		log.Printf("[Config] Heartbeat Interval: %v", HeartbeatInterval)
	} else {
		log.Printf("[Config] Heartbeat Interval: auto (ShardPeerCheckInterval/3, min 30s)")
	}
	log.Printf("[Config] Verbose Logging: %v", VerboseLogging)
}

var (
	// ControlTopicName removed in favor of Tourist Pattern (ephemeral shard joining)
	DiscoveryServiceTag = getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-prod")
	FileWatchFolder     = getEnvString("DLOCKSS_DATA_DIR", "./data")
	MinReplication      = getEnvInt("DLOCKSS_MIN_REPLICATION", 5)
	MaxReplication      = getEnvInt("DLOCKSS_MAX_REPLICATION", 10)
	CheckInterval       = getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute)
	MaxShardLoad        = getEnvInt("DLOCKSS_MAX_SHARD_LOAD", 2000) // Deprecated: kept for backward compatibility
	// Shard splitting tied to replication requirements.
	// With MinReplication=5, we require at least 10 nodes per shard so that the
	// replication target remains achievable even after a split.
	MaxPeersPerShard               = getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", 20)                       // Split when shard exceeds this many peers (default: 20)
	MinPeersPerShard               = getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", 10)                       // Don't split if result would be below this many peers (default: 10)
	ShardPeerCheckInterval         = getEnvDuration("DLOCKSS_SHARD_PEER_CHECK_INTERVAL", 2*time.Minute) // How often to check peer count
	ShardDiscoveryInterval         = getEnvDuration("DLOCKSS_SHARD_DISCOVERY_INTERVAL", 5*time.Minute)  // How often to check for deeper shards when idle
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
	NodeCountry                    = getEnvString("DLOCKSS_NODE_COUNTRY", "US")
	BadBitsPath                    = getEnvString("DLOCKSS_BADBITS_PATH", "badBits.csv")
	ShardOverlapDuration           = getEnvDuration("DLOCKSS_SHARD_OVERLAP_DURATION", 2*time.Minute)
	ReplicationVerificationDelay   = getEnvDuration("DLOCKSS_REPLICATION_VERIFICATION_DELAY", 2*time.Minute) // Grace period before verifying newly pinned files
	DiskUsageHighWaterMark         = getEnvFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", 90.0)
	IPFSNodeAddress                = getEnvString("DLOCKSS_IPFS_NODE", "/ip4/127.0.0.1/tcp/5001")
	APIPort                        = getEnvInt("DLOCKSS_API_PORT", 5050) // observability /metrics and /status
	TrustMode                      = getEnvString("DLOCKSS_TRUST_MODE", "open") // open | allowlist
	TrustStorePath                 = getEnvString("DLOCKSS_TRUST_STORE", "trusted_peers.json")
	SignatureMode                  = getEnvString("DLOCKSS_SIGNATURE_MODE", "warn") // off | warn | strict
	SignatureMaxAge                = getEnvDuration("DLOCKSS_SIGNATURE_MAX_AGE", 10*time.Minute)
	DHTMaxSampleSize               = getEnvInt("DLOCKSS_DHT_MAX_SAMPLE_SIZE", 50)                      // Max providers to query per DHT lookup
	UsePubsubForReplication        = getEnvBool("DLOCKSS_USE_PUBSUB_FOR_REPLICATION", true)            // Use pubsub peers first, DHT as fallback (avoids expensive DHT queries since nodes already know each other)
	MinShardPeersForPubsubOnly     = getEnvInt("DLOCKSS_MIN_SHARD_PEERS_PUBSUB_ONLY", 5)               // Only use pubsub-only if shard has at least this many peers (otherwise query DHT for additional providers)
	ReplicationCacheTTL            = getEnvDuration("DLOCKSS_REPLICATION_CACHE_TTL", 5*time.Minute)    // How long to cache replication counts
	AutoReplicationEnabled         = getEnvBool("DLOCKSS_AUTO_REPLICATION_ENABLED", true)              // Enable automatic replication on ReplicationRequest
	AutoReplicationMaxSize         = getEnvUint64("DLOCKSS_AUTO_REPLICATION_MAX_SIZE", 0)              // Max file size for auto-replication (0 = unlimited)
	AutoReplicationTimeout         = getEnvDuration("DLOCKSS_AUTO_REPLICATION_TIMEOUT", 5*time.Minute) // Timeout for fetching files during replication

	// File operation timeouts and delays
	FileImportTimeout           = getEnvDuration("DLOCKSS_FILE_IMPORT_TIMEOUT", 2*time.Minute)            // Timeout for importing files to IPFS
	DHTProvideTimeout           = getEnvDuration("DLOCKSS_DHT_PROVIDE_TIMEOUT", 60*time.Second)           // Timeout for providing files to DHT (increased to account for retries)
	DHTProvideRetryAttempts     = getEnvInt("DLOCKSS_DHT_PROVIDE_RETRY_ATTEMPTS", 3)                      // Number of retry attempts for DHT provide on transient errors
	DHTProvideRetryDelay        = getEnvDuration("DLOCKSS_DHT_PROVIDE_RETRY_DELAY", 500*time.Millisecond) // Delay between retry attempts
	FileProcessingDelay         = getEnvDuration("DLOCKSS_FILE_PROCESSING_DELAY", 100*time.Millisecond)   // Delay before processing new files
	FileRetryDelay              = getEnvDuration("DLOCKSS_FILE_RETRY_DELAY", 200*time.Millisecond)        // Delay between file processing retries
	MaxConcurrentFileProcessing = getEnvInt("DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING", 5)                  // Maximum number of files processed concurrently

	// Replication configuration
	ReplicationWorkers   = getEnvInt("DLOCKSS_REPLICATION_WORKERS", 5)       // Number of concurrent network probers (reduced for bandwidth)
	ReplicationQueueSize = getEnvInt("DLOCKSS_REPLICATION_QUEUE_SIZE", 1000) // Size of replication job/result channels

	// Replication timeouts
	DHTQueryTimeout = getEnvDuration("DLOCKSS_DHT_QUERY_TIMEOUT", 2*time.Minute) // Timeout for DHT queries during replication checks

	// Shard operation timeouts
	ShardSubscriptionTimeout = getEnvDuration("DLOCKSS_SHARD_SUBSCRIPTION_TIMEOUT", 2*time.Second) // Timeout for shard subscription cleanup

	// Pipeline configuration
	PipelineProberWorkers   = getEnvInt("DLOCKSS_PIPELINE_PROBER_WORKERS", 50)      // Number of concurrent DHT probers
	PipelineJobQueueSize    = getEnvInt("DLOCKSS_PIPELINE_JOB_QUEUE_SIZE", 1000)    // Size of job channel buffer
	PipelineResultQueueSize = getEnvInt("DLOCKSS_PIPELINE_RESULT_QUEUE_SIZE", 1000) // Size of result channel buffer

	// Reshard configuration
	ReshardBatchSize = getEnvInt("DLOCKSS_RESHARD_BATCH_SIZE", 200)           // Files per batch during reshard pass
	ReshardDelay     = getEnvDuration("DLOCKSS_RESHARD_DELAY", 5*time.Second) // Delay after split before starting reshard pass

	// Cryptographic parameters
	NonceSize           = getEnvInt("DLOCKSS_NONCE_SIZE", 16)                             // Size of cryptographic nonces in bytes
	MinNonceSize        = getEnvInt("DLOCKSS_MIN_NONCE_SIZE", 8)                          // Minimum allowed nonce size
	FutureSkewTolerance = getEnvDuration("DLOCKSS_FUTURE_SKEW_TOLERANCE", 30*time.Second) // Tolerance for future timestamps in signature verification

	// Telemetry
	MonitorPeerID        = getEnvString("DLOCKSS_MONITOR_PEER_ID", "")                  // Deprecated - nodes are now monitor-agnostic, telemetry uses pubsub
	TelemetryInterval    = getEnvDuration("DLOCKSS_TELEMETRY_INTERVAL", 30*time.Second) // How often to send telemetry
	TelemetryIncludeCIDs = getEnvBool("DLOCKSS_TELEMETRY_INCLUDE_CIDS", false)          // Include full CID list in telemetry (disabled by default - monitor tracks via pubsub)
	HeartbeatInterval    = getEnvDuration("DLOCKSS_HEARTBEAT_INTERVAL", 0)              // Heartbeat interval (0 = auto-calculate from ShardPeerCheckInterval/3, min 10s)
	VerboseLogging       = getEnvBool("DLOCKSS_VERBOSE_LOGGING", false)                 // extra debug (shard discovery, split, metrics)
)
