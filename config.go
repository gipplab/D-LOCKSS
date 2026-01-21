package main

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

func logConfiguration() {
	log.Printf("[Config] Control Topic: %s", ControlTopicName)
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
	log.Printf("[Config] File Processing Delay: %v", FileProcessingDelay)
	log.Printf("[Config] File Retry Delay: %v", FileRetryDelay)

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
}

var (
	ControlTopicName    = getEnvString("DLOCKSS_CONTROL_TOPIC", "dlockss-v2-creative-commons-control")
	DiscoveryServiceTag = getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-v2-prod")
	FileWatchFolder     = getEnvString("DLOCKSS_DATA_DIR", "./data")
	MinReplication      = getEnvInt("DLOCKSS_MIN_REPLICATION", 5)
	MaxReplication      = getEnvInt("DLOCKSS_MAX_REPLICATION", 10)
	CheckInterval       = getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute)
	MaxShardLoad        = getEnvInt("DLOCKSS_MAX_SHARD_LOAD", 2000) // Deprecated: kept for backward compatibility
	// Shard splitting tied to replication requirements
	// Must have 2 * MinReplication nodes per shard to ensure splits are safe
	// Using hardcoded defaults: MinReplication=5, so split at 20, min 10 per shard
	MaxPeersPerShard               = getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", 20)                        // Split when shard exceeds 2*MinReplication*2 nodes (default: 20)
	MinPeersPerShard               = getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", 10)                        // Don't split if result would be below MinReplication*2 nodes (default: 10)
	ShardPeerCheckInterval         = getEnvDuration("DLOCKSS_SHARD_PEER_CHECK_INTERVAL", 30*time.Second) // How often to check peer count
	MaxConcurrentReplicationChecks = getEnvInt("DLOCKSS_MAX_CONCURRENT_CHECKS", 10)
	RateLimitWindow                = getEnvDuration("DLOCKSS_RATE_LIMIT_WINDOW", 1*time.Minute)
	MaxMessagesPerWindow           = getEnvInt("DLOCKSS_MAX_MESSAGES_PER_WINDOW", 100)
	InitialBackoffDelay            = getEnvDuration("DLOCKSS_INITIAL_BACKOFF", 5*time.Second)
	MaxBackoffDelay                = getEnvDuration("DLOCKSS_MAX_BACKOFF", 5*time.Minute)
	BackoffMultiplier              = getEnvFloat("DLOCKSS_BACKOFF_MULTIPLIER", 2.0)
	MetricsReportInterval          = getEnvDuration("DLOCKSS_METRICS_INTERVAL", 5*time.Second)
	ReplicationCheckCooldown       = getEnvDuration("DLOCKSS_REPLICATION_COOLDOWN", 15*time.Second)
	RemovedFileCooldown            = getEnvDuration("DLOCKSS_REMOVED_COOLDOWN", 2*time.Minute)
	MetricsExportPath              = getEnvString("DLOCKSS_METRICS_EXPORT", "")
	NodeCountry                    = getEnvString("DLOCKSS_NODE_COUNTRY", "US")
	BadBitsPath                    = getEnvString("DLOCKSS_BADBITS_PATH", "badBits.csv")
	ShardOverlapDuration           = getEnvDuration("DLOCKSS_SHARD_OVERLAP_DURATION", 2*time.Minute)
	ReplicationVerificationDelay   = getEnvDuration("DLOCKSS_REPLICATION_VERIFICATION_DELAY", 30*time.Second)
	DiskUsageHighWaterMark         = getEnvFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", 90.0)
	IPFSNodeAddress                = getEnvString("DLOCKSS_IPFS_NODE", "/ip4/127.0.0.1/tcp/5001")
	TrustMode                      = getEnvString("DLOCKSS_TRUST_MODE", "open") // open | allowlist
	TrustStorePath                 = getEnvString("DLOCKSS_TRUST_STORE", "trusted_peers.json")
	SignatureMode                  = getEnvString("DLOCKSS_SIGNATURE_MODE", "warn") // off | warn | strict
	SignatureMaxAge                = getEnvDuration("DLOCKSS_SIGNATURE_MAX_AGE", 10*time.Minute)
	DHTMaxSampleSize               = getEnvInt("DLOCKSS_DHT_MAX_SAMPLE_SIZE", 50)                      // Max providers to query per DHT lookup
	ReplicationCacheTTL            = getEnvDuration("DLOCKSS_REPLICATION_CACHE_TTL", 5*time.Minute)    // How long to cache replication counts
	AutoReplicationEnabled         = getEnvBool("DLOCKSS_AUTO_REPLICATION_ENABLED", true)              // Enable automatic replication on ReplicationRequest
	AutoReplicationMaxSize         = getEnvUint64("DLOCKSS_AUTO_REPLICATION_MAX_SIZE", 0)              // Max file size for auto-replication (0 = unlimited)
	AutoReplicationTimeout         = getEnvDuration("DLOCKSS_AUTO_REPLICATION_TIMEOUT", 5*time.Minute) // Timeout for fetching files during replication

	// File operation timeouts and delays
	FileImportTimeout   = getEnvDuration("DLOCKSS_FILE_IMPORT_TIMEOUT", 2*time.Minute)          // Timeout for importing files to IPFS
	DHTProvideTimeout   = getEnvDuration("DLOCKSS_DHT_PROVIDE_TIMEOUT", 30*time.Second)         // Timeout for providing files to DHT
	FileProcessingDelay = getEnvDuration("DLOCKSS_FILE_PROCESSING_DELAY", 100*time.Millisecond) // Delay before processing new files
	FileRetryDelay      = getEnvDuration("DLOCKSS_FILE_RETRY_DELAY", 200*time.Millisecond)      // Delay between file processing retries

	// Replication configuration
	ReplicationWorkers   = getEnvInt("DLOCKSS_REPLICATION_WORKERS", 50)      // Number of concurrent network probers
	ReplicationQueueSize = getEnvInt("DLOCKSS_REPLICATION_QUEUE_SIZE", 1000) // Size of replication job/result channels

	// Replication timeouts
	DHTQueryTimeout = getEnvDuration("DLOCKSS_DHT_QUERY_TIMEOUT", 2*time.Minute) // Timeout for DHT queries during replication checks

	// Shard operation timeouts
	ShardSubscriptionTimeout = getEnvDuration("DLOCKSS_SHARD_SUBSCRIPTION_TIMEOUT", 2*time.Second) // Timeout for shard subscription cleanup

	// Pipeline configuration
	PipelineProberWorkers   = getEnvInt("DLOCKSS_PIPELINE_PROBER_WORKERS", 50)      // Number of concurrent DHT probers
	PipelineJobQueueSize    = getEnvInt("DLOCKSS_PIPELINE_JOB_QUEUE_SIZE", 1000)    // Size of job channel buffer
	PipelineResultQueueSize = getEnvInt("DLOCKSS_PIPELINE_RESULT_QUEUE_SIZE", 1000) // Size of result channel buffer

	// Cryptographic parameters
	NonceSize           = getEnvInt("DLOCKSS_NONCE_SIZE", 16)                             // Size of cryptographic nonces in bytes
	MinNonceSize        = getEnvInt("DLOCKSS_MIN_NONCE_SIZE", 8)                          // Minimum allowed nonce size
	FutureSkewTolerance = getEnvDuration("DLOCKSS_FUTURE_SKEW_TOLERANCE", 30*time.Second) // Tolerance for future timestamps in signature verification
)
