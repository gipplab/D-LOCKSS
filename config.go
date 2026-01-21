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
	log.Printf("[Config] Max Peers Per Shard: %d (split threshold)", MaxPeersPerShard)
	log.Printf("[Config] Min Peers Per Shard: %d (prevent over-splitting)", MinPeersPerShard)
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
}

var (
	ControlTopicName               = getEnvString("DLOCKSS_CONTROL_TOPIC", "dlockss-v2-creative-commons-control")
	DiscoveryServiceTag            = getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-v2-prod")
	FileWatchFolder                = getEnvString("DLOCKSS_DATA_DIR", "./data")
	MinReplication                 = getEnvInt("DLOCKSS_MIN_REPLICATION", 5)
	MaxReplication                 = getEnvInt("DLOCKSS_MAX_REPLICATION", 10)
	CheckInterval                  = getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute)
	MaxShardLoad                   = getEnvInt("DLOCKSS_MAX_SHARD_LOAD", 2000)                           // Deprecated: kept for backward compatibility
	MaxPeersPerShard               = getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", 150)                       // Production default: split when shard exceeds this many peers
	MinPeersPerShard               = getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", 50)                        // Don't split if shard would drop below this
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
)
