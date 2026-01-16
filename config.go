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

func logConfiguration() {
	log.Printf("[Config] Control Topic: %s", ControlTopicName)
	log.Printf("[Config] Discovery Tag: %s", DiscoveryServiceTag)
	log.Printf("[Config] Data Directory: %s", FileWatchFolder)
	log.Printf("[Config] Replication: %d-%d", MinReplication, MaxReplication)
	log.Printf("[Config] Check Interval: %v", CheckInterval)
	log.Printf("[Config] Max Shard Load: %d", MaxShardLoad)
	log.Printf("[Config] Max Concurrent Checks: %d", MaxConcurrentReplicationChecks)
	log.Printf("[Config] Rate Limit: %d messages per %v", MaxMessagesPerWindow, RateLimitWindow)
	log.Printf("[Config] Backoff: %v - %v (multiplier: %.1f)", InitialBackoffDelay, MaxBackoffDelay, BackoffMultiplier)
	log.Printf("[Config] Metrics: Report interval %v", MetricsReportInterval)
	if MetricsExportPath != "" {
		log.Printf("[Config] Metrics Export: %s", MetricsExportPath)
	}
	log.Printf("[Config] Replication Cooldown: %v", ReplicationCheckCooldown)
	log.Printf("[Config] Removed File Cooldown: %v", RemovedFileCooldown)
}

var (
	ControlTopicName               = getEnvString("DLOCKSS_CONTROL_TOPIC", "dlockss-control")
	DiscoveryServiceTag            = getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-v2-prod")
	FileWatchFolder                = getEnvString("DLOCKSS_DATA_DIR", "./data")
	MinReplication                 = getEnvInt("DLOCKSS_MIN_REPLICATION", 5)
	MaxReplication                 = getEnvInt("DLOCKSS_MAX_REPLICATION", 10)
	CheckInterval                  = getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute)
	MaxShardLoad                   = getEnvInt("DLOCKSS_MAX_SHARD_LOAD", 2000)
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
)
