package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
		slog.Warn("invalid env integer, using default", "key", key, "default", defaultValue)
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
		slog.Warn("invalid env duration, using default", "key", key, "default", defaultValue)
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
		slog.Warn("invalid env float, using default", "key", key, "default", defaultValue)
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
		slog.Warn("invalid env boolean, using default", "key", key, "default", defaultValue)
	}
	return defaultValue
}

func getEnvStringSlice(key string) []string {
	if value := os.Getenv(key); value != "" {
		parts := strings.Split(value, ",")
		var result []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, p)
			}
		}
		return result
	}
	return nil
}

func clusterStorePath(dataDir string) string {
	if p := os.Getenv("DLOCKSS_CLUSTER_STORE"); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(dataDir), "cluster_store")
}

func identityPath(dataDir string) string {
	if p := os.Getenv("DLOCKSS_IDENTITY_PATH"); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(dataDir), "dlockss.key")
}

func nodeNamePath(dataDir string) string {
	return filepath.Join(filepath.Dir(dataDir), "node_name")
}

// DefaultTopicName is the archive topic when none is configured.
const DefaultTopicName = "creative-commons"

// Config holds all runtime configuration for a D-LOCKSS node.
type Config struct {
	DiscoveryServiceTag            string
	PubsubTopicPrefix              string
	TopicName                      string
	IngestAllowlist                []string
	FileWatchFolder                string
	ClusterStorePath               string
	MinReplication                 int
	MaxReplication                 int
	CheckInterval                  time.Duration
	MaxPeersPerShard               int
	MinPeersPerShard               int
	MinPeersAcrossSiblings         int
	ShardPeerCheckInterval         time.Duration
	ShardDiscoveryInterval         time.Duration
	ShardSplitRebroadcastInterval  time.Duration
	BootstrapTimeout               time.Duration
	SeenPeersWindow                time.Duration
	PruneStalePeersInterval        time.Duration
	MaxConcurrentReplicationChecks int
	RateLimitWindow                time.Duration
	MaxMessagesPerWindow           int
	InitialBackoffDelay            time.Duration
	MaxBackoffDelay                time.Duration
	BackoffMultiplier              float64
	MetricsReportInterval          time.Duration
	ReplicationCheckCooldown       time.Duration
	RemovedFileCooldown            time.Duration
	MetricsExportPath              string
	BadBitsPath                    string
	ShardOverlapDuration           time.Duration
	OrphanUnpinGracePeriod         time.Duration
	OrphanHandoffGrace             time.Duration
	OrphanUnpinMinHandoffCount     int
	ReplicationVerificationDelay   time.Duration
	DiskUsageHighWaterMark         float64
	IPFSNodeAddress                string
	APIPort                        int
	TrustMode                      string
	TrustStorePath                 string
	SignatureMode                  string
	SignatureMaxAge                time.Duration
	UsePubsubForReplication        bool
	MinShardPeersForPubsubOnly     int
	ReplicationCacheTTL            time.Duration
	AutoReplicationEnabled         bool
	AutoReplicationTimeout         time.Duration
	CRDTOpTimeout                  time.Duration
	FileImportTimeout              time.Duration
	DHTProvideTimeout              time.Duration
	MaxConcurrentDHTProvides       int
	FileProcessingDelay            time.Duration
	FileStabilityDelay             time.Duration
	MaxConcurrentFileProcessing    int
	DHTQueryTimeout                time.Duration
	ReshardDelay                   time.Duration
	ReshardHandoffDelay            time.Duration
	PinReannounceInterval          time.Duration
	NonceSize                      int
	MinNonceSize                   int
	FutureSkewTolerance            time.Duration
	TelemetryInterval              time.Duration
	TelemetryIncludeCIDs           bool
	HeartbeatInterval              time.Duration
	VerboseLogging                 bool
	MergeUpCooldown                time.Duration
	ProbeTimeoutMerge              time.Duration
	SiblingEmptyMergeAfter         time.Duration
	ShardMoveCooldown              time.Duration
	NodeName                       string
	IdentityPath                   string
	NodeNamePath                   string
	IPFSConfigPath                 string
}

// DefaultConfig returns a Config with all hardcoded defaults (no env reads).
// Useful for tests that need a deterministic baseline.
func DefaultConfig() *Config {
	dataDir := "./data"
	return &Config{
		DiscoveryServiceTag:            "dlockss-prod",
		PubsubTopicPrefix:              DefaultPubsubVersion,
		TopicName:                      DefaultTopicName,
		IngestAllowlist:                nil,
		FileWatchFolder:                dataDir,
		ClusterStorePath:               filepath.Join(filepath.Dir(dataDir), "cluster_store"),
		MinReplication:                 5,
		MaxReplication:                 10,
		CheckInterval:                  1 * time.Minute,
		MaxPeersPerShard:               12,
		MinPeersPerShard:               6,
		MinPeersAcrossSiblings:         10,
		ShardPeerCheckInterval:         2 * time.Minute,
		ShardDiscoveryInterval:         2 * time.Minute,
		ShardSplitRebroadcastInterval:  60 * time.Second,
		BootstrapTimeout:               15 * time.Second,
		SeenPeersWindow:                350 * time.Second,
		PruneStalePeersInterval:        10 * time.Minute,
		MaxConcurrentReplicationChecks: 5,
		RateLimitWindow:                1 * time.Minute,
		MaxMessagesPerWindow:           100,
		InitialBackoffDelay:            5 * time.Second,
		MaxBackoffDelay:                5 * time.Minute,
		BackoffMultiplier:              2.0,
		MetricsReportInterval:          5 * time.Second,
		ReplicationCheckCooldown:       1 * time.Minute,
		RemovedFileCooldown:            2 * time.Minute,
		MetricsExportPath:              "",
		BadBitsPath:                    "badBits.csv",
		ShardOverlapDuration:           2 * time.Minute,
		OrphanUnpinGracePeriod:         6 * time.Minute,
		OrphanHandoffGrace:             6 * time.Minute,
		OrphanUnpinMinHandoffCount:     2,
		ReplicationVerificationDelay:   2 * time.Minute,
		DiskUsageHighWaterMark:         90.0,
		IPFSNodeAddress:                "/ip4/127.0.0.1/tcp/5001",
		APIPort:                        5050,
		TrustMode:                      "open",
		TrustStorePath:                 "trusted_peers.json",
		SignatureMode:                  "strict",
		SignatureMaxAge:                10 * time.Minute,
		UsePubsubForReplication:        true,
		MinShardPeersForPubsubOnly:     5,
		ReplicationCacheTTL:            5 * time.Minute,
		AutoReplicationEnabled:         true,
		AutoReplicationTimeout:         5 * time.Minute,
		CRDTOpTimeout:                  10 * time.Minute,
		FileImportTimeout:              2 * time.Minute,
		DHTProvideTimeout:              60 * time.Second,
		MaxConcurrentDHTProvides:       8,
		FileProcessingDelay:            100 * time.Millisecond,
		FileStabilityDelay:             3 * time.Second,
		MaxConcurrentFileProcessing:    5,
		DHTQueryTimeout:                2 * time.Minute,
		ReshardDelay:                   5 * time.Second,
		ReshardHandoffDelay:            3 * time.Second,
		PinReannounceInterval:          2 * time.Minute,
		NonceSize:                      16,
		MinNonceSize:                   8,
		FutureSkewTolerance:            30 * time.Second,
		TelemetryInterval:              30 * time.Second,
		TelemetryIncludeCIDs:           false,
		HeartbeatInterval:              10 * time.Second,
		VerboseLogging:                 false,
		MergeUpCooldown:                2 * time.Minute,
		ProbeTimeoutMerge:              6 * time.Second,
		SiblingEmptyMergeAfter:         5 * time.Minute,
		ShardMoveCooldown:              30 * time.Second,
		NodeName:                       "",
		IdentityPath:                   filepath.Join(filepath.Dir(dataDir), "dlockss.key"),
		NodeNamePath:                   filepath.Join(filepath.Dir(dataDir), "node_name"),
		IPFSConfigPath:                 "",
	}
}

// LoadFromEnv creates a Config by reading environment variables, falling back
// to hardcoded defaults for any variable that is not set.
func LoadFromEnv() *Config {
	dataDir := getEnvString("DLOCKSS_DATA_DIR", "./data")
	return &Config{
		DiscoveryServiceTag:            getEnvString("DLOCKSS_DISCOVERY_TAG", "dlockss-prod"),
		PubsubTopicPrefix:              getEnvString("DLOCKSS_PUBSUB_TOPIC_PREFIX", DefaultPubsubVersion),
		TopicName:                      getEnvString("DLOCKSS_TOPIC_NAME", DefaultTopicName),
		IngestAllowlist:                getEnvStringSlice("DLOCKSS_INGEST_ALLOWLIST"),
		FileWatchFolder:                dataDir,
		ClusterStorePath:               clusterStorePath(dataDir),
		MinReplication:                 getEnvInt("DLOCKSS_MIN_REPLICATION", 5),
		MaxReplication:                 getEnvInt("DLOCKSS_MAX_REPLICATION", 10),
		CheckInterval:                  getEnvDuration("DLOCKSS_CHECK_INTERVAL", 1*time.Minute),
		MaxPeersPerShard:               getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", 12),
		MinPeersPerShard:               getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", 6),
		MinPeersAcrossSiblings:         getEnvInt("DLOCKSS_MIN_PEERS_ACROSS_SIBLINGS", 10),
		ShardPeerCheckInterval:         getEnvDuration("DLOCKSS_SHARD_PEER_CHECK_INTERVAL", 2*time.Minute),
		ShardDiscoveryInterval:         getEnvDuration("DLOCKSS_SHARD_DISCOVERY_INTERVAL", 2*time.Minute),
		ShardSplitRebroadcastInterval:  getEnvDuration("DLOCKSS_SHARD_SPLIT_REBROADCAST_INTERVAL", 60*time.Second),
		BootstrapTimeout:               getEnvDuration("DLOCKSS_BOOTSTRAP_TIMEOUT", 15*time.Second),
		SeenPeersWindow:                getEnvDuration("DLOCKSS_SEEN_PEERS_WINDOW", 350*time.Second),
		PruneStalePeersInterval:        getEnvDuration("DLOCKSS_PRUNE_STALE_PEERS_INTERVAL", 10*time.Minute),
		MaxConcurrentReplicationChecks: getEnvInt("DLOCKSS_MAX_CONCURRENT_CHECKS", 5),
		RateLimitWindow:                getEnvDuration("DLOCKSS_RATE_LIMIT_WINDOW", 1*time.Minute),
		MaxMessagesPerWindow:           getEnvInt("DLOCKSS_MAX_MESSAGES_PER_WINDOW", 100),
		InitialBackoffDelay:            getEnvDuration("DLOCKSS_INITIAL_BACKOFF", 5*time.Second),
		MaxBackoffDelay:                getEnvDuration("DLOCKSS_MAX_BACKOFF", 5*time.Minute),
		BackoffMultiplier:              getEnvFloat("DLOCKSS_BACKOFF_MULTIPLIER", 2.0),
		MetricsReportInterval:          getEnvDuration("DLOCKSS_METRICS_INTERVAL", 5*time.Second),
		ReplicationCheckCooldown:       getEnvDuration("DLOCKSS_REPLICATION_COOLDOWN", 1*time.Minute),
		RemovedFileCooldown:            getEnvDuration("DLOCKSS_REMOVED_COOLDOWN", 2*time.Minute),
		MetricsExportPath:              getEnvString("DLOCKSS_METRICS_EXPORT", ""),
		BadBitsPath:                    getEnvString("DLOCKSS_BADBITS_PATH", "badBits.csv"),
		ShardOverlapDuration:           getEnvDuration("DLOCKSS_SHARD_OVERLAP_DURATION", 2*time.Minute),
		OrphanUnpinGracePeriod:         getEnvDuration("DLOCKSS_ORPHAN_UNPIN_GRACE", 6*time.Minute),
		OrphanHandoffGrace:             getEnvDuration("DLOCKSS_ORPHAN_HANDOFF_GRACE", 6*time.Minute),
		OrphanUnpinMinHandoffCount:     getEnvInt("DLOCKSS_ORPHAN_MIN_HANDOFF_COUNT", 2),
		ReplicationVerificationDelay:   getEnvDuration("DLOCKSS_REPLICATION_VERIFICATION_DELAY", 2*time.Minute),
		DiskUsageHighWaterMark:         getEnvFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", 90.0),
		IPFSNodeAddress:                getEnvString("DLOCKSS_IPFS_NODE", "/ip4/127.0.0.1/tcp/5001"),
		APIPort:                        getEnvInt("DLOCKSS_API_PORT", 5050),
		TrustMode:                      getEnvString("DLOCKSS_TRUST_MODE", "open"),
		TrustStorePath:                 getEnvString("DLOCKSS_TRUST_STORE", "trusted_peers.json"),
		SignatureMode:                  getEnvString("DLOCKSS_SIGNATURE_MODE", "strict"),
		SignatureMaxAge:                getEnvDuration("DLOCKSS_SIGNATURE_MAX_AGE", 10*time.Minute),
		UsePubsubForReplication:        getEnvBool("DLOCKSS_USE_PUBSUB_FOR_REPLICATION", true),
		MinShardPeersForPubsubOnly:     getEnvInt("DLOCKSS_MIN_SHARD_PEERS_PUBSUB_ONLY", 5),
		ReplicationCacheTTL:            getEnvDuration("DLOCKSS_REPLICATION_CACHE_TTL", 5*time.Minute),
		AutoReplicationEnabled:         getEnvBool("DLOCKSS_AUTO_REPLICATION_ENABLED", true),
		AutoReplicationTimeout:         getEnvDuration("DLOCKSS_AUTO_REPLICATION_TIMEOUT", 5*time.Minute),
		CRDTOpTimeout:                  getEnvDuration("DLOCKSS_CRDT_OP_TIMEOUT", 10*time.Minute),
		FileImportTimeout:              getEnvDuration("DLOCKSS_FILE_IMPORT_TIMEOUT", 2*time.Minute),
		DHTProvideTimeout:              getEnvDuration("DLOCKSS_DHT_PROVIDE_TIMEOUT", 60*time.Second),
		MaxConcurrentDHTProvides:       getEnvInt("DLOCKSS_MAX_CONCURRENT_DHT_PROVIDES", 8),
		FileProcessingDelay:            getEnvDuration("DLOCKSS_FILE_PROCESSING_DELAY", 100*time.Millisecond),
		FileStabilityDelay:             getEnvDuration("DLOCKSS_FILE_STABILITY_DELAY", 3*time.Second),
		MaxConcurrentFileProcessing:    getEnvInt("DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING", 5),
		DHTQueryTimeout:                getEnvDuration("DLOCKSS_DHT_QUERY_TIMEOUT", 2*time.Minute),
		ReshardDelay:                   getEnvDuration("DLOCKSS_RESHARD_DELAY", 5*time.Second),
		ReshardHandoffDelay:            getEnvDuration("DLOCKSS_RESHARD_HANDOFF_DELAY", 3*time.Second),
		PinReannounceInterval:          getEnvDuration("DLOCKSS_PIN_REANNOUNCE_INTERVAL", 2*time.Minute),
		NonceSize:                      getEnvInt("DLOCKSS_NONCE_SIZE", 16),
		MinNonceSize:                   getEnvInt("DLOCKSS_MIN_NONCE_SIZE", 8),
		FutureSkewTolerance:            getEnvDuration("DLOCKSS_FUTURE_SKEW_TOLERANCE", 30*time.Second),
		TelemetryInterval:              getEnvDuration("DLOCKSS_TELEMETRY_INTERVAL", 30*time.Second),
		TelemetryIncludeCIDs:           getEnvBool("DLOCKSS_TELEMETRY_INCLUDE_CIDS", false),
		HeartbeatInterval:              getEnvDuration("DLOCKSS_HEARTBEAT_INTERVAL", 10*time.Second),
		VerboseLogging:                 getEnvBool("DLOCKSS_VERBOSE_LOGGING", false),
		MergeUpCooldown:                getEnvDuration("DLOCKSS_MERGE_UP_COOLDOWN", 2*time.Minute),
		ProbeTimeoutMerge:              getEnvDuration("DLOCKSS_PROBE_TIMEOUT_MERGE", 6*time.Second),
		SiblingEmptyMergeAfter:         getEnvDuration("DLOCKSS_SIBLING_EMPTY_MERGE_AFTER", 5*time.Minute),
		ShardMoveCooldown:              getEnvDuration("DLOCKSS_SHARD_MOVE_COOLDOWN", 30*time.Second),
		NodeName:                       getEnvString("DLOCKSS_NODE_NAME", ""),
		IdentityPath:                   identityPath(dataDir),
		NodeNamePath:                   nodeNamePath(dataDir),
		IPFSConfigPath:                 getEnvString("DLOCKSS_IPFS_CONFIG", ""),
	}
}

// Validate checks and corrects invalid configuration values.
func (c *Config) Validate() {
	if c.SignatureMode != "off" && c.SignatureMode != "warn" && c.SignatureMode != "strict" {
		slog.Warn("unknown signature mode, defaulting to strict", "mode", c.SignatureMode)
		c.SignatureMode = "strict"
	}
	if c.MaxConcurrentFileProcessing < 1 {
		slog.Warn("invalid config value, using default", "key", "MaxConcurrentFileProcessing", "value", c.MaxConcurrentFileProcessing, "default", 5)
		c.MaxConcurrentFileProcessing = 5
	}
	if c.NonceSize < 1 {
		slog.Warn("invalid config value, using default", "key", "NonceSize", "value", c.NonceSize, "default", 16)
		c.NonceSize = 16
	}
	if c.MinNonceSize < 1 {
		slog.Warn("invalid config value, using default", "key", "MinNonceSize", "value", c.MinNonceSize, "default", 8)
		c.MinNonceSize = 8
	}
	if c.MinReplication > c.MaxReplication {
		slog.Warn("MinReplication > MaxReplication, swapping", "min", c.MinReplication, "max", c.MaxReplication)
		c.MinReplication, c.MaxReplication = c.MaxReplication, c.MinReplication
	}
	if c.MaxConcurrentReplicationChecks < 1 {
		slog.Warn("invalid config value, using default", "key", "MaxConcurrentReplicationChecks", "value", c.MaxConcurrentReplicationChecks, "default", 5)
		c.MaxConcurrentReplicationChecks = 5
	}
	if c.DiskUsageHighWaterMark <= 0 || c.DiskUsageHighWaterMark > 100 {
		slog.Warn("disk usage high water mark out of range, using default", "value", c.DiskUsageHighWaterMark, "default", 90.0)
		c.DiskUsageHighWaterMark = 90.0
	}
}

// ValidatePathSafetyCheck checks that state files do not reside inside the
// ingest directory. Returns a non-empty error message listing offenders.
func (c *Config) ValidatePathSafetyCheck() string {
	checks := []struct {
		label string
		path  string
	}{
		{"IdentityPath (DLOCKSS_IDENTITY_PATH)", c.IdentityPath},
		{"NodeNamePath", c.NodeNamePath},
		{"IPFSConfigPath (DLOCKSS_IPFS_CONFIG)", c.IPFSConfigPath},
		{"ClusterStorePath (DLOCKSS_CLUSTER_STORE)", c.ClusterStorePath},
	}
	var problems []string
	for _, ck := range checks {
		if isInsideDir(ck.path, c.FileWatchFolder) {
			problems = append(problems, fmt.Sprintf("  %s = %s", ck.label, ck.path))
		}
	}
	if len(problems) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"the following state paths resolve inside the ingest directory (%s) and would be ingested as data:\n%s\n"+
			"Set DLOCKSS_DATA_DIR to a dedicated subdirectory (e.g. ./data) or override the conflicting paths.",
		c.FileWatchFolder, strings.Join(problems, "\n"))
}

// Log prints all configuration values after calling Validate.
func (c *Config) Log() {
	c.Validate()

	ingestMode := "open"
	if len(c.IngestAllowlist) > 0 {
		ingestMode = fmt.Sprintf("allowlist (%d peers)", len(c.IngestAllowlist))
	}
	slog.Info("config: network",
		"discovery_tag", c.DiscoveryServiceTag,
		"pubsub_prefix", c.PubsubTopicPrefix,
		"topic_name", c.TopicName,
		"ingest_mode", ingestMode,
		"ipfs_node", c.IPFSNodeAddress,
		"api_port", c.APIPort,
		"bootstrap_timeout", c.BootstrapTimeout,
	)
	slog.Info("config: paths",
		"data_dir", c.FileWatchFolder,
		"cluster_store", c.ClusterStorePath,
		"identity", c.IdentityPath,
		"badbits", c.BadBitsPath,
		"trust_store", c.TrustStorePath,
		"metrics_export", c.MetricsExportPath,
	)
	slog.Info("config: sharding",
		"max_peers", c.MaxPeersPerShard,
		"min_peers", c.MinPeersPerShard,
		"min_across_siblings", c.MinPeersAcrossSiblings,
		"peer_check_interval", c.ShardPeerCheckInterval,
		"discovery_interval", c.ShardDiscoveryInterval,
		"split_rebroadcast", c.ShardSplitRebroadcastInterval,
		"seen_peers_window", c.SeenPeersWindow,
		"prune_stale_interval", c.PruneStalePeersInterval,
		"overlap_duration", c.ShardOverlapDuration,
		"move_cooldown", c.ShardMoveCooldown,
		"merge_up_cooldown", c.MergeUpCooldown,
		"probe_timeout_merge", c.ProbeTimeoutMerge,
		"sibling_empty_merge_after", c.SiblingEmptyMergeAfter,
	)
	slog.Info("config: replication",
		"min", c.MinReplication,
		"max", c.MaxReplication,
		"check_interval", c.CheckInterval,
		"max_concurrent_checks", c.MaxConcurrentReplicationChecks,
		"cooldown", c.ReplicationCheckCooldown,
		"removed_cooldown", c.RemovedFileCooldown,
		"verification_delay", c.ReplicationVerificationDelay,
		"use_pubsub", c.UsePubsubForReplication,
		"min_pubsub_peers", c.MinShardPeersForPubsubOnly,
		"cache_ttl", c.ReplicationCacheTTL,
		"auto_enabled", c.AutoReplicationEnabled,
		"auto_timeout", c.AutoReplicationTimeout,
		"crdt_op_timeout", c.CRDTOpTimeout,
		"pin_reannounce", c.PinReannounceInterval,
	)
	slog.Info("config: files",
		"import_timeout", c.FileImportTimeout,
		"dht_provide_timeout", c.DHTProvideTimeout,
		"max_concurrent_dht_provides", c.MaxConcurrentDHTProvides,
		"processing_delay", c.FileProcessingDelay,
		"stability_delay", c.FileStabilityDelay,
		"max_concurrent", c.MaxConcurrentFileProcessing,
		"dht_query_timeout", c.DHTQueryTimeout,
		"reshard_delay", c.ReshardDelay,
		"reshard_handoff_delay", c.ReshardHandoffDelay,
	)
	slog.Info("config: orphan",
		"unpin_grace", c.OrphanUnpinGracePeriod,
		"handoff_grace", c.OrphanHandoffGrace,
		"min_handoff_count", c.OrphanUnpinMinHandoffCount,
	)
	slog.Info("config: security",
		"trust_mode", c.TrustMode,
		"signature_mode", c.SignatureMode,
		"signature_max_age", c.SignatureMaxAge,
		"nonce_size", c.NonceSize,
		"min_nonce_size", c.MinNonceSize,
		"future_skew_tolerance", c.FutureSkewTolerance,
	)
	heartbeat := "auto"
	if c.HeartbeatInterval > 0 {
		heartbeat = c.HeartbeatInterval.String()
	}
	slog.Info("config: telemetry",
		"metrics_interval", c.MetricsReportInterval,
		"telemetry_interval", c.TelemetryInterval,
		"include_cids", c.TelemetryIncludeCIDs,
		"heartbeat_interval", heartbeat,
		"verbose", c.VerboseLogging,
	)
	slog.Info("config: rate limiting",
		"window", c.RateLimitWindow,
		"max_messages", c.MaxMessagesPerWindow,
	)
	slog.Info("config: backoff",
		"initial", c.InitialBackoffDelay,
		"max", c.MaxBackoffDelay,
		"multiplier", c.BackoffMultiplier,
	)
	slog.Info("config: storage",
		"disk_high_water_mark", c.DiskUsageHighWaterMark,
	)
}

// isInsideDir reports whether path resolves to a location inside dir.
func isInsideDir(path, dir string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return false
	}
	return !strings.HasPrefix(rel, "..")
}
