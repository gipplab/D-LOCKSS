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

// ShardingConfig holds parameters that govern shard splitting, merging, and peer tracking.
type ShardingConfig struct {
	MaxPeersPerShard              int
	MinPeersPerShard              int
	MinPeersAcrossSiblings        int
	ShardPeerCheckInterval        time.Duration
	ShardDiscoveryInterval        time.Duration
	ShardSplitRebroadcastInterval time.Duration
	SeenPeersWindow               time.Duration
	PruneStalePeersInterval       time.Duration
	ShardOverlapDuration          time.Duration
	ShardMoveCooldown             time.Duration
	MergeUpCooldown               time.Duration
	ProbeTimeoutMerge             time.Duration
	SiblingEmptyMergeAfter        time.Duration
}

// ReplicationConfig holds parameters for content replication across peers.
type ReplicationConfig struct {
	MinReplication                 int
	MaxReplication                 int
	CheckInterval                  time.Duration
	MaxConcurrentReplicationChecks int
	AutoReplicationEnabled         bool
	AutoReplicationTimeout         time.Duration
	PinReannounceInterval          time.Duration
}

// FileConfig holds parameters for file ingestion and DHT operations.
type FileConfig struct {
	FileImportTimeout           time.Duration
	DHTProvideTimeout           time.Duration
	MaxConcurrentDHTProvides    int
	FileProcessingDelay         time.Duration
	FileStabilityDelay          time.Duration
	MaxConcurrentFileProcessing int
	ReshardDelay                time.Duration
	ReshardHandoffDelay         time.Duration
}

// SecurityConfig holds trust, signing, and nonce parameters.
type SecurityConfig struct {
	TrustMode           string
	TrustStorePath      string
	SignatureMode       string
	SignatureMaxAge     time.Duration
	NonceSize           int
	MinNonceSize        int
	FutureSkewTolerance time.Duration
}

// OrphanConfig holds parameters for orphan file detection and cleanup.
type OrphanConfig struct {
	UnpinGracePeriod   time.Duration
	HandoffGrace       time.Duration
	UnpinMinHandoffCnt int
}

// Config holds all runtime configuration for a D-LOCKSS node.
type Config struct {
	DiscoveryServiceTag    string
	PubsubTopicPrefix      string
	TopicName              string
	IngestAllowlist        []string
	FileWatchFolder        string
	ClusterStorePath       string
	BadBitsPath            string
	IPFSNodeAddress        string
	APIPort                int
	BootstrapTimeout       time.Duration
	HeartbeatInterval      time.Duration
	VerboseLogging         bool
	RateLimitWindow        time.Duration
	MaxMessagesPerWindow   int
	DiskUsageHighWaterMark float64
	NodeName               string
	IdentityPath           string
	NodeNamePath           string
	IPFSConfigPath         string

	Sharding    ShardingConfig
	Replication ReplicationConfig
	Files       FileConfig
	Security    SecurityConfig
	Orphan      OrphanConfig
}

// DefaultConfig returns a Config with all hardcoded defaults (no env reads).
// Useful for tests that need a deterministic baseline.
func DefaultConfig() *Config {
	dataDir := "./data"
	return &Config{
		DiscoveryServiceTag:    "dlockss-prod",
		PubsubTopicPrefix:      DefaultPubsubVersion,
		TopicName:              DefaultTopicName,
		FileWatchFolder:        dataDir,
		ClusterStorePath:       filepath.Join(filepath.Dir(dataDir), "cluster_store"),
		BadBitsPath:            "badBits.csv",
		IPFSNodeAddress:        "/ip4/127.0.0.1/tcp/5001",
		APIPort:                5050,
		BootstrapTimeout:       15 * time.Second,
		HeartbeatInterval:      10 * time.Second,
		RateLimitWindow:        1 * time.Minute,
		MaxMessagesPerWindow:   100,
		DiskUsageHighWaterMark: 90.0,
		IdentityPath:           filepath.Join(filepath.Dir(dataDir), "dlockss.key"),
		NodeNamePath:           filepath.Join(filepath.Dir(dataDir), "node_name"),

		Sharding: ShardingConfig{
			MaxPeersPerShard:              12,
			MinPeersPerShard:              6,
			MinPeersAcrossSiblings:        10,
			ShardPeerCheckInterval:        2 * time.Minute,
			ShardDiscoveryInterval:        2 * time.Minute,
			ShardSplitRebroadcastInterval: 60 * time.Second,
			SeenPeersWindow:               350 * time.Second,
			PruneStalePeersInterval:       10 * time.Minute,
			ShardOverlapDuration:          2 * time.Minute,
			ShardMoveCooldown:             30 * time.Second,
			MergeUpCooldown:               2 * time.Minute,
			ProbeTimeoutMerge:             6 * time.Second,
			SiblingEmptyMergeAfter:        5 * time.Minute,
		},
		Replication: ReplicationConfig{
			MinReplication:                 5,
			MaxReplication:                 10,
			CheckInterval:                  1 * time.Minute,
			MaxConcurrentReplicationChecks: 5,
			AutoReplicationEnabled:         true,
			AutoReplicationTimeout:         5 * time.Minute,
			PinReannounceInterval:          2 * time.Minute,
		},
		Files: FileConfig{
			FileImportTimeout:           2 * time.Minute,
			DHTProvideTimeout:           60 * time.Second,
			MaxConcurrentDHTProvides:    8,
			FileProcessingDelay:         100 * time.Millisecond,
			FileStabilityDelay:          3 * time.Second,
			MaxConcurrentFileProcessing: 5,
			ReshardDelay:                5 * time.Second,
			ReshardHandoffDelay:         3 * time.Second,
		},
		Security: SecurityConfig{
			TrustMode:           "open",
			TrustStorePath:      "trusted_peers.json",
			SignatureMode:       "strict",
			SignatureMaxAge:     10 * time.Minute,
			NonceSize:           16,
			MinNonceSize:        8,
			FutureSkewTolerance: 30 * time.Second,
		},
		Orphan: OrphanConfig{
			UnpinGracePeriod:   6 * time.Minute,
			HandoffGrace:       6 * time.Minute,
			UnpinMinHandoffCnt: 2,
		},
	}
}

// LoadFromEnv creates a Config by reading environment variables, falling back
// to hardcoded defaults for any variable that is not set.
func LoadFromEnv() *Config {
	dataDir := getEnvString("DLOCKSS_DATA_DIR", "./data")

	cfg := DefaultConfig()

	cfg.DiscoveryServiceTag = getEnvString("DLOCKSS_DISCOVERY_TAG", cfg.DiscoveryServiceTag)
	cfg.PubsubTopicPrefix = getEnvString("DLOCKSS_PUBSUB_TOPIC_PREFIX", cfg.PubsubTopicPrefix)
	cfg.TopicName = getEnvString("DLOCKSS_TOPIC_NAME", cfg.TopicName)
	cfg.IngestAllowlist = getEnvStringSlice("DLOCKSS_INGEST_ALLOWLIST")
	cfg.FileWatchFolder = dataDir
	cfg.ClusterStorePath = clusterStorePath(dataDir)
	cfg.BadBitsPath = getEnvString("DLOCKSS_BADBITS_PATH", cfg.BadBitsPath)
	cfg.IPFSNodeAddress = getEnvString("DLOCKSS_IPFS_NODE", cfg.IPFSNodeAddress)
	cfg.APIPort = getEnvInt("DLOCKSS_API_PORT", cfg.APIPort)
	cfg.BootstrapTimeout = getEnvDuration("DLOCKSS_BOOTSTRAP_TIMEOUT", cfg.BootstrapTimeout)
	cfg.HeartbeatInterval = getEnvDuration("DLOCKSS_HEARTBEAT_INTERVAL", cfg.HeartbeatInterval)
	cfg.VerboseLogging = getEnvBool("DLOCKSS_VERBOSE_LOGGING", cfg.VerboseLogging)
	cfg.RateLimitWindow = getEnvDuration("DLOCKSS_RATE_LIMIT_WINDOW", cfg.RateLimitWindow)
	cfg.MaxMessagesPerWindow = getEnvInt("DLOCKSS_MAX_MESSAGES_PER_WINDOW", cfg.MaxMessagesPerWindow)
	cfg.DiskUsageHighWaterMark = getEnvFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", cfg.DiskUsageHighWaterMark)
	cfg.NodeName = getEnvString("DLOCKSS_NODE_NAME", cfg.NodeName)
	cfg.IdentityPath = identityPath(dataDir)
	cfg.NodeNamePath = nodeNamePath(dataDir)
	cfg.IPFSConfigPath = getEnvString("DLOCKSS_IPFS_CONFIG", cfg.IPFSConfigPath)

	// Sharding
	cfg.Sharding.MaxPeersPerShard = getEnvInt("DLOCKSS_MAX_PEERS_PER_SHARD", cfg.Sharding.MaxPeersPerShard)
	cfg.Sharding.MinPeersPerShard = getEnvInt("DLOCKSS_MIN_PEERS_PER_SHARD", cfg.Sharding.MinPeersPerShard)
	cfg.Sharding.MinPeersAcrossSiblings = getEnvInt("DLOCKSS_MIN_PEERS_ACROSS_SIBLINGS", cfg.Sharding.MinPeersAcrossSiblings)
	cfg.Sharding.ShardPeerCheckInterval = getEnvDuration("DLOCKSS_SHARD_PEER_CHECK_INTERVAL", cfg.Sharding.ShardPeerCheckInterval)
	cfg.Sharding.ShardDiscoveryInterval = getEnvDuration("DLOCKSS_SHARD_DISCOVERY_INTERVAL", cfg.Sharding.ShardDiscoveryInterval)
	cfg.Sharding.ShardSplitRebroadcastInterval = getEnvDuration("DLOCKSS_SHARD_SPLIT_REBROADCAST_INTERVAL", cfg.Sharding.ShardSplitRebroadcastInterval)
	cfg.Sharding.SeenPeersWindow = getEnvDuration("DLOCKSS_SEEN_PEERS_WINDOW", cfg.Sharding.SeenPeersWindow)
	cfg.Sharding.PruneStalePeersInterval = getEnvDuration("DLOCKSS_PRUNE_STALE_PEERS_INTERVAL", cfg.Sharding.PruneStalePeersInterval)
	cfg.Sharding.ShardOverlapDuration = getEnvDuration("DLOCKSS_SHARD_OVERLAP_DURATION", cfg.Sharding.ShardOverlapDuration)
	cfg.Sharding.ShardMoveCooldown = getEnvDuration("DLOCKSS_SHARD_MOVE_COOLDOWN", cfg.Sharding.ShardMoveCooldown)
	cfg.Sharding.MergeUpCooldown = getEnvDuration("DLOCKSS_MERGE_UP_COOLDOWN", cfg.Sharding.MergeUpCooldown)
	cfg.Sharding.ProbeTimeoutMerge = getEnvDuration("DLOCKSS_PROBE_TIMEOUT_MERGE", cfg.Sharding.ProbeTimeoutMerge)
	cfg.Sharding.SiblingEmptyMergeAfter = getEnvDuration("DLOCKSS_SIBLING_EMPTY_MERGE_AFTER", cfg.Sharding.SiblingEmptyMergeAfter)

	// Replication
	cfg.Replication.MinReplication = getEnvInt("DLOCKSS_MIN_REPLICATION", cfg.Replication.MinReplication)
	cfg.Replication.MaxReplication = getEnvInt("DLOCKSS_MAX_REPLICATION", cfg.Replication.MaxReplication)
	cfg.Replication.CheckInterval = getEnvDuration("DLOCKSS_CHECK_INTERVAL", cfg.Replication.CheckInterval)
	cfg.Replication.MaxConcurrentReplicationChecks = getEnvInt("DLOCKSS_MAX_CONCURRENT_CHECKS", cfg.Replication.MaxConcurrentReplicationChecks)
	cfg.Replication.AutoReplicationEnabled = getEnvBool("DLOCKSS_AUTO_REPLICATION_ENABLED", cfg.Replication.AutoReplicationEnabled)
	cfg.Replication.AutoReplicationTimeout = getEnvDuration("DLOCKSS_AUTO_REPLICATION_TIMEOUT", cfg.Replication.AutoReplicationTimeout)
	cfg.Replication.PinReannounceInterval = getEnvDuration("DLOCKSS_PIN_REANNOUNCE_INTERVAL", cfg.Replication.PinReannounceInterval)

	// Files
	cfg.Files.FileImportTimeout = getEnvDuration("DLOCKSS_FILE_IMPORT_TIMEOUT", cfg.Files.FileImportTimeout)
	cfg.Files.DHTProvideTimeout = getEnvDuration("DLOCKSS_DHT_PROVIDE_TIMEOUT", cfg.Files.DHTProvideTimeout)
	cfg.Files.MaxConcurrentDHTProvides = getEnvInt("DLOCKSS_MAX_CONCURRENT_DHT_PROVIDES", cfg.Files.MaxConcurrentDHTProvides)
	cfg.Files.FileProcessingDelay = getEnvDuration("DLOCKSS_FILE_PROCESSING_DELAY", cfg.Files.FileProcessingDelay)
	cfg.Files.FileStabilityDelay = getEnvDuration("DLOCKSS_FILE_STABILITY_DELAY", cfg.Files.FileStabilityDelay)
	cfg.Files.MaxConcurrentFileProcessing = getEnvInt("DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING", cfg.Files.MaxConcurrentFileProcessing)
	cfg.Files.ReshardDelay = getEnvDuration("DLOCKSS_RESHARD_DELAY", cfg.Files.ReshardDelay)
	cfg.Files.ReshardHandoffDelay = getEnvDuration("DLOCKSS_RESHARD_HANDOFF_DELAY", cfg.Files.ReshardHandoffDelay)

	// Security
	cfg.Security.TrustMode = getEnvString("DLOCKSS_TRUST_MODE", cfg.Security.TrustMode)
	cfg.Security.TrustStorePath = getEnvString("DLOCKSS_TRUST_STORE", cfg.Security.TrustStorePath)
	cfg.Security.SignatureMode = getEnvString("DLOCKSS_SIGNATURE_MODE", cfg.Security.SignatureMode)
	cfg.Security.SignatureMaxAge = getEnvDuration("DLOCKSS_SIGNATURE_MAX_AGE", cfg.Security.SignatureMaxAge)
	cfg.Security.NonceSize = getEnvInt("DLOCKSS_NONCE_SIZE", cfg.Security.NonceSize)
	cfg.Security.MinNonceSize = getEnvInt("DLOCKSS_MIN_NONCE_SIZE", cfg.Security.MinNonceSize)
	cfg.Security.FutureSkewTolerance = getEnvDuration("DLOCKSS_FUTURE_SKEW_TOLERANCE", cfg.Security.FutureSkewTolerance)

	// Orphan
	cfg.Orphan.UnpinGracePeriod = getEnvDuration("DLOCKSS_ORPHAN_UNPIN_GRACE", cfg.Orphan.UnpinGracePeriod)
	cfg.Orphan.HandoffGrace = getEnvDuration("DLOCKSS_ORPHAN_HANDOFF_GRACE", cfg.Orphan.HandoffGrace)
	cfg.Orphan.UnpinMinHandoffCnt = getEnvInt("DLOCKSS_ORPHAN_MIN_HANDOFF_COUNT", cfg.Orphan.UnpinMinHandoffCnt)

	return cfg
}

// Validate checks and corrects invalid configuration values.
func (c *Config) Validate() {
	if c.Security.SignatureMode != "off" && c.Security.SignatureMode != "warn" && c.Security.SignatureMode != "strict" {
		slog.Warn("unknown signature mode, defaulting to strict", "mode", c.Security.SignatureMode)
		c.Security.SignatureMode = "strict"
	}
	if c.Files.MaxConcurrentFileProcessing < 1 {
		slog.Warn("invalid config value, using default", "key", "MaxConcurrentFileProcessing", "value", c.Files.MaxConcurrentFileProcessing, "default", 5)
		c.Files.MaxConcurrentFileProcessing = 5
	}
	if c.Security.NonceSize < 1 {
		slog.Warn("invalid config value, using default", "key", "NonceSize", "value", c.Security.NonceSize, "default", 16)
		c.Security.NonceSize = 16
	}
	if c.Security.MinNonceSize < 1 {
		slog.Warn("invalid config value, using default", "key", "MinNonceSize", "value", c.Security.MinNonceSize, "default", 8)
		c.Security.MinNonceSize = 8
	}
	if c.Replication.MinReplication > c.Replication.MaxReplication {
		slog.Warn("MinReplication > MaxReplication, swapping", "min", c.Replication.MinReplication, "max", c.Replication.MaxReplication)
		c.Replication.MinReplication, c.Replication.MaxReplication = c.Replication.MaxReplication, c.Replication.MinReplication
	}
	if c.Replication.MaxConcurrentReplicationChecks < 1 {
		slog.Warn("invalid config value, using default", "key", "MaxConcurrentReplicationChecks", "value", c.Replication.MaxConcurrentReplicationChecks, "default", 5)
		c.Replication.MaxConcurrentReplicationChecks = 5
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
		"trust_store", c.Security.TrustStorePath,
	)
	slog.Info("config: sharding",
		"max_peers", c.Sharding.MaxPeersPerShard,
		"min_peers", c.Sharding.MinPeersPerShard,
		"min_across_siblings", c.Sharding.MinPeersAcrossSiblings,
		"peer_check_interval", c.Sharding.ShardPeerCheckInterval,
		"discovery_interval", c.Sharding.ShardDiscoveryInterval,
		"split_rebroadcast", c.Sharding.ShardSplitRebroadcastInterval,
		"seen_peers_window", c.Sharding.SeenPeersWindow,
		"prune_stale_interval", c.Sharding.PruneStalePeersInterval,
		"overlap_duration", c.Sharding.ShardOverlapDuration,
		"move_cooldown", c.Sharding.ShardMoveCooldown,
		"merge_up_cooldown", c.Sharding.MergeUpCooldown,
		"probe_timeout_merge", c.Sharding.ProbeTimeoutMerge,
		"sibling_empty_merge_after", c.Sharding.SiblingEmptyMergeAfter,
	)
	slog.Info("config: replication",
		"min", c.Replication.MinReplication,
		"max", c.Replication.MaxReplication,
		"check_interval", c.Replication.CheckInterval,
		"max_concurrent_checks", c.Replication.MaxConcurrentReplicationChecks,
		"auto_enabled", c.Replication.AutoReplicationEnabled,
		"auto_timeout", c.Replication.AutoReplicationTimeout,
		"pin_reannounce", c.Replication.PinReannounceInterval,
	)
	slog.Info("config: files",
		"import_timeout", c.Files.FileImportTimeout,
		"dht_provide_timeout", c.Files.DHTProvideTimeout,
		"max_concurrent_dht_provides", c.Files.MaxConcurrentDHTProvides,
		"processing_delay", c.Files.FileProcessingDelay,
		"stability_delay", c.Files.FileStabilityDelay,
		"max_concurrent", c.Files.MaxConcurrentFileProcessing,
		"reshard_delay", c.Files.ReshardDelay,
		"reshard_handoff_delay", c.Files.ReshardHandoffDelay,
	)
	slog.Info("config: orphan",
		"unpin_grace", c.Orphan.UnpinGracePeriod,
		"handoff_grace", c.Orphan.HandoffGrace,
		"min_handoff_count", c.Orphan.UnpinMinHandoffCnt,
	)
	slog.Info("config: security",
		"trust_mode", c.Security.TrustMode,
		"signature_mode", c.Security.SignatureMode,
		"signature_max_age", c.Security.SignatureMaxAge,
		"nonce_size", c.Security.NonceSize,
		"min_nonce_size", c.Security.MinNonceSize,
		"future_skew_tolerance", c.Security.FutureSkewTolerance,
	)
	heartbeat := "auto"
	if c.HeartbeatInterval > 0 {
		heartbeat = c.HeartbeatInterval.String()
	}
	slog.Info("config: heartbeat",
		"heartbeat_interval", heartbeat,
		"verbose", c.VerboseLogging,
	)
	slog.Info("config: rate limiting",
		"window", c.RateLimitWindow,
		"max_messages", c.MaxMessagesPerWindow,
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
