// Package config loads the node's operator-facing configuration from the
// environment. Only knobs an operator genuinely sets are configurable;
// protocol timings, concurrency bounds, and security parameters are frozen
// constants in their packages. Invalid values are logged and replaced by the
// default, never fatal.
package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultPubsubVersion is the pubsub topic prefix. It is bumped on breaking
// protocol changes so incompatible versions never share topics.
const DefaultPubsubVersion = "dlockss-v0.0.3"

// DefaultTopicName is the archive (network) name within the topic namespace.
const DefaultTopicName = "creative-commons"

// Config is the node configuration.
type Config struct {
	// Network identity and namespace.
	DiscoveryServiceTag string // DLOCKSS_DISCOVERY_TAG
	PubsubTopicPrefix   string // DLOCKSS_PUBSUB_TOPIC_PREFIX
	TopicName           string // DLOCKSS_TOPIC_NAME
	PrivateNetwork      bool   // DLOCKSS_PRIVATE_NETWORK: skip public bootstrap/NAT/relay
	DHTClient           bool   // DLOCKSS_DHT_CLIENT: rendezvous DHT in client mode

	// Paths.
	StateDir         string // DLOCKSS_STATE_DIR: identity, node name, pin sets, trust store
	ClusterStorePath string // DLOCKSS_CLUSTER_STORE (pin sets)
	BadBitsPath      string // DLOCKSS_BADBITS_PATH
	IdentityPath     string // DLOCKSS_IDENTITY_PATH
	NodeNamePath     string // derived: <parent>/node_name
	IPFSConfigPath   string // DLOCKSS_IPFS_CONFIG (share Kubo's identity)
	TrustStorePath   string // DLOCKSS_TRUST_STORE

	// Node.
	NodeName        string // DLOCKSS_NODE_NAME
	IPFSNodeAddress string // DLOCKSS_IPFS_NODE
	APIPort         int    // DLOCKSS_API_PORT
	VerboseLogging  bool   // DLOCKSS_VERBOSE_LOGGING

	// Behavior.
	MinPeersPerShard       int      // DLOCKSS_MIN_PEERS_PER_SHARD: copies floor
	MaxPeersPerShard       int      // DLOCKSS_MAX_PEERS_PER_SHARD: split above
	TrustMode              string   // DLOCKSS_TRUST_MODE: open | allowlist
	IngestAllowlist        []string // DLOCKSS_INGEST_ALLOWLIST
	DiskUsageHighWaterMark float64  // DLOCKSS_DISK_USAGE_HIGH_WATER_MARK (%)
	PinIngestEnabled       bool     // DLOCKSS_PIN_INGEST
}

// knownVars lists every environment variable this version reads; anything
// else with the DLOCKSS_ prefix gets a warning so operators notice frozen or
// removed knobs.
var knownVars = map[string]struct{}{
	"DLOCKSS_DISCOVERY_TAG": {}, "DLOCKSS_PUBSUB_TOPIC_PREFIX": {}, "DLOCKSS_TOPIC_NAME": {},
	"DLOCKSS_PRIVATE_NETWORK": {}, "DLOCKSS_DHT_CLIENT": {},
	"DLOCKSS_STATE_DIR": {}, "DLOCKSS_CLUSTER_STORE": {}, "DLOCKSS_BADBITS_PATH": {},
	"DLOCKSS_IDENTITY_PATH": {}, "DLOCKSS_IPFS_CONFIG": {}, "DLOCKSS_TRUST_STORE": {},
	"DLOCKSS_NODE_NAME": {}, "DLOCKSS_IPFS_NODE": {}, "DLOCKSS_API_PORT": {},
	"DLOCKSS_VERBOSE_LOGGING": {},
	"DLOCKSS_MIN_PEERS_PER_SHARD": {}, "DLOCKSS_MAX_PEERS_PER_SHARD": {},
	"DLOCKSS_TRUST_MODE": {}, "DLOCKSS_INGEST_ALLOWLIST": {},
	"DLOCKSS_DISK_USAGE_HIGH_WATER_MARK": {}, "DLOCKSS_PIN_INGEST": {},
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envParsed[T any](key string, def T, kind string, parse func(string) (T, error)) T {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	p, err := parse(v)
	if err != nil {
		slog.Warn("invalid env "+kind+", using default", "var", key, "value", v)
		return def
	}
	return p
}

func envInt(key string, def int) int {
	return envParsed(key, def, "int", strconv.Atoi)
}

func envFloat(key string, def float64) float64 {
	return envParsed(key, def, "float", func(s string) (float64, error) { return strconv.ParseFloat(s, 64) })
}

func envBool(key string, def bool) bool {
	return envParsed(key, def, "bool", strconv.ParseBool)
}

func envStringSlice(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	var out []string
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// LoadFromEnv builds the configuration from defaults overlaid with the
// environment.
func LoadFromEnv() *Config {
	stateDir := envString("DLOCKSS_STATE_DIR", "./dlockss-state")

	cfg := &Config{
		DiscoveryServiceTag: envString("DLOCKSS_DISCOVERY_TAG", "dlockss-prod"),
		PubsubTopicPrefix:   envString("DLOCKSS_PUBSUB_TOPIC_PREFIX", DefaultPubsubVersion),
		TopicName:           envString("DLOCKSS_TOPIC_NAME", DefaultTopicName),
		PrivateNetwork:      envBool("DLOCKSS_PRIVATE_NETWORK", false),
		DHTClient:           envBool("DLOCKSS_DHT_CLIENT", false),

		StateDir:         stateDir,
		ClusterStorePath: envString("DLOCKSS_CLUSTER_STORE", filepath.Join(stateDir, "cluster_store")),
		BadBitsPath:      envString("DLOCKSS_BADBITS_PATH", "badBits.csv"),
		IdentityPath:     envString("DLOCKSS_IDENTITY_PATH", filepath.Join(stateDir, "dlockss.key")),
		NodeNamePath:     filepath.Join(stateDir, "node_name"),
		IPFSConfigPath:   os.Getenv("DLOCKSS_IPFS_CONFIG"),
		TrustStorePath:   envString("DLOCKSS_TRUST_STORE", filepath.Join(stateDir, "trusted_peers.json")),

		NodeName:        os.Getenv("DLOCKSS_NODE_NAME"),
		IPFSNodeAddress: envString("DLOCKSS_IPFS_NODE", "/ip4/127.0.0.1/tcp/5001"),
		APIPort:         envInt("DLOCKSS_API_PORT", 5050),
		VerboseLogging:  envBool("DLOCKSS_VERBOSE_LOGGING", false),

		MinPeersPerShard:       envInt("DLOCKSS_MIN_PEERS_PER_SHARD", 6),
		MaxPeersPerShard:       envInt("DLOCKSS_MAX_PEERS_PER_SHARD", 12),
		TrustMode:              envString("DLOCKSS_TRUST_MODE", "open"),
		IngestAllowlist:        envStringSlice("DLOCKSS_INGEST_ALLOWLIST"),
		DiskUsageHighWaterMark: envFloat("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", 90.0),
		PinIngestEnabled:       envBool("DLOCKSS_PIN_INGEST", true),
	}

	warnUnknownVars()
	return cfg
}

func warnUnknownVars() {
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, "DLOCKSS_") {
			continue
		}
		if _, ok := knownVars[name]; !ok {
			slog.Warn("environment variable is not used by this version (frozen or removed knob)", "var", name)
		}
	}
}

// Validate corrects out-of-range values with a warning. It never fails.
func (c *Config) Validate() {
	fix := func(field string, bad bool, apply func()) {
		if bad {
			slog.Warn("invalid config value, using default", "field", field)
			apply()
		}
	}
	fix("TrustMode", c.TrustMode != "open" && c.TrustMode != "allowlist",
		func() { c.TrustMode = "open" })
	fix("MinPeersPerShard", c.MinPeersPerShard < 1, func() { c.MinPeersPerShard = 6 })
	fix("MaxPeersPerShard", c.MaxPeersPerShard < 2, func() { c.MaxPeersPerShard = 12 })
	fix("MinPeersPerShard>MaxPeersPerShard", c.MinPeersPerShard > c.MaxPeersPerShard,
		func() { c.MinPeersPerShard, c.MaxPeersPerShard = c.MaxPeersPerShard, c.MinPeersPerShard })
	fix("DiskUsageHighWaterMark", c.DiskUsageHighWaterMark <= 0 || c.DiskUsageHighWaterMark > 100,
		func() { c.DiskUsageHighWaterMark = 90.0 })
	fix("APIPort", c.APIPort < 1 || c.APIPort > 65535, func() { c.APIPort = 5050 })
}

// ShardTopic returns the shard topic name for the given shard ID.
func (c *Config) ShardTopic(shardID string) string {
	return c.PubsubTopicPrefix + "-" + c.TopicName + "-shard-" + shardID
}

// Log prints the effective configuration at Info level.
func (c *Config) Log() {
	slog.Info("config: network",
		"topic_prefix", c.PubsubTopicPrefix, "topic", c.TopicName,
		"discovery_tag", c.DiscoveryServiceTag, "private_network", c.PrivateNetwork,
		"dht_client", c.DHTClient)
	slog.Info("config: paths",
		"state_dir", c.StateDir, "cluster_store", c.ClusterStorePath,
		"identity", c.IdentityPath, "trust_store", c.TrustStorePath,
		"badbits", c.BadBitsPath, "ipfs", c.IPFSNodeAddress)
	slog.Info("config: behavior",
		"min_peers", c.MinPeersPerShard, "max_peers", c.MaxPeersPerShard,
		"trust_mode", c.TrustMode, "disk_hwm_pct", c.DiskUsageHighWaterMark,
		"pin_ingest", c.PinIngestEnabled, "api_port", c.APIPort)
}
