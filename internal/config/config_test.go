package config

import (
	"path/filepath"
	"testing"
)

func TestDefaults(t *testing.T) {
	cfg := LoadFromEnv()
	if cfg.PubsubTopicPrefix != "dlockss-v0.0.3" || cfg.TopicName != "creative-commons" {
		t.Fatalf("topic defaults wrong: %q %q", cfg.PubsubTopicPrefix, cfg.TopicName)
	}
	if cfg.FileWatchFolder != "./data" {
		t.Fatalf("data dir default: %q", cfg.FileWatchFolder)
	}
	if cfg.ClusterStorePath != filepath.Join(".", "cluster_store") {
		t.Fatalf("cluster store default: %q", cfg.ClusterStorePath)
	}
	if cfg.MaxPeersPerShard != 12 || cfg.MinPeersPerShard != 6 {
		t.Fatal("shard size defaults wrong")
	}
	if cfg.TrustMode != "open" {
		t.Fatal("trust default wrong")
	}
	if !cfg.PinIngestEnabled || cfg.APIPort != 5050 {
		t.Fatal("node defaults wrong")
	}
}

func TestEnvOverridesAndInvalidValues(t *testing.T) {
	t.Setenv("DLOCKSS_MAX_PEERS_PER_SHARD", "20")
	t.Setenv("DLOCKSS_API_PORT", "notanumber")
	t.Setenv("DLOCKSS_PRIVATE_NETWORK", "true")
	cfg := LoadFromEnv()
	if cfg.MaxPeersPerShard != 20 {
		t.Fatal("int override ignored")
	}
	if cfg.APIPort != 5050 {
		t.Fatal("invalid int must keep the default")
	}
	if !cfg.PrivateNetwork {
		t.Fatal("bool override ignored")
	}
}

func TestValidateCorrections(t *testing.T) {
	cfg := LoadFromEnv()
	cfg.TrustMode = "bogus"
	cfg.DiskUsageHighWaterMark = 150
	cfg.MinPeersPerShard, cfg.MaxPeersPerShard = 9, 3
	cfg.APIPort = -1
	cfg.Validate()
	if cfg.TrustMode != "open" {
		t.Fatal("trust mode correction failed")
	}
	if cfg.DiskUsageHighWaterMark != 90.0 {
		t.Fatal("hwm correction failed")
	}
	if cfg.MinPeersPerShard != 3 || cfg.MaxPeersPerShard != 9 {
		t.Fatal("min/max swap failed")
	}
	if cfg.APIPort != 5050 {
		t.Fatal("port correction failed")
	}
}

func TestPathSafety(t *testing.T) {
	cfg := LoadFromEnv()
	dir := t.TempDir()
	cfg.FileWatchFolder = filepath.Join(dir, "data")
	cfg.IdentityPath = filepath.Join(dir, "dlockss.key")
	cfg.NodeNamePath = filepath.Join(dir, "node_name")
	cfg.ClusterStorePath = filepath.Join(dir, "cluster_store")
	cfg.TrustStorePath = filepath.Join(dir, "trusted_peers.json")
	if err := cfg.ValidatePathSafety(); err != nil {
		t.Fatalf("safe layout rejected: %v", err)
	}
	cfg.IdentityPath = filepath.Join(cfg.FileWatchFolder, "dlockss.key")
	if err := cfg.ValidatePathSafety(); err == nil {
		t.Fatal("identity inside the ingest dir must be rejected")
	}
	cfg.IdentityPath = cfg.FileWatchFolder // equal counts as inside
	if err := cfg.ValidatePathSafety(); err == nil {
		t.Fatal("identity equal to the ingest dir must be rejected")
	}
}

func TestShardTopic(t *testing.T) {
	cfg := LoadFromEnv()
	if got := cfg.ShardTopic(""); got != "dlockss-v0.0.3-creative-commons-shard-" {
		t.Fatalf("root topic: %q", got)
	}
	if got := cfg.ShardTopic("01"); got != "dlockss-v0.0.3-creative-commons-shard-01" {
		t.Fatalf("shard topic: %q", got)
	}
}
