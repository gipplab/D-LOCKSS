package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	// Key string fields
	if cfg.DiscoveryServiceTag != "dlockss-prod" {
		t.Errorf("DiscoveryServiceTag = %q, want dlockss-prod", cfg.DiscoveryServiceTag)
	}
	if cfg.PubsubTopicPrefix != DefaultPubsubVersion {
		t.Errorf("PubsubTopicPrefix = %q, want %q", cfg.PubsubTopicPrefix, DefaultPubsubVersion)
	}
	if cfg.FileWatchFolder != "./data" {
		t.Errorf("FileWatchFolder = %q, want ./data", cfg.FileWatchFolder)
	}
	if cfg.BadBitsPath != "badBits.csv" {
		t.Errorf("BadBitsPath = %q, want badBits.csv", cfg.BadBitsPath)
	}
	if cfg.TrustMode != "open" {
		t.Errorf("TrustMode = %q, want open", cfg.TrustMode)
	}
	if cfg.SignatureMode != "strict" {
		t.Errorf("SignatureMode = %q, want strict", cfg.SignatureMode)
	}
	if cfg.IPFSNodeAddress != "/ip4/127.0.0.1/tcp/5001" {
		t.Errorf("IPFSNodeAddress = %q, want /ip4/127.0.0.1/tcp/5001", cfg.IPFSNodeAddress)
	}

	// Key numeric fields
	if cfg.MinReplication != 5 {
		t.Errorf("MinReplication = %d, want 5", cfg.MinReplication)
	}
	if cfg.MaxReplication != 10 {
		t.Errorf("MaxReplication = %d, want 10", cfg.MaxReplication)
	}
	if cfg.APIPort != 5050 {
		t.Errorf("APIPort = %d, want 5050", cfg.APIPort)
	}
	if cfg.MaxPeersPerShard != 12 {
		t.Errorf("MaxPeersPerShard = %d, want 12", cfg.MaxPeersPerShard)
	}
	if cfg.MinPeersPerShard != 6 {
		t.Errorf("MinPeersPerShard = %d, want 6", cfg.MinPeersPerShard)
	}
	if cfg.DiskUsageHighWaterMark != 90.0 {
		t.Errorf("DiskUsageHighWaterMark = %f, want 90.0", cfg.DiskUsageHighWaterMark)
	}
	if cfg.BackoffMultiplier != 2.0 {
		t.Errorf("BackoffMultiplier = %f, want 2.0", cfg.BackoffMultiplier)
	}

	// Key duration fields (non-zero)
	if cfg.CheckInterval != 1*time.Minute {
		t.Errorf("CheckInterval = %v, want 1m", cfg.CheckInterval)
	}
	if cfg.BootstrapTimeout != 15*time.Second {
		t.Errorf("BootstrapTimeout = %v, want 15s", cfg.BootstrapTimeout)
	}
	if cfg.SignatureMaxAge != 10*time.Minute {
		t.Errorf("SignatureMaxAge = %v, want 10m", cfg.SignatureMaxAge)
	}

	// Key bool fields
	if !cfg.UsePubsubForReplication {
		t.Error("UsePubsubForReplication = false, want true")
	}
	if !cfg.AutoReplicationEnabled {
		t.Error("AutoReplicationEnabled = false, want true")
	}
	if cfg.VerboseLogging {
		t.Error("VerboseLogging = true, want false")
	}

	// Path fields derived from data dir
	wantClusterStore := filepath.Join(filepath.Dir("./data"), "cluster_store")
	if cfg.ClusterStorePath != wantClusterStore {
		t.Errorf("ClusterStorePath = %q, want %q", cfg.ClusterStorePath, wantClusterStore)
	}
	wantIdentity := filepath.Join(filepath.Dir("./data"), "dlockss.key")
	if cfg.IdentityPath != wantIdentity {
		t.Errorf("IdentityPath = %q, want %q", cfg.IdentityPath, wantIdentity)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Set specific env vars and verify they are picked up
	t.Setenv("DLOCKSS_DATA_DIR", "/custom/data")
	t.Setenv("DLOCKSS_DISCOVERY_TAG", "dlockss-test")
	t.Setenv("DLOCKSS_MIN_REPLICATION", "3")
	t.Setenv("DLOCKSS_MAX_REPLICATION", "7")
	t.Setenv("DLOCKSS_API_PORT", "9090")
	t.Setenv("DLOCKSS_SIGNATURE_MODE", "warn")
	t.Setenv("DLOCKSS_USE_PUBSUB_FOR_REPLICATION", "false")
	t.Setenv("DLOCKSS_VERBOSE_LOGGING", "true")
	t.Setenv("DLOCKSS_CHECK_INTERVAL", "2m")
	t.Setenv("DLOCKSS_DISK_USAGE_HIGH_WATER_MARK", "85.5")
	t.Setenv("DLOCKSS_BACKOFF_MULTIPLIER", "3.5")
	t.Setenv("DLOCKSS_NODE_NAME", "test-node-1")

	cfg := LoadFromEnv()

	if cfg.FileWatchFolder != "/custom/data" {
		t.Errorf("FileWatchFolder = %q, want /custom/data", cfg.FileWatchFolder)
	}
	if cfg.DiscoveryServiceTag != "dlockss-test" {
		t.Errorf("DiscoveryServiceTag = %q, want dlockss-test", cfg.DiscoveryServiceTag)
	}
	if cfg.MinReplication != 3 {
		t.Errorf("MinReplication = %d, want 3", cfg.MinReplication)
	}
	if cfg.MaxReplication != 7 {
		t.Errorf("MaxReplication = %d, want 7", cfg.MaxReplication)
	}
	if cfg.APIPort != 9090 {
		t.Errorf("APIPort = %d, want 9090", cfg.APIPort)
	}
	if cfg.SignatureMode != "warn" {
		t.Errorf("SignatureMode = %q, want warn", cfg.SignatureMode)
	}
	if cfg.UsePubsubForReplication {
		t.Errorf("UsePubsubForReplication = true, want false")
	}
	if !cfg.VerboseLogging {
		t.Errorf("VerboseLogging = false, want true")
	}
	if cfg.CheckInterval != 2*time.Minute {
		t.Errorf("CheckInterval = %v, want 2m", cfg.CheckInterval)
	}
	if cfg.DiskUsageHighWaterMark != 85.5 {
		t.Errorf("DiskUsageHighWaterMark = %f, want 85.5", cfg.DiskUsageHighWaterMark)
	}
	if cfg.BackoffMultiplier != 3.5 {
		t.Errorf("BackoffMultiplier = %f, want 3.5", cfg.BackoffMultiplier)
	}
	if cfg.NodeName != "test-node-1" {
		t.Errorf("NodeName = %q, want test-node-1", cfg.NodeName)
	}
}

func TestLoadFromEnv_ClusterStoreOverride(t *testing.T) {
	t.Setenv("DLOCKSS_DATA_DIR", "/data")
	t.Setenv("DLOCKSS_CLUSTER_STORE", "/custom/cluster_store")
	cfg := LoadFromEnv()
	if cfg.ClusterStorePath != "/custom/cluster_store" {
		t.Errorf("ClusterStorePath = %q, want /custom/cluster_store", cfg.ClusterStorePath)
	}
}

func TestLoadFromEnv_IdentityPathOverride(t *testing.T) {
	t.Setenv("DLOCKSS_DATA_DIR", "/data")
	t.Setenv("DLOCKSS_IDENTITY_PATH", "/custom/dlockss.key")
	cfg := LoadFromEnv()
	if cfg.IdentityPath != "/custom/dlockss.key" {
		t.Errorf("IdentityPath = %q, want /custom/dlockss.key", cfg.IdentityPath)
	}
}

func TestValidate_InvalidSignatureMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SignatureMode = "invalid-mode"
	cfg.Validate()
	if cfg.SignatureMode != "strict" {
		t.Errorf("SignatureMode after Validate = %q, want strict", cfg.SignatureMode)
	}
}

func TestValidate_ValidSignatureModes(t *testing.T) {
	for _, mode := range []string{"off", "warn", "strict"} {
		cfg := DefaultConfig()
		cfg.SignatureMode = mode
		cfg.Validate()
		if cfg.SignatureMode != mode {
			t.Errorf("SignatureMode %q was changed to %q", mode, cfg.SignatureMode)
		}
	}
}

func TestValidate_MaxConcurrentFileProcessing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrentFileProcessing = 0
	cfg.Validate()
	if cfg.MaxConcurrentFileProcessing != 5 {
		t.Errorf("MaxConcurrentFileProcessing = %d, want 5", cfg.MaxConcurrentFileProcessing)
	}

	cfg.MaxConcurrentFileProcessing = -1
	cfg.Validate()
	if cfg.MaxConcurrentFileProcessing != 5 {
		t.Errorf("MaxConcurrentFileProcessing (negative) = %d, want 5", cfg.MaxConcurrentFileProcessing)
	}
}

func TestValidate_NonceSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NonceSize = 0
	cfg.Validate()
	if cfg.NonceSize != 16 {
		t.Errorf("NonceSize = %d, want 16", cfg.NonceSize)
	}

	cfg.NonceSize = -5
	cfg.Validate()
	if cfg.NonceSize != 16 {
		t.Errorf("NonceSize (negative) = %d, want 16", cfg.NonceSize)
	}
}

func TestValidate_MinNonceSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinNonceSize = 0
	cfg.Validate()
	if cfg.MinNonceSize != 8 {
		t.Errorf("MinNonceSize = %d, want 8", cfg.MinNonceSize)
	}
}

func TestValidate_MinMaxReplicationSwap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinReplication = 20
	cfg.MaxReplication = 5
	cfg.Validate()
	if cfg.MinReplication != 5 || cfg.MaxReplication != 20 {
		t.Errorf("Min/MaxReplication not swapped: min=%d max=%d, want min=5 max=20", cfg.MinReplication, cfg.MaxReplication)
	}
}

func TestValidate_MaxConcurrentReplicationChecks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrentReplicationChecks = 0
	cfg.Validate()
	if cfg.MaxConcurrentReplicationChecks != 5 {
		t.Errorf("MaxConcurrentReplicationChecks = %d, want 5", cfg.MaxConcurrentReplicationChecks)
	}
}

func TestValidate_DiskUsageHighWaterMark(t *testing.T) {
	tests := []struct {
		value float64
		want  float64
	}{
		{0, 90.0},
		{-10, 90.0},
		{101, 90.0},
		{150, 90.0},
		{50.0, 50.0},
		{90.0, 90.0},
		{100.0, 100.0},
	}
	for _, tt := range tests {
		cfg := DefaultConfig()
		cfg.DiskUsageHighWaterMark = tt.value
		cfg.Validate()
		if cfg.DiskUsageHighWaterMark != tt.want {
			t.Errorf("DiskUsageHighWaterMark %f -> %f, want %f", tt.value, cfg.DiskUsageHighWaterMark, tt.want)
		}
	}
}

func TestGetEnvInt_Valid(t *testing.T) {
	t.Setenv("TEST_INT_KEY", "42")
	got := getEnvInt("TEST_INT_KEY", 10)
	if got != 42 {
		t.Errorf("getEnvInt = %d, want 42", got)
	}
}

func TestGetEnvInt_Invalid(t *testing.T) {
	t.Setenv("TEST_INT_INVALID", "not-a-number")
	got := getEnvInt("TEST_INT_INVALID", 99)
	if got != 99 {
		t.Errorf("getEnvInt(invalid) = %d, want 99", got)
	}
}

func TestGetEnvInt_Unset(t *testing.T) {
	t.Setenv("TEST_INT_UNSET", "") // empty = unset for getEnv* semantics
	got := getEnvInt("TEST_INT_UNSET", 7)
	if got != 7 {
		t.Errorf("getEnvInt(unset) = %d, want 7", got)
	}
}

func TestGetEnvDuration_Valid(t *testing.T) {
	t.Setenv("TEST_DUR_KEY", "30s")
	got := getEnvDuration("TEST_DUR_KEY", 5*time.Minute)
	if got != 30*time.Second {
		t.Errorf("getEnvDuration = %v, want 30s", got)
	}
}

func TestGetEnvDuration_Invalid(t *testing.T) {
	t.Setenv("TEST_DUR_INVALID", "not-a-duration")
	got := getEnvDuration("TEST_DUR_INVALID", 2*time.Hour)
	if got != 2*time.Hour {
		t.Errorf("getEnvDuration(invalid) = %v, want 2h", got)
	}
}

func TestGetEnvDuration_Unset(t *testing.T) {
	t.Setenv("TEST_DUR_UNSET", "")
	got := getEnvDuration("TEST_DUR_UNSET", 1*time.Second)
	if got != 1*time.Second {
		t.Errorf("getEnvDuration(unset) = %v, want 1s", got)
	}
}

func TestGetEnvFloat_Valid(t *testing.T) {
	t.Setenv("TEST_FLOAT_KEY", "3.14")
	got := getEnvFloat("TEST_FLOAT_KEY", 1.0)
	if got != 3.14 {
		t.Errorf("getEnvFloat = %f, want 3.14", got)
	}
}

func TestGetEnvFloat_Invalid(t *testing.T) {
	t.Setenv("TEST_FLOAT_INVALID", "xyz")
	got := getEnvFloat("TEST_FLOAT_INVALID", 2.5)
	if got != 2.5 {
		t.Errorf("getEnvFloat(invalid) = %f, want 2.5", got)
	}
}

func TestGetEnvFloat_Unset(t *testing.T) {
	t.Setenv("TEST_FLOAT_UNSET", "")
	got := getEnvFloat("TEST_FLOAT_UNSET", 0.5)
	if got != 0.5 {
		t.Errorf("getEnvFloat(unset) = %f, want 0.5", got)
	}
}

func TestGetEnvBool_Valid(t *testing.T) {
	for _, tc := range []struct {
		env   string
		def   bool
		want  bool
	}{
		{"true", false, true},
		{"1", false, true},
		{"TRUE", false, true},
		{"false", true, false},
		{"0", true, false},
		{"FALSE", true, false},
	} {
		t.Setenv("TEST_BOOL_KEY", tc.env)
		got := getEnvBool("TEST_BOOL_KEY", tc.def)
		if got != tc.want {
			t.Errorf("getEnvBool(%q, %v) = %v, want %v", tc.env, tc.def, got, tc.want)
		}
	}
}

func TestGetEnvBool_Invalid(t *testing.T) {
	t.Setenv("TEST_BOOL_INVALID", "maybe")
	got := getEnvBool("TEST_BOOL_INVALID", true)
	if !got {
		t.Errorf("getEnvBool(invalid) = %v, want true (default)", got)
	}

	t.Setenv("TEST_BOOL_INVALID2", "nope")
	got = getEnvBool("TEST_BOOL_INVALID2", false)
	if got {
		t.Errorf("getEnvBool(invalid) = %v, want false (default)", got)
	}
}

func TestGetEnvBool_Unset(t *testing.T) {
	t.Setenv("TEST_BOOL_UNSET", "")
	got := getEnvBool("TEST_BOOL_UNSET", true)
	if !got {
		t.Errorf("getEnvBool(unset) = %v, want true", got)
	}

	got = getEnvBool("TEST_BOOL_UNSET", false)
	if got {
		t.Errorf("getEnvBool(unset) = %v, want false", got)
	}
}
