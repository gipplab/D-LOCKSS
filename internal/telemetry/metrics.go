package telemetry

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Prometheus Metrics
	promMessagesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_messages_received_total",
		Help: "Total number of P2P messages received",
	})
	promMessagesDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_messages_dropped_total",
		Help: "P2P messages dropped (rate limit or error)",
	})
	promReplicationChecks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_replication_checks_total",
		Help: "Total number of replication checks performed",
	})
	promReplicationSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_replication_success_total",
		Help: "Total number of successful replication checks",
	})
	promReplicationFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_replication_failures_total",
		Help: "Total number of failed replication checks",
	})
	promDHTQueries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_dht_queries_total",
		Help: "Total number of DHT queries performed",
	})
	promDHTTimeouts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_dht_timeouts_total",
		Help: "Total number of DHT queries that timed out",
	})
	promShardSplits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlockss_shard_splits_total",
		Help: "Total number of shard split events",
	})

	// Gauges
	promPinnedFiles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dlockss_pinned_files",
		Help: "Current number of files pinned locally",
	})
	promKnownFiles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dlockss_known_files",
		Help: "Current number of files tracked in known files",
	})
	promActivePeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dlockss_active_peers",
		Help: "Number of peers in the current shard",
	})
	// Cluster-style metrics (per shard, from CRDT)
	promClusterPinsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dlockss_cluster_pins_total",
		Help: "Number of pins in the shard's CRDT consensus state",
	}, []string{"shard"})
	promClusterPeersTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dlockss_cluster_peers_total",
		Help: "Number of peers in the shard's CRDT cluster (from PeerMonitor)",
	}, []string{"shard"})
	promClusterAllocationsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dlockss_cluster_allocations_total",
		Help: "Total allocation count in the shard (sum of len(allocations) over all pins)",
	}, []string{"shard"})
)

func init() {
	// Register metrics
	prometheus.MustRegister(
		promMessagesReceived,
		promMessagesDropped,
		promReplicationChecks,
		promReplicationSuccess,
		promReplicationFailures,
		promDHTQueries,
		promDHTTimeouts,
		promShardSplits,
		promPinnedFiles,
		promKnownFiles,
		promActivePeers,
		promClusterPinsTotal,
		promClusterPeersTotal,
		promClusterAllocationsTotal,
	)
}

// Interfaces for dependencies
type ShardInfoProvider interface {
	GetShardInfo() (string, int)
}

type StorageInfoProvider interface {
	GetStorageStatus() common.StorageSnapshot
	GetReplicationLevels() map[string]int
}

// ClusterInfoProvider supplies cluster-style metrics (pins/peers/allocations per shard).
type ClusterInfoProvider interface {
	GetClusterMetrics(ctx context.Context) (pinsPerShard, peersPerShard, allocationsTotalPerShard map[string]int, err error)
}

type MetricsManager struct {
	mu  sync.RWMutex
	cfg *config.Config

	peerID string

	// Metrics state
	pinnedFilesCount              int
	knownFilesCount               int
	messagesReceived              int64
	messagesDropped               int64
	replicationChecks             int64
	replicationSuccess            int64
	replicationFailures           int64
	shardSplits                   int64
	workerPoolActive              int
	rateLimitedPeers              int
	filesInBackoff                int
	lowReplicationFiles           int
	highReplicationFiles          int
	dhtQueries                    int64
	dhtQueryTimeouts              int64
	lastReportTime                time.Time
	startTime                     time.Time
	replicationDistribution       [11]int
	filesAtTargetReplication      int
	avgReplicationLevel           float64
	filesConvergedTotal           int64
	filesConvergedThisPeriod      int64
	cumulativeMessagesReceived    int64
	cumulativeMessagesDropped     int64
	cumulativeReplicationChecks   int64
	cumulativeReplicationSuccess  int64
	cumulativeReplicationFailures int64
	cumulativeDhtQueries          int64
	cumulativeDhtQueryTimeouts    int64
	cumulativeShardSplits         int64

	// Providers
	shardInfo   ShardInfoProvider
	storageInfo StorageInfoProvider
	clusterInfo ClusterInfoProvider
	rateLimiter *common.RateLimiter
}

func NewMetricsManager(cfg *config.Config) *MetricsManager {
	return &MetricsManager{
		cfg:            cfg,
		lastReportTime: time.Now(),
		startTime:      time.Now(),
	}
}

func (m *MetricsManager) SetPeerID(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerID = peerID
}

// RegisterProviders registers components that provide metrics.
func (m *MetricsManager) RegisterProviders(s ShardInfoProvider, st StorageInfoProvider, rl *common.RateLimiter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shardInfo = s
	m.storageInfo = st
	m.rateLimiter = rl
}

// RegisterClusterProvider registers the cluster metrics provider (pins/peers/allocations per shard).
func (m *MetricsManager) RegisterClusterProvider(c ClusterInfoProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clusterInfo = c
}

func (m *MetricsManager) IncrementMessagesReceived() {
	m.mu.Lock()
	m.messagesReceived++
	m.cumulativeMessagesReceived++
	m.mu.Unlock()
	promMessagesReceived.Inc()
}

func (m *MetricsManager) IncrementMessagesDropped() {
	m.mu.Lock()
	m.messagesDropped++
	m.cumulativeMessagesDropped++
	m.mu.Unlock()
	promMessagesDropped.Inc()
}

func (m *MetricsManager) IncrementReplicationChecks() {
	m.mu.Lock()
	m.replicationChecks++
	m.cumulativeReplicationChecks++
	m.mu.Unlock()
	promReplicationChecks.Inc()
}

func (m *MetricsManager) IncrementReplicationSuccess() {
	m.mu.Lock()
	m.replicationSuccess++
	m.cumulativeReplicationSuccess++
	m.mu.Unlock()
	promReplicationSuccess.Inc()
}

func (m *MetricsManager) IncrementReplicationFailures() {
	m.mu.Lock()
	m.replicationFailures++
	m.cumulativeReplicationFailures++
	m.mu.Unlock()
	promReplicationFailures.Inc()
}

// IncrementDHTQueries increments the number of DHT queries.
func (m *MetricsManager) IncrementDHTQueries() {
	m.mu.Lock()
	m.dhtQueries++
	m.cumulativeDhtQueries++
	m.mu.Unlock()
	promDHTQueries.Inc()
}

// IncrementDHTQueryTimeouts increments the number of DHT query timeouts.
func (m *MetricsManager) IncrementDHTQueryTimeouts() {
	m.mu.Lock()
	m.dhtQueryTimeouts++
	m.cumulativeDhtQueryTimeouts++
	m.mu.Unlock()
	promDHTTimeouts.Inc()
}

// IncrementShardSplits increments the number of shard splits.
func (m *MetricsManager) IncrementShardSplits() {
	m.mu.Lock()
	m.shardSplits++
	m.cumulativeShardSplits++
	m.mu.Unlock()
	promShardSplits.Inc()
}

func (m *MetricsManager) IncrementFilesConverged() {
	m.mu.Lock()
	m.filesConvergedTotal++
	m.filesConvergedThisPeriod++
	m.mu.Unlock()
}

func (m *MetricsManager) SetPinnedFilesCount(count int) {
	m.mu.Lock()
	m.pinnedFilesCount = count
	m.mu.Unlock()
	promPinnedFiles.Set(float64(count))
}

func (m *MetricsManager) SetKnownFilesCount(count int) {
	m.mu.Lock()
	m.knownFilesCount = count
	m.mu.Unlock()
	promKnownFiles.Set(float64(count))
}

func (m *MetricsManager) RunMetricsReporter(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.MetricsReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.UpdateGauges()
			m.ReportMetrics()
		}
	}
}

func (m *MetricsManager) UpdateGauges() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Use stored values for pinned/known since they are pushed by storage
	promPinnedFiles.Set(float64(m.pinnedFilesCount))
	promKnownFiles.Set(float64(m.knownFilesCount))

	if m.shardInfo != nil {
		_, activePeers := m.shardInfo.GetShardInfo()
		promActivePeers.Set(float64(activePeers))
	}

	// Cluster-style metrics (pins/peers/allocations per shard)
	if m.clusterInfo != nil {
		pins, peers, allocs, err := m.clusterInfo.GetClusterMetrics(context.Background())
		if err == nil {
			for shard, count := range pins {
				promClusterPinsTotal.WithLabelValues(shard).Set(float64(count))
			}
			for shard, count := range peers {
				promClusterPeersTotal.WithLabelValues(shard).Set(float64(count))
			}
			for shard, count := range allocs {
				promClusterAllocationsTotal.WithLabelValues(shard).Set(float64(count))
			}
		}
	}
}

func (m *MetricsManager) ReportMetrics() {
	m.mu.RLock()
	now := time.Now()
	elapsed := now.Sub(m.lastReportTime)
	minutes := elapsed.Minutes()
	if minutes < 0.1 {
		minutes = 0.1
	}

	msgRate := float64(m.messagesReceived) / minutes
	dropRate := float64(m.messagesDropped) / minutes

	shardID := ""
	activePeers := 0
	if m.shardInfo != nil {
		shardID, activePeers = m.shardInfo.GetShardInfo()
	}

	rateLimitedPeers := 0
	if m.rateLimiter != nil {
		rateLimitedPeers = m.rateLimiter.Size()
	}

	backoffCount := 0
	levelsMap := make(map[string]int)
	if m.storageInfo != nil {
		backoffCount = m.storageInfo.GetStorageStatus().BackoffCount
		levelsMap = m.storageInfo.GetReplicationLevels()
	}

	m.mu.RUnlock() // Unlock for calculation

	distribution := [11]int{}
	totalFiles := 0
	totalReplication := 0
	for _, count := range levelsMap {
		if count >= 10 {
			distribution[10]++
		} else {
			distribution[count]++
		}
		totalFiles++
		totalReplication += count
	}

	avgReplication := 0.0
	if totalFiles > 0 {
		avgReplication = float64(totalReplication) / float64(totalFiles)
	}

	filesAtTarget := 0
	for _, count := range levelsMap {
		if count >= m.cfg.MinReplication && count <= m.cfg.MaxReplication {
			filesAtTarget++
		}
	}

	lowReplication := 0
	highReplication := 0
	for _, count := range levelsMap {
		if count < m.cfg.MinReplication {
			lowReplication++
		} else if count > m.cfg.MaxReplication {
			highReplication++
		}
	}

	m.mu.Lock()
	m.replicationDistribution = distribution
	m.avgReplicationLevel = avgReplication
	m.filesAtTargetReplication = filesAtTarget
	m.lowReplicationFiles = lowReplication
	m.highReplicationFiles = highReplication
	m.mu.Unlock()

	slog.Debug("metrics report: storage", "pinned", m.pinnedFilesCount, "known", m.knownFilesCount)
	slog.Debug("metrics report: replication",
		"checks", m.replicationChecks, "success", m.replicationSuccess, "failures", m.replicationFailures,
		"low", lowReplication, "high", highReplication, "at_target", filesAtTarget)
	slog.Debug("metrics report: replication distribution",
		"r0", distribution[0], "r1", distribution[1], "r2", distribution[2],
		"r3", distribution[3], "r4", distribution[4], "r5", distribution[5],
		"r6", distribution[6], "r7", distribution[7], "r8", distribution[8],
		"r9", distribution[9], "r10_plus", distribution[10])
	slog.Debug("metrics report: convergence",
		"avg_replication", avgReplication, "converged_total", m.filesConvergedTotal, "converged_this_period", m.filesConvergedThisPeriod)
	slog.Debug("metrics report: network",
		"msg_rate_per_min", msgRate, "drop_rate_per_min", dropRate, "active_peers", activePeers)
	slog.Debug("metrics report: system",
		"shard_splits", m.shardSplits, "current_shard", shardID, "rate_limited_peers", rateLimitedPeers, "files_in_backoff", backoffCount)
	if m.clusterInfo != nil {
		pins, peers, allocs, err := m.clusterInfo.GetClusterMetrics(context.Background())
		if err == nil {
			for shard := range pins {
				slog.Debug("metrics report: cluster shard",
					"shard", shard, "pins", pins[shard], "peers", peers[shard], "allocations_total", allocs[shard])
			}
		}
	}
	uptime := now.Sub(m.startTime)
	slog.Debug("metrics report: cumulative",
		"uptime", uptime.Round(time.Second),
		"msgs", m.cumulativeMessagesReceived, "dropped", m.cumulativeMessagesDropped,
		"checks", m.cumulativeReplicationChecks, "success", m.cumulativeReplicationSuccess,
		"failures", m.cumulativeReplicationFailures, "shard_splits", m.cumulativeShardSplits)

	if m.cfg.MetricsExportPath != "" {
		m.ExportMetricsToFile(now)
	}

	m.mu.Lock()
	m.lastReportTime = now
	m.messagesReceived = 0
	m.messagesDropped = 0
	m.replicationChecks = 0
	m.replicationSuccess = 0
	m.replicationFailures = 0
	m.filesConvergedThisPeriod = 0
	m.mu.Unlock()
}

func (m *MetricsManager) GetStatus() common.StatusResponse {
	m.mu.RLock()
	pinned := m.pinnedFilesCount
	known := m.knownFilesCount
	startTime := m.startTime
	avgRepl := m.avgReplicationLevel
	atTarget := m.filesAtTargetReplication
	dist := m.replicationDistribution
	m.mu.RUnlock()

	shardID := ""
	peers := 0
	if m.shardInfo != nil {
		shardID, peers = m.shardInfo.GetShardInfo()
	}

	activeWorkers := 0
	queueDepth := 0

	var knownCIDs []string
	if m.storageInfo != nil && m.cfg.TelemetryIncludeCIDs {
		knownCIDs = m.storageInfo.GetStorageStatus().KnownCIDs
	}

	m.mu.RLock()
	peerID := m.peerID
	m.mu.RUnlock()

	return common.StatusResponse{
		PeerID:       peerID,
		Version:      "1.0.0",
		CurrentShard: shardID,
		PeersInShard: peers,
		Storage: common.StorageStatus{
			PinnedFiles: pinned,
			KnownFiles:  known,
			KnownCIDs:   knownCIDs,
		},
		Replication: common.ReplicationStatus{
			QueueDepth:              queueDepth,
			ActiveWorkers:           activeWorkers,
			AvgReplicationLevel:     avgRepl,
			FilesAtTarget:           atTarget,
			ReplicationDistribution: dist,
		},
		UptimeSeconds: time.Since(startTime).Seconds(),
	}
}

func (m *MetricsManager) ExportMetricsToFile(timestamp time.Time) {
	path := m.cfg.MetricsExportPath
	if path == "" {
		return
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("failed to create metrics export directory", "error", err)
		return
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("failed to open metrics export file", "error", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	m.mu.RLock()
	defer m.mu.RUnlock()

	uptime := timestamp.Sub(m.startTime).Seconds()
	record := []string{
		timestamp.Format(time.RFC3339),
		fmt.Sprintf("%.2f", uptime),
		strconv.Itoa(m.pinnedFilesCount),
		strconv.Itoa(m.knownFilesCount),
	}
	writer.Write(record)
}
