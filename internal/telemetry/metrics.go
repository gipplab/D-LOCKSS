package telemetry

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
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
	promWorkerPoolActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dlockss_worker_pool_active",
		Help: "Number of active replication workers",
	})
	promQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dlockss_replication_queue_depth",
		Help: "Current depth of the replication job queue",
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
		promWorkerPoolActive,
		promQueueDepth,
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
	GetStorageStatus() (int, int, []string, int) // pinned, known, cids, backoffCount
	GetReplicationLevels() map[string]int
}

// ClusterInfoProvider supplies cluster-style metrics (pins/peers/allocations per shard).
type ClusterInfoProvider interface {
	GetClusterMetrics(ctx context.Context) (pinsPerShard, peersPerShard, allocationsTotalPerShard map[string]int, err error)
}

type MetricsManager struct {
	mu sync.RWMutex

	// Peer ID (set from host so /status returns it; matches monitor and ipfs id when IPFS_PATH set)
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

func NewMetricsManager() *MetricsManager {
	return &MetricsManager{
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
	ticker := time.NewTicker(config.MetricsReportInterval)
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
		_, _, _, backoffCount = m.storageInfo.GetStorageStatus()
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
		if count >= config.MinReplication && count <= config.MaxReplication {
			filesAtTarget++
		}
	}

	lowReplication := 0
	highReplication := 0
	for _, count := range levelsMap {
		if count < config.MinReplication {
			lowReplication++
		} else if count > config.MaxReplication {
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

	if config.VerboseLogging {
		log.Printf("[Metrics] === System Metrics Report ===")
		log.Printf("[Metrics] Storage: pinned=%d, known=%d", m.pinnedFilesCount, m.knownFilesCount)
		log.Printf("[Metrics] Replication: checks=%d, success=%d, failures=%d, low=%d, high=%d, at_target=%d",
			m.replicationChecks, m.replicationSuccess, m.replicationFailures,
			lowReplication, highReplication, filesAtTarget)
		log.Printf("[Metrics] Replication Distribution: 0=%d, 1=%d, 2=%d, 3=%d, 4=%d, 5=%d, 6=%d, 7=%d, 8=%d, 9=%d, 10+=%d",
			distribution[0], distribution[1], distribution[2],
			distribution[3], distribution[4], distribution[5],
			distribution[6], distribution[7], distribution[8],
			distribution[9], distribution[10])
		log.Printf("[Metrics] Convergence: avg_replication=%.2f (shard peers via pubsub), converged_total=%d, converged_this_period=%d",
			avgReplication, m.filesConvergedTotal, m.filesConvergedThisPeriod)
		log.Printf("[Metrics] Network: messages_received=%.1f/min, messages_dropped=%.1f/min, active_peers=%d (shard topic)",
			msgRate, dropRate, activePeers)
		// log.Printf("[Metrics] DHT: queries=%d, timeouts=%d", m.dhtQueries, m.dhtQueryTimeouts)
		// log.Printf("[Metrics] Performance: worker_pool_active=%d/%d, checks_rate=%.1f/min",
		// 	activeWorkers, maxWorkers, checkRate)
		log.Printf("[Metrics] System: shard_splits=%d, current_shard=%s, rate_limited_peers=%d, files_in_backoff=%d",
			m.shardSplits, shardID, rateLimitedPeers, backoffCount)
		if m.clusterInfo != nil {
			pins, peers, allocs, err := m.clusterInfo.GetClusterMetrics(context.Background())
			if err == nil {
				for shard := range pins {
					log.Printf("[Metrics] Cluster shard=%s pins=%d peers=%d allocations_total=%d",
						shard, pins[shard], peers[shard], allocs[shard])
				}
			}
		}

		uptime := now.Sub(m.startTime)
		log.Printf("[Metrics] Cumulative (since startup): uptime=%v, msgs=%d (dropped=%d), checks=%d (success=%d, failures=%d), shard_splits=%d",
			uptime.Round(time.Second),
			m.cumulativeMessagesReceived,
			m.cumulativeMessagesDropped,
			m.cumulativeReplicationChecks,
			m.cumulativeReplicationSuccess,
			m.cumulativeReplicationFailures,
			m.cumulativeShardSplits)
		log.Printf("[Metrics] ================================")
	}

	if config.MetricsExportPath != "" {
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

	knownCIDs := []string(nil)
	if m.storageInfo != nil && config.TelemetryIncludeCIDs {
		_, _, knownCIDs, _ = m.storageInfo.GetStorageStatus()
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
	path := config.MetricsExportPath
	if path == "" {
		return
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[Error] Failed to create metrics export directory: %v", err)
		return
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Error] Failed to open metrics export file: %v", err)
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
