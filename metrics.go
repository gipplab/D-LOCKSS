package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

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
		Help: "Total number of P2P messages dropped due to rate limits or errors",
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
	)
}

func incrementMetric(metric *int64) {
	metrics.Lock()
	defer metrics.Unlock()
	*metric++
	
	// Update Prometheus and legacy struct
	switch metric {
	case &metrics.messagesReceived:
		metrics.cumulativeMessagesReceived++
		promMessagesReceived.Inc()
	case &metrics.messagesDropped:
		metrics.cumulativeMessagesDropped++
		promMessagesDropped.Inc()
	case &metrics.replicationChecks:
		metrics.cumulativeReplicationChecks++
		promReplicationChecks.Inc()
	case &metrics.replicationSuccess:
		metrics.cumulativeReplicationSuccess++
		promReplicationSuccess.Inc()
	case &metrics.replicationFailures:
		metrics.cumulativeReplicationFailures++
		promReplicationFailures.Inc()
	case &metrics.dhtQueries:
		metrics.cumulativeDhtQueries++
		promDHTQueries.Inc()
	case &metrics.dhtQueryTimeouts:
		metrics.cumulativeDhtQueryTimeouts++
		promDHTTimeouts.Inc()
	case &metrics.shardSplits:
		metrics.cumulativeShardSplits++
		promShardSplits.Inc()
	}
}

func updateMetrics(fn func()) {
	metrics.Lock()
	defer metrics.Unlock()
	fn()
}

func runMetricsReporter(ctx context.Context) {
	ticker := time.NewTicker(MetricsReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateGauges()
			reportMetrics()
		}
	}
}

func updateGauges() {
	metrics.RLock()
	defer metrics.RUnlock()
	
	promPinnedFiles.Set(float64(metrics.pinnedFilesCount))
	promKnownFiles.Set(float64(metrics.knownFilesCount))
	
	if shardMgr != nil {
		_, activePeers := getShardInfo()
		promActivePeers.Set(float64(activePeers))
	}
	
	if replicationMgr != nil {
		promWorkerPoolActive.Set(float64(replicationMgr.checkingFiles.Size()))
		// Note: Queue depth isn't directly exposed by ReplicationManager yet, 
		// but we can add that later.
	}
}

func reportMetrics() {
	metrics.RLock()

	now := time.Now()
	elapsed := now.Sub(metrics.lastReportTime)
	minutes := elapsed.Minutes()
	if minutes < 0.1 {
		minutes = 0.1
	}

	msgRate := float64(metrics.messagesReceived) / minutes
	dropRate := float64(metrics.messagesDropped) / minutes
	checkRate := float64(metrics.replicationChecks) / minutes

	// Active peers are the peers currently subscribed to our shard topic
	// (including self). This reflects actual replication neighborhood size,
	// not just rate-limited peers.
	currentShard, activePeers := getShardInfo()

	// Rate-limited peers is a narrower metric: peers currently tracked by the
	// rate limiter due to recent message activity.
	rateLimitedPeers := rateLimiter.Size()
	backoffCount := 0
	if storageMgr != nil {
		backoffCount = storageMgr.failedOperations.Size()
	}

	// Track actual concurrent operations (files currently being checked)
	// This is more accurate than reporting the configured worker count
	activeWorkers := 0
	if replicationMgr != nil {
		activeWorkers = replicationMgr.checkingFiles.Size()
	}
	maxWorkers := ReplicationWorkers // Maximum number of worker goroutines
	if activeWorkers < 0 {
		activeWorkers = 0
	}

	metrics.RUnlock()

	levelsMap := make(map[string]int)
	if storageMgr != nil {
		levelsMap = storageMgr.fileReplicationLevels.All()
	}
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
		if count >= MinReplication && count <= MaxReplication {
			filesAtTarget++
		}
	}

	metrics.Lock()
	metrics.replicationDistribution = distribution
	metrics.avgReplicationLevel = avgReplication
	metrics.filesAtTargetReplication = filesAtTarget
	metrics.Unlock()

	log.Printf("[Metrics] === System Metrics Report ===")
	log.Printf("[Metrics] Storage: pinned=%d, known=%d", metrics.pinnedFilesCount, metrics.knownFilesCount)
	log.Printf("[Metrics] Replication: checks=%d, success=%d, failures=%d, low=%d, high=%d, at_target=%d",
		metrics.replicationChecks, metrics.replicationSuccess, metrics.replicationFailures,
		metrics.lowReplicationFiles, metrics.highReplicationFiles, metrics.filesAtTargetReplication)
	log.Printf("[Metrics] Replication Distribution: 0=%d, 1=%d, 2=%d, 3=%d, 4=%d, 5=%d, 6=%d, 7=%d, 8=%d, 9=%d, 10+=%d",
		metrics.replicationDistribution[0], metrics.replicationDistribution[1], metrics.replicationDistribution[2],
		metrics.replicationDistribution[3], metrics.replicationDistribution[4], metrics.replicationDistribution[5],
		metrics.replicationDistribution[6], metrics.replicationDistribution[7], metrics.replicationDistribution[8],
		metrics.replicationDistribution[9], metrics.replicationDistribution[10])
	log.Printf("[Metrics] Convergence: avg_replication=%.2f (shard peers via pubsub), converged_total=%d, converged_this_period=%d",
		metrics.avgReplicationLevel, metrics.filesConvergedTotal, metrics.filesConvergedThisPeriod)
	log.Printf("[Metrics] Network: messages_received=%.1f/min, messages_dropped=%.1f/min, active_peers=%d (shard topic)",
		msgRate, dropRate, activePeers)
	log.Printf("[Metrics] DHT: queries=%d, timeouts=%d", metrics.dhtQueries, metrics.dhtQueryTimeouts)
	log.Printf("[Metrics] Performance: worker_pool_active=%d/%d, checks_rate=%.1f/min",
		activeWorkers, maxWorkers, checkRate)
	log.Printf("[Metrics] System: shard_splits=%d, current_shard=%s, rate_limited_peers=%d, files_in_backoff=%d",
		metrics.shardSplits, currentShard, rateLimitedPeers, backoffCount)

	uptime := now.Sub(metrics.startTime)
	log.Printf("[Metrics] Cumulative (since startup): uptime=%v, msgs=%d (dropped=%d), checks=%d (success=%d, failures=%d), dht_queries=%d (timeouts=%d), shard_splits=%d",
		uptime.Round(time.Second),
		metrics.cumulativeMessagesReceived,
		metrics.cumulativeMessagesDropped,
		metrics.cumulativeReplicationChecks,
		metrics.cumulativeReplicationSuccess,
		metrics.cumulativeReplicationFailures,
		metrics.cumulativeDhtQueries,
		metrics.cumulativeDhtQueryTimeouts,
		metrics.cumulativeShardSplits)
	log.Printf("[Metrics] ================================")

	if MetricsExportPath != "" {
		exportMetricsToFile(now)
	}

	metrics.Lock()
	metrics.lastReportTime = now
	metrics.messagesReceived = 0
	metrics.messagesDropped = 0
	metrics.replicationChecks = 0
	metrics.replicationSuccess = 0
	metrics.replicationFailures = 0
	metrics.dhtQueries = 0
	metrics.dhtQueryTimeouts = 0
	metrics.lowReplicationFiles = 0
	metrics.highReplicationFiles = 0
	metrics.Unlock()
}

func exportMetricsToFile(timestamp time.Time) {
	if MetricsExportPath == "" {
		return
	}

	dir := filepath.Dir(MetricsExportPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[Error] Failed to create metrics export directory: %v", err)
		return
	}

	fileExists := false
	if _, err := os.Stat(MetricsExportPath); err == nil {
		fileExists = true
	}

	file, err := os.OpenFile(MetricsExportPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Error] Failed to open metrics export file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if !fileExists {
		header := []string{
			"timestamp",
			"uptime_seconds",
			"pinned_files",
			"known_files",
			"messages_received",
			"messages_dropped",
			"replication_checks",
			"replication_success",
			"replication_failures",
			"low_replication_files",
			"high_replication_files",
			"files_at_target_replication",
			"repl_level_0",
			"repl_level_1",
			"repl_level_2",
			"repl_level_3",
			"repl_level_4",
			"repl_level_5",
			"repl_level_6",
			"repl_level_7",
			"repl_level_8",
			"repl_level_9",
			"repl_level_10plus",
			"avg_replication_level",
			"files_converged_total",
			"files_converged_this_period",
			"dht_queries",
			"dht_query_timeouts",
			"shard_splits",
			"worker_pool_active",
			"active_peers",
			"rate_limited_peers",
			"files_in_backoff",
			"current_shard",
			"cumulative_messages_received",
			"cumulative_messages_dropped",
			"cumulative_replication_checks",
			"cumulative_replication_success",
			"cumulative_replication_failures",
			"cumulative_dht_queries",
			"cumulative_dht_query_timeouts",
			"cumulative_shard_splits",
		}
		if err := writer.Write(header); err != nil {
			log.Printf("[Error] Failed to write CSV header: %v", err)
			return
		}
	}

	// Compute shard/peer metrics outside of metrics lock to avoid lock ordering issues.
	currentShard, activePeers := getShardInfo()
	rateLimitedPeers := rateLimiter.Size()
	backoffCount := 0
	if storageMgr != nil {
		backoffCount = storageMgr.failedOperations.Size()
	}

	// Track actual concurrent operations (files currently being checked)
	activeWorkers := 0
	if replicationMgr != nil {
		activeWorkers = replicationMgr.checkingFiles.Size()
	}
	if activeWorkers < 0 {
		activeWorkers = 0
	}

	metrics.RLock()
	uptime := timestamp.Sub(metrics.startTime).Seconds()

	record := []string{
		timestamp.Format(time.RFC3339),
		fmt.Sprintf("%.2f", uptime),
		strconv.Itoa(metrics.pinnedFilesCount),
		strconv.Itoa(metrics.knownFilesCount),
		strconv.FormatInt(metrics.messagesReceived, 10),
		strconv.FormatInt(metrics.messagesDropped, 10),
		strconv.FormatInt(metrics.replicationChecks, 10),
		strconv.FormatInt(metrics.replicationSuccess, 10),
		strconv.FormatInt(metrics.replicationFailures, 10),
		strconv.Itoa(metrics.lowReplicationFiles),
		strconv.Itoa(metrics.highReplicationFiles),
		strconv.Itoa(metrics.filesAtTargetReplication),
		strconv.Itoa(metrics.replicationDistribution[0]),
		strconv.Itoa(metrics.replicationDistribution[1]),
		strconv.Itoa(metrics.replicationDistribution[2]),
		strconv.Itoa(metrics.replicationDistribution[3]),
		strconv.Itoa(metrics.replicationDistribution[4]),
		strconv.Itoa(metrics.replicationDistribution[5]),
		strconv.Itoa(metrics.replicationDistribution[6]),
		strconv.Itoa(metrics.replicationDistribution[7]),
		strconv.Itoa(metrics.replicationDistribution[8]),
		strconv.Itoa(metrics.replicationDistribution[9]),
		strconv.Itoa(metrics.replicationDistribution[10]),
		fmt.Sprintf("%.2f", metrics.avgReplicationLevel),
		strconv.FormatInt(metrics.filesConvergedTotal, 10),
		strconv.FormatInt(metrics.filesConvergedThisPeriod, 10),
		strconv.FormatInt(metrics.dhtQueries, 10),
		strconv.FormatInt(metrics.dhtQueryTimeouts, 10),
		strconv.FormatInt(metrics.shardSplits, 10),
		strconv.Itoa(activeWorkers),
		strconv.Itoa(activePeers),
		strconv.Itoa(rateLimitedPeers),
		strconv.Itoa(backoffCount),
		currentShard,
		strconv.FormatInt(metrics.cumulativeMessagesReceived, 10),
		strconv.FormatInt(metrics.cumulativeMessagesDropped, 10),
		strconv.FormatInt(metrics.cumulativeReplicationChecks, 10),
		strconv.FormatInt(metrics.cumulativeReplicationSuccess, 10),
		strconv.FormatInt(metrics.cumulativeReplicationFailures, 10),
		strconv.FormatInt(metrics.cumulativeDhtQueries, 10),
		strconv.FormatInt(metrics.cumulativeDhtQueryTimeouts, 10),
		strconv.FormatInt(metrics.cumulativeShardSplits, 10),
	}
	metrics.RUnlock()

	if err := writer.Write(record); err != nil {
		log.Printf("[Error] Failed to write CSV record: %v", err)
		return
	}
}
