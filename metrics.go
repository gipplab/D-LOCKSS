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
)

func incrementMetric(metric *int64) {
	metrics.Lock()
	defer metrics.Unlock()
	*metric++
	switch metric {
	case &metrics.messagesReceived:
		metrics.cumulativeMessagesReceived++
	case &metrics.messagesDropped:
		metrics.cumulativeMessagesDropped++
	case &metrics.replicationChecks:
		metrics.cumulativeReplicationChecks++
	case &metrics.replicationSuccess:
		metrics.cumulativeReplicationSuccess++
	case &metrics.replicationFailures:
		metrics.cumulativeReplicationFailures++
	case &metrics.dhtQueries:
		metrics.cumulativeDhtQueries++
	case &metrics.dhtQueryTimeouts:
		metrics.cumulativeDhtQueryTimeouts++
	case &metrics.shardSplits:
		metrics.cumulativeShardSplits++
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
			reportMetrics()
		}
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

	rateLimiter.RLock()
	activePeers := len(rateLimiter.peers)
	rateLimiter.RUnlock()

	failedOperations.RLock()
	backoffCount := len(failedOperations.hashes)
	failedOperations.RUnlock()

	activeWorkers := MaxConcurrentReplicationChecks - len(replicationWorkers)
	if activeWorkers < 0 {
		activeWorkers = 0
	}

	var currentShard string
	if shardMgr != nil {
		shardMgr.mu.RLock()
		currentShard = shardMgr.currentShard
		shardMgr.mu.RUnlock()
	}

	metrics.RUnlock()

	fileReplicationLevels.RLock()
	distribution := [11]int{}
	totalFiles := 0
	totalReplication := 0
	for _, count := range fileReplicationLevels.levels {
		if count >= 10 {
			distribution[10]++
		} else {
			distribution[count]++
		}
		totalFiles++
		totalReplication += count
	}
	fileReplicationLevels.RUnlock()

	avgReplication := 0.0
	if totalFiles > 0 {
		avgReplication = float64(totalReplication) / float64(totalFiles)
	}

	filesAtTarget := 0
	fileReplicationLevels.RLock()
	for _, count := range fileReplicationLevels.levels {
		if count >= MinReplication && count <= MaxReplication {
			filesAtTarget++
		}
	}
	fileReplicationLevels.RUnlock()

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
	log.Printf("[Metrics] Convergence: avg_replication=%.2f, converged_total=%d, converged_this_period=%d",
		metrics.avgReplicationLevel, metrics.filesConvergedTotal, metrics.filesConvergedThisPeriod)
	log.Printf("[Metrics] Network: messages_received=%.1f/min, messages_dropped=%.1f/min, active_peers=%d",
		msgRate, dropRate, activePeers)
	log.Printf("[Metrics] DHT: queries=%d, timeouts=%d", metrics.dhtQueries, metrics.dhtQueryTimeouts)
	log.Printf("[Metrics] Performance: worker_pool_active=%d/%d, checks_rate=%.1f/min",
		activeWorkers, MaxConcurrentReplicationChecks, checkRate)
	log.Printf("[Metrics] System: shard_splits=%d, current_shard=%s, rate_limited_peers=%d, files_in_backoff=%d",
		metrics.shardSplits, currentShard, activePeers, backoffCount)

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

	metrics.RLock()
	uptime := timestamp.Sub(metrics.startTime).Seconds()

	var currentShard string
	if shardMgr != nil {
		shardMgr.mu.RLock()
		currentShard = shardMgr.currentShard
		shardMgr.mu.RUnlock()
	}

	rateLimiter.RLock()
	rateLimitedPeers := len(rateLimiter.peers)
	rateLimiter.RUnlock()

	failedOperations.RLock()
	backoffCount := len(failedOperations.hashes)
	failedOperations.RUnlock()

	activeWorkers := MaxConcurrentReplicationChecks - len(replicationWorkers)
	if activeWorkers < 0 {
		activeWorkers = 0
	}

	activePeers := rateLimitedPeers

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
