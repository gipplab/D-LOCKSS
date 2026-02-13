// Command dlockss-monitor runs the D-LOCKSS network monitor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if v := os.Getenv("DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			nodeCleanupTimeout = d
			log.Printf("[Monitor] Node cleanup timeout: %s (from env)", nodeCleanupTimeout)
		}
	}
	if v := os.Getenv("DLOCKSS_MONITOR_BOOTSTRAP_SHARD_DEPTH"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d >= 0 && d <= 12 {
			bootstrapShardDepth = d
			log.Printf("[Monitor] Bootstrap shard depth: %d (from env)", bootstrapShardDepth)
		}
	}

	monitor := NewMonitor()
	h, err := startLibP2P(ctx, monitor)
	if err != nil {
		log.Fatalf("P2P error: %v", err)
	}
	defer h.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, r *http.Request) {
		monitor.PruneStaleNodes()
		// Build snapshot under lock; release before calling getPinnedInShardForNode to avoid deadlock (getPinnedInShardForNode takes RLock internally).
		monitor.mu.RLock()
		shardCounts := make(map[string]int)
		type nodeSnap struct {
			id            string
			peerID        string
			currentShard  string
			knownFiles    int
			lastSeen      int64
			region        string
			shard         string
			peersInShard  int
			uptimeSeconds float64
			pinnedFiles   int
		}
		var snapshot []nodeSnap
		for _, node := range monitor.nodes {
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			shardCounts[shard]++
		}
		query := strings.ToLower(r.URL.Query().Get("q"))
		for id, node := range monitor.nodes {
			if query != "" {
				match := strings.Contains(strings.ToLower(id), query) ||
					strings.Contains(strings.ToLower(node.Region), query) ||
					strings.Contains(strings.ToLower(node.CurrentShard), query)
				if !match {
					continue
				}
			}
			shard := node.CurrentShard
			if shard == "" && len(node.ShardHistory) > 0 {
				shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
			}
			peersInShard := shardCounts[shard]
			if peersInShard < 1 {
				peersInShard = 1
			}
			firstSeen := node.LastSeen
			if len(node.ShardHistory) > 0 {
				firstSeen = node.ShardHistory[0].FirstSeen
			}
			uptimeSeconds := time.Since(firstSeen).Seconds()
			pinnedFiles := node.PinnedFiles
			if pinnedFiles < 0 {
				pinnedFiles = 0
			}
			snapshot = append(snapshot, nodeSnap{
				id: id, peerID: node.PeerID, currentShard: node.CurrentShard, knownFiles: node.KnownFiles,
				lastSeen: node.LastSeen.Unix(), region: node.Region,
				shard: shard, peersInShard: peersInShard, uptimeSeconds: uptimeSeconds, pinnedFiles: pinnedFiles,
			})
		}
		monitor.mu.RUnlock()

		response := make(map[string]interface{})
		for _, s := range snapshot {
			pinnedInShard := monitor.getPinnedInShardForNode(s.id, s.shard)
			status := StatusResponse{
				PeerID:        s.peerID,
				Version:       "1.0.0",
				CurrentShard:  s.currentShard,
				PeersInShard:  s.peersInShard,
				Storage:       StorageStatus{PinnedFiles: s.pinnedFiles, PinnedInShard: pinnedInShard, KnownFiles: s.knownFiles, KnownCIDs: []string{}},
				Replication:   ReplicationStatus{},
				UptimeSeconds: s.uptimeSeconds,
			}
			response[s.id] = map[string]interface{}{
				"data":      status,
				"last_seen": s.lastSeen,
				"region":    s.region,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	mux.HandleFunc("/api/shard-tree", func(w http.ResponseWriter, r *http.Request) {
		tree := monitor.GetShardTree()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tree)
	})

	mux.HandleFunc("/api/root-topic", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var body struct {
				TopicPrefix string `json:"topic_prefix"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, `{"error":"invalid JSON, expected {\"topic_prefix\":\"...\"}"}`, http.StatusBadRequest)
				return
			}
			monitor.SwitchTopicPrefix(ctx, body.TopicPrefix)
			rootTopic := fmt.Sprintf("%s-creative-commons-shard-", monitor.getTopicPrefix())
			json.NewEncoder(w).Encode(map[string]string{"root_topic": rootTopic, "topic_prefix": monitor.getTopicPrefix()})
			return
		}
		rootTopic := fmt.Sprintf("%s-creative-commons-shard-", monitor.getTopicPrefix())
		json.NewEncoder(w).Encode(map[string]string{"root_topic": rootTopic, "topic_prefix": monitor.getTopicPrefix()})
	})

	mux.HandleFunc("/api/unique-cids", func(w http.ResponseWriter, r *http.Request) {
		monitor.mu.RLock()
		count := len(monitor.uniqueCIDs)
		monitor.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"count": count})
	})

	mux.HandleFunc("/api/replication", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		dist, avg, atTarget := monitor.getReplicationStats()
		byShard := monitor.getReplicationByShard()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"replication_distribution":  dist,
			"avg_replication_level":     avg,
			"files_at_target":           atTarget,
			"files_at_target_per_shard": byShard,
			"replication_note":          "Counts are network-wide (all shards). Nodes unpin files that no longer belong to their shard after a split.",
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(dashboardHTML))
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", WebUIPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				monitor.mu.RLock()
				nodeCount := len(monitor.nodes)
				shardCounts := make(map[string]int)
				totalPinned := 0
				for _, node := range monitor.nodes {
					shard := node.CurrentShard
					if shard == "" && len(node.ShardHistory) > 0 {
						shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
					}
					shardCounts[shard]++
					if node.PinnedFiles > 0 {
						totalPinned += node.PinnedFiles
					}
				}
				monitor.mu.RUnlock()
				shardIDs := make([]string, 0, len(shardCounts))
				for sid := range shardCounts {
					shardIDs = append(shardIDs, sid)
				}
				sort.Strings(shardIDs)
				parts := make([]string, 0, len(shardIDs))
				for _, sid := range shardIDs {
					label := sid
					if label == "" {
						label = "root"
					}
					parts = append(parts, fmt.Sprintf("%s: %d", label, shardCounts[sid]))
				}
				log.Printf("[Monitor] Status: %d nodes, %d shards, %d pinned (%s)", nodeCount, len(shardCounts), totalPinned, strings.Join(parts, ", "))
			}
		}
	}()

	go func() {
		log.Printf("[Monitor] UI: http://localhost:%d | PeerID: %s", WebUIPort, h.ID())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[Error] HTTP server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP Shutdown Error: %v", err)
	}
}
