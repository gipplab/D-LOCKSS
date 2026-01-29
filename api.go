package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// APIServer manages the local observability API
type APIServer struct {
	server *http.Server
}

func NewAPIServer(port int) *APIServer {
	mux := http.NewServeMux()
	
	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
	
	// Status endpoint
	mux.HandleFunc("/status", handleStatus)
	
	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return &APIServer{
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}
}

func (s *APIServer) Start() {
	go func() {
		log.Printf("[API] Starting observability server on %s", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Error] API server failed: %v", err)
		}
	}()
}

func (s *APIServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// StatusResponse defines the JSON structure for /status
type StatusResponse struct {
	PeerID          string  `json:"peer_id"`
	Version         string  `json:"version"`
	CurrentShard    string  `json:"current_shard"`
	PeersInShard    int     `json:"peers_in_shard"`
	Storage         StorageStatus `json:"storage"`
	Replication     ReplicationStatus `json:"replication"`
	UptimeSeconds   float64 `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles int `json:"pinned_files"`
	KnownFiles  int `json:"known_files"`
}

type ReplicationStatus struct {
	QueueDepth      int `json:"queue_depth"`
	ActiveWorkers   int `json:"active_workers"`
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	metrics.RLock()
	pinned := metrics.pinnedFilesCount
	known := metrics.knownFilesCount
	startTime := metrics.startTime
	metrics.RUnlock()
	
	shardID, peers := getShardInfo()
	
	activeWorkers := 0
	queueDepth := 0
	if replicationMgr != nil {
		activeWorkers = replicationMgr.checkingFiles.Size()
		// TODO: Expose queue depth from replication manager
	}

	status := StatusResponse{
		PeerID:       selfPeerID.String(),
		Version:      "1.0.0", // TODO: Get from build info
		CurrentShard: shardID,
		PeersInShard: peers,
		Storage: StorageStatus{
			PinnedFiles: pinned,
			KnownFiles:  known,
		},
		Replication: ReplicationStatus{
			QueueDepth:    queueDepth,
			ActiveWorkers: activeWorkers,
		},
		UptimeSeconds: time.Since(startTime).Seconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
