package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"dlockss/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// APIServer manages the local observability API
type APIServer struct {
	server  *http.Server
	metrics *telemetry.MetricsManager
}

func NewAPIServer(port int, metrics *telemetry.MetricsManager) *APIServer {
	s := &APIServer{
		metrics: metrics,
	}

	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Status endpoint
	mux.HandleFunc("/status", s.handleStatus)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return s
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

func (s *APIServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		http.Error(w, "Metrics not initialized", http.StatusInternalServerError)
		return
	}

	status := s.metrics.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
