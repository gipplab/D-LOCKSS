// Package api exposes the node's small observability HTTP endpoint.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Server is the observability HTTP server: a liveness check, nothing more.
type Server struct {
	srv *http.Server
}

// New builds the server on the given port (all interfaces).
func New(port int) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	return &Server{srv: &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}}
}

// Start serves in the background.
func (s *Server) Start() {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("api server stopped", "err", err)
		}
	}()
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }
