package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
)

// APIServer manages the local observability API
type APIServer struct {
	server *http.Server
}

func NewAPIServer(port int) *APIServer {
	s := &APIServer{}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/", s.handleDashboard)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return s
}

func (s *APIServer) Start() {
	go func() {
		slog.Info("starting observability server", "addr", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("api server failed", "error", err)
		}
	}()
}

func (s *APIServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *APIServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>D-LOCKSS Node</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
    h1 { color: #333; }
    a { color: #0066cc; }
  </style>
</head>
<body>
  <h1>D-LOCKSS Node</h1>
  <p>This node is running. Use the <strong>D-LOCKSS Monitor</strong> for network-wide observability.</p>
  <p><a href="/health">Health check</a></p>
</body>
</html>
`
