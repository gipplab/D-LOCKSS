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

	// Dashboard UI (simple HTML)
	mux.HandleFunc("/", s.handleDashboard)

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
  <title>D-LOCKSS Monitor</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
    h1 { color: #333; }
    .card { background: #f5f5f5; border-radius: 8px; padding: 1rem; margin: 1rem 0; }
    .card h2 { margin-top: 0; font-size: 1rem; color: #666; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 0.25rem 0.5rem 0.25rem 0; }
    th { color: #666; font-weight: 500; }
    a { color: #0066cc; }
    #status { color: #666; font-size: 0.9rem; }
    .links { margin-top: 1.5rem; }
  </style>
</head>
<body>
  <h1>D-LOCKSS Monitor</h1>
  <p id="status">Loading status…</p>
  <div class="card">
    <h2>Storage</h2>
    <table><tbody>
      <tr><th>Pinned files</th><td id="pinned">–</td></tr>
      <tr><th>Known files</th><td id="known">–</td></tr>
    </tbody></table>
  </div>
  <div class="card">
    <h2>Shard &amp; replication</h2>
    <table><tbody>
      <tr><th>Current shard</th><td id="shard">–</td></tr>
      <tr><th>Peers in shard</th><td id="peers">–</td></tr>
      <tr><th>Queue depth</th><td id="queue">–</td></tr>
      <tr><th>Active workers</th><td id="workers">–</td></tr>
      <tr><th>Uptime</th><td id="uptime">–</td></tr>
    </tbody></table>
  </div>
  <div class="links">
    <a href="/status">JSON status</a> · <a href="/metrics">Prometheus metrics</a> · <a href="/health">Health</a>
  </div>
  <script>
    fetch('/status').then(r => r.json()).then(d => {
      document.getElementById('pinned').textContent = d.storage?.pinned_files ?? '–';
      document.getElementById('known').textContent = d.storage?.known_files ?? '–';
      document.getElementById('shard').textContent = d.current_shard || '(none)';
      document.getElementById('peers').textContent = d.peers_in_shard ?? '–';
      document.getElementById('queue').textContent = d.replication?.queue_depth ?? '–';
      document.getElementById('workers').textContent = d.replication?.active_workers ?? '–';
      var u = d.uptime_seconds; document.getElementById('uptime').textContent = u != null ? (Math.floor(u/60) + 'm ' + Math.floor(u%60) + 's') : '–';
      document.getElementById('status').textContent = 'Live data from this server. If this is dlockss-monitor only (no node), values are zero until you scrape a node or use the node’s API.';
    }).catch(function() {
      document.getElementById('status').textContent = 'Could not load /status.';
    });
  </script>
</body>
</html>
`
