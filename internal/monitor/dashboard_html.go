package monitor

import (
	"embed"
	"net/http"
)

// staticFiles holds the dashboard: static/index.html and its assets.
//
//go:embed static
var staticFiles embed.FS

// handleDashboard serves the dashboard at / and 404 for any other path that
// no other route claims.
func (m *Monitor) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFileFS(w, r, staticFiles, "static/index.html")
}

// staticHandler serves /static/ from the embedded files. Browsers check for
// a new version on each load, so an updated monitor is picked up at once.
func staticHandler() http.Handler {
	files := http.FileServer(http.FS(staticFiles))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}
