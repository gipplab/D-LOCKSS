package monitor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
)

const gatewayProxyLimit = 64 << 20

// handleIPFSGateway serves /ipfs/<cid> by reading that path from the configured
// Kubo gateway. The browser never talks to Kubo directly.
func (m *Monitor) handleIPFSGateway(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ipfs/"), "/")
	if rest == "" {
		writeJSONError(w, "missing cid", http.StatusBadRequest)
		return
	}
	root := rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		root = rest[:i]
	}
	if _, err := cid.Decode(root); err != nil {
		writeJSONError(w, "invalid cid", http.StatusBadRequest)
		return
	}

	upstream := m.keywords.Gateway() + "/ipfs/" + rest
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, upstream, nil)
	if err != nil {
		writeJSONError(w, "bad gateway url", http.StatusBadGateway)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeJSONError(w, "gateway: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeJSONError(w, "gateway: "+resp.Status, resp.StatusCode)
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Now().Add(2 * time.Minute))
	}
	_, _ = io.Copy(w, io.LimitReader(resp.Body, gatewayProxyLimit))
}
