package monitor

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
)

const (
	gatewayProxyTimeout = 2 * time.Minute
	gatewayProxyLimit   = 64 << 20 // 64 MiB response cap
)

// handleGateway proxies GET/HEAD /ipfs/<cid>[/path] to the configured Kubo
// gateway, so the browser never needs direct access to Kubo's port 8080.
func (s *Server) handleGateway(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "only GET and HEAD are supported")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/ipfs/")
	root, _, _ := strings.Cut(rest, "/")
	if _, err := cid.Decode(root); err != nil {
		writeError(w, http.StatusBadRequest, "invalid CID")
		return
	}

	upstream := s.indexer.Settings.Gateway() + "/ipfs/" + rest
	if r.URL.RawQuery != "" {
		upstream += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstream, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	client := &http.Client{Timeout: gatewayProxyTimeout}
	resp, err := client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "gateway unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeError(w, http.StatusBadGateway, "gateway returned HTTP "+resp.Status)
		return
	}

	// Large payloads need longer than the server's default write deadline.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(gatewayProxyTimeout))

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if r.Method == http.MethodHead {
		return
	}
	io.Copy(w, io.LimitReader(resp.Body, gatewayProxyLimit))
}
