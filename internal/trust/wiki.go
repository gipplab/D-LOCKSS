package trust

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// wikiKeyTTL is how long an answer of the wiki is used without asking again.
const wikiKeyTTL = 5 * time.Minute

// WikiKeys asks the MathSearch REST API of a wiki whether a key is trusted:
// GET {base}/math/v0/key/{peer ID} answers 204 if it is and 404 if not.
// While the wiki cannot be reached, its last answer for the key is used.
type WikiKeys struct {
	base   string
	client *http.Client

	mu      sync.Mutex
	answers map[peer.ID]wikiAnswer
}

type wikiAnswer struct {
	trusted bool
	at      time.Time
}

// NewWikiKeys returns a WikiKeys for the REST API at base, e.g.
// https://portal.mardi4nfdi.de/w/rest.php.
func NewWikiKeys(base string) *WikiKeys {
	return &WikiKeys{
		base:    strings.TrimSuffix(base, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
		answers: make(map[peer.ID]wikiAnswer),
	}
}

// Trusted reports whether the wiki trusts key.
func (w *WikiKeys) Trusted(ctx context.Context, key peer.ID) (bool, error) {
	w.mu.Lock()
	last, known := w.answers[key]
	w.mu.Unlock()
	if known && time.Since(last.at) < wikiKeyTTL {
		return last.trusted, nil
	}
	trusted, err := w.ask(ctx, key)
	if err != nil {
		if known {
			slog.Warn("wiki unreachable, using its last answer", "key", key, "answered", last.at, "error", err)
			return last.trusted, nil
		}
		return false, err
	}
	w.mu.Lock()
	w.answers[key] = wikiAnswer{trusted: trusted, at: time.Now()}
	w.mu.Unlock()
	return trusted, nil
}

func (w *WikiKeys) ask(ctx context.Context, key peer.ID) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+"/math/v0/key/"+key.String(), nil)
	if err != nil {
		return false, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("wiki answered %s", resp.Status)
}
