package trust

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// WikiKeys asks the MathSearch REST API of a wiki whether a key is trusted:
// GET {base}/math/v0/key/{peer ID} answers 204 if it is and 404 if not.
// Answers are not cached; while the wiki cannot be reached, no key is
// trusted.
type WikiKeys struct {
	base   string
	client *http.Client
}

// NewWikiKeys returns a WikiKeys for the REST API at base, e.g.
// https://portal.mardi4nfdi.de/w/rest.php.
func NewWikiKeys(base string) *WikiKeys {
	return &WikiKeys{
		base:   strings.TrimSuffix(base, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Trusted reports whether the wiki trusts key.
func (w *WikiKeys) Trusted(ctx context.Context, key peer.ID) (bool, error) {
	return w.ask(ctx, key)
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
