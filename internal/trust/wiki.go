package trust

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Wiki is the MathSearch REST API of a wiki: which keys it knows (Trusted)
// and which of them signed a file (Signatures). Answers are not cached;
// while the wiki cannot be reached, no key is accepted and no signature is
// found.
type Wiki struct {
	base   string
	client *http.Client
}

// NewWiki returns a Wiki for the REST API at base, e.g.
// https://portal.mardi4nfdi.de/w/rest.php.
func NewWiki(base string) *Wiki {
	return &Wiki{
		base:   strings.TrimSuffix(base, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// get requests path below the API. The caller closes the body with drain.
func (w *Wiki) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+path, nil)
	if err != nil {
		return nil, err
	}
	return w.client.Do(req)
}

// drain reads what is left of body and closes it, so that the connection
// is used again for the next request.
func drain(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
	body.Close()
}

// Trusted reports whether the wiki knows key, i.e. a user registered it:
// GET {base}/math/v0/key/{peer ID} answers 204 if it does, and 404 if not or
// 400 for a key that is not Ed25519.
func (w *Wiki) Trusted(ctx context.Context, key peer.ID) (bool, error) {
	resp, err := w.get(ctx, "/math/v0/key/"+key.String())
	if err != nil {
		return false, err
	}
	defer drain(resp.Body)
	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound, http.StatusBadRequest:
		return false, nil
	}
	return false, fmt.Errorf("wiki answered %s", resp.Status)
}
