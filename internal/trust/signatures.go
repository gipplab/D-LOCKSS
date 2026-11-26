package trust

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

// WikiSignatures fetches the signatures that users of a wiki made for a
// file: GET {base}/math/v0/signatures/{cid} answers a JSON array of
// {"key": peer ID, "signature": output of 'ipfs key sign'}, signed over
// "dlockss-upload <cid>" (see schema.VerifyUpload).
type WikiSignatures struct {
	base   string
	client *http.Client
}

// NewWikiSignatures returns a WikiSignatures for the REST API at base, e.g.
// https://portal.mardi4nfdi.de/w/rest.php.
func NewWikiSignatures(base string) *WikiSignatures {
	return &WikiSignatures{
		base:   strings.TrimSuffix(base, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Signatures returns the valid signatures of payload, sorted by key, one
// per key. Entries that do not verify are logged and left out.
func (w *WikiSignatures) Signatures(ctx context.Context, payload cid.Cid) ([]schema.UploaderSignature, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+"/math/v0/signatures/"+payload.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wiki answered %s", resp.Status)
	}
	var entries []struct {
		Key       string `json:"key"`
		Signature string `json:"signature"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&entries); err != nil {
		return nil, fmt.Errorf("wiki answer: %w", err)
	}
	var out []schema.UploaderSignature
	for _, e := range entries {
		u, err := parseSignature(payload, e.Key, e.Signature)
		if err != nil {
			slog.Warn("wiki signature skipped", "payload", payload, "key", e.Key, "error", err)
			continue
		}
		out = append(out, u)
	}
	// The manifest CID must not depend on the order of the wiki's answer.
	slices.SortFunc(out, func(a, b schema.UploaderSignature) int { return strings.Compare(string(a.Key), string(b.Key)) })
	return slices.CompactFunc(out, func(a, b schema.UploaderSignature) bool {
		return a.Key == b.Key && bytes.Equal(a.Sig, b.Sig)
	}), nil
}

func parseSignature(payload cid.Cid, key, sig string) (schema.UploaderSignature, error) {
	id, err := peer.Decode(key)
	if err != nil {
		return schema.UploaderSignature{}, err
	}
	sig, ok := strings.CutPrefix(sig, "u")
	raw, err := base64.RawURLEncoding.DecodeString(sig)
	if !ok || err != nil {
		return schema.UploaderSignature{}, fmt.Errorf("not the Signature of 'ipfs key sign' (multibase base64url)")
	}
	if err := schema.VerifyUpload(id, payload, raw); err != nil {
		return schema.UploaderSignature{}, err
	}
	return schema.UploaderSignature{Key: id, Sig: raw}, nil
}
