package trust

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

// wikiSignature is an entry of the wiki's answer: a peer ID and the output
// of 'ipfs key sign' for "dlockss-upload <cid>" (see schema.VerifyUpload).
type wikiSignature struct {
	Key       string `json:"key"`
	Signature string `json:"signature"`
}

// Signatures returns the valid signatures that users of the wiki made for
// payload, sorted by key, one per key: GET {base}/math/v0/signatures/{cid}
// answers a JSON array of wikiSignature. The wiki answers only with keys it
// knows, so every signature that verifies counts; entries that do not
// verify are logged and left out. Answers 400 (a CID the wiki cannot sign,
// e.g. CIDv0) and 404 (the wiki lacks the route) mean that nobody signed;
// other answers are errors.
func (w *Wiki) Signatures(ctx context.Context, payload cid.Cid) ([]schema.UploaderSignature, error) {
	resp, err := w.get(ctx, "/math/v0/signatures/"+payload.String())
	if err != nil {
		return nil, err
	}
	defer drain(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusBadRequest, http.StatusNotFound:
		slog.Debug("wiki has no signatures for the payload", "payload", payload, "status", resp.Status)
		return nil, nil
	default:
		return nil, fmt.Errorf("wiki answered %s", resp.Status)
	}
	var entries []wikiSignature
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&entries); err != nil {
		return nil, fmt.Errorf("wiki answer: %w", err)
	}
	var out []schema.UploaderSignature
	for _, e := range entries {
		u, err := parseSignature(payload, e)
		if err != nil {
			slog.Warn("wiki signature skipped", "payload", payload, "key", e.Key, "error", err)
			continue
		}
		out = append(out, u)
	}
	// Each key gets one manifest, so which of its signatures is kept must not
	// depend on the order of the wiki's answer.
	slices.SortFunc(out, func(a, b schema.UploaderSignature) int {
		if c := strings.Compare(string(a.Key), string(b.Key)); c != 0 {
			return c
		}
		return bytes.Compare(a.Sig, b.Sig)
	})
	return slices.CompactFunc(out, func(a, b schema.UploaderSignature) bool { return a.Key == b.Key }), nil
}

func parseSignature(payload cid.Cid, e wikiSignature) (schema.UploaderSignature, error) {
	id, err := peer.Decode(e.Key)
	if err != nil {
		return schema.UploaderSignature{}, err
	}
	raw, err := schema.DecodeKuboSignature(e.Signature)
	if err != nil {
		return schema.UploaderSignature{}, err
	}
	if err := schema.VerifyUpload(id, payload, raw); err != nil {
		return schema.UploaderSignature{}, err
	}
	return schema.UploaderSignature{Key: id, Sig: raw}, nil
}
