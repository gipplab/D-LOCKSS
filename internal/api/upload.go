package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/fileops"
	"dlockss/pkg/schema"
)

// KeyTrust decides whose uploads the node accepts.
type KeyTrust interface {
	Trusted(ctx context.Context, key peer.ID) (bool, error)
}

// Ingester ingests an uploaded file.
type Ingester interface {
	IngestUpload(ctx context.Context, path, metaRef string, payload cid.Cid, uploader peer.ID, sig []byte) (cid.Cid, error)
}

// UploadHandler serves POST /upload?cid=…&key=…&sig=…[&name=…] with the file
// as the body. sig is the output of
// 'ipfs key sign -k <key>' for the message "dlockss-upload <cid>". The
// signature and the key are checked before the body is read.
func UploadHandler(trust KeyTrust, ingester Ingester) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		payload, err := cid.Decode(q.Get("cid"))
		if err != nil {
			http.Error(w, "cid: "+err.Error(), http.StatusBadRequest)
			return
		}
		key, err := peer.Decode(q.Get("key"))
		if err != nil {
			http.Error(w, "key: "+err.Error(), http.StatusBadRequest)
			return
		}
		rawSig, err := schema.DecodeKuboSignature(q.Get("sig"))
		if err != nil {
			http.Error(w, "sig: want the Signature of 'ipfs key sign' (multibase base64url)", http.StatusBadRequest)
			return
		}
		if err := schema.VerifyUpload(key, payload, rawSig); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		trusted, err := trust.Trusted(r.Context(), key)
		if err != nil {
			http.Error(w, "cannot check the key: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !trusted {
			http.Error(w, "the key "+key.String()+" is not trusted", http.StatusForbidden)
			return
		}

		path, err := saveBody(r.Body)
		if err != nil {
			http.Error(w, "reading the file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer os.Remove(path)
		metaRef := "ipfs://" + payload.String()
		if name := filepath.Base(q.Get("name")); q.Get("name") != "" {
			metaRef = name
		}
		manifest, err := ingester.IngestUpload(r.Context(), path, metaRef, payload, key, rawSig)
		if errors.Is(err, fileops.ErrPayloadMismatch) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err != nil {
			slog.Error("upload failed", "uploader", key, "payload", payload, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"manifest": manifest.String(), "payload": payload.String()})
	})
}

func saveBody(body io.Reader) (string, error) {
	f, err := os.CreateTemp("", "dlockss-upload-*")
	if err != nil {
		return "", err
	}
	_, err = io.Copy(f, body)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}
