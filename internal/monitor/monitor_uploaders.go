package monitor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"time"

	"dlockss/pkg/schema"
)

const (
	uploaderScanEvery = 30 * time.Second
	uploaderScanBatch = 50
)

// RunUploaderIndex reads each announced manifest once and records its
// uploader ("" for manifests without a valid user signature), so the files
// of a key can be listed, e.g. by a node that a user moves to.
func (m *Monitor) RunUploaderIndex(ctx context.Context) {
	ticker := time.NewTicker(uploaderScanEvery)
	defer ticker.Stop()
	for {
		m.scanUploaders(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Monitor) scanUploaders(ctx context.Context) {
	m.mu.RLock()
	var todo []string
	for c := range m.uniqueCIDs {
		if _, done := m.manifestUploader[c]; !done {
			todo = append(todo, c)
		}
		if len(todo) == uploaderScanBatch {
			break
		}
	}
	m.mu.RUnlock()
	for _, c := range todo {
		ro, err := m.fetchManifest(ctx, c)
		if err != nil {
			slog.Debug("uploader index: cannot read manifest", "manifest", c, "error", err)
			continue
		}
		// Anyone can write a key into a manifest; only a signature that
		// verifies puts the file under it.
		uploader := ""
		if ro.Uploader != "" {
			if err := schema.VerifyUpload(ro.Uploader, ro.Payload, ro.UploaderSig); err != nil {
				slog.Warn("uploader index: signature does not verify, file not listed under its key", "manifest", c, "key", ro.Uploader, "error", err)
			} else {
				uploader = ro.Uploader.String()
			}
		}
		m.mu.Lock()
		if _, ok := m.uniqueCIDs[c]; ok {
			m.manifestUploader[c] = uploader
		}
		m.mu.Unlock()
	}
}

// fetchManifest reads a manifest through the IPFS gateway.
func (m *Monitor) fetchManifest(ctx context.Context, manifestCID string) (*schema.ResearchObject, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.keywords.Gateway()+"/ipfs/"+url.PathEscape(manifestCID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway: %s", resp.Status)
	}
	block, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	return &ro, nil
}

type uploaderEntry struct {
	Key   string `json:"key"`
	Files int    `json:"files"`
}

func (m *Monitor) handleUploaders(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	counts := make(map[string]int)
	for _, key := range m.manifestUploader {
		if key != "" {
			counts[key]++
		}
	}
	m.mu.RUnlock()
	entries := make([]uploaderEntry, 0, len(counts))
	for key, n := range counts {
		entries = append(entries, uploaderEntry{Key: key, Files: n})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Files != entries[j].Files {
			return entries[i].Files > entries[j].Files
		}
		return entries[i].Key < entries[j].Key
	})
	writeJSON(w, map[string]interface{}{"uploaders": entries, "count": len(entries)})
}

func (m *Monitor) handleUploaderFiles(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSONError(w, "missing key parameter", http.StatusBadRequest)
		return
	}
	m.mu.RLock()
	files := make(map[string]time.Time)
	for c, uploader := range m.manifestUploader {
		if uploader == key {
			files[c] = m.uniqueCIDs[c]
		}
	}
	entries := m.buildCIDEntriesLocked(files)
	m.mu.RUnlock()
	writeJSON(w, map[string]interface{}{"key": key, "cids": entries, "count": len(entries)})
}
