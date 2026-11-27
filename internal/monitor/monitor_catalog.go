package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

const (
	catalogFileName   = "manifest_catalog.json"
	catalogScanEvery  = 5 * time.Second
	catalogSaveEvery  = time.Minute
	catalogScanBatch  = 50
	catalogWorkers    = 4
	catalogRetryAfter = 5 * time.Minute
)

// manifestInfo is what a manifest says about its file. Manifests never
// change, so entries are kept for good.
type manifestInfo struct {
	Payload  string `json:"payload"`
	Ingester string `json:"ingester"`
	// Uploader is the user's key, set only if its signature verifies.
	Uploader string `json:"uploader,omitempty"`
	// Legacy manifests carry a timestamp; nodes ignore them.
	Legacy bool `json:"legacy,omitempty"`
}

// manifestCatalog maps manifest CIDs to what they say. Its lock is taken
// while Monitor.mu may be held, never the other way round.
type manifestCatalog struct {
	mu      sync.RWMutex
	entries map[string]manifestInfo
	failed  map[string]time.Time
	path    string
	dirty   bool
}

func newManifestCatalog(dataDir string) *manifestCatalog {
	c := &manifestCatalog{entries: make(map[string]manifestInfo), failed: make(map[string]time.Time)}
	if dataDir != "" {
		c.path = filepath.Join(dataDir, catalogFileName)
		c.load()
	}
	return c
}

func (c *manifestCatalog) get(manifest string) (manifestInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.entries[manifest]
	return info, ok
}

func (c *manifestCatalog) put(manifest string, info manifestInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[manifest] = info
	delete(c.failed, manifest)
	c.dirty = true
}

func (c *manifestCatalog) fail(manifest string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed[manifest] = now
}

// todo returns up to limit of candidates that the catalog does not know and
// that did not fail recently.
func (c *manifestCatalog) todo(candidates []string, now time.Time, limit int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for _, m := range candidates {
		if _, ok := c.entries[m]; ok {
			continue
		}
		if t, ok := c.failed[m]; ok && now.Sub(t) < catalogRetryAfter {
			continue
		}
		out = append(out, m)
		if len(out) == limit {
			break
		}
	}
	return out
}

func (c *manifestCatalog) load() {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("manifest catalog: cannot read", "path", c.path, "error", err)
		}
		return
	}
	if err := json.Unmarshal(data, &c.entries); err != nil {
		slog.Warn("manifest catalog: cannot parse", "path", c.path, "error", err)
		c.entries = make(map[string]manifestInfo)
		return
	}
	slog.Info("manifest catalog loaded", "manifests", len(c.entries))
}

// save writes the catalog if it changed since the last save.
func (c *manifestCatalog) save() {
	c.mu.Lock()
	dirty := c.dirty
	c.dirty = false
	c.mu.Unlock()
	if c.path == "" || !dirty {
		return
	}
	c.mu.RLock()
	data, err := json.Marshal(c.entries)
	c.mu.RUnlock()
	if err == nil {
		err = common.WriteFileAtomic(c.path, data)
	}
	if err != nil {
		slog.Error("manifest catalog: cannot save", "path", c.path, "error", err)
	}
}

// manifestInfoOf returns what ro says. Its uploader counts only if the
// signature verifies: anyone can write a key into a manifest.
func manifestInfoOf(manifest string, ro *schema.ResearchObject) manifestInfo {
	info := manifestInfo{Payload: ro.Payload.String(), Ingester: ro.IngestedBy.String(), Legacy: ro.HasLegacyTimestamp}
	if ro.Uploader != "" {
		if err := schema.VerifyUpload(ro.Uploader, ro.Payload, ro.UploaderSig); err != nil {
			slog.Warn("manifest catalog: signature does not verify, uploader ignored", "manifest", manifest, "key", ro.Uploader, "error", err)
		} else {
			info.Uploader = ro.Uploader.String()
		}
	}
	return info
}

// PayloadOf returns the payload CID of a manifest if the catalog knows it.
func (m *Monitor) PayloadOf(manifest string) (string, bool) {
	info, ok := m.catalog.get(manifest)
	if !ok || info.Legacy {
		return "", false
	}
	return info.Payload, true
}

// RunCatalog reads the announced manifests that the catalog does not know
// yet, a batch every few seconds, and saves the catalog once a minute.
func (m *Monitor) RunCatalog(ctx context.Context) {
	scan := time.NewTicker(catalogScanEvery)
	defer scan.Stop()
	save := time.NewTicker(catalogSaveEvery)
	defer save.Stop()
	for {
		select {
		case <-ctx.Done():
			m.catalog.save()
			return
		case <-scan.C:
			m.scanCatalog(ctx)
		case <-save.C:
			m.catalog.save()
		}
	}
}

func (m *Monitor) scanCatalog(ctx context.Context) {
	todo := m.catalog.todo(m.UniqueCIDList(), time.Now(), catalogScanBatch)
	if len(todo) == 0 {
		return
	}
	work := make(chan string)
	var wg sync.WaitGroup
	for range catalogWorkers {
		wg.Go(func() {
			for manifest := range work {
				ro, err := m.keywords.FetchManifest(manifest)
				if err != nil {
					slog.Debug("manifest catalog: cannot read manifest", "manifest", manifest, "error", err)
					m.catalog.fail(manifest, time.Now())
					continue
				}
				m.catalog.put(manifest, manifestInfoOf(manifest, ro))
			}
		})
	}
feed:
	for _, manifest := range todo {
		select {
		case work <- manifest:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
}
