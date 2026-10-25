package ingest

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The folder is ingested by periodic rescan rather than filesystem events:
// a file is picked up once its size and mtime are unchanged across two
// consecutive scans, which doubles as the write-stability check. Archival
// ingest does not need sub-second pickup, and a rescan has none of the
// moving parts of a recursive watcher (per-directory watches, event dedup,
// stability timers, overflow queues).
const (
	scanInterval    = 30 * time.Second
	scanRetryAfter  = 5 * time.Minute // back off failed ingests
	ingestWorkers   = 5
)

type fileState struct {
	size     int64
	modTime  time.Time
	ingested bool
	retryAt  time.Time // zero unless the last ingest failed
}

// WatchFolder rescans the data directory until ctx ends. The first scan only
// records candidates; files are ingested from the second scan on, once
// stable.
func (p *Processor) WatchFolder(ctx context.Context) {
	if err := os.MkdirAll(p.cfg.FileWatchFolder, 0o755); err != nil {
		slog.Error("cannot create the ingest directory", "dir", p.cfg.FileWatchFolder, "err", err)
		return
	}
	seen := map[string]fileState{}
	for {
		p.scanOnce(ctx, seen)
		select {
		case <-ctx.Done():
			return
		case <-time.After(scanInterval):
		}
	}
}

func (p *Processor) scanOnce(ctx context.Context, seen map[string]fileState) {
	now := time.Now()
	present := map[string]struct{}{}
	var ready []string

	err := filepath.Walk(p.cfg.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || p.validPath(path) != nil {
			return nil
		}
		present[path] = struct{}{}
		st, ok := seen[path]
		changed := !ok || st.size != info.Size() || !st.modTime.Equal(info.ModTime())
		if changed {
			// New or still being written: candidate for the next scan.
			seen[path] = fileState{size: info.Size(), modTime: info.ModTime()}
			return nil
		}
		if st.ingested || (!st.retryAt.IsZero() && now.Before(st.retryAt)) {
			return nil
		}
		ready = append(ready, path)
		return nil
	})
	if err != nil {
		slog.Warn("ingest scan failed", "err", err)
	}

	// Forget files that disappeared, so re-added files are re-considered.
	for path := range seen {
		if _, ok := present[path]; !ok {
			delete(seen, path)
		}
	}

	if len(ready) == 0 {
		return
	}
	results := make([]error, len(ready))
	sem := make(chan struct{}, ingestWorkers)
	var wg sync.WaitGroup
	for i, path := range ready {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, path string) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = p.processFile(ctx, path)
		}(i, path)
	}
	wg.Wait()

	for i, path := range ready {
		st := seen[path]
		if results[i] != nil {
			slog.Warn("ingest failed; will retry", "path", path, "err", results[i])
			st.retryAt = now.Add(scanRetryAfter)
		} else {
			st.ingested = true
			st.retryAt = time.Time{}
		}
		seen[path] = st
	}
}
