package fileops

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fileEventInfo tracks file metadata to detect actual content changes.
type fileEventInfo struct {
	size     int64
	modTime  time.Time
	lastSeen time.Time
}

func (fp *FileProcessor) shouldProcessFileEvent(path string) bool {
	const window = 2 * time.Second
	now := time.Now()

	info, err := os.Stat(path)
	if err != nil {
		return true
	}
	if info.IsDir() {
		return false
	}

	currentSize := info.Size()
	currentModTime := info.ModTime()

	fp.deduperMu.Lock()
	defer fp.deduperMu.Unlock()

	if last, ok := fp.deduperInfo[path]; ok {
		if last.size == currentSize &&
			last.modTime.Equal(currentModTime) &&
			now.Sub(last.lastSeen) < window {
			return false
		}
	}

	fp.deduperInfo[path] = fileEventInfo{
		size:     currentSize,
		modTime:  currentModTime,
		lastSeen: now,
	}

	cutoff := now.Add(-10 * window)
	for k, v := range fp.deduperInfo {
		if v.lastSeen.Before(cutoff) {
			delete(fp.deduperInfo, k)
		}
	}

	return true
}

// enqueueWithStabilityCheck enqueues path for processing. If FileStabilityDelay > 0,
// waits for the file size to be unchanged for that duration before enqueueing,
// to avoid ingesting files still being written (e.g. downloads).
func (fp *FileProcessor) enqueueWithStabilityCheck(path string) {
	if fp.cfg.FileStabilityDelay <= 0 {
		_ = fp.EnqueueOrRetry(path)
		return
	}

	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return
	}
	currentSize := info.Size()

	fp.stabilityMu.Lock()
	if t, ok := fp.stabilityTimer[path]; ok {
		t.Stop()
	}
	fp.stabilityPath[path] = currentSize
	fp.stabilityTimer[path] = time.AfterFunc(fp.cfg.FileStabilityDelay, func() {
		if fp.ctx.Err() != nil {
			return
		}

		fp.stabilityMu.Lock()
		expectedSize := fp.stabilityPath[path]
		delete(fp.stabilityPath, path)
		delete(fp.stabilityTimer, path)
		fp.stabilityMu.Unlock()

		info2, err2 := os.Stat(path)
		if err2 != nil || info2.IsDir() {
			return
		}
		if info2.Size() == expectedSize {
			_ = fp.EnqueueOrRetry(path)
		} else {
			slog.Debug("file still changing size, re-scheduling stability check", "expected_size", expectedSize, "current_size", info2.Size(), "path", path)
			fp.enqueueWithStabilityCheck(path)
		}
	})
	fp.stabilityMu.Unlock()
}

// WatchFolder watches the data directory for new files.
func (fp *FileProcessor) WatchFolder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("context cancelled, stopping file watcher")
			return
		default:
			if err := fp.runWatcher(ctx); err != nil {
				slog.Error("watcher exited with error, restarting in 5s", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					slog.Info("restarting file watcher")
				}
			} else {
				slog.Info("watcher exited normally, restarting in 5s")
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					slog.Info("restarting file watcher")
				}
			}
		}
	}
}

// runWatcher runs a single instance of the file watcher.
func (fp *FileProcessor) runWatcher(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	defer watcher.Close()

	if err := os.MkdirAll(fp.cfg.FileWatchFolder, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := watcher.Add(fp.cfg.FileWatchFolder); err != nil {
		return fmt.Errorf("failed to watch data directory: %w", err)
	}

	watchedDirs := make(map[string]bool)
	watchedDirs[fp.cfg.FileWatchFolder] = true

	err = filepath.Walk(fp.cfg.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if !watchedDirs[path] {
				if err := watcher.Add(path); err != nil {
					slog.Error("failed to watch subdirectory", "path", path, "error", err)
				} else {
					watchedDirs[path] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("failed to walk directory for watching", "error", err)
	}

	slog.Info("watching for new files", "dir", fp.cfg.FileWatchFolder)

	var watchedDirsMu sync.RWMutex

	for {
		select {
		case <-ctx.Done():
			slog.Info("context cancelled, stopping file watcher")
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("events channel closed unexpectedly")
			}

			if event.Op&fsnotify.Create == fsnotify.Create {
				info, err := os.Stat(event.Name)
				if err == nil && info.IsDir() {
					watchedDirsMu.RLock()
					alreadyWatched := watchedDirs[event.Name]
					watchedDirsMu.RUnlock()

					if !alreadyWatched {
						if err := watcher.Add(event.Name); err != nil {
							slog.Error("failed to watch new directory", "path", event.Name, "error", err)
						} else {
							watchedDirsMu.Lock()
							watchedDirs[event.Name] = true
							watchedDirsMu.Unlock()
							slog.Debug("added watch for new directory", "path", event.Name)
						}
					}
					go func(dirPath string) {
						time.Sleep(fp.cfg.FileProcessingDelay)

						fileCount := 0
						dirCount := 0

						err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								slog.Warn("error accessing path during directory scan", "path", path, "error", err)
								return nil
							}
							if info.IsDir() {
								if path != dirPath {
									watchedDirsMu.RLock()
									alreadyWatched := watchedDirs[path]
									watchedDirsMu.RUnlock()

									if !alreadyWatched {
										if err := watcher.Add(path); err != nil {
											slog.Error("failed to watch nested directory", "path", path, "error", err)
										} else {
											watchedDirsMu.Lock()
											watchedDirs[path] = true
											watchedDirsMu.Unlock()
											slog.Debug("added watch for nested directory", "path", path)
											dirCount++
										}
									}
								}
								return nil
							}
							if !fp.validateFilePath(path) {
								slog.Debug("file filtered by validation", "path", path)
								return nil
							}
							fileCount++

							fp.enqueueWithStabilityCheck(path)
							return nil
						})
						if err != nil {
							slog.Error("failed to scan new directory", "path", dirPath, "error", err)
						} else {
							slog.Info("scanned directory", "path", dirPath, "files", fileCount, "nested_dirs", dirCount)
						}
					}(event.Name)
					continue
				}
			}

			if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
				path := event.Name

				if !fp.validateFilePath(path) {
					continue
				}

				info, err := os.Stat(path)
				if err != nil || info.IsDir() {
					continue
				}

				if fp.shouldProcessFileEvent(path) {
					fp.enqueueWithStabilityCheck(path)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("errors channel closed unexpectedly")
			}
			slog.Error("watcher error", "error", err)
		}
	}
}
