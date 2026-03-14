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
	if fp.cfg.Files.FileStabilityDelay <= 0 {
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
	fp.stabilityTimer[path] = time.AfterFunc(fp.cfg.Files.FileStabilityDelay, func() {
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
			fp.handleWatcherEvent(event, watcher, watchedDirs, &watchedDirsMu)
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("errors channel closed unexpectedly")
			}
			slog.Error("watcher error", "error", err)
		}
	}
}

func (fp *FileProcessor) handleWatcherEvent(event fsnotify.Event, watcher *fsnotify.Watcher, watchedDirs map[string]bool, mu *sync.RWMutex) {
	if event.Op&fsnotify.Create == fsnotify.Create {
		info, err := os.Stat(event.Name)
		if err == nil && info.IsDir() {
			fp.handleNewDirectory(event.Name, watcher, watchedDirs, mu)
			return
		}
	}

	if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
		path := event.Name
		if !fp.validateFilePath(path) {
			return
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			return
		}
		if fp.shouldProcessFileEvent(path) {
			fp.enqueueWithStabilityCheck(path)
		}
	}
}

func (fp *FileProcessor) handleNewDirectory(dirPath string, watcher *fsnotify.Watcher, watchedDirs map[string]bool, mu *sync.RWMutex) {
	mu.RLock()
	alreadyWatched := watchedDirs[dirPath]
	mu.RUnlock()

	if !alreadyWatched {
		if err := watcher.Add(dirPath); err != nil {
			slog.Error("failed to watch new directory", "path", dirPath, "error", err)
		} else {
			mu.Lock()
			watchedDirs[dirPath] = true
			mu.Unlock()
			slog.Debug("added watch for new directory", "path", dirPath)
		}
	}

	go func() {
		time.Sleep(fp.cfg.Files.FileProcessingDelay)
		fileCount := 0
		dirCount := 0
		err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				slog.Warn("error accessing path during directory scan", "path", path, "error", err)
				return nil
			}
			if info.IsDir() {
				if path != dirPath {
					mu.RLock()
					seen := watchedDirs[path]
					mu.RUnlock()
					if !seen {
						if err := watcher.Add(path); err != nil {
							slog.Error("failed to watch nested directory", "path", path, "error", err)
						} else {
							mu.Lock()
							watchedDirs[path] = true
							mu.Unlock()
							dirCount++
						}
					}
				}
				return nil
			}
			if !fp.validateFilePath(path) {
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
	}()
}
