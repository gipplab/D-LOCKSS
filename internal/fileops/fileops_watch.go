package fileops

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"dlockss/internal/config"
)

// fileEventInfo tracks file metadata to detect actual content changes.
type fileEventInfo struct {
	size     int64
	modTime  time.Time
	lastSeen time.Time
}

var fileEventDeduper = struct {
	mu   sync.Mutex
	info map[string]fileEventInfo
}{
	info: make(map[string]fileEventInfo),
}

// pendingStability tracks files waiting for size to settle before ingest.
var pendingStability = struct {
	mu    sync.Mutex
	path  map[string]int64
	timer map[string]*time.Timer
}{
	path:  make(map[string]int64),
	timer: make(map[string]*time.Timer),
}

func shouldProcessFileEvent(path string) bool {
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

	fileEventDeduper.mu.Lock()
	defer fileEventDeduper.mu.Unlock()

	if last, ok := fileEventDeduper.info[path]; ok {
		if last.size == currentSize &&
			last.modTime.Equal(currentModTime) &&
			now.Sub(last.lastSeen) < window {
			return false
		}
	}

	fileEventDeduper.info[path] = fileEventInfo{
		size:     currentSize,
		modTime:  currentModTime,
		lastSeen: now,
	}

	cutoff := now.Add(-10 * window)
	for k, v := range fileEventDeduper.info {
		if v.lastSeen.Before(cutoff) {
			delete(fileEventDeduper.info, k)
		}
	}

	return true
}

// enqueueWithStabilityCheck enqueues path for processing. If FileStabilityDelay > 0,
// waits for the file size to be unchanged for that duration before enqueueing,
// to avoid ingesting files still being written (e.g. downloads).
func (fp *FileProcessor) enqueueWithStabilityCheck(path string) {
	if config.FileStabilityDelay <= 0 {
		_ = fp.EnqueueOrRetry(path)
		return
	}

	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return
	}
	currentSize := info.Size()

	pendingStability.mu.Lock()
	if t, ok := pendingStability.timer[path]; ok {
		t.Stop()
	}
	pendingStability.path[path] = currentSize
	pendingStability.timer[path] = time.AfterFunc(config.FileStabilityDelay, func() {
		if fp.ctx.Err() != nil {
			return
		}

		pendingStability.mu.Lock()
		expectedSize := pendingStability.path[path]
		delete(pendingStability.path, path)
		delete(pendingStability.timer, path)
		pendingStability.mu.Unlock()

		info2, err2 := os.Stat(path)
		if err2 != nil || info2.IsDir() {
			return
		}
		if info2.Size() == expectedSize {
			_ = fp.EnqueueOrRetry(path)
		} else {
			log.Printf("[FileWatcher] File still changing size (was %d, now %d), re-scheduling stability check: %s",
				expectedSize, info2.Size(), path)
			fp.enqueueWithStabilityCheck(path)
		}
	})
	pendingStability.mu.Unlock()
}

// WatchFolder watches the data directory for new files.
func (fp *FileProcessor) WatchFolder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[FileWatcher] Context cancelled, stopping file watcher")
			return
		default:
			if err := fp.runWatcher(ctx); err != nil {
				log.Printf("[FileWatcher] Watcher exited with error: %v. Restarting in 5 seconds...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					log.Printf("[FileWatcher] Restarting file watcher...")
				}
			} else {
				log.Printf("[FileWatcher] Watcher exited normally. Restarting in 5 seconds...")
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					log.Printf("[FileWatcher] Restarting file watcher...")
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

	if err := os.MkdirAll(config.FileWatchFolder, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := watcher.Add(config.FileWatchFolder); err != nil {
		return fmt.Errorf("failed to watch data directory: %w", err)
	}

	watchedDirs := make(map[string]bool)
	watchedDirs[config.FileWatchFolder] = true

	err = filepath.Walk(config.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if !watchedDirs[path] {
				if err := watcher.Add(path); err != nil {
					log.Printf("[Error] Failed to watch subdirectory %s: %v", path, err)
				} else {
					watchedDirs[path] = true
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("[Error] Failed to walk directory for watching: %v", err)
	}

	log.Printf("[FileWatcher] Watching %s (and subdirectories) for new files...", config.FileWatchFolder)

	var watchedDirsMu sync.RWMutex

	for {
		select {
		case <-ctx.Done():
			log.Printf("[FileWatcher] Context cancelled, stopping file watcher")
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
							log.Printf("[Error] Failed to watch new directory %s: %v", event.Name, err)
						} else {
							watchedDirsMu.Lock()
							watchedDirs[event.Name] = true
							watchedDirsMu.Unlock()
							log.Printf("[FileWatcher] Added watch for new directory: %s", event.Name)
						}
					}
					go func(dirPath string) {
						time.Sleep(config.FileProcessingDelay)

						fileCount := 0
						dirCount := 0

						err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								log.Printf("[Warning] Error accessing %s during directory scan: %v", path, err)
								return nil
							}
							if info.IsDir() {
								if path != dirPath {
									watchedDirsMu.RLock()
									alreadyWatched := watchedDirs[path]
									watchedDirsMu.RUnlock()

									if !alreadyWatched {
										if err := watcher.Add(path); err != nil {
											log.Printf("[Error] Failed to watch nested directory %s: %v", path, err)
										} else {
											watchedDirsMu.Lock()
											watchedDirs[path] = true
											watchedDirsMu.Unlock()
											log.Printf("[FileWatcher] Added watch for nested directory: %s", path)
											dirCount++
										}
									}
								}
								return nil
							}
							if !validateFilePath(path) {
								log.Printf("[FileWatcher] File filtered by validation: %s", path)
								return nil
							}
							fileCount++

							fp.enqueueWithStabilityCheck(path)
							return nil
						})
						if err != nil {
							log.Printf("[Error] Failed to scan new directory %s: %v", dirPath, err)
						} else {
							log.Printf("[FileWatcher] Scanned directory %s: found %d files, %d nested directories", dirPath, fileCount, dirCount)
						}
					}(event.Name)
					continue
				}
			}

			if event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write {
				path := event.Name

				if !validateFilePath(path) {
					continue
				}

				info, err := os.Stat(path)
				if err != nil || info.IsDir() {
					continue
				}

				if shouldProcessFileEvent(path) {
					fp.enqueueWithStabilityCheck(path)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("errors channel closed unexpectedly")
			}
			log.Printf("[FileWatcher] ERROR: Watcher error: %v", err)
		}
	}
}
