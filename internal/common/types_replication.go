package common

import (
	"sync"
)

// FileReplicationLevels tracks replication counts for files.
type FileReplicationLevels struct {
	mu sync.RWMutex
	m  map[string]int
}

func NewFileReplicationLevels() *FileReplicationLevels {
	return &FileReplicationLevels{m: make(map[string]int)}
}

func (frl *FileReplicationLevels) Set(key string, count int) {
	frl.mu.Lock()
	defer frl.mu.Unlock()
	frl.m[key] = count
}

func (frl *FileReplicationLevels) Get(key string) (int, bool) {
	frl.mu.RLock()
	defer frl.mu.RUnlock()
	count, ok := frl.m[key]
	return count, ok
}

func (frl *FileReplicationLevels) Remove(key string) {
	frl.mu.Lock()
	defer frl.mu.Unlock()
	delete(frl.m, key)
}

func (frl *FileReplicationLevels) All() map[string]int {
	frl.mu.RLock()
	defer frl.mu.RUnlock()
	result := make(map[string]int, len(frl.m))
	for k, v := range frl.m {
		result[k] = v
	}
	return result
}
