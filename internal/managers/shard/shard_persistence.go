package shard

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"dlockss/internal/config"
)

const reshardedFilesSaveInterval = 2 * time.Minute

func reshardedFilesPath() string {
	return filepath.Join(filepath.Dir(config.ClusterStorePath), "resharded_files.json")
}

func (sm *ShardManager) loadReshardedFiles() {
	path := reshardedFilesPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[Shard] Failed to load resharded files from %s: %v", path, err)
		}
		return
	}
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		log.Printf("[Shard] Failed to parse resharded files: %v", err)
		return
	}
	for _, k := range keys {
		sm.reshardedFiles.Add(k)
	}
	log.Printf("[Shard] Loaded %d resharded files from %s", len(keys), path)
}

func (sm *ShardManager) saveReshardedFiles() {
	path := reshardedFilesPath()
	keys := sm.reshardedFiles.All()
	arr := make([]string, 0, len(keys))
	for k := range keys {
		arr = append(arr, k)
	}
	data, err := json.Marshal(arr)
	if err != nil {
		log.Printf("[Shard] Failed to marshal resharded files: %v", err)
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[Shard] Failed to create dir for resharded files: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Printf("[Shard] Failed to save resharded files to %s: %v", path, err)
	}
}

func (sm *ShardManager) runReshardedFilesSaveLoop() {
	ticker := time.NewTicker(reshardedFilesSaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			sm.saveReshardedFiles()
			return
		case <-ticker.C:
			sm.saveReshardedFiles()
		}
	}
}
