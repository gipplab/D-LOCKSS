package shard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const reshardedFilesSaveInterval = 2 * time.Minute

func (sm *ShardManager) reshardedFilesPath() string {
	return filepath.Join(filepath.Dir(sm.cfg.ClusterStorePath), "resharded_files.json")
}

func (sm *ShardManager) loadReshardedFiles() error {
	path := sm.reshardedFilesPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load resharded files %s: %w", path, err)
	}
	var keys []string
	if err := json.Unmarshal(data, &keys); err != nil {
		return fmt.Errorf("parse resharded files: %w", err)
	}
	for _, k := range keys {
		sm.reshardedFiles.Add(k)
	}
	slog.Info("loaded resharded files", "count", len(keys), "path", path)
	return nil
}

func (sm *ShardManager) saveReshardedFiles() error {
	path := sm.reshardedFilesPath()
	keys := sm.reshardedFiles.All()
	arr := make([]string, 0, len(keys))
	for k := range keys {
		arr = append(arr, k)
	}
	data, err := json.Marshal(arr)
	if err != nil {
		return fmt.Errorf("marshal resharded files: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory for resharded files: %w", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("save resharded files %s: %w", path, err)
	}
	return nil
}

func (sm *ShardManager) runReshardedFilesSaveLoop() {
	ticker := time.NewTicker(reshardedFilesSaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			if err := sm.saveReshardedFiles(); err != nil {
				slog.Error("failed to save resharded files", "error", err)
			}
		}
	}
}
