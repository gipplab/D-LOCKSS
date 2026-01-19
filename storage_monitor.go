package main

import (
	"context"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

var (
	diskUsage = struct {
		sync.RWMutex
		usagePercent float64
		lastCheck    time.Time
	}{
		usagePercent: 0.0,
		lastCheck:    time.Time{},
	}
)

func getDiskUsagePercent(path string) (float64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, err
	}

	total := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := total - available

	if total == 0 {
		return 0, nil
	}

	usagePercent := float64(used) / float64(total) * 100.0
	return usagePercent, nil
}

func checkDiskUsage() float64 {
	diskUsage.RLock()
	lastCheck := diskUsage.lastCheck
	usagePercent := diskUsage.usagePercent
	diskUsage.RUnlock()

	if time.Since(lastCheck) < 10*time.Second {
		return usagePercent
	}

	absPath, err := os.Stat(FileWatchFolder)
	if err != nil {
		log.Printf("[Warning] Failed to stat data directory: %v", err)
		return usagePercent
	}

	var path string
	if absPath.IsDir() {
		path = FileWatchFolder
	} else {
		path = FileWatchFolder
	}

	usage, err := getDiskUsagePercent(path)
	if err != nil {
		log.Printf("[Warning] Failed to check disk usage: %v", err)
		return usagePercent
	}

	diskUsage.Lock()
	diskUsage.usagePercent = usage
	diskUsage.lastCheck = time.Now()
	diskUsage.Unlock()

	return usage
}

func isDiskUsageHigh() bool {
	usage := checkDiskUsage()
	return usage >= DiskUsageHighWaterMark
}

func canAcceptCustodialFile() bool {
	return !isDiskUsageHigh()
}

func runDiskUsageMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			usage := checkDiskUsage()
			if usage >= DiskUsageHighWaterMark {
				log.Printf("[Storage] Disk usage high: %.1f%% (high water mark: %.1f%%) - rejecting custodial files", usage, DiskUsageHighWaterMark)
			}
		}
	}
}
