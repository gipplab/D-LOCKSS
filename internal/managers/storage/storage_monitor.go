package storage

import (
	"context"
	"log"
	"sync"
	"time"

	"dlockss/internal/config"
)

// getDiskUsagePercent is implemented in storage_monitor_linux.go (Linux) and storage_monitor_stub.go (!linux).

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

func CheckDiskUsage() float64 {
	diskUsage.RLock()
	lastCheck := diskUsage.lastCheck
	usagePercent := diskUsage.usagePercent
	diskUsage.RUnlock()

	if time.Since(lastCheck) < 10*time.Second {
		return usagePercent
	}

	// Use the data directory path directly; Statfs operates on the mount point.
	usage, err := getDiskUsagePercent(config.FileWatchFolder)
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

func IsDiskUsageHigh() bool {
	usage := CheckDiskUsage()
	return usage >= config.DiskUsageHighWaterMark
}

func CanAcceptCustodialFile() bool {
	return !IsDiskUsageHigh()
}

func RunDiskUsageMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			usage := CheckDiskUsage()
			if usage >= config.DiskUsageHighWaterMark {
				log.Printf("[Storage] Disk usage high: %.1f%% (high water mark: %.1f%%) - rejecting custodial files", usage, config.DiskUsageHighWaterMark)
			}
		}
	}
}
