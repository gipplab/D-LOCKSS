package storage

import (
	"context"
	"log"
	"sync"
	"time"

	"dlockss/internal/config"
)

// getDiskUsagePercent is implemented in storage_monitor_linux.go (Linux) and storage_monitor_stub.go (!linux).

const diskUsageCacheTTL = 10 * time.Second

// DiskMonitor tracks disk usage for the data directory and provides
// high-water-mark checks. Safe for concurrent use.
type DiskMonitor struct {
	mu           sync.RWMutex
	usagePercent float64
	lastCheck    time.Time
	path         string
}

// NewDiskMonitor creates a DiskMonitor for the given directory path.
func NewDiskMonitor(path string) *DiskMonitor {
	return &DiskMonitor{path: path}
}

func (dm *DiskMonitor) CheckDiskUsage() float64 {
	dm.mu.RLock()
	lastCheck := dm.lastCheck
	usage := dm.usagePercent
	dm.mu.RUnlock()

	if time.Since(lastCheck) < diskUsageCacheTTL {
		return usage
	}

	newUsage, err := getDiskUsagePercent(dm.path)
	if err != nil {
		log.Printf("[Warning] Failed to check disk usage: %v", err)
		return usage
	}

	dm.mu.Lock()
	dm.usagePercent = newUsage
	dm.lastCheck = time.Now()
	dm.mu.Unlock()

	return newUsage
}

func (dm *DiskMonitor) IsDiskUsageHigh() bool {
	return dm.CheckDiskUsage() >= config.DiskUsageHighWaterMark
}

func (dm *DiskMonitor) CanAcceptCustodialFile() bool {
	return !dm.IsDiskUsageHigh()
}

func (dm *DiskMonitor) RunMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			usage := dm.CheckDiskUsage()
			if usage >= config.DiskUsageHighWaterMark {
				log.Printf("[Storage] Disk usage high: %.1f%% (high water mark: %.1f%%) - rejecting custodial files", usage, config.DiskUsageHighWaterMark)
			}
		}
	}
}
