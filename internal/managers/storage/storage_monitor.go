package storage

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const diskUsageCacheTTL = 10 * time.Second

// DiskMonitor tracks disk usage for the data directory and provides
// high-water-mark checks. Safe for concurrent use.
type DiskMonitor struct {
	mu            sync.RWMutex
	usagePercent  float64
	lastCheck     time.Time
	path          string
	highWaterMark float64
}

// NewDiskMonitor creates a DiskMonitor for the given directory path.
func NewDiskMonitor(path string, highWaterMark float64) *DiskMonitor {
	return &DiskMonitor{path: path, highWaterMark: highWaterMark}
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
		slog.Warn("failed to check disk usage", "error", err)
		return usage
	}

	dm.mu.Lock()
	dm.usagePercent = newUsage
	dm.lastCheck = time.Now()
	dm.mu.Unlock()

	return newUsage
}

func (dm *DiskMonitor) IsDiskUsageHigh() bool {
	return dm.CheckDiskUsage() >= dm.highWaterMark
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
			if usage >= dm.highWaterMark {
				slog.Warn("disk usage high, rejecting custodial files", "usage_pct", usage, "high_water_mark", dm.highWaterMark)
			}
		}
	}
}
