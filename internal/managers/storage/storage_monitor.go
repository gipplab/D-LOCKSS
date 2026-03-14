package storage

import (
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

func (dm *DiskMonitor) CanAcceptCustodialFile() bool {
	dm.mu.RLock()
	lastCheck := dm.lastCheck
	usage := dm.usagePercent
	dm.mu.RUnlock()

	if time.Since(lastCheck) >= diskUsageCacheTTL {
		newUsage, err := getDiskUsagePercent(dm.path)
		if err != nil {
			slog.Warn("failed to check disk usage", "error", err)
		} else {
			usage = newUsage
			dm.mu.Lock()
			dm.usagePercent = newUsage
			dm.lastCheck = time.Now()
			dm.mu.Unlock()
		}
	}

	return usage < dm.highWaterMark
}
