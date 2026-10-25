package store

import (
	"sync"
	"time"
)

// diskUsageCacheTTL bounds how often the filesystem is stat'ed.
const diskUsageCacheTTL = 10 * time.Second

// DiskMonitor watches disk usage of the filesystem holding the node's state
// directory (in documented deployments, the same disk as the Kubo repo).
// Above the high water mark the node advertises PASSIVE, steering custodial
// and replication load away before the disk fills.
type DiskMonitor struct {
	path string
	hwm  float64

	mu        sync.Mutex
	lastCheck time.Time
	lastUsage float64
}

// NewDiskMonitor watches path against the given high-water-mark percentage.
func NewDiskMonitor(path string, highWaterMarkPct float64) *DiskMonitor {
	return &DiskMonitor{path: path, hwm: highWaterMarkPct}
}

// UsagePct returns the cached usage percentage of the filesystem holding the
// watched path. Read errors keep the last known value (initially 0).
func (d *DiskMonitor) UsagePct() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.lastCheck) < diskUsageCacheTTL {
		return d.lastUsage
	}
	d.lastCheck = time.Now()
	if pct, err := diskUsage(d.path); err == nil {
		d.lastUsage = pct
	}
	return d.lastUsage
}

// CanAcceptFiles reports whether the node has headroom for more content.
func (d *DiskMonitor) CanAcceptFiles() bool {
	return d.UsagePct() < d.hwm
}
