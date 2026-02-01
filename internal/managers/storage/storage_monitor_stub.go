//go:build !linux

package storage

// getDiskUsagePercent is a stub on non-Linux; disk usage checks are no-ops (report 0%).
func getDiskUsagePercent(path string) (float64, error) {
	return 0, nil
}
