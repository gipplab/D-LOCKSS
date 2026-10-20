//go:build !linux

package storage

// getDiskUsagePercent always reports 0% on non-Linux platforms, so the
// high-water mark never refuses custodial files there.
func getDiskUsagePercent(path string) (float64, error) {
	return 0, nil
}
