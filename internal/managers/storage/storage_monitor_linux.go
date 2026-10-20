//go:build linux

package storage

import "syscall"

func getDiskUsagePercent(path string) (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}

	total := stat.Blocks * uint64(stat.Bsize)
	if total == 0 {
		return 0, nil
	}
	available := stat.Bavail * uint64(stat.Bsize)
	return float64(total-available) / float64(total) * 100.0, nil
}
