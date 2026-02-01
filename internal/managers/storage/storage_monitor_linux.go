//go:build linux

package storage

import "syscall"

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
