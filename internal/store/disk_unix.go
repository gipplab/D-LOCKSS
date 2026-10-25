//go:build linux || darwin

package store

import "syscall"

// diskUsage returns the used percentage of the filesystem containing path.
func diskUsage(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	total := st.Blocks
	if total == 0 {
		return 0, nil
	}
	used := st.Blocks - uint64(st.Bavail)
	return float64(used) / float64(total) * 100, nil
}
