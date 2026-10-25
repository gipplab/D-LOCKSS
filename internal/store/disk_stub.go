//go:build !linux && !darwin

package store

// diskUsage is unsupported on this platform: report 0% so the node never
// turns PASSIVE for disk reasons.
func diskUsage(string) (float64, error) { return 0, nil }
