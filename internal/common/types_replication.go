package common

import "dlockss/internal/syncmap"

// FileReplicationLevels tracks replication counts for files.
type FileReplicationLevels = syncmap.Map[string, int]

func NewFileReplicationLevels() *FileReplicationLevels {
	return syncmap.New[string, int]()
}
