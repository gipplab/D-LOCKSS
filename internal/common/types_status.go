package common

// StorageSnapshot is a point-in-time view of storage state, used by internal
// metrics and telemetry (not the public API).
type StorageSnapshot struct {
	PinnedCount  int
	KnownCount   int
	KnownCIDs    []string
	BackoffCount int
}

// StatusResponse defines the JSON structure for /status and monitor node views.
type StatusResponse struct {
	PeerID        string            `json:"peer_id"`
	Version       string            `json:"version"`
	CurrentShard  string            `json:"current_shard"`
	Role          string            `json:"role,omitempty"`
	PeersInShard  int               `json:"peers_in_shard"`
	Storage       StorageStatus     `json:"storage"`
	Replication   ReplicationStatus `json:"replication"`
	UptimeSeconds float64           `json:"uptime_seconds"`
}

type StorageStatus struct {
	PinnedFiles   int      `json:"pinned_files"`
	PinnedInShard int      `json:"pinned_in_shard,omitempty"`
	KnownFiles    int      `json:"known_files"`
	KnownCIDs     []string `json:"known_cids,omitempty"`
}

type ReplicationStatus struct {
	QueueDepth              int     `json:"queue_depth"`
	ActiveWorkers           int     `json:"active_workers"`
	AvgReplicationLevel     float64 `json:"avg_replication_level"`
	FilesAtTarget           int     `json:"files_at_target"`
	ReplicationDistribution [11]int `json:"replication_distribution"`
}
