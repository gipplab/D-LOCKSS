package shard

import "time"

const (
	discoveryIntervalOnRoot       = 10 * time.Second
	probeTimeoutDiscovery         = 12 * time.Second
	discoveryIntervalWithChildren = 45 * time.Second
)
