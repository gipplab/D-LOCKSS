package main

import (
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	pinnedFiles = struct {
		sync.RWMutex
		hashes map[string]bool
	}{hashes: make(map[string]bool)}

	knownFiles = struct {
		sync.RWMutex
		hashes map[string]bool
	}{hashes: make(map[string]bool)}

	globalDHT *dht.IpfsDHT
	shardMgr  *ShardManager

	replicationWorkers chan struct{}

	rateLimiter = struct {
		sync.RWMutex
		peers map[peer.ID]*peerRateLimit
	}{peers: make(map[peer.ID]*peerRateLimit)}

	failedOperations = struct {
		sync.RWMutex
		hashes map[string]*operationBackoff
	}{hashes: make(map[string]*operationBackoff)}

	checkingFiles = struct {
		sync.RWMutex
		hashes map[string]bool
	}{hashes: make(map[string]bool)}

	lastCheckTime = struct {
		sync.RWMutex
		times map[string]time.Time
	}{times: make(map[string]time.Time)}

	recentlyRemoved = struct {
		sync.RWMutex
		hashes map[string]time.Time
	}{hashes: make(map[string]time.Time)}

	fileReplicationLevels = struct {
		sync.RWMutex
		levels map[string]int
	}{levels: make(map[string]int)}

	fileConvergenceTime = struct {
		sync.RWMutex
		times map[string]time.Time
	}{times: make(map[string]time.Time)}

	pendingVerifications = struct {
		sync.RWMutex
		hashes map[string]*verificationPending
	}{hashes: make(map[string]*verificationPending)}

	metrics = struct {
		sync.RWMutex
		pinnedFilesCount              int
		knownFilesCount               int
		messagesReceived              int64
		messagesDropped               int64
		replicationChecks             int64
		replicationSuccess            int64
		replicationFailures           int64
		shardSplits                   int64
		workerPoolActive              int
		rateLimitedPeers              int
		filesInBackoff                int
		lowReplicationFiles           int
		highReplicationFiles          int
		dhtQueries                    int64
		dhtQueryTimeouts              int64
		lastReportTime                time.Time
		startTime                     time.Time
		replicationDistribution       [11]int
		filesAtTargetReplication      int
		avgReplicationLevel           float64
		filesConvergedTotal           int64
		filesConvergedThisPeriod      int64
		cumulativeMessagesReceived    int64
		cumulativeMessagesDropped     int64
		cumulativeReplicationChecks   int64
		cumulativeReplicationSuccess  int64
		cumulativeReplicationFailures int64
		cumulativeDhtQueries          int64
		cumulativeDhtQueryTimeouts    int64
		cumulativeShardSplits         int64
	}{
		lastReportTime: time.Now(),
		startTime:      time.Now(),
	}
)

type operationBackoff struct {
	nextRetry time.Time
	delay     time.Duration
	mu        sync.Mutex
}

type peerRateLimit struct {
	messages []time.Time
	mu       sync.Mutex
}

type verificationPending struct {
	firstCount    int
	firstCheckTime time.Time
	verifyTime    time.Time
	responsible   bool
	pinned        bool
}
