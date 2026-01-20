Please generate a prompt that will enable any modern LLM to reproduce the exact program code. Do not include code snippets in the prompt.

**Prompt:**

Act as an expert Systems Engineer. Write a complete, single-file Go program (package `main`) that implements a production-ready, self-healing, sharded P2P file preservation system called "D-LOCKSS".

The code must adhere to the following strict specifications, logical structures, and variable naming conventions.

### 1. Dependencies & Imports
* Use standard libraries for context, crypto/sha256, hex encoding, file I/O, logging, OS signals, path manipulation, sync primitives, time, encoding/csv, strconv, and fmt.
* Use `github.com/fsnotify/fsnotify` for file watching.
* Use the full suite of `libp2p` libraries: core host, network, peer, routing, `go-cid`, `go-multihash`, `go-libp2p-pubsub`, and `go-libp2p-kad-dht` (aliased as `dht`).
* Use `github.com/libp2p/go-libp2p/p2p/discovery/mdns` for local peer discovery.

### 2. Configuration System
All configuration must be environment-variable driven with sensible defaults. Implement helper functions `getEnvString`, `getEnvInt`, `getEnvDuration`, and `getEnvFloat` that read environment variables and return defaults if not set. Log all active configuration on startup using `logConfiguration()`.

**Configuration Variables:**
* Control Topic: "dlockss-v2-creative-commons-control" (env: `DLOCKSS_CONTROL_TOPIC`)
* Discovery Tag: "dlockss-v2-prod" (env: `DLOCKSS_DISCOVERY_TAG`)
* Watch Folder: "./data" (env: `DLOCKSS_DATA_DIR`)
* **Replication Rules:** Minimum = 5, Maximum = 10 (env: `DLOCKSS_MIN_REPLICATION`, `DLOCKSS_MAX_REPLICATION`)
* **Intervals:** Check interval = 15 minutes (env: `DLOCKSS_CHECK_INTERVAL`)
* **Load Balancing:** Max Shard Load = 2000 (env: `DLOCKSS_MAX_SHARD_LOAD`)
* **Concurrency:** Max Concurrent Replication Checks = 10 (env: `DLOCKSS_MAX_CONCURRENT_CHECKS`)
* **Rate Limiting:** Window = 1 minute, Max Messages = 100 per window (env: `DLOCKSS_RATE_LIMIT_WINDOW`, `DLOCKSS_MAX_MESSAGES_PER_WINDOW`)
* **Backoff:** Initial = 30s, Max = 15m, Multiplier = 2.0 (env: `DLOCKSS_INITIAL_BACKOFF`, `DLOCKSS_MAX_BACKOFF`, `DLOCKSS_BACKOFF_MULTIPLIER`)
* **Metrics:** Report interval = 10s, Export path = "" (env: `DLOCKSS_METRICS_INTERVAL`, `DLOCKSS_METRICS_EXPORT`)
* **Cooldowns:** Replication check cooldown = 30s, Removed file cooldown = 2m (env: `DLOCKSS_REPLICATION_COOLDOWN`, `DLOCKSS_REMOVED_COOLDOWN`)

### 3. Global State
Create thread-safe global structures using `sync.RWMutex` and maps:
1. `pinnedFiles`: Tracks files physically stored/pinned (map[string]bool)
2. `knownFiles`: Tracks files being monitored/tracked (map[string]bool)
3. `fileReplicationLevels`: Tracks actual replication count per file (map[string]int)
4. `fileConvergenceTime`: Tracks when files first reach target replication (map[string]time.Time)
5. `rateLimiter`: Per-peer rate limiting (map[peer.ID]*peerRateLimit)
6. `failedOperations`: Backoff tracking for failed operations (map[string]*operationBackoff)
7. `checkingFiles`: Prevents duplicate concurrent replication checks (map[string]bool)
8. `lastCheckTime`: Cooldown tracking for replication checks (map[string]time.Time)
9. `recentlyRemoved`: Prevents immediate re-adding of removed files (map[string]time.Time)
10. `metrics`: Comprehensive metrics struct with mutex protection
11. `globalDHT`: DHT pointer
12. `shardMgr`: ShardManager pointer
13. `replicationWorkers`: Worker pool channel (chan struct{})

### 4. Metrics System
Implement a comprehensive metrics tracking system with:
* **Periodic Metrics:** pinnedFilesCount, knownFilesCount, messagesReceived, messagesDropped, replicationChecks, replicationSuccess, replicationFailures, lowReplicationFiles, highReplicationFiles, filesAtTargetReplication, replicationDistribution[11] (0-10+), avgReplicationLevel, filesConvergedTotal, filesConvergedThisPeriod, dhtQueries, dhtQueryTimeouts, shardSplits, workerPoolActive, rateLimitedPeers, filesInBackoff
* **Cumulative Metrics:** cumulativeMessagesReceived, cumulativeMessagesDropped, cumulativeReplicationChecks, cumulativeReplicationSuccess, cumulativeReplicationFailures, cumulativeDhtQueries, cumulativeDhtQueryTimeouts, cumulativeShardSplits
* **Reporting:** `runMetricsReporter` goroutine that calls `reportMetrics()` every MetricsReportInterval
* **Export:** `exportMetricsToFile()` that writes CSV with all metrics, including header generation for new files
* **Logging:** Structured log output showing all metrics every reporting period

### 5. Rate Limiting System
Implement per-peer rate limiting:
* `peerRateLimit` struct with mutex, message timestamps slice, and window duration
* `checkRateLimit(peerID)` function that uses sliding window algorithm
* Cleanup goroutine `runRateLimiterCleanup()` that removes inactive peer limiters
* Apply rate limiting to DELEGATE, NEED, and NEW messages
* Log rate limit violations and drop messages when limit exceeded

### 6. Backoff System
Implement exponential backoff for failed operations:
* `operationBackoff` struct with nextRetry time, delay duration, and mutex
* `recordFailedOperation(hash)` that calculates exponential backoff
* `shouldSkipDueToBackoff(hash)` that checks if operation should be skipped
* `clearBackoff(hash)` that removes backoff when operation succeeds
* Apply backoff to failed DHT queries and replication checks

### 7. Worker Pool System
Implement worker pool for replication checks:
* Initialize `replicationWorkers` channel with capacity = MaxConcurrentReplicationChecks
* In `runReplicationChecker`, acquire worker before launching goroutine, release when done
* Prevents unbounded goroutine creation and resource exhaustion

### 8. The ShardManager Struct
Create a struct named `ShardManager` to handle dynamic topic switching. It must hold:
* Context, Host, and PubSub pointers
* Mutex, current shard string (binary prefix), shard topic/subscription, control topic/subscription
* `msgCounter` (int) to track load

**ShardManager Methods:**
* **New:** Factory function to initialize and join channels
* **joinChannels:** Subscribe to static Control topic. Subscribe to dynamic Shard topic (`dlockss-v2-creative-commons-shard-{prefix}`). Reset `msgCounter`. Must properly clean up old subscriptions before creating new ones using synchronization (shardDone channel) to prevent resource leaks
* **readControl:** Loop decoding CBOR `DelegateMessage`. Apply rate limiting and disk-pressure checks for custodial requests. If target shard matches node's current shard prefix, add ManifestCID to `knownFiles` and launch replication check.
* **readShard:** Loop decoding CBOR messages. Apply rate limiting. Increment `msgCounter` and trigger `splitShard` when overloaded. Handle `IngestMessage` and `ReplicationRequest` by adding ManifestCID to `knownFiles` and launching replication check.
* **splitShard:** Calculate next binary bit based on node's Peer ID hash at next depth. Update current shard prefix and call `joinChannels` synchronously
* **AmIResponsibleFor:** Boolean check comparing a stable hash of the ManifestCID string against node's current shard prefix
* **Publish helpers:** Methods to publish CBOR bytes to shard/control topics
* **Close:** Gracefully close all subscriptions and clean up resources

### 9. File Handling & Custodial Logic
Implement `processNewFile(path)` with "Custodial" logic:
1. Validate file path (prevent path traversal attacks using `filepath.Abs` and prefix checks)
2. Import file into IPFS UnixFS DAG → PayloadCID
3. Build `ResearchObject` (CBOR), store as dag-cbor block → ManifestCID
4. **Recursive Pin:** Pin ManifestCID recursively (pins payload DAG)
4. Add to `knownFiles` (check `recentlyRemoved` cooldown first)
5. **Routing:**
   * If responsible: Announce CBOR `IngestMessage` to Shard
   * If *not* responsible: Announce CBOR `DelegateMessage` to Control, but keep file pinned (Custodial Mode)

### 10. The Replication Checker (Core Logic)
Implement `checkReplication(ctx, hash)` with comprehensive logic:
1. **Deduplication:** Check `checkingFiles` map to prevent duplicate concurrent checks
2. **Cooldown Check:** Skip if checked within `ReplicationCheckCooldown` period
3. **Recent Removal Check:** Skip if file was recently removed (within `RemovedFileCooldown`)
4. **Backoff Check:** Skip if operation is in backoff period
5. Determine if node is `responsible` and if file is `pinned`
6. **Tracking Cleanup:** If node is neither responsible nor pinning file, remove from `knownFiles` and return
7. **DHT Query:** Find providers for ManifestCID with 2-minute timeout context, limit iteration to MaxReplication+5
8. **Store Replication Level:** Update `fileReplicationLevels` map with actual count
9. **Convergence Tracking:** If count >= MinReplication && count <= MaxReplication, check if first time reaching target and record in `fileConvergenceTime`, increment convergence metrics
10. **Case 1 (Low Redundancy):** If count < MinReplication:
    * If responsible (or custodial), re-pin ManifestCID locally (recursive) and provide to DHT
    * Broadcast CBOR `ReplicationRequest` to shard
11. **Case 2 (Garbage Collection):**
    * If responsible AND count > MaxReplication AND pinned: Unpin ManifestCID (recursive). If count > MaxReplication+3, remove from tracking
    * If *not* responsible (Custodial) AND count >= MinReplication AND pinned: Unpin ManifestCID (handoff complete)
12. **DAG Verification + Liar Detection:** When pinned locally, verify manifest decodes, payload is pinned, and payload size matches `TotalSize`. If mismatch, unpin recursively and drop.
12. **Update Metrics:** Increment replication checks, success/failures, update distribution counters
13. **Record Check Time:** Update `lastCheckTime` for cooldown tracking

### 11. Replication Distribution Calculation
In `reportMetrics()`, calculate replication distribution:
* Iterate `fileReplicationLevels` map
* Count files at each level (0-9 for exact counts, 10 for 10+)
* Calculate average replication level
* Count files at target replication (5-10)
* Update metrics struct with distribution array and statistics

### 12. Helper Functions
* **Pinning:** Thread-safe `pinFile` and `unpinFile` (updates `pinnedFiles` and metrics)
* **Tracking:** Thread-safe `addKnownFile` and `removeKnownFile` (updates `knownFiles`, checks cooldowns, cleans up replication levels)
* **Validation:** `validateHash(hash string)` to ensure hash format is valid (64 hex characters)
* **Binary Math:** Helper functions to convert hex string or raw string into binary string (0s and 1s) of specific depth
* **Hashing:** File to SHA256 hex string with error handling; Hex string to CID with error handling
* **File Operations:** `calculateFileHash(filePath)` with proper error handling
* **DHT Operations:** `provideFile(ctx, hash)` with timeout context

### 13. Input Validation & Security
* Validate all hash formats before use
* Prevent path traversal attacks in file operations
* Validate message formats in `readControl` and `readShard`
* Handle all errors explicitly (no ignored errors)
* Use context timeouts for all DHT operations

### 14. Main Function Execution Flow
1. Setup Context with Cancel for graceful shutdown
2. Initialize libp2p Host (Listen on all TCP interfaces)
3. Initialize and start mDNS discovery
4. Bootstrap DHT (Server mode) with error handling
5. Initialize GossipSub with error handling
6. **Shard Init:** Calculate initial shard (depth 1) from Peer ID and create `ShardManager`
7. Initialize worker pool channel
8. Start `shardMgr.Run()`, `inputLoop`, `runReplicationChecker`, `watchFolder`, `runMetricsReporter`, `runRateLimiterCleanup` in goroutines
9. **FileSystem:** Ensure directory exists with error handling
10. **Startup Scan:** Call `scanExistingFiles` (iterating folder and calling `processNewFile`) **before** starting watcher
11. **Graceful Shutdown:** Wait for termination signal (SIGTERM/Interrupt), cancel context, wait for goroutines with timeout, close ShardManager, close DHT, close host

### 15. Error Handling & Resource Management
* All errors must be explicitly handled and logged (no `_ = func()` or `var, _ = func()`)
* Use `log.Fatalf` for fatal initialization errors (never `panic`)
* All tickers must be stopped with `defer ticker.Stop()`
* All contexts must be properly cancelled
* All subscriptions must be cleaned up in `ShardManager.Close()`
* All goroutines must respect context cancellation

### 16. Metrics Export Format
CSV export must include columns in this order:
timestamp, uptime_seconds, pinned_files, known_files, messages_received, messages_dropped, replication_checks, replication_success, replication_failures, low_replication_files, high_replication_files, files_at_target_replication, repl_level_0, repl_level_1, repl_level_2, repl_level_3, repl_level_4, repl_level_5, repl_level_6, repl_level_7, repl_level_8, repl_level_9, repl_level_10plus, avg_replication_level, files_converged_total, files_converged_this_period, dht_queries, dht_query_timeouts, shard_splits, worker_pool_active, active_peers, rate_limited_peers, files_in_backoff, current_shard, cumulative_messages_received, cumulative_messages_dropped, cumulative_replication_checks, cumulative_replication_success, cumulative_replication_failures, cumulative_dht_queries, cumulative_dht_query_timeouts, cumulative_shard_splits

**Important:** Ensure the code handles "Split Storms" by resetting message counter immediately upon detection, handles "Zombie Records" by verifying counts before unpinning, prevents resource leaks through proper cleanup, implements comprehensive error handling, and provides detailed metrics for scientific analysis.
