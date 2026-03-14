# Replication Performance Analysis

## Architecture Overview

Replication in D-LOCKSS is driven by two complementary mechanisms:

1. **CRDT Cluster Sync** — Each shard runs an embedded IPFS Cluster with CRDT consensus. When a file is pinned to a shard's cluster, the `LocalPinTracker` on every peer in that shard automatically syncs and pins the content locally.

2. **ReplicationRequest Protocol** — The `replicationManager` (extracted from `ShardManager`) periodically broadcasts `ReplicationRequest` messages for pinned manifests. Peers that don't yet have the file perform **auto-replication**: fetch via `PinRecursive` and add to the cluster.

## Key Constants and Defaults

| Parameter | Default | Env Variable | Location |
|-----------|---------|-------------|----------|
| Replication Check Interval | 1 minute | `DLOCKSS_CHECK_INTERVAL` | `config.Replication.CheckInterval` |
| Root Shard Check Interval | 20 seconds | (hardcoded) | `rootReplicationCheckInterval` |
| Request Cooldown Per Manifest | 5 minutes | (hardcoded) | `replicationRequestCooldownDuration` |
| Max Requests Per Cycle | 50 | (hardcoded) | `maxReplicationRequestsPerCycle` |
| Auto-Replication Enabled | true | `DLOCKSS_AUTO_REPLICATION_ENABLED` | `config.Replication.AutoReplicationEnabled` |
| Auto-Replication Timeout | 5 minutes | `DLOCKSS_AUTO_REPLICATION_TIMEOUT` | `config.Replication.AutoReplicationTimeout` |
| Max Concurrent Checks | 5 | `DLOCKSS_MAX_CONCURRENT_CHECKS` | `config.Replication.MaxConcurrentReplicationChecks` |
| Pin Reannounce Interval | 2 minutes | `DLOCKSS_PIN_REANNOUNCE_INTERVAL` | `config.Replication.PinReannounceInterval` |
| Min Replication | 5 | `DLOCKSS_MIN_REPLICATION` | `config.Replication.MinReplication` |
| Max Replication | 10 | `DLOCKSS_MAX_REPLICATION` | `config.Replication.MaxReplication` |

## Convergence Timeline

For a newly ingested file to reach full replication across a shard:

1. **Ingest** (immediate): File pinned locally, `IngestMessage` broadcast to shard, cluster `Pin()` called.
2. **CRDT Sync** (seconds): Cluster state propagates to peers via PubSub; `LocalPinTracker` detects new pin and starts `PinRecursive`.
3. **First Replication Check** (up to 20s at root, 1m elsewhere): `replicationManager.runChecker()` sends `ReplicationRequest` for all pinned manifests.
4. **Auto-Replication** (seconds to minutes): Peers receiving the request that don't have the file fetch it via `PinRecursive` (up to 5-minute timeout).
5. **Cooldown** (5 minutes): After sending a request for a manifest, no new request is sent for that manifest for 5 minutes.

**Typical convergence**: Most files replicate within 1-2 minutes via CRDT sync alone. Files that fail the initial sync (large DAGs, slow block propagation) recover on the next replication cycle after the 5-minute cooldown.

## Current Bottlenecks

### 1. Request Cooldown (5 minutes)

Once a `ReplicationRequest` is sent for a manifest, `replicationRequestCooldownDuration` prevents resending for 5 minutes. If the first request fails (e.g., the receiving peer's `PinRecursive` times out), the file appears "stuck" until the cooldown expires.

**Mitigation**: The cooldown prevents flooding but causes visible delays for files that fail on the first attempt.

### 2. Auto-Replication Timeout (5 minutes)

`PinRecursive` for large files or over slow links may hit the `AutoReplicationTimeout`. The file remains unreplicated until the next replication cycle.

**Mitigation**: Heartbeat-driven re-pin gradually fills in missing blocks (see below).

### 3. Concurrent Replication Limit (5)

The `replicationManager.sem` channel limits concurrent auto-replications to `MaxConcurrentReplicationChecks` (default 5). When all slots are occupied, additional `ReplicationRequest` messages are silently dropped.

**Mitigation**: Increase `DLOCKSS_MAX_CONCURRENT_CHECKS` for nodes with sufficient bandwidth.

### 4. Max Requests Per Cycle (50)

At most 50 `ReplicationRequest` messages are sent per checker cycle. With thousands of files, not all manifests are requested in a single cycle.

**Mitigation**: Subsequent cycles pick up remaining manifests. The cooldown map ensures already-sent requests aren't duplicated.

## Heartbeat-Driven Gradual DAG Completion (Built-In)

Every heartbeat (~10s), each node picks **one** pinned manifest CID (round-robin) and:

1. **Re-pins the ManifestCID recursively** (`PinRecursive`, 2-minute timeout). Idempotent — returns instantly when the DAG is already complete locally, and incrementally fetches missing blocks otherwise.
2. **Pins the PayloadCID as its own root** so Kubo's reprovider (`pinned` strategy) re-announces it.
3. **Provides both CIDs to the DHT** (only if the re-pin succeeded).

A `CompareAndSwap` guard prevents concurrent re-provides from piling up.

**Impact**: Resource-constrained nodes (e.g., Raspberry Pis) that failed the initial `PinRecursive` gradually complete the DAG over successive heartbeats without manual intervention. DHT provider records (which expire after ~24h) are kept fresh.

## Optimization Options

### Reduce Check Interval (Quick Win)
```bash
export DLOCKSS_CHECK_INTERVAL=15s  # Default: 1m
```
Faster detection at non-root shards. Root shards already check every 20s.

### Increase Concurrent Checks (Moderate Impact)
```bash
export DLOCKSS_MAX_CONCURRENT_CHECKS=10  # Default: 5
```
More parallel auto-replications. Higher bandwidth usage.

### Increase Auto-Replication Timeout (Large Files)
```bash
export DLOCKSS_AUTO_REPLICATION_TIMEOUT=10m  # Default: 5m
```
Allows more time for large DAG fetches. Ties up semaphore slots longer.

## Recommended Testnet Configuration

For faster convergence in testnets:

```bash
export DLOCKSS_CHECK_INTERVAL=15s
export DLOCKSS_MAX_CONCURRENT_CHECKS=10
```

## Production Considerations

- Keep `CheckInterval` at 1m for reasonable resource usage (root shards already use 20s).
- Keep `AutoReplicationTimeout` at 5m unless dealing with consistently large files.
- The 5-minute request cooldown is a deliberate trade-off between convergence speed and network overhead; files that fail on the first attempt self-heal after the cooldown expires.

## Monitoring

The monitor's `replication snapshot` log line reports:
- `total_manifests`: Number of known manifests
- `total_at_target`: Files with replica count >= min(MinReplication, shard_peer_count)
- `avg_replication`: Average replica count across all manifests

Node daemon logs to watch:
- `"auto-replication: fetched and pinned"` — successful auto-replication
- `"auto-replication: failed to fetch/pin"` — `PinRecursive` timeout or failure
- `"auto-replication skipped, concurrency limit reached"` — semaphore full
- `"ReplicationRequest sent"` — outbound request (debug level)
