# Replication Performance Analysis

## Current Bottlenecks

Based on code analysis, the following factors contribute to slow replication convergence:

### 1. **Replication Check Interval** (Default: 1 minute)
- **Location**: `CheckInterval = 1*time.Minute`
- **Impact**: Replication levels are only checked once per minute
- **Effect**: Minimum delay of 1 minute before detecting under-replication

### 2. **Hysteresis Verification Delay** (Default: 30 seconds)
- **Location**: `ReplicationVerificationDelay = 30*time.Second`
- **Impact**: When under-replication is detected, system waits ~30 seconds before triggering replication requests
- **Effect**: Adds 30+ seconds delay before NEED messages are broadcast
- **Rationale**: Prevents false alarms from transient DHT issues

### 3. **Replication Check Cooldown** (Default: 15 seconds)
- **Location**: `ReplicationCheckCooldown = 15*time.Second`
- **Impact**: Prevents checking the same file more than once every 15 seconds
- **Effect**: Limits how quickly replication can be re-checked after a change

### 4. **Replication Cache TTL** (Default: 5 minutes)
- **Location**: `ReplicationCacheTTL = 5*time.Minute`
- **Impact**: Cached replication counts prevent frequent DHT queries
- **Effect**: Replication counts may be stale for up to 5 minutes
- **Trade-off**: Reduces DHT load but slows convergence detection

### 5. **DHT Query Timeout** (Default: 2 minutes)
- **Location**: `context.WithTimeout(ctx, 2*time.Minute)` in `checkReplication()`
- **Impact**: DHT queries can take up to 2 minutes to timeout
- **Effect**: Slow DHT queries delay replication checks

### 6. **DHT Max Sample Size** (Default: 50)
- **Location**: `DHTMaxSampleSize = 50`
- **Impact**: Limits how many providers are queried per DHT lookup
- **Effect**: May underestimate replication count in large networks

### 7. **Worker Pool Limit** (Default: 10 concurrent checks)
- **Location**: `MaxConcurrentReplicationChecks = 10`
- **Impact**: Limits parallelism of replication checks
- **Effect**: With many files, checks are serialized

### 8. **Missing Automatic Replication**
- **Issue**: When a `ReplicationRequest` is received, nodes only check replication - they don't automatically fetch and pin missing files
- **Impact**: Nodes must already have the file to replicate it
- **Effect**: Replication requests don't trigger new replication, only verify existing state

## Total Minimum Delay

For a new file to reach target replication:
1. **Initial check**: Up to 1 minute (CheckInterval)
2. **Verification delay**: ~30 seconds (ReplicationVerificationDelay)
3. **Replication request broadcast**: Immediate
4. **Other nodes check**: Up to 1 minute (their CheckInterval)
5. **Re-check after replication**: Up to 1 minute + 15 seconds cooldown

**Minimum time to convergence**: ~3-4 minutes in ideal conditions
**With DHT delays**: Can be 5-10 minutes or more

## Optimization Options

### Option 1: Reduce Check Interval (Quick Win)
```bash
export DLOCKSS_CHECK_INTERVAL=15s  # Default: 1m
```
**Pros**: Faster detection of under-replication
**Cons**: More DHT queries, higher CPU usage
**Recommendation**: Use 15-30s for testnets

### Option 3: Reduce Replication Cooldown (Quick Win)
```bash
export DLOCKSS_REPLICATION_COOLDOWN=5s  # Default: 15s
```
**Pros**: Faster re-checking after replication changes
**Cons**: More frequent checks of same files
**Recommendation**: Use 5s for testnets

### Option 5: Increase Worker Pool (Moderate Impact)
```bash
export DLOCKSS_MAX_CONCURRENT_CHECKS=20  # Default: 10
```
**Pros**: More parallel replication checks
**Cons**: Higher CPU/memory usage
**Recommendation**: Use 20-30 for testnets with many files

### Option 8: Implement Automatic Replication (Major Feature)
**Code Change Required**: Add logic to fetch and pin files when receiving ReplicationRequest
**Pros**: Actually triggers replication, not just checks
**Cons**: Requires IPFS content fetching, bandwidth usage
**Recommendation**: High priority for production

## Recommended Testnet Configuration

For faster convergence in testnets, use:

```bash
export DLOCKSS_CHECK_INTERVAL=15s
export DLOCKSS_REPLICATION_VERIFICATION_DELAY=5s
export DLOCKSS_REPLICATION_COOLDOWN=5s
export DLOCKSS_REPLICATION_CACHE_TTL=30s
export DLOCKSS_MAX_CONCURRENT_CHECKS=20
export DLOCKSS_DHT_MAX_SAMPLE_SIZE=100
```

This reduces minimum convergence time from ~3-4 minutes to ~30-60 seconds.

## Production Considerations

For production networks:
- Keep `ReplicationVerificationDelay` at 30s to prevent false alarms
- Keep `ReplicationCacheTTL` at 5m to reduce DHT load
- Keep `CheckInterval` at 1m for reasonable resource usage
- Consider implementing Option 8 (automatic replication) for better convergence

## Monitoring

Watch these metrics to understand replication performance:
- `replicationChecks`: Number of checks performed
- `dhtQueries`: Number of DHT queries
- `dhtQueryTimeouts`: DHT query failures
- `filesAtTargetReplication`: Files with adequate replication
- `lowReplicationFiles`: Files needing replication
- `avgReplicationLevel`: Average replication across all files
