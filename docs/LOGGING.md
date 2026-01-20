# Logging Conventions

This document describes the logging conventions used throughout the D-LOCKSS codebase.

## Log Level Tags

All log messages use tags in square brackets to indicate their severity and context:

### `[Error]` - Critical Errors
Use for errors that prevent normal operation or indicate a serious problem:
- Failed operations that block core functionality
- Invalid data that cannot be processed
- System-level failures (file system, network, etc.)
- Security violations or blocked operations

**Examples:**
```go
log.Printf("[Error] Failed to initialize IPFS client: %v", err)
log.Printf("[Error] Invalid key format in checkReplication: %s, error: %v", key, err)
log.Printf("[Security] Rejected path outside watch folder: %s", path)
```

### `[Warning]` - Non-Critical Issues
Use for issues that don't prevent operation but should be noted:
- Degraded functionality (e.g., IPFS client unavailable but system continues)
- Recoverable errors
- Configuration issues that fall back to defaults
- Rate limiting or backoff events
- Missing optional resources

**Examples:**
```go
log.Printf("[Warning] Failed to load badBits: %v", err)
log.Printf("[Warning] Missing private key for self peer ID; message signing will be unavailable")
log.Printf("[Warning] DHT query timeout for %s, found %d providers", key, count)
```

### `[Info]` / No Tag - Normal Operations
Use for normal operational messages and status updates:
- Successful operations
- State changes
- Periodic status reports
- Debug information

**Examples:**
```go
log.Printf("[System] IPFS client initialized successfully")
log.Printf("[Storage] Pinned file: %s (total pinned: %d)", key, count)
log.Printf("[Sharding] Active Data Shard: %s", shard)
```

## Context Tags

Additional tags indicate the subsystem or component:

- `[System]` - System initialization and startup
- `[Storage]` - File/storage operations
- `[Replication]` - Replication checking and management
- `[Sharding]` - Shard management and splits
- `[Trust]` - Trust store and authorization
- `[Sig]` - Message signing and verification
- `[RateLimit]` - Rate limiting
- `[Backoff]` - Backoff and retry logic
- `[Metrics]` - Metrics reporting
- `[Cache]` - Cache operations
- `[FileWatcher]` - File system watching
- `[FileOps]` - File ingestion operations
- `[Delegate]` - Delegation messages
- `[Citation]` - Citation resolution
- `[Security]` - Security-related events
- `[Config]` - Configuration

## Error Handling Patterns

### When to Log Errors

1. **Always log** errors that affect user-visible behavior or system state
2. **Always log** errors that indicate potential security issues
3. **Log with context** - include relevant identifiers (file paths, peer IDs, CIDs, etc.)
4. **Use appropriate level** - `[Error]` for critical, `[Warning]` for recoverable

### When to Silently Ignore

Only silently ignore errors in these cases:
- Expected conditions (e.g., file already exists when creating)
- Non-critical operations that have fallbacks
- Cleanup operations where failure is acceptable

**Example:**
```go
if err := os.Mkdir(FileWatchFolder, 0755); err != nil && !os.IsExist(err) {
    log.Printf("[Error] Failed to create directory: %v", err)
}
// os.IsExist is expected and silently ignored
```

### Error Wrapping

Use `fmt.Errorf` with `%w` verb to wrap errors for context:

```go
if err != nil {
    return fmt.Errorf("failed to import file to IPFS: %w", err)
}
```

### Consistent Error Messages

- Start with action verb: "Failed to...", "Unable to...", "Cannot..."
- Include what was being attempted
- Include relevant identifiers
- End with the underlying error

**Good:**
```go
log.Printf("[Error] Failed to convert key to CID: %v", err)
log.Printf("[Error] Invalid key format in checkReplication: %s, error: %v", key, err)
```

**Avoid:**
```go
log.Printf("[Error] Error: %v", err)  // Too generic
log.Printf("Error happened")  // No context
```

## Logging Best Practices

1. **Include identifiers**: Always include relevant IDs (peer IDs, CIDs, file paths) in logs
2. **Truncate long values**: Use `[:min(16, len(value))]+"..."` for long strings
3. **Use structured context**: Include relevant state information
4. **Avoid sensitive data**: Never log private keys, passwords, or sensitive user data
5. **Be consistent**: Use the same format for similar operations across the codebase

## Examples

### Good Logging
```go
log.Printf("[Storage] Pinned ManifestCID: %s (total pinned: %d)", 
    manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", pinnedFiles.Size())

log.Printf("[Replication] Found %d providers for %s (target: %d-%d, sampled up to %d)", 
    count, key[:min(16, len(key))]+"...", MinReplication, MaxReplication, maxCount)

log.Printf("[Error] Failed to initialize IPFS client: %v", err)
log.Printf("[Warning] IPFS client not initialized. Cannot process file: %s", path)
```

### Avoid
```go
log.Printf("Error")  // No context
log.Printf("[Error] %v", err)  // No description
log.Printf("[Storage] Pinned file")  // No identifier or result
```
