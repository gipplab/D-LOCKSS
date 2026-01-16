# D-LOCKSS Chart Guide

```bash
uv run --with-requirements requirements.txt generate_charts.py --auto
```

### Storage Metrics
Shows the number of pinned files (physically stored) and known files (monitored) for a single node, plus low and high replication file counts over time.

### Network Metrics
Tracks message traffic (received/dropped), peer connectivity (active/rate-limited), and DHT query activity (queries/timeouts) for a single node.

### Replication Metrics
Monitors replication check activity (checks, success, failures) per period and cumulative totals since node startup.

### Performance Metrics
Tracks worker pool utilization (active workers) and failed operations (files in backoff) for a single node.

### Cumulative Metrics
Shows long-term trends for cumulative messages and DHT queries since node startup (never reset).



## Network-Wide Overview Charts

### Overview Storage Metrics
Aggregates total pinned files, known files, and low/high replication files across all nodes in the network.

### Overview Network Metrics
Combines message traffic, peer connectivity, and DHT activity from all nodes to show network-wide communication patterns.

### Overview Replication Metrics
Aggregates replication check activity (checks, success, failures) across all nodes to show network-wide replication maintenance.

### Overview Convergence Metrics
Tracks how files move toward target replication (5-10 copies) across the network, showing files at target, average replication level, and convergence rate.

### Overview Replication Distribution
Shows the exact distribution of files across all replication levels (0-10+) network-wide using stacked area and line charts.

### Overview Performance Metrics
Aggregates worker pool utilization and files in backoff across all nodes to show network-wide resource usage.

### Overview Cumulative Metrics
Shows long-term network-wide trends for cumulative messages and DHT queries since testnet startup (never reset).