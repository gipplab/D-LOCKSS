# D-LOCKSS

**Distributed Lots of Copies Keep Stuff Safe**

> **Build from source:** `go build -o dlockss ./cmd/dlockss` then run `./dlockss` (see [Building from Source](#building-from-source)).

## 1. Summary & Vision

**D-LOCKSS** is a decentralized storage network for long-term preservation and authenticity of research data.

*   **Core Philosophy:** "Networked RAID." Just as RAID protects data across multiple hard drives, D-LOCKSS protects data across a distributed network of peers.
*   **Authenticity:** Relies on Content Addressing (CIDs) to guarantee data integrity.
*   **Scope:** Focuses purely on replication, redundancy, and availability.

### Goals
*   **Speed & Safety:** Combine the speed of IPFS Cluster with the safety of LOCKSS.
*   **Automation:** Fast enough for millions of files, smart enough to maintain replication levels without human intervention.

---

## 2. Quick Start

### Prerequisites
*   **OS:** Linux, macOS, WSL, or Windows 10+.
*   **IPFS:** A running IPFS daemon is required.
    *   [Install IPFS CLI](https://docs.ipfs.tech/install/command-line/)
    *   Run: `ipfs daemon`

### Usage
1.  **Start the Node:** Run the binary (see [Building from Source](#building-from-source) to build it):
    ```bash
    ./dlockss
    ```
    *(Windows: `dlockss.exe`)*

2.  **Add Files:**
    Copy any file (e.g., PDF) into the data directory (default `./data` or `DLOCKSS_DATA_DIR`).
    The node will automatically detect, ingest, pin, and replicate the file.

### Configuration
Configure via environment variables:

```bash
# Data Directory
export DLOCKSS_DATA_DIR="$HOME/my-data"

# Replication Targets
export DLOCKSS_MIN_REPLICATION=5
export DLOCKSS_MAX_REPLICATION=10

# Network
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"

# Logging
export DLOCKSS_VERBOSE_LOGGING=true # Enable detailed metrics and status logs
```

See [docs/DLOCKSS_PROTOCOL.md](docs/DLOCKSS_PROTOCOL.md) for protocol details.

---

## 3. Architecture

D-LOCKSS acts as a self-healing, sharded storage cluster using the IPFS/Libp2p stack.

### Key Components
1.  **Shard Manager:** Dynamically splits responsibilities based on peer count to maintain scalability.
2.  **Cluster Manager:** Manages embedded **IPFS Cluster** instances (one per shard) using **CRDTs** for state consensus; nodes in a shard sync and pin content assigned to that shard.
3.  **File Watcher:** Monitors the data directory to automatically ingest content.
4.  **Storage Monitor:** Protects nodes from disk exhaustion by rejecting custodial requests when full.
5.  **BadBits Manager:** Enforces content blocking (e.g., DMCA) based on configured country codes.

### "Networked RAID" Logic
*   **Striping -> Sharding:** Responsibility for files is determined by a stable hash of the **PayloadCID** (TargetShardForPayload); each file lives in exactly one cluster (shard).
*   **Redundancy -> Cluster Consensus:** Each shard runs an embedded IPFS Cluster CRDT. When a file is ingested, it is "pinned" to the shard's cluster state. All peers in that shard sync this state and automatically pin the content locally.
*   **Write Cache -> Custodial Mode:** Nodes temporarily hold files they don't own until they can hand them off to the responsible shard.

Documentation:
*   [Protocol specification](docs/DLOCKSS_PROTOCOL.md)
*   [Replication performance](docs/REPLICATION_PERFORMANCE.md)
*   Architecture diagrams (PlantUML) in `docs/`

---

## 4. Development

### Building from Source
Requires Go 1.21+.

```bash
git clone https://github.com/gipplab/D-LOCKSS
cd D-LOCKSS
go build -ldflags="-s -w" -o dlockss ./cmd/dlockss
./dlockss
```

Optional monitor (dashboard):
```bash
go build -o dlockss-monitor ./cmd/dlockss-monitor
./dlockss-monitor
```
Open http://localhost:8080. Each node has **one peer ID**: when `IPFS_PATH` is set (e.g. in testnet), D-LOCKSS uses the IPFS repo identity so the same ID appears in the monitor and in `node_x.ipfs.log`.

For geographic region display, optionally provide a GeoIP database:
```bash
./dlockss-monitor --geoip-db /path/to/GeoLite2-City.mmdb
# or via environment variable:
export DLOCKSS_MONITOR_GEOIP_DB=/path/to/GeoLite2-City.mmdb
```
Without a local database, the monitor falls back to the ip-api.com batch API with permanent caching.

The monitor bootstrap-subscribes to all shards up to depth 5 (63 shards) so it can see nodes even when started late. Set `DLOCKSS_MONITOR_BOOTSTRAP_SHARD_DEPTH` (0–12) to tune.

Alternatively use: https://dlockss-monitor.wmcloud.org.

### Testnet
From `testnet/`: `./run_testnet.sh` starts multiple D-LOCKSS nodes and IPFS daemons. Each node has **one peer ID** (D-LOCKSS loads the identity from the node's IPFS repo via `IPFS_PATH`). Press Enter in the script to shut down.

### Testing
```bash
go test ./... -v
```

### Project Status
*   **Current Phase:** Phase 4 (Architecture & Refactoring)

---

## 5. Security

*   **Signed Messages:** All protocol messages are signed by the sender's Libp2p key.
*   **Manifest Verification:** ResearchObjects include signatures from the ingester.
*   **Trust Modes:** Supports `open` (default) or `allowlist` trust models.

---

## 6. License
Dual licensed under the [MIT License](LICENSE) or [Apache License 2.0](LICENSE-Apache-2.0), at your option.
