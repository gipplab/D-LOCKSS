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

# Node Identity
export DLOCKSS_NODE_NAME="my-node"            # Human-readable name (shown in monitor)
export DLOCKSS_IDENTITY_PATH="/data/dlockss.key"  # Persistent identity key location
export DLOCKSS_IPFS_CONFIG="/path/to/ipfs/config" # Kubo config JSON (derives identity from IPFS repo)

# Replication Targets
export DLOCKSS_MIN_REPLICATION=5
export DLOCKSS_MAX_REPLICATION=10

# Network
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"

# DHT tuning
export DLOCKSS_MAX_CONCURRENT_DHT_PROVIDES=8 # Limit concurrent DHT provide operations

# Logging
export DLOCKSS_VERBOSE_LOGGING=true # Enable detailed metrics and status logs
```

#### Node Naming

Nodes can have a human-readable name displayed in the monitor dashboard. The name is resolved in order:

1. `DLOCKSS_NODE_NAME` environment variable (highest priority)
2. Persisted name file (`node_name` alongside the data directory)
3. Interactive prompt on first startup (when running outside Docker/testnet)

Testnet nodes are automatically named `testnet_1`, `testnet_2`, etc.

#### Identity Persistence

The node's libp2p identity (private key) determines its **Peer ID**. The identity is resolved in order:

1. **IPFS config** (`DLOCKSS_IPFS_CONFIG` set): Reads `Identity.PrivKey` from the Kubo config JSON so D-LOCKSS and IPFS share one Peer ID. For Docker, mount the single config file read-only.
2. **Persistent key file** (`DLOCKSS_IDENTITY_PATH` or default `{data_dir_parent}/dlockss.key`): Used when connecting to a remote/Docker Kubo node where the repo is not accessible.
3. **Auto-generated**: If no key exists, a new Ed25519 key is generated and saved to the identity path.

For Docker deployments: either mount the Kubo config file and set `DLOCKSS_IPFS_CONFIG`, or mount a persistent volume and set `DLOCKSS_DATA_DIR` to a subdirectory on it. The identity key, node name, and cluster state are stored alongside the data directory and will survive container rebuilds.

> **Path safety:** The node refuses to start if the identity key, node name, or cluster store would be placed *inside* the ingest directory (`DLOCKSS_DATA_DIR`), since the file watcher would try to ingest them. Always set `DLOCKSS_DATA_DIR` to a dedicated subdirectory (e.g. `./data`, not `.`).

#### Docker Compose Example

```yaml
services:
  ipfs:
    image: ipfs/kubo:latest
    volumes:
      - ipfs-data:/data/ipfs
    ports:
      - "4001:4001"     # Swarm
      - "5001:5001"     # API

  dlockss:
    image: dlockss:latest
    depends_on:
      - ipfs
    volumes:
      - ipfs-data:/ipfs-repo:ro           # read-only access to Kubo config
      - dlockss-data:/data
    environment:
      DLOCKSS_IPFS_CONFIG: /ipfs-repo/config   # derive identity from Kubo
      DLOCKSS_IPFS_NODE: /dns4/ipfs/tcp/5001   # connect to Kubo API
      DLOCKSS_DATA_DIR: /data/ingest
      DLOCKSS_NODE_NAME: my-node

volumes:
  ipfs-data:
  dlockss-data:
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
Open http://localhost:8080. The monitor displays each node's **name** (if configured via `DLOCKSS_NODE_NAME`), falling back to the Peer ID. Names propagate via HEARTBEAT/JOIN messages and appear in the node table, charts, and shard modals. Client-side aliases (EDIT button) override server-side names. Each node has **one peer ID**: when `DLOCKSS_IPFS_CONFIG` is set (e.g. in testnet), D-LOCKSS uses the IPFS repo identity so the same ID appears in the monitor and in `node_x.ipfs.log`.

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
From `testnet/`: `./run_testnet.sh` starts multiple D-LOCKSS nodes and IPFS daemons. Each node is automatically named `testnet_1`, `testnet_2`, etc. (visible in the monitor) and has **one peer ID** (D-LOCKSS loads the identity from the node's IPFS repo via `DLOCKSS_IPFS_CONFIG`). Press Enter in the script to shut down.

### Testing
```bash
go test ./... -v
```

### Project Status
*   **Current Phase:** Production — active refactoring for code quality and operational robustness (see [Code Elegance Plan](docs/CODE_ELEGANCE_PLAN.md)).

---

## 5. Security

*   **Signed Messages:** All protocol messages are signed by the sender's Libp2p key.
*   **Manifest Verification:** ResearchObjects include signatures from the ingester.
*   **Trust Modes:** Supports `open` (default) or `allowlist` trust models.

---

## 6. License
Dual licensed under the [MIT License](LICENSE) or [Apache License 2.0](LICENSE-Apache-2.0), at your option.
