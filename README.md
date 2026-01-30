# D-LOCKSS

**Distributed Lots of Copies Keep Stuff Safe**

> 🚀 **New to D-LOCKSS?** Install in one command:
>
> **Linux/macOS:**
> ```bash
> curl -fsSL https://raw.githubusercontent.com/your-org/dlockss/main/install.sh | sh
> ```
>
> **Windows (PowerShell):**
> ```powershell
> irm https://raw.githubusercontent.com/your-org/dlockss/main/install.ps1 | iex
> ```

## 1. Summary & Vision

**D-LOCKSS** is a decentralized storage network designed to ensure the long-term preservation and authenticity of research data.

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

### Installation
The one-line installer above will:
1.  Detect your OS/Arch.
2.  Download the latest optimized binary.
3.  Install to `~/.local/bin` (Linux/macOS) or `%LOCALAPPDATA%\dlockss\bin` (Windows).
4.  Set up the data directory.

### Usage
1.  **Start the Node:**
    ```bash
    dlockss
    ```
    *(Windows: `dlockss.exe`)*

2.  **Add Files:**
    Copy any file (e.g., PDF) into the `data` directory created by the installer.
    *   Linux/macOS: `~/dlockss-data`
    *   Windows: `%USERPROFILE%\dlockss-data`

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

See [docs/project/REQUIREMENTS.md](docs/project/REQUIREMENTS.md) for detailed specs.

---

## 3. Architecture

D-LOCKSS acts as a self-healing, sharded storage cluster using the IPFS/Libp2p stack.

### Key Components
1.  **Shard Manager:** Dynamically splits responsibilities based on peer count to maintain scalability. Nodes that join late (in ROOT with few peers after others have split) periodically explore deeper shards and join the branch that matches their peer ID so they catch up instead of staying stuck in ROOT.
2.  **File Watcher:** Monitors the data directory to automatically ingest content.
3.  **Replication Checker:** Periodically verifies replication levels (default: every 1 min). Uses dual-query hysteresis to prevent false alarms.
4.  **Storage Monitor:** Protects nodes from disk exhaustion by rejecting custodial requests when full.
5.  **BadBits Manager:** Enforces content blocking (e.g., DMCA) based on configured country codes.

### "Networked RAID" Logic
*   **Striping -> Sharding:** Responsibility for files is determined by a stable hash of the ManifestCID.
*   **Redundancy -> Replication:** Enforces `MinReplication` (5) and `MaxReplication` (10).
*   **Write Cache -> Custodial Mode:** Nodes temporarily hold files they don't own until they can hand them off to the responsible shard.

For deep dives, see:
*   [Protocol Specification](docs/architecture/DLOCKSS_PROTOCOL.md)
*   [Async Replication Pipeline](docs/architecture/ASYNC_REPLICATION_PIPELINE_DESIGN.md)
*   [Logging Strategy](docs/architecture/LOGGING.md)

---

## 4. Development

### Building from Source
Requires Go 1.20+.

```bash
git clone https://github.com/your-org/dlockss
cd dlockss
go build -ldflags="-s -w" -o dlockss .
./dlockss
```

See [docs/development/BUILDING_RELEASES.md](docs/development/BUILDING_RELEASES.md) for release builds.

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
MIT / Apache 2.0 (Dual Licensed)
