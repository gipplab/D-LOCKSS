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
    *   [Install IPFS CLI](https://docs.ipfs.tech/install/command-line/) (Kubo **v0.43.1+**)
    *   Run: `ipfs init --profile=unixfs-v1-2025` (first time) then `ipfs daemon`. Existing repos: `ipfs config profile apply unixfs-v1-2025` then restart the daemon.

### Usage
1.  **Start the Node:** Run the binary (see [Building from Source](#building-from-source) to build it):
    ```bash
    ./dlockss
    ```
    *(Windows: `dlockss.exe`)*

2.  **Add Files:**
    Either pin content in the node's Kubo (`ipfs pin add <cid>`, or "Set pinning" in the Kubo WebUI / IPFS Desktop),
    or copy any file (e.g., PDF) into the data directory (default `./data` or `DLOCKSS_DATA_DIR`).
    The node will automatically detect, ingest, pin, and replicate it.

    A pinned directory is ingested as one object. A pin name (`ipfs pin add --name paper.pdf <cid>`) becomes the
    metadata reference, otherwise `ipfs://<cid>`. Files that are only in the WebUI's *Files* view (MFS) are not
    pinned and are not ingested. D-LOCKSS labels its own manifest pins `dlockss:manifest` and never ingests them
    or the payloads they reference. Unpinning in Kubo does not remove ingested content from D-LOCKSS.
    Disable with `DLOCKSS_PIN_INGEST=false` (the poll interval is fixed at 1m).

    Folder ingest is a periodic rescan: a file is picked up once its size is stable across two scans (up to ~1 minute).

### Troubleshooting
*   **"IPFS is not available"** — start the daemon (`ipfs daemon`) and check its API address (`ipfs config Addresses.API`) against `DLOCKSS_IPFS_NODE`.
*   **No peers** — open Kubo's swarm ports 4001/tcp and 4001/udp (the D-LOCKSS node itself uses random ports and NAT traversal).

### Configuration
Configure via environment variables:

```bash
# Data Directory
export DLOCKSS_DATA_DIR="$HOME/my-data"

# Node Identity
export DLOCKSS_NODE_NAME="my-node"            # Human-readable name (shown in monitor)
export DLOCKSS_IDENTITY_PATH="/data/dlockss.key"  # Persistent identity key location (fallback if IPFS node config cannot be read)
export DLOCKSS_IPFS_CONFIG="/path/to/ipfs/config" # Kubo config JSON (derives identity from IPFS repo)

# Copies per file: every shard member holds every file of its shard, so the
# shard size bounds the copies (shards split above max and merge below min).
export DLOCKSS_MIN_PEERS_PER_SHARD=6
export DLOCKSS_MAX_PEERS_PER_SHARD=12

# Network
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"

# Isolated eval / private testnets (skip public DHT bootstrap + AutoRelay)
export DLOCKSS_PRIVATE_NETWORK=true

# Logging
export DLOCKSS_VERBOSE_LOGGING=true # Enable detailed metrics and status logs

# Who this node will store data from (libp2p peer IDs).
# A non-empty trusted_peers.json next to the data directory enables the same gate.
# Include this node's own peer ID if it should ingest local files.
export DLOCKSS_INGEST_ALLOWLIST="12D3KooWPublisherA,12D3KooWPublisherB"
# export DLOCKSS_TRUST_STORE="/data/trusted_peers.json"
# export DLOCKSS_TRUST_MODE=allowlist   # also restrict protocol message senders
```

Protocol timings, concurrency bounds, and security parameters are frozen
constants; setting an unknown `DLOCKSS_*` variable logs a warning. The full
knob list is in [docs/DLOCKSS_PROTOCOL.md](docs/DLOCKSS_PROTOCOL.md) §9.

#### Node Naming

Nodes can have a human-readable name displayed in the monitor dashboard. The name is resolved in order:

1. `DLOCKSS_NODE_NAME` environment variable (highest priority, persisted)
2. Persisted name file (`node_name` alongside the data directory)

Unnamed nodes show their peer ID in the monitor. Testnet nodes are automatically named `testnet_1`, `testnet_2`, etc.

#### Identity Persistence

The node's libp2p identity (private key) determines its **Peer ID**. The identity is resolved in order:

1. **IPFS config** (`DLOCKSS_IPFS_CONFIG` set): Reads `Identity.PrivKey` from the Kubo config JSON so D-LOCKSS and IPFS share one Peer ID. For Docker, mount the single config file read-only.
2. **Persistent key file** (`DLOCKSS_IDENTITY_PATH` or default `{data_dir_parent}/dlockss.key`): Used when connecting to a remote/Docker Kubo node where the repo is not accessible.
3. **Auto-generated**: If no key exists, a new Ed25519 key is generated and saved to the identity path.

For Docker deployments: either mount the Kubo config file and set `DLOCKSS_IPFS_CONFIG`, or mount a persistent volume and set `DLOCKSS_DATA_DIR` to a subdirectory on it. The identity key, node name, and cluster state are stored alongside the data directory and will survive container rebuilds.

> **Path safety:** The node refuses to start if the identity key, node name, or cluster store would be placed *inside* the ingest directory (`DLOCKSS_DATA_DIR`), since the ingest scanner would pick them up. Always set `DLOCKSS_DATA_DIR` to a dedicated subdirectory (e.g. `./data`, not `.`).

#### Docker Compose Example

```yaml
services:
  dlockss-node:
    image: ghcr.io/gipplab/dlockss-single-node:latest
    restart: unless-stopped
    environment:
      DLOCKSS_IPFS_NODE: "/dns4/ipfs/tcp/5001"   # "ipfs" resolves to the Kubo service below
      DLOCKSS_DATA_DIR: "/data/ingest"           # location that D-LOCKSS monitors for ingesting files
      DLOCKSS_IPFS_CONFIG: "/ipfs-repo/config"   # derive identity from IPFS node (shared peer ID)
      # DLOCKSS_NODE_NAME: my-node               # human-readable name shown in the monitor;
      #                                          # if empty the peer ID is displayed instead
    volumes:
      - ./dlockss-files:/data                    # persistent D-LOCKSS data (identity, cluster state, ingested files)
      - ipfs-data:/ipfs-repo:ro                  # read-only access to Kubo config for identity
      # Place trusted_peers.json at ./dlockss-files/trusted_peers.json
      # (inside the container: /data/trusted_peers.json). Restart to apply.
    depends_on:
      - ipfs
    labels:
      - com.centurylinklabs.watchtower.enable=true

  ipfs:
    image: ipfs/kubo:v0.43.1
    restart: unless-stopped
    # Uncomment if you can forward port 4001 (TCP+UDP) for better peering:
    # ports:
    #   - 4001:4001/tcp
    #   - 4001:4001/udp
    volumes:
      - ipfs-staging:/export
      - ipfs-data:/data/ipfs
      # Existing volumes ignore IPFS_PROFILE; this applies unixfs-v1-2025 every start.
      - ./docker/ipfs-init.d:/container-init.d:ro
    environment:
      - IPFS_PROFILE=server,unixfs-v1-2025
    healthcheck:
      test: ["CMD-SHELL", "ipfs id || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5

  ### Optional services

  # Watchtower keeps the D-LOCKSS image up to date automatically.
  # Recommended until a stable release is published.
  watchtower:
    image: containrrr/watchtower
    restart: always
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    command: --label-enable --include-stopped --revive-stopped

volumes:
  ipfs-staging:  # IPFS staging area on /export
  ipfs-data:     # IPFS repo on /data/ipfs (shared read-only with D-LOCKSS for identity)
```

Ingest uses the IPIP-0499 **`unixfs-v1-2025`** profile (CIDv1, 1 MiB raw leaves). `IPFS_PROFILE` applies only when the Kubo volume is first initialized; mount `docker/ipfs-init.d` as above so existing volumes get the same import settings. One-shot on a running host: `docker compose exec ipfs ipfs config profile apply unixfs-v1-2025` then restart the `ipfs` service.

See [docs/DLOCKSS_PROTOCOL.md](docs/DLOCKSS_PROTOCOL.md) for protocol details.

---

## 3. Architecture

D-LOCKSS acts as a self-healing, sharded storage network on the IPFS/libp2p stack.

### Key Components
1.  **Shard Engine** (`internal/shard`): membership tracking, dynamic split/merge/discovery, the replication checker, and one placement pass that re-homes every file (custodial handoff, orphan handoff, and post-move resharding in a single loop).
2.  **Pin Sets** (`internal/pinset`): each shard has a persistent replicated pin set — the manifests every member must hold. A per-shard tracker reconciles the set against Kubo with bounded background fetches. Convergence rides entirely on the signed message protocol (IngestMessage + ReplicationRequest + PINNED announcements); there is no embedded consensus layer.
3.  **Ingest** (`internal/ingest`): a recursive folder watcher and a Kubo pin watcher feed one shared pipeline that imports, wraps, signs, pins, and announces content.
4.  **Kubo client** (`internal/kubo`): a minimal, fully context-aware HTTP RPC client (~12 endpoints) plus a serialized DHT provide worker with retries.
5.  **Security** (`internal/trust`, `internal/protocol`, `internal/badbits`): signed messages with replay protection, origin allowlisting, per-peer rate limiting, and a CID denylist.
6.  **Disk monitor** (`internal/store`): above the high-water mark the node advertises PASSIVE, steering custodial and replication load away.

### "Networked RAID" Logic
*   **Striping -> Sharding:** Responsibility for a file is a stable hash of its **PayloadCID**; each file lives in exactly one shard.
*   **Redundancy -> Shard pin sets:** Every member of a shard pins every file in the shard's pin set, so the shard size (bounded by min/max peers) is the copy count. Periodic ReplicationRequests act as anti-entropy, so late joiners and restarted nodes converge.
*   **Write Cache -> Custodial Mode:** Nodes temporarily hold files they don't own until the responsible shard demonstrably has them (DHT provider checks), then release.

Documentation: [docs/DLOCKSS_PROTOCOL.md](docs/DLOCKSS_PROTOCOL.md) is the canonical specification — wire protocol, node behavior, configuration, monitor, and the differences from the original implementation.

---

## 4. Development

### Building from Source
Requires Go 1.25+.

```bash
git clone https://github.com/gipplab/D-LOCKSS
cd D-LOCKSS
go build -ldflags="-s -w" -o dlockss ./cmd/dlockss
./dlockss
```

Optional monitor (dashboard):
```bash
go build -o dlockss-monitor ./cmd/dlockss-monitor
./dlockss-monitor                           # uses default topic (creative-commons)
./dlockss-monitor -topic my-archive         # monitor a specific topic
./dlockss-monitor -topic my-archive -prefix dlockss-v0.0.4  # custom topic + prefix
```
Open http://localhost:8080. The `-topic` and `-prefix` flags override the `DLOCKSS_TOPIC_NAME` and `DLOCKSS_PUBSUB_TOPIC_PREFIX` environment variables respectively. The topic is fixed at startup (the dashboard displays it read-only).

Keyword search is optional. The dashboard **SETTINGS** button chooses the provider and API key. The first key you save is the password for later changes: settings asks for the API key currently in use. Google AI Studio uses `gemini-2.5-flash-lite` (the stable Gemini model with the highest free-tier daily quota, about 1,000 requests per day). Settings are stored in `~/.dlockss-monitor/llm_settings.json`. You can also set a key before start:

```bash
# ~/.dlockss-monitor/.api_key  (preferred), or ./.api_key, or:
export SAIA_API_KEY="..."
# optional:
# export DLOCKSS_LLM_MODEL=meta-llama-3.1-8b-instruct
# export DLOCKSS_LLM_API_BASE=https://chat-ai.academiccloud.de/v1
# Payload fetches use the Kubo gateway on the compose network by default.
# Change it in the dashboard SETTINGS dialog, or before start:
# export DLOCKSS_IPFS_GATEWAY=http://ipfs:8080
# The browser loads files through the monitor (/ipfs/<cid>), so port 8080
# stays unpublished. Kubo's 8080 is not the dashboard port.
# export DLOCKSS_MONITOR_DATA_DIR="$HOME/.dlockss-monitor"
```

Keywords are generated once per item at index time and searched locally. The indexer reads:

*   PDFs (needs `pdftotext` from `poppler-utils` on the monitor host; without it PDFs are skipped)
*   Word, Excel, PowerPoint, OpenDocument, and EPUB files
*   text formats such as plain text, Markdown, CSV/TSV, JSON, XML, HTML, YAML, LaTeX, BibTeX, and source code
*   directories, from their file listing plus excerpts of up to five readable files

Images, audio, video, archives, other binaries, and files over 20 MB that need a full download (PDF and office formats) are skipped.

The monitor displays each node's **name** (if configured via `DLOCKSS_NODE_NAME`), falling back to the Peer ID. Names propagate via HEARTBEAT/JOIN messages and appear in the node table, charts, and shard modals. Client-side aliases (EDIT button) override server-side names. Each node has **one peer ID**: when `DLOCKSS_IPFS_CONFIG` is set (e.g. in testnet), D-LOCKSS uses the IPFS repo identity so the same ID appears in the monitor and in `node_x.ipfs.log`.

The monitor subscribes to the root shard and follows SPLIT announcements (rebroadcast to ancestor topics every 60 s) down the tree, so it sees nodes even when started late.

Alternatively use: https://dlockss-monitor.wmcloud.org.

### Testnet
From `testnet/`: `./run_testnet.sh` starts multiple D-LOCKSS nodes and IPFS daemons. Each node is automatically named `testnet_1`, `testnet_2`, etc. (visible in the monitor) and has **one peer ID** (D-LOCKSS loads the identity from the node's IPFS repo via `DLOCKSS_IPFS_CONFIG`). Press Enter in the script to shut down.

### Testing
```bash
go test ./... -v
```

### Code Layout
Configuration is grouped into nested structs (`Sharding`, `Replication`, `Files`, `Security`, `Orphan`) in `internal/config`. The node is composed of small packages: `schema` (wire formats), `protocol` (signing, nonces, rate limits, text messages), `kubo` (IPFS RPC), `pinset` (per-shard replicated pin sets), `shard` (the engine), `ingest` (watchers + pipeline), `store` (registries, provides, disk), `trust`, `badbits`, `identity`, `p2p`, and `api`. The monitor lives in `internal/monitor` with keyword indexing in `internal/keywords`.

---

## 5. Security

*   **Signed Messages:** All protocol messages are signed by the sender's Libp2p key.
*   **Manifest Verification:** ResearchObjects include signatures from the ingester.
*   **Data origin allowlist:** If `trusted_peers.json` (default: next to the data directory, or `DLOCKSS_TRUST_STORE`) or `DLOCKSS_INGEST_ALLOWLIST` lists at least one valid peer ID, this node stores only files whose manifest `ingester_id` is on that list. Enforced on ingest announcements, CRDT pin sync, and replication fetches. Include this node's own peer ID to ingest locally. An empty or missing list is default/open mode (accept everyone).
*   **Trust Modes:** `open` (default) or `allowlist`. `allowlist` also restricts signed protocol messages and CRDT writers to the same peer list when that list is non-empty.

---

## 6. License
Dual licensed under the [MIT License](LICENSE) or [Apache License 2.0](LICENSE-Apache-2.0), at your option.
