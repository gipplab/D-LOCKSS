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
    Disable with `DLOCKSS_PIN_INGEST=false`; the poll interval is `DLOCKSS_PIN_INGEST_INTERVAL` (default `1m`).

3.  **Upload to Another Node:**
    A node with `DLOCKSS_WIKI_API` set (e.g. `https://portal.mardi4nfdi.de/w/rest.php`) accepts uploads signed with
    a key that the wiki trusts. Create a key with `ipfs key gen mykey`, register it in the wiki, then:
    ```bash
    cid=$(ipfs add -Q -n --cid-version=1 --raw-leaves --chunker=size-1048576 --hash=sha2-256 --max-file-links=1024 paper.pdf)
    sig=$(printf 'dlockss-upload %s' "$cid" | ipfs key sign -k mykey | jq -r .Signature)
    key=$(ipfs key list -l --ipns-base=b58mh | awk '$2=="mykey"{print $1}')
    curl --data-binary @paper.pdf "http://<node>:5050/upload?cid=$cid&key=$key&sig=$sig&name=paper.pdf"
    ```
    The manifest records the key and the signature. While the wiki is unreachable, the node uses its last answer
    for a key.

### Configuration
Configure via environment variables:

```bash
# Data Directory
export DLOCKSS_DATA_DIR="$HOME/my-data"

# Node Identity
export DLOCKSS_NODE_NAME="my-node"            # Human-readable name (shown in monitor)
export DLOCKSS_IDENTITY_PATH="/data/dlockss.key"  # D-LOCKSS helper key (the node ID is the Kubo peer ID)
export DLOCKSS_IPFS_CONFIG="/path/to/ipfs/config" # Kubo config JSON (RPC auth; not used for the helper key)

# Copies per file: every shard member holds every file of its shard, so the
# shard size bounds the copies (shards split above max and merge below min).
export DLOCKSS_MIN_PEERS_PER_SHARD=6
export DLOCKSS_MAX_PEERS_PER_SHARD=12

# Network
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"
export DLOCKSS_IPFS_TOKEN="" # Kubo RPC token or AuthSecret (bearer:<token>, basic:<user>:<pass>)

# Uploads (empty: disabled); keys are checked with GET {DLOCKSS_WIKI_API}/math/v0/key/{peer ID}
export DLOCKSS_WIKI_API="https://portal.mardi4nfdi.de/w/rest.php"

# DHT tuning
export DLOCKSS_MAX_CONCURRENT_DHT_PROVIDES=8 # Limit concurrent DHT provide operations

# Isolated eval / private testnets (skip public DHT bootstrap + AutoRelay)
export DLOCKSS_PRIVATE_NETWORK=true

# Logging
export DLOCKSS_VERBOSE_LOGGING=true # Enable detailed metrics and status logs

# Who this node will store data from (libp2p peer IDs).
# A non-empty trusted_peers.json next to the data directory enables the same gate.
# Include this node's own peer ID if it should ingest local files.
export DLOCKSS_INGEST_ALLOWLIST="12D3KooWPublisherA,12D3KooWPublisherB"
# export DLOCKSS_TRUST_STORE="/data/trusted_peers.json"
# export DLOCKSS_TRUST_MODE=allowlist   # also restrict protocol/CRDT writers
```

Without `DLOCKSS_IPFS_TOKEN`, the secret is read from `API.Authorizations` in the Kubo config at `DLOCKSS_IPFS_CONFIG`
(the entry named `dlockss`, or the only entry). Restricted `AllowedPaths` must include:

```
/api/v0/version  /api/v0/add  /api/v0/block  /api/v0/pin  /api/v0/files/stat
/api/v0/provide  /api/v0/routing  /api/v0/id  /api/v0/key/sign
```

`/api/v0/id` and `/api/v0/key/sign` are needed at startup: D-LOCKSS reads the node ID and has Kubo sign the helper binding and the manifests with the node key. A node without them does not start.

#### Node Naming

Nodes can have a human-readable name displayed in the monitor dashboard. The name is resolved in order:

1. `DLOCKSS_NODE_NAME` environment variable (highest priority)
2. Persisted name file (`node_name` alongside the data directory)
3. Interactive prompt on first startup (when running outside Docker/testnet)

Testnet nodes are automatically named `testnet_1`, `testnet_2`, etc.

#### Identity Persistence

The **node ID** is the Kubo peer ID. D-LOCKSS runs its own libp2p host with a separate **helper** key and proves the link with a binding signed by Kubo (`docs/DLOCKSS_PROTOCOL.md`, Section 7.1). The helper key is resolved in order:

1. **Persistent key file** (`DLOCKSS_IDENTITY_PATH` or default `{data_dir_parent}/dlockss.key`).
2. **Legacy** `./dlockss.key` in the working directory (copied to the configured path).
3. **Auto-generated** Ed25519 key, saved to the identity path.

`DLOCKSS_IPFS_CONFIG` is only used for the Kubo RPC token when `DLOCKSS_IPFS_TOKEN` is unset.

For Docker deployments: mount a persistent volume and set `DLOCKSS_DATA_DIR` to a subdirectory on it. The helper key, node name, and cluster state live alongside the data directory.

> **Path safety:** The node refuses to start if the identity key, node name, or cluster store would be placed *inside* the ingest directory (`DLOCKSS_DATA_DIR`), since the file watcher would try to ingest them. Always set `DLOCKSS_DATA_DIR` to a dedicated subdirectory (e.g. `./data`, not `.`).

#### Upgrading from a shared peer ID

Earlier versions read `Identity.PrivKey` from `DLOCKSS_IPFS_CONFIG` and ran D-LOCKSS with Kubo's peer ID. The helper binding changes the protocol, so the pubsub prefix moved from `dlockss-v0.0.3` to `dlockss-v0.0.4`: updated and old nodes do not see each other. Update the monitor together with the first node; it then lists only updated nodes. For each node:

1. Update D-LOCKSS and delete a key at `DLOCKSS_IDENTITY_PATH` that was copied from Kubo, so a fresh helper key is created. If the deployment sets `DLOCKSS_PUBSUB_TOPIC_PREFIX` (or the monitor's `-prefix` flag), change it to `dlockss-v0.0.4`.
2. Restart. The node keeps its node ID (the Kubo ID), its shard and its local pin list; the monitor lists it with its first heartbeat.

Until all nodes are updated, files ingested on an old node do not reach updated nodes and vice versa. Stored data is not at risk: each node keeps its CRDT state, and updated nodes reconcile their pin lists once they see each other.

The startup scan re-ingests the files in `DLOCKSS_DATA_DIR`. Kubo signs manifests with a prefix, so a newly signed manifest would get a new CID; the node reuses its pinned manifest with the same content instead. Old manifests keep their old signature, which still verifies.

A node that ran without `DLOCKSS_IPFS_CONFIG` had a peer ID of its own. Its node ID is now the Kubo ID (`ipfs id`): put that on allowlists. The node may move shard, and its files get new manifests because `ingester_id` changes.

#### Docker Compose Example

```yaml
services:
  dlockss-node:
    image: ghcr.io/gipplab/dlockss-single-node:latest
    restart: unless-stopped
    environment:
      DLOCKSS_IPFS_NODE: "/dns4/ipfs/tcp/5001"   # "ipfs" resolves to the Kubo service below
      DLOCKSS_DATA_DIR: "/data/ingest"           # location that D-LOCKSS monitors for ingesting files
      DLOCKSS_IPFS_CONFIG: "/ipfs-repo/config"   # Kubo RPC auth (when DLOCKSS_IPFS_TOKEN is unset)
      # DLOCKSS_NODE_NAME: my-node               # human-readable name shown in the monitor;
      #                                          # if empty the node ID is displayed instead
    volumes:
      - ./dlockss-files:/data                    # persistent D-LOCKSS data (helper key, cluster state, ingested files)
      - ipfs-data:/ipfs-repo:ro                  # read-only access to Kubo config for RPC auth
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

D-LOCKSS acts as a self-healing, sharded storage cluster using the IPFS/Libp2p stack.

### Key Components
1.  **Shard Manager:** Dynamically splits responsibilities based on peer count to maintain scalability. Delegates lifecycle decisions (split/merge/discovery) to a `lifecycleManager` and replication to a `replicationManager`.
2.  **Cluster Manager:** Manages embedded **IPFS Cluster** instances (one per shard) using **CRDTs** for state consensus; nodes in a shard sync and pin content assigned to that shard.
3.  **File Watcher:** Monitors the data directory to automatically ingest content (via `handleWatcherEvent` / `handleNewDirectory`), and polls Kubo's recursive pins to ingest those created by users (`WatchPins`).
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
Requires Go 1.26+.

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

The monitor displays each node's **name** (if configured via `DLOCKSS_NODE_NAME`), falling back to the Kubo node ID. Names propagate via HEARTBEAT/JOIN messages and appear in the node table, charts, and shard modals. Client-side aliases (EDIT button) override server-side names. The helper ID is shown as detail; Identify dials the helper.

The monitor bootstrap-subscribes to all shards up to depth 6 (127 shards) so it can see nodes even when started late. Set `DLOCKSS_MONITOR_BOOTSTRAP_SHARD_DEPTH` (0–12) to tune.

Alternatively use: https://dlockss-monitor.wmcloud.org.

### Testnet
From `testnet/`: `./run_testnet.sh` starts multiple D-LOCKSS nodes and IPFS daemons. Each node is automatically named `testnet_1`, `testnet_2`, etc. (visible in the monitor). The monitor lists Kubo node IDs; each D-LOCKSS process has its own helper key. Press Enter in the script to shut down.

### Testing
```bash
go test ./... -v
```

### Code Layout
Configuration is grouped into nested structs (`Sharding`, `Replication`, `Files`, `Security`, `Orphan`). `ShardManager` delegates replication to `replicationManager` and split/merge/discovery to `lifecycleManager`.

---

## 5. Security

*   **Signed Messages:** All protocol messages are signed by the sender's Libp2p key.
*   **Manifest Verification:** ResearchObjects include signatures from the ingester.
*   **Data origin allowlist:** If `trusted_peers.json` (default: next to the data directory, or `DLOCKSS_TRUST_STORE`) or `DLOCKSS_INGEST_ALLOWLIST` lists at least one valid peer ID, this node stores only files whose manifest `ingester_id` is on that list. List **Kubo node IDs** (`ipfs id`), not D-LOCKSS helper IDs. Enforced on ingest announcements, CRDT pin sync, and replication fetches. Include this node's own Kubo ID to ingest locally. An empty or missing list is default/open mode (accept everyone).
*   **Trust Modes:** `open` (default) or `allowlist`. `allowlist` also restricts shard membership, signed protocol messages and CRDT writers to the helpers of listed nodes (proven by their binding) when that list is non-empty.

---

## 6. License
Dual licensed under the [MIT License](LICENSE) or [Apache License 2.0](LICENSE-Apache-2.0), at your option.
