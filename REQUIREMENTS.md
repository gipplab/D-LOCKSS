# D-LOCKSS Application Requirements

This document lists all functional and non-functional requirements that form the D-LOCKSS application.

---

## Core System Requirements

### SR-1: P2P Network Node
- The application must run as a peer-to-peer network node using libp2p
- Must listen on all available network interfaces (IPv4 and IPv6)
- Must support TCP connections on dynamically assigned ports
- Must generate and display a unique Node ID on startup
- Must display all network addresses on startup

### SR-2: Distributed Hash Table (DHT)
- Must initialize and bootstrap a Kademlia DHT in server mode
- Must use DHT for global routing and provider record storage
- Must support finding providers for content (CIDs)
- Must support providing content to the DHT network

### SR-3: GossipSub Messaging
- Must initialize GossipSub pubsub system
- Must support publishing messages to topics
- Must support subscribing to topics and receiving messages
- Must filter out messages from self (same node ID)

### SR-4: Content Addressing
- Must use **IPFS CIDs** for content addressing and verification
- Must represent a stored unit as a **ResearchObject**:
  - Manifest: a **dag-cbor block** (ManifestCID)
  - Payload: a **UnixFS DAG** (PayloadCID)
- Must ensure bit-for-bit authenticity of stored content

---

## File Management Requirements

### FM-1: File Watching
- Must watch a designated folder (`./data` by default) for new files recursively
- Must automatically detect new files when they are created in any subdirectory
- Must scan existing files in the watch folder and all subdirectories on startup
- Must recursively watch all subdirectories
- Must automatically add new subdirectories to the watcher when they are created
- Must wait 100ms after file creation before processing (to handle file writes)
- Must only process files (ignore directory creation events for processing, but watch them)

### FM-2: File Import
- Must import each file into IPFS as UnixFS and obtain a **PayloadCID**
- Must create a `ResearchObject` manifest (CBOR) referencing the PayloadCID and obtain a **ManifestCID**
- Must store the manifest in IPFS as a **dag-cbor block** (not just computed locally)

### FM-3: File Pinning
- Must maintain a thread-safe list of pinned objects (by **ManifestCID string**)
- Must support pinning a file (adding to storage)
- Must support unpinning a file (removing from storage)
- Must support checking if a file is pinned
- Must track files in two states: pinned (physically stored) and known (monitored)
- Must check BadBits list before pinning to enforce DMCA takedowns
- Must refuse to pin **ManifestCIDs** that are blocked for the node's configured country
- Must log refusal when a CID is blocked

### FM-4: File Processing
- Must process each new file found in the watch folder
- Must pin every ingested **ManifestCID** locally (recursive pin)
- Must provide **ManifestCID** to DHT network immediately after pinning (only if responsible; custodial nodes do NOT announce to DHT)
- Must track all processed manifests in knownFiles list

---

## Sharding Requirements

### SH-1: Shard Assignment
- Must assign each node a binary prefix shard based on node's Peer ID
- Must calculate initial shard using SHA-256 hash of Peer ID at depth 1
- Must determine responsibility using a **stable hash of the PayloadCID string** (content-based, stable across ingests)
- Must support dynamic shard prefix matching (prefix-based routing)

### SH-2: Shard Management
- Must maintain current shard assignment in thread-safe manner
- Must join a control topic (`dlockss-v2-creative-commons-control`) for delegation messages
- Must join a data shard topic (`dlockss-v2-creative-commons-shard-{prefix}`) based on current shard
- Must support switching shards dynamically
- Must clean up old subscriptions when switching shards

### SH-3: Shard Splitting
- Must monitor peer count in current shard topic periodically (default: every 30 seconds)
- Must use GossipSub `ListPeers()` API to estimate peer count in shard
- Must trigger shard split when peer count exceeds replication-safe threshold (20 peers, 2× safety margin)
- Must prevent over-splitting: only split if estimated peers after split >= 10 peers (2× MinReplication)
- Must ensure shard splits never create shards that cannot achieve MinReplication targets
- Must calculate next shard by increasing binary prefix depth
- Must reset message counter after split
- Must prevent infinite split loops (check if new shard equals old shard)
- Must implement shard overlap state: maintain dual subscription to both old and new shard topics for configurable duration (default: 2 minutes)
- Must publish messages to both old and new topics during overlap period
- Must read messages from both old and new topics during overlap period
- Must clean up old subscription after overlap period expires

### SH-4: Responsibility Determination
- Must determine responsibility for a file based on its **PayloadCID** using stable hashing and prefix matching
- Must support prefix matching at variable depths

---

## Replication Requirements

### RP-1: Replication Targets
- Must maintain minimum replication count of 5 copies
- Must maintain maximum replication count of 10 copies
- Must query DHT to count current number of providers for each **ManifestCID**

### RP-2: Replication Checking
- Must run periodic replication checks every 1 minute (configurable via `DLOCKSS_CHECK_INTERVAL`)
- Must check all known files (both pinned and tracked)
- Must check replication status for each file asynchronously
- Must handle replication checks in background goroutines
- Must implement hysteresis (dual-query verification) to prevent false alarms from transient DHT issues
- Must wait a random delay (default: 30 seconds) between first and second DHT query when under-replication is detected
- Must only trigger NEED messages if both queries confirm under-replication
- Must bound each DHT lookup to a maximum sample size (`DLOCKSS_DHT_MAX_SAMPLE_SIZE`) rather than scanning all providers
- Must cache successful replication counts per ManifestCID for a configurable TTL (`DLOCKSS_REPLICATION_CACHE_TTL`) to reduce redundant DHT queries

### RP-3: Low Replication Handling
- If replication count < 5:
  - If node is responsible and manifest is not pinned: must re-pin the **ManifestCID** (recursive)
  - Must provide the ManifestCID to DHT network
  - Must broadcast a **CBOR `ReplicationRequest`** message to shard topic (only nodes in the same shard receive it)
  - Only responsible nodes should respond to `ReplicationRequest` and announce to DHT (shard-local replication)
  - If node is in custodial mode: must keep holding the file

### RP-4: High Replication Handling
- If replication count > 10 and node is responsible and file is pinned:
  - Must unpin the **ManifestCID** recursively (garbage collection)
  - Must keep file in knownFiles for continued monitoring

### RP-5: Custodial Handoff
- If node is NOT responsible (custodial mode) and replication count >= 5:
  - Must unpin the ManifestCID recursively (handoff complete)
  - Must remove from knownFiles if no longer needed

### RP-6: Tracking Cleanup
- Must remove files from knownFiles if:
  - Node is not responsible for the file AND
  - File is not currently pinned

---

## Custodial Mode Requirements

### CM-1: Custodial Detection
- Must detect when a file belongs to a different shard (not node's responsibility)
- Must enter custodial mode for files not matching node's shard prefix

### CM-2: Custodial Storage
- Must pin files in custodial mode (temporary holding)
- Must broadcast a **CBOR `DelegateMessage`** to control topic (includes ManifestCID + target shard)
- Must keep file pinned until proper replication is achieved

### CM-3: Custodial Delegation
- Must listen for DELEGATE messages on control topic
- Must accept delegation if target shard matches node's shard prefix
- Must track delegated files and check their replication
- Must reject DELEGATE messages when disk usage exceeds high water mark (default: 90%)
- Must continue accepting NEW messages for responsible files even when disk is full

---

## Message Protocol Requirements

### MP-1: Control Topic Messages
- Must use **CBOR** messages (no string parsing)
- Control topic must support `DelegateMessage` (fields: type, manifest_cid, target_shard, sender_id, timestamp, nonce, sig)

### MP-2: Shard Topic Messages
- Must use **CBOR** messages (no string parsing)
- Shard topic must support:
  - `IngestMessage` (fields: type, manifest_cid, shard_id, hint_size, sender_id, timestamp, nonce, sig)
  - `ReplicationRequest` (fields: type, manifest_cid, priority, deadline, sender_id, timestamp, nonce, sig)

### MP-3: Message Publishing
- Must publish `IngestMessage` when responsible node ingests a new ResearchObject
- Must publish `DelegateMessage` when non-responsible node ingests a new ResearchObject
- Must publish `ReplicationRequest` when replication is low
- Must support publishing arbitrary messages from stdin (for testing)

---

## Knowledge Graph Requirements (V2)

### KG-1: ResearchObject Schema
- Must store a `ResearchObject` manifest (CBOR) containing:
  - title, authors, ingester_id, signature, timestamp
  - payload CID (UnixFS DAG)
  - references (list of ManifestCIDs)
  - total size hint
 - Must support canonical CBOR serialization without the signature field for signing and verification

### KG-2: Recursive Pinning
- Pinning a ManifestCID must recursively pin:
  - the manifest itself
  - the entire payload DAG

### KG-3: DAG Completeness Verification
- Replication verification must confirm:
  - manifest block decodes successfully
  - manifest and payload are pinned locally

### KG-4: Liar Detection
- Must compare payload size to manifest `TotalSize`
- On mismatch, must unpin the manifest recursively and mark local state as invalid

---

## Discovery Requirements

### DS-1: mDNS Discovery
- Must use mDNS for local network peer discovery
- Must use service tag "dlockss-v2-prod" for discovery
- Must automatically connect to discovered peers
- Must handle peer found events

### DS-2: Peer Connection
- Must check connection status before connecting
- Must connect to peers automatically when discovered
- Must avoid duplicate connections

---

## Operational Requirements

### OR-1: Startup Sequence
1. Initialize libp2p host
2. Start mDNS service
3. Bootstrap DHT
4. Initialize GossipSub
5. Initialize ShardManager with initial shard
6. Load BadBits CSV file (if exists)
7. Start message handlers (control and shard readers)
8. Start stdin input loop
9. Create watch folder if it doesn't exist
10. Scan existing files
11. Start file watcher
12. Start replication checker
13. Start storage monitor
14. Wait for shutdown signal

### OR-2: Shutdown Handling
- Must listen for SIGTERM and SIGINT signals
- Must display shutdown message
- Must support graceful termination (though not fully implemented)

### OR-3: Logging
- Must log node ID and addresses on startup
- Must log when joining control channel
- Must log active data shard and topic name
- Must log file scanning results
- Must log file processing events (responsible vs custodial)
- Must log replication status changes
- Must log shard splits
- Must log delegation acceptance
- Must log new content announcements

### OR-4: Thread Safety
- Must use mutexes for all shared state access
- Must use RWMutex for read-heavy operations (pinnedFiles, knownFiles)
- Must protect shard state with mutexes
- Must ensure concurrent-safe file operations

---

## Configuration Requirements

### CF-1: Configuration Variables (Environment Variables)
- Control topic name: "dlockss-v2-creative-commons-control" (`DLOCKSS_CONTROL_TOPIC`)
- Discovery service tag: "dlockss-v2-prod" (`DLOCKSS_DISCOVERY_TAG`)
- File watch folder: "./data" (`DLOCKSS_DATA_DIR`)
- Minimum replication: 5 (`DLOCKSS_MIN_REPLICATION`)
- Maximum replication: 10 (`DLOCKSS_MAX_REPLICATION`)
- Replication check interval: 1 minute (`DLOCKSS_CHECK_INTERVAL`)
- Maximum shard load: 2000 messages (`DLOCKSS_MAX_SHARD_LOAD`) - **Deprecated**: kept for backward compatibility, no longer used for splitting
- Maximum peers per shard: 150 (`DLOCKSS_MAX_PEERS_PER_SHARD`) - Split threshold
- Minimum peers per shard: 50 (`DLOCKSS_MIN_PEERS_PER_SHARD`) - Prevent over-splitting
- Shard peer check interval: 30 seconds (`DLOCKSS_SHARD_PEER_CHECK_INTERVAL`) - How often to check peer count
- File processing delay: 100ms (hardcoded)
- Node country: "US" (`DLOCKSS_NODE_COUNTRY`)
- BadBits CSV path: "badBits.csv" (`DLOCKSS_BADBITS_PATH`)
- Shard overlap duration: 2 minutes (`DLOCKSS_SHARD_OVERLAP_DURATION`)
- Replication verification delay: 30 seconds (`DLOCKSS_REPLICATION_VERIFICATION_DELAY`)
- DHT max sample size: 50 (`DLOCKSS_DHT_MAX_SAMPLE_SIZE`)
- Replication cache TTL: 5 minutes (`DLOCKSS_REPLICATION_CACHE_TTL`)
- Disk usage high water mark: 90.0% (`DLOCKSS_DISK_USAGE_HIGH_WATER_MARK`)
- Storage monitor interval: 30 seconds (`DLOCKSS_STORAGE_MONITOR_INTERVAL`)
- IPFS API address: "/ip4/127.0.0.1/tcp/5001" (`DLOCKSS_IPFS_NODE`)
- Metrics report interval: 5 seconds (`DLOCKSS_METRICS_INTERVAL`)
- Rate limit window: 1 minute (`DLOCKSS_RATE_LIMIT_WINDOW`)
- Max messages per window: 100 (`DLOCKSS_MAX_MESSAGES_PER_WINDOW`)
- Initial backoff delay: 5 seconds (`DLOCKSS_INITIAL_BACKOFF`)
- Max backoff delay: 5 minutes (`DLOCKSS_MAX_BACKOFF`)
- Backoff multiplier: 2.0 (`DLOCKSS_BACKOFF_MULTIPLIER`)
- Replication check cooldown: 15 seconds (`DLOCKSS_REPLICATION_COOLDOWN`)
- Removed file cooldown: 2 minutes (`DLOCKSS_REMOVED_COOLDOWN`)
- Trust mode: "open" (`DLOCKSS_TRUST_MODE`)
- Trust store path: "trusted_peers.json" (`DLOCKSS_TRUST_STORE`)
- Signature mode: "warn" (`DLOCKSS_SIGNATURE_MODE`)
- Signature max age: 10 minutes (`DLOCKSS_SIGNATURE_MAX_AGE`)

### CF-2: Network Configuration
- Listen on all interfaces: 0.0.0.0 (IPv4) and :: (IPv6)
- Use dynamic port assignment (port 0)
- Support TCP protocol

---

## Utility Requirements

### UT-1: Binary Prefix Calculation
- Must convert strings to binary prefix using SHA-256
- Must convert hex strings to binary prefix
- Must support variable depth prefixes
- Must extract binary representation from bytes (MSB first)

### UT-2: Hash to CID Conversion
- Must convert SHA-256 hex hash to IPFS CID
- Must use multihash format (code 0x12, length 32)
- Must use CIDv1 with Raw codec

---

## Non-Functional Requirements

### NF-1: Performance
- Must process files asynchronously
- Must handle multiple replication checks concurrently
- Must support high message throughput (up to 2000 messages before shard split)

### NF-2: Reliability
- Must handle file I/O errors gracefully
- Must handle network errors gracefully
- Must continue operating if individual operations fail
- Must not crash on invalid input (though error handling could be improved)

### NF-3: Scalability
- Must support dynamic shard splitting for load distribution
- Must support multiple nodes in the network
- Must handle network growth through sharding

### NF-4: Data Integrity
- Must use content addressing (CIDs) for authenticity
- Must verify file integrity through hash verification
- Must ensure no data loss during custodial handoff

---

## Testing/Development Requirements

### TD-1: Manual Input
- Must support reading messages from stdin
- Must publish stdin messages to shard topic (for testing)

### TD-2: Testnet Support
- Must support running multiple nodes simultaneously
- Must work in isolated directories per node
- Must support testnet deployment scripts

---

## BadBits DMCA Requirements

### BB-1: BadBits Loading
- Must load BadBits CSV file on startup (default: `badBits.csv`)
- Must handle missing BadBits file gracefully (DMCA blocking disabled if file doesn't exist)
- Must parse CSV format: `CID,Country` with header row
- Must support multiple country entries for the same CID
- Must store BadBits data in memory for fast lookups

### BB-2: BadBits Checking
- Must check BadBits before pinning any file
- Must convert file hash to CID for checking
- Must check if CID is blocked for node's configured country
- Must refuse to pin blocked CIDs
- Must log refusal with CID and country information
- Must return boolean indicating pin success/failure

### BB-3: BadBits Format
- CSV file must have header row: `CID,Country`
- Each row must contain: CID (IPFS CID string), Country (ISO country code)
- Must support multiple rows with same CID for different countries
- Must ignore malformed rows gracefully

## Storage Monitoring Requirements

### SM-1: Disk Usage Monitoring
- Must monitor disk usage periodically (default: every 30 seconds)
- Must check disk usage for the data directory
- Must calculate disk usage percentage (used / total * 100)
- Must compare against high water mark (default: 90%)
- Must set disk pressure flag when usage exceeds high water mark
- Must log disk usage and pressure status

### SM-2: Storage-Aware Rate Limiting
- Must check disk pressure before accepting DELEGATE messages
- Must reject DELEGATE messages when disk usage is high
- Must continue accepting NEW messages for responsible files even when disk is full
- Must log rejection with disk usage percentage
- Must distinguish disk pressure rejections from rate limit rejections

## Hysteresis Requirements

### HY-1: Replication Verification
- Must implement dual-query verification for under-replication detection
- Must wait random delay (default: 30 seconds) between first and second DHT query
- Must only trigger NEED messages if both queries confirm under-replication
- Must cancel NEED if second query shows adequate replication
- Must track pending verifications with timestamps
- Must prevent duplicate verification requests for same file

### HY-2: Verification State Management
- Must store pending verification state (first count, first check time, verify time)
- Must clean up completed verifications
- Must handle verification timeouts gracefully

## Shard Overlap Requirements

### SO-1: Overlap State Management
- Must maintain dual subscription to old and new shard topics during overlap period
- Must track overlap state (old shard name, old topic, old subscription, end time)
- Must set overlap duration (default: 2 minutes)
- Must clean up old subscription after overlap expires

### SO-2: Overlap Message Handling
- Must publish messages to both old and new topics during overlap
- Must read messages from both old and new topics during overlap
- Must prevent message loss during shard transitions

## Security & Trust Requirements

### ST-1: Signed Protocol Messages
- All protocol messages (`IngestMessage`, `ReplicationRequest`, `DelegateMessage`) must include:
  - a `Timestamp`,
  - a random `Nonce`,
  - and a cryptographic signature (`Sig`) produced with the sender's libp2p private key.
- Nodes must verify signatures, timestamps, and nonces on incoming messages, according to `DLOCKSS_SIGNATURE_MODE`:
  - `off`: no verification,
  - `warn`: log failures but continue,
  - `strict`: treat failures as hard errors.

### ST-2: Signed ResearchObject Manifests
- Each `ResearchObject` manifest must be signed by the ingester.
- Nodes must verify manifest signatures and reject or warn on invalid manifests based on signature mode and trust configuration.

### ST-3: Trust Store (Optional Allowlist)
- When `DLOCKSS_TRUST_MODE=open`, all peers are accepted.
- When `DLOCKSS_TRUST_MODE=allowlist`, only peers listed in `DLOCKSS_TRUST_STORE` must be accepted for:
  - message processing,
  - and ResearchObject verification.
- The trust store must be loaded from a JSON file containing an array of peer IDs.

## Out of Scope (Explicitly Not Required)

- Rights management (handled via BadBits DMCA system)
- File ownership (removed - using CID-based handling only)
- Access control
- User authentication
- File retrieval/retrieval API (storage only)
- Web UI (future roadmap item)
- Targeted replication (future roadmap item)

---

## Summary Statistics

- **Total Requirements:** 80+
- **Core System:** 4 requirements
- **File Management:** 4 requirements
- **Sharding:** 4 requirements (updated with overlap)
- **Replication:** 6 requirements (updated with hysteresis)
- **Custodial Mode:** 3 requirements (updated with storage protection)
- **Message Protocol:** 3 requirements
- **Discovery:** 2 requirements
- **Operational:** 4 requirements (updated startup sequence)
- **Configuration:** 2 requirements (expanded with all env vars)
- **Utility:** 2 requirements
- **BadBits DMCA:** 3 requirements
- **Storage Monitoring:** 2 requirements
- **Hysteresis:** 2 requirements
- **Shard Overlap:** 2 requirements
- **Non-Functional:** 4 requirements
- **Testing:** 2 requirements


