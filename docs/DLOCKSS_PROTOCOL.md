# D-LOCKSS Protocol Specification (V2)

## 1. Overview

D-LOCKSS (Distributed Lots of Copies Keep Stuff Safe) is a decentralized preservation network that ensures long-term data durability through cooperative replication. It functions as a "Networked RAID", where independent nodes collaboratively maintain a target replication level for datasets.

The protocol is built on top of the **IPFS** (InterPlanetary File System) and **libp2p** stack, utilizing content-addressing, peer-to-peer messaging (GossipSub), and distributed hash tables (DHT).

### Key Concepts

*   **Research Object (RO)**: The fundamental unit of preservation. A CBOR-encoded manifest wrapping the actual file content (Payload) with metadata and signatures.
*   **ManifestCID**: The Content ID of the Research Object. Used for tracking and referencing.
*   **PayloadCID**: The Content ID of the actual raw data/file.
*   **Responsibility**: Nodes are assigned to "shards". A node is responsible for a file if the file's PayloadCID hash maps to the node's shard prefix.
*   **Custodial Mode**: A node temporarily holds a file it is not responsible for until a responsible node accepts it.

---

## 2. Network Layer

*   **Transport**: libp2p (TCP/QUIC/WebSockets).
*   **Discovery**: Kademlia DHT (LAN/WAN) + mDNS (Local).
*   **PubSub**: GossipSub v1.1.
*   **Content Transfer**: IPFS Bitswap.

### 2.1 Topic Naming

The network is partitioned into PubSub topics to ensure scalability:

*   **Control Topic**: `dlockss-v2-creative-commons-control`
    *   Used for cross-shard communication (e.g., Delegation).
    *   All nodes subscribe to this.
*   **Shard Topics**: `dlockss-v2-creative-commons-shard-<prefix>`
    *   Examples: `...-shard-0`, `...-shard-1`, `...-shard-01`.
    *   Nodes subscribe only to the shard topic matching their peer ID prefix.
    *   Used for high-frequency coordination (Ingest, Replication Requests).

---

## 3. Data Structures

All data structures are serialized using **DAG-CBOR** (Concise Binary Object Representation).

### 3.1 Research Object (The Manifest)

The immutable record representing a preserved digital artifact.

```go
type ResearchObject struct {
    Title      string   `cbor:"title"`       // Human-readable title
    Authors    []string `cbor:"authors"`     // List of authors
    IngestedBy peer.ID  `cbor:"ingester_id"` // PeerID of ingesting node
    Signature  []byte   `cbor:"sig"`         // Ed25519/RSA signature of the object
    Timestamp  int64    `cbor:"ts"`          // Unix timestamp of creation
    Payload    cid.Cid  `cbor:"payload"`     // CID of the raw content (UnixFS DAG root)
    References []cid.Cid `cbor:"refs"`       // CIDs of related ResearchObjects
    TotalSize  uint64   `cbor:"size"`        // Size of Payload in bytes
}
```

---

## 4. Message Protocol

Messages are propagated over GossipSub. All messages include a signature (`sig`) and nonce (`nonce`) for authentication and replay protection.

### 4.1 Message Types

| Type | ID | Purpose | Topic |
|------|----|---------|-------|
| `Ingest` | 1 | Announce new file entry | Shard |
| `ReplicationRequest` | 2 | Request more replicas (NEED) | Shard |
| `Delegate` | 3 | Hand off file to responsible shard | Control |
| `UnreplicateRequest` | 4 | Request dropping excess replicas | Shard |

### 4.2 IngestMessage

Broadcast when a node ingests a new file.

```cbor
{
  "type": 1,
  "manifest_cid": <CID>,
  "shard_id": <string>,    // e.g. "01"
  "hint_size": <uint64>,
  "sender_id": <PeerID>,
  "ts": <int64>,
  "nonce": <bytes>,
  "sig": <bytes>
}
```

### 4.3 ReplicationRequest

Broadcast when a node detects a file is under-replicated.

```cbor
{
  "type": 2,
  "manifest_cid": <CID>,
  "priority": <uint8>,     // 0=Low, 1=High
  "deadline": <int64>,     // 0 = no deadline
  "sender_id": <PeerID>,
  "ts": <int64>,
  "nonce": <bytes>,
  "sig": <bytes>
}
```

### 4.4 DelegateMessage

Sent when a custodial node (holding a file it's not responsible for) wants to hand it off to the responsible shard.

```cbor
{
  "type": 3,
  "manifest_cid": <CID>,
  "target_shard": <string>, // e.g. "10" (Target prefix)
  "sender_id": <PeerID>,
  "ts": <int64>,
  "nonce": <bytes>,
  "sig": <bytes>
}
```

---

## 5. Sharding Protocol

D-LOCKSS uses dynamic sharding to partition responsibility.

### 5.1 Responsibility Assignment

*   **Node Identity**: Each node has a `currentShard` (binary string, e.g., "01"). This is a prefix of its PeerID (SHA-256 hash).
*   **File Assignment**: A file's `PayloadCID` is hashed (SHA-256).
*   **Match Rule**: A node is responsible for a file if the node's `currentShard` is a prefix of the file hash.

### 5.2 Dynamic Splitting

When a shard becomes too large (too many peers):
1.  **Detection**: `PeerCount > MaxPeersPerShard` (default 20).
2.  **Split**: Node extends its `currentShard` by 1 bit (e.g., "0" -> "01").
3.  **Overlap**: Node subscribes to BOTH old ("0") and new ("01") topics for `ShardOverlapDuration` (2m) to ensure no messages are lost during transition.
4.  **Completion**: Old subscription is dropped.

---

## 6. Workflows

### 6.1 File Ingestion

1.  **Import**: Node imports file to IPFS, getting `PayloadCID`.
2.  **Manifest**: Node creates and signs `ResearchObject` pointing to `PayloadCID`.
3.  **Pin**: Node pins `ManifestCID` recursively (pinning both manifest and payload).
4.  **Check Responsibility**:
    *   **If Responsible**:
        *   Announce `IngestMessage` to **Shard Topic**.
        *   Announce to DHT (`Provide`).
    *   **If Not Responsible (Custodial)**:
        *   Do NOT announce to DHT.
        *   Broadcast `DelegateMessage` to **Control Topic**.

### 6.2 Replication & Repair

Nodes run a periodic replication pipeline:

1.  **Scheduler**: Periodically scans all "Known Files".
2.  **Prober**:
    *   Checks local storage.
    *   Queries DHT for providers (`FindProvidersAsync`).
    *   Verifies local content integrity (Liar Detection: checks size/signature).
3.  **Reconciler**:
    *   **Under-Replication** (`Count < MinReplication`):
        *   Broadcast `ReplicationRequest` to Shard Topic.
        *   Responsible nodes receiving this will fetch and pin the file.
    *   **Over-Replication** (`Count > MaxReplication`):
        *   If locally pinned and we are excess, unpin.
        *   Broadcast `UnreplicateRequest` if needed.

### 6.3 Custodial Handoff

1.  **Custodial Node** sends `DelegateMessage` (target: "10").
2.  **Responsible Node** (in shard "10") receives message.
3.  Responsible Node fetches and pins the file (becoming a replica).
4.  Responsible Node announces to DHT.
5.  **Custodial Node** detects replication count increased.
6.  **Custodial Node** unpins the file (freeing space).

---

## 7. Security Mechanisms

### 7.1 Integrity & Authenticity
*   **Content Addressing**: CIDs guarantee content integrity.
*   **Signatures**: All `ResearchObjects` and protocol messages are signed by the sender's private key.
*   **Nonces**: Protocol messages include nonces to prevent replay attacks.

### 7.2 Liar Detection
*   Nodes verify that the actual file size matches the `TotalSize` claimed in the `ResearchObject` manifest.
*   If a mismatch is detected, the file is unpinned and treated as invalid.

### 7.3 BadBits (Denylist)
*   Nodes maintain a `badbits.csv` denylist of CIDs (e.g., for DMCA compliance).
*   Files matching these CIDs are refused for ingestion and replication.

### 7.4 Rate Limiting
*   Per-peer rate limits on GossipSub messages to prevent flooding.
*   Disk usage limits reject new custodial files when storage is full (>90%).
