# D-LOCKSS Protocol Specification (V2)

## 1. Overview

D-LOCKSS (Distributed Lots of Copies Keep Stuff Safe) is a decentralized preservation network for long-term data durability through cooperative replication. It functions as a "Networked RAID", where independent nodes collaboratively maintain a target replication level for datasets.

The protocol is built on top of the **IPFS** (InterPlanetary File System) and **libp2p** stack, utilizing content-addressing, peer-to-peer messaging (GossipSub), distributed hash tables (DHT), and **IPFS Cluster CRDTs** for state consensus.

### Key Concepts

*   **Research Object (RO)**: The fundamental unit of preservation. A CBOR-encoded manifest wrapping the actual file content (Payload) with metadata and signatures.
*   **ManifestCID**: The Content ID of the Research Object. Used for tracking and referencing.
*   **PayloadCID**: The Content ID of the actual raw data/file.
*   **Responsibility**: Nodes are assigned to "shards". A node is responsible for a file if the file's PayloadCID hash maps to the node's shard prefix.
*   **Cluster Consensus**: Each shard operates as an embedded IPFS Cluster. Nodes in the same shard sync their pinning state via CRDTs, ensuring automatic replication.
*   **Custodial Mode**: A node temporarily holds a file it is not responsible for until a responsible node accepts it.

---

## 2. Network Layer

*   **Transport**: libp2p (TCP/QUIC/WebSockets).
*   **Discovery**: Kademlia DHT (LAN/WAN) + mDNS (Local).
*   **PubSub**: GossipSub v1.1.
*   **Content Transfer**: IPFS Bitswap.

### 2.1 Topic Naming

The network is partitioned into PubSub topics for scalability:

*   **Shard Topics (membership + protocol)**: `<version>-creative-commons-shard-<prefix>`
    *   Version prefix: `DefaultPubsubVersion` (currently `dlockss-v0.0.3`). Bumped on breaking changes to isolate versions.
    *   Examples: `...-shard-`, `...-shard-0`, `...-shard-1`, `...-shard-01`.
    *   Used for: JOIN/HEARTBEAT/LEAVE, PINNED, IngestMessage, ReplicationRequest (CBOR).
    *   Nodes subscribe to the shard topic(s) they belong to (current shard; optionally target shard as tourist).
*   **CRDT topics (cluster consensus, internal)**: `dlockss-shard-<id>`
    *   Used by the embedded IPFS Cluster CRDT per shard. Not subscribed to directly for application messages.

---

## 3. Data Structures

All data structures are serialized using **DAG-CBOR** (Concise Binary Object Representation).

### 3.1 Research Object (The Manifest)

The immutable record representing a preserved digital artifact. Defined in `pkg/schema/types.go`.

```go
type ResearchObject struct {
    MetadataRef string  `cbor:"meta_ref"`    // DOI/URL or file path reference
    IngestedBy  peer.ID `cbor:"ingester_id"` // PeerID of ingesting node
    Signature   []byte  `cbor:"sig"`         // Signature of the object
    Timestamp   int64   `cbor:"ts"`          // Unix timestamp of creation
    Payload     cid.Cid `cbor:"payload"`     // CID of the raw content (UnixFS DAG root)
    TotalSize   uint64  `cbor:"size"`        // Size of Payload in bytes
}
```

---

## 4. Message Protocol

Messages are propagated over GossipSub. All messages include a signature (`sig`) and nonce (`nonce`) for authentication and replay protection.

### 4.1 Message Types

| Type | ID | Purpose | Topic |
|------|----|---------|-------|
| `Ingest` | 1 | Announce new file entry | Shard |
| `ReplicationRequest` | 2 | Request more replicas (under-replicated) | Shard |
| `UnreplicateRequest` | 4 | Request dropping excess replicas | Shard |

Custodial handoff is done by joining the target shard (pubsub + cluster) and publishing **IngestMessage** to the target shard topic; there is no separate Delegate message in the codebase.

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

---

## 5. Sharding Protocol

D-LOCKSS uses dynamic sharding to partition responsibility.

### 5.1 Responsibility Assignment

*   **Node Identity**: Each node has a `currentShard` (binary string, e.g., "01"). This is a prefix of its PeerID (SHA-256 hash).
*   **File Assignment**: A file's `PayloadCID` is hashed (SHA-256).
*   **Match Rule**: A node is responsible for a file if the node's `currentShard` is a prefix of the file hash.

### 5.2 Dynamic Splitting

When a shard becomes too large (too many peers):
1.  **Detection**: `PeerCount >= MaxPeersPerShard` (default 12; configurable) for 2 consecutive checks, estimated peers per child >= MinPeersPerShard, and (child has ≥1 peer OR parent ≥14 to create). Uses ACTIVE peers only (HEARTBEAT/JOIN), not mesh count.
2.  **Split**: Node announces SPLIT on parent topic, sets `currentShard` to binary prefix of its PeerID at new depth (e.g., "0" -> "00" or "01" by hash).
3.  **Overlap**: Node joins new shard (pubsub + cluster), runs MigratePins(oldShard, newShard) after a short flush delay, then keeps old shard for `ShardOverlapDuration` (default 2m).
4.  **Completion**: After overlap, node leaves old shard (LeaveShard pubsub + ClusterManager.LeaveShard). RunReshardPass re-evaluates responsibility; files no longer in our shard are unpinned.

### 5.3 Discovery and Late Joiners

Nodes in any shard (not just root) periodically run discovery to join existing deeper child shards:
- **Intervals**: 10s at root, 2m default (`ShardDiscoveryInterval`), or 45s when SPLIT was received for target/sibling.
- **Requirement**: SPLIT must be announced (avoids phantom joins). Probe timeout 12s.
- **Split rebroadcast**: Nodes in child shards (e.g. 00, 01) periodically rebroadcast SPLIT to ancestor topics (0, root) so late joiners in the parent receive the announcement. Interval: 60s (`ShardSplitRebroadcastInterval`).

---

## 6. Workflows

### 6.1 File Ingestion

1.  **Import**: Node imports file to IPFS, getting `PayloadCID`.
2.  **Manifest**: Node creates and signs `ResearchObject` (meta_ref, ingester_id, payload, size) and stores it in IPFS; gets `ManifestCID`.
3.  **Pin**: Node pins `ManifestCID` recursively (BadBits check first). Tracks via StorageManager.PinFile and announces PINNED on current shard topic.
4.  **Check Responsibility** (AmIResponsibleFor(PayloadCID)):
    *   **If Responsible**:
        *   Pin to current shard's cluster (ClusterManager.Pin).
        *   Publish `IngestMessage` to current **Shard Topic**.
        *   Announce to DHT (`Provide`).
    *   **If Not Responsible (Custodial)**:
        *   Do NOT pin to current cluster. Join **target shard** (TargetShardForPayload(PayloadCID, depth)): pubsub + ClusterManager.JoinShard.
        *   Pin to target shard's cluster (PinToShard).
        *   Publish `IngestMessage` to **target Shard Topic**.
        *   Do NOT announce to DHT (file lives only in target cluster).

### 6.2 Replication & Repair

Replication is handled by the **Cluster Manager** and **LocalPinTracker** per shard:

1.  **Pinning**: When a responsible node ingests or accepts a file, it calls `ClusterManager.Pin(ctx, shardID, cid, ...)` on the shard's embedded cluster. Allocations are chosen deterministically from Peers() (shard mesh via ShardPeerProvider).
2.  **State Sync**: The CRDT (Merkle-DAG based) propagates pin/unpin to all peers in the shard via PubSub (`dlockss-shard-<id>`).
3.  **Local Pin Tracker**:
    *   Each node runs a `LocalPinTracker` per shard that polls CRDT State() (and on TriggerSync).
    *   For each pin in state, if this node is in **Allocations** (or Allocations is empty), it pins the CID locally via IPFS and calls onPinSynced (StorageManager.PinFile, AnnouncePinned).
    *   Pins no longer in state or no longer allocated are unpinned locally and onPinRemoved is called.
4.  **Repair**: Under-replicated files trigger ReplicationRequest on the shard topic; peers that have the file JoinShard(targetShard), Pin, TriggerSync. CRDT sync and LocalPinTracker then replicate to allocated peers.

### 6.3 Custodial Handoff (Tourist)

1.  **Custodial Node** (local ingest; not responsible): computes `targetShard = TargetShardForPayload(PayloadCID, depth)`.
2.  **Custodial Node** joins target shard (ShardManager.JoinShard + ClusterManager.JoinShard).
3.  **Custodial Node** pins the file to the target shard's cluster (PinToShard) and publishes **IngestMessage** to the target shard topic.
4.  **Responsible nodes** in the target shard receive IngestMessage; if responsible they Pin to cluster and AnnouncePinned. CRDT syncs; LocalPinTracker on each allocated peer pins locally.
5.  Custodial node remains in the target shard as a "tourist." There is no automatic unpin when replication count is sufficient; the node may leave the target shard later (e.g. reshard or shutdown).

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
*   Per-peer rate limits (RateLimiter.Check) on protocol messages (Ingest, ReplicationRequest) to prevent flooding. Applied in ShardManager before processing CBOR messages.
