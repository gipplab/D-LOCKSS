# Project Name: D-LOCKSS (v2)

## 1\. Summary & Vision

**D-LOCKSS** (Distributed Lots of Copies Keep Stuff Safe) is a decentralized storage network designed to ensure the long-term preservation and authenticity of research data and documents.

  * **Core Philosophy:** "Networked RAID." Just as RAID protects data across multiple hard drives, D-LOCKSS protects data across a distributed network of peers.
  * **Authenticity:** Relying on Content Addressing (CIDs) to guarantee that the data retrieved is bit-for-bit identical to the data published.
  * **Scope:** The system focuses purely on fundamental storage technology—replication, redundancy, and availability. Rights management and ownership are explicitly out of scope for this phase.

GOAL: Combining the Speed of IPFS Cluster with the Satety of LOCKSS (Fast enough for millions of files but smart enough to ensure enought copies exist without human intervention.)

GOAL: Combining the Speed of IPFS Cluster with the Satety of LOCKSS (Fast enough for millions of files but smart enough to ensure enought copies exist without human intervention.)

-----

## 2\. Technical Architecture

The system acts as a self-healing, sharded storage cluster using the IPFS/Libp2p stack.

### A. The "Networked RAID" Logic

Instead of a central controller, the network uses a Distributed Hash Table (DHT) and dynamic sharding to distribute "ownership" of files.

| RAID Concept | D-LOCKSS Equivalent | Implementation Details |
| :--- | :--- | :--- |
| **Striping** | **Sharding** | The `ShardManager` assigns binary prefixes (e.g., `01*`, `11*`) to nodes. A node only permanently stores files whose SHA-256 hash matches its assigned prefix. |
| **Redundancy** | **Replication** | The system enforces a `MinReplication` of 5 and `MaxReplication` of 10. |
| **Scrubbing** | **Replication Checker** | A background process (`runReplicationChecker`) scans known CIDs every 1 minute. If redundancy < 5, it triggers a `NEED` broadcast; if > 10, it drops the file to save space. |
| **Write Cache** | **Custodial Mode** | If a node receives a file it *should not* own, it holds it temporarily ("Custodial Mode") and broadcasts a `DELEGATE` message to find the correct owner, ensuring no data loss during transit. |

### B. Core Components

1.  **Shard Manager:**
      * Monitors network load (`MaxShardLoad`).
      * Dynamically splits responsibilities (e.g., if handling `0*` becomes too heavy, it splits into `00*` and `01*`).
      * Manages PubSub topics: `dlockss-control` (delegation) and `dlockss-shard-{prefix}` (data announcements).
2.  **File Watcher:**
      * Monitors the `./data` directory using `fsnotify`.
      * Automatically hashes, pins, and announces new files dropped into the folder.
3.  **Discovery:**
      * Uses MDNS for local peer discovery (`dlockss-v2-prod`).
      * Uses Kademlia DHT for global routing and provider record storage.

-----

## 3\. How to Run

### Prerequisites

  * **Go 1.20** or later installed.
  * A working internet connection.
  * An environment allowing P2P connections (ports 4001/tcp/udp and multicast enabled for MDNS).

### Setup

1.  **Initialize Project:**
    Create a directory and initialize the module.

    ```bash
    mkdir dlockss-v2
    cd dlockss-v2
    # Paste main.go here
    go mod init dlockss
    go mod tidy
    ```

2.  **Create Watch Directory:**
    The application watches a directory for incoming files.

    ```bash
    mkdir data
    ```

### Running the Node

Start the application from your terminal:

```bash
go run .
```

You will see logs indicating the node is online, the shard it is managing, and the topics it has joined:

```text
--- Node ID: 12D3KooW... ---
--- Addresses: [/ip4/192.168.1.5/tcp/34567 ...] ---
[System] Joined Control Channel: dlockss-control
[Sharding] Active Data Shard: 0 (Topic: dlockss-shard-0)
Scanning for existing files...
[System] Found 0 existing files.
```

### Usage Workflow

1.  **Add a File:**
    Copy any PDF document into the `./data` directory.
2.  **Ingestion:**
    Within seconds, the application will detect the file, hash it, and determine responsibility.
      * **If Responsible:** It pins the file and announces "NEW:{hash}" to its shard.
      * **If Not Responsible:** It enters **Custodial Mode**, announcing "DELEGATE:{hash}" to the control topic.
3.  **Run a Second Peer:**
    Run `go run .` in a separate terminal (ensure a separate directory if testing on the same machine to avoid lock conflicts, or use containerization). The new peer will auto-discover the first peer, negotiate shards, and begin replication if the redundancy count is below 5.

-----

## 4\. Roadmap & Engineering Challenges (TODO)

### Feature Goals

  * **Targeted Replication:** Request replication by CID to spread content equally across the topology.
  * **User Interface:** Provide a browser-based UI using **Helia** (JS IPFS) to browse and retrieve PDFs without a CLI.

### Architectural Challenge: Scalable Sharding

We are currently evaluating the following strategies for scaling the Sharding logic beyond simple prefixes:

**Strategy A: Channel-based Sharding (Deep Hierarchy)**

  * *Concept:* CID prefix defines the PubSub channel to join (up to 64 channels).
  * *Logic:* Peers join channels matching their prefix. If a channel is empty, they move up to the parent channel.
  * *Replication Check:* Start at the deepest channel (e.g., prefix `1011`) and bubble up to `main` if needed.
  * *SHOWSTOPPER:* If a node hosts 1 million CIDs, it cannot join 1 million specific channels. The overhead is untenable.

**Strategy B: Single Global Channel**

  * *Concept:* Keep all peers in one channel to avoid subscription overhead.
  * *Logic:* Control message flow via PeerID bandwidth allocation.
  * *SHOWSTOPPER:* Bandwidth variance between peers makes synchronization unreliable.

**Strategy C: Probabilistic Gossip (Selected Approach)**

  * *Concept:* Similar to Block propagation in Blockchains.
  * *Logic:* Fine-tune the GossipSub message frequency. We accept that some `NEW` or `NEED` messages may be missed (probabilistic delivery).
  * *Result:* Updates are eventually consistent; the system heals over longer timeframes rather than instantaneously.

### Next Step

Implement **Strategy C** by tuning `pubsub.GossipSubParams` to handle higher message throughput while accepting eventual consistency for replication events.




### Comparative Feature Matrix

| FEATURE | IPFS CLUSTER | SAFE NETWORK | PEERBIT | LOCKSS |
| :--- | :--- | :--- | :--- | :--- |
| **Network Stack**<br>(SR-1, SR-2) | [x] libp2p + DHT<br>(Exact Match) | [ ] Custom (QUIC)<br>(XOR Route) | [x] libp2p<br>(Exact Match) | [ ] Custom/HTTP<br>(LCAP/REST) |
| **Content Addressing**<br>(SR-4, FM-2) | [x] IPFS CIDs<br>(Multihash) | [x] XOR Hash<br>(Cnt Addr) | [x] IPFS CIDs<br>(Multihash) | [~] URL-Based<br>(Hash verify) |
| **Messaging Protocol**<br>(SR-3, MP-1) | [x] GossipSub<br>(PubSub) | [~] Hop Routing<br>(Custom Msg) | [x] GossipSub<br>(PubSub) | [ ] Direct/Poll<br>(Unicast) |
| **Prefix Sharding**<br>(SH-1, SH-3) | [ ] Consensus<br>(Raft/CRDT) | [x] XOR Distance<br>(Prefix Mat) | [~] DB Sharding<br>(Filter) | [ ] Static<br>(Manual List) |
| **Auto-Replication**<br>(RP-1, RP-2) | [x] Min/Max<br>(Configured) | [x] Managed<br>(Net force) | [x] Configurable<br>(Rep Factor) | [x] Polling<br>(Voting) |
| **Custodial Handoff**<br>(RP-5, CM-1) | [ ] Manual<br>(User pins) | [x] Automatic<br>(Churn hdl) | [~] Partial<br>(Sync Logic) | [ ] None<br>(Static) |
| **File System Watcher**<br>(FM-1) | [ ] None<br>(API Only) | [ ] None<br>(Virt Drive) | [ ] None<br>(DB Only) | [~] Crawler<br>(HTTP only) |

Legend:
[x] = Feature Match / Native Support
[~] = Partial Match / Different Approach
[ ] = No Support / Fundamental Mismatch



### Library Use-Case Comparison

| FEATURE | LOCKSS (Classic) | IPFS CLUSTER | D-LOCKSS (Your Spec) | SAFE NETWORK |
| :--- | :--- | :--- | :--- | :--- |
| **Best For** | Regulatory Compliance<br>(Dark archives, audits) | Raw Performance<br>(Big Data transfer, fast sync) | Modern Preservation<br>(Hybrid of Safety & Speed) | Anonymity<br>(Censorship resistance) |
| **Architecture** | Poll-based<br>(HTTP Crawling, static peers) | Push-based<br>(Consensus/Raft, manual pinning) | Reactive P2P<br>(GossipSub events, auto-sharding) | Autonomous<br>(XOR Math, self-encrypting) |
| **Integrity Check** | Active Voting<br>(Constant polls, consensus repairs) | Passive<br>(Manual trigger: 'ipfs repo verify') | Hybrid<br>(Periodic checks + DHT repair) | Self-Encryption<br>(Network relocates & verifies chunks) |
| **Scale (Millions)** | Poor<br>(Slow crawling, bandwidth heavy) | Excellent<br>(Bitswap is fast, deduplicates data) | Good<br>(GossipSub handles high throughput) | Excellent<br>(Global scale, but retrieval is slow) |
| **Setup Cost** | High<br>(Complex config, static IPs, XML) | Medium<br>(DevOps needed for shared keys) | Low<br>(Single binary, auto-discovery) | Zero<br>(Just run client, no control) |