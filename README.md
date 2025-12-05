# Project Name: D-LOCKSS (v2)

## 1\. Summary & Vision

**D-LOCKSS** (Distributed Lots of Copies Keep Stuff Safe) is a decentralized storage network designed to ensure the long-term preservation and authenticity of research data and documents.

  * **Core Philosophy:** "Networked RAID." Just as RAID protects data across multiple hard drives, D-LOCKSS protects data across a distributed network of peers.
  * **Authenticity:** Relying on Content Addressing (CIDs) to guarantee that the data retrieved is bit-for-bit identical to the data published.
  * **Scope:** The system focuses purely on fundamental storage technology—replication, redundancy, and availability. Rights management and ownership are explicitly out of scope for this phase.

-----

## 2\. Technical Architecture

The system acts as a self-healing, sharded storage cluster using the IPFS/Libp2p stack.

### A. The "Networked RAID" Logic

Instead of a central controller, the network uses a Distributed Hash Table (DHT) and dynamic sharding to distribute "ownership" of files.

[Image of distributed hash table architecture]

| RAID Concept | D-LOCKSS Equivalent | Implementation Details |
| :--- | :--- | :--- |
| **Striping** | **Sharding** | The `ShardManager` assigns binary prefixes (e.g., `01*`, `11*`) to nodes. A node only permanently stores files whose SHA-256 hash matches its assigned prefix. |
| **Redundancy** | **Replication** | The system enforces a `MinReplication` of 5 and `MaxReplication` of 10. |
| **Scrubbing** | **Replication Checker** | A background process (`runReplicationChecker`) scans known CIDs every 15 minutes. If redundancy \< 5, it triggers a `NEED` broadcast; if \> 10, it drops the file to save space. |
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