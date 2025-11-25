Please generate a prompt that will enable any modern LLM to reproduce the exact program code. Do not include code snippets in the prompt.

**Prompt:**

Act as an expert Systems Engineer. Write a complete, single-file Go program (package `main`) that implements a production-ready, self-healing, sharded P2P file preservation system called "D-LOCKSS".

The code must adhere to the following strict specifications, logical structures, and variable naming conventions.

### 1. Dependencies & Imports
* Use standard libraries for context, crypto/sha256, hex encoding, file I/O, logging, OS signals, path manipulation, sync primitives, and time.
* Use `github.com/fsnotify/fsnotify` for file watching.
* Use the full suite of `libp2p` libraries: core host, network, peer, routing, `go-cid`, `go-multihash`, `go-libp2p-pubsub`, and `go-libp2p-kad-dht` (aliased as `dht`).

### 2. Configuration Constants
Define the following specific constants:
* Control Topic: "dlockss-control"
* Discovery Tag: "dlockss-v2-prod"
* Watch Folder: "./data"
* **Replication Rules:** Minimum = 5, Maximum = 10.
* **Intervals:** Check interval = 15 minutes.
* **Load Balancing:** Max Shard Load = 2000.

### 3. Global State
Create two specific global thread-safe structures (using `sync.RWMutex` and `map[string]bool`):
1.  `pinnedFiles`: Tracks files physically stored/pinned on the disk.
2.  `knownFiles`: Tracks files we are monitoring/tracking (even if not currently pinned).
3.  Include globals for the DHT pointer and a `ShardManager` pointer.

### 4. The ShardManager Struct
Create a struct named `ShardManager` to handle dynamic topic switching. It must hold:
* Context, Host, and PubSub pointers.
* Mutex, current shard string (binary prefix), shard topic/subscription, and control topic/subscription.
* `msgCounter` (int) to track load.

**ShardManager Methods:**
* **New:** Factory function to initialize and join channels.
* **joinChannels:** Subscribe to the static Control topic. Subscribe to the dynamic Shard topic (`dlockss-shard-{prefix}`). Reset `msgCounter` here.
* **readControl:** Loop listening for "DELEGATE:{hash}:{prefix}" messages. If the target prefix matches the node's current shard prefix, add the hash to `knownFiles` and launch a replication check.
* **readShard:** Loop listening for messages.
    * *Load Balancing:* Increment `msgCounter`. If it exceeds `MaxShardLoad`, **immediately** reset the counter to zero and trigger `splitShard`.
    * *Message Handling:* Listen for "NEED:{hash}" and "NEW:{hash}". For both, add to `knownFiles` and launch a replication check.
* **splitShard:** Calculate the next binary bit based on the node's Peer ID hash at the next depth. Update the current shard prefix and call `joinChannels`.
* **AmIResponsibleFor:** Boolean check comparing a file hash's binary prefix against the node's current shard prefix.
* **Publish helpers:** Methods to publish strings to the current shard or control topic.

### 5. File Handling & Custodial Logic
Implement `processNewFile(path)` with "Custodial" logic:
1.  Calculate SHA256 hash.
2.  **Immediate Pin:** Always pin the file locally and provide it to the DHT immediately.
3.  Add to `knownFiles`.
4.  **Routing:**
    * If responsible: Announce "NEW:{hash}" to the Shard.
    * If *not* responsible: Announce "DELEGATE:{hash}:{target_prefix}" to Control, but keep the file pinned (Custodial Mode).

### 6. The Replication Checker (Core Logic)
Implement `checkReplication(ctx, hash)` with the following logic flow:
1.  Determine if the node is `responsible` and if the file is `pinned`.
2.  **Tracking Cleanup:** If the node is neither responsible nor pinning the file, remove it from `knownFiles` and return.
3.  **DHT Query:** Find providers with a 2-minute timeout context.
4.  **Case 1 (Low Redundancy):** If count < MinReplication:
    * If responsible (or custodial), re-pin the file locally and provide to DHT.
    * Broadcast "NEED:{hash}" to the shard.
5.  **Case 2 (Garbage Collection):**
    * If responsible AND count > MaxReplication AND pinned: Unpin the file (log as "Monitoring only").
    * If *not* responsible (Custodial) AND count >= MinReplication AND pinned: Unpin the file (log as "Handoff Complete").

### 7. Helper Functions
* **Pinning:** Thread-safe `pinFile` and `unpinFile` (updates `pinnedFiles`).
* **Tracking:** Thread-safe `addKnownFile` and `removeKnownFile` (updates `knownFiles`).
* **Binary Math:** Helper functions to convert a hex string or a raw string into a binary string (0s and 1s) of a specific depth.
* **Hashing:** File to SHA256 hex string; Hex string to CID.

### 8. Main Function Execution Flow
1.  Setup Context with Cancel.
2.  Initialize libp2p Host (Listen on all TCP).
3.  Initialize and start mDNS.
4.  Bootstrap DHT (Server mode).
5.  Initialize GossipSub.
6.  **Shard Init:** Calculate initial shard (depth 1) from Peer ID and create `ShardManager`.
7.  Start `shardMgr.Run()` and `inputLoop` (reading stdin to publish to shard).
8.  **FileSystem:** Ensure directory exists.
9.  **Startup Scan:** Call `scanExistingFiles` (iterating the folder and calling `processNewFile`) **before** starting the watcher.
10. Start `watchFolder` (fsnotify loop) and `runReplicationChecker` (ticker loop iterating `knownFiles`) in goroutines.
11. Wait for termination signal (SIGTERM/Interrupt).

**Important:** Ensure the code handles "Split Storms" by resetting the message counter immediately upon detection, and handles "Zombie Records" by verifying counts before unpinning.