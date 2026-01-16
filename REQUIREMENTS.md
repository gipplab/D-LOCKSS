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
- Must use SHA-256 for file hashing
- Must convert file hashes to IPFS CIDs (Content Identifiers)
- Must use CIDs for content addressing and verification
- Must ensure bit-for-bit authenticity of stored content

---

## File Management Requirements

### FM-1: File Watching
- Must watch a designated folder (`./data` by default) for new files
- Must automatically detect new files when they are created
- Must scan existing files in the watch folder on startup
- Must ignore subdirectories (only process files)
- Must wait 100ms after file creation before processing (to handle file writes)

### FM-2: File Hashing
- Must calculate SHA-256 hash of each file
- Must represent hash as 64-character hexadecimal string
- Must handle file reading errors gracefully

### FM-3: File Pinning
- Must maintain a thread-safe list of pinned files (by hash)
- Must support pinning a file (adding to storage)
- Must support unpinning a file (removing from storage)
- Must support checking if a file is pinned
- Must track files in two states: pinned (physically stored) and known (monitored)

### FM-4: File Processing
- Must process each new file found in the watch folder
- Must pin every file locally when first discovered
- Must provide file to DHT network immediately after pinning
- Must track all processed files in knownFiles list

---

## Sharding Requirements

### SH-1: Shard Assignment
- Must assign each node a binary prefix shard based on node's Peer ID
- Must calculate initial shard using SHA-256 hash of Peer ID at depth 1
- Must determine file responsibility by comparing file hash prefix to node's shard prefix
- Must support dynamic shard prefix matching (prefix-based routing)

### SH-2: Shard Management
- Must maintain current shard assignment in thread-safe manner
- Must join a control topic (`dlockss-control`) for delegation messages
- Must join a data shard topic (`dlockss-shard-{prefix}`) based on current shard
- Must support switching shards dynamically
- Must clean up old subscriptions when switching shards

### SH-3: Shard Splitting
- Must monitor message load on current shard
- Must count messages received on shard topic
- Must trigger shard split when message count exceeds threshold (2000 messages)
- Must calculate next shard by increasing binary prefix depth
- Must reset message counter after split
- Must prevent infinite split loops (check if new shard equals old shard)

### SH-4: Responsibility Determination
- Must determine if node is responsible for a file based on hash prefix matching
- Must compare file hash binary prefix to node's current shard prefix
- Must support prefix matching at variable depths

---

## Replication Requirements

### RP-1: Replication Targets
- Must maintain minimum replication count of 5 copies
- Must maintain maximum replication count of 10 copies
- Must query DHT to count current number of providers for each file

### RP-2: Replication Checking
- Must run periodic replication checks every 15 minutes
- Must check all known files (both pinned and tracked)
- Must check replication status for each file asynchronously
- Must handle replication checks in background goroutines

### RP-3: Low Replication Handling
- If replication count < 5:
  - If node is responsible and file is not pinned: must re-pin the file
  - Must provide file to DHT network
  - Must broadcast "NEED:{hash}" message to shard topic
  - If node is in custodial mode: must keep holding the file

### RP-4: High Replication Handling
- If replication count > 10 and node is responsible and file is pinned:
  - Must unpin the file (garbage collection)
  - Must keep file in knownFiles for continued monitoring

### RP-5: Custodial Handoff
- If node is NOT responsible (custodial mode) and replication count >= 5:
  - Must unpin the file (handoff complete)
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
- Must broadcast "DELEGATE:{hash}:{target_shard}" message to control topic
- Must keep file pinned until proper replication is achieved

### CM-3: Custodial Delegation
- Must listen for DELEGATE messages on control topic
- Must accept delegation if target shard matches node's shard prefix
- Must track delegated files and check their replication

---

## Message Protocol Requirements

### MP-1: Control Topic Messages
- Must support "DELEGATE:{hash}:{shard_prefix}" message format
- Must parse delegation messages correctly
- Must validate message format (minimum 3 parts separated by colons)
- Must ignore malformed messages

### MP-2: Shard Topic Messages
- Must support "NEED:{hash}" message format (replication request)
- Must support "NEW:{hash}" message format (new content announcement)
- Must parse and extract hash from messages
- Must track files mentioned in messages

### MP-3: Message Publishing
- Must publish "NEW:{hash}" when responsible node receives new file
- Must publish "DELEGATE:{hash}:{prefix}" when non-responsible node receives file
- Must publish "NEED:{hash}" when replication is low
- Must support publishing arbitrary messages from stdin (for testing)

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
6. Start message handlers (control and shard readers)
7. Start stdin input loop
8. Create watch folder if it doesn't exist
9. Scan existing files
10. Start file watcher
11. Start replication checker
12. Wait for shutdown signal

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

### CF-1: Hardcoded Constants
- Control topic name: "dlockss-control"
- Discovery service tag: "dlockss-v2-prod"
- File watch folder: "./data"
- Minimum replication: 5
- Maximum replication: 10
- Replication check interval: 15 minutes
- Maximum shard load: 2000 messages
- File processing delay: 100ms

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

## Out of Scope (Explicitly Not Required)

- Rights management
- File ownership
- Access control
- User authentication
- File retrieval/retrieval API (storage only)
- Web UI (future roadmap item)
- Targeted replication (future roadmap item)

---

## Summary Statistics

- **Total Requirements:** 60+
- **Core System:** 4 requirements
- **File Management:** 4 requirements
- **Sharding:** 4 requirements
- **Replication:** 6 requirements
- **Custodial Mode:** 3 requirements
- **Message Protocol:** 3 requirements
- **Discovery:** 2 requirements
- **Operational:** 4 requirements
- **Configuration:** 2 requirements
- **Utility:** 2 requirements
- **Non-Functional:** 4 requirements
- **Testing:** 2 requirements


