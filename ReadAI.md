**Prompt:**

Please write a complete, single-file Go program (package `main`) that implements a P2P distributed file replication system (a "LOCKSS" system). The code must match the following specific implementation details, variable names, and logic flow exactly.

### Imports
Include standard libraries: `bufio`, `context`, `crypto/sha256`, `encoding/hex`, `fmt`, `io`, `log`, `os`, `os/signal`, `path/filepath`, `sync`, `syscall`, `time`.
Include third-party libraries:
* `github.com/fsnotify/fsnotify`
* `github.com/ipfs/go-cid`
* `github.com/libp2p/go-libp2p`
* `github.com/libp2p/go-libp2p-kad-dht` (aliased as `dht`)
* `github.com/libp2p/go-libp2p-pubsub`
* `github.com/libp2p/go-libp2p/core/host`
* `github.com/libp2p/go-libp2p/core/peer`
* `github.com/libp2p/go-libp2p/core/routing`
* `github.com/libp2p/go-libp2p/p2p/discovery/mdns`
* `github.com/multiformats/go-multihash`

### Configuration Constants
Define these exact constants:
* `TopicName`: "dlockss-topic"
* `discoveryServiceTag`: "dlockss-example"
* `fileWatchFolder`: "./my-pdfs"
* `minReplication`: 5
* `maxReplication`: 10
* `checkInterval`: 1 * time.Minute

### Global Variables
1.  `pinnedFiles`: An anonymous struct containing `sync.RWMutex` and a map `hashes` (`map[string]bool`). Initialize the map.
2.  `knownFiles`: Same structure as `pinnedFiles`. Initialize the map.
3.  `globalDHT`: A pointer to `dht.IpfsDHT`.

### Main Function Logic
1.  Create a context with cancel. Defer cancel.
2.  **Host Creation:** Initialize `libp2p.New` with:
    * Listen addresses: `/ip4/0.0.0.0/tcp/0` and `/ip6/::/tcp/0`.
    * Routing: A function that initializes `globalDHT` using `dht.New` in `dht.ModeServer` and returns it.
    * Panic on error.
3.  **Print Info:** Print "--- Your Node's Addresses ---", loop through `host.Addrs()` printing in format `%s/p2p/%s`, then print a separator and "Waiting for peers...".
4.  **mDNS:** Create a `discoveryNotifee`, start `mdns.NewMdnsService`, and call `Start()`. Panic on error.
5.  **Bootstrap:** Print "Bootstrapping DHT..." and call `globalDHT.Bootstrap(ctx)`. Panic on error.
6.  **GossipSub:** Create `pubsub.NewGossipSub`. Panic on error.
7.  **Topic:** Join `TopicName`. Subscribe to it. Panic on errors.
8.  **Goroutines:** Launch `readMessages` and `publishMessages`.
9.  **File Setup:**
    * Log "Starting file watcher on:" with the folder path.
    * Ensure the directory exists (`os.Mkdir`, 0755).
    * **Important:** Log "Scanning for existing files in..." then call `scanExistingFiles(ctx, topic)`.
    * Launch `watchFolder(ctx, topic)` in a goroutine.
    * Launch `startReplicationChecker(ctx, topic)` in a goroutine.
10. **Shutdown:** Wait for `os.Interrupt` or `syscall.SIGTERM`. Print "\nShutting down...", close the host, and print "Done."

### Discovery Notifee
Define a struct `discoveryNotifee` with `h` (host) and `ctx`.
Implement `HandlePeerFound`:
* Print "\rDiscovered new peer: [ID]".
* Attempt `n.h.Connect`. On error print "\rError connecting...", on success print "\rConnected to...".

### PubSub Logic
**`readMessages` function:**
* Loop reading `sub.Next(ctx)`. Handle context cancellation errors.
* Ignore messages from self.
* Check message data string:
    * If it starts with "NEW:" (slice index 4), extract hash, print "\r[Network]: New file announced: [hash]", call `addKnownFile`, and launch `checkReplication`.
    * If it starts with "NEED:" (slice index 5), extract hash, call `addKnownFile`. If `!isPinned(hash)`, print "\r[Network]: Received NEED for unpinned file...", and launch `checkReplication`.
    * Else, print standard chat format: "\r[ShortID]: [data]".

**`publishMessages` function:**
* Use `bufio.Scanner` on `os.Stdin`. Print prompt "> ".
* If line is empty, reprint prompt.
* If line starts with "NEW:" or "NEED:", print a warning that these cannot be sent manually.
* Otherwise, `topic.Publish` the line. handle scanner errors.

### Hashing Helpers
* `calculateFileHash(filePath)`: Open file, use `sha256`, copy `io.Copy`, return hex string.
* `hashToCid(hash)`: Decode hex string. Use `multihash.Sum` with code `0x12` (SHA2-256) and length 32. Return `cid.NewCidV1(cid.Raw, mh)`.
* **Important:** Include a comment block labeled `--- FUNCTION REMOVED ---` mentioning that `cidToHash` was the unused function.

### File Watching & Replication Logic
Implement the following helper functions:

1.  `getKnownFiles`: RLock `knownFiles`, return slice of keys.
2.  `addKnownFile`: Lock `knownFiles`, set hash to true.
3.  `startReplicationChecker`:
    * Log "Replication checker started...".
    * Ticker loop (`checkInterval`). On tick:
    * Log "--- Running periodic replication check ---".
    * Get `filesToCheck`. If empty, log "No known files...".
    * Log count of files, loop through them, and launch `checkReplication` for each.
4.  `scanExistingFiles`:
    * `os.ReadDir`. Log errors.
    * Loop entries. Skip directories.
    * Log "Processing existing file: [path]".
    * Calculate hash. Call `pinFile`, `addKnownFile`, `go provideFile`, `announceFile`.
    * Log "Finished processing [count] existing files."
5.  `watchFolder`:
    * `fsnotify.NewWatcher`.
    * Loop events. If `Op` is `Create`:
    * Log "New file detected". Sleep 100ms. Calculate hash. Call `pinFile`, `addKnownFile`, `go provideFile`, `announceFile`.
    * Handle watcher errors. Add watch folder path.
6.  `pinFile` / `unpinFile`: Lock `pinnedFiles` and add/remove hash. Log "[PINNING] [hash]" or "[UNPINNING] [hash]" only if the state changes.
7.  `isPinned`: RLock return bool.
8.  `provideFile`: Convert hash to CID. Log "Telling DHT we are a provider...". Call `globalDHT.Provide`.
9.  `announceFile`: Publish string "NEW:[hash]".

**`checkReplication` (Core Logic):**
* Convert hash to CID.
* Create context with timeout (30s).
* Log "Checking replication for: [hash]".
* Call `globalDHT.FindProvidersAsync`.
* Count the providers from the channel. **Include a comment exactly saying:** `// FIXED: Simplified the range expression` above the loop.
* Check for `DeadlineExceeded` error.
* Log "Found [count] providers...".
* Logic:
    * If `count < minReplication`:
        * Log "Replication low! Announcing NEED...". Publish "NEED:[hash]".
        * If `!amIPinning`:
            * Log "Must pin [hash]".
            * **Include a comment:** `// --- CRITICAL TODO ---` followed by `log.Println("TODO: Fetch file from peer")`.
            * Call `pinFile` and `go provideFile`.
    * Else if `count > maxReplication` AND `amIPinning`:
        * Log "Replication high! Unpinning". Call `unpinFile`.

***

### Instructions for the LLM
Use the prompt above. It maps 1:1 to your source code structure, variable naming conventions, and even includes the specific comments and "TODOs" found in your code snippet.