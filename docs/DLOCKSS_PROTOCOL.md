# D-LOCKSS Specification (V3)

The canonical specification of D-LOCKSS: wire protocol, node behavior,
configuration, monitor, and the differences from the original implementation.

D-LOCKSS (Distributed Lots of Copies Keep Stuff Safe) is a decentralized
preservation network for long-term data durability through cooperative
replication — a "Networked RAID" where independent nodes collaboratively
maintain a target replication level. It is built on the **IPFS** and
**libp2p** stack: content addressing, GossipSub messaging, and the Kademlia
DHT. Since V3, shard pin sets converge purely through the protocol's own
signed gossip — there is no embedded consensus layer. The wire protocol is
unchanged from V2 (`dlockss-v0.0.3`); V2 and V3 nodes interoperate through
the shared message protocol.

Terminology: *manifest* = the CBOR ResearchObject block; *payload* = the
UnixFS DAG of the preserved content; *shard ID* = a string of `0`/`1` bits
(root = `""`).

---

## 1. Network layer

*   **Transport**: libp2p (TCP / QUIC-v1 on random ports, v4+v6), Noise.
*   **Discovery**: Kademlia DHT rendezvous (tag `dlockss-prod`) + mDNS, plus
    a FindPeers connect loop (30 s rounds). Content provides go through the
    node's Kubo daemon, not this DHT.
*   **PubSub**: GossipSub v1.1, default parameters.
*   **Content transfer**: Kubo (Bitswap), driven over its HTTP RPC API.

### 1.1 Topic naming

Shard topics: `<prefix>-<topic>-shard-<shardID>`, e.g.
`dlockss-v0.0.3-creative-commons-shard-01` (the root's suffix is empty:
`...-shard-`). The version prefix is bumped on breaking changes so
incompatible versions never share topics. V2's internal CRDT topics
(`...-crdt-<shard>`) no longer exist.

### 1.2 Text shard messages (unsigned, not rate-limited)

| Message | Format | Purpose |
|---------|--------|---------|
| `HEARTBEAT` | `HEARTBEAT:<PeerID>:<pinnedCount>:<role>:<nodeName>` | Liveness, pinned count, role, optional name |
| `JOIN` | `JOIN:<PeerID>:<role>:<nodeName>` | Announce shard membership |
| `LEAVE` | `LEAVE:<PeerID>` | Announce departure |
| `PINNED` | `PINNED:<ManifestCID>` | Announce a locally pinned manifest |
| `PROBE` | `PROBE:<PeerID>` | Observer presence; full members answer with one HEARTBEAT |
| `SPLIT` | `SPLIT:<child0>:<child1>` | Announce a shard split |

*   Roles: `ACTIVE`, `PASSIVE` (disk full; never counted), `REPLICATOR`
    (stores but may not ingest; counted), `PROBE` (observer; never counted).
    Unknown roles parse as ACTIVE; trailing fields are optional and backward
    compatible.
*   Membership is keyed by the authenticated pubsub sender, never by the ID
    embedded in the payload. Messages from self are ignored; every foreign
    message refreshes the sender's last-seen in that shard. A payload
    starting with `{` is legacy JSON and is consumed and ignored.
*   PROBE replies: one HEARTBEAT, only on the node's current shard and only
    when a full member, globally limited to one per 5 s.

## 2. Data structures

All structures are canonical **DAG-CBOR** maps (keys sorted length-first,
then bytewise). CIDs and peer IDs are encoded as strings.

### 2.1 Research Object (manifest; stored via `block/put`, sha2-256)

```
{
  "meta_ref":    string   // DOI / URL / file or pin-name reference
  "ingester_id": string   // PeerID of the ingesting node
  "sig":         bytes    // signature over the encoding without "sig"
  "payload":     string   // CID of the raw UnixFS DAG
  "size":        int      // payload size in bytes
}
```

The manifest carries **no timestamp**: identical content from the same
ingester always yields the same ManifestCID, so re-ingests dedupe.

## 3. Message protocol

Signed CBOR messages on shard topics. Envelope fields: `type`,
`manifest_cid` (string), message-specific fields, `sender_id` (string), `ts`
(unix seconds), `nonce` (bytes), `sig` (over the encoding without `sig`).

| Type | ID | Purpose |
|------|----|---------|
| `Ingest` | 1 | Announce a new Research Object (`shard_id` names the target shard) |
| `ReplicationRequest` | 2 | Ask shard peers to hold a manifest |

**Verification** (strict, any failure drops the message): the sender must be
present and equal the authenticated pubsub originator (and, in allowlist
trust mode with a non-empty set, be listed); `ts ≠ 0`, within
[now − 10 m, now + 30 s]; nonce length in [8, 64]; the Ed25519 public key is
extracted from the sender's peer ID and the signature verified; the nonce is
then checked and recorded for the max-age window (recorded only after a
valid signature, so attackers cannot poison the replay store). Outgoing
nonces are 16 bytes. CBOR messages are rate-limited per sender
(100 per minute, sliding window); text messages are not.

## 4. Sharding

### 4.1 Placement model

*   A node's shard at depth d = the first d bits (MSB first) of SHA-256 of
    its peer ID string. A file's shard at depth d = the first d bits of
    SHA-256 of its payload CID string; depth < 1 clamps to 1.
*   A node is responsible for a payload when its shard is a prefix of the
    payload's bits; the root covers everything. Placement uses the payload
    CID from the manifest, falling back to the manifest CID string when the
    manifest is unreadable.
*   Every shard member pins every file of its shard: copies per file = shard
    size, bounded by Min/MaxPeersPerShard (6–12 by default).
*   Nodes always start at the root; the current shard is not persisted.

### 4.2 Membership counting

Active = ACTIVE or REPLICATOR peers seen within 350 s, self included once
when counting the node's own shard. Mesh size is never used, so monitors and
observers are never miscounted. A foreign shard is counted by a silent
probe: join the topic as an observer, publish `PROBE`, collect replies for
the probe timeout, count.

### 4.3 Split

*   Gate: not within 30 s of any move; at most one check per 2 m (30 s on
    the root).
*   Trigger: active count (incl. self) ≥ Max on two consecutive checks, and
    hashing all members into the two children gives each ≥ Min (unbalanced
    splits are refused).
*   The deciding node probes its own target child (6 s): it joins a
    populated child, and creates an empty one only when the shard holds
    ≥ 2·Min+2 members. It publishes `SPLIT:<child>:<sibling>` on the parent
    topic and moves; everyone else follows through discovery.

### 4.4 Discovery (descend)

*   Interval: 10 s on the root; 45 s when children are known; else 2 m; plus
    jitter. Blocked within 30 s of a move or 2 m of a merge-up.
*   Descends only into shards a received SPLIT named (no phantom joins);
    known children are cleared on every move. Probes 12 s.
*   Empty target child (half-finished split): move only if the sibling ≥ Min
    and the projected own-child membership ≥ Min.
*   Populated child: over the distinct union of parent/child/sibling actives
    (peers that just merged up must not be double-counted), both projected
    children ≥ Min, and the pair total ≥ 2·Min−2 (alone or together with the
    remaining parent members).
*   Child shards rebroadcast `SPLIT` to every ancestor topic every 60 s (as
    silent observers) so late joiners higher up learn the tree.

### 4.5 Merge-up (never on the root)

First match wins (parent/sibling probed 6 s; never merge when the parent is
at Max; blocked within 30 s of a move or 2 m of a descent):

1.  current < Min → merge now (the shard cannot hold enough copies).
2.  current + sibling ≥ 2·Min−2 → stay (the pair is healthy).
3.  current > sibling → stay (the larger, or only populated, side holds).
4.  Otherwise merge to the parent on the 2nd consecutive observation.

### 4.6 Moving (overlap)

Synchronously: swap the current shard, reset lifecycle state, publish LEAVE
on the old topic, JOIN + HEARTBEAT on the new, join the new shard's pin set.
After 250 ms: migrate pins whose payload prefix matches the new shard from
the old pin set. The old topic and pin set are kept for 2 m (messages there
are still processed), then left. ~5 s after the move an early placement pass
(§6.3) re-evaluates every file against the new shard.

## 5. Ingest

### 5.1 Kubo pin ingest — the only ingest path

Kubo's pin set is the single source of truth: whatever users pin there (CLI,
WebUI, IPFS Desktop) gets preserved. Enabled by `DLOCKSS_PIN_INGEST`
(default true); polls the daemon's recursive
pins every 1 m. Every dag-cbor pin is read to learn whether it is a manifest
and which payload it covers (pins named `dlockss:manifest` are manifests
regardless). Skipped: D-LOCKSS pins, Kubo's canonical empty directory,
payloads covered by any manifest (compared by multihash so a CIDv0 user pin
matches a CIDv1 payload), and pins that failed within 30 m; any unreadable
pin defers the whole pass. A pin is ingested only on its second consecutive
sighting (so a concurrent folder ingest is never raced). A pinned directory
is one object; metaRef = the pin name, else `ipfs://<payloadCID>`; size from
`files/stat` (0 when the lookup fails). The user's pin is never removed.

### 5.2 Ingest pipeline

1.  Refuse unless this node is an authorized ingestor (origin gate, §7.3).
2.  Payloads ingested within the last 30 s are deduplicated.
3.  Build + sign the manifest; store it as a dag-cbor block; check BadBits
    on the ManifestCID (blocked ⇒ refuse); pin the manifest recursively
    under the name `dlockss:manifest`. The user's payload pin is never
    touched.
4.  Register + announce `PINNED`. If responsible: add to the own shard's pin
    set, publish a signed IngestMessage on the current shard (and to the
    payload's child at depth+1, as an observer, when that child is already
    populated — probe 2 s), and provide manifest + payload to the DHT (60 s
    each). If not responsible: publish the IngestMessage to the payload's
    nominal shard as a silent observer and keep a custodial copy until the
    placement pass confirms the handoff (§6.3); no DHT provide, and no
    membership in the foreign pin set (pin sets are local state).

## 6. Replication

### 6.1 Shard pin sets

Each member maintains its shard's pin set locally, persisted as JSON under
the cluster-store path. A tracker loop (10 s poll plus event triggers)
reconciles it against Kubo: missing manifests are pinned in bounded
background fetches (3 per shard, 5 m timeout, in-flight dedup) after passing
the origin gate; files already local are tracked immediately — a slow fetch
never stalls the loop. When a manifest first syncs, the node registers it,
announces `PINNED`, provides the manifest, pins the payload as its own root
(5 m fetch budget, ≤ 8 concurrent across shards; the own-root pin keeps
Kubo's `pinned` reprovide strategy re-announcing it), verifies the size
claim (§7.5), and provides the payload. Removing a manifest from the set
releases tracking but never unpins in IPFS — only the placement pass unpins.

### 6.2 Convergence (anti-entropy)

Pin sets converge through the signed message protocol alone:

1.  **IngestMessage**: responsible receivers add the manifest to their own
    shard's pin set (after signature, sender-is-ingestor, and origin
    checks; the payload is resolved with a 5 s budget and unresolvable
    manifests are pinned anyway).
2.  **ReplicationRequest**: every node broadcasts requests for its pinned
    manifests each cycle (1 m; 20 s on the root) — ≤ 50 per cycle with a 5 m
    per-manifest cooldown. Requests are honored only on shards the node is a
    **full member** of (observer subscriptions never make it adopt foreign
    files): holders trigger a pin-set sync; others auto-replicate (5 slots,
    5 m timeout each, excess requests dropped and retried by later cycles)
    and add the manifest to their pin set.
3.  **Heartbeat re-pin / re-provide**: every heartbeat (10 s) publishes
    HEARTBEAT plus ≤ 20 `PINNED:` lines round-robin, and one single-flight
    reprovide step: re-pin the next manifest (2 m, idempotent — completes
    partial DAGs on weak nodes), provide it, re-pin + size-check + provide
    its payload. DHT provider records expire after ~24 h; this keeps them
    live.

This replaces V2's embedded IPFS-Cluster CRDT. Most files reach full shard
replication within 1–2 minutes; a failed fetch retries after the per-manifest
cooldown, and a shard's whole pin set is re-announced roughly every cooldown
window with the load shared across members. Useful log lines:
`auto-replication: fetched and pinned`, `auto-replication: failed to
fetch/pin`, `auto-replication skipped, concurrency limit reached`.

### 6.3 Placement (custodial, orphan, and post-move handoff)

One periodic **placement pass** (every 2 m, plus once ~5 s after each shard
move; single-flight; ≤ 10 DHT lookups and ≤ 100 nudges per pass; both
children probed once, 4 s each) re-evaluates every held file that is old
enough (pin age ≥ 2 m) and not checked within 30 m (never-checked first,
then least recently checked):

1.  Resolve the payload (5 s; unresolvable ⇒ marked checked only).
2.  Compute the file's **home**: outside this shard's prefix ⇒ its nominal
    shard at this depth; inside ⇒ the covering child when that child is
    populated, else the current shard.
3.  Home = here: with < max(1, Min−1) other DHT providers, re-add to the own
    pin set and broadcast a ReplicationRequest.
4.  Home elsewhere (custodial copy, post-move leftover, or a file that now
    belongs a level deeper): **release** — remove from every joined pin set,
    unpin manifest + payload, unregister — only when ≥ Min other providers
    exist AND ≥ 2 nudges were sent in earlier passes; otherwise **nudge**:
    a ReplicationRequest published to the home shard as a silent observer,
    spaced ≥ 6 m per file. Nudge records die after 12 m of silence.

All repair traffic is ReplicationRequests (never IngestMessages, which peers
accept only from authorized ingesters). Newly-responsible files after a
merge-up need no special casing — the replication checker broadcasts them to
the new shard within a cycle. Bookkeeping is in-memory; a restart simply
rechecks, and nothing is ever released without a fresh provider
confirmation.

## 7. Security

### 7.1 Identity

Ed25519, resolved in order: (1) `DLOCKSS_IPFS_CONFIG` ⇒ the Kubo config's
`Identity.PrivKey` (shares the daemon's Peer ID; any failure fatal); (2) the
key file at `DLOCKSS_IDENTITY_PATH`; (3) generated and saved (0600).
Ed25519 peer IDs embed the public key, so signature verification never needs
a network fetch. State files (key, node name, pin sets, trust store) live in
the state directory (`DLOCKSS_STATE_DIR`).

### 7.2 Signatures

All manifests and CBOR messages are signed; verification is strict (§3).
A node without a private key stores unsigned manifests and sends unsigned
messages with a warning — strict peers drop them.

### 7.3 Trust / origins

`trusted_peers.json` (`{"peers": [...]}`; invalid IDs fail the load; missing
file = open) merged with `DLOCKSS_INGEST_ALLOWLIST` forms the trusted set.
A non-empty set activates **origin restriction** in either trust mode:
content is stored only when the manifest's `ingester_id` is listed (failing
closed on unreadable manifests) — enforced at local ingest, incoming
IngestMessages, pin-set acceptance, and replication fetches. Trust mode
`allowlist` additionally restricts protocol senders to the set. A node that
is not itself an authorized ingestor runs as REPLICATOR and ingests nothing
locally.

### 7.4 BadBits

A CSV denylist of CID strings (first column; header skipped unless it looks
like a CID; missing file = open; exact match). Enforced at ingest, pinned
registration, and pin-set sync.

### 7.5 Liar detection

When a payload is pinned (first sync and reprovide), its actual size is
checked against the manifest's claim; a contradiction unpins the file
everywhere. Claimed sizes of 0 are exempt (the pin-ingest path records 0
when the size lookup fails); unverifiable sizes pass and are rechecked
later.

## 8. Node runtime and storage

*   libp2p host: connection manager 160/192; resource limits scaled to
    max(TotalMemory/8, 384 MiB) and 512 FDs. Public mode adds NATPortMap,
    hole punching, AutoRelay via the default IPFS bootstrap peers, a NAT
    service, and parallel bootstrap dials (15 s budget);
    `DLOCKSS_PRIVATE_NETWORK` skips all of those (DHT + mDNS still run).
*   Kubo ≥ 0.43.1 at `DLOCKSS_IPFS_NODE`; unreachable at startup ⇒ fatal.
    DHT provides use `provide/once` (which queues inside the daemon) behind
    a non-blocking semaphore of 8 — busy slots skip; the heartbeat reprovide
    catches up. Provider counts come from `routing/findprovs` (distinct
    non-self providers).
*   In-memory registries: pinned manifests (CID → pin time; round-robin
    iterator over sorted keys) and known files; rebuilt on restart from the
    pin watcher and pin-set sync.
*   Disk monitor on the state directory's filesystem (usage cached 10 s;
    in documented deployments the same disk as the Kubo repo): at or above the
    high-water mark the node advertises PASSIVE, steering custodial and
    replication load away. Non-Linux/macOS platforms report 0 %.
*   Observability: `GET /health` on `DLOCKSS_API_PORT`.
*   Startup order: config → path safety → badbits → node name → Kubo client
    → identity → host/DHT/discovery → trust → stores → pin sets → shard
    manager → API → shard run → pin watcher. Graceful shutdown on
    SIGINT/SIGTERM.

## 9. Configuration

Only operator-facing knobs are configurable; every protocol timing,
concurrency bound, and security parameter is a frozen constant. Invalid
typed values warn and keep the default; unknown `DLOCKSS_*` variables log a
warning so operators notice frozen or removed knobs.

| Var | Default |
|---|---|
| DLOCKSS_STATE_DIR | ./dlockss-state (identity, node name, pin sets, trust store) |
| DLOCKSS_CLUSTER_STORE | `<state>/cluster_store` (pin sets) |
| DLOCKSS_IDENTITY_PATH | `<state>/dlockss.key` |
| DLOCKSS_TRUST_STORE | `<state>/trusted_peers.json` |
| DLOCKSS_BADBITS_PATH | badBits.csv |
| DLOCKSS_IPFS_NODE | /ip4/127.0.0.1/tcp/5001 |
| DLOCKSS_IPFS_CONFIG | (unset: share Kubo's identity) |
| DLOCKSS_NODE_NAME | (unset; else the `node_name` file; else unnamed) |
| DLOCKSS_API_PORT | 5050 |
| DLOCKSS_DISCOVERY_TAG | dlockss-prod |
| DLOCKSS_PUBSUB_TOPIC_PREFIX | dlockss-v0.0.3 |
| DLOCKSS_TOPIC_NAME | creative-commons |
| DLOCKSS_PRIVATE_NETWORK | false |
| DLOCKSS_DHT_CLIENT | false |
| DLOCKSS_MIN_PEERS_PER_SHARD / MAX_PEERS_PER_SHARD | 6 / 12 (swapped when inverted) |
| DLOCKSS_TRUST_MODE | open (open \| allowlist) |
| DLOCKSS_INGEST_ALLOWLIST | (unset, comma-separated peer IDs) |
| DLOCKSS_DISK_USAGE_HIGH_WATER_MARK | 90.0 (reset when ≤0 or >100) |
| DLOCKSS_PIN_INGEST | true |
| DLOCKSS_VERBOSE_LOGGING | false |

Frozen constants (previously knobs): heartbeat 10 s; shard peer check 2 m
(30 s on root); discovery 2 m; split rebroadcast 60 s; seen-peers window
350 s; peer-entry max age 10 m; overlap 2 m; move cooldown 30 s; merge-up
cooldown 2 m; merge probes 6 s; min-across-siblings = 2·Min−2; replication
check 1 m (20 s on root); 5 auto-replication slots, 5 m timeout; provide
timeout 60 s, 8 concurrent; manifest pin timeout 2 m; pin-ingest poll 1 m; rate limit 100/min per peer; signature max age 10 m, skew 30 s, nonces
16/[8,64]; placement pass 2 m, 10 lookups, 100 nudges, 6 m nudge spacing;
bootstrap dial budget 15 s; connmgr 160/192.

## 10. Monitor (`dlockss-monitor`)

*   Own libp2p host (persistent Ed25519 key `monitor_identity.key`, working
    dir or `~/.dlockss-monitor`); NAT options; GossipSub; public DHT
    bootstrap; advertises + mDNS on `dlockss-prod`; mesh maintenance every
    10 m. Publishes `PROBE` on every shard it joins; PROBE-role nodes and
    itself are never shown or counted.
*   Subscribes to the root shard, then follows the tree: SPLITs subscribe
    both children, and a 2 s dynamic pass follows node shards, recent shard
    history (< 5 m), split parents/children and their immediate children.
    Hard cap: shard IDs ≤ 10 bits.
*   Tracks per node: shard, role, pinned count, name, IP, last seen, shard
    history; per manifest: replica set (peer → last seen) and observed shard
    (deepest wins); split events (kept 10 m). Shard moves are accepted only
    to a descendant (recorded as a split) or a sibling (90 s per-peer
    cooldown); others are stale.
*   Expiry: node timeout 350 s (doubled when between LEAVE and JOIN; pruning
    paused 5 m after a split; run on a 1 m timer and on node API calls);
    replica entries 350 s (sweep 1 m); announcements 30 m (sweep 5 m);
    peerstore GC 5 m; pinned=0 heartbeats clear a node's replicas after a
    30 s grace.
*   Replication: raw counts — replicas = fresh entries from displayable
    nodes; target = min(6, displayable nodes in the observed shard), falling
    back to min(6, all displayable nodes); at-target = count ≥ target;
    11 buckets (0–10+). Counts are briefly pessimistic mid-split.
*   HTTP on port 8080 (fixed; read 5 s / write 20 s / idle 120 s): the
    dashboard on unmatched paths; /api/nodes?q=, /api/shard-tree,
    /api/shard-nodes?shard=, /api/root-topic, /api/node-files?peer=,
    /api/unique-cids, /api/payload-cids-export, /api/replication,
    /api/replication-cids?level=, /api/manifest-payload?cid=,
    /api/identify?peer=, /api/keyword-search?q=, /api/keyword-suggest?q=,
    /api/recent-searches, /api/keyword-stats, POST /api/keyword-api-key, and
    a GET|HEAD /ipfs/<cid>[/path] gateway proxy (valid CID required, 2 m
    timeout, 64 MiB cap, Content-Type forwarded). Errors: `{"error": msg}`.
*   Dashboard: one embedded HTML page (monospace b/w, Chart.js, 5 s polling
    paused while searching/editing): stat cards, replication bar chart with
    clickable buckets, pinned-per-node chart, shard doughnut, CSS shard
    tree, node table (search, localStorage aliases, identify modal), CID
    modals (filter, copy, manifest/payload view via the proxy), keyword
    search (autocomplete, recent chips, pagination), settings modal (the
    first saved API key becomes the dialog password).
*   Keyword indexing (optional; enabled once an API key exists): 2 workers,
    1 pick/s, daily caps SAIA 20 000 / Google 1 000 (UTC reset). Per CID:
    manifest via gateway (1 MiB/30 s), payload (90 s; text 512 KiB; PDF/zip
    spooled ≤ 20 MiB; directories: dag-pb listing ≤ 50 entries + ≤ 5 sampled
    files); extraction covers 39 text extensions, HTML/XML tokenizers,
    `pdftotext -layout`, and OOXML/ODF/EPUB zip parts. Unsupported ⇒
    skipped; errors ⇒ ≤ 3 retries with a 10 m cooldown; LLM 429 ⇒ backoff
    then a 5 m pause; gateway 429 ⇒ 1–5 m pause. Providers: SAIA
    (OpenAI-compatible, chat-ai.academiccloud.de, meta-llama-3.1-8b-instruct,
    1 req/s, ≤ 30 000 B) and Google (gemini-2.5-flash-lite, JSON response
    schema, 1 req/4 s, ≤ 8 000 B). Output: title, broad_field, sub_topic,
    research_niche, ≤ 10 lowercase keywords (1–5 words, ≤ 56 runes). Store:
    `keyword_index.json` + `keyword_failures.json` in
    DLOCKSS_MONITOR_DATA_DIR (default `~/.dlockss-monitor`, atomic 0600);
    inverted label index; AND substring search, newest first. Settings in
    `llm_settings.json` (+ `.api_key`); saving requires the current key as
    password (constant-time; first save free) and clears recorded failures.
    Env: DLOCKSS_IPFS_GATEWAY (default `http://ipfs:8080`),
    DLOCKSS_LLM_PROVIDER/_API_BASE/_MODEL (TRACKER_* fallbacks),
    SAIA_API_KEY; the `-topic`/`-prefix` flags override env.

## 11. Packaging & ops

*   Binaries: `dlockss`, `dlockss-monitor`; pure Go (CGO_ENABLED=0), ≥ 1.25.
*   Docker: node image (alpine, `DLOCKSS_STATE_DIR=/data`, EXPOSE
    5050) and monitor image (alpine + poppler-utils, GOMEMLIMIT=512MiB
    GOGC=50, EXPOSE 8080).
*   Testnet: `testnet/run_testnet.sh` starts N local Kubo daemons (lowpower
    + unixfs-v1-2025 profiles, per-node ports) and N nodes named
    `testnet_<i>` sharing Kubo identities via DLOCKSS_IPFS_CONFIG.
*   CI: `go vet` + `go test`, cross-compile linux/darwin/windows ×
    amd64/arm64 (no windows/arm64), release artifacts, both Docker images.

## 12. Public retrieval

Manifests and payloads are provided to the public IPFS DHT (on ingest, on
pin sync, and continuously via the heartbeat reprovide), so public gateways
can locate providers. Actually serving the content requires the holding
nodes to be reachable (public address or relay).

## 13. Differences from the original implementation

This implementation was rewritten from scratch against the original's
extracted requirements, then deliberately slimmed. Wire compatibility
(topics, text messages, signed CBOR messages, manifest encoding, pin names,
provides) is preserved throughout. The notable differences:

**Architecture**

*   The embedded IPFS Cluster (+ go-ds-crdt, ipfs-lite, gorpc, LevelDB) is
    replaced by local per-shard pin sets converging through the protocol's
    own gossip (§6). The `…-crdt-<shard>` topics are gone; V2 nodes converge
    with V3 nodes through the request/announce path.
*   A hand-rolled, fully context-aware Kubo RPC client (~12 endpoints)
    replaces go-ipfs-api. Kubo ≥ 0.43 is required: provides use
    `provide/once` only, with no client-side retry/fallback machinery.
*   One placement pass (§6.3) replaces the original's custodial-retry,
    orphan, and reshard passes and the `resharded_files.json` persistence.
    All repair traffic is ReplicationRequests — the original's re-inject
    path used IngestMessages, which peers refuse from non-ingesters on
    restricted networks.
*   The folder-ingest path is gone entirely: Kubo's pin set is the single
    ingest source, so the watched data directory, its path-safety rules, and
    the client-side import (with its enforced chunking profile — canonical
    CIDs for locally added content now rest on the documented
    `unixfs-v1-2025` Kubo profile) all disappear. The custodial target is
    the nominal shard (no 8-level populated-descent
    probing); the merge ladder is four rules; the PINNED reannounce loop,
    legacy (timestamped) manifest support, bandwidth snapshots, the
    interactive name prompt, and the legacy key migration are gone.
*   Configuration is ~20 operator knobs; all protocol timings are frozen
    constants (§9).

**Fixes over the original**

*   HEARTBEAT/JOIN roles are parsed correctly (a SplitN bug made every peer
    parse as ACTIVE, so full-disk PASSIVE peers were counted as members).
*   Liar detection (§7.5) is actually enforced; it existed only in the V2
    document.
*   Sender binding uses the authenticated gossipsub originator rather than
    the last-hop relay, whose comparison silently dropped any message that
    traveled more than one gossip hop in strict mode.
*   CBOR handlers run in a bounded worker pool off the pubsub delivery loop;
    inline network fetches could overflow the 32-message subscription buffer
    and drop heartbeats.
*   ReplicationRequests are honored only on shards the node is a full member
    of, so observer subscriptions can never make a node adopt foreign files.
*   Releasing a copy scrubs the manifest from every joined pin set, so a
    released node can never keep re-pinning and advertising a copy it no
    longer holds (which would corrupt handoff confirmation for others).
*   Signature verification is strict-only and Ed25519-only (`warn`/`off`
    modes and the connect-and-fetch public-key machinery are gone). PINNED
    announcements are deliberately never acted on for replication: they are
    unsigned, and acting on them would let any peer make nodes fetch
    arbitrary CIDs.
*   Monitor: the payload-CIDs export exports payload CIDs; the keyword
    "failed" stat no longer double-counts; node pruning also runs on a
    timer; replication counts are raw (no aggregation heuristics); and the
    monitor subscribes to the root only, following SPLITs, instead of
    pre-joining 127 topics.
