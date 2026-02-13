# D-LOCKSS Quick Start Guide

## Prerequisites

- **Go 1.21+:** [Install Go](https://go.dev/doc/install)
- **IPFS (Kubo):** [Install IPFS](https://docs.ipfs.tech/install/command-line/)

```bash
go version   # 1.21+
ipfs version
```

## Build and Run

### 1. Start IPFS (separate terminal)
```bash
ipfs init    # first time only
ipfs daemon
```

### 2. Build the node
```bash
git clone https://github.com/gipplab/D-LOCKSS
cd D-LOCKSS
go build -o dlockss ./cmd/dlockss
```

### 3. Data directory and run
```bash
mkdir -p data
./dlockss
```

Optional: build the monitor dashboard with `go build -o dlockss-monitor ./cmd/dlockss-monitor` (see [README](README.md)).

## Verify It's Working

You should see:
```
--- Node ID: 12D3KooW... ---
[Sharding] Joined shard  (Topic: dlockss-creative-commons-shard-)
[Config] ...
[API] Starting observability server on :5050
[FileWatcher] Watching ./data (and subdirectories) for new files...
```

## Add Your First File

```bash
# Copy a file to the data directory
cp my-document.pdf ./data/

# Watch the logs - you'll see:
# [FileOps] Importing file...
# [Core] I am responsible for PayloadCID...
```

## Common Commands

**View logs:** Run `./dlockss` in foreground, or redirect output if running in background.

**Stop node:** Ctrl+C if running in foreground.

## Configuration

Edit `.env` file or set environment variables:

```bash
# Main settings
export DLOCKSS_DATA_DIR="./data"
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"
export DLOCKSS_NODE_COUNTRY="DE"
```

**Shard tuning (small testnets):** For ~15 nodes, use `DLOCKSS_MAX_PEERS_PER_SHARD=12` and `DLOCKSS_MIN_PEERS_PER_SHARD=6` (defaults) so the network splits into a small number of shards (e.g. root → `0` and `1`) instead of many understaffed ones. If you use a lower max (e.g. 6), keep `DLOCKSS_MIN_PEERS_PER_SHARD` at least 4. `DLOCKSS_SHARD_DISCOVERY_INTERVAL` (default 2m) controls how often nodes discover and migrate to deeper shards. `DLOCKSS_SHARD_SPLIT_REBROADCAST_INTERVAL` (default 60s) controls how often child shards rebroadcast SPLIT to ancestors for late joiners.

See [README](README.md#configuration) for full configuration options.

## Troubleshooting

**"IPFS client not initialized"**
- Run `ipfs daemon` and check it is up
- Check IPFS API address: `ipfs config Addresses.API`

**"Cannot connect to peers"**
- Open ports 4001/tcp and 4001/udp
- Check firewall settings

**Need more help?** See [README](README.md) and [docs/DLOCKSS_PROTOCOL.md](docs/DLOCKSS_PROTOCOL.md).

## Next Steps

- Configure trust store for production
- Set up BadBits filtering
- Enable metrics export
- Join the network!
