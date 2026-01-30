# D-LOCKSS Quick Start Guide

Get a D-LOCKSS node running in 5 minutes!

## Prerequisites Check

```bash
# Check Go (need 1.20+)
go version

# Check IPFS
ipfs version
```

If missing, install:
- Go: https://go.dev/doc/install
- IPFS: https://docs.ipfs.tech/install/command-line/

## Option 1: Automated Setup (Easiest)

```bash
# Run the setup script
./setup.sh

# Follow the prompts
# Start the node
~/.dlockss/run.sh
```

That's it! The script handles everything.

## Option 2: Manual Setup (5 Steps)

### 1. Initialize IPFS (if needed)
```bash
ipfs init
```

### 2. Start IPFS
```bash
ipfs daemon
```
(Keep this running in a separate terminal)

### 3. Build D-LOCKSS
```bash
go build -o dlockss .
```

### 4. Create data directory
```bash
mkdir -p data
```

### 5. Run
```bash
./dlockss
```

## Option 3: Docker (No Dependencies)

```bash
# Start IPFS and D-LOCKSS together
docker-compose up -d

# View logs
docker-compose logs -f dlockss

# Add files
cp my-file.pdf ./data/
```

## Verify It's Working

You should see:
```
--- Node ID: 12D3KooW... ---
[System] Joined Control Channel: dlockss-v2-creative-commons-control
[Sharding] Active Data Shard: 0
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

**Check status:**
```bash
# If using systemd
sudo systemctl status dlockss

# If using Docker
docker-compose ps
```

**View logs:**
```bash
# Direct run
./dlockss

# Systemd
sudo journalctl -u dlockss -f

# Docker
docker-compose logs -f dlockss
```

**Stop node:**
```bash
# Direct: Ctrl+C
# Systemd: sudo systemctl stop dlockss
# Docker: docker-compose down
```

## Configuration

Edit `.env` file or set environment variables:

```bash
# Most important settings
export DLOCKSS_DATA_DIR="./data"
export DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/5001"
export DLOCKSS_NODE_COUNTRY="DE"
```

See `NODE_SETUP.md` for full configuration options.

## Troubleshooting

**"IPFS client not initialized"**
- Make sure `ipfs daemon` is running
- Check IPFS API address matches: `ipfs config Addresses.API`

**"Cannot connect to peers"**
- Ensure ports 4001/tcp and 4001/udp are open
- Check firewall settings

**Need more help?**
- See [NODE_SETUP.md](NODE_SETUP.md) for detailed guide
- Check [README.md](README.md) for full documentation

## Next Steps

- Configure trust store for production
- Set up BadBits filtering
- Enable metrics export
- Join the network!
