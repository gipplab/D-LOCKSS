#!/bin/bash

# Configuration
NODE_COUNT=30
BASE_DIR="testnet_data"
BINARY_NAME="dlockss-node"
PID_FILE="active_nodes.pids"
IPFS_PID_FILE="active_ipfs.pids"

# Per-node IPFS ports (avoid collisions)
IPFS_API_BASE_PORT=5010
IPFS_SWARM_BASE_PORT=4010
IPFS_GATEWAY_BASE_PORT=8090

# Colors for pretty printing
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function cleanup {
    echo -e "\n${RED}--- SHUTTING DOWN NETWORK ---${NC}"
    if [ -f "$PID_FILE" ]; then
        while read pid; do
            kill $pid 2>/dev/null
        done < "$PID_FILE"
        rm "$PID_FILE"
    fi
    if [ -f "$IPFS_PID_FILE" ]; then
        while read pid; do
            kill $pid 2>/dev/null
        done < "$IPFS_PID_FILE"
        rm "$IPFS_PID_FILE"
    fi
    # Force kill any stragglers matching binary name
    pkill -f "$BINARY_NAME"
    # Force kill any IPFS daemons started by this testnet run
    pkill -f "ipfs daemon"
    echo -e "${GREEN}Network successfully scorched earth.${NC}"
}

# Trap ctrl-c to ensure cleanup
trap cleanup EXIT

echo -e "${YELLOW}--- Building Binary ---${NC}"
# Check if binary exists and is newer than all Go source files
NEED_REBUILD=true
if [ -f "$BINARY_NAME" ]; then
    # Check if any .go file is newer than the binary
    if find .. -name "*.go" -newer "$BINARY_NAME" 2>/dev/null | grep -q .; then
        NEED_REBUILD=true
    else
        NEED_REBUILD=false
        echo "Binary is up-to-date, skipping rebuild."
    fi
fi

if [ "$NEED_REBUILD" = true ]; then
    (cd .. && go build -o testnet/$BINARY_NAME)
    if [ $? -ne 0 ]; then
        echo "Build failed."
        exit 1
    fi
    echo "Build completed."
fi

# Check for IPFS binary
if ! command -v ipfs &> /dev/null; then
    echo -e "${RED}Error: ipfs binary not found in PATH. Install IPFS (kubo) to run the V2 testnet.${NC}"
    echo "Installation: https://docs.ipfs.tech/install/command-line/"
    exit 1
fi

echo -e "${YELLOW}--- Cleaning old data ---${NC}"
rm -rf $BASE_DIR
mkdir -p $BASE_DIR
rm -f $PID_FILE
rm -f $IPFS_PID_FILE

echo -e "${GREEN}--- Spawning $NODE_COUNT Nodes ---${NC}"

for i in $(seq 1 $NODE_COUNT); do
    # Create isolated directory for this node
    NODE_DIR="$BASE_DIR/node_$i"
    mkdir -p "$NODE_DIR"
    
    # Copy binary to node dir (so they don't lock the same file if OS is strict)
    cp $BINARY_NAME "$NODE_DIR/"

    # --- Start a per-node IPFS daemon ---
    # Each node gets an isolated IPFS repo and unique ports.
    if ! command -v ipfs >/dev/null 2>&1; then
        echo -e "\n${RED}Error: ipfs binary not found in PATH. Install IPFS (kubo) to run the V2 testnet.${NC}"
        exit 1
    fi

    IPFS_REPO="$NODE_DIR/ipfs_repo"
    IPFS_API_PORT=$((IPFS_API_BASE_PORT + i))
    IPFS_SWARM_PORT=$((IPFS_SWARM_BASE_PORT + i))
    IPFS_GATEWAY_PORT=$((IPFS_GATEWAY_BASE_PORT + i))

    mkdir -p "$IPFS_REPO"

    if [ ! -f "$IPFS_REPO/config" ]; then
        # Initialize repo with a low-resource profile; suppress interactive output.
        (export IPFS_PATH="$IPFS_REPO" && ipfs init -e >/dev/null 2>&1)

        # Bind addresses to unique ports and localhost API.
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Addresses.API "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" >/dev/null)
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Addresses.Gateway "/ip4/127.0.0.1/tcp/$IPFS_GATEWAY_PORT" >/dev/null)
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Addresses.Swarm --json "[\"/ip4/0.0.0.0/tcp/$IPFS_SWARM_PORT\",\"/ip4/0.0.0.0/udp/$IPFS_SWARM_PORT/quic-v1\"]" >/dev/null)
    fi

    # Start daemon in background (log into testnet/testnet_data/)
    (export IPFS_PATH="$IPFS_REPO" && ipfs daemon --enable-gc > "$BASE_DIR/node_$i.ipfs.log" 2>&1 & echo $! >> "$IPFS_PID_FILE")

    # Wait for IPFS API to become ready before starting the D-LOCKSS node.
    # This avoids the startup race where D-LOCKSS can't connect and disables file ops.
    IPFS_READY=false
    for attempt in $(seq 1 200); do
        if (export IPFS_PATH="$IPFS_REPO" && ipfs --api "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" id >/dev/null 2>&1); then
            IPFS_READY=true
            break
        fi
        sleep 0.1
    done
    if [ "$IPFS_READY" != true ]; then
        echo -e "\n${RED}Error: IPFS API did not become ready for node $i on /ip4/127.0.0.1/tcp/$IPFS_API_PORT${NC}"
        echo "Check logs: $BASE_DIR/node_$i.ipfs.log"
        exit 1
    fi
    
    # Run the node in background, redirect logs
    # We cd into the dir so "./my-pdfs" is created inside node_X/
    # Enable metrics export for each node
    (cd "$NODE_DIR" && \
        DLOCKSS_METRICS_EXPORT="metrics.csv" \
        DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/$IPFS_API_PORT" \
        DLOCKSS_MAX_PEERS_PER_SHARD=12 \
        DLOCKSS_MIN_PEERS_PER_SHARD=6 \
        DLOCKSS_SHARD_PEER_CHECK_INTERVAL=10s \
        ./$BINARY_NAME > "../node_$i.log" 2>&1 & echo $! >> "../../$PID_FILE")
    
    # Don't start all at exact same millisecond; also gives IPFS time to settle.
    sleep 0.2
    echo -ne "\rStarted node $i/$NODE_COUNT..."
done

echo -e "\n${GREEN}--- Network Live ---${NC}"
echo "Logs are located in $BASE_DIR/node_X.log"
echo "Active PIDs stored in $PID_FILE"

echo -e "\n${YELLOW}--- TEST INSTRUCTIONS ---${NC}"
echo "1. To trigger ingestion: cp your-file.pdf $BASE_DIR/node_1/data/"
echo "2. Watch logs: tail -f $BASE_DIR/node_*.log"
echo "3. Metrics are exported to: $BASE_DIR/node_X/metrics.csv"
echo "4. Press [ENTER] to kill the network and generate charts."

read -p ""

echo -e "\n${YELLOW}--- Generating Charts ---${NC}"
# Check if Python script exists
if [ -f "../scripts/generate_charts.py" ]; then
    # Use auto-discovery mode to process all nodes with timestamped output
    echo "Auto-discovering and processing all metrics CSV files..."
    if command -v uv &> /dev/null; then
        uv run --with-requirements ../scripts/requirements.txt ../scripts/generate_charts.py --auto --search-dir "$BASE_DIR" --base-output "$BASE_DIR/charts" 2>/dev/null || python3 ../scripts/generate_charts.py --auto --search-dir "$BASE_DIR" --base-output "$BASE_DIR/charts" 2>/dev/null || echo "  (Python dependencies not installed - install with: uv pip install -r ../scripts/requirements.txt)"
    else
        python3 ../scripts/generate_charts.py --auto --search-dir "$BASE_DIR" --base-output "$BASE_DIR/charts" 2>/dev/null || echo "  (Python dependencies not installed - install with: uv pip install -r ../scripts/requirements.txt)"
    fi
    echo -e "${GREEN}Charts generated in $BASE_DIR/charts/charts_YYYYMMDD_HHMMSS/${NC}"
else
    echo "Chart generation script not found at ../scripts/generate_charts.py"
fi