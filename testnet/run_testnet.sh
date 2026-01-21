#!/bin/bash

# Configuration
# Reduced to 15 nodes for bandwidth-limited environments
# Each node runs its own isolated IPFS daemon
NODE_COUNT=50
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
    
    # 1. Kill processes listed in PID files immediately (Force Kill)
    if [ -f "$PID_FILE" ]; then
        while read pid; do
            # Check if process is running (-0) then force kill (-9)
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi

    if [ -f "$IPFS_PID_FILE" ]; then
        while read pid; do
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
        done < "$IPFS_PID_FILE"
        rm -f "$IPFS_PID_FILE"
    fi

    # 2. Sweep by name using SIGKILL (-9)
    # Using full command line matching (-f) to catch the nodes
    pkill -9 -f "$BINARY_NAME"
    pkill -9 -f "ipfs daemon"
    
    # 3. Nuclear Option: Kill the entire process group
    # This ensures any background jobs spawned by this shell are terminated.
    # We trap SIGKILL to prevent the script from killing itself instantly before the echo.
    trap '' SIGTERM && kill -9 0 2>/dev/null

    echo -e "${GREEN}Network successfully scorched earth.${NC}"
}

# Trap EXIT (normal end), INT (Ctrl+C), and TERM (kill command)
trap cleanup EXIT INT TERM

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
        # 1. Init with 'lowpower' profile to keep background overhead low
        (export IPFS_PATH="$IPFS_REPO" && ipfs init -e --profile=lowpower >/dev/null 2>&1)

        # 2. Connection Limits (Bandwidth Protection)
        #    Keep these low to save bandwidth, even if CPU is fine.
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Swarm.ConnMgr.LowWater --json 10)
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Swarm.ConnMgr.HighWater --json 20)
        
        # 3. Disable DHT Server Mode (Bandwidth Protection)
        #    Stops nodes from maintaining the global network map.
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Routing.Type "dhtclient")

        # 4. ENABLE MDNS (Automatic Discovery)
        #    Since you have CPU power, this allows nodes to find each other 
        #    automatically on localhost without manual wiring.
        (export IPFS_PATH="$IPFS_REPO" && ipfs config Discovery.MDNS.Enabled --json true)

        # 5. Disable AutoNAT
        (export IPFS_PATH="$IPFS_REPO" && ipfs config AutoNAT.ServiceMode "disabled")

        # Bind addresses
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
    
# --- Start Node (Main Scope Fix) ---
    
    # 1. Change directory safely without a subshell
    pushd "$NODE_DIR" >/dev/null

    # 2. Run the node in background
    DLOCKSS_METRICS_EXPORT="metrics.csv" \
    DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/$IPFS_API_PORT" \
    DLOCKSS_MAX_PEERS_PER_SHARD=12 \
    DLOCKSS_MIN_PEERS_PER_SHARD=6 \
    DLOCKSS_SHARD_PEER_CHECK_INTERVAL=10s \
    DLOCKSS_CHECK_INTERVAL=15s \
    DLOCKSS_REPLICATION_COOLDOWN=5s \
    DLOCKSS_MAX_CONCURRENT_CHECKS=20 \
    DLOCKSS_AUTO_REPLICATION_ENABLED=true \
    ./$BINARY_NAME > "../node_$i.log" 2>&1 &
    
    # 3. Capture PID
    NODE_PID=$!
    echo $NODE_PID >> "../../$PID_FILE"
    
    # 4. Disown IMMEDIATELY from the main script
    # This prevents the script from reporting "Killed" when cleanup runs
    disown $NODE_PID
    
    # 5. Return to previous directory
    popd >/dev/null
    
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