#!/bin/bash

# Configuration
NODE_COUNT=50
BASE_DIR="testnet_data"
BINARY_NAME="dlockss-node"
PID_FILE="active_nodes.pids"

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
    # Force kill any stragglers matching binary name
    pkill -f "$BINARY_NAME"
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

echo -e "${YELLOW}--- Cleaning old data ---${NC}"
rm -rf $BASE_DIR
mkdir -p $BASE_DIR
rm -f $PID_FILE

echo -e "${GREEN}--- Spawning $NODE_COUNT Nodes ---${NC}"

for i in $(seq 1 $NODE_COUNT); do
    # Create isolated directory for this node
    NODE_DIR="$BASE_DIR/node_$i"
    mkdir -p "$NODE_DIR"
    
    # Copy binary to node dir (so they don't lock the same file if OS is strict)
    cp $BINARY_NAME "$NODE_DIR/"
    
    # Run the node in background, redirect logs
    # We cd into the dir so "./my-pdfs" is created inside node_X/
    # Enable metrics export for each node
    (cd "$NODE_DIR" && DLOCKSS_METRICS_EXPORT="metrics.csv" ./$BINARY_NAME > "../node_$i.log" 2>&1 & echo $! >> "../../$PID_FILE")
    
    # Don't start all at exact same millisecond, helps mDNS storm
    # aggressive start: 0.1s delay
    sleep 0.1
    echo -ne "\rStarted node $i/$NODE_COUNT..."
done

echo -e "\n${GREEN}--- Network Live ---${NC}"
echo "Logs are located in $BASE_DIR/node_X.log"
echo "Active PIDs stored in $PID_FILE"

echo -e "\n${YELLOW}--- TEST INSTRUCTIONS ---${NC}"
echo "1. To trigger replication: cp your-file.pdf $BASE_DIR/node_1/my-pdfs/"
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