#!/bin/bash
# Starts a local D-LOCKSS testnet: one Kubo daemon and one D-LOCKSS node per
# slot, each with its own repo and ports. Press Enter to shut everything down.

NODE_COUNT=3
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BASE_DIR="$SCRIPT_DIR/testnet_data"
BINARY_NAME="dlockss-node"
PID_FILE="active_nodes.pids"
IPFS_PID_FILE="active_ipfs.pids"

IPFS_API_BASE_PORT=5010
IPFS_SWARM_BASE_PORT=4010
IPFS_GATEWAY_BASE_PORT=8090
DLOCKSS_API_BASE_PORT=5050

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

kill_pids() {
    [ -f "$1" ] || return
    while read -r pid; do
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    done < "$1"
    rm -f "$1"
}

cleanup() {
    echo -e "\n${RED}--- Shutting down network ---${NC}"
    kill_pids "$PID_FILE"
    kill_pids "$IPFS_PID_FILE"
    pkill -9 -f "$BINARY_NAME"
    pkill -9 -f "ipfs daemon"
    # Also kill background jobs of this shell. SIGTERM is ignored so the
    # script survives long enough to print the final line.
    trap '' SIGTERM && kill -9 0 2>/dev/null
    echo -e "${GREEN}Network stopped.${NC}"
}

trap cleanup EXIT INT TERM

ipfs_cmd() {
    IPFS_PATH="$1" ipfs "${@:2}"
}

echo -e "${YELLOW}--- Building binaries ---${NC}"
if [ -f "$SCRIPT_DIR/$BINARY_NAME" ] && ! find "$ROOT_DIR" -name "*.go" -newer "$SCRIPT_DIR/$BINARY_NAME" 2>/dev/null | grep -q .; then
    echo "Binary is up to date, skipping rebuild."
else
    echo "Building from $ROOT_DIR..."
    if ! (cd "$ROOT_DIR" && go build -o "$SCRIPT_DIR/$BINARY_NAME" ./cmd/dlockss); then
        echo "Build failed."
        exit 1
    fi
    if ! (cd "$ROOT_DIR" && go build -o "$SCRIPT_DIR/dlockss-monitor" ./cmd/dlockss-monitor); then
        echo "Monitor build failed (continuing anyway)."
    fi
fi

if ! command -v ipfs &> /dev/null; then
    echo -e "${RED}Error: ipfs (Kubo) not found in PATH.${NC}"
    echo "Installation: https://docs.ipfs.tech/install/command-line/"
    exit 1
fi

echo -e "${YELLOW}--- Cleaning old data ---${NC}"
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"
rm -f "$PID_FILE" "$IPFS_PID_FILE"

echo -e "${GREEN}--- Spawning $NODE_COUNT nodes ---${NC}"

for i in $(seq 1 $NODE_COUNT); do
    NODE_DIR="$BASE_DIR/node_$i"
    IPFS_REPO="$NODE_DIR/ipfs_repo"
    IPFS_API_PORT=$((IPFS_API_BASE_PORT + i))
    IPFS_SWARM_PORT=$((IPFS_SWARM_BASE_PORT + i))
    IPFS_GATEWAY_PORT=$((IPFS_GATEWAY_BASE_PORT + i))
    DLOCKSS_API_PORT=$((DLOCKSS_API_BASE_PORT + i))

    mkdir -p "$IPFS_REPO"
    # A private copy keeps the running binaries from sharing one file.
    cp "$SCRIPT_DIR/$BINARY_NAME" "$NODE_DIR/"

    if [ ! -f "$IPFS_REPO/config" ]; then
        ipfs_cmd "$IPFS_REPO" init -e --profile=lowpower,unixfs-v1-2025 >/dev/null 2>&1
        ipfs_cmd "$IPFS_REPO" config Swarm.ConnMgr.LowWater --json 10
        ipfs_cmd "$IPFS_REPO" config Swarm.ConnMgr.HighWater --json 20
        # Nodes 1-3 serve the DHT; the rest are clients to save bandwidth.
        if [ "$i" -le 3 ]; then
            ipfs_cmd "$IPFS_REPO" config Routing.Type "dhtserver"
        else
            ipfs_cmd "$IPFS_REPO" config Routing.Type "dhtclient"
        fi
        # mDNS lets the local daemons find each other without bootstrap peers.
        ipfs_cmd "$IPFS_REPO" config Discovery.MDNS.Enabled --json true
        ipfs_cmd "$IPFS_REPO" config AutoNAT.ServiceMode "disabled"
        ipfs_cmd "$IPFS_REPO" config Addresses.API "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" >/dev/null
        ipfs_cmd "$IPFS_REPO" config Addresses.Gateway "/ip4/127.0.0.1/tcp/$IPFS_GATEWAY_PORT" >/dev/null
        ipfs_cmd "$IPFS_REPO" config Addresses.Swarm --json "[\"/ip4/0.0.0.0/tcp/$IPFS_SWARM_PORT\",\"/ip4/0.0.0.0/udp/$IPFS_SWARM_PORT/quic-v1\"]" >/dev/null
    fi
    ipfs_cmd "$IPFS_REPO" config profile apply unixfs-v1-2025 >/dev/null 2>&1 || true

    (IPFS_PATH="$IPFS_REPO" ipfs daemon --enable-gc > "$BASE_DIR/node_$i.ipfs.log" 2>&1 & echo $! >> "$IPFS_PID_FILE")

    # D-LOCKSS disables file operations if Kubo is not reachable at startup.
    IPFS_READY=false
    for _ in $(seq 1 200); do
        if ipfs_cmd "$IPFS_REPO" --api "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" id >/dev/null 2>&1; then
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

    pushd "$NODE_DIR" >/dev/null
    # Shard limits 12/6 avoid over-splitting a small testnet. The identity is
    # taken from the Kubo config, so each node has a single peer ID.
    DLOCKSS_IPFS_CONFIG="$IPFS_REPO/config" \
    DLOCKSS_NODE_NAME="testnet_$i" \
    DLOCKSS_TOPIC_NAME="cc" \
    DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/$IPFS_API_PORT" \
    DLOCKSS_API_PORT=$DLOCKSS_API_PORT \
    DLOCKSS_MAX_PEERS_PER_SHARD=12 \
    DLOCKSS_MIN_PEERS_PER_SHARD=6 \
    ./$BINARY_NAME > "../node_$i.log" 2>&1 &
    NODE_PID=$!
    echo $NODE_PID >> "../../$PID_FILE"
    # Disowned so the shell does not report "Killed" during cleanup.
    disown $NODE_PID
    popd >/dev/null

    sleep 0.2
    echo -ne "\rStarted node $i/$NODE_COUNT..."
done

echo -e "\n${GREEN}--- Network live ---${NC}"
echo "Logs: $BASE_DIR/node_X.log"
echo "PIDs: $PID_FILE"

echo -e "\n${YELLOW}--- Usage ---${NC}"
echo "Ingest a file:   IPFS_PATH=$BASE_DIR/node_1/ipfs_repo ipfs add your-file.pdf"
echo "Or pin it:       IPFS_PATH=$BASE_DIR/node_1/ipfs_repo ipfs add your-file.pdf"
echo "Watch logs:      tail -f $BASE_DIR/node_*.log"
echo "Press [ENTER] to stop the network."

read -r -p ""
