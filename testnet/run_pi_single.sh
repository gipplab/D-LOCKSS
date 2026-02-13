#!/bin/sh
# Run a single D-LOCKSS node with one IPFS daemon on a Raspberry Pi (e.g. Pi 3).
# Usage: ./run_pi_single.sh [data_dir]
#   Or: bash run_pi_single.sh   (if you use bash)
#   data_dir: optional; default is ./pi_data (or use DLOCKSS_PI_DATA env).
#   DLOCKSS_BINARY: optional path to pre-built dlockss (e.g. /home/crnls/dlockss).
# Installs Kubo (IPFS) if not found, with ARM-friendly config. Builds dlockss for ARM if go is available; else use DLOCKSS_BINARY or a binary next to this script.
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# POSIX way to get script directory (works with sh and bash)
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(dirname "$SCRIPT_DIR")

# Base directory: env, then first arg, then default
BASE_DIR="$DLOCKSS_PI_DATA"
[ -z "$BASE_DIR" ] && BASE_DIR="$1"
[ -z "$BASE_DIR" ] && BASE_DIR="$SCRIPT_DIR/pi_data"
if [ -d "$BASE_DIR" ]; then
  BASE_DIR=$(cd "$BASE_DIR" && pwd)
else
  mkdir -p "$BASE_DIR"
  BASE_DIR=$(cd "$BASE_DIR" && pwd)
fi

BINARY_NAME="dlockss"
# Resolve D-LOCKSS binary: env DLOCKSS_BINARY, or script dir (dlockss, dlockss-linux-ARCH), or $PATH
DLOCKSS_BIN=""
if [ -n "$DLOCKSS_BINARY" ] && [ -f "$DLOCKSS_BINARY" ]; then
  DLOCKSS_BIN="$DLOCKSS_BINARY"
elif [ -f "$SCRIPT_DIR/$BINARY_NAME" ]; then
  DLOCKSS_BIN="$SCRIPT_DIR/$BINARY_NAME"
fi
# If not found yet, try architecture-named binary (e.g. dlockss-linux-arm64 from GitHub Actions)
if [ -z "$DLOCKSS_BIN" ]; then
  ARCH=$(uname -m)
  case "$ARCH" in
    armv7l|armhf)  try="dlockss-linux-arm" ;;
    aarch64|arm64) try="dlockss-linux-arm64" ;;
    x86_64|amd64)  try="dlockss-linux-amd64" ;;
    *)             try="dlockss-linux-$ARCH" ;;
  esac
  for dir in "$SCRIPT_DIR" "$(pwd)" "$HOME"; do
    if [ -f "$dir/$try" ]; then
      DLOCKSS_BIN="$dir/$try"
      break
    fi
  done
fi
IPFS_PID_FILE="$BASE_DIR/ipfs.pid"
DLOCKSS_PID_FILE="$BASE_DIR/dlockss.pid"
IPFS_REPO="$BASE_DIR/ipfs_repo"
IPFS_API_PORT="${IPFS_API_PORT:-5001}"
IPFS_SWARM_PORT="${IPFS_SWARM_PORT:-4001}"
IPFS_GATEWAY_PORT="${IPFS_GATEWAY_PORT:-8080}"
DLOCKSS_API_PORT="${DLOCKSS_API_PORT:-5050}"
LOG_IPFS="$BASE_DIR/ipfs.log"
LOG_DLOCKSS="$BASE_DIR/dlockss.log"
KUBO_DIR="$BASE_DIR/kubo"
KUBO_VERSION="${KUBO_VERSION:-0.32.1}"

cleanup() {
  printf '\n%b--- Shutting down ---%b\n' "$RED" "$NC"
  if [ -f "$DLOCKSS_PID_FILE" ]; then
    pid=$(cat "$DLOCKSS_PID_FILE")
    kill -0 "$pid" 2>/dev/null && kill -15 "$pid" 2>/dev/null
    rm -f "$DLOCKSS_PID_FILE"
  fi
  if [ -f "$IPFS_PID_FILE" ]; then
    pid=$(cat "$IPFS_PID_FILE")
    kill -0 "$pid" 2>/dev/null && kill -15 "$pid" 2>/dev/null
    rm -f "$IPFS_PID_FILE"
  fi
  pkill -15 -f "ipfs daemon" 2>/dev/null || true
  printf '%bStopped.%b\n' "$GREEN" "$NC"
}
trap cleanup EXIT INT TERM

printf '%b--- Raspberry Pi single node ---%b\n' "$YELLOW" "$NC"
echo "Data dir: $BASE_DIR"

# Detect ARM arch for build
ARCH=$(uname -m)
case "$ARCH" in
  armv7l|armhf)  GOARCH="arm" ;;
  aarch64|arm64) GOARCH="arm64" ;;
  *)             GOARCH="$ARCH" ;;
esac
echo "Arch: $ARCH (GOARCH=$GOARCH)"

# Resolve binary again after maybe building (same order: env, dlockss, dlockss-linux-ARCH)
resolve_bin() {
  DLOCKSS_BIN=""
  if [ -n "$DLOCKSS_BINARY" ] && [ -f "$DLOCKSS_BINARY" ]; then
    DLOCKSS_BIN="$DLOCKSS_BINARY"
  elif [ -f "$SCRIPT_DIR/$BINARY_NAME" ]; then
    DLOCKSS_BIN="$SCRIPT_DIR/$BINARY_NAME"
  else
    arch=$(uname -m)
    case "$arch" in
      armv7l|armhf)  try="dlockss-linux-arm" ;;
      aarch64|arm64) try="dlockss-linux-arm64" ;;
      x86_64|amd64)  try="dlockss-linux-amd64" ;;
      *)             try="dlockss-linux-$arch" ;;
    esac
    for dir in "$SCRIPT_DIR" "$(pwd)" "$HOME"; do
      if [ -f "$dir/$try" ]; then
        DLOCKSS_BIN="$dir/$try"
        break
      fi
    done
  fi
}

# Build D-LOCKSS for Linux ARM if binary missing or outdated (and go is available)
NEED_BUILD=0
if [ -z "$DLOCKSS_BIN" ]; then
  NEED_BUILD=1
elif [ -f "$ROOT_DIR/cmd/dlockss/main.go" ] && [ "$ROOT_DIR/cmd/dlockss/main.go" -nt "$DLOCKSS_BIN" ] 2>/dev/null; then
  NEED_BUILD=1
fi

if [ "$NEED_BUILD" -eq 1 ]; then
  if ! command -v go >/dev/null 2>&1; then
    printf '%bGo not found. Either:%b\n' "$RED" "$NC"
    echo "  1. Install Go: https://go.dev/dl/"
    echo "  2. Or build elsewhere and copy the binary:"
    echo "     GOOS=linux GOARCH=$GOARCH go build -o dlockss ./cmd/dlockss"
    echo "     Then either:"
    echo "       - copy 'dlockss' to: $SCRIPT_DIR/"
    echo "       - or set: DLOCKSS_BINARY=/path/to/dlockss"
    echo "     Example: DLOCKSS_BINARY=$HOME/dlockss sh deploydlockss.sh"
    exit 1
  fi
  printf '%bBuilding D-LOCKSS for linux/%s...%b\n' "$YELLOW" "$GOARCH" "$NC"
  if ! (cd "$ROOT_DIR" && GOOS=linux GOARCH=$GOARCH go build -ldflags="-s -w" -o "$SCRIPT_DIR/$BINARY_NAME" ./cmd/dlockss); then
    printf '%bBuild failed.%b\n' "$RED" "$NC"
    exit 1
  fi
  printf '%bBuild OK.%b\n' "$GREEN" "$NC"
fi
resolve_bin

if [ -z "$DLOCKSS_BIN" ] || [ ! -f "$DLOCKSS_BIN" ]; then
  printf '%bNo D-LOCKSS binary found. Set DLOCKSS_BINARY or copy binary to %s/%s%b\n' "$RED" "$SCRIPT_DIR" "$BINARY_NAME" "$NC"
  exit 1
fi

# Resolve IPFS (Kubo) binary: use system ipfs if available, else install to BASE_DIR/kubo
IPFS_CMD=""
if command -v ipfs >/dev/null 2>&1; then
  IPFS_CMD="ipfs"
fi
if [ -z "$IPFS_CMD" ]; then
  # Map arch to Kubo tarball suffix (linux-arm64, linux-arm, etc.)
  case "$ARCH" in
    armv7l|armhf)  KUBO_ARCH="arm" ;;
    aarch64|arm64) KUBO_ARCH="arm64" ;;
    x86_64|amd64)  KUBO_ARCH="amd64" ;;
    *)             KUBO_ARCH="$ARCH" ;;
  esac
  KUBO_TAR="kubo_v${KUBO_VERSION}_linux-${KUBO_ARCH}.tar.gz"
  KUBO_URL="https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/${KUBO_TAR}"
  # Tarball extracts to a single top-level dir "kubo/" (not kubo_vX.X.X_linux-arch)
  KUBO_EXTRACTED="$KUBO_DIR/kubo"

  if [ -f "$KUBO_EXTRACTED/ipfs" ]; then
    chmod +x "$KUBO_EXTRACTED/ipfs" 2>/dev/null || true
    IPFS_CMD="$KUBO_EXTRACTED/ipfs"
    printf '%bUsing Kubo at %s%b\n' "$GREEN" "$IPFS_CMD" "$NC"
  else
    printf '%bKubo (IPFS) not found. Installing v%s for linux-%s...%b\n' "$YELLOW" "$KUBO_VERSION" "$KUBO_ARCH" "$NC"
    if command -v curl >/dev/null 2>&1; then
      DOWNLOAD="curl -sL"
    elif command -v wget >/dev/null 2>&1; then
      DOWNLOAD="wget -q -O -"
    else
      printf '%bNeed curl or wget to download Kubo. Install one of them.%b\n' "$RED" "$NC"
      exit 1
    fi
    mkdir -p "$KUBO_DIR"
    if ! (cd "$KUBO_DIR" && $DOWNLOAD "$KUBO_URL" | tar xzf -); then
      printf '%bKubo download failed. URL: %s%b\n' "$RED" "$KUBO_URL" "$NC"
      exit 1
    fi
    # Tarball has top-level "kubo/" with ipfs binary inside
    if [ ! -f "$KUBO_EXTRACTED/ipfs" ]; then
      IPFS_CMD=""
      for f in "$KUBO_DIR"/kubo/ipfs "$KUBO_DIR"/*/ipfs; do
        if [ -f "$f" ]; then IPFS_CMD="$f"; break; fi
      done
      if [ -z "$IPFS_CMD" ]; then
        printf '%bKubo binary not found after extract. Check %s%b\n' "$RED" "$KUBO_DIR" "$NC"
        exit 1
      fi
    else
      IPFS_CMD="$KUBO_EXTRACTED/ipfs"
    fi
    chmod +x "$IPFS_CMD" 2>/dev/null || true
    printf '%bKubo installed.%b\n' "$GREEN" "$NC"
  fi
fi

mkdir -p "$IPFS_REPO"
if [ ! -f "$IPFS_REPO/config" ]; then
  printf '%bInitializing IPFS repo (lowpower profile, ARM-friendly)...%b\n' "$YELLOW" "$NC"
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" init -e --profile=lowpower >/dev/null 2>&1)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Addresses.API "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" >/dev/null)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Addresses.Gateway "/ip4/127.0.0.1/tcp/$IPFS_GATEWAY_PORT" >/dev/null)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Addresses.Swarm --json "[\"/ip4/0.0.0.0/tcp/$IPFS_SWARM_PORT\",\"/ip4/0.0.0.0/udp/$IPFS_SWARM_PORT/quic-v1\"]" >/dev/null)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Swarm.ConnMgr.LowWater --json 8)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Swarm.ConnMgr.HighWater --json 16)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Discovery.MDNS.Enabled --json true)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config AutoNAT.ServiceMode "disabled")
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Datastore.StorageMax "10GB" 2>/dev/null || true)
  (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" config Routing.Type "dhtclient" 2>/dev/null || true)
  printf '%bIPFS repo ready.%b\n' "$GREEN" "$NC"
fi

printf '%bStarting IPFS daemon...%b\n' "$YELLOW" "$NC"
# Filter reprovider "block not found locally" errors (harmless; occurs when D-LOCKSS unpins after shard moves)
(export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" daemon --enable-gc 2>&1 | grep -v 'reproviding failed both routing/provide' >> "$LOG_IPFS" & echo $! > "$IPFS_PID_FILE")

i=1
while [ "$i" -le 120 ]; do
  if (export IPFS_PATH="$IPFS_REPO" && "$IPFS_CMD" --api "/ip4/127.0.0.1/tcp/$IPFS_API_PORT" id >/dev/null 2>&1); then
    printf '%bIPFS API ready.%b\n' "$GREEN" "$NC"
    break
  fi
  if [ "$i" -eq 120 ]; then
    printf '%bIPFS did not become ready. See %s%b\n' "$RED" "$LOG_IPFS" "$NC"
    exit 1
  fi
  i=$((i + 1))
  sleep 0.5
done

mkdir -p "$BASE_DIR/data"
printf '%bStarting D-LOCKSS...%b\n' "$YELLOW" "$NC"
# Same config as testnet (run_testnet.sh) so Pi nodes behave identically for sharding/splits/replication
(
  cd "$BASE_DIR"
  IPFS_PATH="$IPFS_REPO" \
  DLOCKSS_DATA_DIR="$BASE_DIR/data" \
  DLOCKSS_METRICS_EXPORT="metrics.csv" \
  DLOCKSS_IPFS_NODE="/ip4/127.0.0.1/tcp/$IPFS_API_PORT" \
  DLOCKSS_API_PORT=$DLOCKSS_API_PORT \
  DLOCKSS_MAX_PEERS_PER_SHARD=12 \
  DLOCKSS_MIN_PEERS_PER_SHARD=6 \
  DLOCKSS_SHARD_PEER_CHECK_INTERVAL=10s \
  DLOCKSS_CHECK_INTERVAL=20s \
  DLOCKSS_REPLICATION_COOLDOWN=5s \
  DLOCKSS_REPLICATION_VERIFICATION_DELAY=30s \
  DLOCKSS_REPLICATION_CACHE_TTL=1m \
  DLOCKSS_DHT_QUERY_TIMEOUT=30s \
  DLOCKSS_DHT_PROVIDE_TIMEOUT=15s \
  DLOCKSS_INITIAL_BACKOFF=3s \
  DLOCKSS_MAX_BACKOFF=2m \
  DLOCKSS_BACKOFF_MULTIPLIER=1.8 \
  DLOCKSS_AUTO_REPLICATION_TIMEOUT=2m \
  DLOCKSS_AUTO_REPLICATION_ENABLED=true \
  DLOCKSS_SHARD_OVERLAP_DURATION=1m \
  DLOCKSS_RESHARD_DELAY=2s \
  DLOCKSS_REMOVED_COOLDOWN=30s \
  DLOCKSS_MAX_CONCURRENT_CHECKS=20 \
  "$DLOCKSS_BIN" >> "$LOG_DLOCKSS" 2>&1 &
  echo $! > "$DLOCKSS_PID_FILE"
)

sleep 2
if ! kill -0 "$(cat "$DLOCKSS_PID_FILE")" 2>/dev/null; then
  printf '%bD-LOCKSS exited. Check %s%b\n' "$RED" "$LOG_DLOCKSS" "$NC"
  exit 1
fi

printf '%b--- Node running ---%b\n' "$GREEN" "$NC"
echo "  IPFS API:       http://127.0.0.1:$IPFS_API_PORT"
echo "  D-LOCKSS API:   http://127.0.0.1:$DLOCKSS_API_PORT"
echo "  Data:           $BASE_DIR/data"
echo "  D-LOCKSS log:   $LOG_DLOCKSS"
echo "  IPFS log:       $LOG_IPFS"
echo ""
echo "  To confirm heartbeats: DLOCKSS_VERBOSE_LOGGING=true (then restart); tail -f $LOG_DLOCKSS"
echo ""
echo "Press Enter to stop."
read -r _
exit 0
