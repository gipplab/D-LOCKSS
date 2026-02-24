#!/bin/bash
set -euo pipefail

# Cross-compiles D-LOCKSS for arm64, updates Kubo (IPFS), copies everything
# to your Pis, and starts both in tmux sessions.
#
# Usage:
#   ./deploy/deploy_pis.sh                # all Pis
#   ./deploy/deploy_pis.sh pi2 pi3        # specific hosts

PI_HOSTS=("pi1" "pi2" "pi3" "pi4")
PI_USER="crnls"
REMOTE_BIN="/home/crnls/dlockss-linux-arm64"
IPFS_REPO="/home/crnls/pi_data/ipfs_repo"
KUBO_VERSION="0.39.0"

if [ $# -gt 0 ]; then PI_HOSTS=("$@"); fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BIN="$(mktemp)"
KUBO_TMP="$(mktemp)"
trap "rm -f $BIN $KUBO_TMP" EXIT

echo "Building D-LOCKSS for linux/arm64..."
(cd "$ROOT_DIR" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags="-s -w" -o "$BIN" ./cmd/dlockss)
echo "Done ($(du -h "$BIN" | cut -f1))"

echo "Downloading Kubo v${KUBO_VERSION} for linux/arm64..."
curl -fSL --progress-bar -o "$KUBO_TMP" \
    "https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/kubo_v${KUBO_VERSION}_linux-arm64.tar.gz"
echo "Done"

for HOST in "${PI_HOSTS[@]}"; do
    echo "=== ${HOST} ==="

    echo "  Stopping old processes..."
    ssh "${PI_USER}@${HOST}" "tmux kill-session -t dlockss 2>/dev/null; tmux kill-session -t ipfs 2>/dev/null; pkill -f dlockss-linux-arm64 2>/dev/null; pkill -f 'ipfs daemon' 2>/dev/null; sleep 2; pkill -9 -f dlockss-linux-arm64 2>/dev/null; pkill -9 -f 'ipfs daemon' 2>/dev/null; rm -f ${IPFS_REPO}/repo.lock 2>/dev/null; true" || true

    echo "  Copying D-LOCKSS binary..."
    scp -q "$BIN" "${PI_USER}@${HOST}:${REMOTE_BIN}"
    ssh "${PI_USER}@${HOST}" "chmod 755 ${REMOTE_BIN}"

    echo "  Updating Kubo..."
    scp -q "$KUBO_TMP" "${PI_USER}@${HOST}:/tmp/kubo.tar.gz"
    ssh "${PI_USER}@${HOST}" "cd /tmp && tar xzf kubo.tar.gz && sudo mv kubo/ipfs /usr/local/bin/ipfs && sudo chmod 755 /usr/local/bin/ipfs && rm -rf /tmp/kubo /tmp/kubo.tar.gz && ipfs --version" || true

    echo "  Migrating IPFS repo if needed..."
    ssh "${PI_USER}@${HOST}" "IPFS_PATH=${IPFS_REPO} ipfs repo migrate 2>&1 || true"

    echo "  Starting IPFS..."
    ssh "${PI_USER}@${HOST}" "tmux new-session -d -s ipfs 'IPFS_PATH=${IPFS_REPO} ipfs daemon --enable-gc 2>&1 | tee /tmp/ipfs-start.log'"
    echo "  Waiting for IPFS API (15s)..."
    sleep 15

    # Verify IPFS
    if ssh "${PI_USER}@${HOST}" "IPFS_PATH=${IPFS_REPO} ipfs id &>/dev/null"; then
        echo "  IPFS: OK"
    else
        echo "  IPFS: FAILED - last output:"
        ssh "${PI_USER}@${HOST}" "tail -20 /tmp/ipfs-start.log 2>/dev/null" || true
        echo "  Skipping D-LOCKSS on ${HOST}"
        continue
    fi

    echo "  Starting D-LOCKSS..."
    ssh "${PI_USER}@${HOST}" "tmux new-session -d -s dlockss 'DLOCKSS_NODE_NAME=${HOST} DLOCKSS_DATA_DIR=./data IPFS_PATH=${IPFS_REPO} DLOCKSS_IPFS_NODE=/ip4/127.0.0.1/tcp/5001 ${REMOTE_BIN} 2>&1 | tee /tmp/dlockss-start.log'"
    sleep 3

    # Verify D-LOCKSS
    if ssh "${PI_USER}@${HOST}" "pgrep -f dlockss-linux-arm64 &>/dev/null"; then
        echo "  D-LOCKSS: OK"
    else
        echo "  D-LOCKSS: FAILED - last output:"
        ssh "${PI_USER}@${HOST}" "tail -20 /tmp/dlockss-start.log 2>/dev/null" || true
    fi
done

echo ""
echo "=== Summary ==="
for HOST in "${PI_HOSTS[@]}"; do
    IPFS_OK=$(ssh "${PI_USER}@${HOST}" "pgrep -f 'ipfs daemon' &>/dev/null && echo OK || echo DOWN") || IPFS_OK="UNREACHABLE"
    DLOCKSS_OK=$(ssh "${PI_USER}@${HOST}" "pgrep -f dlockss-linux-arm64 &>/dev/null && echo OK || echo DOWN") || DLOCKSS_OK="UNREACHABLE"
    printf "  %-8s ipfs=%-4s dlockss=%s\n" "$HOST" "$IPFS_OK" "$DLOCKSS_OK"
done
echo ""
echo "  ssh ${PI_USER}@<host> 'tmux attach -t dlockss'   # D-LOCKSS output"
echo "  ssh ${PI_USER}@<host> 'tmux attach -t ipfs'      # IPFS output"
