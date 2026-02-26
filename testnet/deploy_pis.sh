#!/bin/bash
set -euo pipefail

# Cross-compiles D-LOCKSS for arm64, updates Kubo (IPFS), copies everything
# to your Pis, and starts both in tmux sessions.
#
# Usage:
#   ./testnet/deploy_pis.sh                # all Pis
#   ./testnet/deploy_pis.sh pi2 pi3        # specific hosts

PI_HOSTS=("pi1" "pi2" "pi3" "pi4")
PI_USER="crnls"
REMOTE_BIN="/home/crnls/dlockss-linux-arm64"
IPFS_REPO="/home/crnls/pi_data/ipfs_repo"
LOG_DIR="/home/crnls/pi_data"
LOG_ROTATE_HOURS=48
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

    echo "  Ensuring IPFS repo exists..."
    PARENT_DIR="${IPFS_REPO%/*}"
    ssh "${PI_USER}@${HOST}" "mkdir -p '${PARENT_DIR}' && if [ ! -f '${IPFS_REPO}/config' ]; then IPFS_PATH='${IPFS_REPO}' /usr/local/bin/ipfs init && echo 'IPFS repo initialized.'; fi"
    echo "  Migrating IPFS repo if needed..."
    ssh "${PI_USER}@${HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs repo migrate 2>&1 || true"

    echo "  Disabling Kubo AutoTLS (avoids cert errors when Pi is not publicly dialable)..."
    ssh "${PI_USER}@${HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json AutoTLS.Enabled false 2>/dev/null || true"
    echo "  Disabling Kubo WebRTC Direct (avoids webrtc-transport-pion errors behind NAT)..."
    ssh "${PI_USER}@${HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.Transports.Network.WebRTCDirect false 2>/dev/null || true"

    echo "  Applying low-memory Kubo settings for 1GB Pi (reduce OOM risk)..."
    ssh "${PI_USER}@${HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ConnMgr.HighWater 60 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ConnMgr.LowWater 30 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ResourceMgr.Enabled true 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ResourceMgr.MaxMemory '150MB' 2>/dev/null; true" || true

    echo "  Ensuring zram (compressed swap) for 1GB Pi..."
    ssh "${PI_USER}@${HOST}" "command -v zramctl &>/dev/null || (sudo apt-get update -qq 2>/dev/null && sudo apt-get install -y zram-tools 2>/dev/null); (sudo systemctl enable zram-tools 2>/dev/null; sudo systemctl start zram-tools 2>/dev/null); true" || true

    echo "  Disabling unneeded services to free RAM (ModemManager, Bluetooth)..."
    ssh "${PI_USER}@${HOST}" "sudo systemctl stop ModemManager bluetooth 2>/dev/null; sudo systemctl disable ModemManager bluetooth 2>/dev/null; true" || true

    echo "  Installing log rotation (truncate every ${LOG_ROTATE_HOURS}h)..."
    ssh "${PI_USER}@${HOST}" "mkdir -p '${LOG_DIR}' && cat > '${LOG_DIR}/rotate_logs.sh' << ROTATE_EOF
#!/bin/bash
# Truncate D-LOCKSS/IPFS logs every ${LOG_ROTATE_HOURS}h. Run from cron every 12h.
LOG_DIR='${LOG_DIR}'
MARKER=\"\\\${LOG_DIR}/.last_log_rotate\"
AGE_SEC=\$(( ${LOG_ROTATE_HOURS} * 3600 ))
now=\$(date +%s)
last=\$(stat -c %Y \"\$MARKER\" 2>/dev/null || echo 0)
if [ ! -f \"\$MARKER\" ] || [ \$(( now - last )) -ge \$AGE_SEC ]; then
  [ -f \"\\\${LOG_DIR}/dlockss.log\" ] && truncate -s 0 \"\\\${LOG_DIR}/dlockss.log\"
  [ -f \"\\\${LOG_DIR}/ipfs.log\" ] && truncate -s 0 \"\\\${LOG_DIR}/ipfs.log\"
  touch \"\$MARKER\"
fi
ROTATE_EOF
chmod +x '${LOG_DIR}/rotate_logs.sh'"
    ssh "${PI_USER}@${HOST}" "(crontab -l 2>/dev/null | grep -v rotate_logs.sh; echo '0 */12 * * * ${LOG_DIR}/rotate_logs.sh') | crontab -" 2>/dev/null || true

    echo "  Starting IPFS..."
    ssh "${PI_USER}@${HOST}" "tmux new-session -d -s ipfs 'IPFS_PATH=${IPFS_REPO} ipfs daemon --enable-gc 2>&1 | tee -a ${LOG_DIR}/ipfs.log'"
    echo "  Waiting for IPFS API (15s)..."
    sleep 15

    # Verify IPFS
    if ssh "${PI_USER}@${HOST}" "IPFS_PATH=${IPFS_REPO} ipfs id &>/dev/null"; then
        echo "  IPFS: OK"
    else
        echo "  IPFS: FAILED - last output:"
        ssh "${PI_USER}@${HOST}" "tail -20 ${LOG_DIR}/ipfs.log 2>/dev/null" || true
        echo "  Skipping D-LOCKSS on ${HOST}"
        continue
    fi

    echo "  Starting D-LOCKSS..."
    ssh "${PI_USER}@${HOST}" "tmux new-session -d -s dlockss 'GODEBUG=madvdontneed=1 GOMEMLIMIT=280MiB GOGC=50 DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING=2 DLOCKSS_MAX_CONCURRENT_CHECKS=2 DLOCKSS_NODE_NAME=${HOST} DLOCKSS_DATA_DIR=./data IPFS_PATH=${IPFS_REPO} DLOCKSS_IPFS_NODE=/ip4/127.0.0.1/tcp/5001 ${REMOTE_BIN} 2>&1 | tee -a ${LOG_DIR}/dlockss.log'"
    sleep 3

    # Verify D-LOCKSS
    if ssh "${PI_USER}@${HOST}" "pgrep -f dlockss-linux-arm64 &>/dev/null"; then
        echo "  D-LOCKSS: OK"
    else
        echo "  D-LOCKSS: FAILED - last output:"
        ssh "${PI_USER}@${HOST}" "tail -20 ${LOG_DIR}/dlockss.log 2>/dev/null" || true
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
echo "  Logs (persistent, truncated every ${LOG_ROTATE_HOURS}h):"
echo "    ${LOG_DIR}/dlockss.log   ${LOG_DIR}/ipfs.log"
echo "  Attach to tmux:"
echo "    ssh ${PI_USER}@<host> 'tmux attach -t dlockss'   # D-LOCKSS"
echo "    ssh ${PI_USER}@<host> 'tmux attach -t ipfs'     # IPFS"
