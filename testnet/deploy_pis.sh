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
# File swap size on Pi (GB). 4G gives good OOM headroom on a 64GB SD card.
PI_SWAP_GB="${PI_SWAP_GB:-4}"

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
if curl -fSL --progress-bar -o "$KUBO_TMP" \
    "https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/kubo_v${KUBO_VERSION}_linux-arm64.tar.gz"; then
    KUBO_DOWNLOADED=true
    echo "Done"
else
    KUBO_DOWNLOADED=false
    echo "WARNING: Kubo download failed (mirror may be down). Will skip Kubo update if already installed."
fi

declare -A HOST_OVERRIDE
for HOST in "${PI_HOSTS[@]}"; do
    if ! getent hosts "$HOST" &>/dev/null; then
        echo "WARNING: Cannot resolve hostname '${HOST}'"
        read -rp "  Enter IP address for ${HOST} (or press Enter to skip): " IP_ADDR
        if [ -z "$IP_ADDR" ]; then
            echo "  Skipping ${HOST}"
            continue
        fi
        HOST_OVERRIDE["$HOST"]="$IP_ADDR"
        echo "  Using ${IP_ADDR} for ${HOST}"
    fi
done

for HOST in "${PI_HOSTS[@]}"; do
    NODE_NAME="${HOST}"
    CONN_HOST="${HOST_OVERRIDE[$HOST]:-$HOST}"
    echo "=== ${NODE_NAME} (${CONN_HOST}) ==="

    echo "  Stopping old processes..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo systemctl stop dlockss.service 2>/dev/null; sudo systemctl stop ipfs.service 2>/dev/null; tmux kill-session -t dlockss 2>/dev/null; tmux kill-session -t ipfs 2>/dev/null; pkill -f dlockss-linux-arm64 2>/dev/null; pkill -f 'ipfs daemon' 2>/dev/null; sleep 2; pkill -9 -f dlockss-linux-arm64 2>/dev/null; pkill -9 -f 'ipfs daemon' 2>/dev/null; rm -f ${IPFS_REPO}/repo.lock 2>/dev/null; true" || true

    echo "  Copying D-LOCKSS binary..."
    scp -q "$BIN" "${PI_USER}@${CONN_HOST}:${REMOTE_BIN}"
    ssh "${PI_USER}@${CONN_HOST}" "chmod 755 ${REMOTE_BIN}"

    if [ "$KUBO_DOWNLOADED" = true ]; then
        echo "  Updating Kubo..."
        scp -q "$KUBO_TMP" "${PI_USER}@${CONN_HOST}:/tmp/kubo.tar.gz"
        ssh "${PI_USER}@${CONN_HOST}" "cd /tmp && tar xzf kubo.tar.gz && sudo mv kubo/ipfs /usr/local/bin/ipfs && sudo chmod 755 /usr/local/bin/ipfs && rm -rf /tmp/kubo /tmp/kubo.tar.gz && ipfs --version" || true
    else
        REMOTE_VER=$(ssh "${PI_USER}@${CONN_HOST}" "ipfs --version 2>/dev/null" || echo "none")
        echo "  Kubo download skipped; remote version: ${REMOTE_VER}"
    fi

    echo "  Ensuring IPFS repo exists..."
    PARENT_DIR="${IPFS_REPO%/*}"
    ssh "${PI_USER}@${CONN_HOST}" "mkdir -p '${PARENT_DIR}' && if [ ! -f '${IPFS_REPO}/config' ]; then IPFS_PATH='${IPFS_REPO}' /usr/local/bin/ipfs init && echo 'IPFS repo initialized.'; fi"
    echo "  Migrating IPFS repo if needed..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs repo migrate 2>&1 || true"

    echo "  Disabling Kubo AutoTLS (avoids cert errors when Pi is not publicly dialable)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json AutoTLS.Enabled false 2>/dev/null || true"
    echo "  Disabling Kubo WebRTC Direct (avoids webrtc-transport-pion errors behind NAT)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.Transports.Network.WebRTCDirect false 2>/dev/null || true"

    echo "  Applying low-memory Kubo settings for 1GB Pi (reduce OOM risk)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ConnMgr.HighWater 40 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ConnMgr.LowWater 20 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ConnMgr.GracePeriod '\"30s\"' 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ResourceMgr.Enabled true 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.ResourceMgr.MaxMemory '\"120MB\"' 2>/dev/null; true" || true

    echo "  Configuring Kubo reprovider (pinned strategy, 6h DHT refresh interval)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config Provide.Strategy pinned 2>/dev/null; IPFS_PATH='${IPFS_REPO}' ipfs config Provide.DHT.Interval '\"6h\"' 2>/dev/null; true" || true
    echo "  Removing deprecated Reprovider config (Kubo 0.39+ uses Provide)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && python3 -c \"
import json, sys
p = '${IPFS_REPO}/config'
with open(p) as f: c = json.load(f)
if 'Reprovider' in c:
    del c['Reprovider']
    with open(p, 'w') as f: json.dump(c, f, indent=2)
    print('  Removed Reprovider section')
else:
    print('  No Reprovider section found (OK)')
\" 2>/dev/null; true" || true

    echo "  Enabling Kubo relay client (so Pis behind NAT are reachable via relay)..."
    ssh "${PI_USER}@${CONN_HOST}" "test -f '${IPFS_REPO}/config' && IPFS_PATH='${IPFS_REPO}' ipfs config --json Swarm.RelayClient.Enabled true 2>/dev/null; true" || true

    echo "  Enabling cgroup memory controller (needed for systemd MemoryMax)..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo bash -s" << 'CGROUP_EOF'
CMDLINE=/boot/firmware/cmdline.txt
if [ ! -f "$CMDLINE" ]; then CMDLINE=/boot/cmdline.txt; fi
if [ -f "$CMDLINE" ] && grep -q 'cgroup_disable=memory' "$CMDLINE"; then
  sed -i 's/ cgroup_disable=memory//g' "$CMDLINE"
  echo "    Removed cgroup_disable=memory from $CMDLINE (reboot needed for MemoryMax to work)"
else
  echo "    cgroup memory already enabled (OK)"
fi
CGROUP_EOF

    echo "  Ensuring zram (compressed swap) for 1GB Pi..."
    ssh "${PI_USER}@${CONN_HOST}" "command -v zramctl &>/dev/null || (sudo apt-get update -qq 2>/dev/null && sudo apt-get install -y zram-tools 2>/dev/null); (sudo systemctl enable zram-tools 2>/dev/null; sudo systemctl start zram-tools 2>/dev/null); true" || true

    echo "  Ensuring file swap /swapfile (${PI_SWAP_GB}G) for extra OOM headroom..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo bash -s" << SWAP_EOF
SWAPFILE=/swapfile
SIZE=${PI_SWAP_GB}G
DD_MB=$(( PI_SWAP_GB * 1024 ))
if [ ! -f "\$SWAPFILE" ]; then
  fallocate -l \$SIZE "\$SWAPFILE" 2>/dev/null || dd if=/dev/zero of="\$SWAPFILE" bs=1M count=\$DD_MB status=none
  chmod 600 "\$SWAPFILE"
  mkswap "\$SWAPFILE" >/dev/null
  swapon "\$SWAPFILE" && echo "    Created and enabled \$SWAPFILE (\$SIZE)"
  grep -q "^\${SWAPFILE} " /etc/fstab 2>/dev/null || echo "\$SWAPFILE none swap defaults 0 0" | tee -a /etc/fstab >/dev/null
elif ! swapon --show 2>/dev/null | grep -q "\$SWAPFILE"; then
  swapon "\$SWAPFILE" 2>/dev/null && echo "    Enabled existing \$SWAPFILE" || true
fi
SWAP_EOF

    echo "  Disabling unneeded services to free RAM (ModemManager, Bluetooth)..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo systemctl stop ModemManager bluetooth 2>/dev/null; sudo systemctl disable ModemManager bluetooth 2>/dev/null; true" || true

    echo "  Installing log rotation (truncate every ${LOG_ROTATE_HOURS}h)..."
    ssh "${PI_USER}@${CONN_HOST}" "mkdir -p '${LOG_DIR}' && cat > '${LOG_DIR}/rotate_logs.sh' << ROTATE_EOF
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
    ssh "${PI_USER}@${CONN_HOST}" "(crontab -l 2>/dev/null | grep -v rotate_logs.sh; echo '0 */12 * * * ${LOG_DIR}/rotate_logs.sh') | crontab -" 2>/dev/null || true

    echo "  Installing systemd services (auto-start on boot)..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo bash -s" << SYSTEMD_EOF
cat > /etc/systemd/system/ipfs.service << 'UNIT'
[Unit]
Description=IPFS Daemon (Kubo)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${PI_USER}
Environment=IPFS_PATH=${IPFS_REPO}
Environment=GOMEMLIMIT=180MiB
Environment=GOGC=50
ExecStart=/usr/local/bin/ipfs daemon --enable-gc
Restart=on-failure
RestartSec=10
MemoryMax=300M
MemoryHigh=250M
StandardOutput=append:${LOG_DIR}/ipfs.log
StandardError=append:${LOG_DIR}/ipfs.log
OOMScoreAdjust=200

[Install]
WantedBy=multi-user.target
UNIT

cat > /etc/systemd/system/dlockss.service << 'UNIT'
[Unit]
Description=D-LOCKSS Node
After=ipfs.service
Requires=ipfs.service

[Service]
Type=simple
User=${PI_USER}
WorkingDirectory=/home/${PI_USER}
Environment=GODEBUG=madvdontneed=1
Environment=GOMEMLIMIT=220MiB
Environment=GOGC=50
Environment=DLOCKSS_MAX_CONCURRENT_FILE_PROCESSING=2
Environment=DLOCKSS_MAX_CONCURRENT_CHECKS=2
Environment=DLOCKSS_NODE_NAME=${NODE_NAME}
Environment=DLOCKSS_DATA_DIR=./data
Environment=DLOCKSS_IPFS_CONFIG=${IPFS_REPO}/config
Environment=DLOCKSS_IPFS_NODE=/ip4/127.0.0.1/tcp/5001
ExecStartPre=/bin/bash -c 'for i in \$(seq 1 30); do IPFS_PATH=${IPFS_REPO} /usr/local/bin/ipfs swarm peers >/dev/null 2>&1 && exit 0; sleep 3; done; exit 1'
ExecStart=${REMOTE_BIN}
Restart=on-failure
RestartSec=15
MemoryMax=280M
MemoryHigh=230M
StandardOutput=append:${LOG_DIR}/dlockss.log
StandardError=append:${LOG_DIR}/dlockss.log
OOMScoreAdjust=-200

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable ipfs.service dlockss.service
SYSTEMD_EOF

    echo "  Stopping old tmux sessions (migrating to systemd)..."
    ssh "${PI_USER}@${CONN_HOST}" "tmux kill-session -t dlockss 2>/dev/null; tmux kill-session -t ipfs 2>/dev/null; true"

    echo "  Starting IPFS via systemd..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo systemctl restart ipfs.service"
    echo "  Waiting for IPFS API to become ready (up to 90s)..."
    IPFS_READY=false
    for attempt in $(seq 1 30); do
        if ssh "${PI_USER}@${CONN_HOST}" "IPFS_PATH=${IPFS_REPO} ipfs swarm peers &>/dev/null"; then
            IPFS_READY=true
            echo "  IPFS: OK (ready after ~$((attempt * 3))s)"
            break
        fi
        sleep 3
    done
    if [ "$IPFS_READY" != true ]; then
        echo "  IPFS: FAILED to become ready after 90s - last output:"
        ssh "${PI_USER}@${CONN_HOST}" "tail -20 ${LOG_DIR}/ipfs.log 2>/dev/null" || true
        echo "  Skipping D-LOCKSS on ${NODE_NAME}"
        continue
    fi

    echo "  Starting D-LOCKSS via systemd..."
    ssh "${PI_USER}@${CONN_HOST}" "sudo systemctl restart dlockss.service"
    sleep 3

    # Verify D-LOCKSS
    if ssh "${PI_USER}@${CONN_HOST}" "pgrep -f dlockss-linux-arm64 &>/dev/null"; then
        echo "  D-LOCKSS: OK"
    else
        echo "  D-LOCKSS: FAILED - last output:"
        ssh "${PI_USER}@${CONN_HOST}" "tail -20 ${LOG_DIR}/dlockss.log 2>/dev/null" || true
    fi
done

echo ""
echo "=== Summary ==="
for HOST in "${PI_HOSTS[@]}"; do
    CONN_HOST="${HOST_OVERRIDE[$HOST]:-$HOST}"
    IPFS_OK=$(ssh "${PI_USER}@${CONN_HOST}" "pgrep -f 'ipfs daemon' &>/dev/null && echo OK || echo DOWN") || IPFS_OK="UNREACHABLE"
    DLOCKSS_OK=$(ssh "${PI_USER}@${CONN_HOST}" "pgrep -f dlockss-linux-arm64 &>/dev/null && echo OK || echo DOWN") || DLOCKSS_OK="UNREACHABLE"
    printf "  %-8s ipfs=%-4s dlockss=%s\n" "$HOST" "$IPFS_OK" "$DLOCKSS_OK"
done
echo ""
echo "  Logs (persistent, truncated every ${LOG_ROTATE_HOURS}h):"
echo "    ${LOG_DIR}/dlockss.log   ${LOG_DIR}/ipfs.log"
echo "  Manage services:"
echo "    ssh ${PI_USER}@<host> 'sudo systemctl status dlockss'   # D-LOCKSS status"
echo "    ssh ${PI_USER}@<host> 'sudo systemctl status ipfs'      # IPFS status"
echo "    ssh ${PI_USER}@<host> 'sudo journalctl -u dlockss -f'   # live D-LOCKSS logs"
