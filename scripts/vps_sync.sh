#!/bin/bash
# VPS Sync Script — Switch trading from Mac to VPS (or back)
# Usage:
#   ./scripts/vps_sync.sh to-vps    — stop local, sync, start VPS
#   ./scripts/vps_sync.sh to-local  — stop VPS, sync back, start local
#   ./scripts/vps_sync.sh sync-only — just sync code+DB to VPS (no start/stop)
#   ./scripts/vps_sync.sh status    — check VPS status

VPS_HOST="38.180.200.248"
VPS_PASS="47tH2U3qiT"
VPS_DIR="/root/fortix"
LOCAL_DIR="/Users/williamstorm/Documents/Trading (OKX) 1h"

ssh_cmd() {
    sshpass -p "$VPS_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "root@$VPS_HOST" "$@"
}

rsync_cmd() {
    sshpass -p "$VPS_PASS" rsync "$@"
}

sync_to_vps() {
    echo "📦 Syncing code (excluding ecosystem.config.js, data, logs)..."
    rsync_cmd -avz \
        --exclude 'data/' --exclude 'logs/' --exclude '.git/' \
        --exclude '__pycache__/' --exclude '*.pyc' --exclude 'node_modules/' \
        --exclude 'ecosystem.config.js' \
        "$LOCAL_DIR/" "root@$VPS_HOST:$VPS_DIR/" 2>&1 | tail -3

    echo "📦 Syncing .env..."
    sshpass -p "$VPS_PASS" scp -o StrictHostKeyChecking=no \
        "$LOCAL_DIR/.env" "root@$VPS_HOST:$VPS_DIR/.env" 2>&1

    echo "📦 Syncing DB (~1-2 min for delta)..."
    rsync_cmd -avz --progress \
        "$LOCAL_DIR/data/crypto/market.db" \
        "root@$VPS_HOST:$VPS_DIR/data/crypto/" 2>&1 | tail -5

    echo "📦 Syncing models + optimization..."
    rsync_cmd -avz \
        "$LOCAL_DIR/data/crypto/models_4h/" \
        "root@$VPS_HOST:$VPS_DIR/data/crypto/models_4h/" 2>&1 | tail -2
    rsync_cmd -avz \
        "$LOCAL_DIR/data/crypto/coin_optimization/" \
        "root@$VPS_HOST:$VPS_DIR/data/crypto/coin_optimization/" 2>&1 | tail -2
}

sync_from_vps() {
    echo "📦 Syncing DB back from VPS..."
    rsync_cmd -avz --progress \
        "root@$VPS_HOST:$VPS_DIR/data/crypto/market.db" \
        "$LOCAL_DIR/data/crypto/" 2>&1 | tail -5

    echo "📦 Syncing logs from VPS..."
    rsync_cmd -avz \
        "root@$VPS_HOST:$VPS_DIR/logs/" \
        "$LOCAL_DIR/logs/vps_logs/" 2>&1 | tail -2
}

check_vps_running() {
    local status
    status=$(ssh_cmd "pm2 jlist 2>/dev/null" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    for p in d:
        if p.get('name')=='fortix-bybit':
            s=p.get('pm2_env',{}).get('status','unknown')
            print(s.strip())
            sys.exit(0)
    print('not_found')
except: print('error')
" 2>/dev/null)
    echo "$status"
}

case "$1" in
    to-vps)
        echo "🔄 Switching to VPS..."

        # Safety: check VPS is reachable
        if ! ssh_cmd "echo ok" >/dev/null 2>&1; then
            echo "❌ VPS not reachable! Aborting."
            exit 1
        fi

        echo "⏹  Stopping local PM2..."
        cd "$LOCAL_DIR" && pm2 stop fortix-bybit 2>/dev/null
        sleep 3

        sync_to_vps

        echo "▶️  Starting VPS PM2..."
        ssh_cmd "pm2 restart fortix-bybit 2>/dev/null || pm2 start /root/fortix/ecosystem.config.js"
        sleep 10

        # Verify
        VPS_STATUS=$(check_vps_running)
        if echo "$VPS_STATUS" | grep -q "online"; then
            ssh_cmd "pm2 list"
            echo "✅ Trading on VPS"
        else
            echo "❌ VPS failed to start (status: $VPS_STATUS). Starting local back..."
            cd "$LOCAL_DIR" && pm2 restart fortix-bybit
            exit 1
        fi
        ;;

    to-local)
        echo "🔄 Switching to Local..."

        echo "⏹  Stopping VPS PM2..."
        ssh_cmd "pm2 stop fortix-bybit 2>/dev/null"
        sleep 3

        sync_from_vps

        echo "▶️  Starting local PM2..."
        cd "$LOCAL_DIR" && pm2 restart fortix-bybit
        sleep 5
        pm2 list
        echo "✅ Trading on Local"
        ;;

    sync-only)
        echo "🔄 Syncing to VPS (no start/stop)..."
        sync_to_vps
        echo "✅ Sync complete"
        ;;

    status)
        echo "📊 VPS Status:"
        ssh_cmd "pm2 list && echo '---' && pm2 logs fortix-bybit --lines 5 --nostream 2>/dev/null"
        ;;

    *)
        echo "Usage: $0 {to-vps|to-local|sync-only|status}"
        echo ""
        echo "  to-vps    — stop Mac, sync all, start VPS"
        echo "  to-local  — stop VPS, sync DB back, start Mac"
        echo "  sync-only — sync code+DB to VPS without stopping"
        echo "  status    — check VPS PM2 status + last logs"
        exit 1
        ;;
esac
