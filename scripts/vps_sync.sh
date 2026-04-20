#!/bin/bash
# VPS Sync Script — Switch trading from Mac to VPS (or back)
# Usage:
#   ./scripts/vps_sync.sh to-vps    — stop local, sync, start VPS
#   ./scripts/vps_sync.sh to-local  — stop VPS, sync back, start local
#   ./scripts/vps_sync.sh sync-only — just sync DB + code (no start/stop)

VPS="root@38.180.200.248"
VPS_PASS="47tH2U3qiT"
VPS_DIR="/root/fortix"
LOCAL_DIR="/Users/williamstorm/Documents/Trading (OKX) 1h"
SSH="sshpass -p $VPS_PASS ssh -o StrictHostKeyChecking=no $VPS"
SCP="sshpass -p $VPS_PASS"

sync_to_vps() {
    echo "📦 Syncing code..."
    $SCP rsync -avz --delete \
        --exclude 'data/' --exclude 'logs/' --exclude '.git/' \
        --exclude '__pycache__/' --exclude '*.pyc' --exclude 'node_modules/' \
        "$LOCAL_DIR/" "$VPS:$VPS_DIR/" 2>&1 | tail -3

    echo "📦 Syncing .env..."
    $SCP scp "$LOCAL_DIR/.env" "$VPS:$VPS_DIR/.env"

    echo "📦 Syncing DB (this takes ~1-2 min)..."
    $SCP rsync -avz --progress \
        "$LOCAL_DIR/data/crypto/market.db" \
        "$VPS:$VPS_DIR/data/crypto/" 2>&1 | tail -3

    echo "📦 Syncing models..."
    $SCP rsync -avz \
        "$LOCAL_DIR/data/crypto/models_4h/" \
        "$VPS:$VPS_DIR/data/crypto/models_4h/" 2>&1 | tail -3

    $SCP rsync -avz \
        "$LOCAL_DIR/data/crypto/coin_optimization/" \
        "$VPS:$VPS_DIR/data/crypto/coin_optimization/" 2>&1 | tail -3
}

sync_from_vps() {
    echo "📦 Syncing DB back from VPS..."
    $SCP rsync -avz --progress \
        "$VPS:$VPS_DIR/data/crypto/market.db" \
        "$LOCAL_DIR/data/crypto/" 2>&1 | tail -3
}

case "$1" in
    to-vps)
        echo "🔄 Switching to VPS..."
        echo "⏹  Stopping local PM2..."
        cd "$LOCAL_DIR" && pm2 stop fortix-bybit 2>/dev/null
        sleep 3

        sync_to_vps

        echo "▶️  Starting VPS PM2..."
        $SSH "cd $VPS_DIR && pm2 start ecosystem.config.js 2>/dev/null || pm2 restart fortix-bybit 2>/dev/null"
        sleep 5
        $SSH "pm2 list"
        echo "✅ Trading on VPS"
        ;;

    to-local)
        echo "🔄 Switching to Local..."
        echo "⏹  Stopping VPS PM2..."
        $SSH "cd $VPS_DIR && pm2 stop fortix-bybit 2>/dev/null"
        sleep 3

        sync_from_vps

        echo "▶️  Starting local PM2..."
        cd "$LOCAL_DIR" && pm2 restart fortix-bybit
        sleep 5
        pm2 list
        echo "✅ Trading on Local"
        ;;

    sync-only)
        echo "🔄 Syncing without start/stop..."
        sync_to_vps
        echo "✅ Sync complete"
        ;;

    *)
        echo "Usage: $0 {to-vps|to-local|sync-only}"
        exit 1
        ;;
esac
