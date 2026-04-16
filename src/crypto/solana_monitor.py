"""
FORTIX Solana Ecosystem Monitor
================================

Monitors Solana network health, DEX activity, token trends,
and whale movements for trading signal generation.

Data Sources (all free):
  - DexScreener: all DEX pairs, volume, new tokens
  - DeFi Llama: protocol TVL, yields
  - Solana RPC: TPS, network load
  - Birdeye: trending tokens (when API key available)

Tracked Solana tokens for MEXC futures:
  SOL, JUP, RAY, WIF, PYTH, JTO, BOME, POPCAT, MEW, RENDER,
  TNSR, W, HNT, ORCA, KMNO, DRIFT
"""

import os
import time
import json
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

# Solana tokens available on MEXC futures
SOLANA_FUTURES_TOKENS = [
    'SOL', 'JUP', 'RAY', 'WIF', 'PYTH', 'JTO', 'TNSR', 'W',
    'RENDER', 'HNT', 'ORCA', 'KMNO', 'DRIFT', 'BOME', 'POPCAT', 'MEW',
]

# DexScreener token addresses (Solana SPL)
TOKEN_ADDRESSES = {
    'SOL': 'So11111111111111111111111111111111111111112',
    'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
    'RAY': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
    'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
    'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
    'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
    'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
    'BOME': 'ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82',
    'POPCAT': '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr',
    'MEW': 'MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5',
}

# Rate limiting
_last_request = 0
_MIN_INTERVAL = 0.5


def _rate_limit():
    global _last_request
    elapsed = time.time() - _last_request
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request = time.time()


def _get(url, params=None, timeout=15):
    """HTTP GET with retry."""
    for attempt in range(3):
        try:
            _rate_limit()
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 2:
                logger.warning(f"GET {url}: {e}")
                return None
            time.sleep(2)
    return None


def _init_tables():
    """Create Solana-specific tables."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS solana_network (
            date TEXT PRIMARY KEY,
            tps REAL,
            active_wallets INTEGER,
            total_tvl REAL,
            dex_volume_24h REAL,
            avg_fee REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS solana_dex_volume (
            date TEXT, token TEXT,
            volume_24h REAL, price REAL, liquidity REAL,
            price_change_24h REAL, price_change_6h REAL,
            txns_24h INTEGER, buys_24h INTEGER, sells_24h INTEGER,
            PRIMARY KEY (date, token)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS solana_trending (
            date TEXT, token TEXT, rank INTEGER,
            volume_24h REAL, price_change_24h REAL,
            market_cap REAL, holders INTEGER,
            PRIMARY KEY (date, token)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS solana_protocol_tvl (
            date TEXT, protocol TEXT, tvl REAL, tvl_change_1d REAL,
            PRIMARY KEY (date, protocol)
        )
    """)
    conn.commit()
    conn.close()


# ═══ DATA COLLECTORS ═══════════════════════════════════════

def collect_dex_volumes():
    """Collect DEX trading volume for Solana tokens from DexScreener."""
    _init_tables()
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    collected = 0

    for token, address in TOKEN_ADDRESSES.items():
        data = _get(f"https://api.dexscreener.com/latest/dex/tokens/{address}")
        if not data or 'pairs' not in data:
            continue

        # Aggregate across all DEX pairs for this token
        total_vol = 0
        best_pair = None
        total_txns = total_buys = total_sells = 0

        for pair in data['pairs']:
            if pair.get('chainId') != 'solana':
                continue
            vol = pair.get('volume', {}).get('h24', 0) or 0
            total_vol += vol
            total_txns += pair.get('txns', {}).get('h24', {}).get('buys', 0) + \
                         pair.get('txns', {}).get('h24', {}).get('sells', 0)
            total_buys += pair.get('txns', {}).get('h24', {}).get('buys', 0)
            total_sells += pair.get('txns', {}).get('h24', {}).get('sells', 0)

            if not best_pair or vol > (best_pair.get('volume', {}).get('h24', 0) or 0):
                best_pair = pair

        if best_pair:
            price = float(best_pair.get('priceUsd', 0) or 0)
            liq = float(best_pair.get('liquidity', {}).get('usd', 0) or 0)
            chg_24h = float(best_pair.get('priceChange', {}).get('h24', 0) or 0)
            chg_6h = float(best_pair.get('priceChange', {}).get('h6', 0) or 0)

            conn.execute(
                "INSERT OR REPLACE INTO solana_dex_volume VALUES (?,?,?,?,?,?,?,?,?,?)",
                (today, token, total_vol, price, liq, chg_24h, chg_6h,
                 total_txns, total_buys, total_sells)
            )
            collected += 1
            logger.debug(f"  {token}: vol=${total_vol:,.0f}, price=${price:.4f}")

    conn.commit()
    conn.close()
    logger.info(f"Solana DEX volumes: {collected} tokens")
    return collected


def collect_solana_tvl():
    """Collect Solana protocol TVL from DeFi Llama."""
    _init_tables()
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    collected = 0

    # Get all protocols
    data = _get("https://api.llama.fi/protocols")
    if not data:
        conn.close()
        return 0

    # Filter Solana protocols
    solana_protocols = [
        p for p in data
        if 'Solana' in (p.get('chains') or []) and (p.get('tvl') or 0) > 1_000_000
    ]
    solana_protocols.sort(key=lambda p: p.get('tvl', 0), reverse=True)

    for p in solana_protocols[:30]:
        name = p['name']
        tvl = p.get('tvl', 0)
        change_1d = p.get('change_1d', 0)

        conn.execute(
            "INSERT OR REPLACE INTO solana_protocol_tvl VALUES (?,?,?,?)",
            (today, name, tvl, change_1d)
        )
        collected += 1

    # Total Solana TVL
    chain_data = _get("https://api.llama.fi/v2/chains")
    if chain_data:
        sol_chain = next((c for c in chain_data if c.get('gecko_id') == 'solana'), None)
        if sol_chain:
            conn.execute(
                "INSERT OR REPLACE INTO solana_network (date, total_tvl) VALUES (?, ?) "
                "ON CONFLICT(date) DO UPDATE SET total_tvl=excluded.total_tvl",
                (today, sol_chain['tvl'])
            )

    conn.commit()
    conn.close()
    logger.info(f"Solana TVL: {collected} protocols, top: {solana_protocols[0]['name'] if solana_protocols else '?'}")
    return collected


def collect_network_stats():
    """Collect Solana network stats (TPS, fees)."""
    _init_tables()
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Solana TPS from public RPC
    try:
        resp = requests.post(
            "https://api.mainnet-beta.solana.com",
            json={"jsonrpc": "2.0", "id": 1, "method": "getRecentPerformanceSamples", "params": [1]},
            timeout=15
        )
        data = resp.json()
        if 'result' in data and data['result']:
            sample = data['result'][0]
            tps = sample.get('numTransactions', 0) / max(sample.get('samplePeriodSecs', 1), 1)

            conn.execute(
                "INSERT OR REPLACE INTO solana_network (date, tps) VALUES (?, ?) "
                "ON CONFLICT(date) DO UPDATE SET tps=excluded.tps",
                (today, tps)
            )
            logger.info(f"Solana TPS: {tps:.0f}")
    except Exception as e:
        logger.warning(f"Solana RPC failed: {e}")

    conn.commit()
    conn.close()


def detect_volume_anomalies() -> list:
    """Detect tokens with abnormal volume spikes (potential trading opportunities)."""
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    anomalies = []

    for token in SOLANA_FUTURES_TOKENS:
        # Get recent volume history
        rows = conn.execute(
            "SELECT date, volume_24h, price, price_change_24h "
            "FROM solana_dex_volume WHERE token=? ORDER BY date DESC LIMIT 8",
            (token,)
        ).fetchall()

        if len(rows) < 3:
            continue

        current_vol = rows[0][1] if rows[0][1] else 0
        avg_vol = sum(r[1] for r in rows[1:] if r[1]) / max(len(rows) - 1, 1)
        price_change = rows[0][3] or 0

        if avg_vol > 0 and current_vol > avg_vol * 2.0:
            ratio = current_vol / avg_vol
            anomalies.append({
                'token': token,
                'volume_24h': current_vol,
                'volume_ratio': ratio,
                'avg_volume': avg_vol,
                'price_change_24h': price_change,
                'signal': 'VOLUME_SPIKE',
                'description': f"{token}: volume {ratio:.1f}x normal (${current_vol:,.0f} vs avg ${avg_vol:,.0f}), "
                              f"price {price_change:+.1f}%",
            })

    conn.close()
    anomalies.sort(key=lambda a: a['volume_ratio'], reverse=True)
    return anomalies


def detect_tvl_momentum() -> list:
    """Detect protocols with TVL growth (capital flowing in)."""
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    movers = []
    rows = conn.execute(
        "SELECT protocol, tvl, tvl_change_1d FROM solana_protocol_tvl "
        "WHERE date=? AND tvl > 5000000 ORDER BY tvl_change_1d DESC",
        (today,)
    ).fetchall()

    for r in rows:
        if r[2] and abs(r[2]) > 5:  # >5% change
            movers.append({
                'protocol': r[0],
                'tvl': r[1],
                'change_1d': r[2],
                'signal': 'TVL_SURGE' if r[2] > 0 else 'TVL_DROP',
            })

    conn.close()
    return movers


# ═══ MAIN COLLECTOR ════════════════════════════════════════

def collect_all_solana() -> dict:
    """Run all Solana data collectors."""
    logger.info("Collecting Solana ecosystem data...")
    results = {}

    try:
        results['dex_volumes'] = collect_dex_volumes()
    except Exception as e:
        logger.error(f"DEX volumes failed: {e}")
        results['dex_volumes'] = 0

    try:
        results['tvl'] = collect_solana_tvl()
    except Exception as e:
        logger.error(f"TVL failed: {e}")
        results['tvl'] = 0

    try:
        collect_network_stats()
        results['network'] = 1
    except Exception as e:
        logger.error(f"Network stats failed: {e}")
        results['network'] = 0

    total = sum(results.values())
    logger.info(f"Solana collection done: {total} items")
    return results


def get_solana_context() -> str:
    """Build Solana ecosystem context for signal generation."""
    conn = sqlite3.connect(str(DB_PATH))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = ["=== SOLANA ECOSYSTEM ==="]

    # Network stats
    net = conn.execute(
        "SELECT tps, total_tvl, dex_volume_24h FROM solana_network WHERE date=?", (today,)
    ).fetchone()
    if net:
        if net[0]: lines.append(f"  Network TPS: {net[0]:,.0f}")
        if net[1]: lines.append(f"  Total TVL: ${net[1]/1e9:.2f}B")

    # Top volume tokens
    vols = conn.execute(
        "SELECT token, volume_24h, price, price_change_24h "
        "FROM solana_dex_volume WHERE date=? ORDER BY volume_24h DESC LIMIT 5",
        (today,)
    ).fetchall()
    if vols:
        lines.append("  Top DEX volume:")
        for v in vols:
            lines.append(f"    {v[0]:8s}: ${v[1]:>12,.0f} vol, ${v[2]:.4f}, {v[3]:+.1f}%")

    # Volume anomalies
    anomalies = detect_volume_anomalies()
    if anomalies:
        lines.append(f"  Volume anomalies ({len(anomalies)}):")
        for a in anomalies[:3]:
            lines.append(f"    {a['description']}")

    # TVL movers
    movers = detect_tvl_momentum()
    if movers:
        lines.append(f"  TVL movers:")
        for m in movers[:3]:
            lines.append(f"    {m['protocol']}: ${m['tvl']/1e6:.0f}M ({m['change_1d']:+.1f}%)")

    conn.close()
    return "\n".join(lines)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("Collecting Solana ecosystem data...\n")
    results = collect_all_solana()
    print(f"\nResults: {results}")

    print("\n" + get_solana_context())

    print("\nVolume anomalies:")
    for a in detect_volume_anomalies():
        print(f"  {a['description']}")
