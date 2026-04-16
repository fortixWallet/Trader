"""
CoinGlass Historical Backfill — ALL available history for derivatives data.

We're already paying $29/mo (Hobbyist) but only collecting snapshots.
This script backfills ALL-TIME historical daily data for:
  1. Aggregated Open Interest
  2. Funding Rates
  3. Aggregated Liquidations
  4. Long/Short Account Ratios
  5. Taker Buy/Sell Volume
  6. Exchange Balance history

Rate limit: 30 req/min = 2.5s between requests
Max 4500 entries per request at 1d interval = ~12 years per call
Estimated time: ~10-15 minutes for all coins × all endpoints

Usage:
    python -m src.crypto.backfill_coinglass
    python -m src.crypto.backfill_coinglass --oi       # only OI
    python -m src.crypto.backfill_coinglass --funding   # only funding
"""

import os
import sys
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('cg_backfill')

DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'market.db'
API_KEY = os.getenv('COINGLASS_API_KEY', '')
BASE_URL = 'https://open-api-v4.coinglass.com'

# All coins to backfill
COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
    'DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK',
    'UNI', 'AAVE', 'MKR', 'LDO', 'CRV',
    'FET', 'RENDER', 'TAO', 'ARB', 'OP', 'POL',
]

session = requests.Session()
session.headers['CG-API-KEY'] = API_KEY
session.headers['accept'] = 'application/json'
_last_req = 0


def _rate_limit():
    global _last_req
    elapsed = time.time() - _last_req
    if elapsed < 2.5:
        time.sleep(2.5 - elapsed)
    _last_req = time.time()


def _get(path, params=None):
    _rate_limit()
    url = f"{BASE_URL}{path}"
    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            log.warning("  Rate limited, waiting 10s...")
            time.sleep(10)
            resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            log.warning(f"  HTTP {resp.status_code} for {path} ({params})")
            return None
        data = resp.json()
        if data.get('code') != '0' and data.get('code') != 0:
            log.warning(f"  API error: {data.get('msg', 'unknown')}")
            return None
        return data.get('data')
    except Exception as e:
        log.error(f"  Request error: {e}")
        return None


# Symbol mapping: coin → Binance futures symbol
FUTURES_MAP = {
    'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT',
    'BNB': 'BNBUSDT', 'XRP': 'XRPUSDT', 'ADA': 'ADAUSDT',
    'AVAX': 'AVAXUSDT', 'DOT': 'DOTUSDT', 'LINK': 'LINKUSDT',
    'DOGE': 'DOGEUSDT', 'SHIB': 'SHIBUSDT',
    'UNI': 'UNIUSDT', 'AAVE': 'AAVEUSDT', 'MKR': 'MKRUSDT',
    'LDO': 'LDOUSDT', 'CRV': 'CRVUSDT',
    'FET': 'FETUSDT', 'ARB': 'ARBUSDT', 'OP': 'OPUSDT',
    'PEPE': '1000PEPEUSDT', 'BONK': '1000BONKUSDT',
    'WIF': 'WIFUSDT', 'RENDER': 'RENDERUSDT', 'TAO': 'TAOUSDT',
    'POL': 'POLUSDT',
}


# ═══════════════════════════════════════════════════════════════
# 1. AGGREGATED OPEN INTEREST HISTORY
# ═══════════════════════════════════════════════════════════════

def backfill_oi_history():
    """Fetch OI history (daily) from Binance via CoinGlass."""
    log.info("=" * 60)
    log.info("BACKFILL: CoinGlass OI History (daily)")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cg_oi_history (
            coin TEXT,
            timestamp INTEGER,
            oi_open REAL,
            oi_high REAL,
            oi_low REAL,
            oi_close REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    total = 0
    for coin in COINS:
        symbol = FUTURES_MAP.get(coin)
        if not symbol:
            continue
        data = _get('/api/futures/open-interest/history', {
            'exchange': 'Binance',
            'symbol': symbol,
            'interval': '1d',
            'limit': 4500,
        })
        if not data:
            log.info(f"  {coin}: no OI history")
            continue

        inserted = 0
        for row in data:
            ts = int(row.get('time', row.get('t', 0)))
            if ts > 1e12:
                ts = ts // 1000
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO cg_oi_history (coin, timestamp, oi_open, oi_high, oi_low, oi_close) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (coin, ts, float(row.get('open', row.get('o', 0))),
                     float(row.get('high', row.get('h', 0))),
                     float(row.get('low', row.get('l', 0))),
                     float(row.get('close', row.get('c', 0))))
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total += inserted
        if inserted > 0:
            log.info(f"  {coin}: {inserted} OI records")

    conn.close()
    log.info(f"OI DONE: {total} records")
    return total


# ═══════════════════════════════════════════════════════════════
# 2. FUNDING RATES HISTORY (OHLC)
# ═══════════════════════════════════════════════════════════════

def backfill_funding_history():
    """Fetch funding rate history (daily OHLC) from Binance via CoinGlass."""
    log.info("=" * 60)
    log.info("BACKFILL: CoinGlass Funding Rate History (daily)")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cg_funding_history (
            coin TEXT,
            timestamp INTEGER,
            fr_open REAL,
            fr_high REAL,
            fr_low REAL,
            fr_close REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    total = 0
    for coin in COINS:
        symbol = FUTURES_MAP.get(coin)
        if not symbol:
            continue
        data = _get('/api/futures/funding-rate/history', {
            'exchange': 'Binance',
            'symbol': symbol,
            'interval': '1d',
            'limit': 4500,
        })
        if not data:
            log.info(f"  {coin}: no funding history")
            continue

        inserted = 0
        for row in data:
            ts = int(row.get('time', row.get('t', 0)))
            if ts > 1e12:
                ts = ts // 1000
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO cg_funding_history (coin, timestamp, fr_open, fr_high, fr_low, fr_close) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (coin, ts, float(row.get('open', row.get('o', 0))),
                     float(row.get('high', row.get('h', 0))),
                     float(row.get('low', row.get('l', 0))),
                     float(row.get('close', row.get('c', 0))))
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total += inserted
        if inserted > 0:
            log.info(f"  {coin}: {inserted} funding records")

    conn.close()
    log.info(f"FUNDING DONE: {total} records")
    return total


# ═══════════════════════════════════════════════════════════════
# 3. AGGREGATED LIQUIDATIONS HISTORY
# ═══════════════════════════════════════════════════════════════

def backfill_liquidation_history():
    """Fetch liquidation history — skipped (endpoint returns 500 on Hobbyist)."""
    log.info("LIQUIDATION HISTORY: skipped (not available on Hobbyist plan)")
    return 0


# ═══════════════════════════════════════════════════════════════
# 4. LONG/SHORT ACCOUNT RATIO HISTORY
# ═══════════════════════════════════════════════════════════════

def backfill_ls_ratio_history():
    """Fetch global long/short account ratio history from Binance."""
    log.info("=" * 60)
    log.info("BACKFILL: CoinGlass L/S Ratio History (daily)")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cg_ls_history (
            coin TEXT,
            timestamp INTEGER,
            long_ratio REAL,
            short_ratio REAL,
            long_short_ratio REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    total = 0
    for coin in COINS:
        symbol = FUTURES_MAP.get(coin)
        if not symbol:
            continue
        data = _get('/api/futures/global-long-short-account-ratio/history', {
            'exchange': 'Binance',
            'symbol': symbol,
            'interval': '1d',
            'limit': 4500,
        })
        if not data:
            log.info(f"  {coin}: no L/S ratio history")
            continue

        inserted = 0
        rows = data if isinstance(data, list) else []
        for row in rows:
            ts = int(row.get('time', row.get('t', 0)))
            if ts > 1e12:
                ts = ts // 1000
            long_r = row.get('global_account_long_percent', row.get('longRatio', 0))
            short_r = row.get('global_account_short_percent', row.get('shortRatio', 0))
            ls_ratio = row.get('global_account_long_short_ratio', row.get('longShortRatio', 0))

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO cg_ls_history (coin, timestamp, long_ratio, short_ratio, long_short_ratio) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (coin, ts, long_r, short_r, ls_ratio)
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total += inserted
        if inserted > 0:
            log.info(f"  {coin}: {inserted} L/S records")

    conn.close()
    log.info(f"L/S RATIO DONE: {total} records")
    return total


# ═══════════════════════════════════════════════════════════════
# 5. AGGREGATED TAKER BUY/SELL VOLUME HISTORY
# ═══════════════════════════════════════════════════════════════

def backfill_taker_history():
    """Fetch taker buy/sell volume history from Binance (v2 endpoint)."""
    log.info("=" * 60)
    log.info("BACKFILL: CoinGlass Taker Buy/Sell History (daily)")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cg_taker_history (
            coin TEXT,
            timestamp INTEGER,
            buy_vol REAL,
            sell_vol REAL,
            buy_sell_ratio REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    total = 0
    for coin in COINS:
        symbol = FUTURES_MAP.get(coin)
        if not symbol:
            continue
        data = _get('/api/futures/v2/taker-buy-sell-volume/history', {
            'exchange': 'Binance',
            'symbol': symbol,
            'interval': '1d',
            'limit': 4500,
        })
        if not data:
            log.info(f"  {coin}: no taker history")
            continue

        inserted = 0
        rows = data if isinstance(data, list) else []
        for row in rows:
            ts = int(row.get('time', row.get('t', 0)))
            if ts > 1e12:
                ts = ts // 1000
            buy = float(row.get('taker_buy_volume_usd', row.get('buy', 0)) or 0)
            sell = float(row.get('taker_sell_volume_usd', row.get('sell', 0)) or 0)
            ratio = buy / sell if sell > 0 else 1.0

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO cg_taker_history (coin, timestamp, buy_vol, sell_vol, buy_sell_ratio) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (coin, ts, buy, sell, ratio)
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total += inserted
        if inserted > 0:
            log.info(f"  {coin}: {inserted} taker records")

    conn.close()
    log.info(f"TAKER DONE: {total} records")
    return total


# ═══════════════════════════════════════════════════════════════
# 6. EXCHANGE BALANCE HISTORY
# ═══════════════════════════════════════════════════════════════

def backfill_exchange_balance_history():
    """Fetch exchange balance chart history (dict format: time_list + data_map)."""
    log.info("=" * 60)
    log.info("BACKFILL: CoinGlass Exchange Balance History")
    log.info("=" * 60)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cg_balance_history (
            coin TEXT,
            timestamp INTEGER,
            total_balance REAL,
            price REAL,
            PRIMARY KEY (coin, timestamp)
        )
    """)

    balance_coins = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE']
    total = 0

    for coin in balance_coins:
        data = _get('/api/exchange/balance/chart', {'symbol': coin})
        if not data or not isinstance(data, dict):
            log.info(f"  {coin}: no balance history")
            continue

        time_list = data.get('time_list', [])
        price_list = data.get('price_list', [])
        data_map = data.get('data_map', {})

        # Sum balances across all exchanges for each timestamp
        n_times = len(time_list)
        total_balances = [0.0] * n_times
        for exchange, balances in data_map.items():
            if isinstance(balances, list) and len(balances) == n_times:
                for i, val in enumerate(balances):
                    if val is not None:
                        total_balances[i] += float(val)

        inserted = 0
        for i in range(n_times):
            ts = int(time_list[i])
            if ts > 1e12:
                ts = ts // 1000
            price = float(price_list[i]) if i < len(price_list) and price_list[i] else 0

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO cg_balance_history (coin, timestamp, total_balance, price) "
                    "VALUES (?, ?, ?, ?)",
                    (coin, ts, total_balances[i], price)
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()
        total += inserted
        if inserted > 0:
            log.info(f"  {coin}: {inserted} balance records ({len(data_map)} exchanges)")

    conn.close()
    log.info(f"BALANCE DONE: {total} records")
    return total


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def run_all():
    log.info("=" * 60)
    log.info("COINGLASS HISTORICAL BACKFILL — ALL ENDPOINTS")
    log.info("=" * 60)

    if not API_KEY:
        log.error("COINGLASS_API_KEY not set!")
        return

    results = {}
    results['oi'] = backfill_oi_history()
    results['funding'] = backfill_funding_history()
    results['liquidations'] = backfill_liquidation_history()
    results['ls_ratio'] = backfill_ls_ratio_history()
    results['taker'] = backfill_taker_history()
    results['balance'] = backfill_exchange_balance_history()

    log.info("=" * 60)
    log.info("BACKFILL COMPLETE")
    log.info("=" * 60)
    for source, count in results.items():
        log.info(f"  {source:15s}: {count} records")

    # Show date ranges
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    for table in ['cg_oi_history', 'cg_funding_history', 'cg_liq_history',
                  'cg_ls_history', 'cg_taker_history', 'cg_balance_history']:
        try:
            r = conn.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT coin),
                    MIN(date(timestamp, 'unixepoch')), MAX(date(timestamp, 'unixepoch'))
                FROM {table}
            """).fetchone()
            log.info(f"  {table}: {r[0]} rows, {r[1]} coins ({r[2]} to {r[3]})")
        except Exception:
            pass
    conn.close()


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--oi' in args:
        backfill_oi_history()
    elif '--funding' in args:
        backfill_funding_history()
    elif '--liq' in args:
        backfill_liquidation_history()
    elif '--ls' in args:
        backfill_ls_ratio_history()
    elif '--taker' in args:
        backfill_taker_history()
    elif '--balance' in args:
        backfill_exchange_balance_history()
    else:
        run_all()
