"""
Backfill 5 years of historical data for Forecast v3 training.

Sources (ALL FREE):
  1. Binance — OHLCV daily (5-8 years per coin)
  2. Binance Futures — Funding rates (since Sep 2019)
  3. FRED API — FOMC dates, CPI, Fed Funds Rate
  4. Google Trends — weekly search interest
  5. DeFi Llama — TVL historical
  6. Bitcoin halvings — hardcoded

Usage:
    python -m src.crypto.backfill_5y           # all sources
    python -m src.crypto.backfill_5y --prices   # only prices
    python -m src.crypto.backfill_5y --funding  # only funding rates
    python -m src.crypto.backfill_5y --macro    # only FRED macro
    python -m src.crypto.backfill_5y --trends   # only Google Trends
    python -m src.crypto.backfill_5y --tvl      # only DeFi Llama TVL
"""

import os
import sys
import json
import time
import sqlite3
import logging
import requests
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('backfill_5y')

DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'market.db'

# All tracked coins with their Binance symbols
COINS = {
    'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT',
    'BNB': 'BNBUSDT', 'XRP': 'XRPUSDT', 'ADA': 'ADAUSDT',
    'AVAX': 'AVAXUSDT', 'DOT': 'DOTUSDT', 'LINK': 'LINKUSDT',
    'DOGE': 'DOGEUSDT', 'SHIB': 'SHIBUSDT', 'PEPE': 'PEPEUSDT',
    'WIF': 'WIFUSDT', 'BONK': 'BONKUSDT',
    'UNI': 'UNIUSDT', 'AAVE': 'AAVEUSDT', 'MKR': 'MKRUSDT',
    'LDO': 'LDOUSDT', 'CRV': 'CRVUSDT',
    'FET': 'FETUSDT', 'RENDER': 'RENDERUSDT', 'TAO': 'TAOUSDT',
    'ARB': 'ARBUSDT', 'OP': 'OPUSDT', 'POL': 'POLUSDT',
}

# Binance Futures symbols (for funding rates)
FUTURES_SYMBOLS = {
    'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'SOL': 'SOLUSDT',
    'BNB': 'BNBUSDT', 'XRP': 'XRPUSDT', 'ADA': 'ADAUSDT',
    'AVAX': 'AVAXUSDT', 'DOT': 'DOTUSDT', 'LINK': 'LINKUSDT',
    'DOGE': 'DOGEUSDT', 'SHIB': 'SHIBUSDT',
    'UNI': 'UNIUSDT', 'AAVE': 'AAVEUSDT',
    'LDO': 'LDOUSDT', 'CRV': 'CRVUSDT',
    'FET': 'FETUSDT', 'ARB': 'ARBUSDT', 'OP': 'OPUSDT',
    'PEPE': '1000PEPEUSDT', 'BONK': '1000BONKUSDT',
    'WIF': 'WIFUSDT', 'MKR': 'MKRUSDT',
    'RENDER': 'RENDERUSDT', 'TAO': 'TAOUSDT', 'POL': 'POLUSDT',
}

# Bitcoin halving dates
HALVINGS = [
    ('2012-11-28', 1), ('2016-07-09', 2),
    ('2020-05-11', 3), ('2024-04-20', 4),
]


def get_conn():
    _conn = sqlite3.connect(str(DB_PATH), timeout=60)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=60000")
    return _conn


# ═══════════════════════════════════════════════════════════════
# 1. BINANCE OHLCV (FREE, no API key needed)
# ═══════════════════════════════════════════════════════════════

def backfill_prices():
    """Fetch 5+ years of daily OHLCV from Binance spot API."""
    log.info("=" * 60)
    log.info("BACKFILL: Binance OHLCV (daily)")
    log.info("=" * 60)

    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            coin TEXT, timestamp INTEGER, timeframe TEXT,
            open REAL, high REAL, low REAL, close REAL, volume REAL
        )
    """)

    # Start from 5 years ago
    start_ts = int((datetime.now() - timedelta(days=5*365)).timestamp() * 1000)
    total_inserted = 0

    for coin, symbol in COINS.items():
        # Check existing data
        existing = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM prices "
            "WHERE coin=? AND timeframe='1d'", (coin,)
        ).fetchone()

        existing_min = existing[0] if existing[0] else None
        existing_count = existing[2] or 0

        if existing_min and existing_min * 1000 <= start_ts + 86400000:
            log.info(f"  {coin}: already have data from {datetime.fromtimestamp(existing_min).date()}, skip")
            continue

        # Determine start time: either 5y ago or before existing data
        fetch_start = start_ts
        fetch_end = (existing_min * 1000) if existing_min else int(datetime.now().timestamp() * 1000)

        log.info(f"  {coin} ({symbol}): fetching from {datetime.fromtimestamp(fetch_start/1000).date()}...")

        coin_inserted = 0
        current_start = fetch_start

        while current_start < fetch_end:
            url = "https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': '1d',
                'startTime': current_start,
                'endTime': fetch_end,
                'limit': 1000,
            }

            try:
                resp = requests.get(url, params=params, timeout=15)
                if resp.status_code == 400:
                    # Symbol might not exist or different name
                    log.warning(f"  {coin}: 400 error, trying alternative symbol...")
                    # Try without the specific mapping
                    break
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"  {coin}: API error: {e}")
                break

            if not data:
                break

            for candle in data:
                ts = candle[0] // 1000  # ms → seconds
                # Skip if we already have this data point
                exists = conn.execute(
                    "SELECT 1 FROM prices WHERE coin=? AND timestamp=? AND timeframe='1d'",
                    (coin, ts)
                ).fetchone()
                if exists:
                    continue

                conn.execute(
                    "INSERT INTO prices (coin, timestamp, timeframe, open, high, low, close, volume) "
                    "VALUES (?, ?, '1d', ?, ?, ?, ?, ?)",
                    (coin, ts, float(candle[1]), float(candle[2]),
                     float(candle[3]), float(candle[4]), float(candle[5]))
                )
                coin_inserted += 1

            # Move to next batch
            last_ts = data[-1][0]
            if last_ts <= current_start:
                break
            current_start = last_ts + 1

            time.sleep(0.2)  # Rate limiting

        if coin_inserted > 0:
            conn.commit()
            total_inserted += coin_inserted
            log.info(f"  {coin}: inserted {coin_inserted} new candles")
        else:
            log.info(f"  {coin}: no new data")

    conn.close()
    log.info(f"PRICES DONE: {total_inserted} new candles inserted")
    return total_inserted


# ═══════════════════════════════════════════════════════════════
# 2. BINANCE FUNDING RATES (FREE, no API key)
# ═══════════════════════════════════════════════════════════════

def backfill_funding():
    """Fetch funding rates from Binance Futures (since Sep 2019)."""
    log.info("=" * 60)
    log.info("BACKFILL: Binance Funding Rates")
    log.info("=" * 60)

    conn = get_conn()
    # Start from Sep 2019 (Binance Futures launch)
    start_ts = int(datetime(2019, 9, 1, tzinfo=timezone.utc).timestamp() * 1000)
    total_inserted = 0

    for coin, symbol in FUTURES_SYMBOLS.items():
        # Check existing oldest
        existing = conn.execute(
            "SELECT MIN(timestamp) FROM funding_rates WHERE coin=?", (coin,)
        ).fetchone()
        existing_min = existing[0] if existing[0] else None

        if existing_min and existing_min * 1000 <= start_ts + 86400000:
            log.info(f"  {coin}: already have funding from {datetime.fromtimestamp(existing_min).date()}, skip")
            continue

        fetch_end = (existing_min * 1000) if existing_min else int(datetime.now().timestamp() * 1000)
        log.info(f"  {coin} ({symbol}): fetching funding rates...")

        coin_inserted = 0
        current_start = start_ts

        while current_start < fetch_end:
            url = "https://fapi.binance.com/fapi/v1/fundingRate"
            params = {
                'symbol': symbol,
                'startTime': current_start,
                'endTime': fetch_end,
                'limit': 1000,
            }

            try:
                resp = requests.get(url, params=params, timeout=15)
                if resp.status_code != 200:
                    log.warning(f"  {coin}: HTTP {resp.status_code}")
                    break
                data = resp.json()
            except Exception as e:
                log.error(f"  {coin}: API error: {e}")
                break

            if not data:
                break

            for entry in data:
                ts = entry['fundingTime'] // 1000
                rate = float(entry['fundingRate'])

                exists = conn.execute(
                    "SELECT 1 FROM funding_rates WHERE coin=? AND timestamp=?",
                    (coin, ts)
                ).fetchone()
                if exists:
                    continue

                conn.execute(
                    "INSERT INTO funding_rates (coin, timestamp, rate) VALUES (?, ?, ?)",
                    (coin, ts, rate)
                )
                coin_inserted += 1

            last_ts = data[-1]['fundingTime']
            if last_ts <= current_start:
                break
            current_start = last_ts + 1

            time.sleep(0.3)

        if coin_inserted > 0:
            conn.commit()
            total_inserted += coin_inserted
            log.info(f"  {coin}: inserted {coin_inserted} funding rates")

    conn.close()
    log.info(f"FUNDING DONE: {total_inserted} new rates inserted")
    return total_inserted


# ═══════════════════════════════════════════════════════════════
# 3. FRED API — Macro indicators (FREE with API key)
# ═══════════════════════════════════════════════════════════════

def backfill_macro():
    """Fetch FOMC rate decisions, CPI, unemployment from FRED API."""
    log.info("=" * 60)
    log.info("BACKFILL: FRED Macro Data")
    log.info("=" * 60)

    api_key = os.getenv('FRED_API_KEY')
    if not api_key:
        log.error("FRED_API_KEY not set in .env")
        return 0

    conn = get_conn()

    # Create macro_events table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macro_events (
            date TEXT,
            event_type TEXT,
            value REAL,
            previous_value REAL,
            change REAL,
            PRIMARY KEY (date, event_type)
        )
    """)

    # FRED series to fetch
    series = {
        'FEDFUNDS': 'fed_rate',        # Federal Funds Rate (monthly)
        'CPIAUCSL': 'cpi',              # CPI All Urban Consumers (monthly)
        'UNRATE': 'unemployment',       # Unemployment Rate (monthly)
        'T10Y2Y': 'yield_curve',        # 10Y-2Y Treasury Spread (daily)
        'VIXCLS': 'vix',               # VIX volatility index (daily)
        'DGS10': 'treasury_10y',       # 10-Year Treasury Rate (daily)
        'SP500': 'sp500',             # S&P500 index (daily) — crypto correlation
        'NASDAQCOM': 'nasdaq',         # NASDAQ Composite (daily) — tech/risk
        'DTWEXBGS': 'dxy',            # Dollar index (daily) — inverse crypto
    }

    total_inserted = 0
    start_date = '2019-01-01'

    for series_id, event_type in series.items():
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': series_id,
            'api_key': api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'sort_order': 'asc',
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"  {series_id}: API error: {e}")
            continue

        observations = data.get('observations', [])
        inserted = 0
        prev_value = None

        for obs in observations:
            date = obs['date']
            value_str = obs['value']
            if value_str == '.':
                continue
            value = float(value_str)
            change = value - prev_value if prev_value is not None else 0

            try:
                conn.execute(
                    "INSERT OR REPLACE INTO macro_events "
                    "(date, event_type, value, previous_value, change) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (date, event_type, value, prev_value, change)
                )
                inserted += 1
            except Exception:
                pass
            prev_value = value

        conn.commit()
        total_inserted += inserted
        log.info(f"  {series_id} ({event_type}): {inserted} observations")
        time.sleep(0.5)

    # FOMC meeting dates — fetch from FRED releases
    log.info("  Fetching FOMC meeting dates...")
    url = "https://api.stlouisfed.org/fred/release/dates"
    params = {
        'release_id': 17,  # FOMC Press Release
        'api_key': api_key,
        'file_type': 'json',
        'include_release_dates_with_no_data': 'true',
        'sort_order': 'asc',
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        fomc_dates = [rd['date'] for rd in data.get('release_dates', [])]

        for date in fomc_dates:
            conn.execute(
                "INSERT OR REPLACE INTO macro_events "
                "(date, event_type, value, previous_value, change) "
                "VALUES (?, 'fomc_meeting', 1, NULL, NULL)",
                (date,)
            )
        conn.commit()
        log.info(f"  FOMC meetings: {len(fomc_dates)} dates")
        total_inserted += len(fomc_dates)
    except Exception as e:
        log.error(f"  FOMC dates: {e}")

    conn.close()
    log.info(f"MACRO DONE: {total_inserted} observations inserted")
    return total_inserted


# ═══════════════════════════════════════════════════════════════
# 4. GOOGLE TRENDS (FREE, no API key)
# ═══════════════════════════════════════════════════════════════

def backfill_google_trends():
    """Fetch 5 years of weekly Google Trends for crypto keywords."""
    log.info("=" * 60)
    log.info("BACKFILL: Google Trends")
    log.info("=" * 60)

    try:
        from pytrends.request import TrendReq
    except ImportError:
        log.error("pytrends not installed. Run: pip install pytrends")
        return 0

    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS google_trends (
            date TEXT,
            keyword TEXT,
            value INTEGER,
            PRIMARY KEY (date, keyword)
        )
    """)

    keywords = ['bitcoin', 'ethereum', 'crypto', 'altcoin', 'defi']
    pytrends = TrendReq(hl='en-US', tz=0)
    total_inserted = 0

    # Fetch in batches of 5 keywords (pytrends limit)
    try:
        pytrends.build_payload(keywords, timeframe='today 5-y', geo='')
        data = pytrends.interest_over_time()

        if data.empty:
            log.warning("  No Google Trends data returned")
            return 0

        for _, row in data.iterrows():
            date = row.name.strftime('%Y-%m-%d')
            for kw in keywords:
                if kw in data.columns:
                    conn.execute(
                        "INSERT OR REPLACE INTO google_trends (date, keyword, value) "
                        "VALUES (?, ?, ?)",
                        (date, kw, int(row[kw]))
                    )
                    total_inserted += 1

        conn.commit()
        log.info(f"  {len(data)} weeks of data for {len(keywords)} keywords")
    except Exception as e:
        log.error(f"  Google Trends error: {e}")

    conn.close()
    log.info(f"TRENDS DONE: {total_inserted} data points inserted")
    return total_inserted


# ═══════════════════════════════════════════════════════════════
# 5. DEFI LLAMA TVL (FREE, no API key)
# ═══════════════════════════════════════════════════════════════

def backfill_tvl():
    """Fetch historical DeFi TVL from DeFi Llama."""
    log.info("=" * 60)
    log.info("BACKFILL: DeFi Llama TVL")
    log.info("=" * 60)

    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS defi_tvl_history (
            date TEXT PRIMARY KEY,
            total_tvl REAL
        )
    """)

    # Total DeFi TVL
    try:
        resp = requests.get("https://api.llama.fi/v2/historicalChainTvl", timeout=30)
        resp.raise_for_status()
        data = resp.json()

        inserted = 0
        for point in data:
            ts = point.get('date', 0)
            tvl = point.get('tvl', 0)
            date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            conn.execute(
                "INSERT OR REPLACE INTO defi_tvl_history (date, total_tvl) VALUES (?, ?)",
                (date, tvl)
            )
            inserted += 1

        conn.commit()
        log.info(f"  Total TVL: {inserted} daily data points")
    except Exception as e:
        log.error(f"  TVL error: {e}")
        inserted = 0

    # Per-chain TVL for major chains
    chains = ['Ethereum', 'Solana', 'BSC', 'Arbitrum', 'Avalanche', 'Polygon', 'Optimism', 'Base']
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chain_tvl_history (
            date TEXT,
            chain TEXT,
            tvl REAL,
            PRIMARY KEY (date, chain)
        )
    """)

    for chain in chains:
        try:
            resp = requests.get(f"https://api.llama.fi/v2/historicalChainTvl/{chain}", timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
            for point in data:
                ts = point.get('date', 0)
                tvl = point.get('tvl', 0)
                date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                conn.execute(
                    "INSERT OR REPLACE INTO chain_tvl_history (date, chain, tvl) VALUES (?, ?, ?)",
                    (date, chain, tvl)
                )
            log.info(f"  {chain}: {len(data)} data points")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  {chain}: {e}")

    conn.commit()
    conn.close()
    log.info("TVL DONE")
    return inserted


# ═══════════════════════════════════════════════════════════════
# 6. BITCOIN HALVINGS (hardcoded)
# ═══════════════════════════════════════════════════════════════

def backfill_halvings():
    """Store Bitcoin halving dates for feature engineering."""
    log.info("=" * 60)
    log.info("BACKFILL: Bitcoin Halvings")
    log.info("=" * 60)

    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS btc_halvings (
            date TEXT PRIMARY KEY,
            halving_number INTEGER,
            block_reward REAL
        )
    """)

    halvings = [
        ('2012-11-28', 1, 25.0),
        ('2016-07-09', 2, 12.5),
        ('2020-05-11', 3, 6.25),
        ('2024-04-20', 4, 3.125),
    ]

    for date, num, reward in halvings:
        conn.execute(
            "INSERT OR REPLACE INTO btc_halvings (date, halving_number, block_reward) "
            "VALUES (?, ?, ?)", (date, num, reward)
        )

    conn.commit()
    conn.close()
    log.info(f"  {len(halvings)} halvings stored")
    return len(halvings)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def run_all():
    """Run all backfill sources."""
    log.info("=" * 60)
    log.info("BACKFILL 5Y — Starting all sources")
    log.info("=" * 60)

    results = {}

    # 1. Prices (biggest impact)
    results['prices'] = backfill_prices()

    # 2. Funding rates
    results['funding'] = backfill_funding()

    # 3. Macro data
    results['macro'] = backfill_macro()

    # 4. Google Trends
    results['trends'] = backfill_google_trends()

    # 5. DeFi TVL
    results['tvl'] = backfill_tvl()

    # 6. Halvings
    results['halvings'] = backfill_halvings()

    # Summary
    log.info("=" * 60)
    log.info("BACKFILL COMPLETE — Summary")
    log.info("=" * 60)
    for source, count in results.items():
        log.info(f"  {source:15s}: {count} records")

    # Show new data range
    conn = get_conn()
    c = conn.cursor()
    c.execute("""SELECT COUNT(*), COUNT(DISTINCT coin),
        MIN(date(timestamp, 'unixepoch')), MAX(date(timestamp, 'unixepoch'))
        FROM prices WHERE timeframe='1d'""")
    r = c.fetchone()
    log.info(f"\n  PRICES: {r[0]} total rows, {r[1]} coins, {r[2]} to {r[3]}")

    c.execute("""SELECT COUNT(*), COUNT(DISTINCT coin),
        MIN(date(timestamp, 'unixepoch')), MAX(date(timestamp, 'unixepoch'))
        FROM funding_rates""")
    r = c.fetchone()
    log.info(f"  FUNDING: {r[0]} total rows, {r[1]} coins, {r[2]} to {r[3]}")

    c.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM macro_events")
    r = c.fetchone()
    if r[0]:
        log.info(f"  MACRO: {r[0]} events, {r[1]} to {r[2]}")

    c.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM google_trends")
    r = c.fetchone()
    if r and r[0]:
        log.info(f"  TRENDS: {r[0]} data points, {r[1]} to {r[2]}")

    conn.close()
    return results


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--prices' in args:
        backfill_prices()
    elif '--funding' in args:
        backfill_funding()
    elif '--macro' in args:
        backfill_macro()
    elif '--trends' in args:
        backfill_google_trends()
    elif '--tvl' in args:
        backfill_tvl()
    elif '--halvings' in args:
        backfill_halvings()
    else:
        run_all()
