"""
FORTIX — Data Validator
==============================
Validates data freshness and quality before script generation.
Prevents video production with stale/corrupt/missing data.

Usage:
    from src.crypto.data_validator import validate_data
    result = validate_data(max_age_hours=2)
    if not result['ok']:
        print(f"BLOCKED: {result['errors']}")
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger('data_validator')

DB_PATH = Path('data/crypto/market.db')

# Sources and their tables/freshness requirements
REQUIRED_SOURCES = {
    'prices': {
        'table': 'prices',
        'timestamp_col': 'timestamp',
        'description': 'Binance price data',
        'critical': True,
    },
    'market_overview': {
        'table': 'market_overview',
        'timestamp_col': 'timestamp',
        'description': 'CoinGecko market overview',
        'critical': True,
    },
    'fear_greed': {
        'table': 'fear_greed',
        'timestamp_col': 'date',
        'description': 'Fear & Greed Index',
        'critical': False,
        'max_age_hours': 24,  # F&G updates once/day
    },
    'funding_rates': {
        'table': 'funding_rates',
        'timestamp_col': 'timestamp',
        'description': 'Binance funding rates',
        'critical': False,
    },
    'news': {
        'table': 'news',
        'timestamp_col': 'timestamp',
        'description': 'CryptoPanic + RSS news',
        'critical': False,
    },
    'whale_transactions': {
        'table': 'whale_transactions',
        'timestamp_col': 'timestamp',
        'description': 'Whale Alert transactions',
        'critical': False,
    },
    'long_short_ratio': {
        'table': 'long_short_ratio',
        'timestamp_col': 'timestamp',
        'description': 'Binance L/S ratios',
        'critical': False,
    },
    'taker_volume': {
        'table': 'taker_volume',
        'timestamp_col': 'timestamp',
        'description': 'Binance taker volume',
        'critical': False,
    },
    'dxy': {
        'table': 'dxy_rates',
        'timestamp_col': 'date',
        'description': 'DXY (US Dollar Index) from ECB',
        'critical': False,
        'max_age_hours': 72,  # ECB doesn't publish on weekends
    },
    'coinglass_exchange_balance': {
        'table': 'cg_exchange_balance',
        'timestamp_col': 'timestamp',
        'description': 'CoinGlass exchange balance',
        'critical': False,
        'max_age_hours': 4,
    },
    'coinglass_liquidations': {
        'table': 'cg_liquidations',
        'timestamp_col': 'timestamp',
        'description': 'CoinGlass liquidations',
        'critical': False,
        'max_age_hours': 4,
    },
    'coinglass_aggregated_oi': {
        'table': 'cg_aggregated_oi',
        'timestamp_col': 'timestamp',
        'description': 'CoinGlass aggregated open interest',
        'critical': False,
        'max_age_hours': 4,
    },
    'cryptoquant_btc_onchain': {
        'table': 'cq_btc_onchain',
        'timestamp_col': 'date',
        'description': 'CryptoQuant BTC on-chain metrics',
        'critical': False,
        'max_age_hours': 6,  # CryptoQuant updates less frequently
    },
    'cryptoquant_exchange_flows': {
        'table': 'cq_exchange_flows',
        'timestamp_col': 'date',
        'description': 'CryptoQuant exchange flows',
        'critical': False,
        'max_age_hours': 6,
    },
}

# Key coins that must have price data
CRITICAL_COINS = ['BTC', 'ETH']


def validate_data(max_age_hours: float = 2.0, require_all_critical: bool = True) -> dict:
    """Validate data freshness and quality.

    Args:
        max_age_hours: Maximum age of data in hours (default 2h)
        require_all_critical: If True, abort when any critical source is stale

    Returns:
        {
            'ok': bool,           # True = safe to generate
            'errors': [str],      # Blocking issues
            'warnings': [str],    # Non-blocking issues
            'sources': {name: {'fresh': bool, 'age_hours': float, 'rows': int}},
            'btc_price': float,   # Latest BTC price (for sanity check)
            'eth_price': float,   # Latest ETH price
        }
    """
    result = {
        'ok': True,
        'errors': [],
        'warnings': [],
        'sources': {},
        'btc_price': 0.0,
        'eth_price': 0.0,
    }

    if not DB_PATH.exists():
        result['ok'] = False
        result['errors'].append(f"Database not found: {DB_PATH}")
        return result

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.row_factory = sqlite3.Row
    except Exception as e:
        result['ok'] = False
        result['errors'].append(f"Cannot open database: {e}")
        return result

    now = datetime.now(timezone.utc)
    failed_sources = 0

    for name, cfg in REQUIRED_SOURCES.items():
        source_result = {'fresh': False, 'age_hours': -1, 'rows': 0}

        try:
            # Check if table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (cfg['table'],)
            )
            if not cur.fetchone():
                source_result['age_hours'] = -1
                msg = f"{cfg['description']} ({cfg['table']}): table not found"
                if cfg['critical']:
                    result['errors'].append(msg)
                    failed_sources += 1
                else:
                    result['warnings'].append(msg)
                result['sources'][name] = source_result
                continue

            # Count rows
            cur = conn.execute(f"SELECT COUNT(*) FROM {cfg['table']}")
            source_result['rows'] = cur.fetchone()[0]

            if source_result['rows'] == 0:
                msg = f"{cfg['description']}: empty table"
                if cfg['critical']:
                    result['errors'].append(msg)
                    failed_sources += 1
                else:
                    result['warnings'].append(msg)
                result['sources'][name] = source_result
                continue

            # Check freshness
            ts_col = cfg['timestamp_col']
            cur = conn.execute(f"SELECT MAX({ts_col}) FROM {cfg['table']}")
            latest_ts = cur.fetchone()[0]

            if latest_ts:
                # Parse timestamp — try multiple formats
                age_hours = _compute_age_hours(latest_ts, now)
                source_result['age_hours'] = age_hours

                source_max_age = cfg.get('max_age_hours', max_age_hours)
                if age_hours <= source_max_age:
                    source_result['fresh'] = True
                else:
                    msg = f"{cfg['description']}: stale ({age_hours:.1f}h old, max {source_max_age}h)"
                    if cfg['critical']:
                        result['errors'].append(msg)
                        failed_sources += 1
                    else:
                        result['warnings'].append(msg)

        except Exception as e:
            msg = f"{cfg['description']}: check failed ({e})"
            if cfg['critical']:
                result['errors'].append(msg)
                failed_sources += 1
            else:
                result['warnings'].append(msg)

        result['sources'][name] = source_result

    # Check critical coin prices
    for coin in CRITICAL_COINS:
        try:
            cur = conn.execute(
                "SELECT close FROM prices WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                (coin,)
            )
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                if coin == 'BTC':
                    result['btc_price'] = row[0]
                elif coin == 'ETH':
                    result['eth_price'] = row[0]
            else:
                result['errors'].append(f"{coin} price is 0 or NULL")
                failed_sources += 1
        except Exception as e:
            result['errors'].append(f"{coin} price check failed: {e}")
            failed_sources += 1

    # BTC sanity check (should be between $1K and $500K in 2024-2027)
    if result['btc_price'] > 0 and (result['btc_price'] < 1000 or result['btc_price'] > 500000):
        result['errors'].append(f"BTC price out of range: ${result['btc_price']:.0f}")

    # Price anomaly detection: check for sudden >15% jumps in recent data
    for coin in CRITICAL_COINS:
        try:
            rows = conn.execute(
                "SELECT close, timestamp FROM prices WHERE coin=? "
                "ORDER BY timestamp DESC LIMIT 12",  # ~12h of hourly candles
                (coin,)
            ).fetchall()
            if len(rows) >= 2:
                latest = rows[0][0]
                for prev_row in rows[1:]:
                    prev_price = prev_row[0]
                    if prev_price and prev_price > 0:
                        change_pct = abs(latest - prev_price) / prev_price * 100
                        if change_pct > 15:
                            result['warnings'].append(
                                f"{coin} price anomaly: {change_pct:.1f}% change "
                                f"(${prev_price:,.0f} → ${latest:,.0f}) — possible bad data")
                        break  # Only compare with most recent previous candle
        except Exception:
            pass

    # Check Fear & Greed range
    try:
        cur = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            fg = row[0]
            if fg < 0 or fg > 100:
                result['errors'].append(f"Fear & Greed out of range: {fg}")
    except Exception:
        pass

    conn.close()

    # If any errors exist, mark not ok
    if result['errors']:
        result['ok'] = False
        if failed_sources > 3:
            result['errors'].insert(0, f"{failed_sources} sources failed — too many to proceed")

    return result


def _compute_age_hours(timestamp_str, now: datetime) -> float:
    """Parse a timestamp string and compute age in hours."""
    for fmt in [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%d',
    ]:
        try:
            dt = datetime.strptime(str(timestamp_str), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = now - dt
            return delta.total_seconds() / 3600
        except (ValueError, TypeError):
            continue

    # Try unix timestamp
    try:
        ts = float(timestamp_str)
        if ts > 1e12:  # milliseconds
            ts /= 1000
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return (now - dt).total_seconds() / 3600
    except (ValueError, TypeError):
        pass

    return 999.0  # Unknown format — treat as very old


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    result = validate_data(max_age_hours=2.0)

    print("\n" + "=" * 50)
    print("DATA VALIDATION REPORT")
    print("=" * 50)
    print(f"Status: {'PASS' if result['ok'] else 'FAIL'}")
    print(f"BTC: ${result['btc_price']:,.0f}  |  ETH: ${result['eth_price']:,.0f}")

    if result['errors']:
        print(f"\nERRORS ({len(result['errors'])}):")
        for e in result['errors']:
            print(f"  [X] {e}")

    if result['warnings']:
        print(f"\nWARNINGS ({len(result['warnings'])}):")
        for w in result['warnings']:
            print(f"  [!] {w}")

    print(f"\nSOURCES:")
    for name, info in result['sources'].items():
        status = "OK" if info['fresh'] else "STALE"
        age = f"{info['age_hours']:.1f}h" if info['age_hours'] >= 0 else "N/A"
        print(f"  {name:20s}  {status:5s}  age={age:>6s}  rows={info['rows']}")
