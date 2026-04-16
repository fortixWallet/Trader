"""
FORTIX — CoinGlass Data Collector
==========================================
Collects aggregated derivatives data from CoinGlass API V4.

Provides data across ALL major exchanges (not just Binance):
  - Exchange balances (inflow/outflow) — accumulation vs distribution
  - Options max pain (BTC/ETH) — gravitational price target
  - BTC ETF flows — institutional capital direction
  - Stablecoin supply — total buying power in crypto
  - Aggregated OI by exchange — multi-exchange open interest
  - Aggregated liquidations — multi-exchange cascade detection

API: https://open-api-v4.coinglass.com
Auth: CG-API-KEY header
Plan: Hobbyist (30 req/min)

Usage:
    python src/crypto/coinglass_collector.py          # collect all
    python src/crypto/coinglass_collector.py --test    # test API connection
"""

import os
import sys
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('coinglass')

COINGLASS_API_KEY = os.getenv('COINGLASS_API_KEY', '')
BASE_URL = 'https://open-api-v4.coinglass.com'

# Coins we collect exchange balance for (top liquidity)
BALANCE_COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE']

# Coins we collect aggregated OI for
OI_COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE']


class CoinGlassCollector:
    """Collect aggregated crypto derivatives data from CoinGlass API V4."""

    def __init__(self):
        self.api_key = COINGLASS_API_KEY
        self.session = requests.Session()
        self.session.headers['CG-API-KEY'] = self.api_key
        self.session.headers['accept'] = 'application/json'
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce 2.5s between requests (30 req/min plan limit)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 2.5:
            time.sleep(2.5 - elapsed)
        self._last_request_time = time.time()

    def _get(self, path: str, params: dict = None) -> dict:
        """Make authenticated GET request with rate limiting and retry on transient errors."""
        self._rate_limit()
        url = f'{BASE_URL}{path}'
        backoff = (5, 15, 45)
        retryable = (429, 500, 502, 503, 504)
        try:
            resp = None
            for attempt in range(3):
                resp = self.session.get(url, params=params or {}, timeout=20)
                if resp.status_code in retryable and attempt < 2:
                    wait = backoff[attempt]
                    log.warning(f"  HTTP {resp.status_code} on {path}, retry in {wait}s "
                                f"(attempt {attempt + 1}/3)")
                    time.sleep(wait)
                    continue
                break

            data = resp.json()
            code = data.get('code', '?')

            if code == '400' and 'Upgrade' in data.get('msg', ''):
                log.warning(f"  {path}: requires plan upgrade — skipping")
                return {'code': '400', 'data': None}

            if str(code) != '0':
                log.warning(f"  {path}: code={code}, msg={data.get('msg', '')}")
                return {'code': code, 'data': None}

            return data
        except Exception as e:
            log.warning(f"  {path}: request failed — {e}")
            return {'code': 'error', 'data': None}

    # ════════════════════════════════════════
    # 1. EXCHANGE BALANCE — accumulation vs distribution
    # ════════════════════════════════════════

    def collect_exchange_balance(self, conn: sqlite3.Connection) -> int:
        """Collect exchange balance data per coin.

        Outflow from exchanges = accumulation (bullish)
        Inflow to exchanges = sell pressure (bearish)
        """
        log.info("[CoinGlass] Collecting exchange balances...")
        count = 0
        now = int(time.time())

        for coin in BALANCE_COINS:
            data = self._get('/api/exchange/balance/list', {'symbol': coin})
            entries = data.get('data')
            if not entries:
                continue

            for entry in entries:
                exchange = entry.get('exchange_name', 'unknown')
                conn.execute(
                    "INSERT OR REPLACE INTO cg_exchange_balance "
                    "(coin, timestamp, exchange, total_balance, "
                    "change_1d, change_pct_1d, change_7d, change_pct_7d, "
                    "change_30d, change_pct_30d) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        coin, now, exchange,
                        entry.get('total_balance', 0),
                        entry.get('balance_change_1d', 0),
                        entry.get('balance_change_percent_1d', 0),
                        entry.get('balance_change_7d', 0),
                        entry.get('balance_change_percent_7d', 0),
                        entry.get('balance_change_30d', 0),
                        entry.get('balance_change_percent_30d', 0),
                    )
                )
                count += 1

        conn.commit()
        log.info(f"  Stored exchange balance for {len(BALANCE_COINS)} coins ({count} rows)")
        return count

    # ════════════════════════════════════════
    # 2. OPTIONS MAX PAIN — gravitational price target
    # ════════════════════════════════════════

    def collect_options_max_pain(self, conn: sqlite3.Connection) -> int:
        """Collect options max pain for BTC and ETH.

        Max pain = price where most options expire worthless.
        Price tends to gravitate toward max pain near expiry.
        """
        log.info("[CoinGlass] Collecting options max pain...")
        count = 0
        now = int(time.time())

        for coin in ['BTC', 'ETH']:
            data = self._get('/api/option/max-pain', {
                'symbol': coin,
                'exchange': 'Deribit',
            })
            entries = data.get('data')
            if not entries:
                continue

            for entry in entries:
                expiry = entry.get('date', '')
                max_pain = entry.get('max_pain_price', 0)
                try:
                    max_pain = float(max_pain) if max_pain else 0
                except (ValueError, TypeError):
                    max_pain = 0

                conn.execute(
                    "INSERT OR REPLACE INTO cg_options_max_pain "
                    "(coin, timestamp, expiry_date, max_pain_price, "
                    "call_oi, call_oi_value, call_oi_notional, "
                    "put_oi, put_oi_value, put_oi_notional) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        coin, now, expiry, max_pain,
                        entry.get('call_open_interest', 0),
                        entry.get('call_open_interest_market_value', 0),
                        entry.get('call_open_interest_notional', 0),
                        entry.get('put_open_interest', 0),
                        entry.get('put_open_interest_market_value', 0),
                        entry.get('put_open_interest_notional', 0),
                    )
                )
                count += 1

        conn.commit()
        log.info(f"  Stored {count} options max pain entries (BTC + ETH)")
        return count

    # ════════════════════════════════════════
    # 3. ETF FLOWS — institutional capital direction
    # ════════════════════════════════════════

    def collect_etf_flows(self, conn: sqlite3.Connection) -> int:
        """Collect BTC + ETH ETF flow history.

        Net inflow = institutional buying (bullish)
        Net outflow = institutional selling (bearish)
        """
        total = 0
        for asset, endpoint in [('BTC', '/api/etf/bitcoin/flow-history'),
                                 ('ETH', '/api/etf/ethereum/flow-history')]:
            log.info(f"[CoinGlass] Collecting {asset} ETF flows...")
            count = self._collect_etf_asset(conn, asset, endpoint)
            total += count
        return total

    def _collect_etf_asset(self, conn: sqlite3.Connection, asset: str, endpoint: str) -> int:
        count = 0
        data = self._get(endpoint)
        entries = data.get('data')
        if not entries:
            log.warning("  No ETF flow data returned")
            return 0

        for entry in entries:
            ts = entry.get('timestamp', 0)
            # Convert ms timestamp to date string
            if ts > 1e12:
                ts_sec = ts / 1000
            else:
                ts_sec = ts
            date = datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime('%Y-%m-%d')

            flow_usd = entry.get('flow_usd', 0) or 0
            price_usd = entry.get('price_usd', 0) or 0

            conn.execute(
                "INSERT OR REPLACE INTO cg_etf_flows "
                "(date, asset, flow_usd, price_usd) "
                "VALUES (?, ?, ?, ?)",
                (date, asset, flow_usd, price_usd)
            )
            count += 1

        conn.commit()
        log.info(f"  Stored {count} {asset} ETF flow entries")
        return count

    # ════════════════════════════════════════
    # 4. STABLECOIN SUPPLY — total buying power
    # ════════════════════════════════════════

    def collect_stablecoin_supply(self, conn: sqlite3.Connection) -> int:
        """Collect stablecoin market cap history.

        Rising stablecoin supply = more buying power entering crypto (bullish)
        Falling = capital leaving crypto ecosystem (bearish)
        """
        log.info("[CoinGlass] Collecting stablecoin supply history...")
        count = 0

        data = self._get('/api/index/stableCoin-marketCap-history')
        raw = data.get('data')
        if not raw:
            log.warning("  No stablecoin data returned")
            return 0

        data_list = raw.get('data_list', [])
        time_list = raw.get('time_list', [])

        if not data_list or not time_list:
            log.warning("  Stablecoin data empty")
            return 0

        # Only store last 90 days to avoid massive inserts
        n = min(len(data_list), len(time_list))
        start = max(0, n - 90)

        for i in range(start, n):
            ts = time_list[i]
            if ts > 1e12:
                ts = ts / 1000
            date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

            entry = data_list[i]
            if isinstance(entry, dict):
                total_mcap = sum(v for v in entry.values() if isinstance(v, (int, float)))
            else:
                continue

            conn.execute(
                "INSERT OR REPLACE INTO cg_stablecoin_supply "
                "(date, total_market_cap) "
                "VALUES (?, ?)",
                (date, total_mcap)
            )
            count += 1

        conn.commit()
        log.info(f"  Stored {count} stablecoin supply entries (last 90 days)")
        return count

    # ════════════════════════════════════════
    # 5. AGGREGATED OPEN INTEREST — multi-exchange
    # ════════════════════════════════════════

    def collect_aggregated_oi(self, conn: sqlite3.Connection) -> int:
        """Collect aggregated open interest across all exchanges per coin.

        This is superior to single-exchange OI because it captures the
        full market picture (CME institutional OI + retail exchanges).
        """
        log.info("[CoinGlass] Collecting aggregated OI...")
        count = 0
        now = int(time.time())

        for coin in OI_COINS:
            data = self._get('/api/futures/open-interest/exchange-list', {'symbol': coin})
            entries = data.get('data')
            if not entries:
                continue

            for entry in entries:
                exchange = entry.get('exchange', 'unknown')
                conn.execute(
                    "INSERT OR REPLACE INTO cg_aggregated_oi "
                    "(coin, timestamp, exchange, oi_usd, oi_quantity, "
                    "change_pct_1h, change_pct_4h, change_pct_24h) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        coin, now, exchange,
                        entry.get('open_interest_usd', 0),
                        entry.get('open_interest_quantity', 0),
                        entry.get('open_interest_change_percent_1h', 0),
                        entry.get('open_interest_change_percent_4h', 0),
                        entry.get('open_interest_change_percent_24h', 0),
                    )
                )
                count += 1

        conn.commit()
        log.info(f"  Stored aggregated OI for {len(OI_COINS)} coins ({count} rows)")
        return count

    # ════════════════════════════════════════
    # 6. AGGREGATED LIQUIDATIONS — multi-exchange
    # ════════════════════════════════════════

    def collect_aggregated_liquidations(self, conn: sqlite3.Connection) -> int:
        """Collect aggregated liquidation data from liquidation/coin-list.

        Multi-exchange liquidation snapshot — much better than single-exchange
        because liquidation cascades happen across all exchanges simultaneously.
        """
        log.info("[CoinGlass] Collecting aggregated liquidations...")
        count = 0
        now = int(time.time())

        data = self._get('/api/futures/liquidation/coin-list')
        entries = data.get('data')
        if not entries:
            log.warning("  No liquidation data returned")
            return 0

        for entry in entries:
            symbol = entry.get('symbol', '')
            conn.execute(
                "INSERT OR REPLACE INTO cg_liquidations "
                "(coin, timestamp, "
                "liq_usd_24h, long_liq_usd_24h, short_liq_usd_24h, "
                "liq_usd_12h, long_liq_usd_12h, short_liq_usd_12h, "
                "liq_usd_4h, long_liq_usd_4h, short_liq_usd_4h, "
                "liq_usd_1h, long_liq_usd_1h, short_liq_usd_1h) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    symbol, now,
                    entry.get('liquidation_usd_24h', 0),
                    entry.get('long_liquidation_usd_24h', 0),
                    entry.get('short_liquidation_usd_24h', 0),
                    entry.get('liquidation_usd_12h', 0),
                    entry.get('long_liquidation_usd_12h', 0),
                    entry.get('short_liquidation_usd_12h', 0),
                    entry.get('liquidation_usd_4h', 0),
                    entry.get('long_liquidation_usd_4h', 0),
                    entry.get('short_liquidation_usd_4h', 0),
                    entry.get('liquidation_usd_1h', 0),
                    entry.get('long_liquidation_usd_1h', 0),
                    entry.get('short_liquidation_usd_1h', 0),
                )
            )
            count += 1

        conn.commit()
        log.info(f"  Stored aggregated liquidations for {count} coins")
        return count


def collect_all_coinglass(conn: sqlite3.Connection) -> int:
    """Run all CoinGlass collectors. Returns total rows stored."""
    if not COINGLASS_API_KEY:
        log.info("[CoinGlass] Skipped — no API key (set COINGLASS_API_KEY)")
        return 0

    cg = CoinGlassCollector()
    total = 0

    collectors = [
        ('exchange_balance', cg.collect_exchange_balance),
        ('options_max_pain', cg.collect_options_max_pain),
        ('etf_flows', cg.collect_etf_flows),
        ('stablecoin_supply', cg.collect_stablecoin_supply),
        ('aggregated_oi', cg.collect_aggregated_oi),
        ('aggregated_liquidations', cg.collect_aggregated_liquidations),
    ]

    for name, fn in collectors:
        try:
            result = fn(conn)
            total += result
        except Exception as e:
            log.error(f"  CoinGlass {name} failed: {e}")

    log.info(f"[CoinGlass] Total: {total} rows stored")
    return total


if __name__ == '__main__':
    from src.crypto.data_collector import init_db

    if '--test' in sys.argv:
        # Quick API connectivity test
        if not COINGLASS_API_KEY:
            print("ERROR: COINGLASS_API_KEY not set in .env")
            sys.exit(1)
        cg = CoinGlassCollector()
        data = cg._get('/api/exchange/balance/list', {'symbol': 'BTC'})
        if data.get('data'):
            print(f"API OK — got {len(data['data'])} exchange balance entries for BTC")
        else:
            print(f"API FAILED — code={data.get('code')}, msg={data.get('msg', '')}")
    else:
        conn = init_db()
        result = collect_all_coinglass(conn)
        conn.close()
        print(f"\nCoinGlass collection complete: {result} total rows")
