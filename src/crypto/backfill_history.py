"""
FORTIX — Historical Data Backfill
========================================
Backfills market.db with historical data from free APIs for training.

Sources:
  - Fear & Greed Index: alternative.me (full history since 2018)
  - Binance Funding Rates: per coin, paginated (6+ months)
  - DeFi Llama: historical chain TVL (years of data)

Usage:
    python src/crypto/backfill_history.py              # run all backfills
    python src/crypto/backfill_history.py --fg-only     # Fear & Greed only
    python src/crypto/backfill_history.py --funding-only # funding rates only
    python src/crypto/backfill_history.py --tvl-only     # TVL only
    python src/crypto/backfill_history.py --verify       # verify data coverage
"""

import sys
import time
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.crypto.data_collector import (
    init_db, DB_PATH, TRACKED_COINS,
    FUTURES_SYMBOL_MAP, FUTURES_PRICE_DIVISOR
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('backfill')


class HistoryBackfiller:
    """Backfill market.db with historical data from free APIs."""

    FG_URL = 'https://api.alternative.me/fng/'
    BINANCE_FUNDING_URL = 'https://fapi.binance.com/fapi/v1/fundingRate'
    DEFI_LLAMA_URL = 'https://api.llama.fi'

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ────────────────────────────────────────────
    # Fear & Greed — Full History
    # ────────────────────────────────────────────

    def backfill_fear_greed(self) -> int:
        """Fetch complete Fear & Greed history from Alternative.me (since 2018).

        Returns number of rows stored.
        """
        log.info("[Backfill] Fear & Greed — fetching full history...")

        resp = requests.get(self.FG_URL, params={'limit': 0, 'format': 'json'}, timeout=30)
        resp.raise_for_status()
        data = resp.json().get('data', [])

        if not data:
            log.warning("  No F&G data returned")
            return 0

        count = 0
        for entry in data:
            ts = int(entry['timestamp'])
            date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            self.conn.execute(
                "INSERT OR REPLACE INTO fear_greed (date, value, classification) "
                "VALUES (?, ?, ?)",
                (date, int(entry['value']), entry['value_classification'])
            )
            count += 1

        self.conn.commit()

        # Verify
        row = self.conn.execute(
            "SELECT MIN(date), MAX(date), COUNT(*) FROM fear_greed"
        ).fetchone()
        log.info(f"  Stored {count} days of F&G data")
        log.info(f"  Range: {row[0]} to {row[1]} ({row[2]} total rows)")
        return count

    # ────────────────────────────────────────────
    # Binance Funding Rates — Historical Paginated
    # ────────────────────────────────────────────

    def _futures_raw_symbol(self, coin: str) -> str:
        """Get raw futures symbol for Binance REST API."""
        mapped = FUTURES_SYMBOL_MAP.get(coin)
        if mapped:
            return mapped.replace('/USDT', 'USDT')
        return f'{coin}USDT'

    def backfill_funding_rates(self, start_date: str = '2025-02-01',
                                coins: list = None) -> int:
        """Fetch historical funding rates from Binance per coin.

        Binance API: /fapi/v1/fundingRate with pagination (limit=1000).
        Funding rates are every 8 hours, so 1000 entries = ~333 days.

        Args:
            start_date: earliest date to fetch (YYYY-MM-DD)
            coins: list of coins to fetch (default: all TRACKED_COINS)

        Returns total rows stored.
        """
        coins = coins or TRACKED_COINS
        start_ms = int(datetime.strptime(start_date, '%Y-%m-%d').replace(
            tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        log.info(f"[Backfill] Funding rates — {len(coins)} coins from {start_date}...")
        total_count = 0

        for coin in coins:
            symbol = self._futures_raw_symbol(coin)
            coin_count = 0
            cursor_ms = start_ms

            while cursor_ms < end_ms:
                try:
                    resp = requests.get(
                        self.BINANCE_FUNDING_URL,
                        params={
                            'symbol': symbol,
                            'startTime': cursor_ms,
                            'endTime': end_ms,
                            'limit': 1000,
                        },
                        timeout=15
                    )

                    if resp.status_code == 400:
                        # Symbol not found on futures
                        log.debug(f"  {coin} ({symbol}): not available on futures")
                        break

                    resp.raise_for_status()
                    data = resp.json()

                    if not data:
                        break

                    rows = []
                    for entry in data:
                        ts = int(entry['fundingTime']) // 1000  # ms → seconds
                        rate = float(entry['fundingRate'])
                        rows.append((coin, ts, rate))

                    self.conn.executemany(
                        "INSERT OR REPLACE INTO funding_rates (coin, timestamp, rate) "
                        "VALUES (?, ?, ?)", rows
                    )
                    coin_count += len(rows)

                    # Move cursor past last entry
                    last_time_ms = int(data[-1]['fundingTime'])
                    if last_time_ms <= cursor_ms:
                        break  # No progress, stop
                    cursor_ms = last_time_ms + 1

                    # Rate limit
                    time.sleep(0.15)

                except requests.exceptions.HTTPError as e:
                    log.warning(f"  {coin}: HTTP {e}")
                    break
                except Exception as e:
                    log.warning(f"  {coin}: {e}")
                    break

            if coin_count > 0:
                log.info(f"  {coin}: {coin_count} funding rate entries")
            total_count += coin_count

        self.conn.commit()

        # Verify
        row = self.conn.execute(
            "SELECT COUNT(*), MIN(datetime(timestamp, 'unixepoch')), "
            "MAX(datetime(timestamp, 'unixepoch')) FROM funding_rates"
        ).fetchone()
        log.info(f"  Total: {row[0]} funding rate entries ({row[1]} to {row[2]})")
        return total_count

    # ────────────────────────────────────────────
    # DeFi Llama — Historical Chain TVL
    # ────────────────────────────────────────────

    def backfill_defi_tvl(self, days_back: int = 365) -> int:
        """Fetch historical total chain TVL from DeFi Llama.

        Endpoint: /v2/historicalChainTvl returns daily TVL going back years.

        Returns number of rows stored.
        """
        log.info(f"[Backfill] DeFi Llama — historical chain TVL...")

        resp = requests.get(
            f'{self.DEFI_LLAMA_URL}/v2/historicalChainTvl',
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            log.warning("  No TVL data returned")
            return 0

        # Filter to last N days
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
        cutoff_ts = cutoff.timestamp()

        count = 0
        prev_tvl = None
        for entry in data:
            ts = entry.get('date', 0)
            if ts < cutoff_ts:
                prev_tvl = entry.get('tvl', 0)
                continue

            tvl = entry.get('tvl', 0)
            date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

            change_1d = None
            if prev_tvl and prev_tvl > 0:
                change_1d = ((tvl - prev_tvl) / prev_tvl) * 100

            self.conn.execute(
                "INSERT OR REPLACE INTO tvl (protocol, chain, date, tvl_usd, change_1d) "
                "VALUES (?, ?, ?, ?, ?)",
                ('_total_chain', None, date_str, tvl, change_1d)
            )
            prev_tvl = tvl
            count += 1

        # Also fetch per-chain historical TVL for major chains
        major_chains = ['Ethereum', 'BSC', 'Solana', 'Arbitrum', 'Polygon', 'Avalanche', 'Optimism', 'Base']
        for chain in major_chains:
            try:
                resp = requests.get(
                    f'{self.DEFI_LLAMA_URL}/v2/historicalChainTvl/{chain}',
                    timeout=30
                )
                if resp.status_code != 200:
                    continue

                chain_data = resp.json()
                prev_tvl = None
                for entry in chain_data:
                    ts = entry.get('date', 0)
                    if ts < cutoff_ts:
                        prev_tvl = entry.get('tvl', 0)
                        continue

                    tvl = entry.get('tvl', 0)
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

                    change_1d = None
                    if prev_tvl and prev_tvl > 0:
                        change_1d = ((tvl - prev_tvl) / prev_tvl) * 100

                    self.conn.execute(
                        "INSERT OR REPLACE INTO tvl (protocol, chain, date, tvl_usd, change_1d) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (f'_chain_{chain.lower()}', chain, date_str, tvl, change_1d)
                    )
                    prev_tvl = tvl
                    count += 1

                time.sleep(0.2)
            except Exception as e:
                log.warning(f"  Chain {chain}: {e}")

        self.conn.commit()

        row = self.conn.execute(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM tvl"
        ).fetchone()
        log.info(f"  Stored {count} TVL entries ({row[1]} to {row[2]}, {row[0]} total)")
        return count

    # ────────────────────────────────────────────
    # Run All + Verify
    # ────────────────────────────────────────────

    def run_full_backfill(self) -> dict:
        """Run all backfill operations. Returns summary dict."""
        log.info("=" * 60)
        log.info("FULL HISTORICAL BACKFILL")
        log.info("=" * 60)

        results = {}

        # 1. Fear & Greed (single API call, fast)
        results['fear_greed'] = self.backfill_fear_greed()

        # 2. Funding rates (paginated, ~25 coins × 1-2 pages each)
        results['funding_rates'] = self.backfill_funding_rates()

        # 3. DeFi Llama TVL
        results['tvl'] = self.backfill_defi_tvl()

        log.info("=" * 60)
        log.info("BACKFILL COMPLETE")
        for key, count in results.items():
            log.info(f"  {key}: {count} entries")
        log.info("=" * 60)

        return results

    def verify_data_coverage(self) -> dict:
        """Check date ranges for all key tables. Returns coverage report."""
        tables = {
            'prices': ("timestamp", True),
            'fear_greed': ("date", False),
            'funding_rates': ("timestamp", True),
            'long_short_ratio': ("timestamp", True),
            'taker_volume': ("timestamp", True),
            'liquidations': ("timestamp", True),
            'news': ("timestamp", True),
            'tvl': ("date", False),
            'whale_transactions': ("timestamp", True),
            'social_sentiment': ("date", False),
            'global_metrics': ("date", False),
        }

        report = {}
        log.info("\nDATA COVERAGE REPORT")
        log.info("-" * 60)

        for table, (col, is_unix) in tables.items():
            try:
                if is_unix:
                    row = self.conn.execute(
                        f"SELECT COUNT(*), MIN(datetime({col}, 'unixepoch')), "
                        f"MAX(datetime({col}, 'unixepoch')) FROM {table}"
                    ).fetchone()
                else:
                    row = self.conn.execute(
                        f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {table}"
                    ).fetchone()

                report[table] = {
                    'count': row[0],
                    'min_date': row[1],
                    'max_date': row[2],
                }
                log.info(f"  {table:25s}: {row[0]:>6} rows  |  {row[1] or 'N/A'} → {row[2] or 'N/A'}")
            except Exception as e:
                report[table] = {'count': 0, 'error': str(e)}
                log.warning(f"  {table:25s}: ERROR — {e}")

        # Per-coin price coverage
        log.info("\n  Per-coin 1d prices:")
        coin_rows = self.conn.execute(
            "SELECT coin, COUNT(*), MIN(datetime(timestamp, 'unixepoch')), "
            "MAX(datetime(timestamp, 'unixepoch')) FROM prices "
            "WHERE timeframe='1d' GROUP BY coin ORDER BY coin"
        ).fetchall()
        for coin, cnt, min_d, max_d in coin_rows:
            log.info(f"    {coin:6s}: {cnt:>4} days  |  {min_d[:10]} → {max_d[:10]}")

        # Per-coin funding rates
        log.info("\n  Per-coin funding rates:")
        fr_rows = self.conn.execute(
            "SELECT coin, COUNT(*), MIN(datetime(timestamp, 'unixepoch')), "
            "MAX(datetime(timestamp, 'unixepoch')) FROM funding_rates "
            "GROUP BY coin ORDER BY coin"
        ).fetchall()
        for coin, cnt, min_d, max_d in fr_rows:
            log.info(f"    {coin:6s}: {cnt:>5} entries  |  {min_d[:10]} → {max_d[:10]}")

        return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Backfill historical data')
    parser.add_argument('--fg-only', action='store_true', help='Fear & Greed only')
    parser.add_argument('--funding-only', action='store_true', help='Funding rates only')
    parser.add_argument('--tvl-only', action='store_true', help='TVL only')
    parser.add_argument('--verify', action='store_true', help='Verify data coverage')
    parser.add_argument('--start-date', default='2025-02-01',
                        help='Start date for funding rates (default: 2025-02-01)')
    args = parser.parse_args()

    conn = init_db()
    backfiller = HistoryBackfiller(conn)

    if args.verify:
        backfiller.verify_data_coverage()
    elif args.fg_only:
        backfiller.backfill_fear_greed()
    elif args.funding_only:
        backfiller.backfill_funding_rates(start_date=args.start_date)
    elif args.tvl_only:
        backfiller.backfill_defi_tvl()
    else:
        backfiller.run_full_backfill()
        backfiller.verify_data_coverage()

    conn.close()
