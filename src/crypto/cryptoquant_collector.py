"""
FORTIX — CryptoQuant Data Collector
==========================================
Collects on-chain indicators and exchange flow data from CryptoQuant API.

Professional plan provides gold-standard on-chain metrics:
  - BTC indicators: SOPR, NUPL, MVRV, Realized Price, Puell, NVT, CDD, S2F
  - Exchange flows: BTC + ETH netflow/reserve
  - ERC20 exchange flows: LINK, UNI, AAVE, MKR, CRV, SHIB
  - Coinbase Premium: institutional sentiment
  - Miner reserve: miner behavior
  - Active addresses: network activity
  - Stablecoin flows: capital movement

API: https://api.cryptoquant.com/v1
Auth: Authorization: Bearer <key>
Plan: Professional (20 req/min)

Usage:
    python src/crypto/cryptoquant_collector.py          # collect all
    python src/crypto/cryptoquant_collector.py --test    # test API connection
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
log = logging.getLogger('cryptoquant')

CRYPTOQUANT_API_KEY = os.getenv('CRYPTOQUANT_API_KEY', '')
BASE_URL = 'https://api.cryptoquant.com/v1'

# BTC on-chain metrics to collect (correct endpoint paths)
BTC_METRICS = [
    ('sopr', '/btc/market-indicator/sopr'),
    ('nupl', '/btc/network-indicator/nupl'),
    ('mvrv', '/btc/market-indicator/mvrv'),
    ('realized_price', '/btc/market-indicator/realized-price'),
    ('puell_multiple', '/btc/network-indicator/puell-multiple'),
    ('nvt', '/btc/network-indicator/nvt'),
    ('cdd', '/btc/network-indicator/cdd'),
    ('stock_to_flow', '/btc/network-indicator/stock-to-flow'),
]

# ERC20 tokens available on CryptoQuant
# Note: PENDLE replaced MKR in tracking but CQ doesn't have PENDLE flows
# MKR kept here since CQ still provides its exchange flow data
ERC20_TOKENS = ['link', 'uni', 'aave', 'mkr', 'crv', 'shib']

# Map ERC20 token names to our coin symbols
ERC20_TO_COIN = {
    'link': 'LINK', 'uni': 'UNI', 'aave': 'AAVE',
    'mkr': 'MKR', 'crv': 'CRV', 'shib': 'SHIB',
}


class CryptoQuantCollector:
    """Collect on-chain data from CryptoQuant Professional API."""

    def __init__(self):
        self.api_key = CRYPTOQUANT_API_KEY
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {self.api_key}'
        self.session.headers['accept'] = 'application/json'
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce 3.5s between requests (20 req/min plan limit)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 3.5:
            time.sleep(3.5 - elapsed)
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

            if resp.status_code == 403:
                log.warning(f"  {path}: 403 Forbidden — endpoint not available on current plan")
                return {'status': 'error', 'result': {'data': []}}

            if resp.status_code != 200:
                log.warning(f"  {path}: HTTP {resp.status_code}")
                return {'status': 'error', 'result': {'data': []}}

            data = resp.json()
            return data
        except Exception as e:
            log.warning(f"  {path}: request failed — {e}")
            return {'status': 'error', 'result': {'data': []}}

    def _extract_data(self, response: dict) -> list:
        """Extract data list from CryptoQuant response format."""
        result = response.get('result', {})
        if isinstance(result, dict):
            return result.get('data', [])
        return []

    # ════════════════════════════════════════
    # 1. BTC ON-CHAIN INDICATORS
    # ════════════════════════════════════════

    def collect_btc_onchain(self, conn: sqlite3.Connection) -> int:
        """Collect BTC on-chain indicators: SOPR, NUPL, MVRV, etc.

        These are the gold standard of on-chain analysis:
        - SOPR: Spent Output Profit Ratio (profit-taking vs capitulation)
        - NUPL: Net Unrealized Profit/Loss (market cycle stage)
        - MVRV: Market Value / Realized Value (over/undervaluation)
        - Realized Price: average cost basis of all BTC
        - Puell Multiple: miner revenue vs yearly average
        - NVT: Network Value to Transactions (blockchain P/E ratio)
        - CDD: Coin Days Destroyed (old coin movement)
        - S2F: Stock-to-Flow model deviation
        """
        log.info("[CryptoQuant] Collecting BTC on-chain indicators...")
        count = 0

        for metric_name, endpoint in BTC_METRICS:
            data = self._get(endpoint, {'window': 'day', 'limit': 365})
            entries = self._extract_data(data)

            if not entries:
                log.warning(f"  No data for {metric_name}")
                continue

            for entry in entries:
                date = entry.get('date', '')
                if not date:
                    continue
                # Normalize date to YYYY-MM-DD
                date = date[:10]

                # CryptoQuant returns the metric value in different field names
                value = None
                for key in ['value', metric_name, 'sopr', 'nupl', 'mvrv',
                            'realized_price', 'puell_multiple', 'nvt', 'cdd',
                            'stock_to_flow', 'stock_to_flow_reversion']:
                    if key in entry and entry[key] is not None:
                        try:
                            value = float(entry[key])
                            break
                        except (ValueError, TypeError):
                            continue

                if value is None:
                    continue

                conn.execute(
                    "INSERT OR REPLACE INTO cq_btc_onchain "
                    "(date, metric, value) VALUES (?, ?, ?)",
                    (date, metric_name, value)
                )
                count += 1

            log.info(f"  {metric_name}: {len(entries)} entries")

        conn.commit()
        log.info(f"  BTC on-chain total: {count} rows")
        return count

    # ════════════════════════════════════════
    # 2. EXCHANGE FLOWS — BTC + ETH
    # ════════════════════════════════════════

    def collect_exchange_flows(self, conn: sqlite3.Connection) -> int:
        """Collect BTC and ETH exchange netflow and reserve.

        Netflow negative = outflow = accumulation (bullish)
        Netflow positive = inflow = sell pressure (bearish)
        """
        log.info("[CryptoQuant] Collecting exchange flows (BTC + ETH)...")
        count = 0

        for chain, coin in [('btc', 'BTC'), ('eth', 'ETH')]:
            # Netflow
            data = self._get(
                f'/{chain}/exchange-flows/netflow',
                {'exchange': 'all_exchange', 'window': 'day', 'limit': 365}
            )
            entries = self._extract_data(data)

            for entry in entries:
                date = (entry.get('date', '') or '')[:10]
                if not date:
                    continue
                netflow = entry.get('netflow_total', entry.get('value', 0))
                try:
                    netflow = float(netflow) if netflow is not None else 0
                except (ValueError, TypeError):
                    netflow = 0

                conn.execute(
                    "INSERT OR REPLACE INTO cq_exchange_flows "
                    "(date, coin, netflow, reserve, reserve_usd) "
                    "VALUES (?, ?, ?, NULL, NULL)",
                    (date, coin, netflow)
                )
                count += 1

            # Reserve
            data = self._get(
                f'/{chain}/exchange-flows/reserve',
                {'exchange': 'all_exchange', 'window': 'day', 'limit': 365}
            )
            entries = self._extract_data(data)

            for entry in entries:
                date = (entry.get('date', '') or '')[:10]
                if not date:
                    continue
                reserve = entry.get('reserve', entry.get('value', 0))
                reserve_usd = entry.get('reserve_usd', 0)
                try:
                    reserve = float(reserve) if reserve is not None else 0
                    reserve_usd = float(reserve_usd) if reserve_usd is not None else 0
                except (ValueError, TypeError):
                    reserve = 0
                    reserve_usd = 0

                conn.execute(
                    "UPDATE cq_exchange_flows SET reserve = ?, reserve_usd = ? "
                    "WHERE date = ? AND coin = ?",
                    (reserve, reserve_usd, date, coin)
                )
                # If no row to update, insert
                if conn.execute(
                    "SELECT COUNT(*) FROM cq_exchange_flows WHERE date = ? AND coin = ?",
                    (date, coin)
                ).fetchone()[0] == 0:
                    conn.execute(
                        "INSERT INTO cq_exchange_flows "
                        "(date, coin, netflow, reserve, reserve_usd) "
                        "VALUES (?, ?, 0, ?, ?)",
                        (date, coin, reserve, reserve_usd)
                    )
                count += 1

        conn.commit()
        log.info(f"  Exchange flows: {count} rows (BTC + ETH)")
        return count

    # ════════════════════════════════════════
    # 3. ERC20 EXCHANGE FLOWS
    # ════════════════════════════════════════

    def collect_erc20_flows(self, conn: sqlite3.Connection) -> int:
        """Collect ERC20 token exchange netflows.

        Available: LINK, UNI, AAVE, MKR, CRV, SHIB
        Not available: LDO, PEPE, WIF, BONK, FET, RENDER, TAO, ARB, OP
        """
        log.info("[CryptoQuant] Collecting ERC20 exchange flows...")
        count = 0

        for token in ERC20_TOKENS:
            coin = ERC20_TO_COIN[token]
            data = self._get(
                '/erc20/exchange-flows/netflow',
                {'token': token, 'exchange': 'all_exchange', 'window': 'day', 'limit': 365}
            )
            entries = self._extract_data(data)

            if not entries:
                log.warning(f"  No ERC20 flow data for {token}")
                continue

            for entry in entries:
                date = (entry.get('date', '') or '')[:10]
                if not date:
                    continue
                netflow = entry.get('netflow_total', entry.get('value', 0))
                try:
                    netflow = float(netflow) if netflow is not None else 0
                except (ValueError, TypeError):
                    netflow = 0

                conn.execute(
                    "INSERT OR REPLACE INTO cq_exchange_flows "
                    "(date, coin, netflow, reserve, reserve_usd) "
                    "VALUES (?, ?, ?, NULL, NULL)",
                    (date, coin, netflow)
                )
                count += 1

            log.info(f"  {coin}: {len(entries)} entries")

        conn.commit()
        log.info(f"  ERC20 flows: {count} rows")
        return count

    # ════════════════════════════════════════
    # 4. COINBASE PREMIUM — institutional sentiment
    # ════════════════════════════════════════

    def collect_coinbase_premium(self, conn: sqlite3.Connection) -> int:
        """Collect Coinbase Premium Index.

        Positive premium = US institutional demand (bullish)
        Negative premium = US selling pressure (bearish)
        """
        log.info("[CryptoQuant] Collecting Coinbase premium...")
        count = 0

        data = self._get(
            '/btc/market-data/coinbase-premium-index',
            {'window': 'day', 'limit': 365}
        )
        entries = self._extract_data(data)

        if not entries:
            log.warning("  No Coinbase premium data")
            return 0

        for entry in entries:
            date = (entry.get('date', '') or '')[:10]
            if not date:
                continue

            premium_index = entry.get('coinbase_premium_index',
                            entry.get('value', 0))
            premium_gap = entry.get('coinbase_premium_gap', 0)
            try:
                premium_index = float(premium_index) if premium_index is not None else 0
                premium_gap = float(premium_gap) if premium_gap is not None else 0
            except (ValueError, TypeError):
                premium_index = 0
                premium_gap = 0

            conn.execute(
                "INSERT OR REPLACE INTO cq_coinbase_premium "
                "(date, premium_index, premium_gap) VALUES (?, ?, ?)",
                (date, premium_index, premium_gap)
            )
            count += 1

        conn.commit()
        log.info(f"  Coinbase premium: {count} entries")
        return count

    # ════════════════════════════════════════
    # 5. MINER DATA — BTC miner behavior
    # ════════════════════════════════════════

    def collect_miner_data(self, conn: sqlite3.Connection) -> int:
        """Collect BTC miner reserve data.

        Miners selling = bearish (liquidating BTC for operating costs)
        Miners accumulating = bullish (confident in higher prices)
        """
        log.info("[CryptoQuant] Collecting miner reserve...")
        count = 0

        data = self._get(
            '/btc/miner-flows/reserve',
            {'miner': 'all_miner', 'window': 'day', 'limit': 365}
        )
        entries = self._extract_data(data)

        if not entries:
            log.warning("  No miner reserve data")
            return 0

        for entry in entries:
            date = (entry.get('date', '') or '')[:10]
            if not date:
                continue

            reserve = entry.get('reserve', entry.get('value', 0))
            reserve_usd = entry.get('reserve_usd', 0)
            try:
                reserve = float(reserve) if reserve is not None else 0
                reserve_usd = float(reserve_usd) if reserve_usd is not None else 0
            except (ValueError, TypeError):
                reserve = 0
                reserve_usd = 0

            conn.execute(
                "INSERT OR REPLACE INTO cq_miner_data "
                "(date, reserve, reserve_usd) VALUES (?, ?, ?)",
                (date, reserve, reserve_usd)
            )
            count += 1

        conn.commit()
        log.info(f"  Miner reserve: {count} entries")
        return count

    # ════════════════════════════════════════
    # 6. ACTIVE ADDRESSES — network activity
    # ════════════════════════════════════════

    def collect_active_addresses(self, conn: sqlite3.Connection) -> int:
        """Collect active address counts for BTC and ETH.

        Rising active addresses = growing network usage (bullish)
        Falling = declining activity (bearish)
        """
        log.info("[CryptoQuant] Collecting active addresses...")
        count = 0

        for chain, coin in [('btc', 'BTC'), ('eth', 'ETH')]:
            data = self._get(
                f'/{chain}/network-data/addresses-count',
                {'window': 'day', 'limit': 365}
            )
            entries = self._extract_data(data)

            if not entries:
                log.warning(f"  No active address data for {coin}")
                continue

            for entry in entries:
                date = (entry.get('date', '') or '')[:10]
                if not date:
                    continue

                active = entry.get('addresses_count_active',
                         entry.get('value', 0))
                try:
                    active = int(float(active)) if active is not None else 0
                except (ValueError, TypeError):
                    active = 0

                conn.execute(
                    "INSERT OR REPLACE INTO cq_active_addresses "
                    "(date, coin, active, sender, receiver) "
                    "VALUES (?, ?, ?, 0, 0)",
                    (date, coin, active)
                )
                count += 1

            log.info(f"  {coin} active addresses: {len(entries)} entries")

        conn.commit()
        log.info(f"  Active addresses: {count} rows")
        return count

    # ════════════════════════════════════════
    # 7. STABLECOIN FLOWS
    # ════════════════════════════════════════

    def collect_stablecoin_flows(self, conn: sqlite3.Connection) -> int:
        """Collect stablecoin exchange netflow.

        Inflow to exchanges = buying power arriving (bullish)
        Outflow from exchanges = capital leaving (bearish)
        """
        log.info("[CryptoQuant] Collecting stablecoin flows...")
        count = 0

        data = self._get(
            '/stablecoin/exchange-flows/netflow',
            {'token': 'all_token', 'exchange': 'all_exchange',
             'window': 'day', 'limit': 365}
        )
        entries = self._extract_data(data)

        if not entries:
            log.warning("  No stablecoin flow data")
            return 0

        for entry in entries:
            date = (entry.get('date', '') or '')[:10]
            if not date:
                continue

            netflow = entry.get('netflow_total', entry.get('value', 0))
            try:
                netflow = float(netflow) if netflow is not None else 0
            except (ValueError, TypeError):
                netflow = 0

            conn.execute(
                "INSERT OR REPLACE INTO cq_stablecoin_flows "
                "(date, netflow) VALUES (?, ?)",
                (date, netflow)
            )
            count += 1

        conn.commit()
        log.info(f"  Stablecoin flows: {count} entries")
        return count


def collect_all_cryptoquant(conn: sqlite3.Connection) -> int:
    """Run all CryptoQuant collectors. Returns total rows stored."""
    if not CRYPTOQUANT_API_KEY:
        log.info("[CryptoQuant] Skipped — no API key (set CRYPTOQUANT_API_KEY)")
        return 0

    cq = CryptoQuantCollector()
    total = 0

    collectors = [
        ('btc_onchain', cq.collect_btc_onchain),
        ('exchange_flows', cq.collect_exchange_flows),
        ('erc20_flows', cq.collect_erc20_flows),
        ('coinbase_premium', cq.collect_coinbase_premium),
        ('miner_data', cq.collect_miner_data),
        ('active_addresses', cq.collect_active_addresses),
        ('stablecoin_flows', cq.collect_stablecoin_flows),
    ]

    for name, fn in collectors:
        try:
            result = fn(conn)
            total += result
        except Exception as e:
            log.error(f"  CryptoQuant {name} failed: {e}")

    log.info(f"[CryptoQuant] Total: {total} rows stored")
    return total


if __name__ == '__main__':
    from src.crypto.data_collector import init_db

    if '--test' in sys.argv:
        if not CRYPTOQUANT_API_KEY:
            print("ERROR: CRYPTOQUANT_API_KEY not set in .env")
            sys.exit(1)
        cq = CryptoQuantCollector()
        data = cq._get('/btc/market-indicator/sopr', {'window': 'day', 'limit': 1})
        entries = cq._extract_data(data)
        if entries:
            print(f"API OK — got SOPR data: {entries[0]}")
        else:
            print(f"API FAILED — response: {data}")
    else:
        conn = init_db()
        result = collect_all_cryptoquant(conn)
        conn.close()
        print(f"\nCryptoQuant collection complete: {result} total rows")
