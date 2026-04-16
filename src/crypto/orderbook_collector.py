"""
FORTIX — Binance Order Book Imbalance Collector
=================================================
Fetches order book depth from Binance public API (no key required)
and computes imbalance features for all tracked coins.

Features computed:
  - bid_ask_ratio: total bid volume / total ask volume (>1 = buy pressure)
  - bid_wall_pct: largest single bid / total bid volume (concentrated support)
  - ask_wall_pct: largest single ask / total ask volume (concentrated resistance)
  - imbalance_score: (bids - asks) / (bids + asks), ranges -1 to +1

API: https://api.binance.com/api/v3/depth
Rate limit: 1200 req/min (we use 0.5s gaps = 120 req/min, very conservative)

Usage:
    python src/crypto/orderbook_collector.py           # collect all 25 coins
    python src/crypto/orderbook_collector.py --test     # test with BTC only
"""

import sys
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('orderbook')

DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'market.db'

BINANCE_DEPTH_URL = 'https://api.binance.com/api/v3/depth'

# All 25 tracked coins (USDT pairs on Binance)
TRACKED_COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
    'DOGE', 'UNI', 'AAVE', 'FET', 'RENDER', 'TAO', 'PEPE', 'ARB', 'OP',
    'SHIB', 'BONK', 'PENDLE', 'LDO', 'CRV', 'WIF', 'POL',
]


class OrderBookCollector:
    """Collect order book imbalance data from Binance."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers['Accept'] = 'application/json'
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce 0.5s between requests (conservative for 1200/min limit)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
        self._last_request_time = time.time()

    def _fetch_depth(self, symbol: str, limit: int = 20) -> dict | None:
        """Fetch order book depth for a symbol with retry.

        Returns dict with 'bids' and 'asks' lists, or None on failure.
        Each bid/ask is [price_str, qty_str].
        """
        self._rate_limit()
        params = {'symbol': symbol, 'limit': limit}
        backoff = (3, 10, 30)

        for attempt in range(3):
            try:
                resp = self.session.get(BINANCE_DEPTH_URL, params=params, timeout=10)

                if resp.status_code == 400:
                    # Invalid symbol (coin doesn't have USDT pair)
                    return None

                if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                    wait = backoff[attempt]
                    log.warning(f"  HTTP {resp.status_code} for {symbol}, retry in {wait}s "
                                f"(attempt {attempt + 1}/3)")
                    time.sleep(wait)
                    continue

                if resp.status_code != 200:
                    log.warning(f"  {symbol}: HTTP {resp.status_code}")
                    return None

                return resp.json()

            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                if attempt < 2:
                    wait = backoff[attempt]
                    log.warning(f"  {symbol}: {e.__class__.__name__}, retry in {wait}s")
                    time.sleep(wait)
                else:
                    log.warning(f"  {symbol}: request failed after 3 attempts — {e}")
                    return None

        return None

    def _compute_imbalance(self, data: dict) -> dict | None:
        """Compute imbalance features from raw order book data.

        Returns dict with: bid_ask_ratio, bid_wall_pct, ask_wall_pct, imbalance_score.
        Returns None if data is invalid.
        """
        bids = data.get('bids', [])
        asks = data.get('asks', [])

        if not bids or not asks:
            return None

        # Parse volumes (qty at each price level)
        bid_volumes = [float(b[1]) for b in bids]
        ask_volumes = [float(a[1]) for a in asks]

        total_bids = sum(bid_volumes)
        total_asks = sum(ask_volumes)

        if total_bids == 0 or total_asks == 0:
            return None

        bid_ask_ratio = total_bids / total_asks
        bid_wall_pct = max(bid_volumes) / total_bids
        ask_wall_pct = max(ask_volumes) / total_asks
        imbalance_score = (total_bids - total_asks) / (total_bids + total_asks)

        return {
            'bid_ask_ratio': round(bid_ask_ratio, 6),
            'bid_wall_pct': round(bid_wall_pct, 6),
            'ask_wall_pct': round(ask_wall_pct, 6),
            'imbalance_score': round(imbalance_score, 6),
        }

    def _init_table(self, conn: sqlite3.Connection):
        """Create orderbook_imbalance table if not exists."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_imbalance (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                bid_ask_ratio REAL,
                bid_wall_pct REAL,
                ask_wall_pct REAL,
                imbalance_score REAL,
                PRIMARY KEY (coin, timestamp)
            )
        """)
        conn.commit()

    def collect(self, conn: sqlite3.Connection, coins: list = None) -> int:
        """Collect order book imbalance for all tracked coins.

        Args:
            conn: SQLite connection to market.db.
            coins: List of coin symbols (default: TRACKED_COINS).

        Returns:
            Number of rows stored.
        """
        coins = coins or TRACKED_COINS
        self._init_table(conn)
        now = int(time.time())
        count = 0
        skipped = []

        log.info(f"[OrderBook] Collecting depth for {len(coins)} coins...")

        for coin in coins:
            symbol = f"{coin}USDT"
            data = self._fetch_depth(symbol)

            if data is None:
                skipped.append(coin)
                continue

            features = self._compute_imbalance(data)
            if features is None:
                skipped.append(coin)
                log.warning(f"  {coin}: empty or invalid order book")
                continue

            conn.execute(
                "INSERT OR REPLACE INTO orderbook_imbalance "
                "(coin, timestamp, bid_ask_ratio, bid_wall_pct, ask_wall_pct, imbalance_score) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    coin, now,
                    features['bid_ask_ratio'],
                    features['bid_wall_pct'],
                    features['ask_wall_pct'],
                    features['imbalance_score'],
                ),
            )
            count += 1

        conn.commit()

        if skipped:
            log.warning(f"  Skipped {len(skipped)} coins (no USDT pair or error): {skipped}")
        log.info(f"  Stored orderbook imbalance for {count}/{len(coins)} coins")
        return count


def collect_orderbook():
    """Run full orderbook collection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    try:
        collector = OrderBookCollector()
        count = collector.collect(conn)
        log.info(f"[OrderBook] Done. {count} rows stored.")
        return count
    finally:
        conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Binance Order Book Imbalance Collector')
    parser.add_argument('--test', action='store_true', help='Test with BTC only')
    args = parser.parse_args()

    if args.test:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        try:
            collector = OrderBookCollector()
            count = collector.collect(conn, coins=['BTC'])
            log.info(f"[Test] BTC orderbook: {count} row(s)")

            # Show what we stored
            row = conn.execute(
                "SELECT * FROM orderbook_imbalance WHERE coin='BTC' ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if row:
                log.info(f"  coin={row[0]}, ts={row[1]}, "
                         f"bid_ask_ratio={row[2]:.4f}, bid_wall={row[3]:.4f}, "
                         f"ask_wall={row[4]:.4f}, imbalance={row[5]:.4f}")
        finally:
            conn.close()
    else:
        collect_orderbook()
