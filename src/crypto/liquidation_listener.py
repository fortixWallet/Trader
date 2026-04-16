"""
FORTIX — Liquidation WebSocket Listener
===============================================
Connects to Binance Futures WebSocket and saves liquidation events to SQLite.

Stream: wss://fstream.binance.com/ws/!forceOrder@arr
- Public, no API key needed
- Pushes all market liquidations in real-time (~1/sec snapshot)

Usage:
    python src/crypto/liquidation_listener.py              # Run standalone
    python src/crypto/liquidation_listener.py --status      # Show 24h stats

From code:
    from src.crypto.liquidation_listener import LiquidationListener
    listener = LiquidationListener()
    listener.start()   # Non-blocking (background thread)
    listener.stop()
"""

import sys
import json
import time
import sqlite3
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone

import websocket

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('liquidation_listener')

DB_PATH = Path('data/crypto/market.db')
WS_URL = 'wss://fstream.binance.com/ws/!forceOrder@arr'

# Reverse map: futures symbol → our coin symbol
# e.g. '1000SHIBUSDT' → 'SHIB', 'POLUSDT' → 'POL'
REVERSE_SYMBOL_MAP = {
    'POLUSDT': 'POL',
    'RENDERUSDT': 'RENDER',
    '1000SHIBUSDT': 'SHIB',
    '1000PEPEUSDT': 'PEPE',
    '1000BONKUSDT': 'BONK',
}

# Coins we track (only save liquidations for these)
TRACKED_COINS = {
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'POL',
    'UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV',
    'ARB', 'OP',
    'FET', 'RENDER', 'TAO',
    'DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK',
}

# Price divisor for 1000x lot coins
PRICE_DIVISOR = {'SHIB': 1000, 'PEPE': 1000, 'BONK': 1000}


class LiquidationListener:
    """WebSocket listener for Binance Futures liquidations."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self._ws = None
        self._thread = None
        self._running = False
        self._count = 0
        self._count_total = 0  # including non-tracked
        self._lock = threading.Lock()
        self._reconnect_delay = 5

    def _resolve_symbol(self, raw_symbol: str) -> tuple:
        """Convert Binance futures symbol to our coin symbol.

        Returns (coin, divisor) or (None, 1) if not tracked.
        """
        # Check reverse map first (rebrands + 1000x)
        if raw_symbol in REVERSE_SYMBOL_MAP:
            coin = REVERSE_SYMBOL_MAP[raw_symbol]
            return coin, PRICE_DIVISOR.get(coin, 1)

        # Standard: strip USDT suffix
        if raw_symbol.endswith('USDT'):
            coin = raw_symbol[:-4]
            if coin in TRACKED_COINS:
                return coin, PRICE_DIVISOR.get(coin, 1)

        return None, 1

    def _on_message(self, ws, message):
        """Process a liquidation event."""
        try:
            data = json.loads(message)
            order = data.get('o', {})

            raw_symbol = order.get('s', '')
            self._count_total += 1

            coin, divisor = self._resolve_symbol(raw_symbol)
            if coin is None:
                return  # Not a tracked coin

            # Parse fields
            trade_time = int(order.get('T', 0))
            ts = trade_time // 1000 if trade_time > 1e12 else trade_time

            avg_price = float(order.get('ap', 0))
            qty = float(order.get('z', order.get('q', 0)))  # z = filled qty

            # Adjust for 1000x lot coins
            if divisor > 1:
                avg_price = avg_price / divisor
                qty = qty * divisor

            notional = avg_price * qty

            # Side: SELL order = liquidated LONG, BUY order = liquidated SHORT
            order_side = order.get('S', '')
            liq_side = 'LONG' if order_side == 'SELL' else 'SHORT'

            liq_id = f"ws_{coin}_{ts}_{order_side}_{notional:.0f}"

            # Save to DB
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO liquidations "
                        "(id, coin, timestamp, side, price, quantity, notional_usd) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (liq_id, coin, ts, liq_side, avg_price, qty, notional)
                    )
                    conn.commit()
                finally:
                    conn.close()

                with self._lock:
                    self._count += 1

                if notional > 100_000:
                    log.info(f"  LIQUIDATION: {coin} {liq_side} ${notional:,.0f} @ {avg_price}")

            except sqlite3.Error as e:
                log.debug(f"  DB error: {e}")

        except Exception as e:
            log.debug(f"  Parse error: {e}")

    def _on_error(self, ws, error):
        log.warning(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        log.info(f"WebSocket closed (code={close_status_code})")
        if self._running:
            log.info(f"  Reconnecting in {self._reconnect_delay}s...")
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, 60)
            self._connect()

    def _on_open(self, ws):
        log.info("Connected to Binance liquidation stream")
        self._reconnect_delay = 5  # Reset on successful connect

    def _connect(self):
        """Create and run WebSocket connection."""
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        self._ws.run_forever(ping_interval=30, ping_timeout=10)

    def start(self):
        """Start listener in background thread."""
        if self._running:
            log.warning("Listener already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._connect, daemon=True)
        self._thread.start()
        log.info("Liquidation listener started (background)")

    def stop(self):
        """Stop the listener."""
        self._running = False
        if self._ws:
            self._ws.close()
        log.info(f"Liquidation listener stopped ({self._count} tracked events saved)")

    @property
    def count(self):
        with self._lock:
            return self._count

    @staticmethod
    def get_24h_stats(conn: sqlite3.Connection) -> dict:
        """Get liquidation statistics for the last 24 hours."""
        cutoff = int(time.time()) - 86400

        rows = conn.execute(
            "SELECT coin, side, COUNT(*) as cnt, SUM(notional_usd) as total "
            "FROM liquidations WHERE timestamp > ? "
            "GROUP BY coin, side ORDER BY total DESC",
            (cutoff,)
        ).fetchall()

        stats = {}
        total_long = 0.0
        total_short = 0.0

        for coin, side, cnt, total in rows:
            if coin not in stats:
                stats[coin] = {'long_count': 0, 'short_count': 0,
                               'long_usd': 0.0, 'short_usd': 0.0}
            if side == 'LONG':
                stats[coin]['long_count'] = cnt
                stats[coin]['long_usd'] = total or 0.0
                total_long += total or 0.0
            else:
                stats[coin]['short_count'] = cnt
                stats[coin]['short_usd'] = total or 0.0
                total_short += total or 0.0

        return {
            'coins': stats,
            'total_long_usd': total_long,
            'total_short_usd': total_short,
            'total_usd': total_long + total_short,
            'period_hours': 24,
        }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX — Liquidation Listener')
    parser.add_argument('--status', action='store_true', help='Show 24h liquidation stats')
    args = parser.parse_args()

    if args.status:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        stats = LiquidationListener.get_24h_stats(conn)

        total_count = conn.execute(
            "SELECT COUNT(*) FROM liquidations WHERE timestamp > ?",
            (int(time.time()) - 86400,)
        ).fetchone()[0]

        print(f"\n{'='*55}")
        print(f"  LIQUIDATIONS (last 24h)")
        print(f"{'='*55}")
        print(f"  Total: ${stats['total_usd']:,.0f}")
        print(f"  Longs liquidated:  ${stats['total_long_usd']:,.0f}")
        print(f"  Shorts liquidated: ${stats['total_short_usd']:,.0f}")
        print(f"  Events: {total_count}")
        print(f"\n  Per coin:")
        for coin, s in sorted(stats['coins'].items(),
                               key=lambda x: x[1]['long_usd'] + x[1]['short_usd'],
                               reverse=True)[:15]:
            total = s['long_usd'] + s['short_usd']
            print(f"    {coin:<6} ${total:>12,.0f}  "
                  f"(L: ${s['long_usd']:>10,.0f} / S: ${s['short_usd']:>10,.0f})")
        print(f"{'='*55}")
        conn.close()
    else:
        # Run as standalone listener
        from src.crypto.data_collector import init_db
        init_db()  # Ensure tables exist

        listener = LiquidationListener()
        print(f"\n{'='*55}")
        print(f"  ALPHA SIGNAL — Liquidation Listener")
        print(f"  Stream: {WS_URL}")
        print(f"  Tracking: {len(TRACKED_COINS)} coins")
        print(f"  Press Ctrl+C to stop")
        print(f"{'='*55}\n")

        listener.start()

        try:
            while True:
                time.sleep(60)
                log.info(f"  Stats: {listener.count} tracked / "
                         f"{listener._count_total} total events")
        except KeyboardInterrupt:
            listener.stop()
