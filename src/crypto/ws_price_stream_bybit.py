"""
Bybit WebSocket Price Stream
=============================
Public data uses production URL (same data as live).
wss://stream.bybit.com/v5/public/linear
"""

import json
import time
import logging
import threading
import websocket
from typing import Dict, Callable, Optional

logger = logging.getLogger(__name__)

BYBIT_WS_PUBLIC = "wss://stream.bybit.com/v5/public/linear"


class PriceStream:
    def __init__(self, coins: list):
        self.coins = coins
        self._prices: Dict[str, float] = {}
        self._bids: Dict[str, float] = {}
        self._asks: Dict[str, float] = {}
        self._last_update: Dict[str, float] = {}
        self._ws = None
        self._thread = None
        self._running = False
        self._connected = False
        self._reconnect_count = 0
        self.on_price_update: Optional[Callable] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        for _ in range(50):
            if self._connected:
                logger.info(f"Bybit WS connected, {len(self.coins)} coins")
                return
            time.sleep(0.1)
        logger.warning("Bybit WS timeout")

    def stop(self):
        self._running = False
        if self._ws:
            try: self._ws.close()
            except: pass

    def get_price(self, coin: str) -> float:
        return self._prices.get(coin, 0.0)

    def get_bid_ask(self, coin: str) -> tuple:
        return self._bids.get(coin, 0.0), self._asks.get(coin, 0.0)

    def get_age(self, coin: str) -> float:
        last = self._last_update.get(coin, 0)
        return time.time() - last if last > 0 else 999

    def is_connected(self) -> bool:
        return self._connected

    def _run(self):
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    BYBIT_WS_PUBLIC,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.error(f"WS error: {e}")

            if self._running:
                self._connected = False
                self._reconnect_count += 1
                delay = min(30, 2 ** self._reconnect_count)
                logger.info(f"Bybit WS reconnecting in {delay}s")
                time.sleep(delay)

    def _on_open(self, ws):
        self._connected = True
        self._reconnect_count = 0

        # Bybit v5: subscribe to tickers
        # Max 10 args per subscribe message
        symbols = [f"tickers.{coin}USDT" for coin in self.coins]
        for i in range(0, len(symbols), 10):
            batch = symbols[i:i+10]
            ws.send(json.dumps({"op": "subscribe", "args": batch}))

        logger.info(f"Bybit WS subscribed to {len(self.coins)} tickers")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)

            if data.get("op") == "subscribe":
                return

            topic = data.get("topic", "")
            if topic.startswith("tickers.") and "data" in data:
                tick = data["data"]
                symbol = tick.get("symbol", "")  # e.g. "BTCUSDT"
                coin = symbol.replace("USDT", "")

                last = float(tick.get("lastPrice", 0) or 0)
                bid = float(tick.get("bid1Price", 0) or 0)
                ask = float(tick.get("ask1Price", 0) or 0)

                if last > 0:
                    self._prices[coin] = last
                    self._bids[coin] = bid
                    self._asks[coin] = ask
                    self._last_update[coin] = time.time()

                    if self.on_price_update:
                        try:
                            self.on_price_update(coin, last, bid, ask)
                        except Exception:
                            pass

        except Exception:
            pass

    def _on_error(self, ws, error):
        logger.warning(f"Bybit WS error: {error}")
        self._connected = False

    def _on_close(self, ws, close_status, close_msg):
        self._connected = False
