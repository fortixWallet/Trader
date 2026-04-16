"""
OKX WebSocket Price Stream — Real-Time Prices
==============================================
Replaces REST polling (60s delay) with WebSocket (10-50ms delay).

Usage:
    stream = PriceStream(['BTC', 'ETH', 'SOL'])
    stream.start()

    # Get latest price instantly (no API call)
    price = stream.get_price('BTC')  # returns immediately

    # Register callback for price changes
    stream.on_price_update = my_callback
"""

import json
import time
import logging
import threading
import websocket
from typing import Dict, Callable, Optional

logger = logging.getLogger(__name__)

OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_DEMO = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"


class PriceStream:
    """Real-time price stream via WebSocket."""

    def __init__(self, coins: list, demo: bool = True):
        self.coins = coins
        self._url = OKX_WS_DEMO if demo else OKX_WS_PUBLIC
        self._prices: Dict[str, float] = {}
        self._bids: Dict[str, float] = {}
        self._asks: Dict[str, float] = {}
        self._last_update: Dict[str, float] = {}
        self._ws = None
        self._thread = None
        self._running = False
        self._connected = False
        self._reconnect_count = 0
        self.on_price_update: Optional[Callable] = None  # callback(coin, price, bid, ask)

    def start(self):
        """Start WebSocket connection in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        # Wait for connection
        for _ in range(50):  # 5 seconds max
            if self._connected:
                logger.info(f"WS connected, streaming {len(self.coins)} coins")
                return
            time.sleep(0.1)
        logger.warning("WS connection timeout, will retry in background")

    def stop(self):
        """Stop WebSocket."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def get_price(self, coin: str) -> float:
        """Get latest price. Returns 0 if no data yet."""
        return self._prices.get(coin, 0.0)

    def get_bid_ask(self, coin: str) -> tuple:
        """Get (bid, ask) for a coin."""
        return self._bids.get(coin, 0.0), self._asks.get(coin, 0.0)

    def get_age(self, coin: str) -> float:
        """Seconds since last update for this coin."""
        last = self._last_update.get(coin, 0)
        return time.time() - last if last > 0 else 999

    def is_connected(self) -> bool:
        return self._connected

    def _run(self):
        """Main WS loop with auto-reconnect."""
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    self._url,
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
                logger.info(f"WS reconnecting in {delay}s (attempt {self._reconnect_count})")
                time.sleep(delay)

    def _on_open(self, ws):
        """Subscribe to ticker channels for all coins."""
        self._connected = True
        self._reconnect_count = 0

        # Subscribe to tickers (best bid/ask + last price)
        args = []
        for coin in self.coins:
            args.append({"channel": "tickers", "instId": f"{coin}-USDT-SWAP"})

        subscribe_msg = {"op": "subscribe", "args": args}
        ws.send(json.dumps(subscribe_msg))
        logger.info(f"WS subscribed to {len(self.coins)} tickers")

    def _on_message(self, ws, message):
        """Process incoming price updates."""
        try:
            data = json.loads(message)

            # Subscription confirmation
            if data.get("event") == "subscribe":
                return

            # Price data
            if "data" in data and data.get("arg", {}).get("channel") == "tickers":
                for tick in data["data"]:
                    inst_id = tick.get("instId", "")
                    coin = inst_id.replace("-USDT-SWAP", "")

                    last = float(tick.get("last", 0) or 0)
                    bid = float(tick.get("bidPx", 0) or 0)
                    ask = float(tick.get("askPx", 0) or 0)

                    if last > 0:
                        self._prices[coin] = last
                        self._bids[coin] = bid
                        self._asks[coin] = ask
                        self._last_update[coin] = time.time()

                        # Callback
                        if self.on_price_update:
                            try:
                                self.on_price_update(coin, last, bid, ask)
                            except Exception as e:
                                logger.debug(f"Callback error: {e}")

        except Exception as e:
            logger.debug(f"WS parse error: {e}")

    def _on_error(self, ws, error):
        logger.warning(f"WS error: {error}")
        self._connected = False

    def _on_close(self, ws, close_status, close_msg):
        self._connected = False
        logger.info(f"WS closed: {close_status} {close_msg}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    coins = ['BTC', 'ETH', 'SOL', 'ARB', 'DOGE']

    def on_update(coin, price, bid, ask):
        spread = (ask - bid) / price * 100 if price > 0 else 0
        print(f"  {coin:6s} ${price:<12.4f} bid=${bid:.4f} ask=${ask:.4f} spread={spread:.4f}%")

    stream = PriceStream(coins, demo=True)
    stream.on_price_update = on_update
    stream.start()

    print(f"Streaming {len(coins)} coins... (Ctrl+C to stop)")
    try:
        for i in range(30):
            time.sleep(1)
            if i % 5 == 4:
                print(f"\n--- {i+1}s | Connected: {stream.is_connected()} ---")
                for c in coins:
                    age = stream.get_age(c)
                    p = stream.get_price(c)
                    print(f"  {c}: ${p:.4f} (age: {age:.1f}s)")
    except KeyboardInterrupt:
        pass

    stream.stop()
    print("Done")
