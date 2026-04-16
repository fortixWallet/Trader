"""
OKX Demo Trading Client — Production Quality
=============================================
Robust exchange client with:
  - Exponential backoff retry (handles 50001 "temporarily unavailable")
  - Proper error classification (retryable vs fatal)
  - Position sync (prevents duplicates)
  - Contract size cache (avoids repeated API calls)
  - Full logging for debugging
"""

import ccxt
import logging
import time
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent

# Errors that should be retried
RETRYABLE_CODES = {'50001', '50004', '50013', '50026'}  # service unavailable, rate limit, system busy
MAX_RETRIES = 5
BASE_DELAY = 1.0  # seconds


@dataclass
class Position:
    symbol: str
    side: str
    size: float
    entry_price: float
    margin: float
    leverage: float
    unrealized_pnl: float = 0.0


@dataclass
class OrderResult:
    id: str
    symbol: str
    side: str
    amount: float
    price: float
    status: str


class OKXClient:
    """OKX Demo/Live Trading — production-quality client."""

    def __init__(self, api_key: str = '', secret: str = '', passphrase: str = '',
                 demo: bool = True):
        self._api_key = api_key or os.environ.get('OKX_API_KEY', '')
        self._secret = secret or os.environ.get('OKX_SECRET', '')
        self._passphrase = passphrase or os.environ.get('OKX_PASSPHRASE', '')
        self._demo = demo
        self._exchange = None
        self._connected = False
        self._contract_cache: Dict[str, dict] = {}  # cache contract info
        self._markets_loaded = False

    def connect(self) -> bool:
        try:
            config = {
                'apiKey': self._api_key,
                'secret': self._secret,
                'password': self._passphrase,
                'enableRateLimit': True,
                'timeout': 30000,
                'options': {
                    'defaultType': 'swap',
                    'fetchPositions': {'type': 'swap'},
                },
            }
            if self._demo:
                config['headers'] = {'x-simulated-trading': '1'}

            self._exchange = ccxt.okx(config)
            if self._demo:
                self._exchange.set_sandbox_mode(True)

            # Load markets once (caches contract info)
            self._exchange.load_markets()
            self._markets_loaded = True

            # Verify connection
            bal = self._fetch_balance()
            usdt_total = float(bal.get('USDT', {}).get('total', 0) or 0)
            self._connected = True
            logger.info(f"Connected to OKX {'Demo' if self._demo else 'LIVE'} | ${usdt_total:,.2f} USDT")
            return True
        except Exception as e:
            logger.error(f"OKX connection failed: {e}")
            return False

    def _ensure_connected(self):
        if not self._connected:
            self.connect()

    def _symbol(self, coin: str) -> str:
        return f"{coin}/USDT:USDT"

    def _retry(self, func, *args, **kwargs):
        """Robust retry with exponential backoff. Handles OKX-specific errors."""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                result = func(*args, **kwargs)
                return result
            except ccxt.RateLimitExceeded:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Rate limit, waiting {delay:.0f}s (attempt {attempt+1})")
                time.sleep(delay)
                last_error = "Rate limit"
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Network error, retry in {delay:.0f}s: {str(e)[:60]}")
                time.sleep(delay)
                last_error = str(e)
            except ccxt.ExchangeError as e:
                error_str = str(e)
                # Check if retryable OKX error
                is_retryable = any(code in error_str for code in RETRYABLE_CODES)
                if is_retryable and attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"OKX temporary error, retry in {delay:.0f}s: {error_str[:60]}")
                    time.sleep(delay)
                    last_error = error_str
                else:
                    raise  # fatal error, don't retry
        raise ccxt.ExchangeError(f"Max retries exceeded. Last error: {last_error}")

    def _fetch_balance(self) -> dict:
        """Raw balance fetch."""
        return self._retry(self._exchange.fetch_balance, params={'type': 'swap'})

    def get_balance(self) -> dict:
        self._ensure_connected()
        try:
            bal = self._fetch_balance()
            usdt = bal.get('USDT', {})
            return {
                'free': float(usdt.get('free', 0) or 0),
                'used': float(usdt.get('used', 0) or 0),
                'total': float(usdt.get('total', 0) or 0),
            }
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return {'free': 0, 'used': 0, 'total': 0}

    def get_ticker(self, coin: str) -> dict:
        self._ensure_connected()
        try:
            t = self._retry(self._exchange.fetch_ticker, self._symbol(coin))
            return {
                'price': float(t['last'] or 0),
                'bid': float(t.get('bid', 0) or 0),
                'ask': float(t.get('ask', 0) or 0),
                'change_24h': float(t.get('percentage', 0) or 0),
            }
        except Exception as e:
            logger.error(f"Ticker {coin}: {e}")
            return {'price': 0, 'bid': 0, 'ask': 0, 'change_24h': 0}

    def get_positions(self) -> list:
        self._ensure_connected()
        try:
            positions = self._retry(self._exchange.fetch_positions)
            result = []
            for p in positions:
                contracts = float(p.get('contracts', 0) or 0)
                if contracts == 0:
                    continue

                # Determine side from contracts sign or side field
                side = p.get('side', '')
                if not side:
                    side = 'long' if contracts > 0 else 'short'

                result.append(Position(
                    symbol=p['symbol'],
                    side=side,
                    size=abs(contracts),
                    entry_price=float(p.get('entryPrice', 0) or 0),
                    margin=float(p.get('initialMargin', 0) or p.get('collateral', 0) or 0),
                    leverage=float(p.get('leverage', 1) or 1),
                    unrealized_pnl=float(p.get('unrealizedPnl', 0) or 0),
                ))
            return result
        except Exception as e:
            logger.error(f"Positions: {e}")
            return []

    def get_position(self, coin: str) -> Optional[Position]:
        for p in self.get_positions():
            if coin in p.symbol:
                return p
        return None

    def get_contract_info(self, coin: str) -> dict:
        """Get contract specs. Cached after first call."""
        if coin in self._contract_cache:
            return self._contract_cache[coin]

        self._ensure_connected()
        try:
            sym = self._symbol(coin)
            if not self._markets_loaded:
                self._exchange.load_markets()
                self._markets_loaded = True

            market = self._exchange.market(sym)
            info = {
                'contractSize': float(market.get('contractSize') or 1),
                'minAmount': float(market.get('limits', {}).get('amount', {}).get('min') or 1),
                'maxAmount': float(market.get('limits', {}).get('amount', {}).get('max') or 999999),
                'maxLeverage': float(market.get('limits', {}).get('leverage', {}).get('max') or 100),
                'pricePrecision': market.get('precision', {}).get('price', 0.01),
                'amountPrecision': market.get('precision', {}).get('amount', 1),
            }
            self._contract_cache[coin] = info
            return info
        except Exception as e:
            logger.error(f"Contract info {coin}: {e}")
            return {'contractSize': 1, 'minAmount': 1, 'maxAmount': 999999,
                    'maxLeverage': 100, 'pricePrecision': 0.01, 'amountPrecision': 1}

    def set_leverage(self, coin: str, leverage: int, direction: str = 'LONG') -> bool:
        self._ensure_connected()
        try:
            self._retry(self._exchange.set_leverage, leverage,
                       symbol=self._symbol(coin),
                       params={'mgnMode': 'cross'})
            return True
        except Exception as e:
            # "Leverage already set" is not an error
            if '51036' in str(e):
                return True
            logger.warning(f"Leverage {coin}: {e}")
            return False

    def place_limit_order(self, coin: str, side: str, amount: float, price: float = None) -> Optional[OrderResult]:
        """Place limit order at current bid/ask for maker fee (0.02% vs 0.05%).
        If price not specified, uses best bid (for sell) or best ask (for buy).
        Falls back to market order if limit fails after 10s.
        """
        self._ensure_connected()
        try:
            if price is None:
                ticker = self.get_ticker(coin)
                # Place at best price to get filled quickly but as maker
                if side == 'buy':
                    price = ticker['bid'] * 1.0001  # slightly above bid
                else:
                    price = ticker['ask'] * 0.9999  # slightly below ask
                if price <= 0:
                    price = ticker['price']

            order = self._retry(self._exchange.create_order,
                symbol=self._symbol(coin),
                type='limit',
                side=side,
                amount=amount,
                price=price,
                params={'tdMode': 'cross'}
            )
            if not order:
                return None

            # Wait up to 10s for fill
            order_id = order['id']
            for _ in range(5):
                time.sleep(2)
                try:
                    status = self._exchange.fetch_order(order_id, self._symbol(coin))
                    if status['status'] == 'closed':
                        fill_price = float(status.get('average') or status.get('price') or price)
                        filled = float(status.get('filled') or amount)
                        logger.info(f"Limit filled: {side} {coin} {filled} @ ${fill_price:.4f}")
                        return OrderResult(
                            id=str(order_id), symbol=self._symbol(coin), side=side,
                            amount=filled, price=fill_price, status='filled')
                except Exception:
                    pass

            # Not filled in 10s — cancel and use market
            try:
                self._exchange.cancel_order(order_id, self._symbol(coin))
            except Exception:
                pass
            logger.info(f"Limit not filled, falling back to market: {side} {coin}")
            return self.place_market_order(coin, side, amount)

        except Exception as e:
            logger.error(f"Limit order failed {side} {coin}: {e}")
            return self.place_market_order(coin, side, amount)

    def place_market_order(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        """Place market order (fallback). Prefer place_limit_order for lower fees."""
        self._ensure_connected()
        try:
            order = self._retry(self._exchange.create_order,
                symbol=self._symbol(coin),
                type='market',
                side=side,
                amount=amount,
                params={'tdMode': 'cross'}
            )
            if not order:
                return None

            # Get fill price: try average, then price, then fetch from ticker
            fill_price = float(order.get('average') or order.get('price') or 0)
            if fill_price == 0:
                ticker = self.get_ticker(coin)
                fill_price = ticker['price']

            filled = float(order.get('filled') or order.get('amount') or amount)

            logger.info(f"Order OK: {side} {coin} {filled} @ ${fill_price:.4f}")
            return OrderResult(
                id=str(order['id']),
                symbol=order['symbol'],
                side=side,
                amount=filled,
                price=fill_price,
                status=order.get('status', 'filled'),
            )
        except Exception as e:
            logger.error(f"Order FAIL: {side} {coin} {amount}: {e}")
            return None

    def close_position(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        """Close position by placing opposite market order."""
        return self.place_market_order(coin, side, amount)

    def cancel_all(self, coin: str):
        try:
            self._retry(self._exchange.cancel_all_orders, self._symbol(coin))
        except Exception:
            pass

    def get_funding_rate(self, coin: str) -> float:
        """Get current funding rate for a coin."""
        self._ensure_connected()
        try:
            fr = self._retry(self._exchange.fetch_funding_rate, self._symbol(coin))
            return float(fr.get('fundingRate', 0) or 0)
        except Exception:
            return 0.0


def create_client(demo: bool = True) -> OKXClient:
    """Create OKX client from environment variables."""
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    return OKXClient(
        api_key=os.environ.get('OKX_API_KEY', ''),
        secret=os.environ.get('OKX_SECRET', ''),
        passphrase=os.environ.get('OKX_PASSPHRASE', ''),
        demo=demo,
    )
