"""
FORTIX Exchange Client — MEXC Futures via ccxt
===============================================

Handles all exchange communication: orders, positions, balance, leverage.
Designed for MEXC USDT-M perpetual futures.

Key MEXC quirks (handled):
  - Market orders are IOC limit at 10% from price — handle partial fills
  - API fees: maker 0.01%, taker 0.05% (higher than web)
  - No testnet — test with minimum sizes on mainnet
  - Symbol format: BTC/USDT:USDT (ccxt normalized)
  - Rate limit: ~4-20 req/2s for trading, 10-20/2s for market data
"""

import time
import logging
import ccxt
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result of an order placement."""
    order_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    type: str  # 'market', 'limit'
    amount: float  # filled amount
    price: float  # average fill price
    cost: float  # total cost in USDT
    fee: float  # fee paid
    status: str  # 'closed', 'open', 'canceled'
    raw: dict  # raw exchange response


@dataclass
class Position:
    """An open position."""
    symbol: str
    side: str  # 'long' or 'short'
    size: float  # contracts/amount
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    leverage: int
    margin: float
    liquidation_price: float


class ExchangeClient:
    """MEXC Futures exchange client via ccxt."""

    # Rate limiting
    _last_request_time = 0
    _min_interval = 0.5  # 500ms between requests (conservative)

    # Fee structure (MEXC API)
    MAKER_FEE = 0.0001  # 0.01%
    TAKER_FEE = 0.0005  # 0.05%

    def __init__(self, api_key: str, secret: str, testmode: bool = False):
        self.api_key = api_key
        self.secret = secret
        self.testmode = testmode
        self._exchange = None
        self._markets_loaded = False

    def connect(self) -> bool:
        """Initialize connection to MEXC."""
        try:
            self._exchange = ccxt.mexc({
                'apiKey': self.api_key,
                'secret': self.secret,
                'options': {
                    'defaultType': 'swap',  # futures/perpetual
                },
                'enableRateLimit': True,
            })
            # Load markets
            self._exchange.load_markets()
            self._markets_loaded = True
            logger.info("Connected to MEXC futures")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MEXC: {e}")
            return False

    def _rate_limit(self):
        """Enforce rate limiting."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _ensure_connected(self):
        """Ensure exchange is connected."""
        if not self._exchange or not self._markets_loaded:
            if not self.connect():
                raise ConnectionError("Not connected to MEXC")

    # Coins that trade as 1000x on MEXC futures
    SYMBOL_MAP = {
        'BONK': '1000BONK/USDT:USDT',
    }
    # Price divisor: our DB stores real price, MEXC uses 1000x
    # When placing orders for BONK: amount in our terms / 1000 = amount in MEXC terms
    PRICE_MULT = {
        'BONK': 1000,  # MEXC price = our price * 1000
    }

    def _symbol(self, coin: str) -> str:
        """Convert coin name to ccxt swap symbol."""
        if coin in self.SYMBOL_MAP:
            return self.SYMBOL_MAP[coin]
        return f"{coin}/USDT:USDT"

    def _retry(self, fn, max_retries=3, **kwargs):
        """Execute with retry logic."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                return fn(**kwargs)
            except ccxt.RateLimitExceeded:
                wait = (attempt + 1) * 5
                logger.warning(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Network error (attempt {attempt+1}): {e}")
                    time.sleep(2)
                else:
                    raise
            except ccxt.ExchangeError as e:
                logger.error(f"Exchange error: {e}")
                raise
        raise Exception(f"Max retries ({max_retries}) exceeded")

    # ── Market Data ──────────────────────────────────────────

    def get_ticker(self, coin: str) -> dict:
        """Get current price and 24h stats."""
        self._ensure_connected()
        ticker = self._retry(self._exchange.fetch_ticker, symbol=self._symbol(coin))
        return {
            'coin': coin,
            'price': ticker['last'],
            'bid': ticker['bid'],
            'ask': ticker['ask'],
            'change_24h': ticker.get('percentage', 0),
            'volume_24h': ticker.get('quoteVolume', 0),
        }

    def get_orderbook(self, coin: str, depth: int = 10) -> dict:
        """Get order book."""
        self._ensure_connected()
        ob = self._retry(self._exchange.fetch_order_book, symbol=self._symbol(coin), limit=depth)
        return {
            'bids': ob['bids'][:depth],
            'asks': ob['asks'][:depth],
            'spread': ob['asks'][0][0] - ob['bids'][0][0] if ob['bids'] and ob['asks'] else 0,
        }

    def get_funding_rate(self, coin: str) -> Optional[float]:
        """Get current funding rate."""
        self._ensure_connected()
        try:
            fr = self._retry(self._exchange.fetch_funding_rate, symbol=self._symbol(coin))
            return fr.get('fundingRate')
        except Exception:
            return None

    # ── Account ──────────────────────────────────────────────

    def get_balance(self) -> dict:
        """Get USDT balance."""
        self._ensure_connected()
        balance = self._retry(self._exchange.fetch_balance)
        usdt = balance.get('USDT', {})
        return {
            'total': usdt.get('total', 0),
            'free': usdt.get('free', 0),
            'used': usdt.get('used', 0),
        }

    def get_positions(self) -> list:
        """Get all open positions."""
        self._ensure_connected()
        positions = self._retry(self._exchange.fetch_positions)
        result = []
        for p in positions:
            if p.get('contracts', 0) > 0:
                result.append(Position(
                    symbol=p['symbol'],
                    side=p['side'],
                    size=p['contracts'],
                    entry_price=p.get('entryPrice', 0),
                    mark_price=p.get('markPrice', 0),
                    unrealized_pnl=p.get('unrealizedPnl', 0),
                    leverage=p.get('leverage', 1),
                    margin=p.get('initialMargin', 0),
                    liquidation_price=p.get('liquidationPrice', 0),
                ))
        return result

    def get_position(self, coin: str) -> Optional[Position]:
        """Get position for specific coin."""
        positions = self.get_positions()
        symbol = self._symbol(coin)
        for p in positions:
            if p.symbol == symbol:
                return p
        return None

    # ── Trading ──────────────────────────────────────────────

    def set_leverage(self, coin: str, leverage: int, direction: str = 'LONG') -> bool:
        """Set leverage for a symbol. MEXC requires openType and positionType."""
        self._ensure_connected()
        try:
            # MEXC: openType 1=isolated, 2=cross; positionType 1=long, 2=short
            pos_type = 1 if direction == 'LONG' else 2
            params = {'openType': 1, 'positionType': pos_type}
            self._retry(self._exchange.set_leverage, leverage=leverage,
                       symbol=self._symbol(coin), params=params)
            logger.info(f"Set leverage {coin} {direction} → {leverage}x")
            return True
        except Exception as e:
            logger.warning(f"Failed to set leverage for {coin}: {e}")
            return False

    def set_margin_mode(self, coin: str, mode: str = 'isolated') -> bool:
        """Set margin mode (isolated or cross). MEXC requires leverage param."""
        self._ensure_connected()
        try:
            # MEXC: openType 1=isolated, 2=cross
            open_type = 1 if mode == 'isolated' else 2
            params = {'openType': open_type, 'leverage': 5}  # default leverage
            self._retry(self._exchange.set_margin_mode, marginMode=mode,
                       symbol=self._symbol(coin), params=params)
            return True
        except Exception as e:
            logger.debug(f"Margin mode set: {e}")
            return False

    def close_position(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        """Close an existing position with reduceOnly.

        Args:
            coin: e.g. 'BTC'
            side: 'buy' to close short, 'sell' to close long
            amount: contract amount
        """
        self._ensure_connected()
        symbol = self._symbol(coin)
        try:
            order = self._retry(
                self._exchange.create_order,
                symbol=symbol, type='market', side=side, amount=amount,
                params={'reduceOnly': True}
            )
            result = OrderResult(
                order_id=order.get('id', ''),
                symbol=symbol, side=side, type='market',
                amount=order.get('filled', amount),
                price=order.get('average', order.get('price', 0)) or 0,
                cost=order.get('cost', 0) or 0,
                fee=order.get('fee', {}).get('cost', 0) or 0,
                status=order.get('status', 'unknown'),
                raw=order,
            )
            logger.info(f"Position closed {coin} {side}: {result.amount} @ ${result.price:.4f}")
            return result
        except Exception as e:
            logger.error(f"Close position failed {coin}: {e}")
            return None

    def place_market_order(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        """Place market order to OPEN a new position.

        Args:
            coin: e.g. 'BTC'
            side: 'buy' (open long) or 'sell' (open short)
            amount: contract amount in base currency

        Note: MEXC market orders are IOC limit orders at 10% from price.
        May partially fill.
        """
        self._ensure_connected()
        symbol = self._symbol(coin)
        try:
            order = self._retry(
                self._exchange.create_order,
                symbol=symbol, type='market', side=side, amount=amount
            )
            result = OrderResult(
                order_id=order.get('id', '') or '',
                symbol=symbol,
                side=side,
                type='market',
                amount=order.get('filled', amount) or amount,
                price=order.get('average', None) or order.get('price', 0) or 0,
                cost=order.get('cost', 0) or 0,
                fee=(order.get('fee') or {}).get('cost', 0) or 0,
                status=order.get('status', 'unknown') or 'unknown',
                raw=order,
            )
            logger.info(f"Market {side} {coin}: {result.amount} @ ${result.price:.4f} "
                        f"(status={result.status})")
            return result
        except Exception as e:
            logger.error(f"Market order exception {side} {coin} {amount}: {e}")
            # Check if order actually went through despite the error
            try:
                import time; time.sleep(1)
                pos = self.get_position(coin)
                if pos:
                    logger.warning(f"Order exception but position EXISTS: {coin} {pos.side} {pos.size}")
                    ticker = self.get_ticker(coin)
                    return OrderResult(
                        order_id='recovered', symbol=symbol, side=side,
                        type='market', amount=pos.size,
                        price=pos.entry_price or ticker['price'],
                        cost=0, fee=0, status='closed', raw={},
                    )
            except Exception:
                pass
            return None

    def place_limit_order(self, coin: str, side: str, amount: float,
                          price: float) -> Optional[OrderResult]:
        """Place limit order."""
        self._ensure_connected()
        symbol = self._symbol(coin)
        try:
            order = self._retry(
                self._exchange.create_order,
                symbol=symbol, type='limit', side=side, amount=amount, price=price
            )
            result = OrderResult(
                order_id=order['id'],
                symbol=symbol, side=side, type='limit',
                amount=order.get('filled', 0),
                price=price,
                cost=order.get('cost', 0),
                fee=order.get('fee', {}).get('cost', 0),
                status=order.get('status', 'open'),
                raw=order,
            )
            logger.info(f"Limit {side} {coin}: {amount} @ ${price:.2f}")
            return result
        except Exception as e:
            logger.error(f"Limit order failed: {e}")
            return None

    def place_stop_loss(self, coin: str, side: str, amount: float,
                        trigger_price: float) -> Optional[str]:
        """Place stop-loss order (trigger/conditional).

        For a LONG position: side='sell', trigger below entry.
        For a SHORT position: side='buy', trigger above entry.
        """
        self._ensure_connected()
        symbol = self._symbol(coin)
        try:
            # MEXC requires stopPrice and price for stop orders
            # Round to proper precision
            market = self._exchange.market(symbol)
            precision = market.get('precision', {}).get('price', 8)
            trigger_rounded = float(self._exchange.price_to_precision(symbol, trigger_price))

            params = {
                'stopPrice': trigger_rounded,
                'reduceOnly': True,
                'triggerType': 1,  # 1 = last price trigger
            }
            order = self._retry(
                self._exchange.create_order,
                symbol=symbol, type='market', side=side, amount=amount,
                params=params
            )
            logger.info(f"Stop-loss {side} {coin}: {amount} @ trigger ${trigger_rounded:.4f}")
            return order.get('id', 'ok')
        except Exception as e:
            logger.error(f"Stop-loss failed for {coin}: {e}")
            return None

    def place_take_profit(self, coin: str, side: str, amount: float,
                          trigger_price: float) -> Optional[str]:
        """Place take-profit order."""
        self._ensure_connected()
        symbol = self._symbol(coin)
        try:
            params = {
                'stopPrice': trigger_price,
                'reduceOnly': True,
            }
            order = self._retry(
                self._exchange.create_order,
                symbol=symbol, type='market', side=side, amount=amount,
                params=params
            )
            logger.info(f"Take-profit {side} {coin}: {amount} @ trigger ${trigger_price:.2f}")
            return order['id']
        except Exception as e:
            logger.error(f"Take-profit failed: {e}")
            return None

    def cancel_order(self, coin: str, order_id: str) -> bool:
        """Cancel an order."""
        self._ensure_connected()
        try:
            self._retry(self._exchange.cancel_order, id=order_id, symbol=self._symbol(coin))
            logger.info(f"Cancelled order {order_id}")
            return True
        except Exception as e:
            logger.warning(f"Cancel failed: {e}")
            return False

    def cancel_all(self, coin: str) -> int:
        """Cancel all open orders for a coin."""
        self._ensure_connected()
        try:
            result = self._retry(self._exchange.cancel_all_orders, symbol=self._symbol(coin))
            n = len(result) if isinstance(result, list) else 1
            logger.info(f"Cancelled all orders for {coin} ({n})")
            return n
        except Exception as e:
            logger.warning(f"Cancel all failed: {e}")
            return 0

    # ── Info ─────────────────────────────────────────────────

    def get_trading_pairs(self) -> list:
        """Get available futures trading pairs."""
        self._ensure_connected()
        pairs = []
        for symbol, market in self._exchange.markets.items():
            if market.get('swap') and market.get('active') and 'USDT' in symbol:
                pairs.append({
                    'symbol': symbol,
                    'base': market.get('base', ''),
                    'min_amount': market.get('limits', {}).get('amount', {}).get('min', 0),
                    'max_leverage': market.get('limits', {}).get('leverage', {}).get('max', 0),
                })
        return pairs

    def get_contract_info(self, coin: str) -> dict:
        """Get contract specifications for a coin."""
        self._ensure_connected()
        symbol = self._symbol(coin)
        market = self._exchange.market(symbol)
        return {
            'symbol': symbol,
            'min_amount': market.get('limits', {}).get('amount', {}).get('min', 0),
            'max_amount': market.get('limits', {}).get('amount', {}).get('max', 0),
            'amount_step': market.get('precision', {}).get('amount', 0),
            'price_step': market.get('precision', {}).get('price', 0),
            'max_leverage': market.get('limits', {}).get('leverage', {}).get('max', 0),
            'contract_size': market.get('contractSize', 1),
        }

    def is_pair_available(self, coin: str) -> bool:
        """Check if a futures pair is available."""
        self._ensure_connected()
        symbol = self._symbol(coin)
        return symbol in self._exchange.markets


def create_client() -> ExchangeClient:
    """Create exchange client from .env configuration."""
    import os
    from pathlib import Path

    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    api_key = os.environ.get('MEXC_API_KEY', '')
    secret = os.environ.get('MEXC_SECRET', '')

    if not api_key or not secret:
        logger.warning("MEXC_API_KEY or MEXC_SECRET not set in .env")

    return ExchangeClient(api_key, secret)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    client = create_client()

    if client.connect():
        print("Connected to MEXC!")

        # Test balance
        balance = client.get_balance()
        print(f"Balance: ${balance['total']:.2f} (free: ${balance['free']:.2f})")

        # Check available pairs
        pairs = client.get_trading_pairs()
        print(f"Available futures pairs: {len(pairs)}")

        # Check our trading universe
        COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
                 'DOGE', 'FET', 'TAO', 'PEPE', 'AAVE', 'UNI']
        print("\nTrading universe availability:")
        for coin in COINS:
            available = client.is_pair_available(coin)
            if available:
                info = client.get_contract_info(coin)
                ticker = client.get_ticker(coin)
                print(f"  {coin:6s}: ${ticker['price']:<10,.2f} "
                      f"min={info['min_amount']}, max_lev={info['max_leverage']}x ✓")
            else:
                print(f"  {coin:6s}: NOT AVAILABLE ✗")
    else:
        print("Failed to connect. Add MEXC_API_KEY and MEXC_SECRET to .env")
        print("Get keys from: https://www.mexc.com/user/openapi")
