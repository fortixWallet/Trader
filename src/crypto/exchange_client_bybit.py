"""
Bybit Demo Trading Client — Production Quality
================================================
Built from Bybit v5 API documentation.

Key differences from OKX:
  - enable_demo_trading(True) — official ccxt method
  - reduceOnly works correctly
  - contractSize = 1.0 for all coins (no conversion needed)
  - 537 futures pairs, 25 of our model coins
  - $100K USDT + 1 BTC + 1 ETH demo balance
  - WebSocket: wss://stream.bybit.com/v5/public/linear (public)
              wss://stream-demo.bybit.com/v5/private (private)
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

MAX_RETRIES = 5
BASE_DELAY = 1.0


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


class BybitClient:
    """Bybit Demo Trading — production quality."""

    def __init__(self, api_key: str = '', secret: str = '', demo: bool = True):
        self._api_key = api_key or os.environ.get('BYBIT_API_KEY', '')
        self._secret = secret or os.environ.get('BYBIT_SECRET', '')
        self._demo = demo
        self._exchange = None
        self._connected = False
        self._contract_cache: Dict[str, dict] = {}

    def connect(self) -> bool:
        try:
            self._exchange = ccxt.bybit({
                'apiKey': self._api_key,
                'secret': self._secret,
                'enableRateLimit': True,
                'timeout': 30000,
                'options': {
                    'defaultType': 'swap',
                },
            })

            if self._demo:
                self._exchange.enable_demo_trading(True)

            self._exchange.load_markets()

            bal = self._exchange.fetch_balance({'type': 'swap'})
            usdt = float(bal.get('USDT', {}).get('total', 0) or 0)
            self._connected = True
            logger.info(f"Connected to Bybit {'Demo' if self._demo else 'LIVE'} | ${usdt:,.0f} USDT")
            return True
        except Exception as e:
            logger.error(f"Bybit connection failed: {e}")
            return False

    def _ensure_connected(self):
        if not self._connected:
            self.connect()

    def _symbol(self, coin: str) -> str:
        return f"{coin}/USDT:USDT"

    def _retry(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except ccxt.RateLimitExceeded:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Rate limit, wait {delay:.0f}s")
                time.sleep(delay)
                last_error = "Rate limit"
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Network error, retry in {delay:.0f}s")
                time.sleep(delay)
                last_error = str(e)
            except ccxt.BadRequest as e:
                # "leverage not modified" = already set, not an error
                if '110043' in str(e):
                    return None
                raise
            except ccxt.ExchangeError as e:
                error_str = str(e)
                # Retryable Bybit errors
                if any(code in error_str for code in ['10016', '10006']):
                    delay = BASE_DELAY * (2 ** attempt)
                    time.sleep(delay)
                    last_error = error_str
                else:
                    raise
        raise ccxt.ExchangeError(f"Max retries. Last: {last_error}")

    def get_balance(self) -> dict:
        self._ensure_connected()
        try:
            bal = self._retry(self._exchange.fetch_balance, params={'type': 'swap'})
            usdt = bal.get('USDT', {})
            return {
                'free': float(usdt.get('free', 0) or 0),
                'used': float(usdt.get('used', 0) or 0),
                'total': float(usdt.get('total', 0) or 0),
            }
        except Exception as e:
            logger.error(f"Balance: {e}")
            return {'free': 0, 'used': 0, 'total': 0}

    def get_ticker(self, coin: str) -> dict:
        self._ensure_connected()
        try:
            t = self._retry(self._exchange.fetch_ticker, self._symbol(coin))
            return {
                'price': float(t['last'] or 0),
                'bid': float(t.get('bid', 0) or 0),
                'ask': float(t.get('ask', 0) or 0),
            }
        except Exception as e:
            logger.error(f"Ticker {coin}: {e}")
            return {'price': 0, 'bid': 0, 'ask': 0}

    def get_positions(self) -> list:
        self._ensure_connected()
        try:
            positions = self._retry(self._exchange.fetch_positions)
            result = []
            for p in positions:
                size = float(p.get('contracts', 0) or 0)
                if size == 0:
                    continue
                side = p.get('side', '')
                if not side:
                    side = 'long' if size > 0 else 'short'
                result.append(Position(
                    symbol=p['symbol'],
                    side=side,
                    size=abs(size),
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
        if coin in self._contract_cache:
            return self._contract_cache[coin]
        self._ensure_connected()
        try:
            market = self._exchange.market(self._symbol(coin))
            info = {
                'contractSize': float(market.get('contractSize') or 1),
                'minAmount': float(market.get('limits', {}).get('amount', {}).get('min') or 0.001),
                'maxAmount': float(market.get('limits', {}).get('amount', {}).get('max') or 999999),
                'maxLeverage': float(market.get('limits', {}).get('leverage', {}).get('max') or 100),
            }
            self._contract_cache[coin] = info
            return info
        except Exception as e:
            logger.error(f"Contract {coin}: {e}")
            return {'contractSize': 1, 'minAmount': 0.001, 'maxAmount': 999999, 'maxLeverage': 100}

    def set_leverage(self, coin: str, leverage: int, direction: str = 'LONG') -> bool:
        self._ensure_connected()
        try:
            self._retry(self._exchange.set_leverage, leverage, self._symbol(coin))
            return True
        except Exception as e:
            if '110043' in str(e):
                return True  # already set
            logger.warning(f"Leverage {coin}: {e}")
            return False

    def place_level_order(self, coin: str, side: str, amount: float,
                          price: float, sl_price: float = 0,
                          tp_price: float = 0) -> Optional[str]:
        """Place GTC limit order with TP/SL attached.
        When limit fills → Bybit activates SL/TP automatically. Zero delay.
        Returns order_id or None."""
        self._ensure_connected()
        try:
            params = {'timeInForce': 'GTC'}
            if sl_price > 0:
                params['stopLoss'] = {'triggerPrice': str(sl_price)}
            if tp_price > 0:
                params['takeProfit'] = {'triggerPrice': str(tp_price)}

            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'limit', side, amount, price,
                params=params)
            if order:
                oid = str(order['id'])
                sl_str = f" SL=${sl_price:.4f}" if sl_price else ""
                tp_str = f" TP=${tp_price:.4f}" if tp_price else ""
                logger.info(f"Level order: {side} {coin} {amount} @ ${price:.4f}{sl_str}{tp_str} (id={oid})")
                return oid
        except Exception as e:
            logger.error(f"Level order failed {side} {coin} @ ${price:.4f}: {e}")
        return None

    def place_limit_order(self, coin: str, side: str, amount: float, price: float = None) -> Optional[OrderResult]:
        """Aggressive limit order for maker fee (0.02%).
        Places at mid-price (between bid and ask) with 15s timeout.
        Falls back to market only as last resort.
        """
        self._ensure_connected()
        try:
            if price is None:
                ticker = self.get_ticker(coin)
                bid = ticker['bid']
                ask = ticker['ask']
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else ticker['price']

                if side == 'buy':
                    # Buy AT the ask — guarantees fill, still maker if limit order
                    price = ask
                else:
                    # Sell AT the bid — guarantees fill
                    price = bid

                if price <= 0:
                    price = ticker['price']

            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'limit', side, amount, price,
                params={'timeInForce': 'GTC'})

            if order:
                order_id = order['id']
                filled = float(order.get('filled', 0) or 0)

                if filled >= amount * 0.95:
                    fill_price = float(order.get('average') or order.get('price') or price)
                    logger.info(f"Limit filled: {side} {coin} {filled} @ ${fill_price:.4f}")
                    return OrderResult(
                        id=str(order_id), symbol=self._symbol(coin), side=side,
                        amount=filled, price=fill_price, status='filled')

                # Wait up to 15s for fill
                for i in range(15):
                    time.sleep(1)
                    try:
                        status = self._exchange.fetch_order(order_id, self._symbol(coin))
                        if status['status'] == 'closed':
                            fp = float(status.get('average') or status.get('price') or price)
                            fa = float(status.get('filled') or amount)
                            logger.info(f"Limit filled ({i+1}s): {side} {coin} {fa} @ ${fp:.4f}")
                            return OrderResult(
                                id=str(order_id), symbol=self._symbol(coin), side=side,
                                amount=fa, price=fp, status='filled')
                    except Exception:
                        pass

                # Cancel and fallback to market
                try: self._exchange.cancel_order(order_id, self._symbol(coin))
                except: pass
                logger.info(f"Limit not filled in 15s, market: {side} {coin}")
                return self.place_market_order(coin, side, amount)

        except Exception as e:
            logger.error(f"Limit failed {side} {coin}: {e}")
            return self.place_market_order(coin, side, amount)

    def place_market_order(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        self._ensure_connected()
        try:
            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'market', side, amount)
            if not order:
                return None

            fill_price = float(order.get('average') or order.get('price') or 0)
            if fill_price == 0:
                fill_price = self.get_ticker(coin)['price']

            filled = float(order.get('filled') or order.get('amount') or amount)
            logger.info(f"Market: {side} {coin} {filled} @ ${fill_price:.4f}")
            return OrderResult(
                id=str(order['id']), symbol=self._symbol(coin), side=side,
                amount=filled, price=fill_price, status='filled')
        except Exception as e:
            logger.error(f"Market failed {side} {coin} {amount}: {e}")
            return None

    def close_position(self, coin: str, side: str, amount: float) -> Optional[OrderResult]:
        """Close with reduceOnly (works correctly on Bybit)."""
        self._ensure_connected()
        try:
            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'market', side, amount,
                params={'reduceOnly': True})
            if order:
                fill_price = float(order.get('average') or order.get('price') or 0)
                if fill_price == 0:
                    fill_price = self.get_ticker(coin)['price']
                filled = float(order.get('filled') or amount)
                logger.info(f"Closed {coin}: {side} {filled} @ ${fill_price:.4f}")
                return OrderResult(
                    id=str(order['id']), symbol=self._symbol(coin), side=side,
                    amount=filled, price=fill_price, status='closed')
        except Exception as e:
            logger.error(f"Close failed {coin}: {e}")
        return None

    def place_stop_loss(self, coin: str, side: str, amount: float,
                        trigger_price: float) -> Optional[str]:
        """Place SL order ON EXCHANGE — works even if our code is offline.
        Returns order_id or None."""
        self._ensure_connected()
        try:
            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'market', side, amount,
                params={
                    'stopLossPrice': trigger_price,
                    'reduceOnly': True,
                    'triggerType': 'last',
                })
            if order:
                oid = str(order['id'])
                logger.info(f"SL placed {coin}: {side} {amount} @ trigger ${trigger_price:.4f} (id={oid})")
                return oid
        except Exception as e:
            logger.error(f"SL placement failed {coin}: {e}")
        return None

    def place_take_profit(self, coin: str, side: str, amount: float,
                          trigger_price: float) -> Optional[str]:
        """Place TP order ON EXCHANGE — works even if our code is offline.
        Returns order_id or None."""
        self._ensure_connected()
        try:
            order = self._retry(self._exchange.create_order,
                self._symbol(coin), 'market', side, amount,
                params={
                    'takeProfitPrice': trigger_price,
                    'reduceOnly': True,
                    'triggerType': 'last',
                })
            if order:
                oid = str(order['id'])
                logger.info(f"TP placed {coin}: {side} {amount} @ trigger ${trigger_price:.4f} (id={oid})")
                return oid
        except Exception as e:
            logger.error(f"TP placement failed {coin}: {e}")
        return None

    def cancel_order(self, order_id: str, coin: str) -> bool:
        """Cancel a pending order (limit, SL, or TP)."""
        self._ensure_connected()
        try:
            self._exchange.cancel_order(order_id, self._symbol(coin))
            logger.info(f"Cancelled order {order_id} for {coin}")
            return True
        except Exception as e:
            logger.debug(f"Cancel failed {order_id}: {e}")
            return False

    def check_order_status(self, order_id: str, coin: str) -> str:
        """Check order status: 'open', 'closed', 'canceled'."""
        self._ensure_connected()
        try:
            order = self._exchange.fetch_order(order_id, self._symbol(coin))
            return order.get('status', 'unknown')
        except Exception:
            return 'unknown'

    def get_funding_rate(self, coin: str) -> float:
        self._ensure_connected()
        try:
            fr = self._retry(self._exchange.fetch_funding_rate, self._symbol(coin))
            return float(fr.get('fundingRate', 0) or 0)
        except Exception:
            return 0.0


def create_client(demo: bool = True) -> BybitClient:
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()
    return BybitClient(
        api_key=os.environ.get('BYBIT_API_KEY', ''),
        secret=os.environ.get('BYBIT_SECRET', ''),
        demo=demo,
    )
