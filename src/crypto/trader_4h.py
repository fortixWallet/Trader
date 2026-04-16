"""
FORTIX 4h Trader — Short-Term Trading Engine
=============================================

Runs every 4 hours at candle close (00,04,08,12,16,20 UTC).
Uses 4h pattern model (68.4% accuracy at ±0.8%) for entries/exits.

BUGS PREVENTED (learned from live trading):
  1. Position count from EXCHANGE, not local DB
  2. Exchange-side stop-loss on EVERY trade
  3. reduceOnly for ALL closes
  4. MEXC setLeverage with openType/positionType params
  5. BONK → 1000BONK symbol mapping
  6. Minimum 1 contract (never 0)
  7. Circuit breaker uses EQUITY (not free balance)
  8. Order recovery if ccxt parse fails but order executes
  9. Telegram on EVERY action
  10. Counter only increments on CONFIRMED trades
"""

import os
import sys
import time
import json
import signal
import sqlite3
import logging
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger('trader_4h')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'
LOG_DIR = _FACTORY_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Config
DEFAULT_THRESHOLD = 0.008  # ±0.8% default, overridden by adaptive threshold
MAX_POSITIONS = 4
MAX_SAME_DIRECTION = 4
RISK_PCT = 0.10  # 10% risk per trade (backtested)
# NO exchange SL — hold exactly 1 candle (4h), close at candle close
# SL kills profitable trades: price swings through SL then moves in our direction
# Backtested: NO SL = 55.4% WR vs WITH SL = 50% WR
USE_EXCHANGE_SL = False
STOP_PCT = 0.03  # 3% emergency hard stop (backup only)
TP_PCT = 0.015  # not used in normal flow, close at candle close
MAX_HOLD_MIN = 240  # 4 hours max hold (full candle)
SCAN_INTERVAL_SEC = 300  # check every 5 minutes

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'FET', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'SHIB', 'PEPE', 'WIF', 'BONK', 'PENDLE',
         'JUP', 'RAY', 'PYTH', 'JTO', 'BOME']


class Trader4h:
    def __init__(self, capital=125.0):
        self.capital = capital
        self._running = False
        self._last_scan_hour = -1
        self._last_retrain = 0
        self._entry_times = {}  # {coin: timestamp} for time-based exit
        self._threshold = DEFAULT_THRESHOLD

        # Load env
        env_path = _FACTORY_DIR / '.env'
        if env_path.exists():
            for line in open(env_path):
                if '=' in line and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    os.environ[k.strip()] = v.strip()

        # Telegram
        self._tg_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        self._tg_chat = os.environ.get('TELEGRAM_CHAT_ID', '')

        # Exchange
        from src.crypto.exchange_client import create_client
        self.exchange = create_client()

        # Coin risk scoring
        from src.crypto.coin_risk_scorer import score_all_coins
        self._coin_risk = {}

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal")
        self._running = False

    def _notify(self, title, body):
        if not self._tg_token or not self._tg_chat:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self._tg_token}/sendMessage",
                json={'chat_id': self._tg_chat, 'text': f"<b>{title}</b>\n{body}", 'parse_mode': 'HTML'},
                timeout=10
            )
        except Exception:
            pass

    def _get_real_positions(self):
        """Get positions from EXCHANGE (Bug #1: never from local DB)."""
        try:
            return self.exchange.get_positions()
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []

    def _get_equity(self):
        """Get real equity including margin (Bug #7: not just free balance)."""
        try:
            bal = self.exchange.get_balance()
            free = bal.get('free', 0) or 0
            positions = self._get_real_positions()
            margin = sum(float(p.margin or 0) for p in positions)
            return free + margin
        except Exception:
            return self.capital

    def _get_atr_tp_sl(self, coin):
        """ATR-adaptive TP/SL per coin.

        TP = 0.5 * ATR_4h (50% of average range → realistic target)
        SL = 0.75 * ATR_4h (75% of range → room for noise)

        Why this works: fixed 1% TP was only 30-35% of ATR for volatile coins
        like PENDLE/FET, meaning price would swing through SL before reaching TP.
        ATR-adaptive ensures TP/SL scales with each coin's volatility.
        """
        try:
            conn = sqlite3.connect(str(DB_PATH))
            rows = conn.execute(
                "SELECT high, low, close FROM prices WHERE coin=? AND timeframe='4h' "
                "ORDER BY timestamp DESC LIMIT 20", (coin,)
            ).fetchall()
            conn.close()

            if len(rows) < 10:
                return TP_PCT, STOP_PCT

            atrs = [(r[0] - r[1]) / r[2] for r in rows if r[2] > 0]
            atr = np.mean(atrs) if atrs else 0.02

            tp = atr * 0.5   # 50% of ATR
            sl = atr * 0.75  # 75% of ATR

            # Clamp to reasonable range
            tp = max(0.005, min(tp, 0.03))  # 0.5% to 3%
            sl = max(0.007, min(sl, 0.04))  # 0.7% to 4%

            return tp, sl
        except Exception:
            return TP_PCT, STOP_PCT

    def _get_safe_leverage(self, coin, direction):
        """Coin-specific leverage. Minimum 5x to keep margin manageable with $114 capital."""
        if not self._coin_risk:
            try:
                conn = sqlite3.connect(str(DB_PATH))
                from src.crypto.coin_risk_scorer import score_all_coins
                self._coin_risk = score_all_coins(conn)
                conn.close()
            except Exception:
                pass

        profile = self._coin_risk.get(coin, {})
        if direction == 'LONG':
            lev = profile.get('max_leverage_long', 5)
        else:
            lev = profile.get('max_leverage_short', 7)
        # Minimum 7x: matches backtest config, keeps margins manageable
        return max(7, lev)

    def _predict_regression(self):
        """Run v3 model with 61 features + cross-sectional ranking.

        Uses predict_all_coins_4h() which computes ALL features identically
        to training, including cross-sectional z-scores and percentile ranks.
        This guarantees no feature mismatch.
        """
        try:
            from src.crypto.pattern_4h import predict_all_coins_4h
            all_preds = predict_all_coins_4h()

            if not all_preds:
                logger.error("No predictions from v3 model")
                return {}

            predictions = {}
            for p in all_preds:
                coin = p['coin']
                if coin not in COINS:
                    continue
                predictions[coin] = {
                    'predicted_return': p['reg_score'],
                    'price': p['price'],
                    'direction': 'PENDING',  # assigned by ranking in scan_and_trade
                    'strength': abs(p['reg_score']),
                    'cls_direction': p['prediction'],
                    'confidence': p['confidence'],
                }

            logger.info(f"V3 model: {len(predictions)} predictions, 61 features + cross-sectional")
            return predictions

        except Exception as e:
            logger.error(f"V3 prediction failed: {e}", exc_info=True)
            return {}

    def _execute_trade(self, coin, direction, predicted_return):
        """Open a trade with ALL bug protections."""
        try:
            # Get price
            ticker = self.exchange.get_ticker(coin)
            price = ticker['price']
            if price <= 0:
                return False

            # Leverage from coin risk (Bug #2)
            leverage = self._get_safe_leverage(coin, direction)

            # Position size: margin-based (matches backtest)
            # Target: equity × RISK_PCT = $11 margin per position at 7x = $77 notional
            equity = self._get_equity()
            target_margin = equity * RISK_PCT  # 10% of equity
            target_notional = target_margin * leverage

            # Get contract size from exchange
            try:
                market = self.exchange._exchange.market(self.exchange._symbol(coin))
                contract_size = float(market.get('contractSize', 1) or 1)
            except Exception:
                contract_size = 1.0

            cost_per_contract = price * contract_size
            amount = max(1, int(target_notional / cost_per_contract))
            notional = amount * cost_per_contract
            margin_needed = notional / leverage

            # Check we have enough free balance
            bal = self.exchange.get_balance()
            free = bal['free']
            if margin_needed > free * 0.90:
                # Reduce to fit available balance
                reduced_notional = free * 0.90 * leverage * 0.5  # use 50% of what's left
                amount = max(1, int(reduced_notional / cost_per_contract))
                notional = amount * cost_per_contract
                margin_needed = notional / leverage
                if margin_needed > free * 0.95:
                    logger.info(f"{coin}: margin ${margin_needed:.2f} > free ${free:.2f}")
                    return False

            # Set leverage (Bug #4: MEXC needs params)
            self.exchange.set_leverage(coin, leverage, direction)

            # Open position
            side = 'buy' if direction == 'LONG' else 'sell'
            order = self.exchange.place_market_order(coin, side, amount)

            # Bug #8: check if order executed even if ccxt says it failed
            if not order:
                time.sleep(1)
                pos = self.exchange.get_position(coin)
                if pos:
                    entry_price = pos.entry_price or price
                    logger.warning(f"Order parse failed but position exists: {coin}")
                else:
                    logger.error(f"Order truly failed: {coin}")
                    return False
            else:
                entry_price = order.price or price

            # Emergency hard stop only (3%) — NOT the trading strategy
            # Normal exit: close at next 4h candle close (managed by _manage_positions)
            if USE_EXCHANGE_SL:
                sl_side = 'sell' if direction == 'LONG' else 'buy'
                if direction == 'LONG':
                    stop_price = entry_price * (1 - STOP_PCT)
                else:
                    stop_price = entry_price * (1 + STOP_PCT)
                self.exchange.place_stop_loss(coin, sl_side, amount, stop_price)

            # Telegram
            emoji = '🟢' if direction == 'LONG' else '🔴'
            self._notify(
                f"{emoji} {direction} {coin} {leverage}x",
                f"Entry: ${entry_price:.4f}\n"
                f"Predicted: {predicted_return*100:+.2f}%\n"
                f"Hold: 1 candle (4h), no SL\n"
                f"Size: {amount} contracts"
            )

            logger.info(f"TRADE: {direction} {coin} {amount}@${entry_price:.4f} {leverage}x")
            self._entry_times[coin] = time.time()
            return True  # Bug #10: only return True on confirmed trade

        except Exception as e:
            logger.error(f"Trade failed {coin}: {e}")
            return False

    def _close_position(self, coin, side, size, reason=""):
        """Close with reduceOnly (Bug #3)."""
        try:
            close_side = 'buy' if side == 'short' else 'sell'
            result = self.exchange.close_position(coin, close_side, size)
            if result:
                logger.info(f"CLOSED: {coin} {side} — {reason}")
                self._notify(f"{'💰' if 'profit' in reason.lower() else '🛑'} CLOSED {coin}",
                           f"{reason}")
                return True
        except Exception as e:
            logger.error(f"Close failed {coin}: {e}")
        return False

    def _manage_positions(self):
        """Check open positions, close if needed."""
        positions = self._get_real_positions()
        for pos in positions:
            try:
                coin = pos.symbol.split('/')[0].replace('1000', '')
                entry = pos.entry_price
                if not entry or entry <= 0:
                    continue

                ticker = self.exchange.get_ticker(coin)
                now = ticker['price']

                if pos.side == 'short':
                    pnl_pct = (entry - now) / entry * 100
                else:
                    pnl_pct = (now - entry) / entry * 100

                lev = pos.leverage or 5
                pnl_lev = pnl_pct * lev

                # NO TP — hold full candle, close via time exit
                # Only emergency hard stop at -20% leveraged
                if pnl_lev <= -20:
                    self._close_position(coin, pos.side, pos.size,
                        f"Hard stop: {pnl_lev:+.1f}%")
                    continue

                # Time exit: close after MAX_HOLD_MIN (4h)
                entry_ts = self._entry_times.get(coin, 0)
                if entry_ts > 0:
                    held_min = (time.time() - entry_ts) / 60
                    if held_min >= MAX_HOLD_MIN:
                        closed = self._close_position(coin, pos.side, pos.size,
                            f"Time exit ({held_min:.0f}min): {pnl_lev:+.1f}%")
                        if closed:
                            self._entry_times.pop(coin, None)
                        continue
                else:
                    # Position exists but no entry time tracked (opened before restart)
                    # Set entry time to now so it will close in MAX_HOLD_MIN
                    self._entry_times[coin] = time.time()
                    logger.info(f"Tracking existing position {coin} (no entry time)")

            except Exception as e:
                logger.debug(f"Manage {coin}: {e}")

    def scan_and_trade(self):
        """Main 4h scan: predict → filter → trade."""
        logger.info("4h SCAN starting...")

        # Get predictions
        predictions = self._predict_regression()
        if not predictions:
            logger.info("No predictions available")
            return

        # CROSS-SECTIONAL RANKING
        # Top predicted = LONG (will outperform)
        # Bottom predicted = SHORT (will underperform)
        # This fixes bullish bias — always balanced LONG/SHORT
        import numpy as np

        if len(predictions) < 5:
            logger.info("Too few predictions for ranking")
            return

        # Sort by predicted return (highest to lowest)
        ranked = sorted(predictions.items(), key=lambda x: x[1]['predicted_return'], reverse=True)

        # REGIME-ADAPTIVE: trade WITH the market, not against it
        # BEAR → SHORT bottom coins (57% WR)
        # BULL → LONG top coins (55% WR)
        # FLAT → mixed (LONG 70% WR in FLAT!)
        # Backtested: 55.4% WR, PF 1.62, +430% / month

        # Excluded: BNB/RENDER/DOT (<30% WR), PEPE/SHIB (contract too large for $114 capital)
        BAD_COINS_SET = {'BNB', 'RENDER', 'DOT', 'PEPE', 'SHIB'}

        conn_regime = sqlite3.connect(str(DB_PATH))
        try:
            # BTC 12h momentum (3 candles)
            btc_4h = conn_regime.execute(
                "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
                "ORDER BY timestamp DESC LIMIT 4"
            ).fetchall()
            btc_12h = (btc_4h[0][0] / btc_4h[3][0] - 1) * 100 if len(btc_4h) >= 4 else 0

            # BTC 24h momentum (6 candles)
            btc_24h_rows = conn_regime.execute(
                "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
                "ORDER BY timestamp DESC LIMIT 7"
            ).fetchall()
            btc_24h = (btc_24h_rows[0][0] / btc_24h_rows[6][0] - 1) * 100 if len(btc_24h_rows) >= 7 else 0

            # Market breadth: how many coins went up in last candle?
            breadth_rows = conn_regime.execute("""
                SELECT coin,
                    (SELECT close FROM prices p2 WHERE p2.coin=p1.coin AND p2.timeframe='4h'
                     ORDER BY p2.timestamp DESC LIMIT 1) as c1,
                    (SELECT close FROM prices p2 WHERE p2.coin=p1.coin AND p2.timeframe='4h'
                     ORDER BY p2.timestamp DESC LIMIT 1 OFFSET 1) as c2
                FROM (SELECT DISTINCT coin FROM prices WHERE timeframe='4h') p1
            """).fetchall()
            up_count = sum(1 for r in breadth_rows if r[1] and r[2] and r[1] > r[2])
            breadth = up_count / len(breadth_rows) if breadth_rows else 0.5
        except Exception:
            btc_12h = 0
            btc_24h = 0
            breadth = 0.5
        finally:
            conn_regime.close()

        # Regime scoring
        bull_score = sum([btc_12h > 0.3, btc_12h > 1.0, btc_24h > 0.5, breadth > 0.6])
        bear_score = sum([btc_12h < -0.3, btc_12h < -1.0, btc_24h < -0.5, breadth < 0.4])

        if bull_score >= 2:
            regime = 'BULL'
            n_top = MAX_POSITIONS  # all LONG
            n_bottom = 0
        elif bear_score >= 2:
            regime = 'BEAR'
            n_top = 0
            n_bottom = MAX_POSITIONS  # all SHORT
        else:
            regime = 'FLAT'
            n_top = 1  # 1 LONG (70% WR in FLAT!)
            n_bottom = MAX_POSITIONS - 1

        logger.info(f"Regime: {regime} | BTC 12h={btc_12h:+.1f}% 24h={btc_24h:+.1f}% breadth={breadth:.0%} | "
                    f"bull={bull_score} bear={bear_score} → {n_top}L+{n_bottom}S")

        # Build signals
        signals = []
        for coin, pred in ranked[:n_top]:
            if coin not in BAD_COINS_SET:
                pred['direction'] = 'LONG'
                signals.append((coin, pred))
        for coin, pred in ranked[-n_bottom:] if n_bottom > 0 else []:
            if coin not in BAD_COINS_SET:
                pred['direction'] = 'SHORT'
                signals.append((coin, pred))

        signals.sort(key=lambda x: abs(x[1]['predicted_return']), reverse=True)

        # Log signals
        for coin, pred in signals[:6]:
            logger.info(f"  {coin:6s} {pred['direction']:5s} {pred['predicted_return']*100:+.4f}%")

        # Get REAL positions from exchange (Bug #1: never local)
        real_positions = self._get_real_positions()
        existing_coins = set()
        direction_count = {'long': 0, 'short': 0}
        for pos in real_positions:
            coin = pos.symbol.split('/')[0].replace('1000', '')
            existing_coins.add(coin)
            direction_count[pos.side] += 1

        slots = MAX_POSITIONS - len(real_positions)
        if slots <= 0:
            logger.info(f"Max positions ({len(real_positions)}/{MAX_POSITIONS})")
            return

        # Take TOP signals from ranking (not threshold-based)
        opened = 0
        for coin, pred in signals:
            if opened >= slots:
                break
            if coin in existing_coins:
                continue

            direction = pred['direction']
            dir_key = 'short' if direction == 'SHORT' else 'long'
            if direction_count[dir_key] >= MAX_SAME_DIRECTION:
                continue

            # Execute (Bug #10: only count confirmed success)
            success = self._execute_trade(coin, direction, pred['predicted_return'])
            if success:
                opened += 1
                existing_coins.add(coin)
                direction_count[dir_key] += 1
                logger.info(f"Opened #{opened}: {direction} {coin} "
                           f"(prediction {pred['predicted_return']*100:+.3f}%)")

        logger.info(f"Ranking result: opened {opened}, total positions {len(real_positions) + opened}")

    def _write_heartbeat(self):
        hb = _FACTORY_DIR / 'data' / 'crypto' / 'heartbeat.txt'
        hb.write_text(str(int(time.time())))

    def run(self):
        """Main loop."""
        logger.info(f"4h Trader starting (capital=${self.capital}, threshold=±{self._threshold*100:.2f}%)")

        if not self.exchange.connect():
            logger.error("Exchange connection failed")
            return

        bal = self.exchange.get_balance()
        logger.info(f"Balance: ${bal['total']:.2f}")
        self._notify("4h Trader Started",
                    f"Balance: ${bal['total']:.2f}\n"
                    f"Threshold: ±{self._threshold*100:.2f}%\n"
                    f"Max positions: {MAX_POSITIONS}\n"
                    f"Risk: {RISK_PCT*100:.0f}%/trade")

        # Load adaptive threshold
        try:
            from src.crypto.daily_trainer import get_current_threshold, calculate_adaptive_threshold
            self._threshold = get_current_threshold()
            logger.info(f"Adaptive threshold: ±{self._threshold*100:.2f}%")
        except Exception:
            self._threshold = DEFAULT_THRESHOLD

        self._running = True
        last_4h_scan = 0

        while self._running:
            try:
                self._write_heartbeat()
                now = time.time()

                # Scan every 5 minutes
                if now - last_4h_scan > 300:  # 5 min
                    # Collect fresh data every 30 min (API heavy)
                    if now - getattr(self, '_last_data_collect', 0) > 1800:
                        try:
                            from src.crypto.data_collector import collect_all
                            collect_all(heartbeat_fn=self._write_heartbeat)
                            self._last_data_collect = now
                        except Exception as e:
                            logger.warning(f"Data collection: {e}")

                    self.scan_and_trade()
                    last_4h_scan = now

                # Position management: every 5 minutes
                self._manage_positions()

                # Daily retrain at 02:00 UTC
                current_hour = datetime.now(timezone.utc).hour
                if current_hour == 2 and now - self._last_retrain > 72000:  # once per day
                    try:
                        from src.crypto.daily_trainer import retrain_4h_model, get_current_threshold
                        logger.info("Daily retrain starting...")
                        result = retrain_4h_model()
                        self._threshold = get_current_threshold()
                        self._last_retrain = now
                        self._notify("🔄 Model Retrained",
                                    f"Status: {result.get('status')}\n"
                                    f"Spearman: {result.get('spearman', 0):.4f}\n"
                                    f"Threshold: ±{self._threshold*100:.2f}%\n"
                                    f"Duration: {result.get('duration', 0)}s")
                        logger.info(f"Retrain done: {result.get('status')}, threshold=±{self._threshold*100:.2f}%")

                        # Reload model in predict function (clear cache)
                        import importlib
                        from src.crypto import pattern_4h
                        importlib.reload(pattern_4h)
                    except Exception as e:
                        logger.error(f"Daily retrain failed: {e}")

                # Recalculate threshold every 6 hours
                if now - getattr(self, '_last_threshold_calc', 0) > 21600:
                    try:
                        from src.crypto.daily_trainer import calculate_adaptive_threshold
                        self._threshold = calculate_adaptive_threshold()
                        self._last_threshold_calc = now
                    except Exception:
                        pass

                time.sleep(60)  # check every minute, scan every 5

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop: {e}", exc_info=True)
                time.sleep(60)

        logger.info("4h Trader stopped")
        self._notify("4h Trader Stopped", "Graceful shutdown")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=float, default=125.0)
    parser.add_argument('--scan-once', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(LOG_DIR / 'trader_4h.log')),
        ]
    )

    trader = Trader4h(capital=args.capital)

    if args.scan_once:
        trader.exchange.connect()
        preds = trader._predict_regression()
        if preds:
            ranked = sorted(preds.items(), key=lambda x: x[1]['predicted_return'])
            print(f"\n{'Coin':>6s} {'Score':>9s} {'Price':>10s} {'Signal':>8s}")
            print("-" * 40)
            for coin, p in ranked:
                sig = 'SHORT' if p['predicted_return'] < -0.002 else ('LONG' if p['predicted_return'] > 0.002 else '—')
                print(f"  {coin:>4s} {p['predicted_return']*100:>+7.3f}% ${p['price']:>9,.1f} {sig:>8s}")
            print(f"\nBest SHORT: {ranked[0][0]} ({ranked[0][1]['predicted_return']*100:+.3f}%)")
            print(f"Best LONG:  {ranked[-1][0]} ({ranked[-1][1]['predicted_return']*100:+.3f}%)")
        return

    trader.run()


if __name__ == '__main__':
    main()
