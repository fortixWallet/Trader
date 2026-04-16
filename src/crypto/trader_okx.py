"""
FORTIX OKX Demo Trader — Smart Exits + High Frequency
======================================================

Key innovations vs MEXC 4h trader:
  1. SMART EXIT: trailing stop activates at 70% of target
  2. REOPEN: freed slots immediately take new positions
  3. 8-10 simultaneous positions (not 3-4)
  4. Scan every 60s (not 5min)
  5. Budget: 10% of equity per position, never more than 80% total

Backtested: TP 0.7% + reopen = 70% WR, $89/day on $5K
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
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger('trader_okx')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'  # use 4h model (proven)
LOG_DIR = _FACTORY_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# === CONFIG ===
MAX_POSITIONS = 4
TARGET_PCT = 0.007       # 0.7% take-profit target
TRAILING_ACTIVATE = 0.70 # activate trailing at 70% of target
TRAILING_TIGHT = 0.90    # tighten trailing at 90% of target
TRAIL_WIDE = 0.003       # 0.3% trailing distance (before 90%)
TRAIL_TIGHT = 0.0015     # 0.15% trailing distance (after 90%)
EMERGENCY_STOP = -0.04   # -4% hard stop (give model room to be right)
MAX_HOLD_MIN = 240       # 4h max hold
SCAN_INTERVAL = 5        # check positions every 5 seconds (WebSocket provides prices)
RISK_PER_POSITION = 0.10 # 10% of equity per position
MAX_EQUITY_USED = 0.80   # never use more than 80% of equity
MIN_LEVERAGE = 7
BAD_COINS = {'BNB', 'RENDER', 'DOT', 'PEPE', 'SHIB', 'BONK'}

# OKX Demo: only 12 coins actually tradeable
COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'LINK',
         'DOGE', 'AAVE', 'LDO', 'CRV', 'ARB', 'OP']
# No swap on OKX demo: UNI/FET/RENDER/TAO/POL/WIF/BONK/PENDLE/RAY/PYTH/JTO
# Compliance blocked: JUP/BOME/SHIB/PEPE
# Bad WR: BNB/DOT


class TrackedPosition:
    """Track a position with smart exit logic."""
    def __init__(self, coin, direction, entry_price, size, leverage, entry_time):
        self.coin = coin
        self.direction = direction
        self.entry_price = entry_price
        self.size = size
        self.leverage = leverage
        self.entry_time = entry_time
        self.peak_pnl = 0.0       # highest PnL % reached
        self.trailing_active = False
        self.trailing_tight = False
        self.trail_stop_pnl = None  # PnL % at which trailing stop triggers

    def update(self, current_price):
        """Update position state, return exit signal or None."""
        if self.direction == 'SHORT':
            pnl_pct = (self.entry_price - current_price) / self.entry_price
        else:
            pnl_pct = (current_price - self.entry_price) / self.entry_price

        # Track peak
        if pnl_pct > self.peak_pnl:
            self.peak_pnl = pnl_pct

        target = TARGET_PCT
        progress = pnl_pct / target if target > 0 else 0

        # === EMERGENCY STOP ===
        if pnl_pct <= EMERGENCY_STOP:
            return 'EMERGENCY_STOP', pnl_pct

        # === TARGET HIT ===
        if pnl_pct >= target:
            return 'TARGET_HIT', pnl_pct

        # === TRAILING LOGIC ===
        if not self.trailing_active and progress >= TRAILING_ACTIVATE:
            # First activation at 70%+ — wide trailing
            self.trailing_active = True
            self.trail_stop_pnl = self.peak_pnl - TRAIL_WIDE
            return None, pnl_pct

        # Upgrade to tight trailing at 90%+ (use peak, not current — handles reversal)
        peak_progress = self.peak_pnl / target if target > 0 else 0
        if self.trailing_active and not self.trailing_tight and peak_progress >= TRAILING_TIGHT - 0.01:
            self.trailing_tight = True
            self.trail_stop_pnl = self.peak_pnl - TRAIL_TIGHT
            return None, pnl_pct

        # Update trailing stop level
        if self.trailing_active:
            trail_dist = TRAIL_TIGHT if self.trailing_tight else TRAIL_WIDE
            new_stop = self.peak_pnl - trail_dist
            if self.trail_stop_pnl is None or new_stop > self.trail_stop_pnl:
                self.trail_stop_pnl = new_stop

            # Check if trailing stop triggered
            if pnl_pct <= self.trail_stop_pnl:
                return 'TRAILING_STOP', pnl_pct

        # === TIME EXIT ===
        held_sec = time.time() - self.entry_time
        if held_sec >= MAX_HOLD_MIN * 60:
            return 'TIME_EXIT', pnl_pct

        return None, pnl_pct


class OKXTrader:
    def __init__(self, capital=5000.0):
        self.capital = capital
        self._running = False
        self._tracked = {}  # coin -> TrackedPosition
        self._last_signal_time = 0
        self._last_data_collect = 0
        self._trade_count = 0
        self._total_pnl = 0.0
        self._closing_lock = set()
        self._traded_this_cycle = set()
        self._last_4h_candle = 0
        self._last_retrain = 0

        # Trade journal — log every trade for learning
        self._init_trade_journal()

        # Load env
        env_path = _FACTORY_DIR / '.env'
        if env_path.exists():
            for line in open(env_path):
                if '=' in line and not line.startswith('#'):
                    k, v = line.strip().split('=', 1)
                    os.environ[k.strip()] = v.strip()

        self._tg_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        self._tg_chat = os.environ.get('TELEGRAM_CHAT_ID', '')

        # Exchange
        from src.crypto.exchange_client_okx import create_client
        self.exchange = create_client()

        # WebSocket price stream — real-time prices instead of REST polling
        from src.crypto.ws_price_stream import PriceStream
        self._price_stream = PriceStream(list(COINS), demo=True)

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
                json={'chat_id': self._tg_chat, 'text': f"<b>🔷 OKX {title}</b>\n{body}", 'parse_mode': 'HTML'},
                timeout=10
            )
        except Exception:
            pass

    def _init_trade_journal(self):
        """Create SQLite table for trade history — model learns from this."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""CREATE TABLE IF NOT EXISTS okx_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin TEXT, direction TEXT,
                entry_price REAL, exit_price REAL,
                entry_time TEXT, exit_time TEXT,
                held_minutes REAL,
                pnl_pct REAL, pnl_usd REAL,
                exit_reason TEXT,
                regime TEXT, reg_score REAL,
                notional REAL, leverage REAL,
                funding_rate REAL, liq_bias REAL
            )""")
            conn.commit()
            conn.close()
            logger.info("Trade journal ready")
        except Exception as e:
            logger.warning(f"Trade journal init: {e}")

    def _log_trade(self, coin, tracked, exit_price, pnl_pct, pnl_usd, reason):
        """Log completed trade to SQLite for model learning."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""INSERT INTO okx_trades
                (coin, direction, entry_price, exit_price, entry_time, exit_time,
                 held_minutes, pnl_pct, pnl_usd, exit_reason, notional, leverage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (coin, tracked.direction, tracked.entry_price, exit_price,
                 datetime.fromtimestamp(tracked.entry_time, timezone.utc).isoformat(),
                 datetime.now(timezone.utc).isoformat(),
                 (time.time() - tracked.entry_time) / 60,
                 pnl_pct * 100, pnl_usd, reason,
                 tracked.size * tracked.entry_price, tracked.leverage))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Trade log error: {e}")

    def _daily_retrain(self):
        """Retrain model with latest data including today's trades."""
        try:
            logger.info("Daily retrain starting...")
            from src.crypto.pattern_4h import build_4h_dataset, train_model
            df = build_4h_dataset()
            if len(df) < 10000:
                logger.warning(f"Too few samples for retrain: {len(df)}")
                return
            results = train_model(df)
            sp = results.get('spearman', 0)
            logger.info(f"Retrain done: Spearman={sp:.4f}")
            self._notify("Model Retrained",
                        f"Spearman: {sp:.4f}\n"
                        f"Features: {results.get('n_features', 0)}\n"
                        f"Ranking: {results.get('ranking', {})}")
        except Exception as e:
            logger.error(f"Retrain failed: {e}")

    def _get_equity(self):
        """Use only USDT balance (not BTC/ETH/OKB)."""
        try:
            bal = self.exchange.get_balance()
            # Only count USDT — user requested
            return bal.get('total', self.capital) or self.capital
        except:
            return self.capital

    def _get_regime(self):
        """Determine market regime from BTC momentum."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            btc = conn.execute(
                "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 7"
            ).fetchall()
            conn.close()
            if len(btc) < 4:
                return 'FLAT', 0
            btc_12h = (btc[0][0] / btc[3][0] - 1) * 100
            btc_24h = (btc[0][0] / btc[6][0] - 1) * 100 if len(btc) >= 7 else 0

            bull = sum([btc_12h > 0.3, btc_12h > 1.0, btc_24h > 0.5])
            bear = sum([btc_12h < -0.3, btc_12h < -1.0, btc_24h < -0.5])

            if bull >= 2: return 'BULL', btc_12h
            if bear >= 2: return 'BEAR', btc_12h
            return 'FLAT', btc_12h
        except:
            return 'FLAT', 0

    def _get_signals(self):
        """Get ranked signals from 4h model + funding rate + liquidation data."""
        try:
            from src.crypto.pattern_4h import predict_all_coins_4h
            preds = predict_all_coins_4h()
            filtered = [p for p in preds if p['coin'] not in BAD_COINS
                       and p['coin'] in COINS]

            conn = sqlite3.connect(str(DB_PATH))
            for p in filtered:
                adj = p['reg_score']

                # 1. Funding rate bonus
                try:
                    fr = conn.execute(
                        "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                        (p['coin'],)
                    ).fetchone()
                    funding = fr[0] if fr and fr[0] else 0
                    p['funding_rate'] = funding
                    adj -= funding * 100  # positive funding → favor SHORT
                except Exception:
                    p['funding_rate'] = 0

                # 2. Liquidation imbalance bonus
                try:
                    liq = conn.execute(
                        "SELECT long_liq_usd_4h, short_liq_usd_4h FROM cg_liquidations "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                        (p['coin'],)
                    ).fetchone()
                    if liq and liq[0] and liq[1]:
                        long_liq, short_liq = float(liq[0]), float(liq[1])
                        total_liq = long_liq + short_liq
                        if total_liq > 0:
                            # liq_bias: +1 = only longs liquidated (bearish), -1 = only shorts (bullish)
                            liq_bias = (long_liq - short_liq) / total_liq
                            p['liq_bias'] = liq_bias
                            # Strong long liquidations → price dropping → favor SHORT
                            adj -= liq_bias * 0.002
                        else:
                            p['liq_bias'] = 0
                    else:
                        p['liq_bias'] = 0
                except Exception:
                    p['liq_bias'] = 0

                # 3. Orderbook imbalance bonus
                try:
                    ob = conn.execute(
                        "SELECT imbalance_score FROM orderbook_imbalance "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                        (p['coin'],)
                    ).fetchone()
                    if ob and ob[0]:
                        # Positive imbalance = more bids = bullish
                        p['ob_imbalance'] = float(ob[0])
                        adj += float(ob[0]) * 0.001
                    else:
                        p['ob_imbalance'] = 0
                except Exception:
                    p['ob_imbalance'] = 0

                p['reg_score_adj'] = adj

            conn.close()
            return filtered
        except Exception as e:
            logger.error(f"Signal error: {e}")
            return []

    def _calculate_position_size(self, coin, price, leverage):
        """Smart position sizing: use equity %, respect limits."""
        equity = self._get_equity()

        # How much is already allocated?
        used_margin = sum(
            p.size * p.entry_price / p.leverage
            for p in self._tracked.values()
        )
        available = equity * MAX_EQUITY_USED - used_margin
        if available <= 0:
            return 0

        # Target margin for this position
        target_margin = min(equity * RISK_PER_POSITION, available)
        target_notional = target_margin * leverage

        # Get contract size
        info = self.exchange.get_contract_info(coin)
        contract_size = info.get('contractSize', 1)
        min_amount = info.get('minAmount', 1)

        cost_per = price * contract_size
        amount = max(int(min_amount), int(target_notional / cost_per))

        return amount

    def _open_trade(self, coin, direction, score):
        """Open a position with smart sizing."""
        try:
            ticker = self.exchange.get_ticker(coin)
            price = ticker['price']
            if price <= 0:
                return False

            # Use coin's max leverage, capped at MIN_LEVERAGE
            info = self.exchange.get_contract_info(coin)
            max_lev = int(info.get('maxLeverage', 100))
            leverage = min(MIN_LEVERAGE, max_lev)
            self.exchange.set_leverage(coin, leverage, direction)

            amount = self._calculate_position_size(coin, price, leverage)
            if amount <= 0:
                logger.info(f"{coin}: no budget available")
                return False

            side = 'buy' if direction == 'LONG' else 'sell'
            # Use limit order for 0.02% maker fee (vs 0.05% taker)
            order = self.exchange.place_limit_order(coin, side, amount)

            if not order:
                logger.error(f"Order failed: {coin}")
                return False

            entry_price = order.price or price

            # Track position
            self._tracked[coin] = TrackedPosition(
                coin=coin, direction=direction, entry_price=entry_price,
                size=amount, leverage=leverage, entry_time=time.time()
            )

            info = self.exchange.get_contract_info(coin)
            cs = info.get('contractSize', 1)
            notional = amount * cs * entry_price
            margin = notional / leverage

            self._trade_count += 1
            logger.info(f"OPEN #{self._trade_count}: {direction} {coin} {amount}@${entry_price:.4f} "
                       f"{leverage}x margin=${margin:.2f} notional=${notional:.2f}")
            self._notify(
                f"{direction} {coin} {leverage}x",
                f"Entry: ${entry_price:.4f}\nScore: {score:+.4f}\n"
                f"Notional: ${notional:.1f}\nTarget: {TARGET_PCT*100:.1f}%"
            )
            return True
        except Exception as e:
            logger.error(f"Open failed {coin}: {e}")
            return False

    def _close_trade(self, coin, reason, pnl_pct):
        """Close position with limit order for lower fees."""
        tracked = self._tracked.get(coin)
        if not tracked:
            return False

        try:
            close_side = 'sell' if tracked.direction == 'LONG' else 'buy'
            # Use limit for TP/TRAIL (not urgent), market for EMERGENCY
            if reason == 'EMERGENCY_STOP':
                result = self.exchange.close_position(coin, close_side, tracked.size)
            else:
                result = self.exchange.place_limit_order(coin, close_side, tracked.size)

            pnl_lev = pnl_pct * tracked.leverage * 100
            info = self.exchange.get_contract_info(coin)
            cs = info.get('contractSize', 1)
            notional = tracked.size * cs * tracked.entry_price
            pnl_usd = notional * pnl_pct

            self._total_pnl += pnl_usd
            held_min = (time.time() - tracked.entry_time) / 60

            emoji = '💰' if pnl_pct > 0 else '🔻'
            logger.info(f"CLOSE {coin}: {reason} | {pnl_lev:+.1f}% lev | ${pnl_usd:+.2f} | "
                       f"{held_min:.0f}min | total=${self._total_pnl:+.2f}")
            self._notify(
                f"{emoji} CLOSE {coin} — {reason}",
                f"PnL: {pnl_lev:+.1f}% ({pnl_pct*100:+.2f}%)\n"
                f"${pnl_usd:+.2f} in {held_min:.0f}min\n"
                f"Day total: ${self._total_pnl:+.2f}"
            )

            # Log trade for model learning
            self._log_trade(coin, tracked, result.price if result else 0, pnl_pct, pnl_usd, reason)

            del self._tracked[coin]
            return True
        except Exception as e:
            logger.error(f"Close failed {coin}: {e}")
            return False

    def _on_price_tick(self, coin, price, bid, ask):
        """Called on EVERY price update from WebSocket (~6/sec).
        Instant exit when conditions met — no delay.
        """
        if coin not in self._tracked:
            return
        if coin in self._closing_lock:
            return  # already closing, prevent double order

        tracked = self._tracked[coin]
        exit_signal, pnl_pct = tracked.update(price)

        if exit_signal:
            self._closing_lock.add(coin)  # lock before closing
            logger.info(f"WS EXIT {coin}: {exit_signal} at ${price:.4f} (PnL={pnl_pct*100:+.3f}%)")
            self._close_trade(coin, exit_signal, pnl_pct)
            self._closing_lock.discard(coin)

    def _manage_positions(self):
        """Fallback: check positions using REST (if WS down)."""
        for coin in list(self._tracked.keys()):
            try:
                # Use WebSocket price (instant, no API call)
                price = self._price_stream.get_price(coin)
                if price <= 0:
                    # Fallback to REST if WS has no data
                    ticker = self.exchange.get_ticker(coin)
                    price = ticker['price']
                if price <= 0:
                    continue

                tracked = self._tracked[coin]
                exit_signal, pnl_pct = tracked.update(price)

                if exit_signal:
                    self._close_trade(coin, exit_signal, pnl_pct)
            except Exception as e:
                logger.debug(f"Manage {coin}: {e}")

    def _sync_with_exchange(self):
        """Sync tracked positions with exchange reality. Prevents duplicates."""
        try:
            real_positions = self.exchange.get_positions()
            real_coins = {}
            for p in real_positions:
                coin = p.symbol.replace('/USDT:USDT', '')
                real_coins[coin] = p

            for coin in list(self._tracked.keys()):
                if coin not in real_coins:
                    logger.info(f"Sync: {coin} no longer on exchange, removing from tracked")
                    del self._tracked[coin]

            for coin, p in real_coins.items():
                if coin not in self._tracked:
                    direction = 'LONG' if p.side == 'long' else 'SHORT'
                    self._tracked[coin] = TrackedPosition(
                        coin=coin, direction=direction, entry_price=p.entry_price,
                        size=p.size, leverage=p.leverage, entry_time=time.time()
                    )
                    logger.info(f"Sync: tracking existing {direction} {coin}")
        except Exception as e:
            logger.debug(f"Sync error: {e}")

    def _is_new_4h_candle(self):
        """Check if we're at a new 4h candle boundary (00,04,08,12,16,20 UTC)."""
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        # 4h boundary = hour divisible by 4, first 10 minutes
        candle_hour = (now.hour // 4) * 4
        candle_ts = now.replace(hour=candle_hour, minute=0, second=0, microsecond=0).timestamp()

        if candle_ts > self._last_4h_candle:
            self._last_4h_candle = candle_ts
            self._traded_this_cycle.clear()  # reset traded coins for new cycle
            logger.info(f"New 4h candle: {now.strftime('%H:%M')} UTC")
            return True
        return False

    def _open_new_positions(self):
        """Open positions only at 4h candle boundaries, not between them."""
        self._sync_with_exchange()

        # Only look for new entries at 4h boundaries
        if not self._is_new_4h_candle() and len(self._tracked) > 0:
            return  # between candles — only monitor, don't open new

        slots = MAX_POSITIONS - len(self._tracked)
        if slots <= 0:
            return

        preds = self._get_signals()
        if len(preds) < 8:
            return

        regime, btc_mom = self._get_regime()
        # Sort by adjusted score (includes funding rate bonus)
        ranked = sorted(preds, key=lambda x: x.get('reg_score_adj', x['reg_score']), reverse=True)

        # Direction based on regime
        if regime == 'BULL':
            n_long = slots
            n_short = 0
        elif regime == 'BEAR':
            n_long = 0
            n_short = slots
        else:
            n_long = max(1, slots // 3)
            n_short = slots - n_long

        signals = []
        for p in ranked[:n_long]:
            signals.append((p['coin'], 'LONG', p['reg_score'], p.get('funding_rate', 0)))
        for p in ranked[-n_short:] if n_short > 0 else []:
            signals.append((p['coin'], 'SHORT', p['reg_score'], p.get('funding_rate', 0)))

        logger.info(f"Regime: {regime} | BTC {btc_mom:+.1f}% | Slots: {slots} | "
                    f"Signals: {n_long}L+{n_short}S | Tracked: {len(self._tracked)}")

        for signal in signals:
            coin, direction, score = signal[0], signal[1], signal[2]
            funding = signal[3] if len(signal) > 3 else 0

            if coin in self._tracked:
                continue
            if coin in self._traded_this_cycle:
                continue  # already traded this coin in this 4h cycle
            if slots <= 0:
                break

            success = self._open_trade(coin, direction, score)
            if success:
                slots -= 1
                self._traded_this_cycle.add(coin)
                if funding != 0:
                    fr_info = f"(funding {funding*100:.3f}% → {'getting paid' if (direction=='SHORT' and funding>0) or (direction=='LONG' and funding<0) else 'paying'})"
                    logger.info(f"  {coin} {fr_info}")

    def _collect_data(self):
        """Collect fresh market data."""
        try:
            from src.crypto.data_collector import collect_all
            collect_all(heartbeat_fn=self._write_heartbeat)
        except Exception as e:
            logger.warning(f"Data collection: {e}")

    def _write_heartbeat(self):
        hb = _FACTORY_DIR / 'data' / 'crypto' / 'heartbeat.txt'
        hb.write_text(str(int(time.time())))

    def run(self):
        """Main loop: scan every 60s, manage positions continuously."""
        logger.info(f"OKX Demo Trader starting | Capital=${self.capital}")

        if not self.exchange.connect():
            logger.error("OKX connection failed")
            return

        bal = self.exchange.get_balance()
        logger.info(f"Balance: ${bal['total']:.2f} | Max positions: {MAX_POSITIONS}")

        # Start WebSocket price stream with real-time exit callback
        self._price_stream.on_price_update = self._on_price_tick
        self._price_stream.start()
        time.sleep(2)
        ws_status = "LIVE (tick-by-tick)" if self._price_stream.is_connected() else "FALLBACK (REST)"
        logger.info(f"Price stream: {ws_status}")

        self._notify("Trader Started",
                    f"Balance: ${bal['total']:.2f}\n"
                    f"Max positions: {MAX_POSITIONS}\n"
                    f"Target: {TARGET_PCT*100:.1f}% per trade\n"
                    f"Price stream: {ws_status}")

        self._running = True
        last_signal_scan = 0
        scan_count = 0

        while self._running:
            try:
                self._write_heartbeat()
                now = time.time()

                # Manage positions every 60s (smart exits)
                self._manage_positions()

                # Collect data every 30 min
                if now - self._last_data_collect > 1800:
                    self._collect_data()
                    self._last_data_collect = now

                # Open new positions every 5 min (if slots available)
                if now - last_signal_scan > 300:
                    self._open_new_positions()
                    last_signal_scan = now
                    scan_count += 1

                    # Log status every 6 scans (30 min)
                    if scan_count % 6 == 0:
                        eq = self._get_equity()
                        logger.info(f"STATUS: {len(self._tracked)} positions | "
                                   f"equity=${eq:.2f} | trades={self._trade_count} | "
                                   f"pnl=${self._total_pnl:+.2f}")

                # Daily retrain at 02:00 UTC
                current_hour = datetime.now(timezone.utc).hour
                if current_hour == 2 and now - self._last_retrain > 72000:
                    self._daily_retrain()
                    self._last_retrain = now

                time.sleep(SCAN_INTERVAL)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop: {e}", exc_info=True)
                time.sleep(60)

        # Close all on shutdown
        logger.info("Shutting down — closing all positions")
        for coin in list(self._tracked.keys()):
            tracked = self._tracked[coin]
            ticker = self.exchange.get_ticker(coin)
            if ticker['price'] > 0:
                _, pnl = tracked.update(ticker['price'])
                self._close_trade(coin, 'SHUTDOWN', pnl)

        self._price_stream.stop()
        logger.info(f"Stopped. Total P&L: ${self._total_pnl:+.2f} | Trades: {self._trade_count}")
        self._notify("Trader Stopped",
                    f"Total P&L: ${self._total_pnl:+.2f}\n"
                    f"Trades: {self._trade_count}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=float, default=5000.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(LOG_DIR / 'trader_okx.log')),
        ]
    )

    trader = OKXTrader(capital=args.capital)
    trader.run()


if __name__ == '__main__':
    main()
