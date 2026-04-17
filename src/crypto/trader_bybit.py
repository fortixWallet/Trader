"""
FORTIX v4 — Predictive Level-Based Trading
============================================

Strategy:
  - HOURLY scan: Opus analyzes S/R levels for all coins
  - LIMIT ORDERS at levels (price comes to us, not we chase price)
  - SL/TP placed ON BYBIT EXCHANGE (not in code — works if Mac sleeps)
  - 4 max positions + pending orders, 10% equity per position
  - Emergency backup: WS tick-by-tick + -4% hard stop
  - News reactor: impact 7+ cancels pending + manages open positions
  - 25 coins + hot coin discovery (539 pairs)
"""

import os
import sys
import time
import json
import base64
import signal
import sqlite3
import logging
import requests
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger('trader_bybit')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'  # use 4h model (proven)
LOG_DIR = _FACTORY_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# === CONFIG (all percentages — works same on $1K and $1M) ===
MAX_POSITIONS = 50  # no practical limit — simulation had none. Budget controls exposure.
# TARGET_PCT is now adaptive per coin (see _get_coin_tp)
TRAILING_ACTIVATE = 0.70 # activate trailing at 70% of target
TRAILING_TIGHT = 0.90    # tighten trailing at 90% of target
TRAIL_WIDE = 0.003       # 0.3% trailing distance (before 90%)
TRAIL_TIGHT = 0.0015     # 0.15% trailing distance (after 90%)
EMERGENCY_STOP = -0.04   # -4% hard stop
MAX_HOLD_MIN = 240       # 4h max hold
STAGNANT_MIN = 180       # 3h — close stagnant positions early
STAGNANT_THRESHOLD = 0.002  # ±0.2% = stagnant
SCAN_INTERVAL = 5
MAX_PENDING = 8          # simulation used max 3/hour × 2 batches = 6. 8 for safety.
RISK_PER_POSITION = 0.10 # 10% of equity per position
MAX_EQUITY_USED = 0.80
MIN_LEVERAGE = 7
DAILY_LOSS_LIMIT = -0.04 # -4% of equity → stop trading for the day
# Simulation avoided these coins — they break S/R levels (Rules 11, 33, 44)
BAD_COINS = {'BOME', 'DOT', 'AAVE', 'WIF', 'DOGE', 'OP', 'RENDER', 'TAO', 'ARB'}

# Bybit Demo: 36 coins (25 original + 11 quality expansion)
COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO', 'BOME',
         # Quality expansion — all verified on Bybit + have deep profiles
         'SUI', 'LTC', 'BCH', 'TRX', 'NEAR', 'HBAR', 'TON', 'APT',
         'FIL', 'ALGO', 'XLM']
# Excluded: BONK, PEPE, SHIB, FET, CAKE, WLD, ENA (memes/volatile, low WR)

# Sector mapping for diversification
COIN_SECTOR = {
    'BTC': 'major', 'ETH': 'major',
    'SOL': 'L1', 'AVAX': 'L1', 'ADA': 'L1', 'DOT': 'L1',
    'SUI': 'L1', 'NEAR': 'L1', 'APT': 'L1', 'TON': 'L1', 'HBAR': 'L1',
    'AAVE': 'defi', 'UNI': 'defi', 'LDO': 'defi', 'CRV': 'defi', 'PENDLE': 'defi', 'JUP': 'defi',
    'LINK': 'infra', 'RENDER': 'infra', 'TAO': 'infra', 'PYTH': 'infra', 'JTO': 'infra',
    'FIL': 'infra', 'ALGO': 'infra',
    'DOGE': 'meme', 'WIF': 'meme', 'BOME': 'meme',
    'BNB': 'exchange',
    'XRP': 'payment', 'LTC': 'payment', 'BCH': 'payment', 'TRX': 'payment', 'XLM': 'payment',
    'ARB': 'L2', 'OP': 'L2', 'POL': 'L2',
}


class PendingOrder:
    """Limit order waiting to be filled at a S/R level."""
    def __init__(self, coin, direction, entry_price, sl_price, tp_price,
                 size, leverage, order_id, reason="", created_at=None):
        self.coin = coin
        self.direction = direction
        self.entry_price = entry_price
        self.sl_price = sl_price
        self.tp_price = tp_price
        self.size = size
        self.leverage = leverage
        self.order_id = order_id
        self.reason = reason
        self.created_at = created_at or time.time()
        self.trade_id = None  # set by trade journal

    def is_expired(self, max_hours=4):
        return (time.time() - self.created_at) / 3600 >= max_hours


class TrackedPosition:
    """Open position with SL/TP orders on exchange (not in our code)."""
    def __init__(self, coin, direction, entry_price, size, leverage, entry_time,
                 sl_order_id=None, tp_order_id=None, sl_price=0, tp_price=0,
                 reason="", target_pct=0.01, sl_pct=0.01, max_hold_hours=8):
        self.coin = coin
        self.direction = direction
        self.entry_price = entry_price
        self.size = size
        self.leverage = leverage
        self.entry_time = entry_time
        self.sl_order_id = sl_order_id
        self.tp_order_id = tp_order_id
        self.sl_price = sl_price
        self.tp_price = tp_price
        self.reason = reason
        self.target_pct = target_pct
        self.sl_pct = sl_pct
        self.max_hold_hours = max_hold_hours
        self.peak_pnl = 0.0
        self.trailing_active = False
        self.trail_stop_pnl = None
        self.trade_id = None  # set by trade journal

    def update(self, current_price):
        """Backup check — SL/TP should be handled by exchange.
        This only catches emergencies if exchange orders fail."""
        if self.direction == 'SHORT':
            pnl_pct = (self.entry_price - current_price) / self.entry_price
        else:
            pnl_pct = (current_price - self.entry_price) / self.entry_price

        roi = pnl_pct * self.leverage * 100  # ROI in %

        if roi > self.peak_pnl:
            self.peak_pnl = roi

        # Trailing stop: activation +6% ROI, drop -2% from peak
        if self.peak_pnl >= 6.0:
            self.trailing_active = True
        if self.trailing_active and roi <= self.peak_pnl - 2.0:
            return 'TRAILING_STOP', pnl_pct

        # SL: -6.5% ROI
        if roi <= -6.5:
            return 'STOP_LOSS', pnl_pct

        # TP: +13% ROI
        if roi >= 13.0:
            return 'TARGET_HIT', pnl_pct

        # Emergency backup
        if pnl_pct <= EMERGENCY_STOP:
            return 'EMERGENCY_STOP', pnl_pct

        # Time exit backup
        held_hours = (time.time() - self.entry_time) / 3600
        if held_hours >= self.max_hold_hours:
            return 'TIME_EXIT', pnl_pct

        return None, pnl_pct


class BybitTrader:
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
        self._last_signals = []
        self._daily_pnl_pct = 0.0
        self._current_day = ''
        self._weekly_coin_pnl = defaultdict(float)
        self._banned_coins = set()
        self._circuit_breaker_time = 0
        self._post_cb_size_factor = 1.0
        self._coin_cooldown = {}  # coin -> timestamp of last SL (2h cooldown)
        self._pending_orders = {}  # coin -> PendingOrder (limit orders waiting to fill)
        self._processed_closed_pnl = set()  # track already-processed closedPnl entries
        self._last_closed_pnl_ts = int(time.time() * 1000)  # only process closes AFTER this timestamp
        self._api_down = {}  # api_name -> timestamp when detected down
        self._last_api_alert = {}  # api_name -> last alert timestamp

        # Trade journal
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
        from src.crypto.exchange_client_bybit import create_client
        self.exchange = create_client()

        # News Reactor — real-time news impact
        from src.crypto.news_reactor import NewsReactor
        self._news_reactor = NewsReactor()
        self._last_news_check = 0

        # Ensemble predictor — 3 models vote
        from src.crypto.ensemble_predictor import EnsemblePredictor
        self._ensemble = EnsemblePredictor()
        self._ensemble.load()

        # Gemini multi-timeframe analyzer
        from src.crypto.gemini_analyzer import GeminiAnalyzer
        self._gemini = GeminiAnalyzer()

        # Profi (Opus) — deep decision maker
        from src.crypto.profi import Profi
        self._profi = Profi()  # exchange set after connect()
        self._profi_daily_strategy = None

        # Gemini Scanner — fast eyes (Layer 1)
        from src.crypto.gemini_scanner import GeminiScanner
        self._gemini = GeminiScanner(
            api_key=os.environ.get('GEMINI_API_KEY', ''),
            knowledge_text=self._profi._knowledge
        )

        # Consilium — independent voting system
        from src.crypto.consilium import Consilium, Vote
        self._consilium = Consilium()

        # Trade journal — learning system
        from src.crypto.trade_journal import TradeJournal
        self._journal = TradeJournal()

        # WebSocket — Bybit uses production URL for public data
        from src.crypto.ws_price_stream_bybit import PriceStream
        self._price_stream = PriceStream(list(COINS))

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
                json={'chat_id': self._tg_chat, 'text': f"<b>🟪 Bybit {title}</b>\n{body}", 'parse_mode': 'HTML'},
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
        """Daily: self-analysis + retrain ML + recompile knowledge."""
        # 0. Opus self-analysis — learn from today's trades
        try:
            logger.info("Opus daily self-analysis...")
            self._run_daily_self_analysis()
        except Exception as e:
            logger.error(f"Self-analysis failed: {e}")

        # 0.5. Weekly synthesis on Monday — compress week's lessons into rules
        if datetime.now(timezone.utc).weekday() == 0:
            try:
                logger.info("Monday — running weekly synthesis...")
                self._run_weekly_synthesis()
            except Exception as e:
                logger.error(f"Weekly synthesis failed: {e}")

        # 1. Profi reviews yesterday's trades — learns from mistakes
        try:
            logger.info("Profi reviewing yesterday's trades...")
            from src.crypto.profi_learner import run_daily_review
            run_daily_review()
        except Exception as e:
            logger.error(f"Trade review failed: {e}")

        # 2. Recompile knowledge from fresh data
        try:
            logger.info("Recompiling Profi knowledge...")
            from src.crypto.knowledge_compiler import compile_knowledge
            from src.crypto.knowledge_advanced import compile as compile_advanced
            compile_knowledge()
            compile_advanced()
            # Reload into Profi
            self._profi._knowledge = self._profi._load_knowledge()
            logger.info(f"Profi knowledge updated: {len(self._profi._knowledge):,} chars")
        except Exception as e:
            logger.error(f"Knowledge recompile failed: {e}")

        # 2. Retrain ML model
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
            self._notify("Daily Update",
                        f"ML: Spearman={sp:.4f}\n"
                        f"Profi: {len(self._profi._knowledge):,} chars knowledge\n"
                        f"Features: {results.get('n_features', 0)}")
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
        """Determine regime from MARKET BREADTH + BTC, not BTC alone.
        Fixed: BTC +0.6% but 19/22 alts falling = BEAR, not BULL.
        """
        try:
            conn = sqlite3.connect(str(DB_PATH))

            # 1. BTC momentum (secondary signal)
            btc = conn.execute(
                "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 7"
            ).fetchall()
            btc_12h = (btc[0][0] / btc[3][0] - 1) * 100 if len(btc) >= 4 else 0

            # 2. Market breadth (PRIMARY signal) — % of coins going up last candle
            breadth_rows = conn.execute("""
                SELECT p1.coin, p1.close as now, p2.close as prev
                FROM prices p1
                JOIN prices p2 ON p1.coin = p2.coin
                WHERE p1.timeframe = '4h' AND p2.timeframe = '4h'
                AND p1.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h')
                AND p2.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h') - 14400
                AND p1.coin != 'BTC'
            """).fetchall()
            conn.close()

            if breadth_rows:
                up_count = sum(1 for r in breadth_rows if r[1] > r[2])
                total = len(breadth_rows)
                breadth = up_count / total if total > 0 else 0.5
            else:
                breadth = 0.5

            # Regime from breadth (primary) + BTC (secondary)
            bull_score = 0
            bear_score = 0

            # Breadth is the PRIMARY driver
            if breadth > 0.65: bull_score += 2
            elif breadth > 0.55: bull_score += 1
            if breadth < 0.35: bear_score += 2
            elif breadth < 0.45: bear_score += 1

            # BTC as confirmation
            if btc_12h > 0.5: bull_score += 1
            if btc_12h < -0.5: bear_score += 1

            # News sentiment as additional signal
            try:
                news_sentiment = self._news_reactor.get_market_sentiment()
                if news_sentiment == 'BULLISH': bull_score += 1
                elif news_sentiment == 'BEARISH': bear_score += 1
            except Exception:
                pass

            # Gemini multi-timeframe (strongest signal — AI sees full picture)
            try:
                gemini_regime, gemini_conf = self._gemini.get_regime_signal()
                if gemini_conf >= 0.7:
                    if gemini_regime == 'BULL': bull_score += 2
                    elif gemini_regime == 'BEAR': bear_score += 2
                elif gemini_conf >= 0.5:
                    if gemini_regime == 'BULL': bull_score += 1
                    elif gemini_regime == 'BEAR': bear_score += 1
            except Exception:
                pass

            if bull_score >= 2:
                regime = 'BULL'
            elif bear_score >= 2:
                regime = 'BEAR'
            else:
                regime = 'FLAT'

            # MACRO TREND FILTER — don't trade against 7-day trend
            # Prevents: shorting in pullbacks during bull rallies (today's #1 loss cause)
            # Override: if 1-day move > 3% against macro → allow (real crash/reversal)
            try:
                conn2 = sqlite3.connect(str(DB_PATH))
                btc_7d = conn2.execute(
                    "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
                    "ORDER BY timestamp DESC LIMIT 42"
                ).fetchall()  # 42 × 4h = 7 days
                btc_1d = conn2.execute(
                    "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
                    "ORDER BY timestamp DESC LIMIT 6"
                ).fetchall()  # 6 × 4h = 24h
                conn2.close()

                if len(btc_7d) >= 42 and len(btc_1d) >= 6:
                    macro_7d = (btc_7d[0][0] / btc_7d[-1][0] - 1) * 100
                    change_1d = (btc_1d[0][0] / btc_1d[-1][0] - 1) * 100

                    if macro_7d >= 3.0 and change_1d > -3.0 and regime == 'BEAR':
                        logger.info(f"MACRO FILTER: BTC +{macro_7d:.1f}% 7d, {change_1d:+.1f}% 1d → "
                                   f"block BEAR (pullback, not crash) → FLAT")
                        regime = 'FLAT'
                    elif macro_7d <= -3.0 and change_1d < 3.0 and regime == 'BULL':
                        logger.info(f"MACRO FILTER: BTC {macro_7d:.1f}% 7d, {change_1d:+.1f}% 1d → "
                                   f"block BULL (bounce, not reversal) → FLAT")
                        regime = 'FLAT'
            except Exception:
                pass

            return regime, btc_12h

        except Exception:
            return 'FLAT', 0

    def _get_coin_atr(self, coin):
        """Raw ATR % for a coin (4h candles)."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            rows = conn.execute(
                "SELECT high, low, close FROM prices WHERE coin=? AND timeframe='4h' "
                "ORDER BY timestamp DESC LIMIT 30", (coin,)
            ).fetchall()
            conn.close()
            if len(rows) < 10:
                return 0.02
            atrs = [(r[0]-r[1])/r[2] for r in rows if r[2] > 0]
            return np.mean(atrs) if atrs else 0.02
        except Exception:
            return 0.02

    def _get_coin_tp(self, coin):
        """Adaptive TP based on coin's ATR. Volatile coins get higher TP."""
        atr = self._get_coin_atr(coin)
        tp = atr * 0.50  # 50% of ATR
        return max(0.003, min(tp, 0.015))

    def _calculate_confidence(self, regime, btc_mom, ranked):
        """How confident are we? Smooth 0.0-1.0 scale.
        Considers: BTC momentum strength, spread between best/worst coins, regime clarity.
        """
        confidence = 0.5  # baseline

        # 1. BTC momentum strength (stronger move = more confident)
        btc_strength = min(abs(btc_mom) / 2.0, 0.3)  # max +0.3 from BTC
        confidence += btc_strength

        # 2. Spread between top and bottom coins (bigger spread = clearer signal)
        if len(ranked) >= 6:
            top_avg = np.mean([p['reg_score'] for p in ranked[:3]])
            bot_avg = np.mean([p['reg_score'] for p in ranked[-3:]])
            spread = top_avg - bot_avg
            spread_boost = min(spread / 0.01, 0.2)  # max +0.2 from spread
            confidence += spread_boost

        # 3. Regime clarity (BULL/BEAR = more confident than FLAT)
        if regime != 'FLAT':
            confidence += 0.1

        # Clamp to 0.3-1.0 (never fully zero — always allow at least 1 small position)
        confidence = max(0.3, min(confidence, 1.0))

        return confidence

    def _consilium_decide(self, coin, direction, features_dict, score):
        """Run Consilium voting for a trade candidate.
        Each expert votes INDEPENDENTLY (sealed envelope).

        Returns: (should_trade: bool, consilium_confidence: float, report: str)
        """
        from src.crypto.consilium import Vote

        votes = []

        # 1. LightGBM vote (from ensemble)
        try:
            ens = self._ensemble.predict(features_dict)
            lgb_dir = 'LONG' if ens['lgb'] > 0.001 else ('SHORT' if ens['lgb'] < -0.001 else 'WAIT')
            votes.append(Vote(
                expert='lightgbm',
                direction=lgb_dir,
                confidence=min(abs(ens['lgb']) * 100, 1.0),
                reason=f"score={ens['lgb']:.4f}"
            ))

            # 2. XGBoost vote
            xgb_dir = 'LONG' if ens['xgb'] > 0.001 else ('SHORT' if ens['xgb'] < -0.001 else 'WAIT')
            votes.append(Vote(
                expert='xgboost',
                direction=xgb_dir,
                confidence=min(abs(ens['xgb']) * 100, 1.0),
                reason=f"score={ens['xgb']:.4f}"
            ))

            # 3. Neural Net vote
            nn_dir = 'LONG' if ens['nn'] > 0.001 else ('SHORT' if ens['nn'] < -0.001 else 'WAIT')
            votes.append(Vote(
                expert='neural_net',
                direction=nn_dir,
                confidence=min(abs(ens['nn']) * 100, 1.0),
                reason=f"score={ens['nn']:.4f}"
            ))
        except Exception as e:
            logger.debug(f"Ensemble vote error: {e}")

        # 4. Market Breadth vote
        try:
            regime, btc_mom = self._get_regime()
            if regime == 'BULL':
                breadth_dir = 'LONG'
                breadth_conf = 0.7
            elif regime == 'BEAR':
                breadth_dir = 'SHORT'
                breadth_conf = 0.7
            else:
                breadth_dir = 'WAIT'
                breadth_conf = 0.3
            votes.append(Vote(
                expert='market_breadth',
                direction=breadth_dir,
                confidence=breadth_conf,
                reason=f"regime={regime}, BTC mom={btc_mom:+.1f}%"
            ))
        except Exception as e:
            logger.debug(f"Breadth vote error: {e}")

        # 5. News Reactor vote
        try:
            sentiment = self._news_reactor.get_market_sentiment()
            if sentiment == 'BULLISH':
                news_dir = 'LONG'
                news_conf = 0.6
            elif sentiment == 'BEARISH':
                news_dir = 'SHORT'
                news_conf = 0.6
            else:
                news_dir = 'WAIT'
                news_conf = 0.3
            votes.append(Vote(
                expert='news_reactor',
                direction=news_dir,
                confidence=news_conf,
                reason=f"sentiment={sentiment}"
            ))
        except Exception as e:
            logger.debug(f"News vote error: {e}")

        # 6. Profi vote (Claude — independent expert)
        try:
            # Get current price for entry/tp/sl
            ticker = self.exchange.get_ticker(coin)
            price = ticker['price']

            # Build features summary for Profi
            profi_features = {
                'rsi': features_dict.get('rsi_14', 50),
                'bb_position': features_dict.get('bb_position', 0.5),
                'volume_ratio': features_dict.get('volume_ratio', 1.0),
                'atr_pct': features_dict.get('atr_pct_14', 0.02),
                'funding_rate': features_dict.get('funding_rate', 0),
            }

            # Generate charts for Profi's vision analysis (3 timeframes)
            charts = {}
            try:
                from src.crypto.chart_generator import generate_chart
                for tf, days in [('1d', 60), ('4h', 30), ('1h', 7)]:
                    chart_bytes = generate_chart(coin, tf, days)
                    if chart_bytes:
                        charts[tf] = base64.b64encode(chart_bytes).decode()
            except Exception as e:
                logger.debug(f"Chart gen for {coin}: {e}")

            # Profi analyzes INDEPENDENTLY (doesn't see other votes)
            profi_result = self._profi.analyze_trade(
                coin=coin,
                direction=direction,  # suggestion only — Profi forms own opinion
                charts=charts,
                features=profi_features,
                news=""
            )

            profi_dir = profi_result.get('decision', 'WAIT')
            profi_conf = float(profi_result.get('confidence', 0))

            votes.append(Vote(
                expert='profi',
                direction=profi_dir,
                confidence=profi_conf,
                reason=profi_result.get('reason', '')[:60],
                details={
                    'entry': profi_result.get('entry', 0),
                    'tp': profi_result.get('tp', 0),
                    'sl': profi_result.get('sl', 0),
                }
            ))
        except Exception as e:
            logger.debug(f"Profi vote error: {e}")

        # Run Consilium
        decision = self._consilium.decide(coin, votes)

        # Log report
        report = self._consilium.format_report(decision)
        if decision.action != 'SKIP':
            logger.info(f"\n{report}")

        should_trade = decision.action in ('LONG', 'SHORT')
        return should_trade, decision.confidence, decision.action, decision.size_factor, report

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
        """Position sizing: compound — uses live equity, not fixed capital."""
        # Compound: use actual equity (profits grow position sizes)
        equity = self._get_equity()
        budget = max(equity, self.capital) if equity and equity > 0 else self.capital

        used_margin = sum(
            p.size * p.entry_price / p.leverage
            for p in self._tracked.values()
        )
        available = budget * MAX_EQUITY_USED - used_margin
        if available <= 0:
            return 0

        confidence = getattr(self, '_last_confidence', 0.7)
        cb_factor = getattr(self, '_post_cb_size_factor', 1.0)
        target_margin = min(budget * RISK_PER_POSITION * confidence * cb_factor, available)
        target_notional = target_margin * leverage

        info = self.exchange.get_contract_info(coin)
        contract_size = info.get('contractSize', 1)
        min_amount = info.get('minAmount', 0.001)
        max_amount = info.get('maxAmount', 999999)

        cost_per = price * contract_size
        amount = target_notional / cost_per

        # Respect exchange limits
        amount = max(min_amount, min(amount, max_amount * 0.9))  # 90% of max

        # Round to reasonable precision
        if amount >= 100:
            amount = int(amount)
        elif amount >= 1:
            amount = round(amount, 1)
        else:
            amount = round(amount, 3)

        return amount

    def _open_trade(self, coin, direction, score, leverage_override=None,
                    tp_price=None, sl_price=None, max_hold_hours=None):
        """Open a position with Profi-driven parameters."""
        try:
            ticker = self.exchange.get_ticker(coin)
            price = ticker['price']
            if price <= 0:
                return False

            # Use Profi's recommended leverage, or default
            info = self.exchange.get_contract_info(coin)
            max_lev = int(info.get('maxLeverage', 100))
            if leverage_override:
                leverage = min(leverage_override, max_lev)
            else:
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

            if not order.price or order.price <= 0:
                logger.error(f"Order {coin}: no fill price returned — aborting")
                return False
            entry_price = order.price
            amount = order.amount or amount  # use ACTUAL filled amount, not requested

            # TP from Profi's levels or adaptive ATR
            if tp_price and sl_price and entry_price > 0:
                if direction == 'LONG':
                    coin_tp = abs(tp_price - entry_price) / entry_price
                else:
                    coin_tp = abs(entry_price - tp_price) / entry_price
                coin_tp = max(0.003, min(coin_tp, 0.05))
            else:
                coin_tp = self._get_coin_tp(coin)

            # Cap TP/SL by real ATR — Profi often sets unrealistic targets
            coin_atr = self._get_coin_atr(coin)
            max_tp = max(coin_atr, 0.005)  # at least 0.5%, max 1× ATR
            coin_tp = min(coin_tp, max_tp)

            # SL from Profi's levels
            if sl_price and entry_price > 0:
                if direction == 'LONG':
                    coin_sl = abs(entry_price - sl_price) / entry_price
                else:
                    coin_sl = abs(sl_price - entry_price) / entry_price
                coin_sl = max(0.003, min(coin_sl, 0.05))
            else:
                coin_sl = coin_tp  # default: SL = TP

            # Cap SL same as TP
            coin_sl = min(coin_sl, max_tp)

            self._tracked[coin] = TrackedPosition(
                coin=coin, direction=direction, entry_price=entry_price,
                size=amount, leverage=leverage, entry_time=time.time(),
                target_pct=coin_tp, sl_pct=coin_sl,
                max_hold_hours=max_hold_hours or 8,
                reason=getattr(self, '_current_trade_reason', '')
            )

            info = self.exchange.get_contract_info(coin)
            cs = info.get('contractSize', 1)
            notional = amount * cs * entry_price
            margin = notional / leverage

            self._trade_count += 1
            logger.info(f"OPEN #{self._trade_count}: {direction} {coin} {amount}@${entry_price:.4f} "
                       f"{leverage}x margin=${margin:.2f} notional=${notional:.2f} "
                       f"TP={coin_tp*100:.1f}%")
            roi_tp = coin_tp * leverage * 100
            roi_sl = coin_sl * leverage * 100
            self._notify(
                f"{direction} {coin} {leverage}x",
                f"Entry: ${entry_price:.4f}\n"
                f"TP: {coin_tp*100:.1f}% (ROI +{roi_tp:.0f}%)\n"
                f"SL: {coin_sl*100:.1f}% (ROI -{roi_sl:.0f}%)\n"
                f"Margin: ${margin:.1f} | Hold: {max_hold_hours or 8}h"
            )
            return True
        except Exception as e:
            logger.error(f"Open failed {coin}: {e}")
            return False

    def _close_trade(self, coin, reason, pnl_pct):
        """Close position with reduceOnly. Cancel SL/TP from exchange. Verify fill."""
        tracked = self._tracked.get(coin)
        if not tracked:
            return False
        if coin in self._closing_lock:
            return False
        self._closing_lock.add(coin)

        try:
            # Cancel SL/TP orders on exchange BEFORE closing (prevent double execution)
            if tracked.sl_order_id:
                self.exchange.cancel_order(tracked.sl_order_id, coin)
            if tracked.tp_order_id:
                self.exchange.cancel_order(tracked.tp_order_id, coin)

            close_side = 'sell' if tracked.direction == 'LONG' else 'buy'
            # ALL closes use reduceOnly — NEVER create opposite position
            result = self.exchange.close_position(coin, close_side, tracked.size)

            if not result:
                logger.warning(f"Close failed {coin}, retrying...")
                result = self.exchange.close_position(coin, close_side, tracked.size)

            if not result:
                # Still failed — REMOVE from tracking to prevent infinite loop
                # sync_with_exchange will re-adopt if position still exists
                logger.error(f"Close FAILED {coin} — removing from tracking to prevent spam")
                self._tracked.pop(coin, None)
                self._closing_lock.discard(coin)
                return False

            # Close succeeded — NOW remove from tracking
            self._tracked.pop(coin, None)

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
                f"ROI: {pnl_lev:+.1f}% | Price: {pnl_pct*100:+.2f}%\n"
                f"${pnl_usd:+.2f} in {held_min:.0f}min\n"
                f"Day total: ${self._total_pnl:+.2f}"
            )

            equity = self._get_equity() or self.capital
            pnl_pct_of_equity = pnl_usd / equity if equity > 0 else 0
            self._daily_pnl_pct += pnl_pct_of_equity
            self._weekly_coin_pnl[coin] += pnl_pct_of_equity

            self._log_trade(coin, tracked, result.price if result else 0, pnl_pct, pnl_usd, reason)

            # Record in trade journal (learning system)
            if hasattr(tracked, 'trade_id') and tracked.trade_id:
                exit_price = result.price if result else 0
                held_min = (time.time() - tracked.entry_time) / 60
                self._journal.record_close(tracked.trade_id, exit_price, pnl_pct, pnl_usd, reason, held_min)

            # Cooldown after SL — don't reopen same coin for 2 hours
            if reason == 'STOP_LOSS':
                self._coin_cooldown[coin] = time.time()
                logger.info(f"COOLDOWN: {coin} on 2h cooldown after SL")

            self._closing_lock.discard(coin)
            return True
        except Exception as e:
            logger.error(f"Close error {coin}: {e}")
            self._closing_lock.discard(coin)
            return False

    def _reopen_after_profit(self, closed_coin):
        """DISABLED — all new positions go through hourly Opus scan only.
        Next scan will find level-based setups. No blind reopening."""
        return
        # Old code below — kept for reference but NEVER executes
        if len(self._tracked) >= MAX_POSITIONS:
            return

        try:
            preds = self._last_signals or self._get_signals()
            if not preds:
                return

            regime, _ = self._get_regime()
            ranked = sorted(preds, key=lambda x: x.get('reg_score_adj', x['reg_score']), reverse=True)

            for p in (ranked if regime == 'BULL' else reversed(ranked)):
                coin = p['coin']
                if coin in self._tracked or coin in self._traded_this_cycle:
                    continue
                if coin == closed_coin:
                    continue

                direction = 'LONG' if regime == 'BULL' else 'SHORT'
                if regime == 'FLAT':
                    idx = next((i for i, x in enumerate(ranked) if x['coin'] == coin), len(ranked)//2)
                    direction = 'LONG' if idx < len(ranked) // 4 else 'SHORT'

                logger.info(f"REOPEN: {direction} {coin} (after {closed_coin} TP)")
                success = self._open_trade(coin, direction, p['reg_score'])
                if success:
                    self._traded_this_cycle.add(coin)
                break
        except Exception as e:
            logger.debug(f"Reopen error: {e}")

    def _on_price_tick(self, coin, price, bid, ask):
        """Called on EVERY price update from WebSocket (~6/sec)."""
        if coin not in self._tracked:
            return
        if coin in self._closing_lock:
            return

        tracked = self._tracked[coin]
        exit_signal, pnl_pct = tracked.update(price)

        if exit_signal:
            self._closing_lock.add(coin)  # lock before closing
            logger.info(f"WS EXIT {coin}: {exit_signal} at ${price:.4f} (PnL={pnl_pct*100:+.3f}%)")
            self._close_trade(coin, exit_signal, pnl_pct)
            self._closing_lock.discard(coin)

    def _check_news(self):
        """Check for breaking news that affects our positions."""
        try:
            signal = self._news_reactor.check_for_signals()
            if not signal:
                return

            news_dir = signal.get('direction', '')
            impact = signal.get('impact', 0)
            title = signal.get('title', '')[:120]

            if impact < 7 or news_dir not in ('BULLISH', 'BEARISH'):
                return

            # Cancel pending orders that contradict the news (always, any impact 7+)
            cancelled_pending = []
            if self._pending_orders:
                cancel_dir = 'SHORT' if news_dir == 'BULLISH' else 'LONG'
                cancelled_pending = [c for c, po in self._pending_orders.items() if po.direction == cancel_dir]
                if cancelled_pending:
                    self._cancel_all_pending(direction_filter=cancel_dir)
                    logger.warning(f"NEWS: cancelled {len(cancelled_pending)} {cancel_dir} pending orders")

            close_coins = self._news_reactor.should_close_positions(
                self._tracked, price_getter=self._price_stream.get_price)

            if not close_coins:
                return

            # IMPACT 9-10: AUTO-CLOSE (emergency — hack, ban, crash)
            if impact >= 9:
                logger.warning(f"EMERGENCY NEWS ({impact}/10): auto-closing {close_coins} — {title}")
                for coin in close_coins:
                    if coin in self._tracked:
                        price = self._price_stream.get_price(coin)
                        if price > 0:
                            _, pnl = self._tracked[coin].update(price)
                            self._close_trade(coin, 'NEWS_EMERGENCY', pnl)

                self._notify(
                    f"🚨 NEWS EMERGENCY: {news_dir}",
                    f"Impact: {impact}/10\nAuto-closed: {close_coins}\n{title}")
                return

            # IMPACT 7-8: ASK PROFI (important but not catastrophic)
            logger.info(f"NEWS ({impact}/10 {news_dir}): asking Profi about {close_coins} — {title}")

            # Build position context for Profi
            pos_lines = []
            for coin in close_coins:
                if coin in self._tracked:
                    t = self._tracked[coin]
                    price = self._price_stream.get_price(coin)
                    if price > 0:
                        if t.direction == 'SHORT':
                            pnl_pct = (t.entry_price - price) / t.entry_price * 100
                        else:
                            pnl_pct = (price - t.entry_price) / t.entry_price * 100
                        roi = pnl_pct * t.leverage
                        held_min = (time.time() - t.entry_time) / 60
                        pos_lines.append(f"  {t.direction} {coin} {t.leverage}x: ROI {roi:+.1f}%, held {held_min:.0f}min")

            profi_decision = self._profi._call_simple([{
                "role": "user",
                "content": f"""BREAKING NEWS: {title}
Direction: {news_dir} | Impact: {impact}/10

Your open positions that may be affected:
{chr(10).join(pos_lines)}

Is this news SYSTEMIC (affects whole market) or ISOLATED (one exchange/coin)?
For each position decide: CLOSE or HOLD.

Reply JSON: {{"analysis": "brief reason", "decisions": {{"COIN": "CLOSE" or "HOLD", ...}}}}"""
            }], max_tokens=500)

            logger.info(f"PROFI NEWS DECISION: {profi_decision[:200]}")

            # Parse Profi's decision
            close_list = []
            hold_list = []
            try:
                import json as _json
                start = profi_decision.find('{')
                end = profi_decision.rfind('}') + 1
                if start >= 0 and end > start:
                    decision = _json.loads(profi_decision[start:end])
                    decisions = decision.get('decisions', {})
                    for coin, action in decisions.items():
                        if action.upper() == 'CLOSE' and coin in self._tracked:
                            close_list.append(coin)
                        else:
                            hold_list.append(coin)
            except Exception:
                # If parsing fails, be safe: close all (fallback to old behavior)
                logger.warning("Failed to parse Profi news decision — fallback: close all")
                close_list = close_coins

            # Execute Profi's decisions
            if close_list:
                for coin in close_list:
                    if coin in self._tracked:
                        price = self._price_stream.get_price(coin)
                        if price > 0:
                            _, pnl = self._tracked[coin].update(price)
                            self._close_trade(coin, 'NEWS_PROFI_CLOSE', pnl)

            self._notify(
                f"NEWS: {news_dir} ({impact}/10)",
                f"Profi decided:\n"
                f"  CLOSE: {close_list or 'none'}\n"
                f"  HOLD: {hold_list or 'none'}\n"
                f"Cancelled pending: {cancelled_pending or 'none'}\n"
                f"{title}")

            # Use sentiment to adjust regime confidence
            sentiment = self._news_reactor.get_market_sentiment()
            if sentiment != 'NEUTRAL':
                logger.info(f"News sentiment: {sentiment}")

        except Exception as e:
            logger.debug(f"News check: {e}")

    def _check_btc_rapid_move(self):
        """If BTC moved >1.5% in last 30min against our positions → close all.
        This catches sudden market moves between 4h candles."""
        btc_price = self._price_stream.get_price('BTC')
        if btc_price <= 0:
            return

        if not hasattr(self, '_btc_price_history'):
            self._btc_price_history = []

        import time as _t
        now = _t.time()
        self._btc_price_history.append((now, btc_price))

        # Keep last 30 min
        self._btc_price_history = [(t, p) for t, p in self._btc_price_history if now - t < 1800]

        if len(self._btc_price_history) < 2:
            return

        oldest_price = self._btc_price_history[0][1]
        btc_change = (btc_price / oldest_price - 1) * 100

        # If BTC moved >1.5% and we have positions against it
        if abs(btc_change) > 3.0 and len(self._tracked) > 0:
            shorts = sum(1 for t in self._tracked.values() if t.direction == 'SHORT')
            longs = sum(1 for t in self._tracked.values() if t.direction == 'LONG')

            danger = False
            if btc_change > 1.5 and shorts > longs:
                danger = True  # BTC pumping, we're mostly SHORT
            elif btc_change < -1.5 and longs > shorts:
                danger = True  # BTC dumping, we're mostly LONG

            if danger:
                logger.warning(f"BTC RAPID MOVE: {btc_change:+.1f}% in 30min! Closing vulnerable positions")
                # Cancel contradicting pending orders
                cancel_dir = 'SHORT' if btc_change > 0 else 'LONG'
                self._cancel_all_pending(direction_filter=cancel_dir)

                for coin in list(self._tracked.keys()):
                    tr = self._tracked[coin]
                    if (btc_change > 0 and tr.direction == 'SHORT') or \
                       (btc_change < 0 and tr.direction == 'LONG'):
                        price = self._price_stream.get_price(coin)
                        if price > 0:
                            _, pnl = tr.update(price)
                            self._close_trade(coin, 'BTC_RAPID_MOVE', pnl)

    def _check_circuit_breaker(self):
        """DISABLED — CB was killing profitable trades.
        Data: CB caused -$1,178 loss while model earned +$1,142.
        Protection is handled by per-position emergency stop (-4%) instead."""
        pass

    def _profi_review_positions(self):
        """Profi reviews open positions — closes if setup invalidated.
        Runs every 30 min. WS handles hard stops (instant).
        Profi handles strategic exits (setup changed, pattern failed).
        """
        if not self._tracked:
            return

        for coin in list(self._tracked.keys()):
            tracked = self._tracked.get(coin)
            if not tracked:
                continue

            try:
                # Current P&L
                price = self._price_stream.get_price(coin)
                if price <= 0:
                    continue
                if tracked.direction == 'SHORT':
                    pnl_pct = (tracked.entry_price - price) / tracked.entry_price
                else:
                    pnl_pct = (price - tracked.entry_price) / tracked.entry_price

                held_min = (time.time() - tracked.entry_time) / 60

                # Generate LIVE chart with our position marked
                charts = {}
                from src.crypto.chart_generator import generate_live_chart
                pos_info = {'direction': tracked.direction, 'entry_price': tracked.entry_price}
                b = generate_live_chart(coin, '4h', 60, exchange=self.exchange, position=pos_info)
                if b:
                    charts['4h'] = base64.b64encode(b).decode()

                # Ask Profi: is YOUR ORIGINAL REASON still valid?
                review_content = []
                if charts.get('4h'):
                    review_content.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/png",
                                   "data": charts['4h']}
                    })
                review_content.append({
                    "type": "text",
                    "text": f"""POSITION REVIEW — {tracked.direction} {coin} at ${tracked.entry_price:.4f}, {held_min:.0f} minutes ago.
Original reason: "{tracked.reason}"
Current PnL: {pnl_pct*100:+.2f}%

Your SPECIFIC thesis was: "{tracked.reason}"

Has this SPECIFIC thesis BROKEN? Check:
- If you said "price below MA20" → is price NOW above MA20? If still below → HOLD
- If you said "dead cat bounce failure" → did price reclaim the bounce level? If still failed → HOLD
- If you said "breakdown below support" → did price reclaim that support? If still below → HOLD

You MUST hold unless the SPECIFIC pattern you identified has OBJECTIVELY broken.
Being "unsure" is NOT a reason to close. Price fluctuation is NOT a reason to close.

Reply ONLY one word: "HOLD" or "CLOSE" and the SPECIFIC thing that broke."""
                })

                result = self._profi._call_with_tools(
                    [{"role": "user", "content": review_content}],
                    model="claude-sonnet-4-6",
                    max_tokens=200,
                    use_thinking=False  # fast, no deep thinking needed
                )

                response = result.strip().upper() if result else "HOLD"
                should_close = response.startswith("CLOSE")

                if should_close:
                    logger.info(f"PROFI REVIEW: {coin} {tracked.direction} → CLOSE | "
                               f"PnL: {pnl_pct*100:+.2f}% | {result.strip()[:80]}")
                    self._close_trade(coin, 'PROFI_EXIT', pnl_pct)
                else:
                    logger.info(f"PROFI REVIEW: {coin} {tracked.direction} → HOLD | "
                               f"PnL: {pnl_pct*100:+.2f}% | {held_min:.0f}min")

            except Exception as e:
                logger.debug(f"Profi review {coin}: {e}")

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

                # Simulation used 2h max hold. Live = 3h timeout (extra buffer).
                if not exit_signal:
                    held_hours = (time.time() - tracked.entry_time) / 3600
                    if held_hours >= 3.0:
                        exit_signal = 'TIME_EXIT'
                        logger.info(f"TIME_EXIT: {coin} held {held_hours:.1f}h (max 3h)")

                if exit_signal:
                    if coin not in self._closing_lock:
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
                    # Only adopt on FIRST start, not after failed closes
                    if self._trade_count == 0:
                        direction = 'LONG' if p.side == 'long' else 'SHORT'
                        self._tracked[coin] = TrackedPosition(
                            coin=coin, direction=direction, entry_price=p.entry_price,
                            size=p.size, leverage=p.leverage, entry_time=time.time()
                        )
                        logger.info(f"Sync: tracking existing {direction} {coin}")
                    else:
                        direction = 'LONG' if p.side == 'long' else 'SHORT'
                        logger.warning(f"Sync: GHOST position {direction} {coin} on exchange "
                                      f"(not in our tracking) — closing it")
                        close_side = 'sell' if p.side == 'long' else 'buy'
                        try:
                            self.exchange.close_position(coin, close_side, p.size)
                        except Exception:
                            pass
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
            self._traded_this_cycle.clear()
            logger.info(f"New 4h candle: {now.strftime('%H:%M')} UTC")

            # Refresh Profi knowledge every 4h (coin profiles, ATR, S/R levels)
            try:
                import threading
                def _refresh_knowledge():
                    from src.crypto.knowledge_compiler import compile_knowledge
                    from src.crypto.knowledge_advanced import compile as compile_adv
                    compile_knowledge()
                    compile_adv()
                    self._profi._knowledge = self._profi._load_knowledge()
                    logger.info(f"Knowledge refreshed: {len(self._profi._knowledge):,} chars")
                threading.Thread(target=_refresh_knowledge, daemon=True).start()
            except Exception as e:
                logger.debug(f"Knowledge refresh: {e}")

            return True
        return False

    def _open_new_positions(self):
        """FORTIX v4: Hourly level-based scan → limit orders at S/R levels."""
        # Rule 48: H16 UTC (US open) = systematic trap, 53% SL rate
        current_hour_utc = datetime.now(timezone.utc).hour
        if current_hour_utc == 16:
            logger.info("H16 UTC skip (Rule 48: US open trap)")
            return

        self._sync_with_exchange()

        # Daily loss limit
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if today != self._current_day:
            self._current_day = today
            self._daily_pnl_pct = 0.0
            self._post_cb_size_factor = 1.0
            if datetime.now(timezone.utc).weekday() == 0:
                self._banned_coins = {c for c, pnl in self._weekly_coin_pnl.items() if pnl < -0.005}
                if self._banned_coins:
                    logger.info(f"Weekly ban: {self._banned_coins}")
                self._weekly_coin_pnl.clear()

        if self._daily_pnl_pct < DAILY_LOSS_LIMIT:
            logger.info(f"Daily limit hit: {self._daily_pnl_pct*100:.1f}%")
            return

        # Profi daily strategy (cached 4h)
        try:
            strategy = self._profi.get_daily_strategy()
            self._profi_daily_strategy = strategy
            if strategy:
                pref = strategy.get('preferred_direction', 'BOTH')
                logger.info(f"PROFI daily: {strategy.get('regime')} | direction={pref}")
        except Exception as e:
            logger.debug(f"Profi daily strategy: {e}")

        # New scan = fresh levels. Cancel ALL unfilled pending orders.
        # Levels shift every hour — old orders on stale levels are dangerous.
        if self._pending_orders:
            count = len(self._pending_orders)
            self._cancel_all_pending()
            logger.info(f"Pre-scan: cancelled {count} stale pending orders → fresh levels")

        # ========================================================
        # FORTIX v4: Level-Based Predictive Trading
        # 1. Calculate S/R levels for available coins
        # 2. Opus identifies setups at levels (limit orders)
        # 3. Place limit orders → wait for price to come to us
        # 4. When filled → SL/TP placed ON EXCHANGE
        # ========================================================

        excluded = set(self._tracked.keys()) | set(self._banned_coins) | BAD_COINS
        available_coins = [c for c in COINS if c not in excluded]

        # NO hot coins — our 51 rules were trained ONLY on established coins.
        # Hot coins (COAI, RAVE, BLESS, MYX) lost $101 with zero training data.
        all_scan_coins = available_coins

        # Apply cooldown filter
        all_scan_coins = [c for c in all_scan_coins
                         if time.time() - self._coin_cooldown.get(c, 0) >= 7200]

        if not all_scan_coins:
            logger.info("No coins available for scan")
            return

        regime, btc_mom = self._get_regime()

        # Build position context
        pos_info = ""
        if self._tracked:
            parts = []
            for c, t in self._tracked.items():
                price = self._price_stream.get_price(c)
                if price > 0:
                    if t.direction == 'SHORT':
                        pnl = (t.entry_price - price) / t.entry_price * 100
                    else:
                        pnl = (price - t.entry_price) / t.entry_price * 100
                    parts.append(f"{t.direction} {c} ({pnl:+.1f}%)")
            pos_info = ", ".join(parts)

        # Step 1: Calculate S/R levels
        # Our coins → from DB (fast), hot coins → from exchange (live)
        from src.crypto.level_finder import find_levels, find_levels_live
        levels_data = {}
        our_coin_set = set(COINS)

        for coin in all_scan_coins[:30]:  # scan up to 30 coins for levels
            try:
                if coin in our_coin_set:
                    levels = find_levels(coin, timeframe='1h', lookback=100)
                else:
                    levels = find_levels_live(coin, self.exchange, timeframe='1h')
                if levels['resistance'] or levels['support']:
                    levels_data[coin] = levels
            except Exception as e:
                logger.debug(f"Levels {coin}: {e}")

        coins_with_levels = list(levels_data.keys())
        if not coins_with_levels:
            logger.info("No S/R levels found for any coin")
            return

        logger.info(f"v4 SCAN: {len(coins_with_levels)} coins with S/R levels "
                    f"({len([c for c in coins_with_levels if c not in our_coin_set])} hot) | regime={regime}")

        # Get learning feedback from journal
        feedback = self._journal.build_scan_feedback()

        # Step 2: Opus analyzes ALL coins — dynamic batches of 8
        setups = []
        batch_size = 8

        for i in range(0, len(coins_with_levels), batch_size):
            batch = coins_with_levels[i:i + batch_size]
            if not batch:
                break
            try:
                batch_setups = self._profi.find_level_setups(
                    coins=batch, levels_data=levels_data,
                    regime=regime, open_positions=pos_info,
                    sl_history=self._coin_cooldown,
                    trade_feedback=feedback
                )
                if batch_setups:
                    setups.extend(batch_setups)
            except Exception as e:
                logger.warning(f"Opus batch {i//batch_size + 1} error: {e}")

        if not setups:
            logger.info("Opus: no setups at current levels")
            return

        setups.sort(key=lambda s: float(s.get('confidence', 0)), reverse=True)

        logger.info(f"Opus: {len(setups)} level setups → placing limit orders")

        # BTC momentum → decides AGGRESSIVE vs PATIENT entry
        btc_aggressive_long = False
        btc_aggressive_short = False
        try:
            btc_ohlcv = self.exchange._exchange.fetch_ohlcv('BTC/USDT:USDT', '15m', limit=3)
            if btc_ohlcv and len(btc_ohlcv) >= 2:
                btc_15m = (btc_ohlcv[-1][4] - btc_ohlcv[-2][4]) / btc_ohlcv[-2][4] * 100
                if btc_15m > 0.15:
                    btc_aggressive_long = True
                    logger.info(f"BTC +{btc_15m:.2f}% (15m) → AGGRESSIVE LONG (81% accuracy)")
                elif btc_15m < -0.15:
                    btc_aggressive_short = True
                    logger.info(f"BTC {btc_15m:.2f}% (15m) → AGGRESSIVE SHORT (81% accuracy)")
                else:
                    logger.info(f"BTC {btc_15m:+.2f}% (15m) → PATIENT entries (flat)")
        except Exception:
            pass

        # Step 3: Place limit orders at levels
        placed = 0
        for setup in setups:
            if placed >= MAX_PENDING:
                break

            coin = setup.get('coin', '')
            direction = setup.get('direction', '')
            entry = float(setup.get('entry', 0))
            sl = float(setup.get('sl', 0))
            tp = float(setup.get('tp', 0))
            conf = float(setup.get('confidence', 0))
            lev = max(3, min(int(setup.get('leverage', MIN_LEVERAGE)), 15))
            reason = setup.get('reason', '')

            if direction not in ('LONG', 'SHORT') or entry <= 0 or sl <= 0 or tp <= 0:
                logger.debug(f"Invalid setup: {setup}")
                continue

            if coin in self._tracked or coin in self._pending_orders:
                continue

            # No direction filter — Opus already considers regime in its analysis.
            # Limit orders at levels are safe even against the trend.

            # No sector filter for limit orders — they're nets, most won't fill
            # Diversification enforced at fill time by MAX_POSITIONS check

            # Set leverage
            try:
                info = self.exchange.get_contract_info(coin)
                max_lev = int(info.get('maxLeverage', 100))
                lev = min(lev, max_lev)
                self.exchange.set_leverage(coin, lev, direction)
            except Exception as e:
                logger.warning(f"Leverage {coin}: {e}")
                continue

            # Calculate position size — scaled by Profi's per-setup confidence
            self._last_confidence = conf
            amount = self._calculate_position_size(coin, entry, lev)
            if amount <= 0:
                logger.info(f"{coin}: no budget for limit order")
                continue

            # PROFI CONTROLS SL/TP — він бачить walls, S/R, ATR, per-coin optimal params.
            # Fallback: якщо Profi values невалідні → ATR × 0.8, R:R 2.0
            import numpy as np
            try:
                atr_rows = sqlite3.connect(str(DB_PATH)).execute(
                    'SELECT high,low,close FROM prices WHERE coin=? AND timeframe="1h" '
                    'ORDER BY timestamp DESC LIMIT 14', (coin,)).fetchall()
                coin_atr = np.mean([(r[0]-r[1])/r[2] for r in atr_rows if r[2] > 0]) if len(atr_rows) >= 5 else 0.01
            except Exception:
                coin_atr = 0.01

            # Validate Profi's SL/TP
            profi_sl_valid = sl > 0 and (
                (direction == 'LONG' and sl < entry) or
                (direction == 'SHORT' and sl > entry))
            profi_tp_valid = tp > 0 and (
                (direction == 'LONG' and tp > entry) or
                (direction == 'SHORT' and tp < entry))

            if not profi_sl_valid or not profi_tp_valid:
                sl_dist = coin_atr * 0.8
                tp_dist = sl_dist * 2.0
                if direction == 'LONG':
                    sl = round(entry * (1 - sl_dist), 6)
                    tp = round(entry * (1 + tp_dist), 6)
                else:
                    sl = round(entry * (1 + sl_dist), 6)
                    tp = round(entry * (1 - tp_dist), 6)
                logger.info(f"{coin}: Profi SL/TP invalid, using ATR fallback")

            # Smart entry: BTC momentum decides AGGRESSIVE vs PATIENT
            # AGGRESSIVE = live price (fills immediately), PATIENT = Opus entry (waits)
            side = 'buy' if direction == 'LONG' else 'sell'
            original_entry = entry
            entry_mode = 'PATIENT'

            # AGGRESSIVE if: BTC confirms OR strong OB confirms direction
            go_aggressive = False
            if (direction == 'LONG' and btc_aggressive_long) or \
               (direction == 'SHORT' and btc_aggressive_short):
                go_aggressive = True  # BTC confirms

            # Also check per-coin OB — if order book strongly confirms, go aggressive
            if not go_aggressive and conf >= 0.55:
                try:
                    ob = self.exchange._exchange.fetch_order_book(
                        self.exchange._symbol(coin), limit=10)
                    if ob:
                        bid_vol = sum(b[1] * b[0] for b in ob['bids'][:10])
                        ask_vol = sum(a[1] * a[0] for a in ob['asks'][:10])
                        total = bid_vol + ask_vol
                        if total > 0:
                            ob_imb = (bid_vol - ask_vol) / total
                            if direction == 'LONG' and ob_imb > 0.3:
                                go_aggressive = True  # strong buy pressure
                            elif direction == 'SHORT' and ob_imb < -0.3:
                                go_aggressive = True  # strong sell pressure
                except Exception:
                    pass

            if go_aggressive:
                try:
                    ticker = self.exchange.get_ticker(coin)
                    if ticker and ticker.get('price', 0) > 0:
                        live = ticker['price']
                        if side == 'buy':
                            entry = ticker.get('ask', live)
                        else:
                            entry = ticker.get('bid', live)
                        entry_mode = 'AGGRESSIVE'
                except Exception:
                    pass

            # If AGGRESSIVE entry changed, shift TP/SL by same offset
            if entry_mode == 'AGGRESSIVE' and original_entry > 0:
                shift = entry - original_entry
                tp = round(tp + shift, 6)
                sl = round(sl + shift, 6)

            # Log with actual ROI
            if direction == 'LONG':
                actual_tp_roi = (tp - entry) / entry * lev * 100 if entry > 0 else 0
                actual_sl_roi = (entry - sl) / entry * lev * 100 if entry > 0 else 0
            else:
                actual_tp_roi = (entry - tp) / entry * lev * 100 if entry > 0 else 0
                actual_sl_roi = (sl - entry) / entry * lev * 100 if entry > 0 else 0

            logger.info(f"  {entry_mode}: {direction} {coin} @${entry:.4f} "
                       f"TP={actual_tp_roi:.0f}%ROI SL={actual_sl_roi:.0f}%ROI {lev}x (conf={conf:.0%})")

            # Place limit order with TP/SL attached
            order_id = self.exchange.place_level_order(
                coin, side, amount, entry,
                sl_price=sl, tp_price=tp)

            if not order_id:
                logger.warning(f"Failed to place level order: {coin} {side} @ ${entry:.4f}")
                continue

            # Record in trade journal (learning system)
            journal_reason = f"[{entry_mode}] {reason[:180]}"
            trade_id = self._journal.record_order_placed(
                coin, direction, entry, sl, tp, lev, conf, journal_reason, regime, amount)

            # Track order (for cancel on next scan)
            po = PendingOrder(
                coin=coin, direction=direction,
                entry_price=entry, sl_price=sl, tp_price=tp,
                size=amount, leverage=lev,
                order_id=order_id, reason=reason[:200]
            )
            po.trade_id = trade_id
            self._pending_orders[coin] = po
            placed += 1

            logger.info(f"PENDING: {direction} {coin} limit@${entry:.4f} "
                       f"SL=${sl:.4f} TP=${tp:.4f} {lev}x | {reason[:60]}")

        if placed > 0:
            self._notify(
                f"v4: {placed} limit orders placed",
                "\n".join(f"{p.direction} {p.coin} limit@${p.entry_price:.4f} "
                         f"SL=${p.sl_price:.4f} TP=${p.tp_price:.4f} {p.leverage}x"
                         for p in self._pending_orders.values())
            )
        else:
            logger.info("No limit orders placed this scan")

    def _check_pending_orders(self):
        """Detect fills via Bybit order history + track closed positions via closedPnl."""

        # 1. Check for FILLS — which of our pending orders got filled?
        try:
            result = self.exchange._exchange.privateGetV5OrderHistory({
                'category': 'linear', 'orderStatus': 'Filled', 'limit': 20})
            filled_orders = result.get('result', {}).get('list', [])

            # Match filled orders to our pending
            our_order_ids = {po.order_id: coin for coin, po in self._pending_orders.items()}
            filled = []
            for fo in filled_orders:
                oid = fo.get('orderId', '')
                if oid in our_order_ids:
                    filled.append(our_order_ids[oid])
        except Exception as e:
            logger.debug(f"Fill check: {e}")
            filled = []

        # 2. Check for CLOSED positions (TP/SL hit) — record in journal
        try:
            result = self.exchange._exchange.privateGetV5PositionClosedPnl({
                'category': 'linear', 'limit': 10})
            closed_pnl = result.get('result', {}).get('list', [])

            for cp in closed_pnl:
                # Dedup — only process closes newer than our start time
                cp_ts = int(cp.get('updatedTime', 0))
                if cp_ts <= self._last_closed_pnl_ts:
                    continue
                cp_id = cp.get('orderId', '') or str(cp_ts)
                if cp_id in self._processed_closed_pnl:
                    continue
                self._processed_closed_pnl.add(cp_id)

                coin = cp.get('symbol', '').replace('USDT', '')
                pnl_usd = float(cp.get('closedPnl', 0))
                exit_price = float(cp.get('avgExitPrice', 0))
                entry_price = float(cp.get('avgEntryPrice', 0))
                lev = int(cp.get('leverage', 7))
                side = cp.get('side', '')
                direction = 'SHORT' if side == 'Buy' else 'LONG'

                pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
                if direction == 'SHORT':
                    pnl_pct = -pnl_pct
                exit_reason = 'TARGET_HIT' if pnl_usd > 0 else 'STOP_LOSS'
                roi = pnl_pct * lev * 100

                # Record in journal if tracked
                if coin in self._tracked:
                    tracked = self._tracked[coin]
                    held_min = (time.time() - tracked.entry_time) / 60
                    if hasattr(tracked, 'trade_id') and tracked.trade_id:
                        self._journal.record_close(
                            tracked.trade_id, exit_price, pnl_pct, pnl_usd, exit_reason, held_min)
                    self._tracked.pop(coin, None)
                else:
                    held_min = 0

                self._total_pnl += pnl_usd
                self._trade_count += 1
                logger.info(f"CLOSED: {direction} {coin} {lev}x "
                           f"ROI={roi:+.1f}% ${pnl_usd:+.2f} [{exit_reason}]")
                self._notify(
                    f"{'💰' if pnl_usd > 0 else '🔻'} {coin} {exit_reason}",
                    f"ROI: {roi:+.1f}% | ${pnl_usd:+.2f}")

                if exit_reason == 'STOP_LOSS':
                    self._coin_cooldown[coin] = time.time()
        except Exception as e:
            logger.debug(f"Closed PnL check: {e}")

        for coin in filled:
            po = self._pending_orders.pop(coin)

            # Get actual fill price
            try:
                order_info = self.exchange._exchange.fetch_order(po.order_id, self.exchange._symbol(coin))
                fill_price = float(order_info.get('average') or order_info.get('price') or po.entry_price)
                fill_amount = float(order_info.get('filled') or po.size)
            except Exception:
                fill_price = po.entry_price
                fill_amount = po.size

            # Record fill in journal (with market context snapshot)
            if po.trade_id:
                self._journal.record_fill(po.trade_id, fill_price, fill_amount)

            # SL/TP already active on Bybit (attached to limit order). Just track.
            tracked = TrackedPosition(
                coin=coin, direction=po.direction, entry_price=fill_price,
                size=fill_amount, leverage=po.leverage, entry_time=time.time(),
                sl_price=po.sl_price, tp_price=po.tp_price,
                reason=po.reason,
                target_pct=abs(po.tp_price - fill_price) / fill_price if fill_price > 0 else 0.01,
                sl_pct=abs(po.sl_price - fill_price) / fill_price if fill_price > 0 else 0.01,
                max_hold_hours=8
            )
            tracked.trade_id = po.trade_id
            self._tracked[coin] = tracked

            self._trade_count += 1
            info = self.exchange.get_contract_info(coin)
            cs = info.get('contractSize', 1)
            notional = fill_amount * cs * fill_price
            margin = notional / po.leverage
            roi_tp = tracked.target_pct * po.leverage * 100
            roi_sl = tracked.sl_pct * po.leverage * 100

            logger.info(f"FILLED #{self._trade_count}: {po.direction} {coin} "
                       f"{fill_amount}@${fill_price:.4f} {po.leverage}x "
                       f"SL=${po.sl_price:.4f} TP=${po.tp_price:.4f} "
                       f"margin=${margin:.2f} [SL/TP ALREADY ON EXCHANGE]")

            self._notify(
                f"FILLED: {po.direction} {coin} {po.leverage}x",
                f"Entry: ${fill_price:.4f}\n"
                f"SL: ${po.sl_price:.4f} (ROI -{roi_sl:.0f}%) ON EXCHANGE\n"
                f"TP: ${po.tp_price:.4f} (ROI +{roi_tp:.0f}%) ON EXCHANGE\n"
                f"Margin: ${margin:.1f}\n"
                f"Reason: {po.reason[:100]}"
            )

        # Expired orders handled by pre-scan cancel (all pending cancelled each hour)

    def _cancel_expired_pending(self):
        """Cancel all expired pending orders (called before new scan)."""
        expired = [c for c, po in self._pending_orders.items() if po.is_expired()]
        for coin in expired:
            po = self._pending_orders.pop(coin)
            self.exchange.cancel_order(po.order_id, coin)
            logger.info(f"Pre-scan cleanup: cancelled expired {coin} limit@${po.entry_price:.4f}")

    def _cancel_all_pending(self, direction_filter=None):
        """Cancel all pending orders. Retry failures. Also bulk-cancel on exchange as safety net."""
        to_cancel = []
        for coin, po in self._pending_orders.items():
            if direction_filter is None or po.direction == direction_filter:
                to_cancel.append(coin)

        failed = []
        for coin in to_cancel:
            po = self._pending_orders.get(coin)
            if not po:
                continue
            ok = self.exchange.cancel_order(po.order_id, coin)
            if not ok:
                ok = self.exchange.cancel_order(po.order_id, coin)  # retry
            if not ok:
                failed.append(coin)
                logger.warning(f"CANCEL FAILED: {coin} order {po.order_id}")

            # Record cancel in journal (missed opportunity tracking)
            if po.trade_id:
                price = self._price_stream.get_price(coin) if self._price_stream else 0
                self._journal.record_cancel(po.trade_id, price)

            self._pending_orders.pop(coin, None)
            if ok:
                logger.info(f"CANCELLED: {po.direction} {coin} limit@${po.entry_price:.4f}")

        # Safety net: cancel ALL open limit orders on exchange to prevent orphans
        if failed or (direction_filter is None and to_cancel):
            try:
                # Bybit: cancel all open orders for each coin we trade
                for coin in set(to_cancel) | set(failed):
                    try:
                        symbol = self.exchange._symbol(coin)
                        self.exchange._exchange.cancel_all_orders(symbol)
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"Bulk cancel safety net: {e}")

    def _run_weekly_synthesis(self):
        """Monday: Opus reads all daily lessons from the week → compresses into durable rules."""
        lessons_dir = _FACTORY_DIR / 'data' / 'crypto' / 'daily_lessons'
        if not lessons_dir.exists():
            return

        # Read all daily lessons from last 7 days
        daily_texts = []
        for i in range(7):
            from datetime import timedelta
            day = (datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y-%m-%d')
            path = lessons_dir / f'{day}.md'
            if path.exists():
                daily_texts.append(path.read_text())

        if not daily_texts:
            logger.info("No daily lessons found for weekly synthesis")
            return

        # Get week's trade stats
        stats = self._journal.get_stats(days=7)

        # Also get all trades for the week
        trades = self._journal.get_trades_for_analysis(hours=168)  # 7 days
        total = len(trades)
        closed = [t for t in trades if t[3] == 'CLOSED']
        cancelled = [t for t in trades if t[3] == 'CANCELLED']
        wins = sum(1 for t in closed if (t[19] or 0) > 0)
        losses = len(closed) - wins
        wr = wins / len(closed) * 100 if closed else 0

        summary = f"WEEK: {total} orders, {len(closed)} filled ({wins}W/{losses}L = {wr:.0f}% WR), {len(cancelled)} cancelled"

        all_lessons = "\n\n---\n\n".join(daily_texts)

        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": f"""You are FORTIX trading system. This is your WEEKLY review.

{summary}

STATISTICS:
{stats}

DAILY LESSONS FROM THIS WEEK:
{all_lessons[:8000]}

Write 3-5 PERMANENT RULES based on this week's data.
These rules will guide ALL future trading decisions.

Format:
## Rule 1: [clear title]
ALWAYS/NEVER: [specific action]
CONDITION: [when this applies, with numbers]
EVIDENCE: [which days/trades prove this]
EXPECTED IMPACT: [how this improves WR or profit]

Also:
- Which rules from previous weeks should be KEPT?
- Which should be DROPPED (disproven by this week)?
- What is the #1 thing to improve next week?"""}]
        )

        synthesis = response.content[0].text
        logger.info(f"Weekly synthesis: {len(synthesis)} chars")

        # Overwrite profi_lessons.md with compressed rules
        lessons_path = Path(__file__).parent / 'profi_lessons.md'
        week_str = datetime.now(timezone.utc).strftime('%Y-W%W')
        new_content = f"# FORTIX Trading Rules (auto-updated weekly)\nLast updated: {week_str}\n\n{synthesis}"

        # Keep previous weeks as archive (last 3)
        old = lessons_path.read_text() if lessons_path.exists() else ""
        archive = ""
        if "# ARCHIVE" in old:
            archive = old[old.index("# ARCHIVE"):]
        elif old:
            archive = f"# ARCHIVE\n\n## Previous\n{old[:5000]}"

        lessons_path.write_text(f"{new_content}\n\n{archive[:10000]}")
        logger.info(f"Weekly rules saved to profi_lessons.md ({len(synthesis)} chars)")

        # Save weekly file
        (lessons_dir / f'weekly_{week_str}.md').write_text(new_content)

    def _run_daily_self_analysis(self):
        """Opus analyzes its own trades from last 24h. Writes lessons."""
        trades = self._journal.get_trades_for_analysis(hours=24)
        if not trades:
            logger.info("No trades to analyze")
            return

        # Format trades for Opus
        lines = []
        for t in trades:
            trade_id, coin, direction, status, entry_type = t[:5]
            entry_price, fill_price, sl_price, tp_price, exit_price = t[5:10]
            regime, confidence, reason, leverage = t[10:14]
            ob_imb, momentum, funding, atr = t[14:18]
            pnl_pct, pnl_usd, exit_reason, held_min = t[18:22]
            price_cancel, hit_tp, hit_sl = t[22:25]

            if status == 'CLOSED':
                lines.append(f"TRADE: {direction} {coin} {leverage}x ({entry_type}): "
                            f"${pnl_usd or 0:+.2f} [{exit_reason}] {held_min or 0:.0f}min | "
                            f"OB={ob_imb or 0:+.0%} mom={momentum or 0:+.1f}% | {reason[:80]}")
            elif status == 'CANCELLED':
                entry_p = t[5] or 0
                cancel_p = price_cancel or 0
                gap = abs(cancel_p - entry_p) / entry_p * 100 if entry_p > 0 else 0
                if hit_tp == 1:
                    verdict = "MISSED PROFIT — price came to your level AND hit TP"
                elif hit_tp == 2:
                    verdict = "MISSED PROFIT — AGGRESSIVE entry would have hit TP (price didn't come to level but moved right direction)"
                elif hit_sl and hit_sl > 0:
                    verdict = "CORRECT SKIP — would have hit SL"
                else:
                    verdict = "NEUTRAL — no TP or SL would have hit"
                lines.append(f"MISSED: {direction} {coin} ({entry_type}) entry=${entry_p:.4f} "
                            f"cancel_price=${cancel_p:.4f} (gap {gap:.1f}%) → {verdict} | {reason[:50]}")

        trade_text = "\n".join(lines)

        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": f"""You are FORTIX trading system. Analyze YOUR trades from today.

{trade_text}

Write EXACTLY 5 actionable rules based on what worked and what didn't.
Format each rule as:
RULE: [what to do]
WHEN: [specific condition with numbers]
EVIDENCE: [which trades prove this]

Focus on:
- AGGRESSIVE vs PATIENT entries: which works better? Calculate WR for each.
- MISSED orders: how many would have been profitable with AGGRESSIVE entry? This is CRITICAL.
- Which coins make money, which lose? Should any be avoided?
- What OB/momentum conditions predict wins?
- What is the #1 mistake you keep making? How to fix it?
- What is the #1 thing that works? How to do more of it?

Be specific. Use numbers. Every rule must have evidence from today's trades.
Goal: reach 85%+ WR. What needs to change to get there?"""}]
        )

        lessons = response.content[0].text
        logger.info(f"Daily self-analysis: {len(lessons)} chars")

        # Save to daily file
        lessons_dir = _FACTORY_DIR / 'data' / 'crypto' / 'daily_lessons'
        lessons_dir.mkdir(exist_ok=True)
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        (lessons_dir / f'{today}.md').write_text(f"# FORTIX Daily Lessons — {today}\n\n{lessons}")

        # Append to profi_lessons.md (knowledge base)
        lessons_path = Path(__file__).parent / 'profi_lessons.md'
        current = lessons_path.read_text() if lessons_path.exists() else ""
        # Keep last 20KB + new lessons
        if len(current) > 20000:
            current = current[:20000]
        lessons_path.write_text(f"## {today} — Daily Self-Analysis\n{lessons}\n\n{current}")
        logger.info(f"Lessons saved to {lessons_dir / f'{today}.md'} and profi_lessons.md")

    def _ensure_position_tpsl(self):
        """Ensure every open position has TP/SL. Runs at startup + every 5 min."""
        try:
            for p in self.exchange.get_positions():
                coin = p.symbol.split('/')[0]
                symbol = f'{coin}USDT'
                result = self.exchange._exchange.privateGetV5PositionList({
                    'category': 'linear', 'symbol': symbol})
                pos_list = result.get('result', {}).get('list', [])
                for pos in pos_list:
                    if float(pos.get('size', 0)) == 0:
                        continue
                    sl = pos.get('stopLoss', '')
                    tp = pos.get('takeProfit', '')
                    if sl and tp:
                        continue  # already protected

                    # No TP/SL — set dynamic based on ROI targets + leverage
                    t = self.exchange.get_ticker(coin)
                    live = t['price'] if t else p.entry_price
                    lev = int(p.leverage or 7)
                    tp_pct = 0.07 / lev   # 7% ROI target
                    sl_pct = 0.12 / lev   # 12% ROI max loss

                    if p.side == 'long':
                        new_tp = str(round(live * (1 + tp_pct), 6))
                        new_sl = str(round(live * (1 - sl_pct), 6))
                    else:
                        new_tp = str(round(live * (1 - tp_pct), 6))
                        new_sl = str(round(live * (1 + sl_pct), 6))

                    self.exchange._exchange.privatePostV5PositionTradingStop({
                        'category': 'linear', 'symbol': symbol,
                        'takeProfit': new_tp, 'stopLoss': new_sl,
                        'tpTriggerBy': 'LastPrice', 'slTriggerBy': 'LastPrice',
                        'positionIdx': 0,
                    })
                    logger.info(f"PROTECTED: {p.side} {coin} SL=${new_sl} TP=${new_tp} ({dist*100:.1f}%)")
        except Exception as e:
            logger.warning(f"Ensure TP/SL: {e}")

    def _check_api_health(self):
        """Check critical APIs. Alert TG every 60s if any is down."""
        checks = {}

        # Bybit
        try:
            bal = self.exchange.get_balance()
            if bal and bal.get('total', 0) > 0:
                checks['Bybit'] = True
            else:
                checks['Bybit'] = False
        except Exception:
            checks['Bybit'] = False

        # Claude (check if last scan worked — don't call API just to test)
        last_scan_age = time.time() - getattr(self, '_last_successful_scan', 0)
        if last_scan_age > 7200:  # no successful scan in 2h
            checks['Claude API'] = False
        else:
            checks['Claude API'] = True

        # WebSocket
        if hasattr(self, '_price_stream'):
            checks['WebSocket'] = self._price_stream.is_connected()

        now = time.time()
        for api, is_up in checks.items():
            if not is_up:
                if api not in self._api_down:
                    self._api_down[api] = now  # first detection
                # Alert every 60 seconds while down
                last_alert = self._last_api_alert.get(api, 0)
                if now - last_alert >= 60:
                    down_min = (now - self._api_down[api]) / 60
                    self._notify(
                        f"🚨 API DOWN: {api}",
                        f"{api} не відповідає вже {down_min:.0f} хвилин!\n"
                        f"Позиції захищені TP/SL на біржі.\n"
                        f"Але нові трейди НЕ відкриваються!")
                    self._last_api_alert[api] = now
                    logger.error(f"API DOWN: {api} (down {down_min:.0f}min)")
            else:
                if api in self._api_down:
                    down_min = (now - self._api_down[api]) / 60
                    self._notify(
                        f"✅ API RESTORED: {api}",
                        f"{api} працює знову (було down {down_min:.0f} хвилин)")
                    logger.info(f"API RESTORED: {api} (was down {down_min:.0f}min)")
                    del self._api_down[api]
                    self._last_api_alert.pop(api, None)

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
        logger.info(f"Bybit Demo Trader starting | Capital=${self.capital}")

        if not self.exchange.connect():
            logger.error("Bybit connection failed")
            return

        bal = self.exchange.get_balance()
        logger.info(f"Balance: ${bal['total']:.2f} | Max positions: {MAX_POSITIONS}")

        # Give Profi and Journal access to live exchange
        self._profi._exchange = self.exchange
        self._journal.exchange = self.exchange

        # CLEAN START: cancel orphan LIMIT orders only.
        # Position TP/SL (set via trading-stop) are NOT orders — don't touch them.
        try:
            old_orders = self.exchange._exchange.fetch_open_orders()
            limit_orders = [o for o in old_orders if o.get('type') == 'limit']
            if limit_orders:
                for o in limit_orders:
                    try:
                        self.exchange._exchange.cancel_order(o['id'], o['symbol'])
                    except Exception:
                        pass
                logger.info(f"CLEAN START: cancelled {len(limit_orders)} orphan limit orders "
                           f"(kept {len(old_orders) - len(limit_orders)} position SL/TP)")
            elif old_orders:
                logger.info(f"CLEAN START: {len(old_orders)} orders are position SL/TP — kept")
        except Exception as e:
            logger.warning(f"Clean start: {e}")

        # Sync existing POSITIONS from exchange
        self._sync_with_exchange()
        logger.info(f"Synced: {len(self._tracked)} existing positions")

        # Ensure ALL positions have TP/SL via trading-stop
        self._ensure_position_tpsl()

        self._price_stream.on_price_update = self._on_price_tick
        self._price_stream.start()
        time.sleep(2)
        ws_status = "LIVE (tick-by-tick)" if self._price_stream.is_connected() else "FALLBACK (REST)"
        logger.info(f"Price stream: {ws_status}")

        self._notify("Trader Started",
                    f"Balance: ${bal['total']:.2f}\n"
                    f"Max positions: {MAX_POSITIONS}\n"
                    f"Target: adaptive per coin (35% ATR)\n"
                    f"Price stream: {ws_status}")

        self._running = True
        last_signal_scan = 0
        scan_count = 0

        while self._running:
            try:
                self._write_heartbeat()
                now = time.time()

                # News check every 2 min — react to breaking news
                if now - self._last_news_check > 120:
                    self._check_news()
                    self._last_news_check = now

                # Safety checks + manage positions
                self._check_btc_rapid_move()
                self._check_circuit_breaker()
                self._manage_positions()

                # WebSocket health: if disconnected, reconnect
                if not self._price_stream.is_connected() and len(self._tracked) > 0:
                    logger.warning("WS disconnected with open positions! Using REST fallback")
                    # Fallback managed by _manage_positions (REST)

                # Collect data every 30 min — in background thread (NEVER block trading)
                if now - self._last_data_collect > 1800:
                    self._last_data_collect = now
                    import threading
                    threading.Thread(target=self._collect_data, daemon=True).start()

                # Check pending limit orders every 60s (fills → SL/TP on exchange)
                if now - getattr(self, '_last_pending_check', 0) > 60:
                    self._check_pending_orders()
                    self._last_pending_check = now

                # Ensure ALL positions have TP/SL (every 5 min)
                if now - getattr(self, '_last_tpsl_check', 0) > 300:
                    try:
                        self._ensure_position_tpsl()
                    except Exception:
                        pass
                    self._last_tpsl_check = now

                # Fill missed opportunity data (every 30 min)
                if now - getattr(self, '_last_missed_check', 0) > 1800:
                    try:
                        self._journal.fill_missed_opportunities()
                    except Exception:
                        pass
                    self._last_missed_check = now

                # API health check every 5 min — alert TG if down
                if now - getattr(self, '_last_api_health_check', 0) > 300:
                    try:
                        self._check_api_health()
                    except Exception:
                        pass
                    self._last_api_health_check = now

                # Market read every 30 min — Profi updates regime understanding (no orders)
                if now - getattr(self, '_last_market_read', 0) > 1800 and self._tracked:
                    try:
                        strategy = self._profi.get_daily_strategy()
                        if strategy:
                            self._profi_daily_strategy = strategy
                            logger.info(f"MARKET READ: {strategy.get('regime')} | "
                                       f"direction={strategy.get('preferred_direction', 'BOTH')}")
                    except Exception:
                        pass
                    self._last_market_read = now

                # Hourly scan: find S/R levels → place limit orders
                if now - last_signal_scan > 3600:  # orders every 1 hour
                    self._open_new_positions()
                    self._last_successful_scan = now  # for API health check
                    last_signal_scan = now
                    scan_count += 1

                    # Every 2 scans (2 hours): Profi reviews positions + status log
                    if scan_count % 2 == 0:
                        self._profi_review_positions()
                        eq = self._get_equity()
                        pending_info = f" | pending={len(self._pending_orders)}" if self._pending_orders else ""
                        logger.info(f"STATUS: {len(self._tracked)} positions{pending_info} | "
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

        # DON'T close positions on shutdown — sync will pick them up on restart
        # This saves fees on every restart (~$60+ per cycle)
        logger.info(f"Shutting down — keeping {len(self._tracked)} positions open (sync on restart)")
        self._tracked.clear()

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
            logging.FileHandler(str(LOG_DIR / 'trader_bybit.log')),
        ]
    )

    trader = BybitTrader(capital=args.capital)
    trader.run()


if __name__ == '__main__':
    main()
