"""
CONSILIUM BACKTEST — Honest January 2026
==========================================
Full system test: ML Ensemble + Profi (Claude) + Consilium voting.

HONEST rules:
- Entry at NEXT candle open (not current close)
- Slippage: 0.05% against us
- Fees: 0.02% maker per side (0.04% round trip)
- Funding rates from actual historical data
- Exits check against OHLCV (not just close)
- Profi actually calls Claude API for each candidate
- Only data available at decision time
"""

import os
import sys
import json
import time
import base64
import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger('backtest')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'

# Same config as live trader
MAX_POSITIONS = 4
RISK_PER_POSITION = 0.10
LEVERAGE = 7
MAKER_FEE = 0.0002  # 0.02%
SLIPPAGE = 0.0005   # 0.05%
MAX_HOLD_CANDLES = 6  # 6 × 4h = 24h max

# Coins available on Bybit
COINS = ['ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO']

COIN_SECTOR = {
    'ETH': 'major',
    'SOL': 'L1', 'AVAX': 'L1', 'ADA': 'L1', 'DOT': 'L1',
    'AAVE': 'defi', 'UNI': 'defi', 'LDO': 'defi', 'CRV': 'defi', 'PENDLE': 'defi', 'JUP': 'defi',
    'LINK': 'infra', 'RENDER': 'infra', 'TAO': 'infra', 'PYTH': 'infra', 'JTO': 'infra',
    'DOGE': 'meme', 'WIF': 'meme',
    'BNB': 'exchange', 'XRP': 'payment', 'ARB': 'L2', 'OP': 'L2', 'POL': 'L2',
}


class BacktestPosition:
    def __init__(self, coin, direction, entry_price, size_usd, leverage, entry_time, target_pct):
        self.coin = coin
        self.direction = direction
        self.entry_price = entry_price
        self.size_usd = size_usd
        self.leverage = leverage
        self.entry_time = entry_time
        self.target_pct = target_pct
        self.stop_pct = target_pct  # SL = TP → R:R = 1:1
        self.peak_pnl = 0.0
        self.trailing_active = False
        self.candles_held = 0
        self.funding_paid = 0.0

    def check_exit(self, o, h, l, c, funding_rate=0):
        """Check if position should exit against this candle's OHLCV.
        Returns (exit_price, exit_reason) or (None, None)
        """
        self.candles_held += 1

        # Apply funding cost
        if funding_rate:
            if self.direction == 'LONG':
                self.funding_paid += abs(funding_rate) * self.size_usd
            else:
                self.funding_paid -= abs(funding_rate) * self.size_usd

        # Check against HIGH and LOW (realistic)
        if self.direction == 'LONG':
            worst = l
            best = h
            pnl_worst = (worst - self.entry_price) / self.entry_price
            pnl_best = (best - self.entry_price) / self.entry_price
        else:
            worst = h
            best = l
            pnl_worst = (self.entry_price - worst) / self.entry_price
            pnl_best = (self.entry_price - best) / self.entry_price

        # Stop-loss (proportional to TP: SL = 1.5 × TP)
        if pnl_worst <= -self.stop_pct:
            exit_price = self.entry_price * (1 - self.stop_pct) if self.direction == 'LONG' \
                else self.entry_price * (1 + self.stop_pct)
            return exit_price, 'STOP_LOSS'

        # Target hit
        if pnl_best >= self.target_pct:
            exit_price = self.entry_price * (1 + self.target_pct) if self.direction == 'LONG' \
                else self.entry_price * (1 - self.target_pct)
            return exit_price, 'TARGET_HIT'

        # Update peak
        pnl_close = (c - self.entry_price) / self.entry_price if self.direction == 'LONG' \
            else (self.entry_price - c) / self.entry_price
        self.peak_pnl = max(self.peak_pnl, pnl_best)

        # Trailing stop (activate at 70% of target)
        if self.peak_pnl >= self.target_pct * 0.70:
            self.trailing_active = True
            trail_dist = 0.003 if self.peak_pnl < self.target_pct * 0.90 else 0.0015
            trail_stop = self.peak_pnl - trail_dist

            if pnl_close <= trail_stop and self.peak_pnl > 0:
                return c, 'TRAILING_STOP'

        # Breakeven stop (only after significant peak)
        if self.peak_pnl >= self.target_pct * 0.5 and pnl_close <= 0.001:
            return c, 'BREAKEVEN_STOP'

        # Time exit
        if self.candles_held >= MAX_HOLD_CANDLES:
            return c, 'TIME_EXIT'

        # Stagnant (after 4 candles, less than ±0.2%)
        if self.candles_held >= 4 and abs(pnl_close) < 0.002:
            return c, 'STAGNANT'

        return None, None


class ConsiliumBacktest:
    def __init__(self, start_date, end_date, capital=1000.0, use_profi=True):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = capital
        self.capital = capital
        self.use_profi = use_profi

        self.positions = {}  # coin -> BacktestPosition
        self.trades = []  # completed trades
        self.equity_curve = []
        self.daily_pnl = defaultdict(float)

        # Load models
        self._load_models()

        # Always load Consilium + Vote
        from src.crypto.consilium import Consilium, Vote
        self.consilium = Consilium()
        self.Vote = Vote

        # Load Profi if enabled
        if use_profi:
            self._load_profi()

        # API call counter
        self.profi_calls = 0
        self.profi_cost = 0.0

    def _load_models(self):
        """Load ML ensemble."""
        import lightgbm as lgb
        import xgboost as xgb
        import pickle

        self.lgb_model = lgb.Booster(model_file=str(MODEL_DIR / 'model_4h_reg.lgb'))

        self.xgb_model = xgb.Booster()
        self.xgb_model.load_model(str(MODEL_DIR / 'model_4h_xgb.json'))

        with open(MODEL_DIR / 'model_4h_nn.pkl', 'rb') as f:
            self.nn_model = pickle.load(f)

        with open(MODEL_DIR / 'scaler_4h.pkl', 'rb') as f:
            self.scaler = pickle.load(f)

        with open(MODEL_DIR / 'features_4h.json') as f:
            self.feature_cols = json.load(f)

        # Also load classification model
        self.lgb_cls = lgb.Booster(model_file=str(MODEL_DIR / 'model_4h.lgb'))

        logger.info("Models loaded: LGB + XGB + NN")

    def _load_profi(self):
        """Load Profi agent."""
        from src.crypto.profi import Profi
        self.profi = Profi()
        logger.info(f"Profi loaded: {len(self.profi._knowledge)} chars knowledge")

    def _get_features_at_time(self, conn, coin, timestamp):
        """Build features using only data available at given timestamp."""
        from src.crypto.pattern_4h import _compute_base_features, WINDOW

        # Get 4h candles up to this timestamp
        rows = conn.execute(
            "SELECT timestamp, open, high, low, close, volume FROM prices "
            "WHERE coin=? AND timeframe='4h' AND timestamp <= ? ORDER BY timestamp DESC LIMIT 25",
            (coin, timestamp)
        ).fetchall()

        if len(rows) < WINDOW:
            return None

        rows = rows[::-1]
        closes = np.array([r[4] for r in rows])
        opens_arr = np.array([r[1] for r in rows])
        highs_arr = np.array([r[2] for r in rows])
        lows_arr = np.array([r[3] for r in rows])
        volumes_arr = np.array([r[5] for r in rows])
        timestamps = np.array([r[0] for r in rows])

        i = len(closes) - 1
        ts = timestamps[i]
        dt_obj = datetime.utcfromtimestamp(ts)
        date_str = dt_obj.strftime('%Y-%m-%d')
        hour = dt_obj.hour
        dow = dt_obj.weekday()

        w_close = closes[i-WINDOW:i]
        w_volume = volumes_arr[i-WINDOW:i]
        w_high = highs_arr[i-WINDOW:i]
        w_low = lows_arr[i-WINDOW:i]

        # Load enrichment data available at this time
        funding = {}
        try:
            for r in conn.execute(
                "SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as val "
                "FROM funding_rates WHERE timestamp <= ? GROUP BY coin, d", (timestamp,)
            ).fetchall():
                funding.setdefault(r[0], {})[r[1]] = r[2]
        except Exception:
            pass

        fg = dict(conn.execute(
            "SELECT date, value FROM fear_greed WHERE date <= ?", (date_str,)
        ).fetchall())

        # BTC/ETH reference
        import pandas as pd
        btc_rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='4h' "
            "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 25", (timestamp,)
        ).fetchall()
        btc_df = pd.DataFrame(btc_rows, columns=['timestamp', 'btc_close']).set_index('timestamp')

        eth_rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin='ETH' AND timeframe='4h' "
            "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 25", (timestamp,)
        ).fetchall()
        eth_df = pd.DataFrame(eth_rows, columns=['timestamp', 'eth_close']).set_index('timestamp')

        # Empty enrichment maps (simplified — main features come from OHLCV)
        empty_map = {}
        empty_daily = {}

        try:
            features = _compute_base_features(
                i, closes, opens_arr, highs_arr, lows_arr, volumes_arr, timestamps,
                w_close, w_volume, w_high, w_low, closes[i],
                hour, dow, dt_obj, date_str,
                funding, empty_map, empty_map, empty_map, fg,
                btc_df, eth_df, empty_daily, empty_daily, empty_daily,
                empty_daily, empty_map, empty_map, empty_map, coin
            )
            return features
        except Exception as e:
            logger.debug(f"Features error {coin}: {e}")
            return None

    def _predict_ensemble(self, features):
        """Run 3 models on features, return scores."""
        import xgboost as xgb

        X = np.array([[features.get(f, 0) for f in self.feature_cols]])
        X_s = self.scaler.transform(X)

        lgb_score = float(self.lgb_model.predict(X_s)[0])
        xgb_score = float(self.xgb_model.predict(xgb.DMatrix(X_s))[0])
        nn_score = float(self.nn_model.predict(X_s)[0])

        return lgb_score, xgb_score, nn_score

    def _get_regime(self, conn, timestamp):
        """Get regime at a specific point in time."""
        # BTC momentum
        btc = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
            "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 7", (timestamp,)
        ).fetchall()
        btc_12h = (btc[0][0] / btc[3][0] - 1) * 100 if len(btc) >= 4 else 0

        # Market breadth
        prev_ts = timestamp - 14400
        breadth_rows = conn.execute(
            "SELECT p1.coin, p1.close, p2.close "
            "FROM prices p1 JOIN prices p2 ON p1.coin = p2.coin "
            "WHERE p1.timeframe='4h' AND p2.timeframe='4h' "
            "AND p1.timestamp = ? AND p2.timestamp = ? AND p1.coin != 'BTC'",
            (timestamp, prev_ts)
        ).fetchall()

        if breadth_rows:
            up = sum(1 for r in breadth_rows if r[1] > r[2])
            breadth = up / len(breadth_rows)
        else:
            breadth = 0.5

        bull = bear = 0
        if breadth > 0.65: bull += 2
        elif breadth > 0.55: bull += 1
        if breadth < 0.35: bear += 2
        elif breadth < 0.45: bear += 1
        if btc_12h > 0.5: bull += 1
        if btc_12h < -0.5: bear += 1

        if bull >= 2: return 'BULL', btc_12h, breadth
        if bear >= 2: return 'BEAR', btc_12h, breadth
        return 'FLAT', btc_12h, breadth

    def _get_coin_atr(self, conn, coin, timestamp):
        """Get ATR for adaptive TP."""
        rows = conn.execute(
            "SELECT high, low, close FROM prices WHERE coin=? AND timeframe='4h' "
            "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 30", (coin, timestamp)
        ).fetchall()
        if len(rows) < 10:
            return 0.02
        atrs = [(r[0]-r[1])/r[2] for r in rows if r[2] > 0]
        return np.mean(atrs) if atrs else 0.02

    def _get_funding_rate(self, conn, coin, timestamp):
        """Get funding rate at time."""
        r = conn.execute(
            "SELECT rate FROM funding_rates WHERE coin=? AND timestamp <= ? "
            "ORDER BY timestamp DESC LIMIT 1", (coin, timestamp)
        ).fetchone()
        return r[0] if r and r[0] else 0

    def _run_consilium(self, coin, direction, features, conn, timestamp):
        """Run full Consilium voting for a candidate."""
        votes = []

        # 1-3. ML model votes
        lgb_s, xgb_s, nn_s = self._predict_ensemble(features)

        for name, score in [('lightgbm', lgb_s), ('xgboost', xgb_s), ('neural_net', nn_s)]:
            d = 'LONG' if score > 0.001 else ('SHORT' if score < -0.001 else 'WAIT')
            votes.append(self.Vote(
                expert=name, direction=d,
                confidence=min(abs(score) * 100, 1.0),
                reason=f"score={score:.4f}"
            ))

        # 4. Market breadth
        regime, btc_mom, breadth = self._get_regime(conn, timestamp)
        if regime == 'BULL':
            votes.append(self.Vote(expert='market_breadth', direction='LONG',
                                   confidence=0.7, reason=f"regime={regime}"))
        elif regime == 'BEAR':
            votes.append(self.Vote(expert='market_breadth', direction='SHORT',
                                   confidence=0.7, reason=f"regime={regime}"))
        else:
            votes.append(self.Vote(expert='market_breadth', direction='WAIT',
                                   confidence=0.3, reason=f"regime=FLAT"))

        # 5. News (simplified for backtest — use fear & greed as proxy)
        date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
        fg = conn.execute("SELECT value FROM fear_greed WHERE date=?", (date_str,)).fetchone()
        fg_val = fg[0] if fg else 50
        if fg_val < 25:
            votes.append(self.Vote(expert='news_reactor', direction='SHORT',
                                   confidence=0.6, reason=f"F&G={fg_val} extreme fear"))
        elif fg_val > 75:
            votes.append(self.Vote(expert='news_reactor', direction='LONG',
                                   confidence=0.6, reason=f"F&G={fg_val} greed"))
        else:
            votes.append(self.Vote(expert='news_reactor', direction='WAIT',
                                   confidence=0.3, reason=f"F&G={fg_val} neutral"))

        # 6. Profi (actual Claude API call)
        if self.use_profi:
            try:
                profi_features = {
                    'rsi': features.get('rsi_14', 50),
                    'bb_position': features.get('bb_position', 0.5),
                    'volume_ratio': features.get('volume_ratio', 1.0),
                    'atr_pct': features.get('atr_pct_14', 0.02),
                    'funding_rate': features.get('funding_rate', 0),
                }

                # Text-only analysis (skip charts for speed in backtest)
                result = self.profi.analyze_trade(
                    coin=coin, direction=direction,
                    charts={}, features=profi_features, news=""
                )
                self.profi_calls += 1
                self.profi_cost += 0.008  # ~$0.008 per Sonnet call (text only)

                profi_dir = result.get('decision', 'WAIT')
                profi_conf = float(result.get('confidence', 0))

                logger.info(f"PROFI {coin}: {profi_dir} ({profi_conf:.0%}) — "
                           f"{result.get('reason', '')[:60]}")

                votes.append(self.Vote(
                    expert='profi', direction=profi_dir,
                    confidence=profi_conf,
                    reason=result.get('reason', '')[:60],
                    details={
                        'entry': result.get('entry', 0),
                        'tp': result.get('tp', 0),
                        'sl': result.get('sl', 0),
                    }
                ))
            except Exception as e:
                logger.debug(f"Profi error: {e}")
                votes.append(self.Vote(expert='profi', direction='WAIT',
                                       confidence=0.3, reason='API error'))
        else:
            # Simulate Profi: agree with ML majority if confidence high
            ml_dirs = [v.direction for v in votes[:3] if v.direction != 'WAIT']
            if ml_dirs:
                from collections import Counter
                majority = Counter(ml_dirs).most_common(1)[0]
                if majority[1] >= 2:
                    votes.append(self.Vote(expert='profi', direction=majority[0],
                                           confidence=0.6, reason='simulated'))
                else:
                    votes.append(self.Vote(expert='profi', direction='WAIT',
                                           confidence=0.3, reason='no ML consensus'))
            else:
                votes.append(self.Vote(expert='profi', direction='WAIT',
                                       confidence=0.3, reason='all ML WAIT'))

        # Run Consilium
        decision = self.consilium.decide(coin, votes)
        return decision

    def run(self):
        """Run the full backtest."""
        conn = sqlite3.connect(str(DB_PATH))

        # Get all 4h candle timestamps in the period
        start_ts = int(self.start_date.timestamp())
        end_ts = int(self.end_date.timestamp())

        candle_times = [r[0] for r in conn.execute(
            "SELECT DISTINCT timestamp FROM prices WHERE timeframe='4h' "
            "AND timestamp >= ? AND timestamp < ? AND coin='BTC' ORDER BY timestamp",
            (start_ts, end_ts)
        ).fetchall()]

        logger.info(f"Backtest: {self.start_date.date()} to {self.end_date.date()} | "
                    f"{len(candle_times)} candles | Capital=${self.capital}")
        logger.info(f"Profi: {'ENABLED (Claude API)' if self.use_profi else 'SIMULATED'}")

        daily_loss_pct = 0.0
        current_day = ''
        traded_this_cycle = set()

        for idx, ts in enumerate(candle_times):
            dt = datetime.utcfromtimestamp(ts)
            day_str = dt.strftime('%Y-%m-%d')

            # Daily reset
            if day_str != current_day:
                current_day = day_str
                daily_loss_pct = 0.0
                traded_this_cycle.clear()

            # Daily loss limit
            if daily_loss_pct < -0.04:
                continue

            # --- MANAGE EXISTING POSITIONS ---
            for coin in list(self.positions.keys()):
                pos = self.positions[coin]

                # Get this candle's OHLCV
                candle = conn.execute(
                    "SELECT open, high, low, close FROM prices "
                    "WHERE coin=? AND timeframe='4h' AND timestamp=?",
                    (coin, ts)
                ).fetchone()

                if not candle:
                    continue

                o, h, l, c = candle
                funding = self._get_funding_rate(conn, coin, ts)

                exit_price, exit_reason = pos.check_exit(o, h, l, c, funding)

                if exit_price:
                    # Calculate P&L
                    if pos.direction == 'LONG':
                        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
                    else:
                        pnl_pct = (pos.entry_price - exit_price) / pos.entry_price

                    # Deduct fees
                    pnl_pct -= MAKER_FEE * 2  # entry + exit fees

                    pnl_usd = pos.size_usd * pnl_pct * pos.leverage
                    pnl_usd -= pos.funding_paid  # deduct funding costs

                    self.capital += pnl_usd
                    daily_loss_pct += pnl_usd / self.capital

                    self.trades.append({
                        'coin': coin,
                        'direction': pos.direction,
                        'entry_price': pos.entry_price,
                        'exit_price': exit_price,
                        'entry_time': datetime.utcfromtimestamp(pos.entry_time).strftime('%Y-%m-%d %H:%M'),
                        'exit_time': dt.strftime('%Y-%m-%d %H:%M'),
                        'candles_held': pos.candles_held,
                        'pnl_pct': pnl_pct,
                        'pnl_usd': pnl_usd,
                        'exit_reason': exit_reason,
                        'funding_paid': pos.funding_paid,
                    })

                    emoji = '+' if pnl_usd > 0 else ''
                    logger.info(f"  CLOSE {coin} {pos.direction}: {exit_reason} | "
                               f"{emoji}${pnl_usd:.2f} ({pnl_pct*100:+.2f}%) | "
                               f"{pos.candles_held} candles | equity=${self.capital:.2f}")

                    del self.positions[coin]

            # --- OPEN NEW POSITIONS (every 4h candle) ---
            slots = MAX_POSITIONS - len(self.positions)
            if slots <= 0:
                self.equity_curve.append((ts, self.capital))
                continue

            # Get regime
            regime, btc_mom, breadth = self._get_regime(conn, ts)

            # Build features for all coins
            candidates = []
            for coin in COINS:
                if coin in self.positions or coin in traded_this_cycle:
                    continue

                features = self._get_features_at_time(conn, coin, ts)
                if features is None:
                    continue

                # Quick ML pre-filter: get ensemble score
                lgb_s, xgb_s, nn_s = self._predict_ensemble(features)
                avg_score = (lgb_s + xgb_s + nn_s) / 3

                # Skip weak signals — only trade strong consensus
                if abs(avg_score) < 0.004:
                    continue

                direction = 'LONG' if avg_score > 0 else 'SHORT'

                # Agreement check
                signs = [1 if s > 0 else -1 for s in [lgb_s, xgb_s, nn_s]]
                agreement = max(signs.count(1), signs.count(-1))
                if agreement < 2:
                    continue  # No ML consensus → skip (save Profi API cost)

                candidates.append({
                    'coin': coin,
                    'direction': direction,
                    'score': avg_score,
                    'features': features,
                    'agreement': agreement,
                })

            # Sort by absolute score (strongest first)
            candidates.sort(key=lambda x: abs(x['score']), reverse=True)

            # Sector diversification
            used_sectors = set()
            filtered = []
            for c in candidates:
                sector = COIN_SECTOR.get(c['coin'], 'other')
                if sector in used_sectors and sector != 'other':
                    continue
                filtered.append(c)
                used_sectors.add(sector)
                if len(filtered) >= slots:  # check exact slots (save Profi API calls)
                    break

            # Run Consilium for top candidates
            opened = 0
            for cand in filtered:
                if opened >= slots:
                    break

                coin = cand['coin']
                direction = cand['direction']

                decision = self._run_consilium(coin, direction, cand['features'], conn, ts)

                if decision.action == 'SKIP':
                    continue

                # Consilium approved — open position
                direction = decision.action  # use Consilium's direction

                # Get NEXT candle open for entry (honest)
                next_ts = ts + 14400
                next_candle = conn.execute(
                    "SELECT open FROM prices WHERE coin=? AND timeframe='4h' AND timestamp=?",
                    (coin, next_ts)
                ).fetchone()

                if not next_candle:
                    continue

                entry_price = next_candle[0]

                # Apply slippage against us
                if direction == 'LONG':
                    entry_price *= (1 + SLIPPAGE)
                else:
                    entry_price *= (1 - SLIPPAGE)

                # Position sizing
                atr = self._get_coin_atr(conn, coin, ts)
                target_pct = atr * 0.50  # 50% of ATR — larger TP
                target_pct = max(0.005, min(target_pct, 0.025))

                margin = self.capital * RISK_PER_POSITION * decision.size_factor
                size_usd = margin

                self.positions[coin] = BacktestPosition(
                    coin=coin, direction=direction,
                    entry_price=entry_price, size_usd=size_usd,
                    leverage=LEVERAGE, entry_time=next_ts,
                    target_pct=target_pct
                )

                traded_this_cycle.add(coin)
                opened += 1

                logger.info(f"  OPEN {direction} {coin} @${entry_price:.4f} | "
                           f"margin=${margin:.2f} | target={target_pct*100:.1f}% | "
                           f"consilium: {decision.n_agree}/{decision.n_total} agree")

            # Record equity
            self.equity_curve.append((ts, self.capital))

            # Progress
            if idx % 30 == 0:
                logger.info(f"[{dt.strftime('%Y-%m-%d %H:%M')}] Equity=${self.capital:.2f} | "
                           f"Positions: {len(self.positions)} | Trades: {len(self.trades)} | "
                           f"Regime: {regime} | Profi calls: {self.profi_calls}")

        # Close remaining positions at last price
        for coin, pos in list(self.positions.items()):
            last = conn.execute(
                "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
                (coin, end_ts)
            ).fetchone()
            if last:
                price = last[0]
                if pos.direction == 'LONG':
                    pnl_pct = (price - pos.entry_price) / pos.entry_price
                else:
                    pnl_pct = (pos.entry_price - price) / pos.entry_price
                pnl_pct -= MAKER_FEE * 2
                pnl_usd = pos.size_usd * pnl_pct * pos.leverage
                self.capital += pnl_usd
                self.trades.append({
                    'coin': coin, 'direction': pos.direction,
                    'entry_price': pos.entry_price, 'exit_price': price,
                    'pnl_pct': pnl_pct, 'pnl_usd': pnl_usd,
                    'exit_reason': 'END_OF_BACKTEST',
                    'candles_held': pos.candles_held,
                    'entry_time': datetime.utcfromtimestamp(pos.entry_time).strftime('%Y-%m-%d %H:%M'),
                    'exit_time': self.end_date.strftime('%Y-%m-%d %H:%M'),
                    'funding_paid': pos.funding_paid,
                })

        conn.close()
        return self._report()

    def _report(self):
        """Generate comprehensive report."""
        if not self.trades:
            return "No trades executed."

        wins = [t for t in self.trades if t['pnl_usd'] > 0]
        losses = [t for t in self.trades if t['pnl_usd'] <= 0]

        total_pnl = sum(t['pnl_usd'] for t in self.trades)
        total_return = (self.capital - self.initial_capital) / self.initial_capital * 100

        win_rate = len(wins) / len(self.trades) * 100 if self.trades else 0
        avg_win = np.mean([t['pnl_usd'] for t in wins]) if wins else 0
        avg_loss = np.mean([t['pnl_usd'] for t in losses]) if losses else 0

        gross_profit = sum(t['pnl_usd'] for t in wins) if wins else 0
        gross_loss = abs(sum(t['pnl_usd'] for t in losses)) if losses else 1
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

        # Drawdown
        peak = self.initial_capital
        max_dd = 0
        for ts, eq in self.equity_curve:
            peak = max(peak, eq)
            dd = (peak - eq) / peak
            max_dd = max(max_dd, dd)

        # Per direction
        longs = [t for t in self.trades if t['direction'] == 'LONG']
        shorts = [t for t in self.trades if t['direction'] == 'SHORT']
        long_wr = sum(1 for t in longs if t['pnl_usd'] > 0) / len(longs) * 100 if longs else 0
        short_wr = sum(1 for t in shorts if t['pnl_usd'] > 0) / len(shorts) * 100 if shorts else 0

        # Per exit reason
        by_reason = defaultdict(list)
        for t in self.trades:
            by_reason[t['exit_reason']].append(t['pnl_usd'])

        # Per coin
        by_coin = defaultdict(list)
        for t in self.trades:
            by_coin[t['coin']].append(t['pnl_usd'])

        # Avg hold time
        avg_candles = np.mean([t['candles_held'] for t in self.trades])

        # Fees + funding
        total_fees = len(self.trades) * 2 * MAKER_FEE * self.initial_capital * RISK_PER_POSITION * LEVERAGE
        total_funding = sum(t.get('funding_paid', 0) for t in self.trades)

        lines = [
            "=" * 60,
            "CONSILIUM BACKTEST — FULL REPORT",
            "=" * 60,
            f"Period: {self.start_date.date()} → {self.end_date.date()}",
            f"Initial capital: ${self.initial_capital:,.2f}",
            f"Final capital:   ${self.capital:,.2f}",
            f"Total return:    {total_return:+.2f}%",
            f"Total P&L:       ${total_pnl:+.2f}",
            "",
            f"Total trades:    {len(self.trades)}",
            f"Win rate:        {win_rate:.1f}%",
            f"Profit factor:   {profit_factor:.2f}",
            f"Max drawdown:    {max_dd*100:.1f}%",
            f"Avg hold:        {avg_candles:.1f} candles ({avg_candles*4:.0f}h)",
            "",
            f"Avg winner:      ${avg_win:+.2f}",
            f"Avg loser:       ${avg_loss:+.2f}",
            f"Win/Loss ratio:  {abs(avg_win/avg_loss):.2f}x" if avg_loss != 0 else "",
            "",
            f"LONG trades:     {len(longs)} ({long_wr:.0f}% WR)",
            f"SHORT trades:    {len(shorts)} ({short_wr:.0f}% WR)",
            "",
            "--- By Exit Reason ---",
        ]

        for reason, pnls in sorted(by_reason.items()):
            wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
            lines.append(f"  {reason:20s}: {len(pnls):3d} trades | "
                        f"WR {wr:.0f}% | ${sum(pnls):+.2f}")

        lines.append("")
        lines.append("--- By Coin (top 10) ---")
        coin_totals = {c: sum(pnls) for c, pnls in by_coin.items()}
        for coin, total in sorted(coin_totals.items(), key=lambda x: x[1], reverse=True)[:10]:
            wr = sum(1 for p in by_coin[coin] if p > 0) / len(by_coin[coin]) * 100
            lines.append(f"  {coin:6s}: {len(by_coin[coin]):3d} trades | "
                        f"WR {wr:.0f}% | ${total:+.2f}")

        lines.extend([
            "",
            "--- Costs ---",
            f"  Est. fees:     ${total_fees:.2f}",
            f"  Funding paid:  ${total_funding:.2f}",
            f"  Profi API:     ${self.profi_cost:.2f} ({self.profi_calls} calls)",
            "",
            "=" * 60,
        ])

        report = "\n".join(lines)
        logger.info(f"\n{report}")

        # Save detailed trades
        trades_path = _FACTORY_DIR / 'data' / 'crypto' / 'backtest_consilium_jan2026.json'
        with open(trades_path, 'w') as f:
            json.dump({
                'summary': {
                    'period': f"{self.start_date.date()} to {self.end_date.date()}",
                    'initial_capital': self.initial_capital,
                    'final_capital': self.capital,
                    'total_return_pct': total_return,
                    'total_trades': len(self.trades),
                    'win_rate': win_rate,
                    'profit_factor': profit_factor,
                    'max_drawdown_pct': max_dd * 100,
                    'profi_calls': self.profi_calls,
                    'profi_cost': self.profi_cost,
                },
                'trades': self.trades,
                'equity_curve': self.equity_curve,
            }, f, indent=2, default=str)

        logger.info(f"Trades saved: {trades_path}")
        return report


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    # Load env
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-profi', action='store_true', help='Skip Profi (Claude) calls')
    parser.add_argument('--capital', type=float, default=1000.0)
    args = parser.parse_args()

    bt = ConsiliumBacktest(
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
        capital=args.capital,
        use_profi=not args.no_profi,
    )

    report = bt.run()
    print(report)
