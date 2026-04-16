"""
FORTIX — Backtesting Engine
===================================
Walk-forward backtesting of technical analysis signals.

For each day D in the last `window` days:
  1. Take price data UP TO day D
  2. Run technical indicators on that slice
  3. Get signal (BUY/SELL/NEUTRAL)
  4. Check actual price change after `horizon` days
  5. Score: did we predict direction correctly?

No external API needed — uses existing prices table (365 days × 25 coins).
"""

import sys
import sqlite3
import logging
import json
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.crypto.technical_analyzer import (
    score_ma_crossover, score_rsi, score_bollinger,
    score_macd, score_volume_trend,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('backtester')

DB_PATH = Path('data/crypto/market.db')

# Same weights as in analyze_coin()
SIGNAL_WEIGHTS = {
    'ma_crossover': 0.30,
    'rsi': 0.25,
    'bollinger': 0.20,
    'macd': 0.15,
    'volume': 0.10,
}

# Score thresholds for BUY/SELL (matches forecast_engine NEUTRAL band ±0.1)
BUY_THRESHOLD = 0.1
SELL_THRESHOLD = -0.1


class TechnicalBacktester:
    """Walk-forward backtester for technical analysis signals."""

    def __init__(self, conn: sqlite3.Connection = None, coins: list = None,
                 window: int = 180, horizon: int = 7):
        self.conn = conn or sqlite3.connect(str(DB_PATH), timeout=60)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self._own_conn = conn is None
        self.window = window  # How many days to backtest over
        self.horizon = horizon  # How many days ahead to check

        from src.crypto.data_collector import TRACKED_COINS
        self.coins = coins or TRACKED_COINS

    def close(self):
        if self._own_conn:
            self.conn.close()

    def _get_all_daily_prices(self, coin: str) -> list:
        """Get all daily OHLCV data for a coin, sorted by timestamp."""
        rows = self.conn.execute(
            "SELECT timestamp, open, high, low, close, volume FROM prices "
            "WHERE coin = ? AND timeframe = '1d' ORDER BY timestamp ASC",
            (coin,)
        ).fetchall()
        return rows

    def _run_technical_on_slice(self, price_slice: list) -> dict:
        """Run technical analysis on a price data slice.

        Returns composite score + per-signal scores.
        Same logic as analyze_coin() but works on raw price list.
        """
        if len(price_slice) < 30:
            return None

        closes = np.array([p[4] for p in price_slice])

        signals = {}
        try:
            signals['ma_crossover'] = score_ma_crossover(closes)
        except Exception:
            signals['ma_crossover'] = {'score': 0.0}

        try:
            signals['rsi'] = score_rsi(closes)
        except Exception:
            signals['rsi'] = {'score': 0.0}

        try:
            signals['bollinger'] = score_bollinger(closes)
        except Exception:
            signals['bollinger'] = {'score': 0.0}

        try:
            signals['macd'] = score_macd(closes)
        except Exception:
            signals['macd'] = {'score': 0.0}

        try:
            signals['volume'] = score_volume_trend(price_slice)
        except Exception:
            signals['volume'] = {'score': 0.0}

        total_score = sum(
            signals[k]['score'] * SIGNAL_WEIGHTS[k]
            for k in SIGNAL_WEIGHTS
            if k in signals
        )

        return {
            'score': round(float(total_score), 4),
            'signals': {k: v['score'] for k, v in signals.items()},
        }

    def run_walkforward(self) -> dict:
        """Run walk-forward backtest for all coins.

        Returns:
            {
                'total_predictions': int,
                'correct_predictions': int,
                'win_rate': float,
                'per_coin': {coin: {'wins': N, 'total': N, 'win_rate': float}},
                'per_signal': {signal: {'wins': N, 'total': N, 'win_rate': float}},
                'monthly': {month: {'wins': N, 'total': N, 'win_rate': float}},
                'predictions': [list of individual predictions],
            }
        """
        log.info(f"Running walk-forward backtest: window={self.window}D, horizon={self.horizon}D")

        all_predictions = []
        per_coin = defaultdict(lambda: {'wins': 0, 'losses': 0, 'neutral_skip': 0, 'total': 0})
        per_signal = defaultdict(lambda: {'correct_count': 0, 'total_count': 0, 'scores': []})
        monthly = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total': 0})

        for coin in self.coins:
            prices = self._get_all_daily_prices(coin)
            if len(prices) < 60 + self.horizon:
                log.debug(f"  {coin}: only {len(prices)} days, skipping")
                continue

            # We need at least 30 days of data for TA + horizon days for verification
            # Walk from (len - window - horizon) to (len - horizon)
            n = len(prices)
            start_idx = max(30, n - self.window - self.horizon)
            end_idx = n - self.horizon

            coin_count = 0
            for day_idx in range(start_idx, end_idx):
                # Slice: all data up to this day (inclusive)
                price_slice = prices[:day_idx + 1]

                result = self._run_technical_on_slice(price_slice)
                if result is None:
                    continue

                score = result['score']

                # Classify signal
                if score > BUY_THRESHOLD:
                    predicted = 'BUY'
                elif score < SELL_THRESHOLD:
                    predicted = 'SELL'
                else:
                    # Skip NEUTRAL predictions — they're not actionable
                    per_coin[coin]['neutral_skip'] += 1
                    continue

                # What actually happened?
                entry_price = prices[day_idx][4]  # close price on signal day
                exit_price = prices[day_idx + self.horizon][4]  # close price H days later

                if entry_price == 0:
                    continue

                actual_change_pct = (exit_price - entry_price) / entry_price * 100

                # Did we get direction right?
                if predicted == 'BUY':
                    correct = actual_change_pct > 0
                else:  # SELL
                    correct = actual_change_pct < 0

                # Timestamp for monthly grouping
                ts = prices[day_idx][0]
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                month_key = dt.strftime('%Y-%m')

                prediction = {
                    'coin': coin,
                    'date': dt.strftime('%Y-%m-%d'),
                    'score': score,
                    'predicted': predicted,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'actual_change_pct': round(actual_change_pct, 2),
                    'correct': correct,
                }
                all_predictions.append(prediction)

                # Accumulate stats
                per_coin[coin]['total'] += 1
                if correct:
                    per_coin[coin]['wins'] += 1
                else:
                    per_coin[coin]['losses'] += 1

                monthly[month_key]['total'] += 1
                if correct:
                    monthly[month_key]['wins'] += 1
                else:
                    monthly[month_key]['losses'] += 1

                # Per-signal accuracy: which signals were right on their own?
                for sig_name, sig_score in result['signals'].items():
                    if abs(sig_score) < 0.1:
                        continue  # Skip weak signals
                    sig_predicted_up = sig_score > 0
                    sig_correct = (sig_predicted_up and actual_change_pct > 0) or \
                                  (not sig_predicted_up and actual_change_pct < 0)
                    per_signal[sig_name]['total_count'] += 1
                    if sig_correct:
                        per_signal[sig_name]['correct_count'] += 1

                coin_count += 1

            if coin_count > 0:
                wr = per_coin[coin]['wins'] / per_coin[coin]['total'] * 100
                log.debug(f"  {coin}: {coin_count} predictions, win rate {wr:.1f}%")

        # Compute aggregate metrics
        total = len(all_predictions)
        correct = sum(1 for p in all_predictions if p['correct'])
        win_rate = correct / total * 100 if total > 0 else 0

        # Per-coin win rates
        per_coin_rates = {}
        for coin, stats in sorted(per_coin.items()):
            if stats['total'] > 0:
                per_coin_rates[coin] = {
                    'wins': stats['wins'],
                    'losses': stats['losses'],
                    'neutral_skip': stats['neutral_skip'],
                    'total': stats['total'],
                    'win_rate': round(stats['wins'] / stats['total'] * 100, 1),
                }

        # Per-signal win rates
        per_signal_rates = {}
        for sig, stats in sorted(per_signal.items()):
            if stats['total_count'] > 0:
                per_signal_rates[sig] = {
                    'correct': stats['correct_count'],
                    'total': stats['total_count'],
                    'win_rate': round(stats['correct_count'] / stats['total_count'] * 100, 1),
                }

        # Monthly win rates
        monthly_rates = {}
        for month, stats in sorted(monthly.items()):
            if stats['total'] > 0:
                monthly_rates[month] = {
                    'wins': stats['wins'],
                    'total': stats['total'],
                    'win_rate': round(stats['wins'] / stats['total'] * 100, 1),
                }

        # Max consecutive losses (drawdown)
        max_streak = 0
        current_streak = 0
        for p in all_predictions:
            if not p['correct']:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        result = {
            'total_predictions': total,
            'correct_predictions': correct,
            'win_rate': round(win_rate, 1),
            'per_coin': per_coin_rates,
            'per_signal': per_signal_rates,
            'monthly': monthly_rates,
            'max_loss_streak': max_streak,
            'window_days': self.window,
            'horizon_days': self.horizon,
        }

        log.info(f"  Backtest complete: {total} predictions, win rate {win_rate:.1f}%")
        log.info(f"  Max consecutive losses: {max_streak}")

        return result

    def save_results(self, results: dict, conn: sqlite3.Connection = None):
        """Save backtest results to database."""
        c = conn or self.conn
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        c.execute(
            "INSERT OR REPLACE INTO backtest_results "
            "(run_date, backtest_type, window_days, horizon_days, "
            "total_predictions, correct_predictions, win_rate, "
            "per_coin_json, per_signal_json, metadata_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                today, 'technical',
                results['window_days'], results['horizon_days'],
                results['total_predictions'], results['correct_predictions'],
                results['win_rate'],
                json.dumps(results['per_coin']),
                json.dumps(results['per_signal']),
                json.dumps({
                    'monthly': results['monthly'],
                    'max_loss_streak': results['max_loss_streak'],
                }),
            )
        )
        c.commit()
        log.info(f"  Saved backtest results to DB (run_date={today})")

    @staticmethod
    def load_latest(conn: sqlite3.Connection) -> dict:
        """Load latest backtest results from database."""
        row = conn.execute(
            "SELECT * FROM backtest_results "
            "WHERE backtest_type = 'technical' "
            "ORDER BY run_date DESC LIMIT 1"
        ).fetchone()

        if not row:
            return None

        return {
            'run_date': row[1],
            'backtest_type': row[2],
            'window_days': row[3],
            'horizon_days': row[4],
            'total_predictions': row[5],
            'correct_predictions': row[6],
            'win_rate': row[7],
            'per_coin': json.loads(row[8]) if row[8] else {},
            'per_signal': json.loads(row[9]) if row[9] else {},
            'metadata': json.loads(row[10]) if row[10] else {},
        }


if __name__ == '__main__':
    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Walk-Forward Backtest")
    log.info("=" * 60)

    from src.crypto.data_collector import init_db
    conn = init_db()

    bt = TechnicalBacktester(conn=conn, window=180, horizon=7)
    results = bt.run_walkforward()

    print(f"\n{'='*50}")
    print(f"OVERALL: {results['win_rate']:.1f}% "
          f"({results['correct_predictions']}/{results['total_predictions']})")
    print(f"Max consecutive losses: {results['max_loss_streak']}")
    print(f"{'='*50}")

    # Per-signal accuracy
    print(f"\nPer-Signal Accuracy:")
    for sig, stats in sorted(results['per_signal'].items(),
                             key=lambda x: x[1]['win_rate'], reverse=True):
        print(f"  {sig:<15} {stats['win_rate']:>5.1f}% ({stats['correct']}/{stats['total']})")

    # Per-coin accuracy (top 10)
    print(f"\nPer-Coin Accuracy (top 10):")
    sorted_coins = sorted(results['per_coin'].items(),
                          key=lambda x: x[1]['win_rate'], reverse=True)
    for coin, stats in sorted_coins[:10]:
        print(f"  {coin:<8} {stats['win_rate']:>5.1f}% "
              f"({stats['wins']}/{stats['total']}, skipped {stats['neutral_skip']} neutral)")

    # Monthly breakdown
    print(f"\nMonthly Win Rate:")
    for month, stats in sorted(results['monthly'].items()):
        bar = '#' * int(stats['win_rate'] / 5)
        print(f"  {month}  {stats['win_rate']:>5.1f}% ({stats['wins']}/{stats['total']}) {bar}")

    # Save to DB
    bt.save_results(results)
    bt.close()
