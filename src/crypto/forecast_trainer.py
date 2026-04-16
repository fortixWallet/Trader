"""
FORTIX — Forecast Trainer
=================================
Time-travel backtesting + error analysis + pattern learning.

Runs the REAL forecast_coin() on historical data by creating filtered
database snapshots and patching datetime.now().

Usage:
    python src/crypto/forecast_trainer.py --backfill      # Step 1: backfill data
    python src/crypto/forecast_trainer.py --train          # Step 2: run 5 test periods
    python src/crypto/forecast_trainer.py --analyze        # Step 3: error analysis
    python src/crypto/forecast_trainer.py --learn          # Step 4: extract patterns
    python src/crypto/forecast_trainer.py --full           # All steps
    python src/crypto/forecast_trainer.py --report         # Print latest report
"""

import sys
import uuid
import sqlite3
import logging
import json
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('trainer')

MARKET_DB = Path('data/crypto/market.db')
PATTERNS_DB = Path('data/crypto/patterns.db')

# 5 test periods across different market regimes
TEST_PERIODS = [
    {
        'name': 'P1_spring_2025',
        'start': '2025-05-01',
        'end': '2025-05-14',
        'description': 'Early data — prices + F&G only, no MA200',
    },
    {
        'name': 'P2_summer_2025',
        'start': '2025-07-01',
        'end': '2025-07-14',
        'description': 'Mid-year — prices + F&G + funding rates',
    },
    {
        'name': 'P3_autumn_2025',
        'start': '2025-09-15',
        'end': '2025-09-28',
        'description': 'Full technical — MA200 available!',
    },
    {
        'name': 'P4_late_2025',
        'start': '2025-11-01',
        'end': '2025-11-14',
        'description': 'Late year — all signals active',
    },
    {
        'name': 'P5_jan_2026',
        'start': '2026-01-10',
        'end': '2026-01-23',
        'description': 'Recent crash — F&G extreme fear',
    },
]

HORIZON_DAYS = 7  # How far ahead to check


# ════════════════════════════════════════════
# TIME-TRAVEL DATABASE
# ════════════════════════════════════════════

def create_time_travel_db(source_conn: sqlite3.Connection,
                           cutoff_date: datetime) -> sqlite3.Connection:
    """Create in-memory SQLite copy with only data before cutoff_date.

    All timestamp-based tables are filtered to <= cutoff.
    Date-based tables are filtered to <= cutoff_date string.
    """
    cutoff_ts = int(cutoff_date.timestamp())
    cutoff_str = cutoff_date.strftime('%Y-%m-%d')

    mem_conn = sqlite3.connect(':memory:')

    # Copy schema
    schema_rows = source_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL"
    ).fetchall()
    for (sql,) in schema_rows:
        try:
            mem_conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # Index or table already exists

    # Copy indexes
    idx_rows = source_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
    ).fetchall()
    for (sql,) in idx_rows:
        try:
            mem_conn.execute(sql)
        except sqlite3.OperationalError:
            pass

    # Tables with unix timestamp column
    ts_tables = {
        'prices': 'timestamp',
        'funding_rates': 'timestamp',
        'open_interest': 'timestamp',
        'liquidations': 'timestamp',
        'long_short_ratio': 'timestamp',
        'taker_volume': 'timestamp',
        'news': 'timestamp',
        'whale_transactions': 'timestamp',
        'market_overview': 'timestamp',
        # CoinGlass timestamp tables
        'cg_exchange_balance': 'timestamp',
        'cg_options_max_pain': 'timestamp',
        'cg_aggregated_oi': 'timestamp',
        'cg_liquidations': 'timestamp',
    }

    for table, col in ts_tables.items():
        try:
            rows = source_conn.execute(
                f"SELECT * FROM {table} WHERE {col} <= ?", (cutoff_ts,)
            ).fetchall()
            if rows:
                placeholders = ','.join(['?'] * len(rows[0]))
                mem_conn.executemany(
                    f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", rows
                )
        except sqlite3.OperationalError:
            pass

    # Tables with date string column
    date_tables = {
        'fear_greed': 'date',
        'tvl': 'date',
        'social_sentiment': 'date',
        'global_metrics': 'date',
        # CryptoQuant tables (backfilled to 2025-03-20)
        'cq_btc_onchain': 'date',
        'cq_exchange_flows': 'date',
        'cq_coinbase_premium': 'date',
        'cq_miner_data': 'date',
        'cq_active_addresses': 'date',
        'cq_stablecoin_flows': 'date',
        # CoinGlass date tables
        'cg_etf_flows': 'date',
        'cg_stablecoin_supply': 'date',
    }

    for table, col in date_tables.items():
        try:
            rows = source_conn.execute(
                f"SELECT * FROM {table} WHERE {col} <= ?", (cutoff_str,)
            ).fetchall()
            if rows:
                placeholders = ','.join(['?'] * len(rows[0]))
                mem_conn.executemany(
                    f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", rows
                )
        except sqlite3.OperationalError:
            pass

    # Copy backtest_results and predictions as-is (no filtering needed)
    for table in ['backtest_results']:
        try:
            rows = source_conn.execute(f"SELECT * FROM {table}").fetchall()
            if rows:
                placeholders = ','.join(['?'] * len(rows[0]))
                mem_conn.executemany(
                    f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", rows
                )
        except sqlite3.OperationalError:
            pass

    mem_conn.commit()
    return mem_conn


@contextmanager
def time_travel(target_date: datetime):
    """Monkey-patch datetime.now() in forecast_engine to return target_date."""
    import src.crypto.forecast_engine as fe

    original_datetime = fe.datetime

    class MockDatetime(type(original_datetime)):
        """Datetime replacement that returns target_date for now()."""
        @classmethod
        def now(cls, tz=None):
            if tz:
                return target_date.replace(tzinfo=tz)
            return target_date

    # Patch the module
    fe.datetime = MockDatetime
    try:
        yield
    finally:
        fe.datetime = original_datetime


# ════════════════════════════════════════════
# FORECAST TRAINER
# ════════════════════════════════════════════

class ForecastTrainer:
    """Run time-travel forecasts and analyze results."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(MARKET_DB)
        self.source_conn = sqlite3.connect(self.db_path)
        self.run_id = str(uuid.uuid4())[:8]
        self.results = []

        # Init patterns DB
        PATTERNS_DB.parent.mkdir(parents=True, exist_ok=True)
        self.pat_conn = sqlite3.connect(str(PATTERNS_DB))
        self._init_results_table()

    def _init_results_table(self):
        """Create training results table in patterns.db."""
        self.pat_conn.executescript("""
            CREATE TABLE IF NOT EXISTS training_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                coin TEXT NOT NULL,
                forecast_date TEXT NOT NULL,
                check_date TEXT NOT NULL,
                period_name TEXT,

                -- Forecast output
                composite_score REAL,
                prediction TEXT,
                confidence INTEGER,

                -- Per-category scores
                technical_score REAL,
                sentiment_score REAL,
                onchain_score REAL,
                macro_score REAL,
                news_score REAL,
                historical_score REAL,

                -- Per-category data availability
                technical_has_data INTEGER,
                sentiment_has_data INTEGER,
                onchain_has_data INTEGER,
                macro_has_data INTEGER,
                news_has_data INTEGER,
                historical_has_data INTEGER,

                -- Signal details (JSON)
                signal_details_json TEXT,

                -- Actual outcome
                price_at_forecast REAL,
                price_at_check REAL,
                actual_change_pct REAL,

                -- Evaluation
                direction_correct INTEGER,
                prediction_correct INTEGER,

                -- Context features (for pattern learning)
                rsi_at_forecast REAL,
                fg_at_forecast INTEGER,
                funding_rate_at_forecast REAL,
                ma200_trend TEXT,
                volatility_at_forecast REAL,
                btc_change_7d REAL,

                UNIQUE(run_id, coin, forecast_date)
            );

            CREATE TABLE IF NOT EXISTS training_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                n_predictions INTEGER,
                direction_accuracy REAL,
                periods_json TEXT,
                summary_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_tr_run ON training_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_tr_coin ON training_results(coin);
            CREATE INDEX IF NOT EXISTS idx_tr_period ON training_results(period_name);
        """)
        self.pat_conn.commit()

    def get_actual_price(self, coin: str, target_date: datetime) -> Optional[float]:
        """Get closing price on a specific date from prices table."""
        target_str = target_date.strftime('%Y-%m-%d')

        # Find closest 1d candle to target date (within ±1 day)
        row = self.source_conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "AND date(timestamp, 'unixepoch') BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
            "ORDER BY ABS(timestamp - ?) LIMIT 1",
            (coin, target_str, target_str, int(target_date.timestamp()))
        ).fetchone()

        return float(row[0]) if row else None

    def get_context_features(self, tt_conn: sqlite3.Connection,
                             coin: str, forecast_date: datetime) -> dict:
        """Extract context features for pattern learning."""
        from src.crypto.technical_analyzer import get_prices

        features = {
            'rsi': None,
            'fg': None,
            'funding_rate': None,
            'ma200_trend': 'unavailable',
            'volatility': None,
            'btc_change_7d': None,
        }

        try:
            prices = get_prices(tt_conn, coin, '1d', 365)
            if len(prices) < 14:
                return features

            closes = np.array([p[4] for p in prices])

            # RSI
            deltas = np.diff(closes)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            period = 14
            if len(gains) >= period:
                avg_gain = np.mean(gains[-period:])
                avg_loss = np.mean(losses[-period:])
                if avg_loss > 0:
                    rs = avg_gain / avg_loss
                    features['rsi'] = round(100 - (100 / (1 + rs)), 1)

            # MA200
            if len(closes) >= 200:
                sma200 = np.mean(closes[-200:])
                features['ma200_trend'] = 'above' if closes[-1] > sma200 else 'below'

            # Volatility (30d)
            if len(closes) >= 31:
                returns = np.diff(closes[-31:]) / closes[-31:-1]
                features['volatility'] = round(float(np.std(returns)), 6)

            # BTC 7d change
            btc_prices = get_prices(tt_conn, 'BTC', '1d', 14)
            if len(btc_prices) >= 8:
                btc_closes = [p[4] for p in btc_prices]
                features['btc_change_7d'] = round(
                    (btc_closes[-1] - btc_closes[-8]) / btc_closes[-8] * 100, 2
                )

        except Exception as e:
            log.debug(f"  Context features error for {coin}: {e}")

        # Fear & Greed
        try:
            fg_row = tt_conn.execute(
                "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if fg_row:
                features['fg'] = fg_row[0]
        except Exception:
            pass

        # Funding rate
        try:
            fr_row = tt_conn.execute(
                "SELECT AVG(rate) FROM (SELECT rate FROM funding_rates "
                "WHERE coin = ? ORDER BY timestamp DESC LIMIT 30)",
                (coin,)
            ).fetchone()
            if fr_row and fr_row[0] is not None:
                features['funding_rate'] = round(float(fr_row[0]), 8)
        except Exception:
            pass

        return features

    def run_single_forecast(self, coin: str, forecast_date: datetime,
                            tt_conn: sqlite3.Connection,
                            period_name: str = '') -> Optional[dict]:
        """Run one forecast at a historical date and compare to actual.

        Args:
            coin: coin symbol
            forecast_date: date to forecast FROM
            tt_conn: pre-created time-travel database connection
            period_name: test period identifier

        Returns dict with forecast + actual outcome + evaluation.
        """
        from src.crypto.forecast_engine import forecast_coin

        check_date = forecast_date + timedelta(days=HORIZON_DAYS)

        # Get actual prices
        price_at_forecast = self.get_actual_price(coin, forecast_date)
        price_at_check = self.get_actual_price(coin, check_date)

        if price_at_forecast is None or price_at_check is None:
            return None  # Can't evaluate without prices

        if price_at_forecast == 0:
            return None

        actual_change = (price_at_check - price_at_forecast) / price_at_forecast * 100

        # Run forecast with time-travel
        with time_travel(forecast_date):
            try:
                forecast = forecast_coin(tt_conn, coin, regime=None)
            except Exception as e:
                log.debug(f"  {coin} @ {forecast_date.strftime('%Y-%m-%d')}: forecast error — {e}")
                return None

        if forecast.get('error'):
            return None

        composite = forecast['composite_score']
        prediction = forecast['prediction']

        # Evaluate direction
        if prediction in ('STRONG BUY', 'BUY'):
            predicted_direction = 1  # up
        elif prediction in ('STRONG SELL', 'SELL'):
            predicted_direction = -1  # down
        else:
            predicted_direction = 0  # neutral

        actual_direction = 1 if actual_change > 0.5 else (-1 if actual_change < -0.5 else 0)

        if predicted_direction == 0:
            direction_correct = None  # NEUTRAL — skip for accuracy
        else:
            direction_correct = 1 if (predicted_direction == actual_direction) else 0

        # Evaluate prediction precision
        prediction_correct = self._evaluate_prediction(prediction, actual_change)

        # Extract category scores
        cats = forecast.get('categories', {})

        # Context features
        features = self.get_context_features(tt_conn, coin, forecast_date)

        result = {
            'run_id': self.run_id,
            'coin': coin,
            'forecast_date': forecast_date.strftime('%Y-%m-%d'),
            'check_date': check_date.strftime('%Y-%m-%d'),
            'period_name': period_name,
            'composite_score': composite,
            'prediction': prediction,
            'confidence': forecast.get('confidence', 0),
            'technical_score': cats.get('technical', {}).get('score', 0),
            'sentiment_score': cats.get('sentiment', {}).get('score', 0),
            'onchain_score': cats.get('onchain', {}).get('score', 0),
            'macro_score': cats.get('macro', {}).get('score', 0),
            'news_score': cats.get('news', {}).get('score', 0),
            'historical_score': cats.get('historical', {}).get('score', 0),
            'technical_has_data': int(cats.get('technical', {}).get('has_data', False)),
            'sentiment_has_data': int(cats.get('sentiment', {}).get('has_data', False)),
            'onchain_has_data': int(cats.get('onchain', {}).get('has_data', False)),
            'macro_has_data': int(cats.get('macro', {}).get('has_data', False)),
            'news_has_data': int(cats.get('news', {}).get('has_data', False)),
            'historical_has_data': int(cats.get('historical', {}).get('has_data', False)),
            'signal_details_json': json.dumps({
                k: {'score': v.get('score', 0), 'has_data': v.get('has_data', False)}
                for k, v in cats.items()
            }),
            'price_at_forecast': price_at_forecast,
            'price_at_check': price_at_check,
            'actual_change_pct': round(actual_change, 3),
            'direction_correct': direction_correct,
            'prediction_correct': prediction_correct,
            'rsi_at_forecast': features.get('rsi'),
            'fg_at_forecast': features.get('fg'),
            'funding_rate_at_forecast': features.get('funding_rate'),
            'ma200_trend': features.get('ma200_trend', 'unavailable'),
            'volatility_at_forecast': features.get('volatility'),
            'btc_change_7d': features.get('btc_change_7d'),
        }

        return result

    def _evaluate_prediction(self, prediction: str, actual_change: float) -> int:
        """Check if prediction matches actual outcome.
        Uses same ranges as prediction_tracker.py.
        """
        ranges = {
            'STRONG BUY': (5, 100),
            'BUY': (2, 15),
            'NEUTRAL': (-2, 2),
            'SELL': (-15, -2),
            'STRONG SELL': (-100, -5),
        }
        low, high = ranges.get(prediction, (-100, 100))
        return 1 if low <= actual_change <= high else 0

    def run_period(self, period: dict) -> list:
        """Run forecasts for all coins across all days in a period."""
        from src.crypto.data_collector import TRACKED_COINS

        name = period['name']
        start = datetime.strptime(period['start'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end = datetime.strptime(period['end'], '%Y-%m-%d').replace(tzinfo=timezone.utc)

        log.info(f"\n{'='*60}")
        log.info(f"PERIOD: {name} ({period['start']} → {period['end']})")
        log.info(f"  {period['description']}")
        log.info(f"{'='*60}")

        period_results = []
        current_date = start

        while current_date <= end:
            date_str = current_date.strftime('%Y-%m-%d')

            # Create one time-travel DB per day (shared across coins)
            tt_conn = create_time_travel_db(self.source_conn, current_date)

            n_ok = 0
            n_correct = 0
            n_total = 0

            for coin in TRACKED_COINS:
                result = self.run_single_forecast(
                    coin, current_date, tt_conn, period_name=name
                )
                if result:
                    period_results.append(result)
                    n_total += 1
                    if result['direction_correct'] is not None:
                        n_ok += 1
                        n_correct += result['direction_correct']

            tt_conn.close()

            accuracy = (n_correct / n_ok * 100) if n_ok > 0 else 0
            log.info(f"  {date_str}: {n_total} forecasts, "
                     f"{n_correct}/{n_ok} direction correct ({accuracy:.0f}%)")

            current_date += timedelta(days=1)

        # Period summary
        actionable = [r for r in period_results if r['direction_correct'] is not None]
        if actionable:
            period_acc = sum(r['direction_correct'] for r in actionable) / len(actionable) * 100
            log.info(f"\n  PERIOD {name} RESULT: {len(actionable)} actionable predictions, "
                     f"{period_acc:.1f}% direction accuracy")
        else:
            log.info(f"\n  PERIOD {name}: no actionable predictions")

        return period_results

    def run_all_periods(self, periods: list = None) -> list:
        """Run the full training suite across all periods."""
        periods = periods or TEST_PERIODS

        log.info("=" * 60)
        log.info(f"FORECAST TRAINER — run_id: {self.run_id}")
        log.info(f"  Periods: {len(periods)}")
        log.info(f"  Horizon: {HORIZON_DAYS} days")
        log.info("=" * 60)

        # Record run start
        self.pat_conn.execute(
            "INSERT OR REPLACE INTO training_runs (run_id, started_at, periods_json) "
            "VALUES (?, ?, ?)",
            (self.run_id, datetime.now(timezone.utc).isoformat(),
             json.dumps([p['name'] for p in periods]))
        )
        self.pat_conn.commit()

        all_results = []
        for period in periods:
            period_results = self.run_period(period)
            all_results.extend(period_results)

        self.results = all_results

        # Save all results to patterns.db
        self._save_results(all_results)

        # Print summary
        self._print_summary(all_results)

        return all_results

    def _save_results(self, results: list):
        """Save training results to patterns.db."""
        for r in results:
            self.pat_conn.execute(
                "INSERT OR REPLACE INTO training_results "
                "(run_id, coin, forecast_date, check_date, period_name, "
                "composite_score, prediction, confidence, "
                "technical_score, sentiment_score, onchain_score, "
                "macro_score, news_score, historical_score, "
                "technical_has_data, sentiment_has_data, onchain_has_data, "
                "macro_has_data, news_has_data, historical_has_data, "
                "signal_details_json, "
                "price_at_forecast, price_at_check, actual_change_pct, "
                "direction_correct, prediction_correct, "
                "rsi_at_forecast, fg_at_forecast, funding_rate_at_forecast, "
                "ma200_trend, volatility_at_forecast, btc_change_7d) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    r['run_id'], r['coin'], r['forecast_date'], r['check_date'],
                    r['period_name'],
                    r['composite_score'], r['prediction'], r['confidence'],
                    r['technical_score'], r['sentiment_score'], r['onchain_score'],
                    r['macro_score'], r['news_score'], r['historical_score'],
                    r['technical_has_data'], r['sentiment_has_data'], r['onchain_has_data'],
                    r['macro_has_data'], r['news_has_data'], r['historical_has_data'],
                    r['signal_details_json'],
                    r['price_at_forecast'], r['price_at_check'], r['actual_change_pct'],
                    r['direction_correct'], r['prediction_correct'],
                    r['rsi_at_forecast'], r['fg_at_forecast'],
                    r['funding_rate_at_forecast'],
                    r['ma200_trend'], r['volatility_at_forecast'],
                    r['btc_change_7d'],
                )
            )

        # Update run metadata
        actionable = [r for r in results if r['direction_correct'] is not None]
        accuracy = (sum(r['direction_correct'] for r in actionable) / len(actionable)
                    if actionable else 0)

        self.pat_conn.execute(
            "UPDATE training_runs SET completed_at=?, n_predictions=?, "
            "direction_accuracy=? WHERE run_id=?",
            (datetime.now(timezone.utc).isoformat(), len(results), accuracy, self.run_id)
        )
        self.pat_conn.commit()
        log.info(f"\n  Saved {len(results)} results to patterns.db (run_id: {self.run_id})")

    def _print_summary(self, results: list):
        """Print comprehensive training summary."""
        log.info("\n" + "=" * 60)
        log.info("TRAINING SUMMARY")
        log.info("=" * 60)

        actionable = [r for r in results if r['direction_correct'] is not None]
        all_count = len(results)
        act_count = len(actionable)

        if not actionable:
            log.info("  No actionable predictions")
            return

        correct = sum(r['direction_correct'] for r in actionable)
        accuracy = correct / act_count * 100

        log.info(f"  Total predictions:   {all_count}")
        log.info(f"  Actionable (non-NEUTRAL): {act_count}")
        log.info(f"  Direction accuracy:  {correct}/{act_count} ({accuracy:.1f}%)")

        # By period
        log.info(f"\n  {'Period':<20} {'Total':>6} {'Actionable':>10} {'Correct':>8} {'Accuracy':>8}")
        log.info(f"  {'-'*52}")
        for period in TEST_PERIODS:
            p_results = [r for r in results if r['period_name'] == period['name']]
            p_act = [r for r in p_results if r['direction_correct'] is not None]
            p_correct = sum(r['direction_correct'] for r in p_act) if p_act else 0
            p_acc = (p_correct / len(p_act) * 100) if p_act else 0
            log.info(f"  {period['name']:<20} {len(p_results):>6} {len(p_act):>10} "
                     f"{p_correct:>8} {p_acc:>7.1f}%")

        # By category solo contribution
        log.info(f"\n  Category solo direction accuracy:")
        cats = ['technical', 'sentiment', 'onchain', 'macro', 'news', 'historical']
        for cat in cats:
            cat_correct = 0
            cat_total = 0
            for r in actionable:
                score_key = f'{cat}_score'
                has_key = f'{cat}_has_data'
                if not r.get(has_key):
                    continue
                cat_score = r.get(score_key, 0)
                if abs(cat_score) < 0.01:
                    continue
                cat_total += 1
                actual_dir = 1 if r['actual_change_pct'] > 0 else -1
                predicted_dir = 1 if cat_score > 0 else -1
                if predicted_dir == actual_dir:
                    cat_correct += 1

            cat_acc = (cat_correct / cat_total * 100) if cat_total > 0 else 0
            log.info(f"    {cat:12s}: {cat_correct}/{cat_total} ({cat_acc:.1f}%)")

        # Worst failures
        sorted_by_error = sorted(
            actionable,
            key=lambda r: abs(r['actual_change_pct']) if not r['direction_correct'] else 0,
            reverse=True
        )
        log.info(f"\n  Top 5 worst direction failures:")
        for r in sorted_by_error[:5]:
            if r['direction_correct']:
                continue
            log.info(f"    {r['coin']:6s} {r['forecast_date']}: "
                     f"predicted {r['prediction']:12s} (score={r['composite_score']:+.3f}), "
                     f"actual {r['actual_change_pct']:+.1f}%")

    def close(self):
        """Close database connections."""
        self.source_conn.close()
        self.pat_conn.close()


# ════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Forecast Training System')
    parser.add_argument('--backfill', action='store_true', help='Backfill historical data')
    parser.add_argument('--train', action='store_true', help='Run training on all periods')
    parser.add_argument('--analyze', action='store_true', help='Run error analysis')
    parser.add_argument('--learn', action='store_true', help='Extract patterns')
    parser.add_argument('--full', action='store_true', help='All steps')
    parser.add_argument('--report', action='store_true', help='Print latest report')
    args = parser.parse_args()

    if args.backfill or args.full:
        log.info("STEP 1: Backfilling historical data...")
        from src.crypto.backfill_history import HistoryBackfiller
        from src.crypto.data_collector import init_db
        conn = init_db()
        bf = HistoryBackfiller(conn)
        bf.run_full_backfill()
        conn.close()

    if args.train or args.full:
        log.info("\nSTEP 2: Running time-travel training...")
        trainer = ForecastTrainer()
        results = trainer.run_all_periods()
        run_id = trainer.run_id
        trainer.close()

    if args.analyze or args.full:
        log.info("\nSTEP 3: Error analysis...")
        from src.crypto.error_analyzer import ErrorAnalyzer
        analyzer = ErrorAnalyzer(str(PATTERNS_DB))
        # Use latest run_id
        latest = sqlite3.connect(str(PATTERNS_DB)).execute(
            "SELECT run_id FROM training_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if latest:
            report = analyzer.analyze_run(latest[0])
            analyzer.print_report(report)

    if args.learn or args.full:
        log.info("\nSTEP 4: Pattern extraction...")
        from src.crypto.pattern_learner import PatternLearner
        learner = PatternLearner(str(PATTERNS_DB))
        latest = sqlite3.connect(str(PATTERNS_DB)).execute(
            "SELECT run_id FROM training_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if latest:
            patterns = learner.extract_patterns(latest[0])
            learner.print_patterns(patterns)

    if args.report:
        log.info("\nLatest training report:")
        pat_conn = sqlite3.connect(str(PATTERNS_DB))
        latest = pat_conn.execute(
            "SELECT * FROM training_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if latest:
            log.info(f"  Run ID: {latest[0]}")
            log.info(f"  Started: {latest[1]}")
            log.info(f"  Predictions: {latest[3]}")
            log.info(f"  Accuracy: {latest[4]*100:.1f}%" if latest[4] else "  Accuracy: N/A")
        pat_conn.close()

    if not any([args.backfill, args.train, args.analyze, args.learn, args.full, args.report]):
        parser.print_help()
