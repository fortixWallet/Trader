"""
FORTIX — Prediction Tracker
====================================
Checks prediction accuracy after target_date passes.
Fetches actual prices, calculates hit/miss, generates scorecard.

This is the #1 trust feature of the FORTIX channel.

Usage:
    python src/crypto/prediction_tracker.py           # Check all matured predictions
    python src/crypto/prediction_tracker.py --report   # Generate full scorecard report
    python src/crypto/prediction_tracker.py --chart    # Generate scorecard chart
"""

import os
import sys
import sqlite3
import logging
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('prediction_tracker')

DB_PATH = Path('data/crypto/market.db')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY', '')

# Map coin tickers to CoinGecko IDs
COIN_TO_COINGECKO = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana',
    'BNB': 'binancecoin', 'XRP': 'ripple', 'ADA': 'cardano',
    'AVAX': 'avalanche-2', 'DOT': 'polkadot', 'LINK': 'chainlink',
    'POL': 'polygon-ecosystem-token', 'UNI': 'uniswap', 'AAVE': 'aave',
    'MKR': 'maker', 'LDO': 'lido-dao', 'CRV': 'curve-dao-token',
    'ARB': 'arbitrum', 'OP': 'optimism', 'STRK': 'starknet',
    'FET': 'artificial-superintelligence-alliance', 'RENDER': 'render-token',
    'TAO': 'bittensor', 'DOGE': 'dogecoin', 'SHIB': 'shiba-inu',
    'PEPE': 'pepe', 'WIF': 'dogwifcoin', 'BONK': 'bonk',
}

# What each prediction signal expects
PREDICTION_RANGES = {
    'STRONG BUY':  {'min': 5.0,  'max': 15.0, 'direction': 1},
    'BUY':         {'min': 2.0,  'max': 5.0,  'direction': 1},
    'NEUTRAL':     {'min': -2.0, 'max': 2.0,  'direction': 0},
    'SELL':        {'min': -5.0, 'max': -2.0, 'direction': -1},
    'STRONG SELL': {'min': -15.0,'max': -5.0, 'direction': -1},
}


def fetch_current_prices(coins: list) -> dict:
    """Fetch current prices from CoinGecko for a list of coins."""
    ids = [COIN_TO_COINGECKO.get(c) for c in coins if c in COIN_TO_COINGECKO]
    ids = [x for x in ids if x]

    if not ids:
        return {}

    # CoinGecko simple price endpoint
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': ','.join(ids),
        'vs_currencies': 'usd',
    }
    headers = {}
    if COINGECKO_API_KEY:
        headers['x-cg-demo-key'] = COINGECKO_API_KEY

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            log.error(f"CoinGecko error {resp.status_code}: {resp.text[:200]}")
            return {}

        data = resp.json()
        # Reverse map: coingecko_id → ticker
        id_to_ticker = {v: k for k, v in COIN_TO_COINGECKO.items()}
        prices = {}
        for cg_id, price_data in data.items():
            ticker = id_to_ticker.get(cg_id)
            if ticker and 'usd' in price_data:
                prices[ticker] = price_data['usd']

        return prices

    except Exception as e:
        log.error(f"Failed to fetch prices: {e}")
        return {}


def fetch_historical_price(coin: str, date_str: str) -> Optional[float]:
    """Fetch price for a specific date from CoinGecko."""
    cg_id = COIN_TO_COINGECKO.get(coin)
    if not cg_id:
        return None

    # CoinGecko history endpoint: dd-mm-yyyy format
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        cg_date = dt.strftime('%d-%m-%Y')
    except ValueError:
        return None

    url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/history"
    params = {'date': cg_date, 'localization': 'false'}
    headers = {}
    if COINGECKO_API_KEY:
        headers['x-cg-demo-key'] = COINGECKO_API_KEY

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('market_data', {}).get('current_price', {}).get('usd')
    except Exception as e:
        log.warning(f"Historical price fetch failed for {coin} on {date_str}: {e}")

    return None


def evaluate_prediction(prediction: str, actual_change_pct: float) -> dict:
    """
    Evaluate if a prediction was correct.

    Returns:
        dict with 'correct' (0/1), 'accuracy_type' (direction/range/partial), 'notes'
    """
    expected = PREDICTION_RANGES.get(prediction)
    if not expected:
        return {'correct': 0, 'accuracy_type': 'unknown', 'notes': f'Unknown prediction: {prediction}'}

    direction = expected['direction']
    range_min = expected['min']
    range_max = expected['max']

    # Check if actual falls within predicted range (strict match)
    in_range = range_min <= actual_change_pct <= range_max

    # Check direction match (lenient)
    if direction == 1:
        direction_match = actual_change_pct > 0
    elif direction == -1:
        direction_match = actual_change_pct < 0
    else:  # NEUTRAL
        direction_match = abs(actual_change_pct) < 2.0  # Strict: matches v3 training dead zone (±2%)

    if in_range:
        return {
            'correct': 1,
            'accuracy_type': 'range_match',
            'notes': f'Predicted {prediction} ({range_min:+.0f}% to {range_max:+.0f}%), actual {actual_change_pct:+.1f}% — RANGE HIT'
        }
    elif direction_match:
        return {
            'correct': 1,
            'accuracy_type': 'direction_match',
            'notes': f'Predicted {prediction}, actual {actual_change_pct:+.1f}% — DIRECTION HIT'
        }
    else:
        return {
            'correct': 0,
            'accuracy_type': 'miss',
            'notes': f'Predicted {prediction} ({range_min:+.0f}% to {range_max:+.0f}%), actual {actual_change_pct:+.1f}% — MISS'
        }


def check_matured_predictions() -> list:
    """
    Find predictions that have matured (target_date <= today)
    and haven't been evaluated yet. Fetch actual prices and score them.
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Find unscored matured predictions
    unscored = conn.execute(
        "SELECT id, coin, prediction_date, target_date, signal_score, prediction, "
        "actual_price_at_prediction "
        "FROM predictions "
        "WHERE target_date <= ? AND actual_price_at_target IS NULL",
        (today,)
    ).fetchall()

    if not unscored:
        log.info("No matured predictions to evaluate.")
        conn.close()
        return []

    log.info(f"Found {len(unscored)} matured predictions to evaluate")

    # Collect unique coins and target dates
    coins_needed = list(set(r[1] for r in unscored))
    dates_needed = list(set(r[3] for r in unscored))

    # Try fetching prices for the target dates
    # For today's date, use current prices; for past dates, use historical
    price_cache = {}

    for date_str in dates_needed:
        if date_str == today:
            # Use current prices
            current = fetch_current_prices(coins_needed)
            for coin, price in current.items():
                price_cache[(coin, date_str)] = price
        else:
            # Use historical prices (one by one, CoinGecko rate limits)
            for coin in coins_needed:
                import time
                key = (coin, date_str)
                if key not in price_cache:
                    price = fetch_historical_price(coin, date_str)
                    if price:
                        price_cache[key] = price
                    time.sleep(0.5)  # Rate limit

    # Evaluate each prediction
    results = []
    for row in unscored:
        pred_id, coin, pred_date, target_date, score, prediction, price_at_pred = row

        actual_price = price_cache.get((coin, target_date))

        if actual_price is None:
            log.warning(f"  {coin}: No price data for {target_date}, skipping")
            continue

        if price_at_pred is None or price_at_pred <= 0:
            log.warning(f"  {coin}: No prediction price stored, skipping")
            continue

        # Calculate actual change
        actual_change_pct = ((actual_price - price_at_pred) / price_at_pred) * 100

        # Evaluate
        evaluation = evaluate_prediction(prediction, actual_change_pct)

        # Update database
        conn.execute(
            "UPDATE predictions SET "
            "actual_price_at_target = ?, actual_change_pct = ?, "
            "correct = ?, notes = ? "
            "WHERE id = ?",
            (actual_price, actual_change_pct, evaluation['correct'],
             evaluation['notes'], pred_id)
        )

        result = {
            'id': pred_id,
            'coin': coin,
            'prediction_date': pred_date,
            'target_date': target_date,
            'prediction': prediction,
            'signal_score': score,
            'price_at_prediction': price_at_pred,
            'price_at_target': actual_price,
            'actual_change_pct': actual_change_pct,
            'correct': evaluation['correct'],
            'accuracy_type': evaluation['accuracy_type'],
            'notes': evaluation['notes'],
        }
        results.append(result)

        symbol = 'HIT' if evaluation['correct'] else 'MISS'
        log.info(f"  {coin}: {prediction} → {actual_change_pct:+.1f}% — {symbol}")

    conn.commit()
    conn.close()

    hits = sum(1 for r in results if r['correct'])
    log.info(f"\nEvaluated {len(results)} predictions: {hits} hits, {len(results) - hits} misses")
    if results:
        log.info(f"Win rate: {hits/len(results)*100:.0f}%")

    return results


# ════════════════════════════════════════════════════════════════
# Level 5: Auto-evaluation using local price data (no API calls)
# ════════════════════════════════════════════════════════════════

def evaluate_from_local_db() -> list:
    """Evaluate matured predictions using local price data (no API calls).

    Looks up prices from the local `prices` table in market.db.
    This is the preferred evaluation method — no CoinGecko dependency.

    Returns:
        list of dicts with evaluation results (one per scored prediction)
    """
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Find unscored matured predictions
        unscored = conn.execute(
            "SELECT id, coin, prediction_date, target_date, signal_score, prediction, "
            "actual_price_at_prediction, video_type "
            "FROM predictions "
            "WHERE target_date <= ? AND actual_price_at_target IS NULL",
            (today,)
        ).fetchall()

        if not unscored:
            log.info("evaluate_from_local_db: No matured predictions to evaluate")
            conn.close()
            return []

        log.info(f"evaluate_from_local_db: Found {len(unscored)} matured predictions to evaluate")

        results = []
        for row in unscored:
            try:
                pred_id, coin, pred_date, target_date, score, prediction, price_at_pred, video_type = row

                # Backfill price_at_prediction from prices table if missing/zero
                if price_at_pred is None or price_at_pred <= 0:
                    backfill_row = conn.execute(
                        "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                        "AND date(timestamp, 'unixepoch') = ? "
                        "ORDER BY timestamp DESC LIMIT 1",
                        (coin, pred_date)
                    ).fetchone()
                    if backfill_row and backfill_row[0] and backfill_row[0] > 0:
                        price_at_pred = backfill_row[0]
                        conn.execute(
                            "UPDATE predictions SET actual_price_at_prediction = ? WHERE id = ?",
                            (price_at_pred, pred_id)
                        )
                        log.info(f"  {coin}: Backfilled price_at_prediction={price_at_pred} for id={pred_id}")
                    else:
                        log.debug(f"  {coin}: No valid price_at_prediction and cannot backfill, skipping (id={pred_id})")
                        continue

                # Try to get price from local DB for the exact target_date (1d candle)
                price_row = conn.execute(
                    "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                    "AND date(timestamp, 'unixepoch') = ? "
                    "ORDER BY timestamp DESC LIMIT 1",
                    (coin, target_date)
                ).fetchone()

                # Fallback 1: try +/- 1 day (weekends, missing candles)
                if price_row is None:
                    price_row = conn.execute(
                        "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                        "AND date(timestamp, 'unixepoch') BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
                        "ORDER BY ABS(julianday(date(timestamp, 'unixepoch')) - julianday(?)) "
                        "LIMIT 1",
                        (coin, target_date, target_date, target_date)
                    ).fetchone()

                # Fallback 2: try latest available price from 1h candles if target is recent
                if price_row is None:
                    price_row = conn.execute(
                        "SELECT close, timestamp FROM prices WHERE coin = ? "
                        "AND timeframe IN ('1h', '1d') "
                        "ORDER BY timestamp DESC LIMIT 1",
                        (coin,)
                    ).fetchone()
                    if price_row:
                        # Verify the price is within 48h of the target date
                        from datetime import date as date_cls
                        target_dt = datetime.strptime(target_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        price_dt = datetime.fromtimestamp(price_row[1], tz=timezone.utc)
                        if abs((price_dt - target_dt).total_seconds()) > 48 * 3600:
                            price_row = None  # Too far from target date
                        else:
                            price_row = (price_row[0],)  # Keep only close price

                if price_row is None:
                    log.debug(f"  {coin}: No local price for {target_date}, skipping (id={pred_id})")
                    continue

                actual_price = price_row[0]
                if actual_price is None or actual_price <= 0:
                    continue

                actual_change_pct = ((actual_price - price_at_pred) / price_at_pred) * 100

                evaluation = evaluate_prediction(prediction, actual_change_pct)

                # Update database
                conn.execute(
                    "UPDATE predictions SET "
                    "actual_price_at_target = ?, actual_change_pct = ?, "
                    "correct = ?, notes = ? "
                    "WHERE id = ?",
                    (actual_price, round(actual_change_pct, 4), evaluation['correct'],
                     evaluation['notes'], pred_id)
                )

                result = {
                    'id': pred_id,
                    'coin': coin,
                    'prediction_date': pred_date,
                    'target_date': target_date,
                    'prediction': prediction,
                    'signal_score': score,
                    'price_at_prediction': price_at_pred,
                    'price_at_target': actual_price,
                    'actual_change_pct': round(actual_change_pct, 4),
                    'correct': evaluation['correct'],
                    'accuracy_type': evaluation['accuracy_type'],
                    'notes': evaluation['notes'],
                    'video_type': video_type,
                }
                results.append(result)

                symbol = 'HIT' if evaluation['correct'] else 'MISS'
                log.info(f"  {coin}: {prediction} -> {actual_change_pct:+.1f}% -- {symbol}")

            except Exception as e:
                log.warning(f"  Error evaluating prediction id={row[0]}: {e}")
                continue

        conn.commit()
        conn.close()

        hits = sum(1 for r in results if r['correct'])
        log.info(f"evaluate_from_local_db: Evaluated {len(results)} predictions: "
                 f"{hits} hits, {len(results) - hits} misses")
        if results:
            log.info(f"Win rate: {hits / len(results) * 100:.0f}%")

        return results

    except Exception as e:
        log.error(f"evaluate_from_local_db failed: {e}")
        return []


def update_rolling_accuracy():
    """Compute and store rolling accuracy (7d and 30d) per coin.

    Creates the `accuracy_rolling` table if it doesn't exist.
    Queries evaluated predictions for each coin over last 7d and 30d windows.
    Inserts/updates one row per coin per date.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Create table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accuracy_rolling (
                coin TEXT NOT NULL,
                date TEXT NOT NULL,
                accuracy_7d REAL,
                accuracy_30d REAL,
                buy_accuracy_30d REAL,
                sell_accuracy_30d REAL,
                n_evaluated_7d INTEGER DEFAULT 0,
                n_evaluated_30d INTEGER DEFAULT 0,
                PRIMARY KEY (coin, date)
            )
        """)

        # Date boundaries
        date_7d = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
        date_30d = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

        # Get all coins that have evaluated predictions
        coins = conn.execute(
            "SELECT DISTINCT coin FROM predictions WHERE correct IS NOT NULL"
        ).fetchall()

        if not coins:
            log.info("update_rolling_accuracy: No evaluated predictions yet")
            conn.close()
            return

        updated = 0
        for (coin,) in coins:
            try:
                # 7-day accuracy
                rows_7d = conn.execute(
                    "SELECT correct FROM predictions "
                    "WHERE coin = ? AND correct IS NOT NULL AND target_date >= ?",
                    (coin, date_7d)
                ).fetchall()

                n_7d = len(rows_7d)
                acc_7d = sum(r[0] for r in rows_7d) / n_7d if n_7d > 0 else None

                # 30-day accuracy (overall)
                rows_30d = conn.execute(
                    "SELECT correct FROM predictions "
                    "WHERE coin = ? AND correct IS NOT NULL AND target_date >= ?",
                    (coin, date_30d)
                ).fetchall()

                n_30d = len(rows_30d)
                acc_30d = sum(r[0] for r in rows_30d) / n_30d if n_30d > 0 else None

                # 30-day BUY accuracy (BUY + STRONG BUY)
                buy_rows = conn.execute(
                    "SELECT correct FROM predictions "
                    "WHERE coin = ? AND correct IS NOT NULL AND target_date >= ? "
                    "AND prediction IN ('BUY', 'STRONG BUY')",
                    (coin, date_30d)
                ).fetchall()

                n_buy = len(buy_rows)
                buy_acc = sum(r[0] for r in buy_rows) / n_buy if n_buy > 0 else None

                # 30-day SELL accuracy (SELL + STRONG SELL)
                sell_rows = conn.execute(
                    "SELECT correct FROM predictions "
                    "WHERE coin = ? AND correct IS NOT NULL AND target_date >= ? "
                    "AND prediction IN ('SELL', 'STRONG SELL')",
                    (coin, date_30d)
                ).fetchall()

                n_sell = len(sell_rows)
                sell_acc = sum(r[0] for r in sell_rows) / n_sell if n_sell > 0 else None

                # Upsert
                conn.execute(
                    "INSERT INTO accuracy_rolling "
                    "(coin, date, accuracy_7d, accuracy_30d, buy_accuracy_30d, "
                    "sell_accuracy_30d, n_evaluated_7d, n_evaluated_30d) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(coin, date) DO UPDATE SET "
                    "accuracy_7d = excluded.accuracy_7d, "
                    "accuracy_30d = excluded.accuracy_30d, "
                    "buy_accuracy_30d = excluded.buy_accuracy_30d, "
                    "sell_accuracy_30d = excluded.sell_accuracy_30d, "
                    "n_evaluated_7d = excluded.n_evaluated_7d, "
                    "n_evaluated_30d = excluded.n_evaluated_30d",
                    (coin, today, acc_7d, acc_30d, buy_acc, sell_acc, n_7d, n_30d)
                )
                updated += 1

            except Exception as e:
                log.warning(f"  Rolling accuracy error for {coin}: {e}")
                continue

        conn.commit()
        conn.close()
        log.info(f"update_rolling_accuracy: Updated {updated} coins for {today}")

    except Exception as e:
        log.error(f"update_rolling_accuracy failed: {e}")


def generate_scorecard_report() -> dict:
    """Generate a comprehensive scorecard report for all evaluated predictions."""
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    # All evaluated predictions
    all_preds = conn.execute(
        "SELECT coin, prediction_date, target_date, signal_score, prediction, "
        "actual_price_at_prediction, actual_price_at_target, actual_change_pct, "
        "correct, notes, video_type "
        "FROM predictions WHERE actual_change_pct IS NOT NULL "
        "ORDER BY prediction_date DESC"
    ).fetchall()

    if not all_preds:
        pending = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE actual_price_at_target IS NULL"
        ).fetchone()[0]
        conn.close()
        return {
            'total': 0, 'hits': 0, 'misses': 0, 'win_rate': 0,
            'pending': pending,
            'coin_stats': {}, 'signal_stats': {}, 'weekly': {},
            'recent_predictions': [],
            'message': 'No evaluated predictions yet.',
        }

    total = len(all_preds)
    hits = sum(1 for r in all_preds if r[8] == 1)
    misses = total - hits
    win_rate = hits / total * 100

    # Per-coin performance
    coin_stats = {}
    for r in all_preds:
        coin = r[0]
        if coin not in coin_stats:
            coin_stats[coin] = {'total': 0, 'hits': 0, 'changes': []}
        coin_stats[coin]['total'] += 1
        if r[8] == 1:
            coin_stats[coin]['hits'] += 1
        if r[7] is not None:
            coin_stats[coin]['changes'].append(r[7])

    # Per-signal performance
    signal_stats = {}
    for r in all_preds:
        signal = r[4]
        if signal not in signal_stats:
            signal_stats[signal] = {'total': 0, 'hits': 0}
        signal_stats[signal]['total'] += 1
        if r[8] == 1:
            signal_stats[signal]['hits'] += 1

    # Average actual change for hits vs misses
    hit_changes = [r[7] for r in all_preds if r[8] == 1 and r[7] is not None]
    miss_changes = [r[7] for r in all_preds if r[8] == 0 and r[7] is not None]

    # Weekly breakdown
    week_stats = {}
    for r in all_preds:
        week = r[1]  # prediction_date
        if week not in week_stats:
            week_stats[week] = {'total': 0, 'hits': 0}
        week_stats[week]['total'] += 1
        if r[8] == 1:
            week_stats[week]['hits'] += 1

    # Pending predictions (not yet matured)
    pending = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE actual_price_at_target IS NULL"
    ).fetchone()[0]

    conn.close()

    import numpy as np

    report = {
        'total': total,
        'hits': hits,
        'misses': misses,
        'win_rate': round(win_rate, 1),
        'pending': pending,
        'avg_hit_change': round(float(np.mean(hit_changes)), 2) if hit_changes else 0,
        'avg_miss_change': round(float(np.mean(miss_changes)), 2) if miss_changes else 0,
        'coin_stats': {
            coin: {
                'total': s['total'],
                'hits': s['hits'],
                'win_rate': round(s['hits'] / s['total'] * 100, 1) if s['total'] > 0 else 0,
                'avg_change': round(float(np.mean(s['changes'])), 2) if s['changes'] else 0,
            }
            for coin, s in sorted(coin_stats.items(), key=lambda x: x[1]['hits'] / max(x[1]['total'], 1), reverse=True)
        },
        'signal_stats': {
            signal: {
                'total': s['total'],
                'hits': s['hits'],
                'win_rate': round(s['hits'] / s['total'] * 100, 1) if s['total'] > 0 else 0,
            }
            for signal, s in signal_stats.items()
        },
        'weekly': {
            week: {
                'total': s['total'],
                'hits': s['hits'],
                'win_rate': round(s['hits'] / s['total'] * 100, 1) if s['total'] > 0 else 0,
            }
            for week, s in sorted(week_stats.items(), reverse=True)
        },
        'recent_predictions': [
            {
                'coin': r[0],
                'date': r[1],
                'prediction': r[4],
                'score': r[3],
                'price_then': r[5],
                'price_now': r[6],
                'change_pct': r[7],
                'correct': r[8],
                'notes': r[9],
            }
            for r in all_preds[:20]
        ],
    }

    # Persist scorecard to database
    _persist_scorecard(report)

    return report


def _persist_scorecard(report: dict):
    """Write scorecard summary rows to the `scorecard` table in market.db.

    Creates weekly and monthly period rows so other components (script_generator,
    daily_forecast) can read scorecard data without recomputing.
    """
    if not report or report.get('total', 0) == 0:
        return

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS scorecard (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT NOT NULL,
                period_type TEXT NOT NULL,
                total_predictions INTEGER,
                hits INTEGER,
                misses INTEGER,
                accuracy REAL,
                avg_return_hit REAL,
                avg_return_miss REAL,
                best_call TEXT,
                worst_call TEXT,
                computed_at TEXT
            )
        """)

        now = datetime.now(timezone.utc).isoformat()

        # Find the best and worst recent predictions
        best_call = None
        worst_call = None
        recent = report.get('recent_predictions', [])
        if recent:
            hits_only = [p for p in recent if p.get('correct')]
            misses_only = [p for p in recent if not p.get('correct')]
            if hits_only:
                best = max(hits_only, key=lambda p: abs(p.get('change_pct') or 0))
                best_call = f"{best['coin']} {best['prediction']} ({best.get('change_pct', 0):+.1f}%)"
            if misses_only:
                worst = max(misses_only, key=lambda p: abs(p.get('change_pct') or 0))
                worst_call = f"{worst['coin']} {worst['prediction']} ({worst.get('change_pct', 0):+.1f}%)"

        # Insert overall scorecard (period_type='all_time')
        conn.execute(
            "INSERT OR REPLACE INTO scorecard "
            "(period, period_type, total_predictions, hits, misses, accuracy, "
            "avg_return_hit, avg_return_miss, best_call, worst_call, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ('all_time', 'all_time', report['total'], report['hits'], report['misses'],
             report['win_rate'], report.get('avg_hit_change', 0),
             report.get('avg_miss_change', 0), best_call, worst_call, now)
        )

        # Insert weekly scorecard rows
        for week, stats in report.get('weekly', {}).items():
            conn.execute(
                "INSERT OR REPLACE INTO scorecard "
                "(period, period_type, total_predictions, hits, misses, accuracy, "
                "avg_return_hit, avg_return_miss, best_call, worst_call, computed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)",
                (week, 'weekly', stats['total'], stats['hits'],
                 stats['total'] - stats['hits'], stats['win_rate'], now)
            )

        conn.commit()
        conn.close()
        log.info(f"Scorecard persisted: {report['total']} total, "
                 f"{report['win_rate']:.1f}% accuracy, "
                 f"{len(report.get('weekly', {}))} weekly rows")

    except Exception as e:
        log.warning(f"Failed to persist scorecard: {e}")


def generate_scorecard_chart(report: dict, output_path: Path = None):
    """Generate a visual scorecard chart for the video."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    if output_path is None:
        output_path = Path('output/crypto_signal/charts/scorecard.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Theme colors
    BG = '#0D1117'
    GREEN = '#00FF88'
    RED = '#FF4444'
    BLUE = '#3B82F6'
    GOLD = '#FFD700'
    TEXT = '#E6EDF3'
    MUTED = '#8B949E'

    fig, axes = plt.subplots(2, 2, figsize=(16, 10), facecolor=BG)
    fig.suptitle('ALPHA SIGNAL — PREDICTION SCORECARD', fontsize=20,
                 fontweight='bold', color=GOLD, y=0.97)

    for ax in axes.flat:
        ax.set_facecolor(BG)
        ax.tick_params(colors=MUTED)
        for spine in ax.spines.values():
            spine.set_color('#30363D')

    # ─── 1. Win Rate Gauge (top-left) ───
    ax1 = axes[0, 0]
    win_rate = report.get('win_rate', 0)
    total = report.get('total', 0)
    hits = report.get('hits', 0)
    misses = report.get('misses', 0)

    if total > 0:
        sizes = [hits, misses]
        colors = [GREEN, RED]
        wedges, _ = ax1.pie(sizes, colors=colors, startangle=90,
                            wedgeprops={'width': 0.35, 'edgecolor': BG})
        ax1.text(0, 0, f'{win_rate:.0f}%', ha='center', va='center',
                 fontsize=36, fontweight='bold', color=TEXT)
        ax1.text(0, -0.15, f'{hits}W / {misses}L', ha='center', va='center',
                 fontsize=12, color=MUTED)
    else:
        ax1.text(0.5, 0.5, 'NO DATA\nYET', ha='center', va='center',
                 fontsize=18, color=MUTED, transform=ax1.transAxes)

    ax1.set_title('Overall Win Rate', color=TEXT, fontsize=14, fontweight='bold')

    # ─── 2. Per-Signal Performance (top-right) ───
    ax2 = axes[0, 1]
    signal_stats = report.get('signal_stats', {})
    if signal_stats:
        signals = list(signal_stats.keys())
        rates = [signal_stats[s]['win_rate'] for s in signals]
        totals = [signal_stats[s]['total'] for s in signals]
        bar_colors = [GREEN if r >= 50 else RED for r in rates]

        bars = ax2.barh(signals, rates, color=bar_colors, edgecolor=BG, height=0.6)
        for bar, total_count in zip(bars, totals):
            ax2.text(bar.get_width() + 2, bar.get_y() + bar.get_height() / 2,
                     f'({total_count})', va='center', color=MUTED, fontsize=10)

        ax2.set_xlim(0, 110)
        ax2.axvline(x=50, color=MUTED, linestyle='--', alpha=0.5)
        ax2.set_xlabel('Win Rate %', color=MUTED)
        ax2.tick_params(axis='y', colors=TEXT)
    else:
        ax2.text(0.5, 0.5, 'NO DATA', ha='center', va='center',
                 fontsize=18, color=MUTED, transform=ax2.transAxes)

    ax2.set_title('By Signal Type', color=TEXT, fontsize=14, fontweight='bold')

    # ─── 3. Per-Coin Performance (bottom-left) ───
    ax3 = axes[1, 0]
    coin_stats = report.get('coin_stats', {})
    if coin_stats:
        # Top 10 coins by volume
        top_coins = list(coin_stats.keys())[:10]
        coin_rates = [coin_stats[c]['win_rate'] for c in top_coins]
        coin_totals = [coin_stats[c]['total'] for c in top_coins]
        bar_colors = [GREEN if r >= 50 else RED for r in coin_rates]

        bars = ax3.bar(top_coins, coin_rates, color=bar_colors, edgecolor=BG, width=0.6)
        for bar, total_count in zip(bars, coin_totals):
            ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                     f'{total_count}', ha='center', color=MUTED, fontsize=9)

        ax3.set_ylim(0, 110)
        ax3.axhline(y=50, color=MUTED, linestyle='--', alpha=0.5)
        ax3.set_ylabel('Win Rate %', color=MUTED)
        ax3.tick_params(axis='x', rotation=45, colors=TEXT)
    else:
        ax3.text(0.5, 0.5, 'NO DATA', ha='center', va='center',
                 fontsize=18, color=MUTED, transform=ax3.transAxes)

    ax3.set_title('By Coin', color=TEXT, fontsize=14, fontweight='bold')

    # ─── 4. Recent Predictions Table (bottom-right) ───
    ax4 = axes[1, 1]
    ax4.axis('off')
    recent = report.get('recent_predictions', [])[:8]

    if recent:
        headers = ['Coin', 'Call', 'Change', 'Result']
        table_data = []
        cell_colors = []

        for r in recent:
            change = f"{r['change_pct']:+.1f}%" if r.get('change_pct') is not None else 'N/A'
            result = 'HIT' if r.get('correct') else 'MISS'
            table_data.append([r['coin'], r['prediction'], change, result])

            row_color = '#0D2818' if r.get('correct') else '#2D0D0D'
            cell_colors.append([row_color] * 4)

        table = ax4.table(cellText=table_data, colLabels=headers,
                          cellColours=cell_colors,
                          colColours=[BG] * 4,
                          loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1.0, 1.8)

        for key, cell in table.get_celld().items():
            cell.set_edgecolor('#30363D')
            if key[0] == 0:  # Header
                cell.set_text_props(color=GOLD, fontweight='bold')
            else:
                row_data = table_data[key[0] - 1] if key[0] - 1 < len(table_data) else None
                if row_data:
                    result_col = 3
                    if key[1] == result_col:
                        cell.set_text_props(color=GREEN if row_data[result_col] == 'HIT' else RED,
                                            fontweight='bold')
                    else:
                        cell.set_text_props(color=TEXT)
    else:
        ax4.text(0.5, 0.5, 'NO EVALUATED\nPREDICTIONS', ha='center', va='center',
                 fontsize=14, color=MUTED)

    ax4.set_title('Recent Predictions', color=TEXT, fontsize=14, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(str(output_path), dpi=150, bbox_inches='tight',
                facecolor=BG, edgecolor='none')
    plt.close()

    log.info(f"Scorecard chart saved: {output_path}")
    return output_path


def print_report(report: dict):
    """Print a human-readable scorecard report."""
    print("\n" + "=" * 70)
    print("  ALPHA SIGNAL — PREDICTION SCORECARD")
    print("=" * 70)

    print(f"\n  Overall: {report['hits']}W / {report['misses']}L "
          f"({report['win_rate']:.1f}% win rate)")
    print(f"  Total evaluated: {report['total']} | Pending: {report['pending']}")

    if report.get('avg_hit_change'):
        print(f"  Avg change on hits: {report['avg_hit_change']:+.2f}%")
        print(f"  Avg change on misses: {report['avg_miss_change']:+.2f}%")

    # Signal breakdown
    signal_stats = report.get('signal_stats', {})
    if signal_stats:
        print(f"\n  {'Signal':<14} {'W/L':>8} {'Win Rate':>10}")
        print("  " + "-" * 34)
        for signal, s in signal_stats.items():
            print(f"  {signal:<14} {s['hits']}/{s['total'] - s['hits']:>3}    {s['win_rate']:>6.1f}%")

    # Coin breakdown
    coin_stats = report.get('coin_stats', {})
    if coin_stats:
        print(f"\n  {'Coin':<8} {'W/L':>8} {'Win Rate':>10} {'Avg Chg':>10}")
        print("  " + "-" * 40)
        for coin, s in list(coin_stats.items())[:15]:
            print(f"  {coin:<8} {s['hits']}/{s['total'] - s['hits']:>3}    "
                  f"{s['win_rate']:>6.1f}%  {s['avg_change']:>+8.2f}%")

    # Weekly breakdown
    weekly = report.get('weekly', {})
    if weekly:
        print(f"\n  {'Week':<12} {'W/L':>8} {'Win Rate':>10}")
        print("  " + "-" * 32)
        for week, s in weekly.items():
            print(f"  {week:<12} {s['hits']}/{s['total'] - s['hits']:>3}    {s['win_rate']:>6.1f}%")

    # Recent predictions
    recent = report.get('recent_predictions', [])
    if recent:
        print(f"\n  {'Coin':<8} {'Date':<12} {'Call':<14} {'Score':>7} {'Change':>8} {'Result':>8}")
        print("  " + "-" * 62)
        for r in recent[:15]:
            change = f"{r['change_pct']:+.1f}%" if r.get('change_pct') is not None else 'N/A'
            result = 'HIT' if r.get('correct') else 'MISS'
            print(f"  {r['coin']:<8} {r['date']:<12} {r['prediction']:<14} "
                  f"{r['score']:>+7.3f} {change:>8} {result:>8}")

    print("\n" + "=" * 70)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX — Prediction Tracker')
    parser.add_argument('--report', action='store_true', help='Generate full scorecard report')
    parser.add_argument('--chart', action='store_true', help='Generate scorecard chart')
    parser.add_argument('--check-only', action='store_true',
                        help='Only check matured predictions (no report)')
    parser.add_argument('--local-eval', action='store_true',
                        help='Evaluate using local DB prices (no API calls)')

    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Prediction Tracker")
    log.info("=" * 60)

    # Evaluate matured predictions
    if args.local_eval:
        # Level 5: Use local price data (no CoinGecko API)
        results = evaluate_from_local_db()
        if results:
            update_rolling_accuracy()
    else:
        # Legacy: Use CoinGecko API
        results = check_matured_predictions()

    if args.check_only:
        sys.exit(0)

    # Generate report
    report = generate_scorecard_report()
    print_report(report)

    if args.report:
        # Save report as JSON
        report_path = Path('output/crypto_signal/scorecard_report.json')
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        log.info(f"Report saved: {report_path}")

    if args.chart:
        chart_path = generate_scorecard_chart(report)
        log.info(f"Chart saved: {chart_path}")
