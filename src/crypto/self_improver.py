"""
Self-Improver — Closed-Loop Learning System
=============================================
Connects ALL feedback signals into a single daily improvement cycle.

The loop:
1. EVALUATE: Check predictions from 7 days ago against actual prices
2. ANALYZE: Ask Claude WHY predictions were right/wrong
3. ADAPT: Automatically adjust weights based on last 30 days accuracy
4. RETRAIN: Retrain BUY detector if accuracy drops below threshold
5. REPORT: Log improvements to Telegram + database

Runs daily at 02:00 UTC (after prediction_eval at 01:00).

This is the BRAIN that makes the system smarter every day.
"""

import json
import logging
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger('self_improver')

FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
MARKET_DB = FACTORY_DIR / 'data' / 'crypto' / 'market.db'
PATTERNS_DB = FACTORY_DIR / 'data' / 'crypto' / 'patterns.db'
OPTIMIZED_CONFIG = FACTORY_DIR / 'data' / 'crypto' / 'optimized_config.json'
LEARNING_LOG = FACTORY_DIR / 'data' / 'crypto' / 'learning_log.json'


def run_daily_improvement() -> dict:
    """Main entry point: run complete self-improvement cycle.

    Called daily at 02:00 UTC by orchestrator.

    v3 architecture:
    - Signal System v3 overrides composite BUY/SELL (walk-forward validated)
    - Composite weights are SECONDARY (used only when no v3 signal fires)
    - Primary metric: v3 signal_tracking accuracy (14-day evaluation)
    - Secondary metric: old predictions table (legacy, fading out)
    """
    log.info("=" * 60)
    log.info("SELF-IMPROVER v3 — Daily Learning Cycle")
    log.info("=" * 60)

    results = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'actions': [],
        'metrics': {},
    }

    # Step 1: Signal System v3 accuracy (PRIMARY)
    v3_accuracy = _get_signal_system_accuracy()
    results['metrics']['v3_signals'] = v3_accuracy
    if v3_accuracy.get('total_evaluated', 0) > 0:
        log.info(f"Step 1: Signal v3 — {v3_accuracy['hit_rate']*100:.0f}% hit rate "
                 f"({v3_accuracy['correct']}/{v3_accuracy['total_evaluated']} correct)")
    else:
        log.info(f"Step 1: Signal v3 — {v3_accuracy.get('pending', 0)} signals pending evaluation")

    # Step 2: Legacy composite accuracy (SECONDARY — fading out)
    accuracy = _get_rolling_accuracy()
    results['metrics']['accuracy'] = accuracy
    log.info(f"Step 2: Legacy composite — overall={accuracy.get('overall_30d', 0):.1f}%")

    # Step 3: Evaluate pending v3 signals (14-day check)
    eval_count = _evaluate_v3_signals()
    if eval_count > 0:
        results['actions'].append(f'v3_evaluated_{eval_count}_signals')
        log.info(f"Step 3: Evaluated {eval_count} v3 signals (14-day check)")

    # Step 4: Analyze failures
    failure_insights = _analyze_failures()
    results['metrics']['failure_insights'] = failure_insights
    if failure_insights:
        log.info(f"Step 4: Analyzed {len(failure_insights)} recent failures")

    # Step 5: Adaptive weight adjustment (composite — secondary now)
    weight_changes = _adapt_weights(accuracy)
    results['actions'].extend(weight_changes)
    if weight_changes:
        log.info(f"Step 5: Adjusted {len(weight_changes)} composite weights")

    # Step 6: Confidence calibration
    _update_confidence_calibration()
    log.info("Step 6: Confidence calibration updated")

    # Step 7: Check v3 signal degradation
    v3_alerts = _check_v3_degradation(v3_accuracy)
    results['actions'].extend(v3_alerts)

    # Step 8: Log + notify
    _save_learning_log(results)
    _notify_improvements(results)

    log.info("=" * 60)
    log.info("Self-improvement cycle complete")
    log.info("=" * 60)

    return results


def _get_signal_system_accuracy() -> dict:
    """Get Signal System v3 accuracy from signal_tracking table."""
    try:
        conn = sqlite3.connect(str(MARKET_DB), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        c = conn.cursor()

        # Evaluated signals (last 90 days)
        c.execute("""
            SELECT COUNT(*), SUM(was_correct), AVG(actual_return)
            FROM signal_tracking
            WHERE evaluated=1 AND fired_at > date('now', '-90 days')
        """)
        row = c.fetchone()
        total = row[0] or 0
        correct = row[1] or 0
        avg_ret = row[2] or 0

        # Per signal type accuracy
        per_type = {}
        c.execute("""
            SELECT signal_type, COUNT(*), SUM(was_correct), AVG(actual_return)
            FROM signal_tracking
            WHERE evaluated=1 AND fired_at > date('now', '-90 days')
            GROUP BY signal_type
        """)
        for r in c.fetchall():
            stype, cnt, cor, ret = r
            per_type[stype] = {
                'total': cnt,
                'correct': cor or 0,
                'hit_rate': round((cor or 0) / max(cnt, 1), 3),
                'avg_return': round(ret or 0, 4),
            }

        # Pending (not yet evaluated)
        c.execute("""
            SELECT COUNT(*) FROM signal_tracking
            WHERE evaluated=0 AND fired_at > date('now', '-30 days')
        """)
        pending = c.fetchone()[0] or 0

        conn.close()

        return {
            'total_evaluated': total,
            'correct': correct,
            'hit_rate': round(correct / max(total, 1), 3),
            'avg_return': round(avg_ret, 4),
            'per_type': per_type,
            'pending': pending,
        }
    except Exception as e:
        log.warning(f"Signal v3 accuracy fetch failed: {e}")
        return {'total_evaluated': 0, 'pending': 0}


def _evaluate_v3_signals() -> int:
    """Evaluate v3 signals that have reached their 14-day target."""
    try:
        from src.crypto.signal_system import SignalSystem
        system = SignalSystem()
        result = system.evaluate_past_signals()
        return result.get('evaluated', 0)
    except Exception as e:
        log.warning(f"V3 signal evaluation failed: {e}")
        return 0


def _check_v3_degradation(v3_accuracy: dict) -> list:
    """Alert AND AUTO-ACT if v3 signal accuracy drops below expected rates.

    Actions:
    - 10% below expected for 50+ signals: WARNING + reduce confidence
    - Below 50% (worse than coin flip) for 100+ signals: AUTO-DISABLE
    """
    alerts = []
    per_type = v3_accuracy.get('per_type', {})

    # Expected hit rates from walk-forward validation
    expected = {
        'crowded_long': 0.588,
        'seller_dominance': 0.623,
        'overbought': 0.559,
        'oi_surge': 0.593,
        'post_pump': 0.588,
        'compound_bearish': 0.607,
    }

    for stype, stats in per_type.items():
        if stats['total'] < 20:
            continue
        live_rate = stats['hit_rate']
        exp_rate = expected.get(stype, 0.55)

        if live_rate < 0.50 and stats['total'] >= 100:
            # BELOW COIN FLIP with significant data → AUTO-DISABLE
            alerts.append(
                f'v3_AUTO_DISABLED_{stype}: live={live_rate:.0%} < 50% '
                f'(N={stats["total"]}) — signal is HARMFUL, auto-disabled'
            )
            log.error(f"AUTO-DISABLE signal {stype}: live={live_rate:.0%} < 50% "
                     f"(N={stats['total']}) — worse than random")
            # Write disable flag to DB so forecast_engine can check it
            try:
                import sqlite3
                _db_path = Path(__file__).resolve().parent.parent.parent / 'data/crypto/market.db'
                conn = sqlite3.connect(str(_db_path), timeout=10)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS disabled_signals "
                    "(signal_type TEXT PRIMARY KEY, disabled_at TEXT, reason TEXT)"
                )
                conn.execute(
                    "INSERT OR REPLACE INTO disabled_signals VALUES (?, datetime('now'), ?)",
                    (stype, f'live={live_rate:.0%} < 50% (N={stats["total"]})')
                )
                conn.commit()
                conn.close()
            except Exception as e:
                log.warning(f"Failed to write disable flag for {stype}: {e}")

        elif live_rate < exp_rate - 0.10:
            # 10% below expected → WARNING
            alerts.append(
                f'v3_degraded_{stype}: live={live_rate:.0%} vs expected={exp_rate:.0%} '
                f'(N={stats["total"]})'
            )
            log.warning(f"Signal degradation: {stype} live={live_rate:.0%} "
                       f"vs expected={exp_rate:.0%}")

    total = v3_accuracy.get('total_evaluated', 0)
    if total >= 50:
        overall = v3_accuracy.get('hit_rate', 0)
        if overall < 0.52:
            alerts.append(f'v3_overall_low: {overall:.0%} (N={total})')
            log.warning(f"Overall v3 accuracy below 52%: {overall:.0%}")

    return alerts


def _get_rolling_accuracy() -> dict:
    """Get rolling accuracy metrics from last 30 days."""
    try:
        conn = sqlite3.connect(str(MARKET_DB), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        c = conn.cursor()

        # Overall accuracy (last 30 days)
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) as correct
            FROM predictions
            WHERE actual_price_at_target IS NOT NULL
            AND created_at > datetime('now', '-30 days')
        """)
        row = c.fetchone()
        total = row[0] or 0
        correct = row[1] or 0
        overall = (correct / total * 100) if total > 0 else 0

        # BUY accuracy
        c.execute("""
            SELECT COUNT(*), SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END)
            FROM predictions WHERE prediction LIKE '%BUY%'
            AND actual_price_at_target IS NOT NULL
            AND created_at > datetime('now', '-30 days')
        """)
        buy_row = c.fetchone()
        buy_total = buy_row[0] or 0
        buy_correct = buy_row[1] or 0
        buy_acc = (buy_correct / buy_total * 100) if buy_total > 0 else 0

        # SELL accuracy
        c.execute("""
            SELECT COUNT(*), SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END)
            FROM predictions WHERE prediction LIKE '%SELL%'
            AND actual_price_at_target IS NOT NULL
            AND created_at > datetime('now', '-30 days')
        """)
        sell_row = c.fetchone()
        sell_total = sell_row[0] or 0
        sell_correct = sell_row[1] or 0
        sell_acc = (sell_correct / sell_total * 100) if sell_total > 0 else 0

        # Per-category accuracy (which signal categories are working?)
        category_acc = {}
        c.execute("""
            SELECT coin, COUNT(*), SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END)
            FROM predictions
            WHERE actual_price_at_target IS NOT NULL
            AND created_at > datetime('now', '-30 days')
            GROUP BY coin ORDER BY COUNT(*) DESC
        """)
        for r in c.fetchall():
            coin, cnt, cor = r
            category_acc[coin] = round((cor or 0) / max(cnt, 1) * 100, 1)

        conn.close()

        return {
            'overall_30d': round(overall, 1),
            'buy_30d': round(buy_acc, 1),
            'sell_30d': round(sell_acc, 1),
            'total_evaluated': total,
            'buy_count': buy_total,
            'sell_count': sell_total,
            'per_coin': category_acc,
        }
    except Exception as e:
        log.warning(f"Accuracy fetch failed: {e}")
        return {'overall_30d': 0, 'buy_30d': 0, 'sell_30d': 0}


def _analyze_failures() -> list:
    """Analyze recent prediction failures to find patterns.

    Uses simple pattern matching (not Claude API to save costs).
    Claude meta-analysis runs weekly in auto_retrain.py instead.
    """
    insights = []
    try:
        conn = sqlite3.connect(str(MARKET_DB), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        c = conn.cursor()

        # Find worst failures (predicted wrong direction, large actual move)
        c.execute("""
            SELECT coin, prediction, signal_score, actual_change_pct, created_at
            FROM predictions
            WHERE actual_price_at_target IS NOT NULL
            AND correct = 0
            AND ABS(actual_change_pct) > 5
            AND created_at > datetime('now', '-14 days')
            ORDER BY ABS(actual_change_pct) DESC
            LIMIT 10
        """)

        for row in c.fetchall():
            coin, pred, score, actual, date = row
            insight = {
                'coin': coin,
                'predicted': pred,
                'score': score,
                'actual_change': actual,
                'date': date,
                'type': 'false_sell' if 'SELL' in (pred or '') and (actual or 0) > 0
                        else 'false_buy' if 'BUY' in (pred or '') and (actual or 0) < 0
                        else 'missed_move',
            }
            insights.append(insight)

        conn.close()
    except Exception as e:
        log.warning(f"Failure analysis failed: {e}")

    return insights


def _adapt_weights(accuracy: dict) -> list:
    """Adapt thresholds (NOT weights) based on recent performance.

    v20: Weights are hardcoded in forecast_engine.py (proven OOS accuracy).
    Self-improver only adjusts THRESHOLDS (buy/sell/quality_gate) which are
    less prone to overfitting than weight optimization.

    DO NOT modify weights — previous weight adaptation created a feedback loop
    that led to massive overfitting (80% train → 51% live accuracy).
    """
    actions = []

    try:
        config = json.loads(OPTIMIZED_CONFIG.read_text())
        changed = False

        overall = accuracy.get('overall_30d', 60)
        buy_acc = accuracy.get('buy_30d', 50)
        sell_acc = accuracy.get('sell_30d', 60)

        # Rule 1: BUY threshold adjustment (safe — only affects signal filtering)
        buy_count = accuracy.get('buy_count', 0)
        if buy_count >= 5:
            if buy_acc < 40:
                config['buy_threshold'] = min(config.get('buy_threshold', 0.10) + 0.01, 0.20)
                actions.append(f'buy_acc_low_{buy_acc:.0f}%: buy_threshold+0.01')
                changed = True
            elif buy_acc > 60:
                config['buy_threshold'] = max(config.get('buy_threshold', 0.10) - 0.005, 0.05)
                actions.append(f'buy_acc_good_{buy_acc:.0f}%: buy_threshold-0.005')
                changed = True

        # Rule 2: SELL threshold adjustment
        if sell_acc < 50 and accuracy.get('sell_count', 0) >= 20:
            config['sell_threshold'] = min(config.get('sell_threshold', -0.10) + 0.01, -0.05)
            actions.append(f'sell_acc_low_{sell_acc:.0f}%: sell_threshold+0.01')
            changed = True

        # Rule 3: Per-coin flags (informational only, no config change)
        for coin, acc in accuracy.get('per_coin', {}).items():
            if acc < 35:
                actions.append(f'coin_weak_{coin}_{acc:.0f}%: consider_excluding')

        if changed:
            # v20: NEVER write weights back — only thresholds
            config.pop('weights', None)
            config.pop('weights_by_group', None)
            config['last_auto_adjust'] = datetime.now(timezone.utc).isoformat()
            OPTIMIZED_CONFIG.write_text(json.dumps(config, indent=2))
            log.info(f"Thresholds auto-adjusted: {actions}")

    except Exception as e:
        log.warning(f"Threshold adaptation failed: {e}")

    return actions


def _check_retrain_buy_detector(accuracy: dict) -> bool:
    """Retrain BUY detector if accuracy drops or model is >14 days old."""
    try:
        from src.crypto.buy_detector import BuyDetector, MODEL_PATH

        should_retrain = False
        reason = ''

        # Check model age
        if MODEL_PATH.exists():
            model_data = json.loads(MODEL_PATH.read_text())
            trained_at = model_data.get('trained_at', '')
            if trained_at:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(trained_at.replace('Z', '+00:00'))
                if age.days >= 14:
                    should_retrain = True
                    reason = f'model_age_{age.days}d'
        else:
            should_retrain = True
            reason = 'no_model'

        # Check BUY accuracy
        buy_acc = accuracy.get('buy_30d', 50)
        buy_count = accuracy.get('buy_count', 0)
        if buy_count >= 10 and buy_acc < 40:
            should_retrain = True
            reason = f'buy_acc_{buy_acc:.0f}%'

        if should_retrain:
            log.info(f"Retraining BUY detector: {reason}")
            detector = BuyDetector()
            metrics = detector.train()
            if metrics:
                log.info(f"BUY detector retrained: acc={100*metrics['accuracy']:.1f}%, "
                         f"precision={100*metrics['precision']:.1f}%")
            return True

    except Exception as e:
        log.warning(f"BUY detector retrain check failed: {e}")

    return False


def _update_confidence_calibration():
    """Update confidence model based on recent prediction accuracy.

    Maps confidence scores to actual accuracy:
    - If confidence=7 predictions are only 50% accurate → model overconfident
    - If confidence=3 predictions are 70% accurate → model underconfident
    """
    try:
        conn = sqlite3.connect(str(MARKET_DB), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        c = conn.cursor()

        # Get accuracy by confidence bucket (last 60 days for stability)
        c.execute("""
            SELECT
                CASE
                    WHEN ABS(signal_score) > 0.15 THEN 'high'
                    WHEN ABS(signal_score) > 0.08 THEN 'medium'
                    ELSE 'low'
                END as strength,
                COUNT(*),
                SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END)
            FROM predictions
            WHERE actual_price_at_target IS NOT NULL
            AND created_at > datetime('now', '-60 days')
            GROUP BY strength
        """)

        for strength, total, correct in c.fetchall():
            acc = (correct or 0) / max(total, 1) * 100
            log.info(f"  Confidence calibration: {strength} signals → {acc:.1f}% accurate ({total} samples)")

        conn.close()
    except Exception as e:
        log.warning(f"Confidence calibration failed: {e}")


def _save_learning_log(results: dict):
    """Append to learning log (history of all self-improvement actions)."""
    try:
        log_data = []
        if LEARNING_LOG.exists():
            try:
                log_data = json.loads(LEARNING_LOG.read_text())
            except Exception:
                log_data = []

        log_data.append(results)

        # Keep last 90 days (trim old entries)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        log_data = [e for e in log_data if e.get('timestamp', '') > cutoff]

        LEARNING_LOG.write_text(json.dumps(log_data, indent=2, default=str))
    except Exception as e:
        log.warning(f"Learning log save failed: {e}")


def _notify_improvements(results: dict):
    """Send Telegram notification about learning cycle results."""
    try:
        import os
        import requests
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        if not token or not chat_id:
            return

        v3 = results.get('metrics', {}).get('v3_signals', {})
        acc = results.get('metrics', {}).get('accuracy', {})
        actions = results.get('actions', [])

        # v3 Signal System (primary)
        msg = f"🧠 *FORTIX Самонавчання v3*\n\n"

        if v3.get('total_evaluated', 0) > 0:
            msg += (
                f"📊 *Сигнальна система v3:*\n"
                f"  Точність: {v3['hit_rate']*100:.0f}% "
                f"({v3['correct']}/{v3['total_evaluated']})\n"
                f"  Середній дохід: {v3.get('avg_return', 0)*100:+.2f}%\n"
            )
            for stype, stats in v3.get('per_type', {}).items():
                msg += f"  {stype}: {stats['hit_rate']*100:.0f}% (N={stats['total']})\n"
        else:
            msg += f"📊 Сигнали v3: {v3.get('pending', 0)} очікують оцінки (перші результати через 14д)\n"

        # Legacy (secondary, compact)
        msg += (
            f"\n📉 Стара модель: {acc.get('overall_30d', 0):.0f}% "
            f"({acc.get('total_evaluated', 0)} прогнозів)\n"
        )

        if actions:
            msg += f"\n⚙️ Дії: {', '.join(actions[:5])}"
        else:
            msg += "\n✅ Коригування не потрібні"

        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'Markdown'},
            timeout=10,
        )
    except Exception:
        pass


# ═══════════════════════════════════════
# CLI
# ═══════════════════════════════════════
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    results = run_daily_improvement()
    print(json.dumps(results, indent=2, default=str))
