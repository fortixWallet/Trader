"""
FORTIX — Error Analyzer
===============================
Deep error analysis for forecast training results.

Analyzes: per-category accuracy, per-regime performance, calibration,
signal conflicts, worst failures, and generates diagnostic reports.

Usage:
    from src.crypto.error_analyzer import ErrorAnalyzer
    analyzer = ErrorAnalyzer('data/crypto/patterns.db')
    report = analyzer.analyze_run(run_id)
    analyzer.print_report(report)
"""

import sqlite3
import logging
import json
import numpy as np
from pathlib import Path
from collections import defaultdict

log = logging.getLogger('error_analyzer')


class ErrorAnalyzer:
    """Analyze forecast errors to identify systematic biases and failures."""

    def __init__(self, db_path: str = 'data/crypto/patterns.db'):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def analyze_run(self, run_id: str) -> dict:
        """Full error analysis for a training run."""
        rows = self.conn.execute(
            "SELECT * FROM training_results WHERE run_id = ?", (run_id,)
        ).fetchall()

        if not rows:
            return {'error': f'No results found for run_id={run_id}'}

        results = [dict(r) for r in rows]
        actionable = [r for r in results if r['direction_correct'] is not None]

        report = {
            'run_id': run_id,
            'overall': self._overall_stats(results, actionable),
            'by_period': self._by_period(results),
            'by_coin': self._by_coin(actionable),
            'by_prediction': self._by_prediction(actionable),
            'by_category': self._by_category(actionable),
            'by_regime': self._by_regime(actionable),
            'direction_vs_magnitude': self._direction_vs_magnitude(actionable),
            'category_conflicts': self._category_conflicts(actionable),
            'worst_failures': self._worst_failures(actionable),
            'calibration': self._calibration(actionable),
            'feature_insights': self._feature_insights(actionable),
        }

        return report

    def _overall_stats(self, results: list, actionable: list) -> dict:
        correct = sum(r['direction_correct'] for r in actionable) if actionable else 0
        pred_correct = sum(r['prediction_correct'] for r in results if r['prediction_correct'] is not None)
        return {
            'total': len(results),
            'actionable': len(actionable),
            'neutral': len(results) - len(actionable),
            'correct_direction': correct,
            'direction_accuracy': correct / len(actionable) if actionable else 0,
            'prediction_correct': pred_correct,
            'prediction_accuracy': pred_correct / len(results) if results else 0,
            'avg_actual_change': np.mean([r['actual_change_pct'] for r in results]),
            'avg_confidence': np.mean([r['confidence'] for r in results]),
        }

    def _by_period(self, results: list) -> dict:
        periods = defaultdict(list)
        for r in results:
            periods[r['period_name']].append(r)

        out = {}
        for name, rs in periods.items():
            act = [r for r in rs if r['direction_correct'] is not None]
            correct = sum(r['direction_correct'] for r in act) if act else 0
            out[name] = {
                'total': len(rs),
                'actionable': len(act),
                'correct': correct,
                'accuracy': correct / len(act) if act else 0,
                'avg_change': np.mean([r['actual_change_pct'] for r in rs]),
                'avg_confidence': np.mean([r['confidence'] for r in rs]),
            }
        return out

    def _by_coin(self, actionable: list) -> dict:
        coins = defaultdict(list)
        for r in actionable:
            coins[r['coin']].append(r)

        out = {}
        for coin, rs in sorted(coins.items()):
            correct = sum(r['direction_correct'] for r in rs)
            out[coin] = {
                'total': len(rs),
                'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
                'avg_change': np.mean([r['actual_change_pct'] for r in rs]),
                'avg_score': np.mean([r['composite_score'] for r in rs]),
            }
        return out

    def _by_prediction(self, actionable: list) -> dict:
        preds = defaultdict(list)
        for r in actionable:
            preds[r['prediction']].append(r)

        out = {}
        for pred, rs in preds.items():
            correct = sum(r['direction_correct'] for r in rs)
            out[pred] = {
                'total': len(rs),
                'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
                'avg_actual_change': np.mean([r['actual_change_pct'] for r in rs]),
                'avg_confidence': np.mean([r['confidence'] for r in rs]),
            }
        return out

    def _by_category(self, actionable: list) -> dict:
        """Per-category solo accuracy: if we only used this category."""
        cats = ['technical', 'sentiment', 'onchain', 'macro', 'news', 'historical']
        out = {}

        for cat in cats:
            score_key = f'{cat}_score'
            has_key = f'{cat}_has_data'

            correct = 0
            total = 0
            misleading = 0  # Category pointed wrong way
            contribution_correct = []
            contribution_wrong = []

            for r in actionable:
                if not r.get(has_key):
                    continue
                cat_score = r.get(score_key, 0)
                if abs(cat_score) < 0.01:
                    continue

                total += 1
                actual_dir = 1 if r['actual_change_pct'] > 0 else -1
                cat_dir = 1 if cat_score > 0 else -1

                if cat_dir == actual_dir:
                    correct += 1
                    contribution_correct.append(abs(cat_score))
                else:
                    misleading += 1
                    contribution_wrong.append(abs(cat_score))

            out[cat] = {
                'total': total,
                'correct': correct,
                'solo_accuracy': correct / total if total > 0 else 0,
                'misleading_rate': misleading / total if total > 0 else 0,
                'avg_magnitude_correct': np.mean(contribution_correct) if contribution_correct else 0,
                'avg_magnitude_wrong': np.mean(contribution_wrong) if contribution_wrong else 0,
            }

        return out

    def _by_regime(self, actionable: list) -> dict:
        """Accuracy by market regime (based on BTC 7d change)."""
        regimes = {'bull': [], 'bear': [], 'sideways': []}

        for r in actionable:
            btc_change = r.get('btc_change_7d')
            if btc_change is None:
                regimes['sideways'].append(r)
            elif btc_change > 3:
                regimes['bull'].append(r)
            elif btc_change < -3:
                regimes['bear'].append(r)
            else:
                regimes['sideways'].append(r)

        out = {}
        for regime, rs in regimes.items():
            correct = sum(r['direction_correct'] for r in rs) if rs else 0
            out[regime] = {
                'total': len(rs),
                'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
                'avg_actual_change': np.mean([r['actual_change_pct'] for r in rs]) if rs else 0,
            }
        return out

    def _direction_vs_magnitude(self, actionable: list) -> dict:
        """Separate direction accuracy from magnitude accuracy."""
        correct_dir = [r for r in actionable if r['direction_correct']]
        wrong_dir = [r for r in actionable if not r['direction_correct']]

        return {
            'direction_accuracy': len(correct_dir) / len(actionable) if actionable else 0,
            'avg_magnitude_when_correct': np.mean(
                [abs(r['actual_change_pct']) for r in correct_dir]
            ) if correct_dir else 0,
            'avg_magnitude_when_wrong': np.mean(
                [abs(r['actual_change_pct']) for r in wrong_dir]
            ) if wrong_dir else 0,
            'avg_predicted_score_correct': np.mean(
                [abs(r['composite_score']) for r in correct_dir]
            ) if correct_dir else 0,
            'avg_predicted_score_wrong': np.mean(
                [abs(r['composite_score']) for r in wrong_dir]
            ) if wrong_dir else 0,
        }

    def _category_conflicts(self, actionable: list) -> dict:
        """When categories disagree, who's right?"""
        cats = ['technical', 'sentiment', 'onchain', 'macro', 'news', 'historical']

        agreement_stats = {'high_agreement': [], 'low_agreement': []}

        for r in actionable:
            active_scores = []
            for cat in cats:
                if r.get(f'{cat}_has_data') and abs(r.get(f'{cat}_score', 0)) > 0.01:
                    active_scores.append(r[f'{cat}_score'])

            if len(active_scores) < 2:
                continue

            n_pos = sum(1 for s in active_scores if s > 0)
            n_neg = sum(1 for s in active_scores if s < 0)
            agreement = max(n_pos, n_neg) / len(active_scores)

            if agreement >= 0.7:
                agreement_stats['high_agreement'].append(r)
            else:
                agreement_stats['low_agreement'].append(r)

        out = {}
        for level, rs in agreement_stats.items():
            correct = sum(r['direction_correct'] for r in rs) if rs else 0
            out[level] = {
                'total': len(rs),
                'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
            }
        return out

    def _worst_failures(self, actionable: list, top_n: int = 10) -> list:
        """Top N biggest direction misses."""
        failures = [r for r in actionable if not r['direction_correct']]
        failures.sort(key=lambda r: abs(r['actual_change_pct']), reverse=True)

        return [
            {
                'coin': r['coin'],
                'date': r['forecast_date'],
                'period': r['period_name'],
                'prediction': r['prediction'],
                'composite_score': r['composite_score'],
                'actual_change': r['actual_change_pct'],
                'confidence': r['confidence'],
                'rsi': r.get('rsi_at_forecast'),
                'fg': r.get('fg_at_forecast'),
                'ma200': r.get('ma200_trend'),
            }
            for r in failures[:top_n]
        ]

    def _calibration(self, actionable: list) -> dict:
        """Confidence calibration: do higher confidence predictions perform better?"""
        bins = [(1, 3), (3, 5), (5, 7), (7, 10)]
        out = {}

        for low, high in bins:
            bin_rs = [r for r in actionable if low <= r['confidence'] < high]
            correct = sum(r['direction_correct'] for r in bin_rs) if bin_rs else 0
            out[f'conf_{low}_{high}'] = {
                'total': len(bin_rs),
                'correct': correct,
                'accuracy': correct / len(bin_rs) if bin_rs else 0,
            }
        return out

    def _feature_insights(self, actionable: list) -> dict:
        """Insights from context features: what conditions predict success?"""
        insights = {}

        # RSI bins
        rsi_bins = [(0, 30, 'oversold'), (30, 50, 'mild_bear'), (50, 70, 'mild_bull'), (70, 100, 'overbought')]
        for low, high, label in rsi_bins:
            rs = [r for r in actionable if r.get('rsi_at_forecast') and low <= r['rsi_at_forecast'] < high]
            correct = sum(r['direction_correct'] for r in rs) if rs else 0
            insights[f'rsi_{label}'] = {
                'total': len(rs), 'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
            }

        # F&G bins
        fg_bins = [(0, 20, 'extreme_fear'), (20, 40, 'fear'), (40, 60, 'neutral'), (60, 80, 'greed'), (80, 100, 'extreme_greed')]
        for low, high, label in fg_bins:
            rs = [r for r in actionable if r.get('fg_at_forecast') and low <= r['fg_at_forecast'] < high]
            correct = sum(r['direction_correct'] for r in rs) if rs else 0
            insights[f'fg_{label}'] = {
                'total': len(rs), 'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
            }

        # MA200 trend
        for trend in ['above', 'below']:
            rs = [r for r in actionable if r.get('ma200_trend') == trend]
            correct = sum(r['direction_correct'] for r in rs) if rs else 0
            insights[f'ma200_{trend}'] = {
                'total': len(rs), 'correct': correct,
                'accuracy': correct / len(rs) if rs else 0,
            }

        return insights

    def print_report(self, report: dict):
        """Print formatted analysis report."""
        if 'error' in report:
            log.info(f"  Error: {report['error']}")
            return

        o = report['overall']
        log.info("\n" + "=" * 60)
        log.info("ERROR ANALYSIS REPORT")
        log.info("=" * 60)
        log.info(f"  Run ID: {report['run_id']}")
        log.info(f"  Total predictions:    {o['total']}")
        log.info(f"  Actionable:           {o['actionable']}")
        log.info(f"  Direction accuracy:   {o['direction_accuracy']*100:.1f}%")
        log.info(f"  Prediction accuracy:  {o['prediction_accuracy']*100:.1f}%")
        log.info(f"  Avg confidence:       {o['avg_confidence']:.1f}")

        # By period
        log.info(f"\n  BY PERIOD:")
        for name, stats in report['by_period'].items():
            log.info(f"    {name:<20s}: {stats['accuracy']*100:.1f}% "
                     f"({stats['correct']}/{stats['actionable']}), "
                     f"avg change {stats['avg_change']:+.1f}%")

        # By category
        log.info(f"\n  CATEGORY SOLO ACCURACY:")
        for cat, stats in report['by_category'].items():
            log.info(f"    {cat:12s}: {stats['solo_accuracy']*100:.1f}% "
                     f"({stats['correct']}/{stats['total']}), "
                     f"misleading {stats['misleading_rate']*100:.0f}%")

        # By regime
        log.info(f"\n  BY REGIME:")
        for regime, stats in report['by_regime'].items():
            log.info(f"    {regime:10s}: {stats['accuracy']*100:.1f}% "
                     f"({stats['correct']}/{stats['total']})")

        # Calibration
        log.info(f"\n  CONFIDENCE CALIBRATION:")
        for level, stats in report['calibration'].items():
            log.info(f"    {level}: {stats['accuracy']*100:.1f}% "
                     f"({stats['correct']}/{stats['total']})")

        # Category conflicts
        log.info(f"\n  SIGNAL AGREEMENT:")
        for level, stats in report['category_conflicts'].items():
            log.info(f"    {level:17s}: {stats['accuracy']*100:.1f}% "
                     f"({stats['correct']}/{stats['total']})")

        # Feature insights
        log.info(f"\n  FEATURE INSIGHTS:")
        for feature, stats in report['feature_insights'].items():
            if stats['total'] > 0:
                log.info(f"    {feature:20s}: {stats['accuracy']*100:.1f}% "
                         f"({stats['correct']}/{stats['total']})")

        # Direction vs magnitude
        dvm = report['direction_vs_magnitude']
        log.info(f"\n  DIRECTION vs MAGNITUDE:")
        log.info(f"    Avg magnitude when correct: {dvm['avg_magnitude_when_correct']:.1f}%")
        log.info(f"    Avg magnitude when wrong:   {dvm['avg_magnitude_when_wrong']:.1f}%")

        # Worst failures
        log.info(f"\n  TOP WORST FAILURES:")
        for f in report['worst_failures'][:5]:
            log.info(f"    {f['coin']:6s} {f['date']}: "
                     f"predicted {f['prediction']:12s}, "
                     f"actual {f['actual_change']:+.1f}%")

    def close(self):
        self.conn.close()
