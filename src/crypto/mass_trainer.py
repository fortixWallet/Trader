"""
FORTIX — Mass Trainer (50 periods)
==========================================
Runs time-travel training on 50 diverse periods across all available data.
Dense coverage for maximum pattern extraction and accuracy improvement.

Usage:
    python src/crypto/mass_trainer.py
"""

import sys
import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('mass_trainer')

MARKET_DB = Path('data/crypto/market.db')
PATTERNS_DB = Path('data/crypto/patterns.db')

# 50 training periods across all available data (Apr 2025 - Feb 2026)
# Dense coverage for maximum pattern extraction
MASS_PERIODS = [
    # === Phase 1: Early data (no MA200, limited signals) ===
    {'name': 'T01_apr_early',    'start': '2025-04-01', 'end': '2025-04-07', 'description': 'Early spring, minimal data'},
    {'name': 'T02_apr_10',       'start': '2025-04-10', 'end': '2025-04-16', 'description': 'Early April trend'},
    {'name': 'T03_apr_mid',      'start': '2025-04-15', 'end': '2025-04-21', 'description': 'Mid April'},
    {'name': 'T04_apr_late',     'start': '2025-04-22', 'end': '2025-04-28', 'description': 'Late April'},
    {'name': 'T05_may_early',    'start': '2025-05-01', 'end': '2025-05-07', 'description': 'May start'},
    {'name': 'T06_may_mid',      'start': '2025-05-10', 'end': '2025-05-16', 'description': 'Mid May'},
    {'name': 'T07_may_late',     'start': '2025-05-20', 'end': '2025-05-26', 'description': 'Late May'},
    {'name': 'T08_jun_early',    'start': '2025-06-01', 'end': '2025-06-07', 'description': 'June start'},
    {'name': 'T09_jun_10',       'start': '2025-06-10', 'end': '2025-06-16', 'description': 'Early-mid June'},
    {'name': 'T10_jun_mid',      'start': '2025-06-15', 'end': '2025-06-21', 'description': 'Mid June'},
    {'name': 'T11_jun_late',     'start': '2025-06-22', 'end': '2025-06-28', 'description': 'Late June'},
    {'name': 'T12_jul_early',    'start': '2025-07-01', 'end': '2025-07-07', 'description': 'July — summer rally?'},
    {'name': 'T13_jul_peak',     'start': '2025-07-08', 'end': '2025-07-14', 'description': 'Summer rally peak zone'},
    {'name': 'T14_jul_mid',      'start': '2025-07-15', 'end': '2025-07-21', 'description': 'Mid July'},
    {'name': 'T15_jul_late',     'start': '2025-07-22', 'end': '2025-07-28', 'description': 'Late July'},
    {'name': 'T16_aug_early',    'start': '2025-08-01', 'end': '2025-08-07', 'description': 'August start'},
    {'name': 'T17_aug_mid',      'start': '2025-08-10', 'end': '2025-08-16', 'description': 'Mid August'},

    # === Phase 2: MA200 appearing (~Sep 2025), richer signals ===
    {'name': 'T18_aug_late',     'start': '2025-08-20', 'end': '2025-08-26', 'description': 'Late August'},
    {'name': 'T19_aug_end',      'start': '2025-08-26', 'end': '2025-09-01', 'description': 'August → September transition'},
    {'name': 'T20_sep_early',    'start': '2025-09-01', 'end': '2025-09-07', 'description': 'September — MA200 appears'},
    {'name': 'T21_sep_10',       'start': '2025-09-08', 'end': '2025-09-14', 'description': 'Early-mid September'},
    {'name': 'T22_sep_mid',      'start': '2025-09-15', 'end': '2025-09-21', 'description': 'Mid September'},
    {'name': 'T23_sep_late',     'start': '2025-09-22', 'end': '2025-09-28', 'description': 'Autumn correction zone'},
    {'name': 'T24_oct_early',    'start': '2025-10-01', 'end': '2025-10-07', 'description': 'October start'},
    {'name': 'T25_oct_10',       'start': '2025-10-08', 'end': '2025-10-14', 'description': 'Early-mid October'},
    {'name': 'T26_oct_mid',      'start': '2025-10-15', 'end': '2025-10-21', 'description': 'Mid October'},
    {'name': 'T27_oct_late',     'start': '2025-10-25', 'end': '2025-10-31', 'description': 'Late October'},
    {'name': 'T28_nov_early',    'start': '2025-11-01', 'end': '2025-11-07', 'description': 'November — regime shift?'},
    {'name': 'T29_nov_shift',    'start': '2025-11-05', 'end': '2025-11-11', 'description': 'Regime shift zone'},
    {'name': 'T30_nov_mid',      'start': '2025-11-15', 'end': '2025-11-21', 'description': 'Mid November'},

    # === Phase 3: Full data, diverse conditions ===
    {'name': 'T31_nov_late',     'start': '2025-11-25', 'end': '2025-12-01', 'description': 'Late November'},
    {'name': 'T32_dec_early',    'start': '2025-12-01', 'end': '2025-12-07', 'description': 'December start'},
    {'name': 'T33_dec_05',       'start': '2025-12-05', 'end': '2025-12-11', 'description': 'Early December'},
    {'name': 'T34_dec_10',       'start': '2025-12-10', 'end': '2025-12-16', 'description': 'December volatility'},
    {'name': 'T35_dec_mid',      'start': '2025-12-15', 'end': '2025-12-21', 'description': 'Mid December'},
    {'name': 'T36_dec_20',       'start': '2025-12-19', 'end': '2025-12-25', 'description': 'Pre-Christmas'},
    {'name': 'T37_xmas',         'start': '2025-12-23', 'end': '2025-12-29', 'description': 'Christmas — low vol?'},
    {'name': 'T38_dec_end',      'start': '2025-12-27', 'end': '2026-01-02', 'description': 'Year-end transition'},
    {'name': 'T39_newyear',      'start': '2026-01-02', 'end': '2026-01-08', 'description': 'New Year start'},
    {'name': 'T40_jan_05',       'start': '2026-01-05', 'end': '2026-01-11', 'description': 'Early January'},
    {'name': 'T41_jan_mid',      'start': '2026-01-10', 'end': '2026-01-16', 'description': 'Mid January 2026'},
    {'name': 'T42_jan_fear',     'start': '2026-01-15', 'end': '2026-01-21', 'description': 'Peak fear zone'},
    {'name': 'T43_jan_18',       'start': '2026-01-18', 'end': '2026-01-24', 'description': 'Deepening fear'},
    {'name': 'T44_jan_late',     'start': '2026-01-22', 'end': '2026-01-28', 'description': 'Late January'},
    {'name': 'T45_jan_end',      'start': '2026-01-26', 'end': '2026-02-01', 'description': 'January → February'},
    {'name': 'T46_feb_early',    'start': '2026-02-01', 'end': '2026-02-07', 'description': 'February start'},
    {'name': 'T47_feb_03',       'start': '2026-02-03', 'end': '2026-02-09', 'description': 'Early February'},
    {'name': 'T48_feb_recent',   'start': '2026-02-05', 'end': '2026-02-11', 'description': 'Recent data window 1'},
    {'name': 'T49_feb_07',       'start': '2026-02-07', 'end': '2026-02-13', 'description': 'Recent data window 2'},
    {'name': 'T50_feb_latest',   'start': '2026-02-09', 'end': '2026-02-15', 'description': 'Most recent data'},
]


if __name__ == '__main__':
    from src.crypto.forecast_trainer import ForecastTrainer
    from src.crypto.error_analyzer import ErrorAnalyzer
    from src.crypto.pattern_learner import PatternLearner

    log.info("=" * 70)
    log.info("ALPHA SIGNAL — MASS TRAINING (50 PERIODS)")
    log.info(f"  {len(MASS_PERIODS)} periods, 25 coins, 7-day horizon")
    log.info("=" * 70)

    # ═══ STEP 1: Mass training ═══
    trainer = ForecastTrainer(db_path=str(MARKET_DB))
    all_results = trainer.run_all_periods(MASS_PERIODS)
    run_id = trainer.run_id

    # ═══ STEP 2: Error analysis ═══
    log.info("\n" + "=" * 70)
    log.info("STEP 2: ERROR ANALYSIS")
    log.info("=" * 70)

    analyzer = ErrorAnalyzer(str(PATTERNS_DB))
    report = analyzer.analyze_run(run_id)
    analyzer.print_report(report)
    analyzer.close()

    # ═══ STEP 3: Pattern learning ═══
    log.info("\n" + "=" * 70)
    log.info("STEP 3: PATTERN LEARNING")
    log.info("=" * 70)

    learner = PatternLearner(str(PATTERNS_DB))
    patterns = learner.extract_patterns(run_id)
    learner.print_patterns(patterns)
    learner.close()

    # ═══ STEP 4: Category recommendations ═══
    log.info("\n" + "=" * 70)
    log.info("STEP 4: WEIGHT RECOMMENDATIONS")
    log.info("=" * 70)

    cats = report.get('by_category', {})
    for cat, stats in sorted(cats.items(), key=lambda x: -x[1].get('solo_accuracy', 0)):
        acc = stats.get('solo_accuracy', 0) * 100
        n = stats.get('total', 0)
        if n > 0:
            if acc > 70:
                verdict = "★ STRONG — increase weight"
            elif acc > 55:
                verdict = "OK — keep weight"
            elif acc > 45:
                verdict = "WEAK — consider reducing"
            else:
                verdict = "✗ HARMFUL — reduce or remove"
            log.info(f"  {cat:12s}: {acc:5.1f}% (n={n:>4}) → {verdict}")

    regimes = report.get('by_regime', {})
    log.info("\n  Regime balance:")
    for regime, stats in regimes.items():
        log.info(f"    {regime:10s}: {stats.get('accuracy', 0)*100:.1f}% (n={stats.get('total', 0)})")

    # ═══ STEP 5: Weight optimization ═══
    log.info("\n" + "=" * 70)
    log.info("STEP 5: WEIGHT OPTIMIZATION (scipy)")
    log.info("=" * 70)

    try:
        from src.crypto.weight_optimizer import optimize_v2
        optimized = optimize_v2(run_id=run_id)
        if optimized:
            log.info(f"  Optimized accuracy: {optimized['training_accuracy']*100:.1f}%")
            log.info(f"  WF accuracy: {optimized.get('wf_accuracy', 0)*100:.1f}%")
            log.info(f"  Rank correlation: {optimized.get('rank_correlation', 0):.4f}")
            log.info(f"  Improvement: {optimized.get('improvement_pp', 0):+.1f}pp")
        else:
            log.warning("  Optimization returned no results")
    except Exception as e:
        log.warning(f"  Optimization failed: {e}")

    log.info(f"\n{'='*70}")
    log.info(f"COMPLETE — run_id: {run_id}")
    log.info(f"  Total predictions: {len(all_results)}")
    actionable = [r for r in all_results if r.get('direction_correct') is not None]
    correct = sum(1 for r in actionable if r['direction_correct'])
    log.info(f"  Actionable: {len(actionable)}, Correct: {correct}")
    log.info(f"  FINAL ACCURACY: {correct/len(actionable)*100:.1f}%")
    log.info(f"{'='*70}")
