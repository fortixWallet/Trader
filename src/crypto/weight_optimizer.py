"""
FORTIX — Weight Optimizer v2
===================================
Scipy-based optimization of category weights, thresholds, and quality gate.
Uses stored training results from patterns.db (no re-running backtester needed).

Key design choices:
- Only optimizes weights for categories that have data (others fixed at 0)
- Objective: maximize n_correct / (n_correct + 2*n_wrong) — penalizes errors more
- Cross-validation within objective to prevent overfitting
- Regime dampening NOT replicated (training has btc_7d, engine uses btc_30d)

Usage:
    python src/crypto/weight_optimizer.py                    # optimize latest run
    python src/crypto/weight_optimizer.py <run_id>           # optimize specific run
"""

import sys
import json
import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('weight_optimizer')

PATTERNS_DB = Path('data/crypto/patterns.db')
CONFIG_OUT = Path('data/crypto/optimized_config.json')

CATEGORY_NAMES = [
    'technical', 'sentiment', 'onchain', 'macro', 'news',
    'news_claude', 'historical', 'learned', 'meta_analyst',
    'coinglass', 'cryptoquant',
]

# Current v19 config for comparison
CURRENT_WEIGHTS = {
    'technical': 0.23, 'sentiment': 0.02, 'onchain': 0.00,
    'macro': 0.05, 'news': 0.02, 'news_claude': 0.17,
    'historical': 0.16, 'learned': 0.09, 'meta_analyst': 0.12,
    'coinglass': 0.08, 'cryptoquant': 0.06,
}


def load_training_data(run_id=None):
    """Load training results into numpy arrays for fast vectorized computation."""
    conn = sqlite3.connect(str(PATTERNS_DB))
    conn.row_factory = sqlite3.Row

    if run_id is None:
        row = conn.execute(
            "SELECT run_id, COUNT(*) as n FROM training_results "
            "GROUP BY run_id ORDER BY n DESC LIMIT 1"
        ).fetchone()
        if not row:
            log.error("No training results found")
            conn.close()
            return None, None
        run_id = row['run_id']

    rows = conn.execute(
        "SELECT * FROM training_results WHERE run_id = ?", (run_id,)
    ).fetchall()
    conn.close()

    n = len(rows)
    n_cats = len(CATEGORY_NAMES)

    # Pre-allocate numpy arrays
    scores_matrix = np.zeros((n, n_cats), dtype=np.float64)
    has_data_matrix = np.zeros((n, n_cats), dtype=np.float64)
    actual_changes = np.zeros(n, dtype=np.float64)
    volatilities = np.zeros(n, dtype=np.float64)
    btc_changes = np.zeros(n, dtype=np.float64)
    ma200_below = np.zeros(n, dtype=bool)
    period_names = []
    forecast_dates = []
    coins_list = []

    for i, r in enumerate(rows):
        details = json.loads(r['signal_details_json']) if r['signal_details_json'] else {}
        for j, cat in enumerate(CATEGORY_NAMES):
            cat_info = details.get(cat, {})
            scores_matrix[i, j] = cat_info.get('score', 0.0)
            has_data_matrix[i, j] = 1.0 if cat_info.get('has_data', False) else 0.0

        actual_changes[i] = r['actual_change_pct'] if r['actual_change_pct'] is not None else 0.0
        volatilities[i] = r['volatility_at_forecast'] or 0
        btc_changes[i] = r['btc_change_7d'] or 0
        ma200_below[i] = r['ma200_trend'] == 'below'
        period_names.append(r['period_name'] or '')
        forecast_dates.append(r['forecast_date'] or '')
        coins_list.append(r['coin'] or '')

    # Agreement ratio (pre-compute — doesn't change with weights)
    agreement_ratios = np.zeros(n, dtype=np.float64)
    for i in range(n):
        data_scores = scores_matrix[i][has_data_matrix[i] > 0]
        if len(data_scores) > 0:
            n_pos = np.sum(data_scores > 0)
            n_neg = np.sum(data_scores < 0)
            agreement_ratios[i] = max(n_pos, n_neg) / len(data_scores)

    # Pre-compute date indices for walk-forward CV and ranking
    dates_array = np.array(forecast_dates)
    unique_dates = np.sort(np.unique(dates_array))
    date_idx = np.searchsorted(unique_dates, dates_array)

    data = {
        'scores': scores_matrix,
        'has_data': has_data_matrix,
        'actual': actual_changes,
        'volatility': volatilities,
        'btc_7d': btc_changes,
        'ma200_below': ma200_below,
        'agreement': agreement_ratios,
        'periods': np.array(period_names),
        'dates': dates_array,
        'coins': np.array(coins_list),
        'unique_dates': unique_dates,
        'date_idx': date_idx,
        'n': n,
    }

    log.info(f"Loaded {n} results from run {run_id}")
    return data, run_id


def detect_active_categories(data):
    """Find categories that have data in >10% of rows."""
    n = data['n']
    has_data_sums = data['has_data'].sum(axis=0)

    active = []
    inactive = []
    for j, cat in enumerate(CATEGORY_NAMES):
        if has_data_sums[j] > n * 0.1:
            active.append(cat)
        else:
            inactive.append(cat)

    log.info(f"  Active categories ({len(active)}): {', '.join(active)}")
    if inactive:
        log.info(f"  Inactive (fixed at 0): {', '.join(inactive)}")

    return active, inactive


def recompute_fast(data, weight_vec, active_indices, buy_threshold=0.1,
                   sell_threshold=-0.1, quality_gate=0.0, ma200_buy_mult=1.5):
    """Vectorized prediction re-computation.

    weight_vec: numpy array of weights for active categories
    active_indices: column indices in scores_matrix for active categories
    """
    scores = data['scores']
    has_data = data['has_data']
    actual = data['actual']
    vol = data['volatility']
    ma200_below = data['ma200_below']
    agreement = data['agreement']
    n = data['n']

    # 1. Compute composite: weighted sum of active category scores
    # Only count scores where has_data = 1
    active_scores = scores[:, active_indices]
    active_has = has_data[:, active_indices]

    # Weighted scores (0 where no data)
    weighted = active_scores * active_has * weight_vec[np.newaxis, :]
    composite = weighted.sum(axis=1)

    # Normalize by active weight sum per row
    active_w = (active_has * weight_vec[np.newaxis, :]).sum(axis=1)
    need_norm = (active_w > 0) & (active_w < 0.95)
    composite[need_norm] = composite[need_norm] / active_w[need_norm]
    composite = np.clip(composite, -1.0, 1.0)

    # 2. Dynamic buy/sell thresholds from volatility
    bt = np.full(n, buy_threshold)
    st = np.full(n, sell_threshold)
    high_vol = vol > 0.06
    mid_vol = (vol > 0.035) & ~high_vol
    bt[high_vol] = np.maximum(bt[high_vol], 0.18)
    st[high_vol] = np.minimum(st[high_vol], -0.18)
    bt[mid_vol] = np.maximum(bt[mid_vol], 0.14)
    st[mid_vol] = np.minimum(st[mid_vol], -0.14)

    # MA200 filter
    bt[ma200_below] *= ma200_buy_mult

    # 3. Predictions (vectorized)
    is_strong_buy = composite > 0.4
    is_buy = ~is_strong_buy & (composite > bt)
    is_strong_sell = composite < -0.4
    is_sell = ~is_strong_sell & (composite < st)
    is_actionable = is_strong_buy | is_buy | is_sell | is_strong_sell

    # 4. Quality gate
    if quality_gate > 0:
        low_agreement = agreement < quality_gate
        is_actionable = is_actionable & ~low_agreement

    # 5. Evaluate
    predicted_up = is_strong_buy | is_buy
    actual_up = actual > 0

    correct_mask = is_actionable & (predicted_up == actual_up)
    wrong_mask = is_actionable & (predicted_up != actual_up)

    n_actionable = int(is_actionable.sum())
    n_correct = int(correct_mask.sum())
    n_wrong = int(wrong_mask.sum())
    accuracy = n_correct / n_actionable if n_actionable > 0 else 0.0

    # Per-regime stats
    btc_7d = data['btc_7d']
    regime_stats = {}
    for regime_name, mask in [
        ('bull', btc_7d > 5),
        ('bear', btc_7d < -5),
        ('ranging', (btc_7d >= -5) & (btc_7d <= 5)),
    ]:
        r_actionable = is_actionable & mask
        r_correct = correct_mask & mask
        r_total = int(r_actionable.sum())
        r_corr = int(r_correct.sum())
        if r_total > 0:
            regime_stats[regime_name] = {'correct': r_corr, 'total': r_total}

    n_buy = int((is_strong_buy | is_buy).sum())
    n_sell = int((is_strong_sell | is_sell).sum())

    # BUY-specific accuracy
    buy_mask = is_actionable & predicted_up
    buy_correct = int((buy_mask & actual_up).sum())
    n_buy_actionable = int(buy_mask.sum())
    buy_accuracy = buy_correct / n_buy_actionable if n_buy_actionable > 0 else 0.0

    return {
        'actionable': n_actionable,
        'correct': n_correct,
        'wrong': n_wrong,
        'accuracy': accuracy,
        'n_buy': n_buy_actionable,
        'n_sell': n_actionable - n_buy_actionable,
        'buy_accuracy': buy_accuracy,
        'regime_stats': regime_stats,
    }


def params_to_config(params, active_cats):
    """Convert flat parameter vector to named config.

    Only active categories get optimized weights. Inactive are 0.
    """
    n_active = len(active_cats)
    raw_weights = params[:n_active]
    w_sum = sum(raw_weights)
    if w_sum > 0:
        norm_weights = [w / w_sum for w in raw_weights]
    else:
        norm_weights = [1.0 / n_active] * n_active

    weights = {cat: 0.0 for cat in CATEGORY_NAMES}
    for i, cat in enumerate(active_cats):
        weights[cat] = norm_weights[i]

    return {
        'weights': weights,
        'buy_threshold': params[n_active],
        'sell_threshold': -abs(params[n_active + 1]),
        'quality_gate': params[n_active + 2],
        'ma200_buy_mult': params[n_active + 3],
    }


def objective(params, data, active_indices, n_active, min_actionable=50):
    """Objective: maximize profit-weighted accuracy.

    Score = n_correct - 2 * n_wrong (penalizes mistakes heavily)
    Penalty if too few actionable.

    params: flat array [n_active weights + buy_thr + sell_thr + quality_gate + ma200_mult]
    active_indices: numpy array of column indices for active categories
    """
    raw_w = params[:n_active]
    w_sum = raw_w.sum()
    weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active

    buy_thr = params[n_active]
    sell_thr = -abs(params[n_active + 1])
    quality_gate = params[n_active + 2]
    ma200_mult = params[n_active + 3]

    result = recompute_fast(
        data, weight_vec, active_indices,
        buy_threshold=buy_thr,
        sell_threshold=sell_thr,
        quality_gate=quality_gate,
        ma200_buy_mult=ma200_mult,
    )

    n_correct = result['correct']
    n_wrong = result['wrong']
    n_actionable = result['actionable']

    # Score: correct predictions earn +1, wrong cost -2
    score = n_correct - 2.0 * n_wrong

    # Penalty for too few actionable
    if n_actionable < min_actionable:
        penalty = 5.0 * (1.0 - n_actionable / max(min_actionable, 1))
        score -= penalty

    return -score  # minimize negative = maximize score


def cross_validate(data, params, active_indices, n_active, n_folds=5):
    """Leave-period-group-out cross-validation using numpy slicing."""
    periods_arr = data['periods']
    unique_periods = sorted(set(p for p in periods_arr if p))
    if len(unique_periods) < n_folds:
        n_folds = max(2, len(unique_periods))

    np.random.seed(42)
    shuffled = list(unique_periods)
    np.random.shuffle(shuffled)
    fold_size = len(shuffled) // n_folds
    folds = []
    for i in range(n_folds):
        start = i * fold_size
        end = start + fold_size if i < n_folds - 1 else len(shuffled)
        folds.append(set(shuffled[start:end]))

    # Parse params once
    raw_w = params[:n_active]
    w_sum = raw_w.sum()
    weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active
    buy_thr = params[n_active]
    sell_thr = -abs(params[n_active + 1])
    quality_gate = params[n_active + 2]
    ma200_mult = params[n_active + 3]

    total_correct = 0
    total_n = 0

    for test_periods in folds:
        # Build boolean mask for this fold's test data
        mask = np.array([p in test_periods for p in periods_arr])
        if not mask.any():
            continue

        # Slice numpy data for this fold
        fold_data = {
            'scores': data['scores'][mask],
            'has_data': data['has_data'][mask],
            'actual': data['actual'][mask],
            'volatility': data['volatility'][mask],
            'btc_7d': data['btc_7d'][mask],
            'ma200_below': data['ma200_below'][mask],
            'agreement': data['agreement'][mask],
            'periods': data['periods'][mask],
            'n': int(mask.sum()),
        }

        result = recompute_fast(
            fold_data, weight_vec, active_indices,
            buy_threshold=buy_thr, sell_threshold=sell_thr,
            quality_gate=quality_gate, ma200_buy_mult=ma200_mult,
        )

        if result['actionable'] > 0:
            total_correct += result['correct']
            total_n += result['actionable']

    return total_correct / total_n if total_n else 0.0, total_n


def optimize(run_id=None):
    """Main optimization routine."""
    from scipy.optimize import differential_evolution

    data, actual_run_id = load_training_data(run_id)
    if data is None:
        return None

    n_total = data['n']
    n_with_outcome = int((data['actual'] != 0).sum())
    log.info(f"  Total rows: {n_total}, with outcomes: {n_with_outcome}")

    # Detect which categories have data
    active_cats, inactive_cats = detect_active_categories(data)
    n_active = len(active_cats)
    active_indices = np.array([CATEGORY_NAMES.index(c) for c in active_cats])

    # === Baseline (v19 weights) ===
    baseline_w = np.array([CURRENT_WEIGHTS.get(c, 0.0) for c in active_cats])
    bw_sum = baseline_w.sum()
    if bw_sum > 0:
        baseline_w = baseline_w / bw_sum

    baseline = recompute_fast(
        data, baseline_w, active_indices,
        buy_threshold=0.1, sell_threshold=-0.1,
        quality_gate=0.0, ma200_buy_mult=1.5,
    )
    log.info(f"\n  BASELINE (v19 config):")
    log.info(f"    Accuracy: {baseline['accuracy']*100:.1f}% ({baseline['correct']}/{baseline['actionable']})")
    for regime, stats in sorted(baseline['regime_stats'].items()):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] else 0
        log.info(f"    {regime:12s}: {acc:.0f}% (n={stats['total']})")

    # === Optimization ===
    # Cap 'learned' at 0.30 to prevent circular leakage domination
    weight_bounds = []
    for cat in active_cats:
        if cat == 'learned':
            weight_bounds.append((0.0, 0.30))
        else:
            weight_bounds.append((0.0, 0.50))
    bounds = (
        weight_bounds +
        [(0.03, 0.20),               # buy_threshold
         (0.03, 0.20),               # sell_threshold (stored as positive, negated in objective)
         (0.0, 0.75),                # quality_gate
         (1.0, 3.0)]                 # ma200_buy_mult
    )

    min_actionable = max(20, int(baseline['actionable'] * 0.5))
    log.info(f"\n  Optimizing ({n_active} active categories, min_actionable={min_actionable})...")
    log.info(f"  This may take 30-120 seconds...")

    result = differential_evolution(
        objective,
        bounds=bounds,
        args=(data, active_indices, n_active, min_actionable),
        seed=42,
        maxiter=150,
        tol=1e-4,
        popsize=15,
        mutation=(0.5, 1.5),
        recombination=0.8,
        disp=False,
        workers=1,
    )

    # Reconstruct optimized config
    opt_params = result.x
    raw_w = opt_params[:n_active]
    w_sum = raw_w.sum()
    opt_weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active
    opt_buy = opt_params[n_active]
    opt_sell = -abs(opt_params[n_active + 1])
    opt_qg = opt_params[n_active + 2]
    opt_ma200 = opt_params[n_active + 3]

    opt_result = recompute_fast(
        data, opt_weight_vec, active_indices,
        buy_threshold=opt_buy, sell_threshold=opt_sell,
        quality_gate=opt_qg, ma200_buy_mult=opt_ma200,
    )

    log.info(f"\n  OPTIMIZED:")
    log.info(f"    Accuracy: {opt_result['accuracy']*100:.1f}% ({opt_result['correct']}/{opt_result['actionable']})")
    for regime, stats in sorted(opt_result['regime_stats'].items()):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] else 0
        log.info(f"    {regime:12s}: {acc:.0f}% (n={stats['total']})")

    # Build named weights dict
    opt_weights_dict = {cat: 0.0 for cat in CATEGORY_NAMES}
    for i, cat in enumerate(active_cats):
        opt_weights_dict[cat] = float(opt_weight_vec[i])

    log.info(f"\n  Optimized weights:")
    for cat in CATEGORY_NAMES:
        old = CURRENT_WEIGHTS.get(cat, 0)
        new = opt_weights_dict[cat]
        delta = new - old
        marker = " \u2605" if abs(delta) > 0.02 else ""
        log.info(f"    {cat:15s}: {old:.3f} \u2192 {new:.3f} ({delta:+.3f}){marker}")

    log.info(f"\n  Optimized thresholds:")
    log.info(f"    buy_threshold:     0.10 \u2192 {opt_buy:.3f}")
    log.info(f"    sell_threshold:   -0.10 \u2192 {opt_sell:.3f}")
    log.info(f"    quality_gate:      0.00 \u2192 {opt_qg:.3f}")
    log.info(f"    ma200_buy_mult:    1.50 \u2192 {opt_ma200:.3f}")

    # === Cross-validation ===
    log.info(f"\n  Cross-validating...")
    cv_accuracy, cv_n = cross_validate(data, result.x, active_indices, n_active)
    log.info(f"    CV accuracy: {cv_accuracy*100:.1f}%")

    improvement = opt_result['accuracy'] - baseline['accuracy']
    log.info(f"\n  IMPROVEMENT: {baseline['accuracy']*100:.1f}% \u2192 {opt_result['accuracy']*100:.1f}% ({improvement*100:+.1f}pp)")
    log.info(f"  ACTIONABLE:  {baseline['actionable']} \u2192 {opt_result['actionable']}")

    # === Save config ===
    output = {
        'weights': {cat: round(w, 4) for cat, w in opt_weights_dict.items()},
        'buy_threshold': round(float(opt_buy), 4),
        'sell_threshold': round(float(opt_sell), 4),
        'quality_gate': round(float(opt_qg), 4),
        'ma200_buy_mult': round(float(opt_ma200), 4),
        'training_accuracy': round(opt_result['accuracy'], 4),
        'cv_accuracy': round(cv_accuracy, 4),
        'n_actionable': opt_result['actionable'],
        'baseline_accuracy': round(baseline['accuracy'], 4),
        'improvement_pp': round(improvement * 100, 1),
        'active_categories': active_cats,
        'optimized_at': datetime.now(timezone.utc).isoformat(),
        'run_id': actual_run_id,
    }

    CONFIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_OUT.write_text(json.dumps(output, indent=2))
    log.info(f"\n  Saved to {CONFIG_OUT}")

    return output


# ═══════════════════════════════════════════════════════════════
# COIN-GROUP OPTIMIZATION (Level 1 Phase A)
# Per-group weights: majors, l1_alts, defi, ai, meme
# Fallback: overall single-weight config (backward compatible)
# ═══════════════════════════════════════════════════════════════

COIN_GROUPS = {
    'majors': ['BTC', 'ETH'],
    'l1_alts': ['SOL', 'BNB', 'ADA', 'AVAX', 'DOT', 'XRP'],
    'defi': ['AAVE', 'UNI', 'MKR', 'CRV', 'LDO', 'LINK'],
    'ai': ['FET', 'RENDER', 'TAO'],
    'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}


def load_training_data_by_group(group, run_id=None):
    """Load training results filtered by coin group.

    Same as load_training_data() but with WHERE coin IN (...) filter.
    Returns (data_dict, run_id) or (None, None) if insufficient data.
    """
    coins = COIN_GROUPS.get(group, [])
    if not coins:
        return None, None

    conn = sqlite3.connect(str(PATTERNS_DB))
    conn.row_factory = sqlite3.Row

    if run_id is None:
        row = conn.execute(
            "SELECT run_id, COUNT(*) as n FROM training_results "
            "GROUP BY run_id ORDER BY n DESC LIMIT 1"
        ).fetchone()
        if not row:
            conn.close()
            return None, None
        run_id = row['run_id']

    placeholders = ','.join('?' * len(coins))
    rows = conn.execute(
        f"SELECT * FROM training_results WHERE run_id = ? AND coin IN ({placeholders})",
        [run_id] + coins
    ).fetchall()
    conn.close()

    if not rows:
        return None, None

    n = len(rows)
    n_cats = len(CATEGORY_NAMES)

    # Pre-allocate numpy arrays (same structure as load_training_data)
    scores_matrix = np.zeros((n, n_cats), dtype=np.float64)
    has_data_matrix = np.zeros((n, n_cats), dtype=np.float64)
    actual_changes = np.zeros(n, dtype=np.float64)
    volatilities = np.zeros(n, dtype=np.float64)
    btc_changes = np.zeros(n, dtype=np.float64)
    ma200_below = np.zeros(n, dtype=bool)
    period_names = []
    forecast_dates = []
    coins_list = []

    for i, r in enumerate(rows):
        details = json.loads(r['signal_details_json']) if r['signal_details_json'] else {}
        for j, cat in enumerate(CATEGORY_NAMES):
            cat_info = details.get(cat, {})
            scores_matrix[i, j] = cat_info.get('score', 0.0)
            has_data_matrix[i, j] = 1.0 if cat_info.get('has_data', False) else 0.0

        actual_changes[i] = r['actual_change_pct'] if r['actual_change_pct'] is not None else 0.0
        volatilities[i] = r['volatility_at_forecast'] or 0
        btc_changes[i] = r['btc_change_7d'] or 0
        ma200_below[i] = r['ma200_trend'] == 'below'
        period_names.append(r['period_name'] or '')
        forecast_dates.append(r['forecast_date'] or '')
        coins_list.append(r['coin'] or '')

    # Agreement ratio (pre-compute — doesn't change with weights)
    agreement_ratios = np.zeros(n, dtype=np.float64)
    for i in range(n):
        data_scores = scores_matrix[i][has_data_matrix[i] > 0]
        if len(data_scores) > 0:
            n_pos = np.sum(data_scores > 0)
            n_neg = np.sum(data_scores < 0)
            agreement_ratios[i] = max(n_pos, n_neg) / len(data_scores)

    # Pre-compute date indices for walk-forward CV and ranking
    dates_array = np.array(forecast_dates)
    unique_dates = np.sort(np.unique(dates_array))
    date_idx = np.searchsorted(unique_dates, dates_array)

    data = {
        'scores': scores_matrix,
        'has_data': has_data_matrix,
        'actual': actual_changes,
        'volatility': volatilities,
        'btc_7d': btc_changes,
        'ma200_below': ma200_below,
        'agreement': agreement_ratios,
        'periods': np.array(period_names),
        'dates': dates_array,
        'coins': np.array(coins_list),
        'unique_dates': unique_dates,
        'date_idx': date_idx,
        'n': n,
    }

    log.info(f"  Group '{group}': loaded {n} results from run {run_id}")
    return data, run_id


def optimize_group(group, run_id=None):
    """Run differential_evolution for a specific coin group.

    Returns dict with group-specific weights, thresholds, accuracy, or None if insufficient data.
    """
    from scipy.optimize import differential_evolution

    data, actual_run_id = load_training_data_by_group(group, run_id)
    if data is None or data['n'] < 100:
        log.warning(f"  Group '{group}': insufficient data ({data['n'] if data else 0} rows, need >=100)")
        return None

    log.info(f"\n  Optimizing group '{group}' ({data['n']} samples)...")

    active_cats, inactive_cats = detect_active_categories(data)
    n_active = len(active_cats)
    active_indices = np.array([CATEGORY_NAMES.index(c) for c in active_cats])

    # Same bounds and optimization as optimize()
    bounds = (
        [(0.0, 0.50)] * n_active +
        [(0.03, 0.20),    # buy_threshold
         (0.03, 0.20),    # sell_threshold
         (0.0, 0.75),     # quality_gate
         (1.0, 3.0)]      # ma200_buy_mult
    )

    min_actionable = max(10, int(data['n'] * 0.02))

    result = differential_evolution(
        objective, bounds=bounds,
        args=(data, active_indices, n_active, min_actionable),
        seed=42, maxiter=150, tol=1e-4, popsize=15,
        mutation=(0.5, 1.5), recombination=0.8, disp=False, workers=1,
    )

    # Extract weights
    raw_w = result.x[:n_active]
    w_sum = raw_w.sum()
    opt_weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active

    weights = {cat: 0.0 for cat in CATEGORY_NAMES}
    for i, cat in enumerate(active_cats):
        weights[cat] = float(opt_weight_vec[i])

    # Evaluate
    opt_buy = result.x[n_active]
    opt_sell = -abs(result.x[n_active + 1])
    opt_qg = result.x[n_active + 2]
    opt_ma200 = result.x[n_active + 3]

    opt_result = recompute_fast(
        data, opt_weight_vec, active_indices,
        buy_threshold=opt_buy,
        sell_threshold=opt_sell,
        quality_gate=opt_qg,
        ma200_buy_mult=opt_ma200,
    )

    log.info(f"    {group}: {opt_result['accuracy']*100:.1f}% "
             f"({opt_result['correct']}/{opt_result['actionable']})")

    # Cross-validate this group
    cv_accuracy, cv_n = cross_validate(data, result.x, active_indices, n_active)
    log.info(f"    {group} CV: {cv_accuracy*100:.1f}% (n={cv_n})")

    return {
        'weights': weights,
        'buy_threshold': float(opt_buy),
        'sell_threshold': float(opt_sell),
        'quality_gate': float(opt_qg),
        'ma200_buy_mult': float(opt_ma200),
        'accuracy': opt_result['accuracy'],
        'cv_accuracy': cv_accuracy,
        'n_actionable': opt_result['actionable'],
        'n_correct': opt_result['correct'],
        'group': group,
        'coins': COIN_GROUPS[group],
    }


def optimize_all_groups(run_id=None):
    """Optimize weights for each coin group and save grouped config.

    Also runs overall optimization as fallback. Saves everything to optimized_config.json
    in a backward-compatible format (overall weights at top level, group weights in 'weights_by_group').
    """
    log.info("=" * 60)
    log.info("COIN-GROUP WEIGHT OPTIMIZATION")
    log.info("=" * 60)

    group_results = {}
    for group in COIN_GROUPS:
        result = optimize_group(group, run_id)
        if result:
            group_results[group] = result

    if not group_results:
        log.error("No groups optimized successfully")
        return None

    # Summary
    log.info(f"\n  GROUP SUMMARY:")
    for group, r in sorted(group_results.items()):
        log.info(f"    {group:10s}: {r['accuracy']*100:.1f}% train, "
                 f"{r['cv_accuracy']*100:.1f}% CV "
                 f"({r['n_correct']}/{r['n_actionable']})")

    # Also run overall optimization as fallback
    log.info(f"\n  Running overall optimization (fallback)...")
    overall = optimize(run_id)

    # Build grouped config (backward compatible)
    config = {
        'weights_by_group': {
            group: {
                'weights': {k: round(v, 4) for k, v in r['weights'].items()},
                'buy_threshold': round(r['buy_threshold'], 4),
                'sell_threshold': round(r['sell_threshold'], 4),
                'quality_gate': round(r['quality_gate'], 4),
                'ma200_buy_mult': round(r['ma200_buy_mult'], 4),
                'accuracy': round(r['accuracy'], 4),
                'cv_accuracy': round(r['cv_accuracy'], 4),
                'n_actionable': r['n_actionable'],
                'coins': r['coins'],
            }
            for group, r in group_results.items()
        },
        # Keep overall as fallback (backward compatible)
        'weights': overall['weights'] if overall else {},
        'buy_threshold': overall.get('buy_threshold', 0.1) if overall else 0.1,
        'sell_threshold': overall.get('sell_threshold', -0.1) if overall else -0.1,
        'quality_gate': overall.get('quality_gate', 0.0) if overall else 0.0,
        'ma200_buy_mult': overall.get('ma200_buy_mult', 1.5) if overall else 1.5,
        'training_accuracy': overall.get('training_accuracy', 0.0) if overall else 0.0,
        'cv_accuracy': overall.get('cv_accuracy', 0.0) if overall else 0.0,
        'n_actionable': overall.get('n_actionable', 0) if overall else 0,
        'baseline_accuracy': overall.get('baseline_accuracy', 0.0) if overall else 0.0,
        'active_categories': overall.get('active_categories', []) if overall else [],
        'optimized_at': datetime.now(timezone.utc).isoformat(),
        'run_id': run_id or (overall.get('run_id') if overall else 'latest'),
    }

    CONFIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_OUT.write_text(json.dumps(config, indent=2))
    log.info(f"\nGrouped config saved to {CONFIG_OUT}")

    return config


# ═══════════════════════════════════════════════════════════════
# LEVEL 3: RANKING OBJECTIVE + WALK-FORWARD CV
# Replaces n_correct-2*n_wrong with multi-component objective:
#   60% Spearman rank correlation (per-date coin ranking)
#   25% Signal accuracy (BUY/SELL correctness)
#   15% Calibration (higher |composite| → higher accuracy)
# Walk-forward CV replaces random period shuffle.
# ═══════════════════════════════════════════════════════════════


def spearman_rank_fast(x, y):
    """Fast Spearman rank correlation using numpy only.

    Returns correlation in [-1, 1] or NaN if insufficient data.
    Optimized for speed — called thousands of times during optimization.
    """
    n = len(x)
    if n < 3:
        return np.nan
    x_ranks = np.argsort(np.argsort(x)).astype(np.float64)
    y_ranks = np.argsort(np.argsort(y)).astype(np.float64)
    x_dev = x_ranks - x_ranks.mean()
    y_dev = y_ranks - y_ranks.mean()
    denom = np.sqrt((x_dev ** 2).sum() * (y_dev ** 2).sum())
    if denom < 1e-10:
        return np.nan
    return float((x_dev * y_dev).sum() / denom)


def compute_composite_scores(data, weight_vec, active_indices):
    """Compute raw composite scores (vectorized). Returns array of shape (n,).

    Extracted from recompute_fast() so ranking objective can access raw scores.
    """
    scores = data['scores']
    has_data = data['has_data']

    active_scores = scores[:, active_indices]
    active_has = has_data[:, active_indices]

    weighted = active_scores * active_has * weight_vec[np.newaxis, :]
    composite = weighted.sum(axis=1)

    active_w = (active_has * weight_vec[np.newaxis, :]).sum(axis=1)
    need_norm = (active_w > 0) & (active_w < 0.95)
    composite[need_norm] = composite[need_norm] / active_w[need_norm]
    composite = np.clip(composite, -1.0, 1.0)

    return composite


def objective_v2(params, data, active_indices, n_active, min_actionable=50):
    """Multi-component ranking objective.

    Score = 0.60 * rank_corr + 0.25 * signal_accuracy - 0.15 * calibration_error
    For small groups (<5 coins/date), falls back to accuracy-focused objective.
    """
    raw_w = params[:n_active]
    w_sum = raw_w.sum()
    weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active

    buy_thr = params[n_active]
    sell_thr = -abs(params[n_active + 1])
    quality_gate = params[n_active + 2]
    ma200_mult = params[n_active + 3]

    # Raw composite scores for ranking
    composite = compute_composite_scores(data, weight_vec, active_indices)
    actual = data['actual']

    # Component 1: Per-date Spearman rank correlation
    date_idx = data['date_idx']
    n_unique_dates = len(data['unique_dates'])
    rank_corrs = []
    for d in range(n_unique_dates):
        mask = date_idx == d
        n_d = mask.sum()
        if n_d >= 5:
            rc = spearman_rank_fast(composite[mask], actual[mask])
            if not np.isnan(rc):
                rank_corrs.append(rc)
    avg_rank_corr = np.mean(rank_corrs) if rank_corrs else 0.0

    # Component 2: Signal accuracy (BUY/SELL correctness)
    result = recompute_fast(
        data, weight_vec, active_indices,
        buy_threshold=buy_thr, sell_threshold=sell_thr,
        quality_gate=quality_gate, ma200_buy_mult=ma200_mult,
    )
    n_actionable = result['actionable']
    signal_acc = result['accuracy'] if n_actionable > 0 else 0.0

    # Penalty for too few actionable signals
    actionable_penalty = 0.0
    if n_actionable < min_actionable:
        actionable_penalty = 0.5 * (1.0 - n_actionable / max(min_actionable, 1))

    # Component 3: Calibration error
    # Higher |composite| should correlate with higher accuracy
    abs_comp = np.abs(composite)
    buckets = [(0.0, 0.05), (0.05, 0.15), (0.15, 1.01)]
    bucket_accs = []
    for lo, hi in buckets:
        bucket_mask = (abs_comp >= lo) & (abs_comp < hi)
        n_b = bucket_mask.sum()
        if n_b >= 20:
            sign_match = np.sign(composite[bucket_mask]) == np.sign(actual[bucket_mask])
            bucket_accs.append(float(sign_match.mean()))
        else:
            bucket_accs.append(None)

    cal_error = 0.0
    valid_accs = [(i, a) for i, a in enumerate(bucket_accs) if a is not None]
    if len(valid_accs) >= 2:
        for j in range(len(valid_accs) - 1):
            _, acc_lo = valid_accs[j]
            _, acc_hi = valid_accs[j + 1]
            if acc_lo > acc_hi:  # Miscalibrated: lower confidence but higher accuracy
                cal_error += (acc_lo - acc_hi)

    # Coverage penalty: continuous penalty for low actionable ratio
    # Target: at least 5% of predictions should be actionable
    actionable_ratio = n_actionable / max(data['n'], 1)
    if actionable_ratio < 0.05:
        actionable_penalty += 1.0 * (0.05 - actionable_ratio) / 0.05

    # BUY signal diversity reward (v21): stronger penalty for zero BUY signals
    # History: 0% BUY accuracy from weak signals. Fix: require quality BUY signals.
    n_buy = result.get('n_buy', 0)
    buy_acc = result.get('buy_accuracy', 0.0)
    buy_bonus = 0.0
    if n_buy >= 5 and buy_acc >= 0.55:
        # Reward: up to 0.5 for having accurate BUY signals
        buy_bonus = 0.5 * min(n_buy / max(n_actionable, 1), 0.3) * buy_acc
    elif n_buy >= 3 and buy_acc >= 0.50:
        # Small bonus for having some BUY signals with decent accuracy
        buy_bonus = 0.15 * buy_acc
    elif n_buy == 0 and n_actionable > 20:
        # Strong penalty: no BUY signals when plenty of SELL = unbalanced model
        buy_bonus = -0.5
    elif n_buy > 0 and buy_acc < 0.30:
        # Penalty for low-quality BUY signals (worse than random)
        buy_bonus = -0.3

    # Combine components — rank_corr scaled by 10x (typical range 0-0.1 vs accuracy 0.5-1.0)
    if len(rank_corrs) >= 10:
        score = (10.0 * avg_rank_corr + 0.5 * signal_acc
                 - 0.3 * cal_error - actionable_penalty + buy_bonus)
    else:
        # Not enough cross-coin dates for ranking — accuracy-focused
        score = 0.80 * signal_acc - 0.20 * cal_error - actionable_penalty

    return -score  # minimize negative = maximize


def walk_forward_cv(data, params, active_indices, n_active, n_splits=5):
    """Walk-forward cross-validation: always train on past, evaluate on future.

    Expanding window: first 60% = training only, last 40% split into test windows.
    Returns (accuracy, total_evaluated, avg_rank_correlation).
    """
    unique_dates = data['unique_dates']
    date_idx = data['date_idx']
    n_dates = len(unique_dates)

    if n_dates < 30:
        log.warning("  Walk-forward CV: too few dates (<30)")
        return 0.0, 0, 0.0

    # Parse params
    raw_w = params[:n_active]
    w_sum = raw_w.sum()
    weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active
    buy_thr = params[n_active]
    sell_thr = -abs(params[n_active + 1])
    quality_gate = params[n_active + 2]
    ma200_mult = params[n_active + 3]

    # First 60% = train-only, last 40% split into n_splits test windows
    train_end_idx = int(n_dates * 0.6)
    test_dates_count = n_dates - train_end_idx
    window_size = max(5, test_dates_count // n_splits)

    total_correct = 0
    total_actionable = 0
    all_rank_corrs = []

    for i in range(n_splits):
        test_start = train_end_idx + i * window_size
        test_end = min(test_start + window_size, n_dates)

        if test_start >= n_dates:
            break

        # Build test mask using pre-computed date indices
        test_mask = np.zeros(len(date_idx), dtype=bool)
        for d in range(test_start, test_end):
            test_mask |= (date_idx == d)

        if test_mask.sum() < 10:
            continue

        # Slice test data
        fold_data = {
            'scores': data['scores'][test_mask],
            'has_data': data['has_data'][test_mask],
            'actual': data['actual'][test_mask],
            'volatility': data['volatility'][test_mask],
            'btc_7d': data['btc_7d'][test_mask],
            'ma200_below': data['ma200_below'][test_mask],
            'agreement': data['agreement'][test_mask],
            'periods': data['periods'][test_mask],
            'n': int(test_mask.sum()),
        }

        # Signal accuracy on test fold
        result = recompute_fast(
            fold_data, weight_vec, active_indices,
            buy_threshold=buy_thr, sell_threshold=sell_thr,
            quality_gate=quality_gate, ma200_buy_mult=ma200_mult,
        )

        if result['actionable'] > 0:
            total_correct += result['correct']
            total_actionable += result['actionable']

        # Rank correlation on test fold
        composite = compute_composite_scores(fold_data, weight_vec, active_indices)
        fold_dates = data['dates'][test_mask]
        for d in np.unique(fold_dates):
            d_mask = fold_dates == d
            if d_mask.sum() >= 5:
                rc = spearman_rank_fast(composite[d_mask], fold_data['actual'][d_mask])
                if not np.isnan(rc):
                    all_rank_corrs.append(rc)

    accuracy = total_correct / total_actionable if total_actionable > 0 else 0.0
    avg_rank_corr = np.mean(all_rank_corrs) if all_rank_corrs else 0.0

    return accuracy, total_actionable, avg_rank_corr


def optimize_v2(run_id=None, use_groups=False):
    """Optimization with ranking objective + walk-forward CV validation.

    Uses objective_v2 for optimization, walk_forward_cv for validation.
    Also runs old random CV for comparison to show leakage reduction.
    """
    from scipy.optimize import differential_evolution

    if use_groups:
        return _optimize_all_groups_v2(run_id)

    data, actual_run_id = load_training_data(run_id)
    if data is None:
        return None

    if 'date_idx' not in data:
        log.error("Data missing date information — cannot use v2 objective")
        return None

    n_total = data['n']
    n_dates = len(data['unique_dates'])
    log.info(f"  Total rows: {n_total}, unique dates: {n_dates}")

    active_cats, inactive_cats = detect_active_categories(data)
    n_active = len(active_cats)
    active_indices = np.array([CATEGORY_NAMES.index(c) for c in active_cats])

    # === Baseline (v19 weights) ===
    baseline_w = np.array([CURRENT_WEIGHTS.get(c, 0.0) for c in active_cats])
    bw_sum = baseline_w.sum()
    if bw_sum > 0:
        baseline_w = baseline_w / bw_sum

    baseline = recompute_fast(
        data, baseline_w, active_indices,
        buy_threshold=0.1, sell_threshold=-0.1,
        quality_gate=0.0, ma200_buy_mult=1.5,
    )
    baseline_composite = compute_composite_scores(data, baseline_w, active_indices)
    baseline_spread = float(baseline_composite.max() - baseline_composite.min())

    log.info(f"\n  BASELINE:")
    log.info(f"    Accuracy: {baseline['accuracy']*100:.1f}% ({baseline['correct']}/{baseline['actionable']})")
    log.info(f"    Score spread: {baseline_spread:.4f}")

    # === Optimize with v2 ranking objective ===
    # Cap 'learned' at 0.30 to prevent circular leakage domination
    weight_bounds = []
    for cat in active_cats:
        if cat == 'learned':
            weight_bounds.append((0.0, 0.30))
        else:
            weight_bounds.append((0.0, 0.50))
    bounds = (
        weight_bounds +
        [(0.03, 0.20),    # buy_threshold
         (0.03, 0.20),    # sell_threshold
         (0.0, 0.75),     # quality_gate
         (1.0, 3.0)]      # ma200_buy_mult
    )

    min_actionable = max(300, int(data['n'] * 0.05))
    log.info(f"\n  Optimizing with ranking objective ({n_active} categories, min_actionable={min_actionable})...")
    log.info(f"  This may take 60-180 seconds...")

    result = differential_evolution(
        objective_v2,
        bounds=bounds,
        args=(data, active_indices, n_active, min_actionable),
        seed=42,
        maxiter=200,
        tol=1e-5,
        popsize=20,
        mutation=(0.5, 1.5),
        recombination=0.8,
        disp=False,
        workers=1,
    )

    # === Extract results ===
    opt_params = result.x
    raw_w = opt_params[:n_active]
    w_sum = raw_w.sum()
    opt_weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active
    opt_buy = opt_params[n_active]
    opt_sell = -abs(opt_params[n_active + 1])
    opt_qg = opt_params[n_active + 2]
    opt_ma200 = opt_params[n_active + 3]

    opt_result = recompute_fast(
        data, opt_weight_vec, active_indices,
        buy_threshold=opt_buy, sell_threshold=opt_sell,
        quality_gate=opt_qg, ma200_buy_mult=opt_ma200,
    )

    # Rank correlation on full data
    opt_composite = compute_composite_scores(data, opt_weight_vec, active_indices)
    rank_corrs = []
    for d in range(len(data['unique_dates'])):
        mask = data['date_idx'] == d
        if mask.sum() >= 5:
            rc = spearman_rank_fast(opt_composite[mask], data['actual'][mask])
            if not np.isnan(rc):
                rank_corrs.append(rc)
    avg_rank = np.mean(rank_corrs) if rank_corrs else 0.0
    opt_spread = float(opt_composite.max() - opt_composite.min())

    log.info(f"\n  OPTIMIZED (v2 ranking objective):")
    log.info(f"    Accuracy: {opt_result['accuracy']*100:.1f}% ({opt_result['correct']}/{opt_result['actionable']})")
    log.info(f"    Rank correlation: {avg_rank:.4f}")
    log.info(f"    Score spread: {opt_spread:.4f}")
    for regime, stats in sorted(opt_result['regime_stats'].items()):
        acc = stats['correct'] / stats['total'] * 100 if stats['total'] else 0
        log.info(f"    {regime:12s}: {acc:.0f}% (n={stats['total']})")

    # Weights
    opt_weights_dict = {cat: 0.0 for cat in CATEGORY_NAMES}
    for i, cat in enumerate(active_cats):
        opt_weights_dict[cat] = float(opt_weight_vec[i])

    log.info(f"\n  Optimized weights:")
    for cat in CATEGORY_NAMES:
        old = CURRENT_WEIGHTS.get(cat, 0)
        new = opt_weights_dict[cat]
        delta = new - old
        marker = " \u2605" if abs(delta) > 0.02 else ""
        log.info(f"    {cat:15s}: {old:.3f} \u2192 {new:.3f} ({delta:+.3f}){marker}")

    # === Walk-forward CV (temporal, no leakage) ===
    log.info(f"\n  Walk-forward cross-validation (5 splits, 60% train)...")
    wf_accuracy, wf_n, wf_rank = walk_forward_cv(data, result.x, active_indices, n_active)
    log.info(f"    WF accuracy: {wf_accuracy*100:.1f}% (n={wf_n})")
    log.info(f"    WF rank correlation: {wf_rank:.4f}")

    # Random CV for comparison (shows how much leakage inflated old results)
    log.info(f"  Random CV (for comparison)...")
    cv_accuracy, cv_n = cross_validate(data, result.x, active_indices, n_active)
    log.info(f"    Random CV: {cv_accuracy*100:.1f}% (n={cv_n})")
    if wf_accuracy < cv_accuracy:
        log.info(f"    \u2192 WF < Random by {(cv_accuracy-wf_accuracy)*100:.1f}pp (expected: random CV inflates)")

    improvement = opt_result['accuracy'] - baseline['accuracy']
    log.info(f"\n  IMPROVEMENT: {baseline['accuracy']*100:.1f}% \u2192 {opt_result['accuracy']*100:.1f}% ({improvement*100:+.1f}pp)")
    log.info(f"  SPREAD: {baseline_spread:.4f} \u2192 {opt_spread:.4f}")

    # === Save config ===
    output = {
        'weights': {cat: round(w, 4) for cat, w in opt_weights_dict.items()},
        'buy_threshold': round(float(opt_buy), 4),
        'sell_threshold': round(float(opt_sell), 4),
        'quality_gate': round(float(opt_qg), 4),
        'ma200_buy_mult': round(float(opt_ma200), 4),
        'training_accuracy': round(opt_result['accuracy'], 4),
        'wf_accuracy': round(wf_accuracy, 4),
        'wf_rank_correlation': round(wf_rank, 4),
        'cv_accuracy': round(cv_accuracy, 4),
        'rank_correlation': round(avg_rank, 4),
        'score_spread': round(opt_spread, 4),
        'n_actionable': opt_result['actionable'],
        'baseline_accuracy': round(baseline['accuracy'], 4),
        'improvement_pp': round(improvement * 100, 1),
        'active_categories': active_cats,
        'objective': 'v2_ranking',
        'optimized_at': datetime.now(timezone.utc).isoformat(),
        'run_id': actual_run_id,
    }

    CONFIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_OUT.write_text(json.dumps(output, indent=2))
    log.info(f"\n  Saved to {CONFIG_OUT}")

    return output


def _optimize_all_groups_v2(run_id=None):
    """Per-group optimization with ranking objective + walk-forward CV."""
    from scipy.optimize import differential_evolution

    log.info("=" * 60)
    log.info("COIN-GROUP OPTIMIZATION (v2 ranking objective)")
    log.info("=" * 60)

    group_results = {}
    for group in COIN_GROUPS:
        data, actual_run_id = load_training_data_by_group(group, run_id)
        if data is None or data['n'] < 100:
            log.warning(f"  Group '{group}': insufficient data ({data['n'] if data else 0} rows)")
            continue

        if 'date_idx' not in data:
            log.warning(f"  Group '{group}': missing date info")
            continue

        n_coins = len(COIN_GROUPS[group])
        log.info(f"\n  Optimizing group '{group}' ({data['n']} samples, "
                 f"{len(data['unique_dates'])} dates, {n_coins} coins)...")

        active_cats, _ = detect_active_categories(data)
        n_active = len(active_cats)
        active_indices = np.array([CATEGORY_NAMES.index(c) for c in active_cats])

        weight_bounds = []
        for cat in active_cats:
            if cat == 'learned':
                weight_bounds.append((0.0, 0.30))
            else:
                weight_bounds.append((0.0, 0.50))
        bounds = (
            weight_bounds +
            [(0.03, 0.20), (0.03, 0.20), (0.0, 0.75), (1.0, 3.0)]
        )

        min_actionable = max(10, int(data['n'] * 0.02))

        result = differential_evolution(
            objective_v2, bounds=bounds,
            args=(data, active_indices, n_active, min_actionable),
            seed=42, maxiter=200, tol=1e-5, popsize=20,
            mutation=(0.5, 1.5), recombination=0.8, disp=False, workers=1,
        )

        # Extract weights
        raw_w = result.x[:n_active]
        w_sum = raw_w.sum()
        opt_weight_vec = raw_w / w_sum if w_sum > 0 else np.ones(n_active) / n_active

        weights = {cat: 0.0 for cat in CATEGORY_NAMES}
        for i, cat in enumerate(active_cats):
            weights[cat] = float(opt_weight_vec[i])

        opt_buy = result.x[n_active]
        opt_sell = -abs(result.x[n_active + 1])
        opt_qg = result.x[n_active + 2]
        opt_ma200 = result.x[n_active + 3]

        # Evaluate
        opt_result = recompute_fast(
            data, opt_weight_vec, active_indices,
            buy_threshold=opt_buy, sell_threshold=opt_sell,
            quality_gate=opt_qg, ma200_buy_mult=opt_ma200,
        )

        # Rank correlation (lower min for small groups)
        composite = compute_composite_scores(data, opt_weight_vec, active_indices)
        rank_corrs = []
        min_coins_rank = min(3, n_coins)
        for d in range(len(data['unique_dates'])):
            mask = data['date_idx'] == d
            if mask.sum() >= min_coins_rank:
                rc = spearman_rank_fast(composite[mask], data['actual'][mask])
                if not np.isnan(rc):
                    rank_corrs.append(rc)
        avg_rank = np.mean(rank_corrs) if rank_corrs else 0.0

        # Walk-forward CV
        wf_accuracy, wf_n, wf_rank = walk_forward_cv(
            data, result.x, active_indices, n_active
        )

        spread = float(composite.max() - composite.min())

        log.info(f"    {group}: {opt_result['accuracy']*100:.1f}% train, "
                 f"{wf_accuracy*100:.1f}% WF, rank={avg_rank:.3f}, spread={spread:.3f}")

        group_results[group] = {
            'weights': weights,
            'buy_threshold': float(opt_buy),
            'sell_threshold': float(opt_sell),
            'quality_gate': float(opt_qg),
            'ma200_buy_mult': float(opt_ma200),
            'accuracy': opt_result['accuracy'],
            'wf_accuracy': wf_accuracy,
            'rank_correlation': avg_rank,
            'score_spread': spread,
            'n_actionable': opt_result['actionable'],
            'n_correct': opt_result['correct'],
            'group': group,
            'coins': COIN_GROUPS[group],
        }

    if not group_results:
        log.error("No groups optimized")
        return None

    # Summary
    log.info(f"\n  GROUP SUMMARY (v2):")
    for group, r in sorted(group_results.items()):
        log.info(f"    {group:10s}: {r['accuracy']*100:.1f}% train, "
                 f"{r['wf_accuracy']*100:.1f}% WF, "
                 f"rank={r['rank_correlation']:.3f}, "
                 f"spread={r['score_spread']:.3f}")

    # Overall v2 optimization as fallback
    log.info(f"\n  Running overall v2 optimization (fallback)...")
    overall = optimize_v2(run_id, use_groups=False)

    # Build grouped config (backward compatible)
    config = {
        'weights_by_group': {
            group: {
                'weights': {k: round(v, 4) for k, v in r['weights'].items()},
                'buy_threshold': round(r['buy_threshold'], 4),
                'sell_threshold': round(r['sell_threshold'], 4),
                'quality_gate': round(r['quality_gate'], 4),
                'ma200_buy_mult': round(r['ma200_buy_mult'], 4),
                'accuracy': round(r['accuracy'], 4),
                'wf_accuracy': round(r['wf_accuracy'], 4),
                'rank_correlation': round(r['rank_correlation'], 4),
                'score_spread': round(r['score_spread'], 4),
                'n_actionable': r['n_actionable'],
                'coins': r['coins'],
            }
            for group, r in group_results.items()
        },
        # Keep overall as fallback (backward compatible)
        'weights': overall['weights'] if overall else {},
        'buy_threshold': overall.get('buy_threshold', 0.1) if overall else 0.1,
        'sell_threshold': overall.get('sell_threshold', -0.1) if overall else -0.1,
        'quality_gate': overall.get('quality_gate', 0.0) if overall else 0.0,
        'ma200_buy_mult': overall.get('ma200_buy_mult', 1.5) if overall else 1.5,
        'training_accuracy': overall.get('training_accuracy', 0.0) if overall else 0.0,
        'wf_accuracy': overall.get('wf_accuracy', 0.0) if overall else 0.0,
        'rank_correlation': overall.get('rank_correlation', 0.0) if overall else 0.0,
        'cv_accuracy': overall.get('cv_accuracy', 0.0) if overall else 0.0,
        'n_actionable': overall.get('n_actionable', 0) if overall else 0,
        'baseline_accuracy': overall.get('baseline_accuracy', 0.0) if overall else 0.0,
        'active_categories': overall.get('active_categories', []) if overall else [],
        'objective': 'v2_ranking',
        'optimized_at': datetime.now(timezone.utc).isoformat(),
        'run_id': run_id or (overall.get('run_id') if overall else 'latest'),
    }

    CONFIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_OUT.write_text(json.dumps(config, indent=2))
    log.info(f"\nGrouped v2 config saved to {CONFIG_OUT}")

    return config


if __name__ == '__main__':
    run_id = None
    use_groups = False
    use_v2 = False
    for arg in sys.argv[1:]:
        if arg == '--groups':
            use_groups = True
        elif arg == '--v2':
            use_v2 = True
        else:
            run_id = arg

    if use_v2:
        log.info("=" * 70)
        log.info("ALPHA SIGNAL \u2014 WEIGHT OPTIMIZER v3 (Ranking + Walk-Forward)")
        log.info("=" * 70)

        result = optimize_v2(run_id, use_groups=use_groups)
        if result:
            wf = result.get('wf_accuracy', 0)
            rank = result.get('rank_correlation', 0)
            log.info(f"\n{'='*70}")
            if use_groups:
                groups_done = list(result.get('weights_by_group', {}).keys())
                log.info(f"DONE v2 — groups: {', '.join(groups_done)}")
            else:
                log.info(f"DONE v2 — {result['training_accuracy']*100:.1f}% train, "
                         f"{wf*100:.1f}% WF, rank={rank:.3f}")
            log.info(f"{'='*70}")
    else:
        log.info("=" * 70)
        log.info("ALPHA SIGNAL \u2014 WEIGHT OPTIMIZER v2")
        log.info("=" * 70)

        if use_groups:
            result = optimize_all_groups(run_id)
            if result:
                log.info(f"\n{'='*70}")
                groups_done = list(result.get('weights_by_group', {}).keys())
                log.info(f"DONE — groups: {', '.join(groups_done)}")
                log.info(f"{'='*70}")
        else:
            result = optimize(run_id)
            if result:
                log.info(f"\n{'='*70}")
                log.info(f"DONE — {result['training_accuracy']*100:.1f}% train, {result['cv_accuracy']*100:.1f}% CV")
                log.info(f"{'='*70}")
