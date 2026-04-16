"""
FORTIX — Regression Model (Level 2+4)
=============================================
Per-group ridge regression using continuous + polynomial features.
Base features: [RSI, F&G, funding_rate, volatility, btc_7d, ma200_below]
Polynomial (L4): [rsi_sq, funding_sq, rsi×funding, fg×vol, btc×fg, rsi×ma200]
Walk-forward validation: train on first 70%, test on last 30%.

Usage:
    python src/crypto/regression_model.py              # train all groups
    python src/crypto/regression_model.py <run_id>     # train on specific run
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
log = logging.getLogger('regression_model')

PATTERNS_DB = Path('data/crypto/patterns.db')
MARKET_DB = Path('data/crypto/market.db')
MODELS_DIR = Path('data/crypto/regression_models')

BASE_FEATURE_NAMES = ['rsi', 'fg', 'funding_rate', 'volatility', 'btc_7d', 'ma200_below']
POLY_FEATURE_NAMES = ['rsi_sq', 'funding_sq', 'rsi_x_funding', 'fg_x_vol', 'btc_x_fg', 'rsi_x_ma200']
# 11 category scores from forecast engine (signal_details_json)
CATEGORY_FEATURE_NAMES = [
    'cat_technical', 'cat_sentiment', 'cat_onchain', 'cat_macro',
    'cat_news', 'cat_news_claude', 'cat_historical', 'cat_learned',
    'cat_meta_analyst', 'cat_coinglass', 'cat_cryptoquant',
]
FEATURE_NAMES = BASE_FEATURE_NAMES + POLY_FEATURE_NAMES + CATEGORY_FEATURE_NAMES

COIN_GROUPS = {
    'majors': ['BTC', 'ETH'],
    'l1_alts': ['SOL', 'BNB', 'ADA', 'AVAX', 'DOT', 'XRP'],
    'defi': ['AAVE', 'UNI', 'MKR', 'CRV', 'LDO', 'LINK'],
    'ai': ['FET', 'RENDER', 'TAO'],
    'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}


def _get_coin_group(coin):
    for group, coins in COIN_GROUPS.items():
        if coin in coins:
            return group
    return 'l1_alts'


# ═══════════════════════════════════════════════════════════════
# FEATURE NORMALIZER
# ═══════════════════════════════════════════════════════════════

class FeatureNormalizer:
    """Z-score normalization per feature, fitted on training data."""

    def __init__(self):
        self.means = None
        self.stds = None

    def fit(self, X):
        self.means = np.mean(X, axis=0)
        self.stds = np.std(X, axis=0)
        self.stds[self.stds < 1e-8] = 1.0  # avoid division by zero

    def transform(self, X):
        if self.means is None:
            return X
        return (X - self.means) / self.stds

    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

    def to_dict(self):
        return {
            'means': self.means.tolist(),
            'stds': self.stds.tolist(),
        }

    def from_dict(self, d):
        self.means = np.array(d['means'])
        self.stds = np.array(d['stds'])
        return self


# ═══════════════════════════════════════════════════════════════
# FEATURE EXTRACTION
# ═══════════════════════════════════════════════════════════════

def _compute_poly_features(rsi, fg, funding, vol, btc_7d, ma200_below):
    """Compute polynomial interaction features from base features (Level 4).

    Returns 6 polynomial features:
    - rsi_sq: (rsi-50)^2/2500 — U-shape, both extremes predict reversion
    - funding_sq: clipped(funding*100)^2 — extreme funding = liquidation risk
    - rsi_x_funding: centered RSI × scaled funding — oversold+negative = bounce
    - fg_x_vol: centered F&G × scaled volatility — fear+high vol = different
    - btc_x_fg: BTC trend × F&G — bull+greed = overextended
    - rsi_x_ma200: centered RSI × MA200 position — oversold below MA200 = stronger
    """
    rsi_centered = (rsi - 50.0) / 50.0                        # [-1, 1]
    fg_centered = (fg - 50.0) / 50.0                          # [-1, 1]
    funding_scaled = np.clip(funding * 100.0, -1.0, 1.0)      # [-1, 1]
    vol_scaled = np.clip(vol * 10.0, 0.0, 2.0)                # [0, 2]
    btc_scaled = np.clip(btc_7d / 10.0, -1.0, 1.0)            # [-1, 1]
    ma200_sign = 2.0 * ma200_below - 1.0                      # -1 (above) or +1 (below)

    rsi_sq = rsi_centered ** 2
    funding_sq = funding_scaled ** 2
    rsi_x_funding = rsi_centered * funding_scaled
    fg_x_vol = fg_centered * vol_scaled
    btc_x_fg = btc_scaled * fg_centered
    rsi_x_ma200 = rsi_centered * ma200_sign

    return [rsi_sq, funding_sq, rsi_x_funding, fg_x_vol, btc_x_fg, rsi_x_ma200]


def _extract_category_scores(row):
    """Extract 11 category scores from signal_details_json or column fallback.

    Returns list of 11 floats: [tech, sent, onchain, macro, news, news_claude,
                                 historical, learned, meta_analyst, coinglass, cryptoquant]
    """
    # Try signal_details_json first (has all 11 categories)
    json_str = row['signal_details_json'] if 'signal_details_json' in row.keys() else None
    if json_str:
        try:
            details = json.loads(json_str)
            return [
                details.get('technical', {}).get('score', 0.0),
                details.get('sentiment', {}).get('score', 0.0),
                details.get('onchain', {}).get('score', 0.0),
                details.get('macro', {}).get('score', 0.0),
                details.get('news', {}).get('score', 0.0),
                details.get('news_claude', {}).get('score', 0.0),
                details.get('historical', {}).get('score', 0.0),
                details.get('learned', {}).get('score', 0.0),
                details.get('meta_analyst', {}).get('score', 0.0),
                details.get('coinglass', {}).get('score', 0.0),
                details.get('cryptoquant', {}).get('score', 0.0),
            ]
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: use column-level scores (only 6 available)
    return [
        float(row['technical_score']) if row['technical_score'] else 0.0,
        float(row['sentiment_score']) if row['sentiment_score'] else 0.0,
        float(row['onchain_score']) if row['onchain_score'] else 0.0,
        float(row['macro_score']) if row['macro_score'] else 0.0,
        float(row['news_score']) if row['news_score'] else 0.0,
        0.0,  # news_claude (not in columns)
        float(row['historical_score']) if row['historical_score'] else 0.0,
        0.0,  # learned
        0.0,  # meta_analyst
        0.0,  # coinglass
        0.0,  # cryptoquant
    ]


def extract_features_from_row(row):
    """Extract feature vector from a training_results row (sqlite3.Row).

    Returns numpy array of shape (23,) with 6 base + 6 polynomial + 11 category features.
    """
    rsi = row['rsi_at_forecast'] if row['rsi_at_forecast'] is not None else 50.0
    fg = float(row['fg_at_forecast']) if row['fg_at_forecast'] is not None else 50.0
    funding = row['funding_rate_at_forecast'] if row['funding_rate_at_forecast'] is not None else 0.0
    vol = row['volatility_at_forecast'] if row['volatility_at_forecast'] is not None else 0.05
    btc_7d = row['btc_change_7d'] if row['btc_change_7d'] is not None else 0.0
    ma200_below = 1.0 if row['ma200_trend'] == 'below' else 0.0

    base = [rsi, fg, funding, vol, btc_7d, ma200_below]
    poly = _compute_poly_features(rsi, fg, funding, vol, btc_7d, ma200_below)
    cats = _extract_category_scores(row)

    return np.array(base + poly + cats, dtype=np.float64)


def extract_features_live(conn, coin, category_scores=None):
    """Extract features from market.db for live prediction.

    Args:
        conn: sqlite3 connection to market.db
        coin: coin symbol
        category_scores: optional dict of {category: score} from forecast engine

    Returns numpy array of shape (23,) or None if insufficient data.
    """
    try:
        # 1. RSI (computed from 15 daily closes — RSI-14)
        rsi_rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 15", (coin,)
        ).fetchall()
        if len(rsi_rows) >= 15:
            closes_rsi = [r[0] for r in reversed(rsi_rows) if r[0] is not None]
            if len(closes_rsi) >= 15:
                changes = [closes_rsi[i] - closes_rsi[i-1] for i in range(1, len(closes_rsi))]
                gains = [max(0, c) for c in changes]
                losses = [max(0, -c) for c in changes]
                avg_gain = sum(gains) / len(gains)
                avg_loss = sum(losses) / len(losses)
                if avg_loss > 0:
                    rs = avg_gain / avg_loss
                    rsi = 100.0 - (100.0 / (1.0 + rs))
                else:
                    rsi = 100.0
            else:
                rsi = 50.0
        else:
            rsi = 50.0

        # 2. Fear & Greed
        fg_row = conn.execute(
            "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1"
        ).fetchone()
        fg = float(fg_row[0]) if fg_row and fg_row[0] is not None else 50.0

        # 3. Funding rate
        funding_row = conn.execute(
            "SELECT rate FROM funding_rates WHERE coin=? "
            "ORDER BY timestamp DESC LIMIT 1", (coin,)
        ).fetchone()
        funding = float(funding_row[0]) if funding_row and funding_row[0] is not None else 0.0

        # 4. Volatility (7d std of daily returns)
        prices = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 8", (coin,)
        ).fetchall()
        if len(prices) >= 2:
            closes = [p[0] for p in prices if p[0] is not None]
            if len(closes) >= 2:
                returns = [(closes[i] - closes[i+1]) / closes[i+1]
                           for i in range(len(closes)-1) if closes[i+1] != 0]
                vol = float(np.std(returns)) if returns else 0.05
            else:
                vol = 0.05
        else:
            vol = 0.05

        # 5. BTC 7d change
        btc_prices = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 8",
        ).fetchall()
        if len(btc_prices) >= 2:
            btc_now = btc_prices[0][0]
            btc_7d_ago = btc_prices[-1][0]
            btc_7d = ((btc_now - btc_7d_ago) / btc_7d_ago * 100) if btc_7d_ago else 0.0
        else:
            btc_7d = 0.0

        # 6. MA200 trend
        ma200_rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 200", (coin,)
        ).fetchall()
        if len(ma200_rows) >= 200:
            ma200 = np.mean([p[0] for p in ma200_rows if p[0]])
            current = ma200_rows[0][0] if ma200_rows[0][0] else 0
            ma200_below = 1.0 if current < ma200 else 0.0
        else:
            ma200_below = 0.5  # unknown — neutral value

        base = [rsi, fg, funding, vol, btc_7d, ma200_below]
        poly = _compute_poly_features(rsi, fg, funding, vol, btc_7d, ma200_below)

        # Category scores from forecast engine (passed in during live prediction)
        if category_scores:
            cats = [
                category_scores.get('technical', 0.0),
                category_scores.get('sentiment', 0.0),
                category_scores.get('onchain', 0.0),
                category_scores.get('macro', 0.0),
                category_scores.get('news', 0.0),
                category_scores.get('news_claude', 0.0),
                category_scores.get('historical', 0.0),
                category_scores.get('learned', 0.0),
                category_scores.get('meta_analyst', 0.0),
                category_scores.get('coinglass', 0.0),
                category_scores.get('cryptoquant', 0.0),
            ]
        else:
            cats = [0.0] * 11  # neutral when not available

        return np.array(base + poly + cats, dtype=np.float64)

    except Exception as e:
        log.warning(f"Feature extraction failed for {coin}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# TRAINING
# ═══════════════════════════════════════════════════════════════

LAMBDA_CANDIDATES = [10, 30, 50, 100, 200, 500, 1000, 2000]
L1_RATIO_CANDIDATES = [0.0, 0.1, 0.3, 0.5, 0.7]


def _fit_ridge(X_train_norm, y_train_w, lambda_reg):
    """Fit ridge regression, return beta coefficients."""
    n_features = X_train_norm.shape[1]
    XtX = X_train_norm.T @ X_train_norm + lambda_reg * np.eye(n_features)
    Xty = X_train_norm.T @ y_train_w
    return np.linalg.solve(XtX, Xty)


def _fit_elastic_net(X, y, alpha, l1_ratio, max_iter=2000, tol=1e-5):
    """Coordinate descent Elastic Net.

    Minimizes: 0.5/n * ||y - X@beta||^2 + alpha * [l1_ratio * ||beta||_1
               + 0.5 * (1-l1_ratio) * ||beta||_2^2]

    When l1_ratio=0 → pure Ridge. When l1_ratio=1 → pure Lasso.
    L1 component drives unimportant features to exactly zero.
    """
    n, p = X.shape
    beta = np.zeros(p)

    # Pre-compute X column norms (cached for efficiency)
    col_sq_norms = np.sum(X ** 2, axis=0) / n

    l1_pen = alpha * l1_ratio
    l2_pen = alpha * (1.0 - l1_ratio)

    for iteration in range(max_iter):
        beta_old = beta.copy()

        for j in range(p):
            # Compute partial residual correlation
            residual = y - X @ beta + X[:, j] * beta[j]
            rho_j = X[:, j] @ residual / n

            # Soft-thresholding (L1) + L2 shrinkage
            denom = col_sq_norms[j] + l2_pen
            if denom < 1e-12:
                beta[j] = 0.0
            elif rho_j > l1_pen:
                beta[j] = (rho_j - l1_pen) / denom
            elif rho_j < -l1_pen:
                beta[j] = (rho_j + l1_pen) / denom
            else:
                beta[j] = 0.0

        # Check convergence
        if np.max(np.abs(beta - beta_old)) < tol:
            break

    n_nonzero = np.sum(np.abs(beta) > 1e-8)
    return beta, n_nonzero


def _eval_predictions(y_pred, y_true, dates_arr):
    """Evaluate predictions: dir_acc, MAE, R2, rank_corr."""
    dir_acc = float(np.mean(np.sign(y_pred) == np.sign(y_true)))
    mae = float(np.mean(np.abs(y_true - y_pred)))
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    try:
        from src.crypto.weight_optimizer import spearman_rank_fast
        rank_corrs = []
        for d in np.unique(dates_arr):
            d_mask = dates_arr == d
            if d_mask.sum() >= 3:
                rc = spearman_rank_fast(y_pred[d_mask], y_true[d_mask])
                if not np.isnan(rc):
                    rank_corrs.append(rc)
        rank_corr = float(np.mean(rank_corrs)) if rank_corrs else 0.0
    except Exception:
        rank_corr = 0.0

    return dir_acc, mae, r2, rank_corr


def _tune_hyperparams_wf(X_train, y_train_w, dates_train):
    """Tune alpha and l1_ratio via nested walk-forward CV on training data.

    Splits training data into inner_train (first 60%) and inner_val (last 40%).
    Tries all combinations of alpha × l1_ratio.
    Returns (best_alpha, best_l1_ratio) maximizing composite score.
    """
    unique_dates = np.sort(np.unique(dates_train))
    inner_split = int(len(unique_dates) * 0.6)
    if inner_split < 5:
        return 100.0, 0.0  # fallback to pure ridge

    inner_train_dates = set(unique_dates[:inner_split])

    inner_train_mask = np.array([d in inner_train_dates for d in dates_train])
    inner_val_mask = ~inner_train_mask

    X_inner_train = X_train[inner_train_mask]
    y_inner_train = y_train_w[inner_train_mask]
    X_inner_val = X_train[inner_val_mask]
    y_inner_val = y_train_w[inner_val_mask]
    dates_inner_val = dates_train[inner_val_mask]

    if len(y_inner_train) < 20 or len(y_inner_val) < 10:
        return 100.0, 0.0

    # Add intercept + normalize on inner_train
    X_itr_i = np.column_stack([X_inner_train, np.ones(len(X_inner_train))])
    X_ival_i = np.column_stack([X_inner_val, np.ones(len(X_inner_val))])

    norm = FeatureNormalizer()
    X_itr_n = norm.fit_transform(X_itr_i)
    X_ival_n = norm.transform(X_ival_i)

    best_score = -999
    best_alpha = 100.0
    best_l1 = 0.0

    for alpha in LAMBDA_CANDIDATES:
        for l1_ratio in L1_RATIO_CANDIDATES:
            if l1_ratio == 0.0:
                # Pure ridge — faster via closed-form
                beta = _fit_ridge(X_itr_n, y_inner_train, alpha)
            else:
                beta, _ = _fit_elastic_net(X_itr_n, y_inner_train, alpha, l1_ratio)

            y_pred_val = X_ival_n @ beta
            dir_acc, mae, r2, rank_corr = _eval_predictions(
                y_pred_val, y_inner_val, dates_inner_val)

            # Composite: dir_acc is primary, rank_corr is secondary
            composite = dir_acc + 0.5 * max(rank_corr, 0.0)
            if composite > best_score:
                best_score = composite
                best_alpha = alpha
                best_l1 = l1_ratio

    return best_alpha, best_l1


def train_all_groups(run_id=None, lambda_reg=None):
    """Train ridge regression for each coin group using walk-forward split.

    Walk-forward: train on first 70% of dates, test on last 30%.
    Lambda auto-tuned per group via nested inner CV if lambda_reg=None.
    Targets winsorized to [-20%, 20%] to reduce outlier influence.
    Intercept term added for bias correction.
    12 features: 6 base + 6 polynomial interactions (Level 4).
    Saves models to data/crypto/regression_models/{group}.json
    Returns dict with per-group metrics.
    """
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
            return None
        run_id = row['run_id']

    rows = conn.execute(
        "SELECT * FROM training_results WHERE run_id = ? "
        "AND actual_change_pct IS NOT NULL", (run_id,)
    ).fetchall()
    conn.close()

    log.info(f"Training regression models on {len(rows)} samples (run {run_id})")
    log.info(f"  Features: {len(FEATURE_NAMES)} ({len(BASE_FEATURE_NAMES)} base + "
             f"{len(POLY_FEATURE_NAMES)} polynomial)")

    # Extract features and targets
    all_features = []
    all_targets = []
    all_coins = []
    all_dates = []

    for r in rows:
        feat = extract_features_from_row(r)
        target = r['actual_change_pct']
        all_features.append(feat)
        all_targets.append(target)
        all_coins.append(r['coin'])
        all_dates.append(r['forecast_date'])

    X_all = np.array(all_features)
    y_all = np.array(all_targets)
    coins_all = np.array(all_coins)
    dates_all = np.array(all_dates)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    for group, group_coins in COIN_GROUPS.items():
        mask = np.array([c in group_coins for c in coins_all])
        X_group = X_all[mask]
        y_group = y_all[mask]
        dates_group = dates_all[mask]

        n = len(y_group)
        if n < 50:
            log.warning(f"  {group}: too few samples ({n}), skipping")
            continue

        # Walk-forward split: train on first 70% of dates, test on last 30%
        unique_dates = np.sort(np.unique(dates_group))
        split_idx = int(len(unique_dates) * 0.7)
        train_dates_set = set(unique_dates[:split_idx])

        train_mask = np.array([d in train_dates_set for d in dates_group])
        test_mask = ~train_mask

        X_train, y_train = X_group[train_mask], y_group[train_mask]
        X_test, y_test = X_group[test_mask], y_group[test_mask]

        if len(y_train) < 30:
            log.warning(f"  {group}: too few training samples ({len(y_train)}), skipping")
            continue

        # Winsorize targets to [-20%, 20%] (reduces outlier influence)
        y_train_w = np.clip(y_train, -20.0, 20.0)
        dates_train = dates_group[train_mask]
        dates_test = dates_group[test_mask]

        # Auto-tune alpha + l1_ratio per group via nested walk-forward CV
        if lambda_reg is None:
            best_alpha, best_l1 = _tune_hyperparams_wf(X_train, y_train_w, dates_train)
        else:
            best_alpha, best_l1 = lambda_reg, 0.0

        # Add intercept column (bias term)
        X_train_i = np.column_stack([X_train, np.ones(len(X_train))])
        X_test_i = np.column_stack([X_test, np.ones(len(X_test))])
        n_features_i = X_train_i.shape[1]

        # Normalize features (fit on training data only)
        normalizer = FeatureNormalizer()
        X_train_norm = normalizer.fit_transform(X_train_i)
        X_test_norm = normalizer.transform(X_test_i)

        # Elastic Net with tuned hyperparams (l1_ratio=0 → pure Ridge)
        if best_l1 == 0.0:
            beta = _fit_ridge(X_train_norm, y_train_w, best_alpha)
            n_nonzero = np.sum(np.abs(beta) > 1e-8)
        else:
            beta, n_nonzero = _fit_elastic_net(
                X_train_norm, y_train_w, best_alpha, best_l1)

        # Evaluate on train and test
        y_pred_train = X_train_norm @ beta
        y_pred_test = X_test_norm @ beta

        train_dir_acc, train_mae, _, _ = _eval_predictions(
            y_pred_train, y_train, dates_train)
        test_dir_acc, test_mae, r2, test_rank_corr = _eval_predictions(
            y_pred_test, y_test, dates_test)

        # Feature importance (|beta| after normalization, excluding intercept)
        importance = np.abs(beta[:-1])  # exclude intercept
        importance_norm = importance / importance.sum() if importance.sum() > 0 else importance

        # Determine blend weight based on test quality
        # Only enable regression for groups where it demonstrably helps
        min_dir_acc = 0.52  # must beat random (50%) by at least 2pp
        if test_dir_acc >= 0.60:
            blend_weight = 0.15  # strong model
        elif test_dir_acc >= min_dir_acc:
            blend_weight = 0.08  # weak but useful
        else:
            blend_weight = 0.0   # disabled — model hurts

        # Save model
        model = {
            'beta': beta.tolist(),
            'normalizer': normalizer.to_dict(),
            'feature_names': FEATURE_NAMES,
            'alpha': best_alpha,
            'l1_ratio': best_l1,
            'n_nonzero': int(n_nonzero),
            'train_mae': round(train_mae, 4),
            'test_mae': round(test_mae, 4),
            'train_dir_acc': round(train_dir_acc, 4),
            'test_dir_acc': round(test_dir_acc, 4),
            'r2': round(r2, 4),
            'test_rank_corr': round(test_rank_corr, 4),
            'blend_weight': blend_weight,
            'feature_importance': {name: round(float(imp), 4)
                                   for name, imp in zip(FEATURE_NAMES, importance_norm)},
            'n_train': len(y_train),
            'n_test': len(y_test),
            'group': group,
            'coins': group_coins,
            'run_id': run_id,
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }

        model_path = MODELS_DIR / f'{group}.json'
        model_path.write_text(json.dumps(model, indent=2))

        l1_str = f", l1={best_l1}" if best_l1 > 0 else ""
        log.info(f"  {group:10s}: MAE={test_mae:.2f}%, dir_acc={test_dir_acc*100:.1f}%, "
                 f"R2={r2:.4f}, rank={test_rank_corr:.3f}, "
                 f"alpha={best_alpha}{l1_str}, features={n_nonzero}/{n_features_i} "
                 f"(train={len(y_train)}, test={len(y_test)})")

        # Top features
        top = sorted(zip(FEATURE_NAMES, importance_norm), key=lambda x: -x[1])[:4]
        top_str = ', '.join(f'{name}={imp:.2f}' for name, imp in top)
        log.info(f"    Features: {top_str}")

        results[group] = model

    return results


def train_from_features(force: bool = False) -> dict:
    """Train regression models using raw features from feature_builder.

    This eliminates the circular dependency on forecast_engine category scores.
    Uses the same 77+ features that V3/V5 LightGBM use, with proper walk-forward CV.
    Target: label_7d (7-day forward return, winsorized to [-20%, +20%]).
    """
    from src.crypto.feature_builder import FeatureBuilder

    log.info("Training regression models from raw features (no circular dependency)")
    fb = FeatureBuilder(db_path=str(MARKET_DB))
    df = fb.build_dataset(include_labels=True)

    target_col = 'label_7d'
    if target_col not in df.columns:
        log.error(f"No {target_col} in dataset")
        return {}

    df = df.dropna(subset=[target_col]).copy()
    # Winsorize target
    df[target_col] = df[target_col].clip(-0.20, 0.20) * 100  # to percent

    feature_cols = [c for c in fb.FEATURE_COLS if c in df.columns]
    log.info(f"  Dataset: {len(df)} rows, {len(feature_cols)} features")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    for group, group_coins in COIN_GROUPS.items():
        df_g = df[df['coin'].isin(group_coins)].copy()
        if len(df_g) < 100:
            log.warning(f"  {group}: too few samples ({len(df_g)}), skipping")
            continue

        # Walk-forward: train first 70% dates, test last 30%
        dates = sorted(df_g['date'].unique())
        split_idx = int(len(dates) * 0.7)
        train_dates = set(dates[:split_idx])

        train_mask = df_g['date'].isin(train_dates)
        X_train = df_g.loc[train_mask, feature_cols].values
        y_train = df_g.loc[train_mask, target_col].values
        X_test = df_g.loc[~train_mask, feature_cols].values
        y_test = df_g.loc[~train_mask, target_col].values
        dates_train = df_g.loc[train_mask, 'date'].values
        dates_test = df_g.loc[~train_mask, 'date'].values

        if len(y_train) < 50:
            continue

        # Replace NaN with column median (Elastic Net can't handle NaN)
        col_medians = np.nanmedian(X_train, axis=0)
        col_medians = np.where(np.isnan(col_medians), 0.0, col_medians)
        for j in range(X_train.shape[1]):
            nan_mask_tr = np.isnan(X_train[:, j])
            X_train[nan_mask_tr, j] = col_medians[j]
            nan_mask_te = np.isnan(X_test[:, j])
            X_test[nan_mask_te, j] = col_medians[j]

        # Auto-tune hyperparameters
        best_alpha, best_l1 = _tune_hyperparams_wf(X_train, y_train, dates_train)

        # Add intercept + normalize
        X_train_i = np.column_stack([X_train, np.ones(len(X_train))])
        X_test_i = np.column_stack([X_test, np.ones(len(X_test))])

        normalizer = FeatureNormalizer()
        X_train_norm = normalizer.fit_transform(X_train_i)
        X_test_norm = normalizer.transform(X_test_i)

        if best_l1 == 0.0:
            beta = _fit_ridge(X_train_norm, y_train, best_alpha)
            n_nonzero = np.sum(np.abs(beta) > 1e-8)
        else:
            beta, n_nonzero = _fit_elastic_net(X_train_norm, y_train, best_alpha, best_l1)

        y_pred_test = X_test_norm @ beta
        test_dir_acc, test_mae, r2, test_rank_corr = _eval_predictions(
            y_pred_test, y_test, dates_test)

        # Feature importance
        importance = np.abs(beta[:-1])
        importance_norm = importance / importance.sum() if importance.sum() > 0 else importance

        if test_dir_acc >= 0.60:
            blend_weight = 0.15
        elif test_dir_acc >= 0.52:
            blend_weight = 0.08
        else:
            blend_weight = 0.0

        model = {
            'beta': beta.tolist(),
            'normalizer': normalizer.to_dict(),
            'feature_names': feature_cols,
            'col_medians': col_medians.tolist(),
            'alpha': best_alpha,
            'l1_ratio': best_l1,
            'n_nonzero': int(n_nonzero),
            'test_dir_acc': round(test_dir_acc, 4),
            'test_mae': round(test_mae, 4),
            'r2': round(r2, 4),
            'test_rank_corr': round(test_rank_corr, 4),
            'blend_weight': blend_weight,
            'n_train': len(y_train),
            'n_test': len(y_test),
            'group': group,
            'data_source': 'feature_builder',
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }

        model_path = MODELS_DIR / f'{group}.json'
        model_path.write_text(json.dumps(model, indent=2))

        log.info(f"  {group:10s}: dir_acc={test_dir_acc*100:.1f}%, R2={r2:.4f}, "
                 f"rank={test_rank_corr:.3f}, blend={blend_weight} "
                 f"(train={len(y_train)}, test={len(y_test)})")

        top = sorted(zip(feature_cols, importance_norm), key=lambda x: -x[1])[:5]
        top_str = ', '.join(f'{name}={imp:.3f}' for name, imp in top)
        log.info(f"    Top features: {top_str}")

        results[group] = model
        _model_cache.clear()  # invalidate cache

    return results


# ═══════════════════════════════════════════════════════════════
# PREDICTION
# ═══════════════════════════════════════════════════════════════

# Model cache to avoid re-reading JSON per coin
_model_cache = {}
_blend_weight_cache = {}


def load_model(group):
    """Load saved model for a group. Returns (beta, normalizer) or (None, None).

    Caches models in memory after first load.
    """
    if group in _model_cache:
        return _model_cache[group]

    model_path = MODELS_DIR / f'{group}.json'
    if not model_path.exists():
        _model_cache[group] = (None, None)
        return None, None

    try:
        model = json.loads(model_path.read_text())
        beta = np.array(model['beta'])
        normalizer = FeatureNormalizer().from_dict(model['normalizer'])
        _model_cache[group] = (beta, normalizer)
        # Also cache blend_weight separately
        _blend_weight_cache[group] = model.get('blend_weight', 0.0)
        return beta, normalizer
    except Exception as e:
        log.warning(f"Failed to load model for {group}: {e}")
        _model_cache[group] = (None, None)
        return None, None


def get_blend_weight(group):
    """Get the recommended blend weight for a group's regression model.

    Returns 0.0-0.15 based on model quality. 0.0 means disabled.
    """
    if group not in _blend_weight_cache:
        load_model(group)  # triggers cache population
    return _blend_weight_cache.get(group, 0.0)


def predict_change(features, group):
    """Predict actual_change_pct using group's ridge regression model.

    features: numpy array of shape (n_features,) — raw (unnormalized)
    group: coin group name
    Returns: predicted change % or None if no model
    """
    beta, normalizer = load_model(group)
    if beta is None:
        return None

    # Add intercept column (must match training)
    features_i = np.append(features, 1.0)

    # Shape check: features_i must match beta length
    if len(features_i) != len(beta):
        log.warning(f"Feature shape mismatch for {group}: "
                    f"features={len(features_i)}, beta={len(beta)}. Retrain needed.")
        return None

    features_norm = normalizer.transform(features_i.reshape(1, -1))[0]
    prediction = float(features_norm @ beta)

    # Clip to reasonable range (±20%)
    return max(-20.0, min(20.0, prediction))


def predict_score(features, group):
    """Predict and convert to [-1, 1] score for forecast_engine integration.

    Maps predicted change % to a score:
    - ±10% → ±1.0 (full signal)
    - ±5% → ±0.5 (moderate signal)
    - 0% → 0 (neutral)
    """
    change = predict_change(features, group)
    if change is None:
        return None

    # Linear mapping: 10% change → score 1.0
    score = change / 10.0
    return max(-1.0, min(1.0, score))


def clear_cache():
    """Clear model cache (call after retraining)."""
    _model_cache.clear()
    _blend_weight_cache.clear()


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    run_id = sys.argv[1] if len(sys.argv) > 1 else None

    log.info("=" * 60)
    log.info("REGRESSION MODEL TRAINING (Level 2+4)")
    log.info("  12 features (6 base + 6 polynomial)")
    log.info("  Per-group lambda auto-tuning via nested WF-CV")
    log.info("=" * 60)

    results = train_all_groups(run_id)
    if results:
        log.info(f"\n{'='*60}")
        log.info(f"Trained {len(results)} group models")
        for group, model in sorted(results.items()):
            log.info(f"  {group:10s}: test_dir_acc={model['test_dir_acc']*100:.1f}%, "
                     f"R\u00b2={model['r2']:.4f}, rank={model['test_rank_corr']:.3f}")
        log.info(f"Models saved to {MODELS_DIR}/")
        log.info(f"{'='*60}")
    else:
        log.error("No models trained!")
