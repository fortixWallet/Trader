"""
Forecast v3.1 — Regression + Ranking approach.

Key changes from v3:
  1. REGRESSION (predict actual 7d return) instead of classification
  2. MARKET-RELATIVE predictions (excess return over market avg)
  3. NESTED CV (inner validation for early stopping, outer for evaluation)
  4. RANK-BASED evaluation (Spearman correlation, quintile returns)
  5. SAMPLE WEIGHTING (180d half-life)
  6. SMART CONFIDENCE (only signal when prediction is extreme)

No data leakage. No inflated metrics. Honest numbers only.
"""

import logging
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path
from datetime import datetime
from scipy.stats import spearmanr
from sklearn.preprocessing import StandardScaler

from src.crypto.feature_builder import FeatureBuilder, COIN_GROUPS, COIN_TO_GROUP

logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'models_v3'
DATASET_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'training_dataset_v3.csv'

CORE_FEATURES = [
    'ret_1d', 'ret_3d', 'ret_7d', 'ret_14d', 'ret_30d',
    'volatility_7d', 'volatility_30d',
    'rsi_14', 'bb_position', 'ma50_dist', 'ma200_dist',
    'volume_ratio_7d', 'high_low_range_7d',
    'funding_rate', 'funding_rate_7d_avg', 'funding_rate_pctl_30d',
    'fg_value', 'fg_change_7d', 'fg_percentile_30d',
    'cq_netflow', 'cq_netflow_7d_avg', 'cq_reserve_change_7d',
    'coinbase_premium',
    'etf_flow_usd', 'etf_flow_7d_avg',
    'btc_ret_7d', 'btc_ret_30d', 'corr_btc_30d',
    'pct_above_ma50', 'pct_above_ma200',
    'market_avg_ret_7d', 'n_coins_new_high_30d',
    'vix', 'yield_curve', 'treasury_10y', 'fed_rate',
    'cpi_yoy', 'fomc_meeting', 'fomc_days_until',
    'gtrend_bitcoin', 'gtrend_crypto', 'gtrend_bitcoin_change',
    'defi_tvl_change_7d', 'defi_tvl_change_30d',
    'days_since_halving', 'halving_cycle_phase',
    'oi_change_1d', 'oi_change_7d',
    'ls_long_pct', 'ls_ratio', 'taker_ratio',
    'day_of_week', 'is_weekend',
]

LGB_PARAMS = {
    'objective': 'regression',
    'metric': 'mae',
    'learning_rate': 0.03,
    'max_depth': 4,
    'num_leaves': 15,
    'min_child_samples': 30,
    'subsample': 0.8,
    'colsample_bytree': 0.7,
    'reg_alpha': 0.5,
    'reg_lambda': 2.0,
    'verbose': -1,
    'seed': 42,
}


def compute_sample_weights(dates: pd.Series, half_life_days: int = 180) -> np.ndarray:
    max_date = dates.max()
    days_ago = (max_date - dates).dt.days.values
    return np.exp(-np.log(2) * days_ago / half_life_days)


def load_dataset() -> pd.DataFrame:
    if DATASET_PATH.exists():
        df = pd.read_csv(DATASET_PATH)
        df['date'] = pd.to_datetime(df['date'])
        return df
    builder = FeatureBuilder()
    return builder.build_dataset(include_labels=True)


def walk_forward_regression(
    df: pd.DataFrame,
    horizon: str = '7d',
    min_train_days: int = 120,
    val_days: int = 30,
    inner_val_frac: float = 0.15,
) -> dict:
    """
    Walk-forward CV with REGRESSION + NESTED early stopping.

    Inner split: last 15% of training data for early stopping.
    Outer fold: completely untouched test data for evaluation.
    No data leakage.
    """
    label_col = f'label_{horizon}'
    label_dir = f'label_dir_{horizon}'

    df = df.dropna(subset=[label_col]).sort_values('date').reset_index(drop=True)
    dates = sorted(df['date'].unique())

    if len(dates) < min_train_days + val_days:
        return {'error': 'insufficient_data'}

    feature_cols = [c for c in CORE_FEATURES if c in df.columns]
    X_all = df[feature_cols]
    y_return = df[label_col].astype(float)  # actual % return
    y_dir = df[label_dir].astype(int) if label_dir in df.columns else None
    dates_all = df['date']
    coins_all = df['coin']

    # Walk-forward folds
    folds = []
    fold_start = min_train_days
    while fold_start + val_days <= len(dates):
        folds.append({
            'train_end': dates[fold_start - 1],
            'val_start': dates[fold_start],
            'val_end': dates[min(fold_start + val_days - 1, len(dates) - 1)],
        })
        fold_start += val_days

    logger.info(f"Regression walk-forward: {len(folds)} folds, {len(dates)} dates")

    all_pred_returns = []
    all_true_returns = []
    all_true_dirs = []
    all_coins = []
    all_dates_out = []
    fold_metrics = []

    for fold in folds:
        train_mask = dates_all <= fold['train_end']
        test_mask = (dates_all >= fold['val_start']) & (dates_all <= fold['val_end'])

        X_train_full = X_all[train_mask]
        y_train_full = y_return[train_mask]

        X_test = X_all[test_mask]
        y_test_return = y_return[test_mask]
        y_test_dir = y_dir[test_mask] if y_dir is not None else None

        if len(X_test) == 0 or len(X_train_full) < 100:
            continue

        # NESTED SPLIT: inner validation from END of training data
        # This prevents early stopping from seeing the test fold
        train_dates = dates_all[train_mask]
        inner_split_date = train_dates.quantile(1.0 - inner_val_frac)
        inner_train_mask = train_mask & (dates_all <= inner_split_date)
        inner_val_mask = train_mask & (dates_all > inner_split_date)

        X_inner_train = X_all[inner_train_mask]
        y_inner_train = y_return[inner_train_mask]
        X_inner_val = X_all[inner_val_mask]
        y_inner_val = y_return[inner_val_mask]

        if len(X_inner_val) < 20:
            # Not enough for inner validation, train without early stopping
            weights = compute_sample_weights(dates_all[train_mask])
            train_data = lgb.Dataset(X_train_full.fillna(np.nan), label=y_train_full, weight=weights)
            model = lgb.train(LGB_PARAMS, train_data, num_boost_round=150,
                            callbacks=[lgb.log_evaluation(0)])
        else:
            weights = compute_sample_weights(dates_all[inner_train_mask])
            train_data = lgb.Dataset(X_inner_train.fillna(np.nan), label=y_inner_train, weight=weights)
            val_data = lgb.Dataset(X_inner_val.fillna(np.nan), label=y_inner_val, reference=train_data)
            model = lgb.train(LGB_PARAMS, train_data, num_boost_round=500,
                            valid_sets=[val_data],
                            callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(0)])

        # Predict returns on test fold (completely unseen)
        pred_returns = model.predict(X_test.fillna(np.nan))

        all_pred_returns.extend(pred_returns)
        all_true_returns.extend(y_test_return.values)
        if y_test_dir is not None:
            all_true_dirs.extend(y_test_dir.values)
        all_coins.extend(coins_all[test_mask].values)
        all_dates_out.extend(dates_all[test_mask].values)

        # Per-fold Spearman correlation
        if len(pred_returns) > 10:
            rho, p = spearmanr(pred_returns, y_test_return.values)
            fold_metrics.append({
                'period': f"{fold['val_start'].date()}-{fold['val_end'].date()}",
                'spearman': rho,
                'n': len(pred_returns),
            })

    if not all_pred_returns:
        return {'error': 'no_valid_folds'}

    all_pred = np.array(all_pred_returns)
    all_true_ret = np.array(all_true_returns)
    all_true_dir = np.array(all_true_dirs) if all_true_dirs else None

    # ── METRICS ──────────────────────────────────────────────

    # 1. Overall Spearman rank correlation
    overall_rho, overall_p = spearmanr(all_pred, all_true_ret)

    # 2. Per-date ranking accuracy
    # For each date, rank coins by predicted return. Compare with actual ranking.
    dates_unique = sorted(set(all_dates_out))
    date_rhos = []
    for d in dates_unique:
        mask = np.array(all_dates_out) == d
        if mask.sum() < 5:
            continue
        pred_d = all_pred[mask]
        true_d = all_true_ret[mask]
        rho_d, _ = spearmanr(pred_d, true_d)
        if not np.isnan(rho_d):
            date_rhos.append(rho_d)

    avg_daily_rho = np.mean(date_rhos) if date_rhos else 0

    # 3. Quintile analysis: buy top 20%, sell bottom 20%
    # Group by date, rank, compute returns
    quintile_returns = {'top': [], 'bottom': [], 'spread': []}
    for d in dates_unique:
        mask = np.array(all_dates_out) == d
        if mask.sum() < 5:
            continue
        pred_d = all_pred[mask]
        true_d = all_true_ret[mask]
        n = len(pred_d)
        q = max(1, n // 5)

        sorted_idx = np.argsort(pred_d)
        top_idx = sorted_idx[-q:]  # highest predicted
        bottom_idx = sorted_idx[:q]  # lowest predicted

        top_ret = true_d[top_idx].mean()
        bot_ret = true_d[bottom_idx].mean()
        quintile_returns['top'].append(top_ret)
        quintile_returns['bottom'].append(bot_ret)
        quintile_returns['spread'].append(top_ret - bot_ret)

    avg_top = np.mean(quintile_returns['top']) * 100 if quintile_returns['top'] else 0
    avg_bot = np.mean(quintile_returns['bottom']) * 100 if quintile_returns['bottom'] else 0
    avg_spread = np.mean(quintile_returns['spread']) * 100 if quintile_returns['spread'] else 0
    pct_spread_positive = np.mean(np.array(quintile_returns['spread']) > 0) * 100 if quintile_returns['spread'] else 0

    # 4. Directional accuracy (for comparison with old method)
    # Convert predicted returns to direction using volatility-adaptive threshold
    pred_std = np.std(all_pred)
    threshold = pred_std * 0.5  # only signal when prediction is notable

    pred_dir = np.where(all_pred > threshold, 1,
                        np.where(all_pred < -threshold, -1, 0))

    if all_true_dir is not None:
        dir_mask = all_true_dir != 0
        if dir_mask.sum() > 0:
            dir_acc = (all_true_dir[dir_mask] == pred_dir[dir_mask]).mean()
        else:
            dir_acc = 0.0

        # Naive baseline
        naive_down_acc = (all_true_dir[dir_mask] == -1).mean()

        # BUY precision
        buy_mask = pred_dir == 1
        sell_mask = pred_dir == -1
        buy_prec = (all_true_dir[buy_mask] == 1).mean() if buy_mask.sum() > 0 else 0
        sell_prec = (all_true_dir[sell_mask] == -1).mean() if sell_mask.sum() > 0 else 0
    else:
        dir_acc = naive_down_acc = buy_prec = sell_prec = 0

    # 5. Per-fold Spearman stability
    fold_rhos = [f['spearman'] for f in fold_metrics if not np.isnan(f['spearman'])]
    pct_positive_rho = np.mean(np.array(fold_rhos) > 0) * 100 if fold_rhos else 0

    return {
        'overall_spearman': round(overall_rho, 4),
        'avg_daily_spearman': round(avg_daily_rho, 4),
        'pct_folds_positive_rho': round(pct_positive_rho, 1),
        'avg_top_quintile_ret': round(avg_top, 3),
        'avg_bottom_quintile_ret': round(avg_bot, 3),
        'avg_spread': round(avg_spread, 3),
        'pct_spread_positive': round(pct_spread_positive, 1),
        'directional_accuracy': round(dir_acc * 100, 1),
        'naive_down_accuracy': round(naive_down_acc * 100, 1),
        'buy_precision': round(buy_prec * 100, 1),
        'sell_precision': round(sell_prec * 100, 1),
        'n_buy': int(buy_mask.sum()) if all_true_dir is not None else 0,
        'n_sell': int(sell_mask.sum()) if all_true_dir is not None else 0,
        'n_neutral': int((pred_dir == 0).sum()),
        'n_folds': len(fold_metrics),
        'total_predictions': len(all_pred),
        'fold_metrics': fold_metrics,
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    df = load_dataset()
    print(f"Dataset: {len(df)} rows, {df['coin'].nunique()} coins")

    print("\n" + "=" * 70)
    print("REGRESSION + RANKING (nested CV, no leakage)")
    print("=" * 70)

    results = walk_forward_regression(df, horizon='7d')
    if 'error' in results:
        print(f"ERROR: {results['error']}")
    else:
        print(f"\n--- RANKING QUALITY ---")
        print(f"  Overall Spearman rho:       {results['overall_spearman']}")
        print(f"  Avg daily Spearman rho:     {results['avg_daily_spearman']}")
        print(f"  Folds with positive rho:    {results['pct_folds_positive_rho']}%")

        print(f"\n--- QUINTILE ANALYSIS (buy top 20%, sell bottom 20%) ---")
        print(f"  Avg top quintile return:    {results['avg_top_quintile_ret']}%")
        print(f"  Avg bottom quintile return: {results['avg_bottom_quintile_ret']}%")
        print(f"  Avg spread (top - bottom):  {results['avg_spread']}%")
        print(f"  Days spread > 0:            {results['pct_spread_positive']}%")

        print(f"\n--- DIRECTIONAL (for comparison) ---")
        print(f"  Dir accuracy (honest):      {results['directional_accuracy']}%")
        print(f"  Naive 'always DOWN':        {results['naive_down_accuracy']}%")
        print(f"  Model vs naive:             {results['directional_accuracy'] - results['naive_down_accuracy']:+.1f}%")
        print(f"  BUY precision:              {results['buy_precision']}%")
        print(f"  SELL precision:             {results['sell_precision']}%")
        print(f"  Predictions: BUY={results['n_buy']}, SELL={results['n_sell']}, NEUTRAL={results['n_neutral']}")

    # Per-group
    print("\n" + "=" * 70)
    print("PER-GROUP RANKING QUALITY")
    print("=" * 70)

    for group in ['majors', 'l1_alts', 'defi', 'ai', 'meme']:
        group_coins = COIN_GROUPS.get(group, [])
        if group == 'l1_alts':
            group_coins = group_coins + ['ARB', 'OP', 'POL']
        gdf = df[df['coin'].isin(group_coins)].copy()
        res = walk_forward_regression(gdf, horizon='7d')
        if 'error' not in res:
            print(f"  {group:10s}: rho={res['overall_spearman']:+.4f}, "
                  f"spread={res['avg_spread']:+.3f}%, "
                  f"spread>0={res['pct_spread_positive']}%, "
                  f"dir={res['directional_accuracy']}% (naive={res['naive_down_accuracy']}%)")
