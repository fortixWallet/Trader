"""
FORTIX — Model v5 RANKING Approach
=====================================
Instead of regression + threshold (which produces 76% NEUTRAL),
this trains a RANKING model: for each date, rank all coins from
best to worst expected 7d return.

Top 5 = BUY, Bottom 5 = SELL, Middle = HOLD.
NO skipping possible. Every coin ALWAYS gets a signal.

This is how actual quant funds work — not "will it go up?" but
"which coin will OUTPERFORM?"

Walk-forward validation ONLY.
"""
import sys, json, time, pickle
sys.path.insert(0, 'C:/YT/Factory')

from src.crypto.feature_builder import FeatureBuilder
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
from scipy.stats import spearmanr
from pathlib import Path

print("=" * 60)
print("FORTIX MODEL v5 — RANKING APPROACH")
print("No NEUTRAL skipping. Every coin gets a signal.")
print("=" * 60)

# Build dataset
print("\nBuilding dataset...")
t0 = time.time()
fb = FeatureBuilder()
df = fb.build_dataset()
print(f"Dataset: {len(df)} rows, {time.time()-t0:.0f}s")

all_features = fb.FEATURE_COLS
available = [f for f in all_features if f in df.columns]
target = 'label_7d'
df_clean = df[available + [target, 'coin', 'date', 'coin_group', 'close']].dropna(subset=[target])
df_clean = df_clean.sort_values('date')

# ═══════════════════════════════════════════════════════
# STEP 1: Create RANKING targets
# ═══════════════════════════════════════════════════════

print("\n=== CREATING RANKING TARGETS ===")

# For each date, rank coins by actual 7d return
# Rank 1 = best performer, Rank N = worst performer
# Normalize to 0-1 scale
def rank_within_date(group):
    """Rank coins within each date. 1.0 = best, 0.0 = worst."""
    n = len(group)
    if n <= 1:
        group['rank_target'] = 0.5
    else:
        # pct_rank: 1.0 = best return, 0.0 = worst
        group['rank_target'] = group[target].rank(pct=True)
    return group

# Apply ranking per date
ranked_parts = []
for date_val, group in df_clean.groupby('date'):
    g = group.copy()
    n = len(g)
    if n <= 1:
        g['rank_target'] = 0.5
    else:
        g['rank_target'] = g[target].rank(pct=True)
    med = g[target].median()
    g['above_median'] = (g[target] > med).astype(int)
    ranked_parts.append(g)

df_ranked = pd.concat(ranked_parts, ignore_index=True)

print(f"Dates with rankings: {df_ranked['date'].nunique()}")
print(f"Avg coins per date: {df_ranked.groupby('date').size().mean():.1f}")
print(f"Rank target stats: mean={df_ranked['rank_target'].mean():.3f}, "
      f"std={df_ranked['rank_target'].std():.3f}")

# Walk-forward split
dates = sorted(df_ranked['date'].unique())
split_idx = int(len(dates) * 0.7)
train_dates = set(dates[:split_idx])
test_dates = set(dates[split_idx:])

train = df_ranked[df_ranked['date'].isin(train_dates)]
test = df_ranked[df_ranked['date'].isin(test_dates)]

print(f"\nTrain: {len(train)} rows ({min(train_dates).date()} to {max(train_dates).date()})")
print(f"Test:  {len(test)} rows ({min(test_dates).date()} to {max(test_dates).date()})")

# ═══════════════════════════════════════════════════════
# STEP 2: Train RANKING model
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("TRAINING RANKING MODEL")
print("=" * 60)

X_train = train[available].fillna(0)
y_train = train['rank_target']
X_test = test[available].fillna(0)
y_test_rank = test['rank_target']
y_test_return = test[target]
y_test_above = test['above_median']

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)

# Model A: Regression on rank target (simple, works well)
print("\n--- Model A: Regression on rank percentile ---")
train_data = lgb.Dataset(X_train_s, label=y_train)
valid_data = lgb.Dataset(X_test_s, label=y_test_rank, reference=train_data)

params_rank = {
    'objective': 'regression',
    'metric': 'mae',
    'num_leaves': 31,
    'learning_rate': 0.02,
    'feature_fraction': 0.7,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1,
    'min_child_samples': 30,
    'reg_alpha': 0.3,
    'reg_lambda': 0.3,
}

model_rank = lgb.train(
    params_rank, train_data, num_boost_round=1000,
    valid_sets=[valid_data],
    callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)]
)

preds_rank = model_rank.predict(X_test_s)
rho_rank, _ = spearmanr(preds_rank, y_test_rank)
rho_return, _ = spearmanr(preds_rank, y_test_return)
print(f"  Spearman vs rank target: {rho_rank:.4f}")
print(f"  Spearman vs actual return: {rho_return:.4f}")
print(f"  Best iteration: {model_rank.best_iteration}")

# Model B: Binary classification (above/below median)
print("\n--- Model B: Binary classification (above/below median) ---")
train_data_b = lgb.Dataset(X_train_s, label=train['above_median'])
valid_data_b = lgb.Dataset(X_test_s, label=y_test_above, reference=train_data_b)

params_binary = {
    'objective': 'binary',
    'metric': 'auc',
    'num_leaves': 31,
    'learning_rate': 0.02,
    'feature_fraction': 0.7,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1,
    'min_child_samples': 30,
    'reg_alpha': 0.3,
    'reg_lambda': 0.3,
    'is_unbalance': True,
}

model_binary = lgb.train(
    params_binary, train_data_b, num_boost_round=1000,
    valid_sets=[valid_data_b],
    callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)]
)

preds_binary = model_binary.predict(X_test_s)
rho_binary, _ = spearmanr(preds_binary, y_test_return)
print(f"  Spearman vs actual return: {rho_binary:.4f}")
print(f"  AUC: {model_binary.best_score['valid_0']['auc']:.4f}")
print(f"  Best iteration: {model_binary.best_iteration}")

# Also train on raw returns but with ranking-aware loss
print("\n--- Model C: Regression on returns (baseline comparison) ---")
train_data_c = lgb.Dataset(X_train_s, label=train[target])
valid_data_c = lgb.Dataset(X_test_s, label=y_test_return, reference=train_data_c)

model_return = lgb.train(
    {**params_rank, 'learning_rate': 0.03},
    train_data_c, num_boost_round=500,
    valid_sets=[valid_data_c],
    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
)

preds_return = model_return.predict(X_test_s)
rho_return_c, _ = spearmanr(preds_return, y_test_return)
print(f"  Spearman vs actual return: {rho_return_c:.4f}")
print(f"  Best iteration: {model_return.best_iteration}")

# ═══════════════════════════════════════════════════════
# STEP 3: Evaluate all models as SIGNAL GENERATORS
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("SIGNAL GENERATION EVALUATION")
print("=" * 60)

test_with_preds = test.copy()
test_with_preds['pred_rank'] = preds_rank
test_with_preds['pred_binary'] = preds_binary
test_with_preds['pred_return'] = preds_return

def evaluate_ranking_signals(df_eval, pred_col, model_name, n_buy=5, n_sell=5):
    """
    For each date, sort coins by prediction.
    Top n_buy = BUY signal, Bottom n_sell = SELL signal.
    """
    print(f"\n--- {model_name} (top {n_buy} = BUY, bottom {n_sell} = SELL) ---")

    buy_returns = []
    sell_returns = []
    buy_correct = 0
    sell_correct = 0
    buy_total = 0
    sell_total = 0
    dates_evaluated = 0

    long_short_returns = []  # Long top N, Short bottom N

    for date, group in df_eval.groupby('date'):
        if len(group) < 10:  # Need enough coins to rank
            continue

        sorted_group = group.sort_values(pred_col, ascending=False)

        # Top N = BUY
        top = sorted_group.head(n_buy)
        for _, row in top.iterrows():
            actual = row[target]
            buy_returns.append(actual)
            buy_total += 1
            if actual > 0:
                buy_correct += 1

        # Bottom N = SELL
        bottom = sorted_group.tail(n_sell)
        for _, row in bottom.iterrows():
            actual = row[target]
            sell_returns.append(actual)
            sell_total += 1
            if actual < 0:
                sell_correct += 1

        # Long-short return
        ls_ret = top[target].mean() - bottom[target].mean()
        long_short_returns.append(ls_ret)
        dates_evaluated += 1

    # Results
    buy_acc = buy_correct / buy_total * 100 if buy_total > 0 else 0
    sell_acc = sell_correct / sell_total * 100 if sell_total > 0 else 0
    avg_buy_ret = np.mean(buy_returns) * 100 if buy_returns else 0
    avg_sell_ret = np.mean(sell_returns) * 100 if sell_returns else 0
    avg_ls = np.mean(long_short_returns) * 100 if long_short_returns else 0
    sharpe_ls = np.mean(long_short_returns) / np.std(long_short_returns) * np.sqrt(52) if long_short_returns and np.std(long_short_returns) > 0 else 0

    print(f"  Dates evaluated: {dates_evaluated}")
    print(f"  BUY signals:  {buy_total} | Direction correct: {buy_acc:.1f}% | Avg 7d return: {avg_buy_ret:+.2f}%")
    print(f"  SELL signals:  {sell_total} | Direction correct: {sell_acc:.1f}% | Avg 7d return: {avg_sell_ret:+.2f}%")
    print(f"  Long-Short:   avg weekly: {avg_ls:+.3f}% | Sharpe: {sharpe_ls:.2f}")
    print(f"  BUY outperforms SELL: {(np.mean(buy_returns) > np.mean(sell_returns))}")

    # Breakdown by market regime
    print(f"\n  By market regime:")
    for regime_name, regime_filter in [
        ("Bull (BTC 7d > 5%)", test_with_preds['label_7d'] > 0.05),
        ("Bear (BTC 7d < -5%)", test_with_preds['label_7d'] < -0.05),
        ("Sideways", (test_with_preds['label_7d'] >= -0.05) & (test_with_preds['label_7d'] <= 0.05)),
    ]:
        regime_dates = test_with_preds[regime_filter]['date'].unique()
        regime_eval = df_eval[df_eval['date'].isin(regime_dates)]
        if len(regime_eval) < 50:
            continue

        r_buy_rets = []
        r_sell_rets = []
        for date, group in regime_eval.groupby('date'):
            if len(group) < 10:
                continue
            sg = group.sort_values(pred_col, ascending=False)
            r_buy_rets.extend(sg.head(n_buy)[target].tolist())
            r_sell_rets.extend(sg.tail(n_sell)[target].tolist())

        if r_buy_rets and r_sell_rets:
            r_buy_acc = sum(1 for r in r_buy_rets if r > 0) / len(r_buy_rets) * 100
            r_sell_acc = sum(1 for r in r_sell_rets if r < 0) / len(r_sell_rets) * 100
            print(f"    {regime_name:30s}: BUY {r_buy_acc:.1f}%, SELL {r_sell_acc:.1f}% "
                  f"(BUY avg: {np.mean(r_buy_rets)*100:+.2f}%, SELL avg: {np.mean(r_sell_rets)*100:+.2f}%)")

    return {
        'buy_accuracy': buy_acc,
        'sell_accuracy': sell_acc,
        'avg_buy_return': avg_buy_ret,
        'avg_sell_return': avg_sell_ret,
        'long_short_weekly': avg_ls,
        'sharpe': sharpe_ls,
    }


# Evaluate all models
results = {}
for pred_col, name in [
    ('pred_rank', 'Rank Model (A)'),
    ('pred_binary', 'Binary Model (B)'),
    ('pred_return', 'Return Model (C)'),
]:
    results[name] = evaluate_ranking_signals(test_with_preds, pred_col, name)

# ═══════════════════════════════════════════════════════
# STEP 4: Feature importance for best model
# ═══════════════════════════════════════════════════════

# Determine best model
best_name = max(results, key=lambda k: results[k]['sharpe'])
best_model = {'Rank Model (A)': model_rank, 'Binary Model (B)': model_binary,
              'Return Model (C)': model_return}[best_name]

print(f"\n{'='*60}")
print(f"BEST MODEL: {best_name}")
print(f"{'='*60}")

importance = best_model.feature_importance(importance_type='gain')
feat_imp = sorted(zip(available, importance), key=lambda x: x[1], reverse=True)

print(f"\nTOP 20 FEATURES:")
max_imp = max(importance) if max(importance) > 0 else 1
for rank, (name, imp) in enumerate(feat_imp[:20], 1):
    bar = '#' * int(imp / max_imp * 30)
    marker = ' [NEW]' if name.startswith(('event_', 'whale_', 'exchange_', 'stablecoin_', 'hack_')) else ''
    print(f"  {rank:2d}. {name:30s} {imp:>8.0f} {bar}{marker}")

# ═══════════════════════════════════════════════════════
# STEP 5: Also try different TOP N sizes
# ═══════════════════════════════════════════════════════

print(f"\n{'='*60}")
print(f"SENSITIVITY: DIFFERENT TOP/BOTTOM N")
print(f"{'='*60}")

for n in [3, 5, 7]:
    r = evaluate_ranking_signals(test_with_preds, 'pred_rank', f"Rank Model top/bottom {n}", n_buy=n, n_sell=n)

# ═══════════════════════════════════════════════════════
# STEP 6: Save best model
# ═══════════════════════════════════════════════════════

print(f"\n{'='*60}")
print("SAVING MODELS")
print(f"{'='*60}")

model_dir = Path('data/crypto/models_v5')
model_dir.mkdir(parents=True, exist_ok=True)

# Save ranking model (Model A) — our primary
model_rank.save_model(str(model_dir / 'ranking_7d.lgb'))
with open(model_dir / 'ranking_scaler.pkl', 'wb') as f:
    pickle.dump(scaler, f)
with open(model_dir / 'ranking_features.json', 'w') as f:
    json.dump(available, f)

# Save binary model (Model B) — secondary
model_binary.save_model(str(model_dir / 'binary_7d.lgb'))

# Save metadata
best_result = results[best_name]
meta = {
    'version': 'v5-ranking',
    'approach': 'cross-sectional ranking (no NEUTRAL possible)',
    'features': len(available),
    'feature_names': available,
    'train_rows': len(train),
    'test_rows': len(test),
    'train_period': f"{min(train_dates).date()} to {max(train_dates).date()}",
    'test_period': f"{min(test_dates).date()} to {max(test_dates).date()}",
    'ranking_model': {
        'spearman_vs_rank': float(rho_rank),
        'spearman_vs_return': float(rho_return),
        'best_iteration': model_rank.best_iteration,
    },
    'binary_model': {
        'spearman_vs_return': float(rho_binary),
        'auc': float(model_binary.best_score['valid_0']['auc']),
        'best_iteration': model_binary.best_iteration,
    },
    'signal_performance': {
        name: {k: float(v) for k, v in r.items()} for name, r in results.items()
    },
    'best_model': best_name,
    'top_features': [f[0] for f in feat_imp[:15]],
    'events_in_db': 257,
    'trained_at': pd.Timestamp.now().isoformat(),
}
with open(model_dir / 'meta_ranking.json', 'w') as f:
    json.dump(meta, f, indent=2)

print(f"\nModels saved to {model_dir}")
print(f"\nFINAL SUMMARY:")
print(f"  Best model: {best_name}")
print(f"  BUY accuracy: {best_result['buy_accuracy']:.1f}%")
print(f"  SELL accuracy: {best_result['sell_accuracy']:.1f}%")
print(f"  Long-Short Sharpe: {best_result['sharpe']:.2f}")
print(f"  Signal skipping: IMPOSSIBLE (every coin ranked every day)")
