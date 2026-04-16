"""
FORTIX — Model v5 Retraining
==============================
LightGBM with 83 features including:
- 76 original (price, funding, F&G, macro, derivatives, etc.)
- 8 event features (severity, sentiment, regulatory, hack, days_since_major)
- 4 whale features (volume, tx_count, anomaly, coin-specific)
- 3 exchange flow anomaly features
- 2 stablecoin supply features
- 2 hack impact features

Walk-forward validation ONLY (no random CV).
Per-group models: majors, l1_alts, defi, ai, meme.
"""
import sys, json, time, pickle, os
sys.path.insert(0, 'C:/YT/Factory')

from src.crypto.feature_builder import FeatureBuilder
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
from scipy.stats import spearmanr
from pathlib import Path

print("=" * 60)
print("FORTIX MODEL v5 — RETRAINING WITH EVENT DATA")
print("=" * 60)

# Build dataset with new features
print("\nBuilding dataset (this takes ~2 min)...")
t0 = time.time()
fb = FeatureBuilder()
df = fb.build_dataset()
print(f"Dataset built in {time.time()-t0:.0f}s: {len(df)} rows, {len(df.columns)} columns")
print(f"Coins: {sorted(df['coin'].unique())}")
print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

# Use ALL available features
all_features = fb.FEATURE_COLS
available = [f for f in all_features if f in df.columns]
print(f"\nTotal features: {len(available)}")

# Show new feature stats
print("\n=== NEW FEATURE STATS ===")
new_feats = [f for f in available if f.startswith(('event_', 'whale_', 'exchange_', 'stablecoin_', 'hack_'))]
for f in new_feats:
    col = df[f]
    nz = (col != 0).sum() if col.notna().any() else 0
    print(f"  {f:35s}: {col.notna().mean()*100:5.1f}% avail, {nz:>6} non-zero, "
          f"mean={col.mean():.4f}" if col.notna().any() else f"  {f}: all NaN")

target = 'label_7d'
df_clean = df[available + [target, 'coin', 'date', 'coin_group']].dropna(subset=[target])
df_clean = df_clean.sort_values('date')

# Walk-forward: 70% train, 30% test
dates = sorted(df_clean['date'].unique())
split_idx = int(len(dates) * 0.7)
train_dates = set(dates[:split_idx])
test_dates = set(dates[split_idx:])

train = df_clean[df_clean['date'].isin(train_dates)]
test = df_clean[df_clean['date'].isin(test_dates)]
print(f"\nTrain: {len(train)} rows ({min(train_dates).date()} to {max(train_dates).date()})")
print(f"Test:  {len(test)} rows ({min(test_dates).date()} to {max(test_dates).date()})")

# Prepare data
X_train = train[available].fillna(0)
y_train = train[target]
X_test = test[available].fillna(0)
y_test = test[target]

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)

# ═══════════════════════════════════════════════════════
# ALL COINS MODEL
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("ALL COINS MODEL")
print("=" * 60)

train_data = lgb.Dataset(X_train_s, label=y_train)
valid_data = lgb.Dataset(X_test_s, label=y_test, reference=train_data)

params = {
    'objective': 'regression',
    'metric': 'mae',
    'num_leaves': 31,
    'learning_rate': 0.03,
    'feature_fraction': 0.7,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'verbose': -1,
    'n_jobs': -1,
    'min_child_samples': 50,
    'reg_alpha': 0.5,
    'reg_lambda': 0.5,
}

model = lgb.train(
    params, train_data, num_boost_round=500,
    valid_sets=[valid_data],
    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
)

preds = model.predict(X_test_s)
rho, _ = spearmanr(preds, y_test)
print(f"Spearman correlation: {rho:.4f}")

# Signal analysis at multiple thresholds
print("\n=== SIGNAL ANALYSIS ===")
for thresh_name, thresh in [('0.5%', 0.005), ('1%', 0.01), ('2%', 0.02), ('3%', 0.03)]:
    pred_up = preds > thresh
    pred_down = preds < -thresh
    actual_up = y_test.values > thresh
    actual_down = y_test.values < -thresh

    n_buy = pred_up.sum()
    n_sell = pred_down.sum()
    n_neutral = len(preds) - n_buy - n_sell

    buy_acc = actual_up[pred_up].mean() * 100 if n_buy > 0 else 0
    sell_acc = actual_down[pred_down].mean() * 100 if n_sell > 0 else 0

    correct = ((pred_up & actual_up) | (pred_down & actual_down)).sum()
    actionable = n_buy + n_sell
    dir_acc = correct / actionable * 100 if actionable > 0 else 0

    print(f"  Threshold ±{thresh_name}: "
          f"BUY={n_buy} ({buy_acc:.1f}%), SELL={n_sell} ({sell_acc:.1f}%), "
          f"Dir={dir_acc:.1f}%, Neutral={n_neutral}")

# Feature importance — detailed
importance = model.feature_importance(importance_type='gain')
feat_imp = sorted(zip(available, importance), key=lambda x: x[1], reverse=True)

print(f"\n=== TOP 25 FEATURES (by gain) ===")
max_imp = max(importance) if max(importance) > 0 else 1
for rank, (name, imp) in enumerate(feat_imp[:25], 1):
    bar = '#' * int(imp / max_imp * 30)
    # Mark new features
    marker = ' [NEW]' if name.startswith(('event_', 'whale_', 'exchange_', 'stablecoin_', 'hack_')) else ''
    print(f"  {rank:2d}. {name:30s} {imp:>8.0f} {bar}{marker}")

# Show where new features rank
print(f"\n=== NEW FEATURES RANKING ===")
for rank, (name, imp) in enumerate(feat_imp, 1):
    if name.startswith(('event_', 'whale_', 'exchange_', 'stablecoin_', 'hack_')):
        print(f"  #{rank:2d} {name:35s} importance={imp:.0f}")

# ═══════════════════════════════════════════════════════
# PER-GROUP MODELS
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("PER-GROUP MODELS")
print("=" * 60)

groups = ['majors', 'l1_alts', 'defi', 'meme', 'ai']
group_models = {}

for group in groups:
    g_train = train[train['coin_group'] == group]
    g_test = test[test['coin_group'] == group]

    if len(g_train) < 100 or len(g_test) < 50:
        print(f"\n  {group}: too few samples (train={len(g_train)}, test={len(g_test)})")
        continue

    gX_train = g_train[available].fillna(0)
    gy_train = g_train[target]
    gX_test = g_test[available].fillna(0)
    gy_test = g_test[target]

    gs = StandardScaler()
    gX_train_s = gs.fit_transform(gX_train)
    gX_test_s = gs.transform(gX_test)

    gtrain_d = lgb.Dataset(gX_train_s, label=gy_train)
    gvalid_d = lgb.Dataset(gX_test_s, label=gy_test, reference=gtrain_d)

    # Slightly different params per group
    gparams = params.copy()
    if group in ('ai', 'meme'):
        gparams['min_child_samples'] = 20  # Fewer samples
        gparams['num_leaves'] = 15

    gmodel = lgb.train(
        gparams, gtrain_d, num_boost_round=300,
        valid_sets=[gvalid_d],
        callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)]
    )

    gpreds = gmodel.predict(gX_test_s)
    grho, _ = spearmanr(gpreds, gy_test)

    # BUY/SELL at 2% threshold
    g_buy = gpreds > 0.02
    g_sell = gpreds < -0.02
    g_actual_up = gy_test.values > 0.02
    g_actual_down = gy_test.values < -0.02

    buy_acc = g_actual_up[g_buy].mean() * 100 if g_buy.sum() > 0 else 0
    sell_acc = g_actual_down[g_sell].mean() * 100 if g_sell.sum() > 0 else 0

    correct = ((g_buy & g_actual_up) | (g_sell & g_actual_down)).sum()
    actionable = g_buy.sum() + g_sell.sum()
    dir_acc = correct / actionable * 100 if actionable > 0 else 0

    print(f"\n  {group:10s}: Spearman={grho:+.4f}, Dir={dir_acc:.1f}%")
    print(f"    BUY: {g_buy.sum()} calls, {buy_acc:.1f}% precision")
    print(f"    SELL: {g_sell.sum()} calls, {sell_acc:.1f}% precision")
    print(f"    Train: {len(g_train)}, Test: {len(g_test)}")

    group_models[group] = {'model': gmodel, 'scaler': gs, 'spearman': grho}

# ═══════════════════════════════════════════════════════
# SAVE MODELS
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("SAVING MODELS")
print("=" * 60)

model_dir = Path('data/crypto/models_v5')
model_dir.mkdir(parents=True, exist_ok=True)

# All-coins model
model.save_model(str(model_dir / 'all_7d.lgb'))
with open(model_dir / 'scaler.pkl', 'wb') as f:
    pickle.dump(scaler, f)
with open(model_dir / 'features.json', 'w') as f:
    json.dump(available, f)

# Per-group models
for group, gdata in group_models.items():
    gdata['model'].save_model(str(model_dir / f'{group}_7d.lgb'))
    with open(model_dir / f'{group}_scaler.pkl', 'wb') as f:
        pickle.dump(gdata['scaler'], f)

# Final thresholds at 2%
threshold = 0.02
pred_up = preds > threshold
pred_down = preds < -threshold
actual_up = y_test.values > threshold
actual_down = y_test.values < -threshold
buy_acc = actual_up[pred_up].mean() * 100 if pred_up.sum() > 0 else 0
sell_acc = actual_down[pred_down].mean() * 100 if pred_down.sum() > 0 else 0
correct = ((pred_up & actual_up) | (pred_down & actual_down)).sum()
actionable = pred_up.sum() + pred_down.sum()
dir_acc = correct / actionable * 100 if actionable > 0 else 0

meta = {
    'version': 'v5',
    'features': len(available),
    'feature_names': available,
    'new_features': new_feats,
    'train_rows': len(train),
    'test_rows': len(test),
    'train_period': f"{min(train_dates).date()} to {max(train_dates).date()}",
    'test_period': f"{min(test_dates).date()} to {max(test_dates).date()}",
    'spearman': float(rho),
    'direction_accuracy_2pct': float(dir_acc),
    'buy_precision_2pct': float(buy_acc),
    'sell_precision_2pct': float(sell_acc),
    'buy_calls': int(pred_up.sum()),
    'sell_calls': int(pred_down.sum()),
    'top_features': [f[0] for f in feat_imp[:15]],
    'events_in_db': 257,
    'trained_at': pd.Timestamp.now().isoformat(),
}
with open(model_dir / 'meta.json', 'w') as f:
    json.dump(meta, f, indent=2)

print(f"\nModels saved to {model_dir}")
print(f"Features: {len(available)}")
print(f"Spearman: {rho:.4f}")
print(f"Buy precision: {buy_acc:.1f}% ({pred_up.sum()} calls)")
print(f"Sell precision: {sell_acc:.1f}% ({pred_down.sum()} calls)")

# ═══════════════════════════════════════════════════════
# COMPARISON WITH v4
# ═══════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("v4 → v5 COMPARISON")
print("=" * 60)
try:
    with open('data/crypto/models_v4/meta.json') as f:
        v4 = json.load(f)
    print(f"{'Metric':25s} {'v4':>10s} {'v5':>10s} {'Change':>10s}")
    print("-" * 58)
    print(f"{'Features':25s} {v4.get('features',42):>10d} {len(available):>10d}")
    print(f"{'Spearman':25s} {v4.get('spearman',0):>10.4f} {rho:>10.4f} {rho - v4.get('spearman',0):>+10.4f}")
    print(f"{'Buy precision':25s} {v4.get('buy_precision',0):>9.1f}% {buy_acc:>9.1f}%")
    print(f"{'Sell precision':25s} {v4.get('sell_precision',0):>9.1f}% {sell_acc:>9.1f}%")
    print(f"{'Buy calls':25s} {v4.get('buy_calls','?'):>10} {pred_up.sum():>10d}")
    print(f"{'Sell calls':25s} {v4.get('sell_calls','?'):>10} {pred_down.sum():>10d}")
except Exception as e:
    print(f"v4 meta not found: {e}")

print("\nDone!")
