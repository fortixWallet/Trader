"""Phase 3: LightGBM retraining with expanded macro features."""
import sys, json, time
sys.path.insert(0, 'C:/YT/Factory')
from src.crypto.feature_builder import FeatureBuilder
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.preprocessing import StandardScaler
from scipy.stats import spearmanr

print("Building dataset...")
fb = FeatureBuilder()
df = fb.build_dataset()
print(f"Dataset: {len(df)} rows")

# Top predictive features (from correlation analysis + all available)
top_features = [
    'liq_ratio', 'btc_ret_30d', 'defi_tvl_change_30d', 'gtrend_crypto',
    'ret_30d', 'sp500', 'n_coins_new_high_30d', 'nasdaq', 'cpi_yoy',
    'liq_long_24h', 'ret_14d', 'funding_rate_7d_avg', 'cq_reserve_change_7d',
    'ma50_dist', 'funding_rate', 'rsi_14', 'coinbase_premium', 'fg_value',
    'high_low_range_7d', 'sp500_ret_7d', 'ret_7d', 'bb_position',
    'volatility_7d', 'volume_ratio_7d', 'etf_flow_7d_avg',
    'corr_btc_30d', 'btc_ret_7d', 'pct_above_ma50',
    'vix', 'yield_curve', 'fomc_days_until', 'day_of_week',
    'dxy', 'nasdaq_ret_7d', 'ret_1d', 'ret_3d',
    'volatility_30d', 'ma200_dist', 'fg_change_7d',
    'oi_change_7d', 'ls_long_pct', 'taker_ratio',
]

available = [f for f in top_features if f in df.columns]
print(f"Using {len(available)} features")

target = 'label_7d'
df_clean = df[available + [target, 'coin', 'date', 'coin_group']].dropna(subset=[target])
df_clean = df_clean.sort_values('date')

# Walk-forward: train 70%, test 30%
dates = sorted(df_clean['date'].unique())
split_idx = int(len(dates) * 0.7)
train_dates = set(dates[:split_idx])
test_dates = set(dates[split_idx:])

train = df_clean[df_clean['date'].isin(train_dates)]
test = df_clean[df_clean['date'].isin(test_dates)]

print(f"Train: {len(train)} ({len(train_dates)} dates)")
print(f"Test:  {len(test)} ({len(test_dates)} dates)")

X_train = train[available].fillna(0)
y_train = train[target]
X_test = test[available].fillna(0)
y_test = test[target]

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)

# === Per-group training ===
groups = ['majors', 'l1_alts', 'defi', 'meme', 'ai']
results = {}

# First: all coins together
print("\n=== ALL COINS ===")
train_data = lgb.Dataset(X_train_s, label=y_train)
valid_data = lgb.Dataset(X_test_s, label=y_test, reference=train_data)

params = {
    'objective': 'regression', 'metric': 'mae',
    'num_leaves': 31, 'learning_rate': 0.03,
    'feature_fraction': 0.7, 'bagging_fraction': 0.7, 'bagging_freq': 5,
    'verbose': -1, 'n_jobs': -1, 'min_child_samples': 50,
    'reg_alpha': 0.5, 'reg_lambda': 0.5,
}

model = lgb.train(params, train_data, num_boost_round=300,
    valid_sets=[valid_data], callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)])

preds = model.predict(X_test_s)
rho, _ = spearmanr(preds, y_test)
print(f"Spearman: {rho:.4f}")

threshold = 0.02
pred_up = preds > threshold
pred_down = preds < -threshold
actual_up = y_test.values > threshold
actual_down = y_test.values < -threshold

correct = ((pred_up & actual_up) | (pred_down & actual_down)).sum()
actionable = (pred_up | pred_down).sum()
if actionable > 0:
    dir_acc = correct / actionable * 100
    print(f"Direction: {dir_acc:.1f}% ({correct}/{actionable})")

if pred_up.sum() > 0:
    buy_acc = actual_up[pred_up].mean() * 100
    print(f"BUY:  {buy_acc:.1f}% ({pred_up.sum()} calls)")
if pred_down.sum() > 0:
    sell_acc = actual_down[pred_down].mean() * 100
    print(f"SELL: {sell_acc:.1f}% ({pred_down.sum()} calls)")

# Feature importance
importance = model.feature_importance(importance_type='gain')
feat_imp = sorted(zip(available, importance), key=lambda x: x[1], reverse=True)
print(f"\nTOP 15 FEATURES:")
for name, imp in feat_imp[:15]:
    bar = '#' * min(int(imp / max(importance) * 30), 30)
    print(f"  {name:25} {imp:>8.0f} {bar}")

# Now per-group
print("\n=== PER-GROUP RESULTS ===")
for group in groups:
    g_train = train[train['coin_group'] == group]
    g_test = test[test['coin_group'] == group]
    if len(g_train) < 100 or len(g_test) < 50:
        print(f"{group}: too few samples")
        continue

    gX_train = g_train[available].fillna(0)
    gy_train = g_train[target]
    gX_test = g_test[available].fillna(0)
    gy_test = g_test[target]

    gs = StandardScaler()
    gX_train_s = gs.fit_transform(gX_train)
    gX_test_s = gs.transform(gX_test)

    gtrain = lgb.Dataset(gX_train_s, label=gy_train)
    gvalid = lgb.Dataset(gX_test_s, label=gy_test, reference=gtrain)

    gmodel = lgb.train(params, gtrain, num_boost_round=200,
        valid_sets=[gvalid], callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)])

    gpreds = gmodel.predict(gX_test_s)
    grho, _ = spearmanr(gpreds, gy_test)

    g_pred_up = gpreds > threshold
    g_pred_down = gpreds < -threshold
    g_actual_up = gy_test.values > threshold
    g_actual_down = gy_test.values < -threshold
    g_correct = ((g_pred_up & g_actual_up) | (g_pred_down & g_actual_down)).sum()
    g_act = (g_pred_up | g_pred_down).sum()
    g_dir = g_correct / g_act * 100 if g_act > 0 else 0

    print(f"  {group:10}: Spearman={grho:+.4f} Dir={g_dir:.1f}% ({g_act} calls, {len(g_test)} test)")

# Save model
print("\nSaving model...")
import pickle, os
from pathlib import Path

model_dir = Path('data/crypto/models_v4')
model_dir.mkdir(parents=True, exist_ok=True)
model.save_model(str(model_dir / 'all_7d.lgb'))

with open(model_dir / 'scaler.pkl', 'wb') as f:
    pickle.dump(scaler, f)
with open(model_dir / 'features.json', 'w') as f:
    json.dump(available, f)
with open(model_dir / 'meta.json', 'w') as f:
    json.dump({
        'features': len(available),
        'train_rows': len(train),
        'test_rows': len(test),
        'spearman': rho,
        'direction_accuracy': dir_acc if actionable > 0 else 0,
        'buy_precision': float(buy_acc) if pred_up.sum() > 0 else 0,
        'sell_precision': float(sell_acc) if pred_down.sum() > 0 else 0,
        'top_features': [f[0] for f in feat_imp[:10]],
        'trained_at': pd.Timestamp.now().isoformat(),
    }, f, indent=2)

print("Done!")
