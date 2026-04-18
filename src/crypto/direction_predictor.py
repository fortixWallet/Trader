"""
FORTIX Direction Predictor
===========================
Predicts price direction 1-2h ahead using CoinGlass derivatives data.

Features (all at 30min resolution):
- OI change rate + OI vs price divergence
- CVD (futures + spot) trend + divergence from price
- Taker buy/sell ratio + momentum
- Top trader L/S shift (smart money)
- Global L/S shift (contrarian retail)
- Funding rate level + velocity
- Orderbook bid/ask imbalance + change
- Liquidation imbalance (long vs short)
- Price momentum (multiple windows)
- RSI

Target: price direction 1h and 2h ahead (UP/DOWN)

Standalone — run to analyze, backtest, and train.
"""

import sqlite3
import numpy as np
import logging
from pathlib import Path
from datetime import datetime

log = logging.getLogger('direction_predictor')
DB_PATH = Path('data/crypto/market.db')


def _get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def build_feature_matrix(coin='BTC', lookback_hours=4):
    """Build aligned feature matrix from all prediction tables.

    Returns: timestamps[], features[], labels_1h[], labels_2h[], feature_names[]
    """
    conn = _get_conn()

    # 1. Get price data (1h candles) as backbone
    prices = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='1h' "
        "ORDER BY timestamp ASC", (coin,)
    ).fetchall()

    if len(prices) < 50:
        conn.close()
        return None

    price_ts = {p[0]: p[1] for p in prices}
    all_ts = sorted(price_ts.keys())

    # 2. Load all prediction tables into dicts {timestamp: values}
    def load_table(table, cols, coin=coin):
        rows = conn.execute(
            f"SELECT timestamp, {','.join(cols)} FROM {table} WHERE coin=? ORDER BY timestamp",
            (coin,)
        ).fetchall()
        result = {}
        for r in rows:
            ts = r[0]
            # Round to nearest hour for alignment
            ts_hour = (ts // 3600) * 3600
            if ts_hour not in result:
                result[ts_hour] = list(r[1:])
            else:
                # Keep latest 30min reading for the hour
                result[ts_hour] = list(r[1:])
        return result

    oi_data = load_table('pred_oi_history', ['open_interest', 'h', 'l', 'c'])
    cvd_f = load_table('pred_cvd_futures', ['buy_volume', 'sell_volume', 'cvd'])
    cvd_s = load_table('pred_cvd_spot', ['buy_volume', 'sell_volume', 'cvd'])
    taker = load_table('pred_taker_volume', ['buy_volume', 'sell_volume', 'ratio'])
    top_ls = load_table('pred_top_trader_ls', ['long_ratio', 'short_ratio', 'long_short_ratio'])
    global_ls = load_table('pred_global_ls', ['long_ratio', 'short_ratio', 'long_short_ratio'])
    funding = load_table('pred_funding_oi_weight', ['funding_rate'])
    ob_depth = load_table('pred_orderbook_depth', ['bid_amount', 'ask_amount', 'imbalance'])
    liq = load_table('pred_liq_history', ['long_liq_usd', 'short_liq_usd'])

    conn.close()

    # 3. Build features for each hourly timestamp
    feature_names = [
        # OI features
        'oi_change_1h', 'oi_change_4h', 'oi_range',
        # CVD features
        'cvd_futures', 'cvd_futures_change_1h', 'cvd_spot',
        'cvd_divergence',  # futures CVD vs spot CVD direction
        # Taker features
        'taker_ratio', 'taker_ratio_change_1h', 'taker_buy_dominance',
        # Top trader L/S
        'top_ls_ratio', 'top_ls_change_1h', 'top_ls_extreme',
        # Global L/S (retail)
        'global_ls_ratio', 'global_ls_change_1h', 'global_ls_extreme',
        # Funding
        'funding_rate', 'funding_velocity_4h',
        # Orderbook
        'ob_imbalance', 'ob_imbalance_change_1h',
        # Liquidations
        'liq_ratio', 'liq_total',
        # Price features
        'price_mom_1h', 'price_mom_4h', 'price_rsi_14',
        # Divergences (price vs indicator)
        'oi_price_div', 'cvd_price_div', 'taker_price_div',
    ]

    timestamps = []
    features = []
    labels_1h = []
    labels_2h = []

    # Only use timestamps where we have CoinGlass data
    cg_timestamps = set(oi_data.keys()) & set(cvd_f.keys()) & set(taker.keys()) & \
                    set(top_ls.keys()) & set(global_ls.keys()) & set(ob_depth.keys())
    if not cg_timestamps:
        conn.close()
        return None

    for i in range(max(14, lookback_hours * 2), len(all_ts) - 2):
        ts = all_ts[i]
        if ts not in cg_timestamps:
            continue
        ts_prev1 = all_ts[i - 1] if i >= 1 else ts  # 1h ago
        ts_prev4 = all_ts[i - 4] if i >= 4 else ts  # 4h ago

        price_now = price_ts[ts]
        price_1h_ago = price_ts.get(ts_prev1, price_now)
        price_4h_ago = price_ts.get(ts_prev4, price_now)
        price_1h_later = price_ts.get(all_ts[i + 1], None)
        price_2h_later = price_ts.get(all_ts[i + 2], None) if i + 2 < len(all_ts) else None

        if not price_1h_later or not price_2h_later:
            continue
        if price_now == 0:
            continue

        # OI features
        oi_now = oi_data.get(ts, [0, 0, 0, 0])
        oi_1h = oi_data.get(ts_prev1, [0, 0, 0, 0])
        oi_4h = oi_data.get(ts_prev4, [0, 0, 0, 0])
        oi_close = float(oi_now[3] or oi_now[0] or 0)
        oi_1h_close = float(oi_1h[3] or oi_1h[0] or 0)
        oi_4h_close = float(oi_4h[3] or oi_4h[0] or 0)
        oi_change_1h = (oi_close / oi_1h_close - 1) * 100 if oi_1h_close > 0 else 0
        oi_change_4h = (oi_close / oi_4h_close - 1) * 100 if oi_4h_close > 0 else 0
        oi_h = float(oi_now[1] or 0)
        oi_l = float(oi_now[2] or 0)
        oi_range = (oi_h / oi_l - 1) * 100 if oi_l > 0 else 0

        # CVD features
        cvd_f_now = cvd_f.get(ts, [0, 0, 0])
        cvd_f_1h = cvd_f.get(ts_prev1, [0, 0, 0])
        cvd_s_now = cvd_s.get(ts, [0, 0, 0])
        cvd_futures_val = float(cvd_f_now[2] or 0)
        cvd_futures_1h = float(cvd_f_1h[2] or 0)
        cvd_futures_change = cvd_futures_val - cvd_futures_1h
        cvd_spot_val = float(cvd_s_now[2] or 0)
        # Divergence: spot buying but futures selling = real accumulation
        cvd_divergence = 1 if (cvd_spot_val > 0 and cvd_futures_val < 0) else \
                        -1 if (cvd_spot_val < 0 and cvd_futures_val > 0) else 0

        # Taker features
        taker_now = taker.get(ts, [0, 0, 1.0])
        taker_1h = taker.get(ts_prev1, [0, 0, 1.0])
        taker_ratio_val = float(taker_now[2] or 1.0)
        taker_ratio_1h = float(taker_1h[2] or 1.0)
        taker_ratio_change = taker_ratio_val - taker_ratio_1h
        taker_buy = float(taker_now[0] or 0)
        taker_sell = float(taker_now[1] or 0)
        taker_total = taker_buy + taker_sell
        taker_buy_dom = taker_buy / taker_total if taker_total > 0 else 0.5

        # Top trader L/S
        top_now = top_ls.get(ts, [50, 50, 1.0])
        top_1h = top_ls.get(ts_prev1, [50, 50, 1.0])
        top_ls_ratio_val = float(top_now[2] or 1.0)
        top_ls_1h = float(top_1h[2] or 1.0)
        top_ls_change = top_ls_ratio_val - top_ls_1h
        top_ls_extreme = 1 if top_ls_ratio_val > 1.5 else (-1 if top_ls_ratio_val < 0.67 else 0)

        # Global L/S (retail)
        glob_now = global_ls.get(ts, [50, 50, 1.0])
        glob_1h = global_ls.get(ts_prev1, [50, 50, 1.0])
        glob_ls_ratio_val = float(glob_now[2] or 1.0)
        glob_ls_1h = float(glob_1h[2] or 1.0)
        glob_ls_change = glob_ls_ratio_val - glob_ls_1h
        glob_ls_extreme = 1 if glob_ls_ratio_val > 1.5 else (-1 if glob_ls_ratio_val < 0.67 else 0)

        # Funding
        fund_now = funding.get(ts, [0])
        fund_4h = funding.get(ts_prev4, [0])
        funding_rate_val = float(fund_now[0] or 0)
        funding_4h_val = float(fund_4h[0] or 0)
        funding_velocity = funding_rate_val - funding_4h_val

        # Orderbook
        ob_now = ob_depth.get(ts, [0, 0, 0])
        ob_1h = ob_depth.get(ts_prev1, [0, 0, 0])
        ob_imbalance_val = float(ob_now[2] or 0)
        ob_imbalance_1h = float(ob_1h[2] or 0)
        ob_imbalance_change = ob_imbalance_val - ob_imbalance_1h

        # Liquidations
        liq_now = liq.get(ts, [0, 0])
        long_liq = float(liq_now[0] or 0)
        short_liq = float(liq_now[1] or 0)
        liq_total = long_liq + short_liq
        liq_ratio = (long_liq - short_liq) / liq_total if liq_total > 0 else 0

        # Price features
        price_mom_1h = (price_now / price_1h_ago - 1) * 100 if price_1h_ago > 0 else 0
        price_mom_4h = (price_now / price_4h_ago - 1) * 100 if price_4h_ago > 0 else 0

        # RSI
        rsi_window = [price_ts.get(all_ts[j], 0) for j in range(max(0, i - 14), i + 1)]
        rsi_window = [x for x in rsi_window if x > 0]
        if len(rsi_window) >= 14:
            deltas = np.diff(rsi_window)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_gain = np.mean(gains[-14:])
            avg_loss = np.mean(losses[-14:])
            rs = avg_gain / (avg_loss + 1e-10)
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 50.0

        # Divergences: price up but indicator down (or vice versa)
        oi_price_div = 1 if (price_mom_1h > 0.2 and oi_change_1h < -0.5) else \
                      -1 if (price_mom_1h < -0.2 and oi_change_1h > 0.5) else 0

        cvd_price_div = 1 if (price_mom_1h > 0.2 and cvd_futures_change < 0) else \
                       -1 if (price_mom_1h < -0.2 and cvd_futures_change > 0) else 0

        taker_price_div = 1 if (price_mom_1h > 0.2 and taker_ratio_val < 0.95) else \
                         -1 if (price_mom_1h < -0.2 and taker_ratio_val > 1.05) else 0

        # Build feature vector
        feat = [
            oi_change_1h, oi_change_4h, oi_range,
            cvd_futures_val, cvd_futures_change, cvd_spot_val,
            cvd_divergence,
            taker_ratio_val, taker_ratio_change, taker_buy_dom,
            top_ls_ratio_val, top_ls_change, top_ls_extreme,
            glob_ls_ratio_val, glob_ls_change, glob_ls_extreme,
            funding_rate_val, funding_velocity,
            ob_imbalance_val, ob_imbalance_change,
            liq_ratio, liq_total,
            price_mom_1h, price_mom_4h, rsi,
            oi_price_div, cvd_price_div, taker_price_div,
        ]

        # Labels: 1 = UP, 0 = DOWN
        label_1h = 1 if price_1h_later > price_now else 0
        label_2h = 1 if price_2h_later > price_now else 0

        timestamps.append(ts)
        features.append(feat)
        labels_1h.append(label_1h)
        labels_2h.append(label_2h)

    return (np.array(timestamps), np.array(features),
            np.array(labels_1h), np.array(labels_2h), feature_names)


def analyze_features(coin='BTC'):
    """Analyze feature importance and correlation with direction."""
    result = build_feature_matrix(coin)
    if result is None:
        print(f"Not enough data for {coin}")
        return

    timestamps, X, y1h, y2h, names = result
    print(f"\n{'='*70}")
    print(f"  DIRECTION PREDICTION ANALYSIS: {coin}")
    print(f"  {len(timestamps)} samples, {X.shape[1]} features")
    print(f"  Period: {datetime.fromtimestamp(timestamps[0]):%Y-%m-%d} → {datetime.fromtimestamp(timestamps[-1]):%Y-%m-%d}")
    print(f"  1h UP rate: {y1h.mean():.1%} | 2h UP rate: {y2h.mean():.1%}")
    print(f"{'='*70}")

    # Feature correlation with 1h direction
    print(f"\nFEATURE CORRELATION WITH 1H DIRECTION (sorted by |corr|):")
    print(f"{'Feature':<25s} {'Corr':>8s} {'Mean(UP)':>10s} {'Mean(DOWN)':>10s} {'Signal':>8s}")
    print("-" * 65)

    correlations = []
    for i, name in enumerate(names):
        col = X[:, i]
        # Remove NaN/inf
        mask = np.isfinite(col)
        if mask.sum() < 20:
            continue
        corr = np.corrcoef(col[mask], y1h[mask])[0, 1] if mask.sum() > 0 else 0
        mean_up = col[mask & (y1h == 1)].mean() if (mask & (y1h == 1)).sum() > 0 else 0
        mean_down = col[mask & (y1h == 0)].mean() if (mask & (y1h == 0)).sum() > 0 else 0
        correlations.append((name, corr, mean_up, mean_down))

    correlations.sort(key=lambda x: abs(x[1]), reverse=True)

    for name, corr, mean_up, mean_down in correlations:
        signal = "STRONG" if abs(corr) > 0.15 else "MEDIUM" if abs(corr) > 0.08 else "weak"
        direction = "↑" if corr > 0 else "↓"
        print(f"  {name:<23s} {corr:>+7.3f} {direction} {mean_up:>10.4f} {mean_down:>10.4f} {signal:>8s}")

    # Try simple prediction: combine top features
    print(f"\n{'='*70}")
    print("  SIMPLE PREDICTION MODEL (top features combined)")
    print(f"{'='*70}")

    # Use top 5 features by correlation
    top_feats = [c[0] for c in correlations[:5]]
    top_idx = [names.index(f) for f in top_feats]

    # Simple: weighted sum of top features (sign = correlation direction)
    top_corrs = [correlations[i][1] for i in range(min(5, len(correlations)))]
    scores = np.zeros(len(timestamps))
    for j, idx in enumerate(top_idx):
        col = X[:, idx]
        col_std = col.std()
        if col_std > 0:
            scores += (col / col_std) * np.sign(top_corrs[j])

    # Threshold: predict UP if score > 0
    pred_1h = (scores > 0).astype(int)
    accuracy = (pred_1h == y1h).mean()
    print(f"  Simple model accuracy (1h): {accuracy:.1%}")

    # By confidence bucket
    print(f"\n  By prediction confidence:")
    for threshold in [0.5, 1.0, 1.5, 2.0]:
        strong_up = scores > threshold
        strong_down = scores < -threshold
        if strong_up.sum() > 5:
            acc_up = y1h[strong_up].mean()
            print(f"    Score > +{threshold:.1f}: {strong_up.sum():>4d} predictions, {acc_up:.1%} actually UP")
        if strong_down.sum() > 5:
            acc_down = (1 - y1h[strong_down]).mean()
            print(f"    Score < -{threshold:.1f}: {strong_down.sum():>4d} predictions, {acc_down:.1%} actually DOWN")

    # Walk-forward test (train on first 70%, test on last 30%)
    print(f"\n{'='*70}")
    print("  WALK-FORWARD BACKTEST (70/30 split)")
    print(f"{'='*70}")

    split = int(len(timestamps) * 0.7)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y1h[:split], y1h[split:]

    try:
        import lightgbm as lgb
        dtrain = lgb.Dataset(X_train, label=y_train, feature_name=names)
        params = {
            'objective': 'binary', 'metric': 'binary_logloss',
            'num_leaves': 31, 'learning_rate': 0.05,
            'feature_fraction': 0.8, 'bagging_fraction': 0.8,
            'bagging_freq': 5, 'verbose': -1,
            'min_child_samples': 10,
        }
        model = lgb.train(params, dtrain, num_boost_round=200,
                         valid_sets=[lgb.Dataset(X_test, label=y_test)],
                         callbacks=[lgb.early_stopping(30, verbose=False)])

        pred_proba = model.predict(X_test)
        pred_binary = (pred_proba > 0.5).astype(int)
        accuracy = (pred_binary == y_test).mean()

        print(f"  LightGBM accuracy: {accuracy:.1%}")
        print(f"  Baseline (always UP): {y_test.mean():.1%}")
        print(f"  Edge over baseline: {(accuracy - max(y_test.mean(), 1-y_test.mean()))*100:+.1f}%")

        # Feature importance
        importance = model.feature_importance(importance_type='gain')
        imp_sorted = sorted(zip(names, importance), key=lambda x: x[1], reverse=True)
        print(f"\n  TOP FEATURES (LightGBM importance):")
        for fname, imp in imp_sorted[:10]:
            bar = '█' * int(imp / max(importance) * 30)
            print(f"    {fname:<23s} {imp:>8.0f} {bar}")

        # Confidence buckets
        print(f"\n  PREDICTION CONFIDENCE:")
        for lo, hi in [(0.5, 0.55), (0.55, 0.65), (0.65, 0.75), (0.75, 1.0)]:
            mask = (pred_proba >= lo) & (pred_proba < hi)
            if mask.sum() > 3:
                acc = (y_test[mask] == 1).mean()
                print(f"    P(UP) {lo:.0%}-{hi:.0%}: {mask.sum():>4d} predictions, {acc:.1%} actually UP")
        for lo, hi in [(0.25, 0.45), (0.1, 0.25), (0.0, 0.1)]:
            mask = (pred_proba >= lo) & (pred_proba < hi)
            if mask.sum() > 3:
                acc = (y_test[mask] == 0).mean()
                print(f"    P(DN) {1-hi:.0%}-{1-lo:.0%}: {mask.sum():>4d} predictions, {acc:.1%} actually DOWN")

    except ImportError:
        print("  LightGBM not available — install with: pip install lightgbm")
        print("  Falling back to simple correlation model only")

    return correlations


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    for coin in ['BTC', 'ETH', 'SOL']:
        analyze_features(coin)
