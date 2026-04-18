"""
FORTIX Direction Predictor v2
==============================
Predicts price direction 1-2h ahead using CoinGlass derivatives data.
167 days of hourly data for 24 coins.

Features: 45 total across 9 categories.
Model: LightGBM with walk-forward validation.
Output: predict_direction(coin) → {direction, confidence, features}
"""

import sqlite3
import numpy as np
import pickle
import logging
import time as _time
from pathlib import Path
from datetime import datetime

log = logging.getLogger('direction_predictor')
DB_PATH = Path('data/crypto/market.db')
MODEL_DIR = Path('data/crypto/pred_models')


def _get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def _pct_change(new, old):
    return (new / old - 1) * 100 if old and old != 0 else 0


def _calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        return 100.0
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


FEATURE_NAMES = [
    # OI (6)
    'oi_chg_1h', 'oi_chg_2h', 'oi_chg_4h', 'oi_chg_8h', 'oi_range_1h', 'oi_accel',
    # CVD futures (5)
    'cvd_f_chg_1h', 'cvd_f_chg_4h', 'cvd_f_ratio_1h', 'cvd_f_accel', 'cvd_f_vs_spot',
    # Taker (5)
    'taker_ratio', 'taker_ratio_chg_1h', 'taker_ratio_chg_4h', 'taker_buy_dom', 'taker_imbalance_accel',
    # Top trader L/S (4)
    'top_ls', 'top_ls_chg_1h', 'top_ls_chg_4h', 'top_ls_extreme',
    # Global L/S retail (4)
    'glob_ls', 'glob_ls_chg_1h', 'glob_ls_chg_4h', 'glob_ls_extreme',
    # Funding (3)
    'funding', 'funding_vel_4h', 'funding_extreme',
    # Orderbook (4)
    'ob_imbal', 'ob_imbal_chg_1h', 'ob_imbal_chg_4h', 'ob_bid_ask_ratio',
    # Liquidations (4)
    'liq_ratio', 'liq_total_norm', 'liq_ratio_chg_1h', 'liq_cascade',
    # Price (7)
    'mom_1h', 'mom_2h', 'mom_4h', 'mom_8h', 'rsi_14', 'vol_ratio', 'price_vs_bb',
    # Divergences (3)
    'div_oi_price', 'div_cvd_price', 'div_taker_price',
]


def build_features(coin='BTC', min_hours=100):
    """Build aligned feature matrix from prediction tables + prices.
    Returns: (timestamps, X, y_1h, y_2h) or None.
    """
    conn = _get_conn()

    # Load all data as {timestamp: [values]}
    def load(table, cols):
        rows = conn.execute(
            f"SELECT timestamp, {','.join(cols)} FROM {table} WHERE coin=? ORDER BY timestamp",
            (coin,)
        ).fetchall()
        return {r[0]: [float(x or 0) for x in r[1:]] for r in rows}

    prices_raw = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='1h' ORDER BY timestamp", (coin,)
    ).fetchall()

    if len(prices_raw) < min_hours:
        conn.close()
        return None
        return None

    prices = {r[0]: {'o': r[1], 'h': r[2], 'l': r[3], 'c': r[4], 'v': r[5] or 0} for r in prices_raw}
    all_ts = sorted(prices.keys())
    price_close = {ts: prices[ts]['c'] for ts in all_ts}

    # Load all prediction data (conn still open)
    oi = load('pred_oi_history', ['open_interest', 'h', 'l', 'c'])
    cvd_f = load('pred_cvd_futures', ['buy_volume', 'sell_volume', 'cvd'])
    cvd_s = load('pred_cvd_spot', ['buy_volume', 'sell_volume', 'cvd'])
    taker = load('pred_taker_volume', ['buy_volume', 'sell_volume', 'ratio'])
    top_ls = load('pred_top_trader_ls', ['long_ratio', 'short_ratio', 'long_short_ratio'])
    glob_ls = load('pred_global_ls', ['long_ratio', 'short_ratio', 'long_short_ratio'])
    funding = load('pred_funding_oi_weight', ['funding_rate'])
    ob = load('pred_orderbook_depth', ['bid_amount', 'ask_amount', 'imbalance'])
    liq = load('pred_liq_history', ['long_liq_usd', 'short_liq_usd'])
    conn.close()

    cg_ts = set(oi.keys()) & set(cvd_f.keys()) & set(taker.keys()) & set(top_ls.keys()) & set(ob.keys())
    if len(cg_ts) < min_hours:
        return None

    def get(data, ts, idx=0, default=0):
        return data.get(ts, [default] * (idx + 1))[idx] if ts in data else default

    timestamps, features, labels_1h, labels_2h = [], [], [], []

    for i in range(10, len(all_ts) - 2):
        ts = all_ts[i]
        if ts not in cg_ts:
            continue

        p = price_close[ts]
        if p == 0:
            continue

        # Future prices for labels
        p_1h = price_close.get(all_ts[i + 1]) if i + 1 < len(all_ts) else None
        p_2h = price_close.get(all_ts[i + 2]) if i + 2 < len(all_ts) else None
        if not p_1h or not p_2h:
            continue

        # Helper: get value N hours ago
        def ago(n):
            return all_ts[i - n] if i >= n else ts

        # OI features
        oi_c = get(oi, ts, 3)
        oi_1h = get(oi, ago(1), 3)
        oi_2h = get(oi, ago(2), 3)
        oi_4h = get(oi, ago(4), 3)
        oi_8h = get(oi, ago(8), 3)
        oi_h = get(oi, ts, 1)
        oi_l = get(oi, ts, 2)

        oi_chg_1h = _pct_change(oi_c, oi_1h)
        oi_chg_2h = _pct_change(oi_c, oi_2h)
        oi_chg_4h = _pct_change(oi_c, oi_4h)
        oi_chg_8h = _pct_change(oi_c, oi_8h)
        oi_range = _pct_change(oi_h, oi_l) if oi_l > 0 else 0
        oi_accel = oi_chg_1h - _pct_change(oi_1h, oi_2h)

        # CVD futures
        cvd_fc = get(cvd_f, ts, 2)
        cvd_f1 = get(cvd_f, ago(1), 2)
        cvd_f4 = get(cvd_f, ago(4), 2)
        cvd_fb = get(cvd_f, ts, 0)
        cvd_fs = get(cvd_f, ts, 1)
        cvd_f_chg_1h = cvd_fc - cvd_f1
        cvd_f_chg_4h = cvd_fc - cvd_f4
        cvd_f_total = cvd_fb + cvd_fs
        cvd_f_ratio = cvd_fb / cvd_f_total if cvd_f_total > 0 else 0.5
        cvd_f_accel = cvd_f_chg_1h - (cvd_f1 - get(cvd_f, ago(2), 2))
        cvd_sc = get(cvd_s, ts, 2)
        cvd_f_vs_spot = 1 if (cvd_fc > 0 and cvd_sc < 0) else (-1 if (cvd_fc < 0 and cvd_sc > 0) else 0)

        # Taker
        tk_r = get(taker, ts, 2, 1.0)
        tk_r1 = get(taker, ago(1), 2, 1.0)
        tk_r4 = get(taker, ago(4), 2, 1.0)
        tk_b = get(taker, ts, 0)
        tk_s = get(taker, ts, 1)
        tk_total = tk_b + tk_s
        tk_dom = tk_b / tk_total if tk_total > 0 else 0.5
        tk_imb_accel = (tk_r - tk_r1) - (tk_r1 - get(taker, ago(2), 2, 1.0))

        # Top trader L/S
        tl_r = get(top_ls, ts, 2, 1.0)
        tl_r1 = get(top_ls, ago(1), 2, 1.0)
        tl_r4 = get(top_ls, ago(4), 2, 1.0)
        tl_ext = 1 if tl_r > 1.5 else (-1 if tl_r < 0.67 else 0)

        # Global L/S
        gl_r = get(glob_ls, ts, 2, 1.0)
        gl_r1 = get(glob_ls, ago(1), 2, 1.0)
        gl_r4 = get(glob_ls, ago(4), 2, 1.0)
        gl_ext = 1 if gl_r > 1.5 else (-1 if gl_r < 0.67 else 0)

        # Funding
        fd = get(funding, ts, 0)
        fd4 = get(funding, ago(4), 0)
        fd_vel = fd - fd4
        fd_ext = 1 if fd > 0.01 else (-1 if fd < -0.01 else 0)

        # Orderbook
        ob_i = get(ob, ts, 2)
        ob_i1 = get(ob, ago(1), 2)
        ob_i4 = get(ob, ago(4), 2)
        ob_b = get(ob, ts, 0)
        ob_a = get(ob, ts, 1)
        ob_ba = ob_b / ob_a if ob_a > 0 else 1.0

        # Liquidations
        lq_l = get(liq, ts, 0)
        lq_s = get(liq, ts, 1)
        lq_total = lq_l + lq_s
        lq_ratio = (lq_l - lq_s) / lq_total if lq_total > 0 else 0
        lq_l1 = get(liq, ago(1), 0)
        lq_s1 = get(liq, ago(1), 1)
        lq_t1 = lq_l1 + lq_s1
        lq_r1 = (lq_l1 - lq_s1) / lq_t1 if lq_t1 > 0 else 0
        lq_norm = lq_total / (p * 1000) if p > 0 else 0
        lq_cascade = 1 if lq_total > lq_t1 * 3 and lq_t1 > 0 else 0

        # Price features
        p1 = price_close.get(ago(1), p)
        p2 = price_close.get(ago(2), p)
        p4 = price_close.get(ago(4), p)
        p8 = price_close.get(ago(8), p)
        mom_1h = _pct_change(p, p1)
        mom_2h = _pct_change(p, p2)
        mom_4h = _pct_change(p, p4)
        mom_8h = _pct_change(p, p8)

        # RSI
        rsi_prices = [price_close.get(all_ts[j], 0) for j in range(max(0, i - 16), i + 1)]
        rsi_prices = [x for x in rsi_prices if x > 0]
        rsi = _calc_rsi(rsi_prices) if len(rsi_prices) >= 15 else 50.0

        # Volume ratio (current vs 8h avg)
        vols = [prices.get(all_ts[j], {}).get('v', 0) for j in range(max(0, i - 8), i + 1)]
        vol_avg = np.mean(vols[:-1]) if len(vols) > 1 else 1
        vol_ratio = vols[-1] / vol_avg if vol_avg > 0 else 1.0

        # Bollinger position
        closes_20 = [price_close.get(all_ts[j], 0) for j in range(max(0, i - 20), i + 1)]
        closes_20 = [x for x in closes_20 if x > 0]
        if len(closes_20) >= 20:
            bb_mid = np.mean(closes_20[-20:])
            bb_std = np.std(closes_20[-20:])
            price_vs_bb = (p - bb_mid) / bb_std if bb_std > 0 else 0
        else:
            price_vs_bb = 0

        # Divergences
        div_oi = 1 if (mom_1h > 0.2 and oi_chg_1h < -0.5) else (-1 if (mom_1h < -0.2 and oi_chg_1h > 0.5) else 0)
        div_cvd = 1 if (mom_1h > 0.2 and cvd_f_chg_1h < 0) else (-1 if (mom_1h < -0.2 and cvd_f_chg_1h > 0) else 0)
        div_taker = 1 if (mom_1h > 0.2 and tk_r < 0.95) else (-1 if (mom_1h < -0.2 and tk_r > 1.05) else 0)

        feat = [
            oi_chg_1h, oi_chg_2h, oi_chg_4h, oi_chg_8h, oi_range, oi_accel,
            cvd_f_chg_1h, cvd_f_chg_4h, cvd_f_ratio, cvd_f_accel, cvd_f_vs_spot,
            tk_r, tk_r - tk_r1, tk_r - tk_r4, tk_dom, tk_imb_accel,
            tl_r, tl_r - tl_r1, tl_r - tl_r4, tl_ext,
            gl_r, gl_r - gl_r1, gl_r - gl_r4, gl_ext,
            fd, fd_vel, fd_ext,
            ob_i, ob_i - ob_i1, ob_i - ob_i4, ob_ba,
            lq_ratio, lq_norm, lq_ratio - lq_r1, lq_cascade,
            mom_1h, mom_2h, mom_4h, mom_8h, rsi, vol_ratio, price_vs_bb,
            div_oi, div_cvd, div_taker,
        ]

        timestamps.append(ts)
        features.append(feat)
        labels_1h.append(1 if p_1h > p else 0)
        labels_2h.append(1 if p_2h > p else 0)

    if len(timestamps) < min_hours:
        return None

    return np.array(timestamps), np.array(features), np.array(labels_1h), np.array(labels_2h)


def walk_forward_test(coin='BTC', n_splits=5):
    """Walk-forward cross-validation."""
    result = build_features(coin, min_hours=200)
    if result is None:
        print(f"  {coin}: insufficient data")
        return None

    ts, X, y1, y2 = result
    n = len(ts)

    try:
        import lightgbm as lgb
    except ImportError:
        print("  LightGBM required: pip install lightgbm")
        return None

    params = {
        'objective': 'binary', 'metric': 'binary_logloss',
        'num_leaves': 31, 'learning_rate': 0.03, 'n_estimators': 300,
        'feature_fraction': 0.7, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'min_child_samples': 15, 'verbose': -1, 'reg_alpha': 0.1, 'reg_lambda': 0.1,
    }

    fold_size = n // (n_splits + 1)
    accuracies = []
    all_preds = []
    all_labels = []
    importances = np.zeros(len(FEATURE_NAMES))

    for fold in range(n_splits):
        train_end = fold_size * (fold + 2)
        test_start = train_end
        test_end = min(test_start + fold_size, n)

        if test_end <= test_start:
            continue

        X_train, y_train = X[:train_end], y1[:train_end]
        X_test, y_test = X[test_start:test_end], y1[test_start:test_end]

        dtrain = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_NAMES)
        dval = lgb.Dataset(X_test, label=y_test, feature_name=FEATURE_NAMES)

        model = lgb.train(params, dtrain, num_boost_round=300,
                         valid_sets=[dval],
                         callbacks=[lgb.early_stopping(30, verbose=False)])

        pred = model.predict(X_test)
        acc = ((pred > 0.5).astype(int) == y_test).mean()
        accuracies.append(acc)
        all_preds.extend(pred.tolist())
        all_labels.extend(y_test.tolist())
        importances += model.feature_importance(importance_type='gain')

    if not accuracies:
        return None

    all_preds = np.array(all_preds)
    all_labels = np.array(all_labels)
    importances /= n_splits

    return {
        'coin': coin,
        'samples': n,
        'accuracy': np.mean(accuracies),
        'accuracy_std': np.std(accuracies),
        'baseline': max(y1.mean(), 1 - y1.mean()),
        'edge': np.mean(accuracies) - max(y1.mean(), 1 - y1.mean()),
        'fold_accs': accuracies,
        'importances': dict(sorted(zip(FEATURE_NAMES, importances), key=lambda x: -x[1])[:15]),
        'preds': all_preds,
        'labels': all_labels,
    }


def train_and_save(coin='BTC'):
    """Train final model on all data and save to disk."""
    result = build_features(coin, min_hours=200)
    if result is None:
        return None

    ts, X, y1, y2 = result

    try:
        import lightgbm as lgb
    except ImportError:
        return None

    params = {
        'objective': 'binary', 'metric': 'binary_logloss',
        'num_leaves': 31, 'learning_rate': 0.03,
        'feature_fraction': 0.7, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'min_child_samples': 15, 'verbose': -1, 'reg_alpha': 0.1, 'reg_lambda': 0.1,
    }

    # Train on 90%, validate on last 10%
    split = int(len(ts) * 0.9)
    dtrain = lgb.Dataset(X[:split], label=y1[:split], feature_name=FEATURE_NAMES)
    dval = lgb.Dataset(X[split:], label=y1[split:], feature_name=FEATURE_NAMES)

    model = lgb.train(params, dtrain, num_boost_round=500,
                     valid_sets=[dval],
                     callbacks=[lgb.early_stopping(50, verbose=False)])

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(str(MODEL_DIR / f'direction_{coin}.lgb'))

    val_pred = model.predict(X[split:])
    val_acc = ((val_pred > 0.5).astype(int) == y1[split:]).mean()

    return {'coin': coin, 'val_accuracy': val_acc, 'samples': len(ts),
            'model_path': str(MODEL_DIR / f'direction_{coin}.lgb')}


def predict_now(coin='BTC'):
    """Real-time prediction: load model + build features for latest hour."""
    model_path = MODEL_DIR / f'direction_{coin}.lgb'
    if not model_path.exists():
        return None

    try:
        import lightgbm as lgb
        model = lgb.Booster(model_file=str(model_path))
    except Exception:
        return None

    result = build_features(coin, min_hours=20)
    if result is None:
        return None

    ts, X, y1, y2 = result
    latest_x = X[-1:]
    pred = model.predict(latest_x)[0]

    direction = 'UP' if pred > 0.5 else 'DOWN'
    confidence = abs(pred - 0.5) * 2  # 0-1 scale

    return {
        'coin': coin,
        'direction': direction,
        'probability': round(pred, 4),
        'confidence': round(confidence, 4),
        'timestamp': int(ts[-1]),
        'features': {FEATURE_NAMES[i]: round(float(X[-1, i]), 4) for i in range(len(FEATURE_NAMES))
                     if abs(X[-1, i]) > 0.001},
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    ALL_COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE', 'BNB',
                 'LDO', 'CRV', 'UNI', 'PENDLE', 'TON', 'POL', 'ARB', 'OP',
                 'FET', 'RENDER', 'APT', 'FIL', 'NEAR', 'PEPE', 'WIF']

    print("=" * 70)
    print("  DIRECTION PREDICTOR v2 — WALK-FORWARD VALIDATION")
    print("=" * 70)

    results = []
    for coin in ALL_COINS:
        r = walk_forward_test(coin, n_splits=5)
        if r:
            results.append(r)
            edge_str = f"+{r['edge']*100:.1f}%" if r['edge'] > 0 else f"{r['edge']*100:.1f}%"
            print(f"  {coin:8s} acc={r['accuracy']:.1%}±{r['accuracy_std']:.1%}  "
                  f"baseline={r['baseline']:.1%}  edge={edge_str}  "
                  f"samples={r['samples']}")

    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    if results:
        avg_acc = np.mean([r['accuracy'] for r in results])
        avg_edge = np.mean([r['edge'] for r in results])
        best = max(results, key=lambda x: x['edge'])
        worst = min(results, key=lambda x: x['edge'])
        print(f"  Average accuracy: {avg_acc:.1%}")
        print(f"  Average edge: {avg_edge*100:+.1f}%")
        print(f"  Best: {best['coin']} ({best['accuracy']:.1%}, edge {best['edge']*100:+.1f}%)")
        print(f"  Worst: {worst['coin']} ({worst['accuracy']:.1%}, edge {worst['edge']*100:+.1f}%)")

        # High confidence analysis on best coin
        r = best
        preds, labels = r['preds'], r['labels']
        print(f"\n  HIGH CONFIDENCE ({r['coin']}):")
        for lo, hi, direction in [(0.6, 1.0, 'UP'), (0.55, 0.6, 'UP'), (0.0, 0.4, 'DOWN'), (0.4, 0.45, 'DOWN')]:
            mask = (preds >= lo) & (preds < hi)
            if mask.sum() >= 5:
                if direction == 'UP':
                    acc = labels[mask].mean()
                else:
                    acc = (1 - labels[mask]).mean()
                print(f"    P({direction}) {lo:.0%}-{hi:.0%}: {mask.sum():>4d} preds, {acc:.1%} correct")

        # Feature importance across all coins
        print(f"\n  TOP FEATURES (avg across all coins):")
        all_imp = {}
        for r in results:
            for fname, imp in r['importances'].items():
                all_imp[fname] = all_imp.get(fname, 0) + imp
        for fname, imp in sorted(all_imp.items(), key=lambda x: -x[1])[:12]:
            bar = '█' * int(imp / max(all_imp.values()) * 25)
            print(f"    {fname:<25s} {bar}")

    # Train and save models for coins with edge > 2%
    print(f"\n{'='*70}")
    print(f"  TRAINING FINAL MODELS")
    print(f"{'='*70}")
    for r in results:
        if r['edge'] > 0.02:
            saved = train_and_save(r['coin'])
            if saved:
                print(f"  {r['coin']:8s} saved → {saved['model_path']} (val_acc={saved['val_accuracy']:.1%})")

    # Real-time predictions
    print(f"\n{'='*70}")
    print(f"  LIVE PREDICTIONS (right now)")
    print(f"{'='*70}")
    for coin in ALL_COINS[:10]:
        pred = predict_now(coin)
        if pred:
            print(f"  {coin:8s} → {pred['direction']:4s} (P={pred['probability']:.1%}, conf={pred['confidence']:.1%})")
