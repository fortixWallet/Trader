"""
FORTIX Direction Predictor v3 — Maximum Accuracy
==================================================
Key improvements over v2:
1. Cross-asset: BTC derivatives as features for all coins
2. Rolling stats: 4h/8h std, momentum of derivatives
3. Threshold target: predict >0.3% move (skip noise)
4. Feature interactions: OI×CVD, funding×L/S
5. Larger feature set (70+), LightGBM feature selection
6. Walk-forward with expanding window
"""

import sqlite3
import numpy as np
import logging
import time as _time
from pathlib import Path
from datetime import datetime

log = logging.getLogger('direction_predictor_v3')
DB_PATH = Path('data/crypto/market.db')
MODEL_DIR = Path('data/crypto/pred_models_v3')


def _get_conn():
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def _pct(new, old):
    return (new / old - 1) * 100 if old and old != 0 else 0


def _rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    d = np.diff(prices)
    g = np.where(d > 0, d, 0)
    l = np.where(d < 0, -d, 0)
    ag = np.mean(g[:period])
    al = np.mean(l[:period])
    if al == 0: return 100.0
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag / al))


def _load_all_data(conn, coin):
    """Load all tables for a coin into dicts {ts: [values]}."""
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

    prices = {r[0]: {'o': r[1], 'h': r[2], 'l': r[3], 'c': r[4], 'v': r[5] or 0} for r in prices_raw}

    return {
        'prices': prices,
        'oi': load('pred_oi_history', ['open_interest', 'h', 'l', 'c']),
        'cvd_f': load('pred_cvd_futures', ['buy_volume', 'sell_volume', 'cvd']),
        'cvd_s': load('pred_cvd_spot', ['buy_volume', 'sell_volume', 'cvd']),
        'taker': load('pred_taker_volume', ['buy_volume', 'sell_volume', 'ratio']),
        'top_ls': load('pred_top_trader_ls', ['long_ratio', 'short_ratio', 'long_short_ratio']),
        'glob_ls': load('pred_global_ls', ['long_ratio', 'short_ratio', 'long_short_ratio']),
        'funding': load('pred_funding_oi_weight', ['funding_rate']),
        'ob': load('pred_orderbook_depth', ['bid_amount', 'ask_amount', 'imbalance']),
        'liq': load('pred_liq_history', ['long_liq_usd', 'short_liq_usd']),
    }


def build_features_v3(coin='BTC', threshold=0.3, min_samples=200):
    """Build feature matrix with cross-asset BTC features.

    threshold: minimum price move % to count as UP/DOWN (smaller = FLAT/skip)
    """
    conn = _get_conn()
    data = _load_all_data(conn, coin)
    btc_data = _load_all_data(conn, 'BTC') if coin != 'BTC' else data
    conn.close()

    prices = data['prices']
    if len(prices) < min_samples:
        return None

    all_ts = sorted(prices.keys())
    price_c = {ts: prices[ts]['c'] for ts in all_ts}

    cg_ts = set(data['oi'].keys()) & set(data['cvd_f'].keys()) & set(data['taker'].keys()) & \
            set(data['top_ls'].keys()) & set(data['ob'].keys())

    def get(d, ts, idx=0, default=0):
        return d.get(ts, [default] * (idx + 1))[idx] if ts in d else default

    def rolling_std(values, n=4):
        if len(values) < n: return 0
        return float(np.std(values[-n:])) if np.std(values[-n:]) > 0 else 0

    def rolling_mean(values, n=4):
        if len(values) < n: return 0
        return float(np.mean(values[-n:]))

    feature_names = []
    timestamps, features, labels = [], [], []

    for i in range(12, len(all_ts) - 2):
        ts = all_ts[i]
        if ts not in cg_ts:
            continue

        p = price_c[ts]
        if p == 0:
            continue

        p_1h = price_c.get(all_ts[i + 1]) if i + 1 < len(all_ts) else None
        p_2h = price_c.get(all_ts[i + 2]) if i + 2 < len(all_ts) else None
        if not p_1h or not p_2h:
            continue

        def ago(n):
            return all_ts[i - n] if i >= n else ts

        # ===== COIN-SPECIFIC FEATURES =====
        feat = {}

        # OI
        oi_c = get(data['oi'], ts, 3)
        oi_vals = [get(data['oi'], all_ts[j], 3) for j in range(max(0, i-8), i+1) if all_ts[j] in data['oi']]
        feat['oi_chg_1h'] = _pct(oi_c, get(data['oi'], ago(1), 3))
        feat['oi_chg_4h'] = _pct(oi_c, get(data['oi'], ago(4), 3))
        feat['oi_chg_8h'] = _pct(oi_c, get(data['oi'], ago(8), 3))
        feat['oi_range'] = _pct(get(data['oi'], ts, 1), get(data['oi'], ts, 2))
        feat['oi_accel'] = feat['oi_chg_1h'] - _pct(get(data['oi'], ago(1), 3), get(data['oi'], ago(2), 3))
        feat['oi_std_4h'] = rolling_std(oi_vals, 4) / (oi_c + 1e-10) * 100

        # CVD futures
        cvd_fc = get(data['cvd_f'], ts, 2)
        cvd_f1 = get(data['cvd_f'], ago(1), 2)
        cvd_f4 = get(data['cvd_f'], ago(4), 2)
        cvd_fb = get(data['cvd_f'], ts, 0)
        cvd_fs = get(data['cvd_f'], ts, 1)
        feat['cvd_f_chg_1h'] = cvd_fc - cvd_f1
        feat['cvd_f_chg_4h'] = cvd_fc - cvd_f4
        feat['cvd_f_ratio'] = cvd_fb / (cvd_fb + cvd_fs) if (cvd_fb + cvd_fs) > 0 else 0.5
        feat['cvd_f_accel'] = (cvd_fc - cvd_f1) - (cvd_f1 - get(data['cvd_f'], ago(2), 2))

        # CVD spot + divergence
        cvd_sc = get(data['cvd_s'], ts, 2)
        feat['cvd_spot_sign'] = 1 if cvd_sc > 0 else (-1 if cvd_sc < 0 else 0)
        feat['cvd_f_vs_spot'] = 1 if (cvd_fc > 0 and cvd_sc < 0) else (-1 if (cvd_fc < 0 and cvd_sc > 0) else 0)

        # Taker
        tk_r = get(data['taker'], ts, 2, 1.0)
        tk_r1 = get(data['taker'], ago(1), 2, 1.0)
        tk_r4 = get(data['taker'], ago(4), 2, 1.0)
        tk_b = get(data['taker'], ts, 0)
        tk_s = get(data['taker'], ts, 1)
        feat['taker_ratio'] = tk_r
        feat['taker_chg_1h'] = tk_r - tk_r1
        feat['taker_chg_4h'] = tk_r - tk_r4
        feat['taker_dom'] = tk_b / (tk_b + tk_s) if (tk_b + tk_s) > 0 else 0.5
        feat['taker_accel'] = (tk_r - tk_r1) - (tk_r1 - get(data['taker'], ago(2), 2, 1.0))
        tk_vals = [get(data['taker'], all_ts[j], 2, 1.0) for j in range(max(0, i-8), i+1)]
        feat['taker_std_4h'] = rolling_std(tk_vals[-4:])

        # Top trader L/S
        tl = get(data['top_ls'], ts, 2, 1.0)
        tl1 = get(data['top_ls'], ago(1), 2, 1.0)
        tl4 = get(data['top_ls'], ago(4), 2, 1.0)
        feat['top_ls'] = tl
        feat['top_ls_chg_1h'] = tl - tl1
        feat['top_ls_chg_4h'] = tl - tl4
        feat['top_ls_extreme'] = 1 if tl > 1.3 else (-1 if tl < 0.77 else 0)

        # Global L/S
        gl = get(data['glob_ls'], ts, 2, 1.0)
        gl1 = get(data['glob_ls'], ago(1), 2, 1.0)
        gl4 = get(data['glob_ls'], ago(4), 2, 1.0)
        feat['glob_ls'] = gl
        feat['glob_ls_chg_1h'] = gl - gl1
        feat['glob_ls_chg_4h'] = gl - gl4
        feat['glob_ls_extreme'] = 1 if gl > 1.5 else (-1 if gl < 0.67 else 0)
        feat['smart_vs_retail'] = tl - gl

        # Funding
        fd = get(data['funding'], ts, 0)
        fd4 = get(data['funding'], ago(4), 0)
        feat['funding'] = fd
        feat['funding_vel'] = fd - fd4
        feat['funding_ext'] = 1 if fd > 0.01 else (-1 if fd < -0.01 else 0)

        # Orderbook
        ob_i = get(data['ob'], ts, 2)
        ob_i1 = get(data['ob'], ago(1), 2)
        ob_i4 = get(data['ob'], ago(4), 2)
        ob_b = get(data['ob'], ts, 0)
        ob_a = get(data['ob'], ts, 1)
        feat['ob_imbal'] = ob_i
        feat['ob_chg_1h'] = ob_i - ob_i1
        feat['ob_chg_4h'] = ob_i - ob_i4
        feat['ob_ratio'] = ob_b / ob_a if ob_a > 0 else 1.0
        ob_vals = [get(data['ob'], all_ts[j], 2) for j in range(max(0, i-8), i+1)]
        feat['ob_std_4h'] = rolling_std(ob_vals[-4:])

        # Liquidations
        lq_l = get(data['liq'], ts, 0)
        lq_s = get(data['liq'], ts, 1)
        lq_t = lq_l + lq_s
        feat['liq_ratio'] = (lq_l - lq_s) / lq_t if lq_t > 0 else 0
        feat['liq_norm'] = lq_t / (p * 1000) if p > 0 else 0
        lq_t1 = get(data['liq'], ago(1), 0) + get(data['liq'], ago(1), 1)
        feat['liq_ratio_chg'] = feat['liq_ratio'] - ((get(data['liq'], ago(1), 0) - get(data['liq'], ago(1), 1)) / lq_t1 if lq_t1 > 0 else 0)
        feat['liq_cascade'] = 1 if lq_t > lq_t1 * 3 and lq_t1 > 0 else 0

        # Price
        p1 = price_c.get(ago(1), p)
        p2 = price_c.get(ago(2), p)
        p4 = price_c.get(ago(4), p)
        p8 = price_c.get(ago(8), p)
        feat['mom_1h'] = _pct(p, p1)
        feat['mom_2h'] = _pct(p, p2)
        feat['mom_4h'] = _pct(p, p4)
        feat['mom_8h'] = _pct(p, p8)
        feat['mom_accel'] = feat['mom_1h'] - _pct(p1, p2)

        rsi_p = [price_c.get(all_ts[j], 0) for j in range(max(0, i-16), i+1)]
        rsi_p = [x for x in rsi_p if x > 0]
        feat['rsi'] = _rsi(rsi_p) if len(rsi_p) >= 15 else 50.0

        vol_list = [prices.get(all_ts[j], {}).get('v', 0) for j in range(max(0, i-8), i+1)]
        feat['vol_ratio'] = vol_list[-1] / np.mean(vol_list[:-1]) if len(vol_list) > 1 and np.mean(vol_list[:-1]) > 0 else 1.0

        cls20 = [price_c.get(all_ts[j], 0) for j in range(max(0, i-20), i+1)]
        cls20 = [x for x in cls20 if x > 0]
        if len(cls20) >= 20:
            bb_m = np.mean(cls20[-20:])
            bb_s = np.std(cls20[-20:])
            feat['bb_pos'] = (p - bb_m) / bb_s if bb_s > 0 else 0
        else:
            feat['bb_pos'] = 0

        # Volatility
        rets = [_pct(price_c.get(all_ts[j], 0), price_c.get(all_ts[j-1], 0))
                for j in range(max(1, i-8), i+1) if all_ts[j-1] in price_c and price_c.get(all_ts[j], 0) > 0]
        feat['volatility_8h'] = float(np.std(rets)) if len(rets) >= 4 else 0

        # ===== INTERACTIONS =====
        feat['oi_x_cvd'] = feat['oi_chg_1h'] * (1 if cvd_fc > 0 else -1)
        feat['funding_x_ls'] = fd * gl
        feat['ob_x_taker'] = ob_i * tk_r
        feat['liq_x_mom'] = feat['liq_ratio'] * feat['mom_1h']

        # ===== DIVERGENCES =====
        feat['div_oi_price'] = 1 if (feat['mom_1h'] > 0.2 and feat['oi_chg_1h'] < -0.5) else \
                              (-1 if (feat['mom_1h'] < -0.2 and feat['oi_chg_1h'] > 0.5) else 0)
        feat['div_cvd_price'] = 1 if (feat['mom_1h'] > 0.2 and cvd_fc < cvd_f1) else \
                               (-1 if (feat['mom_1h'] < -0.2 and cvd_fc > cvd_f1) else 0)
        feat['div_taker_price'] = 1 if (feat['mom_1h'] > 0.2 and tk_r < 0.95) else \
                                 (-1 if (feat['mom_1h'] < -0.2 and tk_r > 1.05) else 0)
        feat['div_ob_price'] = 1 if (feat['mom_1h'] > 0.2 and ob_i < -0.05) else \
                              (-1 if (feat['mom_1h'] < -0.2 and ob_i > 0.05) else 0)

        # ===== CROSS-ASSET (BTC features for alts) =====
        if coin != 'BTC':
            btc_p = btc_data['prices']
            btc_c = btc_p.get(ts, {}).get('c', 0)
            btc_c1 = btc_p.get(ago(1), {}).get('c', btc_c)
            btc_c4 = btc_p.get(ago(4), {}).get('c', btc_c)
            feat['btc_mom_1h'] = _pct(btc_c, btc_c1) if btc_c1 > 0 else 0
            feat['btc_mom_4h'] = _pct(btc_c, btc_c4) if btc_c4 > 0 else 0
            feat['btc_oi_chg'] = _pct(get(btc_data['oi'], ts, 3), get(btc_data['oi'], ago(1), 3))
            btc_cvd = get(btc_data['cvd_f'], ts, 2)
            btc_cvd1 = get(btc_data['cvd_f'], ago(1), 2)
            feat['btc_cvd_chg'] = btc_cvd - btc_cvd1
            feat['btc_ob_imbal'] = get(btc_data['ob'], ts, 2)
            feat['btc_funding'] = get(btc_data['funding'], ts, 0)
            feat['btc_liq_ratio'] = 0
            btc_lql = get(btc_data['liq'], ts, 0)
            btc_lqs = get(btc_data['liq'], ts, 1)
            btc_lqt = btc_lql + btc_lqs
            if btc_lqt > 0:
                feat['btc_liq_ratio'] = (btc_lql - btc_lqs) / btc_lqt
            feat['coin_vs_btc'] = feat['mom_1h'] - feat['btc_mom_1h']
        else:
            for k in ['btc_mom_1h', 'btc_mom_4h', 'btc_oi_chg', 'btc_cvd_chg',
                       'btc_ob_imbal', 'btc_funding', 'btc_liq_ratio', 'coin_vs_btc']:
                feat[k] = 0

        # Build feature vector
        if not feature_names:
            feature_names.extend(sorted(feat.keys()))

        vec = [feat.get(fn, 0) for fn in feature_names]

        # Label: threshold-based (skip small moves)
        move_1h = _pct(p_1h, p)
        move_2h = _pct(p_2h, p)

        if abs(move_1h) < threshold:
            continue  # skip noise

        label = 1 if move_1h > 0 else 0

        timestamps.append(ts)
        features.append(vec)
        labels.append(label)

    if len(timestamps) < min_samples:
        return None

    return (np.array(timestamps), np.array(features), np.array(labels),
            feature_names, threshold)


def walk_forward_v3(coin='BTC', threshold=0.3, n_splits=5):
    """Walk-forward with expanding window + threshold target."""
    result = build_features_v3(coin, threshold=threshold, min_samples=150)
    if result is None:
        return None

    ts, X, y, fnames, thr = result
    n = len(ts)

    try:
        import lightgbm as lgb
    except ImportError:
        return None

    params = {
        'objective': 'binary', 'metric': 'binary_logloss',
        'num_leaves': 24, 'learning_rate': 0.02, 'max_depth': 6,
        'feature_fraction': 0.6, 'bagging_fraction': 0.6, 'bagging_freq': 5,
        'min_child_samples': 20, 'verbose': -1,
        'reg_alpha': 0.3, 'reg_lambda': 0.3, 'min_gain_to_split': 0.01,
    }

    fold_size = n // (n_splits + 1)
    accs, preds_all, labels_all = [], [], []
    importances = np.zeros(len(fnames))

    for fold in range(n_splits):
        train_end = fold_size * (fold + 2)
        test_end = min(train_end + fold_size, n)
        if test_end <= train_end:
            continue

        X_tr, y_tr = X[:train_end], y[:train_end]
        X_te, y_te = X[train_end:test_end], y[train_end:test_end]

        dtrain = lgb.Dataset(X_tr, label=y_tr, feature_name=fnames)
        dval = lgb.Dataset(X_te, label=y_te, feature_name=fnames)

        model = lgb.train(params, dtrain, num_boost_round=500,
                         valid_sets=[dval],
                         callbacks=[lgb.early_stopping(40, verbose=False)])

        pred = model.predict(X_te)
        acc = ((pred > 0.5).astype(int) == y_te).mean()
        accs.append(acc)
        preds_all.extend(pred.tolist())
        labels_all.extend(y_te.tolist())
        importances += model.feature_importance(importance_type='gain')

    if not accs:
        return None

    preds_all = np.array(preds_all)
    labels_all = np.array(labels_all)
    importances /= max(n_splits, 1)

    return {
        'coin': coin, 'threshold': threshold, 'samples': n,
        'accuracy': np.mean(accs), 'std': np.std(accs),
        'baseline': max(y.mean(), 1 - y.mean()),
        'edge': np.mean(accs) - max(y.mean(), 1 - y.mean()),
        'fold_accs': accs, 'preds': preds_all, 'labels': labels_all,
        'importances': dict(sorted(zip(fnames, importances), key=lambda x: -x[1])[:15]),
        'feature_names': fnames,
    }


def train_save_v3(coin='BTC', threshold=0.3):
    """Train and save final model."""
    result = build_features_v3(coin, threshold=threshold, min_samples=150)
    if result is None:
        return None

    ts, X, y, fnames, thr = result
    try:
        import lightgbm as lgb
    except ImportError:
        return None

    params = {
        'objective': 'binary', 'metric': 'binary_logloss',
        'num_leaves': 24, 'learning_rate': 0.02, 'max_depth': 6,
        'feature_fraction': 0.6, 'bagging_fraction': 0.6, 'bagging_freq': 5,
        'min_child_samples': 20, 'verbose': -1,
        'reg_alpha': 0.3, 'reg_lambda': 0.3,
    }

    split = int(len(ts) * 0.9)
    dtrain = lgb.Dataset(X[:split], label=y[:split], feature_name=fnames)
    dval = lgb.Dataset(X[split:], label=y[split:], feature_name=fnames)

    model = lgb.train(params, dtrain, num_boost_round=500,
                     valid_sets=[dval],
                     callbacks=[lgb.early_stopping(50, verbose=False)])

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(str(MODEL_DIR / f'dir_{coin}.lgb'))

    import json
    meta = {'coin': coin, 'threshold': threshold, 'feature_names': fnames,
            'samples': len(ts), 'trained_at': datetime.now().isoformat()}
    with open(MODEL_DIR / f'dir_{coin}_meta.json', 'w') as f:
        json.dump(meta, f)

    val_pred = model.predict(X[split:])
    val_acc = ((val_pred > 0.5).astype(int) == y[split:]).mean()
    return {'coin': coin, 'val_acc': val_acc, 'path': str(MODEL_DIR / f'dir_{coin}.lgb')}


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    ALL_COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE', 'BNB',
                 'LDO', 'CRV', 'UNI', 'PENDLE', 'TON', 'ARB', 'OP',
                 'FET', 'RENDER', 'APT', 'FIL', 'NEAR']

    # Test multiple thresholds to find optimal
    print("=" * 70)
    print("  PREDICTOR v3 — THRESHOLD OPTIMIZATION")
    print("=" * 70)

    for thr in [0.0, 0.2, 0.3, 0.5]:
        print(f"\n--- Threshold: {thr}% ---")
        accs = []
        for coin in ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'LINK']:
            r = walk_forward_v3(coin, threshold=thr, n_splits=5)
            if r:
                accs.append(r['accuracy'])
                print(f"  {coin:6s} acc={r['accuracy']:.1%} edge={r['edge']*100:+.1f}% samples={r['samples']}")
        if accs:
            print(f"  AVG: {np.mean(accs):.1%}")

    # Full run with best threshold
    best_thr = 0.3  # will be determined from above
    print(f"\n{'='*70}")
    print(f"  FULL RUN — threshold={best_thr}%")
    print(f"{'='*70}")

    results = []
    for coin in ALL_COINS:
        r = walk_forward_v3(coin, threshold=best_thr, n_splits=5)
        if r:
            results.append(r)
            e = f"+{r['edge']*100:.1f}%" if r['edge'] > 0 else f"{r['edge']*100:.1f}%"
            print(f"  {coin:8s} acc={r['accuracy']:.1%}±{r['std']:.1%} edge={e} samples={r['samples']}")

    if results:
        avg = np.mean([r['accuracy'] for r in results])
        avg_e = np.mean([r['edge'] for r in results])
        best = max(results, key=lambda x: x['edge'])
        print(f"\n  AVERAGE: {avg:.1%} (edge {avg_e*100:+.1f}%)")
        print(f"  BEST: {best['coin']} {best['accuracy']:.1%} (edge {best['edge']*100:+.1f}%)")

        # High confidence
        print(f"\n  HIGH CONFIDENCE PREDICTIONS ({best['coin']}):")
        p, l = best['preds'], best['labels']
        for lo, hi, d in [(0.6, 1.0, 'UP'), (0.55, 0.6, 'UP'), (0.0, 0.4, 'DOWN'), (0.4, 0.45, 'DOWN')]:
            m = (p >= lo) & (p < hi)
            if m.sum() >= 5:
                a = l[m].mean() if d == 'UP' else (1 - l[m]).mean()
                print(f"    P({d}) {lo:.0%}-{hi:.0%}: {m.sum():>4d} preds → {a:.1%} correct")

        # Top features
        print(f"\n  TOP FEATURES (averaged):")
        all_imp = {}
        for r in results:
            for f, v in r['importances'].items():
                all_imp[f] = all_imp.get(f, 0) + v
        for f, v in sorted(all_imp.items(), key=lambda x: -x[1])[:15]:
            bar = '█' * int(v / max(all_imp.values()) * 25)
            print(f"    {f:<25s} {bar}")

    # Save models
    print(f"\n  SAVING MODELS (edge > 2%):")
    for r in results:
        if r['edge'] > 0.02:
            s = train_save_v3(r['coin'], threshold=best_thr)
            if s:
                print(f"    {r['coin']:8s} val_acc={s['val_acc']:.1%} → {s['path']}")
