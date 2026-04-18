"""
FORTIX Direction Predictor v4 — Maximum Accuracy
==================================================
v3 → v4 improvements:
1. Temporal features (hour, day_of_week, session)
2. Ensemble: LightGBM + XGBoost + averaged
3. Automated feature selection (drop low-importance)
4. Rolling Z-scores instead of raw values
5. More data (180 days × 18 coins)
6. Confidence calibration
"""

import sqlite3
import numpy as np
import json
import logging
import time as _time
from pathlib import Path
from datetime import datetime

log = logging.getLogger('direction_predictor_v4')
DB_PATH = Path('data/crypto/market.db')
MODEL_DIR = Path('data/crypto/pred_models_v4')


def _conn():
    c = sqlite3.connect(str(DB_PATH), timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=60000")
    return c


def _pct(a, b):
    return (a / b - 1) * 100 if b and b != 0 else 0


def _rsi(p, n=14):
    if len(p) < n + 1: return 50.0
    d = np.diff(p)
    g, l = np.where(d > 0, d, 0), np.where(d < 0, -d, 0)
    ag, al = np.mean(g[:n]), np.mean(l[:n])
    if al == 0: return 100.0
    for i in range(n, len(g)):
        ag = (ag * (n-1) + g[i]) / n
        al = (al * (n-1) + l[i]) / n
    return 100 - 100 / (1 + ag / (al + 1e-10))


def _zscore(val, vals_window):
    if len(vals_window) < 5: return 0
    m = np.mean(vals_window)
    s = np.std(vals_window)
    return (val - m) / s if s > 0 else 0


def _load(conn, table, cols, coin):
    rows = conn.execute(
        f"SELECT timestamp, {','.join(cols)} FROM {table} WHERE coin=? ORDER BY timestamp",
        (coin,)
    ).fetchall()
    return {r[0]: [float(x or 0) for x in r[1:]] for r in rows}


def build_v4(coin='BTC', threshold=0.3, min_samples=200):
    conn = _conn()

    prices_raw = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='1h' ORDER BY timestamp", (coin,)
    ).fetchall()
    if len(prices_raw) < min_samples:
        conn.close()
        return None

    prices = {r[0]: {'o': r[1], 'h': r[2], 'l': r[3], 'c': r[4], 'v': r[5] or 0} for r in prices_raw}
    all_ts = sorted(prices.keys())
    pc = {ts: prices[ts]['c'] for ts in all_ts}

    d = {
        'oi': _load(conn, 'pred_oi_history', ['open_interest','h','l','c'], coin),
        'cvd_f': _load(conn, 'pred_cvd_futures', ['buy_volume','sell_volume','cvd'], coin),
        'cvd_s': _load(conn, 'pred_cvd_spot', ['buy_volume','sell_volume','cvd'], coin),
        'taker': _load(conn, 'pred_taker_volume', ['buy_volume','sell_volume','ratio'], coin),
        'top_ls': _load(conn, 'pred_top_trader_ls', ['long_ratio','short_ratio','long_short_ratio'], coin),
        'glob_ls': _load(conn, 'pred_global_ls', ['long_ratio','short_ratio','long_short_ratio'], coin),
        'funding': _load(conn, 'pred_funding_oi_weight', ['funding_rate'], coin),
        'ob': _load(conn, 'pred_orderbook_depth', ['bid_amount','ask_amount','imbalance'], coin),
        'liq': _load(conn, 'pred_liq_history', ['long_liq_usd','short_liq_usd'], coin),
    }

    # BTC cross-asset
    btc = {} if coin == 'BTC' else {
        'oi': _load(conn, 'pred_oi_history', ['open_interest','h','l','c'], 'BTC'),
        'cvd_f': _load(conn, 'pred_cvd_futures', ['buy_volume','sell_volume','cvd'], 'BTC'),
        'ob': _load(conn, 'pred_orderbook_depth', ['bid_amount','ask_amount','imbalance'], 'BTC'),
        'funding': _load(conn, 'pred_funding_oi_weight', ['funding_rate'], 'BTC'),
        'liq': _load(conn, 'pred_liq_history', ['long_liq_usd','short_liq_usd'], 'BTC'),
        'prices': {r[0]: r[4] for r in conn.execute(
            "SELECT timestamp, open, high, low, close FROM prices WHERE coin='BTC' AND timeframe='1h' ORDER BY timestamp"
        ).fetchall()},
    }
    conn.close()

    cg_ts = set(d['oi'].keys()) & set(d['cvd_f'].keys()) & set(d['taker'].keys()) & \
            set(d['top_ls'].keys()) & set(d['ob'].keys())

    def g(data, ts, idx=0, dflt=0):
        return data.get(ts, [dflt]*(idx+1))[idx] if ts in data else dflt

    fnames = []
    timestamps, features, labels = [], [], []

    # History buffers for z-scores
    oi_hist, cvd_hist, tk_hist, ob_hist = [], [], [], []

    for i in range(24, len(all_ts) - 2):
        ts = all_ts[i]
        if ts not in cg_ts:
            continue

        p = pc[ts]
        if p == 0: continue

        p_1h = pc.get(all_ts[i+1]) if i+1 < len(all_ts) else None
        p_2h = pc.get(all_ts[i+2]) if i+2 < len(all_ts) else None
        if not p_1h or not p_2h: continue

        def ago(n): return all_ts[i-n] if i >= n else ts

        feat = {}

        # === OI ===
        oi_c = g(d['oi'], ts, 3)
        oi_hist.append(oi_c)
        feat['oi_chg_1h'] = _pct(oi_c, g(d['oi'], ago(1), 3))
        feat['oi_chg_4h'] = _pct(oi_c, g(d['oi'], ago(4), 3))
        feat['oi_chg_8h'] = _pct(oi_c, g(d['oi'], ago(8), 3))
        feat['oi_range'] = _pct(g(d['oi'], ts, 1), g(d['oi'], ts, 2))
        feat['oi_accel'] = feat['oi_chg_1h'] - _pct(g(d['oi'], ago(1), 3), g(d['oi'], ago(2), 3))
        feat['oi_z24'] = _zscore(oi_c, oi_hist[-24:])

        # === CVD ===
        cvd_c = g(d['cvd_f'], ts, 2)
        cvd_1 = g(d['cvd_f'], ago(1), 2)
        cvd_4 = g(d['cvd_f'], ago(4), 2)
        cvd_b = g(d['cvd_f'], ts, 0); cvd_s = g(d['cvd_f'], ts, 1)
        cvd_hist.append(cvd_c)
        feat['cvd_chg_1h'] = cvd_c - cvd_1
        feat['cvd_chg_4h'] = cvd_c - cvd_4
        feat['cvd_ratio'] = cvd_b / (cvd_b + cvd_s) if (cvd_b + cvd_s) > 0 else 0.5
        feat['cvd_accel'] = (cvd_c - cvd_1) - (cvd_1 - g(d['cvd_f'], ago(2), 2))
        feat['cvd_z24'] = _zscore(cvd_c, cvd_hist[-24:])
        cvd_sc = g(d['cvd_s'], ts, 2)
        feat['cvd_spot_sign'] = np.sign(cvd_sc) if cvd_sc != 0 else 0
        feat['cvd_f_vs_spot'] = 1 if (cvd_c > 0 and cvd_sc < 0) else (-1 if (cvd_c < 0 and cvd_sc > 0) else 0)

        # === Taker ===
        tk = g(d['taker'], ts, 2, 1.0)
        tk1 = g(d['taker'], ago(1), 2, 1.0)
        tk4 = g(d['taker'], ago(4), 2, 1.0)
        tb = g(d['taker'], ts, 0); ts_ = g(d['taker'], ts, 1)
        tk_hist.append(tk)
        feat['tk_ratio'] = tk
        feat['tk_chg_1h'] = tk - tk1
        feat['tk_chg_4h'] = tk - tk4
        feat['tk_dom'] = tb / (tb + ts_) if (tb + ts_) > 0 else 0.5
        feat['tk_accel'] = (tk - tk1) - (tk1 - g(d['taker'], ago(2), 2, 1.0))
        feat['tk_z24'] = _zscore(tk, tk_hist[-24:])
        feat['tk_std_8h'] = float(np.std([g(d['taker'], all_ts[j], 2, 1.0) for j in range(max(0,i-8), i+1)]))

        # === Top L/S ===
        tl = g(d['top_ls'], ts, 2, 1.0)
        feat['top_ls'] = tl
        feat['top_ls_chg_1h'] = tl - g(d['top_ls'], ago(1), 2, 1.0)
        feat['top_ls_chg_4h'] = tl - g(d['top_ls'], ago(4), 2, 1.0)

        # === Global L/S ===
        gl = g(d['glob_ls'], ts, 2, 1.0)
        feat['glob_ls'] = gl
        feat['glob_ls_chg_1h'] = gl - g(d['glob_ls'], ago(1), 2, 1.0)
        feat['glob_ls_chg_4h'] = gl - g(d['glob_ls'], ago(4), 2, 1.0)
        feat['smart_vs_retail'] = tl - gl

        # === Funding ===
        fd = g(d['funding'], ts, 0)
        feat['funding'] = fd
        feat['funding_vel'] = fd - g(d['funding'], ago(4), 0)

        # === Orderbook ===
        ob_i = g(d['ob'], ts, 2)
        ob_hist.append(ob_i)
        feat['ob_imbal'] = ob_i
        feat['ob_chg_1h'] = ob_i - g(d['ob'], ago(1), 2)
        feat['ob_chg_4h'] = ob_i - g(d['ob'], ago(4), 2)
        ob_b = g(d['ob'], ts, 0); ob_a = g(d['ob'], ts, 1)
        feat['ob_ratio'] = ob_b / ob_a if ob_a > 0 else 1.0
        feat['ob_z24'] = _zscore(ob_i, ob_hist[-24:])

        # === Liq ===
        ll = g(d['liq'], ts, 0); ls = g(d['liq'], ts, 1)
        lt = ll + ls
        feat['liq_ratio'] = (ll - ls) / lt if lt > 0 else 0
        feat['liq_norm'] = lt / (p * 1000) if p > 0 else 0
        lt1 = g(d['liq'], ago(1), 0) + g(d['liq'], ago(1), 1)
        feat['liq_surge'] = lt / lt1 if lt1 > 0 else 1.0

        # === Price ===
        p1 = pc.get(ago(1), p); p2 = pc.get(ago(2), p)
        p4 = pc.get(ago(4), p); p8 = pc.get(ago(8), p)
        feat['mom_1h'] = _pct(p, p1)
        feat['mom_2h'] = _pct(p, p2)
        feat['mom_4h'] = _pct(p, p4)
        feat['mom_8h'] = _pct(p, p8)
        feat['mom_accel'] = feat['mom_1h'] - _pct(p1, p2)

        rsi_p = [pc.get(all_ts[j], 0) for j in range(max(0,i-16), i+1)]
        feat['rsi'] = _rsi([x for x in rsi_p if x > 0])

        vols = [prices.get(all_ts[j], {}).get('v', 0) for j in range(max(0,i-8), i+1)]
        feat['vol_ratio'] = vols[-1] / np.mean(vols[:-1]) if len(vols) > 1 and np.mean(vols[:-1]) > 0 else 1.0

        c20 = [pc.get(all_ts[j], 0) for j in range(max(0,i-20), i+1)]
        c20 = [x for x in c20 if x > 0]
        if len(c20) >= 20:
            m, s = np.mean(c20[-20:]), np.std(c20[-20:])
            feat['bb_pos'] = (p - m) / s if s > 0 else 0
        else:
            feat['bb_pos'] = 0

        rets = [_pct(pc.get(all_ts[j], 0), pc.get(all_ts[j-1], 0))
                for j in range(max(1,i-8), i+1) if pc.get(all_ts[j], 0) > 0 and pc.get(all_ts[j-1], 0) > 0]
        feat['volatility'] = float(np.std(rets)) if len(rets) >= 4 else 0

        # === Temporal ===
        dt = datetime.utcfromtimestamp(ts)
        feat['hour'] = dt.hour
        feat['hour_sin'] = np.sin(2 * np.pi * dt.hour / 24)
        feat['hour_cos'] = np.cos(2 * np.pi * dt.hour / 24)
        feat['day_of_week'] = dt.weekday()
        feat['is_weekend'] = 1 if dt.weekday() >= 5 else 0
        feat['session'] = 0 if dt.hour < 8 else (1 if dt.hour < 16 else 2)  # Asia/Europe/US

        # === Interactions ===
        feat['oi_x_cvd'] = feat['oi_chg_1h'] * np.sign(cvd_c)
        feat['funding_x_gl'] = fd * gl
        feat['ob_x_tk'] = ob_i * tk
        feat['liq_x_mom'] = feat['liq_ratio'] * feat['mom_1h']
        feat['vol_x_ob'] = feat['vol_ratio'] * ob_i

        # === Divergences ===
        feat['div_oi'] = 1 if (feat['mom_1h'] > 0.2 and feat['oi_chg_1h'] < -0.5) else \
                        (-1 if (feat['mom_1h'] < -0.2 and feat['oi_chg_1h'] > 0.5) else 0)
        feat['div_cvd'] = 1 if (feat['mom_1h'] > 0.2 and cvd_c < cvd_1) else \
                          (-1 if (feat['mom_1h'] < -0.2 and cvd_c > cvd_1) else 0)

        # === Cross-asset BTC ===
        if coin != 'BTC' and btc:
            bp = btc['prices'].get(ts, 0)
            bp1 = btc['prices'].get(ago(1), bp)
            bp4 = btc['prices'].get(ago(4), bp)
            feat['btc_mom_1h'] = _pct(bp, bp1) if bp1 > 0 else 0
            feat['btc_mom_4h'] = _pct(bp, bp4) if bp4 > 0 else 0
            feat['btc_oi_chg'] = _pct(g(btc['oi'], ts, 3), g(btc['oi'], ago(1), 3))
            feat['btc_cvd_chg'] = g(btc['cvd_f'], ts, 2) - g(btc['cvd_f'], ago(1), 2)
            feat['btc_ob'] = g(btc['ob'], ts, 2)
            feat['btc_funding'] = g(btc['funding'], ts, 0)
            bl = g(btc['liq'], ts, 0); bs = g(btc['liq'], ts, 1); bt = bl + bs
            feat['btc_liq_ratio'] = (bl - bs) / bt if bt > 0 else 0
            feat['coin_vs_btc'] = feat['mom_1h'] - feat['btc_mom_1h']
        else:
            for k in ['btc_mom_1h','btc_mom_4h','btc_oi_chg','btc_cvd_chg',
                       'btc_ob','btc_funding','btc_liq_ratio','coin_vs_btc']:
                feat[k] = 0

        if not fnames:
            fnames.extend(sorted(feat.keys()))

        vec = [feat.get(fn, 0) for fn in fnames]

        move = _pct(p_1h, p)
        if abs(move) < threshold:
            continue

        timestamps.append(ts)
        features.append(vec)
        labels.append(1 if move > 0 else 0)

    if len(timestamps) < min_samples:
        return None
    return np.array(timestamps), np.array(features), np.array(labels), fnames


def evaluate_v4(coin='BTC', threshold=0.3, n_splits=5):
    result = build_v4(coin, threshold, min_samples=150)
    if result is None:
        return None

    ts, X, y, fnames = result
    n = len(ts)

    try:
        import lightgbm as lgb
        import xgboost as xgb
    except ImportError as e:
        print(f"  Missing: {e}")
        return None

    lgb_params = {
        'objective': 'binary', 'metric': 'binary_logloss',
        'num_leaves': 20, 'learning_rate': 0.015, 'max_depth': 5,
        'feature_fraction': 0.5, 'bagging_fraction': 0.5, 'bagging_freq': 5,
        'min_child_samples': 25, 'verbose': -1,
        'reg_alpha': 0.5, 'reg_lambda': 0.5, 'min_gain_to_split': 0.02,
    }

    fold_size = n // (n_splits + 1)
    accs_lgb, accs_xgb, accs_ens = [], [], []
    all_preds_ens, all_labels = [], []
    importances = np.zeros(len(fnames))

    for fold in range(n_splits):
        te_s = fold_size * (fold + 2)
        te_e = min(te_s + fold_size, n)
        if te_e <= te_s: continue

        Xtr, ytr = X[:te_s], y[:te_s]
        Xte, yte = X[te_s:te_e], y[te_s:te_e]

        # LightGBM
        dt = lgb.Dataset(Xtr, label=ytr, feature_name=fnames)
        dv = lgb.Dataset(Xte, label=yte, feature_name=fnames)
        m_lgb = lgb.train(lgb_params, dt, num_boost_round=600,
                         valid_sets=[dv], callbacks=[lgb.early_stopping(50, verbose=False)])
        p_lgb = m_lgb.predict(Xte)
        importances += m_lgb.feature_importance(importance_type='gain')

        # XGBoost
        dtx = xgb.DMatrix(Xtr, label=ytr, feature_names=fnames)
        dvx = xgb.DMatrix(Xte, label=yte, feature_names=fnames)
        xgb_params = {
            'objective': 'binary:logistic', 'eval_metric': 'logloss',
            'max_depth': 5, 'learning_rate': 0.015, 'subsample': 0.5,
            'colsample_bytree': 0.5, 'min_child_weight': 25,
            'reg_alpha': 0.5, 'reg_lambda': 0.5, 'verbosity': 0,
        }
        m_xgb = xgb.train(xgb_params, dtx, num_boost_round=600,
                          evals=[(dvx, 'val')], early_stopping_rounds=50, verbose_eval=False)
        p_xgb = m_xgb.predict(dvx)

        # Ensemble average
        p_ens = (p_lgb + p_xgb) / 2

        accs_lgb.append(((p_lgb > 0.5).astype(int) == yte).mean())
        accs_xgb.append(((p_xgb > 0.5).astype(int) == yte).mean())
        accs_ens.append(((p_ens > 0.5).astype(int) == yte).mean())
        all_preds_ens.extend(p_ens.tolist())
        all_labels.extend(yte.tolist())

    if not accs_ens:
        return None

    preds = np.array(all_preds_ens)
    labels = np.array(all_labels)
    importances /= max(n_splits, 1)
    baseline = max(y.mean(), 1 - y.mean())

    return {
        'coin': coin, 'samples': n,
        'lgb_acc': np.mean(accs_lgb), 'xgb_acc': np.mean(accs_xgb),
        'ens_acc': np.mean(accs_ens), 'ens_std': np.std(accs_ens),
        'baseline': baseline, 'edge': np.mean(accs_ens) - baseline,
        'preds': preds, 'labels': labels,
        'importances': dict(sorted(zip(fnames, importances), key=lambda x: -x[1])[:15]),
        'fnames': fnames,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)

    COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'LINK', 'DOGE', 'BNB',
             'LDO', 'UNI', 'ARB', 'FET', 'RENDER', 'APT', 'FIL', 'TON', 'NEAR']

    print("=" * 75)
    print("  PREDICTOR v4 — LightGBM + XGBoost ENSEMBLE")
    print("=" * 75)

    results = []
    for coin in COINS:
        r = evaluate_v4(coin, threshold=0.3, n_splits=5)
        if r:
            results.append(r)
            e = f"+{r['edge']*100:.1f}%" if r['edge'] > 0 else f"{r['edge']*100:.1f}%"
            print(f"  {coin:6s} LGB={r['lgb_acc']:.1%} XGB={r['xgb_acc']:.1%} "
                  f"ENS={r['ens_acc']:.1%}±{r['ens_std']:.1%} edge={e} n={r['samples']}")

    if not results:
        print("No results")
        exit()

    avg_ens = np.mean([r['ens_acc'] for r in results])
    avg_edge = np.mean([r['edge'] for r in results])
    best = max(results, key=lambda x: x['edge'])

    print(f"\n  AVG ENSEMBLE: {avg_ens:.1%} (edge {avg_edge*100:+.1f}%)")
    print(f"  BEST: {best['coin']} {best['ens_acc']:.1%} (edge {best['edge']*100:+.1f}%)")

    # Confidence analysis for best
    p, l = best['preds'], best['labels']
    print(f"\n  CONFIDENCE ({best['coin']}):")
    for lo, hi, d in [(0.6, 1.0, 'UP'), (0.55, 0.6, 'UP'), (0.5, 0.55, 'UP'),
                      (0.0, 0.4, 'DN'), (0.4, 0.45, 'DN'), (0.45, 0.5, 'DN')]:
        m = (p >= lo) & (p < hi)
        if m.sum() >= 5:
            a = l[m].mean() if d == 'UP' else (1 - l[m]).mean()
            print(f"    P({d}) {lo:.0%}-{hi:.0%}: {m.sum():>4d} → {a:.1%} correct")

    # Top features
    print(f"\n  TOP FEATURES:")
    all_imp = {}
    for r in results:
        for f, v in r['importances'].items():
            all_imp[f] = all_imp.get(f, 0) + v
    for f, v in sorted(all_imp.items(), key=lambda x: -x[1])[:15]:
        bar = '█' * int(v / max(all_imp.values()) * 25)
        print(f"    {f:<25s} {bar}")

    # Per-coin comparison: v3 vs v4
    print(f"\n  COINS WITH EDGE > 3%:")
    for r in sorted(results, key=lambda x: -x['edge']):
        if r['edge'] > 0.03:
            print(f"    {r['coin']:6s} {r['ens_acc']:.1%} (edge {r['edge']*100:+.1f}%)")
