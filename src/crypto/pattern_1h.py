"""
FORTIX 1h Pattern Engine — Short-Term Scalping Model
======================================================

Predicts 1h forward return using features optimized for 1h timeframe.
Shorter windows, faster indicators, same cross-sectional ranking approach.

Key differences from 4h model:
  - WINDOW = 12 (12h lookback instead of 80h)
  - Faster RSI (7-period instead of 14)
  - More time-sensitive features (session, hour momentum)
  - Higher trade frequency: 24 scans/day × 4-5 positions = ~100 trades/day
"""

import sqlite3
import numpy as np
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_1h'
MODEL_DIR.mkdir(exist_ok=True)

COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'FET', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE',
         'JUP', 'RAY', 'PYTH', 'JTO', 'BOME']

WINDOW = 12  # 12 x 1h = 12 hours lookback
FORWARD_CANDLES = 1  # predict next 1h

CS_FEATURES = ['ret_1', 'ret_4', 'rsi', 'bb_position', 'volume_ratio', 'funding_rate']
RANK_FEATURES = ['ret_1', 'ret_4', 'volume_ratio', 'rsi']
DROP_FEATURES = set()  # start clean, drop after analysis


def build_1h_dataset() -> pd.DataFrame:
    """Build training dataset from 1h candles + enrichment data."""
    conn = sqlite3.connect(str(DB_PATH))

    logger.info("Loading 1h candle data...")
    candles = pd.read_sql_query(
        "SELECT coin, timestamp, open, high, low, close, volume "
        "FROM prices WHERE timeframe='1h' ORDER BY coin, timestamp",
        conn
    )
    logger.info(f"  Loaded {len(candles):,} candles for {candles['coin'].nunique()} coins")

    # Enrichment data (daily resolution, matched by date)
    funding = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as val FROM funding_rates GROUP BY coin, d")
    ls_ratio = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, long_ratio as val FROM cg_ls_history")
    taker = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, buy_sell_ratio as val FROM cg_taker_history")
    fg = dict(conn.execute("SELECT date, value FROM fear_greed").fetchall())

    # BTC for correlation
    btc = candles[candles['coin'] == 'BTC'][['timestamp', 'close']].copy()
    btc = btc.rename(columns={'close': 'btc_close'}).set_index('timestamp')

    # 4h candles for multi-timeframe context
    candles_4h = pd.read_sql_query(
        "SELECT coin, timestamp, close FROM prices WHERE timeframe='4h' ORDER BY coin, timestamp", conn)
    daily_candles = pd.read_sql_query(
        "SELECT coin, timestamp, close FROM prices WHERE timeframe='1d' ORDER BY coin, timestamp", conn)

    # Build daily close maps for longer-term context
    daily_close_map = {}
    for coin in COINS:
        cd = daily_candles[daily_candles['coin'] == coin].sort_values('timestamp')
        if len(cd) == 0: continue
        dates = [datetime.utcfromtimestamp(t).strftime('%Y-%m-%d') for t in cd['timestamp'].values]
        daily_close_map[coin] = dict(zip(dates, cd['close'].values))

    conn.close()

    logger.info("Building 1h features...")
    all_rows = []

    for coin in COINS:
        coin_data = candles[candles['coin'] == coin].sort_values('timestamp').reset_index(drop=True)
        if len(coin_data) < WINDOW + FORWARD_CANDLES + 5:
            continue

        closes = coin_data['close'].values
        opens = coin_data['open'].values
        highs = coin_data['high'].values
        lows = coin_data['low'].values
        volumes = coin_data['volume'].values
        timestamps = coin_data['timestamp'].values

        for i in range(WINDOW, len(coin_data) - FORWARD_CANDLES):
            ts = timestamps[i]
            dt_obj = datetime.utcfromtimestamp(ts)
            date_str = dt_obj.strftime('%Y-%m-%d')
            hour = dt_obj.hour

            w_close = closes[i-WINDOW:i]
            w_volume = volumes[i-WINDOW:i]
            w_high = highs[i-WINDOW:i]
            w_low = lows[i-WINDOW:i]
            current_price = closes[i]

            # TARGET: next 1h return
            future_price = closes[i + FORWARD_CANDLES]
            fwd_ret = (future_price / current_price) - 1

            features = {}

            # === Returns (5) — shorter periods for 1h ===
            features['ret_1'] = (closes[i] / closes[i-1] - 1) if i >= 1 else 0
            features['ret_2'] = (closes[i] / closes[i-2] - 1) if i >= 2 else 0
            features['ret_4'] = (closes[i] / closes[i-4] - 1) if i >= 4 else 0
            features['ret_8'] = (closes[i] / closes[i-8] - 1) if i >= 8 else 0
            features['ret_12'] = (closes[i] / closes[i-WINDOW] - 1) if i >= WINDOW else 0

            # === RSI 7-period (faster for 1h) ===
            deltas = np.diff(w_close)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_gain = np.mean(gains[-7:]) if len(gains) >= 7 else 0.001
            avg_loss = np.mean(losses[-7:]) if len(losses) >= 7 else 0.001
            features['rsi'] = 100 - 100 / (1 + avg_gain / (avg_loss + 1e-10))

            # RSI delta (momentum of RSI)
            if len(gains) >= 11:
                prev_g = np.mean(gains[-11:-7])
                prev_l = np.mean(losses[-11:-7])
                features['rsi_delta'] = features['rsi'] - (100 - 100 / (1 + prev_g / (prev_l + 1e-10)))
            else:
                features['rsi_delta'] = 0

            # === Bollinger Bands (12-period for 1h) ===
            ma = np.mean(w_close)
            std = np.std(w_close)
            bb_range = 4 * std
            features['bb_position'] = (current_price - (ma - 2*std)) / bb_range if bb_range > 0 else 0.5
            features['bb_width'] = bb_range / ma if ma > 0 else 0

            # === Volatility ===
            rets = np.diff(w_close) / w_close[:-1]
            features['volatility'] = np.std(rets) if len(rets) > 0 else 0
            features['atr_pct'] = np.mean((w_high - w_low) / w_close)

            # === Volume ===
            vol_avg = np.mean(w_volume)
            features['volume_ratio'] = volumes[i] / (vol_avg + 1e-10)
            features['volume_trend'] = np.mean(w_volume[-4:]) / (np.mean(w_volume[:-4]) + 1e-10) if len(w_volume) >= 8 else 1

            # === Candle patterns ===
            body = closes[i] - opens[i]
            full_range = highs[i] - lows[i]
            features['body_ratio'] = body / (full_range + 1e-10)
            features['upper_wick'] = (highs[i] - max(closes[i], opens[i])) / (full_range + 1e-10)
            features['lower_wick'] = (min(closes[i], opens[i]) - lows[i]) / (full_range + 1e-10)

            # === Range position ===
            features['range_position'] = (current_price - np.min(w_low)) / (np.max(w_high) - np.min(w_low) + 1e-10)

            # === Trend ===
            green_count = sum(1 for j in range(i-6, i) if closes[j] > opens[j])
            features['green_ratio_6'] = green_count / 6

            # === Momentum acceleration ===
            if len(rets) >= 8:
                features['momentum_accel'] = np.mean(rets[-4:]) - np.mean(rets[-8:-4])
            else:
                features['momentum_accel'] = 0

            # === Consecutive moves ===
            consec_up = 0
            for j in range(i-1, max(i-8, 0), -1):
                if closes[j] > opens[j]: consec_up += 1
                else: break
            features['consecutive_green'] = consec_up

            # === Derivatives (daily) ===
            features['funding_rate'] = funding.get(coin, {}).get(date_str, 0) or 0
            features['ls_long_pct'] = ls_ratio.get(coin, {}).get(date_str, 50) or 50
            features['taker_ratio'] = taker.get(coin, {}).get(date_str, 1.0) or 1.0
            features['fg_value'] = fg.get(date_str, 50) or 50

            # === BTC correlation ===
            try:
                btc_now = btc.loc[ts, 'btc_close'] if ts in btc.index else None
                btc_prev = btc.loc[timestamps[i-4], 'btc_close'] if timestamps[i-4] in btc.index else None
                btc_1 = btc.loc[timestamps[i-1], 'btc_close'] if timestamps[i-1] in btc.index else None
                if btc_now and btc_prev and btc_prev > 0:
                    features['btc_divergence'] = features['ret_4'] - (btc_now / btc_prev - 1)
                else:
                    features['btc_divergence'] = 0
                features['btc_ret_1'] = (btc_now / btc_1 - 1) if btc_now and btc_1 and btc_1 > 0 else 0
            except:
                features['btc_divergence'] = 0
                features['btc_ret_1'] = 0

            # === Daily context (from 1d candles) ===
            dcm = daily_close_map.get(coin, {})
            if dcm:
                d_closes = []
                for d_off in range(8):
                    d = (dt_obj - timedelta(days=d_off)).strftime('%Y-%m-%d')
                    if d in dcm: d_closes.append(dcm[d])
                features['daily_ret_7d'] = (d_closes[0] / d_closes[-1] - 1) if len(d_closes) >= 7 else 0
            else:
                features['daily_ret_7d'] = 0

            # === Time features ===
            features['hour'] = hour
            features['is_europe'] = 1 if 8 <= hour < 16 else 0
            features['is_us'] = 1 if 16 <= hour < 24 else 0

            # === Interactions ===
            features['rsi_volume'] = (50 - features['rsi']) / 50 * features['volume_ratio']
            features['market_breadth'] = features['btc_ret_1'] * features['ret_1']

            # Metadata
            features['coin'] = coin
            features['timestamp'] = ts
            features['date'] = date_str
            features['fwd_return'] = fwd_ret

            all_rows.append(features)

    df = pd.DataFrame(all_rows)

    # Cross-sectional features
    logger.info("Adding cross-sectional features...")
    for feat in CS_FEATURES:
        if feat in df.columns:
            df[f'cs_{feat}'] = df.groupby('timestamp')[feat].transform(
                lambda x: (x - x.mean()) / (x.std() + 1e-10))
    for feat in RANK_FEATURES:
        if feat in df.columns:
            df[f'rank_{feat}'] = df.groupby('timestamp')[feat].transform(
                lambda x: x.rank(pct=True))

    meta_cols = ['coin', 'timestamp', 'date', 'fwd_return']
    n_features = len([c for c in df.columns if c not in meta_cols])
    logger.info(f"Dataset: {len(df):,} rows, {df['coin'].nunique()} coins, {n_features} features")

    return df


def _load_daily_map(conn, query):
    try:
        rows = conn.execute(query).fetchall()
        result = {}
        for r in rows:
            if r[0] not in result: result[r[0]] = {}
            result[r[0]][r[1]] = r[2]
        return result
    except:
        return {}


def train_model(df: pd.DataFrame = None) -> dict:
    """Train 1h regression model for cross-sectional ranking."""
    import lightgbm as lgb
    from sklearn.preprocessing import StandardScaler
    from scipy.stats import spearmanr
    import pickle, json

    if df is None:
        df = build_1h_dataset()

    meta_cols = ['coin', 'timestamp', 'date', 'fwd_return']
    feature_cols = [c for c in df.columns if c not in meta_cols]

    logger.info(f"Training 1h model: {len(feature_cols)} features, {len(df):,} samples")

    # Walk-forward 70/30
    df = df.sort_values('timestamp').dropna(subset=['fwd_return'])
    dates = sorted(df['date'].unique())
    split = dates[int(len(dates) * 0.7)]
    train = df[df['date'] < split]
    test = df[df['date'] >= split]

    logger.info(f"Train: {len(train):,} ({train['date'].min()} to {train['date'].max()})")
    logger.info(f"Test:  {len(test):,} ({test['date'].min()} to {test['date'].max()})")

    X_tr = train[feature_cols].fillna(0).values
    X_te = test[feature_cols].fillna(0).values
    y_tr = train['fwd_return'].values
    y_te = test['fwd_return'].values

    scaler = StandardScaler()
    X_trs = scaler.fit_transform(X_tr)
    X_tes = scaler.transform(X_te)

    # Time weights
    train_dates = pd.to_datetime(train['date'])
    weights = np.exp(-(train_dates.max() - train_dates).dt.days / 30)  # 30-day half-life (shorter for 1h)

    si = int(len(X_trs) * 0.85)

    params = {
        'objective': 'regression', 'metric': 'mae',
        'learning_rate': 0.005, 'max_depth': 6, 'num_leaves': 40,
        'min_child_samples': 30, 'subsample': 0.8, 'colsample_bytree': 0.6,
        'reg_alpha': 1.0, 'reg_lambda': 2.0, 'seed': 42, 'verbose': -1,
    }

    td = lgb.Dataset(X_trs[:si], y_tr[:si], weight=weights[:si])
    vd = lgb.Dataset(X_trs[si:], y_tr[si:], reference=td)
    model = lgb.train(params, td, 3000, valid_sets=[vd],
                      callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)])

    preds = model.predict(X_tes)
    sp, _ = spearmanr(preds, y_te)
    mae = np.mean(np.abs(preds - y_te))

    logger.info(f"Spearman={sp:.4f}, MAE={mae*100:.3f}%, iter={model.best_iteration}")

    # Ranking accuracy
    test_df = test.copy()
    test_df['pred'] = preds
    results = {}

    for top_n in [1, 2, 3, 5]:
        c_s = c_l = t_s = t_l = 0
        for ts_val, group in test_df.groupby('timestamp'):
            if len(group) < 8: continue
            sg = group.sort_values('pred')
            shorts = sg.head(top_n)
            longs = sg.tail(top_n)
            c_s += (shorts['fwd_return'] < 0).sum()
            t_s += len(shorts)
            c_l += (longs['fwd_return'] > 0).sum()
            t_l += len(longs)
        short_acc = c_s / t_s * 100 if t_s > 0 else 0
        long_acc = c_l / t_l * 100 if t_l > 0 else 0
        combined = (c_s + c_l) / (t_s + t_l) * 100 if (t_s + t_l) > 0 else 0
        results[f'top{top_n}'] = {'short': round(short_acc, 1), 'long': round(long_acc, 1), 'combined': round(combined, 1)}
        logger.info(f"  Top-{top_n}: SHORT={short_acc:.1f}%, LONG={long_acc:.1f}%, Combined={combined:.1f}%")

    # Feature importance
    imp = model.feature_importance(importance_type='gain')
    imp_idx = np.argsort(imp)[::-1]

    # Save
    model.save_model(str(MODEL_DIR / 'model_1h.lgb'))
    with open(MODEL_DIR / 'scaler_1h.pkl', 'wb') as f:
        pickle.dump(scaler, f)
    with open(MODEL_DIR / 'features_1h.json', 'w') as f:
        json.dump(feature_cols, f)
    with open(MODEL_DIR / 'meta_1h.json', 'w') as f:
        json.dump({
            'spearman': float(sp), 'mae': float(mae),
            'iterations': model.best_iteration,
            'n_features': len(feature_cols),
            'ranking': results,
            'top_features': [feature_cols[i] for i in imp_idx[:15]],
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)

    logger.info(f"Model saved to {MODEL_DIR}")
    return {'spearman': float(sp), 'mae': float(mae), 'ranking': results,
            'top_features': [feature_cols[i] for i in imp_idx[:10]]}


def predict_all_coins_1h(conn=None) -> list:
    """Predict all coins — batch with cross-sectional features."""
    import lightgbm as lgb
    import pickle, json

    model_path = MODEL_DIR / 'model_1h.lgb'
    if not model_path.exists():
        return []

    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB_PATH))

    try:
        model = lgb.Booster(model_file=str(model_path))
        with open(MODEL_DIR / 'scaler_1h.pkl', 'rb') as f:
            scaler = pickle.load(f)
        with open(MODEL_DIR / 'features_1h.json') as f:
            feature_cols = json.load(f)

        # Load enrichment
        funding = _load_daily_map(conn, "SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as val FROM funding_rates GROUP BY coin, d")
        ls_ratio = _load_daily_map(conn, "SELECT coin, date(timestamp, 'unixepoch') as d, long_ratio as val FROM cg_ls_history")
        taker_data = _load_daily_map(conn, "SELECT coin, date(timestamp, 'unixepoch') as d, buy_sell_ratio as val FROM cg_taker_history")
        fg = dict(conn.execute("SELECT date, value FROM fear_greed").fetchall())

        btc_rows = conn.execute("SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='1h' ORDER BY timestamp DESC LIMIT 15").fetchall()
        btc_idx = {r[0]: r[1] for r in btc_rows}

        daily_close_map = {}
        for coin in COINS:
            rows = conn.execute("SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp DESC LIMIT 8", (coin,)).fetchall()
            if rows:
                daily_close_map[coin] = {datetime.utcfromtimestamp(r[0]).strftime('%Y-%m-%d'): r[1] for r in rows}

        all_features = {}
        coin_prices = {}

        for coin in COINS:
            rows = conn.execute(
                "SELECT timestamp, open, high, low, close, volume FROM prices "
                "WHERE coin=? AND timeframe='1h' ORDER BY timestamp DESC LIMIT 15", (coin,)
            ).fetchall()
            if len(rows) < WINDOW: continue
            rows = rows[::-1]

            closes = np.array([r[4] for r in rows])
            opens_arr = np.array([r[1] for r in rows])
            highs = np.array([r[2] for r in rows])
            lows = np.array([r[3] for r in rows])
            volumes = np.array([r[5] for r in rows])
            timestamps = np.array([r[0] for r in rows])

            i = len(closes) - 1
            ts = timestamps[i]
            dt_obj = datetime.utcfromtimestamp(ts)
            date_str = dt_obj.strftime('%Y-%m-%d')
            hour = dt_obj.hour

            w_close = closes[i-WINDOW:i]
            w_volume = volumes[i-WINDOW:i]
            w_high = highs[i-WINDOW:i]
            w_low = lows[i-WINDOW:i]

            f = {}
            f['ret_1'] = closes[i] / closes[i-1] - 1
            f['ret_2'] = closes[i] / closes[i-2] - 1 if i >= 2 else 0
            f['ret_4'] = closes[i] / closes[i-4] - 1 if i >= 4 else 0
            f['ret_8'] = closes[i] / closes[i-8] - 1 if i >= 8 else 0
            f['ret_12'] = closes[i] / closes[i-WINDOW] - 1

            deltas = np.diff(w_close)
            gains = np.where(deltas > 0, deltas, 0)
            loss_arr = np.where(deltas < 0, -deltas, 0)
            ag = np.mean(gains[-7:]) if len(gains) >= 7 else 0.001
            al = np.mean(loss_arr[-7:]) if len(loss_arr) >= 7 else 0.001
            f['rsi'] = 100 - 100 / (1 + ag / (al + 1e-10))
            f['rsi_delta'] = 0

            ma = np.mean(w_close); std = np.std(w_close); bbr = 4 * std
            f['bb_position'] = (closes[i] - (ma - 2*std)) / bbr if bbr > 0 else 0.5
            f['bb_width'] = bbr / ma if ma > 0 else 0

            rets = np.diff(w_close) / w_close[:-1]
            f['volatility'] = np.std(rets)
            f['atr_pct'] = np.mean((w_high - w_low) / w_close)
            f['volume_ratio'] = volumes[i] / (np.mean(w_volume) + 1e-10)
            f['volume_trend'] = np.mean(w_volume[-4:]) / (np.mean(w_volume[:-4]) + 1e-10) if len(w_volume) >= 8 else 1

            body = closes[i] - opens_arr[i]; full_range = highs[i] - lows[i]
            f['body_ratio'] = body / (full_range + 1e-10)
            f['upper_wick'] = (highs[i] - max(closes[i], opens_arr[i])) / (full_range + 1e-10)
            f['lower_wick'] = (min(closes[i], opens_arr[i]) - lows[i]) / (full_range + 1e-10)
            f['range_position'] = (closes[i] - np.min(w_low)) / (np.max(w_high) - np.min(w_low) + 1e-10)
            f['green_ratio_6'] = sum(1 for j in range(i-6, i) if closes[j] > opens_arr[j]) / 6
            f['momentum_accel'] = (np.mean(rets[-4:]) - np.mean(rets[-8:-4])) if len(rets) >= 8 else 0
            consec = 0
            for j in range(i-1, max(i-8, 0), -1):
                if closes[j] > opens_arr[j]: consec += 1
                else: break
            f['consecutive_green'] = consec

            f['funding_rate'] = funding.get(coin, {}).get(date_str, 0) or 0
            f['ls_long_pct'] = ls_ratio.get(coin, {}).get(date_str, 50) or 50
            f['taker_ratio'] = taker_data.get(coin, {}).get(date_str, 1.0) or 1.0
            f['fg_value'] = fg.get(date_str, 50) or 50

            btc_now = btc_idx.get(ts)
            btc_4 = btc_idx.get(timestamps[i-4]) if i >= 4 else None
            btc_1 = btc_idx.get(timestamps[i-1]) if i >= 1 else None
            f['btc_divergence'] = f['ret_4'] - (btc_now / btc_4 - 1) if btc_now and btc_4 and btc_4 > 0 else 0
            f['btc_ret_1'] = (btc_now / btc_1 - 1) if btc_now and btc_1 and btc_1 > 0 else 0

            dcm = daily_close_map.get(coin, {})
            d_vals = list(dcm.values())
            f['daily_ret_7d'] = (d_vals[0] / d_vals[-1] - 1) if len(d_vals) >= 7 else 0

            f['hour'] = hour
            f['is_europe'] = 1 if 8 <= hour < 16 else 0
            f['is_us'] = 1 if 16 <= hour < 24 else 0
            f['rsi_volume'] = (50 - f['rsi']) / 50 * f['volume_ratio']
            f['market_breadth'] = f['btc_ret_1'] * f['ret_1']

            all_features[coin] = f
            coin_prices[coin] = float(closes[i])

        if not all_features: return []

        # Cross-sectional
        fdf = pd.DataFrame(all_features).T
        for feat in CS_FEATURES:
            if feat in fdf.columns:
                m, s = fdf[feat].mean(), fdf[feat].std()
                for c in all_features:
                    all_features[c][f'cs_{feat}'] = (all_features[c].get(feat, 0) - m) / (s + 1e-10)
        for feat in RANK_FEATURES:
            if feat in fdf.columns:
                ranks = fdf[feat].rank(pct=True)
                for c in all_features:
                    all_features[c][f'rank_{feat}'] = float(ranks.get(c, 0.5))

        results = []
        for coin, feat in all_features.items():
            X = np.array([[feat.get(f, 0) for f in feature_cols]])
            X_s = scaler.transform(X)
            reg_score = float(model.predict(X_s)[0])
            results.append({
                'coin': coin, 'reg_score': reg_score,
                'price': coin_prices.get(coin, 0),
            })

        results.sort(key=lambda x: x['reg_score'])
        return results

    except Exception as e:
        logger.error(f"1h prediction failed: {e}", exc_info=True)
        return []
    finally:
        if own_conn: conn.close()


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    logging.basicConfig(level=logging.INFO)

    print("FORTIX 1H MODEL — Training")
    print("=" * 50)

    df = build_1h_dataset()
    print(f"\nDataset: {len(df):,} rows")

    results = train_model(df)
    print(f"\nSpearman: {results['spearman']:.4f}")
    print(f"Ranking: {results['ranking']}")
    print(f"Top features: {results['top_features']}")
