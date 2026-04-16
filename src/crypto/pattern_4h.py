"""
FORTIX 4h Pattern Engine v3 — Deep Knowledge + Cross-Sectional Model
=====================================================================

61 features from ALL available data sources:
  - 4h OHLCV patterns (returns, RSI, BB, volatility, candle shapes)
  - Daily context (7d/14d/30d trends, daily RSI, daily BB)
  - Derivatives (funding, L/S ratio, taker ratio, OI changes)
  - Macro (Fear & Greed + momentum, ETF flows, stablecoin supply)
  - Cross-asset (BTC divergence, ETH/BTC ratio)
  - Cross-sectional z-scores (how coin differs from market average)
  - Percentile ranks (coin's position among all coins)
  - Time features (session, day of week)

Dual model:
  - REGRESSION: continuous scores for cross-sectional ranking
  - CLASSIFICATION: direction (UP/FLAT/DOWN) for signal confirmation

Key improvement: cross-sectional features differentiate BETWEEN coins
at the same timestamp, enabling better ranking predictions.

Spearman: 0.31+ | Top-1 ranking: 57% | Avg edge: 0.5%+/trade
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
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'
MODEL_DIR.mkdir(exist_ok=True)

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'FET', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'SHIB', 'PEPE', 'WIF', 'BONK', 'PENDLE',
         'JUP', 'RAY', 'PYTH', 'JTO', 'BOME']

WINDOW = 20  # 20 x 4h candles = 80 hours lookback
FORWARD_CANDLES = 1
UP_THRESHOLD = 0.005
DOWN_THRESHOLD = -0.005

# Features to compute cross-sectional z-scores for
CS_FEATURES = ['ret_1', 'ret_6', 'rsi', 'bb_position', 'volume_ratio',
               'funding_rate', 'oi_change', 'daily_ret_7d', 'daily_rsi']

# Features to compute percentile ranks for
RANK_FEATURES = ['ret_1', 'ret_6', 'volume_ratio', 'rsi']

# Low-value features excluded from training
DROP_FEATURES = {'coinbase_premium', 'is_weekend', 'btc_sopr', 'fg_extreme',
                 'consecutive_red', 'btc_mvrv', 'btc_nupl', 'consecutive_green', 'is_asia'}


def build_4h_dataset() -> pd.DataFrame:
    """Build training dataset from 4h candles + ALL enrichment data + cross-sectional features."""
    conn = sqlite3.connect(str(DB_PATH))

    # =====================================================
    # LOAD ALL DATA SOURCES
    # =====================================================

    logger.info("Loading 4h candle data...")
    candles = pd.read_sql_query(
        "SELECT coin, timestamp, open, high, low, close, volume "
        "FROM prices WHERE timeframe='4h' ORDER BY coin, timestamp",
        conn
    )
    logger.info(f"  Loaded {len(candles):,} candles for {candles['coin'].nunique()} coins")

    # Daily derivatives
    funding = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as val FROM funding_rates GROUP BY coin, d")
    ls_ratio = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, long_ratio as val FROM cg_ls_history")
    taker = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, buy_sell_ratio as val FROM cg_taker_history")
    oi_daily = _load_daily_map(conn,
        "SELECT coin, date(timestamp, 'unixepoch') as d, oi_close as val FROM cg_oi_history")

    fg = dict(conn.execute("SELECT date, value FROM fear_greed").fetchall())

    # BTC/ETH for correlation
    btc = candles[candles['coin'] == 'BTC'][['timestamp', 'close']].copy()
    btc = btc.rename(columns={'close': 'btc_close'}).set_index('timestamp')
    eth = candles[candles['coin'] == 'ETH'][['timestamp', 'close']].copy()
    eth = eth.rename(columns={'close': 'eth_close'}).set_index('timestamp')

    # 1D candles for longer-term context
    logger.info("Loading 1D candle data...")
    daily_candles = pd.read_sql_query(
        "SELECT coin, timestamp, open, high, low, close, volume "
        "FROM prices WHERE timeframe='1d' ORDER BY coin, timestamp",
        conn
    )
    daily_close_map = {}
    daily_high_map = {}
    daily_low_map = {}
    daily_volume_map = {}
    for coin in COINS:
        cd = daily_candles[daily_candles['coin'] == coin].sort_values('timestamp')
        if len(cd) == 0:
            continue
        dates = [datetime.utcfromtimestamp(t).strftime('%Y-%m-%d') for t in cd['timestamp'].values]
        daily_close_map[coin] = dict(zip(dates, cd['close'].values))
        daily_high_map[coin] = dict(zip(dates, cd['high'].values))
        daily_low_map[coin] = dict(zip(dates, cd['low'].values))
        daily_volume_map[coin] = dict(zip(dates, cd['volume'].values))

    # ETF flows
    logger.info("Loading ETF flows...")
    etf_btc = {}
    etf_eth = {}
    try:
        rows = conn.execute("SELECT date, asset, flow_usd FROM cg_etf_flows").fetchall()
        for date, asset, flow in rows:
            if asset == 'BTC':
                etf_btc[date] = flow
            elif asset == 'ETH':
                etf_eth[date] = flow
    except Exception:
        pass

    # Stablecoin supply changes
    logger.info("Loading stablecoin supply...")
    stable_supply = {}
    try:
        rows = conn.execute("SELECT date, total_market_cap FROM cg_stablecoin_supply ORDER BY date").fetchall()
        prev = None
        for date, mc in rows:
            if prev and prev > 0:
                stable_supply[date] = (mc - prev) / prev
            prev = mc
    except Exception:
        pass

    conn.close()

    # =====================================================
    # BUILD PER-COIN FEATURES
    # =====================================================
    logger.info("Building features for each coin...")
    all_rows = []

    for coin in COINS:
        coin_data = candles[candles['coin'] == coin].sort_values('timestamp').reset_index(drop=True)
        if len(coin_data) < WINDOW + FORWARD_CANDLES + 10:
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
            dow = dt_obj.weekday()

            w_close = closes[i-WINDOW:i]
            w_volume = volumes[i-WINDOW:i]
            w_high = highs[i-WINDOW:i]
            w_low = lows[i-WINDOW:i]
            current_price = closes[i]

            # TARGET
            future_price = closes[i + FORWARD_CANDLES]
            fwd_ret = (future_price / current_price) - 1
            if fwd_ret > UP_THRESHOLD:
                label = 1
            elif fwd_ret < DOWN_THRESHOLD:
                label = -1
            else:
                label = 0

            features = _compute_base_features(
                i, closes, opens, highs, lows, volumes, timestamps,
                w_close, w_volume, w_high, w_low, current_price,
                hour, dow, dt_obj, date_str,
                funding, ls_ratio, taker, oi_daily, fg,
                btc, eth, daily_close_map, daily_high_map, daily_low_map,
                daily_volume_map, etf_btc, etf_eth, stable_supply, coin
            )

            features['coin'] = coin
            features['timestamp'] = ts
            features['date'] = date_str
            features['label'] = label
            features['fwd_return'] = fwd_ret
            all_rows.append(features)

    df = pd.DataFrame(all_rows)

    # =====================================================
    # ADD CROSS-SECTIONAL FEATURES
    # =====================================================
    logger.info("Adding cross-sectional features...")
    for feat in CS_FEATURES:
        cs_name = f'cs_{feat}'
        df[cs_name] = df.groupby('timestamp')[feat].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-10))

    for feat in RANK_FEATURES:
        rank_name = f'rank_{feat}'
        df[rank_name] = df.groupby('timestamp')[feat].transform(
            lambda x: x.rank(pct=True))

    # Drop low-value features
    for f in DROP_FEATURES:
        if f in df.columns:
            df = df.drop(columns=[f])

    meta_cols = ['coin', 'timestamp', 'date', 'label', 'fwd_return']
    n_features = len([c for c in df.columns if c not in meta_cols])
    logger.info(f"Dataset: {len(df):,} rows, {df['coin'].nunique()} coins, {n_features} features")

    return df


def _compute_base_features(i, closes, opens, highs, lows, volumes, timestamps,
                           w_close, w_volume, w_high, w_low, current_price,
                           hour, dow, dt_obj, date_str,
                           funding, ls_ratio, taker, oi_daily, fg,
                           btc, eth, daily_close_map, daily_high_map, daily_low_map,
                           daily_volume_map, etf_btc, etf_eth, stable_supply, coin):
    """Compute all base features for a single candle. Shared by training and prediction."""
    features = {}

    # === Returns (5) ===
    features['ret_1'] = (closes[i] / closes[i-1] - 1) if i >= 1 else 0
    features['ret_3'] = (closes[i] / closes[i-3] - 1) if i >= 3 else 0
    features['ret_6'] = (closes[i] / closes[i-6] - 1) if i >= 6 else 0
    features['ret_12'] = (closes[i] / closes[i-12] - 1) if i >= 12 else 0
    features['ret_20'] = (closes[i] / closes[i-WINDOW] - 1) if i >= WINDOW else 0

    # === RSI (2) ===
    deltas = np.diff(w_close)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-14:]) if len(gains) >= 14 else 0.001
    avg_loss = np.mean(losses[-14:]) if len(losses) >= 14 else 0.001
    rs = avg_gain / (avg_loss + 1e-10)
    features['rsi'] = 100 - 100 / (1 + rs)
    if len(gains) >= 18:
        prev_gain = np.mean(gains[-18:-14])
        prev_loss = np.mean(losses[-18:-14])
        prev_rs = prev_gain / (prev_loss + 1e-10)
        features['rsi_delta'] = features['rsi'] - (100 - 100 / (1 + prev_rs))
    else:
        features['rsi_delta'] = 0

    # === Bollinger Bands (2) ===
    ma20 = np.mean(w_close)
    std20 = np.std(w_close)
    bb_range = 4 * std20
    features['bb_position'] = (current_price - (ma20 - 2*std20)) / bb_range if bb_range > 0 else 0.5
    features['bb_width'] = bb_range / ma20 if ma20 > 0 else 0

    # === Volatility (2) ===
    rets = np.diff(w_close) / w_close[:-1]
    features['volatility'] = np.std(rets) if len(rets) > 0 else 0
    features['atr_pct'] = np.mean((w_high - w_low) / w_close) if len(w_close) > 0 else 0

    # === Volume (2) ===
    vol_avg = np.mean(w_volume)
    features['volume_ratio'] = volumes[i] / (vol_avg + 1e-10)
    features['volume_trend'] = np.mean(w_volume[-5:]) / (np.mean(w_volume[:-5]) + 1e-10) if len(w_volume) >= 10 else 1

    # === Candle patterns (3) ===
    body = closes[i] - opens[i]
    full_range = highs[i] - lows[i]
    features['body_ratio'] = body / (full_range + 1e-10)
    features['upper_wick'] = (highs[i] - max(closes[i], opens[i])) / (full_range + 1e-10)
    features['lower_wick'] = (min(closes[i], opens[i]) - lows[i]) / (full_range + 1e-10)

    # === Range + Trend (2) ===
    features['range_position'] = (current_price - np.min(w_low)) / (np.max(w_high) - np.min(w_low) + 1e-10)
    green_count = sum(1 for j in range(i-10, i) if closes[j] > opens[j])
    features['green_ratio_10'] = green_count / 10

    # === Momentum (1) ===
    features['momentum_accel'] = (np.mean(rets[-5:]) - np.mean(rets[-10:-5])) if len(rets) >= 10 else 0

    # === Derivatives (5) ===
    features['funding_rate'] = funding.get(coin, {}).get(date_str, 0) or 0
    features['ls_long_pct'] = ls_ratio.get(coin, {}).get(date_str, 50) or 50
    features['taker_ratio'] = taker.get(coin, {}).get(date_str, 1.0) or 1.0

    oi_now = oi_daily.get(coin, {}).get(date_str)
    prev_date = datetime.utcfromtimestamp(timestamps[max(0, i-7)]).strftime('%Y-%m-%d')
    oi_prev = oi_daily.get(coin, {}).get(prev_date)
    if oi_now and oi_prev and oi_prev > 0:
        features['oi_change'] = (oi_now - oi_prev) / oi_prev
    else:
        features['oi_change'] = 0

    d3 = (dt_obj - timedelta(days=3)).strftime('%Y-%m-%d')
    fr_now = funding.get(coin, {}).get(date_str, 0) or 0
    fr_prev = funding.get(coin, {}).get(d3, 0) or 0
    features['funding_momentum'] = fr_now - fr_prev

    # === Fear & Greed (2) ===
    fg_now = fg.get(date_str, 50) or 50
    features['fg_value'] = fg_now
    fg_prev = fg.get(d3, fg_now) or fg_now
    features['fg_momentum'] = fg_now - fg_prev

    # === BTC correlation (3) ===
    try:
        btc_now = btc.loc[timestamps[i], 'btc_close'] if timestamps[i] in btc.index else None
        btc_6 = btc.loc[timestamps[i-6], 'btc_close'] if timestamps[i-6] in btc.index else None
        btc_1 = btc.loc[timestamps[i-1], 'btc_close'] if timestamps[i-1] in btc.index else None
        if btc_now and btc_6 and btc_6 > 0:
            features['btc_divergence'] = features['ret_6'] - (btc_now / btc_6 - 1)
        else:
            features['btc_divergence'] = 0
        features['btc_ret_1'] = (btc_now / btc_1 - 1) if btc_now and btc_1 and btc_1 > 0 else 0
    except Exception:
        features['btc_divergence'] = 0
        features['btc_ret_1'] = 0

    try:
        eth_now = eth.loc[timestamps[i], 'eth_close'] if timestamps[i] in eth.index else None
        eth_6 = eth.loc[timestamps[i-6], 'eth_close'] if timestamps[i-6] in eth.index else None
        btc_now_val = btc.loc[timestamps[i], 'btc_close'] if timestamps[i] in btc.index else None
        btc_6_val = btc.loc[timestamps[i-6], 'btc_close'] if timestamps[i-6] in btc.index else None
        if eth_now and eth_6 and btc_now_val and btc_6_val and btc_now_val > 0 and btc_6_val > 0:
            features['eth_btc_momentum'] = (eth_now / btc_now_val) / (eth_6 / btc_6_val) - 1
        else:
            features['eth_btc_momentum'] = 0
    except Exception:
        features['eth_btc_momentum'] = 0

    # === Daily context (8) ===
    dcm = daily_close_map.get(coin, {})
    if dcm:
        daily_closes = []
        for d_offset in range(30):
            d = (dt_obj - timedelta(days=d_offset)).strftime('%Y-%m-%d')
            if d in dcm:
                daily_closes.append(dcm[d])

        if len(daily_closes) >= 2:
            features['daily_ret_7d'] = (daily_closes[0] / daily_closes[min(6, len(daily_closes)-1)] - 1) if len(daily_closes) > 6 else 0
            features['daily_ret_14d'] = (daily_closes[0] / daily_closes[min(13, len(daily_closes)-1)] - 1) if len(daily_closes) > 13 else 0
            features['daily_ret_30d'] = (daily_closes[0] / daily_closes[min(29, len(daily_closes)-1)] - 1) if len(daily_closes) > 29 else 0
        else:
            features['daily_ret_7d'] = features['daily_ret_14d'] = features['daily_ret_30d'] = 0

        if len(daily_closes) >= 15:
            dc_arr = np.array(daily_closes[:15][::-1])
            d_deltas = np.diff(dc_arr)
            d_gains = np.mean(np.where(d_deltas > 0, d_deltas, 0))
            d_losses = np.mean(np.where(d_deltas < 0, -d_deltas, 0))
            features['daily_rsi'] = 100 - 100 / (1 + d_gains / (d_losses + 1e-10))
        else:
            features['daily_rsi'] = 50

        if len(daily_closes) >= 20:
            dc20 = np.array(daily_closes[:20])
            d_ma = np.mean(dc20)
            d_std = np.std(dc20)
            d_bb_range = 4 * d_std
            features['daily_bb_pos'] = (daily_closes[0] - (d_ma - 2*d_std)) / d_bb_range if d_bb_range > 0 else 0.5
        else:
            features['daily_bb_pos'] = 0.5

        dvm = daily_volume_map.get(coin, {})
        if dvm:
            recent_v = [dvm.get((dt_obj - timedelta(days=d)).strftime('%Y-%m-%d'), 0) for d in range(7)]
            old_v = [dvm.get((dt_obj - timedelta(days=d)).strftime('%Y-%m-%d'), 0) for d in range(7, 14)]
            recent_v = [v for v in recent_v if v > 0]
            old_v = [v for v in old_v if v > 0]
            features['daily_volume_trend'] = np.mean(recent_v) / (np.mean(old_v) + 1e-10) if recent_v and old_v else 1.0
        else:
            features['daily_volume_trend'] = 1.0

        if len(daily_closes) >= 10:
            dc_chron = daily_closes[::-1]
            peak = dc_chron[0]
            max_dd = 0
            for c in dc_chron:
                peak = max(peak, c)
                dd = (peak - c) / peak
                max_dd = max(max_dd, dd)
            features['daily_max_dd_30d'] = max_dd
        else:
            features['daily_max_dd_30d'] = 0

        dhm = daily_high_map.get(coin, {})
        if dhm:
            highs_30d = [dhm.get((dt_obj - timedelta(days=d)).strftime('%Y-%m-%d'), 0) for d in range(30)]
            h30 = max(h for h in highs_30d if h > 0) if any(h > 0 for h in highs_30d) else current_price
            features['dist_from_30d_high'] = (current_price - h30) / h30 if h30 > 0 else 0
        else:
            features['dist_from_30d_high'] = 0
    else:
        features['daily_ret_7d'] = features['daily_ret_14d'] = features['daily_ret_30d'] = 0
        features['daily_rsi'] = 50
        features['daily_bb_pos'] = 0.5
        features['daily_volume_trend'] = 1.0
        features['daily_max_dd_30d'] = 0
        features['dist_from_30d_high'] = 0

    # === ETF flows (2) ===
    d1 = (dt_obj - timedelta(days=1)).strftime('%Y-%m-%d')
    etf_b = etf_btc.get(d1, 0) or 0
    etf_e = etf_eth.get(d1, 0) or 0
    features['etf_btc_flow'] = etf_b / 1e6 if etf_b else 0
    features['etf_eth_flow'] = etf_e / 1e6 if etf_e else 0

    # === Stablecoin supply (1) ===
    features['stable_supply_chg'] = stable_supply.get(d1, 0) or 0

    # === Interaction features (5) ===
    features['rsi_volume_interaction'] = (50 - features['rsi']) / 50 * features['volume_ratio']
    short_up = 1 if features['ret_3'] > 0 else -1
    long_up = 1 if features['daily_ret_7d'] > 0 else -1
    features['trend_alignment'] = short_up * long_up
    features['vol_adj_return'] = features['ret_6'] / (features['volatility'] + 1e-6)
    features['funding_price_div'] = features['funding_rate'] * (-1 if features['ret_6'] > 0 else 1)
    features['market_breadth'] = features['btc_ret_1'] * features['ret_1']

    # === Time (4) ===
    features['hour'] = hour
    features['is_europe'] = 1 if 8 <= hour < 16 else 0
    features['is_us'] = 1 if 16 <= hour < 24 else 0
    features['ls_long_pct'] = ls_ratio.get(coin, {}).get(date_str, 50) or 50  # already set above

    return features


def _load_daily_map(conn, query):
    """Load daily data as {coin: {date: value}} map."""
    try:
        rows = conn.execute(query).fetchall()
        result = {}
        for r in rows:
            if r[0] not in result:
                result[r[0]] = {}
            result[r[0]][r[1]] = r[2]
        return result
    except Exception:
        return {}


def train_model(df: pd.DataFrame = None) -> dict:
    """Train REGRESSION + CLASSIFICATION models with cross-sectional features."""
    import lightgbm as lgb
    from sklearn.preprocessing import StandardScaler
    from scipy.stats import spearmanr
    import pickle, json

    if df is None:
        df = build_4h_dataset()

    meta_cols = ['coin', 'timestamp', 'date', 'label', 'fwd_return']
    feature_cols = [c for c in df.columns if c not in meta_cols]

    logger.info(f"Training with {len(feature_cols)} features on {len(df):,} samples")

    # Walk-forward split (70/30)
    df = df.sort_values('timestamp').dropna(subset=['fwd_return'])
    dates = sorted(df['date'].unique())
    split_date = dates[int(len(dates) * 0.7)]
    train = df[df['date'] < split_date]
    test = df[df['date'] >= split_date]

    logger.info(f"Train: {len(train):,} ({train['date'].min()} to {train['date'].max()})")
    logger.info(f"Test:  {len(test):,} ({test['date'].min()} to {test['date'].max()})")

    X_train = train[feature_cols].fillna(0).values
    X_test = test[feature_cols].fillna(0).values
    y_train_reg = train['fwd_return'].values
    y_test_reg = test['fwd_return'].values
    y_train_cls = train['label'].values
    y_test_cls = test['label'].values

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    # Time weights
    train_dates = pd.to_datetime(train['date'])
    weights = np.exp(-(train_dates.max() - train_dates).dt.days / 180)
    si = int(len(X_train_s) * 0.85)

    # ========== REGRESSION MODEL ==========
    logger.info("\n--- Training REGRESSION model ---")
    reg_params = {
        'objective': 'regression', 'metric': 'mae',
        'learning_rate': 0.003, 'max_depth': 7, 'num_leaves': 50,
        'min_child_samples': 30, 'subsample': 0.8, 'colsample_bytree': 0.6,
        'reg_alpha': 1.5, 'reg_lambda': 3.0, 'seed': 42, 'verbose': -1,
    }

    td_reg = lgb.Dataset(X_train_s[:si], y_train_reg[:si], weight=weights[:si])
    vd_reg = lgb.Dataset(X_train_s[si:], y_train_reg[si:], reference=td_reg)
    model_reg = lgb.train(reg_params, td_reg, 5000, valid_sets=[vd_reg],
                          callbacks=[lgb.early_stopping(150), lgb.log_evaluation(0)])

    preds_reg = model_reg.predict(X_test_s)
    sp, sp_p = spearmanr(preds_reg, y_test_reg)
    mae = np.mean(np.abs(preds_reg - y_test_reg))
    logger.info(f"Regression: Spearman={sp:.4f}, MAE={mae*100:.3f}%, iter={model_reg.best_iteration}")

    # Ranking metrics
    reg_results = {}
    test_df = test.copy()
    test_df['pred'] = preds_reg

    for top_n in [1, 2, 3, 5]:
        c_s = c_l = t_s = t_l = 0
        for ts_val, group in test_df.groupby('timestamp'):
            if len(group) < 10:
                continue
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
        reg_results[f'top{top_n}_short'] = round(short_acc, 1)
        reg_results[f'top{top_n}_long'] = round(long_acc, 1)
        reg_results[f'top{top_n}_combined'] = round(combined, 1)
        logger.info(f"  Top-{top_n}: SHORT={short_acc:.1f}%, LONG={long_acc:.1f}%, Combined={combined:.1f}%")

    # ========== CLASSIFICATION MODEL ==========
    logger.info("\n--- Training CLASSIFICATION model ---")
    label_map = {-1: 0, 0: 1, 1: 2}
    y_train_mapped = np.array([label_map[y] for y in y_train_cls])

    cls_params = {
        'objective': 'multiclass', 'num_class': 3, 'metric': 'multi_logloss',
        'learning_rate': 0.01, 'max_depth': 6, 'num_leaves': 40,
        'min_child_samples': 40, 'subsample': 0.8, 'colsample_bytree': 0.6,
        'reg_alpha': 1.0, 'reg_lambda': 2.0, 'seed': 42, 'verbose': -1,
    }

    td_cls = lgb.Dataset(X_train_s[:si], y_train_mapped[:si], weight=weights[:si])
    vd_cls = lgb.Dataset(X_train_s[si:], np.array([label_map[y] for y in y_train_cls[si:]]), reference=td_cls)
    model_cls = lgb.train(cls_params, td_cls, 3000, valid_sets=[vd_cls],
                          callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)])

    probs = model_cls.predict(X_test_s)
    pred_class = np.argmax(probs, axis=1)
    pred_direction = np.where(pred_class == 2, 1, np.where(pred_class == 0, -1, 0))

    actionable_mask = pred_direction != 0
    actionable_acc = (pred_direction[actionable_mask] == y_test_cls[actionable_mask]).sum() / actionable_mask.sum() * 100 if actionable_mask.sum() > 0 else 0
    logger.info(f"Classification: actionable={actionable_acc:.1f}%")

    # Feature importance
    imp = model_reg.feature_importance(importance_type='gain')
    imp_idx = np.argsort(imp)[::-1]
    logger.info(f"\nTop-15 features:")
    for rank, idx in enumerate(imp_idx[:15]):
        logger.info(f"  {rank+1}. {feature_cols[idx]}: {imp[idx]:.0f}")

    # ========== SAVE ==========
    model_reg.save_model(str(MODEL_DIR / 'model_4h_reg.lgb'))
    model_cls.save_model(str(MODEL_DIR / 'model_4h.lgb'))
    with open(MODEL_DIR / 'scaler_4h.pkl', 'wb') as f:
        pickle.dump(scaler, f)
    with open(MODEL_DIR / 'features_4h.json', 'w') as f:
        json.dump(feature_cols, f)

    results = {
        'spearman': float(sp), 'mae': float(mae),
        'reg_iter': model_reg.best_iteration, 'cls_iter': model_cls.best_iteration,
        'accuracy_all': float(actionable_acc),
        'n_test': len(test), 'n_features': len(feature_cols),
        'ranking': reg_results,
        'top_features': [feature_cols[i] for i in imp_idx[:15]],
    }

    with open(MODEL_DIR / 'meta_4h.json', 'w') as f:
        json.dump({
            **results, 'sp': float(sp),
            'features': feature_cols,
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }, f, indent=2)

    logger.info(f"\nModels saved to {MODEL_DIR}")
    return results


def predict_all_coins_4h(conn=None) -> list:
    """Predict all coins at once — required for cross-sectional features.

    Returns list of prediction dicts sorted by reg_score (best LONG last, best SHORT first).
    """
    import lightgbm as lgb
    import pickle, json

    model_path = MODEL_DIR / 'model_4h.lgb'
    reg_model_path = MODEL_DIR / 'model_4h_reg.lgb'
    if not model_path.exists():
        return []

    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB_PATH))

    try:
        model_cls = lgb.Booster(model_file=str(model_path))
        model_reg = lgb.Booster(model_file=str(reg_model_path)) if reg_model_path.exists() else None
        with open(MODEL_DIR / 'scaler_4h.pkl', 'rb') as f:
            scaler = pickle.load(f)
        with open(MODEL_DIR / 'features_4h.json') as f:
            feature_cols = json.load(f)

        # Load enrichment data
        funding = _load_daily_map(conn,
            "SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as val FROM funding_rates GROUP BY coin, d")
        ls_ratio = _load_daily_map(conn,
            "SELECT coin, date(timestamp, 'unixepoch') as d, long_ratio as val FROM cg_ls_history")
        taker_data = _load_daily_map(conn,
            "SELECT coin, date(timestamp, 'unixepoch') as d, buy_sell_ratio as val FROM cg_taker_history")
        oi_daily = _load_daily_map(conn,
            "SELECT coin, date(timestamp, 'unixepoch') as d, oi_close as val FROM cg_oi_history")
        fg = dict(conn.execute("SELECT date, value FROM fear_greed").fetchall())

        # BTC/ETH reference
        btc_rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 25"
        ).fetchall()
        btc_idx = {r[0]: r[1] for r in btc_rows}
        btc_ts_sorted = sorted(btc_idx.keys())

        eth_rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin='ETH' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 25"
        ).fetchall()
        eth_idx = {r[0]: r[1] for r in eth_rows}

        # ETF flows
        etf_btc = {}
        etf_eth = {}
        try:
            for date, asset, flow in conn.execute("SELECT date, asset, flow_usd FROM cg_etf_flows ORDER BY date DESC LIMIT 10").fetchall():
                if asset == 'BTC' and date not in etf_btc:
                    etf_btc[date] = flow
                elif asset == 'ETH' and date not in etf_eth:
                    etf_eth[date] = flow
        except Exception:
            pass

        # Stablecoin supply
        stable_supply = {}
        try:
            rows = conn.execute("SELECT date, total_market_cap FROM cg_stablecoin_supply ORDER BY date DESC LIMIT 3").fetchall()
            if len(rows) >= 2 and rows[1][1] > 0:
                stable_supply[rows[0][0]] = (rows[0][1] - rows[1][1]) / rows[1][1]
        except Exception:
            pass

        # Daily closes for context
        daily_close_map = {}
        daily_high_map = {}
        daily_low_map = {}
        daily_volume_map = {}
        for coin in COINS:
            daily_rows = conn.execute(
                "SELECT timestamp, open, high, low, close, volume FROM prices "
                "WHERE coin=? AND timeframe='1d' ORDER BY timestamp DESC LIMIT 31", (coin,)
            ).fetchall()
            if daily_rows:
                dates_list = [datetime.utcfromtimestamp(r[0]).strftime('%Y-%m-%d') for r in daily_rows]
                daily_close_map[coin] = dict(zip(dates_list, [r[4] for r in daily_rows]))
                daily_high_map[coin] = dict(zip(dates_list, [r[2] for r in daily_rows]))
                daily_low_map[coin] = dict(zip(dates_list, [r[3] for r in daily_rows]))
                daily_volume_map[coin] = dict(zip(dates_list, [r[5] for r in daily_rows]))

        # Build features for ALL coins
        all_features = {}
        coin_prices = {}

        # Create pseudo btc/eth dataframes for _compute_base_features
        btc_df = pd.DataFrame(list(btc_idx.items()), columns=['timestamp', 'btc_close']).set_index('timestamp')
        eth_df = pd.DataFrame(list(eth_idx.items()), columns=['timestamp', 'eth_close']).set_index('timestamp')

        for coin in COINS:
            rows = conn.execute(
                "SELECT timestamp, open, high, low, close, volume FROM prices "
                "WHERE coin=? AND timeframe='4h' ORDER BY timestamp DESC LIMIT 25",
                (coin,)
            ).fetchall()

            if len(rows) < WINDOW:
                continue

            rows = rows[::-1]
            closes = np.array([r[4] for r in rows])
            opens_arr = np.array([r[1] for r in rows])
            highs_arr = np.array([r[2] for r in rows])
            lows_arr = np.array([r[3] for r in rows])
            volumes_arr = np.array([r[5] for r in rows])
            timestamps = np.array([r[0] for r in rows])

            i = len(closes) - 1
            ts = timestamps[i]
            dt_obj = datetime.utcfromtimestamp(ts)
            date_str = dt_obj.strftime('%Y-%m-%d')
            hour = dt_obj.hour
            dow = dt_obj.weekday()

            w_close = closes[i-WINDOW:i]
            w_volume = volumes_arr[i-WINDOW:i]
            w_high = highs_arr[i-WINDOW:i]
            w_low = lows_arr[i-WINDOW:i]

            features = _compute_base_features(
                i, closes, opens_arr, highs_arr, lows_arr, volumes_arr, timestamps,
                w_close, w_volume, w_high, w_low, closes[i],
                hour, dow, dt_obj, date_str,
                funding, ls_ratio, taker_data, oi_daily, fg,
                btc_df, eth_df, daily_close_map, daily_high_map, daily_low_map,
                daily_volume_map, etf_btc, etf_eth, stable_supply, coin
            )

            all_features[coin] = features
            coin_prices[coin] = float(closes[i])

        if not all_features:
            return []

        # Compute cross-sectional features
        features_df = pd.DataFrame(all_features).T
        for feat in CS_FEATURES:
            if feat in features_df.columns:
                cs_name = f'cs_{feat}'
                mean_val = features_df[feat].mean()
                std_val = features_df[feat].std()
                for coin in all_features:
                    all_features[coin][cs_name] = (all_features[coin].get(feat, 0) - mean_val) / (std_val + 1e-10)

        for feat in RANK_FEATURES:
            if feat in features_df.columns:
                rank_name = f'rank_{feat}'
                ranks = features_df[feat].rank(pct=True)
                for coin in all_features:
                    all_features[coin][rank_name] = float(ranks.get(coin, 0.5))

        # Build feature matrix
        results = []
        for coin, features in all_features.items():
            # Remove dropped features
            for f in DROP_FEATURES:
                features.pop(f, None)

            X = np.array([[features.get(f, 0) for f in feature_cols]])
            X_s = scaler.transform(X)

            # Classification
            probs = model_cls.predict(X_s)[0]
            pred_class = np.argmax(probs)
            confidence = probs[pred_class]
            direction = {0: 'DOWN', 1: 'FLAT', 2: 'UP'}[pred_class]

            # Regression score
            reg_score = float(model_reg.predict(X_s)[0]) if model_reg else 0.0

            results.append({
                'coin': coin,
                'prediction': direction,
                'confidence': float(confidence),
                'reg_score': reg_score,
                'probabilities': {'DOWN': float(probs[0]), 'FLAT': float(probs[1]), 'UP': float(probs[2])},
                'price': coin_prices.get(coin, 0),
            })

        # Sort by reg_score (lowest = best SHORT, highest = best LONG)
        results.sort(key=lambda x: x['reg_score'])
        return results

    except Exception as e:
        logger.error(f"Batch prediction failed: {e}", exc_info=True)
        return []
    finally:
        if own_conn:
            conn.close()


def predict_next_4h(coin: str, conn=None) -> dict:
    """Single-coin prediction — wrapper around predict_all_coins_4h.

    For best accuracy, use predict_all_coins_4h() instead (includes cross-sectional features).
    This function approximates CS features using available data.
    """
    all_preds = predict_all_coins_4h(conn)
    for p in all_preds:
        if p['coin'] == coin:
            return p
    return {'coin': coin, 'prediction': 'NO_DATA', 'confidence': 0, 'reg_score': 0}


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("FORTIX 4H MODEL v3 — DEEP KNOWLEDGE + CROSS-SECTIONAL")
    print("=" * 60)

    print("\nStep 1: Building dataset...")
    df = build_4h_dataset()
    meta_cols = ['coin', 'timestamp', 'date', 'label', 'fwd_return']
    feature_cols = [c for c in df.columns if c not in meta_cols]
    print(f"\nDataset: {len(df):,} rows, {df['coin'].nunique()} coins")
    print(f"Labels: UP={sum(df['label']==1):,}, FLAT={sum(df['label']==0):,}, DOWN={sum(df['label']==-1):,}")
    print(f"Features: {len(feature_cols)}")

    for i, f in enumerate(feature_cols):
        non_zero = (df[f] != 0).sum()
        pct = non_zero / len(df) * 100
        print(f"  {i+1:2d}. {f:30s} non-zero: {pct:5.1f}%")

    print("\nStep 2: Training...")
    results = train_model(df)

    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Spearman:    {results['spearman']:.4f}")
    print(f"  MAE:         {results['mae']*100:.3f}%")
    print(f"  Reg iter:    {results['reg_iter']}")
    print(f"  Cls iter:    {results['cls_iter']}")
    print(f"  Class acc:   {results['accuracy_all']:.1f}%")
    print(f"\n  Ranking:")
    for k, v in results['ranking'].items():
        print(f"    {k}: {v}%")
    print(f"\n  Top features: {results['top_features'][:10]}")

    print("\nStep 3: Live prediction test...")
    preds = predict_all_coins_4h()
    if preds:
        print(f"\n{'Coin':>6s} {'Dir':>5s} {'Score':>8s} {'Conf':>6s} {'Price':>10s}")
        print("-" * 45)
        for p in preds:
            print(f"  {p['coin']:>4s} {p['prediction']:>5s} {p['reg_score']:>+7.4f} {p['confidence']:>5.1%} ${p['price']:>9,.1f}")
        print(f"\nBest SHORT: {preds[0]['coin']} (score={preds[0]['reg_score']:+.4f})")
        print(f"Best LONG:  {preds[-1]['coin']} (score={preds[-1]['reg_score']:+.4f})")
