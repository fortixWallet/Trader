"""
Forecast v3 — Feature & Label Pipeline
Builds ML-ready features from market.db for training and production.

NO future data leaks. All lookback from day T backwards.
LightGBM handles NaN natively — missing features are OK.
"""

import json
import sqlite3
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Coin groups (same as forecast_engine.py)
COIN_GROUPS = {
    'majors': ['BTC', 'ETH'],
    'l1_alts': ['SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK'],
    'defi': ['UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV'],
    'ai': ['FET', 'RENDER', 'TAO'],
    'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}

# Reverse lookup: coin -> group
COIN_TO_GROUP = {}
for group, coins in COIN_GROUPS.items():
    for coin in coins:
        COIN_TO_GROUP[coin] = group
# Remaining coins
for coin in ['ARB', 'OP', 'POL']:
    COIN_TO_GROUP[coin] = 'l1_alts'

DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'market.db'


class FeatureBuilder:
    """Builds features and labels for Forecast v3 training."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(DB_PATH)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ── Data Loaders ─────────────────────────────────────────────

    def _load_prices(self) -> pd.DataFrame:
        """Load daily OHLCV prices."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT coin, timestamp, open, high, low, close, volume "
            "FROM prices WHERE timeframe='1d' ORDER BY coin, timestamp",
            conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _load_funding(self) -> pd.DataFrame:
        """Load funding rates (8-hourly → aggregate to daily)."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT coin, timestamp, rate FROM funding_rates ORDER BY coin, timestamp",
            conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
        df['date'] = pd.to_datetime(df['date'])
        # Daily average funding rate
        daily = df.groupby(['coin', 'date']).agg(
            funding_rate=('rate', 'mean'),
            funding_count=('rate', 'count'),
        ).reset_index()
        return daily

    def _load_fear_greed(self) -> pd.DataFrame:
        """Load Fear & Greed index."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT date, value FROM fear_greed ORDER BY date", conn
        )
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        df.rename(columns={'value': 'fg_value'}, inplace=True)
        return df

    def _load_cq_exchange_flows(self) -> pd.DataFrame:
        """Load CryptoQuant exchange flows (8 coins only)."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT date, coin, netflow, reserve FROM cq_exchange_flows ORDER BY coin, date",
            conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _load_coinbase_premium(self) -> pd.DataFrame:
        """Load Coinbase premium index."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT date, premium_index FROM cq_coinbase_premium ORDER BY date", conn
        )
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _load_etf_flows(self) -> pd.DataFrame:
        """Load BTC ETF flows."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT date, flow_usd FROM cg_etf_flows WHERE asset='BTC' ORDER BY date",
            conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _load_open_interest(self) -> pd.DataFrame:
        """Load OI from CoinGlass historical (5+ years) with fallback to old table."""
        conn = self._conn()
        # Try CoinGlass history first (39,840 records, 2020-2026)
        try:
            df = pd.read_sql_query(
                "SELECT coin, timestamp, oi_close as oi_usdt FROM cg_oi_history ORDER BY coin, timestamp",
                conn,
            )
            if len(df) > 100:
                conn.close()
                df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
                df['date'] = pd.to_datetime(df['date'])
                daily = df.groupby(['coin', 'date']).agg(oi_usdt=('oi_usdt', 'last')).reset_index()
                return daily
        except Exception:
            pass

        # Fallback to old table
        df = pd.read_sql_query(
            "SELECT coin, timestamp, oi_usdt FROM open_interest ORDER BY coin, timestamp", conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
        df['date'] = pd.to_datetime(df['date'])
        daily = df.groupby(['coin', 'date']).agg(oi_usdt=('oi_usdt', 'last')).reset_index()
        return daily

    def _load_long_short(self) -> pd.DataFrame:
        """Load L/S ratios — prefer FRESH data over historical.

        BUG FIX: cg_ls_history was stale (5 days old) but was always used
        because it had >100 rows. Now we ALWAYS use long_short_ratio (live data)
        and MERGE with cg_ls_history for historical coverage.
        """
        conn = self._conn()
        frames = []

        # Primary: long_short_ratio (LIVE, updated hourly)
        try:
            df = pd.read_sql_query(
                "SELECT coin, timestamp, long_ratio, short_ratio, long_short_ratio "
                "FROM long_short_ratio WHERE ratio_type='global' ORDER BY coin, timestamp",
                conn,
            )
            if not df.empty:
                df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
                df['date'] = pd.to_datetime(df['date'])
                daily = df.groupby(['coin', 'date']).agg(
                    ls_long_pct=('long_ratio', 'last'),
                    ls_ratio=('long_short_ratio', 'last'),
                ).reset_index()
                frames.append(daily)
        except Exception:
            pass

        # Secondary: cg_ls_history (historical backfill, may be stale)
        try:
            df2 = pd.read_sql_query(
                "SELECT coin, timestamp, long_ratio, short_ratio, long_short_ratio "
                "FROM cg_ls_history ORDER BY coin, timestamp",
                conn,
            )
            if not df2.empty:
                df2['date'] = pd.to_datetime(df2['timestamp'], unit='s').dt.date
                df2['date'] = pd.to_datetime(df2['date'])
                daily2 = df2.groupby(['coin', 'date']).agg(
                    ls_long_pct=('long_ratio', 'last'),
                    ls_ratio=('long_short_ratio', 'last'),
                ).reset_index()
                frames.append(daily2)
        except Exception:
            pass

        conn.close()

        if not frames:
            return pd.DataFrame()

        # Concat and deduplicate (live data takes priority over historical)
        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=['coin', 'date'], keep='first')
        combined = combined.sort_values(['coin', 'date'])
        return combined

    def _load_taker_volume(self) -> pd.DataFrame:
        """Load taker buy/sell — prefer FRESH live data over historical."""
        conn = self._conn()
        frames = []

        # Primary: taker_volume (LIVE, updated hourly)
        try:
            df = pd.read_sql_query(
                "SELECT coin, timestamp, buy_sell_ratio FROM taker_volume ORDER BY coin, timestamp", conn,
            )
            if not df.empty:
                df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
                df['date'] = pd.to_datetime(df['date'])
                daily = df.groupby(['coin', 'date']).agg(taker_ratio=('buy_sell_ratio', 'last')).reset_index()
                frames.append(daily)
        except Exception:
            pass

        # Secondary: cg_taker_history (historical backfill)
        try:
            df2 = pd.read_sql_query(
                "SELECT coin, timestamp, buy_sell_ratio as taker_ratio "
                "FROM cg_taker_history ORDER BY coin, timestamp", conn,
            )
            if not df2.empty:
                df2['date'] = pd.to_datetime(df2['timestamp'], unit='s').dt.date
                df2['date'] = pd.to_datetime(df2['date'])
                daily2 = df2.groupby(['coin', 'date']).agg(taker_ratio=('taker_ratio', 'last')).reset_index()
                frames.append(daily2)
        except Exception:
            pass

        conn.close()
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=['coin', 'date'], keep='first')
        return combined.sort_values(['coin', 'date'])

    def _load_macro(self) -> pd.DataFrame:
        """Load FRED macro indicators (VIX, yield curve, fed rate, CPI, etc.)."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, event_type, value FROM macro_events "
                "WHERE event_type NOT IN ('fomc_meeting') ORDER BY date",
                conn,
            )
        except Exception:
            conn.close()
            return pd.DataFrame()
        conn.close()
        if df.empty:
            return df
        df['date'] = pd.to_datetime(df['date'])
        # Pivot: each event_type becomes a column
        pivot = df.pivot_table(index='date', columns='event_type', values='value', aggfunc='last')
        pivot = pivot.reset_index().sort_values('date')
        # Forward-fill monthly/infrequent data (CPI, fed_rate, unemployment)
        pivot = pivot.ffill()
        return pivot

    def _load_fomc_dates(self) -> set:
        """Load FOMC meeting dates as a set."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT date FROM macro_events WHERE event_type='fomc_meeting'"
            ).fetchall()
        except Exception:
            conn.close()
            return set()
        conn.close()
        return {pd.to_datetime(r[0]) for r in rows}

    def _load_google_trends(self) -> pd.DataFrame:
        """Load Google Trends weekly data."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, keyword, value FROM google_trends ORDER BY date", conn
            )
        except Exception:
            conn.close()
            return pd.DataFrame()
        conn.close()
        if df.empty:
            return df
        df['date'] = pd.to_datetime(df['date'])
        pivot = df.pivot_table(index='date', columns='keyword', values='value', aggfunc='last')
        pivot = pivot.reset_index().sort_values('date')
        # Rename columns
        pivot.columns = ['date'] + [f'gtrend_{c}' for c in pivot.columns[1:]]
        return pivot

    def _load_defi_tvl(self) -> pd.DataFrame:
        """Load total DeFi TVL history."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, total_tvl FROM defi_tvl_history ORDER BY date", conn
            )
        except Exception:
            conn.close()
            return pd.DataFrame()
        conn.close()
        if df.empty:
            return df
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _load_halvings(self) -> list:
        """Load Bitcoin halving dates."""
        conn = self._conn()
        try:
            rows = conn.execute("SELECT date FROM btc_halvings ORDER BY date").fetchall()
        except Exception:
            conn.close()
            return []
        conn.close()
        return [pd.to_datetime(r[0]) for r in rows]

    def _load_cg_liquidations(self) -> pd.DataFrame:
        """Load CoinGlass aggregated liquidations (limited to ~28 days)."""
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT coin, timestamp, long_liq_usd_24h, short_liq_usd_24h, liq_usd_24h "
            "FROM cg_liquidations ORDER BY coin, timestamp",
            conn,
        )
        conn.close()
        df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
        df['date'] = pd.to_datetime(df['date'])
        daily = df.groupby(['coin', 'date']).agg(
            liq_long_24h=('long_liq_usd_24h', 'last'),
            liq_short_24h=('short_liq_usd_24h', 'last'),
            liq_total_24h=('liq_usd_24h', 'last'),
        ).reset_index()
        return daily

    # ── Feature Computation ──────────────────────────────────────

    def _compute_price_features(self, coin_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute price-based features. All lookback — no future data.

        Features:
          ret_1d..ret_30d: past returns
          volatility_7d/30d: rolling stdev of daily returns
          rsi_14: relative strength index
          bb_position: position within 20-day Bollinger Bands (0=lower, 1=upper)
          ma50_dist, ma200_dist: % distance from moving averages
          volume_ratio_7d: today's volume / 7d avg
          high_low_range_7d: (7d high - 7d low) / 7d avg price
        """
        df = coin_df.copy()
        df = df.sort_values('date').reset_index(drop=True)

        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']

        # Returns (% change)
        for d in [1, 3, 7, 14, 30]:
            df[f'ret_{d}d'] = close.pct_change(d)

        # Daily return for rolling calcs
        daily_ret = close.pct_change()

        # Volatility
        df['volatility_7d'] = daily_ret.rolling(7).std()
        df['volatility_30d'] = daily_ret.rolling(30).std()

        # RSI 14
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(span=14, adjust=False).mean()
        avg_loss = loss.ewm(span=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi_14'] = 100 - (100 / (1 + rs))

        # Bollinger Bands position
        ma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        bb_upper = ma20 + 2 * std20
        bb_lower = ma20 - 2 * std20
        bb_range = (bb_upper - bb_lower).replace(0, np.nan)
        df['bb_position'] = (close - bb_lower) / bb_range

        # MA distances
        ma50 = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        df['ma50_dist'] = (close - ma50) / ma50.replace(0, np.nan)
        df['ma200_dist'] = (close - ma200) / ma200.replace(0, np.nan)

        # Volume ratio (spike detection)
        vol_avg_7d = volume.rolling(7).mean()
        df['volume_ratio_7d'] = volume / vol_avg_7d.replace(0, np.nan)

        # High-low range over 7 days
        high_7d = high.rolling(7).max()
        low_7d = low.rolling(7).min()
        avg_7d = close.rolling(7).mean()
        df['high_low_range_7d'] = (high_7d - low_7d) / avg_7d.replace(0, np.nan)

        return df

    def _add_funding_features(
        self, df: pd.DataFrame, funding: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """
        Add funding rate features.

        Features:
          funding_rate: daily avg funding rate
          funding_rate_7d_avg: 7-day rolling average
          funding_rate_percentile_30d: percentile in 30-day window
        """
        coin_fund = funding[funding['coin'] == coin][['date', 'funding_rate']].copy()
        if coin_fund.empty:
            df['funding_rate'] = np.nan
            df['funding_rate_7d_avg'] = np.nan
            df['funding_rate_pctl_30d'] = np.nan
            return df

        coin_fund = coin_fund.sort_values('date')
        coin_fund['funding_rate_7d_avg'] = coin_fund['funding_rate'].rolling(7).mean()

        # Percentile within 30-day window
        coin_fund['funding_rate_pctl_30d'] = (
            coin_fund['funding_rate']
            .rolling(30)
            .apply(lambda x: (x.values[-1] >= x.values[:-1]).mean() if len(x) > 1 else 0.5, raw=False)
        )

        df = df.merge(
            coin_fund[['date', 'funding_rate', 'funding_rate_7d_avg', 'funding_rate_pctl_30d']],
            on='date', how='left',
        )
        # Forward-fill: funding rate data may lag
        df['funding_rate'] = df['funding_rate'].ffill()
        df['funding_rate_7d_avg'] = df['funding_rate_7d_avg'].ffill()
        df['funding_rate_pctl_30d'] = df['funding_rate_pctl_30d'].ffill()
        return df

    def _add_fg_features(self, df: pd.DataFrame, fg: pd.DataFrame) -> pd.DataFrame:
        """
        Add Fear & Greed features.

        Features:
          fg_value: current F&G index (0-100)
          fg_change_7d: 7-day change
          fg_percentile_30d: 30-day percentile
        """
        fg = fg.copy().sort_values('date')
        fg['fg_change_7d'] = fg['fg_value'].diff(7)
        fg['fg_percentile_30d'] = (
            fg['fg_value']
            .rolling(30)
            .apply(lambda x: (x.values[-1] >= x.values[:-1]).mean() if len(x) > 1 else 0.5, raw=False)
        )
        df = df.merge(
            fg[['date', 'fg_value', 'fg_change_7d', 'fg_percentile_30d']],
            on='date', how='left',
        )
        # Forward-fill: F&G data may lag 1 day
        df['fg_value'] = df['fg_value'].ffill()
        df['fg_change_7d'] = df['fg_change_7d'].ffill()
        df['fg_percentile_30d'] = df['fg_percentile_30d'].ffill()
        return df

    def _add_cq_features(
        self, df: pd.DataFrame, cq_flows: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """
        Add CryptoQuant exchange flow features (available for ~8 coins).

        Features:
          cq_netflow: exchange netflow (positive = inflow = sell pressure)
          cq_netflow_7d_avg: 7-day avg netflow
          cq_reserve_change_7d: % change in exchange reserves over 7 days
        """
        coin_cq = cq_flows[cq_flows['coin'] == coin][['date', 'netflow', 'reserve']].copy()
        if coin_cq.empty:
            df['cq_netflow'] = np.nan
            df['cq_netflow_7d_avg'] = np.nan
            df['cq_reserve_change_7d'] = np.nan
            return df

        coin_cq = coin_cq.sort_values('date')
        coin_cq['cq_netflow'] = coin_cq['netflow']
        coin_cq['cq_netflow_7d_avg'] = coin_cq['netflow'].rolling(7).mean()
        coin_cq['cq_reserve_change_7d'] = coin_cq['reserve'].pct_change(7)

        df = df.merge(
            coin_cq[['date', 'cq_netflow', 'cq_netflow_7d_avg', 'cq_reserve_change_7d']],
            on='date', how='left',
        )
        # Forward-fill: CQ data may lag 1 day
        df['cq_netflow'] = df['cq_netflow'].ffill()
        df['cq_netflow_7d_avg'] = df['cq_netflow_7d_avg'].ffill()
        df['cq_reserve_change_7d'] = df['cq_reserve_change_7d'].ffill()
        return df

    def _add_coinbase_premium(self, df: pd.DataFrame, cb_prem: pd.DataFrame) -> pd.DataFrame:
        """
        Add Coinbase premium index (global, BTC proxy for market sentiment).

        Features:
          coinbase_premium: premium index value
        """
        df = df.merge(
            cb_prem[['date', 'premium_index']].rename(columns={'premium_index': 'coinbase_premium'}),
            on='date', how='left',
        )
        # Forward-fill: coinbase premium may lag 1 day
        df['coinbase_premium'] = df['coinbase_premium'].ffill()
        return df

    def _add_etf_features(self, df: pd.DataFrame, etf: pd.DataFrame, coin: str) -> pd.DataFrame:
        """
        Add BTC ETF flow features (useful for all coins — BTC flows affect market).

        Features:
          etf_flow_usd: daily ETF flow
          etf_flow_7d_avg: 7-day average
        """
        etf = etf.copy().sort_values('date')
        etf['etf_flow_7d_avg'] = etf['flow_usd'].rolling(7).mean()
        etf.rename(columns={'flow_usd': 'etf_flow_usd'}, inplace=True)

        df = df.merge(
            etf[['date', 'etf_flow_usd', 'etf_flow_7d_avg']],
            on='date', how='left',
        )
        # Forward-fill: ETF data may lag on weekends/holidays
        df['etf_flow_usd'] = df['etf_flow_usd'].ffill()
        df['etf_flow_7d_avg'] = df['etf_flow_7d_avg'].ffill()
        return df

    def _add_derivatives_features(
        self, df: pd.DataFrame, coin: str,
        oi: pd.DataFrame, ls: pd.DataFrame,
        taker: pd.DataFrame, liq: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Add derivatives features (OI, L/S, taker, liquidations).
        NOTE: Only ~28 days of data available — will be NaN for most training.

        Features:
          oi_change_1d: OI % change 1 day
          oi_change_7d: OI % change 7 days
          ls_long_pct: % of traders long
          ls_ratio: long/short ratio
          taker_ratio: buy/sell ratio
          liq_long_24h, liq_short_24h: USD liquidated
          liq_ratio: long_liq / total_liq
        """
        # Open Interest
        coin_oi = oi[oi['coin'] == coin][['date', 'oi_usdt']].copy()
        if not coin_oi.empty:
            coin_oi = coin_oi.sort_values('date')
            coin_oi['oi_change_1d'] = coin_oi['oi_usdt'].pct_change(1)
            coin_oi['oi_change_7d'] = coin_oi['oi_usdt'].pct_change(7)
            df = df.merge(
                coin_oi[['date', 'oi_change_1d', 'oi_change_7d']],
                on='date', how='left',
            )
            # Forward-fill: OI data may lag 1 day
            df['oi_change_1d'] = df['oi_change_1d'].ffill()
            df['oi_change_7d'] = df['oi_change_7d'].ffill()
        else:
            df['oi_change_1d'] = np.nan
            df['oi_change_7d'] = np.nan

        # Long/Short ratio
        coin_ls = ls[ls['coin'] == coin][['date', 'ls_long_pct', 'ls_ratio']].copy()
        if not coin_ls.empty:
            df = df.merge(coin_ls, on='date', how='left')
            df['ls_long_pct'] = df['ls_long_pct'].ffill()
            df['ls_ratio'] = df['ls_ratio'].ffill()
        else:
            df['ls_long_pct'] = np.nan
            df['ls_ratio'] = np.nan

        # Taker volume
        coin_taker = taker[taker['coin'] == coin][['date', 'taker_ratio']].copy()
        if not coin_taker.empty:
            df = df.merge(coin_taker, on='date', how='left')
            df['taker_ratio'] = df['taker_ratio'].ffill()
        else:
            df['taker_ratio'] = np.nan

        # Liquidations
        coin_liq = liq[liq['coin'] == coin][['date', 'liq_long_24h', 'liq_short_24h', 'liq_total_24h']].copy()
        if not coin_liq.empty:
            coin_liq['liq_ratio'] = coin_liq['liq_long_24h'] / coin_liq['liq_total_24h'].replace(0, np.nan)
            df = df.merge(
                coin_liq[['date', 'liq_long_24h', 'liq_short_24h', 'liq_ratio']],
                on='date', how='left',
            )
            df['liq_long_24h'] = df['liq_long_24h'].ffill()
            df['liq_short_24h'] = df['liq_short_24h'].ffill()
            df['liq_ratio'] = df['liq_ratio'].ffill()
        else:
            df['liq_long_24h'] = np.nan
            df['liq_short_24h'] = np.nan
            df['liq_ratio'] = np.nan

        return df

    def _add_btc_cross_features(
        self, df: pd.DataFrame, coin: str, btc_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Add BTC cross-asset features.

        Features:
          btc_ret_7d: BTC 7-day return
          btc_ret_30d: BTC 30-day return
          corr_btc_30d: 30-day correlation with BTC
        """
        btc = btc_prices[['date', 'close']].copy().rename(columns={'close': 'btc_close'})
        btc['btc_ret_7d'] = btc['btc_close'].pct_change(7)
        btc['btc_ret_30d'] = btc['btc_close'].pct_change(30)

        df = df.merge(
            btc[['date', 'btc_ret_7d', 'btc_ret_30d']],
            on='date', how='left',
        )
        df['btc_ret_7d'] = df['btc_ret_7d'].ffill()
        df['btc_ret_30d'] = df['btc_ret_30d'].ffill()

        # Correlation with BTC (30d rolling)
        if coin == 'BTC':
            df['corr_btc_30d'] = 1.0
        else:
            # We need BTC daily returns aligned with this coin's returns
            btc_rets = btc.set_index('date')['btc_close'].pct_change()
            coin_rets = df.set_index('date')['close'].pct_change()
            # Align
            aligned = pd.DataFrame({'coin': coin_rets, 'btc': btc_rets}).dropna()
            if len(aligned) >= 30:
                corr = aligned['coin'].rolling(30).corr(aligned['btc'])
                corr_df = corr.reset_index()
                corr_df.columns = ['date', 'corr_btc_30d']
                df = df.merge(corr_df, on='date', how='left')
                df['corr_btc_30d'] = df['corr_btc_30d'].ffill()
            else:
                df['corr_btc_30d'] = np.nan

        return df

    def _add_macro_features(
        self, df: pd.DataFrame, macro: pd.DataFrame, fomc_dates: set,
    ) -> pd.DataFrame:
        """
        Add macro features from FRED.

        Features:
          vix: VIX volatility index
          yield_curve: 10Y-2Y treasury spread
          treasury_10y: 10-year treasury rate
          fed_rate: federal funds rate
          cpi_yoy: CPI year-over-year change
          fomc_meeting: 1 if FOMC meeting today, 0 otherwise
          fomc_days_until: days until next FOMC meeting
        """
        if macro.empty:
            for col in ['vix', 'vix_change_1d', 'yield_curve', 'treasury_10y', 'fed_rate',
                        'cpi_yoy', 'fomc_meeting', 'fomc_days_until',
                        'sp500', 'sp500_ret_1d', 'sp500_ret_7d',
                        'nasdaq', 'nasdaq_ret_1d', 'nasdaq_ret_7d', 'dxy']:
                df[col] = np.nan
            return df

        # Merge macro data (including new S&P500, NASDAQ, DXY)
        macro_cols = []
        for col in ['vix', 'yield_curve', 'treasury_10y', 'fed_rate', 'sp500', 'nasdaq', 'dxy']:
            if col in macro.columns:
                macro_cols.append(col)

        if macro_cols:
            df = df.merge(macro[['date'] + macro_cols], on='date', how='left')
            # Forward-fill for weekends/holidays
            for col in macro_cols:
                if col in df.columns:
                    df[col] = df[col].ffill()

        # S&P500 / NASDAQ / DXY returns (1d and 7d) — crypto correlation features
        for idx_col in ['sp500', 'nasdaq', 'dxy']:
            if idx_col in df.columns:
                # Sort by coin+date to calculate returns correctly per coin-group
                # These are market-wide so same for all coins on same date
                df[f'{idx_col}_ret_1d'] = df.groupby('coin')[idx_col].pct_change(1)
                df[f'{idx_col}_ret_7d'] = df.groupby('coin')[idx_col].pct_change(7)

        # VIX change (crypto fear indicator)
        if 'vix' in df.columns:
            df['vix_change_1d'] = df.groupby('coin')['vix'].diff(1)

        # CPI year-over-year change
        if 'cpi' in macro.columns:
            cpi = macro[['date', 'cpi']].dropna()
            cpi = cpi.sort_values('date')
            cpi['cpi_yoy'] = cpi['cpi'].pct_change(12)  # 12 months
            df = df.merge(cpi[['date', 'cpi_yoy']], on='date', how='left')
            df['cpi_yoy'] = df['cpi_yoy'].ffill()
        else:
            df['cpi_yoy'] = np.nan

        # FOMC meeting flag
        df['fomc_meeting'] = df['date'].isin(fomc_dates).astype(int)

        # Days until next FOMC
        if fomc_dates:
            sorted_fomc = sorted(fomc_dates)
            def days_until_fomc(d):
                for fd in sorted_fomc:
                    if fd >= d:
                        return (fd - d).days
                return np.nan
            df['fomc_days_until'] = df['date'].apply(days_until_fomc)
        else:
            df['fomc_days_until'] = np.nan

        return df

    def _add_trends_features(self, df: pd.DataFrame, trends: pd.DataFrame) -> pd.DataFrame:
        """
        Add Google Trends features.

        Features:
          gtrend_bitcoin: Google search interest for "bitcoin" (0-100)
          gtrend_crypto: Google search interest for "crypto"
          gtrend_bitcoin_change: week-over-week change
        """
        if trends.empty:
            df['gtrend_bitcoin'] = np.nan
            df['gtrend_crypto'] = np.nan
            df['gtrend_bitcoin_change'] = np.nan
            return df

        # Trends are weekly — forward-fill to daily
        trend_cols = [c for c in trends.columns if c.startswith('gtrend_')]
        keep_cols = ['date'] + [c for c in ['gtrend_bitcoin', 'gtrend_crypto'] if c in trend_cols]

        if len(keep_cols) <= 1:
            df['gtrend_bitcoin'] = np.nan
            df['gtrend_crypto'] = np.nan
            df['gtrend_bitcoin_change'] = np.nan
            return df

        df = df.merge(trends[keep_cols], on='date', how='left')
        for col in keep_cols[1:]:
            df[col] = df[col].ffill()

        # Week-over-week change for bitcoin trend
        if 'gtrend_bitcoin' in df.columns:
            df['gtrend_bitcoin_change'] = df['gtrend_bitcoin'].diff(7)
        else:
            df['gtrend_bitcoin_change'] = np.nan

        return df

    def _add_tvl_features(self, df: pd.DataFrame, tvl: pd.DataFrame) -> pd.DataFrame:
        """
        Add DeFi TVL features.

        Features:
          defi_tvl_change_7d: 7-day % change in total DeFi TVL
          defi_tvl_change_30d: 30-day % change
        """
        if tvl.empty:
            df['defi_tvl_change_7d'] = np.nan
            df['defi_tvl_change_30d'] = np.nan
            return df

        tvl = tvl.copy().sort_values('date')
        tvl['defi_tvl_change_7d'] = tvl['total_tvl'].pct_change(7)
        tvl['defi_tvl_change_30d'] = tvl['total_tvl'].pct_change(30)

        df = df.merge(
            tvl[['date', 'defi_tvl_change_7d', 'defi_tvl_change_30d']],
            on='date', how='left',
        )
        df['defi_tvl_change_7d'] = df['defi_tvl_change_7d'].ffill()
        df['defi_tvl_change_30d'] = df['defi_tvl_change_30d'].ffill()
        return df

    def _add_halving_features(self, df: pd.DataFrame, halvings: list) -> pd.DataFrame:
        """
        Add Bitcoin halving cycle features.

        Features:
          days_since_halving: days since last halving
          halving_cycle_phase: 0-1 (0=just halved, 1=about to halve, ~4yr cycle)
        """
        if not halvings:
            df['days_since_halving'] = np.nan
            df['halving_cycle_phase'] = np.nan
            return df

        avg_cycle = 1460  # ~4 years in days

        def halving_features(d):
            days_since = None
            for h in reversed(halvings):
                if d >= h:
                    days_since = (d - h).days
                    break
            if days_since is None:
                return np.nan, np.nan
            phase = min(days_since / avg_cycle, 1.0)
            return days_since, phase

        features = df['date'].apply(halving_features)
        df['days_since_halving'] = features.apply(lambda x: x[0])
        df['halving_cycle_phase'] = features.apply(lambda x: x[1])
        return df

    def _add_calendar_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calendar features.

        Features:
          day_of_week: 0=Monday, 6=Sunday
          is_weekend: 0/1
        """
        df['day_of_week'] = df['date'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        return df

    # ── Event Features ─────────────────────────────────────────────

    def _load_events(self) -> pd.DataFrame:
        """Load crypto_events table."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, event_type, severity, sentiment, coins_affected, "
                "impact_24h, impact_7d FROM crypto_events ORDER BY date",
                conn,
            )
            conn.close()
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _add_event_features(self, df: pd.DataFrame, events: pd.DataFrame,
                            coin: str) -> pd.DataFrame:
        """
        Event-based features from crypto_events table.

        Features:
          event_severity_7d: max severity of events in last 7 days
          event_count_30d: number of events in last 30 days
          event_sentiment_7d: net sentiment score in last 7 days (+1 pos, -1 neg)
          regulatory_events_30d: count of regulatory events in last 30 days
          hack_severity_30d: max hack severity in last 30 days
          days_since_major_event: days since last severity >= 8 event
          event_impact_avg_7d: average historical 24h impact of recent events
          coin_event_7d: whether this coin was specifically affected by an event (0/1)
        """
        if events.empty:
            for col in ['event_severity_7d', 'event_count_30d', 'event_sentiment_7d',
                        'regulatory_events_30d', 'hack_severity_30d',
                        'days_since_major_event', 'event_impact_avg_7d', 'coin_event_7d']:
                df[col] = np.nan
            return df

        # Pre-compute per-date event metrics
        ev = events.copy()
        ev['sent_score'] = ev['sentiment'].map(
            {'positive': 1, 'negative': -1, 'mixed': 0}
        ).fillna(0)

        # For each date in df, compute lookback features
        dates = df['date'].unique()
        event_feats = {}

        for d in dates:
            d_ts = pd.Timestamp(d)
            # 7-day window
            mask_7d = (ev['date'] >= d_ts - pd.Timedelta(days=7)) & (ev['date'] <= d_ts)
            ev_7d = ev[mask_7d]
            # 30-day window
            mask_30d = (ev['date'] >= d_ts - pd.Timedelta(days=30)) & (ev['date'] <= d_ts)
            ev_30d = ev[mask_30d]

            # Max severity in 7 days
            sev_7d = ev_7d['severity'].max() if len(ev_7d) > 0 else 0
            # Event count in 30 days
            count_30d = len(ev_30d)
            # Net sentiment in 7 days
            sent_7d = ev_7d['sent_score'].sum() if len(ev_7d) > 0 else 0
            # Regulatory events in 30 days
            reg_30d = len(ev_30d[ev_30d['event_type'] == 'regulatory'])
            # Hack severity in 30 days
            hacks = ev_30d[ev_30d['event_type'] == 'hack']
            hack_sev = hacks['severity'].max() if len(hacks) > 0 else 0
            # Days since major event (severity >= 8)
            major = ev[ev['severity'] >= 8]
            major_before = major[major['date'] <= d_ts]
            if len(major_before) > 0:
                days_since = (d_ts - major_before['date'].max()).days
            else:
                days_since = 365  # Long time ago
            # Average historical impact of recent events
            avg_impact = ev_7d['impact_24h'].mean() if len(ev_7d) > 0 and ev_7d['impact_24h'].notna().any() else 0
            # Coin-specific event
            coin_ev = 0
            if len(ev_7d) > 0:
                for _, row in ev_7d.iterrows():
                    try:
                        coins_list = json.loads(row['coins_affected']) if row['coins_affected'] else []
                        if coin in coins_list:
                            coin_ev = 1
                            break
                    except (json.JSONDecodeError, TypeError):
                        pass

            event_feats[d_ts] = {
                'event_severity_7d': sev_7d,
                'event_count_30d': count_30d,
                'event_sentiment_7d': sent_7d,
                'regulatory_events_30d': reg_30d,
                'hack_severity_30d': hack_sev,
                'days_since_major_event': days_since,
                'event_impact_avg_7d': avg_impact,
                'coin_event_7d': coin_ev,
            }

        ef_df = pd.DataFrame.from_dict(event_feats, orient='index')
        ef_df.index.name = 'date'
        ef_df = ef_df.reset_index()

        df = df.merge(ef_df, on='date', how='left')
        return df

    def _load_whale_data(self) -> pd.DataFrame:
        """Load whale transactions."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT coin, timestamp, amount_usd, from_label, to_label "
                "FROM whale_transactions WHERE amount_usd > 0 ORDER BY timestamp",
                conn,
            )
            conn.close()
            df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.normalize()
            return df
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _add_whale_features(self, df: pd.DataFrame, whales: pd.DataFrame,
                            coin: str) -> pd.DataFrame:
        """
        Whale transaction features.

        Features:
          whale_volume_7d: total whale transaction volume in USD (7d)
          whale_tx_count_7d: number of whale transactions (7d)
          whale_volume_anomaly: whale volume vs 30d mean (z-score)
          whale_coin_volume_7d: whale volume specifically for this coin (7d)
        """
        if whales.empty:
            for col in ['whale_volume_7d', 'whale_tx_count_7d',
                        'whale_volume_anomaly', 'whale_coin_volume_7d']:
                df[col] = np.nan
            return df

        dates = df['date'].unique()
        whale_feats = {}

        for d in dates:
            d_ts = pd.Timestamp(d)
            mask_7d = (whales['date'] >= d_ts - pd.Timedelta(days=7)) & (whales['date'] <= d_ts)
            w7 = whales[mask_7d]
            mask_30d = (whales['date'] >= d_ts - pd.Timedelta(days=30)) & (whales['date'] <= d_ts)
            w30 = whales[mask_30d]

            vol_7d = w7['amount_usd'].sum() if len(w7) > 0 else 0
            tx_7d = len(w7)

            # Z-score vs 30d
            if len(w30) > 7:
                daily_vol = w30.groupby('date')['amount_usd'].sum()
                mean_30 = daily_vol.mean()
                std_30 = daily_vol.std()
                daily_7d = vol_7d / 7
                anomaly = (daily_7d - mean_30) / std_30 if std_30 > 0 else 0
            else:
                anomaly = 0

            # Coin-specific
            w7_coin = w7[w7['coin'] == coin]
            coin_vol = w7_coin['amount_usd'].sum() if len(w7_coin) > 0 else 0

            whale_feats[d_ts] = {
                'whale_volume_7d': vol_7d,
                'whale_tx_count_7d': tx_7d,
                'whale_volume_anomaly': anomaly,
                'whale_coin_volume_7d': coin_vol,
            }

        wf_df = pd.DataFrame.from_dict(whale_feats, orient='index')
        wf_df.index.name = 'date'
        wf_df = wf_df.reset_index()
        df = df.merge(wf_df, on='date', how='left')
        # Forward-fill: whale data may lag 1 day
        for c in ['whale_volume_7d', 'whale_tx_count_7d', 'whale_volume_anomaly', 'whale_coin_volume_7d']:
            if c in df.columns:
                df[c] = df[c].ffill()
        return df

    def _add_exchange_flow_anomaly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Exchange flow anomaly detection features (built from existing cq_netflow).

        Features:
          exchange_deposit_anomaly: 1 if netflow > 2 std above 30d mean (sell signal)
          exchange_withdrawal_anomaly: 1 if netflow < -2 std below 30d mean (accumulation)
          exchange_flow_zscore: z-score of daily netflow vs 30d average
        """
        if 'cq_netflow' not in df.columns:
            df['exchange_deposit_anomaly'] = np.nan
            df['exchange_withdrawal_anomaly'] = np.nan
            df['exchange_flow_zscore'] = np.nan
            return df

        nf = df['cq_netflow']
        mean_30 = nf.rolling(30, min_periods=7).mean()
        std_30 = nf.rolling(30, min_periods=7).std()

        zscore = (nf - mean_30) / std_30.replace(0, np.nan)
        df['exchange_flow_zscore'] = zscore
        df['exchange_deposit_anomaly'] = (zscore > 2).astype(float)
        df['exchange_withdrawal_anomaly'] = (zscore < -2).astype(float)

        return df

    def _load_stablecoin_supply(self) -> pd.DataFrame:
        """Load stablecoin supply data."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, total_market_cap AS total_supply FROM cg_stablecoin_supply ORDER BY date",
                conn,
            )
            conn.close()
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _add_stablecoin_features(self, df: pd.DataFrame,
                                  stable: pd.DataFrame) -> pd.DataFrame:
        """
        Stablecoin supply features — buying power / capital flows.

        Features:
          stablecoin_supply_change_7d: % change in total stablecoin supply (7d)
          stablecoin_supply_growth: 1 if supply growing, 0 if shrinking
        """
        if stable.empty or 'total_supply' not in stable.columns:
            df['stablecoin_supply_change_7d'] = np.nan
            df['stablecoin_supply_growth'] = np.nan
            return df

        s = stable.sort_values('date').drop_duplicates('date')
        s['supply_change_7d'] = s['total_supply'].pct_change(7) * 100
        s['supply_growth'] = (s['supply_change_7d'] > 0).astype(float)

        s = s[['date', 'supply_change_7d', 'supply_growth']].rename(columns={
            'supply_change_7d': 'stablecoin_supply_change_7d',
            'supply_growth': 'stablecoin_supply_growth',
        })

        df = df.merge(s, on='date', how='left')
        # Forward-fill: stablecoin data may lag 1 day (global, not per-coin)
        df['stablecoin_supply_change_7d'] = df['stablecoin_supply_change_7d'].ffill()
        df['stablecoin_supply_growth'] = df['stablecoin_supply_growth'].ffill()
        return df

    def _load_hacks_data(self) -> pd.DataFrame:
        """Load crypto hacks data."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT date, amount_usd FROM crypto_hacks ORDER BY date",
                conn,
            )
            conn.close()
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _load_twitter_sentiment(self) -> pd.DataFrame:
        """Load Twitter/X sentiment data from social_sentiment table."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT coin, date, score, volume, positive, negative "
                "FROM social_sentiment WHERE source='twitter' ORDER BY coin, date",
                conn,
            )
            conn.close()
            if df.empty:
                return df
            df['date'] = pd.to_datetime(df['date'])
            return df
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _load_orderbook(self) -> pd.DataFrame:
        """Load order book imbalance data."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT coin, timestamp, bid_ask_ratio, imbalance_score "
                "FROM orderbook_imbalance ORDER BY coin, timestamp",
                conn,
            )
            conn.close()
            if df.empty:
                return df
            df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
            df['date'] = pd.to_datetime(df['date'])
            # Daily last value (most recent snapshot per day)
            daily = df.groupby(['coin', 'date']).agg(
                ob_bid_ask_ratio=('bid_ask_ratio', 'last'),
                ob_imbalance_score=('imbalance_score', 'last'),
            ).reset_index()
            return daily
        except Exception:
            conn.close()
            return pd.DataFrame()

    def _add_hack_features(self, df: pd.DataFrame,
                           hacks: pd.DataFrame) -> pd.DataFrame:
        """
        Hack impact features — fear gauge.

        Features:
          hack_total_30d: total $ hacked in last 30 days
          hack_count_30d: number of hacks in last 30 days
        """
        if hacks.empty:
            df['hack_total_30d'] = np.nan
            df['hack_count_30d'] = np.nan
            return df

        dates = df['date'].unique()
        hack_feats = {}

        for d in dates:
            d_ts = pd.Timestamp(d)
            mask = (hacks['date'] >= d_ts - pd.Timedelta(days=30)) & (hacks['date'] <= d_ts)
            h30 = hacks[mask]
            hack_feats[d_ts] = {
                'hack_total_30d': h30['amount_usd'].sum() if len(h30) > 0 else 0,
                'hack_count_30d': len(h30),
            }

        hf_df = pd.DataFrame.from_dict(hack_feats, orient='index')
        hf_df.index.name = 'date'
        hf_df = hf_df.reset_index()
        df = df.merge(hf_df, on='date', how='left')
        return df

    def _add_twitter_features(
        self, df: pd.DataFrame, twitter: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """
        Add Twitter/X sentiment features.

        Features:
          twitter_sentiment: raw sentiment score (-1 to +1)
          twitter_volume: tweet volume for this coin
          twitter_sentiment_7d: 7-day rolling average sentiment
          twitter_volume_anomaly: volume z-score vs 30d mean (unusual activity)
        """
        coin_tw = twitter[twitter['coin'] == coin][['date', 'score', 'volume']].copy()
        if coin_tw.empty:
            df['twitter_sentiment'] = np.nan
            df['twitter_volume'] = np.nan
            df['twitter_sentiment_7d'] = np.nan
            df['twitter_volume_anomaly'] = np.nan
            return df

        coin_tw = coin_tw.sort_values('date').drop_duplicates('date', keep='last')
        coin_tw.rename(columns={'score': 'twitter_sentiment', 'volume': 'twitter_volume'}, inplace=True)

        # 7-day rolling average sentiment
        coin_tw['twitter_sentiment_7d'] = coin_tw['twitter_sentiment'].rolling(7, min_periods=1).mean()

        # Volume z-score vs 30d mean
        vol_mean = coin_tw['twitter_volume'].rolling(30, min_periods=7).mean()
        vol_std = coin_tw['twitter_volume'].rolling(30, min_periods=7).std()
        coin_tw['twitter_volume_anomaly'] = (
            (coin_tw['twitter_volume'] - vol_mean) / vol_std.replace(0, np.nan)
        )

        df = df.merge(
            coin_tw[['date', 'twitter_sentiment', 'twitter_volume',
                      'twitter_sentiment_7d', 'twitter_volume_anomaly']],
            on='date', how='left',
        )
        for c in ['twitter_sentiment', 'twitter_volume', 'twitter_sentiment_7d', 'twitter_volume_anomaly']:
            df[c] = df[c].ffill()
        return df

    def _add_orderbook_features(
        self, df: pd.DataFrame, orderbook: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """
        Add order book imbalance features.

        Features:
          ob_bid_ask_ratio: bid/ask volume ratio (>1 = buy pressure)
          ob_imbalance_score: normalized imbalance (-1 to +1)
          ob_imbalance_7d_avg: 7-day rolling average of imbalance score
        """
        coin_ob = orderbook[orderbook['coin'] == coin][['date', 'ob_bid_ask_ratio', 'ob_imbalance_score']].copy()
        if coin_ob.empty:
            df['ob_bid_ask_ratio'] = np.nan
            df['ob_imbalance_score'] = np.nan
            df['ob_imbalance_7d_avg'] = np.nan
            return df

        coin_ob = coin_ob.sort_values('date').drop_duplicates('date', keep='last')

        # 7-day rolling average of imbalance score
        coin_ob['ob_imbalance_7d_avg'] = coin_ob['ob_imbalance_score'].rolling(7, min_periods=1).mean()

        df = df.merge(
            coin_ob[['date', 'ob_bid_ask_ratio', 'ob_imbalance_score', 'ob_imbalance_7d_avg']],
            on='date', how='left',
        )
        for c in ['ob_bid_ask_ratio', 'ob_imbalance_score', 'ob_imbalance_7d_avg']:
            df[c] = df[c].ffill()
        return df

    # ── v20: NEW DATA LOADERS ────────────────────────────────────

    def _load_liquidation_features(self):
        """Load aggregated liquidation data from multiple sources.

        Priority: daily_liquidation_features (from WebSocket, reliable) +
        cg_liquidations (CoinGlass snapshots, more detail but shorter history).
        """
        conn = self._conn()
        try:
            # Source 1: WebSocket daily aggregates (longer history)
            try:
                ws_df = pd.read_sql_query(
                    "SELECT coin, date, total_usd as liq_total_24h, "
                    "long_usd as liq_long_24h_cg, short_usd as liq_short_24h_cg, "
                    "total_usd * 0.25 as liq_total_4h, "
                    "long_usd * 0.25 as liq_long_4h, "
                    "short_usd * 0.25 as liq_short_4h "
                    "FROM daily_liquidation_features",
                    conn
                )
            except Exception:
                ws_df = pd.DataFrame()

            # Source 2: CoinGlass snapshots (more granular)
            try:
                cg_df = pd.read_sql_query(
                    "SELECT coin, date(timestamp, 'unixepoch') as date, "
                    "AVG(liq_usd_24h) as liq_total_24h, "
                    "AVG(long_liq_usd_24h) as liq_long_24h_cg, "
                    "AVG(short_liq_usd_24h) as liq_short_24h_cg, "
                    "AVG(liq_usd_4h) as liq_total_4h, "
                    "AVG(long_liq_usd_4h) as liq_long_4h, "
                    "AVG(short_liq_usd_4h) as liq_short_4h "
                    "FROM cg_liquidations GROUP BY coin, date(timestamp, 'unixepoch')",
                    conn
                )
            except Exception:
                cg_df = pd.DataFrame()

            # Combine: CoinGlass takes priority (more granular), WebSocket fills gaps
            if not cg_df.empty and not ws_df.empty:
                combined = pd.concat([ws_df, cg_df]).drop_duplicates(['coin', 'date'], keep='last')
            elif not cg_df.empty:
                combined = cg_df
            elif not ws_df.empty:
                combined = ws_df
            else:
                return pd.DataFrame()

            return combined
        except Exception as e:
            logger.warning(f"Failed to load liquidation features: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def _load_exchange_balance_features(self):
        """Load exchange balance data (total across exchanges, per coin)."""
        conn = self._conn()
        try:
            # Sum across exchanges per coin per snapshot
            df = pd.read_sql_query(
                "SELECT coin, date(timestamp, 'unixepoch') as date, "
                "SUM(total_balance) as exch_balance, "
                "SUM(change_1d) as exch_balance_change_1d, "
                "SUM(change_7d) as exch_balance_change_7d, "
                "SUM(change_30d) as exch_balance_change_30d "
                "FROM cg_exchange_balance "
                "GROUP BY coin, date(timestamp, 'unixepoch')",
                conn
            )
            return df
        except Exception as e:
            logger.warning(f"Failed to load exchange balance: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def _load_options_features(self):
        """Load options max pain + put/call ratio (BTC/ETH only)."""
        conn = self._conn()
        try:
            # Get nearest expiry max pain per day
            df = pd.read_sql_query(
                "SELECT coin, date(timestamp, 'unixepoch') as date, "
                "max_pain_price, call_oi, put_oi, "
                "CASE WHEN call_oi > 0 THEN put_oi * 1.0 / call_oi ELSE NULL END as put_call_ratio "
                "FROM cg_options_max_pain "
                "WHERE expiry_date = ("
                "  SELECT MIN(o2.expiry_date) FROM cg_options_max_pain o2 "
                "  WHERE o2.coin = cg_options_max_pain.coin "
                "  AND o2.timestamp = cg_options_max_pain.timestamp "
                "  AND o2.expiry_date >= strftime('%y%m%d', timestamp, 'unixepoch')"
                ")",
                conn
            )
            return df
        except Exception as e:
            logger.warning(f"Failed to load options features: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def _load_4h_indicators(self):
        """Compute RSI, BB, momentum from 4h candles."""
        conn = self._conn()
        try:
            rows = pd.read_sql_query(
                "SELECT coin, timestamp, close, volume FROM prices "
                "WHERE timeframe='4h' ORDER BY coin, timestamp",
                conn
            )
            if rows.empty:
                return pd.DataFrame()

            rows['date'] = pd.to_datetime(rows['timestamp'], unit='s').dt.strftime('%Y-%m-%d')
            results = []

            for coin, gdf in rows.groupby('coin'):
                gdf = gdf.sort_values('timestamp')
                closes = gdf['close'].values
                if len(closes) < 30:
                    continue

                # RSI 14 on 4h
                deltas = np.diff(closes)
                gains = np.where(deltas > 0, deltas, 0)
                losses = np.where(deltas < 0, -deltas, 0)
                avg_gain = pd.Series(gains).ewm(span=14, min_periods=14).mean().values
                avg_loss = pd.Series(losses).ewm(span=14, min_periods=14).mean().values
                rs = avg_gain / (avg_loss + 1e-10)
                rsi_4h = 100 - 100 / (1 + rs)

                # BB position on 4h (20-period)
                ma20 = pd.Series(closes[1:]).rolling(20, min_periods=20).mean().values
                std20 = pd.Series(closes[1:]).rolling(20, min_periods=20).std().values
                bb_upper = ma20 + 2 * std20
                bb_lower = ma20 - 2 * std20
                bb_range = bb_upper - bb_lower
                bb_pos_4h = np.where(bb_range > 0, (closes[1:] - bb_lower) / bb_range, 0.5)

                # 4h momentum (6 candles = 24h, 18 candles = 3 days)
                ret_24h_4h = np.full(len(closes) - 1, np.nan)
                for i in range(6, len(closes) - 1):
                    ret_24h_4h[i] = closes[i + 1] / closes[i - 5] - 1

                # Take last value per day
                temp = gdf.iloc[1:].copy()
                temp['rsi_4h'] = rsi_4h
                temp['bb_position_4h'] = bb_pos_4h
                temp['ret_24h_4h'] = ret_24h_4h

                daily = temp.groupby('date').last()[['rsi_4h', 'bb_position_4h', 'ret_24h_4h']].reset_index()
                daily['coin'] = coin
                results.append(daily)

            if results:
                return pd.concat(results, ignore_index=True)
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to compute 4h indicators: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    # ── v20.3: CATEGORY 1 DATA LOADERS (from existing data) ─────

    def _load_exchange_specific_flows(self):
        """Load Coinbase vs Binance balance changes for institutional/retail divergence."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT coin, date(timestamp, 'unixepoch') as date, exchange, "
                "change_1d, change_7d, change_30d, total_balance "
                "FROM cg_exchange_balance "
                "WHERE exchange IN ('Coinbase','Binance') "
                "ORDER BY coin, timestamp",
                conn
            )
            return df
        except Exception as e:
            logger.warning(f"Failed to load exchange-specific flows: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def _load_news_impact_scores(self):
        """Load Claude-evaluated news impact scores."""
        conn = self._conn()
        try:
            df = pd.read_sql_query(
                "SELECT n.timestamp, ne.impact_score, ne.urgency, ne.category, "
                "n.coins_mentioned "
                "FROM news_evaluations ne "
                "JOIN news n ON ne.news_hash = hex(substr(n.title, 1, 8)) "
                "WHERE ne.impact_score IS NOT NULL",
                conn
            )
            if df.empty:
                # Fallback: use news_evaluations directly with date from evaluated_at
                df = pd.read_sql_query(
                    "SELECT impact_score, urgency, category, evaluated_at "
                    "FROM news_evaluations WHERE impact_score IS NOT NULL",
                    conn
                )
            return df
        except Exception as e:
            logger.warning(f"Failed to load news impact: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    # ── v20.3: CATEGORY 1 FEATURE ADDERS ─────────────────────

    def _add_exchange_divergence_features(
        self, df: pd.DataFrame, exch_flows: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """Coinbase vs Binance flow divergence.

        Coinbase = institutional (US), Binance = retail (global).
        When they diverge: institutional buying while retail selling = strong bullish.

        Features:
          coinbase_binance_divergence: Coinbase 7d change - Binance 7d change (normalized)
          institutional_flow_7d: Coinbase 7d flow direction (negative = outflow = buying)
        """
        if exch_flows.empty or coin not in exch_flows['coin'].values:
            df['coinbase_binance_divergence'] = np.nan
            df['institutional_flow_7d'] = np.nan
            return df

        coin_flows = exch_flows[exch_flows['coin'] == coin].copy()
        coin_flows['date'] = pd.to_datetime(coin_flows['date'])

        # Pivot: one row per date with Coinbase and Binance columns
        cb = coin_flows[coin_flows['exchange'] == 'Coinbase'][['date', 'change_7d', 'total_balance']].copy()
        cb = cb.rename(columns={'change_7d': 'cb_7d', 'total_balance': 'cb_bal'})
        bn = coin_flows[coin_flows['exchange'] == 'Binance'][['date', 'change_7d', 'total_balance']].copy()
        bn = bn.rename(columns={'change_7d': 'bn_7d', 'total_balance': 'bn_bal'})

        merged = cb.merge(bn, on='date', how='outer').sort_values('date').drop_duplicates('date', keep='last')

        # Divergence: normalize by balance size
        merged['coinbase_binance_divergence'] = np.where(
            (merged['cb_bal'] > 0) & (merged['bn_bal'] > 0),
            (merged['cb_7d'] / merged['cb_bal']) - (merged['bn_7d'] / merged['bn_bal']),
            0
        )
        # Institutional flow: Coinbase outflow = buying (negative = bullish)
        merged['institutional_flow_7d'] = np.where(
            merged['cb_bal'] > 0, merged['cb_7d'] / merged['cb_bal'], 0
        )

        merge_cols = ['date', 'coinbase_binance_divergence', 'institutional_flow_7d']
        df = df.merge(merged[merge_cols], on='date', how='left')
        for c in merge_cols[1:]:
            df[c] = df[c].ffill()
        return df

    def _add_futures_basis_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Futures basis (annualized) from funding rates.

        Basis = funding_rate * 3 * 365 (8h → annual).
        High basis (>20%) = market overheated, expect correction.
        Negative basis = bearish, shorts dominating.

        Features:
          futures_basis_ann: annualized basis from funding rate
          basis_momentum: 7d change in basis (accelerating vs decelerating)
        """
        if 'funding_rate_7d_avg' in df.columns:
            df['futures_basis_ann'] = df['funding_rate_7d_avg'] * 3 * 365
            df['basis_momentum'] = df['futures_basis_ann'] - df['futures_basis_ann'].shift(7)
        else:
            df['futures_basis_ann'] = np.nan
            df['basis_momentum'] = np.nan
        return df

    def _add_whale_entity_features(
        self, df: pd.DataFrame, whale_data: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """Whale entity classification: institutional vs retail exchange flows.

        Track whether whales are sending TO exchanges (selling) or FROM (buying).

        Features:
          whale_to_exchange_ratio: fraction of whale volume going TO exchanges
          whale_net_exchange_flow: net flow to/from exchanges (positive = selling)
        """
        if whale_data.empty:
            df['whale_to_exchange_ratio'] = np.nan
            df['whale_net_exchange_flow'] = np.nan
            return df

        EXCHANGE_KEYWORDS = ['binance', 'coinbase', 'kraken', 'bitfinex', 'okex',
                            'bybit', 'kucoin', 'huobi', 'gate', 'gemini', 'bitstamp']

        coin_whales = whale_data[whale_data['coin'] == coin].copy() if 'coin' in whale_data.columns else whale_data.copy()
        if coin_whales.empty:
            df['whale_to_exchange_ratio'] = np.nan
            df['whale_net_exchange_flow'] = np.nan
            return df

        # Classify: to_exchange vs from_exchange
        def is_exchange(label):
            if not label or pd.isna(label):
                return False
            label_lower = str(label).lower()
            return any(ex in label_lower for ex in EXCHANGE_KEYWORDS)

        coin_whales['to_exch'] = coin_whales['to_label'].apply(is_exchange) if 'to_label' in coin_whales.columns else False
        coin_whales['from_exch'] = coin_whales['from_label'].apply(is_exchange) if 'from_label' in coin_whales.columns else False

        # Daily aggregation
        daily = coin_whales.groupby('date').agg(
            vol_to_exch=('amount_usd', lambda x: x[coin_whales.loc[x.index, 'to_exch']].sum()),
            vol_from_exch=('amount_usd', lambda x: x[coin_whales.loc[x.index, 'from_exch']].sum()),
            total_vol=('amount_usd', 'sum'),
        ).reset_index()

        daily['date'] = pd.to_datetime(daily['date'])
        daily['whale_to_exchange_ratio'] = np.where(
            daily['total_vol'] > 0, daily['vol_to_exch'] / daily['total_vol'], 0.5
        )
        daily['whale_net_exchange_flow'] = daily['vol_to_exch'] - daily['vol_from_exch']

        # 7-day rolling
        daily = daily.sort_values('date')
        daily['whale_to_exchange_ratio'] = daily['whale_to_exchange_ratio'].rolling(7, min_periods=1).mean()
        daily['whale_net_exchange_flow'] = daily['whale_net_exchange_flow'].rolling(7, min_periods=1).sum()

        merge_cols = ['date', 'whale_to_exchange_ratio', 'whale_net_exchange_flow']
        df = df.merge(daily[merge_cols], on='date', how='left')
        for c in merge_cols[1:]:
            df[c] = df[c].ffill()
        return df

    def _add_news_impact_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """News impact scores from Claude evaluations.

        Features:
          news_avg_impact_7d: average Claude impact score (1-10) over 7 days
          news_high_impact_count_7d: count of high-impact (>=7) news in 7 days
        """
        conn = self._conn()
        try:
            # Get daily news impact aggregates
            news_df = pd.read_sql_query(
                "SELECT date(evaluated_at) as date, "
                "AVG(impact_score) as avg_impact, "
                "SUM(CASE WHEN impact_score >= 7 THEN 1 ELSE 0 END) as high_impact_count, "
                "COUNT(*) as news_count "
                "FROM news_evaluations "
                "GROUP BY date(evaluated_at)",
                conn
            )
            if news_df.empty:
                df['news_avg_impact_7d'] = np.nan
                df['news_high_impact_count_7d'] = np.nan
                return df

            news_df['date'] = pd.to_datetime(news_df['date'])
            news_df = news_df.sort_values('date')

            # 7-day rolling
            news_df['news_avg_impact_7d'] = news_df['avg_impact'].rolling(7, min_periods=1).mean()
            news_df['news_high_impact_count_7d'] = news_df['high_impact_count'].rolling(7, min_periods=1).sum()

            merge_cols = ['date', 'news_avg_impact_7d', 'news_high_impact_count_7d']
            df = df.merge(news_df[merge_cols], on='date', how='left')
            for c in merge_cols[1:]:
                df[c] = df[c].ffill()
        except Exception as e:
            logger.warning(f"Failed to add news impact features: {e}")
            df['news_avg_impact_7d'] = np.nan
            df['news_high_impact_count_7d'] = np.nan
        finally:
            conn.close()
        return df

    # ── v20.3: CATEGORY 2 FEATURES (new free APIs) ──────────────

    def _add_m2_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Global M2 money supply — strongest macro predictor of BTC.

        BTC follows M2 with ~90 day lag. M2 growing = liquidity = bullish.
        """
        conn = self._conn()
        try:
            m2 = pd.read_sql_query(
                "SELECT date, value FROM macro_events WHERE event_type='m2_money_supply' ORDER BY date",
                conn
            )
            if m2.empty:
                df['m2_growth_90d'] = np.nan
                df['m2_momentum'] = np.nan
                return df

            m2['date'] = pd.to_datetime(m2['date'])
            m2 = m2.sort_values('date').drop_duplicates('date', keep='last')
            # M2 is weekly — forward-fill to daily
            m2 = m2.set_index('date').resample('D').ffill().reset_index()

            # 90-day growth (the key predictor)
            m2['m2_growth_90d'] = m2['value'].pct_change(90)
            # Momentum: is growth accelerating or decelerating?
            m2['m2_momentum'] = m2['m2_growth_90d'] - m2['m2_growth_90d'].shift(30)

            merge_cols = ['date', 'm2_growth_90d', 'm2_momentum']
            df = df.merge(m2[merge_cols], on='date', how='left')
            for c in merge_cols[1:]:
                df[c] = df[c].ffill()
        except Exception as e:
            logger.warning(f"M2 features failed: {e}")
            df['m2_growth_90d'] = np.nan
            df['m2_momentum'] = np.nan
        finally:
            conn.close()
        return df

    def _add_hashrate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Bitcoin hashrate — miner confidence/capitulation indicator.

        Rising hashrate = miners investing = bullish conviction.
        Falling hashrate = miners capitulating = bearish / difficulty adjustment.
        """
        conn = self._conn()
        try:
            hr = pd.read_sql_query(
                "SELECT date, hashrate, difficulty FROM btc_hashrate ORDER BY date", conn
            )
            if hr.empty:
                df['hashrate_change_30d'] = np.nan
                df['difficulty_change'] = np.nan
                return df

            hr['date'] = pd.to_datetime(hr['date'])
            hr = hr.sort_values('date').drop_duplicates('date', keep='last')

            # 30-day hashrate change
            hr['hashrate_change_30d'] = hr['hashrate'].pct_change(30)
            # Difficulty change (biweekly adjustments)
            hr['difficulty_change'] = hr['difficulty'].pct_change(14)

            merge_cols = ['date', 'hashrate_change_30d', 'difficulty_change']
            df = df.merge(hr[merge_cols], on='date', how='left')
            for c in merge_cols[1:]:
                df[c] = df[c].ffill()
        except Exception as e:
            logger.warning(f"Hashrate features failed: {e}")
            df['hashrate_change_30d'] = np.nan
            df['difficulty_change'] = np.nan
        finally:
            conn.close()
        return df

    # ── v20: NEW FEATURE ADDERS ────────────────────────────────

    def _add_liquidation_cascade_features(
        self, df: pd.DataFrame, liq_data: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """Liquidation cascade features from CoinGlass aggregated data.

        Features:
          liq_cascade_ratio: long vs short liquidation asymmetry (-1 to +1)
          liq_intensity_24h: total liq USD normalized by coin (z-score)
          liq_4h_spike: 4h liquidation as fraction of 24h (spike detection)
        """
        coin_liq = liq_data[liq_data['coin'] == coin].copy() if not liq_data.empty else pd.DataFrame()
        if coin_liq.empty:
            df['liq_cascade_ratio'] = np.nan
            df['liq_intensity_24h'] = np.nan
            df['liq_4h_spike'] = np.nan
            return df

        coin_liq['date'] = pd.to_datetime(coin_liq['date'])
        coin_liq = coin_liq.sort_values('date').drop_duplicates('date', keep='last')

        # Long/short asymmetry: (long - short) / total → +1 = all longs liquidated (bearish before, now bullish)
        total = coin_liq['liq_long_24h_cg'] + coin_liq['liq_short_24h_cg']
        coin_liq['liq_cascade_ratio'] = np.where(
            total > 0,
            (coin_liq['liq_long_24h_cg'] - coin_liq['liq_short_24h_cg']) / total,
            0
        )

        # Intensity z-score (how abnormal is today's liquidation volume)
        rolling_mean = coin_liq['liq_total_24h'].rolling(14, min_periods=3).mean()
        rolling_std = coin_liq['liq_total_24h'].rolling(14, min_periods=3).std()
        coin_liq['liq_intensity_24h'] = np.where(
            rolling_std > 0,
            (coin_liq['liq_total_24h'] - rolling_mean) / rolling_std,
            0
        )

        # 4h spike: if 4h liq is >40% of 24h → cascade in progress
        coin_liq['liq_4h_spike'] = np.where(
            coin_liq['liq_total_24h'] > 0,
            coin_liq['liq_total_4h'] / coin_liq['liq_total_24h'],
            0
        )

        merge_cols = ['date', 'liq_cascade_ratio', 'liq_intensity_24h', 'liq_4h_spike']
        df = df.merge(coin_liq[merge_cols], on='date', how='left')
        for c in ['liq_cascade_ratio', 'liq_intensity_24h', 'liq_4h_spike']:
            df[c] = df[c].ffill()
        return df

    def _add_exchange_balance_change_features(
        self, df: pd.DataFrame, bal_data: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """Exchange balance change features.

        Features:
          exch_balance_change_pct_7d: 7-day % change in exchange balance (negative = accumulation)
          exch_balance_change_pct_30d: 30-day % change
        """
        coin_bal = bal_data[bal_data['coin'] == coin].copy() if not bal_data.empty else pd.DataFrame()
        if coin_bal.empty:
            df['exch_balance_change_pct_7d'] = np.nan
            df['exch_balance_change_pct_30d'] = np.nan
            return df

        coin_bal['date'] = pd.to_datetime(coin_bal['date'])
        coin_bal = coin_bal.sort_values('date').drop_duplicates('date', keep='last')

        # Compute % changes
        coin_bal['exch_balance_change_pct_7d'] = np.where(
            (coin_bal['exch_balance'] - coin_bal['exch_balance_change_7d']) > 0,
            coin_bal['exch_balance_change_7d'] / (coin_bal['exch_balance'] - coin_bal['exch_balance_change_7d']),
            0
        )
        coin_bal['exch_balance_change_pct_30d'] = np.where(
            (coin_bal['exch_balance'] - coin_bal['exch_balance_change_30d']) > 0,
            coin_bal['exch_balance_change_30d'] / (coin_bal['exch_balance'] - coin_bal['exch_balance_change_30d']),
            0
        )

        merge_cols = ['date', 'exch_balance_change_pct_7d', 'exch_balance_change_pct_30d']
        df = df.merge(coin_bal[merge_cols], on='date', how='left')
        for c in merge_cols[1:]:
            df[c] = df[c].ffill()
        return df

    def _add_options_features(
        self, df: pd.DataFrame, options_data: pd.DataFrame, coin: str, prices_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Options max pain distance + put/call ratio.

        Features:
          options_max_pain_dist: (price - max_pain) / price → positive = above max pain
          options_put_call_ratio: put OI / call OI → >1 = bearish positioning
        """
        coin_opt = options_data[options_data['coin'] == coin].copy() if not options_data.empty else pd.DataFrame()
        if coin_opt.empty or coin not in ('BTC', 'ETH'):
            df['options_max_pain_dist'] = np.nan
            df['options_put_call_ratio'] = np.nan
            return df

        coin_opt['date'] = pd.to_datetime(coin_opt['date'])
        coin_opt = coin_opt.sort_values('date').drop_duplicates('date', keep='last')

        # Merge with prices to compute distance
        coin_prices = prices_df[prices_df['coin'] == coin][['date', 'close']].copy()
        coin_prices['date'] = pd.to_datetime(coin_prices['date'])
        coin_prices = coin_prices.drop_duplicates('date', keep='last')
        coin_opt = coin_opt.merge(coin_prices, on='date', how='left')
        coin_opt['options_max_pain_dist'] = np.where(
            coin_opt['close'] > 0,
            (coin_opt['close'] - coin_opt['max_pain_price']) / coin_opt['close'],
            0
        )
        coin_opt['options_put_call_ratio'] = coin_opt['put_call_ratio']

        merge_cols = ['date', 'options_max_pain_dist', 'options_put_call_ratio']
        df = df.merge(coin_opt[merge_cols], on='date', how='left')
        for c in merge_cols[1:]:
            df[c] = df[c].ffill()
        return df

    def _add_4h_features(
        self, df: pd.DataFrame, h4_data: pd.DataFrame, coin: str
    ) -> pd.DataFrame:
        """4-hour timeframe indicators for entry timing.

        Features:
          rsi_4h: RSI on 4h candles (faster momentum)
          bb_position_4h: Bollinger position on 4h (faster mean reversion)
          ret_24h_4h: 24h return computed from 4h candles (intraday momentum)
        """
        coin_4h = h4_data[h4_data['coin'] == coin].copy() if not h4_data.empty else pd.DataFrame()
        if coin_4h.empty:
            df['rsi_4h'] = np.nan
            df['bb_position_4h'] = np.nan
            df['ret_24h_4h'] = np.nan
            return df

        coin_4h['date'] = pd.to_datetime(coin_4h['date'])
        coin_4h = coin_4h.sort_values('date').drop_duplicates('date', keep='last')
        merge_cols = ['date', 'rsi_4h', 'bb_position_4h', 'ret_24h_4h']
        available = [c for c in merge_cols if c in coin_4h.columns]
        df = df.merge(coin_4h[available], on='date', how='left')
        for c in ['rsi_4h', 'bb_position_4h', 'ret_24h_4h']:
            if c not in df.columns:
                df[c] = np.nan
        return df

    # ── v20.2: DERIVED SMART FEATURES ───────────────────────────

    def _add_volume_divergence(self, df: pd.DataFrame) -> pd.DataFrame:
        """Volume divergence: detect weak rallies/selloffs.

        Key finding: weak selloff (price down + low volume) → 70.7% UP (strongest bullish signal found).

        Features:
          vol_price_divergence: volume declining while price moving = weak move
          vol_ratio_to_30d: 7d avg volume / 30d avg volume
        """
        if 'volume_ratio_7d' in df.columns and 'ret_7d' in df.columns:
            # vol_ratio_7d already exists (7d vol / 20d vol)
            # Add: is this a weak move? (low volume + price change)
            df['vol_price_divergence'] = np.where(
                df['volume_ratio_7d'] < 0.7,  # volume declining
                np.where(df['ret_7d'] > 0, -1.0, 1.0),  # weak rally = -1, weak selloff = +1 (bullish)
                np.where(df['volume_ratio_7d'] > 1.3,
                    np.where(df['ret_7d'] > 0, 1.0, -1.0),  # strong rally = +1, capitulation = -1
                    0.0  # normal volume
                )
            )
        else:
            df['vol_price_divergence'] = np.nan
        return df

    def _add_range_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Price position within recent range (support/resistance proximity).

        Near support (<10%): 58.1% UP (+4.6pp edge).
        Near resistance (>90%): 57.8% UP (breakout momentum).
        """
        if 'close' not in df.columns:
            df['range_position_20d'] = np.nan
            return df

        high_20 = df['close'].rolling(20, min_periods=10).max()
        low_20 = df['close'].rolling(20, min_periods=10).min()
        rng = high_20 - low_20
        df['range_position_20d'] = np.where(rng > 0, (df['close'] - low_20) / rng, 0.5)
        return df

    def _add_funding_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Funding rate rate-of-change (momentum, not level).

        FR momentum negative → +2.38% avg 7d return (+1.33pp edge).
        """
        if 'funding_rate' in df.columns:
            df['funding_momentum'] = df['funding_rate'].rolling(7, min_periods=3).mean() - \
                                     df['funding_rate'].rolling(21, min_periods=7).mean()
        else:
            df['funding_momentum'] = np.nan
        return df

    def _add_stablecoin_acceleration(self, df: pd.DataFrame) -> pd.DataFrame:
        """Stablecoin supply acceleration (not just growth, but change in growth).

        Growth >1%/wk → +1.81% avg. Shrinking → -0.37%.
        """
        if 'stablecoin_supply_change_7d' in df.columns:
            df['stablecoin_acceleration'] = df['stablecoin_supply_change_7d'] - \
                                            df['stablecoin_supply_change_7d'].shift(7)
        else:
            df['stablecoin_acceleration'] = np.nan
        return df

    def _add_etf_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """ETF flow acceleration.

        Inflow >$500M/7d → +1.51%. Outflow → -0.47%.
        """
        if 'etf_flow_usd' in df.columns:
            df['etf_flow_acceleration'] = df['etf_flow_7d_avg'] - df['etf_flow_7d_avg'].shift(7) \
                if 'etf_flow_7d_avg' in df.columns else np.nan
        else:
            df['etf_flow_acceleration'] = np.nan
        return df

    def _add_oi_divergence(self, df: pd.DataFrame) -> pd.DataFrame:
        """OI vs price divergence.

        Short covering (price up + OI down) → different from new longs (price up + OI up).
        """
        if 'oi_change_7d' in df.columns and 'ret_7d' in df.columns:
            # OI direction vs price direction
            df['oi_price_divergence'] = np.where(
                (df['ret_7d'] > 0.02) & (df['oi_change_7d'] < -0.05), 1.0,   # short covering rally
                np.where(
                    (df['ret_7d'] < -0.02) & (df['oi_change_7d'] > 0.05), -1.0,  # bear buildup
                    0.0
                )
            )
        else:
            df['oi_price_divergence'] = np.nan
        return df

    def _add_breadth_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cross-coin market breadth features (computed across all coins per date).

        Features:
          pct_above_ma50: % of coins trading above their 50d MA
          pct_above_ma200: % of coins above 200d MA
          market_avg_ret_7d: average 7d return across all coins
          n_coins_new_high_30d: number of coins at 30d high
        """
        if 'ma50_dist' not in df.columns:
            df['pct_above_ma50'] = np.nan
            df['pct_above_ma200'] = np.nan
            df['market_avg_ret_7d'] = np.nan
            df['n_coins_new_high_30d'] = np.nan
            return df

        # Compute per-date aggregates
        daily = df.groupby('date').agg(
            pct_above_ma50=('ma50_dist', lambda x: (x > 0).mean()),
            pct_above_ma200=('ma200_dist', lambda x: (x > 0).mean()),
            market_avg_ret_7d=('ret_7d', 'mean'),
            n_coins_new_high_30d=('ret_30d', lambda x: (x > 0.1).sum()),
        ).reset_index()

        # Drop old columns if they exist, then merge
        for col in ['pct_above_ma50', 'pct_above_ma200', 'market_avg_ret_7d', 'n_coins_new_high_30d']:
            if col in df.columns:
                df = df.drop(columns=[col])

        df = df.merge(daily, on='date', how='left')
        return df

    def _add_group_feature(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add coin group as categorical feature."""
        df['coin_group'] = df['coin'].map(COIN_TO_GROUP).fillna('other')
        return df

    # ── Labels ───────────────────────────────────────────────────

    def _compute_labels(self, coin_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute forward-looking labels for training.
        These use FUTURE data — only for training, never for production.

        Labels (fixed threshold):
          label_3d, label_7d: % return over next 3/7 days
          label_dir_3d, label_dir_7d: +1/>threshold, -1/<-threshold, 0/neutral

        Labels (volatility-adjusted):
          label_up_7d: binary 1 if return > 1.0 × vol_30d, else 0
          label_down_7d: binary 1 if return < -1.0 × vol_30d, else 0
          label_up_3d: binary for 3d
          label_down_3d: binary for 3d
        """
        df = coin_df.copy()
        close = df['close']

        # Forward returns (shift negative = look ahead)
        df['label_3d'] = close.shift(-3) / close - 1
        df['label_7d'] = close.shift(-7) / close - 1

        # Fixed-threshold direction labels
        df['label_dir_3d'] = np.where(
            df['label_3d'].isna(), np.nan,
            np.where(df['label_3d'] > 0.015, 1,
                     np.where(df['label_3d'] < -0.015, -1, 0))
        )
        df['label_dir_7d'] = np.where(
            df['label_7d'].isna(), np.nan,
            np.where(df['label_7d'] > 0.02, 1,
                     np.where(df['label_7d'] < -0.02, -1, 0))
        )

        # Volatility-adjusted binary labels
        # Threshold = 1.0 × rolling 30d volatility × sqrt(horizon)
        # This makes "significant move" relative to each coin's natural volatility
        daily_ret = close.pct_change()
        vol_30d = daily_ret.rolling(30).std()

        for horizon, suffix in [(7, '7d'), (3, '3d')]:
            label_col = f'label_{suffix}'
            vol_threshold = vol_30d * np.sqrt(horizon) * 1.0

            df[f'label_up_{suffix}'] = np.where(
                df[label_col].isna(), np.nan,
                np.where(df[label_col] > vol_threshold, 1, 0)
            )
            df[f'label_down_{suffix}'] = np.where(
                df[label_col].isna(), np.nan,
                np.where(df[label_col] < -vol_threshold, 1, 0)
            )

        return df

    # ── Main Builder ─────────────────────────────────────────────

    # All feature columns (order matters for reproducibility)
    FEATURE_COLS = [
        # Price features (13)
        'ret_1d', 'ret_3d', 'ret_7d', 'ret_14d', 'ret_30d',
        'volatility_7d', 'volatility_30d',
        'rsi_14', 'bb_position',
        'ma50_dist', 'ma200_dist',
        'volume_ratio_7d', 'high_low_range_7d',
        # Funding features (3)
        'funding_rate', 'funding_rate_7d_avg', 'funding_rate_pctl_30d',
        # Fear & Greed features (3)
        'fg_value', 'fg_change_7d', 'fg_percentile_30d',
        # CQ exchange flows (3)
        'cq_netflow', 'cq_netflow_7d_avg', 'cq_reserve_change_7d',
        # Coinbase premium (1)
        'coinbase_premium',
        # ETF flows (2)
        'etf_flow_usd', 'etf_flow_7d_avg',
        # BTC cross-asset (3)
        'btc_ret_7d', 'btc_ret_30d', 'corr_btc_30d',
        # Cross-coin / Market breadth (4)
        'pct_above_ma50', 'pct_above_ma200',
        'market_avg_ret_7d', 'n_coins_new_high_30d',
        # Macro features (7 + 7 new = 14)
        'vix', 'vix_change_1d', 'yield_curve', 'treasury_10y', 'fed_rate',
        'cpi_yoy', 'fomc_meeting', 'fomc_days_until',
        'sp500', 'sp500_ret_1d', 'sp500_ret_7d',
        'nasdaq', 'nasdaq_ret_1d', 'nasdaq_ret_7d',
        'dxy',
        # Google Trends (3)
        'gtrend_bitcoin', 'gtrend_crypto', 'gtrend_bitcoin_change',
        # DeFi TVL (2)
        'defi_tvl_change_7d', 'defi_tvl_change_30d',
        # Halving cycle (2)
        'days_since_halving', 'halving_cycle_phase',
        # Derivatives (8) — mostly NaN for training
        'oi_change_1d', 'oi_change_7d',
        'ls_long_pct', 'ls_ratio',
        'taker_ratio',
        'liq_long_24h', 'liq_short_24h', 'liq_ratio',
        # Calendar (2)
        'day_of_week', 'is_weekend',
        # Event features (8)
        'event_severity_7d', 'event_count_30d', 'event_sentiment_7d',
        'regulatory_events_30d', 'hack_severity_30d',
        'days_since_major_event', 'event_impact_avg_7d', 'coin_event_7d',
        # Whale features (4)
        'whale_volume_7d', 'whale_tx_count_7d',
        'whale_volume_anomaly', 'whale_coin_volume_7d',
        # Exchange flow anomaly (3)
        'exchange_flow_zscore', 'exchange_deposit_anomaly', 'exchange_withdrawal_anomaly',
        # Stablecoin supply (2)
        'stablecoin_supply_change_7d', 'stablecoin_supply_growth',
        # Hack impact (2)
        'hack_total_30d', 'hack_count_30d',
        # Twitter/X sentiment (4)
        'twitter_sentiment', 'twitter_volume',
        'twitter_sentiment_7d', 'twitter_volume_anomaly',
        # Order book imbalance (3)
        'ob_bid_ask_ratio', 'ob_imbalance_score', 'ob_imbalance_7d_avg',
        # v20: Liquidation cascade (3) — from 3.7M CoinGlass records
        'liq_cascade_ratio', 'liq_intensity_24h', 'liq_4h_spike',
        # v20: Exchange balance changes (2) — accumulation/distribution
        'exch_balance_change_pct_7d', 'exch_balance_change_pct_30d',
        # v20: Options max pain (2) — BTC/ETH only
        'options_max_pain_dist', 'options_put_call_ratio',
        # v20: 4h timeframe indicators (3) — faster momentum
        'rsi_4h', 'bb_position_4h', 'ret_24h_4h',
        # v20.2: Derived smart features (6) — computed from existing data
        'vol_price_divergence',      # weak selloff = bullish (+17pp edge!)
        'range_position_20d',        # support/resistance proximity
        'funding_momentum',          # FR rate-of-change (+1.33pp edge)
        'stablecoin_acceleration',   # supply growth acceleration
        'etf_flow_acceleration',     # ETF flow momentum
        'oi_price_divergence',       # OI vs price direction conflict
        # v20.3: Category 1 features (8) — from existing data
        'coinbase_binance_divergence',  # institutional vs retail flow
        'institutional_flow_7d',        # Coinbase direction (negative=buying)
        'futures_basis_ann',            # annualized basis from funding
        'basis_momentum',               # basis rate of change
        'whale_to_exchange_ratio',      # fraction of whale vol going to exchanges
        'whale_net_exchange_flow',      # net flow to/from exchanges
        'news_avg_impact_7d',           # Claude-scored news impact
        'news_high_impact_count_7d',    # high-impact news count
        # v20.3: Category 2 features (new free APIs)
        'm2_growth_90d',                # Global M2 money supply 90d growth
        'm2_momentum',                  # M2 growth acceleration
        'hashrate_change_30d',          # BTC hashrate 30d change
        'difficulty_change',            # BTC difficulty adjustment
    ]

    LABEL_COLS = [
        'label_3d', 'label_7d', 'label_dir_3d', 'label_dir_7d',
        'label_up_7d', 'label_down_7d', 'label_up_3d', 'label_down_3d',
    ]
    META_COLS = ['coin', 'date', 'coin_group', 'close']

    def build_dataset(self, include_labels: bool = True) -> pd.DataFrame:
        """
        Build complete dataset with features and labels for all coins.

        Args:
            include_labels: If True, compute forward-looking labels (training only).

        Returns:
            DataFrame with columns: META_COLS + FEATURE_COLS + (LABEL_COLS if include_labels)
        """
        logger.info("Loading data from market.db...")

        # Load all data sources
        prices = self._load_prices()
        funding = self._load_funding()
        fg = self._load_fear_greed()
        cq_flows = self._load_cq_exchange_flows()
        cb_prem = self._load_coinbase_premium()
        etf = self._load_etf_flows()
        oi = self._load_open_interest()
        ls = self._load_long_short()
        taker = self._load_taker_volume()
        liq = self._load_cg_liquidations()
        macro = self._load_macro()
        fomc_dates = self._load_fomc_dates()
        trends = self._load_google_trends()
        tvl = self._load_defi_tvl()
        halvings = self._load_halvings()
        events = self._load_events()
        whales = self._load_whale_data()
        stablecoin = self._load_stablecoin_supply()
        hacks = self._load_hacks_data()
        twitter = self._load_twitter_sentiment()
        orderbook = self._load_orderbook()

        # v20: new data sources
        liq_features = self._load_liquidation_features()
        exch_balance = self._load_exchange_balance_features()
        options = self._load_options_features()
        h4_indicators = self._load_4h_indicators()

        # v20.3: category 1 data (from existing DB)
        exchange_specific = self._load_exchange_specific_flows()

        # BTC prices for cross-asset features
        btc_prices = prices[prices['coin'] == 'BTC'][['date', 'close']].copy()
        btc_prices = btc_prices.sort_values('date').drop_duplicates('date')

        coins = sorted(prices['coin'].unique())
        logger.info(f"Building features for {len(coins)} coins...")

        all_dfs = []
        for coin in coins:
            coin_prices = prices[prices['coin'] == coin].copy()
            if len(coin_prices) < 50:
                logger.warning(f"Skipping {coin}: only {len(coin_prices)} price records")
                continue

            # Price features
            df = self._compute_price_features(coin_prices)

            # Add all other features
            df = self._add_funding_features(df, funding, coin)
            df = self._add_fg_features(df, fg)
            df = self._add_cq_features(df, cq_flows, coin)
            df = self._add_coinbase_premium(df, cb_prem)
            df = self._add_etf_features(df, etf, coin)
            df = self._add_derivatives_features(df, coin, oi, ls, taker, liq)
            df = self._add_btc_cross_features(df, coin, btc_prices)
            df = self._add_macro_features(df, macro, fomc_dates)
            df = self._add_trends_features(df, trends)
            df = self._add_tvl_features(df, tvl)
            df = self._add_halving_features(df, halvings)
            df = self._add_calendar_features(df)
            df = self._add_event_features(df, events, coin)
            df = self._add_whale_features(df, whales, coin)
            df = self._add_exchange_flow_anomaly(df)
            df = self._add_stablecoin_features(df, stablecoin)
            df = self._add_hack_features(df, hacks)
            df = self._add_twitter_features(df, twitter, coin)
            df = self._add_orderbook_features(df, orderbook, coin)

            # v20: new data-driven features
            df = self._add_liquidation_cascade_features(df, liq_features, coin)
            df = self._add_exchange_balance_change_features(df, exch_balance, coin)
            df = self._add_options_features(df, options, coin, prices)
            df = self._add_4h_features(df, h4_indicators, coin)

            # v20.2: derived smart features (computed from existing columns)
            df = self._add_volume_divergence(df)
            df = self._add_range_position(df)
            df = self._add_funding_momentum(df)
            df = self._add_stablecoin_acceleration(df)
            df = self._add_etf_momentum(df)
            df = self._add_oi_divergence(df)

            # v20.3: category 1 features (from existing data)
            df = self._add_exchange_divergence_features(df, exchange_specific, coin)
            df = self._add_futures_basis_features(df)
            df = self._add_whale_entity_features(df, whales, coin)
            df = self._add_news_impact_features(df)

            # v20.3: category 2 features (new free APIs)
            df = self._add_m2_features(df)
            df = self._add_hashrate_features(df)

            # Labels
            if include_labels:
                df = self._compute_labels(df)

            df['coin'] = coin
            all_dfs.append(df)

        result = pd.concat(all_dfs, ignore_index=True)
        result = self._add_group_feature(result)

        # Cross-coin breadth features (computed across all coins per date)
        result = self._add_breadth_features(result)

        # Select and order columns
        cols = self.META_COLS + self.FEATURE_COLS
        if include_labels:
            cols += self.LABEL_COLS
        # Only keep columns that exist
        cols = [c for c in cols if c in result.columns]
        result = result[cols]

        # Drop rows where we can't compute basic features (first 30 days)
        result = result.dropna(subset=['ret_7d', 'volatility_7d', 'rsi_14'])

        if include_labels:
            n_total = len(result)
            n_with_labels = result['label_7d'].notna().sum()
            logger.info(f"Dataset: {n_total} rows, {n_with_labels} with 7d labels")
        else:
            logger.info(f"Dataset: {len(result)} rows (no labels)")

        return result

    _cache = None
    _cache_time = None

    def build_features_single(self, coin: str, date_str: str) -> dict:
        """
        Build features for a single (coin, date) — for production use.
        Caches the full dataset for 30 minutes to avoid rebuilding.
        Returns dict of feature_name → value.
        """
        import time
        now = time.time()

        # Cache for 15 min (v20: reduced from 30 for fresher data)
        if (FeatureBuilder._cache is None or
                FeatureBuilder._cache_time is None or
                now - FeatureBuilder._cache_time > 900):
            FeatureBuilder._cache = self.build_dataset(include_labels=False)
            FeatureBuilder._cache_time = now

        target_date = pd.to_datetime(date_str)
        row = FeatureBuilder._cache[
            (FeatureBuilder._cache['coin'] == coin) & (FeatureBuilder._cache['date'] == target_date)
        ]
        if row.empty:
            return {}
        return row[self.FEATURE_COLS].iloc[0].to_dict()

    def save_dataset(self, path: str = None, include_labels: bool = True) -> str:
        """Build dataset and save as CSV. Returns path."""
        if path is None:
            path = str(Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'training_dataset_v3.csv')

        df = self.build_dataset(include_labels=include_labels)
        df.to_csv(path, index=False)
        logger.info(f"Saved dataset to {path}: {len(df)} rows, {len(df.columns)} columns")
        return path

    def dataset_report(self, df: pd.DataFrame = None) -> str:
        """Generate a summary report of the dataset."""
        if df is None:
            df = self.build_dataset(include_labels=True)

        lines = ["=" * 60, "FORECAST V3 — DATASET REPORT", "=" * 60]

        # Basic stats
        lines.append(f"\nTotal rows: {len(df)}")
        lines.append(f"Coins: {df['coin'].nunique()} ({', '.join(sorted(df['coin'].unique()))})")
        lines.append(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

        # Per-group counts
        lines.append("\nPer group:")
        for group in ['majors', 'l1_alts', 'defi', 'ai', 'meme']:
            g = df[df['coin_group'] == group]
            lines.append(f"  {group:10s}: {len(g):5d} rows, {g['coin'].nunique()} coins")

        # Feature availability
        lines.append("\nFeature availability (% non-NaN):")
        for feat in self.FEATURE_COLS:
            if feat in df.columns:
                pct = df[feat].notna().mean() * 100
                lines.append(f"  {feat:30s}: {pct:5.1f}%")

        # Label distribution
        if 'label_dir_7d' in df.columns:
            labeled = df[df['label_dir_7d'].notna()]
            lines.append(f"\nLabel distribution (7d direction, {len(labeled)} labeled):")
            for val, name in [(1, 'UP'), (0, 'NEUTRAL'), (-1, 'DOWN')]:
                n = (labeled['label_dir_7d'] == val).sum()
                pct = n / len(labeled) * 100
                lines.append(f"  {name:8s}: {n:5d} ({pct:.1f}%)")

            # 3d labels
            labeled_3d = df[df['label_dir_3d'].notna()]
            lines.append(f"\nLabel distribution (3d direction, {len(labeled_3d)} labeled):")
            for val, name in [(1, 'UP'), (0, 'NEUTRAL'), (-1, 'DOWN')]:
                n = (labeled_3d['label_dir_3d'] == val).sum()
                pct = n / len(labeled_3d) * 100
                lines.append(f"  {name:8s}: {n:5d} ({pct:.1f}%)")

        return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    builder = FeatureBuilder()

    print("\nBuilding dataset...")
    df = builder.build_dataset(include_labels=True)

    print(builder.dataset_report(df))

    # Save
    path = builder.save_dataset()
    print(f"\nSaved to: {path}")
