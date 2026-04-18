"""
FORTIX Prediction Data Collector
=================================
Collects HIGH-FREQUENCY leading indicator data from CoinGlass PRO API.
Runs every 5 minutes (independent of hourly scan).

Data collected (all at 5min intervals):
1. Aggregated OI History — OI divergence from price
2. Futures CVD — cumulative volume delta (buyer/seller flow)
3. Spot CVD — real money flow (not leveraged)
4. Taker Buy/Sell Volume — actual order flow
5. Top Trader L/S Ratio — smart money positioning
6. Global L/S Account Ratio — retail sentiment (contrarian)
7. OI-Weighted Funding Rate — crowding signal
8. Liquidation Heatmap — WHERE liquidation clusters are
9. Aggregated Order Book Depth — bid/ask imbalance

Standalone module — does NOT affect live trading.
Run: python3 -m src.crypto.prediction_collector
"""

import os
import sys
import time
import json
import sqlite3
import logging
import requests
from pathlib import Path

log = logging.getLogger('prediction_collector')

COINGLASS_API_KEY = os.getenv('COINGLASS_API_KEY', '')
BASE_URL = 'https://open-api-v4.coinglass.com'
DB_PATH = Path('data/crypto/market.db')

# Coins to collect prediction data for
PRED_COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'ADA', 'AVAX', 'LINK',
              'DOGE', 'LDO', 'CRV', 'UNI', 'PENDLE', 'TON']

EXCHANGES = 'Binance,OKX,Bybit'


class PredictionCollector:

    def __init__(self):
        if not COINGLASS_API_KEY:
            # Try loading from .env
            env_path = Path(__file__).parent.parent.parent / '.env'
            if not env_path.exists():
                env_path = Path('/Users/williamstorm/Documents/Factory/.env')
            if env_path.exists():
                for line in open(env_path):
                    if '=' in line and not line.startswith('#'):
                        k, v = line.strip().split('=', 1)
                        os.environ[k.strip()] = v.strip()

        self.api_key = os.environ.get('COINGLASS_API_KEY', '')
        self.session = requests.Session()
        self.session.headers['CG-API-KEY'] = self.api_key
        self.session.headers['accept'] = 'application/json'
        self._last_req = 0
        self._req_count = 0

    def _rate_limit(self):
        """STARTUP plan: ~120 req/min. Use 0.6s gap for safety."""
        elapsed = time.time() - self._last_req
        if elapsed < 0.6:
            time.sleep(0.6 - elapsed)
        self._last_req = time.time()
        self._req_count += 1

    def _get(self, path, params=None):
        self._rate_limit()
        try:
            url = f"{BASE_URL}{path}"
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                log.warning("Rate limited — sleeping 5s")
                time.sleep(5)
                resp = self.session.get(url, params=params, timeout=15)
            data = resp.json()
            if data.get('code') == '0' or data.get('success'):
                return data.get('data', data)
            else:
                log.debug(f"API error {path}: {data.get('msg', data.get('message', ''))}")
                return None
        except Exception as e:
            log.debug(f"Request failed {path}: {e}")
            return None

    def _init_tables(self, conn):
        """Create prediction-specific tables."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS pred_oi_history (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open_interest REAL,
                h REAL, l REAL, c REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_cvd_futures (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                buy_volume REAL,
                sell_volume REAL,
                cvd REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_cvd_spot (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                buy_volume REAL,
                sell_volume REAL,
                cvd REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_taker_volume (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                buy_volume REAL,
                sell_volume REAL,
                ratio REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_top_trader_ls (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                long_ratio REAL,
                short_ratio REAL,
                long_short_ratio REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_global_ls (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                long_ratio REAL,
                short_ratio REAL,
                long_short_ratio REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_funding_oi_weight (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                funding_rate REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_orderbook_depth (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                bid_amount REAL,
                ask_amount REAL,
                imbalance REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_liq_history (
                coin TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                long_liq_usd REAL,
                short_liq_usd REAL,
                PRIMARY KEY (coin, timestamp)
            );

            CREATE TABLE IF NOT EXISTS pred_liq_heatmap (
                coin TEXT NOT NULL,
                collected_at INTEGER NOT NULL,
                price_level REAL,
                liq_amount REAL,
                side TEXT,
                PRIMARY KEY (coin, collected_at, price_level)
            );
        """)

    def _ts(self, row):
        """Extract timestamp from row, handle ms→s conversion."""
        ts = int(row.get('time', row.get('t', 0)))
        if ts > 1e12:
            ts = ts // 1000
        return ts

    def collect_oi_history(self, conn, coin='BTC', interval='30m', limit=200):
        """Aggregated OI History — OHLC format."""
        data = self._get('/api/futures/open-interest/aggregated-history', {
            'symbol': coin, 'interval': interval, 'limit': limit, 'unit': 'usd'
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            conn.execute(
                "INSERT OR REPLACE INTO pred_oi_history VALUES (?,?,?,?,?,?)",
                (coin, ts, float(row.get('open', 0)), float(row.get('high', 0)),
                 float(row.get('low', 0)), float(row.get('close', 0)))
            )
            count += 1
        conn.commit()
        return count

    def collect_futures_cvd(self, conn, coin='BTC', interval='30m', limit=200):
        """Aggregated CVD — futures order flow."""
        data = self._get('/api/futures/aggregated-cvd/history', {
            'exchange_list': EXCHANGES, 'symbol': coin,
            'interval': interval, 'limit': limit, 'unit': 'usd'
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            buy = float(row.get('agg_taker_buy_vol', 0) or 0)
            sell = float(row.get('agg_taker_sell_vol', 0) or 0)
            cvd = float(row.get('cum_vol_delta', buy - sell) or 0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_cvd_futures VALUES (?,?,?,?,?)",
                (coin, ts, buy, sell, cvd)
            )
            count += 1
        conn.commit()
        return count

    def collect_spot_cvd(self, conn, coin='BTC', interval='30m', limit=200):
        """Spot CVD — real money flow (not leveraged)."""
        data = self._get('/api/spot/cvd/history', {
            'exchange': 'Binance', 'symbol': f'{coin}USDT',
            'interval': interval, 'limit': limit, 'unit': 'usd'
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            buy = float(row.get('agg_taker_buy_vol', row.get('buyVolume', 0)) or 0)
            sell = float(row.get('agg_taker_sell_vol', row.get('sellVolume', 0)) or 0)
            cvd = float(row.get('cum_vol_delta', row.get('cvd', buy - sell)) or 0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_cvd_spot VALUES (?,?,?,?,?)",
                (coin, ts, buy, sell, cvd)
            )
            count += 1
        conn.commit()
        return count

    def collect_taker_volume(self, conn, coin='BTC', interval='30m', limit=200):
        """Aggregated taker buy/sell volume — actual order flow."""
        data = self._get('/api/futures/aggregated-taker-buy-sell-volume/history', {
            'exchange_list': EXCHANGES, 'symbol': coin,
            'interval': interval, 'limit': limit, 'unit': 'usd'
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            buy = float(row.get('aggregated_buy_volume_usd', 0) or 0)
            sell = float(row.get('aggregated_sell_volume_usd', 0) or 0)
            ratio = buy / sell if sell > 0 else 1.0
            conn.execute(
                "INSERT OR REPLACE INTO pred_taker_volume VALUES (?,?,?,?,?)",
                (coin, ts, buy, sell, round(ratio, 4))
            )
            count += 1
        conn.commit()
        return count

    def collect_top_trader_ls(self, conn, coin='BTC', interval='30m', limit=200):
        """Top trader long/short ratio — smart money."""
        data = self._get('/api/futures/top-long-short-position-ratio/history', {
            'exchange': 'Binance', 'symbol': f'{coin}USDT',
            'interval': interval, 'limit': limit
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            lr = float(row.get('top_position_long_percent', 0) or 0)
            sr = float(row.get('top_position_short_percent', 0) or 0)
            lsr = float(row.get('top_position_long_short_ratio', lr / sr if sr > 0 else 1.0) or 1.0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_top_trader_ls VALUES (?,?,?,?,?)",
                (coin, ts, lr, sr, lsr)
            )
            count += 1
        conn.commit()
        return count

    def collect_global_ls(self, conn, coin='BTC', interval='30m', limit=200):
        """Global L/S account ratio — retail sentiment."""
        data = self._get('/api/futures/global-long-short-account-ratio/history', {
            'exchange': 'Binance', 'symbol': f'{coin}USDT',
            'interval': interval, 'limit': limit
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            lr = float(row.get('global_account_long_percent', 0) or 0)
            sr = float(row.get('global_account_short_percent', 0) or 0)
            lsr = float(row.get('global_account_long_short_ratio', lr / sr if sr > 0 else 1.0) or 1.0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_global_ls VALUES (?,?,?,?,?)",
                (coin, ts, lr, sr, lsr)
            )
            count += 1
        conn.commit()
        return count

    def collect_funding_oi_weight(self, conn, coin='BTC', interval='1h', limit=200):
        """OI-weighted funding rate — better than single exchange."""
        data = self._get('/api/futures/funding-rate/oi-weight-history', {
            'symbol': coin, 'interval': interval, 'limit': limit
        })
        if not data:
            return 0
        count = 0
        for row in data:
            ts = int(row.get('t', row.get('time', 0)))
            if ts > 1e12:
                ts = ts // 1000
            rate = float(row.get('c', row.get('fundingRate', 0)) or 0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_funding_oi_weight VALUES (?,?,?)",
                (coin, ts, rate)
            )
            count += 1
        conn.commit()
        return count

    def collect_orderbook_depth(self, conn, coin='BTC', interval='30m', limit=200):
        """Aggregated order book bid/ask depth — support/resistance quality."""
        data = self._get('/api/futures/orderbook/aggregated-ask-bids-history', {
            'exchange_list': 'ALL', 'symbol': coin,
            'interval': interval, 'limit': limit, 'range': '1'
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            bids = float(row.get('aggregated_bids_usd', 0) or 0)
            asks = float(row.get('aggregated_asks_usd', 0) or 0)
            total = bids + asks
            imbalance = (bids - asks) / total if total > 0 else 0
            conn.execute(
                "INSERT OR REPLACE INTO pred_orderbook_depth VALUES (?,?,?,?,?)",
                (coin, ts, bids, asks, round(imbalance, 4))
            )
            count += 1
        conn.commit()
        return count

    def collect_liq_history(self, conn, coin='BTC', interval='30m', limit=200):
        """Aggregated liquidation history — cascade detection."""
        data = self._get('/api/futures/liquidation/aggregated-history', {
            'exchange_list': EXCHANGES, 'symbol': coin,
            'interval': interval, 'limit': limit
        })
        if not data or not isinstance(data, list):
            return 0
        count = 0
        for row in data:
            ts = self._ts(row)
            long_liq = float(row.get('aggregated_long_liquidation_usd', 0) or 0)
            short_liq = float(row.get('aggregated_short_liquidation_usd', 0) or 0)
            conn.execute(
                "INSERT OR REPLACE INTO pred_liq_history (coin, timestamp, long_liq_usd, short_liq_usd) "
                "VALUES (?,?,?,?)",
                (coin, ts, long_liq, short_liq)
            )
            count += 1
        conn.commit()
        return count

    def collect_liq_heatmap(self, conn, coin='BTC'):
        """Liquidation heatmap — WHERE liquidation clusters are."""
        data = self._get('/api/futures/liquidation/aggregated-heatmap/model1', {
            'symbol': coin, 'range': '3d'
        })
        if not data:
            return 0
        now = int(time.time())
        count = 0
        if isinstance(data, dict):
            prices = data.get('prices', data.get('y', []))
            liq_data = data.get('data', data.get('z', []))
            if prices and liq_data:
                for i, price in enumerate(prices):
                    if i < len(liq_data):
                        amount = float(liq_data[i]) if isinstance(liq_data[i], (int, float)) else 0
                        if amount > 0:
                            conn.execute(
                                "INSERT OR REPLACE INTO pred_liq_heatmap VALUES (?,?,?,?,?)",
                                (coin, now, float(price), amount, 'unknown')
                            )
                            count += 1
        elif isinstance(data, list):
            for row in data:
                price = float(row.get('price', 0))
                amount = float(row.get('amount', row.get('value', 0)) or 0)
                side = row.get('side', 'unknown')
                if amount > 0:
                    conn.execute(
                        "INSERT OR REPLACE INTO pred_liq_heatmap VALUES (?,?,?,?,?)",
                        (coin, now, price, amount, side)
                    )
                    count += 1
        conn.commit()
        return count

    def collect_all(self, coins=None, interval='30m', limit=200):
        """Collect all prediction data for given coins."""
        if not self.api_key:
            log.error("No CoinGlass API key!")
            return {}

        coins = coins or ['BTC', 'ETH']
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables(conn)

        results = {}
        self._req_count = 0
        t0 = time.time()

        for coin in coins:
            coin_results = {}
            try:
                coin_results['oi'] = self.collect_oi_history(conn, coin, interval, limit)
                coin_results['cvd_futures'] = self.collect_futures_cvd(conn, coin, interval, limit)
                coin_results['taker'] = self.collect_taker_volume(conn, coin, interval, limit)
                coin_results['top_ls'] = self.collect_top_trader_ls(conn, coin, interval, limit)
                coin_results['global_ls'] = self.collect_global_ls(conn, coin, interval, limit)
                coin_results['funding'] = self.collect_funding_oi_weight(conn, coin, '1h', limit)
                coin_results['orderbook'] = self.collect_orderbook_depth(conn, coin, interval, limit)
                coin_results['liq'] = self.collect_liq_history(conn, coin, interval, limit)

                # Spot CVD only for major coins
                if coin in ('BTC', 'ETH', 'SOL'):
                    coin_results['cvd_spot'] = self.collect_spot_cvd(conn, coin, interval, limit)

            except Exception as e:
                log.error(f"Error collecting {coin}: {e}")
                coin_results['error'] = str(e)

            results[coin] = coin_results
            total = sum(v for v in coin_results.values() if isinstance(v, int))
            log.info(f"  {coin}: {total} records ({', '.join(f'{k}={v}' for k, v in coin_results.items() if isinstance(v, int) and v > 0)})")

        elapsed = time.time() - t0
        log.info(f"Prediction data collected: {len(coins)} coins, "
                 f"{self._req_count} API calls in {elapsed:.1f}s")

        conn.close()
        return results


def backfill(coins=None, days=7):
    """Backfill prediction data for N days."""
    coins = coins or ['BTC', 'ETH', 'SOL']
    pc = PredictionCollector()

    # 5min intervals: 12 per hour × 24h × days
    limit = min(1000, 12 * 24 * days)

    log.info(f"Backfilling {days} days ({limit} candles) for {coins}")
    results = pc.collect_all(coins, interval='30m', limit=limit)

    for coin, data in results.items():
        total = sum(v for v in data.values() if isinstance(v, int))
        print(f"  {coin}: {total} total records")

    return results


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--backfill', type=int, default=0, help='Backfill N days')
    parser.add_argument('--coins', nargs='+', default=['BTC', 'ETH', 'SOL'])
    args = parser.parse_args()

    if args.backfill > 0:
        backfill(args.coins, args.backfill)
    else:
        # Single collection run
        pc = PredictionCollector()
        print("=" * 60)
        print("  PREDICTION DATA COLLECTION")
        print("=" * 60)
        results = pc.collect_all(args.coins, interval='30m', limit=200)
        print()
        for coin, data in results.items():
            total = sum(v for v in data.values() if isinstance(v, int))
            print(f"{coin}: {total} records")
            for k, v in data.items():
                if isinstance(v, int) and v > 0:
                    print(f"  {k}: {v}")
