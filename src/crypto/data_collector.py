"""
FORTIX — Data Collector
==============================
Collects crypto market data from multiple APIs and stores in SQLite.

Available sources (no key required):
  - Binance: prices, candles, funding rates, open interest
  - DeFi Llama: TVL by protocol/chain
  - Alternative.me: Fear & Greed Index
  - RSS: CoinDesk, CoinTelegraph headlines

Available sources (key required):
  - CoinGecko (paid): market data, trending, global metrics
  - Etherscan V2 (paid): whale transactions, token transfers
  - CryptoPanic: news with sentiment
  - LunarCrush: social/X sentiment
  - Santiment: on-chain + social
  - Whale Alert: whale transactions
  - Reddit/PRAW: social sentiment

Usage:
    python src/crypto/data_collector.py              # collect all available
    python src/crypto/data_collector.py --source=binance  # specific source
    python src/crypto/data_collector.py --status     # show data status
"""

import os
import sys
import json
import time
import sqlite3
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

import numpy as np
import ccxt
import requests
import feedparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('collector')


# ════════════════════════════════════════════
# RETRY WITH EXPONENTIAL BACKOFF
# ════════════════════════════════════════════

def _retry_request(method, url, max_retries=3, backoff=(5, 15, 45),
                   retryable_codes=(429, 500, 502, 503, 504, 408),
                   session=None, **kwargs):
    """HTTP request with exponential backoff for transient errors.

    Retries on: timeout, connection error, and retryable HTTP status codes.
    Returns: requests.Response object.
    Raises: last exception if all retries exhausted.
    """
    kwargs.setdefault('timeout', 15)
    requester = session or requests

    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requester.request(method, url, **kwargs)
            if resp.status_code in retryable_codes and attempt < max_retries - 1:
                wait = backoff[min(attempt, len(backoff) - 1)]
                log.warning(f"  HTTP {resp.status_code} from {url[:60]}... retry in {wait}s "
                            f"(attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
            return resp
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries - 1:
                wait = backoff[min(attempt, len(backoff) - 1)]
                log.warning(f"  {type(e).__name__} for {url[:60]}... retry in {wait}s "
                            f"(attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            raise  # Non-retryable errors propagate immediately
    if last_exc:
        raise last_exc


def _retry_get(url, max_retries=3, session=None, **kwargs):
    """Shorthand for _retry_request('GET', ...)."""
    return _retry_request('GET', url, max_retries=max_retries,
                          session=session, **kwargs)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
COINGECKO_KEY = os.getenv('COINGECKO_API_KEY', '')
ETHERSCAN_KEY = os.getenv('ETHERSCAN_API_KEY', '')
CRYPTOPANIC_KEY = os.getenv('CRYPTOPANIC_API_KEY', '')
WHALE_ALERT_KEY = os.getenv('WHALE_ALERT_API_KEY', '')
LUNARCRUSH_KEY = os.getenv('LUNARCRUSH_API_KEY', '')
SANTIMENT_KEY = os.getenv('SANTIMENT_API_KEY', '')
TWITTER_API_KEY = os.getenv('TWITTER_API_KEY', '')
TWITTER_API_URL = os.getenv('TWITTER_API_URL', 'https://api.twitterapi.io')
COINGLASS_KEY = os.getenv('COINGLASS_API_KEY', '')
CRYPTOQUANT_KEY = os.getenv('CRYPTOQUANT_API_KEY', '')

# Coins we track
TRACKED_COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'POL',
    'UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV',
    'ARB', 'OP',
    'FET', 'RENDER', 'TAO',
    'DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK',
]

# Map coin symbols to Binance spot pairs (used for price candles)
# MKR delisted from Binance Spot (~Sep 2025) — uses futures fallback
SPOT_SYMBOL_MAP = {
    # RNDR→RENDER and MATIC→POL rebrands: internal tickers updated, no mapping needed
}
# Coins delisted from Binance Spot — will be collected via Futures instead
SPOT_DELISTED = set()  # MKR removed, replaced with PENDLE
BINANCE_PAIRS = {coin: SPOT_SYMBOL_MAP.get(coin, f'{coin}/USDT')
                 for coin in TRACKED_COINS if coin not in SPOT_DELISTED}

# Map coin symbols to Binance futures symbols (rebrands + 1000x lots)
FUTURES_SYMBOL_MAP = {
    'SHIB': '1000SHIB/USDT',
    'PEPE': '1000PEPE/USDT',
    'BONK': '1000BONK/USDT',
}
# Price divisor: 1000x futures pairs need price divided by 1000
FUTURES_PRICE_DIVISOR = {'SHIB': 1000, 'PEPE': 1000, 'BONK': 1000}

# Map coin symbols to CoinGecko IDs
COINGECKO_IDS = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'BNB': 'binancecoin',
    'XRP': 'ripple', 'ADA': 'cardano', 'AVAX': 'avalanche-2', 'DOT': 'polkadot',
    'LINK': 'chainlink', 'POL': 'polygon-ecosystem-token', 'UNI': 'uniswap',
    'AAVE': 'aave', 'PENDLE': 'pendle', 'LDO': 'lido-dao', 'CRV': 'curve-dao-token',
    'ARB': 'arbitrum', 'OP': 'optimism',
    'FET': 'fetch-ai', 'RENDER': 'render-token', 'TAO': 'bittensor',
    'DOGE': 'dogecoin', 'SHIB': 'shiba-inu', 'PEPE': 'pepe',
    'WIF': 'dogwifcoin', 'BONK': 'bonk',
}

RSS_FEEDS = {
    'coindesk': 'https://www.coindesk.com/arc/outboundfeeds/rss/',
    'cointelegraph': 'https://cointelegraph.com/rss',
}


# ════════════════════════════════════════════
# DATABASE SETUP
# ════════════════════════════════════════════

def init_db() -> sqlite3.Connection:
    """Initialize SQLite database with all tables."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS prices (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '1h',
            open REAL, high REAL, low REAL, close REAL,
            volume REAL,
            PRIMARY KEY (coin, timestamp, timeframe)
        );

        CREATE TABLE IF NOT EXISTS market_overview (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            price_usd REAL,
            market_cap REAL,
            volume_24h REAL,
            change_1h REAL,
            change_24h REAL,
            change_7d REAL,
            change_30d REAL,
            ath REAL,
            ath_change_pct REAL,
            circulating_supply REAL,
            total_supply REAL,
            rank INTEGER,
            PRIMARY KEY (coin, timestamp)
        );

        CREATE TABLE IF NOT EXISTS fear_greed (
            date TEXT NOT NULL PRIMARY KEY,
            value INTEGER NOT NULL,
            classification TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS funding_rates (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            rate REAL NOT NULL,
            PRIMARY KEY (coin, timestamp)
        );

        CREATE TABLE IF NOT EXISTS open_interest (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            oi_usdt REAL NOT NULL,
            PRIMARY KEY (coin, timestamp)
        );

        CREATE TABLE IF NOT EXISTS liquidations (
            id TEXT PRIMARY KEY,
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            notional_usd REAL
        );

        CREATE TABLE IF NOT EXISTS long_short_ratio (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            period TEXT NOT NULL,
            ratio_type TEXT NOT NULL,
            long_ratio REAL,
            short_ratio REAL,
            long_short_ratio REAL,
            PRIMARY KEY (coin, timestamp, period, ratio_type)
        );

        CREATE TABLE IF NOT EXISTS taker_volume (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            period TEXT NOT NULL,
            buy_sell_ratio REAL,
            buy_volume REAL,
            sell_volume REAL,
            PRIMARY KEY (coin, timestamp, period)
        );

        CREATE TABLE IF NOT EXISTS news (
            id TEXT NOT NULL PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            title TEXT NOT NULL,
            source TEXT,
            url TEXT,
            sentiment TEXT,
            coins_mentioned TEXT,
            shock_score REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS tvl (
            protocol TEXT NOT NULL,
            chain TEXT,
            date TEXT NOT NULL,
            tvl_usd REAL NOT NULL,
            change_1d REAL,
            PRIMARY KEY (protocol, date)
        );

        CREATE TABLE IF NOT EXISTS whale_transactions (
            tx_hash TEXT NOT NULL PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            blockchain TEXT,
            from_addr TEXT,
            from_label TEXT,
            to_addr TEXT,
            to_label TEXT,
            amount REAL,
            amount_usd REAL,
            coin TEXT
        );

        CREATE TABLE IF NOT EXISTS social_sentiment (
            coin TEXT NOT NULL,
            date TEXT NOT NULL,
            source TEXT NOT NULL,
            score REAL,
            volume INTEGER,
            positive REAL,
            negative REAL,
            PRIMARY KEY (coin, date, source)
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin TEXT NOT NULL,
            created_at TEXT NOT NULL,
            prediction_date TEXT NOT NULL,
            target_date TEXT NOT NULL,
            signal_score REAL NOT NULL,
            prediction TEXT NOT NULL,
            predicted_change_pct REAL,
            actual_price_at_prediction REAL,
            actual_price_at_target REAL,
            actual_change_pct REAL,
            correct INTEGER,
            video_type TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS global_metrics (
            date TEXT NOT NULL PRIMARY KEY,
            total_market_cap REAL,
            total_volume_24h REAL,
            btc_dominance REAL,
            eth_dominance REAL,
            defi_market_cap REAL,
            stablecoin_volume REAL,
            active_cryptocurrencies INTEGER
        );

        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT NOT NULL,
            backtest_type TEXT NOT NULL,
            window_days INTEGER,
            horizon_days INTEGER,
            total_predictions INTEGER,
            correct_predictions INTEGER,
            win_rate REAL,
            per_coin_json TEXT,
            per_signal_json TEXT,
            metadata_json TEXT,
            UNIQUE(run_date, backtest_type)
        );

        -- CoinGlass: Exchange balance (inflow/outflow)
        CREATE TABLE IF NOT EXISTS cg_exchange_balance (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            total_balance REAL,
            change_1d REAL,
            change_pct_1d REAL,
            change_7d REAL,
            change_pct_7d REAL,
            change_30d REAL,
            change_pct_30d REAL,
            PRIMARY KEY (coin, timestamp, exchange)
        );

        -- CoinGlass: Options max pain (BTC/ETH)
        CREATE TABLE IF NOT EXISTS cg_options_max_pain (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            expiry_date TEXT NOT NULL,
            max_pain_price REAL,
            call_oi REAL,
            call_oi_value REAL,
            call_oi_notional REAL,
            put_oi REAL,
            put_oi_value REAL,
            put_oi_notional REAL,
            PRIMARY KEY (coin, timestamp, expiry_date)
        );

        -- CoinGlass: BTC ETF flows
        CREATE TABLE IF NOT EXISTS cg_etf_flows (
            date TEXT NOT NULL,
            asset TEXT NOT NULL DEFAULT 'BTC',
            flow_usd REAL,
            price_usd REAL,
            PRIMARY KEY (date, asset)
        );

        -- CoinGlass: Stablecoin market cap
        CREATE TABLE IF NOT EXISTS cg_stablecoin_supply (
            date TEXT NOT NULL PRIMARY KEY,
            total_market_cap REAL
        );

        -- CoinGlass: Aggregated OI by exchange
        CREATE TABLE IF NOT EXISTS cg_aggregated_oi (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            oi_usd REAL,
            oi_quantity REAL,
            change_pct_1h REAL,
            change_pct_4h REAL,
            change_pct_24h REAL,
            PRIMARY KEY (coin, timestamp, exchange)
        );

        -- CoinGlass: Aggregated liquidations
        CREATE TABLE IF NOT EXISTS cg_liquidations (
            coin TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            liq_usd_24h REAL,
            long_liq_usd_24h REAL,
            short_liq_usd_24h REAL,
            liq_usd_12h REAL,
            long_liq_usd_12h REAL,
            short_liq_usd_12h REAL,
            liq_usd_4h REAL,
            long_liq_usd_4h REAL,
            short_liq_usd_4h REAL,
            liq_usd_1h REAL,
            long_liq_usd_1h REAL,
            short_liq_usd_1h REAL,
            PRIMARY KEY (coin, timestamp)
        );

        -- CryptoQuant: BTC on-chain indicators (SOPR, NUPL, MVRV, etc.)
        CREATE TABLE IF NOT EXISTS cq_btc_onchain (
            date TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL,
            PRIMARY KEY (date, metric)
        );

        -- CryptoQuant: Exchange flows (BTC, ETH, ERC20)
        CREATE TABLE IF NOT EXISTS cq_exchange_flows (
            date TEXT NOT NULL,
            coin TEXT NOT NULL,
            netflow REAL,
            reserve REAL,
            reserve_usd REAL,
            PRIMARY KEY (date, coin)
        );

        -- CryptoQuant: Coinbase Premium
        CREATE TABLE IF NOT EXISTS cq_coinbase_premium (
            date TEXT NOT NULL PRIMARY KEY,
            premium_index REAL,
            premium_gap REAL
        );

        -- CryptoQuant: BTC miner reserve
        CREATE TABLE IF NOT EXISTS cq_miner_data (
            date TEXT NOT NULL PRIMARY KEY,
            reserve REAL,
            reserve_usd REAL
        );

        -- CryptoQuant: Active addresses (BTC, ETH)
        CREATE TABLE IF NOT EXISTS cq_active_addresses (
            date TEXT NOT NULL,
            coin TEXT NOT NULL,
            active INTEGER,
            sender INTEGER,
            receiver INTEGER,
            PRIMARY KEY (date, coin)
        );

        -- CryptoQuant: Stablecoin exchange flows
        CREATE TABLE IF NOT EXISTS cq_stablecoin_flows (
            date TEXT NOT NULL PRIMARY KEY,
            netflow REAL
        );

        -- Frankfurter (ECB): DXY components for US Dollar Index calculation
        CREATE TABLE IF NOT EXISTS dxy_rates (
            date TEXT NOT NULL PRIMARY KEY,
            eur REAL,
            jpy REAL,
            gbp REAL,
            cad REAL,
            sek REAL,
            chf REAL,
            dxy_value REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_prices_coin_ts ON prices(coin, timestamp);
        CREATE INDEX IF NOT EXISTS idx_news_ts ON news(timestamp);
        CREATE INDEX IF NOT EXISTS idx_whale_ts ON whale_transactions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_predictions_coin ON predictions(coin, target_date);
    """)
    conn.commit()
    return conn


# ════════════════════════════════════════════
# BINANCE — Prices, Candles, Funding Rates, OI
# ════════════════════════════════════════════

class BinanceCollector:
    """Collect data from Binance (free, no API key needed for public endpoints)."""

    def __init__(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
        })
        self.spot = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'},
        })

    def collect_candles(self, conn: sqlite3.Connection, timeframe: str = '1h',
                        limit: int = 200):
        """Fetch OHLCV candles for all tracked coins (uses spot exchange)."""
        log.info(f"[Binance] Collecting {timeframe} candles (limit={limit})...")
        count = 0
        for coin, pair in BINANCE_PAIRS.items():
            try:
                ohlcv = self.spot.fetch_ohlcv(pair, timeframe, limit=limit)
                if not ohlcv:
                    continue
                rows = [
                    (coin, int(candle[0] / 1000), timeframe,
                     candle[1], candle[2], candle[3], candle[4], candle[5])
                    for candle in ohlcv
                ]
                conn.executemany(
                    "INSERT OR REPLACE INTO prices (coin, timestamp, timeframe, open, high, low, close, volume) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
                )
                count += len(rows)
            except Exception as e:
                if 'does not have' in str(e) or 'not found' in str(e).lower():
                    log.debug(f"  {coin}: pair not available on Binance spot")
                else:
                    log.warning(f"  {coin}: {e}")
            time.sleep(0.1)

        # Fallback: collect delisted coins from Binance Futures
        for coin in SPOT_DELISTED:
            try:
                fsymbol = self._futures_symbol(coin)
                ohlcv = self.exchange.fetch_ohlcv(fsymbol, timeframe, limit=limit)
                if not ohlcv:
                    continue
                divisor = FUTURES_PRICE_DIVISOR.get(coin, 1)
                rows = [
                    (coin, int(c[0] / 1000), timeframe,
                     c[1] / divisor, c[2] / divisor, c[3] / divisor,
                     c[4] / divisor, c[5])
                    for c in ohlcv
                ]
                conn.executemany(
                    "INSERT OR REPLACE INTO prices "
                    "(coin, timestamp, timeframe, open, high, low, close, volume) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
                )
                count += len(rows)
                log.info(f"  {coin}: {len(rows)} candles from Futures (spot delisted)")
            except Exception as e:
                log.warning(f"  {coin} futures fallback failed: {e}")
            time.sleep(0.1)

        conn.commit()
        log.info(f"  Stored {count} candles for {len(BINANCE_PAIRS) + len(SPOT_DELISTED)} coins")
        return count

    def _futures_symbol(self, coin: str) -> str:
        """Get correct futures symbol for a coin (handles rebrands + 1000x)."""
        mapped = FUTURES_SYMBOL_MAP.get(coin)
        if mapped:
            return mapped.replace('/USDT', '/USDT:USDT')
        return f'{coin}/USDT:USDT'

    def _futures_raw_symbol(self, coin: str) -> str:
        """Get raw futures symbol (no slash/colon) for REST API calls."""
        mapped = FUTURES_SYMBOL_MAP.get(coin)
        if mapped:
            return mapped.replace('/USDT', 'USDT')
        return f'{coin}USDT'

    def collect_funding_rates(self, conn: sqlite3.Connection):
        """Fetch funding rates for perpetual futures."""
        log.info("[Binance] Collecting funding rates...")
        count = 0
        for coin in TRACKED_COINS:
            symbol = self._futures_symbol(coin)
            try:
                rates = self.exchange.fetch_funding_rate_history(symbol, limit=100)
                if not rates:
                    continue
                rows = [
                    (coin, int(r['timestamp'] / 1000), r['fundingRate'])
                    for r in rates if r.get('fundingRate') is not None
                ]
                conn.executemany(
                    "INSERT OR REPLACE INTO funding_rates (coin, timestamp, rate) "
                    "VALUES (?, ?, ?)", rows
                )
                count += len(rows)
            except Exception as e:
                log.debug(f"  {coin} funding: {e}")
            time.sleep(0.1)

        conn.commit()
        log.info(f"  Stored {count} funding rate entries")
        return count

    def collect_open_interest(self, conn: sqlite3.Connection):
        """Fetch open interest from Binance Futures for tracked coins."""
        log.info("[Binance] Collecting open interest...")
        count = 0
        now = int(time.time())

        for coin in TRACKED_COINS:
            symbol = self._futures_raw_symbol(coin)
            divisor = FUTURES_PRICE_DIVISOR.get(coin, 1)
            try:
                resp = _retry_get(
                    'https://fapi.binance.com/fapi/v1/openInterest',
                    params={'symbol': symbol},
                    timeout=10
                )
                if resp.status_code == 200:
                    data = resp.json()
                    oi_usdt = float(data.get('openInterest', 0))
                    # Get mark price to convert from contracts to USDT
                    price_resp = _retry_get(
                        'https://fapi.binance.com/fapi/v1/ticker/price',
                        params={'symbol': symbol},
                        timeout=10
                    )
                    if price_resp.status_code == 200:
                        price = float(price_resp.json().get('price', 0))
                        oi_usdt_value = oi_usdt * price / divisor
                    else:
                        oi_usdt_value = oi_usdt

                    conn.execute(
                        "INSERT OR REPLACE INTO open_interest (coin, timestamp, oi_usdt) "
                        "VALUES (?, ?, ?)",
                        (coin, now, oi_usdt_value)
                    )
                    count += 1
            except Exception as e:
                log.debug(f"  {coin} OI: {e}")
            time.sleep(0.15)

        conn.commit()
        log.info(f"  Stored OI for {count} coins")
        return count

    def collect_liquidations(self, conn: sqlite3.Connection):
        """Report liquidation data status.

        Liquidation data is collected via WebSocket listener
        (src/crypto/liquidation_listener.py) running as a background service.
        This method just reports what's in the DB.
        """
        cutoff = int(time.time()) - 86400
        row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(notional_usd), 0) "
            "FROM liquidations WHERE timestamp > ?", (cutoff,)
        ).fetchone()
        count, total_usd = row[0], row[1]

        if count > 0:
            log.info(f"[Binance] Liquidations (24h): {count} events, ${total_usd:,.0f} total")
        else:
            log.info("[Binance] Liquidations: no data — start liquidation_listener.py "
                     "or run scheduler --daemon")
        return count

    def collect_long_short_ratios(self, conn: sqlite3.Connection):
        """Fetch long/short ratios from 3 Binance endpoints.

        - globalLongShortAccountRatio: all traders
        - topLongShortAccountRatio: top traders by accounts
        - topLongShortPositionRatio: top traders by positions
        """
        log.info("[Binance] Collecting long/short ratios...")
        count = 0
        endpoints = {
            'global': '/futures/data/globalLongShortAccountRatio',
            'top_trader_accounts': '/futures/data/topLongShortAccountRatio',
            'top_trader_positions': '/futures/data/topLongShortPositionRatio',
        }
        for coin in TRACKED_COINS:
            symbol = self._futures_raw_symbol(coin)
            for ratio_type, path in endpoints.items():
                try:
                    resp = _retry_get(
                        f'https://fapi.binance.com{path}',
                        params={'symbol': symbol, 'period': '1h', 'limit': 24},
                        timeout=10,
                    )
                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    for entry in data:
                        ts = int(entry.get('timestamp', 0))
                        ts = ts // 1000 if ts > 1e12 else ts
                        conn.execute(
                            "INSERT OR REPLACE INTO long_short_ratio "
                            "(coin, timestamp, period, ratio_type, long_ratio, short_ratio, long_short_ratio) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                coin, ts, '1h', ratio_type,
                                float(entry.get('longAccount', entry.get('longPosition', 0))),
                                float(entry.get('shortAccount', entry.get('shortPosition', 0))),
                                float(entry.get('longShortRatio', 0)),
                            )
                        )
                        count += 1
                except Exception as e:
                    log.debug(f"  {coin} L/S {ratio_type}: {e}")
                time.sleep(0.15)

        conn.commit()
        log.info(f"  Stored {count} long/short ratio entries")
        return count

    def collect_taker_volume(self, conn: sqlite3.Connection):
        """Fetch taker buy/sell volume ratio from Binance Futures.

        Ratio > 1 = aggressive buyers dominating (bullish pressure)
        Ratio < 1 = aggressive sellers dominating (bearish pressure)
        """
        log.info("[Binance] Collecting taker buy/sell volume...")
        count = 0

        for coin in TRACKED_COINS:
            symbol = self._futures_raw_symbol(coin)
            try:
                resp = _retry_get(
                    'https://fapi.binance.com/futures/data/takerlongshortRatio',
                    params={'symbol': symbol, 'period': '1h', 'limit': 24},
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue

                data = resp.json()
                for entry in data:
                    ts = int(entry.get('timestamp', 0))
                    ts = ts // 1000 if ts > 1e12 else ts
                    conn.execute(
                        "INSERT OR REPLACE INTO taker_volume "
                        "(coin, timestamp, period, buy_sell_ratio, buy_volume, sell_volume) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            coin, ts, '1h',
                            float(entry.get('buySellRatio', 0)),
                            float(entry.get('buyVol', 0)),
                            float(entry.get('sellVol', 0)),
                        )
                    )
                    count += 1
            except Exception as e:
                log.debug(f"  {coin} taker volume: {e}")
            time.sleep(0.15)

        conn.commit()
        log.info(f"  Stored {count} taker volume entries")
        return count

    def collect_tickers(self, conn: sqlite3.Connection):
        """Fetch current ticker data (24h stats)."""
        log.info("[Binance] Collecting current tickers...")

        # Switch to spot for ticker data
        spot = ccxt.binance({'enableRateLimit': True})
        tickers = spot.fetch_tickers([f'{c}/USDT' for c in TRACKED_COINS])

        now = int(time.time())
        count = 0
        for coin in TRACKED_COINS:
            pair = f'{coin}/USDT'
            if pair not in tickers:
                continue
            t = tickers[pair]
            conn.execute(
                "INSERT OR REPLACE INTO market_overview "
                "(coin, timestamp, price_usd, volume_24h, change_24h) "
                "VALUES (?, ?, ?, ?, ?)",
                (coin, now, t.get('last'), t.get('quoteVolume'), t.get('percentage'))
            )
            count += 1

        conn.commit()
        log.info(f"  Stored tickers for {count} coins")
        return count


# ════════════════════════════════════════════
# COINGECKO — Market Data, Trending, Global
# ════════════════════════════════════════════

class CoinGeckoCollector:
    """Collect data from CoinGecko (paid key available)."""

    BASE_URL = 'https://api.coingecko.com/api/v3'
    PRO_URL = 'https://pro-api.coingecko.com/api/v3'

    def __init__(self):
        self.api_key = COINGECKO_KEY
        self.base = self.PRO_URL if self.api_key else self.BASE_URL
        self.session = requests.Session()
        if self.api_key:
            self.session.headers['x-cg-pro-api-key'] = self.api_key

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f'{self.base}{endpoint}'
        resp = _retry_get(url, params=params or {}, timeout=30, session=self.session)
        resp.raise_for_status()
        return resp.json()

    def collect_market_data(self, conn: sqlite3.Connection):
        """Fetch current market data for all tracked coins."""
        log.info("[CoinGecko] Collecting market data...")
        ids = ','.join(COINGECKO_IDS[c] for c in TRACKED_COINS if c in COINGECKO_IDS)

        data = self._get('/coins/markets', {
            'vs_currency': 'usd',
            'ids': ids,
            'order': 'market_cap_desc',
            'per_page': 50,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h,7d,30d',
        })

        # Reverse map: coingecko_id → symbol
        id_to_sym = {v: k for k, v in COINGECKO_IDS.items()}
        now = int(time.time())
        count = 0

        for coin_data in data:
            sym = id_to_sym.get(coin_data['id'])
            if not sym:
                continue

            conn.execute(
                "INSERT OR REPLACE INTO market_overview "
                "(coin, timestamp, price_usd, market_cap, volume_24h, "
                "change_1h, change_24h, change_7d, change_30d, "
                "ath, ath_change_pct, circulating_supply, total_supply, rank) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    sym, now,
                    coin_data.get('current_price'),
                    coin_data.get('market_cap'),
                    coin_data.get('total_volume'),
                    coin_data.get('price_change_percentage_1h_in_currency'),
                    coin_data.get('price_change_percentage_24h'),
                    coin_data.get('price_change_percentage_7d_in_currency'),
                    coin_data.get('price_change_percentage_30d_in_currency'),
                    coin_data.get('ath'),
                    coin_data.get('ath_change_percentage'),
                    coin_data.get('circulating_supply'),
                    coin_data.get('total_supply'),
                    coin_data.get('market_cap_rank'),
                )
            )
            count += 1

        conn.commit()
        log.info(f"  Stored market data for {count} coins")
        return count

    def collect_global_metrics(self, conn: sqlite3.Connection):
        """Fetch global crypto market metrics."""
        log.info("[CoinGecko] Collecting global metrics...")
        data = self._get('/global')['data']

        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        conn.execute(
            "INSERT OR REPLACE INTO global_metrics "
            "(date, total_market_cap, total_volume_24h, btc_dominance, eth_dominance, "
            "defi_market_cap, active_cryptocurrencies) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                today,
                data.get('total_market_cap', {}).get('usd'),
                data.get('total_volume', {}).get('usd'),
                data.get('market_cap_percentage', {}).get('btc'),
                data.get('market_cap_percentage', {}).get('eth'),
                data.get('total_market_cap', {}).get('usd', 0) * data.get('defi_volume_percentage', 0) / 100 if data.get('defi_volume_percentage') else None,
                data.get('active_cryptocurrencies'),
            )
        )
        conn.commit()
        log.info(f"  Global metrics stored for {today}")
        return 1

    def collect_trending(self, conn: sqlite3.Connection):
        """Fetch trending coins on CoinGecko."""
        log.info("[CoinGecko] Collecting trending coins...")
        data = self._get('/search/trending')
        trending = data.get('coins', [])
        log.info(f"  Trending: {', '.join(c['item']['symbol'] for c in trending[:7])}")
        return trending


# ════════════════════════════════════════════
# FEAR & GREED INDEX
# ════════════════════════════════════════════

class FearGreedCollector:
    """Collect Fear & Greed Index (100% free, no key needed)."""

    URL = 'https://api.alternative.me/fng/'

    def collect(self, conn: sqlite3.Connection, days: int = 30):
        """Fetch Fear & Greed history."""
        log.info(f"[Fear&Greed] Collecting last {days} days...")
        resp = _retry_get(self.URL, params={'limit': days}, timeout=15)
        resp.raise_for_status()
        data = resp.json().get('data', [])

        for entry in data:
            ts = int(entry['timestamp'])
            date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            conn.execute(
                "INSERT OR REPLACE INTO fear_greed (date, value, classification) "
                "VALUES (?, ?, ?)",
                (date, int(entry['value']), entry['value_classification'])
            )

        conn.commit()
        if data:
            latest = data[0]
            log.info(f"  Today: {latest['value']} ({latest['value_classification']})")
        log.info(f"  Stored {len(data)} days of F&G data")
        return len(data)


# ════════════════════════════════════════════
# DXY (US Dollar Index) — FloatRates + Frankfurter
# ════════════════════════════════════════════

class DXYCollector:
    """Collect USD exchange rates for DXY calculation (100% free, no key needed).

    Primary: FloatRates (floatrates.com) — latest rates, very reliable.
    Fallback: Frankfurter API (ECB data) — if FloatRates fails.
    History builds over time (one row per collection run).
    """

    FLOATRATES_URL = 'https://www.floatrates.com/daily/usd.json'
    FRANKFURTER_URL = 'https://api.frankfurter.app/latest'

    # DXY basket weights (ICE formula)
    DXY_CONSTANT = 50.14348112
    DXY_WEIGHTS = {
        'EUR': 0.576,
        'JPY': 0.136,
        'GBP': 0.119,
        'CAD': 0.091,
        'SEK': 0.042,
        'CHF': 0.036,
    }

    @classmethod
    def compute_dxy(cls, rates: dict) -> float:
        """Compute DXY from USD-base rates.

        With base=USD rates (1 USD = X foreign currency):
          DXY formula: 50.14348112 × EUR^0.576 × JPY^0.136 × GBP^0.119
                       × CAD^0.091 × SEK^0.042 × CHF^0.036
        """
        result = cls.DXY_CONSTANT
        for currency, weight in cls.DXY_WEIGHTS.items():
            rate = rates.get(currency)
            if not rate or rate <= 0:
                return 0.0
            result *= rate ** weight
        return round(result, 4)

    def _fetch_floatrates(self) -> dict:
        """Fetch latest rates from FloatRates (primary source)."""
        resp = _retry_get(self.FLOATRATES_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # FloatRates uses lowercase keys with nested 'rate' field
        rates = {}
        for currency in self.DXY_WEIGHTS:
            key = currency.lower()
            if key in data and 'rate' in data[key]:
                rates[currency] = data[key]['rate']
        return rates

    def _fetch_frankfurter(self) -> dict:
        """Fetch latest rates from Frankfurter (fallback)."""
        symbols = ','.join(self.DXY_WEIGHTS.keys())
        resp = _retry_get(
            self.FRANKFURTER_URL,
            params={'base': 'USD', 'symbols': symbols},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get('rates', {})

    def collect(self, conn, days: int = 30) -> int:
        """Fetch today's USD rates and compute DXY. History builds over time."""
        log.info("[DXY] Collecting USD exchange rates...")

        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Try FloatRates first, then Frankfurter as fallback
        rates = {}
        source = ''
        try:
            rates = self._fetch_floatrates()
            source = 'FloatRates'
        except Exception as e:
            log.warning(f"  FloatRates failed: {e}, trying Frankfurter...")
            try:
                rates = self._fetch_frankfurter()
                source = 'Frankfurter'
            except Exception as e2:
                log.error(f"  Both rate sources failed: {e2}")
                raise

        if not all(c in rates for c in self.DXY_WEIGHTS):
            missing = [c for c in self.DXY_WEIGHTS if c not in rates]
            log.warning(f"  Missing currencies: {missing}")
            return 0

        dxy_value = self.compute_dxy(rates)
        if dxy_value <= 0:
            log.warning("  DXY computation returned 0")
            return 0

        conn.execute(
            "INSERT OR REPLACE INTO dxy_rates "
            "(date, eur, jpy, gbp, cad, sek, chf, dxy_value) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                today,
                rates['EUR'], rates['JPY'], rates['GBP'],
                rates['CAD'], rates['SEK'], rates['CHF'],
                dxy_value,
            )
        )
        conn.commit()
        log.info(f"  DXY = {dxy_value:.2f} (via {source})")
        return 1


# ════════════════════════════════════════════
# DEFI LLAMA — TVL Data
# ════════════════════════════════════════════

class DefiLlamaCollector:
    """Collect DeFi TVL data (100% free, no key needed)."""

    BASE_URL = 'https://api.llama.fi'

    def collect_top_protocols(self, conn: sqlite3.Connection, limit: int = 50):
        """Fetch top DeFi protocols by TVL."""
        log.info(f"[DeFi Llama] Collecting top {limit} protocols...")
        resp = _retry_get(f'{self.BASE_URL}/protocols', timeout=30)
        resp.raise_for_status()
        protocols = resp.json()[:limit]

        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        count = 0
        for p in protocols:
            conn.execute(
                "INSERT OR REPLACE INTO tvl (protocol, chain, date, tvl_usd, change_1d) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    p.get('name'),
                    p.get('chain'),
                    today,
                    p.get('tvl'),
                    p.get('change_1d'),
                )
            )
            count += 1

        conn.commit()
        total_tvl = sum(p.get('tvl', 0) for p in protocols)
        log.info(f"  Stored {count} protocols, total TVL: ${total_tvl/1e9:.1f}B")
        return count

    def collect_chain_tvl(self, conn: sqlite3.Connection):
        """Fetch TVL by chain."""
        log.info("[DeFi Llama] Collecting chain TVL...")
        resp = _retry_get(f'{self.BASE_URL}/v2/chains', timeout=30)
        resp.raise_for_status()
        chains = resp.json()

        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        count = 0
        for chain in chains[:30]:
            conn.execute(
                "INSERT OR REPLACE INTO tvl (protocol, chain, date, tvl_usd, change_1d) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    f"_chain_{chain.get('name', 'unknown')}",
                    chain.get('name'),
                    today,
                    chain.get('tvl'),
                    None,
                )
            )
            count += 1

        conn.commit()
        log.info(f"  Stored TVL for {count} chains")
        return count


# ════════════════════════════════════════════
# RSS NEWS — CoinDesk, CoinTelegraph
# ════════════════════════════════════════════

class RSSNewsCollector:
    """Collect crypto news from RSS feeds (free, no key needed)."""

    def collect(self, conn: sqlite3.Connection):
        """Fetch latest news from all RSS feeds."""
        log.info("[RSS] Collecting news from CoinDesk + CoinTelegraph...")
        total = 0

        import socket
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(15)  # 15s timeout for RSS feeds

        for source, url in RSS_FEEDS.items():
            try:
                feed = feedparser.parse(url, request_headers={'User-Agent': 'FORTIX/1.0'})
                count = 0
                for entry in feed.entries[:30]:
                    # Generate stable ID
                    raw_id = entry.get('id', entry.get('link', entry.get('title', '')))
                    news_id = hashlib.md5(raw_id.encode()).hexdigest()[:16]

                    # Parse timestamp
                    ts = entry.get('published_parsed')
                    if ts:
                        timestamp = int(time.mktime(ts))
                    else:
                        timestamp = int(time.time())

                    title = entry.get('title', '').strip()
                    link = entry.get('link', '')

                    # Detect mentioned coins
                    mentioned = []
                    title_upper = title.upper()
                    for coin in TRACKED_COINS:
                        if coin in title_upper or COINGECKO_IDS.get(coin, '').lower() in title.lower():
                            mentioned.append(coin)

                    conn.execute(
                        "INSERT OR IGNORE INTO news (id, timestamp, title, source, url, coins_mentioned) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (news_id, timestamp, title, source, link,
                         ','.join(mentioned) if mentioned else None)
                    )
                    count += 1

                total += count
                log.info(f"  {source}: {count} articles")
            except Exception as e:
                log.warning(f"  {source} failed: {e}")

        socket.setdefaulttimeout(old_timeout)  # Restore original timeout
        conn.commit()
        log.info(f"  Total: {total} news articles stored")
        return total


# ════════════════════════════════════════════
# CRYPTOPANIC — News with Sentiment
# ════════════════════════════════════════════

# In-memory rate-limit guard — prevents hitting CryptoPanic more than once
# per 30-minute window even when collect_all() runs every 10 minutes.
_CRYPTOPANIC_COOLDOWN_SEC = 30 * 60  # 30 minutes
_cryptopanic_last_fetch: dict = {'ts': None}  # datetime (UTC) of last successful API call


class CryptoPanicCollector:
    """Collect news with crowd-sourced sentiment (Developer V2 API)."""

    BASE_URL = 'https://cryptopanic.com/api/developer/v2/posts/'

    def __init__(self):
        self.api_key = CRYPTOPANIC_KEY

    def collect(self, conn: sqlite3.Connection):
        if not self.api_key:
            log.info("[CryptoPanic] Skipped — no API key (set CRYPTOPANIC_API_KEY)")
            return 0

        # ── Rate-limit guard: skip if last fetch was less than 30 min ago ──
        now = datetime.now(timezone.utc)
        last = _cryptopanic_last_fetch['ts']
        if last and (now - last).total_seconds() < _CRYPTOPANIC_COOLDOWN_SEC:
            age_min = (now - last).total_seconds() / 60
            log.info(f"[CryptoPanic] Skipped — last fetch {age_min:.0f}m ago (cooldown {_CRYPTOPANIC_COOLDOWN_SEC // 60}m)")
            return 0

        log.info("[CryptoPanic] Collecting news (Developer V2)...")
        count = 0

        for filter_type in ['hot', 'rising', 'important']:
            try:
                resp = _retry_get(self.BASE_URL, params={
                    'auth_token': self.api_key,
                    'filter': filter_type,
                    'public': 'true',
                }, timeout=15)
                resp.raise_for_status()

                results = resp.json().get('results', [])
                for item in results:
                    title = item.get('title', '').strip()
                    if not title:
                        continue

                    # Generate stable ID from title + date
                    raw_id = title + item.get('created_at', '')
                    news_id = 'cp_' + hashlib.md5(raw_id.encode()).hexdigest()[:12]

                    ts_str = item.get('created_at', item.get('published_at', ''))
                    try:
                        timestamp = int(datetime.fromisoformat(
                            ts_str.replace('Z', '+00:00')).timestamp())
                    except (ValueError, AttributeError):
                        timestamp = int(time.time())

                    # Detect mentioned coins from title
                    mentioned = []
                    title_upper = title.upper()
                    for coin in TRACKED_COINS:
                        if coin in title_upper:
                            mentioned.append(coin)
                        elif COINGECKO_IDS.get(coin, '').split('-')[0].upper() in title_upper:
                            mentioned.append(coin)

                    # Use description for basic sentiment (keyword-based)
                    desc = (item.get('description', '') or '').lower()
                    sentiment = 'neutral'
                    bull_words = ['surge', 'rally', 'bull', 'gain', 'pump', 'breakout', 'soar', 'jump', 'approval', 'partnership']
                    bear_words = ['crash', 'drop', 'bear', 'dump', 'hack', 'exploit', 'ban', 'trouble', 'decline', 'fall', 'fear']
                    bull_hits = sum(1 for w in bull_words if w in desc or w in title.lower())
                    bear_hits = sum(1 for w in bear_words if w in desc or w in title.lower())
                    if bull_hits > bear_hits:
                        sentiment = 'bullish'
                    elif bear_hits > bull_hits:
                        sentiment = 'bearish'

                    conn.execute(
                        "INSERT OR IGNORE INTO news (id, timestamp, title, source, sentiment, coins_mentioned) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (news_id, timestamp, title, f'cryptopanic_{filter_type}',
                         sentiment, ','.join(mentioned) if mentioned else None)
                    )
                    count += 1

                time.sleep(0.5)
            except Exception as e:
                log.warning(f"  CryptoPanic {filter_type}: {e}")

        conn.commit()
        # Mark successful fetch time for rate-limit guard
        _cryptopanic_last_fetch['ts'] = datetime.now(timezone.utc)
        log.info(f"  Stored {count} news items (hot + rising + important)")
        return count


# ════════════════════════════════════════════
# WHALE ALERT
# ════════════════════════════════════════════

class WhaleAlertCollector:
    """Collect large transactions (paid key required, free = 10 req/min)."""

    BASE_URL = 'https://api.whale-alert.io/v1'

    def __init__(self):
        self.api_key = WHALE_ALERT_KEY

    def collect(self, conn: sqlite3.Connection, min_usd: int = 1_000_000):
        if not self.api_key:
            log.info("[Whale Alert] Skipped — no API key (set WHALE_ALERT_API_KEY)")
            return 0

        log.info(f"[Whale Alert] Collecting transactions >${min_usd/1e6:.0f}M...")
        since = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())

        resp = _retry_get(f'{self.BASE_URL}/transactions', params={
            'api_key': self.api_key,
            'min_value': min_usd,
            'start': since,
        }, timeout=15)
        resp.raise_for_status()

        txs = resp.json().get('transactions', [])
        count = 0
        for tx in txs:
            conn.execute(
                "INSERT OR REPLACE INTO whale_transactions "
                "(tx_hash, timestamp, blockchain, from_addr, from_label, "
                "to_addr, to_label, amount, amount_usd, coin) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    tx.get('hash', f"wa_{tx.get('id', '')}"),
                    tx.get('timestamp', 0),
                    tx.get('blockchain', ''),
                    tx.get('from', {}).get('address', ''),
                    tx.get('from', {}).get('owner', ''),
                    tx.get('to', {}).get('address', ''),
                    tx.get('to', {}).get('owner', ''),
                    tx.get('amount', 0),
                    tx.get('amount_usd', 0),
                    tx.get('symbol', '').upper(),
                )
            )
            count += 1

        conn.commit()
        log.info(f"  Stored {count} whale transactions (24h)")
        return count


# ════════════════════════════════════════════
# ETHERSCAN V2 — Large Token Transfers
# ════════════════════════════════════════════

class EtherscanCollector:
    """Collect large ETH/ERC-20 transfers via Etherscan V2 (paid key)."""

    BASE_URL = 'https://api.etherscan.io/v2/api'

    def __init__(self):
        self.api_key = ETHERSCAN_KEY

    def collect_large_transfers(self, conn: sqlite3.Connection,
                                 min_blocks_back: int = 1000):
        """Fetch recent large ETH transfers."""
        if not self.api_key:
            log.info("[Etherscan] Skipped — no API key (set ETHERSCAN_API_KEY)")
            return 0

        log.info("[Etherscan V2] Collecting large ETH transfers...")

        # Get latest block
        resp = _retry_get(self.BASE_URL, params={
            'chainid': 1,
            'module': 'proxy',
            'action': 'eth_blockNumber',
            'apikey': self.api_key,
        }, timeout=15)
        resp.raise_for_status()
        latest_block = int(resp.json().get('result', '0x0'), 16)

        if latest_block == 0:
            log.warning("  Could not get latest block")
            return 0

        # Get recent internal transactions (large ETH movements)
        start_block = latest_block - min_blocks_back
        resp = _retry_get(self.BASE_URL, params={
            'chainid': 1,
            'module': 'account',
            'action': 'txlistinternal',
            'startblock': start_block,
            'endblock': latest_block,
            'page': 1,
            'offset': 50,
            'sort': 'desc',
            'apikey': self.api_key,
        }, timeout=15)
        resp.raise_for_status()

        result = resp.json().get('result', [])
        if isinstance(result, str):
            log.warning(f"  Etherscan returned message: {result}")
            return 0

        count = 0
        for tx in result:
            value_eth = int(tx.get('value', '0')) / 1e18
            if value_eth < 100:  # Only large transfers (100+ ETH)
                continue

            conn.execute(
                "INSERT OR REPLACE INTO whale_transactions "
                "(tx_hash, timestamp, blockchain, from_addr, to_addr, amount, coin) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    tx.get('hash', ''),
                    int(tx.get('timeStamp', 0)),
                    'ethereum',
                    tx.get('from', ''),
                    tx.get('to', ''),
                    value_eth,
                    'ETH',
                )
            )
            count += 1

        conn.commit()
        log.info(f"  Stored {count} large ETH transfers")
        return count


# ════════════════════════════════════════════
# TWITTER/X — Crypto Sentiment via TwitterAPI.io
# ════════════════════════════════════════════

class TwitterCollector:
    """Collect crypto sentiment from X/Twitter via TwitterAPI.io ($0.15/1000 tweets)."""

    # Key crypto accounts to monitor
    INFLUENCERS = [
        'elonmusk', 'VitalikButerin', 'sabordo', 'CryptoCapo_',
        'AltcoinGordon', 'WhalePanda', 'CryptoCred', 'inversebrah',
        'CryptoHayes', 'TheCryptoDog', 'blaborneaux', 'CryptoKaleo',
    ]

    # Search queries for crypto sentiment
    QUERIES = [
        'bitcoin OR BTC -is:retweet lang:en',
        'ethereum OR ETH merge -is:retweet lang:en',
        'solana OR SOL -is:retweet lang:en',
        'crypto market crash OR pump OR rally -is:retweet lang:en',
        'altcoin season -is:retweet lang:en',
    ]

    # Sentiment keywords
    BULL_WORDS = {
        'bullish', 'moon', 'pump', 'rally', 'breakout', 'buy', 'long',
        'accumulate', 'undervalued', 'gem', 'ath', 'launch', 'adoption',
        'parabolic', 'surge', 'recovery', 'uptrend', 'green', 'bottom',
    }
    BEAR_WORDS = {
        'bearish', 'dump', 'crash', 'sell', 'short', 'overvalued', 'scam',
        'rug', 'fear', 'liquidation', 'capitulation', 'dead', 'bleeding',
        'collapse', 'downtrend', 'red', 'top', 'bubble', 'ponzi',
    }

    def __init__(self):
        self.api_key = TWITTER_API_KEY
        self.base_url = TWITTER_API_URL
        self.session = requests.Session()
        self.session.headers['X-API-Key'] = self.api_key

    def _search_tweets(self, query: str, max_results: int = 50) -> list:
        """Search tweets via TwitterAPI.io advanced search with retry."""
        for attempt in range(3):
            try:
                resp = self.session.get(
                    f'{self.base_url}/twitter/tweet/advanced_search',
                    params={
                        'query': query,
                        'queryType': 'Latest',
                        'cursor': '',
                    },
                    timeout=20,
                )
                if resp.status_code == 429:
                    wait = 3 * (attempt + 1)
                    log.debug(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                tweets = data.get('tweets', [])
                return tweets[:max_results]
            except requests.exceptions.HTTPError as e:
                if '429' in str(e) and attempt < 2:
                    time.sleep(3 * (attempt + 1))
                    continue
                log.warning(f"  Twitter search failed for '{query[:30]}...': {e}")
                return []
            except Exception as e:
                log.warning(f"  Twitter search failed for '{query[:30]}...': {e}")
                return []
        return []

    def _analyze_sentiment(self, tweet: dict) -> tuple:
        """Engagement-weighted keyword sentiment: returns (sentiment_str, score).
        Tweets with more likes/retweets/views get higher weight."""
        text = tweet.get('text', '') if isinstance(tweet, dict) else str(tweet)
        text_lower = text.lower()
        words = set(text_lower.split())
        bull = len(words & self.BULL_WORDS)
        bear = len(words & self.BEAR_WORDS)

        # Engagement multiplier: high-engagement tweets matter more
        if isinstance(tweet, dict):
            likes = tweet.get('likeCount', 0) or 0
            retweets = tweet.get('retweetCount', 0) or 0
            views = tweet.get('viewCount', 0) or 0
            # Logarithmic weighting: 10 likes = 1.0x, 100 likes = 2.0x, 1000 likes = 3.0x
            engagement = max(likes + retweets * 2, 1)
            weight = 1.0 + min(np.log10(engagement), 3.0) * 0.3
        else:
            weight = 1.0

        if bull > bear:
            return 'bullish', min(bull * 0.2 * weight, 1.0)
        elif bear > bull:
            return 'bearish', max(-bear * 0.2 * weight, -1.0)
        return 'neutral', 0.0

    def _detect_coins(self, text: str) -> list:
        """Detect which tracked coins are mentioned."""
        text_upper = text.upper()
        found = []
        coin_names = {
            'BITCOIN': 'BTC', 'ETHEREUM': 'ETH', 'SOLANA': 'SOL',
            'CARDANO': 'ADA', 'DOGECOIN': 'DOGE', 'AVALANCHE': 'AVAX',
            'POLKADOT': 'DOT', 'CHAINLINK': 'LINK', 'UNISWAP': 'UNI',
        }
        for coin in TRACKED_COINS:
            if f'${coin}' in text_upper or f' {coin} ' in f' {text_upper} ':
                found.append(coin)
        for name, sym in coin_names.items():
            if name in text_upper and sym not in found:
                found.append(sym)
        return found

    def collect(self, conn: sqlite3.Connection) -> int:
        """Collect crypto tweets and aggregate sentiment."""
        if not self.api_key:
            log.info("[Twitter/X] Skipped -- no API key (set TWITTER_API_KEY)")
            return 0

        log.info("[Twitter/X] Collecting crypto sentiment from X...")
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        all_tweets = []

        # Search coin-specific tweets using cashtags (native Twitter feature)
        # min_faves:5 filters spam, -filter:replies removes reply noise
        for coin in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE']:
            query = f'${coin} min_faves:5 -filter:replies lang:en'
            tweets = self._search_tweets(query, max_results=20)
            all_tweets.extend([(coin, t) for t in tweets])
            time.sleep(2.0)

        # Search broad crypto market sentiment
        broad_tweets = self._search_tweets(
            'crypto market min_faves:10 -filter:replies lang:en', max_results=20
        )
        all_tweets.extend([('MARKET', t) for t in broad_tweets])
        time.sleep(2.0)

        # Search crypto fear/panic tweets
        fear_tweets = self._search_tweets(
            'crypto crash OR capitulation OR liquidation min_faves:5 -filter:replies lang:en',
            max_results=20
        )
        all_tweets.extend([('FEAR', t) for t in fear_tweets])

        log.info(f"  Fetched {len(all_tweets)} tweets total")

        # Aggregate sentiment per coin (engagement-weighted)
        coin_sentiment = {}
        for tag, tweet in all_tweets:
            text = tweet.get('text', '')
            sentiment, score = self._analyze_sentiment(tweet)
            coins = self._detect_coins(text)
            if not coins and tag in TRACKED_COINS:
                coins = [tag]

            for coin in coins:
                if coin not in coin_sentiment:
                    coin_sentiment[coin] = {
                        'scores': [], 'bullish': 0, 'bearish': 0, 'neutral': 0,
                        'total': 0,
                    }
                coin_sentiment[coin]['scores'].append(score)
                coin_sentiment[coin][sentiment] += 1
                coin_sentiment[coin]['total'] += 1

        # Also aggregate market-wide sentiment
        market_scores = []
        for tag, tweet in all_tweets:
            _, score = self._analyze_sentiment(tweet)
            market_scores.append(score)

        # Store aggregated sentiment per coin
        count = 0
        for coin, data in coin_sentiment.items():
            if data['total'] == 0:
                continue
            avg_score = np.mean(data['scores']) if data['scores'] else 0
            positive = data['bullish'] / data['total'] if data['total'] > 0 else 0
            negative = data['bearish'] / data['total'] if data['total'] > 0 else 0

            conn.execute(
                "INSERT OR REPLACE INTO social_sentiment "
                "(coin, date, source, score, volume, positive, negative) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (coin, today, 'twitter', float(avg_score), data['total'],
                 float(positive), float(negative))
            )
            count += 1

        # Store market-wide sentiment
        if market_scores:
            conn.execute(
                "INSERT OR REPLACE INTO social_sentiment "
                "(coin, date, source, score, volume, positive, negative) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ('MARKET', today, 'twitter', float(np.mean(market_scores)),
                 len(market_scores), 0, 0)
            )
            count += 1

        conn.commit()
        log.info(f"  Stored sentiment for {count} coins from {len(all_tweets)} tweets")
        return count


# ════════════════════════════════════════════
# MAIN COLLECTOR
# ════════════════════════════════════════════

def collect_all(sources: list = None, heartbeat_fn=None):
    """Run all data collectors."""
    conn = init_db()

    all_sources = {
        'binance': lambda: _collect_binance(conn, heartbeat_fn=heartbeat_fn),
        'coingecko': lambda: _collect_coingecko(conn),
        'fear_greed': lambda: FearGreedCollector().collect(conn),
        'defi_llama': lambda: _collect_defi_llama(conn),
        'rss': lambda: RSSNewsCollector().collect(conn),
        'cryptopanic': lambda: CryptoPanicCollector().collect(conn),
        'whale_alert': lambda: WhaleAlertCollector().collect(conn),
        'etherscan': lambda: EtherscanCollector().collect_large_transfers(conn),
        'twitter': lambda: TwitterCollector().collect(conn),
        'coinglass': lambda: _collect_coinglass(conn),
        'cryptoquant': lambda: _collect_cryptoquant(conn),
        'dxy': lambda: DXYCollector().collect(conn),
    }

    if sources:
        to_run = {k: v for k, v in all_sources.items() if k in sources}
    else:
        to_run = all_sources

    results = {}
    import socket
    for name, collector_fn in to_run.items():
        t_start = time.time()
        # Set socket-level timeout per collector (prevents indefinite hangs)
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(30)  # 30s max per HTTP request
        try:
            log.info(f"\n{'='*50}")
            result = collector_fn()
            results[name] = result
            elapsed = time.time() - t_start
            log.info(f"  [{name}] completed in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t_start
            log.error(f"[{name}] FAILED after {elapsed:.1f}s: {e}")
            results[name] = f"ERROR: {e}"
        finally:
            socket.setdefaulttimeout(old_timeout)
            if heartbeat_fn:
                heartbeat_fn()

    conn.close()
    return results


def _collect_binance(conn, heartbeat_fn=None):
    bc = BinanceCollector()
    bc.collect_candles(conn, '1h', 200)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_candles(conn, '4h', 200)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_candles(conn, '1d', 365)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_funding_rates(conn)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_open_interest(conn)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_liquidations(conn)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_long_short_ratios(conn)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_taker_volume(conn)
    if heartbeat_fn: heartbeat_fn()
    bc.collect_tickers(conn)


def _collect_coingecko(conn):
    cg = CoinGeckoCollector()
    cg.collect_market_data(conn)
    cg.collect_global_metrics(conn)
    cg.collect_trending(conn)


def _collect_coinglass(conn):
    from src.crypto.coinglass_collector import collect_all_coinglass
    return collect_all_coinglass(conn)


def _collect_cryptoquant(conn):
    from src.crypto.cryptoquant_collector import collect_all_cryptoquant
    return collect_all_cryptoquant(conn)


def _collect_defi_llama(conn):
    dl = DefiLlamaCollector()
    dl.collect_top_protocols(conn)
    dl.collect_chain_tvl(conn)


def show_status():
    """Show data status report."""
    if not DB_PATH.exists():
        print("No database found. Run collector first.")
        return

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    tables = {
        'prices': 'SELECT COUNT(*), COUNT(DISTINCT coin), MIN(datetime(timestamp, "unixepoch")), MAX(datetime(timestamp, "unixepoch")) FROM prices',
        'market_overview': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM market_overview',
        'fear_greed': 'SELECT COUNT(*), MIN(date), MAX(date) FROM fear_greed',
        'funding_rates': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM funding_rates',
        'open_interest': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM open_interest',
        'liquidations': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM liquidations',
        'long_short_ratio': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM long_short_ratio',
        'taker_volume': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM taker_volume',
        'news': 'SELECT COUNT(*), COUNT(DISTINCT source) FROM news',
        'tvl': 'SELECT COUNT(*), COUNT(DISTINCT protocol) FROM tvl',
        'whale_transactions': 'SELECT COUNT(*) FROM whale_transactions',
        'predictions': 'SELECT COUNT(*) FROM predictions',
        'cg_exchange_balance': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM cg_exchange_balance',
        'cg_options_max_pain': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM cg_options_max_pain',
        'cg_etf_flows': 'SELECT COUNT(*), MIN(date), MAX(date) FROM cg_etf_flows',
        'cg_stablecoin_supply': 'SELECT COUNT(*), MIN(date), MAX(date) FROM cg_stablecoin_supply',
        'cg_aggregated_oi': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM cg_aggregated_oi',
        'cg_liquidations': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM cg_liquidations',
        'cq_btc_onchain': 'SELECT COUNT(*), COUNT(DISTINCT metric), MIN(date), MAX(date) FROM cq_btc_onchain',
        'cq_exchange_flows': 'SELECT COUNT(*), COUNT(DISTINCT coin), MIN(date), MAX(date) FROM cq_exchange_flows',
        'cq_coinbase_premium': 'SELECT COUNT(*), MIN(date), MAX(date) FROM cq_coinbase_premium',
        'cq_miner_data': 'SELECT COUNT(*), MIN(date), MAX(date) FROM cq_miner_data',
        'cq_active_addresses': 'SELECT COUNT(*), COUNT(DISTINCT coin) FROM cq_active_addresses',
        'cq_stablecoin_flows': 'SELECT COUNT(*), MIN(date), MAX(date) FROM cq_stablecoin_flows',
    }

    print("\n" + "=" * 60)
    print("ALPHA SIGNAL — Data Status")
    print("=" * 60)

    for table, query in tables.items():
        try:
            row = conn.execute(query).fetchone()
            print(f"\n  {table}:")
            if table == 'prices':
                print(f"    Rows: {row[0]:,} | Coins: {row[1]} | From: {row[2]} | To: {row[3]}")
            elif table in ('cq_btc_onchain', 'cq_exchange_flows'):
                print(f"    Rows: {row[0]:,} | Types: {row[1]} | From: {row[2]} | To: {row[3]}")
            elif table in ('fear_greed', 'cg_etf_flows', 'cg_stablecoin_supply',
                          'cq_coinbase_premium', 'cq_miner_data', 'cq_stablecoin_flows'):
                print(f"    Days: {row[0]} | From: {row[1]} | To: {row[2]}")
            elif table in ('market_overview', 'funding_rates', 'open_interest',
                          'liquidations', 'long_short_ratio', 'taker_volume',
                          'cg_exchange_balance', 'cg_options_max_pain',
                          'cg_aggregated_oi', 'cg_liquidations',
                          'cq_active_addresses'):
                print(f"    Rows: {row[0]:,} | Coins: {row[1]}")
            elif table == 'news':
                print(f"    Articles: {row[0]:,} | Sources: {row[1]}")
            elif table == 'tvl':
                print(f"    Rows: {row[0]:,} | Protocols: {row[1]}")
            else:
                print(f"    Rows: {row[0]:,}")
        except Exception as e:
            print(f"    Error: {e}")

    # Show latest Fear & Greed
    try:
        fg = conn.execute("SELECT date, value, classification FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
        if fg:
            print(f"\n  Latest Fear & Greed: {fg[1]} ({fg[2]}) on {fg[0]}")
    except:
        pass

    # Check API key status
    print("\n  API Keys:")
    keys = {
        'CoinGecko': bool(COINGECKO_KEY),
        'Etherscan V2': bool(ETHERSCAN_KEY),
        'CryptoPanic': bool(CRYPTOPANIC_KEY),
        'Whale Alert': bool(WHALE_ALERT_KEY),
        'LunarCrush': bool(LUNARCRUSH_KEY),
        'Santiment': bool(SANTIMENT_KEY),
        'Twitter/X': bool(TWITTER_API_KEY),
        'CoinGlass': bool(COINGLASS_KEY),
        'CryptoQuant': bool(CRYPTOQUANT_KEY),
    }
    for name, has_key in keys.items():
        status = 'OK' if has_key else 'MISSING'
        print(f"    {name}: [{status}]")

    print("=" * 60)
    conn.close()


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--status' in args:
        show_status()
    elif any(a.startswith('--source=') for a in args):
        sources = [a.split('=')[1] for a in args if a.startswith('--source=')]
        results = collect_all(sources)
        print(f"\nResults: {json.dumps(results, indent=2, default=str)}")
    else:
        log.info("=" * 60)
        log.info("ALPHA SIGNAL — Data Collection")
        log.info("=" * 60)
        results = collect_all()
        log.info("\n" + "=" * 60)
        log.info("COLLECTION COMPLETE")
        for name, result in results.items():
            log.info(f"  {name}: {result}")
        log.info("=" * 60)

        # Show status
        show_status()
