"""
FORTIX — Technical Analyzer
===================================
Calculates technical indicators from price data in SQLite.

Signals:
  - Moving Average crossovers (50/200 SMA)
  - RSI (Relative Strength Index)
  - Bollinger Bands position
  - Support/Resistance levels
  - MACD crossover
  - Volume trend

Each signal returns a score from -1.0 (bearish) to +1.0 (bullish).
"""

import sys
import sqlite3
import logging
import numpy as np
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('technical')

DB_PATH = Path('data/crypto/market.db')


def get_prices(conn: sqlite3.Connection, coin: str, timeframe: str = '1d',
               limit: int = 365) -> list:
    """Get price history as list of (timestamp, open, high, low, close, volume).
    Returns the LATEST `limit` candles in ascending (chronological) order."""
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM "
        "(SELECT timestamp, open, high, low, close, volume FROM prices "
        " WHERE coin = ? AND timeframe = ? ORDER BY timestamp DESC LIMIT ?) "
        "ORDER BY timestamp ASC", (coin, timeframe, limit)
    ).fetchall()
    return rows


def calc_sma(closes: np.ndarray, period: int) -> np.ndarray:
    """Simple Moving Average."""
    if len(closes) < period:
        return np.full(len(closes), np.nan)
    sma = np.convolve(closes, np.ones(period) / period, mode='full')[:len(closes)]
    sma[:period - 1] = np.nan
    return sma


def calc_ema(closes: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average."""
    ema = np.zeros(len(closes))
    ema[:] = np.nan
    if len(closes) < period:
        return ema
    ema[period - 1] = np.mean(closes[:period])
    multiplier = 2 / (period + 1)
    for i in range(period, len(closes)):
        ema[i] = closes[i] * multiplier + ema[i - 1] * (1 - multiplier)
    return ema


def calc_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Relative Strength Index."""
    rsi = np.full(len(closes), np.nan)
    if len(closes) < period + 1:
        return rsi

    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100 - (100 / (1 + rs))

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i + 1] = 100 - (100 / (1 + rs))

    return np.clip(rsi, 0.0, 100.0)


def calc_bollinger(closes: np.ndarray, period: int = 20, std_dev: float = 2.0):
    """Bollinger Bands: (middle, upper, lower)."""
    middle = calc_sma(closes, period)
    std = np.full(len(closes), np.nan)
    for i in range(period - 1, len(closes)):
        std[i] = np.std(closes[i - period + 1:i + 1])
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    return middle, upper, lower


def calc_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Average True Range — measures volatility.

    Returns ATR as a percentage of current price (e.g., 0.03 = 3%).
    """
    if len(closes) < period + 1:
        return 0.03  # Default 3% if insufficient data

    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1])
        )
    )

    # Wilder smoothing
    atr = np.mean(tr[:period])
    for i in range(period, len(tr)):
        atr = (atr * (period - 1) + tr[i]) / period

    current_price = closes[-1]
    if current_price == 0:
        return 0.03

    return float(atr / current_price)  # Return as percentage


def calc_support_resistance(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                            n_levels: int = 3, lookback: int = 5) -> dict:
    """Find Support/Resistance levels from swing highs/lows.

    Uses swing point detection (a high/low that's higher/lower than `lookback`
    candles on both sides) then clusters nearby levels within 1.5%.

    Returns:
        {'support': [price1, price2, ...], 'resistance': [price1, price2, ...]}
        Levels sorted by distance from current price (closest first).
    """
    current_price = closes[-1]
    if len(closes) < lookback * 2 + 1:
        return {'support': [], 'resistance': []}

    swing_highs = []
    swing_lows = []

    for i in range(lookback, len(highs) - lookback):
        # Swing high: higher than `lookback` candles on both sides
        if highs[i] == max(highs[i - lookback:i + lookback + 1]):
            swing_highs.append(float(highs[i]))
        # Swing low: lower than `lookback` candles on both sides
        if lows[i] == min(lows[i - lookback:i + lookback + 1]):
            swing_lows.append(float(lows[i]))

    def cluster_levels(levels, threshold_pct=0.015):
        """Cluster nearby price levels within threshold_pct of each other."""
        if not levels:
            return []
        sorted_levels = sorted(levels)
        clusters = []
        current_cluster = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            if (level - current_cluster[0]) / current_cluster[0] < threshold_pct:
                current_cluster.append(level)
            else:
                clusters.append(current_cluster)
                current_cluster = [level]
        clusters.append(current_cluster)

        # Return mean of each cluster, weighted by count (more touches = stronger)
        return [(np.mean(c), len(c)) for c in clusters]

    high_clusters = cluster_levels(swing_highs)
    low_clusters = cluster_levels(swing_lows)

    # All levels with strength
    all_levels = []
    for price, strength in high_clusters + low_clusters:
        level_type = 'resistance' if price > current_price else 'support'
        all_levels.append((price, level_type, strength))

    # Sort by distance from current price
    all_levels.sort(key=lambda x: abs(x[0] - current_price))

    # Smart rounding: enough decimals for micro-priced coins
    import math
    if current_price >= 1:
        decimals = 2
    elif current_price >= 0.001:
        decimals = 4
    else:
        decimals = max(2, -int(math.floor(math.log10(abs(current_price)))) + 2) if current_price > 0 else 2

    support = [round(l[0], decimals) for l in all_levels if l[1] == 'support'][:n_levels]
    resistance = [round(l[0], decimals) for l in all_levels if l[1] == 'resistance'][:n_levels]

    return {'support': support, 'resistance': resistance}


def calc_macd(closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD: (macd_line, signal_line, histogram)."""
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    macd_line = ema_fast - ema_slow

    # Signal line = EMA of MACD
    valid_start = slow - 1
    signal_line = np.full(len(closes), np.nan)
    if len(closes) > valid_start + signal:
        macd_valid = macd_line[valid_start:]
        ema_sig = calc_ema(macd_valid, signal)
        signal_line[valid_start:] = ema_sig

    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


# ════════════════════════════════════════════
# SIGNAL SCORERS — each returns [-1.0, +1.0]
# ════════════════════════════════════════════

def score_ma_crossover(closes: np.ndarray) -> dict:
    """Score based on MA50/MA200 crossover (golden/death cross)."""
    if len(closes) < 200:
        return {'score': 0.0, 'reason': 'Insufficient data for MA200'}

    sma50 = calc_sma(closes, 50)
    sma200 = calc_sma(closes, 200)

    current_50 = sma50[-1]
    current_200 = sma200[-1]
    prev_50 = sma50[-2]
    prev_200 = sma200[-2]

    price = closes[-1]

    # Golden cross (50 crosses above 200)
    if prev_50 <= prev_200 and current_50 > current_200:
        return {'score': 0.8, 'reason': 'Golden Cross (MA50 crossed above MA200)'}
    # Death cross
    if prev_50 >= prev_200 and current_50 < current_200:
        return {'score': -0.8, 'reason': 'Death Cross (MA50 crossed below MA200)'}

    # Trend strength: how far apart are the MAs?
    spread = (current_50 - current_200) / current_200
    score = np.clip(spread * 5, -0.6, 0.6)

    # Price vs MA200
    price_vs_200 = (price - current_200) / current_200
    if price > current_200:
        reason = f'Price {price_vs_200*100:.1f}% above MA200 (bullish trend)'
    else:
        reason = f'Price {price_vs_200*100:.1f}% below MA200 (bearish trend)'

    return {'score': float(score), 'reason': reason}


def score_rsi(closes: np.ndarray) -> dict:
    """Score based on RSI (overbought/oversold)."""
    rsi = calc_rsi(closes)
    current_rsi = rsi[-1]

    if np.isnan(current_rsi):
        return {'score': 0.0, 'reason': 'RSI not available'}

    if current_rsi < 20:
        score = 0.8
        reason = f'RSI={current_rsi:.0f} (extremely oversold — strong buy signal)'
    elif current_rsi < 30:
        score = 0.5
        reason = f'RSI={current_rsi:.0f} (oversold — buy signal)'
    elif current_rsi < 45:
        score = 0.2
        reason = f'RSI={current_rsi:.0f} (mildly oversold)'
    elif current_rsi > 80:
        score = -0.8
        reason = f'RSI={current_rsi:.0f} (extremely overbought — strong sell signal)'
    elif current_rsi > 70:
        score = -0.5
        reason = f'RSI={current_rsi:.0f} (overbought — sell signal)'
    elif current_rsi > 55:
        score = -0.2
        reason = f'RSI={current_rsi:.0f} (mildly overbought)'
    else:
        score = 0.0
        reason = f'RSI={current_rsi:.0f} (neutral zone)'

    return {'score': float(score), 'reason': reason}


def score_bollinger(closes: np.ndarray) -> dict:
    """Score based on Bollinger Band position."""
    middle, upper, lower = calc_bollinger(closes)

    if np.isnan(upper[-1]) or np.isnan(lower[-1]):
        return {'score': 0.0, 'reason': 'Bollinger Bands not available'}

    price = closes[-1]
    band_width = upper[-1] - lower[-1]
    if band_width < 1e-8 * max(abs(price), 1):
        return {'score': 0.0, 'reason': 'Bollinger Bands width is zero'}

    position = (price - lower[-1]) / band_width  # 0 = at lower, 1 = at upper

    if position < 0.05:
        score = 0.7
        reason = f'Price at lower Bollinger Band (potential bounce)'
    elif position < 0.2:
        score = 0.4
        reason = f'Price near lower band ({position*100:.0f}%)'
    elif position > 0.95:
        score = -0.7
        reason = f'Price at upper Bollinger Band (potential reversal)'
    elif position > 0.8:
        score = -0.4
        reason = f'Price near upper band ({position*100:.0f}%)'
    else:
        score = 0.0
        reason = f'Price in middle of bands ({position*100:.0f}%)'

    return {'score': float(score), 'reason': reason}


def score_macd(closes: np.ndarray) -> dict:
    """Score based on MACD crossover and histogram."""
    macd_line, signal_line, histogram = calc_macd(closes)

    if np.isnan(histogram[-1]) or np.isnan(histogram[-2]):
        return {'score': 0.0, 'reason': 'MACD not available'}

    # Bullish crossover (MACD crosses above signal)
    if histogram[-2] <= 0 and histogram[-1] > 0:
        return {'score': 0.6, 'reason': 'MACD bullish crossover'}
    # Bearish crossover
    if histogram[-2] >= 0 and histogram[-1] < 0:
        return {'score': -0.6, 'reason': 'MACD bearish crossover'}

    # Histogram momentum
    if histogram[-1] > 0 and histogram[-1] > histogram[-2]:
        score = 0.3
        reason = 'MACD histogram growing (bullish momentum)'
    elif histogram[-1] < 0 and histogram[-1] < histogram[-2]:
        score = -0.3
        reason = 'MACD histogram declining (bearish momentum)'
    elif histogram[-1] > 0:
        score = 0.1
        reason = 'MACD above signal (mild bullish)'
    elif histogram[-1] < 0:
        score = -0.1
        reason = 'MACD below signal (mild bearish)'
    else:
        score = 0.0
        reason = 'MACD neutral'

    return {'score': float(score), 'reason': reason}


def score_volume_trend(prices: list) -> dict:
    """Score based on volume trend (rising volume confirms trend)."""
    if len(prices) < 20:
        return {'score': 0.0, 'reason': 'Insufficient volume data'}

    volumes = np.array([p[5] for p in prices if p[5]])
    closes = np.array([p[4] for p in prices if p[4]])

    if len(volumes) < 20:
        return {'score': 0.0, 'reason': 'Insufficient volume data'}

    # Compare recent 5-day avg volume to 20-day avg
    recent_vol = np.mean(volumes[-5:])
    avg_vol = np.mean(volumes[-20:])

    if avg_vol == 0:
        return {'score': 0.0, 'reason': 'No volume data'}

    vol_ratio = recent_vol / avg_vol

    # Price direction
    price_change = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] != 0 else 0

    if vol_ratio > 1.5 and price_change > 0:
        score = 0.5
        reason = f'Volume surge ({vol_ratio:.1f}x avg) + rising price'
    elif vol_ratio > 1.5 and price_change < 0:
        score = -0.5
        reason = f'Volume surge ({vol_ratio:.1f}x avg) + falling price (sell pressure)'
    elif vol_ratio < 0.5:
        score = 0.0
        reason = f'Very low volume ({vol_ratio:.1f}x avg) — lack of interest'
    else:
        score = 0.1 if price_change > 0 else -0.1
        reason = f'Normal volume ({vol_ratio:.1f}x avg)'

    return {'score': float(score), 'reason': reason}


# ════════════════════════════════════════════
# MAIN: Full Technical Analysis for a coin
# ════════════════════════════════════════════

def analyze_coin(conn: sqlite3.Connection, coin: str) -> dict:
    """Full technical analysis for one coin. Returns composite score + breakdown."""
    prices = get_prices(conn, coin, '1d', 365)
    if len(prices) < 30:
        return {
            'coin': coin,
            'score': 0.0,
            'confidence': 0,
            'signals': {},
            'error': f'Only {len(prices)} days of data (need 30+)',
        }

    closes = np.array([p[4] for p in prices])
    highs = np.array([p[2] for p in prices])
    lows = np.array([p[3] for p in prices])
    current_price = closes[-1]

    # Calculate all signals
    signals = {
        'ma_crossover': score_ma_crossover(closes),
        'rsi': score_rsi(closes),
        'bollinger': score_bollinger(closes),
        'macd': score_macd(closes),
        'volume': score_volume_trend(prices),
    }

    # Weighted average
    weights = {
        'ma_crossover': 0.30,
        'rsi': 0.25,
        'bollinger': 0.20,
        'macd': 0.15,
        'volume': 0.10,
    }

    total_score = sum(
        signals[k]['score'] * weights[k]
        for k in signals
    )

    # Trend filter: attenuate counter-trend signals
    # BUY below MA200 is unreliable; SELL above MA200 is unreliable
    sma200 = calc_sma(closes, 200)
    if len(closes) >= 200 and not np.isnan(sma200[-1]):
        price_above_ma200 = closes[-1] > sma200[-1]
        if total_score > 0 and not price_above_ma200:
            total_score *= 0.6  # BUY in downtrend → weaken by 40%
        elif total_score < 0 and price_above_ma200:
            total_score *= 0.6  # SELL in uptrend → weaken by 40%

    # Confidence based on signal agreement
    scores = [s['score'] for s in signals.values()]
    n_positive = sum(1 for s in scores if s > 0)
    n_negative = sum(1 for s in scores if s < 0)
    n_total = len(scores)
    agreement = max(n_positive, n_negative) / n_total if n_total > 0 else 0
    avg_mag = np.mean([abs(s) for s in scores])
    confidence = int(np.clip(avg_mag * 10 + agreement * 5, 1, 10))

    # ATR and S/R levels
    atr_pct = calc_atr(highs, lows, closes)
    sr_levels = calc_support_resistance(highs, lows, closes)

    # Bollinger data for price targets
    bb_middle, bb_upper, bb_lower = calc_bollinger(closes)
    bollinger_data = {
        'upper': float(bb_upper[-1]) if not np.isnan(bb_upper[-1]) else None,
        'lower': float(bb_lower[-1]) if not np.isnan(bb_lower[-1]) else None,
        'middle': float(bb_middle[-1]) if not np.isnan(bb_middle[-1]) else None,
    }

    # Price context
    change_24h = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) > 1 else 0
    change_7d = (closes[-1] - closes[-7]) / closes[-7] * 100 if len(closes) > 7 else 0
    change_30d = (closes[-1] - closes[-30]) / closes[-30] * 100 if len(closes) > 30 else 0

    return {
        'coin': coin,
        'price': float(current_price),
        'change_24h': float(change_24h),
        'change_7d': float(change_7d),
        'change_30d': float(change_30d),
        'score': round(float(total_score), 3),
        'confidence': confidence,
        'signals': signals,
        'atr_pct': atr_pct,
        'support_resistance': sr_levels,
        'bollinger': bollinger_data,
    }


def analyze_all(coins: list = None) -> dict:
    """Analyze all tracked coins."""
    from src.crypto.data_collector import TRACKED_COINS
    coins = coins or TRACKED_COINS

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    results = {}

    for coin in coins:
        result = analyze_coin(conn, coin)
        results[coin] = result

    conn.close()
    return results


if __name__ == '__main__':
    from src.crypto.data_collector import TRACKED_COINS

    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Technical Analysis")
    log.info("=" * 60)

    results = analyze_all()

    # Sort by score
    sorted_coins = sorted(results.items(), key=lambda x: x[1]['score'], reverse=True)

    print(f"\n{'Coin':<8} {'Price':>10} {'24h':>8} {'7d':>8} {'30d':>8} {'Score':>7} {'Signal':<15}")
    print("-" * 72)

    for coin, data in sorted_coins:
        if 'error' in data:
            print(f"{coin:<8} {'N/A':>10} {'':>8} {'':>8} {'':>8} {'N/A':>7} {data['error']}")
            continue

        score = data['score']
        if score > 0.5:
            signal = 'STRONG BUY'
        elif score > 0.2:
            signal = 'BUY'
        elif score > -0.2:
            signal = 'NEUTRAL'
        elif score > -0.5:
            signal = 'SELL'
        else:
            signal = 'STRONG SELL'

        print(f"{coin:<8} ${data['price']:>9,.1f} {data['change_24h']:>+7.1f}% {data['change_7d']:>+7.1f}% {data['change_30d']:>+7.1f}% {score:>+7.3f} {signal}")

    print()
