"""
Level Finder — S/R Calculator from Price History
==================================================
Finds support and resistance levels for any coin.
Uses: swing highs/lows, price clusters, round numbers.
Pure math — no AI needed.
"""

import sqlite3
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'


def find_levels(coin: str, timeframe: str = '4h', lookback: int = 200) -> dict:
    """Find S/R levels for a coin.

    Returns:
        {
            'resistance': [list of resistance prices, strongest first],
            'support': [list of support prices, strongest first],
            'nearest_resistance': float,
            'nearest_support': float,
            'current_price': float,
            'atr': float (ATR as decimal, e.g. 0.025 = 2.5%)
        }
    """
    conn = sqlite3.connect(str(DB_PATH))

    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe=? ORDER BY timestamp DESC LIMIT ?",
        (coin, timeframe, lookback)
    ).fetchall()
    conn.close()

    if len(rows) < 30:
        return {'resistance': [], 'support': [], 'nearest_resistance': 0,
                'nearest_support': 0, 'current_price': 0, 'atr': 0.02}

    rows = rows[::-1]  # chronological
    highs = np.array([r[2] for r in rows])
    lows = np.array([r[3] for r in rows])
    closes = np.array([r[4] for r in rows])
    volumes = np.array([r[5] for r in rows])

    current_price = closes[-1]

    # ATR
    tr = np.maximum(highs[1:] - lows[1:],
                    np.maximum(abs(highs[1:] - closes[:-1]), abs(lows[1:] - closes[:-1])))
    atr = np.mean(tr[-14:]) / current_price if current_price > 0 else 0.02

    # Method 1: Swing highs and lows (local extremes)
    swing_levels = []
    window = 5
    for i in range(window, len(rows) - window):
        # Swing high: higher than neighbors
        if highs[i] == max(highs[i-window:i+window+1]):
            swing_levels.append(('R', highs[i], volumes[i]))
        # Swing low: lower than neighbors
        if lows[i] == min(lows[i-window:i+window+1]):
            swing_levels.append(('S', lows[i], volumes[i]))

    # Method 2: Price cluster analysis (where price spent most time)
    n_bins = 100
    price_range = max(highs) - min(lows)
    if price_range <= 0:
        price_range = current_price * 0.1
    bin_size = price_range / n_bins

    # Volume-weighted price histogram
    vol_profile = {}
    for i in range(len(rows)):
        bin_idx = int((closes[i] - min(lows)) / bin_size)
        bin_idx = min(bin_idx, n_bins - 1)
        price_level = min(lows) + bin_idx * bin_size + bin_size / 2
        vol_profile[price_level] = vol_profile.get(price_level, 0) + volumes[i]

    # Top volume levels = strongest S/R
    sorted_levels = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)

    # Method 3: Round numbers
    round_levels = []
    magnitude = 10 ** int(np.log10(current_price)) if current_price > 0 else 1
    step = magnitude / 10  # e.g., $100 for BTC, $10 for ETH, $0.1 for DOGE
    base = int(current_price / step) * step
    for mult in range(-5, 6):
        level = base + mult * step
        if level > 0:
            round_levels.append(level)

    # Combine all levels, score by strength
    level_scores = {}

    # Swing levels (strongest — price actually reversed here)
    for type_, price, vol in swing_levels:
        key = round(price, 6)
        if key not in level_scores:
            level_scores[key] = {'score': 0, 'type': type_}
        level_scores[key]['score'] += 3  # swing = high weight
        if vol > np.mean(volumes):
            level_scores[key]['score'] += 1  # high volume swing = extra

    # Volume cluster levels
    for price, vol in sorted_levels[:20]:
        key = round(price, 6)
        if key not in level_scores:
            # Determine if S or R based on current price
            type_ = 'R' if price > current_price else 'S'
            level_scores[key] = {'score': 0, 'type': type_}
        level_scores[key]['score'] += 2

    # Merge nearby levels (within 0.3% of each other)
    merged = {}
    for price in sorted(level_scores.keys()):
        found = False
        for existing in merged:
            if abs(price - existing) / existing < 0.003:
                merged[existing]['score'] += level_scores[price]['score']
                found = True
                break
        if not found:
            merged[price] = level_scores[price].copy()

    # Split into resistance and support
    resistance = []
    support = []
    for price, info in sorted(merged.items(), key=lambda x: x[1]['score'], reverse=True):
        if price > current_price * 1.001:
            resistance.append(price)
        elif price < current_price * 0.999:
            support.append(price)

    # Sort: nearest first
    resistance.sort()
    support.sort(reverse=True)

    nearest_r = resistance[0] if resistance else current_price * (1 + atr)
    nearest_s = support[0] if support else current_price * (1 - atr)

    return {
        'resistance': resistance[:5],
        'support': support[:5],
        'nearest_resistance': nearest_r,
        'nearest_support': nearest_s,
        'current_price': current_price,
        'atr': atr,
    }


def find_levels_live(coin: str, exchange, timeframe: str = '4h', lookback: int = 200) -> dict:
    """Find S/R levels from LIVE exchange data (for coins not in our DB).
    Uses ccxt fetch_ohlcv directly."""
    try:
        symbol = f"{coin}/USDT:USDT"
        ohlcv = exchange._exchange.fetch_ohlcv(symbol, timeframe, limit=lookback)
        if not ohlcv or len(ohlcv) < 30:
            return {'resistance': [], 'support': [], 'nearest_resistance': 0,
                    'nearest_support': 0, 'current_price': 0, 'atr': 0.02}

        highs = np.array([c[2] for c in ohlcv])
        lows = np.array([c[3] for c in ohlcv])
        closes = np.array([c[4] for c in ohlcv])
        volumes = np.array([c[5] for c in ohlcv])
        current_price = closes[-1]

        # ATR
        tr = np.maximum(highs[1:] - lows[1:],
                        np.maximum(abs(highs[1:] - closes[:-1]), abs(lows[1:] - closes[:-1])))
        atr = np.mean(tr[-14:]) / current_price if current_price > 0 else 0.02

        # Swing highs/lows
        swing_levels = []
        window = 5
        for i in range(window, len(ohlcv) - window):
            if highs[i] == max(highs[i-window:i+window+1]):
                swing_levels.append(('R', highs[i], volumes[i]))
            if lows[i] == min(lows[i-window:i+window+1]):
                swing_levels.append(('S', lows[i], volumes[i]))

        # Volume clusters
        n_bins = 100
        price_range = max(highs) - min(lows)
        if price_range <= 0:
            price_range = current_price * 0.1
        bin_size = price_range / n_bins
        vol_profile = {}
        for i in range(len(ohlcv)):
            bin_idx = min(int((closes[i] - min(lows)) / bin_size), n_bins - 1)
            price_level = min(lows) + bin_idx * bin_size + bin_size / 2
            vol_profile[price_level] = vol_profile.get(price_level, 0) + volumes[i]
        sorted_levels = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)

        # Combine + score
        level_scores = {}
        for type_, price, vol in swing_levels:
            key = round(price, 6)
            if key not in level_scores:
                level_scores[key] = {'score': 0, 'type': type_}
            level_scores[key]['score'] += 3
            if vol > np.mean(volumes):
                level_scores[key]['score'] += 1
        for price, vol in sorted_levels[:20]:
            key = round(price, 6)
            if key not in level_scores:
                type_ = 'R' if price > current_price else 'S'
                level_scores[key] = {'score': 0, 'type': type_}
            level_scores[key]['score'] += 2

        # Merge nearby
        merged = {}
        for price in sorted(level_scores.keys()):
            found = False
            for existing in merged:
                if abs(price - existing) / existing < 0.003:
                    merged[existing]['score'] += level_scores[price]['score']
                    found = True
                    break
            if not found:
                merged[price] = level_scores[price].copy()

        resistance = []
        support = []
        for price, info in sorted(merged.items(), key=lambda x: x[1]['score'], reverse=True):
            if price > current_price * 1.001:
                resistance.append(price)
            elif price < current_price * 0.999:
                support.append(price)

        resistance.sort()
        support.sort(reverse=True)
        nearest_r = resistance[0] if resistance else current_price * (1 + atr)
        nearest_s = support[0] if support else current_price * (1 - atr)

        return {
            'resistance': resistance[:5],
            'support': support[:5],
            'nearest_resistance': nearest_r,
            'nearest_support': nearest_s,
            'current_price': current_price,
            'atr': atr,
        }
    except Exception as e:
        logger.debug(f"Live levels {coin}: {e}")
        return {'resistance': [], 'support': [], 'nearest_resistance': 0,
                'nearest_support': 0, 'current_price': 0, 'atr': 0.02}


def format_levels(coin: str, levels: dict) -> str:
    """Format levels as text for Opus prompt."""
    r_str = ', '.join(f'${p:.4f}' for p in levels['resistance'][:3])
    s_str = ', '.join(f'${p:.4f}' for p in levels['support'][:3])
    return (f"{coin}: price=${levels['current_price']:.4f} | "
            f"ATR={levels['atr']*100:.1f}% | "
            f"R=[{r_str}] | S=[{s_str}]")


if __name__ == '__main__':
    for coin in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'LINK', 'DOGE']:
        levels = find_levels(coin)
        print(format_levels(coin, levels))
