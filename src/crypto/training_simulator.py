"""
FORTIX Training Simulator
===========================
Prepares historical data for Opus training.
For each hour: calculates S/R levels, BTC momentum, ATR.
Then checks what happened (did TP/SL hit?) for validation.

Usage:
    python src/crypto/training_simulator.py --start 2026-02-10 --end 2026-04-13
"""

import sqlite3
import json
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta

DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'market.db'
OUTPUT_DIR = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'training'


def get_1h_candles(conn, coin, before_ts, limit=48):
    """Get 1h candles before a timestamp (what Profi would see)."""
    rows = conn.execute("""
        SELECT timestamp, open, high, low, close, volume FROM prices
        WHERE coin=? AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT ?
    """, (coin, before_ts, limit)).fetchall()
    return list(reversed(rows))


def calc_sr_levels(candles):
    """Calculate S/R levels from candles (same logic as level_finder)."""
    if len(candles) < 20:
        return {'support': [], 'resistance': [], 'atr': 0.02}

    highs = np.array([c[2] for c in candles])
    lows = np.array([c[3] for c in candles])
    closes = np.array([c[4] for c in candles])
    current = closes[-1]

    # ATR
    tr = np.maximum(highs[1:] - lows[1:],
                    np.maximum(abs(highs[1:] - closes[:-1]), abs(lows[1:] - closes[:-1])))
    atr = float(np.mean(tr[-14:])) / current if current > 0 else 0.02

    # Swing highs/lows
    levels = {}
    window = 3
    for i in range(window, len(candles) - window):
        if highs[i] == max(highs[i-window:i+window+1]):
            p = round(float(highs[i]), 6)
            levels[p] = levels.get(p, 0) + 2
        if lows[i] == min(lows[i-window:i+window+1]):
            p = round(float(lows[i]), 6)
            levels[p] = levels.get(p, 0) + 2

    resistance = sorted([p for p in levels if p > current * 1.001], key=lambda x: levels[x], reverse=True)[:3]
    support = sorted([p for p in levels if p < current * 0.999], key=lambda x: levels[x], reverse=True)[:3]
    resistance.sort()
    support.sort(reverse=True)

    return {
        'support': [float(s) for s in support],
        'resistance': [float(r) for r in resistance],
        'current_price': float(current),
        'atr': float(atr),
    }


def check_outcome(conn, coin, entry_price, direction, sl_price, tp_price, entry_ts, max_hours=2):
    """Check if TP or SL hit within max_hours after entry."""
    end_ts = entry_ts + max_hours * 3600
    candles = conn.execute("""
        SELECT high, low, close FROM prices
        WHERE coin=? AND timeframe='1h' AND timestamp > ? AND timestamp <= ?
        ORDER BY timestamp
    """, (coin, entry_ts, end_ts)).fetchall()

    for c in candles:
        high, low, close = c
        if direction == 'LONG':
            if low <= sl_price:
                return 'SL', float((sl_price - entry_price) / entry_price * 100)
            if high >= tp_price:
                return 'TP', float((tp_price - entry_price) / entry_price * 100)
        else:
            if high >= sl_price:
                return 'SL', float((sl_price - entry_price) / entry_price * -100)
            if low <= tp_price:
                return 'TP', float((entry_price - tp_price) / entry_price * 100)

    # Neither hit — check last close
    if candles:
        last_close = candles[-1][2]
        if direction == 'LONG':
            pnl = (last_close - entry_price) / entry_price * 100
        else:
            pnl = (entry_price - last_close) / entry_price * 100
        return 'TIMEOUT', float(pnl)
    return 'NO_DATA', 0.0


def prepare_hour(conn, ts, coins):
    """Prepare all data for one hourly scan."""
    hour_data = {
        'timestamp': ts,
        'datetime': datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
    }

    # BTC momentum
    btc_candles = get_1h_candles(conn, 'BTC', ts, 4)
    if len(btc_candles) >= 2:
        btc_now = btc_candles[-1][4]
        btc_prev = btc_candles[-2][4]
        hour_data['btc_price'] = float(btc_now)
        hour_data['btc_momentum_1h'] = float((btc_now - btc_prev) / btc_prev * 100)
    else:
        hour_data['btc_price'] = 0
        hour_data['btc_momentum_1h'] = 0

    # Per coin data
    hour_data['coins'] = {}
    for coin in coins:
        candles = get_1h_candles(conn, coin, ts, 48)
        if len(candles) < 20:
            continue

        levels = calc_sr_levels(candles)
        price = levels['current_price']

        # Funding rate (closest before ts)
        fr = conn.execute("""
            SELECT rate FROM funding_rates WHERE coin=? AND timestamp <= ?
            ORDER BY timestamp DESC LIMIT 1
        """, (coin, ts)).fetchone()

        hour_data['coins'][coin] = {
            'price': price,
            'atr_1h': levels['atr'],
            'support': levels['support'],
            'resistance': levels['resistance'],
            'funding': float(fr[0]) if fr else 0,
            # Last 4 candles for momentum
            'last_4_candles': [
                {'o': float(c[1]), 'h': float(c[2]), 'l': float(c[3]), 'c': float(c[4])}
                for c in candles[-4:]
            ],
        }

    return hour_data


def run_simulation(start_date, end_date, coins=None):
    """Prepare training data for date range."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if coins is None:
        coins = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'LINK',
                 'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
                 'ARB', 'OP', 'WIF', 'PENDLE']

    conn = sqlite3.connect(str(DB_PATH))

    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())

    # Process day by day
    current = start_ts
    day_count = 0

    while current < end_ts:
        day_str = datetime.fromtimestamp(current, tz=timezone.utc).strftime('%Y-%m-%d')
        day_file = OUTPUT_DIR / f'{day_str}.json'

        if day_file.exists():
            current += 86400
            day_count += 1
            continue

        print(f"Preparing {day_str}...")
        day_data = {'date': day_str, 'hours': []}

        for hour in range(24):
            ts = current + hour * 3600
            hour_data = prepare_hour(conn, ts, coins)
            if hour_data.get('coins'):
                day_data['hours'].append(hour_data)

        # Save
        day_file.write_text(json.dumps(day_data, indent=2))
        day_count += 1
        current += 86400

    conn.close()
    print(f"Prepared {day_count} days in {OUTPUT_DIR}")
    return day_count


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2026-02-10')
    parser.add_argument('--end', default='2026-04-14')
    args = parser.parse_args()

    run_simulation(args.start, args.end)
