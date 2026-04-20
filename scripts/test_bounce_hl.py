#!/usr/bin/env python3
"""
Test TWO reversal detection approaches + their combination vs baseline.

Approach 0 (Baseline): Enter LONG immediately when cp < 0.45
Approach 1 (Bounce Speed): cp < 0.45 AND price rises >0.5% within next 2 candles (30min)
Approach 2 (Higher Low): cp < 0.45 AND current swing low > previous swing low on 1H
Approach 3 (Combination): Both bounce speed AND higher low must be true
"""

import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime, timezone

DB_PATH = "data/crypto/market.db"

COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOT", "LINK", "POL",
    "UNI", "PENDLE", "LDO", "CRV", "ARB", "OP", "APT", "FIL", "NEAR", "TON",
    "FET", "RENDER", "DOGE", "PEPE", "WIF"
]

# Date range
TS_START = int(datetime(2025, 5, 1, tzinfo=timezone.utc).timestamp())
TS_END = int(datetime(2026, 4, 19, 23, 59, 59, tzinfo=timezone.utc).timestamp())

# Trade params
LEVERAGE = 8
POSITION_PCT = 0.10
CAPITAL = 50_000
POSITION_SIZE = CAPITAL * POSITION_PCT  # $5000
NOTIONAL = POSITION_SIZE * LEVERAGE  # $40000

SL_ROI = -5.5  # %
TP_ROI = 6.5   # %
SL_PRICE_PCT = SL_ROI / LEVERAGE  # -0.6875%
TP_PRICE_PCT = TP_ROI / LEVERAGE  # +0.8125%

MAX_HOLD_CANDLES = 12  # 3 hours of 15m candles
CP_THRESHOLD = 0.45
LOOKBACK_15M = 16
BOUNCE_THRESHOLD = 0.005  # 0.5%
BOUNCE_CANDLES = 2


def load_data(conn, coin, timeframe, ts_start, ts_end):
    """Load candle data as numpy arrays, sorted by timestamp."""
    # Need some lookback before ts_start for cp calculation
    lookback_seconds = LOOKBACK_15M * 900 + 3600 * 24  # extra buffer
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe=? AND timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp",
        (coin, timeframe, ts_start - lookback_seconds, ts_end)
    )
    rows = cur.fetchall()
    if not rows:
        return None
    dtype = [('ts', 'i8'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'),
             ('close', 'f8'), ('volume', 'f8')]
    return np.array(rows, dtype=dtype)


def compute_close_positions(data_15m, hourly_timestamps):
    """For each hourly timestamp, compute cp using last 16 fifteen-minute candles."""
    ts_arr = data_15m['ts']
    results = {}

    for h_ts in hourly_timestamps:
        # Find candles with ts <= h_ts, take last 16
        mask = ts_arr <= h_ts
        indices = np.where(mask)[0]
        if len(indices) < LOOKBACK_15M:
            continue
        idx_end = indices[-1] + 1
        idx_start = idx_end - LOOKBACK_15M
        if idx_start < 0:
            continue

        chunk = data_15m[idx_start:idx_end]
        high_max = chunk['high'].max()
        low_min = chunk['low'].min()
        rng = high_max - low_min
        if rng < 1e-10:
            continue

        close_val = chunk['close'][-1]
        cp = (close_val - low_min) / rng
        results[h_ts] = (cp, close_val, idx_end - 1)  # cp, entry_price, index in data_15m

    return results


def find_swing_lows_1h(data_1h):
    """Find swing lows: candle whose low < both neighbors' lows. Returns dict ts -> low."""
    swing_lows = {}
    for i in range(1, len(data_1h) - 1):
        if data_1h['low'][i] < data_1h['low'][i - 1] and data_1h['low'][i] < data_1h['low'][i + 1]:
            swing_lows[data_1h['ts'][i]] = data_1h['low'][i]
    return swing_lows


def check_higher_low(swing_lows_list, current_ts):
    """Check if the most recent swing low before current_ts is higher than the one before it."""
    # Get swing lows before current_ts
    recent = [(ts, low) for ts, low in swing_lows_list if ts <= current_ts]
    if len(recent) < 2:
        return False
    # Last two swing lows
    recent.sort(key=lambda x: x[0])
    return recent[-1][1] > recent[-2][1]


def check_bounce_speed(data_15m, entry_idx, entry_price):
    """Check if price rises >0.5% within next 2 candles after entry."""
    for i in range(1, BOUNCE_CANDLES + 1):
        idx = entry_idx + i
        if idx >= len(data_15m):
            return False
        high = data_15m['high'][idx]
        if (high - entry_price) / entry_price >= BOUNCE_THRESHOLD:
            return True
    return False


def simulate_trade(data_15m, entry_idx, entry_price):
    """Simulate a trade from entry_idx, return PnL in dollars."""
    sl_price = entry_price * (1 + SL_PRICE_PCT / 100)
    tp_price = entry_price * (1 + TP_PRICE_PCT / 100)

    for i in range(1, MAX_HOLD_CANDLES + 1):
        idx = entry_idx + i
        if idx >= len(data_15m):
            # Use last available close
            last_idx = min(entry_idx + i - 1, len(data_15m) - 1)
            exit_price = data_15m['close'][last_idx]
            pnl_pct = (exit_price - entry_price) / entry_price
            return pnl_pct * NOTIONAL, "DATA_END"

        candle_low = data_15m['low'][idx]
        candle_high = data_15m['high'][idx]

        # Check SL first (conservative)
        if candle_low <= sl_price:
            pnl_pct = SL_PRICE_PCT / 100
            return pnl_pct * NOTIONAL, "SL"

        # Check TP
        if candle_high >= tp_price:
            pnl_pct = TP_PRICE_PCT / 100
            return pnl_pct * NOTIONAL, "TP"

    # Time exit
    exit_price = data_15m['close'][min(entry_idx + MAX_HOLD_CANDLES, len(data_15m) - 1)]
    pnl_pct = (exit_price - entry_price) / entry_price
    return pnl_pct * NOTIONAL, "TIME"


def run_test():
    conn = sqlite3.connect(DB_PATH)

    # Results: approach -> list of (coin, ts, pnl, exit_type)
    results = {0: [], 1: [], 2: [], 3: []}

    for coin in COINS:
        # Load data
        data_15m = load_data(conn, coin, '15m', TS_START, TS_END)
        data_1h = load_data(conn, coin, '1h', TS_START, TS_END)

        if data_15m is None or data_1h is None:
            print(f"  {coin}: no data, skipping")
            continue

        if len(data_15m) < LOOKBACK_15M + MAX_HOLD_CANDLES + 5:
            print(f"  {coin}: insufficient data ({len(data_15m)} rows), skipping")
            continue

        # Get hourly timestamps in range
        hourly_ts = sorted(set(
            ts for ts in data_15m['ts']
            if ts >= TS_START and ts <= TS_END and ts % 3600 == 0
        ))

        # Compute cp for each hourly timestamp
        cp_data = compute_close_positions(data_15m, hourly_ts)

        # Find swing lows on 1H data
        swing_lows_dict = find_swing_lows_1h(data_1h)
        swing_lows_list = sorted(swing_lows_dict.items(), key=lambda x: x[0])

        # Track last signal hour per coin for dedup
        signals_found = 0

        for h_ts in hourly_ts:
            if h_ts not in cp_data:
                continue

            cp, entry_price, entry_idx = cp_data[h_ts]

            if cp >= CP_THRESHOLD:
                continue

            # Need enough future candles for trade simulation + bounce check
            if entry_idx + MAX_HOLD_CANDLES + 2 >= len(data_15m):
                continue

            signals_found += 1

            # Approach 0: Baseline — always enter
            pnl, exit_type = simulate_trade(data_15m, entry_idx, entry_price)
            month_key = datetime.fromtimestamp(h_ts, tz=timezone.utc).strftime("%Y-%m")
            results[0].append((coin, h_ts, pnl, exit_type, month_key))

            # Check filters
            has_bounce = check_bounce_speed(data_15m, entry_idx, entry_price)
            has_hl = check_higher_low(swing_lows_list, h_ts)

            # Approach 1: Bounce Speed
            if has_bounce:
                # Entry after bounce confirmation = 2 candles later
                # But we enter at entry_price (the hourly close where cp < 0.45)
                # The bounce is just confirmation, entry is same price
                results[1].append((coin, h_ts, pnl, exit_type, month_key))

            # Approach 2: Higher Low
            if has_hl:
                results[2].append((coin, h_ts, pnl, exit_type, month_key))

            # Approach 3: Both
            if has_bounce and has_hl:
                results[3].append((coin, h_ts, pnl, exit_type, month_key))

        print(f"  {coin}: {len(cp_data)} hourly points, {signals_found} signals (cp<{CP_THRESHOLD})")

    conn.close()
    return results


def print_results(results):
    approach_names = {
        0: "Baseline (no filter)",
        1: "Bounce Speed (>0.5% in 30m)",
        2: "Higher Low (1H swing)",
        3: "Bounce + HL combined",
    }

    print("\n" + "=" * 100)
    print("REVERSAL DETECTION COMPARISON")
    print(f"Period: 2025-05-01 to 2026-04-19 | {len(COINS)} coins")
    print(f"Position: ${POSITION_SIZE} x {LEVERAGE}x = ${NOTIONAL} notional")
    print(f"SL: {SL_ROI}% ROI ({SL_PRICE_PCT:.4f}% price) | TP: {TP_ROI}% ROI ({TP_PRICE_PCT:.4f}% price)")
    print(f"Max hold: {MAX_HOLD_CANDLES} candles (3h) | cp threshold: {CP_THRESHOLD}")
    print("=" * 100)

    # Summary table
    print(f"\n{'Approach':<30} {'Trades':>7} {'Wins':>6} {'Losses':>7} {'Time':>6} "
          f"{'WinRate':>8} {'TotalPnL':>12} {'AvgPnL':>10} {'PF':>6} {'Filter%':>8}")
    print("-" * 110)

    best_approach = None
    best_pnl = -999999

    for a_id in [0, 1, 2, 3]:
        trades = results[a_id]
        n = len(trades)
        if n == 0:
            print(f"{approach_names[a_id]:<30} {'0':>7}")
            continue

        wins = sum(1 for t in trades if t[3] == "TP")
        losses = sum(1 for t in trades if t[3] == "SL")
        time_exits = sum(1 for t in trades if t[3] == "TIME")
        total_pnl = sum(t[2] for t in trades)
        avg_pnl = total_pnl / n
        win_rate = wins / n * 100

        gross_win = sum(t[2] for t in trades if t[2] > 0)
        gross_loss = abs(sum(t[2] for t in trades if t[2] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')

        baseline_n = len(results[0])
        filter_pct = (1 - n / baseline_n) * 100 if baseline_n > 0 and a_id > 0 else 0

        print(f"{approach_names[a_id]:<30} {n:>7} {wins:>6} {losses:>7} {time_exits:>6} "
              f"{win_rate:>7.1f}% {total_pnl:>11,.0f}$ {avg_pnl:>9,.1f}$ {pf:>5.2f} "
              f"{filter_pct:>7.1f}%")

        if total_pnl > best_pnl:
            best_pnl = total_pnl
            best_approach = a_id

    # Per-exit-type breakdown
    print(f"\n{'Approach':<30} {'SL avg$':>10} {'TP avg$':>10} {'TIME avg$':>10} {'TIME win%':>10}")
    print("-" * 80)
    for a_id in [0, 1, 2, 3]:
        trades = results[a_id]
        if not trades:
            continue
        sl_trades = [t[2] for t in trades if t[3] == "SL"]
        tp_trades = [t[2] for t in trades if t[3] == "TP"]
        time_trades = [t[2] for t in trades if t[3] == "TIME"]

        sl_avg = np.mean(sl_trades) if sl_trades else 0
        tp_avg = np.mean(tp_trades) if tp_trades else 0
        time_avg = np.mean(time_trades) if time_trades else 0
        time_win = sum(1 for t in time_trades if t > 0) / len(time_trades) * 100 if time_trades else 0

        print(f"{approach_names[a_id]:<30} {sl_avg:>9,.1f}$ {tp_avg:>9,.1f}$ {time_avg:>9,.1f}$ {time_win:>9.1f}%")

    # Monthly breakdown for best approach
    print(f"\n{'=' * 80}")
    print(f"MONTHLY BREAKDOWN — Best: {approach_names[best_approach]} (total PnL: ${best_pnl:,.0f})")
    print(f"{'=' * 80}")

    monthly = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
    for t in results[best_approach]:
        m = t[4]
        monthly[m]["trades"] += 1
        monthly[m]["pnl"] += t[2]
        if t[3] == "TP":
            monthly[m]["wins"] += 1
        elif t[3] == "SL":
            monthly[m]["losses"] += 1

    print(f"\n{'Month':<12} {'Trades':>7} {'Wins':>6} {'Losses':>7} {'WinRate':>8} {'PnL':>12} {'Avg':>10}")
    print("-" * 70)

    for m in sorted(monthly.keys()):
        d = monthly[m]
        wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
        avg = d["pnl"] / d["trades"] if d["trades"] > 0 else 0
        print(f"{m:<12} {d['trades']:>7} {d['wins']:>6} {d['losses']:>7} "
              f"{wr:>7.1f}% {d['pnl']:>11,.0f}$ {avg:>9,.1f}$")

    total_months = len(monthly)
    profitable_months = sum(1 for d in monthly.values() if d["pnl"] > 0)
    print(f"\nProfitable months: {profitable_months}/{total_months}")

    # Per-coin breakdown for best approach
    print(f"\n{'=' * 80}")
    print(f"PER-COIN BREAKDOWN — {approach_names[best_approach]}")
    print(f"{'=' * 80}")

    coin_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
    for t in results[best_approach]:
        coin_stats[t[0]]["trades"] += 1
        coin_stats[t[0]]["pnl"] += t[2]
        if t[3] == "TP":
            coin_stats[t[0]]["wins"] += 1

    print(f"\n{'Coin':<10} {'Trades':>7} {'WinRate':>8} {'PnL':>12} {'Avg':>10}")
    print("-" * 55)

    for coin in sorted(coin_stats.keys(), key=lambda c: coin_stats[c]["pnl"], reverse=True):
        d = coin_stats[coin]
        wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
        avg = d["pnl"] / d["trades"] if d["trades"] > 0 else 0
        print(f"{coin:<10} {d['trades']:>7} {wr:>7.1f}% {d['pnl']:>11,.0f}$ {avg:>9,.1f}$")


if __name__ == "__main__":
    print("Loading data and running reversal detection test...")
    results = run_test()
    print_results(results)
