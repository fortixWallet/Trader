#!/usr/bin/env python3 -u
"""Grid search: find optimal SL/TP per coin individually."""
import sys; sys.stdout.reconfigure(line_buffering=True)
import sqlite3, json
from datetime import datetime, timedelta
from collections import defaultdict

DB = 'data/crypto/market.db'
conn = sqlite3.connect(DB)

ALL_COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','POL',
             'UNI','PENDLE','LDO','CRV','ARB','OP','APT','FIL','NEAR','TON',
             'FET','RENDER','DOGE','PEPE','WIF']

LEV = 8
FEE_ROI = 0.88  # round-trip fee as ROI%
MARGIN = 3466
CP_TOP, CP_BOT = 0.55, 0.45
OI_THRESH = 0.5
MIN_CONF = 0.75
DEDUP = 3600  # 1h
MAX_SL_PER_DAY = 3

SL_GRID = [4, 5, 6, 7, 8, 10, 12]
TP_GRID = [4, 5, 6, 7, 8, 10, 12]

START = int(datetime(2025, 5, 1).timestamp())
END   = int(datetime(2026, 4, 19).timestamp())

# ── Load data ──
print("Loading data into memory...")

price_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, high, low, close FROM prices WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)).fetchall()
    price_cache[coin] = {r[0]: (r[1], r[2], r[3]) for r in rows}
print(f"  15m prices: {sum(len(v) for v in price_cache.values()):,} records")

oi_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, c FROM pred_oi_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    oi_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
print(f"  OI: {sum(len(v) for v in oi_cache.values()):,} records")

tk_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, ratio FROM pred_taker_volume WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    tk_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
print(f"  Taker: {sum(len(v) for v in tk_cache.values()):,} records")

liq_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, long_liq_usd, short_liq_usd FROM pred_liq_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    liq_cache[coin] = [(r[0], float(r[1]), float(r[2])) for r in rows]
print(f"  Liq: {sum(len(v) for v in liq_cache.values()):,} records")

cvd_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, cvd FROM pred_cvd_futures WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    cvd_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
print(f"  CVD: {sum(len(v) for v in cvd_cache.values()):,} records")

conn.close()
print("Data loaded!\n")


def find_nearest(data_list, ts, max_gap=7200):
    if not data_list: return None
    lo, hi = 0, len(data_list) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if data_list[mid][0] < ts: lo = mid + 1
        else: hi = mid
    best = lo
    if best > 0 and abs(data_list[best-1][0] - ts) < abs(data_list[best][0] - ts):
        best = best - 1
    if abs(data_list[best][0] - ts) > max_gap: return None
    return data_list[best]


# ── Step 1: Pre-compute signals per coin ──
print("Pre-computing signals...")
# signals[coin] = [(ts, direction, conf, entry_price), ...]
signals = defaultdict(list)

for coin in ALL_COINS:
    pc = price_cache.get(coin, {})
    oi_data = oi_cache.get(coin, [])
    tk_data = tk_cache.get(coin, [])
    liq_data = liq_cache.get(coin, [])
    cvd_data = cvd_cache.get(coin, [])

    last_sig_ts = 0
    coin_sl_day = {}  # day_key -> count

    ts = START
    while ts <= END:
        # Dedup
        if ts - last_sig_ts < DEDUP:
            ts += 3600
            continue

        # Max SL per day check
        day_key = ts // 86400
        if coin_sl_day.get(day_key, 0) >= MAX_SL_PER_DAY:
            ts += 3600
            continue

        # Get 16 fifteen-minute candles
        candles = []
        for i in range(16):
            t = ts - i * 900
            if t in pc:
                candles.append(pc[t])
        if len(candles) < 12:
            ts += 3600
            continue

        p = candles[0][2]  # close of latest
        if p == 0:
            ts += 3600
            continue

        highs = [c[0] for c in candles]
        lows  = [c[1] for c in candles]
        rng_h, rng_l = max(highs), min(lows)
        cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

        # OI
        oi_now_r = find_nearest(oi_data, ts)
        oi_4h_r  = find_nearest(oi_data, ts - 14400)
        if not oi_now_r or not oi_4h_r or oi_4h_r[1] == 0:
            ts += 3600
            continue
        oi = (oi_now_r[1] / oi_4h_r[1] - 1) * 100

        # Taker
        tk_r = find_nearest(tk_data, ts)
        tk = tk_r[1] if tk_r else 1.0

        # Liq
        liq_r = find_nearest(liq_data, ts)
        liq = None
        if liq_r and (liq_r[1] + liq_r[2]) > 0:
            liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

        # CVD
        cvd_now_r = find_nearest(cvd_data, ts)
        cvd_4h_r  = find_nearest(cvd_data, ts - 14400)
        cvd_val = None
        if cvd_now_r and cvd_4h_r:
            cvd_val = (cvd_now_r[1] - cvd_4h_r[1]) / 1e6

        # Score
        direction = None; ss = 0; ls = 0
        if cp > CP_TOP and oi < -OI_THRESH:
            if liq is not None and liq > 0.3: ss += 5
            if tk < 0.9: ss += 5
            if cvd_val is not None and cvd_val < 0: ss += 3
            ss += 1
        if cp < CP_BOT and oi > OI_THRESH:
            if liq is not None and liq < -0.3: ls += 5
            if tk > 1.1: ls += 5
            if cvd_val is not None and cvd_val > 0: ls += 3
            ls += 1

        conf = 0
        if ss >= 5: direction = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
        elif ss >= 3: direction = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
        elif ls >= 5: direction = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
        elif ls >= 3: direction = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)

        if not direction or conf < MIN_CONF:
            ts += 3600
            continue

        last_sig_ts = ts

        # Pre-compute future candle data for simulation (next 12 x 15m)
        future = []
        for i in range(1, 13):
            ft = ts + i * 900
            if ft in pc:
                future.append(pc[ft])
            else:
                future.append(None)

        signals[coin].append((ts, direction, conf, p, future))
        ts += 3600

    print(f"  {coin}: {len(signals[coin])} signals")

total_sigs = sum(len(v) for v in signals.values())
print(f"Total signals: {total_sigs}\n")

# Note: MAX_SL dedup is applied during signal generation above,
# but for grid search we need to re-apply per SL value since different SL
# values produce different SL counts. For simplicity and matching the original
# backtest, we applied it at signal generation with a generous approach.
# The SL-per-day filter at signal generation stage doesn't know which trades
# will SL (depends on SL param). We'll handle it properly in simulation.

# Actually, let's fix this: remove the SL-per-day filter from signal generation
# (it depends on SL param) and apply it per-combo in simulation.
# Re-generate signals without the SL filter.

print("Re-computing signals (without SL-day filter, applied per combo)...")
signals = defaultdict(list)

for coin in ALL_COINS:
    pc = price_cache.get(coin, {})
    oi_data = oi_cache.get(coin, [])
    tk_data = tk_cache.get(coin, [])
    liq_data = liq_cache.get(coin, [])
    cvd_data = cvd_cache.get(coin, [])

    last_sig_ts = 0

    ts = START
    while ts <= END:
        if ts - last_sig_ts < DEDUP:
            ts += 3600
            continue

        candles = []
        for i in range(16):
            t = ts - i * 900
            if t in pc:
                candles.append(pc[t])
        if len(candles) < 12:
            ts += 3600
            continue

        p = candles[0][2]
        if p == 0:
            ts += 3600
            continue

        highs = [c[0] for c in candles]
        lows  = [c[1] for c in candles]
        rng_h, rng_l = max(highs), min(lows)
        cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

        oi_now_r = find_nearest(oi_data, ts)
        oi_4h_r  = find_nearest(oi_data, ts - 14400)
        if not oi_now_r or not oi_4h_r or oi_4h_r[1] == 0:
            ts += 3600
            continue
        oi = (oi_now_r[1] / oi_4h_r[1] - 1) * 100

        tk_r = find_nearest(tk_data, ts)
        tk = tk_r[1] if tk_r else 1.0

        liq_r = find_nearest(liq_data, ts)
        liq = None
        if liq_r and (liq_r[1] + liq_r[2]) > 0:
            liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

        cvd_now_r = find_nearest(cvd_data, ts)
        cvd_4h_r  = find_nearest(cvd_data, ts - 14400)
        cvd_val = None
        if cvd_now_r and cvd_4h_r:
            cvd_val = (cvd_now_r[1] - cvd_4h_r[1]) / 1e6

        direction = None; ss = 0; ls = 0
        if cp > CP_TOP and oi < -OI_THRESH:
            if liq is not None and liq > 0.3: ss += 5
            if tk < 0.9: ss += 5
            if cvd_val is not None and cvd_val < 0: ss += 3
            ss += 1
        if cp < CP_BOT and oi > OI_THRESH:
            if liq is not None and liq < -0.3: ls += 5
            if tk > 1.1: ls += 5
            if cvd_val is not None and cvd_val > 0: ls += 3
            ls += 1

        conf = 0
        if ss >= 5: direction = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
        elif ss >= 3: direction = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
        elif ls >= 5: direction = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
        elif ls >= 3: direction = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)

        if not direction or conf < MIN_CONF:
            ts += 3600
            continue

        last_sig_ts = ts

        future = []
        for i in range(1, 13):
            ft = ts + i * 900
            if ft in pc:
                future.append(pc[ft])
            else:
                future.append(None)

        signals[coin].append((ts, direction, conf, p, future))
        ts += 3600

    print(f"  {coin}: {len(signals[coin])} signals")

total_sigs = sum(len(v) for v in signals.values())
print(f"Total signals: {total_sigs}\n")


# ── Step 2: Simulate per coin per SL/TP combo ──
def simulate(coin_signals, sl_roi, tp_roi):
    """Simulate trades for a list of signals with given SL/TP. Returns stats dict."""
    trades = []
    coin_sl_day = {}  # day_key -> SL count

    for (ts, direction, conf, entry, future) in coin_signals:
        day_key = ts // 86400
        if coin_sl_day.get(day_key, 0) >= MAX_SL_PER_DAY:
            continue

        exit_roi = None
        cr = 0
        for candle in future:
            if candle is None:
                continue
            fh, fl, fc = candle
            if direction == 'LONG':
                best  = (fh / entry - 1) * 100 * LEV
                worst = (fl / entry - 1) * 100 * LEV
                cr    = (fc / entry - 1) * 100 * LEV
            else:
                best  = (entry / fl - 1) * 100 * LEV
                worst = (entry / fh - 1) * 100 * LEV
                cr    = (entry / fc - 1) * 100 * LEV

            if worst <= -sl_roi:
                exit_roi = -sl_roi
                break
            if best >= tp_roi:
                exit_roi = tp_roi
                break

        if exit_roi is None:
            exit_roi = cr

        # Net PnL after fees
        pnl = (exit_roi - FEE_ROI) / 100 * MARGIN
        trades.append(pnl)

        # Track SL for daily limit
        if exit_roi <= -(sl_roi - 0.5):
            coin_sl_day[day_key] = coin_sl_day.get(day_key, 0) + 1

    if not trades:
        return {'pnl': 0, 'wr': 0, 'n': 0, 'pf': 0}

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    gross_win = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0.001
    return {
        'pnl': sum(trades),
        'wr': len(wins) / len(trades) * 100,
        'n': len(trades),
        'pf': gross_win / gross_loss,
    }


print("Running grid search (49 combos x 25 coins = 1225 simulations)...")
print("=" * 80)

# Results: results[coin][(sl, tp)] = stats
results = {}
for coin in ALL_COINS:
    results[coin] = {}
    for sl in SL_GRID:
        for tp in TP_GRID:
            results[coin][(sl, tp)] = simulate(signals[coin], sl, tp)

# ── Step 3: Find optimal per coin ──
UNIFORM_SL, UNIFORM_TP = 10, 8

optimal = {}
print(f"\n{'Coin':>6s} | {'opt_SL':>6s} {'opt_TP':>6s} | {'PnL':>10s} {'WR':>5s} {'Trades':>6s} {'PF':>6s} | {'Uni PnL':>10s} {'Uni WR':>5s} | {'Delta':>10s}")
print("-" * 95)

total_opt_pnl = 0
total_uni_pnl = 0
json_out = {}

for coin in ALL_COINS:
    # Find best by PnL
    best_combo = max(results[coin].keys(), key=lambda k: results[coin][k]['pnl'])
    best = results[coin][best_combo]

    # Uniform
    uni = results[coin][(UNIFORM_SL, UNIFORM_TP)]

    optimal[coin] = best_combo
    total_opt_pnl += best['pnl']
    total_uni_pnl += uni['pnl']
    delta = best['pnl'] - uni['pnl']

    json_out[coin] = {"sl": best_combo[0], "tp": best_combo[1]}

    print(f"{coin:>6s} | SL{best_combo[0]:>3d}% TP{best_combo[1]:>3d}% | ${best['pnl']:>+9,.0f} {best['wr']:>4.0f}% {best['n']:>6d} {best['pf']:>5.2f} | ${uni['pnl']:>+9,.0f} {uni['wr']:>4.0f}% | ${delta:>+9,.0f}")

print("-" * 95)
print(f"{'TOTAL':>6s} | {'per-coin':>13s} | ${total_opt_pnl:>+9,.0f} {'':>5s} {'':>6s} {'':>6s} | ${total_uni_pnl:>+9,.0f} {'':>5s} | ${total_opt_pnl - total_uni_pnl:>+9,.0f}")
print(f"\n  Per-coin optimal total: ${total_opt_pnl:+,.0f}")
print(f"  Uniform SL{UNIFORM_SL}/TP{UNIFORM_TP} total: ${total_uni_pnl:+,.0f}")
print(f"  Delta: ${total_opt_pnl - total_uni_pnl:+,.0f}")

# ── Step 4: JSON output ──
print(f"\n{'='*80}")
print("JSON config (per-coin optimal SL/TP):")
print(json.dumps(json_out, indent=2))

# Save to file
out_path = 'data/crypto/coin_optimization/grid_search_optimal_sl_tp.json'
import os
os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, 'w') as f:
    json.dump(json_out, f, indent=2)
print(f"\nSaved to {out_path}")

# ── Bonus: show top-3 combos per coin ──
print(f"\n{'='*80}")
print("Top-3 SL/TP combos per coin:")
print(f"{'Coin':>6s} | {'#1':>16s} | {'#2':>16s} | {'#3':>16s}")
print("-" * 65)
for coin in ALL_COINS:
    sorted_combos = sorted(results[coin].keys(), key=lambda k: results[coin][k]['pnl'], reverse=True)
    top3 = []
    for k in sorted_combos[:3]:
        r = results[coin][k]
        top3.append(f"SL{k[0]:>2d}/TP{k[1]:>2d} ${r['pnl']:>+6,.0f}")
    print(f"{coin:>6s} | {' | '.join(top3)}")
