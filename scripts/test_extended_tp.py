#!/usr/bin/env python3 -u
"""Test extended TP values (uniform across all coins) to find diminishing returns."""
import sys; sys.stdout.reconfigure(line_buffering=True)
import sqlite3
from datetime import datetime
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
SL_FIXED = 10  # fixed SL

TP_GRID = [8, 10, 12, 14, 16, 18, 20, 25, 30]

START = int(datetime(2025, 5, 1).timestamp())
END   = int(datetime(2026, 4, 19).timestamp())
DAYS  = (END - START) / 86400

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


# ── Pre-compute signals (no SL-day filter, applied per TP in simulation) ──
print("Pre-computing signals...")
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

        # Future 12 x 15m candles (3h hold max)
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


# ── Simulate for each TP value ──
def simulate_all(tp_roi):
    """Simulate all coins with fixed SL and given TP. Returns aggregate stats."""
    total_trades = []
    tp_hits = 0
    sl_hits = 0
    time_exits = 0

    for coin in ALL_COINS:
        coin_sl_day = {}
        for (ts, direction, conf, entry, future) in signals[coin]:
            day_key = ts // 86400
            if coin_sl_day.get(day_key, 0) >= MAX_SL_PER_DAY:
                continue

            exit_roi = None
            cr = 0
            hit_type = 'TIME'
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

                if worst <= -SL_FIXED:
                    exit_roi = -SL_FIXED
                    hit_type = 'SL'
                    break
                if best >= tp_roi:
                    exit_roi = tp_roi
                    hit_type = 'TP'
                    break

            if exit_roi is None:
                exit_roi = cr
                hit_type = 'TIME'

            pnl = (exit_roi - FEE_ROI) / 100 * MARGIN
            total_trades.append(pnl)

            if hit_type == 'TP': tp_hits += 1
            elif hit_type == 'SL': sl_hits += 1
            else: time_exits += 1

            if exit_roi <= -(SL_FIXED - 0.5):
                coin_sl_day[day_key] = coin_sl_day.get(day_key, 0) + 1

    n = len(total_trades)
    if n == 0:
        return None

    wins = [t for t in total_trades if t > 0]
    total_pnl = sum(total_trades)

    return {
        'n': n,
        'wr': len(wins) / n * 100,
        'tp_pct': tp_hits / n * 100,
        'sl_pct': sl_hits / n * 100,
        'time_pct': time_exits / n * 100,
        'total_pnl': total_pnl,
        'per_day': total_pnl / DAYS,
        'avg_pnl': total_pnl / n,
    }


print(f"Extended TP test | SL={SL_FIXED}% fixed | LEV={LEV}x | MARGIN=${MARGIN:,}")
print(f"Period: 2025-05-01 to 2026-04-19 ({DAYS:.0f} days)")
print("=" * 110)
print(f"{'TP%':>4s} | {'Trades':>6s} | {'WR%':>5s} | {'TP_hit%':>7s} | {'SL_hit%':>7s} | {'TIME%':>6s} | {'Total PnL':>12s} | {'$/day':>8s} | {'Avg/trade':>10s}")
print("-" * 110)

results = {}
for tp in TP_GRID:
    r = simulate_all(tp)
    results[tp] = r
    print(f"  {tp:>2d} | {r['n']:>6d} | {r['wr']:>5.1f} | {r['tp_pct']:>7.1f} | {r['sl_pct']:>7.1f} | {r['time_pct']:>5.1f} | ${r['total_pnl']:>+11,.0f} | ${r['per_day']:>+7,.0f} | ${r['avg_pnl']:>+9.2f}")

print("=" * 110)

# ── Analysis: show marginal improvement ──
print("\nMarginal improvement (delta vs previous TP):")
print(f"{'TP%':>4s} | {'Total PnL':>12s} | {'Delta':>10s} | {'Delta/day':>10s} | {'TP_hit%':>7s} | Note")
print("-" * 75)

prev_pnl = None
best_tp = max(results.keys(), key=lambda k: results[k]['total_pnl'])

for tp in TP_GRID:
    r = results[tp]
    delta = r['total_pnl'] - prev_pnl if prev_pnl is not None else 0
    delta_day = delta / DAYS
    note = ""
    if tp == best_tp:
        note = "<-- BEST"
    elif prev_pnl is not None and delta < 0:
        note = "<-- WORSE"
    elif prev_pnl is not None and abs(delta_day) < 5:
        note = "<-- diminishing"

    if prev_pnl is not None:
        print(f"  {tp:>2d} | ${r['total_pnl']:>+11,.0f} | ${delta:>+9,.0f} | ${delta_day:>+8.1f}/d | {r['tp_pct']:>7.1f} | {note}")
    else:
        print(f"  {tp:>2d} | ${r['total_pnl']:>+11,.0f} | {'(base)':>10s} | {'':>10s} | {r['tp_pct']:>7.1f} |")

    prev_pnl = r['total_pnl']

print(f"\nBest TP = {best_tp}% (PnL ${results[best_tp]['total_pnl']:+,.0f}, ${results[best_tp]['per_day']:+,.0f}/day)")
print(f"TP hit rate at best: {results[best_tp]['tp_pct']:.1f}%")
