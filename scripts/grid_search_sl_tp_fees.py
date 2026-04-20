#!/usr/bin/env python3 -u
"""Grid search for optimal SL/TP with Bybit trading fees."""
import sys; sys.stdout.reconfigure(line_buffering=True)
import sqlite3, numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import time as _time

# ── Config ──────────────────────────────────────────────────────────
ALL_COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','POL',
             'UNI','PENDLE','LDO','CRV','ARB','OP','APT','FIL','NEAR','TON',
             'FET','RENDER','DOGE','PEPE','WIF']

CP_TOP, CP_BOT, OI_THRESH = 0.60, 0.40, 0.3
MIN_CONF, DEDUP, MAX_SL_PER_DAY = 0.75, 3600, 3
LEV = 8
MARGIN = 3466  # current live position size
FEE_PER_SIDE = 0.055  # % of notional
FEE_ROI_ROUND_TRIP = FEE_PER_SIDE * 2 * LEV  # 0.055 * 2 * 8 = 0.88% ROI

SL_GRID = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 10.0]
TP_GRID = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 10.0, 12.0, 15.0]

START = datetime(2025, 5, 1)
END = datetime(2026, 4, 19)
start_ts = int(START.timestamp())
end_ts = int(END.timestamp())

# ── Load data ───────────────────────────────────────────────────────
t0 = _time.time()
conn = sqlite3.connect('data/crypto/market.db')
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

tk_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, ratio FROM pred_taker_volume WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    tk_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]

liq_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, long_liq_usd, short_liq_usd FROM pred_liq_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    liq_cache[coin] = [(r[0], float(r[1]), float(r[2])) for r in rows]

cvd_cache = {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, cvd FROM pred_cvd_futures WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    cvd_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]

conn.close()
print(f"Data loaded in {_time.time()-t0:.1f}s\n")

# ── Helpers ─────────────────────────────────────────────────────────
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

# ── Phase 1: Generate all signals ──────────────────────────────────
print("Generating signals...")
t1 = _time.time()

# Signal = (coin, ts, direction, confidence)
# Also precompute the 12-candle price path for each signal (for fast SL/TP sim)
# path[i] = (best_roi, worst_roi, close_roi) for candle i (1..12)

signals = []  # list of (coin, ts, direction, conf, day_key, path)

current = START
while current <= END:
    day_start = int(current.timestamp())
    day_key = current.strftime('%Y-%m-%d')
    last_sig = {}
    coin_sl_count = defaultdict(int)

    for hour in range(24):
        ts = day_start + hour * 3600
        if ts > end_ts:
            break
        for coin in ALL_COINS:
            if coin in last_sig and ts - last_sig[coin] < DEDUP:
                continue
            # We can't enforce MAX_SL_PER_DAY at signal generation time because
            # SL depends on the SL_ROI parameter. We'll handle it in simulation.
            # But we DO need dedup.

            # Get 15m candles
            candles = []
            for offset in range(16):
                t = ts - offset * 900
                if t in price_cache.get(coin, {}):
                    candles.append(price_cache[coin][t])
            if len(candles) < 12:
                continue
            p = candles[0][2]  # latest close
            if p == 0:
                continue

            highs = [c[0] for c in candles]
            lows = [c[1] for c in candles]
            rng_h, rng_l = max(highs), min(lows)
            cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

            # OI
            oi_now_r = find_nearest(oi_cache.get(coin, []), ts)
            oi_4h_r = find_nearest(oi_cache.get(coin, []), ts - 14400)
            if not oi_now_r or not oi_4h_r or oi_4h_r[1] == 0:
                continue
            oi = (oi_now_r[1] / oi_4h_r[1] - 1) * 100

            # Taker
            tk_r = find_nearest(tk_cache.get(coin, []), ts)
            tk = tk_r[1] if tk_r else 1.0

            # Liq
            liq_r = find_nearest(liq_cache.get(coin, []), ts)
            liq = None
            if liq_r and (liq_r[1] + liq_r[2]) > 0:
                liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

            # CVD
            cvd_now_r = find_nearest(cvd_cache.get(coin, []), ts)
            cvd_4h_r = find_nearest(cvd_cache.get(coin, []), ts - 14400)
            cvd = None
            if cvd_now_r and cvd_4h_r:
                cvd = (cvd_now_r[1] - cvd_4h_r[1]) / 1e6

            # Signal logic
            ss = 0; ls = 0
            at_top = cp > CP_TOP; at_bot = cp < CP_BOT

            if at_top and oi < -OI_THRESH:
                if liq is not None and liq > 0.3: ss += 5
                if tk < 0.9: ss += 5
                if cvd is not None and cvd < 0: ss += 3
                ss += 1
            if at_bot and oi > OI_THRESH:
                if liq is not None and liq < -0.3: ls += 5
                if tk > 1.1: ls += 5
                if cvd is not None and cvd > 0: ls += 3
                ls += 1

            direction = None; conf = 0
            if ss >= 5: direction = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
            elif ss >= 3: direction = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
            elif ls >= 5: direction = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
            elif ls >= 3: direction = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)

            if not direction or conf < MIN_CONF:
                continue
            last_sig[coin] = ts

            # Precompute price path: for each of 12 future candles, compute ROI extremes
            entry = p
            path = []
            for foffset in range(1, 13):
                ft = ts + foffset * 900
                if ft not in price_cache.get(coin, {}):
                    path.append(None)
                    continue
                fh, fl, fc = price_cache[coin][ft]
                if direction == 'LONG':
                    best_roi = (fh/entry - 1) * 100 * LEV
                    worst_roi = (fl/entry - 1) * 100 * LEV
                    close_roi = (fc/entry - 1) * 100 * LEV
                else:
                    best_roi = (entry/fl - 1) * 100 * LEV
                    worst_roi = (entry/fh - 1) * 100 * LEV
                    close_roi = (entry/fc - 1) * 100 * LEV
                path.append((best_roi, worst_roi, close_roi))

            signals.append((coin, ts, direction, conf, day_key, path))

    current += timedelta(days=1)

print(f"Generated {len(signals):,} signals in {_time.time()-t1:.1f}s")
print(f"  LONG: {sum(1 for s in signals if s[2]=='LONG'):,}  SHORT: {sum(1 for s in signals if s[2]=='SHORT'):,}\n")

# ── Phase 2: Grid search ───────────────────────────────────────────
print(f"Running grid search: {len(SL_GRID)} x {len(TP_GRID)} = {len(SL_GRID)*len(TP_GRID)} combinations...")
print(f"Fee: {FEE_ROI_ROUND_TRIP:.2f}% ROI round trip (taker {FEE_PER_SIDE}% x 2 x {LEV}x lev)")
print()

results = []
t2 = _time.time()

for sl_roi in SL_GRID:
    for tp_roi in TP_GRID:
        trades = []
        daily_pnl = defaultdict(float)
        coin_sl_per_day = defaultdict(lambda: defaultdict(int))
        wins = 0; losses = 0; time_exits = 0

        for coin, ts, direction, conf, day_key, path in signals:
            # Max SL per coin per day check
            if coin_sl_per_day[day_key][coin] >= MAX_SL_PER_DAY:
                continue

            # Simulate trade
            exit_roi = None
            last_close_roi = 0
            for candle in path:
                if candle is None:
                    continue
                best_roi, worst_roi, close_roi = candle
                last_close_roi = close_roi
                if worst_roi <= -sl_roi:
                    exit_roi = -sl_roi
                    break
                if best_roi >= tp_roi:
                    exit_roi = tp_roi
                    break

            if exit_roi is None:
                exit_roi = last_close_roi

            # Apply fees
            net_roi = exit_roi - FEE_ROI_ROUND_TRIP
            pnl = net_roi / 100 * MARGIN

            trades.append(pnl)
            daily_pnl[day_key] += pnl

            if exit_roi == tp_roi:
                wins += 1
            elif exit_roi == -sl_roi:
                losses += 1
                coin_sl_per_day[day_key][coin] += 1
            else:
                time_exits += 1
                if pnl > 0: wins += 1
                else: losses += 1

        if not trades:
            continue

        n = len(trades)
        total_pnl = sum(trades)
        w = sum(1 for t in trades if t > 0)
        wr = w / n * 100

        # Daily stats
        days_list = sorted(daily_pnl.keys())
        n_days = len(days_list) if days_list else 1
        daily_vals = [daily_pnl[d] for d in days_list]
        avg_daily = np.mean(daily_vals) if daily_vals else 0
        std_daily = np.std(daily_vals) if daily_vals else 1
        sharpe = avg_daily / std_daily if std_daily > 0 else 0

        # Profit factor
        gross_win = sum(t for t in trades if t > 0)
        gross_loss = abs(sum(t for t in trades if t < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999

        # Max drawdown (daily)
        cum = np.cumsum(daily_vals)
        running_max = np.maximum.accumulate(cum)
        dd = running_max - cum
        max_dd = np.max(dd) if len(dd) > 0 else 0

        profit_days = sum(1 for v in daily_vals if v > 0)

        results.append({
            'sl': sl_roi, 'tp': tp_roi,
            'n': n, 'wr': wr, 'wins': w,
            'total_pnl': total_pnl,
            'avg_daily': avg_daily,
            'std_daily': std_daily,
            'sharpe': sharpe,
            'pf': pf,
            'max_dd': max_dd,
            'profit_days': profit_days,
            'n_days': n_days,
            'time_exits': time_exits,
        })

elapsed = _time.time() - t2
print(f"Grid search done in {elapsed:.1f}s\n")

# ── Phase 3: Results ────────────────────────────────────────────────
print("=" * 100)
print("TOP 20 BY TOTAL PnL")
print("=" * 100)
header = f"{'SL%':>5s} {'TP%':>5s} {'Trades':>7s} {'WR':>5s} {'PnL':>11s} {'$/day':>8s} {'Sharpe':>7s} {'PF':>6s} {'MaxDD':>9s} {'P.days':>7s} {'TimeEx':>7s}"
print(header)
print("-" * 100)
by_pnl = sorted(results, key=lambda x: x['total_pnl'], reverse=True)
for r in by_pnl[:20]:
    print(f"{r['sl']:>5.1f} {r['tp']:>5.1f} {r['n']:>7d} {r['wr']:>4.0f}% ${r['total_pnl']:>+10,.0f} ${r['avg_daily']:>+7,.0f} {r['sharpe']:>7.3f} {r['pf']:>5.2f} ${r['max_dd']:>8,.0f} {r['profit_days']:>3d}/{r['n_days']:>3d} {r['time_exits']:>7d}")

print()
print("=" * 100)
print("TOP 20 BY PROFIT FACTOR")
print("=" * 100)
print(header)
print("-" * 100)
by_pf = sorted([r for r in results if r['n'] >= 100], key=lambda x: x['pf'], reverse=True)
for r in by_pf[:20]:
    print(f"{r['sl']:>5.1f} {r['tp']:>5.1f} {r['n']:>7d} {r['wr']:>4.0f}% ${r['total_pnl']:>+10,.0f} ${r['avg_daily']:>+7,.0f} {r['sharpe']:>7.3f} {r['pf']:>5.2f} ${r['max_dd']:>8,.0f} {r['profit_days']:>3d}/{r['n_days']:>3d} {r['time_exits']:>7d}")

print()
print("=" * 100)
print("TOP 20 BY SHARPE RATIO (avg daily / std daily)")
print("=" * 100)
print(header)
print("-" * 100)
by_sharpe = sorted([r for r in results if r['n'] >= 100], key=lambda x: x['sharpe'], reverse=True)
for r in by_sharpe[:20]:
    print(f"{r['sl']:>5.1f} {r['tp']:>5.1f} {r['n']:>7d} {r['wr']:>4.0f}% ${r['total_pnl']:>+10,.0f} ${r['avg_daily']:>+7,.0f} {r['sharpe']:>7.3f} {r['pf']:>5.2f} ${r['max_dd']:>8,.0f} {r['profit_days']:>3d}/{r['n_days']:>3d} {r['time_exits']:>7d}")

# ── Phase 4: Current config comparison ──────────────────────────────
print()
print("=" * 100)
print("CURRENT CONFIG: SL=5.5% TP=6.5% — WITH vs WITHOUT FEES")
print("=" * 100)

# Find current config in results
current = next((r for r in results if r['sl'] == 5.5 and r['tp'] == 6.5), None)

# Recompute without fees for comparison
if current:
    trades_nofee = []
    daily_pnl_nofee = defaultdict(float)
    coin_sl_day_nf = defaultdict(lambda: defaultdict(int))
    w_nf = 0

    for coin, ts, direction, conf, day_key, path in signals:
        if coin_sl_day_nf[day_key][coin] >= MAX_SL_PER_DAY:
            continue
        exit_roi = None; last_close_roi = 0
        for candle in path:
            if candle is None: continue
            best_roi, worst_roi, close_roi = candle
            last_close_roi = close_roi
            if worst_roi <= -5.5: exit_roi = -5.5; break
            if best_roi >= 6.5: exit_roi = 6.5; break
        if exit_roi is None: exit_roi = last_close_roi
        pnl_nofee = exit_roi / 100 * MARGIN
        trades_nofee.append(pnl_nofee)
        daily_pnl_nofee[day_key] += pnl_nofee
        if pnl_nofee > 0: w_nf += 1
        if exit_roi == -5.5: coin_sl_day_nf[day_key][coin] += 1

    total_nofee = sum(trades_nofee)
    days_nf = sorted(daily_pnl_nofee.keys())
    daily_nf = [daily_pnl_nofee[d] for d in days_nf]
    n_days_nf = len(days_nf)

    fee_impact = total_nofee - current['total_pnl']
    fee_per_trade = fee_impact / current['n'] if current['n'] > 0 else 0

    print(f"{'':>20s} {'WITHOUT fees':>15s}  {'WITH fees':>15s}  {'Difference':>15s}")
    print(f"{'Total PnL':>20s} ${total_nofee:>+14,.0f}  ${current['total_pnl']:>+14,.0f}  ${-fee_impact:>+14,.0f}")
    print(f"{'$/day':>20s} ${total_nofee/n_days_nf:>+14,.0f}  ${current['avg_daily']:>+14,.0f}  ${-(fee_impact/n_days_nf):>+14,.0f}")
    print(f"{'$/trade (avg)':>20s} ${total_nofee/current['n']:>+14,.1f}  ${current['total_pnl']/current['n']:>+14,.1f}  ${-fee_per_trade:>+14,.1f}")
    print(f"{'Trades':>20s} {current['n']:>15,d}  {current['n']:>15,d}")
    print(f"{'WR':>20s} {w_nf/current['n']*100:>14.1f}%  {current['wr']:>14.1f}%")
    print(f"{'Fee per trade':>20s} {'$0.00':>15s}  ${FEE_ROI_ROUND_TRIP/100*MARGIN:>14.1f}")
    print(f"{'Fee total':>20s} {'$0':>15s}  ${FEE_ROI_ROUND_TRIP/100*MARGIN*current['n']:>14,.0f}")
else:
    print("  Config SL=5.5 TP=6.5 not found in results!")

# ── Phase 5: Recommendation ────────────────────────────────────────
print()
print("=" * 100)
print("RECOMMENDATION")
print("=" * 100)

# Score: weighted combo of sharpe, pnl, pf
scored = []
for r in results:
    if r['n'] < 100:
        continue
    # Normalize
    max_pnl = max(x['total_pnl'] for x in results)
    min_pnl = min(x['total_pnl'] for x in results)
    max_sh = max(x['sharpe'] for x in results if x['n'] >= 100)
    max_pf = min(max(x['pf'] for x in results if x['n'] >= 100), 5)

    pnl_score = (r['total_pnl'] - min_pnl) / (max_pnl - min_pnl) if max_pnl != min_pnl else 0
    sh_score = r['sharpe'] / max_sh if max_sh > 0 else 0
    pf_score = min(r['pf'], 5) / max_pf if max_pf > 0 else 0

    # Weight: 40% sharpe, 35% pnl, 25% pf
    score = 0.40 * sh_score + 0.35 * pnl_score + 0.25 * pf_score
    scored.append((score, r))

scored.sort(key=lambda x: x[0], reverse=True)

print(f"\nTop 10 by composite score (40% Sharpe + 35% PnL + 25% PF):")
print(header)
print("-" * 100)
for score, r in scored[:10]:
    marker = " <-- CURRENT" if r['sl'] == 5.5 and r['tp'] == 6.5 else ""
    print(f"{r['sl']:>5.1f} {r['tp']:>5.1f} {r['n']:>7d} {r['wr']:>4.0f}% ${r['total_pnl']:>+10,.0f} ${r['avg_daily']:>+7,.0f} {r['sharpe']:>7.3f} {r['pf']:>5.2f} ${r['max_dd']:>8,.0f} {r['profit_days']:>3d}/{r['n_days']:>3d} {r['time_exits']:>7d}{marker}")

best = scored[0][1]
print(f"\n>>> OPTIMAL: SL={best['sl']}% TP={best['tp']}%")
print(f"    PnL: ${best['total_pnl']:+,.0f} | $/day: ${best['avg_daily']:+,.0f} | Sharpe: {best['sharpe']:.3f} | PF: {best['pf']:.2f} | WR: {best['wr']:.0f}%")

if current:
    delta = best['total_pnl'] - current['total_pnl']
    print(f"    vs current (SL=5.5/TP=6.5): ${delta:+,.0f} difference ({delta/abs(current['total_pnl'])*100:+.0f}%)" if current['total_pnl'] != 0 else f"    vs current: ${delta:+,.0f} difference")

print(f"\nTotal time: {_time.time()-t0:.1f}s")
