#!/usr/bin/env python3 -u
"""
STATE MACHINE BACKTEST — Two-pass: precompute signals, then compare sizing strategies.
Pass 1: compute ALL signals + trade outcomes (direction, roi, exit_type) for every coin×hour
Pass 2a: baseline (all full size)
Pass 2b: state machine (full with trend, $1 probe against trend, flip on probe TPs + full SLs)
"""
import sys; sys.stdout.reconfigure(line_buffering=True)
import sqlite3
from datetime import datetime, timedelta
from bisect import bisect_right

conn = sqlite3.connect('data/crypto/market.db')

ALL_COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','POL',
             'UNI','PENDLE','LDO','CRV','ARB','OP','APT','FIL','NEAR','TON',
             'FET','RENDER','DOGE','PEPE','WIF']

print("Loading data...")

price_cache = {}
for coin in ALL_COINS:
    rows = conn.execute(
        "SELECT timestamp, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)
    ).fetchall()
    price_cache[coin] = {r[0]: (r[1], r[2], r[3]) for r in rows}
print(f"  15m: {sum(len(v) for v in price_cache.values()):,}")

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

# BTC 4h EMA precomputed
btc_4h_rows = conn.execute(
    "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp"
).fetchall()
btc_4h_ema = {}
ema = btc_4h_rows[0][1] if btc_4h_rows else 0
for ts, close in btc_4h_rows:
    ema = close * 2/13 + ema * 11/13
    btc_4h_ema[ts] = (close - ema) / ema * 100 if ema > 0 else 0
btc_4h_ts_sorted = sorted(btc_4h_ema.keys())
print(f"  BTC 4h: {len(btc_4h_ema)}")

conn.close()
print("Data loaded!\n")

# Config
CP_TOP, CP_BOT = 0.55, 0.45
OI_THRESH = 0.5
MIN_CONF = 0.75
SL_ROI, TP_ROI = 5.5, 6.5
LEV = 8
DEDUP = 3600
MAX_SL_PER_DAY = 3
CAPITAL = 50_000
FULL_MARGIN = CAPITAL * 0.10
PROBE_MARGIN = 1.0

start_ts = int(datetime(2025, 5, 1).timestamp())
end_ts = int(datetime(2026, 4, 19, 23, 59, 59).timestamp())


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


# ====== PASS 1: Precompute all signals ======
print("PASS 1: Computing all signals...")
# signals_by_hour[ts] = [(coin, direction, conf, roi, exit_type), ...]
signals_by_hour = {}
total_signals = 0

for coin in ALL_COINS:
    coin_signals = 0
    pc = price_cache.get(coin, {})
    oi_c = oi_cache.get(coin, [])
    tk_c = tk_cache.get(coin, [])
    liq_c = liq_cache.get(coin, [])
    cvd_c = cvd_cache.get(coin, [])

    ts = start_ts
    while ts <= end_ts:
        # Get 15m candles
        candles = []
        for offset in range(16):
            t = ts - offset * 900
            if t in pc:
                candles.append(pc[t])
        if len(candles) < 12:
            ts += 3600
            continue

        p = candles[0][2]  # close
        if p == 0:
            ts += 3600
            continue

        highs = [c[0] for c in candles]
        lows = [c[1] for c in candles]
        rng_h, rng_l = max(highs), min(lows)
        cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

        at_top = cp > CP_TOP
        at_bot = cp < CP_BOT

        if not at_top and not at_bot:
            ts += 3600
            continue

        # OI
        oi_now_r = find_nearest(oi_c, ts)
        oi_4h_r = find_nearest(oi_c, ts - 14400)
        if not oi_now_r or not oi_4h_r or oi_4h_r[1] == 0:
            ts += 3600
            continue
        oi = (oi_now_r[1] / oi_4h_r[1] - 1) * 100

        tk_r = find_nearest(tk_c, ts)
        tk = tk_r[1] if tk_r else 1.0

        liq_r = find_nearest(liq_c, ts)
        liq = None
        if liq_r and (liq_r[1] + liq_r[2]) > 0:
            liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

        cvd_now_r = find_nearest(cvd_c, ts)
        cvd_4h_r = find_nearest(cvd_c, ts - 14400)
        cvd = None
        if cvd_now_r and cvd_4h_r:
            cvd = (cvd_now_r[1] - cvd_4h_r[1]) / 1e6

        ss = 0; ls = 0
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
            ts += 3600
            continue

        # Simulate trade outcome
        entry = p
        exit_roi = None
        last_roi = 0
        for off in range(1, 13):
            ft = ts + off * 900
            if ft not in pc: continue
            fh, fl, fc = pc[ft]
            if direction == 'LONG':
                best = (fh/entry - 1) * 100 * LEV
                worst = (fl/entry - 1) * 100 * LEV
                last_roi = (fc/entry - 1) * 100 * LEV
            else:
                best = (entry/fl - 1) * 100 * LEV
                worst = (entry/fh - 1) * 100 * LEV
                last_roi = (entry/fc - 1) * 100 * LEV
            if worst <= -SL_ROI: exit_roi = -SL_ROI; break
            if best >= TP_ROI: exit_roi = TP_ROI; break

        if exit_roi is None: exit_roi = last_roi
        exit_type = 'SL' if exit_roi <= -SL_ROI + 0.5 else ('TP' if exit_roi >= TP_ROI - 0.5 else 'TIME')

        if ts not in signals_by_hour:
            signals_by_hour[ts] = []
        signals_by_hour[ts].append((coin, direction, conf, exit_roi, exit_type))
        coin_signals += 1
        ts += 3600
        continue

        ts += 3600

    total_signals += coin_signals
    print(f"  {coin:>6s}: {coin_signals:,} signals")

print(f"\nTotal: {total_signals:,} signals across {len(signals_by_hour):,} hours\n")


# ====== PASS 2: Apply sizing strategies ======

def get_btc_trend(ts):
    idx = bisect_right(btc_4h_ts_sorted, ts) - 1
    if idx < 0: return 'RANGE'
    pct = btc_4h_ema[btc_4h_ts_sorted[idx]]
    if pct > 0.5: return 'UP'
    if pct < -0.5: return 'DOWN'
    return 'RANGE'


def run_strategy(use_sm):
    monthly = []
    state = 'RANGE'
    recent_results = []
    total_flips = 0
    state_log = []
    FLIP_WINDOW = 6 * 3600

    day_ts = start_ts
    current_month_str = ''
    month_trades = []
    month_daily = []

    while day_ts <= end_ts:
        dt = datetime.utcfromtimestamp(day_ts)
        ms = dt.strftime('%Y-%m')

        if ms != current_month_str:
            if current_month_str and month_trades:
                n = len(month_trades)
                w = sum(1 for t in month_trades if t > 0)
                pnl = sum(month_trades)
                pd = sum(1 for d in month_daily if d > 0)
                worst = min(month_daily) if month_daily else 0
                monthly.append({'month': current_month_str, 'n': n, 'w': w, 'pnl': pnl,
                               'pd': pd, 'days': len(month_daily), 'worst': worst})
            current_month_str = ms
            month_trades = []
            month_daily = []

        day_pnl = 0
        last_sig = {}
        coin_sl = {}

        for hour in range(24):
            ts = day_ts + hour * 3600

            if use_sm:
                bt = get_btc_trend(ts)
                if state == 'RANGE' and bt != 'RANGE':
                    old = state; state = bt
                    if old != state:
                        state_log.append((ts, old, state, 'BTC'))

                cutoff = ts - FLIP_WINDOW
                recent_results[:] = [r for r in recent_results if r[0] > cutoff]
                ptps = sum(1 for r in recent_results if r[1] == 'probe' and r[2] == 'TP')
                fsls = sum(1 for r in recent_results if r[1] == 'full' and r[2] == 'SL')

                if ptps >= 2 and fsls >= 2:
                    pdirs = [r[3] for r in recent_results if r[1] == 'probe' and r[2] == 'TP']
                    if pdirs:
                        ns = 'UP' if pdirs[-1] == 'LONG' else 'DOWN'
                        if ns != state:
                            state_log.append((ts, state, ns, f'FLIP({ptps}tp+{fsls}sl)'))
                            state = ns
                            total_flips += 1
                            recent_results.clear()

            if ts not in signals_by_hour:
                continue

            for coin, direction, conf, exit_roi, exit_type in signals_by_hour[ts]:
                if coin in last_sig and ts - last_sig[coin] < DEDUP: continue
                if coin_sl.get(coin, 0) >= MAX_SL_PER_DAY: continue

                last_sig[coin] = ts

                if use_sm:
                    with_trend = (
                        (state == 'DOWN' and direction == 'SHORT') or
                        (state == 'UP' and direction == 'LONG') or
                        state == 'RANGE'
                    )
                    margin = FULL_MARGIN if with_trend else PROBE_MARGIN
                    ttype = 'full' if with_trend else 'probe'
                else:
                    margin = FULL_MARGIN
                    ttype = 'full'

                pnl = exit_roi / 100 * margin
                day_pnl += pnl
                month_trades.append(pnl)

                if exit_type == 'SL':
                    coin_sl[coin] = coin_sl.get(coin, 0) + 1

                if use_sm:
                    recent_results.append((ts, ttype, exit_type, direction))

        month_daily.append(day_pnl)
        day_ts += 86400

    # Last month
    if month_trades:
        n = len(month_trades)
        w = sum(1 for t in month_trades if t > 0)
        pnl = sum(month_trades)
        pd = sum(1 for d in month_daily if d > 0)
        worst = min(month_daily) if month_daily else 0
        monthly.append({'month': current_month_str, 'n': n, 'w': w, 'pnl': pnl,
                       'pd': pd, 'days': len(month_daily), 'worst': worst})

    return monthly, total_flips, state_log


def pr(label, monthly, flips=0, state_log=None):
    print(f"\n{'='*70}")
    print(f"  {label}")
    if flips: print(f"  (trend flips: {flips})")
    print(f"{'='*70}")
    print(f"{'Month':>8s} {'Trades':>7s} {'WR':>5s} {'PnL':>12s} {'$/day':>10s} {'P.days':>7s} {'Worst':>10s}")
    print("-"*70)
    for m in monthly:
        wr = m['w']/m['n']*100 if m['n'] else 0
        pd = m['pnl']/m['days'] if m['days'] else 0
        icon = '✅' if m['pnl'] > 0 else '❌'
        print(f"{m['month']:>8s} {m['n']:>7d} {wr:>4.0f}% ${m['pnl']:>+11,.0f} ${pd:>+9,.0f} "
              f"{m['pd']:>3d}/{m['days']:>2d} ${m['worst']:>+9,.0f} {icon}")
    print("-"*70)
    tn = sum(m['n'] for m in monthly)
    tw = sum(m['w'] for m in monthly)
    tp = sum(m['pnl'] for m in monthly)
    td = sum(m['days'] for m in monthly)
    ppd = sum(m['pd'] for m in monthly)
    print(f"{'TOTAL':>8s} {tn:>7d} {tw/tn*100:>4.0f}% ${tp:>+11,.0f} ${tp/td:>+9,.0f} {ppd:>3d}/{td:>3d}")
    print(f"\n  Cumulative: ${tp:+,.0f} = {tp/CAPITAL*100:+.1f}%")
    print(f"  Avg daily: ${tp/td:+,.0f} | Profit days: {ppd}/{td} ({ppd/td*100:.0f}%)")
    if state_log:
        print(f"\n  Flips (first 15):")
        for ts, old, new, reason in state_log[:15]:
            print(f"    {datetime.utcfromtimestamp(ts).strftime('%m-%d %H:%M')}: {old}→{new} ({reason})")


print("PASS 2: Running strategies...")
print("\n--- BASELINE ---")
b, _, _ = run_strategy(False)
pr("BASELINE — All trades $5K margin", b)

print("\n--- STATE MACHINE ---")
s, sf, sl = run_strategy(True)
pr("STATE MACHINE — Full with trend, $1 probe against", s, sf, sl)

bp = sum(m['pnl'] for m in b)
sp = sum(m['pnl'] for m in s)
bd = sum(m['days'] for m in b)
sd = sum(m['days'] for m in s)
bw = min(m['worst'] for m in b)
sw = min(m['worst'] for m in s)
print(f"\n{'='*70}")
print(f"  COMPARISON")
print(f"{'='*70}")
print(f"  {'Metric':<25s} {'Baseline':>15s} {'StateMachine':>15s} {'Delta':>12s}")
print(f"  {'-'*67}")
print(f"  {'Total PnL':<25s} ${bp:>+14,.0f} ${sp:>+14,.0f} ${sp-bp:>+11,.0f}")
print(f"  {'Avg $/day':<25s} ${bp/bd:>+14,.0f} ${sp/sd:>+14,.0f} ${sp/sd-bp/bd:>+11,.0f}")
print(f"  {'Worst day':<25s} ${bw:>+14,.0f} ${sw:>+14,.0f}")
print(f"  {'Trend flips':<25s} {'—':>15s} {sf:>15d}")
