#!/usr/bin/env python3 -u
"""
BACKTEST: Reversal Guard — close positions when signal reverses.

Tests the idea: every 15min, check if open position's direction
conflicts with current signal. If so, close immediately instead
of waiting for SL -10%.

Usage:
  python3 scripts/backtest_reversal_guard.py                     # today
  python3 scripts/backtest_reversal_guard.py 2026-04-20          # specific date
  python3 scripts/backtest_reversal_guard.py 2026-04-18 2026-04-20  # range

Compares WITH vs WITHOUT reversal guard side-by-side.
"""
import sys, sqlite3
sys.stdout.reconfigure(line_buffering=True)

from datetime import datetime, timezone, timedelta

LEV = 8
FEE = 0.88
SL = 10.0
TP = 8.0
MARGIN = 3466
DEDUP = 3600
COOLDOWN_SL = 7200
MAX_SL_PER_DAY = 3

COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
         'UNI','LDO','CRV','RENDER','ARB','OP','POL','WIF','PENDLE',
         'APT','FIL','NEAR','TON']

if len(sys.argv) >= 3:
    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=1)
elif len(sys.argv) >= 2:
    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=1)
else:
    start_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=1)

start_ts = int(start_date.timestamp())
end_ts = int(end_date.timestamp())

conn = sqlite3.connect('data/crypto/market.db')
print("Loading data...")

price_cache = {}
for coin in COINS:
    rows = conn.execute(
        "SELECT timestamp, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)).fetchall()
    price_cache[coin] = {r[0]: (r[1], r[2], r[3]) for r in rows}

oi_cache, tk_cache, liq_cache, cvd_cache = {}, {}, {}, {}
for coin in COINS:
    rows = conn.execute("SELECT timestamp, c FROM pred_oi_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    oi_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
    rows = conn.execute("SELECT timestamp, ratio FROM pred_taker_volume WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    tk_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
    rows = conn.execute("SELECT timestamp, long_liq_usd, short_liq_usd FROM pred_liq_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    liq_cache[coin] = [(r[0], float(r[1]), float(r[2])) for r in rows]
    rows = conn.execute("SELECT timestamp, cvd FROM pred_cvd_futures WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    cvd_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
conn.close()


def find_nearest(dl, ts, mg=7200):
    if not dl: return None
    lo, hi = 0, len(dl) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if dl[mid][0] < ts: lo = mid + 1
        else: hi = mid
    b = lo
    if b > 0 and abs(dl[b-1][0] - ts) < abs(dl[b][0] - ts): b = b - 1
    if abs(dl[b][0] - ts) > mg: return None
    return dl[b]


def get_signal(coin, ts):
    candles = []
    for off in range(16):
        t = ts - off * 900
        if t in price_cache.get(coin, {}): candles.append(price_cache[coin][t])
    if len(candles) < 12: return None, 0
    p = candles[0][2]
    if p == 0: return None, 0
    highs = [c[0] for c in candles]; lows = [c[1] for c in candles]
    rng_h, rng_l = max(highs), min(lows)
    cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

    oi_now = find_nearest(oi_cache.get(coin, []), ts)
    oi_4h = find_nearest(oi_cache.get(coin, []), ts - 14400)
    if not oi_now or not oi_4h or oi_4h[1] == 0: return None, 0
    oi = (oi_now[1] / oi_4h[1] - 1) * 100

    tk_r = find_nearest(tk_cache.get(coin, []), ts); tk = tk_r[1] if tk_r else 1.0
    liq_r = find_nearest(liq_cache.get(coin, []), ts); liq = None
    if liq_r and (liq_r[1] + liq_r[2]) > 0: liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])
    cvd_now = find_nearest(cvd_cache.get(coin, []), ts)
    cvd_4h = find_nearest(cvd_cache.get(coin, []), ts - 14400); cvd = None
    if cvd_now and cvd_4h: cvd = (cvd_now[1] - cvd_4h[1]) / 1e6

    at_top = cp > 0.55; at_bot = cp < 0.45; ss = 0; ls = 0
    if at_top and oi < -0.5:
        if liq and liq > 0.3: ss += 5
        if tk < 0.9: ss += 5
        if cvd and cvd < 0: ss += 3
    if at_bot and oi > 0.5:
        if liq and liq < -0.3: ls += 5
        if tk > 1.1: ls += 5
        if cvd and cvd > 0: ls += 3

    d = None; conf = 0
    if ss >= 5: d = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
    elif ss >= 3: d = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
    elif ls >= 5: d = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
    elif ls >= 3: d = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)
    if not d or conf < 0.75: return None, 0
    return d, conf


def run_backtest(use_guard):
    open_pos = {}; trades = []; last_sig = {}; coin_sl = {}; coin_cd = {}
    reversal_details = []

    ts = start_ts
    while ts < end_ts:
        # TIME EXIT
        for coin in list(open_pos.keys()):
            d, entry, ets, conf = open_pos[coin]
            if ts - ets >= 12 * 900:
                if ts in price_cache.get(coin, {}):
                    fc = price_cache[coin][ts][2]
                    roi = ((fc/entry-1) if d == 'LONG' else (entry/fc-1)) * 100 * LEV
                else: roi = 0
                pnl = (roi - FEE) / 100 * MARGIN
                edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
                trades.append({'dt': dt, 'edt': edt, 'coin': coin, 'dir': d,
                              'roi': roi, 'exit': 'TIME', 'pnl': pnl})
                del open_pos[coin]

        # SL/TP
        for coin in list(open_pos.keys()):
            d, entry, ets, conf = open_pos[coin]
            if ts not in price_cache.get(coin, {}): continue
            fh, fl, fc = price_cache[coin][ts]
            if d == 'LONG': best = (fh/entry-1)*100*LEV; worst = (fl/entry-1)*100*LEV
            else: best = (entry/fl-1)*100*LEV; worst = (entry/fh-1)*100*LEV
            if worst <= -SL:
                pnl = (-SL - FEE) / 100 * MARGIN
                edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
                trades.append({'dt': dt, 'edt': edt, 'coin': coin, 'dir': d,
                              'roi': -SL, 'exit': 'SL', 'pnl': pnl})
                del open_pos[coin]
                coin_sl[coin] = coin_sl.get(coin, 0) + 1
                coin_cd[coin] = ts
            elif best >= TP:
                pnl = (TP - FEE) / 100 * MARGIN
                edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
                trades.append({'dt': dt, 'edt': edt, 'coin': coin, 'dir': d,
                              'roi': TP, 'exit': 'TP', 'pnl': pnl})
                del open_pos[coin]

        # REVERSAL GUARD
        if use_guard:
            for coin in list(open_pos.keys()):
                d, entry, ets, conf = open_pos[coin]
                sig_now, sig_conf = get_signal(coin, ts)
                if sig_now and sig_conf >= 0.75:
                    conflict = (d == 'LONG' and sig_now == 'SHORT') or (d == 'SHORT' and sig_now == 'LONG')
                    if conflict:
                        if ts in price_cache.get(coin, {}):
                            fc = price_cache[coin][ts][2]
                            roi = ((fc/entry-1) if d == 'LONG' else (entry/fc-1)) * 100 * LEV
                            pnl = (roi - FEE) / 100 * MARGIN
                            sl_pnl = (-SL - FEE) / 100 * MARGIN
                            saved = pnl - sl_pnl
                            edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
                            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
                            trades.append({'dt': dt, 'edt': edt, 'coin': coin, 'dir': d,
                                          'roi': roi, 'exit': 'REV', 'pnl': pnl})
                            reversal_details.append({
                                'dt': dt, 'coin': coin, 'was': d, 'now': sig_now,
                                'roi': roi, 'pnl': pnl, 'saved': saved
                            })
                            del open_pos[coin]

        # Scan new signals
        for coin in COINS:
            if coin in open_pos: continue
            if coin in last_sig and ts - last_sig[coin] < DEDUP: continue
            if coin_sl.get(coin, 0) >= MAX_SL_PER_DAY: continue
            if coin in coin_cd and ts - coin_cd[coin] < COOLDOWN_SL: continue
            d, conf = get_signal(coin, ts)
            if not d: continue
            last_sig[coin] = ts
            if ts in price_cache.get(coin, {}):
                open_pos[coin] = (d, price_cache[coin][ts][2], ts, conf)
        ts += 900

    return trades, reversal_details


print(f"REVERSAL GUARD BACKTEST")
print(f"Period: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
print(f"SL={SL}% TP={TP}% LEV={LEV}x FEE={FEE}%")
print()

# Run both
trades_off, _ = run_backtest(False)
trades_on, rev_details = run_backtest(True)

for label, trades in [("WITHOUT reversal guard", trades_off), ("WITH reversal guard", trades_on)]:
    n = len(trades)
    if n == 0:
        print(f"{label}: No trades")
        continue
    w = sum(1 for t in trades if t['pnl'] > 0)
    total = sum(t['pnl'] for t in trades)
    tp_n = sum(1 for t in trades if t['exit'] == 'TP')
    sl_n = sum(1 for t in trades if t['exit'] == 'SL')
    time_n = sum(1 for t in trades if t['exit'] == 'TIME')
    rev_n = sum(1 for t in trades if t['exit'] == 'REV')

    print(f"{label}:")
    print(f"  {n} trades | WR: {w}/{n} = {w/n*100:.0f}% | PnL: ${total:+,.0f}")
    print(f"  TP={tp_n} SL={sl_n} TIME={time_n} REV={rev_n}")
    print()

if rev_details:
    print("REVERSAL EXIT DETAILS:")
    print(f"{'Close':>5s} {'Coin':>8s} {'Was':>5s} {'Now':>5s} {'ROI':>6s} {'PnL':>8s} {'Saved':>8s}")
    print("-" * 50)
    total_saved = 0
    for r in rev_details:
        print(f"{r['dt']:>5s} {r['coin']:>8s} {r['was']:>5s} {r['now']:>5s} {r['roi']:>+5.1f}% ${r['pnl']:>+7.0f} ${r['saved']:>+7.0f}")
        total_saved += r['saved']
    print("-" * 50)
    print(f"Total reversal exits: {len(rev_details)}")
    print(f"Total saved vs SL: ${total_saved:+,.0f}")
    avg_roi = sum(r['roi'] for r in rev_details) / len(rev_details)
    print(f"Avg exit ROI: {avg_roi:+.1f}%")

# Comparison
print()
n1 = len(trades_off); n2 = len(trades_on)
p1 = sum(t['pnl'] for t in trades_off); p2 = sum(t['pnl'] for t in trades_on)
print(f"IMPROVEMENT: ${p2 - p1:+,.0f} ({(p2-p1)/abs(p1)*100:+.1f}%)")
