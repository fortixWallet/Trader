#!/usr/bin/env python3 -u
"""
REALISTIC BACKTEST — CORE RULES ONLY
Position tracking + cooldown + hold time + fees.
Signal logic: ONLY cp+OI+liq/taker/CVD (no RSI/BB/trend/exhaustion).
Matches backtest_360d.py signal logic exactly.

Usage:
  python3 scripts/backtest_realistic_core.py                    # today
  python3 scripts/backtest_realistic_core.py 2026-04-20         # specific date
  python3 scripts/backtest_realistic_core.py 2026-04-18 2026-04-20  # date range
"""
import sys, sqlite3
sys.stdout.reconfigure(line_buffering=True)

from datetime import datetime, timezone, timedelta

# Config
LEV = 8
FEE = 0.88
SL = 10.0
TP = 8.0
MARGIN = 3466
DEDUP = 3600
COOLDOWN_SL = 7200
MAX_SL_PER_DAY = 3
CP_TOP = 0.55
CP_BOT = 0.45
OI_THRESH = 0.5
MIN_CONF = 0.75

COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
         'UNI','LDO','CRV','RENDER','ARB','OP','POL','WIF','PENDLE',
         'APT','FIL','NEAR','TON']  # 23 coins — matches live exactly (no FET/PEPE)

# Parse dates
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

# Load data
conn = sqlite3.connect('data/crypto/market.db')

print("Loading data...")
price_cache = {}
for coin in COINS:
    rows = conn.execute(
        "SELECT timestamp, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)
    ).fetchall()
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


def find_nearest(data_list, ts, max_gap=7200):
    if not data_list:
        return None
    lo, hi = 0, len(data_list) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if data_list[mid][0] < ts:
            lo = mid + 1
        else:
            hi = mid
    best = lo
    if best > 0 and abs(data_list[best-1][0] - ts) < abs(data_list[best][0] - ts):
        best = best - 1
    if abs(data_list[best][0] - ts) > max_gap:
        return None
    return data_list[best]


def get_core_signal(coin, ts):
    """Core signal only: cp + OI + liq/taker/CVD. No RSI/BB/trend."""
    candles = []
    for off in range(16):
        t = ts - off * 900
        if t in price_cache.get(coin, {}):
            candles.append(price_cache[coin][t])
    if len(candles) < 12:
        return None, 0

    p = candles[0][2]
    if p == 0:
        return None, 0

    highs = [c[0] for c in candles]
    lows = [c[1] for c in candles]
    rng_h, rng_l = max(highs), min(lows)
    cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

    oi_now = find_nearest(oi_cache.get(coin, []), ts)
    oi_4h = find_nearest(oi_cache.get(coin, []), ts - 14400)
    if not oi_now or not oi_4h or oi_4h[1] == 0:
        return None, 0
    oi = (oi_now[1] / oi_4h[1] - 1) * 100

    tk_r = find_nearest(tk_cache.get(coin, []), ts)
    tk = tk_r[1] if tk_r else 1.0

    liq_r = find_nearest(liq_cache.get(coin, []), ts)
    liq = None
    if liq_r and (liq_r[1] + liq_r[2]) > 0:
        liq = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

    cvd_now = find_nearest(cvd_cache.get(coin, []), ts)
    cvd_4h = find_nearest(cvd_cache.get(coin, []), ts - 14400)
    cvd = None
    if cvd_now and cvd_4h:
        cvd = (cvd_now[1] - cvd_4h[1]) / 1e6

    at_top = cp > CP_TOP
    at_bot = cp < CP_BOT
    ss = 0
    ls = 0

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

    direction = None
    conf = 0
    if ss >= 5:
        direction = 'SHORT'
        conf = min(0.95, 0.7 + ss * 0.03)
    elif ss >= 3:
        direction = 'SHORT'
        conf = min(0.85, 0.6 + ss * 0.03)
    elif ls >= 5:
        direction = 'LONG'
        conf = min(0.95, 0.7 + ls * 0.03)
    elif ls >= 3:
        direction = 'LONG'
        conf = min(0.85, 0.6 + ls * 0.03)

    if not direction or conf < MIN_CONF:
        return None, 0

    return direction, conf


# Run
print(f"REALISTIC BACKTEST — CORE RULES ONLY")
print(f"Period: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
print(f"SL={SL}% TP={TP}% LEV={LEV}x FEE={FEE}% MARGIN=${MARGIN}")
print(f"Position tracking + 2h cooldown + 3h hold")
print()

open_positions = {}
all_trades = []
last_sig = {}
coin_sl_count = {}
coin_cooldown = {}

ts = start_ts
while ts < end_ts:
    # TIME EXIT
    for coin in list(open_positions.keys()):
        d, entry, ets, conf = open_positions[coin]
        if ts - ets >= 12 * 900:
            if ts in price_cache.get(coin, {}):
                fc = price_cache[coin][ts][2]
                roi = ((fc/entry-1) if d == 'LONG' else (entry/fc-1)) * 100 * LEV
            else:
                roi = 0
            pnl = (roi - FEE) / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            all_trades.append({
                'dt': dt, 'entry_dt': edt, 'coin': coin, 'dir': d,
                'roi': roi, 'exit': 'TIME', 'pnl': pnl, 'conf': conf
            })
            del open_positions[coin]

    # SL/TP
    for coin in list(open_positions.keys()):
        d, entry, ets, conf = open_positions[coin]
        if ts not in price_cache.get(coin, {}):
            continue
        fh, fl, fc = price_cache[coin][ts]
        if d == 'LONG':
            best = (fh/entry-1)*100*LEV
            worst = (fl/entry-1)*100*LEV
        else:
            best = (entry/fl-1)*100*LEV
            worst = (entry/fh-1)*100*LEV

        if worst <= -SL:
            pnl = (-SL - FEE) / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            all_trades.append({
                'dt': dt, 'entry_dt': edt, 'coin': coin, 'dir': d,
                'roi': -SL, 'exit': 'SL', 'pnl': pnl, 'conf': conf
            })
            del open_positions[coin]
            coin_sl_count[coin] = coin_sl_count.get(coin, 0) + 1
            coin_cooldown[coin] = ts
        elif best >= TP:
            pnl = (TP - FEE) / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            all_trades.append({
                'dt': dt, 'entry_dt': edt, 'coin': coin, 'dir': d,
                'roi': TP, 'exit': 'TP', 'pnl': pnl, 'conf': conf
            })
            del open_positions[coin]

    # Scan
    for coin in COINS:
        if coin in open_positions:
            continue
        if coin in last_sig and ts - last_sig[coin] < DEDUP:
            continue
        if coin_sl_count.get(coin, 0) >= MAX_SL_PER_DAY:
            continue
        if coin in coin_cooldown and ts - coin_cooldown[coin] < COOLDOWN_SL:
            continue

        direction, conf = get_core_signal(coin, ts)
        if not direction:
            continue

        last_sig[coin] = ts

        if ts in price_cache.get(coin, {}):
            entry = price_cache[coin][ts][2]
        else:
            continue

        open_positions[coin] = (direction, entry, ts, conf)

    ts += 900

# Print
print(f"{'Close':>5s} {'Entry':>5s} {'Coin':>8s} {'Dir':>5s} {'Conf':>5s} {'ROI':>6s} {'Exit':>4s} {'PnL':>8s}")
print("-" * 52)

longs = []
shorts = []
for t in all_trades:
    icon = '+' if t['pnl'] > 0 else '-'
    print(f"{t['dt']:>5s} {t['entry_dt']:>5s} {t['coin']:>8s} {t['dir']:>5s} {t['conf']:>4.0%} {t['roi']:>+5.1f}% {t['exit']:>4s} ${t['pnl']:>+7.0f} {icon}")
    if t['dir'] == 'LONG':
        longs.append(t)
    else:
        shorts.append(t)

for coin, (d, entry, ets, conf) in open_positions.items():
    edt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
    print(f"  OPEN {edt} {coin:>8s} {d:>5s} {conf:.0%}")

print("-" * 52)
n = len(all_trades)
if n == 0:
    print("No trades")
    sys.exit()

w = sum(1 for t in all_trades if t['pnl'] > 0)
total = sum(t['pnl'] for t in all_trades)
l_w = sum(1 for t in longs if t['pnl'] > 0) if longs else 0
s_w = sum(1 for t in shorts if t['pnl'] > 0) if shorts else 0
l_pnl = sum(t['pnl'] for t in longs)
s_pnl = sum(t['pnl'] for t in shorts)

print(f"Total: {n} trades | WR: {w}/{n} = {w/n*100:.0f}% | PnL: ${total:+,.0f}")
if longs:
    print(f"LONG:  {len(longs)} | WR: {l_w}/{len(longs)} = {l_w/len(longs)*100:.0f}% | PnL: ${l_pnl:+,.0f}")
if shorts:
    print(f"SHORT: {len(shorts)} | WR: {s_w}/{len(shorts)} = {s_w/len(shorts)*100:.0f}% | PnL: ${s_pnl:+,.0f}")
print(f"Open: {len(open_positions)}")
