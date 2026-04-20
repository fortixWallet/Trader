#!/usr/bin/env python3 -u
"""
REALISTIC BACKTEST — matches live system behavior:
- Position tracking (1 position per coin)
- 2h cooldown after SL, 24h after 3 SL
- 3h max hold (TIME EXIT)
- Fees included (0.88% ROI round trip)
- Scan every 15 min
- Uses SAME signal_scanner.py as live (ALL rules)

Usage:
  python3 scripts/backtest_realistic.py                    # today
  python3 scripts/backtest_realistic.py 2026-04-19         # specific date
  python3 scripts/backtest_realistic.py 2026-04-18 2026-04-20  # date range
"""
import sys, sqlite3, time
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, '.')

from datetime import datetime, timezone, timedelta
from src.crypto.signal_scanner import scan_coin, _conn

# Config — MUST match live
LEV = 8
FEE = 0.88
SL = 10.0
TP = 8.0
MARGIN = 3466
HOLD_CANDLES = 12  # 3h
DEDUP = 3600
COOLDOWN_SL = 7200  # 2h
MAX_SL_PER_DAY = 3

COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
         'UNI','LDO','CRV','RENDER','ARB','OP','POL','WIF','PENDLE',
         'APT','FIL','NEAR','TON']

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

print(f"REALISTIC BACKTEST (live-matched)")
print(f"Period: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
print(f"SL={SL}% TP={TP}% LEV={LEV}x FEE={FEE}% MARGIN=${MARGIN}")
print(f"Position tracking + cooldown + 3h hold")
print(f"Using signal_scanner.py (ALL rules)")
print()

# Load price cache for SL/TP/TIME simulation
conn = sqlite3.connect('data/crypto/market.db')
price_cache = {}
for coin in COINS:
    rows = conn.execute(
        "SELECT timestamp, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)
    ).fetchall()
    price_cache[coin] = {r[0]: (r[1], r[2], r[3]) for r in rows}
conn.close()

# State
open_positions = {}  # coin → (direction, entry_price, entry_ts, conf)
all_trades = []
last_sig = {}
coin_sl_count = {}
coin_cooldown = {}

scan_conn = _conn()

ts = start_ts
while ts < end_ts:
    # 1. TIME EXIT (3h hold)
    for coin in list(open_positions.keys()):
        d, entry, ets, conf = open_positions[coin]
        if ts - ets >= HOLD_CANDLES * 900:
            if ts in price_cache.get(coin, {}):
                fc = price_cache[coin][ts][2]
                roi = ((fc/entry-1) if d=='LONG' else (entry/fc-1)) * 100 * LEV
            else:
                roi = 0
            pnl = (roi - FEE) / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            all_trades.append({
                'dt': dt, 'coin': coin, 'dir': d, 'roi': roi,
                'exit': 'TIME', 'pnl': pnl, 'conf': conf,
                'entry_dt': datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            })
            del open_positions[coin]

    # 2. SL/TP check
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
            all_trades.append({
                'dt': dt, 'coin': coin, 'dir': d, 'roi': -SL,
                'exit': 'SL', 'pnl': pnl, 'conf': conf,
                'entry_dt': datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            })
            del open_positions[coin]
            coin_sl_count[coin] = coin_sl_count.get(coin, 0) + 1
            coin_cooldown[coin] = ts
        elif best >= TP:
            pnl = (TP - FEE) / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            all_trades.append({
                'dt': dt, 'coin': coin, 'dir': d, 'roi': TP,
                'exit': 'TP', 'pnl': pnl, 'conf': conf,
                'entry_dt': datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
            })
            del open_positions[coin]

    # 3. Scan signals (using LIVE signal_scanner)
    for coin in COINS:
        if coin in open_positions:
            continue
        if coin in last_sig and ts - last_sig[coin] < DEDUP:
            continue
        if coin_sl_count.get(coin, 0) >= MAX_SL_PER_DAY:
            continue
        if coin in coin_cooldown and ts - coin_cooldown[coin] < COOLDOWN_SL:
            continue

        result = scan_coin(scan_conn, coin, ts)
        if result['signal'] == 'NEUTRAL':
            continue
        if result['confidence'] < 0.75:
            continue

        direction = 'SHORT' if 'SHORT' in result['signal'] else 'LONG'
        conf = result['confidence']
        last_sig[coin] = ts

        # Get entry price (candle close at ts)
        if ts in price_cache.get(coin, {}):
            entry = price_cache[coin][ts][2]
        else:
            continue

        open_positions[coin] = (direction, entry, ts, conf)

    ts += 900  # every 15 min

scan_conn.close()

# Print results
print(f"{'Time':>5s} {'Entry':>5s} {'Coin':>8s} {'Dir':>5s} {'Conf':>5s} {'ROI':>6s} {'Exit':>4s} {'PnL':>8s}")
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

# Still open
for coin, (d, entry, ets, conf) in open_positions.items():
    dt = datetime.fromtimestamp(ets, tz=timezone.utc).strftime('%H:%M')
    print(f"  OPEN {dt} {coin:>8s} {d:>5s} {conf:.0%}")

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
print(f"Open: {len(open_positions)} positions")
