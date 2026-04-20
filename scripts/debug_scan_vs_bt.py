#!/usr/bin/env python3 -u
"""
Debug: run LIVE scanner and BACKTEST scanner side-by-side on current timestamp.
Shows EVERY value for EVERY coin to find where they diverge.
"""
import sys, sqlite3, time, requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, '.')

DB_PATH = 'data/crypto/market.db'
COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
         'UNI','LDO','CRV','RENDER','ARB','OP','POL','WIF','PENDLE',
         'APT','FIL','NEAR','TON']

# Step 1: Fetch fresh candles from Binance (like live does)
print("Fetching fresh candles from Binance...")
fresh_candles = {}
def fetch_one(coin):
    try:
        r = requests.get('https://fapi.binance.com/fapi/v1/klines',
                        params={'symbol': coin + 'USDT', 'interval': '15m', 'limit': 2}, timeout=5)
        data = r.json()
        result = []
        for k in data:
            result.append((int(k[0])//1000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        return coin, result
    except:
        return coin, []

with ThreadPoolExecutor(max_workers=10) as pool:
    for coin, data in pool.map(fetch_one, COINS):
        if data:
            fresh_candles[coin] = data

now_ts = int(time.time())
print(f"Time: {datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC")
print()

# Step 2: Run LIVE scanner
from src.crypto.signal_scanner import scan_signals
live_results = scan_signals(coins=COINS, fresh_candles=fresh_candles)

# Step 3: Run BACKTEST logic on same timestamp
conn = sqlite3.connect(DB_PATH)

# Load caches
price_cache = {}
for coin in COINS:
    rows = conn.execute("SELECT timestamp, high, low, close FROM prices WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)).fetchall()
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
    lo,hi=0,len(dl)-1
    while lo<hi:
        mid=(lo+hi)//2
        if dl[mid][0]<ts: lo=mid+1
        else: hi=mid
    b=lo
    if b>0 and abs(dl[b-1][0]-ts)<abs(dl[b][0]-ts): b=b-1
    if abs(dl[b][0]-ts)>mg: return None
    return dl[b]

# Round ts to latest closed 15min candle (scan is AFTER close now)
bt_ts = (now_ts // 900) * 900
print(f"BT timestamp: {datetime.fromtimestamp(bt_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC")
print()

print(f"{'Coin':>8s} | {'LIVE cp':>8s} {'BT cp':>8s} {'Δcp':>6s} | {'LIVE oi':>8s} {'BT oi':>8s} | {'LIVE tk':>8s} {'BT tk':>8s} | {'LIVE liq':>9s} {'BT liq':>9s} | {'LIVE cvd':>9s} {'BT cvd':>9s} | {'LIVE':>14s} {'BT':>14s} | {'MATCH':>5s}")
print("-" * 160)

for coin in COINS:
    # Live values
    ld = live_results[coin]['details']
    l_cp = ld.get('close_pos', None)
    l_oi = ld.get('oi_chg', None)
    l_tk = ld.get('taker', None)
    l_liq = ld.get('liq_ratio', None)
    l_cvd = ld.get('cvd_chg', None)
    l_sig = live_results[coin]['signal']
    l_conf = live_results[coin]['confidence']

    # Backtest values
    candles = []
    for off in range(16):
        t = bt_ts - off * 900
        if t in price_cache.get(coin, {}):
            candles.append(price_cache[coin][t])

    if len(candles) >= 12:
        p = candles[0][2]
        highs = [c[0] for c in candles]; lows = [c[1] for c in candles]
        rng_h, rng_l = max(highs), min(lows)
        b_cp = round((p - rng_l) / (rng_h - rng_l), 3) if rng_h > rng_l else 0.5
    else:
        b_cp = None; p = 0

    oi_now = find_nearest(oi_cache.get(coin, []), bt_ts)
    oi_4h = find_nearest(oi_cache.get(coin, []), bt_ts - 14400)
    b_oi = round((oi_now[1] / oi_4h[1] - 1) * 100, 2) if oi_now and oi_4h and oi_4h[1] > 0 else None

    tk_r = find_nearest(tk_cache.get(coin, []), bt_ts)
    b_tk = round(tk_r[1], 3) if tk_r else None

    liq_r = find_nearest(liq_cache.get(coin, []), bt_ts)
    b_liq = None
    if liq_r and (liq_r[1] + liq_r[2]) > 0:
        b_liq = round((liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2]), 2)

    cvd_now = find_nearest(cvd_cache.get(coin, []), bt_ts)
    cvd_4h = find_nearest(cvd_cache.get(coin, []), bt_ts - 14400)
    b_cvd = round((cvd_now[1] - cvd_4h[1]) / 1e6, 1) if cvd_now and cvd_4h else None

    # BT signal
    at_top = b_cp is not None and b_cp > 0.55
    at_bot = b_cp is not None and b_cp < 0.45
    oi_drop = b_oi is not None and b_oi < -0.5
    oi_rise = b_oi is not None and b_oi > 0.5
    ss = 0; ls = 0
    if at_top and oi_drop:
        if b_liq is not None and b_liq > 0.3: ss += 5
        if b_tk is not None and b_tk < 0.9: ss += 5
        if b_cvd is not None and b_cvd < 0: ss += 3
    if at_bot and oi_rise:
        if b_liq is not None and b_liq < -0.3: ls += 5
        if b_tk is not None and b_tk > 1.1: ls += 5
        if b_cvd is not None and b_cvd > 0: ls += 3

    b_sig = 'NEUTRAL'; b_conf = 0
    if ss >= 5: b_sig = 'STRONG_SHORT'; b_conf = min(0.95, 0.7 + ss * 0.03)
    elif ss >= 3: b_sig = 'SHORT'; b_conf = min(0.85, 0.6 + ss * 0.03)
    elif ls >= 5: b_sig = 'STRONG_LONG'; b_conf = min(0.95, 0.7 + ls * 0.03)
    elif ls >= 3: b_sig = 'LONG'; b_conf = min(0.85, 0.6 + ls * 0.03)
    if b_conf < 0.75: b_sig = 'NEUTRAL'; b_conf = 0

    # Compare
    cp_diff = abs((l_cp or 0) - (b_cp or 0))
    match = "YES" if l_sig == b_sig else "NO!!!"

    l_sig_short = f"{l_sig[:6]} {l_conf:.0%}" if l_sig != 'NEUTRAL' else 'NEUTRAL'
    b_sig_short = f"{b_sig[:6]} {b_conf:.0%}" if b_sig != 'NEUTRAL' else 'NEUTRAL'

    flag = " <<<" if match == "NO!!!" else ""
    def f(v, w=8):
        return str(v)[:w].rjust(w) if v is not None else "None".rjust(w)
    print(f"{coin:>8s} | {f(l_cp)} {f(b_cp)} {cp_diff:>5.3f} | {f(l_oi)} {f(b_oi)} | {f(l_tk)} {f(b_tk)} | {f(l_liq,9)} {f(b_liq,9)} | {f(l_cvd,9)} {f(b_cvd,9)} | {l_sig_short:>14s} {b_sig_short:>14s} | {match:>5s}{flag}")
