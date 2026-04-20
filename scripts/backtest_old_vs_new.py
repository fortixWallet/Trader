#!/usr/bin/env python3 -u
"""
Compare OLD system (today's live) vs NEW system (backtest-matched).

OLD: scan every 15min, ALL rules (RSI/BB/trend/wick/exhaustion), 25 coins with AAVE/TAO
NEW: scan every 1h, core rules only (cp+OI+liq/taker/CVD), 23 backtest coins

Run on April 18-19 to see the difference.
"""
import sys; sys.stdout.reconfigure(line_buffering=True)
import sqlite3, numpy as np
from datetime import datetime, timezone, timedelta

conn = sqlite3.connect('data/crypto/market.db')

# OLD coin list (what was running today)
OLD_COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
             'UNI','AAVE','LDO','CRV','RENDER','TAO','ARB','OP','POL','WIF',
             'PENDLE','JUP','PYTH','JTO','BOME']

# NEW coin list (backtest-matched, deployed now)
NEW_COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK','DOGE',
             'UNI','LDO','CRV','RENDER','ARB','OP','POL','WIF','PENDLE',
             'APT','FIL','NEAR','TON']

ALL_COINS = list(set(OLD_COINS + NEW_COINS))

print("Loading data...")
price_cache = {}
for coin in ALL_COINS:
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='15m' ORDER BY timestamp", (coin,)
    ).fetchall()
    price_cache[coin] = {r[0]: (r[1], r[2], r[3], r[4]) for r in rows}

oi_cache, tk_cache, liq_cache, cvd_cache = {}, {}, {}, {}
for coin in ALL_COINS:
    rows = conn.execute("SELECT timestamp, c FROM pred_oi_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    oi_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
    rows = conn.execute("SELECT timestamp, ratio FROM pred_taker_volume WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    tk_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]
    rows = conn.execute("SELECT timestamp, long_liq_usd, short_liq_usd FROM pred_liq_history WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    liq_cache[coin] = [(r[0], float(r[1]), float(r[2])) for r in rows]
    rows = conn.execute("SELECT timestamp, cvd FROM pred_cvd_futures WHERE coin=? ORDER BY timestamp", (coin,)).fetchall()
    cvd_cache[coin] = [(r[0], float(r[1])) for r in rows if r[1]]

conn.close()
print("Loaded.\n")


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


def get_indicators(coin, ts):
    """Get all indicators for a coin at timestamp."""
    pc = price_cache.get(coin, {})
    candles = []
    for offset in range(16):
        t = ts - offset * 900
        if t in pc:
            candles.append(pc[t])
    if len(candles) < 12:
        return None

    opens = [c[0] for c in candles]
    highs = [c[1] for c in candles]
    lows = [c[2] for c in candles]
    closes = [c[3] for c in candles]
    p = closes[0]
    if p == 0:
        return None

    rng_h, rng_l = max(highs), min(lows)
    cp = (p - rng_l) / (rng_h - rng_l) if rng_h > rng_l else 0.5

    m30 = (p / closes[1] - 1) * 100 if len(closes) >= 2 and closes[1] > 0 else 0
    prev_m30 = (closes[1] / closes[3] - 1) * 100 if len(closes) >= 4 and closes[3] > 0 else 0
    accel = m30 - prev_m30

    upper_wick = max((highs[j] - max(opens[j], closes[j])) / p * 100
                     for j in range(min(4, len(candles))) if p > 0)
    lower_wick = max((min(opens[j], closes[j]) - lows[j]) / p * 100
                     for j in range(min(4, len(candles))) if p > 0)

    deltas = np.diff(closes[::-1])  # chronological order
    g = np.where(deltas > 0, deltas, 0)
    l = np.where(deltas < 0, -deltas, 0)
    rsi = 100 - 100 / (1 + np.mean(g[-14:]) / (np.mean(l[-14:]) + 1e-10)) if len(g) >= 14 else 50
    bb_m, bb_s = np.mean(closes), np.std(closes)
    bb = (p - bb_m) / bb_s if bb_s > 0 else 0

    # OI
    oi_now_r = find_nearest(oi_cache.get(coin, []), ts)
    oi_4h_r = find_nearest(oi_cache.get(coin, []), ts - 14400)
    oi_chg = None
    if oi_now_r and oi_4h_r and oi_4h_r[1] > 0:
        oi_chg = (oi_now_r[1] / oi_4h_r[1] - 1) * 100

    tk_r = find_nearest(tk_cache.get(coin, []), ts)
    tk = tk_r[1] if tk_r else None

    liq_r = find_nearest(liq_cache.get(coin, []), ts)
    liq_ratio = None
    if liq_r and (liq_r[1] + liq_r[2]) > 0:
        liq_ratio = (liq_r[1] - liq_r[2]) / (liq_r[1] + liq_r[2])

    cvd_now_r = find_nearest(cvd_cache.get(coin, []), ts)
    cvd_4h_r = find_nearest(cvd_cache.get(coin, []), ts - 14400)
    cvd_chg = None
    if cvd_now_r and cvd_4h_r:
        cvd_chg = (cvd_now_r[1] - cvd_4h_r[1]) / 1e6

    # 4H trend
    h4_rows = []
    for off in range(7):
        t = ts - off * 14400
        # Find nearest 4h candle
        for dt in range(-1800, 1800, 900):
            if (t + dt) in price_cache.get(coin, {}):
                h4_rows.append(price_cache[coin][t + dt][3])
                break
    trend_4h = 0
    if len(h4_rows) >= 6:
        ema = h4_rows[-1]
        for c in reversed(h4_rows[:-1]):
            ema = c * 2/13 + ema * 11/13
        trend_4h = (p - ema) / ema * 100

    return {
        'cp': cp, 'p': p, 'accel': accel, 'rsi': rsi, 'bb': bb,
        'upper_wick': upper_wick, 'lower_wick': lower_wick,
        'oi_chg': oi_chg, 'tk': tk, 'liq_ratio': liq_ratio,
        'cvd_chg': cvd_chg, 'trend_4h': trend_4h
    }


def signal_old(ind):
    """OLD signal scanner: ALL rules (RSI, BB, trend, wick, exhaustion)."""
    if ind is None:
        return None, 0
    cp = ind['cp']; oi_chg = ind['oi_chg']; tk = ind['tk']
    liq_ratio = ind['liq_ratio']; cvd_chg = ind['cvd_chg']
    rsi = ind['rsi']; bb = ind['bb']; accel = ind['accel']
    upper_wick = ind['upper_wick']; lower_wick = ind['lower_wick']
    trend_4h = ind['trend_4h']

    at_top = cp > 0.55
    at_bot = cp < 0.45
    oi_dropping = oi_chg is not None and oi_chg < -0.5
    oi_rising = oi_chg is not None and oi_chg > 0.5
    taker_sell = tk is not None and tk < 0.9
    taker_buy = tk is not None and tk > 1.1
    liq_longs = liq_ratio is not None and liq_ratio > 0.3
    liq_shorts = liq_ratio is not None and liq_ratio < -0.3
    cvd_dropping = cvd_chg is not None and cvd_chg < 0
    cvd_rising = cvd_chg is not None and cvd_chg > 0

    ss = 0; ls = 0

    # Core SHORT
    if at_top and oi_dropping and liq_longs: ss += 5
    if at_top and oi_dropping and taker_sell: ss += 5
    if at_top and oi_dropping and cvd_dropping: ss += 3
    if at_top and oi_dropping and upper_wick > 0.05: ss += 3
    # Extra SHORT
    if at_top and rsi > 70 and oi_dropping and accel < -0.05: ss += 3
    if at_top and bb > 2.0 and oi_dropping and trend_4h > 1.5: ss += 3
    if at_top and accel < -0.05 and upper_wick > 0.05: ss += 1

    # Core LONG
    if at_bot and oi_rising and liq_shorts: ls += 5
    if at_bot and oi_rising and taker_buy: ls += 5
    if at_bot and oi_rising and cvd_rising: ls += 3
    if at_bot and oi_rising and lower_wick > 0.05: ls += 3
    # Extra LONG
    if at_bot and rsi < 30 and oi_rising and accel > 0.05: ls += 3
    if at_bot and bb < -2.0 and oi_rising and trend_4h < -1.0: ls += 3
    if at_bot and accel > 0.05 and lower_wick > 0.05: ls += 1

    # Trend continuation
    oi_strong = oi_chg is not None and oi_chg > 1.0
    if trend_4h > 1.0 and oi_strong and taker_buy and cp < 0.4: ls += 4
    elif trend_4h > 2.0 and oi_rising and cp < 0.4 and (tk is None or tk > 1.0): ls += 3
    elif trend_4h > 1.5 and oi_rising and taker_buy and cp < 0.4: ls += 2
    if trend_4h < -1.0 and oi_strong and taker_sell and cp > 0.6: ss += 3
    elif trend_4h < -2.0 and oi_rising and cp > 0.6 and (tk is None or tk < 1.0): ss += 2

    direction = None; conf = 0
    if ss >= 5: direction = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
    elif ss >= 3: direction = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
    elif ls >= 5: direction = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
    elif ls >= 3: direction = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)
    if not direction or conf < 0.75: return None, 0
    return direction, conf


def signal_new(ind):
    """NEW signal scanner: core rules only (matching backtest)."""
    if ind is None:
        return None, 0
    cp = ind['cp']; oi_chg = ind['oi_chg']; tk = ind['tk']
    liq_ratio = ind['liq_ratio']; cvd_chg = ind['cvd_chg']

    at_top = cp > 0.55
    at_bot = cp < 0.45
    oi_dropping = oi_chg is not None and oi_chg < -0.5
    oi_rising = oi_chg is not None and oi_chg > 0.5

    ss = 0; ls = 0
    if at_top and oi_dropping:
        if liq_ratio is not None and liq_ratio > 0.3: ss += 5
        if tk is not None and tk < 0.9: ss += 5
        if cvd_chg is not None and cvd_chg < 0: ss += 3
        ss += 1
    if at_bot and oi_rising:
        if liq_ratio is not None and liq_ratio < -0.3: ls += 5
        if tk is not None and tk > 1.1: ls += 5
        if cvd_chg is not None and cvd_chg > 0: ls += 3
        ls += 1

    direction = None; conf = 0
    if ss >= 5: direction = 'SHORT'; conf = min(0.95, 0.7 + ss * 0.03)
    elif ss >= 3: direction = 'SHORT'; conf = min(0.85, 0.6 + ss * 0.03)
    elif ls >= 5: direction = 'LONG'; conf = min(0.95, 0.7 + ls * 0.03)
    elif ls >= 3: direction = 'LONG'; conf = min(0.85, 0.6 + ls * 0.03)
    if not direction or conf < 0.75: return None, 0
    return direction, conf


def simulate_trade(coin, ts, direction, lev=8):
    pc = price_cache.get(coin, {})
    if ts not in pc: return None, None
    entry = pc[ts][3]
    if entry == 0: return None, None
    SL_ROI, TP_ROI = 5.5, 6.5
    last_roi = 0
    for off in range(1, 13):
        ft = ts + off * 900
        if ft not in pc: continue
        fh, fl, fc = pc[ft][1], pc[ft][2], pc[ft][3]
        if direction == 'LONG':
            best = (fh/entry - 1) * 100 * lev
            worst = (fl/entry - 1) * 100 * lev
            last_roi = (fc/entry - 1) * 100 * lev
        else:
            best = (entry/fl - 1) * 100 * lev
            worst = (entry/fh - 1) * 100 * lev
            last_roi = (entry/fc - 1) * 100 * lev
        if worst <= -SL_ROI: return -SL_ROI, 'SL'
        if best >= TP_ROI: return TP_ROI, 'TP'
    return last_roi, 'TIME'


def run_day(day_ts, coins, signal_fn, scan_interval_sec, label):
    """Run one day. scan_interval_sec: 900 for old (every 15m), 3600 for new (hourly)."""
    MARGIN = 3466.0
    LEV = 8
    trades = []
    last_sig = {}
    coin_sl = {}

    scan_times = list(range(day_ts, day_ts + 86400, scan_interval_sec))

    for ts in scan_times:
        for coin in coins:
            if coin in last_sig and ts - last_sig[coin] < 3600: continue
            if coin_sl.get(coin, 0) >= 3: continue

            ind = get_indicators(coin, ts)
            direction, conf = signal_fn(ind)
            if not direction: continue

            last_sig[coin] = ts
            roi, exit_type = simulate_trade(coin, ts, direction, LEV)
            if roi is None: continue

            pnl = roi / 100 * MARGIN
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')
            trades.append({
                'time': dt, 'coin': coin, 'dir': direction,
                'conf': conf, 'roi': roi, 'exit': exit_type, 'pnl': pnl
            })
            if exit_type == 'SL':
                coin_sl[coin] = coin_sl.get(coin, 0) + 1

    return trades


# === Run for April 18 and April 19 ===
days = [
    datetime(2026, 4, 18, tzinfo=timezone.utc),
    datetime(2026, 4, 19, tzinfo=timezone.utc),
]

for day in days:
    day_ts = int(day.timestamp())
    day_str = day.strftime('%Y-%m-%d (%A)')

    print(f"\n{'='*75}")
    print(f"  {day_str}")
    print(f"{'='*75}")

    # OLD system: scan every 15min, all rules, old coins
    old_trades = run_day(day_ts, OLD_COINS, signal_old, 900, "OLD")
    # NEW system: scan every 1h, core rules, new coins
    new_trades = run_day(day_ts, NEW_COINS, signal_new, 3600, "NEW")

    for label, trades in [("OLD (15min scan, all rules, 25 coins)", old_trades),
                           ("NEW (hourly scan, core rules, 23 coins)", new_trades)]:
        n = len(trades)
        if n == 0:
            print(f"\n  {label}: 0 trades")
            continue
        wins = sum(1 for t in trades if t['pnl'] > 0)
        total = sum(t['pnl'] for t in trades)
        longs = [t for t in trades if t['dir'] == 'LONG']
        shorts = [t for t in trades if t['dir'] == 'SHORT']
        l_pnl = sum(t['pnl'] for t in longs)
        s_pnl = sum(t['pnl'] for t in shorts)
        l_wr = sum(1 for t in longs if t['pnl'] > 0) / len(longs) * 100 if longs else 0
        s_wr = sum(1 for t in shorts if t['pnl'] > 0) / len(shorts) * 100 if shorts else 0

        print(f"\n  {label}")
        print(f"  Trades: {n} | WR: {wins}/{n} = {wins/n*100:.0f}% | PnL: ${total:+,.0f}")
        print(f"  LONG:  {len(longs):>3d} trades, WR {l_wr:.0f}%, PnL ${l_pnl:+,.0f}")
        print(f"  SHORT: {len(shorts):>3d} trades, WR {s_wr:.0f}%, PnL ${s_pnl:+,.0f}")

        print(f"\n  {'Time':>5s} {'Coin':>8s} {'Dir':>5s} {'Conf':>5s} {'ROI':>6s} {'Exit':>4s} {'PnL':>8s}")
        print(f"  {'-'*50}")
        for t in trades:
            icon = '+' if t['pnl'] > 0 else '-'
            print(f"  {t['time']:>5s} {t['coin']:>8s} {t['dir']:>5s} {t['conf']:>4.0%} "
                  f"{t['roi']:>+5.1f}% {t['exit']:>4s} ${t['pnl']:>+7.0f} {icon}")

    # Summary comparison
    old_pnl = sum(t['pnl'] for t in old_trades)
    new_pnl = sum(t['pnl'] for t in new_trades)
    old_n = len(old_trades)
    new_n = len(new_trades)
    old_wr = sum(1 for t in old_trades if t['pnl'] > 0) / old_n * 100 if old_n else 0
    new_wr = sum(1 for t in new_trades if t['pnl'] > 0) / new_n * 100 if new_n else 0

    print(f"\n  {'COMPARISON':>10s} {'OLD':>12s} {'NEW':>12s} {'Delta':>10s}")
    print(f"  {'-'*47}")
    print(f"  {'Trades':>10s} {old_n:>12d} {new_n:>12d} {new_n-old_n:>+10d}")
    print(f"  {'WR':>10s} {old_wr:>11.0f}% {new_wr:>11.0f}% {new_wr-old_wr:>+9.0f}%")
    print(f"  {'PnL':>10s} ${old_pnl:>+11,.0f} ${new_pnl:>+11,.0f} ${new_pnl-old_pnl:>+9,.0f}")
