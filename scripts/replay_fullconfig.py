"""
Replay April 15-16 2026 trades with FULL current system config.
Log times = EEST (UTC+3). All analysis in UTC.

Full config:
  1. Macro filter: BTC 7d >= +3% AND 1d > -3% -> block SHORT (FLAT)
  2. News reactor: impact 7-8 -> Profi HOLD (non-systemic). 9-10 -> auto-close
  3. Per-coin optimal params from optimal_config.json (SL_mult, R:R, hold_bars)
  4. Profi controls SL/TP (code doesn't override)
  5. MAX_PENDING = 8, 27 active coins
  6. BAD_COINS = LTC, TRX, JTO
"""

import sqlite3
import json
import re
import csv
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / 'data' / 'crypto' / 'market.db'
LOG_PATH = BASE / 'logs' / 'trader_bybit.log'
CONFIG_PATH = BASE / 'data' / 'crypto' / 'coin_optimization' / 'optimal_config.json'
OUT_DIR = BASE / 'data' / 'crypto' / 'fullconfig_replay'
OUT_DIR.mkdir(exist_ok=True)

# ── Load per-coin optimal config ──
with open(CONFIG_PATH) as f:
    COIN_CFG = json.load(f)

# ── Current Config ──
BAD_COINS = {'LTC', 'TRX', 'JTO'}
# 27 active coins = all coins in optimal_config minus BAD_COINS
ACTIVE_COINS = set(COIN_CFG.keys()) - BAD_COINS
MAX_PENDING = 8
FEE_RATE = 0.0004  # 0.04% per side
EEST_OFFSET = timedelta(hours=3)

# News events on Apr 16 with impact 7-8 (from DB)
# Schwab: impact 8 BULLISH at 14:03 UTC -> triggered NEWS_REACTION at ~14:01 UTC
# Zonda: impact 8 BEARISH at 16:18 UTC -> triggered NEWS_REACTION at ~16:18 UTC
# Under new config: impact 7-8 -> Profi HOLD (assume non-systemic)

def parse_eest_to_utc(ts_str):
    dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
    dt_eest = dt.replace(tzinfo=timezone(EEST_OFFSET))
    return dt_eest.astimezone(timezone.utc)


def get_coin_candles_extended(conn, coin, start_ts, end_ts):
    """Get 1h candles for a coin in a time range."""
    cur = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp",
        (coin, start_ts, end_ts)
    )
    return cur.fetchall()


def compute_atr(candles, period=14):
    """Compute ATR from list of (ts, open, high, low, close) candles."""
    if len(candles) < period + 1:
        # Not enough data, use available
        if len(candles) < 2:
            return (candles[0][2] - candles[0][3]) if candles else 0.01
        period = len(candles) - 1

    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i-1][4]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    # Use last `period` TRs
    return sum(trs[-period:]) / period


def get_btc_macro(conn, utc_dt):
    """Compute BTC 7d and 1d percent change at given time."""
    target_ts = int(utc_dt.timestamp())
    # Get BTC candles for last 8 days
    start_ts = target_ts - 8 * 24 * 3600
    candles = get_coin_candles_extended(conn, 'BTC', start_ts, target_ts)
    if not candles:
        return 0.0, 0.0

    current_close = candles[-1][4]
    ts_7d = target_ts - 168 * 3600
    ts_1d = target_ts - 24 * 3600

    close_7d = None
    close_1d = None
    for c in candles:
        if c[0] <= ts_7d:
            close_7d = c[4]
        if c[0] <= ts_1d:
            close_1d = c[4]

    pct_7d = ((current_close - close_7d) / close_7d * 100) if close_7d else 0.0
    pct_1d = ((current_close - close_1d) / close_1d * 100) if close_1d else 0.0
    return pct_7d, pct_1d


def parse_trades_from_log():
    """Parse FILLED and CLOSE/CLOSED lines for Apr 15-16."""
    fills = []
    closes = []

    with open(LOG_PATH, 'r') as f:
        for line in f:
            if '2026-04-15' not in line and '2026-04-16' not in line:
                continue

            ts_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if not ts_match:
                continue
            ts_str = ts_match.group(1)
            utc_dt = parse_eest_to_utc(ts_str)

            # Parse FILLED lines
            fill_m = re.search(
                r'FILLED #\d+: (LONG|SHORT) (\w+) [\d.]+@\$([0-9.]+) (\d+)x '
                r'SL=\$([0-9.]+) TP=\$([0-9.]+) margin=\$([0-9.]+)',
                line
            )
            if fill_m:
                direction = fill_m.group(1)
                coin = fill_m.group(2)
                entry_price = float(fill_m.group(3))
                leverage = int(fill_m.group(4))
                sl_price = float(fill_m.group(5))
                tp_price = float(fill_m.group(6))
                margin = float(fill_m.group(7))
                fills.append((utc_dt, coin, direction, entry_price, sl_price, tp_price, leverage, margin))
                continue

            # Parse CLOSE lines (TIME_EXIT, NEWS_REACTION, PROFI_EXIT)
            close_m = re.search(
                r'CLOSE (\w+): (\w+) \| ([+-]?[0-9.]+)% lev \| \$([+-]?[0-9.]+)',
                line
            )
            if close_m:
                coin = close_m.group(1)
                reason = close_m.group(2)
                roi = float(close_m.group(3))
                pnl = float(close_m.group(4))
                closes.append((utc_dt, coin, '', 8, roi, pnl, reason))
                continue

            # Parse CLOSED: lines (exchange confirmation)
            closed_m = re.search(
                r'CLOSED: (LONG|SHORT) (\w+) (\d+)x ROI=([+-][0-9.]+)% \$([+-]?[0-9.]+) \[(\w+)\]',
                line
            )
            if closed_m:
                direction = closed_m.group(1)
                coin = closed_m.group(2)
                leverage = int(closed_m.group(3))
                roi = float(closed_m.group(4))
                pnl = float(closed_m.group(5))
                reason = closed_m.group(6)
                closes.append((utc_dt, coin, direction, leverage, roi, pnl, reason))

    return fills, closes


def match_fills_to_closes(fills, closes):
    """Match each FILLED to its first corresponding CLOSE line."""
    trades = []
    close_by_coin = defaultdict(list)
    for c in closes:
        close_by_coin[c[1]].append(c)

    # Sort closes by time
    for coin in close_by_coin:
        close_by_coin[coin].sort(key=lambda x: x[0])

    used_closes = set()

    for fill in fills:
        utc_dt, coin, direction, entry_price, sl, tp, lev, margin = fill
        best = None
        for c in close_by_coin[coin]:
            c_id = id(c)
            if c_id in used_closes:
                continue
            if c[0] >= utc_dt:
                # Prefer CLOSE lines over CLOSED: lines for PnL (CLOSE has the actual lev PnL)
                best = c
                used_closes.add(c_id)
                break

        if best:
            trades.append({
                'entry_utc': utc_dt,
                'exit_utc': best[0],
                'coin': coin,
                'direction': direction,
                'entry_price': entry_price,
                'sl_price': sl,
                'tp_price': tp,
                'leverage': lev,
                'margin': margin,
                'actual_roi': best[4],
                'actual_pnl': best[5],
                'exit_reason': best[6],
            })

    return trades


def simulate_trade_fullconfig(conn, trade):
    """Simulate trade with full config: per-coin R:R, macro filter, news hold."""
    result = dict(trade)
    coin = trade['coin']
    direction = trade['direction']
    entry_utc = trade['entry_utc']
    entry_price = trade['entry_price']
    lev = trade['leverage']
    margin = trade['margin']
    notional = margin * lev

    # ── Filter 1: BAD_COINS ──
    if coin in BAD_COINS:
        result['sim_status'] = 'BLOCKED_BAD_COIN'
        result['sim_pnl'] = 0.0
        result['sim_roi'] = 0.0
        result['sim_exit_reason'] = ''
        result['block_reason'] = f'{coin} in BAD_COINS'
        return result

    # ── Filter 2: Coin in active list ──
    if coin not in ACTIVE_COINS:
        result['sim_status'] = 'BLOCKED_COIN_FILTER'
        result['sim_pnl'] = 0.0
        result['sim_roi'] = 0.0
        result['sim_exit_reason'] = ''
        result['block_reason'] = f'{coin} not in 27 active coins'
        return result

    # ── Filter 3: MACRO FILTER (shorts only) ──
    if direction == 'SHORT':
        pct_7d, pct_1d = get_btc_macro(conn, entry_utc)
        if pct_7d >= 3.0 and pct_1d > -3.0:
            result['sim_status'] = 'BLOCKED_MACRO'
            result['sim_pnl'] = 0.0
            result['sim_roi'] = 0.0
            result['sim_exit_reason'] = ''
            result['block_reason'] = f'MACRO: BTC 7d={pct_7d:+.1f}% 1d={pct_1d:+.1f}% -> no SHORT'
            return result

    # ── Filter 4: NEWS_REACTION -> Profi HOLD for impact 7-8 ──
    is_news_hold = trade['exit_reason'] == 'NEWS_REACTION'

    # ── Get per-coin optimal params ──
    cfg = COIN_CFG.get(coin, {'sl_mult': 0.8, 'rr': 2.0, 'hold_bars': 3})
    sl_mult = cfg['sl_mult']
    rr = cfg['rr']
    hold_bars_4h = cfg['hold_bars']
    hold_bars_1h = hold_bars_4h * 4  # convert 4h units to 1h

    # ── Compute new SL/TP using per-coin optimal params ──
    entry_ts = int(entry_utc.timestamp())
    entry_hour_ts = entry_ts - (entry_ts % 3600)

    # Get candles for ATR calculation (14 bars before entry)
    atr_start = entry_hour_ts - 15 * 3600
    atr_candles = get_coin_candles_extended(conn, coin, atr_start, entry_hour_ts)
    atr = compute_atr(atr_candles, 14)

    # Compute SL and TP distances
    sl_dist = sl_mult * atr
    tp_dist = sl_dist * rr

    if direction == 'LONG':
        new_sl = entry_price - sl_dist
        new_tp = entry_price + tp_dist
    else:  # SHORT
        new_sl = entry_price + sl_dist
        new_tp = entry_price - tp_dist

    # ── Simulate bar-by-bar ──
    # Get candles from entry to entry + hold_bars_1h + 1 (for safety)
    sim_end_ts = entry_hour_ts + (hold_bars_1h + 2) * 3600
    candles = get_coin_candles_extended(conn, coin, entry_hour_ts, sim_end_ts)

    sim_exit_price = None
    sim_exit_reason = None
    bars_held = 0

    for candle in candles:
        c_ts, c_open, c_high, c_low, c_close = candle
        if c_ts < entry_hour_ts:
            continue

        # For entry candle, skip it (already filled mid-candle)
        if c_ts == entry_hour_ts:
            # Only check if significant time within candle
            pass  # We'll check from the next candle to be conservative
            # Actually, check from next full candle
            bars_held = 0
            continue

        bars_held += 1

        if direction == 'LONG':
            # Check SL first (conservative - SL triggers before TP on same candle)
            if c_low <= new_sl:
                sim_exit_price = new_sl
                sim_exit_reason = 'STOP_LOSS'
                break
            if c_high >= new_tp:
                sim_exit_price = new_tp
                sim_exit_reason = 'TARGET_HIT'
                break
        else:  # SHORT
            if c_high >= new_sl:
                sim_exit_price = new_sl
                sim_exit_reason = 'STOP_LOSS'
                break
            if c_low <= new_tp:
                sim_exit_price = new_tp
                sim_exit_reason = 'TARGET_HIT'
                break

        # Check time limit
        if bars_held >= hold_bars_1h:
            sim_exit_price = c_close
            sim_exit_reason = 'TIME_EXIT'
            break

    if sim_exit_price is None:
        # Use last available candle close
        if candles:
            sim_exit_price = candles[-1][4]
            sim_exit_reason = 'NO_DATA_EXIT'
        else:
            sim_exit_price = entry_price
            sim_exit_reason = 'NO_DATA'

    # Compute PnL
    if direction == 'LONG':
        raw_roi = (sim_exit_price - entry_price) / entry_price
    else:
        raw_roi = (entry_price - sim_exit_price) / entry_price

    lev_roi = raw_roi * lev
    fee_total = notional * FEE_RATE * 2
    pnl = notional * raw_roi - fee_total

    result['sim_status'] = 'EXECUTED'
    result['sim_exit_price'] = round(sim_exit_price, 6)
    result['sim_exit_reason'] = sim_exit_reason
    result['sim_roi'] = round(lev_roi * 100, 2)
    result['sim_pnl'] = round(pnl, 2)
    result['new_sl'] = round(new_sl, 6)
    result['new_tp'] = round(new_tp, 6)
    result['atr'] = round(atr, 6)
    result['opt_sl_mult'] = sl_mult
    result['opt_rr'] = rr
    result['opt_hold_1h'] = hold_bars_1h
    result['news_hold'] = is_news_hold
    result['block_reason'] = ''

    return result


def main():
    conn = sqlite3.connect(str(DB_PATH))

    fills, closes = parse_trades_from_log()

    # Define Apr 15-16 range in UTC (EEST 00:00 = UTC 21:00 prev day)
    apr15_start = datetime(2026, 4, 14, 21, 0, 0, tzinfo=timezone.utc)
    apr17_start = datetime(2026, 4, 16, 21, 0, 0, tzinfo=timezone.utc)

    fills_in_range = [f for f in fills if apr15_start <= f[0] < apr17_start]
    trades = match_fills_to_closes(fills_in_range, closes)

    print(f"Parsed {len(fills_in_range)} fills, matched to {len(trades)} complete trades\n")

    # ── Identify carryover closes (trades opened before Apr 15) ──
    all_close_times = {(t['coin'], t['exit_utc']) for t in trades}
    carryover_closes = []
    for c in closes:
        if apr15_start <= c[0] < apr17_start:
            matched = any(
                t['coin'] == c[1] and abs((t['exit_utc'] - c[0]).total_seconds()) < 60
                for t in trades
            )
            if not matched:
                # Check it's not a CLOSED line that duplicates a CLOSE line
                # Only count distinct close events
                carryover_closes.append(c)

    # Deduplicate carryovers (keep CLOSE over CLOSED for same coin/time)
    seen = set()
    unique_carry = []
    for c in carryover_closes:
        key = (c[1], c[0].strftime('%Y-%m-%d %H:%M'))
        if key not in seen:
            seen.add(key)
            unique_carry.append(c)
    carryover_closes = unique_carry

    # ── Simulate each trade ──
    results = []
    for trade in trades:
        r = simulate_trade_fullconfig(conn, trade)
        results.append(r)

    # ── Analysis ──
    # Group by day
    for day_label, day_date in [('Apr 15', '2026-04-15'), ('Apr 16', '2026-04-16')]:
        day_start = parse_eest_to_utc(f'{day_date} 00:00:00')
        day_end = parse_eest_to_utc(f'{day_date} 23:59:59')

        day_trades = [r for r in results if day_start <= r['entry_utc'] <= day_end]

        # Actual stats
        actual_pnl = sum(t['actual_pnl'] for t in day_trades)
        actual_wins = sum(1 for t in day_trades if t['actual_pnl'] > 0)
        actual_n = len(day_trades)

        # Sim stats
        executed = [r for r in day_trades if r.get('sim_status') == 'EXECUTED']
        blocked_bad = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_BAD_COIN']
        blocked_macro = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_MACRO']
        blocked_coin = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_COIN_FILTER']

        sim_pnl = sum(r['sim_pnl'] for r in executed)
        sim_wins = sum(1 for r in executed if r['sim_pnl'] > 0)
        sim_n = len(executed)

        # News holds
        news_holds = [r for r in executed if r.get('news_hold')]

        # Trades with changed exit due to per-coin R:R
        changed_exit = [r for r in executed
                        if r.get('sim_exit_reason') != r.get('exit_reason')
                        and not r.get('news_hold')]

        wr_actual = (actual_wins / actual_n * 100) if actual_n else 0
        wr_sim = (sim_wins / sim_n * 100) if sim_n else 0

        print(f"{'='*65}")
        print(f"  {day_label} ({day_date})  [EEST]")
        print(f"{'='*65}")
        print(f"\n  {'Metric':<20} {'Actual':>10} {'Simulated':>12} {'Delta':>10}")
        print(f"  {'-'*52}")
        print(f"  {'Trades':<20} {actual_n:>10} {sim_n:>12} {sim_n - actual_n:>+10}")
        print(f"  {'Wins':<20} {actual_wins:>10} {sim_wins:>12} {sim_wins - actual_wins:>+10}")
        print(f"  {'WR':<20} {wr_actual:>9.0f}% {wr_sim:>11.0f}%")
        print(f"  {'PnL':<20} ${actual_pnl:>+9.2f} ${sim_pnl:>+11.2f} ${sim_pnl - actual_pnl:>+9.2f}")

        if blocked_bad:
            print(f"\n  Blocked BAD_COINS ({len(blocked_bad)}):")
            for t in blocked_bad:
                print(f"    {t['direction']:5} {t['coin']:6} actual=${t['actual_pnl']:+.2f}")
        if blocked_macro:
            print(f"\n  Blocked MACRO ({len(blocked_macro)}):")
            for t in blocked_macro:
                print(f"    {t['direction']:5} {t['coin']:6} actual=${t['actual_pnl']:+.2f} | {t['block_reason']}")
        if blocked_coin:
            print(f"\n  Blocked COIN_FILTER ({len(blocked_coin)}):")
            for t in blocked_coin:
                print(f"    {t['direction']:5} {t['coin']:6} actual=${t['actual_pnl']:+.2f}")
        if news_holds:
            print(f"\n  NEWS HOLD (impact 7-8, Profi keeps) ({len(news_holds)}):")
            actual_news_pnl = sum(r['actual_pnl'] for r in news_holds)
            sim_news_pnl = sum(r['sim_pnl'] for r in news_holds)
            print(f"    Actual (closed early): ${actual_news_pnl:+.2f}")
            print(f"    Simulated (held to SL/TP/TIME): ${sim_news_pnl:+.2f}")
            print(f"    Delta: ${sim_news_pnl - actual_news_pnl:+.2f}")
            for r in news_holds:
                print(f"    {r['direction']:5} {r['coin']:6} actual=${r['actual_pnl']:+.2f} -> sim=${r['sim_pnl']:+.2f} [{r['sim_exit_reason']}]")
        if changed_exit:
            print(f"\n  Changed exit (per-coin R:R) ({len(changed_exit)}):")
            for r in changed_exit:
                old_exit = r['exit_reason']
                new_exit = r['sim_exit_reason']
                print(f"    {r['direction']:5} {r['coin']:6} {old_exit} -> {new_exit} | actual=${r['actual_pnl']:+.2f} sim=${r['sim_pnl']:+.2f} | RR={r['opt_rr']}")
        print()

    # ── Per-coin comparison ──
    print(f"\n{'='*65}")
    print(f"  PER-COIN COMPARISON")
    print(f"{'='*65}")
    coin_actual = defaultdict(lambda: {'pnl': 0, 'n': 0})
    coin_sim = defaultdict(lambda: {'pnl': 0, 'n': 0})

    for r in results:
        coin_actual[r['coin']]['pnl'] += r['actual_pnl']
        coin_actual[r['coin']]['n'] += 1
        if r.get('sim_status') == 'EXECUTED':
            coin_sim[r['coin']]['pnl'] += r['sim_pnl']
            coin_sim[r['coin']]['n'] += 1

    all_coins = sorted(set(list(coin_actual.keys()) + list(coin_sim.keys())))
    print(f"\n  {'Coin':<8} {'Actual PnL':>12} {'Sim PnL':>12} {'Delta':>10} {'N_act':>6} {'N_sim':>6} {'R:R':>5}")
    print(f"  {'-'*60}")
    for coin in all_coins:
        a = coin_actual[coin]
        s = coin_sim[coin]
        cfg_rr = COIN_CFG.get(coin, {}).get('rr', 'N/A')
        delta = s['pnl'] - a['pnl']
        print(f"  {coin:<8} ${a['pnl']:>+10.2f} ${s['pnl']:>+10.2f} ${delta:>+8.2f} {a['n']:>6} {s['n']:>6} {cfg_rr:>5}")

    # ── Combined summary ──
    all_actual_pnl = sum(r['actual_pnl'] for r in results)
    all_executed = [r for r in results if r.get('sim_status') == 'EXECUTED']
    all_sim_pnl = sum(r['sim_pnl'] for r in all_executed)
    all_blocked = [r for r in results if 'BLOCKED' in r.get('sim_status', '')]
    all_blocked_pnl = sum(r['actual_pnl'] for r in all_blocked)

    print(f"\n{'='*65}")
    print(f"  COMBINED 2-DAY SUMMARY")
    print(f"{'='*65}")
    print(f"  Actual total PnL:    ${all_actual_pnl:+.2f} ({len(results)} trades)")
    print(f"  Simulated PnL:       ${all_sim_pnl:+.2f} ({len(all_executed)} trades)")
    print(f"  Blocked trades:      {len(all_blocked)} (actual PnL: ${all_blocked_pnl:+.2f})")
    print(f"  DELTA:               ${all_sim_pnl - all_actual_pnl:+.2f}")
    print(f"  Blocked savings:     ${-all_blocked_pnl:+.2f}")

    # ── Write CSV ──
    csv_path = OUT_DIR / 'comparison.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_utc', 'coin', 'direction', 'leverage', 'entry_price',
            'actual_sl', 'actual_tp', 'margin',
            'actual_roi%', 'actual_pnl', 'actual_exit',
            'sim_status', 'new_sl', 'new_tp', 'opt_sl_mult', 'opt_rr', 'opt_hold_1h', 'atr',
            'sim_roi%', 'sim_pnl', 'sim_exit',
            'block_reason', 'news_hold'
        ])
        for r in results:
            writer.writerow([
                r['entry_utc'].strftime('%Y-%m-%d %H:%M UTC'),
                r['coin'], r['direction'], r['leverage'], r['entry_price'],
                r['sl_price'], r['tp_price'], r['margin'],
                r['actual_roi'], r['actual_pnl'], r['exit_reason'],
                r.get('sim_status', ''),
                r.get('new_sl', ''), r.get('new_tp', ''),
                r.get('opt_sl_mult', ''), r.get('opt_rr', ''),
                r.get('opt_hold_1h', ''), r.get('atr', ''),
                r.get('sim_roi', ''), r.get('sim_pnl', ''),
                r.get('sim_exit_reason', ''), r.get('block_reason', ''),
                r.get('news_hold', False)
            ])
    print(f"\nCSV saved: {csv_path}")

    # ── Write summary.md ──
    md_path = OUT_DIR / 'summary.md'
    with open(md_path, 'w') as f:
        f.write("# Full Config Replay: April 15-16 2026\n\n")
        f.write("## Config Applied\n\n")
        f.write("| Parameter | Value |\n|---|---|\n")
        f.write("| MAX_PENDING | 8 |\n")
        f.write("| Active coins | 27 (excl LTC, TRX, JTO) |\n")
        f.write("| Per-coin R:R | From optimal_config.json |\n")
        f.write("| Per-coin SL mult | From optimal_config.json |\n")
        f.write("| Per-coin hold bars | From optimal_config.json (4h -> 1h) |\n")
        f.write("| Macro filter | BTC 7d>=+3% AND 1d>-3% blocks SHORT |\n")
        f.write("| News reactor | Impact 7-8: Profi HOLD, 9-10: auto-close |\n\n")

        # Per-day summary tables
        for day_label, day_date in [('Apr 15', '2026-04-15'), ('Apr 16', '2026-04-16')]:
            day_start = parse_eest_to_utc(f'{day_date} 00:00:00')
            day_end = parse_eest_to_utc(f'{day_date} 23:59:59')
            day_trades = [r for r in results if day_start <= r['entry_utc'] <= day_end]

            a_pnl = sum(t['actual_pnl'] for t in day_trades)
            a_n = len(day_trades)
            a_w = sum(1 for t in day_trades if t['actual_pnl'] > 0)
            ex = [r for r in day_trades if r.get('sim_status') == 'EXECUTED']
            s_pnl = sum(r['sim_pnl'] for r in ex)
            s_n = len(ex)
            s_w = sum(1 for r in ex if r['sim_pnl'] > 0)
            blk = [r for r in day_trades if 'BLOCKED' in r.get('sim_status', '')]

            f.write(f"### {day_label}\n\n")
            f.write("| Metric | Actual | Simulated | Delta |\n|---|---|---|---|\n")
            f.write(f"| Trades | {a_n} | {s_n} | {s_n - a_n:+d} |\n")
            f.write(f"| Wins | {a_w} | {s_w} | {s_w - a_w:+d} |\n")
            wr_a = a_w / max(a_n, 1) * 100
            wr_s = s_w / max(s_n, 1) * 100
            f.write(f"| WR | {wr_a:.0f}% | {wr_s:.0f}% | |\n")
            f.write(f"| PnL | ${a_pnl:+.2f} | ${s_pnl:+.2f} | ${s_pnl - a_pnl:+.2f} |\n")
            f.write(f"| Blocked | - | {len(blk)} | |\n\n")

        # Combined
        f.write("### Combined\n\n")
        f.write(f"- **Actual**: ${all_actual_pnl:+.2f} ({len(results)} trades)\n")
        f.write(f"- **Simulated**: ${all_sim_pnl:+.2f} ({len(all_executed)} trades)\n")
        f.write(f"- **Delta**: ${all_sim_pnl - all_actual_pnl:+.2f}\n")
        f.write(f"- **Blocked**: {len(all_blocked)} trades (avoided ${all_blocked_pnl:+.2f})\n\n")

        # Per-coin
        f.write("### Per-Coin Comparison\n\n")
        f.write("| Coin | Actual PnL | Sim PnL | Delta | R:R | SL_mult |\n")
        f.write("|---|---|---|---|---|---|\n")
        for coin in all_coins:
            a = coin_actual[coin]
            s = coin_sim[coin]
            cfg = COIN_CFG.get(coin, {})
            delta = s['pnl'] - a['pnl']
            f.write(f"| {coin} | ${a['pnl']:+.2f} | ${s['pnl']:+.2f} | ${delta:+.2f} | {cfg.get('rr', 'N/A')} | {cfg.get('sl_mult', 'N/A')} |\n")

    print(f"Summary saved: {md_path}")
    conn.close()


if __name__ == '__main__':
    main()
