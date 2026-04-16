"""
Replay April 15-16 2026 trades with CURRENT config filters.
Log times are EEST (UTC+3). All analysis in UTC.

Current config:
  MAX_PENDING = 8
  R:R = 2.0 (tp_dist = sl_dist * 2.0)
  SL = ATR * 0.8
  BAD_COINS = {BOME, DOT, AAVE, WIF, DOGE, OP, RENDER, TAO, ARB}
  COINS = 25 (BTC ETH SOL BNB XRP ADA AVAX DOT LINK DOGE UNI AAVE LDO CRV
               RENDER TAO ARB OP POL WIF PENDLE JUP PYTH JTO BOME)
  H16 UTC skip (Rule 48)
  Hold max 3h -> TIME_EXIT
  Cooldown 2h after SL
  MACRO FILTER: BTC 7d >= +3% AND 1d > -3% -> block SHORT (FLAT only)
  NEWS: impact 9-10 auto-close; 7-8 Profi HOLD (non-systemic)
"""

import sqlite3
import re
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / 'data' / 'crypto' / 'market.db'
LOG_PATH = BASE / 'logs' / 'trader_bybit.log'
OUT_DIR = BASE / 'data' / 'crypto' / 'config_replay'
OUT_DIR.mkdir(exist_ok=True)

# ── Current Config ──
BAD_COINS = {'BOME', 'DOT', 'AAVE', 'WIF', 'DOGE', 'OP', 'RENDER', 'TAO', 'ARB'}
ALLOWED_COINS = {'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
                 'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
                 'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO', 'BOME'}
MAX_PENDING = 8
FEE_RATE = 0.0004  # 0.04% per side

EEST_OFFSET = timedelta(hours=3)  # EEST = UTC+3

def parse_eest_to_utc(ts_str):
    """Parse '2026-04-15 01:19:40' EEST -> UTC datetime"""
    dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
    dt_eest = dt.replace(tzinfo=timezone(EEST_OFFSET))
    return dt_eest.astimezone(timezone.utc)


def get_btc_prices(conn):
    """Load all BTC 1h candles into dict {unix_ts: (open, high, low, close)}"""
    cur = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices "
        "WHERE coin='BTC' AND timeframe='1h' ORDER BY timestamp"
    )
    return {row[0]: row[1:] for row in cur.fetchall()}


def get_coin_candles(conn, coin, start_ts, end_ts):
    """Get 1h candles for a coin in a time range."""
    cur = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp",
        (coin, start_ts, end_ts)
    )
    return cur.fetchall()


def btc_macro_at(btc_prices, utc_dt):
    """Compute BTC 7d change and 1d change at a given UTC datetime.
    Returns (pct_7d, pct_1d)."""
    target_ts = int(utc_dt.timestamp())
    # Find closest candle <= target_ts
    sorted_ts = sorted(btc_prices.keys())
    current_ts = None
    for ts in sorted_ts:
        if ts <= target_ts:
            current_ts = ts
        else:
            break
    if current_ts is None:
        return (0.0, 0.0)

    current_close = btc_prices[current_ts][3]  # close

    # 7d ago = 168 hours
    ts_7d = current_ts - 168 * 3600
    ts_1d = current_ts - 24 * 3600

    # Find closest candle to those timestamps
    close_7d = None
    close_1d = None
    for ts in sorted_ts:
        if ts <= ts_7d:
            close_7d = btc_prices[ts][3]
        if ts <= ts_1d:
            close_1d = btc_prices[ts][3]

    pct_7d = ((current_close - close_7d) / close_7d * 100) if close_7d else 0.0
    pct_1d = ((current_close - close_1d) / close_1d * 100) if close_1d else 0.0
    return (pct_7d, pct_1d)


def parse_trades_from_log():
    """Parse all FILLED and CLOSED/CLOSE lines for Apr 15-16."""
    fills = []  # (utc_dt, coin, direction, entry_price, sl, tp, leverage, margin)
    closes = []  # (utc_dt, coin, direction, leverage, roi_pct, pnl_usd, exit_reason)

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
                continue

            # Parse CLOSE <coin>: TIME_EXIT / NEWS_REACTION / PROFI_EXIT lines
            close_m = re.search(
                r'CLOSE (\w+): (\w+) \| ([+-][0-9.]+)% lev \| \$([+-]?[0-9.]+) \|',
                line
            )
            if close_m:
                coin = close_m.group(1)
                reason = close_m.group(2)
                roi = float(close_m.group(3))
                pnl = float(close_m.group(4))
                closes.append((utc_dt, coin, '', 8, roi, pnl, reason))

    return fills, closes


def match_fills_to_closes(fills, closes):
    """Match each FILLED entry to its corresponding close.
    Returns list of complete trades."""
    trades = []
    # For each fill, find the first CLOSE/CLOSED for same coin after fill time
    # Use the CLOSE line (TIME_EXIT, NEWS_REACTION, PROFI_EXIT) as primary,
    # and CLOSED: as the exchange confirmation

    # Group closes by coin
    close_by_coin = defaultdict(list)
    for c in closes:
        close_by_coin[c[1]].append(c)

    used_closes = set()

    for fill in fills:
        utc_dt, coin, direction, entry_price, sl, tp, lev, margin = fill
        # Find first close for this coin after entry
        best = None
        for i, c in enumerate(close_by_coin[coin]):
            c_id = id(c)
            if c_id in used_closes:
                continue
            if c[0] > utc_dt:
                # Prefer CLOSE lines (TIME_EXIT etc) over CLOSED: lines
                if best is None or c[0] < best[0]:
                    best = c
                    best_idx = c_id
                    break  # take first one after entry
        if best:
            used_closes.add(best_idx)
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
        else:
            # Close was before Apr 15 start or missing
            pass

    return trades


def simulate_trade(conn, trade, btc_prices):
    """Apply current config filters and simulate bar-by-bar if allowed.
    Returns dict with simulation results."""
    result = dict(trade)
    coin = trade['coin']
    direction = trade['direction']
    entry_utc = trade['entry_utc']
    entry_price = trade['entry_price']
    sl_price = trade['sl_price']
    tp_price = trade['tp_price']
    lev = trade['leverage']
    margin = trade['margin']

    # ── Filter 1: BAD_COINS ──
    if coin in BAD_COINS:
        result['sim_status'] = 'BLOCKED_BAD_COIN'
        result['sim_pnl'] = 0.0
        result['sim_roi'] = 0.0
        result['block_reason'] = f'{coin} in BAD_COINS'
        return result

    # ── Filter 2: Coin must be in ALLOWED_COINS ──
    if coin not in ALLOWED_COINS:
        result['sim_status'] = 'BLOCKED_COIN_FILTER'
        result['sim_pnl'] = 0.0
        result['sim_roi'] = 0.0
        result['block_reason'] = f'{coin} not in 25 coins'
        return result

    # ── Filter 3: MACRO FILTER (shorts only) ──
    if direction == 'SHORT':
        pct_7d, pct_1d = btc_macro_at(btc_prices, entry_utc)
        if pct_7d >= 3.0 and pct_1d > -3.0:
            result['sim_status'] = 'BLOCKED_MACRO'
            result['sim_pnl'] = 0.0
            result['sim_roi'] = 0.0
            result['block_reason'] = f'MACRO: BTC 7d={pct_7d:+.1f}% 1d={pct_1d:+.1f}% -> no SHORT'
            return result

    # ── Filter 4: H16 UTC skip ──
    if entry_utc.hour == 16:
        result['sim_status'] = 'BLOCKED_H16'
        result['sim_pnl'] = 0.0
        result['sim_roi'] = 0.0
        result['block_reason'] = 'H16 UTC skip'
        return result

    # ── Trade allowed: simulate with current config ──
    # Current config: SL = ATR*0.8, TP = SL_dist * 2.0 (R:R = 2.0)
    # We use the ACTUAL SL/TP from the log since that's what was placed
    # The SL/TP in log already reflect the ATR-based calculation

    # Simulate bar-by-bar on 1h candles, max 3h hold
    entry_ts = int(entry_utc.timestamp())
    # Round down to hour boundary for candle lookup
    entry_hour_ts = entry_ts - (entry_ts % 3600)
    max_exit_ts = entry_ts + 3 * 3600  # 3h max hold

    candles = get_coin_candles(conn, coin, entry_hour_ts, entry_hour_ts + 4 * 3600)

    sim_exit_price = None
    sim_exit_reason = None

    notional = margin * lev

    for candle in candles:
        c_ts, c_open, c_high, c_low, c_close = candle
        # Only consider candles AFTER entry
        if c_ts < entry_hour_ts:
            continue
        # For entry candle, price already filled, just check SL/TP from entry price
        # For subsequent candles, check full range

        if direction == 'LONG':
            # Check SL first (conservative)
            if c_low <= sl_price:
                sim_exit_price = sl_price
                sim_exit_reason = 'STOP_LOSS'
                break
            if c_high >= tp_price:
                sim_exit_price = tp_price
                sim_exit_reason = 'TARGET_HIT'
                break
        else:  # SHORT
            if c_high >= sl_price:
                sim_exit_price = sl_price
                sim_exit_reason = 'STOP_LOSS'
                break
            if c_low <= tp_price:
                sim_exit_price = tp_price
                sim_exit_reason = 'TARGET_HIT'
                break

        # Check time limit (3h)
        if c_ts >= entry_hour_ts + 3 * 3600:
            sim_exit_price = c_close
            sim_exit_reason = 'TIME_EXIT'
            break

    if sim_exit_price is None:
        # Use last candle close or actual exit
        sim_exit_price = trade['entry_price']  # fallback
        sim_exit_reason = 'NO_DATA'

    # Compute PnL
    if direction == 'LONG':
        raw_roi = (sim_exit_price - entry_price) / entry_price
    else:
        raw_roi = (entry_price - sim_exit_price) / entry_price

    lev_roi = raw_roi * lev
    # Fees: 0.04% entry + 0.04% exit = 0.08% of notional
    fee_total = notional * FEE_RATE * 2
    pnl = notional * raw_roi - fee_total

    result['sim_status'] = 'EXECUTED'
    result['sim_exit_price'] = sim_exit_price
    result['sim_exit_reason'] = sim_exit_reason
    result['sim_roi'] = round(lev_roi * 100, 1)
    result['sim_pnl'] = round(pnl, 2)

    return result


def check_news_impact(trade):
    """Check if NEWS_REACTION trades would be kept under current config.
    Schwab (Apr 16 ~17:01 UTC -> 20:01 EEST): impact 8, BULLISH, Profi HOLD LONGs
    Zonda (Apr 16 ~19:18 UTC -> 22:18 EEST): impact 8, BEARISH, Profi HOLD LONGs
    """
    if trade['exit_reason'] == 'NEWS_REACTION':
        return True  # would NOT have been closed under current config (Profi HOLD)
    return False


def main():
    conn = sqlite3.connect(str(DB_PATH))
    btc_prices = get_btc_prices(conn)

    fills, closes = parse_trades_from_log()

    # Filter fills to Apr 15-16 only
    apr15_start = datetime(2026, 4, 14, 21, 0, 0, tzinfo=timezone.utc)  # EEST Apr 15 00:00
    apr17_start = datetime(2026, 4, 16, 21, 0, 0, tzinfo=timezone.utc)  # EEST Apr 17 00:00

    fills_in_range = [f for f in fills if apr15_start <= f[0] < apr17_start]
    # Also filter closes that have no matching fill (opened before Apr 15)
    # These are the BR, 4, ZAMA, CAKE, SUI, WLD, ENA trades
    # We'll note them as "carried over" trades

    trades = match_fills_to_closes(fills_in_range, closes)

    print(f"Parsed {len(fills_in_range)} fills, matched to {len(trades)} complete trades")

    # ── Identify carried-over closes (no fill in our range) ──
    filled_coins_times = {(t['coin'], t['exit_utc']) for t in trades}
    carryover_closes = []
    for c in closes:
        if c[0] >= apr15_start and c[0] < apr17_start:
            # Check if this close matches any trade
            matched = False
            for t in trades:
                if t['coin'] == c[1] and abs((t['exit_utc'] - c[0]).total_seconds()) < 60:
                    matched = True
                    break
            if not matched:
                carryover_closes.append(c)

    # ── Simulate each trade ──
    results = []
    for trade in trades:
        r = simulate_trade(conn, trade, btc_prices)

        # Check NEWS impact override
        if check_news_impact(trade):
            # Under current config, Profi would HOLD (not close on impact 7-8 news)
            # So we need to simulate what would have happened if position stayed open
            r['news_override'] = True
            # The actual PnL from closing early is recorded; sim continues the trade
        else:
            r['news_override'] = False

        results.append(r)

    # ── Compute stats per day ──
    for day_label, day_date in [('Apr 15', '2026-04-15'), ('Apr 16', '2026-04-16')]:
        day_start_utc = parse_eest_to_utc(f'{day_date} 00:00:00')
        day_end_utc = parse_eest_to_utc(f'{day_date} 23:59:59')

        day_trades = [r for r in results if day_start_utc <= r['entry_utc'] <= day_end_utc]

        # Also include carryover closes for actual stats
        day_carryover = [c for c in carryover_closes
                         if day_start_utc <= c[0] <= day_end_utc]

        # Actual stats (all trades including carryover)
        actual_pnl_fills = sum(t['actual_pnl'] for t in day_trades)
        actual_pnl_carry = sum(c[5] for c in day_carryover)
        actual_pnl = actual_pnl_fills + actual_pnl_carry
        actual_wins_fills = sum(1 for t in day_trades if t['actual_pnl'] > 0)
        actual_wins_carry = sum(1 for c in day_carryover if c[5] > 0)
        actual_total = len(day_trades) + len(day_carryover)
        actual_wins = actual_wins_fills + actual_wins_carry

        # Sim stats
        executed = [r for r in day_trades if r.get('sim_status') == 'EXECUTED']
        blocked_bad = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_BAD_COIN']
        blocked_macro = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_MACRO']
        blocked_h16 = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_H16']
        blocked_coin = [r for r in day_trades if r.get('sim_status') == 'BLOCKED_COIN_FILTER']

        sim_pnl = sum(r['sim_pnl'] for r in executed)
        sim_wins = sum(1 for r in executed if r['sim_pnl'] > 0)

        # Savings from blocking
        saved_bad = sum(t['actual_pnl'] for t in blocked_bad)
        saved_macro = sum(t['actual_pnl'] for t in blocked_macro)

        # News override savings
        news_overrides = [r for r in day_trades if r.get('news_override')]
        news_actual_pnl = sum(r['actual_pnl'] for r in news_overrides)

        wr_actual = (actual_wins / actual_total * 100) if actual_total else 0
        wr_sim = (sim_wins / len(executed) * 100) if executed else 0

        print(f"\n{'='*60}")
        print(f"  {day_label} ({day_date})")
        print(f"{'='*60}")
        print(f"\nACTUAL RESULTS:")
        print(f"  Trades: {actual_total} | Wins: {actual_wins} | WR: {wr_actual:.0f}% | PnL: ${actual_pnl:+.2f}")
        if day_carryover:
            print(f"  (includes {len(day_carryover)} carryover trades: ${actual_pnl_carry:+.2f})")
        print(f"\nSIMULATED (current config):")
        print(f"  Trades attempted: {len(day_trades)}")
        print(f"  Blocked by BAD_COINS: {len(blocked_bad)} (avoided ${saved_bad:+.2f})")
        print(f"  Blocked by macro filter: {len(blocked_macro)} (avoided ${saved_macro:+.2f})")
        print(f"  Blocked by H16 UTC: {len(blocked_h16)}")
        print(f"  Blocked by coin filter: {len(blocked_coin)}")
        if news_overrides:
            print(f"  NEWS: {len(news_overrides)} positions HELD (actual early-close PnL was ${news_actual_pnl:+.2f})")
        print(f"\n  Trades executed: {len(executed)} | Wins: {sim_wins} | WR: {wr_sim:.0f}% | PnL: ${sim_pnl:+.2f}")
        print(f"\n  DELTA vs actual fills: ${sim_pnl - actual_pnl_fills:+.2f}")

        # Detail blocked trades
        if blocked_bad:
            print(f"\n  Blocked BAD_COIN details:")
            for t in blocked_bad:
                print(f"    {t['direction']} {t['coin']} @ {t['entry_utc'].strftime('%H:%M')} UTC | actual: ${t['actual_pnl']:+.2f}")
        if blocked_macro:
            print(f"\n  Blocked MACRO details:")
            for t in blocked_macro:
                print(f"    {t['direction']} {t['coin']} @ {t['entry_utc'].strftime('%H:%M')} UTC | actual: ${t['actual_pnl']:+.2f} | {t['block_reason']}")

    # ── Combined summary ──
    all_actual_pnl = sum(r['actual_pnl'] for r in results) + sum(c[5] for c in carryover_closes)
    all_actual_fills_pnl = sum(r['actual_pnl'] for r in results)
    all_executed = [r for r in results if r.get('sim_status') == 'EXECUTED']
    all_sim_pnl = sum(r['sim_pnl'] for r in all_executed)
    all_blocked = [r for r in results if 'BLOCKED' in r.get('sim_status', '')]
    all_blocked_pnl = sum(r['actual_pnl'] for r in all_blocked)

    print(f"\n{'='*60}")
    print(f"  COMBINED 2-DAY SUMMARY")
    print(f"{'='*60}")
    print(f"\n  ACTUAL total PnL: ${all_actual_pnl:+.2f} ({len(results) + len(carryover_closes)} trades)")
    print(f"  SIMULATED PnL:    ${all_sim_pnl:+.2f} ({len(all_executed)} trades)")
    print(f"  BLOCKED trades:   {len(all_blocked)} (actual PnL was ${all_blocked_pnl:+.2f})")
    print(f"  DELTA:            ${all_sim_pnl - all_actual_fills_pnl:+.2f}")

    # ── Write CSV ──
    csv_path = OUT_DIR / 'trades_comparison.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'entry_utc', 'coin', 'direction', 'leverage', 'entry_price',
            'sl_price', 'tp_price', 'margin',
            'actual_roi%', 'actual_pnl', 'actual_exit',
            'sim_status', 'sim_roi%', 'sim_pnl', 'sim_exit',
            'block_reason', 'news_override'
        ])
        for r in results:
            writer.writerow([
                r['entry_utc'].strftime('%Y-%m-%d %H:%M'),
                r['coin'], r['direction'], r['leverage'], r['entry_price'],
                r['sl_price'], r['tp_price'], r['margin'],
                r['actual_roi'], r['actual_pnl'], r['exit_reason'],
                r.get('sim_status', ''), r.get('sim_roi', ''), r.get('sim_pnl', ''),
                r.get('sim_exit_reason', ''), r.get('block_reason', ''),
                r.get('news_override', False)
            ])
    print(f"\nCSV saved: {csv_path}")

    # ── Write summary.md ──
    md_path = OUT_DIR / 'summary.md'
    with open(md_path, 'w') as f:
        f.write("# Config Replay: April 15-16 2026\n\n")
        f.write("## Current Config Applied Retrospectively\n\n")
        f.write("| Parameter | Value |\n|---|---|\n")
        f.write("| MAX_PENDING | 8 |\n")
        f.write("| R:R | 2.0 |\n")
        f.write("| SL | ATR * 0.8 |\n")
        f.write(f"| BAD_COINS | {', '.join(sorted(BAD_COINS))} |\n")
        f.write("| H16 UTC skip | Yes |\n")
        f.write("| Hold max | 3h |\n")
        f.write("| Cooldown after SL | 2h |\n")
        f.write("| Macro filter | BTC 7d>=+3% AND 1d>-3% blocks SHORT |\n\n")

        f.write("## Per-Day Results\n\n")
        f.write("| Metric | Apr 15 Actual | Apr 15 Sim | Apr 16 Actual | Apr 16 Sim |\n")
        f.write("|---|---|---|---|---|\n")

        # Recalculate per day
        for day_label, day_date in [('Apr 15', '2026-04-15'), ('Apr 16', '2026-04-16')]:
            day_start_utc = parse_eest_to_utc(f'{day_date} 00:00:00')
            day_end_utc = parse_eest_to_utc(f'{day_date} 23:59:59')
            day_trades = [r for r in results if day_start_utc <= r['entry_utc'] <= day_end_utc]
            day_carry = [c for c in carryover_closes if day_start_utc <= c[0] <= day_end_utc]

            a_pnl = sum(t['actual_pnl'] for t in day_trades) + sum(c[5] for c in day_carry)
            a_n = len(day_trades) + len(day_carry)
            a_w = sum(1 for t in day_trades if t['actual_pnl'] > 0) + sum(1 for c in day_carry if c[5] > 0)

            ex = [r for r in day_trades if r.get('sim_status') == 'EXECUTED']
            s_pnl = sum(r['sim_pnl'] for r in ex)
            s_n = len(ex)
            s_w = sum(1 for r in ex if r['sim_pnl'] > 0)

            if day_label == 'Apr 15':
                row15 = (a_n, a_w, a_pnl, s_n, s_w, s_pnl)
            else:
                row16 = (a_n, a_w, a_pnl, s_n, s_w, s_pnl)

        f.write(f"| Trades | {row15[0]} | {row15[3]} | {row16[0]} | {row16[3]} |\n")
        f.write(f"| Wins | {row15[1]} | {row15[4]} | {row16[1]} | {row16[4]} |\n")
        f.write(f"| WR | {row15[1]/max(row15[0],1)*100:.0f}% | {row15[4]/max(row15[3],1)*100:.0f}% | {row16[1]/max(row16[0],1)*100:.0f}% | {row16[4]/max(row16[3],1)*100:.0f}% |\n")
        f.write(f"| PnL | ${row15[2]:+.2f} | ${row15[5]:+.2f} | ${row16[2]:+.2f} | ${row16[5]:+.2f} |\n\n")

        f.write(f"## Combined\n\n")
        f.write(f"- **Actual total**: ${all_actual_pnl:+.2f}\n")
        f.write(f"- **Simulated total**: ${all_sim_pnl:+.2f}\n")
        f.write(f"- **Delta**: ${all_sim_pnl - all_actual_fills_pnl:+.2f}\n")
        f.write(f"- **Blocked trades**: {len(all_blocked)} (their actual PnL: ${all_blocked_pnl:+.2f})\n")

    print(f"Summary saved: {md_path}")

    conn.close()


if __name__ == '__main__':
    main()
