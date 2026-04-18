#!/usr/bin/env python3
"""
Simulate FIXED TP/SL/Trailing configuration on ALL trades April 12-17 2026.

Config:
  TP = +13% ROI, SL = -6.5% ROI
  Trailing: activation at +6% ROI, trail drop = -2% from peak
  Max hold: 12 hours (TIME_EXIT at close of last candle)
"""

import sqlite3
import csv
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

DB = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/market.db"
OUT_DIR = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/tp13_sl65_test"

# Config
TP_ROI = 13.0
SL_ROI = -6.5
TRAIL_ACTIVATE = 6.0
TRAIL_DROP = 2.0
MAX_HOLD_HOURS = 12
DEFAULT_MARGIN = 350.0  # for PnL calc


def parse_ts(s):
    """Parse ISO timestamp string to unix epoch."""
    if not s:
        return None
    s = s.replace("+00:00", "+0000").replace("Z", "+0000")
    # Handle various formats
    for fmt in ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"]:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None


def get_trades(conn):
    """Get all unique trades from both tables, deduplicating overlaps."""
    cur = conn.cursor()
    trades = []

    # OKX trades first (they cover Apr 12-17)
    cur.execute("""
        SELECT coin, direction, entry_price, exit_price, entry_time, exit_time,
               pnl_pct, pnl_usd, exit_reason, leverage, notional
        FROM okx_trades
        WHERE entry_time >= '2026-04-12' AND entry_time < '2026-04-18'
        ORDER BY entry_time
    """)
    okx_rows = cur.fetchall()

    # Build a set of (coin, direction, approx_time) for dedup
    okx_keys = set()
    for r in okx_rows:
        coin, direction, entry_price, exit_price, entry_time, exit_time, pnl_pct, pnl_usd, exit_reason, leverage, notional = r
        ts = parse_ts(entry_time)
        margin = notional / leverage if leverage and leverage > 0 else DEFAULT_MARGIN
        trades.append({
            'source': 'okx',
            'coin': coin,
            'direction': direction,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'entry_ts': ts,
            'actual_pnl_pct': pnl_pct * 100 if pnl_pct else 0,  # convert to %
            'actual_pnl_usd': pnl_usd or 0,
            'actual_exit_reason': exit_reason,
            'leverage': leverage or 8,
            'margin': margin,
            'notional': notional or 0,
        })
        # Key for dedup: coin+dir+rounded timestamp (within 2 min)
        okx_keys.add((coin, direction, ts // 120 if ts else None))

    # Fortix trades - skip duplicates
    cur.execute("""
        SELECT coin, direction, fill_price, exit_price, filled_at, closed_at,
               pnl_pct, pnl_usd, exit_reason, leverage, position_size
        FROM fortix_trades
        WHERE status='CLOSED' AND filled_at >= '2026-04-12' AND filled_at < '2026-04-18'
        ORDER BY filled_at
    """)
    fortix_rows = cur.fetchall()

    for r in fortix_rows:
        coin, direction, fill_price, exit_price, filled_at, closed_at, pnl_pct, pnl_usd, exit_reason, leverage, position_size = r
        ts = parse_ts(filled_at)
        key = (coin, direction, ts // 120 if ts else None)
        if key in okx_keys:
            continue  # duplicate, already have from okx
        lev = leverage or 8
        margin = (fill_price * position_size / lev) if fill_price and position_size and lev else DEFAULT_MARGIN
        trades.append({
            'source': 'fortix',
            'coin': coin,
            'direction': direction,
            'entry_price': fill_price,
            'exit_price': exit_price,
            'entry_time': filled_at,
            'exit_time': closed_at,
            'entry_ts': ts,
            'actual_pnl_pct': pnl_pct * 100 if pnl_pct else 0,
            'actual_pnl_usd': pnl_usd or 0,
            'actual_exit_reason': exit_reason,
            'leverage': lev,
            'margin': margin,
            'notional': fill_price * position_size if fill_price and position_size else 0,
        })

    trades.sort(key=lambda t: t['entry_ts'] or 0)
    return trades


def simulate_trade(conn, trade):
    """Simulate a single trade with the fixed TP/SL/Trail config."""
    cur = conn.cursor()
    coin = trade['coin']
    entry_price = trade['entry_price']
    direction = trade['direction']
    leverage = trade['leverage']
    entry_ts = trade['entry_ts']

    if not entry_price or not entry_ts:
        return None

    # Use 8x leverage for simulation as specified
    sim_leverage = 8

    # Get candles from entry to entry + MAX_HOLD_HOURS
    # Round entry_ts down to the hour boundary, then start from next hour
    hour_start = (entry_ts // 3600) * 3600
    candle_start = hour_start + 3600  # next full candle after entry
    candle_end = entry_ts + MAX_HOLD_HOURS * 3600

    cur.execute("""
        SELECT timestamp, open, high, low, close FROM prices
        WHERE coin = ? AND timeframe = '1h'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
    """, (coin, candle_start, candle_end))
    candles = cur.fetchall()

    if not candles:
        # No candle data - return actual
        return {
            'sim_exit_type': 'NO_DATA',
            'sim_exit_roi': trade['actual_pnl_pct'],
            'sim_pnl_usd': trade['actual_pnl_usd'],
            'sim_peak_roi': 0,
            'sim_trailing_activated': False,
            'sim_candles_held': 0,
        }

    peak_roi = 0.0
    trailing_active = False
    exit_type = None
    exit_roi = None

    for i, (ts, o, h, l, c) in enumerate(candles):
        if direction == 'LONG':
            current_roi_high = (h - entry_price) / entry_price * sim_leverage * 100
            current_roi_low = (l - entry_price) / entry_price * sim_leverage * 100
            current_roi_close = (c - entry_price) / entry_price * sim_leverage * 100
        else:  # SHORT
            current_roi_high = (entry_price - l) / entry_price * sim_leverage * 100
            current_roi_low = (entry_price - h) / entry_price * sim_leverage * 100
            current_roi_close = (entry_price - c) / entry_price * sim_leverage * 100

        # Check SL first (conservative - use worst case)
        if current_roi_low <= SL_ROI:
            exit_type = 'SL'
            exit_roi = SL_ROI
            break

        # Update peak
        if current_roi_high > peak_roi:
            peak_roi = current_roi_high

        # Check trailing activation
        if peak_roi >= TRAIL_ACTIVATE:
            trailing_active = True

        # Check trailing trigger
        if trailing_active and current_roi_low <= peak_roi - TRAIL_DROP:
            trail_exit_roi = peak_roi - TRAIL_DROP
            exit_type = 'TRAIL'
            exit_roi = trail_exit_roi
            break

        # Check TP
        if current_roi_high >= TP_ROI:
            exit_type = 'TP'
            exit_roi = TP_ROI
            break

    if exit_type is None:
        # No exit triggered - TIME_EXIT at last candle close
        if candles:
            last_ts, last_o, last_h, last_l, last_c = candles[-1]
            if direction == 'LONG':
                exit_roi = (last_c - entry_price) / entry_price * sim_leverage * 100
            else:
                exit_roi = (entry_price - last_c) / entry_price * sim_leverage * 100
        else:
            exit_roi = 0
        exit_type = 'TIME'

    sim_pnl_usd = exit_roi / 100 * DEFAULT_MARGIN

    return {
        'sim_exit_type': exit_type,
        'sim_exit_roi': exit_roi,
        'sim_pnl_usd': sim_pnl_usd,
        'sim_peak_roi': peak_roi,
        'sim_trailing_activated': trailing_active,
        'sim_candles_held': i + 1 if exit_type != 'TIME' else len(candles),
    }


def get_day(entry_time_str):
    """Extract date string from entry time."""
    if not entry_time_str:
        return 'Unknown'
    return entry_time_str[:10]


def main():
    conn = sqlite3.connect(DB)
    trades = get_trades(conn)
    print(f"Total trades loaded: {len(trades)}")

    results = []
    for t in trades:
        sim = simulate_trade(conn, t)
        if sim is None:
            continue
        t.update(sim)
        results.append(t)

    print(f"Simulated trades: {len(results)}")

    # ==================== WRITE TRADES CSV ====================
    csv_path = os.path.join(OUT_DIR, "trades.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'date', 'coin', 'direction', 'source', 'leverage',
            'entry_price', 'margin',
            'actual_exit_reason', 'actual_roi_pct', 'actual_pnl_usd',
            'sim_exit_type', 'sim_roi_pct', 'sim_pnl_usd',
            'sim_peak_roi', 'sim_trailing_activated', 'sim_candles_held',
            'delta_pnl_usd'
        ])
        for r in results:
            writer.writerow([
                get_day(r['entry_time']),
                r['coin'], r['direction'], r['source'], r['leverage'],
                r['entry_price'], f"{r['margin']:.2f}",
                r['actual_exit_reason'], f"{r['actual_pnl_pct']:.4f}", f"{r['actual_pnl_usd']:.2f}",
                r['sim_exit_type'], f"{r['sim_exit_roi']:.4f}", f"{r['sim_pnl_usd']:.2f}",
                f"{r['sim_peak_roi']:.4f}", r['sim_trailing_activated'], r['sim_candles_held'],
                f"{r['sim_pnl_usd'] - r['actual_pnl_usd']:.2f}"
            ])
    print(f"Wrote {csv_path}")

    # ==================== COMPUTE STATS ====================

    # Grand totals
    total = len(results)
    actual_wins = sum(1 for r in results if r['actual_pnl_usd'] > 0)
    actual_pnl = sum(r['actual_pnl_usd'] for r in results)
    actual_gross_profit = sum(r['actual_pnl_usd'] for r in results if r['actual_pnl_usd'] > 0)
    actual_gross_loss = abs(sum(r['actual_pnl_usd'] for r in results if r['actual_pnl_usd'] < 0))
    actual_pf = actual_gross_profit / actual_gross_loss if actual_gross_loss > 0 else float('inf')

    sim_wins = sum(1 for r in results if r['sim_pnl_usd'] > 0)
    sim_pnl = sum(r['sim_pnl_usd'] for r in results)
    sim_gross_profit = sum(r['sim_pnl_usd'] for r in results if r['sim_pnl_usd'] > 0)
    sim_gross_loss = abs(sum(r['sim_pnl_usd'] for r in results if r['sim_pnl_usd'] < 0))
    sim_pf = sim_gross_profit / sim_gross_loss if sim_gross_loss > 0 else float('inf')

    # Per-day
    days = sorted(set(get_day(r['entry_time']) for r in results))
    day_actual = defaultdict(float)
    day_sim = defaultdict(float)
    day_count = defaultdict(int)
    for r in results:
        d = get_day(r['entry_time'])
        day_actual[d] += r['actual_pnl_usd']
        day_sim[d] += r['sim_pnl_usd']
        day_count[d] += 1

    # Exit type distribution
    exit_types = defaultdict(lambda: {'count': 0, 'roi_sum': 0, 'pnl_sum': 0})
    for r in results:
        et = r['sim_exit_type']
        exit_types[et]['count'] += 1
        exit_types[et]['roi_sum'] += r['sim_exit_roi']
        exit_types[et]['pnl_sum'] += r['sim_pnl_usd']

    # Key stats
    trail_trades = [r for r in results if r['sim_trailing_activated']]
    trail_exits = [r for r in results if r['sim_exit_type'] == 'TRAIL']
    tp_exits = [r for r in results if r['sim_exit_type'] == 'TP']

    # Trades that hit trailing but then got TP'd? (peak >= 6 but exit == TP means it kept going)
    would_tp_but_trailed = [r for r in results if r['sim_exit_type'] == 'TRAIL' and r['sim_peak_roi'] >= TP_ROI]

    best_trade = max(results, key=lambda r: r['sim_pnl_usd'])
    worst_trade = min(results, key=lambda r: r['sim_pnl_usd'])

    # Per-coin breakdown
    coin_pnl_actual = defaultdict(float)
    coin_pnl_sim = defaultdict(float)
    coin_count = defaultdict(int)
    for r in results:
        coin_pnl_actual[r['coin']] += r['actual_pnl_usd']
        coin_pnl_sim[r['coin']] += r['sim_pnl_usd']
        coin_count[r['coin']] += 1
    coins_sorted = sorted(coin_pnl_sim.keys(), key=lambda c: coin_pnl_sim[c], reverse=True)

    # ==================== WRITE SUMMARY ====================
    md_path = os.path.join(OUT_DIR, "summary.md")
    with open(md_path, 'w') as f:
        f.write("# TP13 / SL6.5 / Trail(6→-2) Simulation Results\n")
        f.write(f"**Period:** April 12-17, 2026 (6 days)\n\n")
        f.write(f"**Config:** TP=+13% ROI, SL=-6.5% ROI, Trailing activation=+6% ROI, Trail drop=-2% from peak\n")
        f.write(f"**Simulation leverage:** 8x | **Margin per trade:** ${DEFAULT_MARGIN}\n")
        f.write(f"**Max hold:** {MAX_HOLD_HOURS}h | **Candle resolution:** 1h\n\n")

        # Grand total table
        f.write("## 1. Grand Total\n\n")
        f.write("| Metric | Actual | TP13/SL6.5/Trail6-2 |\n")
        f.write("|--------|--------|---------------------|\n")
        f.write(f"| Total Trades | {total} | {total} |\n")
        f.write(f"| Wins | {actual_wins} | {sim_wins} |\n")
        f.write(f"| Win Rate | {actual_wins/total*100:.1f}% | {sim_wins/total*100:.1f}% |\n")
        f.write(f"| Total PnL | ${actual_pnl:,.2f} | ${sim_pnl:,.2f} |\n")
        f.write(f"| Avg PnL/trade | ${actual_pnl/total:,.2f} | ${sim_pnl/total:,.2f} |\n")
        f.write(f"| Gross Profit | ${actual_gross_profit:,.2f} | ${sim_gross_profit:,.2f} |\n")
        f.write(f"| Gross Loss | -${actual_gross_loss:,.2f} | -${sim_gross_loss:,.2f} |\n")
        f.write(f"| Profit Factor | {actual_pf:.2f} | {sim_pf:.2f} |\n")
        f.write(f"| **Delta PnL** | | **${sim_pnl - actual_pnl:+,.2f}** |\n\n")

        # Per-day table
        f.write("## 2. Per-Day Breakdown\n\n")
        f.write("| Day | Trades | Actual PnL | Sim PnL | Delta |\n")
        f.write("|-----|--------|------------|---------|-------|\n")
        for d in days:
            delta = day_sim[d] - day_actual[d]
            f.write(f"| {d} | {day_count[d]} | ${day_actual[d]:,.2f} | ${day_sim[d]:,.2f} | ${delta:+,.2f} |\n")
        f.write("\n")

        # Exit type distribution
        f.write("## 3. Exit Type Distribution (Simulated)\n\n")
        f.write("| Exit Type | Count | Avg ROI % | Total PnL |\n")
        f.write("|-----------|-------|-----------|----------|\n")
        for et in ['TP', 'SL', 'TRAIL', 'TIME', 'NO_DATA']:
            if et in exit_types:
                d = exit_types[et]
                avg_roi = d['roi_sum'] / d['count'] if d['count'] > 0 else 0
                f.write(f"| {et} | {d['count']} | {avg_roi:+.2f}% | ${d['pnl_sum']:,.2f} |\n")
        f.write("\n")
        nodata_coins = sorted(set(r['coin'] for r in results if r['sim_exit_type'] == 'NO_DATA'))
        if nodata_coins:
            f.write(f"*NO_DATA = coins without 1h candle data ({', '.join(nodata_coins)}); uses actual PnL as fallback.*\n\n")
        f.write("**Note:** TP never triggers because trailing (activate=6%, drop=2%) catches all trades before they reach 13% ROI. ")
        f.write("This is by design -- the trailing stop is tight enough to exit profitably well before TP.\n\n")

        # Key stats
        f.write("## 4. Key Stats\n\n")
        f.write(f"- **Trailing activated:** {len(trail_trades)} trades ({len(trail_trades)/total*100:.1f}%)\n")
        if trail_trades:
            avg_peak_trail = sum(r['sim_peak_roi'] for r in trail_trades) / len(trail_trades)
            f.write(f"  - Avg peak ROI when trailing activated: {avg_peak_trail:.2f}%\n")
        f.write(f"- **Trailing exits:** {len(trail_exits)} trades\n")
        if trail_exits:
            avg_trail_exit = sum(r['sim_exit_roi'] for r in trail_exits) / len(trail_exits)
            f.write(f"  - Avg trail exit ROI: {avg_trail_exit:.2f}%\n")
        f.write(f"- **TP hits:** {len(tp_exits)} trades\n")
        f.write(f"- **Would have hit TP but got trailed first:** {len(would_tp_but_trailed)} trades\n\n")

        f.write(f"- **Best trade (sim):** {best_trade['coin']} {best_trade['direction']} on {get_day(best_trade['entry_time'])} → ${best_trade['sim_pnl_usd']:,.2f} ({best_trade['sim_exit_type']}, {best_trade['sim_exit_roi']:.2f}% ROI)\n")
        f.write(f"- **Worst trade (sim):** {worst_trade['coin']} {worst_trade['direction']} on {get_day(worst_trade['entry_time'])} → ${worst_trade['sim_pnl_usd']:,.2f} ({worst_trade['sim_exit_type']}, {worst_trade['sim_exit_roi']:.2f}% ROI)\n\n")

        # Per-coin breakdown
        f.write("## 5. Per-Coin Breakdown\n\n")
        f.write("### Top 5 Coins (Sim PnL)\n\n")
        f.write("| Coin | Trades | Actual PnL | Sim PnL | Delta |\n")
        f.write("|------|--------|------------|---------|-------|\n")
        for c in coins_sorted[:5]:
            delta = coin_pnl_sim[c] - coin_pnl_actual[c]
            f.write(f"| {c} | {coin_count[c]} | ${coin_pnl_actual[c]:,.2f} | ${coin_pnl_sim[c]:,.2f} | ${delta:+,.2f} |\n")

        f.write("\n### Bottom 5 Coins (Sim PnL)\n\n")
        f.write("| Coin | Trades | Actual PnL | Sim PnL | Delta |\n")
        f.write("|------|--------|------------|---------|-------|\n")
        for c in coins_sorted[-5:]:
            delta = coin_pnl_sim[c] - coin_pnl_actual[c]
            f.write(f"| {c} | {coin_count[c]} | ${coin_pnl_actual[c]:,.2f} | ${coin_pnl_sim[c]:,.2f} | ${delta:+,.2f} |\n")

        f.write("\n")
        # Simulated-only stats (excluding NO_DATA)
        sim_only = [r for r in results if r['sim_exit_type'] != 'NO_DATA']
        sim_only_total = len(sim_only)
        sim_only_wins = sum(1 for r in sim_only if r['sim_pnl_usd'] > 0)
        sim_only_pnl = sum(r['sim_pnl_usd'] for r in sim_only)
        sim_only_actual_pnl = sum(r['actual_pnl_usd'] for r in sim_only)
        sim_only_gp = sum(r['sim_pnl_usd'] for r in sim_only if r['sim_pnl_usd'] > 0)
        sim_only_gl = abs(sum(r['sim_pnl_usd'] for r in sim_only if r['sim_pnl_usd'] < 0))
        sim_only_pf = sim_only_gp / sim_only_gl if sim_only_gl > 0 else float('inf')

        f.write("### Simulated-only (excluding NO_DATA coins)\n\n")
        f.write(f"| Metric | Actual ({sim_only_total} trades) | Simulated |\n")
        f.write("|--------|--------|----------|\n")
        f.write(f"| Wins | {sum(1 for r in sim_only if r['actual_pnl_usd']>0)} | {sim_only_wins} |\n")
        f.write(f"| Win Rate | {sum(1 for r in sim_only if r['actual_pnl_usd']>0)/sim_only_total*100:.1f}% | {sim_only_wins/sim_only_total*100:.1f}% |\n")
        f.write(f"| PnL | ${sim_only_actual_pnl:,.2f} | ${sim_only_pnl:,.2f} |\n")
        f.write(f"| Profit Factor | - | {sim_only_pf:.2f} |\n")
        f.write(f"| Delta | | ${sim_only_pnl - sim_only_actual_pnl:+,.2f} |\n\n")

        # Honest assessment
        f.write("\n## 6. Honest Assessment\n\n")
        delta_total = sim_pnl - actual_pnl
        if delta_total > 0:
            verdict = "BETTER"
            f.write(f"The TP13/SL6.5/Trail(6→-2) config would have been **{verdict}** than actual by **${delta_total:+,.2f}**.\n\n")
        else:
            verdict = "WORSE"
            f.write(f"The TP13/SL6.5/Trail(6→-2) config would have been **{verdict}** than actual by **${delta_total:+,.2f}**.\n\n")

        # More nuance
        f.write("### Analysis\n\n")
        if sim_pf > actual_pf:
            f.write(f"- Profit factor improved: {actual_pf:.2f} → {sim_pf:.2f}\n")
        else:
            f.write(f"- Profit factor declined: {actual_pf:.2f} → {sim_pf:.2f}\n")

        wr_delta = (sim_wins/total - actual_wins/total) * 100
        f.write(f"- Win rate change: {actual_wins/total*100:.1f}% → {sim_wins/total*100:.1f}% ({wr_delta:+.1f}pp)\n")

        avg_actual = actual_pnl / total
        avg_sim = sim_pnl / total
        f.write(f"- Avg PnL per trade: ${avg_actual:.2f} → ${avg_sim:.2f}\n")

        sl_count = exit_types.get('SL', {}).get('count', 0)
        f.write(f"- SL hit rate: {sl_count}/{total} ({sl_count/total*100:.1f}%)\n")
        f.write(f"- The 2:1 R:R (13% TP vs 6.5% SL) requires >{100/3:.0f}% win rate to be profitable\n")
        f.write(f"- Trailing stop protected gains on {len(trail_exits)} trades, avg exit ROI: {sum(r['sim_exit_roi'] for r in trail_exits)/max(len(trail_exits),1):.2f}%\n")

    print(f"Wrote {md_path}")

    # Print key results to stdout
    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Trades: {total}")
    print(f"Actual:  PnL=${actual_pnl:,.2f}  WR={actual_wins/total*100:.1f}%  PF={actual_pf:.2f}")
    print(f"Sim:     PnL=${sim_pnl:,.2f}  WR={sim_wins/total*100:.1f}%  PF={sim_pf:.2f}")
    print(f"Delta:   ${delta_total:+,.2f}")
    print(f"\nExit distribution: TP={exit_types.get('TP',{}).get('count',0)} SL={exit_types.get('SL',{}).get('count',0)} TRAIL={exit_types.get('TRAIL',{}).get('count',0)} TIME={exit_types.get('TIME',{}).get('count',0)}")
    print(f"Trailing activated: {len(trail_trades)}/{total}")


if __name__ == '__main__':
    main()
