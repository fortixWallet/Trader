#!/usr/bin/env python3
"""
Trailing Stop Simulation — Tests 20 configurations across all available trades.

Uses:
  1. postmortem.csv (117 trades, 98 with entry data)
  2. fortix_trades table (103 closed with fill_price)
  3. okx_trades table (162 trades)

Reconstructs 1h price paths from market.db prices table.
"""

import csv
import sqlite3
import os
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from itertools import product

BASE = "/Users/williamstorm/Documents/Trading (OKX) 1h"
DB_PATH = os.path.join(BASE, "data/crypto/market.db")
POSTMORTEM = os.path.join(BASE, "data/crypto/trade_postmortem/postmortem.csv")
OUT_DIR = os.path.join(BASE, "data/crypto/trailing_test")

ACTIVATIONS = [3, 5, 6, 8, 10]
TRAIL_DROPS = [2, 3, 4, 5]

# ─── helpers ───

def ts_from_iso(s):
    """Parse ISO timestamp to unix epoch."""
    if not s or s.strip() == '':
        return None
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return int(dt.timestamp())
    except:
        return None

def get_hourly_candles(conn, coin, start_ts, end_ts):
    """Get 1h candles for coin from fill time to exit+4h.

    Important: start from the candle that CONTAINS the fill time, not before.
    1h candle at timestamp T covers period [T, T+3600).
    So the first relevant candle has timestamp <= start_ts and timestamp + 3600 > start_ts.
    """
    cur = conn.cursor()
    # Find the candle containing the fill time (floor to hour)
    candle_start = (start_ts // 3600) * 3600
    cur.execute("""
        SELECT timestamp, open, high, low, close FROM prices
        WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
    """, (coin.upper(), candle_start, end_ts + 4*3600))
    return cur.fetchall()

def simulate_trailing(candles, entry_price, direction, leverage, activation, trail_drop,
                      actual_exit_ts=None, actual_roi=None):
    """
    Simulate trailing stop on hourly candles.
    Returns dict with trailing result or None if never activated.
    """
    if not candles or entry_price <= 0:
        return None

    peak_roi = -999
    activated = False
    trailing_exit_roi = None
    trailing_exit_ts = None
    trailing_exit_bar = None

    for i, (ts, o, h, l, c) in enumerate(candles):
        # Compute ROI at each price point within the candle
        if direction == 'LONG':
            best_price = h  # best case within candle
            worst_price = l  # worst case
            close_price = c
        else:  # SHORT
            best_price = l
            worst_price = h
            close_price = c

        # ROI at best point in candle
        if direction == 'LONG':
            roi_best = (best_price - entry_price) / entry_price * leverage * 100
            roi_worst = (worst_price - entry_price) / entry_price * leverage * 100
            roi_close = (close_price - entry_price) / entry_price * leverage * 100
        else:
            roi_best = (entry_price - best_price) / entry_price * leverage * 100
            roi_worst = (entry_price - worst_price) / entry_price * leverage * 100
            roi_close = (entry_price - close_price) / entry_price * leverage * 100

        # Check activation on best price
        if not activated:
            if roi_best >= activation:
                activated = True
                peak_roi = roi_best
                # Also check if trail triggers in same candle
                if roi_worst < peak_roi - trail_drop:
                    trailing_exit_roi = roi_close  # approximate exit at close
                    trailing_exit_ts = ts
                    trailing_exit_bar = i
                    break
                continue
            continue

        # Already activated — update peak
        if roi_best > peak_roi:
            peak_roi = roi_best

        # Check trail trigger: does ROI drop trail_drop from peak?
        if roi_worst < peak_roi - trail_drop:
            # Trail triggered — exit at approximately (peak - trail_drop) but use close as conservative
            trailing_exit_roi = roi_close
            trailing_exit_ts = ts
            trailing_exit_bar = i
            break

    if not activated:
        return None  # trailing never activated — use actual exit

    if trailing_exit_roi is None:
        # Activated but never triggered trail drop — position still open at end of data
        # Use last candle close
        last = candles[-1]
        if direction == 'LONG':
            trailing_exit_roi = (last[4] - entry_price) / entry_price * leverage * 100
        else:
            trailing_exit_roi = (entry_price - last[4]) / entry_price * leverage * 100
        trailing_exit_ts = last[0]
        trailing_exit_bar = len(candles) - 1

    return {
        'activated': True,
        'peak_roi': peak_roi,
        'trailing_exit_roi': trailing_exit_roi,
        'trailing_exit_ts': trailing_exit_ts,
        'trailing_exit_bar': trailing_exit_bar,
    }


def load_postmortem_trades():
    """Load trades from postmortem.csv that have entry data."""
    trades = []
    with open(POSTMORTEM) as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = float(row['entry_price'])
            if entry <= 0:
                continue
            fill_ts = ts_from_iso(row['fill_time_utc'])
            close_ts = ts_from_iso(row['close_time_utc'])
            if not fill_ts or not close_ts:
                continue
            trades.append({
                'source': 'postmortem',
                'trade_id': row['trade_id'],
                'coin': row['coin'].upper(),
                'direction': row['direction'].upper(),
                'leverage': float(row['leverage']),
                'entry_price': entry,
                'fill_ts': fill_ts,
                'close_ts': close_ts,
                'actual_roi': float(row['roi_pct']),
                'actual_pnl': float(row['pnl']),
                'exit_reason': row['exit_reason'],
                'peak_roi': float(row['peak_roi']) if row['peak_roi'] else 0,
                'margin': float(row['margin']) if row['margin'] else 350,
            })
    return trades


def load_fortix_trades(conn):
    """Load closed fortix_trades with fill data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_id, coin, direction, leverage, fill_price, exit_price,
               pnl_pct, pnl_usd, exit_reason, filled_at, closed_at, held_minutes,
               sl_price, tp_price
        FROM fortix_trades
        WHERE status='CLOSED' AND fill_price > 0
    """)
    trades = []
    for row in cur.fetchall():
        fill_ts = ts_from_iso(row[9])
        close_ts = ts_from_iso(row[10])
        if not fill_ts or not close_ts:
            continue
        pnl_pct = row[6] if row[6] else 0
        pnl_usd = row[7] if row[7] else 0
        lev = row[3] if row[3] else 5
        trades.append({
            'source': 'fortix',
            'trade_id': str(row[0]),
            'coin': row[1].upper(),
            'direction': row[2].upper(),
            'leverage': float(lev),
            'entry_price': float(row[4]),
            'fill_ts': fill_ts,
            'close_ts': close_ts,
            'actual_roi': float(pnl_pct) * 100 * float(lev),  # pnl_pct is decimal
            'actual_pnl': float(pnl_usd),
            'exit_reason': row[8] or 'UNKNOWN',
            'peak_roi': 0,
            'margin': abs(float(pnl_usd) / float(pnl_pct)) if pnl_pct and pnl_pct != 0 else 350,
        })
    return trades


def load_okx_trades(conn):
    """Load OKX trades."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, coin, direction, entry_price, exit_price,
               entry_time, exit_time, held_minutes,
               pnl_pct, pnl_usd, exit_reason, leverage
        FROM okx_trades
        WHERE entry_price > 0
    """)
    trades = []
    for row in cur.fetchall():
        fill_ts = ts_from_iso(row[5])
        close_ts = ts_from_iso(row[6])
        if not fill_ts or not close_ts:
            continue
        lev = row[11] if row[11] else 5
        pnl_pct = row[8] if row[8] else 0
        pnl_usd = row[9] if row[9] else 0
        trades.append({
            'source': 'okx',
            'trade_id': f"okx_{row[0]}",
            'coin': row[1].upper(),
            'direction': row[2].upper(),
            'leverage': float(lev),
            'entry_price': float(row[3]),
            'fill_ts': fill_ts,
            'close_ts': close_ts,
            'actual_roi': float(pnl_pct) * 100 * float(lev),
            'actual_pnl': float(pnl_usd),
            'exit_reason': row[10] or 'UNKNOWN',
            'peak_roi': 0,
            'margin': abs(float(pnl_usd) / float(pnl_pct)) if pnl_pct and pnl_pct != 0 else 350,
        })
    return trades


def deduplicate_trades(all_trades):
    """Remove duplicates (same coin, direction, close within 5 min)."""
    seen = set()
    unique = []
    for t in all_trades:
        key = (t['coin'], t['direction'], t['close_ts'] // 300)
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique


def main():
    conn = sqlite3.connect(DB_PATH)

    # ─── Load all trades ───
    print("Loading trades...")
    pm_trades = load_postmortem_trades()
    print(f"  Postmortem: {len(pm_trades)} trades with entry data")

    ft_trades = load_fortix_trades(conn)
    print(f"  Fortix: {len(ft_trades)} closed trades")

    okx_trades = load_okx_trades(conn)
    print(f"  OKX: {len(okx_trades)} trades")

    # Merge and deduplicate
    all_trades = pm_trades + ft_trades + okx_trades
    all_trades = deduplicate_trades(all_trades)
    print(f"  After dedup: {len(all_trades)} unique trades")

    # Filter trades where we have price data
    available_coins = set()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT coin FROM prices WHERE timeframe='1h'")
    available_coins = {r[0] for r in cur.fetchall()}

    usable = [t for t in all_trades if t['coin'] in available_coins]
    print(f"  With price data: {len(usable)} trades")

    # Separate Apr 15-17 vs historical
    apr15 = int(datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc).timestamp())
    apr18 = int(datetime(2026, 4, 18, 0, 0, tzinfo=timezone.utc).timestamp())

    recent_trades = [t for t in usable if apr15 <= t['fill_ts'] < apr18]
    historical_trades = [t for t in usable if t['fill_ts'] < apr15]
    all_period_trades = usable

    print(f"  Apr 15-17: {len(recent_trades)} trades")
    print(f"  Historical (before Apr 15): {len(historical_trades)} trades")
    print()

    # ─── Pre-fetch candle data for all trades ───
    print("Fetching candle data...")
    trade_candles = {}
    for i, t in enumerate(usable):
        candles = get_hourly_candles(conn, t['coin'], t['fill_ts'], t['close_ts'])
        if candles:
            trade_candles[id(t)] = candles
    print(f"  Got candles for {len(trade_candles)}/{len(usable)} trades")
    print()

    # ─── Run simulations ───
    configs = list(product(ACTIVATIONS, TRAIL_DROPS))

    # Results storage
    config_results = {}  # (act, drop) -> metrics
    per_trade_results = []  # list of dicts

    for act, drop in configs:
        config_key = f"act{act}_drop{drop}"

        metrics = {
            'activation': act, 'trail_drop': drop,
            'total_trades': 0, 'trades_activated': 0,
            'actual_total_pnl': 0, 'trailing_total_pnl': 0,
            'winners_hurt': 0, 'winners_hurt_pnl_lost': 0,
            'losers_saved': 0, 'losers_saved_pnl': 0,
            'false_activations': 0, 'false_activation_lost': 0,
            # Per-period
            'recent_actual_pnl': 0, 'recent_trailing_pnl': 0,
            'recent_trades': 0,
            'hist_actual_pnl': 0, 'hist_trailing_pnl': 0,
            'hist_trades': 0,
            # Per-day
            'daily': defaultdict(lambda: {'actual': 0, 'trailing': 0, 'count': 0}),
            # Per-coin
            'coin': defaultdict(lambda: {'actual': 0, 'trailing': 0, 'count': 0, 'saved': 0}),
        }

        for t in usable:
            candles = trade_candles.get(id(t))
            if not candles:
                continue

            metrics['total_trades'] += 1
            actual_pnl = t['actual_pnl']
            actual_roi = t['actual_roi']
            metrics['actual_total_pnl'] += actual_pnl

            # Determine margin for PnL calculation
            margin = t.get('margin', 350)
            if margin <= 0 or margin > 10000:
                margin = 350

            result = simulate_trailing(
                candles, t['entry_price'], t['direction'], t['leverage'],
                act, drop, t['close_ts'], actual_roi
            )

            if result is None or not result['activated']:
                # Trail never activated — use actual exit
                trailing_pnl = actual_pnl
                trailing_roi = actual_roi
                trail_activated = False
            else:
                trail_activated = True
                metrics['trades_activated'] += 1
                trailing_roi = result['trailing_exit_roi']

                # Compute trailing PnL using ratio to actual
                # This preserves the correct margin/notional for each trade
                if actual_roi != 0 and abs(actual_roi) > 0.01:
                    trailing_pnl = actual_pnl * (trailing_roi / actual_roi)
                else:
                    # Fallback: use margin estimate
                    trailing_pnl = trailing_roi / 100 * margin

                # Did trailing exit BEFORE actual exit?
                if result['trailing_exit_ts'] and t['close_ts'] and result['trailing_exit_ts'] <= t['close_ts']:
                    # Trailing exited early
                    pass
                else:
                    # Trailing would exit after actual — actual exit stands
                    trailing_pnl = actual_pnl
                    trailing_roi = actual_roi

            metrics['trailing_total_pnl'] += trailing_pnl

            # Classify
            pnl_diff = trailing_pnl - actual_pnl

            if trail_activated:
                if actual_pnl > 0 and trailing_pnl < actual_pnl:
                    # Winner hurt by trailing
                    metrics['winners_hurt'] += 1
                    metrics['winners_hurt_pnl_lost'] += (actual_pnl - trailing_pnl)

                if actual_pnl < 0 and trailing_pnl > actual_pnl:
                    # Loser saved by trailing
                    metrics['losers_saved'] += 1
                    metrics['losers_saved_pnl'] += (trailing_pnl - actual_pnl)

                # False activation: trail activates and exits with profit,
                # but actual TP would have given more
                if t['exit_reason'] == 'TARGET_HIT' and trailing_pnl < actual_pnl:
                    metrics['false_activations'] += 1
                    metrics['false_activation_lost'] += (actual_pnl - trailing_pnl)

            # Per-period
            is_recent = apr15 <= t['fill_ts'] < apr18
            if is_recent:
                metrics['recent_actual_pnl'] += actual_pnl
                metrics['recent_trailing_pnl'] += trailing_pnl
                metrics['recent_trades'] += 1
            else:
                metrics['hist_actual_pnl'] += actual_pnl
                metrics['hist_trailing_pnl'] += trailing_pnl
                metrics['hist_trades'] += 1

            # Per-day
            day = datetime.utcfromtimestamp(t['fill_ts']).strftime('%Y-%m-%d')
            metrics['daily'][day]['actual'] += actual_pnl
            metrics['daily'][day]['trailing'] += trailing_pnl
            metrics['daily'][day]['count'] += 1

            # Per-coin
            metrics['coin'][t['coin']]['actual'] += actual_pnl
            metrics['coin'][t['coin']]['trailing'] += trailing_pnl
            metrics['coin'][t['coin']]['count'] += 1
            if trail_activated and trailing_pnl > actual_pnl:
                metrics['coin'][t['coin']]['saved'] += 1

            # Store per-trade for best config (will filter later)
            per_trade_results.append({
                'trade_id': t['trade_id'],
                'source': t['source'],
                'coin': t['coin'],
                'direction': t['direction'],
                'leverage': t['leverage'],
                'entry_price': t['entry_price'],
                'exit_reason': t['exit_reason'],
                'actual_roi': round(actual_roi, 2),
                'actual_pnl': round(actual_pnl, 2),
                'activation': act,
                'trail_drop': drop,
                'trail_activated': trail_activated,
                'trailing_roi': round(trailing_roi, 2),
                'trailing_pnl': round(trailing_pnl, 2),
                'pnl_diff': round(trailing_pnl - actual_pnl, 2),
                'fill_date': datetime.utcfromtimestamp(t['fill_ts']).strftime('%Y-%m-%d'),
            })

        config_results[(act, drop)] = metrics

    # ─── Find best config ───
    best_key = max(config_results, key=lambda k: config_results[k]['trailing_total_pnl'])
    best = config_results[best_key]

    # ─── Write config_comparison.csv ───
    print("Writing outputs...")
    with open(os.path.join(OUT_DIR, "config_comparison.csv"), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['activation', 'trail_drop', 'total_trades', 'activated', 'activation_pct',
                     'actual_pnl', 'trailing_pnl', 'net_diff', 'net_diff_pct',
                     'winners_hurt', 'winners_hurt_lost', 'losers_saved', 'losers_saved_amt',
                     'false_activations', 'false_act_lost',
                     'recent_actual', 'recent_trailing', 'hist_actual', 'hist_trailing'])
        for (act, drop) in sorted(config_results.keys()):
            m = config_results[(act, drop)]
            act_pct = m['trades_activated'] / m['total_trades'] * 100 if m['total_trades'] > 0 else 0
            net_diff = m['trailing_total_pnl'] - m['actual_total_pnl']
            net_pct = net_diff / abs(m['actual_total_pnl']) * 100 if m['actual_total_pnl'] != 0 else 0
            w.writerow([
                act, drop, m['total_trades'], m['trades_activated'], f"{act_pct:.1f}",
                f"{m['actual_total_pnl']:.2f}", f"{m['trailing_total_pnl']:.2f}",
                f"{net_diff:.2f}", f"{net_pct:.1f}",
                m['winners_hurt'], f"{m['winners_hurt_pnl_lost']:.2f}",
                m['losers_saved'], f"{m['losers_saved_pnl']:.2f}",
                m['false_activations'], f"{m['false_activation_lost']:.2f}",
                f"{m['recent_actual_pnl']:.2f}", f"{m['recent_trailing_pnl']:.2f}",
                f"{m['hist_actual_pnl']:.2f}", f"{m['hist_trailing_pnl']:.2f}",
            ])

    # ─── Write per_trade.csv (best config only) ───
    best_act, best_drop = best_key
    best_trades = [r for r in per_trade_results if r['activation'] == best_act and r['trail_drop'] == best_drop]
    with open(os.path.join(OUT_DIR, "per_trade.csv"), 'w', newline='') as f:
        if best_trades:
            w = csv.DictWriter(f, fieldnames=best_trades[0].keys())
            w.writeheader()
            w.writerows(best_trades)

    # ─── Write summary.md ───
    lines = []
    lines.append("# Trailing Stop Simulation Results")
    lines.append(f"\nTotal unique trades analyzed: {best['total_trades']}")
    lines.append(f"Data sources: postmortem ({len(pm_trades)}), fortix ({len(ft_trades)}), okx ({len(okx_trades)})")
    lines.append(f"After dedup with price data: {len(usable)}")
    lines.append("")

    # Config comparison table
    lines.append("## 1. Config Comparison (Activation x Trail Drop -> Net PnL Diff)")
    lines.append("")
    header = "| Act \\ Drop |" + "|".join(f" {d}% " for d in TRAIL_DROPS) + "|"
    lines.append(header)
    lines.append("|" + "---|" * (len(TRAIL_DROPS) + 1))
    for act in ACTIVATIONS:
        row = f"| **{act}%** |"
        for drop in TRAIL_DROPS:
            m = config_results[(act, drop)]
            diff = m['trailing_total_pnl'] - m['actual_total_pnl']
            marker = " **BEST**" if (act, drop) == best_key else ""
            row += f" ${diff:+.0f}{marker} |"
        lines.append(row)
    lines.append("")

    # Best config
    lines.append(f"## 2. Best Config: Activation={best_act}%, Trail Drop={best_drop}%")
    lines.append(f"- Actual total PnL: **${best['actual_total_pnl']:.2f}**")
    lines.append(f"- Trailing total PnL: **${best['trailing_total_pnl']:.2f}**")
    net = best['trailing_total_pnl'] - best['actual_total_pnl']
    lines.append(f"- Net improvement: **${net:+.2f}** ({net/abs(best['actual_total_pnl'])*100:+.1f}%)" if best['actual_total_pnl'] != 0 else f"- Net improvement: **${net:+.2f}**")
    lines.append(f"- Trades where trailing activated: {best['trades_activated']}/{best['total_trades']} ({best['trades_activated']/best['total_trades']*100:.0f}%)")
    lines.append("")

    # Winners hurt
    lines.append("## 3. Winners Hurt by Trailing")
    lines.append(f"- Count: {best['winners_hurt']} trades")
    lines.append(f"- Total PnL lost: ${best['winners_hurt_pnl_lost']:.2f}")
    lines.append("")

    # Losers saved
    lines.append("## 4. Losers Saved by Trailing")
    lines.append(f"- Count: {best['losers_saved']} trades")
    lines.append(f"- Total PnL saved: ${best['losers_saved_pnl']:.2f}")
    lines.append(f"- Net (saved - lost): ${best['losers_saved_pnl'] - best['winners_hurt_pnl_lost']:+.2f}")
    lines.append("")

    # False activations
    lines.append("## 5. False Activations (left money on table)")
    lines.append(f"- Count: {best['false_activations']} trades (TP would have hit but trailing exited early)")
    lines.append(f"- Money left: ${best['false_activation_lost']:.2f}")
    lines.append("")

    # Per-day
    lines.append("## 6. Per-Day Breakdown")
    lines.append("| Date | Trades | Actual PnL | Trailing PnL | Diff |")
    lines.append("|------|--------|------------|--------------|------|")
    for day in sorted(best['daily'].keys()):
        d = best['daily'][day]
        diff = d['trailing'] - d['actual']
        lines.append(f"| {day} | {d['count']} | ${d['actual']:.0f} | ${d['trailing']:.0f} | ${diff:+.0f} |")
    lines.append("")

    # Per-coin
    lines.append("## 7. Per-Coin Analysis (best config)")
    lines.append("| Coin | Trades | Actual PnL | Trailing PnL | Diff | Saved |")
    lines.append("|------|--------|------------|--------------|------|-------|")
    coin_data = sorted(best['coin'].items(), key=lambda x: x[1]['trailing'] - x[1]['actual'], reverse=True)
    for coin, d in coin_data:
        diff = d['trailing'] - d['actual']
        lines.append(f"| {coin} | {d['count']} | ${d['actual']:.0f} | ${d['trailing']:.0f} | ${diff:+.0f} | {d['saved']} |")
    lines.append("")

    # Recent vs historical
    lines.append("## 8. Recent (Apr 15-17) vs Historical")
    lines.append(f"- **Apr 15-17**: {best['recent_trades']} trades, actual ${best['recent_actual_pnl']:.0f}, trailing ${best['recent_trailing_pnl']:.0f}, diff ${best['recent_trailing_pnl']-best['recent_actual_pnl']:+.0f}")
    lines.append(f"- **Historical**: {best['hist_trades']} trades, actual ${best['hist_actual_pnl']:.0f}, trailing ${best['hist_trailing_pnl']:.0f}, diff ${best['hist_trailing_pnl']-best['hist_actual_pnl']:+.0f}")
    lines.append("")

    # Honest assessment
    lines.append("## 9. HONEST Assessment")
    lines.append("")

    # Calculate key stats for assessment
    all_configs_positive = sum(1 for k in config_results if config_results[k]['trailing_total_pnl'] > config_results[k]['actual_total_pnl'])
    all_configs_negative = len(config_results) - all_configs_positive
    best_diff = best['trailing_total_pnl'] - best['actual_total_pnl']
    worst_key = min(config_results, key=lambda k: config_results[k]['trailing_total_pnl'] - config_results[k]['actual_total_pnl'])
    worst_diff = config_results[worst_key]['trailing_total_pnl'] - config_results[worst_key]['actual_total_pnl']

    lines.append(f"- Configs with positive net: {all_configs_positive}/{len(config_results)}")
    lines.append(f"- Configs with negative net: {all_configs_negative}/{len(config_results)}")
    lines.append(f"- Best config improvement: ${best_diff:+.2f}")
    lines.append(f"- Worst config damage: ${worst_diff:+.2f}")
    lines.append(f"- Spread: ${best_diff - worst_diff:.0f} between best and worst")
    lines.append("")

    if all_configs_positive < len(config_results) * 0.6:
        lines.append("**WARNING: Less than 60% of configs are positive. This suggests trailing stop is NOT robustly beneficial.**")
        lines.append("The best config may be the result of overfitting to this specific dataset.")
    elif best_diff < 50:
        lines.append("**WARNING: Even the best config improves PnL by less than $50. This is noise, not signal.**")

    if best['winners_hurt'] > best['losers_saved']:
        lines.append(f"\n**CONCERN: Trailing HURTS more trades ({best['winners_hurt']}) than it SAVES ({best['losers_saved']}). Net positive only because saved amounts are larger per trade.**")

    if best['false_activations'] > 0:
        lines.append(f"\n**FALSE ACTIVATIONS: {best['false_activations']} trades activated trailing then exited early, missing the TP. Total left on table: ${best['false_activation_lost']:.0f}**")

    # Check consistency across periods
    recent_diff = best['recent_trailing_pnl'] - best['recent_actual_pnl']
    hist_diff = best['hist_trailing_pnl'] - best['hist_actual_pnl']
    if (recent_diff > 0) != (hist_diff > 0):
        lines.append(f"\n**INCONSISTENCY: Trailing is {'positive' if recent_diff > 0 else 'negative'} on recent data but {'positive' if hist_diff > 0 else 'negative'} on historical. This is a red flag for robustness.**")

    lines.append("")
    lines.append("### Bottom Line")
    if all_configs_positive > len(config_results) * 0.7 and best_diff > 100:
        lines.append("Trailing stop shows CONSISTENT benefit across most configurations. Worth implementing with the best config as starting point, but monitor live performance closely.")
    elif all_configs_positive > len(config_results) * 0.5:
        lines.append("Trailing stop shows MIXED results. Some configs help, some hurt. If implemented, use conservative settings and expect variance. Could be curve-fitting.")
    else:
        lines.append("Trailing stop is NOT reliably beneficial on this data. Most configurations hurt performance. The apparent benefit of the 'best' config is likely overfitting. DO NOT implement without further evidence.")

    with open(os.path.join(OUT_DIR, "summary.md"), 'w') as f:
        f.write('\n'.join(lines))

    # ─── Print summary to console ───
    print(f"\n{'='*60}")
    print(f"TRAILING STOP SIMULATION — {best['total_trades']} trades")
    print(f"{'='*60}")
    print(f"\nBest config: Activation={best_act}%, Trail Drop={best_drop}%")
    print(f"  Actual PnL:   ${best['actual_total_pnl']:>10.2f}")
    print(f"  Trailing PnL: ${best['trailing_total_pnl']:>10.2f}")
    print(f"  Net diff:     ${net:>+10.2f} ({net/abs(best['actual_total_pnl'])*100:+.1f}%)" if best['actual_total_pnl'] != 0 else f"  Net diff:     ${net:>+10.2f}")
    print(f"\n  Activated:    {best['trades_activated']}/{best['total_trades']}")
    print(f"  Winners hurt: {best['winners_hurt']} (${best['winners_hurt_pnl_lost']:.0f} lost)")
    print(f"  Losers saved: {best['losers_saved']} (${best['losers_saved_pnl']:.0f} saved)")
    print(f"  False activs: {best['false_activations']} (${best['false_activation_lost']:.0f} left)")
    print(f"\n  Configs positive: {all_configs_positive}/20")
    print(f"  Configs negative: {all_configs_negative}/20")

    print(f"\nConfig grid (Net PnL diff):")
    print(f"  {'':>6}", end='')
    for d in TRAIL_DROPS:
        print(f"  drop{d}%", end='')
    print()
    for act in ACTIVATIONS:
        print(f"  act{act:>2}%", end='')
        for drop in TRAIL_DROPS:
            m = config_results[(act, drop)]
            diff = m['trailing_total_pnl'] - m['actual_total_pnl']
            marker = "*" if (act, drop) == best_key else " "
            print(f"  {diff:>+6.0f}{marker}", end='')
        print()

    print(f"\nOutputs saved to {OUT_DIR}/")
    print("  - config_comparison.csv")
    print("  - per_trade.csv")
    print("  - summary.md")


if __name__ == '__main__':
    main()
