#!/usr/bin/env python3
"""
Full System Test: Actual vs Trailing Stop vs Trailing + Tighter TP
Simulates ALL closed trades bar-by-bar using 1h candle data.
"""

import sqlite3
import csv
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_PATH = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/market.db"
OUT_DIR = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/full_system_test"

# Trailing stop parameters
TRAIL_ACTIVATE_ROI = 8.0   # % ROI to activate trailing
TRAIL_DROP = 3.0            # % drop from peak ROI to trigger exit
TIGHT_TP_FACTOR = 0.80      # close at 80% of TP distance

# Fee already baked into actual PnL; apply same to simulated exits
FEE_RATE = 0.0004  # 0.04% roundtrip


def ts_to_epoch(ts_str):
    """Convert ISO timestamp string to epoch seconds (rounded to hour)."""
    if not ts_str:
        return None
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    # Round down to hour boundary for candle matching
    dt = dt.replace(minute=0, second=0, microsecond=0)
    return int(dt.timestamp())


def load_trades(conn):
    """Load all closed trades from both tables, deduplicating."""
    cur = conn.cursor()
    trades = []

    # --- fortix_trades (have SL/TP) ---
    cur.execute("""
        SELECT coin, direction, fill_price, sl_price, tp_price, exit_price,
               pnl_usd, exit_reason, leverage, filled_at, closed_at,
               position_size, notional, pnl_pct
        FROM fortix_trades
        WHERE status = 'CLOSED' AND fill_price > 0
        ORDER BY filled_at
    """)
    seen_keys = set()
    for row in cur.fetchall():
        (coin, direction, fill_price, sl_price, tp_price, exit_price,
         pnl_usd, exit_reason, leverage, filled_at, closed_at,
         position_size, notional, pnl_pct) = row

        # Dedup key: coin + direction + entry time (rounded to ms)
        key = (coin, direction, filled_at[:23] if filled_at else "")
        seen_keys.add(key)

        # Compute notional from position_size if notional is 0
        if (not notional or notional == 0) and position_size and fill_price:
            notional = fill_price * position_size

        trades.append({
            "source": "fortix",
            "coin": coin,
            "direction": direction,
            "entry_price": fill_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "exit_price": exit_price,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "exit_reason": exit_reason,
            "leverage": leverage or 1,
            "entry_time": filled_at,
            "exit_time": closed_at,
            "notional": notional or 0,
            "has_sl_tp": bool(sl_price and tp_price),
        })

    # --- okx_trades (no SL/TP) ---
    cur.execute("""
        SELECT coin, direction, entry_price, exit_price, pnl_usd, pnl_pct,
               exit_reason, leverage, entry_time, exit_time, notional
        FROM okx_trades
        ORDER BY entry_time
    """)
    for row in cur.fetchall():
        (coin, direction, entry_price, exit_price, pnl_usd, pnl_pct,
         exit_reason, leverage, entry_time, exit_time, notional) = row

        # Skip duplicates already in fortix
        key = (coin, direction, entry_time[:23] if entry_time else "")
        if key in seen_keys:
            continue

        trades.append({
            "source": "okx",
            "coin": coin,
            "direction": direction,
            "entry_price": entry_price,
            "sl_price": None,
            "tp_price": None,
            "exit_price": exit_price,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "exit_reason": exit_reason,
            "leverage": leverage or 1,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "notional": notional or 0,
            "has_sl_tp": False,
        })

    return trades


def get_candles(conn, coin, start_epoch, end_epoch):
    """Get 1h candles for a coin between start and end epochs."""
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, open, high, low, close
        FROM prices
        WHERE coin = ? AND timeframe = '1h'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
    """, (coin, start_epoch, end_epoch))
    return cur.fetchall()


def compute_roi(entry_price, current_price, direction, leverage):
    """Compute ROI % for a trade."""
    if direction == "LONG":
        return (current_price - entry_price) / entry_price * leverage * 100
    else:
        return (entry_price - current_price) / entry_price * leverage * 100


def compute_pnl_usd(entry_price, exit_price, direction, notional, leverage):
    """Compute PnL in USD from prices."""
    if notional <= 0:
        return 0
    margin = notional / leverage if leverage else notional
    if direction == "LONG":
        roi = (exit_price - entry_price) / entry_price
    else:
        roi = (entry_price - exit_price) / entry_price
    pnl = margin * leverage * roi
    # Subtract fees
    pnl -= notional * FEE_RATE
    return pnl


def simulate_trade(trade, candles, exit_epoch):
    """
    Simulate a trade through candles for all 3 scenarios.

    Key logic:
    - Walk candles from entry to actual exit time normally.
    - At the actual exit candle: if trailing is NOT active, fall back to actual exit.
    - Only continue PAST actual exit if trailing IS active (to capture trail profit).
    - NEWS_REACTION, PROFI_EXIT, etc. are honored if trailing hasn't activated.

    Returns dict with results for each scenario.
    """
    entry = trade["entry_price"]
    direction = trade["direction"]
    leverage = trade["leverage"]
    sl = trade["sl_price"]
    tp = trade["tp_price"]
    notional = trade["notional"]
    has_sl_tp = trade["has_sl_tp"]

    # Scenario 1: Actual
    actual_pnl = trade["pnl_usd"]
    actual_exit = trade["exit_reason"]

    # If no candle data or no SL/TP, scenarios 2 & 3 = actual
    if not candles or not has_sl_tp:
        return {
            "actual_pnl": actual_pnl,
            "actual_exit": actual_exit,
            "trail_pnl": actual_pnl,
            "trail_exit": actual_exit,
            "trail_tight_pnl": actual_pnl,
            "trail_tight_exit": actual_exit,
            "simulated": False,
            "peak_roi": 0,
            "note": "no_candles" if not candles else "no_sl_tp",
        }

    # Compute tight TP price
    tp_dist = abs(tp - entry)
    if direction == "LONG":
        tight_tp_price = entry + TIGHT_TP_FACTOR * tp_dist
    else:
        tight_tp_price = entry - TIGHT_TP_FACTOR * tp_dist

    # --- Scenario 2: Trailing Stop ---
    trail_active = False
    peak_roi = -999
    s2_pnl = None
    s2_exit = None
    s2_exit_price = None

    # --- Scenario 3: Trailing + Tighter TP ---
    s3_pnl = None
    s3_exit = None
    s3_exit_price = None
    s3_trail_active = False
    s3_peak_roi = -999

    # Within the trade window, we trust actual SL/TP behavior.
    # Only check trailing stop + tight TP as NEW mechanisms.
    # SL/TP checks only within window for trades that actually hit SL/TP.
    # Past the exit window, only continue if trailing is active.

    for ts, o, h, l, c in candles:
        past_exit = (exit_epoch is not None and ts > exit_epoch)

        # Compute ROI
        roi_close = compute_roi(entry, c, direction, leverage)

        if direction == "LONG":
            best_roi = compute_roi(entry, h, direction, leverage)
            worst_roi = compute_roi(entry, l, direction, leverage)
            sl_hit = l <= sl
            tp_hit = h >= tp
            tight_tp_hit = h >= tight_tp_price
        else:
            best_roi = compute_roi(entry, l, direction, leverage)
            worst_roi = compute_roi(entry, h, direction, leverage)
            sl_hit = h >= sl
            tp_hit = l <= tp
            tight_tp_hit = l <= tight_tp_price

        # === SCENARIO 2: + Trailing Stop ===
        if s2_pnl is None:
            # Always update peak ROI
            if best_roi > peak_roi:
                peak_roi = best_roi
            if peak_roi >= TRAIL_ACTIVATE_ROI:
                trail_active = True

            if not past_exit:
                # Within trade window:
                # - Trailing can fire early if activated (new mechanism)
                # - SL/TP fire as they actually did
                if trail_active and roi_close < peak_roi - TRAIL_DROP:
                    # Trailing fires before actual exit
                    s2_exit_price = c
                    s2_pnl = compute_pnl_usd(entry, c, direction, notional, leverage) if notional > 0 else actual_pnl
                    s2_exit = "TRAIL_EXIT"
                # No early SL/TP override - trust actual timing
            else:
                # Past actual exit
                if not trail_active:
                    # Trailing never activated -> use actual exit
                    s2_pnl = actual_pnl
                    s2_exit = actual_exit
                else:
                    # Trailing is active, extend trade past original exit
                    if sl_hit:
                        s2_pnl = compute_pnl_usd(entry, sl, direction, notional, leverage) if notional > 0 else trade["pnl_usd"]
                        s2_exit = "STOP_LOSS"
                        s2_exit_price = sl
                    elif tp_hit:
                        s2_pnl = compute_pnl_usd(entry, tp, direction, notional, leverage) if notional > 0 else trade["pnl_usd"]
                        s2_exit = "TARGET_HIT"
                        s2_exit_price = tp
                    elif roi_close < peak_roi - TRAIL_DROP:
                        s2_exit_price = c
                        s2_pnl = compute_pnl_usd(entry, c, direction, notional, leverage) if notional > 0 else actual_pnl
                        s2_exit = "TRAIL_EXIT"

        # === SCENARIO 3: + Trailing + Tighter TP ===
        if s3_pnl is None:
            if best_roi > s3_peak_roi:
                s3_peak_roi = best_roi
            if s3_peak_roi >= TRAIL_ACTIVATE_ROI:
                s3_trail_active = True

            if not past_exit:
                # Within trade window: tight TP and trailing can fire early
                if tight_tp_hit:
                    # Tight TP fires before actual exit
                    s3_exit_price = tight_tp_price
                    s3_pnl = compute_pnl_usd(entry, tight_tp_price, direction, notional, leverage) if notional > 0 else actual_pnl
                    s3_exit = "TIGHT_TP"
                elif s3_trail_active and roi_close < s3_peak_roi - TRAIL_DROP:
                    s3_exit_price = c
                    s3_pnl = compute_pnl_usd(entry, c, direction, notional, leverage) if notional > 0 else actual_pnl
                    s3_exit = "TRAIL_EXIT"
            else:
                if not s3_trail_active:
                    s3_pnl = actual_pnl
                    s3_exit = actual_exit
                else:
                    # Trailing active, extend past exit
                    if sl_hit:
                        s3_pnl = compute_pnl_usd(entry, sl, direction, notional, leverage) if notional > 0 else trade["pnl_usd"]
                        s3_exit = "STOP_LOSS"
                        s3_exit_price = sl
                    elif tight_tp_hit:
                        s3_exit_price = tight_tp_price
                        s3_pnl = compute_pnl_usd(entry, tight_tp_price, direction, notional, leverage) if notional > 0 else actual_pnl
                        s3_exit = "TIGHT_TP"
                    elif tp_hit:
                        s3_pnl = compute_pnl_usd(entry, tp, direction, notional, leverage) if notional > 0 else trade["pnl_usd"]
                        s3_exit = "TARGET_HIT"
                        s3_exit_price = tp
                    elif roi_close < s3_peak_roi - TRAIL_DROP:
                        s3_exit_price = c
                        s3_pnl = compute_pnl_usd(entry, c, direction, notional, leverage) if notional > 0 else actual_pnl
                        s3_exit = "TRAIL_EXIT"

    # If simulation didn't trigger exit, use actual outcome
    if s2_pnl is None:
        s2_pnl = actual_pnl
        s2_exit = actual_exit
    if s3_pnl is None:
        s3_pnl = actual_pnl
        s3_exit = actual_exit

    return {
        "actual_pnl": actual_pnl,
        "actual_exit": actual_exit,
        "trail_pnl": s2_pnl,
        "trail_exit": s2_exit,
        "trail_tight_pnl": s3_pnl,
        "trail_tight_exit": s3_exit,
        "simulated": True,
        "peak_roi": peak_roi,
        "note": "",
    }


def main():
    conn = sqlite3.connect(DB_PATH)
    trades = load_trades(conn)
    print(f"Loaded {len(trades)} trades ({sum(1 for t in trades if t['source']=='fortix')} fortix, "
          f"{sum(1 for t in trades if t['source']=='okx')} okx)")

    results = []
    skipped = []
    candle_cache = {}

    for i, trade in enumerate(trades):
        coin = trade["coin"]
        entry_epoch = ts_to_epoch(trade["entry_time"])
        exit_epoch = ts_to_epoch(trade["exit_time"])

        if not entry_epoch or not exit_epoch:
            skipped.append((coin, "no_timestamps"))
            result = {
                "actual_pnl": trade["pnl_usd"],
                "actual_exit": trade["exit_reason"],
                "trail_pnl": trade["pnl_usd"],
                "trail_exit": trade["exit_reason"],
                "trail_tight_pnl": trade["pnl_usd"],
                "trail_tight_exit": trade["exit_reason"],
                "simulated": False,
                "peak_roi": 0,
                "note": "no_timestamps",
            }
        else:
            # Extend 4 hours past exit for trailing opportunity
            end_epoch = exit_epoch + 4 * 3600

            cache_key = (coin, entry_epoch, end_epoch)
            if cache_key not in candle_cache:
                candle_cache[cache_key] = get_candles(conn, coin, entry_epoch, end_epoch)
            candles = candle_cache[cache_key]

            if not candles and trade["has_sl_tp"]:
                skipped.append((coin, "no_candle_data"))

            result = simulate_trade(trade, candles, exit_epoch)

        result["coin"] = coin
        result["direction"] = trade["direction"]
        result["leverage"] = trade["leverage"]
        result["entry_time"] = trade["entry_time"]
        result["exit_time"] = trade["exit_time"]
        result["source"] = trade["source"]
        result["notional"] = trade["notional"]
        result["entry_price"] = trade["entry_price"]
        result["sl_price"] = trade["sl_price"]
        result["tp_price"] = trade["tp_price"]
        result["has_sl_tp"] = trade["has_sl_tp"]
        results.append(result)

    conn.close()

    # Print skip info
    if skipped:
        print(f"\nSkipped/degraded {len(skipped)} trades:")
        for coin, reason in skipped:
            print(f"  {coin}: {reason}")

    # === COMPUTE METRICS ===
    # Per-trade CSV
    trade_rows = []
    for r in results:
        date = r["entry_time"][:10] if r["entry_time"] else "unknown"
        trade_rows.append({
            "date": date,
            "coin": r["coin"],
            "direction": r["direction"],
            "leverage": r["leverage"],
            "source": r["source"],
            "entry_price": r["entry_price"],
            "sl_price": r["sl_price"] or "",
            "tp_price": r["tp_price"] or "",
            "actual_pnl": round(r["actual_pnl"], 2),
            "actual_exit": r["actual_exit"],
            "trail_pnl": round(r["trail_pnl"], 2),
            "trail_exit": r["trail_exit"],
            "trail_tight_pnl": round(r["trail_tight_pnl"], 2),
            "trail_tight_exit": r["trail_tight_exit"],
            "peak_roi": round(r["peak_roi"], 2),
            "simulated": r["simulated"],
            "note": r.get("note", ""),
        })

    # Write trades CSV
    trades_csv = os.path.join(OUT_DIR, "trades.csv")
    with open(trades_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trade_rows[0].keys())
        writer.writeheader()
        writer.writerows(trade_rows)
    print(f"\nWrote {len(trade_rows)} trades to {trades_csv}")

    # === DAILY METRICS ===
    daily = defaultdict(lambda: {
        "s1_trades": 0, "s1_wins": 0, "s1_pnl": 0, "s1_win_pnl": 0, "s1_loss_pnl": 0,
        "s2_trades": 0, "s2_wins": 0, "s2_pnl": 0, "s2_win_pnl": 0, "s2_loss_pnl": 0,
        "s3_trades": 0, "s3_wins": 0, "s3_pnl": 0, "s3_win_pnl": 0, "s3_loss_pnl": 0,
    })

    for r in results:
        date = r["entry_time"][:10] if r["entry_time"] else "unknown"
        d = daily[date]

        for scenario, pnl_key in [("s1", "actual_pnl"), ("s2", "trail_pnl"), ("s3", "trail_tight_pnl")]:
            pnl = r[pnl_key]
            d[f"{scenario}_trades"] += 1
            d[f"{scenario}_pnl"] += pnl
            if pnl > 0:
                d[f"{scenario}_wins"] += 1
                d[f"{scenario}_win_pnl"] += pnl
            else:
                d[f"{scenario}_loss_pnl"] += pnl

    # Write daily CSV
    daily_rows = []
    for date in sorted(daily.keys()):
        d = daily[date]
        row = {"date": date}
        for s in ["s1", "s2", "s3"]:
            trades_n = d[f"{s}_trades"]
            wins = d[f"{s}_wins"]
            losses = trades_n - wins
            pnl = d[f"{s}_pnl"]
            win_pnl = d[f"{s}_win_pnl"]
            loss_pnl = d[f"{s}_loss_pnl"]

            row[f"{s}_trades"] = trades_n
            row[f"{s}_wins"] = wins
            row[f"{s}_win_pct"] = round(wins / trades_n * 100, 1) if trades_n > 0 else 0
            row[f"{s}_pnl"] = round(pnl, 2)
            row[f"{s}_roi_pct"] = round(pnl / 5000 * 100, 2)
            row[f"{s}_avg_win"] = round(win_pnl / wins, 2) if wins > 0 else 0
            row[f"{s}_avg_loss"] = round(loss_pnl / losses, 2) if losses > 0 else 0
            pf_denom = abs(loss_pnl) if loss_pnl != 0 else 1
            row[f"{s}_pf"] = round(win_pnl / pf_denom, 2) if loss_pnl != 0 else (999 if win_pnl > 0 else 0)
        daily_rows.append(row)

    daily_csv = os.path.join(OUT_DIR, "daily_comparison.csv")
    with open(daily_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=daily_rows[0].keys())
        writer.writeheader()
        writer.writerows(daily_rows)
    print(f"Wrote daily comparison to {daily_csv}")

    # === GRAND TOTALS ===
    def compute_totals(results, pnl_key):
        total_pnl = 0
        wins = 0
        losses = 0
        win_pnl = 0
        loss_pnl = 0
        equity = 5000
        peak_equity = 5000
        max_dd = 0
        equity_curve = []

        for r in sorted(results, key=lambda x: x.get("entry_time", "")):
            pnl = r[pnl_key]
            total_pnl += pnl
            equity += pnl
            equity_curve.append(equity)
            if equity > peak_equity:
                peak_equity = equity
            dd = (peak_equity - equity) / peak_equity * 100
            if dd > max_dd:
                max_dd = dd
            if pnl > 0:
                wins += 1
                win_pnl += pnl
            else:
                losses += 1
                loss_pnl += pnl

        n = len(results)
        return {
            "trades": n,
            "wins": wins,
            "losses": losses,
            "win_pct": round(wins / n * 100, 1) if n > 0 else 0,
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(total_pnl / 5000 * 100, 2),
            "avg_win": round(win_pnl / wins, 2) if wins > 0 else 0,
            "avg_loss": round(loss_pnl / losses, 2) if losses > 0 else 0,
            "profit_factor": round(win_pnl / abs(loss_pnl), 2) if loss_pnl != 0 else 999,
            "max_dd_pct": round(max_dd, 2),
            "final_equity": round(equity, 2),
        }

    s1 = compute_totals(results, "actual_pnl")
    s2 = compute_totals(results, "trail_pnl")
    s3 = compute_totals(results, "trail_tight_pnl")

    # Winning days
    for s, pnl_key in [(s1, "s1_pnl"), (s2, "s2_pnl"), (s3, "s3_pnl")]:
        win_days = sum(1 for d in daily.values() if d[pnl_key] > 0)
        total_days = len(daily)
        s["winning_days"] = f"{win_days}/{total_days}"

    # === CATEGORY SHIFTS ===
    exit_shifts = defaultdict(int)
    for r in results:
        if r["actual_exit"] != r["trail_exit"]:
            exit_shifts[f"{r['actual_exit']} -> {r['trail_exit']}"] += 1
    exit_shifts_tight = defaultdict(int)
    for r in results:
        if r["actual_exit"] != r["trail_tight_exit"]:
            exit_shifts_tight[f"{r['actual_exit']} -> {r['trail_tight_exit']}"] += 1

    # === COIN ANALYSIS ===
    coin_impact = defaultdict(lambda: {"actual": 0, "trail": 0, "tight": 0, "count": 0})
    for r in results:
        ci = coin_impact[r["coin"]]
        ci["actual"] += r["actual_pnl"]
        ci["trail"] += r["trail_pnl"]
        ci["tight"] += r["trail_tight_pnl"]
        ci["count"] += 1

    # === WRITE SUMMARY ===
    summary_path = os.path.join(OUT_DIR, "summary.md")
    with open(summary_path, "w") as f:
        f.write("# Full System Test: Trailing Stop + Tighter TP\n\n")
        f.write(f"**Date range:** {sorted(daily.keys())[0]} to {sorted(daily.keys())[-1]}\n")
        f.write(f"**Total trades:** {len(results)} ({sum(1 for r in results if r['source']=='fortix')} fortix, "
                f"{sum(1 for r in results if r['source']=='okx')} okx)\n")
        f.write(f"**Simulated (has SL/TP + candles):** {sum(1 for r in results if r['simulated'])}\n")
        f.write(f"**Not simulated (no SL/TP):** {sum(1 for r in results if not r['simulated'])}\n\n")

        f.write("## 1. Grand Total Comparison\n\n")
        f.write("| Metric | S1: Actual | S2: +Trailing | S3: +Trail+TightTP |\n")
        f.write("|--------|-----------|--------------|--------------------|\n")
        for metric in ["trades", "wins", "win_pct", "total_pnl", "roi_pct",
                       "avg_win", "avg_loss", "profit_factor", "max_dd_pct",
                       "final_equity", "winning_days"]:
            label = metric.replace("_", " ").title()
            v1 = s1[metric]
            v2 = s2[metric]
            v3 = s3[metric]
            if isinstance(v1, float):
                f.write(f"| {label} | {v1:.2f} | {v2:.2f} | {v3:.2f} |\n")
            else:
                f.write(f"| {label} | {v1} | {v2} | {v3} |\n")

        f.write("\n## 2. Per-Day Comparison\n\n")
        f.write("| Date | S1 PnL | S1 Win% | S2 PnL | S2 Win% | S3 PnL | S3 Win% |\n")
        f.write("|------|--------|---------|--------|---------|--------|--------|\n")
        for row in daily_rows:
            f.write(f"| {row['date']} | ${row['s1_pnl']:.2f} | {row['s1_win_pct']}% | "
                    f"${row['s2_pnl']:.2f} | {row['s2_win_pct']}% | "
                    f"${row['s3_pnl']:.2f} | {row['s3_win_pct']}% |\n")

        f.write("\n## 3. Win Rate Comparison\n\n")
        f.write(f"- S1 Actual: {s1['win_pct']}% ({s1['wins']}/{s1['trades']})\n")
        f.write(f"- S2 Trailing: {s2['win_pct']}% ({s2['wins']}/{s2['trades']})\n")
        f.write(f"- S3 Trail+Tight: {s3['win_pct']}% ({s3['wins']}/{s3['trades']})\n")

        f.write("\n## 4. Exit Category Shifts\n\n")
        f.write("### S2 (Trailing) shifts from actual:\n")
        for shift, count in sorted(exit_shifts.items(), key=lambda x: -x[1]):
            f.write(f"- {shift}: {count} trades\n")
        if not exit_shifts:
            f.write("- No shifts\n")

        f.write("\n### S3 (Trail+TightTP) shifts from actual:\n")
        for shift, count in sorted(exit_shifts_tight.items(), key=lambda x: -x[1]):
            f.write(f"- {shift}: {count} trades\n")
        if not exit_shifts_tight:
            f.write("- No shifts\n")

        f.write("\n## 5. Coins That BENEFIT Most from Trailing\n\n")
        f.write("| Coin | Trades | Actual PnL | Trail PnL | Diff | Trail+Tight PnL | Diff |\n")
        f.write("|------|--------|-----------|-----------|------|----------------|------|\n")
        sorted_coins = sorted(coin_impact.items(), key=lambda x: x[1]["trail"] - x[1]["actual"], reverse=True)
        for coin, ci in sorted_coins[:10]:
            diff2 = ci["trail"] - ci["actual"]
            diff3 = ci["tight"] - ci["actual"]
            f.write(f"| {coin} | {ci['count']} | ${ci['actual']:.2f} | ${ci['trail']:.2f} | "
                    f"${diff2:+.2f} | ${ci['tight']:.2f} | ${diff3:+.2f} |\n")

        f.write("\n## 6. Coins HURT by Trailing\n\n")
        f.write("| Coin | Trades | Actual PnL | Trail PnL | Diff | Trail+Tight PnL | Diff |\n")
        f.write("|------|--------|-----------|-----------|------|----------------|------|\n")
        for coin, ci in sorted_coins[-10:]:
            diff2 = ci["trail"] - ci["actual"]
            diff3 = ci["tight"] - ci["actual"]
            if diff2 < 0 or diff3 < 0:
                f.write(f"| {coin} | {ci['count']} | ${ci['actual']:.2f} | ${ci['trail']:.2f} | "
                        f"${diff2:+.2f} | ${ci['tight']:.2f} | ${diff3:+.2f} |\n")

        f.write("\n## 7. Days Where Trailing SAVED vs HURT\n\n")
        for row in daily_rows:
            date = row["date"]
            s1p = row["s1_pnl"]
            s2p = row["s2_pnl"]
            s3p = row["s3_pnl"]
            diff2 = s2p - s1p
            diff3 = s3p - s1p
            verdict2 = "SAVED" if diff2 > 0 else "HURT" if diff2 < 0 else "SAME"
            verdict3 = "SAVED" if diff3 > 0 else "HURT" if diff3 < 0 else "SAME"
            f.write(f"- **{date}**: Trail {verdict2} (${diff2:+.2f}), Trail+Tight {verdict3} (${diff3:+.2f})\n")

        f.write("\n## 8. Robustness Assessment\n\n")
        # Check if improvement is driven by few outliers
        diffs = [(r["trail_pnl"] - r["actual_pnl"]) for r in results if r["simulated"]]
        diffs_tight = [(r["trail_tight_pnl"] - r["actual_pnl"]) for r in results if r["simulated"]]
        simulated_count = sum(1 for r in results if r["simulated"])

        if diffs:
            positive_diffs = [d for d in diffs if d > 0]
            negative_diffs = [d for d in diffs if d < 0]
            f.write(f"**Simulated trades:** {simulated_count} of {len(results)} total\n\n")
            f.write(f"**S2 Trailing impact on simulated trades:**\n")
            f.write(f"- Trades improved: {len(positive_diffs)}\n")
            f.write(f"- Trades worsened: {len(negative_diffs)}\n")
            f.write(f"- Trades unchanged: {len(diffs) - len(positive_diffs) - len(negative_diffs)}\n")
            if positive_diffs:
                f.write(f"- Total improvement: ${sum(positive_diffs):.2f}\n")
                f.write(f"- Largest single improvement: ${max(positive_diffs):.2f}\n")
            if negative_diffs:
                f.write(f"- Total worsening: ${sum(negative_diffs):.2f}\n")
                f.write(f"- Largest single worsening: ${min(negative_diffs):.2f}\n")
            net = sum(diffs)
            f.write(f"- **Net impact: ${net:+.2f}**\n")

            # Top 3 contributors
            trade_diffs = [(r, r["trail_pnl"] - r["actual_pnl"]) for r in results if r["simulated"]]
            trade_diffs.sort(key=lambda x: abs(x[1]), reverse=True)
            f.write(f"\nTop 5 biggest changes (trailing):\n")
            for r, d in trade_diffs[:5]:
                f.write(f"- {r['coin']} {r['direction']} ({r['entry_time'][:16]}): ${d:+.2f} "
                        f"(actual={r['actual_exit']}, trail={r['trail_exit']})\n")

            # Is it driven by outliers?
            if positive_diffs and len(positive_diffs) >= 2:
                top2 = sorted(positive_diffs, reverse=True)[:2]
                outlier_pct = sum(top2) / sum(positive_diffs) * 100 if sum(positive_diffs) > 0 else 0
                f.write(f"\nOutlier check: top 2 improvements = ${sum(top2):.2f} = "
                        f"{outlier_pct:.0f}% of total improvement\n")
                if outlier_pct > 70:
                    f.write("**WARNING: Improvement heavily driven by few outliers. Not robust.**\n")
                else:
                    f.write("Improvement reasonably distributed across trades.\n")
            if net > 0 and simulated_count < 30:
                f.write(f"\n**WARNING: Only {simulated_count} simulated trades. Sample too small for confidence.**\n")

        f.write(f"\n**S3 Trailing+TightTP impact on simulated trades:**\n")
        if diffs_tight:
            positive_dt = [d for d in diffs_tight if d > 0]
            negative_dt = [d for d in diffs_tight if d < 0]
            f.write(f"- Trades improved: {len(positive_dt)}\n")
            f.write(f"- Trades worsened: {len(negative_dt)}\n")
            f.write(f"- Trades unchanged: {len(diffs_tight) - len(positive_dt) - len(negative_dt)}\n")
            net3 = sum(diffs_tight)
            f.write(f"- **Net impact: ${net3:+.2f}**\n")

        f.write("\n## 9. Projected Daily ROI if Deployed\n\n")
        n_days = len(daily)
        if n_days > 0:
            for label, totals in [("S1 Actual", s1), ("S2 Trailing", s2), ("S3 Trail+Tight", s3)]:
                daily_pnl = totals["total_pnl"] / n_days
                daily_roi = daily_pnl / 5000 * 100
                f.write(f"- **{label}**: ${daily_pnl:.2f}/day = {daily_roi:.2f}%/day on $5000\n")
            f.write(f"\n*Based on {n_days} trading days, {len(results)} total trades.*\n")
            f.write(f"*{sum(1 for r in results if not r['simulated'])} trades could not be simulated "
                    f"(no SL/TP data) — scenarios 2 & 3 use actual PnL for those.*\n")

    print(f"Wrote summary to {summary_path}")

    # === PRINT KEY RESULTS ===
    print("\n" + "=" * 70)
    print("GRAND TOTAL COMPARISON")
    print("=" * 70)
    header = f"{'Metric':<20} {'S1:Actual':>12} {'S2:+Trail':>12} {'S3:+Trail+TTP':>14}"
    print(header)
    print("-" * 60)
    for metric in ["trades", "wins", "win_pct", "total_pnl", "roi_pct",
                   "avg_win", "avg_loss", "profit_factor", "max_dd_pct",
                   "final_equity", "winning_days"]:
        label = metric.replace("_", " ").title()
        v1 = s1[metric]
        v2 = s2[metric]
        v3 = s3[metric]
        if isinstance(v1, float):
            print(f"{label:<20} {v1:>12.2f} {v2:>12.2f} {v3:>14.2f}")
        else:
            print(f"{label:<20} {str(v1):>12} {str(v2):>12} {str(v3):>14}")

    print("\n" + "=" * 70)
    print("PER-DAY PNL")
    print("=" * 70)
    print(f"{'Date':<12} {'S1 PnL':>10} {'S2 PnL':>10} {'S3 PnL':>10} {'S2 diff':>10} {'S3 diff':>10}")
    print("-" * 62)
    for row in daily_rows:
        s2d = row["s2_pnl"] - row["s1_pnl"]
        s3d = row["s3_pnl"] - row["s1_pnl"]
        print(f"{row['date']:<12} ${row['s1_pnl']:>8.2f} ${row['s2_pnl']:>8.2f} ${row['s3_pnl']:>8.2f} "
              f"${s2d:>+8.2f} ${s3d:>+8.2f}")

    # Total
    print("-" * 62)
    s2td = s2["total_pnl"] - s1["total_pnl"]
    s3td = s3["total_pnl"] - s1["total_pnl"]
    print(f"{'TOTAL':<12} ${s1['total_pnl']:>8.2f} ${s2['total_pnl']:>8.2f} ${s3['total_pnl']:>8.2f} "
          f"${s2td:>+8.2f} ${s3td:>+8.2f}")


if __name__ == "__main__":
    main()
