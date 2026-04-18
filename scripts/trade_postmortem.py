#!/usr/bin/env python3
"""
Trade Postmortem Analysis — Apr 15-17 2026
Reconstructs price paths for every closed trade, classifies outcomes,
and simulates trailing stops.
"""

import re
import csv
import sqlite3
from datetime import datetime, timezone, timedelta

BASE = "/Users/williamstorm/Documents/Trading (OKX) 1h"
LOG = f"{BASE}/logs/trader_bybit.log"
DB = f"{BASE}/data/crypto/market.db"
OUT = f"{BASE}/data/crypto/trade_postmortem"

EEST = timezone(timedelta(hours=3))

# ── Step 1: Parse all FILLED and CLOSED lines ───────────────────────────────

def parse_trades():
    """Match FILLED entries to their CLOSED exits for Apr 15-17."""

    filled_re = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\] trader_bybit: "
        r"FILLED #\d+: (LONG|SHORT) (\S+) ([\d.]+)@\$([\d.]+) (\d+)x "
        r"SL=\$([\d.]+) TP=\$([\d.]+) margin=\$([\d.]+)"
    )

    closed_re = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\] trader_bybit: "
        r"CLOSED: (LONG|SHORT) (\S+) (\d+)x ROI=([+-][\d.]+)% \$([+-]?[\d.]+) "
        r"\[(\w+)\]"
    )

    # Also parse TIME_EXIT / NEWS_REACTION / PROFI_EXIT closes
    time_exit_re = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[INFO\] trader_bybit: "
        r"CLOSE (\S+): (\w+) \| ([+-]?[\d.]+)% lev \| \$([+-]?[\d.]+) \| (\d+)min"
    )

    filled_stack = {}   # coin -> list of filled entries (FIFO)
    trades = []

    with open(LOG) as f:
        for line in f:
            # Filter to Apr 15-17
            if not (line.startswith("2026-04-15") or line.startswith("2026-04-16") or line.startswith("2026-04-17")):
                continue

            m = filled_re.search(line)
            if m:
                ts_str, direction, coin, qty, price, leverage, sl, tp, margin = m.groups()
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EEST)
                entry = {
                    "fill_time_eest": ts,
                    "fill_time_utc": ts.astimezone(timezone.utc),
                    "direction": direction,
                    "coin": coin,
                    "qty": float(qty),
                    "entry_price": float(price),
                    "leverage": int(leverage),
                    "sl": float(sl),
                    "tp": float(tp),
                    "margin": float(margin),
                }
                filled_stack.setdefault(coin, []).append(entry)
                continue

            m = closed_re.search(line)
            if m:
                ts_str, direction, coin, leverage, roi, pnl, reason = m.groups()
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EEST)
                # Match to earliest filled entry for this coin+direction
                stack = filled_stack.get(coin, [])
                entry = None
                for i, e in enumerate(stack):
                    if e["direction"] == direction:
                        entry = stack.pop(i)
                        break
                if entry is None:
                    # Might be a trade opened before Apr 15
                    entry = {
                        "fill_time_eest": None,
                        "fill_time_utc": None,
                        "direction": direction,
                        "coin": coin,
                        "qty": 0,
                        "entry_price": 0,
                        "leverage": int(leverage),
                        "sl": 0,
                        "tp": 0,
                        "margin": 350,
                    }

                trades.append({
                    **entry,
                    "close_time_eest": ts,
                    "close_time_utc": ts.astimezone(timezone.utc),
                    "exit_reason": reason,
                    "roi_pct": float(roi),
                    "pnl": float(pnl),
                })
                continue

            m = time_exit_re.search(line)
            if m:
                ts_str, coin, reason, roi, pnl, mins = m.groups()
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EEST)
                # Find matching fill
                stack = filled_stack.get(coin, [])
                entry = None
                for i, e in enumerate(stack):
                    entry = stack.pop(i)
                    break
                if entry is None:
                    continue

                trades.append({
                    **entry,
                    "close_time_eest": ts,
                    "close_time_utc": ts.astimezone(timezone.utc),
                    "exit_reason": reason,
                    "roi_pct": float(roi),
                    "pnl": float(pnl),
                })

    return trades


# ── Step 2-4: Price path reconstruction ─────────────────────────────────────

def reconstruct_paths(trades):
    """For each trade, get 1h candles between entry and exit, compute unrealized PnL."""
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for t in trades:
        if t["fill_time_utc"] is None or t["entry_price"] == 0:
            t["candles"] = []
            t["peak_roi"] = 0
            t["peak_time_utc"] = None
            t["peak_minutes"] = 0
            t["worst_roi"] = 0
            t["worst_time_utc"] = None
            t["tp_distance_reached_pct"] = 0
            continue

        entry_ts = int(t["fill_time_utc"].timestamp())
        # Add buffer: start from hour before entry, go to hour after exit
        close_ts = int(t["close_time_utc"].timestamp())
        # Round to hour boundaries
        start_hour = (entry_ts // 3600) * 3600
        end_hour = ((close_ts // 3600) + 1) * 3600

        cur.execute(
            "SELECT timestamp, open, high, low, close FROM prices "
            "WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp",
            (t["coin"], start_hour, end_hour)
        )
        candles = cur.fetchall()

        entry_price = t["entry_price"]
        direction = t["direction"]
        leverage = t["leverage"]

        peak_roi = -999
        worst_roi = 999
        peak_ts = None
        worst_ts = None

        candle_data = []
        for ts, o, h, l, c in candles:
            if direction == "LONG":
                best_in_candle = ((h - entry_price) / entry_price) * leverage * 100
                worst_in_candle = ((l - entry_price) / entry_price) * leverage * 100
                close_roi = ((c - entry_price) / entry_price) * leverage * 100
            else:  # SHORT
                best_in_candle = ((entry_price - l) / entry_price) * leverage * 100
                worst_in_candle = ((entry_price - h) / entry_price) * leverage * 100
                close_roi = ((entry_price - c) / entry_price) * leverage * 100

            candle_data.append({
                "ts": ts,
                "open": o, "high": h, "low": l, "close": c,
                "best_roi": best_in_candle,
                "worst_roi": worst_in_candle,
                "close_roi": close_roi,
            })

            if best_in_candle > peak_roi:
                peak_roi = best_in_candle
                peak_ts = ts
            if worst_in_candle < worst_roi:
                worst_roi = worst_in_candle
                worst_ts = ts

        t["candles"] = candle_data
        t["peak_roi"] = round(peak_roi, 2) if peak_roi > -999 else 0
        t["peak_time_utc"] = datetime.fromtimestamp(peak_ts, tz=timezone.utc) if peak_ts else None
        t["peak_minutes"] = round((peak_ts - entry_ts) / 60) if peak_ts else 0
        t["worst_roi"] = round(worst_roi, 2) if worst_roi < 999 else 0
        t["worst_time_utc"] = datetime.fromtimestamp(worst_ts, tz=timezone.utc) if worst_ts else None

        # TP distance reached
        tp = t["tp"]
        sl = t["sl"]
        if direction == "LONG":
            tp_roi = ((tp - entry_price) / entry_price) * leverage * 100
        else:
            tp_roi = ((entry_price - tp) / entry_price) * leverage * 100

        t["tp_roi_target"] = round(tp_roi, 2)
        if tp_roi > 0:
            t["tp_distance_reached_pct"] = round(min(100, peak_roi / tp_roi * 100), 1)
        else:
            t["tp_distance_reached_pct"] = 0

        # Check what happened AFTER SL hit — did price eventually reach TP?
        # (For "too tight SL" classification)
        t["would_have_hit_tp_after_sl"] = False
        if t["exit_reason"] == "STOP_LOSS" and candle_data:
            sl_ts = int(t["close_time_utc"].timestamp())
            for cd in candle_data:
                if cd["ts"] > sl_ts:
                    if direction == "LONG" and cd["high"] >= tp:
                        t["would_have_hit_tp_after_sl"] = True
                        break
                    elif direction == "SHORT" and cd["low"] <= tp:
                        t["would_have_hit_tp_after_sl"] = True
                        break

        # Extended check: query candles for 12h after SL
        if t["exit_reason"] == "STOP_LOSS" and not t["would_have_hit_tp_after_sl"]:
            sl_ts = int(t["close_time_utc"].timestamp())
            cur.execute(
                "SELECT timestamp, high, low FROM prices "
                "WHERE coin=? AND timeframe='1h' AND timestamp > ? AND timestamp <= ? "
                "ORDER BY timestamp",
                (t["coin"], sl_ts, sl_ts + 43200)
            )
            for ts2, h2, l2 in cur.fetchall():
                if direction == "LONG" and h2 >= tp:
                    t["would_have_hit_tp_after_sl"] = True
                    break
                elif direction == "SHORT" and l2 <= tp:
                    t["would_have_hit_tp_after_sl"] = True
                    break

    conn.close()
    return trades


# ── Step 5: Classify trades ─────────────────────────────────────────────────

def classify(t):
    reason = t["exit_reason"]
    peak = t["peak_roi"]
    pnl = t["pnl"]
    tp_reached = t.get("tp_distance_reached_pct", 0)

    if reason == "TARGET_HIT":
        return "A", "WINNER (TP hit)"

    if reason in ("TIME_EXIT", "NEWS_REACTION", "PROFI_EXIT"):
        if pnl >= 0:
            return "F", "TIME_EXIT OK (profit)"
        else:
            return "G", "TIME_EXIT LOST (loss)"

    # STOP_LOSS cases
    if reason == "STOP_LOSS":
        # Could-have-won: peak > 5% ROI
        if peak > 5:
            return "B", "COULD-HAVE-WON (trailing would save)"

        # Too tight SL: had some profit, hit SL, then TP would have been reached
        if peak > 2 and t.get("would_have_hit_tp_after_sl", False):
            return "D", "TOO TIGHT SL (wider SL saves)"

        # Too wide TP: reached 50-80% of TP distance but reversed
        if 50 <= tp_reached < 100 and peak > 2:
            return "E", "TOO WIDE TP (tighter TP captures)"

        # Wrong direction: never had > 2% profit
        if peak <= 2:
            return "C", "WRONG DIRECTION (no fix)"

        # Default SL with some profit but doesn't fit above
        if peak > 2:
            return "E", "TOO WIDE TP (tighter TP captures)"

    return "C", "WRONG DIRECTION (no fix)"


# ── Step 6: Trailing stop simulation ────────────────────────────────────────

def simulate_trailing(t):
    """For B trades: simulate trailing stop that activates at peak-3% and closes at 50% of peak."""
    if not t["candles"] or t["entry_price"] == 0:
        return None

    entry_price = t["entry_price"]
    direction = t["direction"]
    leverage = t["leverage"]
    margin = t["margin"]

    peak_seen = 0
    trailing_active = False
    activation_threshold = 3.0  # Activate trailing after 3% ROI
    trailing_pct = 50  # Close when drops to 50% of peak

    for cd in t["candles"]:
        if direction == "LONG":
            candle_best = ((cd["high"] - entry_price) / entry_price) * leverage * 100
            candle_close = ((cd["close"] - entry_price) / entry_price) * leverage * 100
        else:
            candle_best = ((entry_price - cd["low"]) / entry_price) * leverage * 100
            candle_close = ((entry_price - cd["close"]) / entry_price) * leverage * 100

        if candle_best > peak_seen:
            peak_seen = candle_best

        if peak_seen >= activation_threshold:
            trailing_active = True

        if trailing_active:
            trail_stop_level = peak_seen * (trailing_pct / 100)
            # Check if candle dipped below trailing stop
            if candle_close <= trail_stop_level or cd["worst_roi"] <= trail_stop_level:
                # Would close at trailing level
                trailing_roi = trail_stop_level
                trailing_pnl = margin * (trailing_roi / 100)
                return {
                    "trailing_roi": round(trailing_roi, 2),
                    "trailing_pnl": round(trailing_pnl, 2),
                    "peak_roi_at_trail": round(peak_seen, 2),
                    "saved_vs_actual": round(trailing_pnl - t["pnl"], 2),
                }

    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Parsing trades from log...")
    trades = parse_trades()
    print(f"  Found {len(trades)} closed trades in Apr 15-17")

    # Remove duplicates (TIME_EXIT + CLOSED for same trade)
    # The TIME_EXIT CLOSE lines come first, then CLOSED lines follow
    # We need to deduplicate: prefer the CLOSED line (has [TARGET_HIT]/[STOP_LOSS])
    # Group by coin + approximate close time
    seen = {}
    unique_trades = []
    for t in trades:
        key = (t["coin"], t["direction"], t["close_time_utc"].strftime("%Y-%m-%d %H:%M") if t["close_time_utc"] else "")
        # If we already have a CLOSED entry for same time window (within 2 min), skip
        close_ts = int(t["close_time_utc"].timestamp()) if t["close_time_utc"] else 0
        found_dup = False
        for existing_key, existing_ts, existing_reason in list(seen.values()):
            if t["coin"] == existing_key[0] and t["direction"] == existing_key[1]:
                if abs(close_ts - existing_ts) < 120:  # within 2 min
                    found_dup = True
                    break

        if not found_dup:
            idx = len(unique_trades)
            seen[idx] = (
                (t["coin"], t["direction"]),
                close_ts,
                t["exit_reason"]
            )
            unique_trades.append(t)

    trades = unique_trades
    print(f"  After dedup: {len(trades)} unique trades")

    print("Reconstructing price paths...")
    trades = reconstruct_paths(trades)

    print("Classifying trades...")
    categories = {"A": [], "B": [], "C": [], "D": [], "E": [], "F": [], "G": []}

    for t in trades:
        cat, desc = classify(t)
        t["category"] = cat
        t["category_desc"] = desc
        categories[cat].append(t)

        # Simulate trailing for B trades
        t["trailing_sim"] = None
        if cat == "B":
            t["trailing_sim"] = simulate_trailing(t)

    # ── Write CSV ────────────────────────────────────────────────────────
    csv_path = f"{OUT}/postmortem.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "trade_id", "coin", "direction", "leverage", "entry_price",
            "sl", "tp", "margin",
            "fill_time_utc", "close_time_utc", "hold_minutes",
            "exit_reason", "roi_pct", "pnl",
            "peak_roi", "peak_time_utc", "peak_minutes",
            "worst_roi", "tp_target_roi", "tp_distance_reached_pct",
            "would_hit_tp_after_sl",
            "category", "category_desc",
            "trailing_roi", "trailing_pnl", "trailing_saved",
        ])
        for i, t in enumerate(trades):
            fill_utc = t["fill_time_utc"].strftime("%Y-%m-%d %H:%M") if t["fill_time_utc"] else ""
            close_utc = t["close_time_utc"].strftime("%Y-%m-%d %H:%M") if t["close_time_utc"] else ""

            hold_min = ""
            if t["fill_time_utc"] and t["close_time_utc"]:
                hold_min = round((t["close_time_utc"] - t["fill_time_utc"]).total_seconds() / 60)

            peak_utc = t["peak_time_utc"].strftime("%Y-%m-%d %H:%M") if t.get("peak_time_utc") else ""

            ts = t.get("trailing_sim")
            w.writerow([
                i + 1, t["coin"], t["direction"], t["leverage"], t["entry_price"],
                t["sl"], t["tp"], t["margin"],
                fill_utc, close_utc, hold_min,
                t["exit_reason"], t["roi_pct"], t["pnl"],
                t.get("peak_roi", ""), peak_utc, t.get("peak_minutes", ""),
                t.get("worst_roi", ""), t.get("tp_roi_target", ""), t.get("tp_distance_reached_pct", ""),
                t.get("would_have_hit_tp_after_sl", ""),
                t["category"], t["category_desc"],
                ts["trailing_roi"] if ts else "",
                ts["trailing_pnl"] if ts else "",
                ts["saved_vs_actual"] if ts else "",
            ])

    print(f"  CSV written: {csv_path}")

    # ── Summary ──────────────────────────────────────────────────────────
    total = len(trades)
    total_pnl = sum(t["pnl"] for t in trades)

    lines = []
    lines.append("# Trade Postmortem: Apr 15-17 2026")
    lines.append("")
    lines.append(f"**Total trades: {total} | Net PnL: ${total_pnl:+.2f}**")
    lines.append("")

    # Category distribution
    lines.append("## Category Distribution")
    lines.append("")
    lines.append("| Cat | Description | Count | % | Total PnL |")
    lines.append("|-----|------------|-------|---|-----------|")

    for cat in "ABCDEFG":
        tlist = categories[cat]
        n = len(tlist)
        pct = n / total * 100 if total else 0
        cat_pnl = sum(t["pnl"] for t in tlist)
        desc_map = {
            "A": "Winners (TP hit)",
            "B": "Could-have-won (trailing saves)",
            "C": "Wrong direction (no fix)",
            "D": "Too tight SL",
            "E": "Too wide TP",
            "F": "TIME_EXIT OK",
            "G": "TIME_EXIT lost",
        }
        lines.append(f"| {cat} | {desc_map[cat]} | {n} | {pct:.0f}% | ${cat_pnl:+.2f} |")

    lines.append("")

    # Top COULD-HAVE-WON trades
    b_trades = categories["B"]
    if b_trades:
        lines.append("## Top COULD-HAVE-WON Trades (Category B)")
        lines.append("")
        lines.append("| Coin | Dir | Entry | Peak ROI | Peak Min | Actual ROI | PnL | Trail PnL | Saved |")
        lines.append("|------|-----|-------|----------|----------|------------|-----|-----------|-------|")
        b_sorted = sorted(b_trades, key=lambda x: x["peak_roi"], reverse=True)
        for t in b_sorted[:10]:
            ts = t.get("trailing_sim")
            trail_pnl = f"${ts['trailing_pnl']:+.2f}" if ts else "N/A"
            saved = f"${ts['saved_vs_actual']:+.2f}" if ts else "N/A"
            lines.append(
                f"| {t['coin']} | {t['direction']} | ${t['entry_price']} | "
                f"+{t['peak_roi']:.1f}% | {t['peak_minutes']}m | "
                f"{t['roi_pct']:+.1f}% | ${t['pnl']:+.2f} | {trail_pnl} | {saved} |"
            )
        lines.append("")

    # Trailing stop simulation
    trailing_total_saved = sum(
        t["trailing_sim"]["saved_vs_actual"]
        for t in b_trades if t.get("trailing_sim")
    )
    lines.append("## Trailing Stop Simulation Results")
    lines.append("")
    lines.append(f"- Activation: after +3% ROI")
    lines.append(f"- Trail: close at 50% of peak ROI")
    lines.append(f"- B trades with trailing sim: {sum(1 for t in b_trades if t.get('trailing_sim'))}")
    lines.append(f"- **Total saved by trailing: ${trailing_total_saved:+.2f}**")
    lines.append("")

    # Too wide TP
    e_trades = categories["E"]
    e_potential = sum(
        t["margin"] * (t["peak_roi"] * 0.5 / 100) - t["pnl"]
        for t in e_trades if t["peak_roi"] > 0
    )

    # Too tight SL
    d_trades = categories["D"]
    d_potential = sum(abs(t["pnl"]) + abs(t["pnl"]) * 0.5 for t in d_trades)  # rough estimate

    # Wrong direction
    c_pnl = sum(t["pnl"] for t in categories["C"])

    lines.append("## Money Left on Table")
    lines.append("")
    lines.append("| Source | Amount | Fix |")
    lines.append("|--------|--------|-----|")
    lines.append(f"| B: Trailing stop would save | ${trailing_total_saved:+.2f} | Add trailing stop (activate +3%, trail 50%) |")
    lines.append(f"| E: Tighter TP would capture | ${e_potential:+.2f} | Reduce TP distance or use partial TP |")
    lines.append(f"| D: Wider SL would save | ${sum(t['pnl'] for t in d_trades):+.2f} lost | Widen SL for high-conviction setups |")
    lines.append(f"| C: Unfixable (wrong direction) | ${c_pnl:+.2f} | Better signal filtering |")
    lines.append(f"| G: TIME_EXIT losses | ${sum(t['pnl'] for t in categories['G']):+.2f} | Earlier exits / tighter TIME_EXIT |")
    lines.append("")

    # Per-trade detail table
    lines.append("## All Trades Detail")
    lines.append("")
    lines.append("| # | Coin | Dir | Lev | Entry | Exit Reason | ROI | PnL | Peak ROI | Peak@min | Cat |")
    lines.append("|---|------|-----|-----|-------|-------------|-----|-----|----------|----------|-----|")
    for i, t in enumerate(trades):
        lines.append(
            f"| {i+1} | {t['coin']} | {t['direction'][:1]} | {t['leverage']}x | "
            f"${t['entry_price']} | {t['exit_reason']} | "
            f"{t['roi_pct']:+.1f}% | ${t['pnl']:+.2f} | "
            f"{t.get('peak_roi', 0):+.1f}% | {t.get('peak_minutes', '')}m | "
            f"{t['category']} |"
        )
    lines.append("")

    # Recommendations
    lines.append("## Recommendations")
    lines.append("")
    lines.append("1. **Implement trailing stop** — Biggest single improvement. Activate after +3% ROI, trail at 50% of peak.")
    lines.append("2. **Tighter TP for weak setups** — Trades reaching 50-80% of TP often reverse. Consider partial exits.")
    lines.append("3. **Filter wrong-direction trades** — C-category trades lost with no recovery. Stricter entry criteria needed.")
    lines.append("4. **TIME_EXIT improvements** — Review G-category trades for earlier exit signals.")
    lines.append("")

    summary_path = f"{OUT}/summary.md"
    with open(summary_path, "w") as f:
        f.write("\n".join(lines))

    print(f"  Summary written: {summary_path}")

    # ── Console output ───────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"POSTMORTEM SUMMARY — {total} trades, Net PnL: ${total_pnl:+.2f}")
    print("=" * 70)
    for cat in "ABCDEFG":
        tlist = categories[cat]
        n = len(tlist)
        if n == 0:
            continue
        pct = n / total * 100
        cat_pnl = sum(t["pnl"] for t in tlist)
        desc_map = {
            "A": "Winners (TP hit)",
            "B": "Could-have-won",
            "C": "Wrong direction",
            "D": "Too tight SL",
            "E": "Too wide TP",
            "F": "TIME_EXIT OK",
            "G": "TIME_EXIT lost",
        }
        print(f"  {cat}) {desc_map[cat]:30s} {n:3d} ({pct:4.0f}%)  ${cat_pnl:+8.2f}")

    print(f"\nACTIONABLE:")
    print(f"  Trailing stop saves:     ${trailing_total_saved:+.2f} from {len(b_trades)} B-trades")
    print(f"  Tighter TP captures:     ${e_potential:+.2f} from {len(e_trades)} E-trades")
    print(f"  Wider SL saves:          ${sum(abs(t['pnl']) for t in d_trades):+.2f} from {len(d_trades)} D-trades")
    print(f"  Unfixable (wrong dir):   ${c_pnl:+.2f} from {len(categories['C'])} C-trades")
    print(f"  TIME_EXIT losses:        ${sum(t['pnl'] for t in categories['G']):+.2f} from {len(categories['G'])} G-trades")


if __name__ == "__main__":
    main()
