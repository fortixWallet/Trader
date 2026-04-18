#!/usr/bin/env python3
"""
Deterministic simulation comparing 1H scan vs 4H scan systems.
Period: April 10-16, 2026.
"""
import sqlite3
import json
import csv
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────
BASE = "/Users/williamstorm/Documents/Trading (OKX) 1h"
DB_PATH = os.path.join(BASE, "data/crypto/market.db")
OPT_PATH = os.path.join(BASE, "data/crypto/coin_optimization/optimal_config.json")
OUT_DIR = os.path.join(BASE, "data/crypto/1h_vs_4h")

ACCOUNT_SIZE = 5000
POS_SIZE_PCT = 0.04  # 4% per position = $200

BAD_COINS = {"BOME", "DOT", "AAVE", "WIF", "DOGE", "OP", "RENDER", "TAO", "ARB"}

COINS_WANTED = [
    "BTC","ETH","SOL","BNB","XRP","ADA","AVAX","LINK","UNI","LDO",
    "CRV","POL","PENDLE","SUI","LTC","BCH","NEAR","HBAR","TON","APT",
    "FIL","ALGO","XLM"
]

START_DT = datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc)
END_DT   = datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc)
START_TS = int(START_DT.timestamp())
END_TS   = int(END_DT.timestamp())

# ── Load data ───────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

with open(OPT_PATH) as f:
    OPT_CFG = json.load(f)

# Pre-load all needed price data (with lookback)
LOOKBACK_TS = START_TS - 200 * 3600  # 200 hours before start
BTC_7D_LOOKBACK = START_TS - 8 * 86400

def load_candles(timeframe):
    """Load candles into dict: {coin: [(ts, o, h, l, c, v), ...]} sorted by ts."""
    data = {}
    min_ts = BTC_7D_LOOKBACK if timeframe == '1h' else LOOKBACK_TS
    cur.execute(
        "SELECT coin, timestamp, open, high, low, close, volume "
        "FROM prices WHERE timeframe=? AND timestamp >= ? AND timestamp < ? "
        "ORDER BY coin, timestamp",
        (timeframe, min_ts, END_TS)
    )
    for row in cur.fetchall():
        coin = row[0]
        if coin not in data:
            data[coin] = []
        data[coin].append(row[1:])
    return data

print("Loading candles...")
candles_1h = load_candles('1h')
candles_4h = load_candles('4h')

# Filter coins to those with actual 1h data in the sim period
COINS = [c for c in COINS_WANTED if c in candles_1h and c not in BAD_COINS]
print(f"Active coins: {len(COINS)}: {COINS}")

# Build timestamp-indexed lookups for fast access
def build_ts_index(candle_dict):
    idx = {}
    for coin, bars in candle_dict.items():
        idx[coin] = {bar[0]: bar for bar in bars}
    return idx

idx_1h = build_ts_index(candles_1h)
idx_4h = build_ts_index(candles_4h)

# ── Helper functions ────────────────────────────────────────────────────────

def get_candles_before(coin, ts, n, timeframe='1h'):
    """Get last n candles before (and including) ts."""
    source = candles_1h if timeframe == '1h' else candles_4h
    if coin not in source:
        return []
    bars = source[coin]
    # Binary search for ts
    lo, hi = 0, len(bars) - 1
    pos = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        if bars[mid][0] <= ts:
            pos = mid
            lo = mid + 1
        else:
            hi = mid - 1
    end = pos + 1
    start = max(0, end - n)
    return bars[start:end]


def compute_atr(candles, period=14):
    """ATR from candle list [(ts,o,h,l,c,v), ...]."""
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i-1][4]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[-period:]) / period
    return atr


def compute_ema(candles, period):
    """EMA of close prices."""
    if len(candles) < period:
        return None
    closes = [c[4] for c in candles]
    mult = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for c in closes[period:]:
        ema = c * mult + ema * (1 - mult)
    return ema


def find_sr_levels(candles, n_levels=5):
    """Find support/resistance from recent swing highs/lows."""
    if len(candles) < 5:
        return []
    levels = []
    for i in range(2, len(candles) - 2):
        h = candles[i][2]
        l = candles[i][3]
        # Swing high
        if h >= candles[i-1][2] and h >= candles[i-2][2] and h >= candles[i+1][2] and h >= candles[i+2][2]:
            levels.append(h)
        # Swing low
        if l <= candles[i-1][3] and l <= candles[i-2][3] and l <= candles[i+1][3] and l <= candles[i+2][3]:
            levels.append(l)
    # Also add recent highs/lows
    if candles:
        levels.append(max(c[2] for c in candles[-20:]))
        levels.append(min(c[3] for c in candles[-20:]))
    # Cluster nearby levels (within 0.3%)
    if not levels:
        return []
    levels.sort()
    clustered = [levels[0]]
    for lv in levels[1:]:
        if abs(lv - clustered[-1]) / clustered[-1] > 0.003:
            clustered.append(lv)
        else:
            clustered[-1] = (clustered[-1] + lv) / 2
    return clustered


def get_regime(ts):
    """
    Regime from 4h breadth + BTC 12h trend.
    Breadth: count coins where 4h close > EMA20_4h. If >60% => BULL, <40% => BEAR, else NEUTRAL.
    BTC 12h: EMA12 of BTC on 4h (=~12h equivalent). If close > EMA => BULL bias.
    Combined: both agree => that direction, else NEUTRAL.
    """
    # 4h breadth
    bull_count = 0
    total = 0
    for coin in COINS:
        bars = get_candles_before(coin, ts, 30, '4h')
        if len(bars) < 20:
            continue
        ema20 = compute_ema(bars, 20)
        if ema20 is None:
            continue
        total += 1
        if bars[-1][4] > ema20:
            bull_count += 1

    if total == 0:
        return "NEUTRAL"

    breadth_pct = bull_count / total

    # BTC 12h trend (use 4h bars, EMA3 = ~12h)
    btc_bars = get_candles_before("BTC", ts, 30, '4h')
    if len(btc_bars) < 5:
        return "NEUTRAL"
    btc_ema = compute_ema(btc_bars, 3)
    if btc_ema is None:
        return "NEUTRAL"
    btc_close = btc_bars[-1][4]

    btc_bull = btc_close > btc_ema

    if breadth_pct > 0.6 and btc_bull:
        return "BULL"
    elif breadth_pct < 0.4 and not btc_bull:
        return "BEAR"
    else:
        return "NEUTRAL"


def btc_7d_change(ts):
    """BTC price change over last 7 days."""
    bars_now = get_candles_before("BTC", ts, 1, '1h')
    bars_7d = get_candles_before("BTC", ts - 7 * 86400, 1, '1h')
    if not bars_now or not bars_7d:
        return 0
    return (bars_now[-1][4] - bars_7d[-1][4]) / bars_7d[-1][4]


def get_ema_alignment(coin, ts):
    """Check if price is aligned with EMA on 1h and 4h."""
    bars_1h = get_candles_before(coin, ts, 30, '1h')
    bars_4h = get_candles_before(coin, ts, 30, '4h')
    if len(bars_1h) < 20 or len(bars_4h) < 20:
        return 0
    ema_1h = compute_ema(bars_1h, 20)
    ema_4h = compute_ema(bars_4h, 20)
    if ema_1h is None or ema_4h is None:
        return 0
    close = bars_1h[-1][4]
    score = 0
    if close > ema_1h:
        score += 1
    if close > ema_4h:
        score += 1
    return score  # 0=bearish aligned, 1=mixed, 2=bullish aligned


def simulate_fill(coin, entry_price, direction, sl_pct, tp_pct, hold_bars, entry_ts):
    """
    Simulate order fill and outcome using 1h candles.
    Returns: (filled, exit_price, exit_ts, exit_type, bars_held)
    """
    source = candles_1h.get(coin, [])
    if not source:
        return (False, 0, 0, "NO_DATA", 0)

    # Find candles starting from entry_ts
    start_idx = None
    for i, bar in enumerate(source):
        if bar[0] >= entry_ts:
            start_idx = i
            break
    if start_idx is None:
        return (False, 0, 0, "NO_DATA", 0)

    if direction == "LONG":
        sl_price = entry_price * (1 - sl_pct)
        tp_price = entry_price * (1 + tp_pct)
    else:
        sl_price = entry_price * (1 + sl_pct)
        tp_price = entry_price * (1 - tp_pct)

    bars_checked = 0
    for i in range(start_idx, min(start_idx + hold_bars, len(source))):
        bar = source[i]
        bars_checked += 1
        h, l, c = bar[2], bar[3], bar[4]

        if direction == "LONG":
            # SL first if both hit
            if l <= sl_price:
                return (True, sl_price, bar[0], "SL", bars_checked)
            if h >= tp_price:
                return (True, tp_price, bar[0], "TP", bars_checked)
        else:
            if h >= sl_price:
                return (True, sl_price, bar[0], "SL", bars_checked)
            if l <= tp_price:
                return (True, tp_price, bar[0], "TP", bars_checked)

    # Time exit
    if bars_checked > 0:
        last_bar = source[min(start_idx + hold_bars - 1, len(source) - 1)]
        return (True, last_bar[4], last_bar[0], "TIME", bars_checked)

    return (False, 0, 0, "NO_FILL", 0)


# ── System A: 1H Scan ──────────────────────────────────────────────────────

def run_system_a():
    """1H scan system."""
    trades = []
    active_positions = []  # (coin, entry_ts, exit_ts_max)

    ts = START_TS
    while ts < END_TS:
        # Clean expired positions
        active_positions = [(c, et, mx) for c, et, mx in active_positions if ts < mx]

        if len(active_positions) >= 8:
            ts += 3600
            continue

        regime = get_regime(ts)

        # Score all coins
        candidates = []
        for coin in COINS:
            # Skip if already in position
            if any(c == coin for c, _, _ in active_positions):
                continue

            bars = get_candles_before(coin, ts, 100, '1h')
            if len(bars) < 20:
                continue

            current_price = bars[-1][4]
            atr = compute_atr(bars, 14)
            if atr is None or atr <= 0:
                continue

            atr_pct = atr / current_price

            sr_levels = find_sr_levels(bars)
            if not sr_levels:
                continue

            # Find nearest S/R
            nearest = min(sr_levels, key=lambda lv: abs(lv - current_price))
            proximity = abs(nearest - current_price) / current_price

            # Direction
            if regime == "BULL":
                direction = "LONG"
            elif regime == "BEAR":
                direction = "SHORT"
            else:
                # Neutral: use nearest S/R direction
                if nearest < current_price:
                    direction = "LONG"  # at support
                else:
                    direction = "SHORT"  # at resistance

            ema_align = get_ema_alignment(coin, ts)
            # Score: prefer close proximity, high ATR, EMA alignment
            score = (1 - proximity * 10) + atr_pct * 5

            if direction == "LONG":
                score += ema_align * 0.5
            else:
                score += (2 - ema_align) * 0.5

            candidates.append((coin, nearest, direction, atr, atr_pct, proximity, score))

        # Sort by score descending, take top slots
        candidates.sort(key=lambda x: x[6], reverse=True)
        slots = 8 - len(active_positions)

        for coin, entry_price, direction, atr, atr_pct, proximity, score in candidates[:slots]:
            # System A: SL = 0.8 * ATR_1h, TP = SL * 2.0
            sl_abs = 0.8 * atr
            sl_pct = sl_abs / entry_price
            tp_pct = sl_pct * 2.0

            hold_bars = 3  # max 3h

            filled, exit_price, exit_ts, exit_type, bars_held = simulate_fill(
                coin, entry_price, direction, sl_pct, tp_pct, hold_bars, ts
            )

            if filled:
                if direction == "LONG":
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:
                    pnl_pct = (entry_price - exit_price) / entry_price

                pos_size = ACCOUNT_SIZE * POS_SIZE_PCT
                pnl_dollar = pnl_pct * pos_size

                trade = {
                    "system": "1H",
                    "coin": coin,
                    "direction": direction,
                    "entry_ts": ts,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_ts": exit_ts,
                    "exit_type": exit_type,
                    "sl_pct": sl_pct,
                    "tp_pct": tp_pct,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": pnl_dollar,
                    "bars_held": bars_held,
                    "regime": regime,
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                }
                trades.append(trade)
                max_exit = ts + hold_bars * 3600
                active_positions.append((coin, ts, max_exit))

        ts += 3600  # next 1h scan

    return trades


# ── System B: 4H Scan ──────────────────────────────────────────────────────

def run_system_b():
    """4H scan system."""
    trades = []
    active_positions = []  # (coin, entry_ts, exit_ts_max)

    # Scan times: 00, 04, 08, 12, 16, 20 UTC each day
    scan_times = []
    day = START_DT
    while day < END_DT:
        for hour in [0, 4, 8, 12, 16, 20]:
            st = day.replace(hour=hour, minute=0, second=0)
            scan_times.append(int(st.timestamp()))
        day += timedelta(days=1)

    for ts in scan_times:
        if ts >= END_TS:
            break

        # Clean expired positions
        active_positions = [(c, et, mx) for c, et, mx in active_positions if ts < mx]

        if len(active_positions) >= 8:
            continue

        regime = get_regime(ts)

        # Macro filter: BTC 7d >= +3% => block BEAR
        btc_7d = btc_7d_change(ts)
        if btc_7d >= 0.03 and regime == "BEAR":
            regime = "NEUTRAL"  # block bear signals

        # Get 4h close price
        candidates = []
        for coin in COINS:
            if any(c == coin for c, _, _ in active_positions):
                continue

            cfg = OPT_CFG.get(coin)
            if cfg is None:
                continue

            # 4h bar at this scan time
            bars_4h = get_candles_before(coin, ts, 5, '4h')
            if not bars_4h:
                continue
            close_4h = bars_4h[-1][4]

            # 1h S/R levels
            bars_1h = get_candles_before(coin, ts, 100, '1h')
            if len(bars_1h) < 20:
                continue

            atr = compute_atr(bars_1h, 14)
            if atr is None or atr <= 0:
                continue

            current_price = bars_1h[-1][4]
            atr_pct = atr / current_price

            # ATR filter: need > 0.5%
            if atr_pct < 0.005:
                continue

            sr_levels = find_sr_levels(bars_1h)
            if not sr_levels:
                continue

            # Find nearest S/R within 0.5% of 4h close
            nearby_sr = [lv for lv in sr_levels if abs(lv - close_4h) / close_4h <= 0.005]
            if not nearby_sr:
                continue

            nearest = min(nearby_sr, key=lambda lv: abs(lv - close_4h))
            proximity = abs(nearest - close_4h) / close_4h

            # Direction from regime
            if regime == "BULL":
                direction = "LONG"
            elif regime == "BEAR":
                direction = "SHORT"
            else:
                if nearest < close_4h:
                    direction = "LONG"
                else:
                    direction = "SHORT"

            # EMA4h alignment
            ema_4h = compute_ema(bars_4h, 20) if len(bars_4h) >= 20 else None
            ema_align = 0
            if ema_4h:
                if direction == "LONG" and close_4h > ema_4h:
                    ema_align = 1
                elif direction == "SHORT" and close_4h < ema_4h:
                    ema_align = 1

            # Regime alignment score
            regime_align = 1 if (regime == "BULL" and direction == "LONG") or (regime == "BEAR" and direction == "SHORT") else 0

            score = atr_pct * 10 + regime_align * 2 + ema_align * 1.5 + (1 - proximity * 100)

            candidates.append((coin, nearest, direction, atr, atr_pct, proximity, score, cfg))

        # Sort by score, take top 5
        candidates.sort(key=lambda x: x[6], reverse=True)
        slots = min(5, 8 - len(active_positions))

        for coin, entry_price, direction, atr, atr_pct, proximity, score, cfg in candidates[:slots]:
            # Per-coin optimal SL and R:R
            sl_mult = cfg["sl_mult"]
            rr = cfg["rr"]
            sl_abs = sl_mult * atr
            sl_pct = sl_abs / entry_price
            tp_pct = sl_pct * rr

            hold_bars = 8  # max 8h (2 x 4h bars)

            filled, exit_price, exit_ts, exit_type, bars_held = simulate_fill(
                coin, entry_price, direction, sl_pct, tp_pct, hold_bars, ts
            )

            if filled:
                if direction == "LONG":
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:
                    pnl_pct = (entry_price - exit_price) / entry_price

                pos_size = ACCOUNT_SIZE * POS_SIZE_PCT
                pnl_dollar = pnl_pct * pos_size

                trade = {
                    "system": "4H",
                    "coin": coin,
                    "direction": direction,
                    "entry_ts": ts,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_ts": exit_ts,
                    "exit_type": exit_type,
                    "sl_pct": sl_pct,
                    "tp_pct": tp_pct,
                    "pnl_pct": pnl_pct,
                    "pnl_dollar": pnl_dollar,
                    "bars_held": bars_held,
                    "regime": regime,
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                }
                trades.append(trade)
                max_exit = ts + hold_bars * 3600
                active_positions.append((coin, ts, max_exit))

    return trades


# ── Run simulations ─────────────────────────────────────────────────────────
print("\n=== Running System A (1H scan) ===")
trades_a = run_system_a()
print(f"  Trades: {len(trades_a)}")

print("\n=== Running System B (4H scan) ===")
trades_b = run_system_b()
print(f"  Trades: {len(trades_b)}")


# ── Analysis ────────────────────────────────────────────────────────────────

def analyze_trades(trades, label):
    """Compute metrics from trade list."""
    if not trades:
        return {"label": label, "trades": 0}

    wins = [t for t in trades if t["pnl_dollar"] > 0]
    losses = [t for t in trades if t["pnl_dollar"] <= 0]
    total_pnl = sum(t["pnl_dollar"] for t in trades)
    wr = len(wins) / len(trades) * 100 if trades else 0
    avg_win = sum(t["pnl_dollar"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_dollar"] for t in losses) / len(losses) if losses else 0
    gross_win = sum(t["pnl_dollar"] for t in wins)
    gross_loss = abs(sum(t["pnl_dollar"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    avg_hold = sum(t["bars_held"] for t in trades) / len(trades)

    sl_count = sum(1 for t in trades if t["exit_type"] == "SL")
    tp_count = sum(1 for t in trades if t["exit_type"] == "TP")
    time_count = sum(1 for t in trades if t["exit_type"] == "TIME")

    # Max concurrent positions (approximate)
    max_conc = 0
    events = []
    for t in trades:
        events.append((t["entry_ts"], 1))
        events.append((t["exit_ts"], -1))
    events.sort()
    conc = 0
    for _, delta in events:
        conc += delta
        max_conc = max(max_conc, conc)

    # Running drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for t in sorted(trades, key=lambda x: x["entry_ts"]):
        equity += t["pnl_dollar"]
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    return {
        "label": label,
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "wr": wr,
        "total_pnl": total_pnl,
        "roi_pct": total_pnl / ACCOUNT_SIZE * 100,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "pf": pf,
        "avg_hold": avg_hold,
        "sl_rate": sl_count / len(trades) * 100,
        "tp_rate": tp_count / len(trades) * 100,
        "time_rate": time_count / len(trades) * 100,
        "max_concurrent": max_conc,
        "max_dd": max_dd,
    }


def analyze_by_day(trades):
    """Group metrics by date."""
    by_day = defaultdict(list)
    for t in trades:
        by_day[t["date"]].append(t)
    results = {}
    for date in sorted(by_day):
        results[date] = analyze_trades(by_day[date], date)
    return results


def analyze_by_coin(trades):
    """Group metrics by coin."""
    by_coin = defaultdict(list)
    for t in trades:
        by_coin[t["coin"]].append(t)
    results = {}
    for coin in sorted(by_coin):
        results[coin] = analyze_trades(by_coin[coin], coin)
    return results


# ── Generate outputs ────────────────────────────────────────────────────────

# 1. comparison.csv
all_trades = trades_a + trades_b
csv_path = os.path.join(OUT_DIR, "comparison.csv")
fields = ["system","coin","direction","entry_ts","entry_price","exit_price",
          "exit_ts","exit_type","sl_pct","tp_pct","pnl_pct","pnl_dollar",
          "bars_held","regime","date"]
with open(csv_path, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for t in sorted(all_trades, key=lambda x: x["entry_ts"]):
        w.writerow(t)
print(f"\nSaved {csv_path}")

# 2. Summary
totals_a = analyze_trades(trades_a, "1H System")
totals_b = analyze_trades(trades_b, "4H System")
daily_a = analyze_by_day(trades_a)
daily_b = analyze_by_day(trades_b)
coin_a = analyze_by_coin(trades_a)
coin_b = analyze_by_coin(trades_b)

dates = sorted(set(list(daily_a.keys()) + list(daily_b.keys())))

lines = []
lines.append("# 1H vs 4H Scan System Comparison")
lines.append(f"Period: April 10-16, 2026 | Account: ${ACCOUNT_SIZE}")
lines.append("")

# Daily comparison
lines.append("## Daily Comparison")
lines.append("")
lines.append("| Date | 1H Trades | 1H WR% | 1H PnL | 4H Trades | 4H WR% | 4H PnL |")
lines.append("|------|-----------|--------|--------|-----------|--------|--------|")
for d in dates:
    a = daily_a.get(d, {"trades": 0, "wr": 0, "total_pnl": 0})
    b = daily_b.get(d, {"trades": 0, "wr": 0, "total_pnl": 0})
    lines.append(f"| {d} | {a['trades']} | {a['wr']:.1f} | ${a['total_pnl']:.2f} | {b['trades']} | {b['wr']:.1f} | ${b['total_pnl']:.2f} |")
lines.append("")

# Total comparison
lines.append("## Total Comparison")
lines.append("")
lines.append("| Metric | 1H System | 4H System |")
lines.append("|--------|-----------|-----------|")
for metric, fmt, label in [
    ("trades", "d", "Trades"),
    ("wr", ".1f", "Win Rate %"),
    ("total_pnl", ".2f", "Total PnL $"),
    ("roi_pct", ".2f", "ROI %"),
    ("avg_win", ".2f", "Avg Win $"),
    ("avg_loss", ".2f", "Avg Loss $"),
    ("pf", ".2f", "Profit Factor"),
    ("avg_hold", ".1f", "Avg Hold (bars)"),
    ("sl_rate", ".1f", "SL Rate %"),
    ("tp_rate", ".1f", "TP Rate %"),
    ("time_rate", ".1f", "TIME Exit %"),
    ("max_concurrent", "d", "Max Concurrent"),
    ("max_dd", ".2f", "Max Drawdown $"),
]:
    va = totals_a.get(metric, 0)
    vb = totals_b.get(metric, 0)
    if fmt == "d":
        lines.append(f"| {label} | {va} | {vb} |")
    elif "$" in label or "Drawdown" in label:
        lines.append(f"| {label} | ${va:{fmt}} | ${vb:{fmt}} |")
    else:
        lines.append(f"| {label} | {va:{fmt}} | {vb:{fmt}} |")
lines.append("")

# Per-coin comparison
lines.append("## Per-Coin Comparison")
lines.append("")
lines.append("| Coin | 1H Trades | 1H WR% | 1H PnL | 4H Trades | 4H WR% | 4H PnL | Better |")
lines.append("|------|-----------|--------|--------|-----------|--------|--------|--------|")
all_coins = sorted(set(list(coin_a.keys()) + list(coin_b.keys())))
for coin in all_coins:
    a = coin_a.get(coin, {"trades": 0, "wr": 0, "total_pnl": 0})
    b = coin_b.get(coin, {"trades": 0, "wr": 0, "total_pnl": 0})
    better = "4H" if b.get("total_pnl", 0) > a.get("total_pnl", 0) else "1H"
    if a.get("trades", 0) == 0 and b.get("trades", 0) == 0:
        better = "-"
    lines.append(f"| {coin} | {a.get('trades',0)} | {a.get('wr',0):.1f} | ${a.get('total_pnl',0):.2f} | {b.get('trades',0)} | {b.get('wr',0):.1f} | ${b.get('total_pnl',0):.2f} | {better} |")
lines.append("")

# Recommendation
lines.append("## Recommendation")
lines.append("")
if totals_b["total_pnl"] > totals_a["total_pnl"] and totals_b["pf"] > totals_a["pf"]:
    lines.append("**4H system outperforms** on both PnL and Profit Factor.")
    lines.append(f"4H generates {totals_b['trades']} trades vs {totals_a['trades']} (1H), "
                 f"with better capital efficiency ({totals_b['roi_pct']:.2f}% vs {totals_a['roi_pct']:.2f}% ROI).")
elif totals_b["total_pnl"] > totals_a["total_pnl"]:
    lines.append("**4H system has higher PnL** but lower Profit Factor.")
    lines.append("Consider 4H for higher returns with potentially more risk per trade.")
elif totals_a["pf"] > totals_b["pf"] and totals_a["total_pnl"] > totals_b["total_pnl"]:
    lines.append("**1H system outperforms** on both PnL and Profit Factor.")
    lines.append(f"1H generates more trading opportunities ({totals_a['trades']} vs {totals_b['trades']}) "
                 f"with {totals_a['roi_pct']:.2f}% vs {totals_b['roi_pct']:.2f}% ROI.")
else:
    lines.append("**Mixed results** - each system has strengths.")
    lines.append(f"1H: {totals_a['trades']} trades, ${totals_a['total_pnl']:.2f} PnL, PF={totals_a['pf']:.2f}")
    lines.append(f"4H: {totals_b['trades']} trades, ${totals_b['total_pnl']:.2f} PnL, PF={totals_b['pf']:.2f}")

summary_path = os.path.join(OUT_DIR, "summary.md")
with open(summary_path, 'w') as f:
    f.write('\n'.join(lines))
print(f"Saved {summary_path}")

# Print summary to stdout
print("\n" + "="*70)
for line in lines:
    print(line)
print("="*70)
