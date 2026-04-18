#!/usr/bin/env python3
"""
Coin Optimizer - Grid search across direction strategies, SL multipliers,
R:R ratios, and hold times for 27 coins using 4h candle data.
"""

import sqlite3
import numpy as np
import json
import csv
import time
import os
from itertools import product
from collections import defaultdict

DB_PATH = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/market.db"
OUT_DIR = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/coin_optimization"

COINS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "UNI", "LDO",
    "CRV", "POL", "PENDLE", "JUP", "PYTH", "JTO", "SUI", "LTC", "BCH", "TRX",
    "NEAR", "HBAR", "TON", "APT", "FIL", "ALGO", "XLM"
]

# Grid parameters
DIRECTION_STRATEGIES = ["BTC_FOLLOW", "OWN_EMA", "FUNDING_CONTRARIAN", "BTC_PLUS_OWN", "COMBINED"]
SL_MULTS = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2]
RR_RATIOS = [1.0, 1.2, 1.5, 1.8, 2.0, 2.5]
HOLD_BARS = [1, 2, 3, 4]  # in 4h bars = 4h, 8h, 12h, 16h

WARMUP = 50
ATR_PERIOD = 14
EMA_FAST = 8
EMA_SLOW = 21
COOLDOWN_BARS = 2
FEE_ROUNDTRIP = 0.0004  # 0.04%
FUNDING_PER_8H = 0.0001  # 0.01% per 8h

MIN_TRADES = 50


def load_data():
    """Load all price and funding data."""
    conn = sqlite3.connect(DB_PATH)

    # Load 4h prices for all coins
    print("Loading 4h price data...")
    cur = conn.cursor()
    cur.execute("""
        SELECT coin, timestamp, open, high, low, close, volume
        FROM prices WHERE timeframe='4h'
        ORDER BY coin, timestamp
    """)
    rows = cur.fetchall()

    prices = defaultdict(lambda: {"ts": [], "o": [], "h": [], "l": [], "c": [], "v": []})
    for coin, ts, o, h, l, c, v in rows:
        if coin in COINS or coin == "BTC":
            prices[coin]["ts"].append(ts)
            prices[coin]["o"].append(o)
            prices[coin]["h"].append(h)
            prices[coin]["l"].append(l)
            prices[coin]["c"].append(c)
            prices[coin]["v"].append(v)

    # Convert to numpy
    price_data = {}
    for coin in prices:
        d = prices[coin]
        price_data[coin] = {
            "ts": np.array(d["ts"], dtype=np.int64),
            "o": np.array(d["o"], dtype=np.float64),
            "h": np.array(d["h"], dtype=np.float64),
            "l": np.array(d["l"], dtype=np.float64),
            "c": np.array(d["c"], dtype=np.float64),
            "v": np.array(d["v"], dtype=np.float64),
        }

    # Load funding rates
    print("Loading funding data...")
    cur.execute("SELECT coin, timestamp, rate FROM funding_rates ORDER BY coin, timestamp")
    funding_rows = cur.fetchall()
    funding = defaultdict(lambda: {"ts": [], "rate": []})
    for coin, ts, rate in funding_rows:
        if coin in COINS:
            funding[coin]["ts"].append(ts)
            funding[coin]["rate"].append(rate)

    funding_data = {}
    for coin in funding:
        d = funding[coin]
        if len(d["ts"]) > 0:
            funding_data[coin] = {
                "ts": np.array(d["ts"], dtype=np.int64),
                "rate": np.array(d["rate"], dtype=np.float64),
            }

    conn.close()
    print(f"Loaded {len(price_data)} coins, {len(funding_data)} with funding data")
    return price_data, funding_data


def compute_ema(arr, period):
    """Compute EMA using numpy."""
    alpha = 2.0 / (period + 1)
    ema = np.empty_like(arr)
    ema[0] = arr[0]
    for i in range(1, len(arr)):
        ema[i] = alpha * arr[i] + (1 - alpha) * ema[i - 1]
    return ema


def compute_atr_pct(highs, lows, closes, period=14):
    """Compute ATR as percentage of close, rolling."""
    tr = (highs - lows) / closes
    atr = np.empty_like(tr)
    atr[:period] = np.nan
    atr[period] = np.mean(tr[:period])
    alpha = 1.0 / period
    for i in range(period + 1, len(tr)):
        atr[i] = atr[i-1] * (1 - alpha) + tr[i] * alpha
    return atr


def get_btc_12h_change(btc_data):
    """BTC 12h change = 3 bars of 4h."""
    c = btc_data["c"]
    change = np.full(len(c), np.nan)
    for i in range(3, len(c)):
        change[i] = (c[i] - c[i-3]) / c[i-3]
    return change


def get_funding_at_bar(funding_data, bar_ts):
    """Get most recent funding rate at or before bar timestamp."""
    if funding_data is None:
        return np.nan
    idx = np.searchsorted(funding_data["ts"], bar_ts, side="right") - 1
    if idx < 0:
        return np.nan
    # Only use if within 24h
    if bar_ts - funding_data["ts"][idx] > 86400:
        return np.nan
    return funding_data["rate"][idx]


def precompute_signals(coin, price_data, funding_data, btc_12h_change, btc_ts):
    """Precompute all direction signals for a coin."""
    d = price_data[coin]
    n = len(d["c"])

    # EMA signals
    ema_fast = compute_ema(d["c"], EMA_FAST)
    ema_slow = compute_ema(d["c"], EMA_SLOW)
    ema_bull = ema_fast > ema_slow  # True = LONG signal

    # ATR
    atr = compute_atr_pct(d["h"], d["l"], d["c"], ATR_PERIOD)

    # BTC 12h change aligned to this coin's timestamps
    btc_change_aligned = np.full(n, np.nan)
    if coin == "BTC":
        btc_change_aligned = get_btc_12h_change(d)
    else:
        # Align BTC data to coin timestamps
        for i in range(n):
            idx = np.searchsorted(btc_ts, d["ts"][i], side="right") - 1
            if 0 <= idx < len(btc_12h_change):
                btc_change_aligned[i] = btc_12h_change[idx]

    # Funding aligned
    fd = funding_data.get(coin)
    funding_aligned = np.full(n, np.nan)
    if fd is not None and len(fd["ts"]) > 100:
        for i in range(n):
            funding_aligned[i] = get_funding_at_bar(fd, d["ts"][i])

    # Direction signals per strategy: +1=LONG, -1=SHORT, 0=SKIP
    signals = {}

    # BTC_FOLLOW
    s = np.zeros(n, dtype=np.int8)
    s[btc_change_aligned > 0.005] = 1
    s[btc_change_aligned < -0.005] = -1
    signals["BTC_FOLLOW"] = s

    # OWN_EMA
    s = np.zeros(n, dtype=np.int8)
    s[ema_bull] = 1
    s[~ema_bull] = -1
    signals["OWN_EMA"] = s

    # FUNDING_CONTRARIAN
    has_funding = not np.all(np.isnan(funding_aligned))
    s = np.zeros(n, dtype=np.int8)
    if has_funding:
        s[funding_aligned < -0.0003] = 1   # funding negative → LONG
        s[funding_aligned > 0.0003] = -1   # funding positive → SHORT
    signals["FUNDING_CONTRARIAN"] = s

    # BTC_PLUS_OWN: both must agree
    btc_sig = signals["BTC_FOLLOW"]
    ema_sig = signals["OWN_EMA"]
    s = np.zeros(n, dtype=np.int8)
    agree = (btc_sig == ema_sig) & (btc_sig != 0)
    s[agree] = btc_sig[agree]
    signals["BTC_PLUS_OWN"] = s

    # COMBINED: BTC + EMA agree + funding doesn't contradict
    fund_sig = signals["FUNDING_CONTRARIAN"]
    s = np.copy(signals["BTC_PLUS_OWN"])
    # If funding contradicts, skip
    contradicts = (s == 1) & (fund_sig == -1) | (s == -1) & (fund_sig == 1)
    s[contradicts] = 0
    signals["COMBINED"] = s

    return signals, atr, d


def simulate_combination(direction_signal, atr, highs, lows, closes, sl_mult, rr, hold_bars):
    """
    Vectorized-ish simulation for one combination.
    Returns array of trade PnLs.
    """
    n = len(closes)
    trades = []
    i = WARMUP
    cooldown_until = -1

    while i < n:
        sig = direction_signal[i]
        if sig == 0 or np.isnan(atr[i]) or i <= cooldown_until:
            i += 1
            continue

        entry = closes[i]
        cur_atr = atr[i]
        sl_dist = sl_mult * cur_atr
        tp_dist = sl_dist * rr

        if sig == 1:  # LONG
            sl_price = entry * (1 - sl_dist)
            tp_price = entry * (1 + tp_dist)
        else:  # SHORT
            sl_price = entry * (1 + sl_dist)
            tp_price = entry * (1 - tp_dist)

        # Check subsequent bars
        exit_price = None
        exit_type = None
        bars_held = 0

        for j in range(1, hold_bars + 1):
            k = i + j
            if k >= n:
                # Time exit at last available
                exit_price = closes[min(i + j - 1, n - 1)] if j > 1 else entry
                exit_type = "TIME_EXIT"
                bars_held = j - 1 if j > 1 else 0
                break

            bars_held = j
            bar_h = highs[k]
            bar_l = lows[k]

            if sig == 1:  # LONG
                # Check SL first (conservative)
                if bar_l <= sl_price:
                    exit_price = sl_price
                    exit_type = "STOP_LOSS"
                    break
                if bar_h >= tp_price:
                    exit_price = tp_price
                    exit_type = "TARGET_HIT"
                    break
            else:  # SHORT
                if bar_h >= sl_price:
                    exit_price = sl_price
                    exit_type = "STOP_LOSS"
                    break
                if bar_l <= tp_price:
                    exit_price = tp_price
                    exit_type = "TARGET_HIT"
                    break

        if exit_price is None:
            # Time exit
            last_bar = min(i + hold_bars, n - 1)
            exit_price = closes[last_bar]
            exit_type = "TIME_EXIT"
            bars_held = hold_bars

        # Calculate PnL
        if sig == 1:
            pnl = (exit_price - entry) / entry
        else:
            pnl = (entry - exit_price) / entry

        # Subtract fees
        pnl -= FEE_ROUNDTRIP

        # Subtract funding cost (approximate)
        hours_held = bars_held * 4
        funding_periods = hours_held / 8.0
        pnl -= FUNDING_PER_8H * funding_periods

        trades.append(pnl * 100)  # as percentage

        # Cooldown after SL
        if exit_type == "STOP_LOSS":
            cooldown_until = i + bars_held + COOLDOWN_BARS

        # Advance past this trade
        i = i + bars_held + 1

    return np.array(trades)


def compute_metrics(trades):
    """Compute trading metrics from array of trade PnLs (in %)."""
    if len(trades) < 5:
        return None

    total = len(trades)
    wins = np.sum(trades > 0)
    wr = wins / total * 100

    total_pnl = np.sum(trades)
    avg_pnl = np.mean(trades)

    gross_profit = np.sum(trades[trades > 0]) if wins > 0 else 0
    gross_loss = abs(np.sum(trades[trades < 0])) if np.any(trades < 0) else 0.001
    pf = gross_profit / gross_loss

    # Max consecutive losses
    max_consec = 0
    cur_consec = 0
    for t in trades:
        if t <= 0:
            cur_consec += 1
            max_consec = max(max_consec, cur_consec)
        else:
            cur_consec = 0

    # Sharpe-like
    std = np.std(trades)
    sharpe = avg_pnl / std if std > 0 else 0

    return {
        "trades": total,
        "wins": int(wins),
        "wr": round(wr, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(avg_pnl, 4),
        "pf": round(pf, 3),
        "max_consec_loss": max_consec,
        "sharpe": round(sharpe, 4),
    }


def optimize_coin(coin, price_data, funding_data, btc_12h_change, btc_ts):
    """Run full grid search for one coin."""
    if coin not in price_data:
        print(f"  {coin}: NO DATA - skipping")
        return [], None

    signals, atr, d = precompute_signals(coin, price_data, funding_data, btc_12h_change, btc_ts)
    n = len(d["c"])
    print(f"  {coin}: {n} candles, running 840 combos...")

    results = []
    best = None
    best_sharpe = -999

    for strat in DIRECTION_STRATEGIES:
        sig = signals[strat]
        # Skip strategies with zero signals
        active = np.sum(sig != 0)
        if active < 30:
            # Not enough signals for this strategy
            for sl_m in SL_MULTS:
                for rr_val in RR_RATIOS:
                    for hb in HOLD_BARS:
                        results.append({
                            "coin": coin,
                            "strategy": strat,
                            "sl_mult": sl_m,
                            "rr": rr_val,
                            "hold_bars": hb,
                            "trades": 0,
                            "wins": 0,
                            "wr": 0,
                            "total_pnl": 0,
                            "avg_pnl": 0,
                            "pf": 0,
                            "max_consec_loss": 0,
                            "sharpe": 0,
                        })
            continue

        for sl_m in SL_MULTS:
            for rr_val in RR_RATIOS:
                for hb in HOLD_BARS:
                    trades = simulate_combination(sig, atr, d["h"], d["l"], d["c"], sl_m, rr_val, hb)
                    metrics = compute_metrics(trades)

                    if metrics is None:
                        row = {
                            "coin": coin, "strategy": strat, "sl_mult": sl_m,
                            "rr": rr_val, "hold_bars": hb,
                            "trades": len(trades), "wins": 0, "wr": 0,
                            "total_pnl": 0, "avg_pnl": 0, "pf": 0,
                            "max_consec_loss": 0, "sharpe": 0,
                        }
                    else:
                        row = {
                            "coin": coin, "strategy": strat, "sl_mult": sl_m,
                            "rr": rr_val, "hold_bars": hb, **metrics,
                        }
                        if metrics["trades"] >= MIN_TRADES and metrics["sharpe"] > best_sharpe:
                            best_sharpe = metrics["sharpe"]
                            best = row.copy()

                    results.append(row)

    if best:
        print(f"    BEST: {best['strategy']} sl={best['sl_mult']} rr={best['rr']} hold={best['hold_bars']} "
              f"→ WR={best['wr']}% PnL={best['total_pnl']}% Sharpe={best['sharpe']} ({best['trades']} trades)")
    else:
        print(f"    NO valid config found (min {MIN_TRADES} trades)")

    return results, best


def compute_btc_correlation(coin, price_data):
    """Compute correlation between coin and BTC returns."""
    if coin == "BTC":
        return 1.0
    if coin not in price_data or "BTC" not in price_data:
        return 0.0

    btc = price_data["BTC"]
    cd = price_data[coin]

    # Align by timestamp
    common_ts = np.intersect1d(btc["ts"], cd["ts"])
    if len(common_ts) < 100:
        return 0.0

    btc_idx = np.searchsorted(btc["ts"], common_ts)
    cd_idx = np.searchsorted(cd["ts"], common_ts)

    btc_ret = np.diff(btc["c"][btc_idx]) / btc["c"][btc_idx[:-1]]
    cd_ret = np.diff(cd["c"][cd_idx]) / cd["c"][cd_idx[:-1]]

    if len(btc_ret) < 100:
        return 0.0
    return round(float(np.corrcoef(btc_ret, cd_ret)[0, 1]), 3)


def main():
    t0 = time.time()
    print("=" * 60)
    print("COIN OPTIMIZER - Grid Search")
    print("=" * 60)

    price_data, funding_data = load_data()

    # Precompute BTC signals
    btc_12h_change = get_btc_12h_change(price_data["BTC"])
    btc_ts = price_data["BTC"]["ts"]

    all_results = []
    optimal_configs = {}

    for coin in COINS:
        results, best = optimize_coin(coin, price_data, funding_data, btc_12h_change, btc_ts)
        all_results.extend(results)

        if best:
            corr = compute_btc_correlation(coin, price_data)
            optimal_configs[coin] = {
                "direction_strategy": best["strategy"],
                "sl_mult": best["sl_mult"],
                "rr": best["rr"],
                "hold_bars": best["hold_bars"],
                "backtest_wr": best["wr"],
                "backtest_pnl_pct": best["total_pnl"],
                "backtest_trades": best["trades"],
                "backtest_sharpe": best["sharpe"],
                "backtest_pf": best["pf"],
                "btc_correlation": corr,
            }
        else:
            optimal_configs[coin] = {
                "direction_strategy": "NONE",
                "sl_mult": 0,
                "rr": 0,
                "hold_bars": 0,
                "backtest_wr": 0,
                "backtest_pnl_pct": 0,
                "backtest_trades": 0,
                "backtest_sharpe": 0,
                "backtest_pf": 0,
                "btc_correlation": compute_btc_correlation(coin, price_data),
            }

    # Save optimal_config.json
    with open(os.path.join(OUT_DIR, "optimal_config.json"), "w") as f:
        json.dump(optimal_configs, f, indent=2)
    print(f"\nSaved optimal_config.json")

    # Save full_results.csv
    if all_results:
        keys = ["coin", "strategy", "sl_mult", "rr", "hold_bars", "trades", "wins",
                "wr", "total_pnl", "avg_pnl", "pf", "max_consec_loss", "sharpe"]
        with open(os.path.join(OUT_DIR, "full_results.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in all_results:
                w.writerow(r)
    print(f"Saved full_results.csv ({len(all_results)} rows)")

    # Generate summary
    generate_summary(optimal_configs, all_results)

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")


def generate_summary(optimal_configs, all_results):
    """Generate summary.md with analysis."""
    lines = []
    lines.append("# Coin Optimization Summary")
    lines.append(f"\nGenerated: 2026-04-15")
    lines.append(f"Grid: 5 strategies x 7 SL x 6 RR x 4 hold = 840 combos/coin")
    lines.append(f"Coins: {len(COINS)}")
    lines.append(f"Total combos tested: {len(all_results)}")
    lines.append("")

    # Per-coin best config table
    lines.append("## Per-Coin Optimal Configuration")
    lines.append("")
    lines.append("| Coin | Strategy | SL | R:R | Hold | WR% | PnL% | Trades | Sharpe | PF | BTC Corr |")
    lines.append("|------|----------|----|-----|------|-----|------|--------|--------|----|----------|")

    profitable = []
    unprofitable = []
    bad_coins = []

    for coin in COINS:
        cfg = optimal_configs[coin]
        if cfg["direction_strategy"] == "NONE":
            lines.append(f"| {coin} | NONE | - | - | - | - | - | - | - | - | {cfg['btc_correlation']} |")
            bad_coins.append(coin)
        else:
            lines.append(f"| {coin} | {cfg['direction_strategy']} | {cfg['sl_mult']} | {cfg['rr']} | {cfg['hold_bars']} | {cfg['backtest_wr']} | {cfg['backtest_pnl_pct']} | {cfg['backtest_trades']} | {cfg['backtest_sharpe']} | {cfg['backtest_pf']} | {cfg['btc_correlation']} |")
            if cfg["backtest_pnl_pct"] > 0:
                profitable.append((coin, cfg))
            else:
                unprofitable.append((coin, cfg))
                if cfg["backtest_wr"] < 50:
                    bad_coins.append(coin)

    lines.append("")

    # Strategy distribution
    lines.append("## Strategy Distribution (which wins most often)")
    lines.append("")
    strat_counts = defaultdict(int)
    for coin in COINS:
        s = optimal_configs[coin]["direction_strategy"]
        if s != "NONE":
            strat_counts[s] += 1
    for s, c in sorted(strat_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{s}**: {c} coins")
    lines.append("")

    # R:R distribution
    lines.append("## R:R Distribution")
    lines.append("")
    rr_counts = defaultdict(int)
    for coin in COINS:
        rr = optimal_configs[coin]["rr"]
        if rr > 0:
            rr_counts[rr] += 1
    for rr, c in sorted(rr_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{rr}**: {c} coins")
    lines.append("")

    # SL distribution
    lines.append("## SL Multiplier Distribution")
    lines.append("")
    sl_counts = defaultdict(int)
    for coin in COINS:
        sl = optimal_configs[coin]["sl_mult"]
        if sl > 0:
            sl_counts[sl] += 1
    for sl, c in sorted(sl_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{sl}**: {c} coins")
    lines.append("")

    # Top 10 by Sharpe
    lines.append("## Top 10 Coins by Sharpe Ratio")
    lines.append("")
    ranked = sorted(
        [(c, cfg) for c, cfg in optimal_configs.items() if cfg["direction_strategy"] != "NONE"],
        key=lambda x: -x[1]["backtest_sharpe"]
    )
    for i, (coin, cfg) in enumerate(ranked[:10]):
        lines.append(f"{i+1}. **{coin}**: Sharpe={cfg['backtest_sharpe']}, PnL={cfg['backtest_pnl_pct']}%, WR={cfg['backtest_wr']}%")
    lines.append("")

    # Bottom 10
    lines.append("## Bottom 10 Coins by Sharpe Ratio")
    lines.append("")
    for i, (coin, cfg) in enumerate(ranked[-10:]):
        lines.append(f"{i+1}. **{coin}**: Sharpe={cfg['backtest_sharpe']}, PnL={cfg['backtest_pnl_pct']}%, WR={cfg['backtest_wr']}%")
    lines.append("")

    # Bad coins
    lines.append("## BAD COINS (WR < 50% or no valid config)")
    lines.append("")
    if bad_coins:
        lines.append(f"Coins to avoid: **{', '.join(bad_coins)}**")
    else:
        lines.append("All coins have at least one profitable configuration.")
    lines.append("")

    # Coins where optimal WR < 50
    lines.append("## Honest Assessment: Coins with WR < 50%")
    lines.append("")
    for coin in COINS:
        cfg = optimal_configs[coin]
        if cfg["direction_strategy"] != "NONE" and cfg["backtest_wr"] < 50:
            lines.append(f"- **{coin}**: WR={cfg['backtest_wr']}% (profitable via R:R={cfg['rr']}, PF={cfg['backtest_pf']})")
    lines.append("")

    with open(os.path.join(OUT_DIR, "summary.md"), "w") as f:
        f.write("\n".join(lines))
    print("Saved summary.md")


if __name__ == "__main__":
    main()
