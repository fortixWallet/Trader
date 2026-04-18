#!/usr/bin/env python3
"""
Combined Signal Test: Do 8 signals together beat any single signal?
Period: Mar 27 - Apr 17 2026 (21 days), 4h outcome horizon.
"""

import sqlite3
import csv
import os
from datetime import datetime, timezone, timedelta
from itertools import combinations
from collections import defaultdict

DB = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/market.db"
OUT = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/combined_signal_test"

# 22 coins with L/S ratio data (the most constrained signal)
COINS = [
    'ADA','ALGO','APT','AVAX','BCH','BNB','BTC','CRV','ETH','FIL',
    'HBAR','LDO','LINK','NEAR','PENDLE','POL','SOL','SUI','TON','UNI','XLM','XRP'
]

SIGNAL_NAMES = ['macro','ema','ls_contrarian','funding_contrarian','oi_change','fng','sopr','momentum']

START_TS = int(datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc).timestamp())
END_TS   = int(datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc).timestamp())

conn = sqlite3.connect(DB)

# ── Pre-load data into memory for speed ──────────────────────────────

print("Loading data...")

# 4h prices for all coins + BTC
prices_4h = {}  # (coin, ts) -> close
for row in conn.execute(
    "SELECT coin, timestamp, close FROM prices WHERE timeframe='4h' AND timestamp BETWEEN ? AND ?",
    (START_TS - 86400*10, END_TS + 86400*2)
):
    prices_4h[(row[0], row[1])] = row[2]

# Build sorted timestamp list per coin for 4h
coin_4h_ts = defaultdict(list)
for (c, ts) in sorted(prices_4h.keys()):
    coin_4h_ts[c].append(ts)

# 1h prices for outcome measurement
prices_1h = {}
for row in conn.execute(
    "SELECT coin, timestamp, close FROM prices WHERE timeframe='1h' AND timestamp BETWEEN ? AND ?",
    (START_TS - 3600, END_TS + 86400)
):
    prices_1h[(row[0], row[1])] = row[2]

# Bybit L/S ratio
ls_data = {}  # (coin, ts) -> buy_ratio
for row in conn.execute("SELECT coin, timestamp, buy_ratio FROM bybit_ls_ratio"):
    ls_data[(row[0], row[1])] = row[2]

# Funding rates
funding_data = {}
for row in conn.execute("SELECT coin, timestamp, rate FROM funding_rates WHERE timestamp BETWEEN ? AND ?",
                        (START_TS - 86400*2, END_TS + 86400)):
    funding_data[(row[0], row[1])] = row[2]

# OI (aggregated, 'All' exchange)
oi_data = {}
for row in conn.execute(
    "SELECT coin, timestamp, oi_usd FROM cg_aggregated_oi WHERE exchange='All' AND timestamp BETWEEN ? AND ?",
    (START_TS - 86400*2, END_TS + 86400)
):
    oi_data[(row[0], row[1])] = row[2]

# Fear & Greed (daily)
fng_data = {}
for row in conn.execute("SELECT date, value FROM fear_greed"):
    fng_data[row[0]] = row[1]

# SOPR (daily)
sopr_data = {}
for row in conn.execute("SELECT date, value FROM cq_btc_onchain WHERE metric='sopr'"):
    sopr_data[row[0]] = row[1]

print(f"Loaded: {len(prices_4h)} 4h prices, {len(prices_1h)} 1h prices, {len(ls_data)} LS, {len(funding_data)} funding, {len(oi_data)} OI")

# ── Helper functions ─────────────────────────────────────────────────

def get_closest_before(data_dict, coin, ts, max_age=86400):
    """Find closest data point at or before ts within max_age seconds."""
    best_ts = None
    best_val = None
    for delta in range(0, max_age, 3600):
        check = ts - delta
        if (coin, check) in data_dict:
            return data_dict[(coin, check)]
    return None

def get_closest_before_any(data_dict, coin, ts, max_age=86400*2, step=3600):
    """More flexible search."""
    for delta in range(0, max_age, step):
        check = ts - delta
        if (coin, check) in data_dict:
            return data_dict[(coin, check)]
    return None

def compute_ema(prices_list, period):
    """Compute EMA from a list of prices (oldest first)."""
    if len(prices_list) < period:
        return None
    k = 2 / (period + 1)
    ema = prices_list[0]
    for p in prices_list[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def get_4h_closes_before(coin, ts, n):
    """Get n 4h closes at or before ts, returned oldest-first."""
    all_ts = coin_4h_ts.get(coin, [])
    # Binary search for position
    import bisect
    idx = bisect.bisect_right(all_ts, ts) - 1
    if idx < n - 1:
        return []
    result = []
    for i in range(idx - n + 1, idx + 1):
        t = all_ts[i]
        result.append(prices_4h[(coin, t)])
    return result

def compute_signals(coin, ts):
    """Compute all 8 signals for a coin at timestamp ts."""
    signals = {}
    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

    # 1. BTC macro: 7-day trend on 4h (42 bars)
    btc_closes = get_4h_closes_before('BTC', ts, 42)
    if len(btc_closes) >= 42:
        trend = (btc_closes[-1] / btc_closes[0] - 1) * 100
        signals['macro'] = 1 if trend > 1.0 else -1 if trend < -1.0 else 0
    else:
        signals['macro'] = 0

    # 2. Coin EMA: EMA8 vs EMA21 on 4h
    closes_21 = get_4h_closes_before(coin, ts, 21)
    if len(closes_21) >= 21:
        ema8 = compute_ema(closes_21, 8)
        ema21 = compute_ema(closes_21, 21)
        if ema8 and ema21:
            signals['ema'] = 1 if ema8 > ema21 else -1
        else:
            signals['ema'] = 0
    else:
        signals['ema'] = 0

    # 3. Bybit L/S ratio (contrarian)
    ls_val = get_closest_before(ls_data, coin, ts, max_age=86400)
    if ls_val is not None:
        signals['ls_contrarian'] = -1 if ls_val > 0.55 else 1 if ls_val < 0.45 else 0
    else:
        signals['ls_contrarian'] = 0

    # 4. Funding rate (contrarian)
    fr_val = get_closest_before(funding_data, coin, ts, max_age=86400*2)
    if fr_val is not None:
        signals['funding_contrarian'] = -1 if fr_val > 0.0003 else 1 if fr_val < -0.0003 else 0
    else:
        signals['funding_contrarian'] = 0

    # 5. OI change (4h change from cg_aggregated_oi)
    oi_now = get_closest_before_any(oi_data, coin, ts, max_age=86400, step=60)
    oi_prev = get_closest_before_any(oi_data, coin, ts - 14400, max_age=86400, step=60)
    if oi_now and oi_prev and oi_prev > 0:
        oi_chg = (oi_now / oi_prev - 1) * 100
        # Rising OI with trend = confirmation. We simplify: rising OI = +1 (leverage building, trend continues)
        # Falling OI = -1 (unwind)
        signals['oi_change'] = 1 if oi_chg > 1.0 else -1 if oi_chg < -1.0 else 0
    else:
        signals['oi_change'] = 0

    # 6. Fear & Greed (contrarian)
    fng_val = fng_data.get(date_str)
    if fng_val is None:
        # Try yesterday
        prev_date = (datetime.fromtimestamp(ts, tz=timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        fng_val = fng_data.get(prev_date)
    if fng_val is not None:
        signals['fng'] = 1 if fng_val < 30 else -1 if fng_val > 70 else 0
    else:
        signals['fng'] = 0

    # 7. SOPR (BTC on-chain)
    sopr_val = sopr_data.get(date_str)
    if sopr_val is None:
        prev_date = (datetime.fromtimestamp(ts, tz=timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        sopr_val = sopr_data.get(prev_date)
    if sopr_val is not None:
        signals['sopr'] = -1 if sopr_val > 1.02 else 1 if sopr_val < 0.98 else 0
    else:
        signals['sopr'] = 0

    # 8. Coin momentum: 4h close > 4h close 3 bars ago
    closes_4 = get_4h_closes_before(coin, ts, 4)
    if len(closes_4) >= 4:
        signals['momentum'] = 1 if closes_4[-1] > closes_4[0] else -1
    else:
        signals['momentum'] = 0

    return signals

def get_outcome_4h(coin, ts):
    """Get price change 4 hours after ts using 1h data."""
    p_now = prices_1h.get((coin, ts))
    p_later = prices_1h.get((coin, ts + 4*3600))
    if p_now and p_later and p_now > 0:
        return (p_later / p_now - 1) * 100
    return None

# ── Main loop ────────────────────────────────────────────────────────

print("Computing signals...")

# Generate hourly timestamps in range
results = []
ts = START_TS
while ts <= END_TS:
    for coin in COINS:
        sigs = compute_signals(coin, ts)
        combined = sum(sigs.values())
        outcome = get_outcome_4h(coin, ts)
        if outcome is not None:
            predicted_dir = 1 if combined > 0 else -1 if combined < 0 else 0
            actual_dir = 1 if outcome > 0 else -1
            correct = 1 if (predicted_dir != 0 and predicted_dir == actual_dir) else 0
            results.append({
                'coin': coin,
                'timestamp': ts,
                'datetime': datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M'),
                'combined_score': combined,
                'actual_dir': actual_dir,
                'price_change_4h': round(outcome, 4),
                'correct': correct,
                **{f'sig_{k}': v for k, v in sigs.items()}
            })
    ts += 3600

print(f"Total observations: {len(results)}")

# ── Save results.csv ─────────────────────────────────────────────────

csv_path = os.path.join(OUT, "results.csv")
fieldnames = ['coin','timestamp','datetime','combined_score','actual_dir','price_change_4h','correct'] + [f'sig_{s}' for s in SIGNAL_NAMES]
with open(csv_path, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in results:
        w.writerow(r)
print(f"Saved {csv_path}")

# ── Analysis ─────────────────────────────────────────────────────────

# 1. Per-score WR
score_stats = defaultdict(lambda: {'total': 0, 'correct': 0, 'returns': []})
for r in results:
    s = r['combined_score']
    score_stats[s]['total'] += 1
    if s != 0:
        predicted = 1 if s > 0 else -1
        if predicted == r['actual_dir']:
            score_stats[s]['correct'] += 1
    score_stats[s]['returns'].append(r['price_change_4h'])

# 2. Individual signal WR
sig_stats = {}
for sig_name in SIGNAL_NAMES:
    key = f'sig_{sig_name}'
    total = 0
    correct = 0
    for r in results:
        val = r[key]
        if val != 0:
            total += 1
            if val == r['actual_dir']:
                correct += 1
    sig_stats[sig_name] = {'total': total, 'correct': correct, 'wr': correct/total*100 if total > 0 else 0}

# 3. Combined WR at various thresholds
threshold_stats = {}
for thresh in [2, 3, 4, 5, 6]:
    # Long side (score >= thresh)
    long_total = sum(1 for r in results if r['combined_score'] >= thresh)
    long_correct = sum(1 for r in results if r['combined_score'] >= thresh and r['actual_dir'] == 1)
    long_wr = long_correct / long_total * 100 if long_total > 0 else 0

    # Short side (score <= -thresh)
    short_total = sum(1 for r in results if r['combined_score'] <= -thresh)
    short_correct = sum(1 for r in results if r['combined_score'] <= -thresh and r['actual_dir'] == -1)
    short_wr = short_correct / short_total * 100 if short_total > 0 else 0

    both_total = long_total + short_total
    both_correct = long_correct + short_correct
    both_wr = both_correct / both_total * 100 if both_total > 0 else 0

    # avg return
    returns_at_thresh = []
    for r in results:
        if r['combined_score'] >= thresh:
            returns_at_thresh.append(r['price_change_4h'])
        elif r['combined_score'] <= -thresh:
            returns_at_thresh.append(-r['price_change_4h'])  # flip for short
    avg_ret = sum(returns_at_thresh) / len(returns_at_thresh) if returns_at_thresh else 0

    threshold_stats[thresh] = {
        'long_total': long_total, 'long_wr': long_wr,
        'short_total': short_total, 'short_wr': short_wr,
        'both_total': both_total, 'both_wr': both_wr,
        'avg_return': avg_ret,
        'trades_per_day': both_total / 21
    }

# 4. Optimal subset search
print("\nSearching optimal signal subsets...")
best_subsets = []
for size in range(4, 9):
    best_wr = 0
    best_combo = None
    best_n = 0
    for combo in combinations(SIGNAL_NAMES, size):
        # For each observation, compute subscore using only these signals
        total = 0
        correct = 0
        for r in results:
            subscore = sum(r[f'sig_{s}'] for s in combo)
            if abs(subscore) >= 3:  # threshold of 3 for the subset
                total += 1
                predicted = 1 if subscore > 0 else -1
                if predicted == r['actual_dir']:
                    correct += 1
        if total >= 50:  # minimum sample
            wr = correct / total * 100
            if wr > best_wr:
                best_wr = wr
                best_combo = combo
                best_n = total
    if best_combo:
        best_subsets.append((size, best_combo, best_wr, best_n))
        print(f"  Best {size}-signal subset: {best_combo} → WR={best_wr:.1f}% (n={best_n})")

# Also try subset with threshold of 2
print("\nSubsets with threshold >= 2:")
best_subsets_t2 = []
for size in range(4, 9):
    best_wr = 0
    best_combo = None
    best_n = 0
    for combo in combinations(SIGNAL_NAMES, size):
        total = 0
        correct = 0
        for r in results:
            subscore = sum(r[f'sig_{s}'] for s in combo)
            if abs(subscore) >= 2:
                total += 1
                predicted = 1 if subscore > 0 else -1
                if predicted == r['actual_dir']:
                    correct += 1
        if total >= 100:
            wr = correct / total * 100
            if wr > best_wr:
                best_wr = wr
                best_combo = combo
                best_n = total
    if best_combo:
        best_subsets_t2.append((size, best_combo, best_wr, best_n))
        print(f"  Best {size}-signal subset (t>=2): {best_combo} → WR={best_wr:.1f}% (n={best_n})")

# ── Generate summary.md ──────────────────────────────────────────────

md_path = os.path.join(OUT, "summary.md")
with open(md_path, 'w') as f:
    f.write("# Combined Signal Test Results\n")
    f.write(f"**Period:** Mar 27 - Apr 17, 2026 (21 days) | **Coins:** {len(COINS)} | **Observations:** {len(results):,}\n\n")

    f.write("## 1. Win Rate by Combined Score\n\n")
    f.write("| Score | Count | WR% | Avg 4h Return |\n")
    f.write("|------:|------:|----:|---------------:|\n")
    for s in sorted(score_stats.keys()):
        st = score_stats[s]
        wr = st['correct'] / st['total'] * 100 if st['total'] > 0 and s != 0 else 0
        avg_r = sum(st['returns']) / len(st['returns']) if st['returns'] else 0
        tag = ""
        if s > 0:
            tag = f" (LONG pred)"
        elif s < 0:
            tag = f" (SHORT pred)"
        f.write(f"| {s:+d} | {st['total']:,} | {wr:.1f}% | {avg_r:+.3f}%{tag} |\n")

    f.write("\n## 2. Individual Signal Win Rates\n\n")
    f.write("| Signal | Fired | WR% |\n")
    f.write("|--------|------:|----:|\n")
    for sig in sorted(sig_stats, key=lambda x: sig_stats[x]['wr'], reverse=True):
        st = sig_stats[sig]
        f.write(f"| {sig} | {st['total']:,} | {st['wr']:.1f}% |\n")

    f.write("\n## 3. Combined Signal at Thresholds\n\n")
    f.write("| Threshold | Long (n) | Long WR | Short (n) | Short WR | Total (n) | Total WR | Avg Ret | Trades/Day |\n")
    f.write("|----------:|---------:|--------:|----------:|---------:|----------:|---------:|--------:|-----------:|\n")
    for t in sorted(threshold_stats):
        st = threshold_stats[t]
        f.write(f"| >= {t} | {st['long_total']:,} | {st['long_wr']:.1f}% | {st['short_total']:,} | {st['short_wr']:.1f}% | {st['both_total']:,} | {st['both_wr']:.1f}% | {st['avg_return']:+.3f}% | {st['trades_per_day']:.1f} |\n")

    f.write("\n## 4. Optimal Signal Subsets (threshold >= 3)\n\n")
    f.write("| Size | Signals | WR% | Trades |\n")
    f.write("|-----:|---------|----:|-------:|\n")
    for size, combo, wr, n in best_subsets:
        f.write(f"| {size} | {', '.join(combo)} | {wr:.1f}% | {n:,} |\n")

    f.write("\n## 5. Optimal Signal Subsets (threshold >= 2)\n\n")
    f.write("| Size | Signals | WR% | Trades |\n")
    f.write("|-----:|---------|----:|-------:|\n")
    for size, combo, wr, n in best_subsets_t2:
        f.write(f"| {size} | {', '.join(combo)} | {wr:.1f}% | {n:,} |\n")

    # Find the best overall
    all_best = best_subsets + best_subsets_t2
    if all_best:
        top = max(all_best, key=lambda x: x[2])
        f.write(f"\n## 6. Recommendation\n\n")
        f.write(f"**Best subset:** {', '.join(top[1])} ({top[0]} signals)\n")
        f.write(f"**Win rate:** {top[2]:.1f}% over {top[3]:,} trades\n\n")

    # Best threshold for all-8
    best_t = max(threshold_stats, key=lambda t: threshold_stats[t]['both_wr'] if threshold_stats[t]['both_total'] >= 20 else 0)
    st = threshold_stats[best_t]
    f.write(f"**Best all-8 threshold:** >= {best_t} → WR {st['both_wr']:.1f}% ({st['both_total']:,} trades, {st['trades_per_day']:.1f}/day)\n")

    # Compare combined best vs individual best
    best_indiv = max(sig_stats, key=lambda x: sig_stats[x]['wr'])
    bi = sig_stats[best_indiv]
    f.write(f"\n**Best individual signal:** {best_indiv} → WR {bi['wr']:.1f}% ({bi['total']:,} fires)\n")
    f.write(f"**Combined advantage:** {st['both_wr'] - bi['wr']:+.1f}pp over best individual\n")

print(f"\nSaved {md_path}")

# ── Print summary to console ────────────────────────────────────────

print("\n" + "="*70)
print("COMBINED SIGNAL TEST - SUMMARY")
print("="*70)

print("\nPer-Score Win Rates:")
print(f"{'Score':>6} {'Count':>7} {'WR%':>7} {'AvgRet':>8}")
for s in sorted(score_stats.keys()):
    st = score_stats[s]
    wr = st['correct'] / st['total'] * 100 if st['total'] > 0 and s != 0 else 0
    avg_r = sum(st['returns']) / len(st['returns']) if st['returns'] else 0
    print(f"{s:+6d} {st['total']:7,} {wr:6.1f}% {avg_r:+7.3f}%")

print("\nIndividual Signal WRs:")
for sig in sorted(sig_stats, key=lambda x: sig_stats[x]['wr'], reverse=True):
    st = sig_stats[sig]
    print(f"  {sig:20s} → WR {st['wr']:5.1f}% (n={st['total']:,})")

print("\nThreshold Analysis:")
for t in sorted(threshold_stats):
    st = threshold_stats[t]
    print(f"  |score| >= {t}: WR {st['both_wr']:5.1f}% (n={st['both_total']:,}, {st['trades_per_day']:.1f}/day, avg_ret={st['avg_return']:+.3f}%)")

print("\nBest subsets (threshold >= 3):")
for size, combo, wr, n in best_subsets:
    print(f"  {size} signals: {combo} → WR {wr:.1f}% (n={n})")

print("\nDone!")
