#!/usr/bin/env python3
"""
Checklist Calibration: 7-point objective confidence scoring vs Profi's gut feel.
Analyzes all CLOSED trades from fortix_trades Apr 11-17, 2026.
"""

import sqlite3
import csv
import os
import re
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path

BASE = Path("/Users/williamstorm/Documents/Trading (OKX) 1h")
DB_PATH = BASE / "data" / "crypto" / "market.db"
OUT_DIR = BASE / "data" / "crypto" / "checklist_calibration"
OUT_DIR.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ── Load all CLOSED trades Apr 11-17 ──
cur.execute("""
    SELECT * FROM fortix_trades
    WHERE status = 'CLOSED'
      AND created_at >= '2026-04-11'
      AND created_at < '2026-04-18'
    ORDER BY filled_at
""")
trades = [dict(r) for r in cur.fetchall()]
print(f"Loaded {len(trades)} closed trades")


def iso_to_unix(iso_str):
    """Parse ISO timestamp to Unix UTC seconds."""
    if not iso_str:
        return None
    # Handle timezone offset
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp())


def get_btc_7d_trend(entry_ts):
    """BTC 7-day price change at entry time. Returns pct change."""
    ts_7d_ago = entry_ts - 7 * 86400
    cur.execute("""
        SELECT close FROM prices
        WHERE coin='BTC' AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (entry_ts,))
    row_now = cur.fetchone()
    cur.execute("""
        SELECT close FROM prices
        WHERE coin='BTC' AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (ts_7d_ago,))
    row_7d = cur.fetchone()
    if row_now and row_7d:
        return (row_now['close'] - row_7d['close']) / row_7d['close']
    return None


def get_4h_ema_alignment(coin, entry_ts, direction):
    """Check if coin's 4H EMA8 vs EMA21 aligns with trade direction."""
    # Get last 30 4h candles before entry
    cur.execute("""
        SELECT close FROM prices
        WHERE coin=? AND timeframe='4h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 30
    """, (coin, entry_ts))
    rows = cur.fetchall()
    if len(rows) < 21:
        return None
    closes = [r['close'] for r in reversed(rows)]

    # Compute EMA
    def ema(data, period):
        k = 2.0 / (period + 1)
        val = data[0]
        for p in data[1:]:
            val = p * k + val * (1 - k)
        return val

    ema8 = ema(closes, 8)
    ema21 = ema(closes, 21)

    if direction == 'LONG':
        return ema8 > ema21
    else:
        return ema8 < ema21


def parse_ob_from_reason(reason, direction):
    """Parse OB imbalance from reason text. Returns True if confirms direction."""
    if not reason:
        return None

    # Look for patterns like "OB buy +23%", "OB sell -48%", "OB SELL -83%", "OB neutral +10%"
    m = re.search(r'OB\s+(?:buy|sell|SELL|BUY|neutral|NEUTRAL)\s+([+-]?\d+)%', reason, re.IGNORECASE)
    if m:
        ob_pct = int(m.group(1))
        if direction == 'LONG' and ob_pct > 10:
            return True
        elif direction == 'SHORT' and ob_pct < -10:
            return True
        else:
            return False

    # Also check fill_ob_imbalance if available
    return None


def get_funding_contrarian(coin, entry_ts, direction):
    """Check if funding rate is contrarian to direction."""
    cur.execute("""
        SELECT rate FROM funding_rates
        WHERE coin=? AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (coin, entry_ts))
    row = cur.fetchone()
    if row:
        rate = row['rate']
        if direction == 'LONG' and rate < 0:
            return True
        elif direction == 'SHORT' and rate > 0:
            return True
        return False
    return None


def compute_rsi(coin, entry_ts, period=14):
    """Compute RSI(14) on 1h candles at entry time."""
    cur.execute("""
        SELECT close FROM prices
        WHERE coin=? AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT ?
    """, (coin, entry_ts, period + 1))
    rows = cur.fetchall()
    if len(rows) < period + 1:
        return None
    closes = [r['close'] for r in reversed(rows)]
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_coin_historical_wr(coin):
    """Compute historical WR for this coin from all closed trades."""
    cur.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins
        FROM fortix_trades
        WHERE coin=? AND status='CLOSED'
    """, (coin,))
    row = cur.fetchone()
    if row and row['total'] > 0:
        return row['wins'] / row['total']
    return None


def get_entry_distance(coin, entry_ts, fill_price):
    """Distance from fill price to closest 1h candle close."""
    if not fill_price:
        return None
    cur.execute("""
        SELECT close FROM prices
        WHERE coin=? AND timeframe='1h' AND timestamp <= ?
        ORDER BY timestamp DESC LIMIT 1
    """, (coin, entry_ts))
    row = cur.fetchone()
    if row and row['close'] > 0:
        return abs(fill_price - row['close']) / row['close']
    return None


# ── Score each trade ──
scored = []

for t in trades:
    entry_ts = iso_to_unix(t['filled_at'])
    if not entry_ts:
        entry_ts = iso_to_unix(t['created_at'])
    if not entry_ts:
        continue

    coin = t['coin']
    direction = t['direction']
    fill_price = t['fill_price'] or t['entry_price']
    pnl_usd = t['pnl_usd'] or 0
    pnl_pct = t['pnl_pct'] or 0
    is_win = 1 if pnl_usd > 0 else 0
    profi_conf = t['confidence']

    scores = {}

    # 1. MACRO: BTC 7d trend
    btc_7d = get_btc_7d_trend(entry_ts)
    if btc_7d is not None:
        if direction == 'LONG':
            scores['macro'] = 1 if btc_7d > 0 else 0
        else:
            scores['macro'] = 1 if btc_7d < 0 else 0
    else:
        scores['macro'] = 0.5

    # 2. Coin 4H EMA alignment
    alignment = get_4h_ema_alignment(coin, entry_ts, direction)
    if alignment is not None:
        scores['ema_4h'] = 1 if alignment else 0
    else:
        scores['ema_4h'] = 0.5

    # 3. OB confirms
    ob_reason = parse_ob_from_reason(t.get('reason'), direction)
    if ob_reason is not None:
        scores['ob'] = 1 if ob_reason else 0
    else:
        # Fallback: use fill_ob_imbalance
        imb = t.get('fill_ob_imbalance')
        if imb is not None:
            if direction == 'LONG' and imb > 0.6:
                scores['ob'] = 1
            elif direction == 'SHORT' and imb < 0.4:
                scores['ob'] = 1
            else:
                scores['ob'] = 0
        else:
            scores['ob'] = 0.5

    # 4. Funding contrarian
    # Use fill_funding_rate first if available
    ffr = t.get('fill_funding_rate')
    if ffr is not None:
        if direction == 'LONG' and ffr < 0:
            scores['funding'] = 1
        elif direction == 'SHORT' and ffr > 0:
            scores['funding'] = 1
        else:
            scores['funding'] = 0
    else:
        funding_ok = get_funding_contrarian(coin, entry_ts, direction)
        if funding_ok is not None:
            scores['funding'] = 1 if funding_ok else 0
        else:
            scores['funding'] = 0.5

    # 5. RSI not extreme
    rsi = compute_rsi(coin, entry_ts)
    if rsi is not None:
        if direction == 'LONG':
            scores['rsi'] = 1 if rsi < 75 else 0
        else:
            scores['rsi'] = 1 if rsi > 25 else 0
    else:
        scores['rsi'] = 0.5

    # 6. Per-coin historical WR > 50%
    wr = get_coin_historical_wr(coin)
    if wr is not None:
        scores['coin_wr'] = 1 if wr > 0.5 else 0
    else:
        scores['coin_wr'] = 0.5

    # 7. Entry distance < 0.5%
    dist = get_entry_distance(coin, entry_ts, fill_price)
    if dist is not None:
        scores['entry_dist'] = 1 if dist < 0.005 else 0
    else:
        scores['entry_dist'] = 0.5

    total_score = sum(scores.values())

    scored.append({
        'trade_id': t['trade_id'],
        'coin': coin,
        'direction': direction,
        'filled_at': t['filled_at'],
        'fill_price': fill_price,
        'pnl_usd': pnl_usd,
        'pnl_pct': pnl_pct,
        'exit_reason': t['exit_reason'],
        'is_win': is_win,
        'profi_conf': profi_conf,
        'macro': scores['macro'],
        'ema_4h': scores['ema_4h'],
        'ob': scores['ob'],
        'funding': scores['funding'],
        'rsi': scores['rsi'],
        'coin_wr': scores['coin_wr'],
        'entry_dist': scores['entry_dist'],
        'checklist_score': total_score,
        'checklist_pct': total_score / 7.0,
        'rsi_value': rsi,
        'btc_7d_pct': btc_7d,
    })

print(f"Scored {len(scored)} trades")

# ── Write trades_scored.csv ──
csv_path = OUT_DIR / "trades_scored.csv"
if scored:
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=scored[0].keys())
        writer.writeheader()
        writer.writerows(scored)
    print(f"Wrote {csv_path}")

# ── Analysis ──

# Group by rounded checklist score
from collections import defaultdict

score_buckets = defaultdict(list)
for s in scored:
    bucket = round(s['checklist_score'])  # round to nearest int
    score_buckets[bucket].append(s)

print("\n=== SCORE DISTRIBUTION ===")
print(f"{'Score':>6} | {'Trades':>6} | {'WR%':>6} | {'Avg PnL $':>10} | {'Tot PnL $':>10}")
print("-" * 50)
score_rows = []
for bucket in sorted(score_buckets.keys()):
    trades_b = score_buckets[bucket]
    n = len(trades_b)
    wins = sum(1 for t in trades_b if t['is_win'])
    wr = wins / n * 100 if n > 0 else 0
    avg_pnl = sum(t['pnl_usd'] for t in trades_b) / n if n > 0 else 0
    tot_pnl = sum(t['pnl_usd'] for t in trades_b)
    print(f"{bucket:>6} | {n:>6} | {wr:>5.1f}% | ${avg_pnl:>+9.2f} | ${tot_pnl:>+9.2f}")
    score_rows.append((bucket, n, wr, avg_pnl, tot_pnl))

# Per-factor analysis
print("\n=== PER-FACTOR PREDICTIVENESS ===")
factors = ['macro', 'ema_4h', 'ob', 'funding', 'rsi', 'coin_wr', 'entry_dist']
factor_results = []
for f_name in factors:
    yes_trades = [s for s in scored if s[f_name] == 1]
    no_trades = [s for s in scored if s[f_name] == 0]

    yes_wr = sum(1 for t in yes_trades if t['is_win']) / len(yes_trades) * 100 if yes_trades else 0
    no_wr = sum(1 for t in no_trades if t['is_win']) / len(no_trades) * 100 if no_trades else 0
    yes_pnl = sum(t['pnl_usd'] for t in yes_trades) / len(yes_trades) if yes_trades else 0
    no_pnl = sum(t['pnl_usd'] for t in no_trades) / len(no_trades) if no_trades else 0
    delta_wr = yes_wr - no_wr

    print(f"  {f_name:>12}: YES({len(yes_trades):>2}) WR={yes_wr:>5.1f}% avg${yes_pnl:>+7.1f} | NO({len(no_trades):>2}) WR={no_wr:>5.1f}% avg${no_pnl:>+7.1f} | delta_WR={delta_wr:>+5.1f}pp")
    factor_results.append((f_name, len(yes_trades), yes_wr, yes_pnl, len(no_trades), no_wr, no_pnl, delta_wr))

# Profi confidence vs checklist correlation
print("\n=== PROFI CONFIDENCE vs CHECKLIST ===")
profi_vals = [s['profi_conf'] for s in scored if s['profi_conf'] is not None]
check_vals = [s['checklist_pct'] for s in scored if s['profi_conf'] is not None]

if profi_vals and check_vals:
    mean_p = sum(profi_vals) / len(profi_vals)
    mean_c = sum(check_vals) / len(check_vals)
    cov = sum((p - mean_p) * (c - mean_c) for p, c in zip(profi_vals, check_vals))
    std_p = math.sqrt(sum((p - mean_p) ** 2 for p in profi_vals))
    std_c = math.sqrt(sum((c - mean_c) ** 2 for c in check_vals))
    corr = cov / (std_p * std_c) if std_p > 0 and std_c > 0 else 0
    print(f"  Pearson correlation: {corr:.3f}")
    print(f"  Profi mean: {mean_p:.3f}, Checklist mean: {mean_c:.3f}")

# Overall comparison: would filtering by checklist >= 5 improve results?
print("\n=== THRESHOLD ANALYSIS ===")
for threshold in [3, 4, 5, 6]:
    above = [s for s in scored if s['checklist_score'] >= threshold]
    below = [s for s in scored if s['checklist_score'] < threshold]
    if above:
        wr_above = sum(1 for t in above if t['is_win']) / len(above) * 100
        pnl_above = sum(t['pnl_usd'] for t in above)
        avg_above = pnl_above / len(above)
    else:
        wr_above = pnl_above = avg_above = 0
    if below:
        wr_below = sum(1 for t in below if t['is_win']) / len(below) * 100
        pnl_below = sum(t['pnl_usd'] for t in below)
    else:
        wr_below = pnl_below = 0
    print(f"  Score >= {threshold}: {len(above)} trades, WR={wr_above:.1f}%, total=${pnl_above:+.2f}, avg=${avg_above:+.2f}")
    print(f"  Score <  {threshold}: {len(below)} trades, WR={wr_below:.1f}%, total=${pnl_below:+.2f}")

# ── Build summary.md ──
overall_wr = sum(1 for s in scored if s['is_win']) / len(scored) * 100 if scored else 0
overall_pnl = sum(s['pnl_usd'] for s in scored)

summary = f"""# Checklist Calibration Report — Apr 11-17, 2026

**Trades analyzed**: {len(scored)} closed trades over 6 days
**Overall WR**: {overall_wr:.1f}% | **Total PnL**: ${overall_pnl:+,.2f}

## Score Distribution

| Score | Trades | WR% | Avg PnL | Total PnL |
|------:|-------:|----:|--------:|----------:|
"""
for bucket, n, wr, avg_pnl, tot_pnl in score_rows:
    summary += f"| {bucket} | {n} | {wr:.1f}% | ${avg_pnl:+.2f} | ${tot_pnl:+.2f} |\n"

# Best threshold
best_t = None
best_metric = -999999
for threshold in [3, 4, 5, 6]:
    above = [s for s in scored if s['checklist_score'] >= threshold]
    if len(above) >= 5:
        wr_a = sum(1 for t in above if t['is_win']) / len(above) * 100
        pnl_a = sum(t['pnl_usd'] for t in above)
        # Use total PnL as metric (practical)
        if pnl_a > best_metric:
            best_metric = pnl_a
            best_t = threshold

summary += f"""
## Best Threshold

**Recommended minimum score: {best_t}** (maximizes total PnL from filtered trades)

## Profi Confidence vs Checklist

- Pearson correlation: **{corr:.3f}**
- Profi assigns narrow range ({min(profi_vals):.0%}-{max(profi_vals):.0%}), checklist gives wider spread

## Per-Factor Analysis (most predictive first)

| Factor | YES(n) | YES WR | NO(n) | NO WR | Delta WR |
|--------|-------:|-------:|------:|------:|---------:|
"""
# Sort by absolute delta_wr
factor_results.sort(key=lambda x: abs(x[7]), reverse=True)
for f_name, yn, ywr, ypnl, nn, nwr, npnl, dwr in factor_results:
    summary += f"| {f_name} | {yn} | {ywr:.1f}% | {nn} | {nwr:.1f}% | {dwr:+.1f}pp |\n"

# Verdict
summary += f"""
## Verdict

"""
# Check if high-score trades outperform
high = [s for s in scored if s['checklist_score'] >= (best_t or 5)]
low = [s for s in scored if s['checklist_score'] < (best_t or 5)]
high_wr = sum(1 for t in high if t['is_win']) / len(high) * 100 if high else 0
low_wr = sum(1 for t in low if t['is_win']) / len(low) * 100 if low else 0

if high_wr > low_wr + 5:
    summary += f"The checklist **improves** trade quality. Trades scoring >={best_t} had {high_wr:.1f}% WR vs {low_wr:.1f}% for lower scores (+{high_wr-low_wr:.1f}pp). "
    summary += f"Filtering would have saved ${sum(t['pnl_usd'] for t in low):+.2f} in losses from low-quality setups.\n"
elif high_wr > low_wr:
    summary += f"The checklist shows **marginal improvement**. Scores >={best_t} had {high_wr:.1f}% WR vs {low_wr:.1f}% (+{high_wr-low_wr:.1f}pp). "
    summary += f"Not strong enough to replace gut feel, but useful as a confirmation filter.\n"
else:
    summary += f"The checklist **does not improve** over Profi's gut feel in this sample. High-score WR ({high_wr:.1f}%) was not better than low-score ({low_wr:.1f}%). "
    summary += f"Profi's intuition may capture factors not in this checklist.\n"

md_path = OUT_DIR / "summary.md"
with open(md_path, 'w') as f:
    f.write(summary)
print(f"\nWrote {md_path}")
print("\nDone.")
