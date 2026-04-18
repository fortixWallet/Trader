#!/usr/bin/env python3
"""
Deep validation of 5-factor checklist on ALL available trade data.
Factors: MACRO, EMA_4H, COIN_WR, ENTRY_DIST, RSI_OK
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from itertools import combinations
import warnings
warnings.filterwarnings('ignore')

DB = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/market.db"
OUT = "/Users/williamstorm/Documents/Trading (OKX) 1h/data/crypto/checklist_deep"

conn = sqlite3.connect(DB)

# ── Load all trades ──────────────────────────────────────────────────
fortix = pd.read_sql("""
    SELECT id, coin, direction, entry_price, fill_price, sl_price, tp_price,
           pnl_usd, exit_reason, regime, filled_at, closed_at,
           fill_ob_imbalance, fill_momentum_15m, fill_funding_rate, fill_atr_1h
    FROM fortix_trades WHERE status='CLOSED' AND pnl_usd IS NOT NULL
""", conn)
fortix['source'] = 'fortix'
fortix['entry_time'] = pd.to_datetime(fortix['filled_at'])
fortix['ts'] = fortix['entry_time'].apply(lambda x: int(x.timestamp()) if pd.notna(x) else None)

okx = pd.read_sql("""
    SELECT id, coin, direction, entry_price, exit_price, pnl_usd, exit_reason,
           entry_time as entry_time_str, exit_time, regime, funding_rate
    FROM okx_trades WHERE pnl_usd IS NOT NULL
""", conn)
okx['source'] = 'okx'
okx['fill_price'] = okx['entry_price']
okx['entry_time'] = pd.to_datetime(okx['entry_time_str'])
okx['ts'] = okx['entry_time'].apply(lambda x: int(x.timestamp()) if pd.notna(x) else None)
okx['fill_ob_imbalance'] = None
okx['fill_funding_rate'] = okx['funding_rate']

# Combine
trades = pd.concat([fortix, okx], ignore_index=True)
trades['win'] = (trades['pnl_usd'] > 0).astype(int)
trades = trades.sort_values('entry_time').reset_index(drop=True)
print(f"Total trades: {len(trades)} (fortix: {len(fortix)}, okx: {len(okx)})")

# ── Preload price data ──────────────────────────────────────────────
prices_4h = pd.read_sql("SELECT coin, timestamp, close FROM prices WHERE timeframe='4h' ORDER BY coin, timestamp", conn)
prices_1h = pd.read_sql("SELECT coin, timestamp, close FROM prices WHERE timeframe='1h' ORDER BY coin, timestamp", conn)
prices_4h_btc = prices_4h[prices_4h.coin == 'BTC'].set_index('timestamp')['close']

# Build per-coin price series
coin_4h = {}
for c, g in prices_4h.groupby('coin'):
    coin_4h[c] = g.set_index('timestamp')['close']

coin_1h = {}
for c, g in prices_1h.groupby('coin'):
    coin_1h[c] = g.set_index('timestamp')['close']


def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── Precompute EMAs and RSI ─────────────────────────────────────────
btc_4h_ema8 = ema(prices_4h_btc, 8)
btc_4h_ema21 = ema(prices_4h_btc, 21)
# BTC 7d trend: compare current price to price 42 4h candles ago (7 days)
btc_4h_7d_ago = prices_4h_btc.shift(42)

coin_4h_ema8 = {}
coin_4h_ema21 = {}
for c in coin_4h:
    coin_4h_ema8[c] = ema(coin_4h[c], 8)
    coin_4h_ema21[c] = ema(coin_4h[c], 21)

coin_1h_rsi = {}
for c in coin_1h:
    coin_1h_rsi[c] = rsi(coin_1h[c])


def find_nearest_ts(index, target_ts, max_gap=14400):
    """Find nearest timestamp <= target_ts in a sorted index."""
    pos = index.searchsorted(target_ts, side='right') - 1
    if pos < 0:
        return None
    ts = index[pos]
    if abs(ts - target_ts) > max_gap:
        return None
    return ts


# ── Compute factors for each trade ──────────────────────────────────
results = []
# Track per-coin historical WR (only past trades)
coin_history = {}  # coin -> list of (entry_time, win)

for idx, t in trades.iterrows():
    ts = t['ts']
    coin = t['coin']
    direction = t['direction'].upper() if isinstance(t['direction'], str) else ''
    is_long = direction == 'LONG'

    factors = {}

    # 1. MACRO: BTC 7d trend matches direction
    btc_ts = find_nearest_ts(prices_4h_btc.index, ts, max_gap=14400*2)
    if btc_ts is not None and btc_ts in btc_4h_7d_ago.index:
        btc_now = prices_4h_btc[btc_ts]
        btc_7d = btc_4h_7d_ago[btc_ts]
        if pd.notna(btc_7d):
            btc_trend_up = btc_now > btc_7d
            factors['macro'] = int((is_long and btc_trend_up) or (not is_long and not btc_trend_up))
        else:
            factors['macro'] = np.nan
    else:
        factors['macro'] = np.nan

    # 2. EMA_4H: Coin EMA8 > EMA21 matches direction
    if coin in coin_4h_ema8:
        cts = find_nearest_ts(coin_4h_ema8[coin].index, ts, max_gap=14400*2)
        if cts is not None:
            e8 = coin_4h_ema8[coin][cts]
            e21 = coin_4h_ema21[coin][cts]
            if pd.notna(e8) and pd.notna(e21):
                ema_bullish = e8 > e21
                factors['ema_4h'] = int((is_long and ema_bullish) or (not is_long and not ema_bullish))
            else:
                factors['ema_4h'] = np.nan
        else:
            factors['ema_4h'] = np.nan
    else:
        factors['ema_4h'] = np.nan

    # 3. COIN_WR: Historical WR > 50% (only past trades)
    if coin in coin_history and len(coin_history[coin]) >= 3:
        past_wins = [w for _, w in coin_history[coin]]
        factors['coin_wr'] = int(sum(past_wins) / len(past_wins) > 0.5)
    else:
        factors['coin_wr'] = np.nan  # Not enough history

    # Update coin history AFTER computing factor
    if coin not in coin_history:
        coin_history[coin] = []
    coin_history[coin].append((t['entry_time'], t['win']))

    # 4. ENTRY_DIST: |fill_price - entry_price| / entry_price < 0.5%
    fp = t['fill_price']
    ep = t['entry_price']
    if pd.notna(fp) and pd.notna(ep) and ep > 0:
        dist = abs(fp - ep) / ep
        factors['entry_dist'] = int(dist < 0.005)
    else:
        factors['entry_dist'] = np.nan

    # 5. RSI_OK: RSI(14) not extreme
    if coin in coin_1h_rsi:
        rts = find_nearest_ts(coin_1h_rsi[coin].index, ts, max_gap=3600*2)
        if rts is not None:
            rsi_val = coin_1h_rsi[coin][rts]
            if pd.notna(rsi_val):
                if is_long:
                    factors['rsi_ok'] = int(rsi_val < 75)
                else:
                    factors['rsi_ok'] = int(rsi_val > 25)
            else:
                factors['rsi_ok'] = np.nan
        else:
            factors['rsi_ok'] = np.nan
    else:
        factors['rsi_ok'] = np.nan

    # OB and funding (for inverted test)
    # OB confirms direction
    ob = t.get('fill_ob_imbalance')
    if pd.notna(ob):
        factors['ob_confirms'] = int((is_long and ob > 0) or (not is_long and ob < 0))
    else:
        factors['ob_confirms'] = np.nan

    # Funding contrarian (for longs: negative funding = contrarian = good?)
    fr = t.get('fill_funding_rate')
    if pd.notna(fr):
        factors['funding_contrarian'] = int((is_long and fr < 0) or (not is_long and fr > 0))
    else:
        factors['funding_contrarian'] = np.nan

    # Score (5 main factors)
    main_factors = ['macro', 'ema_4h', 'coin_wr', 'entry_dist', 'rsi_ok']
    valid_factors = {k: v for k, v in factors.items() if k in main_factors and not (isinstance(v, float) and np.isnan(v))}
    score = sum(valid_factors.values()) if valid_factors else np.nan
    n_valid = len(valid_factors)

    results.append({
        'trade_idx': idx,
        'id': t['id'],
        'source': t['source'],
        'coin': coin,
        'direction': direction,
        'entry_time': t['entry_time'],
        'pnl_usd': t['pnl_usd'],
        'win': t['win'],
        **factors,
        'score': score,
        'n_valid_factors': n_valid,
    })

df = pd.DataFrame(results)
df.to_csv(f"{OUT}/full_results.csv", index=False)
print(f"\nScored {len(df)} trades, saved to full_results.csv")
print(f"Trades with all 5 factors: {(df.n_valid_factors == 5).sum()}")
print(f"Trades with >=4 factors: {(df.n_valid_factors >= 4).sum()}")
print(f"Trades with >=3 factors: {(df.n_valid_factors >= 3).sum()}")

# ── TASK 1: Score vs WR ─────────────────────────────────────────────
print("\n" + "="*60)
print("TASK 1: SCORE VS WIN RATE (full dataset)")
print("="*60)
# Use trades with at least 4 valid factors
dfv = df[df.n_valid_factors >= 4].copy()
print(f"Using {len(dfv)} trades with >=4 valid factors\n")

score_table = []
for s in sorted(dfv.score.dropna().unique()):
    sub = dfv[dfv.score == s]
    wr = sub.win.mean() * 100
    pnl = sub.pnl_usd.sum()
    score_table.append({'score': int(s), 'trades': len(sub), 'wins': sub.win.sum(),
                        'WR%': round(wr, 1), 'total_pnl': round(pnl, 2),
                        'avg_pnl': round(sub.pnl_usd.mean(), 2)})
    print(f"  Score {int(s)}: {len(sub):3d} trades, WR {wr:5.1f}%, PnL ${pnl:+.2f}, Avg ${sub.pnl_usd.mean():+.2f}")

# ── TASK 2: Threshold comparison ────────────────────────────────────
print("\n" + "="*60)
print("TASK 2: THRESHOLD COMPARISON")
print("="*60)

# Date range for trades/day
date_range = (dfv.entry_time.max() - dfv.entry_time.min()).total_seconds() / 86400
print(f"Date range: {date_range:.1f} days\n")

threshold_results = []
for thresh in [2, 3, 4, 5]:
    passing = dfv[dfv.score >= thresh]
    failing = dfv[dfv.score < thresh]
    n = len(passing)
    if n == 0:
        continue
    wr = passing.win.mean() * 100
    pnl = passing.pnl_usd.sum()
    avg_pnl = passing.pnl_usd.mean()
    std_pnl = passing.pnl_usd.std() if n > 1 else 1
    sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0
    tpd = n / max(date_range, 1)

    fail_wr = failing.win.mean() * 100 if len(failing) > 0 else 0
    fail_pnl = failing.pnl_usd.sum() if len(failing) > 0 else 0

    threshold_results.append({
        'threshold': f'>={thresh}',
        'pass_trades': n, 'pass_WR': round(wr, 1), 'pass_pnl': round(pnl, 2),
        'pass_avg_pnl': round(avg_pnl, 2), 'sharpe': round(sharpe, 3),
        'trades_per_day': round(tpd, 1),
        'fail_trades': len(failing), 'fail_WR': round(fail_wr, 1), 'fail_pnl': round(fail_pnl, 2),
        'wr_gap': round(wr - fail_wr, 1)
    })
    print(f"  >={thresh}: {n:3d} trades ({tpd:.1f}/day), WR {wr:.1f}%, PnL ${pnl:+.2f}, "
          f"Sharpe {sharpe:.3f} | Rejected: {len(failing)} trades, WR {fail_wr:.1f}%, PnL ${fail_pnl:+.2f} | Gap: {wr-fail_wr:+.1f}pp")

pd.DataFrame(threshold_results).to_csv(f"{OUT}/threshold_comparison.csv", index=False)

# ── TASK 3: Time-split robustness ───────────────────────────────────
print("\n" + "="*60)
print("TASK 3: TIME-SPLIT ROBUSTNESS")
print("="*60)

mid_time = dfv.entry_time.median()
first_half = dfv[dfv.entry_time <= mid_time]
second_half = dfv[dfv.entry_time > mid_time]
print(f"Split at: {mid_time}")
print(f"First half:  {len(first_half)} trades ({first_half.entry_time.min()} to {first_half.entry_time.max()})")
print(f"Second half: {len(second_half)} trades ({second_half.entry_time.min()} to {second_half.entry_time.max()})")

for thresh in [3, 4, 5]:
    h1_pass = first_half[first_half.score >= thresh]
    h1_fail = first_half[first_half.score < thresh]
    h2_pass = second_half[second_half.score >= thresh]
    h2_fail = second_half[second_half.score < thresh]

    h1_wr = h1_pass.win.mean()*100 if len(h1_pass) > 0 else 0
    h2_wr = h2_pass.win.mean()*100 if len(h2_pass) > 0 else 0
    h1_fail_wr = h1_fail.win.mean()*100 if len(h1_fail) > 0 else 0
    h2_fail_wr = h2_fail.win.mean()*100 if len(h2_fail) > 0 else 0

    print(f"\n  Threshold >={thresh}:")
    print(f"    H1: Pass {len(h1_pass)} ({h1_wr:.1f}%), Fail {len(h1_fail)} ({h1_fail_wr:.1f}%), Gap {h1_wr-h1_fail_wr:+.1f}pp")
    print(f"    H2: Pass {len(h2_pass)} ({h2_wr:.1f}%), Fail {len(h2_fail)} ({h2_fail_wr:.1f}%), Gap {h2_wr-h2_fail_wr:+.1f}pp")
    consistent = "CONSISTENT" if (h1_wr - h1_fail_wr > 0 and h2_wr - h2_fail_wr > 0) else "INCONSISTENT"
    print(f"    → {consistent}")

# ── TASK 4: Per-factor stability ────────────────────────────────────
print("\n" + "="*60)
print("TASK 4: PER-FACTOR STABILITY")
print("="*60)

main_factors = ['macro', 'ema_4h', 'coin_wr', 'entry_dist', 'rsi_ok']
factor_stability = []

for f in main_factors:
    sub = dfv[dfv[f].notna() & (dfv[f].isin([0, 1]))]
    yes = sub[sub[f] == 1]
    no = sub[sub[f] == 0]

    wr_yes = yes.win.mean()*100 if len(yes) > 0 else 0
    wr_no = no.win.mean()*100 if len(no) > 0 else 0
    gap = wr_yes - wr_no

    # Time split
    h1 = sub[sub.entry_time <= mid_time]
    h2 = sub[sub.entry_time > mid_time]

    h1_yes_wr = h1[h1[f]==1].win.mean()*100 if len(h1[h1[f]==1]) > 0 else 0
    h1_no_wr = h1[h1[f]==0].win.mean()*100 if len(h1[h1[f]==0]) > 0 else 0
    h2_yes_wr = h2[h2[f]==1].win.mean()*100 if len(h2[h2[f]==1]) > 0 else 0
    h2_no_wr = h2[h2[f]==0].win.mean()*100 if len(h2[h2[f]==0]) > 0 else 0

    h1_gap = h1_yes_wr - h1_no_wr
    h2_gap = h2_yes_wr - h2_no_wr
    stable = "STABLE" if (h1_gap > 0 and h2_gap > 0) else ("INVERTED" if (h1_gap < 0 and h2_gap < 0) else "UNSTABLE")

    factor_stability.append({
        'factor': f, 'yes_trades': len(yes), 'no_trades': len(no),
        'WR_yes': round(wr_yes,1), 'WR_no': round(wr_no,1), 'gap': round(gap,1),
        'H1_gap': round(h1_gap,1), 'H2_gap': round(h2_gap,1), 'stability': stable
    })

    print(f"\n  {f.upper()}:")
    print(f"    YES: {len(yes)} trades, WR {wr_yes:.1f}%  |  NO: {len(no)} trades, WR {wr_no:.1f}%  |  Gap: {gap:+.1f}pp")
    print(f"    H1 gap: {h1_gap:+.1f}pp  |  H2 gap: {h2_gap:+.1f}pp  |  {stable}")

# ── TASK 5: Inverted OB and Funding ─────────────────────────────────
print("\n" + "="*60)
print("TASK 5: INVERTED FACTORS (OB, Funding)")
print("="*60)

for f, fname in [('ob_confirms', 'OB Confirms Direction'), ('funding_contrarian', 'Funding Contrarian')]:
    sub = df[df[f].notna() & (df[f].isin([0, 1]))]
    if len(sub) == 0:
        print(f"\n  {fname}: NO DATA")
        continue
    yes = sub[sub[f] == 1]
    no = sub[sub[f] == 0]
    wr_yes = yes.win.mean()*100 if len(yes) > 0 else 0
    wr_no = no.win.mean()*100 if len(no) > 0 else 0

    print(f"\n  {fname}:")
    print(f"    YES (confirms): {len(yes)} trades, WR {wr_yes:.1f}%, PnL ${yes.pnl_usd.sum():+.2f}")
    print(f"    NO (against):   {len(no)} trades, WR {wr_no:.1f}%, PnL ${no.pnl_usd.sum():+.2f}")
    print(f"    Gap: {wr_yes - wr_no:+.1f}pp")
    if wr_yes < wr_no:
        print(f"    → INVERTED: when {fname.lower()} → WORSE performance")
        print(f"    → Could use as NEGATIVE factor (-1 when YES)")
    else:
        print(f"    → NORMAL: when {fname.lower()} → BETTER performance")

# ── TASK 6: Optimal factor subset ───────────────────────────────────
print("\n" + "="*60)
print("TASK 6: OPTIMAL FACTOR COMBINATION")
print("="*60)

# Also include inverted OB and funding as potential factors
all_possible = main_factors + ['ob_inv', 'funding_inv']

# Create inverted columns
df['ob_inv'] = df['ob_confirms'].apply(lambda x: 1 - x if pd.notna(x) and x in [0,1] else np.nan)
df['funding_inv'] = df['funding_contrarian'].apply(lambda x: 1 - x if pd.notna(x) and x in [0,1] else np.nan)

best_combos = []

for n_factors in [3, 4, 5, 6, 7]:
    for combo in combinations(all_possible, n_factors):
        combo = list(combo)
        # Skip if both normal and inverted of same factor
        if 'ob_confirms' in combo and 'ob_inv' in combo:
            continue
        if 'funding_contrarian' in combo and 'funding_inv' in combo:
            continue

        sub = df.copy()
        valid_mask = True
        for f in combo:
            valid_mask = valid_mask & sub[f].notna()
        sub = sub[valid_mask]

        if len(sub) < 20:
            continue

        sub['combo_score'] = sum(sub[f] for f in combo)
        max_score = n_factors

        # Try threshold = ceil(n_factors/2) + 1 (majority+)
        for thresh_pct in [0.6, 0.7, 0.8]:
            thresh = int(np.ceil(n_factors * thresh_pct))
            passing = sub[sub.combo_score >= thresh]
            failing = sub[sub.combo_score < thresh]

            if len(passing) < 15:
                continue

            wr = passing.win.mean() * 100
            pnl = passing.pnl_usd.sum()
            avg_pnl = passing.pnl_usd.mean()
            std_pnl = passing.pnl_usd.std()
            sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0
            fail_wr = failing.win.mean()*100 if len(failing) > 0 else 0

            best_combos.append({
                'factors': '+'.join(combo),
                'n_factors': n_factors,
                'threshold': thresh,
                'n_pass': len(passing),
                'WR_pass': round(wr, 1),
                'WR_fail': round(fail_wr, 1),
                'gap': round(wr - fail_wr, 1),
                'pnl': round(pnl, 2),
                'sharpe': round(sharpe, 3),
            })

combos_df = pd.DataFrame(best_combos)
if len(combos_df) > 0:
    # Sort by Sharpe, show top combos
    combos_df = combos_df.sort_values('sharpe', ascending=False)
    print("\nTop 15 combos by Sharpe (min 15 passing trades):")
    for _, row in combos_df.head(15).iterrows():
        print(f"  {row['factors']} (>={row['threshold']}): "
              f"{row['n_pass']} trades, WR {row['WR_pass']}% vs {row['WR_fail']}% (gap {row['gap']:+.1f}pp), "
              f"PnL ${row['pnl']:+.2f}, Sharpe {row['sharpe']:.3f}")

    # Also show top by WR gap
    combos_by_gap = combos_df.sort_values('gap', ascending=False)
    print("\nTop 10 combos by WR gap:")
    for _, row in combos_by_gap.head(10).iterrows():
        print(f"  {row['factors']} (>={row['threshold']}): "
              f"{row['n_pass']} trades, WR {row['WR_pass']}% vs {row['WR_fail']}% (gap {row['gap']:+.1f}pp), "
              f"Sharpe {row['sharpe']:.3f}")

# ── Generate summary.md ─────────────────────────────────────────────
print("\n\nGenerating summary.md...")

with open(f"{OUT}/summary.md", 'w') as f:
    f.write("# Checklist Deep Validation Results\n\n")
    f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    f.write(f"**Total trades**: {len(trades)} (fortix: {len(fortix)}, okx: {len(okx)})\n")
    f.write(f"**Trades with >=4 valid factors**: {len(dfv)}\n")
    f.write(f"**Date range**: {trades.entry_time.min()} to {trades.entry_time.max()}\n\n")

    f.write("## 1. Score vs Win Rate\n\n")
    f.write("| Score | Trades | WR% | Total PnL | Avg PnL |\n")
    f.write("|-------|--------|-----|-----------|--------|\n")
    for row in score_table:
        f.write(f"| {row['score']} | {row['trades']} | {row['WR%']}% | ${row['total_pnl']:+.2f} | ${row['avg_pnl']:+.2f} |\n")

    f.write("\n## 2. Optimal Threshold\n\n")
    f.write("| Threshold | Pass | WR% | PnL | Sharpe | /day | Fail WR% | Gap |\n")
    f.write("|-----------|------|-----|-----|--------|------|----------|-----|\n")
    for row in threshold_results:
        f.write(f"| {row['threshold']} | {row['pass_trades']} | {row['pass_WR']}% | ${row['pass_pnl']:+.2f} | {row['sharpe']} | {row['trades_per_day']} | {row['fail_WR']}% | {row['wr_gap']:+.1f}pp |\n")

    f.write("\n## 3. Time-Split Robustness\n\n")
    f.write(f"Split point: {mid_time}\n\n")
    for thresh in [3, 4, 5]:
        h1_pass = first_half[first_half.score >= thresh]
        h1_fail = first_half[first_half.score < thresh]
        h2_pass = second_half[second_half.score >= thresh]
        h2_fail = second_half[second_half.score < thresh]
        h1_wr = h1_pass.win.mean()*100 if len(h1_pass) > 0 else 0
        h2_wr = h2_pass.win.mean()*100 if len(h2_pass) > 0 else 0
        h1_fail_wr = h1_fail.win.mean()*100 if len(h1_fail) > 0 else 0
        h2_fail_wr = h2_fail.win.mean()*100 if len(h2_fail) > 0 else 0
        consistent = "CONSISTENT" if ((h1_wr-h1_fail_wr > 0) and (h2_wr-h2_fail_wr > 0)) else "INCONSISTENT"
        f.write(f"**Threshold >={thresh}**: H1 gap {h1_wr-h1_fail_wr:+.1f}pp, H2 gap {h2_wr-h2_fail_wr:+.1f}pp → {consistent}\n\n")

    f.write("## 4. Per-Factor Stability\n\n")
    f.write("| Factor | YES WR | NO WR | Gap | H1 Gap | H2 Gap | Stability |\n")
    f.write("|--------|--------|-------|-----|--------|--------|----------|\n")
    for row in factor_stability:
        f.write(f"| {row['factor']} | {row['WR_yes']}% | {row['WR_no']}% | {row['gap']:+.1f}pp | {row['H1_gap']:+.1f}pp | {row['H2_gap']:+.1f}pp | {row['stability']} |\n")

    f.write("\n## 5. Inverted Factors\n\n")
    for fac, fname in [('ob_confirms', 'OB'), ('funding_contrarian', 'Funding')]:
        sub = df[df[fac].notna() & (df[fac].isin([0, 1]))]
        if len(sub) == 0:
            f.write(f"**{fname}**: No data available\n\n")
            continue
        yes = sub[sub[fac] == 1]
        no = sub[sub[fac] == 0]
        wr_yes = yes.win.mean()*100 if len(yes) > 0 else 0
        wr_no = no.win.mean()*100 if len(no) > 0 else 0
        inverted = "INVERTED" if wr_yes < wr_no else "NORMAL"
        f.write(f"**{fname}**: YES WR {wr_yes:.1f}%, NO WR {wr_no:.1f}%, Gap {wr_yes-wr_no:+.1f}pp → {inverted}\n\n")

    f.write("## 6. Best Factor Combinations\n\n")
    if len(combos_df) > 0:
        top5 = combos_df.head(5)
        f.write("| Factors | Threshold | Trades | WR% | Gap | Sharpe |\n")
        f.write("|---------|-----------|--------|-----|-----|--------|\n")
        for _, row in top5.iterrows():
            f.write(f"| {row['factors']} | >={row['threshold']} | {row['n_pass']} | {row['WR_pass']}% | {row['gap']:+.1f}pp | {row['sharpe']} |\n")

    f.write("\n## 7. Final Recommendation\n\n")

    # Determine best threshold
    if len(threshold_results) > 0:
        best_thresh = max(threshold_results, key=lambda x: x['sharpe'])
        f.write(f"**Best threshold**: {best_thresh['threshold']} (Sharpe {best_thresh['sharpe']})\n\n")
        f.write(f"- Passes {best_thresh['pass_trades']} trades at {best_thresh['pass_WR']}% WR\n")
        f.write(f"- Rejected trades: {best_thresh['fail_trades']} at {best_thresh['fail_WR']}% WR\n")
        f.write(f"- WR gap: {best_thresh['wr_gap']:+.1f}pp\n")
        f.write(f"- {best_thresh['trades_per_day']} trades/day\n\n")

    # Stable factors
    stable_factors = [r for r in factor_stability if r['stability'] == 'STABLE']
    if stable_factors:
        f.write(f"**Stable factors**: {', '.join(r['factor'] for r in stable_factors)}\n\n")
    unstable_factors = [r for r in factor_stability if r['stability'] != 'STABLE']
    if unstable_factors:
        f.write(f"**Unstable/inverted factors**: {', '.join(r['factor'] for r in unstable_factors)}\n\n")

    if len(combos_df) > 0:
        best = combos_df.iloc[0]
        f.write(f"**Best combo**: {best['factors']} (>={best['threshold']}): {best['n_pass']} trades, "
                f"WR {best['WR_pass']}%, Sharpe {best['sharpe']}\n")

print("Done! Files saved to", OUT)
conn.close()
