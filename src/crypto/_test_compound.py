"""Test compound signals with walk-forward validation."""
import sys; sys.stdout.reconfigure(encoding='utf-8')
import numpy as np, pandas as pd, sqlite3

conn = sqlite3.connect('data/crypto/market.db')

prices = pd.read_sql_query("SELECT coin, date(timestamp, 'unixepoch') as date, close FROM prices WHERE timeframe='1d' ORDER BY coin, timestamp", conn)
prices['date'] = pd.to_datetime(prices['date'])

ls = pd.read_sql_query("SELECT coin, date(timestamp, 'unixepoch') as date, long_ratio FROM cg_ls_history ORDER BY coin, timestamp", conn)
ls['date'] = pd.to_datetime(ls['date'])
ls_d = ls.groupby(['coin','date']).agg(ls_long=('long_ratio','last')).reset_index()

taker = pd.read_sql_query("SELECT coin, date(timestamp, 'unixepoch') as date, buy_sell_ratio FROM cg_taker_history ORDER BY coin, timestamp", conn)
taker['date'] = pd.to_datetime(taker['date'])
taker_d = taker.groupby(['coin','date']).agg(taker_r=('buy_sell_ratio','last')).reset_index()

oi_h = pd.read_sql_query("SELECT coin, date(timestamp, 'unixepoch') as date, oi_close FROM cg_oi_history ORDER BY coin, timestamp", conn)
oi_h['date'] = pd.to_datetime(oi_h['date'])
oi_d = oi_h.groupby(['coin','date']).agg(oi=('oi_close','last')).reset_index()

exf = pd.read_sql_query("SELECT date, coin, netflow FROM cq_exchange_flows ORDER BY date", conn)
exf['date'] = pd.to_datetime(exf['date'])

etf = pd.read_sql_query("SELECT date, flow_usd FROM cg_etf_flows WHERE asset='BTC' ORDER BY date", conn)
etf['date'] = pd.to_datetime(etf['date'])

cbp = pd.read_sql_query("SELECT date, premium_index FROM cq_coinbase_premium ORDER BY date", conn)
cbp['date'] = pd.to_datetime(cbp['date'])

fg = pd.read_sql_query("SELECT date, value as fg FROM fear_greed ORDER BY date", conn)
fg['date'] = pd.to_datetime(fg['date'])
conn.close()

all_rows = []
for coin in prices['coin'].unique():
    cp = prices[prices['coin']==coin].sort_values('date')
    if len(cp) < 50: continue
    df = cp[['date','close']].copy()
    df['ret14'] = df['close'].shift(-14) / df['close'] - 1

    # BB
    ma20 = df['close'].rolling(20).mean()
    std20 = df['close'].rolling(20).std()
    bb_range = (4*std20).replace(0, 0.001)
    df['bb'] = (df['close'] - (ma20 - 2*std20)) / bb_range

    df = df.merge(ls_d[ls_d['coin']==coin][['date','ls_long']], on='date', how='left')
    df = df.merge(taker_d[taker_d['coin']==coin][['date','taker_r']], on='date', how='left')

    oi_c = oi_d[oi_d['coin']==coin].sort_values('date').copy()
    oi_c['oi_chg7'] = oi_c['oi'].pct_change(7)
    df = df.merge(oi_c[['date','oi_chg7']], on='date', how='left')

    ef = exf[exf['coin']==coin][['date','netflow']].sort_values('date').copy()
    if len(ef) > 7:
        ef['flow_7d'] = ef['netflow'].rolling(7).sum()
        df = df.merge(ef[['date','flow_7d']], on='date', how='left')
    else:
        df['flow_7d'] = np.nan

    df = df.merge(etf[['date','flow_usd']].rename(columns={'flow_usd':'etf_flow'}), on='date', how='left')
    df = df.merge(cbp[['date','premium_index']].rename(columns={'premium_index':'cb_prem'}), on='date', how='left')
    df = df.merge(fg[['date','fg']], on='date', how='left')

    # Bearish signals
    df['sig_ls'] = (df['ls_long'] > 65).astype(int)
    df['sig_taker'] = (df['taker_r'] < 0.9).astype(int)
    df['sig_bb_hi'] = (df['bb'] > 0.95).astype(int)
    df['sig_oi'] = (df['oi_chg7'] > 0.20).astype(int)
    df['n_bear'] = df[['sig_ls','sig_taker','sig_bb_hi','sig_oi']].sum(axis=1)

    # Bullish signals
    df['sig_etf'] = (df['etf_flow'] > 0).astype(int)
    df['sig_cb'] = (df['cb_prem'] > 0).astype(int)
    df['sig_outflow'] = (df['flow_7d'] < 0).astype(int)
    df['sig_fg_lo'] = (df['fg'] < 30).astype(int)
    df['sig_bb_lo'] = (df['bb'] < 0.05).astype(int)
    df['n_bull'] = df[['sig_etf','sig_cb','sig_outflow','sig_fg_lo','sig_bb_lo']].sum(axis=1)

    df['coin'] = coin
    all_rows.append(df)

full = pd.concat(all_rows)
split = pd.to_datetime('2024-06-01')

print("=" * 70)
print("COMPOUND BEARISH (N simultaneous bearish signals per coin)")
print("=" * 70)
for n in [1, 2, 3, 4]:
    sub = full[full['n_bear'] >= n].dropna(subset=['ret14'])
    tr = sub[sub['date'] < split]
    te = sub[sub['date'] >= split]
    if len(te) < 20: continue
    print(f"  {n}+ bearish: Train {(tr['ret14']<0).mean()*100:.1f}% DN (N={len(tr)}) | Test {(te['ret14']<0).mean()*100:.1f}% DN (N={len(te)}) avg={te['ret14'].mean()*100:+.2f}%")

print("\n" + "=" * 70)
print("COMPOUND BULLISH (N simultaneous bullish signals per coin)")
print("=" * 70)
for n in [1, 2, 3, 4]:
    sub = full[full['n_bull'] >= n].dropna(subset=['ret14'])
    tr = sub[sub['date'] < split]
    te = sub[sub['date'] >= split]
    if len(te) < 20: continue
    print(f"  {n}+ bullish: Train {(tr['ret14']>0).mean()*100:.1f}% UP (N={len(tr)}) | Test {(te['ret14']>0).mean()*100:.1f}% UP (N={len(te)}) avg={te['ret14'].mean()*100:+.2f}%")

print("\n" + "=" * 70)
print("NET SIGNAL (bullish - bearish)")
print("=" * 70)
full['net'] = full['n_bull'] - full['n_bear']
for net in [-3, -2, -1, 0, 1, 2, 3]:
    sub = full[full['net'] == net].dropna(subset=['ret14'])
    te = sub[sub['date'] >= split]
    if len(te) < 30: continue
    te_up = (te['ret14']>0).mean()*100
    te_avg = te['ret14'].mean()*100
    tag = " <<<BEARISH" if net <= -2 else " <<<BULLISH" if net >= 2 else ""
    print(f"  Net={net:+d}: {te_up:.1f}% UP, avg={te_avg:+.2f}% (N={len(te)}){tag}")

print("\n" + "=" * 70)
print("STRONGEST: 3+ bearish AND 0 bullish vs 3+ bullish AND 0 bearish")
print("=" * 70)
pure_bear = full[(full['n_bear'] >= 3) & (full['n_bull'] == 0)].dropna(subset=['ret14'])
pure_bull = full[(full['n_bull'] >= 3) & (full['n_bear'] == 0)].dropna(subset=['ret14'])

te_bear = pure_bear[pure_bear['date'] >= split]
te_bull = pure_bull[pure_bull['date'] >= split]

if len(te_bear) > 5:
    print(f"  PURE BEARISH (3+ bear, 0 bull): {(te_bear['ret14']<0).mean()*100:.1f}% DOWN, avg={te_bear['ret14'].mean()*100:+.2f}% (N={len(te_bear)})")
if len(te_bull) > 5:
    print(f"  PURE BULLISH (3+ bull, 0 bear): {(te_bull['ret14']>0).mean()*100:.1f}% UP, avg={te_bull['ret14'].mean()*100:+.2f}% (N={len(te_bull)})")
