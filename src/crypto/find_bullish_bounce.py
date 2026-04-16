"""Find optimal bullish bounce conditions from 1000 days of historical data."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import sqlite3
import numpy as np
from datetime import datetime, timezone, timedelta

conn = sqlite3.connect('data/crypto/market.db', timeout=30)

COINS = ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOT','LINK',
         'DOGE','SHIB','PEPE','WIF','BONK','UNI','AAVE','PENDLE','LDO','CRV',
         'FET','RENDER','TAO','ARB','OP','POL']

print('Building feature matrix...')
rows_data = []

for coin in COINS:
    prices = conn.execute(
        'SELECT timestamp, close, volume FROM prices WHERE coin=? AND timeframe="1d" ORDER BY timestamp ASC',
        (coin,)
    ).fetchall()
    if len(prices) < 60:
        continue

    for i in range(30, len(prices) - 14):
        ts = prices[i][0]
        current = prices[i][1]
        future_14d = prices[i+14][1]
        ret_14d_forward = (future_14d / current - 1)
        bounced = 1 if ret_14d_forward > 0.02 else 0

        closes = [prices[j][1] for j in range(i, max(i-31, -1), -1)]
        volumes = [prices[j][2] for j in range(i, max(i-31, -1), -1)]
        if len(closes) < 30:
            continue

        ret_7d = (closes[0] / closes[7] - 1) if len(closes) > 7 else 0
        ret_14d = (closes[0] / closes[14] - 1) if len(closes) > 14 else 0
        ret_30d = (closes[0] / closes[30] - 1) if len(closes) > 30 else 0

        daily_rets = [(closes[j] / closes[j+1] - 1) for j in range(min(14, len(closes)-1))]
        vol_7d = np.std(daily_rets[:7]) if len(daily_rets) >= 7 else 0

        deltas = [closes[j] - closes[j+1] for j in range(min(14, len(closes)-1))]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_gain = np.mean(gains) if gains else 0.001
        avg_loss = np.mean(losses) if losses else 0.001
        rsi = 100 - 100 / (1 + (avg_gain / avg_loss if avg_loss > 0 else 100))

        ma20 = np.mean(closes[:20])
        std20 = np.std(closes[:20])
        bb_low = ma20 - 2*std20
        bb_range = 4*std20
        bb_pos = (closes[0] - bb_low) / bb_range if bb_range > 0 else 0.5

        vol_avg = np.mean(volumes[:7]) if len(volumes) >= 7 else 1
        vol_ratio = volumes[0] / vol_avg if vol_avg > 0 else 1

        day_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

        funding = None
        try:
            fr = conn.execute('SELECT rate FROM funding_rates WHERE coin=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1', (coin, ts)).fetchone()
            if fr: funding = fr[0]
        except: pass

        ls_long = None
        try:
            ls = conn.execute('SELECT long_ratio FROM cg_ls_history WHERE coin=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1', (coin, ts)).fetchone()
            if ls: ls_long = ls[0]
        except: pass

        taker = None
        try:
            tk = conn.execute('SELECT buy_sell_ratio FROM cg_taker_history WHERE coin=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1', (coin, ts)).fetchone()
            if tk: taker = tk[0]
        except: pass

        fg = None
        try:
            fg_row = conn.execute('SELECT value FROM fear_greed WHERE date<=? ORDER BY date DESC LIMIT 1', (day_str,)).fetchone()
            if fg_row: fg = fg_row[0]
        except: pass

        rows_data.append({
            'coin': coin, 'ts': ts, 'bounced': bounced, 'ret_fwd': ret_14d_forward,
            'rsi': rsi, 'bb_pos': bb_pos, 'ret_7d': ret_7d, 'ret_14d': ret_14d,
            'ret_30d': ret_30d, 'vol_7d': vol_7d, 'vol_ratio': vol_ratio,
            'funding': funding, 'ls_long': ls_long, 'taker': taker, 'fg': fg,
        })

conn.close()
print(f'Built {len(rows_data)} coin-day observations')

baseline = sum(r['bounced'] for r in rows_data) / len(rows_data)
print(f'Baseline bounce rate (>2% in 14d): {baseline*100:.1f}%')
print()

conditions = [
    # Single conditions
    ('RSI < 25', lambda r: r['rsi'] < 25),
    ('RSI < 20', lambda r: r['rsi'] < 20),
    ('RSI < 30 + vol spike 2x', lambda r: r['rsi'] < 30 and r['vol_ratio'] > 2),
    ('RSI < 25 + vol spike 3x', lambda r: r['rsi'] < 25 and r['vol_ratio'] > 3),
    ('BB < 0.05', lambda r: r['bb_pos'] < 0.05),
    ('BB < 0.02', lambda r: r['bb_pos'] < 0.02),
    ('BB < 0 (below lower band)', lambda r: r['bb_pos'] < 0),
    ('Funding < -0.01%', lambda r: r['funding'] is not None and r['funding'] < -0.0001),
    ('Funding < -0.03%', lambda r: r['funding'] is not None and r['funding'] < -0.0003),
    ('Funding < -0.05%', lambda r: r['funding'] is not None and r['funding'] < -0.0005),
    ('L/S < 40% long', lambda r: r['ls_long'] is not None and r['ls_long'] < 40),
    ('L/S < 35% long', lambda r: r['ls_long'] is not None and r['ls_long'] < 35),
    ('Taker > 1.1 (buyers)', lambda r: r['taker'] is not None and r['taker'] > 1.1),
    ('Taker > 1.2 (strong buy)', lambda r: r['taker'] is not None and r['taker'] > 1.2),
    ('F&G < 15', lambda r: r['fg'] is not None and r['fg'] < 15),
    ('F&G < 10', lambda r: r['fg'] is not None and r['fg'] < 10),
    ('14d drop > 20%', lambda r: r['ret_14d'] < -0.20),
    ('14d drop > 30%', lambda r: r['ret_14d'] < -0.30),
    ('30d drop > 30%', lambda r: r['ret_30d'] < -0.30),
    ('30d drop > 40%', lambda r: r['ret_30d'] < -0.40),
    # Combos - 2 conditions
    ('RSI<30 + funding neg', lambda r: r['rsi'] < 30 and r['funding'] is not None and r['funding'] < -0.0001),
    ('RSI<25 + F&G<25', lambda r: r['rsi'] < 25 and r['fg'] is not None and r['fg'] < 25),
    ('RSI<30 + taker>1.1', lambda r: r['rsi'] < 30 and r['taker'] is not None and r['taker'] > 1.1),
    ('BB<0.05 + funding neg', lambda r: r['bb_pos'] < 0.05 and r['funding'] is not None and r['funding'] < -0.0001),
    ('BB<0.05 + taker>1.1', lambda r: r['bb_pos'] < 0.05 and r['taker'] is not None and r['taker'] > 1.1),
    ('14d drop>20% + funding neg', lambda r: r['ret_14d'] < -0.20 and r['funding'] is not None and r['funding'] < -0.0001),
    ('14d drop>20% + taker>1.1', lambda r: r['ret_14d'] < -0.20 and r['taker'] is not None and r['taker'] > 1.1),
    ('14d drop>20% + L/S<45%', lambda r: r['ret_14d'] < -0.20 and r['ls_long'] is not None and r['ls_long'] < 45),
    ('30d drop>30% + taker>1.1', lambda r: r['ret_30d'] < -0.30 and r['taker'] is not None and r['taker'] > 1.1),
    ('30d drop>30% + funding neg', lambda r: r['ret_30d'] < -0.30 and r['funding'] is not None and r['funding'] < -0.0001),
    ('L/S<40% + taker>1.1', lambda r: r['ls_long'] is not None and r['ls_long'] < 40 and r['taker'] is not None and r['taker'] > 1.1),
    ('14d drop>20% + vol 2x + RSI<30', lambda r: r['ret_14d'] < -0.20 and r['vol_ratio'] > 2 and r['rsi'] < 30),
    # Combos - 3 conditions
    ('RSI<25 + funding neg + F&G<30', lambda r: r['rsi'] < 25 and r['funding'] is not None and r['funding'] < -0.0001 and r['fg'] is not None and r['fg'] < 30),
    ('BB<0.05 + funding neg + taker>1.05', lambda r: r['bb_pos'] < 0.05 and r['funding'] is not None and r['funding'] < -0.0001 and r['taker'] is not None and r['taker'] > 1.05),
    ('14d drop>20% + RSI<30 + funding neg', lambda r: r['ret_14d'] < -0.20 and r['rsi'] < 30 and r['funding'] is not None and r['funding'] < -0.0001),
    ('14d drop>20% + taker>1.1 + F&G<30', lambda r: r['ret_14d'] < -0.20 and r['taker'] is not None and r['taker'] > 1.1 and r['fg'] is not None and r['fg'] < 30),
    ('L/S<40% + funding neg + RSI<35', lambda r: r['ls_long'] is not None and r['ls_long'] < 40 and r['funding'] is not None and r['funding'] < -0.0001 and r['rsi'] < 35),
    ('30d drop>30% + RSI<30 + taker>1.05', lambda r: r['ret_30d'] < -0.30 and r['rsi'] < 30 and r['taker'] is not None and r['taker'] > 1.05),
    ('BB<0 + funding neg', lambda r: r['bb_pos'] < 0 and r['funding'] is not None and r['funding'] < -0.0001),
    ('BB<0 + taker>1.1', lambda r: r['bb_pos'] < 0 and r['taker'] is not None and r['taker'] > 1.1),
    ('RSI<20 + taker>1.1', lambda r: r['rsi'] < 20 and r['taker'] is not None and r['taker'] > 1.1),
    ('RSI<20 + funding neg', lambda r: r['rsi'] < 20 and r['funding'] is not None and r['funding'] < -0.0001),
]

print(f'{"Condition":55s} {"Bounce%":>8s} {"N":>6s} {"AvgRet":>8s} {"Edge":>6s}')
print('-' * 90)

winners = []
for name, fn in conditions:
    matching = [r for r in rows_data if fn(r)]
    if len(matching) < 30:
        continue
    bounce_rate = sum(r['bounced'] for r in matching) / len(matching)
    avg_ret = np.mean([r['ret_fwd'] for r in matching])
    edge = bounce_rate - baseline
    marker = ' ***' if bounce_rate > 0.55 and len(matching) >= 50 else ''
    print(f'{name:55s} {bounce_rate*100:7.1f}% {len(matching):6d} {avg_ret*100:+7.1f}% {edge*100:+5.1f}%{marker}')
    if bounce_rate > 0.55 and len(matching) >= 30:
        winners.append((name, bounce_rate, len(matching), avg_ret))

print()
if winners:
    print('=== WINNERS (>55% bounce, N>=30) ===')
    for name, rate, n, ret in sorted(winners, key=lambda x: x[1], reverse=True):
        print(f'  {name}: {rate*100:.1f}% (N={n}, avg ret {ret*100:+.1f}%)')
else:
    print('No winners found')
