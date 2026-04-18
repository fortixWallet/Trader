#!/usr/bin/env python3
"""
Hybrid 1D Direction + 4H Entry Backtest System
Tests on ALL available 4H data (~850 days).
"""

import sqlite3
import json
import csv
import math
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

# ── Paths ──
BASE = Path("/Users/williamstorm/Documents/Trading (OKX) 1h")
DB_PATH = BASE / "data/crypto/market.db"
CFG_PATH = BASE / "data/crypto/coin_optimization/optimal_config.json"
OUT_DIR = BASE / "data/crypto/hybrid_test"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ──
with open(CFG_PATH) as f:
    COIN_CFG = json.load(f)

BAD_COINS = {'LTC', 'TRX', 'JTO'}
ACTIVE_COINS = sorted(set(COIN_CFG.keys()) - BAD_COINS)
print(f"Active coins ({len(ACTIVE_COINS)}): {ACTIVE_COINS}")

FEE_RATE = 0.0004  # 0.04% per side
LEVERAGE = 8
START_EQUITY = 5000.0

# ── Load Data ──
print("Loading data from DB...")
conn = sqlite3.connect(str(DB_PATH))
cur = conn.cursor()

def load_candles(timeframe, coins=None):
    if coins:
        placeholders = ','.join('?' * len(coins))
        cur.execute(f"""SELECT coin, timestamp, open, high, low, close, volume
                       FROM prices WHERE timeframe=? AND coin IN ({placeholders})
                       ORDER BY coin, timestamp""", [timeframe] + list(coins))
    else:
        cur.execute("""SELECT coin, timestamp, open, high, low, close, volume
                       FROM prices WHERE timeframe=? ORDER BY coin, timestamp""", (timeframe,))

    data = defaultdict(list)
    for row in cur.fetchall():
        data[row[0]].append(row[1:])
    return dict(data)

def load_fear_greed():
    cur.execute("SELECT date, value FROM fear_greed")
    return {row[0]: row[1] for row in cur.fetchall()}

all_coins_needed = set(ACTIVE_COINS) | {'BTC'}
candles_1d = load_candles('1d', all_coins_needed)
candles_4h = load_candles('4h', all_coins_needed)
candles_1h = load_candles('1h', all_coins_needed)
fg_data = load_fear_greed()
conn.close()

print(f"Loaded: 1D={sum(len(v) for v in candles_1d.values())} candles, "
      f"4H={sum(len(v) for v in candles_4h.values())}, "
      f"1H={sum(len(v) for v in candles_1h.values())}")

# ── Helper: EMA, RSI, ATR ──
def calc_ema(values, period):
    if len(values) < period:
        return [None] * len(values)
    ema = [None] * (period - 1)
    sma = sum(values[:period]) / period
    ema.append(sma)
    mult = 2.0 / (period + 1)
    for i in range(period, len(values)):
        ema.append(values[i] * mult + ema[-1] * (1 - mult))
    return ema

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return [None] * len(closes)
    rsi = [None] * period
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        rsi.append(100.0)
    else:
        rsi.append(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rsi.append(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
    return rsi

def calc_atr(bars, period=14):
    if len(bars) < period + 1:
        return [None] * len(bars)
    trs = [None]
    for i in range(1, len(bars)):
        h, l, pc = bars[i][2], bars[i][3], bars[i-1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = [None] * period
    first_atr = sum(t for t in trs[1:period+1]) / period
    atr.append(first_atr)
    for i in range(period + 1, len(trs)):
        atr.append((atr[-1] * (period - 1) + trs[i]) / period)
    return atr

# ── Pre-compute indicators ──
print("Computing indicators...")

btc_1d = candles_1d.get('BTC', [])
btc_1d_closes = [b[4] for b in btc_1d]
btc_1d_ma5 = calc_ema(btc_1d_closes, 5)
btc_1d_ma20 = calc_ema(btc_1d_closes, 20)
btc_1d_ts_to_idx = {b[0]: i for i, b in enumerate(btc_1d)}

coin_4h_ema8 = {}
coin_4h_ema21 = {}
coin_4h_atr = {}
coin_4h_ts_idx = {}
coin_1h_rsi = {}
coin_1h_ts_idx = {}

for coin in ACTIVE_COINS:
    if coin in candles_4h:
        bars = candles_4h[coin]
        closes = [b[4] for b in bars]
        coin_4h_ema8[coin] = calc_ema(closes, 8)
        coin_4h_ema21[coin] = calc_ema(closes, 21)
        coin_4h_atr[coin] = calc_atr(bars, 14)
        coin_4h_ts_idx[coin] = {b[0]: i for i, b in enumerate(bars)}
    if coin in candles_1h:
        closes = [b[4] for b in candles_1h[coin]]
        coin_1h_rsi[coin] = calc_rsi(closes, 14)
        coin_1h_ts_idx[coin] = {b[0]: i for i, b in enumerate(candles_1h[coin])}

# ── Daily Direction ──
def get_daily_direction(day_ts):
    """
    day_ts = midnight UTC for today.
    Uses yesterday's completed candle for direction.
    """
    yesterday_ts = day_ts - 86400
    score = 0

    idx_yesterday = btc_1d_ts_to_idx.get(yesterday_ts)
    if idx_yesterday is not None:
        bar = btc_1d[idx_yesterday]
        if bar[4] > bar[1]:
            score += 1
        elif bar[4] < bar[1]:
            score -= 1

        ma5 = btc_1d_ma5[idx_yesterday] if idx_yesterday < len(btc_1d_ma5) else None
        ma20 = btc_1d_ma20[idx_yesterday] if idx_yesterday < len(btc_1d_ma20) else None
        if ma5 is not None and ma20 is not None:
            if ma5 > ma20:
                score += 1
            elif ma5 < ma20:
                score -= 1

    if score >= 2:
        return 'STRONG_LONG', score
    elif score == 1:
        return 'LONG', score
    elif score == 0:
        return 'NEUTRAL', score
    elif score == -1:
        return 'SHORT', score
    else:
        return 'STRONG_SHORT', score


def score_coin_entry(coin, ts_4h, daily_dir, daily_score):
    if coin not in coin_4h_ts_idx or coin not in coin_1h_ts_idx:
        return None

    idx_4h = coin_4h_ts_idx[coin].get(ts_4h)
    if idx_4h is None or idx_4h < 21:
        return None

    cfg = COIN_CFG.get(coin)
    if cfg is None:
        return None

    bar_4h = candles_4h[coin][idx_4h]
    price = bar_4h[4]

    ema8 = coin_4h_ema8[coin][idx_4h] if idx_4h < len(coin_4h_ema8[coin]) else None
    ema21 = coin_4h_ema21[coin][idx_4h] if idx_4h < len(coin_4h_ema21[coin]) else None
    if ema8 is None or ema21 is None:
        return None

    atr = coin_4h_atr[coin][idx_4h] if idx_4h < len(coin_4h_atr[coin]) else None
    if atr is None or atr <= 0:
        return None

    if daily_dir in ('STRONG_LONG', 'LONG'):
        direction = 'LONG'
    elif daily_dir in ('STRONG_SHORT', 'SHORT'):
        direction = 'SHORT'
    else:
        return None

    score = 0
    if direction == 'LONG' and ema8 > ema21:
        score += 1
    elif direction == 'SHORT' and ema8 < ema21:
        score += 1

    rsi_val = None
    for offset in [0, 3600, -3600, 7200, -7200]:
        ih = coin_1h_ts_idx[coin].get(ts_4h + offset)
        if ih is not None and ih < len(coin_1h_rsi[coin]):
            rsi_val = coin_1h_rsi[coin][ih]
            break
    if rsi_val is not None and 30 <= rsi_val <= 70:
        score += 1

    sr_hit = False
    for offset in range(24):
        check_ts = ts_4h - offset * 3600
        ih = coin_1h_ts_idx[coin].get(check_ts)
        if ih is not None:
            bar_1h = candles_1h[coin][ih]
            h, l = bar_1h[2], bar_1h[3]
            if abs(price - h) / price < 0.005 or abs(price - l) / price < 0.005:
                sr_hit = True
                break
    if sr_hit:
        score += 1

    if cfg.get('backtest_wr', 0) > 50:
        score += 1

    sl_mult = cfg.get('sl_mult', 0.8)
    rr = cfg.get('rr', 2.0)
    sl_dist = atr * sl_mult
    tp_dist = sl_dist * rr

    if direction == 'LONG':
        sl_price = price - sl_dist
        tp_price = price + tp_dist
    else:
        sl_price = price + sl_dist
        tp_price = price - tp_dist

    return (score, price, sl_price, tp_price, direction, atr, rsi_val)


def simulate_trade(coin, entry_price, sl_price, tp_price, direction, entry_ts, max_hold_bars):
    """
    Walk forward on 1H candles from entry.
    max_hold_bars = 4H bars = max_hold_bars * 4 hours.
    Returns: (pnl_pct_leveraged, exit_reason, exit_ts, exit_price, hours_held)
    """
    cfg = COIN_CFG.get(coin, {})
    hold_hours = max_hold_bars * 4
    max_ts = entry_ts + hold_hours * 3600

    if coin not in candles_1h:
        return (0, 'NO_DATA', entry_ts, entry_price, 0)

    bars_1h = candles_1h[coin]
    # Find start index
    start_idx = coin_1h_ts_idx[coin].get(entry_ts)
    if start_idx is None:
        for offset in [3600, -3600, 7200, -7200]:
            start_idx = coin_1h_ts_idx[coin].get(entry_ts + offset)
            if start_idx is not None:
                break
    if start_idx is None:
        return (0, 'NO_DATA', entry_ts, entry_price, 0)

    for i in range(start_idx + 1, min(start_idx + hold_hours + 2, len(bars_1h))):
        bar = bars_1h[i]
        ts, o, h, l, c, vol = bar

        # Check SL/TP within bar
        if direction == 'LONG':
            if l <= sl_price:
                pnl = (sl_price - entry_price) / entry_price * LEVERAGE - FEE_RATE * 2
                return (pnl, 'STOP_LOSS', ts, sl_price, i - start_idx)
            if h >= tp_price:
                pnl = (tp_price - entry_price) / entry_price * LEVERAGE - FEE_RATE * 2
                return (pnl, 'TARGET_HIT', ts, tp_price, i - start_idx)
        else:
            if h >= sl_price:
                pnl = (entry_price - sl_price) / entry_price * LEVERAGE - FEE_RATE * 2
                return (pnl, 'STOP_LOSS', ts, sl_price, i - start_idx)
            if l <= tp_price:
                pnl = (entry_price - tp_price) / entry_price * LEVERAGE - FEE_RATE * 2
                return (pnl, 'TARGET_HIT', ts, tp_price, i - start_idx)

        # Time exit
        if ts >= max_ts:
            pnl = ((c - entry_price) / entry_price if direction == 'LONG'
                   else (entry_price - c) / entry_price) * LEVERAGE - FEE_RATE * 2
            return (pnl, 'TIME_EXIT', ts, c, i - start_idx)

    # Ran out of candles
    if start_idx + 1 < len(bars_1h):
        last = bars_1h[min(start_idx + hold_hours + 1, len(bars_1h) - 1)]
        c = last[4]
        pnl = ((c - entry_price) / entry_price if direction == 'LONG'
               else (entry_price - c) / entry_price) * LEVERAGE - FEE_RATE * 2
        return (pnl, 'TIME_EXIT', last[0], c, min(hold_hours, len(bars_1h) - start_idx - 1))

    return (0, 'NO_DATA', entry_ts, entry_price, 0)


# ── BTC 4H timestamps ──
btc_4h_timestamps = sorted([b[0] for b in candles_4h.get('BTC', [])])
print(f"BTC 4H bars: {len(btc_4h_timestamps)} ({datetime.fromtimestamp(btc_4h_timestamps[0], tz=timezone.utc).date()} "
      f"to {datetime.fromtimestamp(btc_4h_timestamps[-1], tz=timezone.utc).date()})")

# ── Scenarios ──
SCENARIOS = {
    'A_Conservative': {'max_new': 3, 'max_concurrent': 6, 'compound': False, 'no_filter': False},
    'B_Moderate':     {'max_new': 5, 'max_concurrent': 8, 'compound': False, 'no_filter': False},
    'C_Aggressive':   {'max_new': 5, 'max_concurrent': 10, 'compound': True, 'no_filter': False},
    'D_NoFilter':     {'max_new': 5, 'max_concurrent': 8, 'compound': False, 'no_filter': True},
}

all_scenario_results = {}

for scenario_name, scfg in SCENARIOS.items():
    print(f"\n{'='*60}")
    print(f"Scenario: {scenario_name} (new={scfg['max_new']}, concurrent={scfg['max_concurrent']}, "
          f"compound={scfg['compound']}, no_filter={scfg['no_filter']})")
    print(f"{'='*60}")

    max_new = scfg['max_new']
    max_concurrent = scfg['max_concurrent']
    compound = scfg['compound']
    no_filter = scfg['no_filter']

    equity = START_EQUITY
    # Open positions: list of {coin, exit_ts} — we track expected exit time
    open_positions = []
    all_trades = []
    daily_pnl = defaultdict(float)
    daily_trade_count = defaultdict(int)
    daily_equity = {}
    coin_stats = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0.0})
    direction_stats = defaultdict(int)
    direction_wr = defaultdict(lambda: {'trades': 0, 'wins': 0})

    prev_day = None
    daily_dir = 'NEUTRAL'
    daily_score = 0
    entries_this_scan = 0
    trade_count = 0

    for ts_4h in btc_4h_timestamps:
        dt = datetime.fromtimestamp(ts_4h, tz=timezone.utc)
        day_str = dt.strftime('%Y-%m-%d')
        day_midnight = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())

        # Expire closed positions
        open_positions = [p for p in open_positions if p['exit_ts'] > ts_4h]

        # New day: compute direction
        if day_str != prev_day:
            if no_filter:
                # For no-filter baseline, alternate LONG/SHORT based on BTC EMA
                # Actually: trade both directions, pick direction per coin from 4H EMA
                daily_dir = 'LONG'  # will be overridden per-coin
                daily_score = 1
            else:
                daily_dir, daily_score = get_daily_direction(day_midnight)
            daily_equity[day_str] = equity
            direction_stats[daily_dir] += 1
            prev_day = day_str

        # How many slots free?
        active_count = len(open_positions)
        slots_free = max_concurrent - active_count

        if slots_free <= 0:
            continue

        # Score all coins for entry
        candidates = []
        open_coins = {p['coin'] for p in open_positions}

        for coin in ACTIVE_COINS:
            if coin in open_coins:
                continue

            if no_filter:
                # For baseline: use 4H EMA direction instead of daily
                if coin not in coin_4h_ts_idx:
                    continue
                idx_4h = coin_4h_ts_idx[coin].get(ts_4h)
                if idx_4h is None or idx_4h < 21:
                    continue
                e8 = coin_4h_ema8[coin][idx_4h] if idx_4h < len(coin_4h_ema8[coin]) else None
                e21 = coin_4h_ema21[coin][idx_4h] if idx_4h < len(coin_4h_ema21[coin]) else None
                if e8 is None or e21 is None:
                    continue
                fake_dir = 'STRONG_LONG' if e8 > e21 else 'STRONG_SHORT'
                result = score_coin_entry(coin, ts_4h, fake_dir, 2)
            else:
                result = score_coin_entry(coin, ts_4h, daily_dir, daily_score)

            if result is not None:
                score, entry_p, sl_p, tp_p, direction, atr, rsi_val = result
                candidates.append({
                    'coin': coin, 'score': score, 'entry': entry_p,
                    'sl': sl_p, 'tp': tp_p, 'direction': direction,
                    'atr': atr, 'rsi': rsi_val
                })

        # Sort by score desc, take top N
        candidates.sort(key=lambda x: (-x['score'], x['coin']))
        to_enter = candidates[:min(max_new, slots_free)]

        for cand in to_enter:
            coin = cand['coin']
            cfg = COIN_CFG.get(coin, {})
            hold_bars = cfg.get('hold_bars', 3)

            confidence = max(0.5, min(1.0, cand['score'] / 4.0))
            if compound:
                pos_size = equity * 0.10 * confidence
            else:
                pos_size = START_EQUITY * 0.10 * confidence

            pnl_pct, exit_reason, exit_ts, exit_price, hours_held = simulate_trade(
                coin, cand['entry'], cand['sl'], cand['tp'], cand['direction'],
                ts_4h, hold_bars
            )

            if exit_reason == 'NO_DATA':
                continue

            pnl_usd = pos_size * pnl_pct
            equity += pnl_usd
            if equity < 100:
                equity = 100

            exit_dt = datetime.fromtimestamp(exit_ts, tz=timezone.utc)
            exit_day = exit_dt.strftime('%Y-%m-%d')

            trade_record = {
                'scenario': scenario_name,
                'coin': coin,
                'direction': cand['direction'],
                'entry_price': cand['entry'],
                'sl_price': cand['sl'],
                'tp_price': cand['tp'],
                'exit_price': exit_price,
                'entry_ts': ts_4h,
                'exit_ts': exit_ts,
                'entry_time': dt.strftime('%Y-%m-%d %H:%M'),
                'exit_time': exit_dt.strftime('%Y-%m-%d %H:%M'),
                'pnl_pct': round(pnl_pct * 100, 2),
                'pnl_usd': round(pnl_usd, 2),
                'exit_reason': exit_reason,
                'score': cand['score'],
                'hours_held': hours_held,
                'daily_dir': daily_dir,
                'equity_after': round(equity, 2),
                'pos_size': round(pos_size, 2),
                'notional': round(pos_size * LEVERAGE, 2),
            }
            all_trades.append(trade_record)

            # Add to daily stats (credit PnL to entry day)
            daily_pnl[day_str] += pnl_usd
            daily_trade_count[day_str] += 1

            coin_stats[coin]['trades'] += 1
            coin_stats[coin]['pnl'] += pnl_usd
            if pnl_pct > 0:
                coin_stats[coin]['wins'] += 1

            direction_wr[daily_dir]['trades'] += 1
            if pnl_pct > 0:
                direction_wr[daily_dir]['wins'] += 1

            # Track open position
            open_positions.append({
                'coin': coin,
                'exit_ts': exit_ts,
            })

            trade_count += 1

    # ── Stats ──
    total_trades = len(all_trades)
    wins = sum(1 for t in all_trades if t['pnl_pct'] > 0)
    wr = wins / total_trades * 100 if total_trades > 0 else 0

    sl_count = sum(1 for t in all_trades if t['exit_reason'] == 'STOP_LOSS')
    tp_count = sum(1 for t in all_trades if t['exit_reason'] == 'TARGET_HIT')
    time_count = sum(1 for t in all_trades if t['exit_reason'] == 'TIME_EXIT')

    all_days = sorted(daily_equity.keys())
    daily_rois = []
    for d in all_days:
        eq_start = daily_equity[d]
        pnl = daily_pnl.get(d, 0)
        roi = pnl / eq_start * 100 if eq_start > 0 else 0
        daily_rois.append(roi)

    days_profitable = sum(1 for r in daily_rois if r > 0)
    days_10pct = sum(1 for r in daily_rois if r >= 10)

    peak = START_EQUITY
    max_dd = 0
    running_eq = START_EQUITY
    for d in all_days:
        running_eq += daily_pnl.get(d, 0)
        if running_eq > peak:
            peak = running_eq
        dd = (peak - running_eq) / peak * 100
        if dd > max_dd:
            max_dd = dd

    avg_roi = sum(daily_rois) / len(daily_rois) if daily_rois else 0
    sorted_rois = sorted(daily_rois)
    median_roi = sorted_rois[len(sorted_rois)//2] if sorted_rois else 0
    min_roi = min(daily_rois) if daily_rois else 0
    max_roi = max(daily_rois) if daily_rois else 0

    total_days = len(all_days)
    trades_per_day = total_trades / total_days if total_days > 0 else 0

    avg_pnl_pct = sum(t['pnl_pct'] for t in all_trades) / total_trades if total_trades else 0

    result = {
        'total_trades': total_trades,
        'wins': wins,
        'losses': total_trades - wins,
        'wr': round(wr, 1),
        'sl_count': sl_count,
        'tp_count': tp_count,
        'time_count': time_count,
        'sl_rate': round(sl_count / total_trades * 100, 1) if total_trades else 0,
        'tp_rate': round(tp_count / total_trades * 100, 1) if total_trades else 0,
        'total_days': total_days,
        'trades_per_day': round(trades_per_day, 1),
        'avg_daily_roi': round(avg_roi, 3),
        'median_daily_roi': round(median_roi, 3),
        'min_daily_roi': round(min_roi, 2),
        'max_daily_roi': round(max_roi, 2),
        'days_profitable_pct': round(days_profitable / total_days * 100, 1) if total_days else 0,
        'days_10pct_pct': round(days_10pct / total_days * 100, 1) if total_days else 0,
        'max_drawdown': round(max_dd, 1),
        'final_equity': round(equity, 2),
        'total_return_pct': round((equity - START_EQUITY) / START_EQUITY * 100, 1),
        'avg_trade_pnl_pct': round(avg_pnl_pct, 2),
        'coin_stats': dict(coin_stats),
        'direction_stats': dict(direction_stats),
        'direction_wr': {k: {'trades': v['trades'], 'wins': v['wins'],
                             'wr': round(v['wins']/v['trades']*100, 1) if v['trades'] > 0 else 0}
                        for k, v in direction_wr.items()},
        'daily_rois': daily_rois,
        'all_trades': all_trades,
        'daily_pnl': dict(daily_pnl),
        'daily_equity': dict(daily_equity),
        'daily_trade_count': dict(daily_trade_count),
    }

    all_scenario_results[scenario_name] = result

    print(f"  Total trades: {total_trades} | WR: {wr:.1f}% | Trades/day: {trades_per_day:.1f}")
    print(f"  SL/TP/Time: {sl_count}/{tp_count}/{time_count}")
    print(f"  Avg daily ROI: {avg_roi:.3f}% | Median: {median_roi:.3f}%")
    print(f"  Days profitable: {days_profitable}/{total_days} ({days_profitable/total_days*100:.1f}%)")
    print(f"  Days >= 10% ROI: {days_10pct}/{total_days} ({days_10pct/total_days*100:.1f}%)")
    print(f"  Max drawdown: {max_dd:.1f}% | Final equity: ${equity:,.2f}")
    print(f"  Avg trade PnL: {avg_pnl_pct:.2f}%")

    # Show some direction breakdown
    for d_name in ['STRONG_LONG', 'LONG', 'NEUTRAL', 'SHORT', 'STRONG_SHORT']:
        dw = direction_wr.get(d_name, {'trades': 0, 'wins': 0})
        dwr_pct = dw['wins']/dw['trades']*100 if dw['trades'] > 0 else 0
        print(f"    {d_name}: {dw['trades']} trades, {dwr_pct:.1f}% WR")


# ── Save trades.csv ──
print("\nSaving outputs...")
trades_csv = OUT_DIR / "trades.csv"
all_trades_combined = []
for sname, res in all_scenario_results.items():
    all_trades_combined.extend(res['all_trades'])

fieldnames = ['scenario', 'coin', 'direction', 'entry_price', 'sl_price', 'tp_price',
              'exit_price', 'entry_time', 'exit_time', 'pnl_pct', 'pnl_usd',
              'exit_reason', 'score', 'hours_held', 'daily_dir', 'equity_after', 'pos_size', 'notional']
with open(trades_csv, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(all_trades_combined)
print(f"  trades.csv: {len(all_trades_combined)} trades")

# ── Save daily_results.csv ──
daily_csv = OUT_DIR / "daily_results.csv"
daily_rows = []
for sname in ['A_Conservative', 'B_Moderate', 'C_Aggressive']:
    res = all_scenario_results[sname]
    all_days = sorted(res['daily_equity'].keys())
    running_eq = START_EQUITY
    for d in all_days:
        pnl = res['daily_pnl'].get(d, 0)
        eq_start = res['daily_equity'][d]
        roi = pnl / eq_start * 100 if eq_start > 0 else 0
        running_eq = eq_start + pnl
        daily_rows.append({
            'scenario': sname,
            'date': d,
            'equity_start': round(eq_start, 2),
            'pnl_usd': round(pnl, 2),
            'roi_pct': round(roi, 2),
            'equity_end': round(running_eq, 2),
            'trades': res['daily_trade_count'].get(d, 0),
        })

with open(daily_csv, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['scenario', 'date', 'equity_start', 'pnl_usd', 'roi_pct', 'equity_end', 'trades'])
    writer.writeheader()
    writer.writerows(daily_rows)
print(f"  daily_results.csv: {len(daily_rows)} rows")


# ── Save summary.md ──
summary_path = OUT_DIR / "summary.md"
with open(summary_path, 'w') as f:
    f.write("# Hybrid 1D Direction + 4H Entry Backtest Results\n\n")
    f.write(f"**Test period**: {datetime.fromtimestamp(btc_4h_timestamps[0], tz=timezone.utc).strftime('%Y-%m-%d')} to "
            f"{datetime.fromtimestamp(btc_4h_timestamps[-1], tz=timezone.utc).strftime('%Y-%m-%d')}\n")
    res_b = all_scenario_results['B_Moderate']
    f.write(f"**Total days**: {res_b['total_days']}\n")
    f.write(f"**Active coins**: {len(ACTIVE_COINS)} ({', '.join(ACTIVE_COINS)})\n")
    f.write(f"**Start equity**: ${START_EQUITY:,.0f} | **Leverage**: {LEVERAGE}x | **Fees**: {FEE_RATE*2*100:.2f}% roundtrip\n\n")

    # Scenario comparison
    f.write("## Scenario Comparison\n\n")
    f.write("| Metric | A) Conservative | B) Moderate | C) Aggressive | D) No Filter |\n")
    f.write("|--------|----------------|-------------|---------------|---------------|\n")

    scens = ['A_Conservative', 'B_Moderate', 'C_Aggressive', 'D_NoFilter']
    metrics = [
        ('Total Trades', 'total_trades', ''),
        ('Win Rate', 'wr', '%'),
        ('Trades/Day', 'trades_per_day', ''),
        ('SL Rate', 'sl_rate', '%'),
        ('TP Rate', 'tp_rate', '%'),
        ('Avg Daily ROI', 'avg_daily_roi', '%'),
        ('Median Daily ROI', 'median_daily_roi', '%'),
        ('Min Daily ROI', 'min_daily_roi', '%'),
        ('Max Daily ROI', 'max_daily_roi', '%'),
        ('Days Profitable', 'days_profitable_pct', '%'),
        ('Days >= 10% ROI', 'days_10pct_pct', '%'),
        ('Max Drawdown', 'max_drawdown', '%'),
        ('Final Equity', 'final_equity', '$'),
        ('Total Return', 'total_return_pct', '%'),
        ('Avg Trade PnL', 'avg_trade_pnl_pct', '%'),
    ]

    for label, key, unit in metrics:
        vals = []
        for s in scens:
            v = all_scenario_results[s].get(key, 0)
            if unit == '$':
                vals.append(f"${v:,.0f}")
            else:
                vals.append(f"{v}{unit}")
        f.write(f"| {label} | {' | '.join(vals)} |\n")

    # Direction analysis
    f.write("\n## Daily Direction Distribution & Win Rate (Scenario B)\n\n")
    f.write("| Direction | Days | Trades | Wins | WR |\n")
    f.write("|-----------|------|--------|------|----|\n")
    for d in ['STRONG_LONG', 'LONG', 'NEUTRAL', 'SHORT', 'STRONG_SHORT']:
        days = res_b['direction_stats'].get(d, 0)
        dwr = res_b['direction_wr'].get(d, {})
        trades = dwr.get('trades', 0)
        wins_d = dwr.get('wins', 0)
        wr_d = dwr.get('wr', 0)
        f.write(f"| {d} | {days} | {trades} | {wins_d} | {wr_d}% |\n")

    # Per-coin
    f.write("\n## Per-Coin Performance (Scenario B)\n\n")
    f.write("| Coin | Trades | Wins | WR | Total PnL |\n")
    f.write("|------|--------|------|-----|----------|\n")
    coin_data = []
    for coin in sorted(res_b['coin_stats'].keys()):
        cs = res_b['coin_stats'][coin]
        cwr = cs['wins'] / cs['trades'] * 100 if cs['trades'] > 0 else 0
        coin_data.append((coin, cs['trades'], cs['wins'], cwr, cs['pnl']))
    coin_data.sort(key=lambda x: x[4], reverse=True)
    for coin, trades, wins_c, cwr, pnl in coin_data:
        f.write(f"| {coin} | {trades} | {wins_c} | {cwr:.1f}% | ${pnl:+,.2f} |\n")

    # Histogram
    f.write("\n## Daily ROI Distribution (Scenario B)\n\n")
    f.write("```\n")
    rois = res_b['daily_rois']
    bins = [(-999, -20), (-20, -10), (-10, -5), (-5, -2), (-2, 0),
            (0, 0.001), (0.001, 2), (2, 5), (5, 10), (10, 20), (20, 999)]
    bin_labels = ['< -20%', '-20 to -10%', '-10 to -5%', '-5 to -2%', '-2 to 0%',
                  '= 0% (idle)', '0 to 2%', '2 to 5%', '5 to 10%', '10 to 20%', '> 20%']
    for (lo, hi), label in zip(bins, bin_labels):
        count = sum(1 for r in rois if lo <= r < hi)
        bar_len = min(count, 80)
        bar = '#' * bar_len
        pct = count / len(rois) * 100 if rois else 0
        f.write(f"{label:>14} | {bar} ({count}, {pct:.1f}%)\n")
    f.write("```\n")

    # Key Questions
    f.write("\n## Answers to Key Questions\n\n")

    wr_b = all_scenario_results['B_Moderate']['wr']
    wr_d = all_scenario_results['D_NoFilter']['wr']

    f.write("### 1. Hybrid WR vs No-Filter Baseline?\n")
    f.write(f"- Hybrid (1D+4H): **{wr_b}%** WR\n")
    f.write(f"- No daily filter: **{wr_d}%** WR\n")
    f.write(f"- Delta: **{wr_b - wr_d:+.1f}%** -- daily direction {'improves' if wr_b > wr_d else 'does not improve'} WR\n\n")

    f.write("### 2. Realistic Trades Per Day?\n")
    for s in ['A_Conservative', 'B_Moderate', 'C_Aggressive']:
        f.write(f"- {s}: **{all_scenario_results[s]['trades_per_day']}**/day\n")
    f.write("\n")

    f.write("### 3. What % of Days Achieve 10%+ ROI?\n")
    for s in ['A_Conservative', 'B_Moderate', 'C_Aggressive']:
        f.write(f"- {s}: **{all_scenario_results[s]['days_10pct_pct']}%** of days\n")
    f.write("\n")

    f.write("### 4. Realistic Daily ROI (Median)?\n")
    for s in ['A_Conservative', 'B_Moderate', 'C_Aggressive']:
        f.write(f"- {s}: **{all_scenario_results[s]['median_daily_roi']}%** median daily ROI\n")
    f.write("\n")

    f.write("### 5. Max Drawdown?\n")
    for s in ['A_Conservative', 'B_Moderate', 'C_Aggressive']:
        f.write(f"- {s}: **{all_scenario_results[s]['max_drawdown']}%**\n")
    f.write("\n")

    f.write("### 6. Best Coins in Hybrid System?\n")
    f.write("Top 5 by total PnL (Scenario B):\n")
    for i, (coin, trades, wins_c, cwr, pnl) in enumerate(coin_data[:5]):
        f.write(f"  {i+1}. **{coin}** -- {trades} trades, {cwr:.1f}% WR, ${pnl:+,.2f}\n")
    f.write("\nWorst 3:\n")
    for coin, trades, wins_c, cwr, pnl in coin_data[-3:]:
        f.write(f"  - **{coin}** -- {trades} trades, {cwr:.1f}% WR, ${pnl:+,.2f}\n")
    f.write("\n")

    f.write("### 7. Does Daily Direction Improve WR vs Random?\n")
    f.write("Direction-specific WR (Scenario B):\n")
    for d in ['STRONG_LONG', 'LONG', 'NEUTRAL', 'SHORT', 'STRONG_SHORT']:
        dwr = res_b['direction_wr'].get(d, {})
        f.write(f"  - {d}: {dwr.get('wr', 0)}% ({dwr.get('trades', 0)} trades)\n")
    f.write(f"\nOverall hybrid: {wr_b}% vs no-filter: {wr_d}%\n")
    improvement = wr_b - wr_d
    f.write(f"Daily direction adds **{improvement:+.1f}%** to WR.\n\n")

    # Final recommendation
    f.write("## Final Recommendation\n\n")
    med_b = all_scenario_results['B_Moderate']['median_daily_roi']
    d10_b = all_scenario_results['B_Moderate']['days_10pct_pct']
    dd_b = all_scenario_results['B_Moderate']['max_drawdown']
    med_c = all_scenario_results['C_Aggressive']['median_daily_roi']
    d10_c = all_scenario_results['C_Aggressive']['days_10pct_pct']
    avg_b = all_scenario_results['B_Moderate']['avg_daily_roi']

    if med_b >= 10:
        f.write(f"10% daily ROI is achievable as median ({med_b}%) with the Moderate scenario.\n")
    elif d10_b >= 30:
        f.write(f"10% daily ROI happens {d10_b}% of days with Moderate scenario, but median is {med_b}%.\n")
    else:
        f.write(f"10% daily ROI is NOT consistently achievable. Median daily ROI = {med_b}% (Moderate), {med_c}% (Aggressive).\n")
        f.write(f"Avg daily ROI = {avg_b}% (Moderate). Only {d10_b}% of days reach 10%+ (Moderate), {d10_c}% (Aggressive).\n")

    f.write(f"\nMax drawdown: {dd_b}% -- {'acceptable' if dd_b < 30 else 'DANGEROUS'} for ${START_EQUITY:,.0f} account.\n")
    f.write(f"Recommended: Scenario B (Moderate) as baseline.\n")

print(f"\nAll saved to {OUT_DIR}")
print("Done!")
