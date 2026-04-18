"""
FORTIX Replay — Full Profi Simulation
=======================================
Runs Profi (Opus) on historical data as if trading live.
For each 4H candle: builds charts, market data, calls Opus, records decisions.
Then simulates fills and tracks PnL.

Usage: python3 scripts/replay_profi.py --start 2026-04-11 --end 2026-04-18
"""

import os, sys, json, time, sqlite3, argparse, base64
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = 'data/crypto/market.db'
COINS = ['BTC','ETH','SOL','XRP','ADA','AVAX','LINK','DOGE','BNB',
         'LDO','UNI','CRV','PENDLE','TON','ARB']

LEVERAGE = 8
SL_ROI = 6.5
TRAILING_ACT = 6.0
TRAILING_DROP = 2.0
HOLD_HOURS = 4
CAPITAL = 5000
RISK_PER_TRADE = 0.10  # 10% of capital

from pathlib import Path as _P
for _env in [_P(__file__).parent.parent / '.env', _P('/Users/williamstorm/Documents/Factory/.env')]:
    if _env.exists():
        for _line in open(_env):
            if '=' in _line and not _line.startswith('#'):
                _k, _v = _line.strip().split('=', 1)
                os.environ[_k.strip()] = _v.strip()
import anthropic


def get_price_at(conn, coin, ts, tf='1h'):
    r = conn.execute(
        "SELECT open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1",
        (coin, tf, ts)
    ).fetchone()
    return {'o': r[0], 'h': r[1], 'l': r[2], 'c': r[3], 'v': r[4]} if r else None


def get_candles(conn, coin, tf, ts, limit=30):
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe=? AND timestamp<=? ORDER BY timestamp DESC LIMIT ?",
        (coin, tf, ts, limit)
    ).fetchall()
    return list(reversed(rows))


def calc_ema(closes, period=12):
    if not closes: return 0
    ema = closes[0]
    m = 2 / (period + 1)
    for c in closes[1:]:
        ema = c * m + ema * (1 - m)
    return ema


def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return 50
    d = np.diff(closes)
    g = np.where(d > 0, d, 0)
    l = np.where(d < 0, -d, 0)
    ag, al = np.mean(g[:period]), np.mean(l[:period])
    if al == 0: return 100
    for i in range(period, len(g)):
        ag = (ag * (period-1) + g[i]) / period
        al = (al * (period-1) + l[i]) / period
    return 100 - 100 / (1 + ag / (al + 1e-10))


def build_profi_prompt(conn, coins, ts, open_positions):
    """Build the prompt Profi would see at timestamp ts."""

    # BTC context
    btc_4h = get_candles(conn, 'BTC', '4h', ts, 30)
    btc_1h = get_candles(conn, 'BTC', '1h', ts, 24)
    btc_price = btc_1h[-1][4] if btc_1h else 0
    btc_1h_mom = (btc_1h[-1][4] / btc_1h[-2][4] - 1) * 100 if len(btc_1h) >= 2 else 0

    # 4H EMA12 for trend
    btc_closes_4h = [c[4] for c in btc_4h]
    btc_ema12 = calc_ema(btc_closes_4h, 12)
    btc_trend = "UP" if btc_price > btc_ema12 else "DOWN"
    btc_trend_str = abs(btc_price - btc_ema12) / btc_ema12 * 100

    # Per-coin data
    coin_data = []
    for coin in coins:
        c_4h = get_candles(conn, coin, '4h', ts, 20)
        c_1h = get_candles(conn, coin, '1h', ts, 24)
        if not c_1h or not c_4h:
            continue

        price = c_1h[-1][4]
        closes_4h = [c[4] for c in c_4h]
        closes_1h = [c[4] for c in c_1h]
        ema12 = calc_ema(closes_4h, 12)
        rsi = calc_rsi(closes_1h)
        trend = "UP" if price > ema12 else "DOWN"
        trend_pct = (price - ema12) / ema12 * 100
        last_4h_candle = "GREEN" if c_4h[-1][4] > c_4h[-1][1] else "RED"
        mom_1h = (c_1h[-1][4] / c_1h[-2][4] - 1) * 100 if len(c_1h) >= 2 else 0
        mom_4h = (c_4h[-1][4] / c_4h[-2][4] - 1) * 100 if len(c_4h) >= 2 else 0

        # ATR
        atrs = [(c[2]-c[3])/c[4] for c in c_1h[-14:] if c[4] > 0]
        atr = np.mean(atrs) * 100 if atrs else 1.0

        coin_data.append(
            f"[{coin}: ${price:.4f} | 4H trend={trend} ({trend_pct:+.1f}% from EMA12) | "
            f"last 4H candle={last_4h_candle} | 1H mom={mom_1h:+.2f}% | 4H mom={mom_4h:+.2f}% | "
            f"RSI={rsi:.0f} | ATR={atr:.2f}%]"
        )

    coins_str = "\n".join(coin_data)

    # Open positions info
    pos_str = ""
    if open_positions:
        parts = [f"{p['dir']} {p['coin']} ROI={p.get('roi', 0):+.1f}%" for p in open_positions]
        pos_str = f"\nOPEN POSITIONS: {', '.join(parts)}"

    dt = datetime.utcfromtimestamp(ts)

    prompt = f"""Time: {dt.strftime('%Y-%m-%d %H:%M')} UTC

BTC: ${btc_price:.0f} | 4H trend={btc_trend} (EMA12 {btc_trend_str:.1f}% away) | 1H mom={btc_1h_mom:+.2f}%
{pos_str}

COINS:
{coins_str}

STRATEGY: 4H primary direction + 1H pullback entry.
- 4H trend (EMA12) sets direction for each coin INDIVIDUALLY
- Enter on 1H pullback in trend direction
- Only enter when 4H trend is strong (>1% from EMA12)
- Only enter when RSI is NOT extreme (30-70)
- Hold: {HOLD_HOURS}h max. SL: -{SL_ROI}% ROI. Trailing: act +{TRAILING_ACT}%, drop -{TRAILING_DROP}%.
- Leverage: {LEVERAGE}x. Capital: ${CAPITAL}, risk {RISK_PER_TRADE*100:.0f}% per trade.

RULES:
- Each coin decides direction INDEPENDENTLY based on ITS 4H trend
- If 4H trend is weak (<1% from EMA) → SKIP that coin
- If last 4H candle contradicts trend → lower confidence
- Pullback entry: wait for 1H candle to pull back slightly in trend direction
- MAX 5 positions at a time

Reply JSON array ONLY (0-5 setups):
[{{"coin": "BTC", "direction": "LONG", "entry": 84000, "confidence": 0.72, "reason": "4H UP +2.1%, pullback -0.3% on 1H, RSI=55"}}]

Empty [] is valid if no strong setups."""

    return prompt


def simulate_trade(conn, coin, direction, entry_price, entry_ts):
    """Simulate a trade from entry_ts forward."""
    candles = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices "
        "WHERE coin=? AND timeframe='1h' AND timestamp>? ORDER BY timestamp LIMIT ?",
        (coin, entry_ts, HOLD_HOURS)
    ).fetchall()

    if not candles:
        return {'roi': 0, 'exit': 'NO_DATA', 'held': 0}

    peak_roi = 0
    trailing_on = False
    sl_pct = SL_ROI / 100 / LEVERAGE

    for i, c in enumerate(candles):
        h, l, cl = c[2], c[3], c[4]

        if direction == 'LONG':
            best = (h / entry_price - 1) * 100 * LEVERAGE
            worst = (l / entry_price - 1) * 100 * LEVERAGE
            close_roi = (cl / entry_price - 1) * 100 * LEVERAGE
        else:
            best = (entry_price / l - 1) * 100 * LEVERAGE
            worst = (entry_price / h - 1) * 100 * LEVERAGE
            close_roi = (entry_price / cl - 1) * 100 * LEVERAGE

        if worst <= -SL_ROI:
            return {'roi': -SL_ROI, 'exit': 'STOP_LOSS', 'held': (i+1)*60}

        if best > peak_roi:
            peak_roi = best
        if peak_roi >= TRAILING_ACT:
            trailing_on = True
        if trailing_on and close_roi <= peak_roi - TRAILING_DROP:
            return {'roi': close_roi, 'exit': 'TRAILING', 'held': (i+1)*60, 'peak': peak_roi}

    # TIME_EXIT
    last = candles[-1][4]
    if direction == 'LONG':
        roi = (last / entry_price - 1) * 100 * LEVERAGE
    else:
        roi = (entry_price / last - 1) * 100 * LEVERAGE

    return {'roi': roi, 'exit': 'TIME_EXIT', 'held': len(candles)*60, 'peak': peak_roi}


def run_replay(start_date, end_date):
    conn = sqlite3.connect(DB_PATH)
    client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))

    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())

    # Get all 4H timestamps in range
    scan_times = conn.execute(
        "SELECT DISTINCT timestamp FROM prices WHERE timeframe='4h' "
        "AND timestamp >= ? AND timestamp <= ? AND coin='BTC' ORDER BY timestamp",
        (start_ts, end_ts)
    ).fetchall()
    scan_times = [r[0] for r in scan_times]

    print(f"{'='*80}")
    print(f"  PROFI REPLAY: {start_date} → {end_date}")
    print(f"  {len(scan_times)} scan points (every 4H)")
    print(f"  Strategy: 4H primary + 1H pullback, hold {HOLD_HOURS}h")
    print(f"  SL: -{SL_ROI}% ROI | Trailing: act +{TRAILING_ACT}%, drop -{TRAILING_DROP}%")
    print(f"{'='*80}")

    all_trades = []
    open_positions = []
    balance = CAPITAL

    for scan_idx, ts in enumerate(scan_times):
        dt = datetime.utcfromtimestamp(ts)

        # Close expired positions
        still_open = []
        for pos in open_positions:
            if ts - pos['entry_ts'] >= HOLD_HOURS * 3600:
                result = simulate_trade(conn, pos['coin'], pos['dir'], pos['entry'], pos['entry_ts'])
                pnl_usd = result['roi'] / 100 * pos['margin']
                balance += pnl_usd
                all_trades.append({**pos, **result, 'pnl_usd': pnl_usd})
                icon = '✅' if pnl_usd > 0 else '❌'
                print(f"  {icon} CLOSE {pos['dir']:5s} {pos['coin']:6s} ROI={result['roi']:+.1f}% "
                      f"${pnl_usd:+.1f} [{result['exit']}]")
            else:
                # Update ROI
                p_now = get_price_at(conn, pos['coin'], ts)
                if p_now:
                    if pos['dir'] == 'LONG':
                        pos['roi'] = (p_now['c'] / pos['entry'] - 1) * 100 * LEVERAGE
                    else:
                        pos['roi'] = (pos['entry'] / p_now['c'] - 1) * 100 * LEVERAGE
                still_open.append(pos)
        open_positions = still_open

        if len(open_positions) >= 5:
            continue

        # Build prompt and call Profi
        available_coins = [c for c in COINS if c not in [p['coin'] for p in open_positions]]
        prompt = build_profi_prompt(conn, available_coins, ts, open_positions)

        print(f"\n[{dt.strftime('%m-%d %H:%M')}] Scan #{scan_idx+1} | "
              f"Balance: ${balance:.0f} | Open: {len(open_positions)} | "
              f"Calling Profi...")

        try:
            resp = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=600,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text

            # Parse JSON
            start_j = text.find('[')
            end_j = text.rfind(']') + 1
            if start_j >= 0 and end_j > start_j:
                setups = json.loads(text[start_j:end_j])
            else:
                setups = []

            if not setups:
                print(f"  → SKIP (no setups)")
                continue

            for s in setups[:3]:  # max 3 per scan
                if len(open_positions) >= 5:
                    break

                coin = s.get('coin', '')
                direction = s.get('direction', '')
                conf = s.get('confidence', 0.5)
                reason = s.get('reason', '')[:60]
                entry = s.get('entry', 0)

                if coin not in COINS or direction not in ('LONG', 'SHORT'):
                    continue
                if coin in [p['coin'] for p in open_positions]:
                    continue

                # Get actual price at this time
                p = get_price_at(conn, coin, ts)
                if not p:
                    continue
                actual_entry = p['c']

                margin = balance * RISK_PER_TRADE * conf
                pos = {
                    'coin': coin, 'dir': direction, 'entry': actual_entry,
                    'entry_ts': ts, 'margin': margin, 'conf': conf,
                    'reason': reason, 'roi': 0
                }
                open_positions.append(pos)
                print(f"  → {direction:5s} {coin:6s} @${actual_entry:.4f} conf={conf:.0%} | {reason}")

        except Exception as e:
            print(f"  → ERROR: {e}")
            continue

        time.sleep(0.5)

    # Close remaining positions
    for pos in open_positions:
        result = simulate_trade(conn, pos['coin'], pos['dir'], pos['entry'], pos['entry_ts'])
        pnl_usd = result['roi'] / 100 * pos['margin']
        balance += pnl_usd
        all_trades.append({**pos, **result, 'pnl_usd': pnl_usd})

    # Summary
    print(f"\n{'='*80}")
    print(f"  REPLAY RESULTS")
    print(f"{'='*80}")

    if not all_trades:
        print("  No trades executed")
        conn.close()
        return

    wins = [t for t in all_trades if t['pnl_usd'] > 0]
    losses = [t for t in all_trades if t['pnl_usd'] <= 0]
    total_pnl = sum(t['pnl_usd'] for t in all_trades)

    print(f"\n  Trades: {len(all_trades)} | Wins: {len(wins)} | Losses: {len(losses)}")
    print(f"  WR: {len(wins)/len(all_trades)*100:.0f}%")
    print(f"  Total PnL: ${total_pnl:+.2f}")
    print(f"  Start: ${CAPITAL:,.0f} → End: ${balance:,.0f}")
    print(f"  ROI: {(balance/CAPITAL-1)*100:+.1f}%")

    if wins:
        print(f"  Avg win: ${np.mean([t['pnl_usd'] for t in wins]):+.2f} (ROI {np.mean([t['roi'] for t in wins]):+.1f}%)")
    if losses:
        print(f"  Avg loss: ${np.mean([t['pnl_usd'] for t in losses]):+.2f} (ROI {np.mean([t['roi'] for t in losses]):+.1f}%)")

    # Exit reasons
    exits = {}
    for t in all_trades:
        e = t.get('exit', '?')
        exits[e] = exits.get(e, 0) + 1
    print(f"  Exits: {exits}")

    # Per-trade detail
    print(f"\n{'#':>3s} {'Coin':>6s} {'Dir':>5s} {'ROI':>7s} {'PnL':>8s} {'Exit':>10s} {'Hold':>6s} {'Conf':>5s} {'Reason'}")
    print("-" * 80)
    for i, t in enumerate(all_trades, 1):
        icon = '✅' if t['pnl_usd'] > 0 else '❌'
        held = f"{t.get('held',0):.0f}m"
        print(f"{icon}{i:>2d} {t['coin']:>6s} {t['dir']:>5s} {t['roi']:>+6.1f}% ${t['pnl_usd']:>+7.2f} "
              f"{t.get('exit',''):>10s} {held:>6s} {t['conf']:>4.0%} {t.get('reason','')[:40]}")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2026-04-14', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default='2026-04-18', help='End date YYYY-MM-DD')
    args = parser.parse_args()

    run_replay(args.start, args.end)
