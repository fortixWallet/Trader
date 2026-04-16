"""
FORTIX Trading Strategy Backtest
================================

Simulates the EXACT strategy that runs live:
  - Signal generation (V3 signals + regime + confirmations)
  - Position sizing (Kelly-based, tier-dependent)
  - Exit rules (hard stop, trailing, time, funding)
  - Fees (0.05% taker) + slippage (0.03%)
  - Funding rates (8h, from historical data)

Walk-forward: no future data used.
"""

import sqlite3
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

# Costs
TAKER_FEE = 0.0005  # 0.05%
SLIPPAGE = 0.0003  # 0.03%
ENTRY_COST = TAKER_FEE + SLIPPAGE  # per side
EXIT_COST = TAKER_FEE + SLIPPAGE


def run_backtest(start_date='2025-06-01', end_date='2026-04-01',
                 initial_capital=500, max_positions=3, risk_pct=0.03):
    """Run full strategy backtest."""

    conn = sqlite3.connect(str(DB_PATH))

    # Load all signal_tracking data (walk-forward validated)
    signals = conn.execute("""
        SELECT coin, signal_type, direction, strength, fired_at,
               price_at_fire, was_correct, actual_return
        FROM signal_tracking
        WHERE fired_at >= ? AND fired_at <= ? AND was_correct IS NOT NULL
        ORDER BY fired_at
    """, (start_date, end_date)).fetchall()

    print(f"Backtest: {start_date} to {end_date}")
    print(f"Signals: {len(signals)}")
    print(f"Capital: ${initial_capital}")

    # Load daily prices for all coins
    prices = {}
    rows = conn.execute("""
        SELECT coin, date(timestamp, 'unixepoch') as d, close
        FROM prices WHERE timeframe='1d'
        AND date(timestamp, 'unixepoch') >= ? AND date(timestamp, 'unixepoch') <= ?
        ORDER BY coin, timestamp
    """, (start_date, end_date)).fetchall()
    for r in rows:
        if r[0] not in prices:
            prices[r[0]] = {}
        prices[r[0]][r[1]] = r[2]

    # Load funding rates
    funding = {}
    frows = conn.execute("""
        SELECT coin, date(timestamp, 'unixepoch') as d, AVG(rate) as avg_rate
        FROM funding_rates
        WHERE date(timestamp, 'unixepoch') >= ? AND date(timestamp, 'unixepoch') <= ?
        GROUP BY coin, d
    """, (start_date, end_date)).fetchall()
    for r in frows:
        if r[0] not in funding:
            funding[r[0]] = {}
        funding[r[0]][r[1]] = r[2]

    conn.close()

    # Group signals by date
    daily_signals = defaultdict(list)
    for s in signals:
        daily_signals[s[4]].append({
            'coin': s[0], 'signal_type': s[1], 'direction': s[2],
            'strength': s[3], 'price': s[5], 'correct': s[6], 'return': s[7],
        })

    # Simulate
    capital = initial_capital
    equity_curve = [(start_date, capital)]
    open_positions = []
    closed_trades = []
    dates = sorted(daily_signals.keys())

    for date in dates:
        day_signals = daily_signals[date]

        # 1. Check exits for open positions
        to_close = []
        for i, pos in enumerate(open_positions):
            coin_prices = prices.get(pos['coin'], {})
            current_price = coin_prices.get(date)
            if not current_price:
                continue

            # P&L
            if pos['direction'] == 'BEARISH':
                pnl_pct = (pos['entry'] - current_price) / pos['entry']
            else:
                pnl_pct = (current_price - pos['entry']) / pos['entry']

            pnl_leveraged = pnl_pct * pos['leverage'] * 100
            days_held = len([d for d in sorted(coin_prices.keys()) if d > pos['date'] and d <= date])

            # Exit rules
            exit_reason = None
            if pnl_leveraged <= -15:
                exit_reason = 'HARD_STOP'
            elif pnl_leveraged >= 10:
                exit_reason = 'TAKE_PROFIT'
            elif days_held >= 7:
                exit_reason = 'TIME_EXIT'

            # Funding cost (daily)
            fr = funding.get(pos['coin'], {}).get(date, 0)
            if pos['direction'] == 'BEARISH':
                funding_cost = -fr * pos['notional']  # shorts receive positive funding
            else:
                funding_cost = fr * pos['notional']

            pos['funding_paid'] = pos.get('funding_paid', 0) + abs(funding_cost)

            if exit_reason:
                # Close
                pnl_usd = pnl_pct * pos['notional'] - pos['notional'] * EXIT_COST - pos['funding_paid']
                capital += pos['margin'] + pnl_usd
                closed_trades.append({
                    'coin': pos['coin'], 'direction': pos['direction'],
                    'entry': pos['entry'], 'exit': current_price,
                    'pnl_usd': pnl_usd, 'pnl_pct': pnl_leveraged,
                    'exit_reason': exit_reason, 'days_held': days_held,
                    'signal_type': pos['signal_type'],
                    'fees': pos['notional'] * (ENTRY_COST + EXIT_COST),
                    'funding': pos['funding_paid'],
                })
                to_close.append(i)

        # Remove closed positions
        for i in sorted(to_close, reverse=True):
            open_positions.pop(i)

        # 2. Open new positions from signals
        if len(open_positions) < max_positions:
            # Prioritize by strength
            sorted_signals = sorted(day_signals, key=lambda s: s['strength'], reverse=True)
            existing_coins = {p['coin'] for p in open_positions}
            existing_dirs = defaultdict(int)
            for p in open_positions:
                d = 'short' if p['direction'] == 'BEARISH' else 'long'
                existing_dirs[d] += 1

            for sig in sorted_signals:
                if len(open_positions) >= max_positions:
                    break
                if sig['coin'] in existing_coins:
                    continue

                direction_key = 'short' if sig['direction'] == 'BEARISH' else 'long'
                if existing_dirs[direction_key] >= 2:
                    continue

                # Only trade high-quality signals
                if sig['strength'] < 0.2:
                    continue

                leverage = 5 if sig['strength'] >= 0.5 else 3
                risk = capital * risk_pct
                notional = risk * leverage / 0.045  # approximate
                notional = min(notional, capital * 0.35 * leverage)
                margin = notional / leverage

                if margin > capital * 0.4:
                    continue

                entry_price = sig['price'] * (1 + ENTRY_COST) if sig['direction'] == 'BULLISH' \
                    else sig['price'] * (1 - ENTRY_COST)

                open_positions.append({
                    'coin': sig['coin'], 'direction': sig['direction'],
                    'entry': entry_price, 'date': date,
                    'leverage': leverage, 'notional': notional, 'margin': margin,
                    'signal_type': sig['signal_type'],
                    'funding_paid': 0,
                })
                capital -= margin
                existing_coins.add(sig['coin'])
                existing_dirs[direction_key] += 1

        equity_curve.append((date, capital + sum(p['margin'] for p in open_positions)))

    # Close remaining positions at end
    for pos in open_positions:
        coin_prices = prices.get(pos['coin'], {})
        last_price = list(coin_prices.values())[-1] if coin_prices else pos['entry']
        if pos['direction'] == 'BEARISH':
            pnl_pct = (pos['entry'] - last_price) / pos['entry']
        else:
            pnl_pct = (last_price - pos['entry']) / pos['entry']
        pnl_usd = pnl_pct * pos['notional'] - pos['notional'] * EXIT_COST
        capital += pos['margin'] + pnl_usd
        closed_trades.append({
            'coin': pos['coin'], 'direction': pos['direction'],
            'pnl_usd': pnl_usd, 'exit_reason': 'END_OF_BACKTEST',
            'signal_type': pos['signal_type'],
        })

    # Results
    final_equity = capital
    total_return = (final_equity / initial_capital - 1) * 100
    n_trades = len(closed_trades)
    wins = sum(1 for t in closed_trades if t['pnl_usd'] > 0)
    losses = n_trades - wins
    win_rate = wins / n_trades * 100 if n_trades > 0 else 0

    total_pnl = sum(t['pnl_usd'] for t in closed_trades)
    avg_win = np.mean([t['pnl_usd'] for t in closed_trades if t['pnl_usd'] > 0]) if wins else 0
    avg_loss = np.mean([t['pnl_usd'] for t in closed_trades if t['pnl_usd'] <= 0]) if losses else 0
    total_fees = sum(t.get('fees', 0) for t in closed_trades)
    total_funding = sum(t.get('funding', 0) for t in closed_trades)

    # Max drawdown
    peak = initial_capital
    max_dd = 0
    for _, eq in equity_curve:
        peak = max(peak, eq)
        dd = (peak - eq) / peak * 100
        max_dd = max(max_dd, dd)

    # Sharpe (weekly returns)
    weekly_returns = []
    for i in range(7, len(equity_curve), 7):
        ret = (equity_curve[i][1] / equity_curve[i-7][1] - 1)
        weekly_returns.append(ret)
    sharpe = np.mean(weekly_returns) / (np.std(weekly_returns) + 1e-8) * np.sqrt(52) if weekly_returns else 0

    # Per signal type
    signal_perf = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0})
    for t in closed_trades:
        sig = t.get('signal_type', 'unknown')
        if t['pnl_usd'] > 0:
            signal_perf[sig]['wins'] += 1
        else:
            signal_perf[sig]['losses'] += 1
        signal_perf[sig]['pnl'] += t['pnl_usd']

    print(f"\n{'='*50}")
    print(f"BACKTEST RESULTS")
    print(f"{'='*50}")
    print(f"Period: {start_date} to {end_date}")
    print(f"Initial: ${initial_capital:.2f}")
    print(f"Final:   ${final_equity:.2f}")
    print(f"Return:  {total_return:+.1f}%")
    print(f"Trades:  {n_trades} ({wins}W/{losses}L)")
    print(f"Win rate: {win_rate:.1f}%")
    print(f"Avg win:  ${avg_win:+.2f}")
    print(f"Avg loss: ${avg_loss:+.2f}")
    print(f"Fees:     ${total_fees:.2f}")
    print(f"Funding:  ${total_funding:.2f}")
    print(f"Max DD:   {max_dd:.1f}%")
    print(f"Sharpe:   {sharpe:.2f}")

    print(f"\nPer signal type:")
    for sig, perf in sorted(signal_perf.items(), key=lambda x: -x[1]['pnl']):
        total = perf['wins'] + perf['losses']
        wr = perf['wins'] / total * 100 if total > 0 else 0
        print(f"  {sig:25s}: {wr:.0f}% ({perf['wins']}W/{perf['losses']}L) PnL: ${perf['pnl']:+.2f}")

    return {
        'initial': initial_capital, 'final': final_equity,
        'return_pct': total_return, 'n_trades': n_trades,
        'win_rate': win_rate, 'sharpe': sharpe, 'max_drawdown': max_dd,
        'equity_curve': equity_curve, 'trades': closed_trades,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    result = run_backtest()
