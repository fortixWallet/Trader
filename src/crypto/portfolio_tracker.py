"""
FORTIX — Portfolio Performance Tracker
=========================================
Simulates portfolio performance based on signal predictions.

BUY signals: "bought" at actual_price_at_prediction, "sold" when next SELL signal
             for same coin arrives OR after 7 days (whichever comes first).
SELL signals: "shorted" at actual_price_at_prediction, "covered" after 7 days.

Computes cumulative P&L, compares against BTC buy-and-hold,
generates dark-theme chart, and provides text summary for script injection.

Usage:
    python src/crypto/portfolio_tracker.py              # Print summary
    python src/crypto/portfolio_tracker.py --chart      # Generate chart
    python src/crypto/portfolio_tracker.py --days 60    # Custom lookback
"""

import sys
import sqlite3
import logging
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('portfolio_tracker')

DB_PATH = Path('data/crypto/market.db')
CHARTS_DIR = Path('output/crypto_signal/charts')

# Color scheme — matches chart_generator.py
COLORS = {
    'bg': '#0D1117',
    'bg_secondary': '#161B22',
    'text': '#E6EDF3',
    'text_dim': '#8B949E',
    'green': '#00FF88',
    'red': '#FF4444',
    'blue': '#3B82F6',
    'yellow': '#FFD700',
    'grid': '#1C2333',
}


def _get_conn():
    _conn = sqlite3.connect(str(DB_PATH), timeout=60)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=60000")
    return _conn


def _get_exit_price(conn, coin: str, entry_date: str, max_hold_days: int = 7) -> tuple:
    """Find exit price for a BUY trade.

    Exit when:
      1. Next SELL/STRONG SELL signal for the same coin arrives, OR
      2. After max_hold_days, whichever comes first.

    Returns: (exit_price, exit_date, exit_reason)
    """
    entry_dt = datetime.strptime(entry_date, '%Y-%m-%d')
    max_date = (entry_dt + timedelta(days=max_hold_days)).strftime('%Y-%m-%d')

    # Check for SELL signal before max_hold_days
    sell_signal = conn.execute(
        "SELECT actual_price_at_prediction, prediction_date FROM predictions "
        "WHERE coin = ? AND prediction IN ('SELL', 'STRONG SELL') "
        "AND prediction_date > ? AND prediction_date <= ? "
        "AND actual_price_at_prediction IS NOT NULL AND actual_price_at_prediction > 0 "
        "ORDER BY prediction_date ASC LIMIT 1",
        (coin, entry_date, max_date)
    ).fetchone()

    if sell_signal:
        return sell_signal[0], sell_signal[1], 'sell_signal'

    # No sell signal found — exit after max_hold_days using local price data
    price_row = conn.execute(
        "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
        "AND date(timestamp, 'unixepoch') = ? "
        "ORDER BY timestamp DESC LIMIT 1",
        (coin, max_date)
    ).fetchone()

    if price_row:
        return price_row[0], max_date, 'timeout'

    # Fallback: try closest available price within +-2 days of max_date
    max_dt = datetime.strptime(max_date, '%Y-%m-%d')
    for offset_days in [1, -1, 2, -2]:
        alt_date = (max_dt + timedelta(days=offset_days)).strftime('%Y-%m-%d')
        price_row = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "AND date(timestamp, 'unixepoch') = ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (coin, alt_date)
        ).fetchone()
        if price_row:
            return price_row[0], alt_date, 'timeout'

    return None, None, 'no_data'


def _get_short_exit_price(conn, coin: str, entry_date: str, max_hold_days: int = 7) -> tuple:
    """Find exit price for a SELL (short) trade. Covers after max_hold_days.

    Returns: (exit_price, exit_date, exit_reason)
    """
    entry_dt = datetime.strptime(entry_date, '%Y-%m-%d')
    max_date = (entry_dt + timedelta(days=max_hold_days)).strftime('%Y-%m-%d')

    # For shorts we always cover after max_hold_days (no early exit on BUY signal
    # to keep logic simple and conservative)
    price_row = conn.execute(
        "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
        "AND date(timestamp, 'unixepoch') = ? "
        "ORDER BY timestamp DESC LIMIT 1",
        (coin, max_date)
    ).fetchone()

    if price_row:
        return price_row[0], max_date, 'timeout'

    # Fallback
    max_dt = datetime.strptime(max_date, '%Y-%m-%d')
    for offset_days in [1, -1, 2, -2]:
        alt_date = (max_dt + timedelta(days=offset_days)).strftime('%Y-%m-%d')
        price_row = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "AND date(timestamp, 'unixepoch') = ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (coin, alt_date)
        ).fetchone()
        if price_row:
            return price_row[0], alt_date, 'timeout'

    return None, None, 'no_data'


def calculate_portfolio_performance(days: int = 30) -> dict:
    """Simulate portfolio performance from prediction signals.

    For each BUY/STRONG BUY: buy at actual_price_at_prediction, exit on
        next SELL or after 7 days.
    For each SELL/STRONG SELL: short at actual_price_at_prediction, cover
        after 7 days.

    Returns dict with total_return_pct, btc_return_pct, alpha, win_rate,
    n_trades, best_trade, worst_trade, equity_curve, trades list.
    """
    conn = _get_conn()
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=days)).strftime('%Y-%m-%d')

    # Fetch all actionable predictions in the period
    rows = conn.execute(
        "SELECT id, coin, prediction_date, prediction, signal_score, "
        "actual_price_at_prediction "
        "FROM predictions "
        "WHERE prediction_date >= ? "
        "AND prediction IN ('BUY', 'STRONG BUY', 'SELL', 'STRONG SELL') "
        "AND actual_price_at_prediction IS NOT NULL "
        "AND actual_price_at_prediction > 0 "
        "ORDER BY prediction_date ASC",
        (cutoff,)
    ).fetchall()

    if not rows:
        conn.close()
        return {
            'total_return_pct': 0.0,
            'btc_return_pct': 0.0,
            'alpha': 0.0,
            'win_rate': 0.0,
            'n_trades': 0,
            'best_trade': None,
            'worst_trade': None,
            'equity_curve': [],
            'trades': [],
            'days': days,
        }

    trades = []
    for row in rows:
        pred_id, coin, pred_date, prediction, score, entry_price = row
        is_buy = prediction in ('BUY', 'STRONG BUY')

        if is_buy:
            exit_price, exit_date, exit_reason = _get_exit_price(conn, coin, pred_date)
        else:
            exit_price, exit_date, exit_reason = _get_short_exit_price(conn, coin, pred_date)

        if exit_price is None or exit_price <= 0:
            continue  # Skip trades we can't resolve

        # Calculate return
        if is_buy:
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        else:
            # Short: profit when price goes down
            pnl_pct = ((entry_price - exit_price) / entry_price) * 100

        trades.append({
            'id': pred_id,
            'coin': coin,
            'entry_date': pred_date,
            'exit_date': exit_date,
            'signal': prediction,
            'score': score,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl_pct': round(pnl_pct, 2),
            'exit_reason': exit_reason,
            'is_buy': is_buy,
        })

    if not trades:
        conn.close()
        return {
            'total_return_pct': 0.0,
            'btc_return_pct': 0.0,
            'alpha': 0.0,
            'win_rate': 0.0,
            'n_trades': 0,
            'best_trade': None,
            'worst_trade': None,
            'equity_curve': [],
            'trades': [],
            'days': days,
        }

    # Sort trades by entry date
    trades.sort(key=lambda t: t['entry_date'])

    # Build equity curve — equal weight per trade, cumulative
    # Each trade contributes pnl_pct / n_trades to the portfolio (simple average approach)
    equity = 100.0  # Start at 100%
    equity_curve = [(trades[0]['entry_date'], 0.0)]  # (date, cumulative_pct)

    for t in trades:
        # Each trade moves equity by its pnl proportionally
        # Use equal allocation: each trade gets 1/active_capital weight
        trade_impact = t['pnl_pct'] / max(len(trades), 1) * len(trades) * 0.1
        # Simpler: just track cumulative return as average of all trades so far
        pass

    # Simpler equity curve: track running average return
    cumulative = 0.0
    equity_points = []
    for i, t in enumerate(trades):
        cumulative += t['pnl_pct']
        avg_return = cumulative / (i + 1)
        equity_points.append((t['exit_date'] or t['entry_date'], cumulative, avg_return))

    # Equity curve based on cumulative P&L (sum of individual trade returns)
    equity_curve = []
    running_pnl = 0.0
    for t in trades:
        running_pnl += t['pnl_pct']
        equity_curve.append((t['exit_date'] or t['entry_date'], round(running_pnl, 2)))

    # BTC buy-and-hold over the same period
    btc_start_row = conn.execute(
        "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
        "AND date(timestamp, 'unixepoch') >= ? "
        "ORDER BY timestamp ASC LIMIT 1",
        (cutoff,)
    ).fetchone()

    btc_end_row = conn.execute(
        "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
        "ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()

    btc_return_pct = 0.0
    if btc_start_row and btc_end_row and btc_start_row[0] > 0:
        btc_return_pct = ((btc_end_row[0] - btc_start_row[0]) / btc_start_row[0]) * 100

    # BTC equity curve for comparison (daily)
    btc_daily = conn.execute(
        "SELECT date(timestamp, 'unixepoch') as d, close FROM prices "
        "WHERE coin = 'BTC' AND timeframe = '1d' "
        "AND date(timestamp, 'unixepoch') >= ? "
        "ORDER BY timestamp ASC",
        (cutoff,)
    ).fetchall()

    btc_curve = []
    if btc_daily and btc_daily[0][1] > 0:
        btc_base = btc_daily[0][1]
        for d, close in btc_daily:
            btc_pct = ((close - btc_base) / btc_base) * 100
            btc_curve.append((d, round(btc_pct, 2)))

    conn.close()

    # Statistics
    n_trades = len(trades)
    wins = [t for t in trades if t['pnl_pct'] > 0]
    win_rate = (len(wins) / n_trades * 100) if n_trades > 0 else 0.0
    total_return = sum(t['pnl_pct'] for t in trades) / n_trades if n_trades > 0 else 0.0
    alpha = total_return - btc_return_pct

    best_trade = max(trades, key=lambda t: t['pnl_pct']) if trades else None
    worst_trade = min(trades, key=lambda t: t['pnl_pct']) if trades else None

    return {
        'total_return_pct': round(total_return, 2),
        'btc_return_pct': round(btc_return_pct, 2),
        'alpha': round(alpha, 2),
        'win_rate': round(win_rate, 1),
        'n_trades': n_trades,
        'best_trade': best_trade,
        'worst_trade': worst_trade,
        'equity_curve': equity_curve,
        'btc_curve': btc_curve,
        'trades': trades,
        'days': days,
    }


def chart_portfolio_performance(perf_data: dict, output_path: str = None,
                                vertical: bool = False) -> Optional[str]:
    """Generate portfolio performance chart — FORTIX signals vs BTC buy-and-hold.

    Dark theme matching existing charts (#0D1117 bg).
    Green line = FORTIX signal portfolio equity curve.
    Gray line = BTC buy-and-hold.
    Yellow title, FORTIX watermark, responsive fonts for vertical mode.
    """
    equity_curve = perf_data.get('equity_curve', [])
    btc_curve = perf_data.get('btc_curve', [])

    if not equity_curve and not btc_curve:
        log.warning("No data for portfolio performance chart")
        return None

    # Responsive font sizes
    is_vert = vertical
    _fs_title = 22 if is_vert else 16
    _fs_subtitle = 16 if is_vert else 12
    _fs_label = 14 if is_vert else 11
    _fs_tick = 13 if is_vert else 10
    _fs_legend = 14 if is_vert else 11
    _fs_watermark = 12 if is_vert else 10
    _fs_stats = 15 if is_vert else 11

    figsize = (9, 14) if is_vert else (16, 9)
    dpi = 200 if is_vert else 150

    # Setup dark style
    plt.rcParams.update({
        'figure.facecolor': COLORS['bg'],
        'axes.facecolor': COLORS['bg_secondary'],
        'axes.edgecolor': COLORS['grid'],
        'axes.labelcolor': COLORS['text'],
        'text.color': COLORS['text'],
        'xtick.color': COLORS['text_dim'],
        'ytick.color': COLORS['text_dim'],
        'grid.color': COLORS['grid'],
        'grid.alpha': 0.5 if is_vert else 0.3,
        'font.family': 'sans-serif',
        'font.size': 20 if is_vert else 12,
    })

    fig, ax = plt.subplots(figsize=figsize)

    # Plot BTC buy-and-hold (gray, behind)
    if btc_curve:
        btc_dates = []
        btc_vals = []
        for d, v in btc_curve:
            try:
                btc_dates.append(datetime.strptime(d, '%Y-%m-%d'))
            except (ValueError, TypeError):
                continue
            btc_vals.append(v)

        if btc_dates:
            ax.plot(btc_dates, btc_vals, color=COLORS['text_dim'], linewidth=2.0,
                    alpha=0.7, label=f"BTC Hold ({perf_data.get('btc_return_pct', 0):+.1f}%)",
                    linestyle='--')
            ax.fill_between(btc_dates, 0, btc_vals, alpha=0.05, color=COLORS['text_dim'])

    # Plot FORTIX signal portfolio (green, on top)
    if equity_curve:
        eq_dates = []
        eq_vals = []
        for d, v in equity_curve:
            try:
                eq_dates.append(datetime.strptime(d, '%Y-%m-%d'))
            except (ValueError, TypeError):
                continue
            eq_vals.append(v)

        if eq_dates:
            line_color = COLORS['green'] if perf_data.get('total_return_pct', 0) >= 0 else COLORS['red']
            ax.plot(eq_dates, eq_vals, color=line_color, linewidth=2.5,
                    label=f"FORTIX Signals ({perf_data.get('total_return_pct', 0):+.1f}%)",
                    zorder=5)
            fill_color = COLORS['green'] if perf_data.get('total_return_pct', 0) >= 0 else COLORS['red']
            ax.fill_between(eq_dates, 0, eq_vals, alpha=0.08, color=fill_color)

    # Zero line
    ax.axhline(y=0, color=COLORS['text_dim'], linewidth=1, linestyle='-', alpha=0.4)

    # Title
    days = perf_data.get('days', 30)
    ax.set_title(f'FORTIX SIGNAL PERFORMANCE ({days}D)',
                 fontsize=_fs_title, fontweight='bold',
                 color=COLORS['yellow'], pad=15)

    # Axis labels
    ax.set_ylabel('Cumulative Return (%)', fontsize=_fs_label, color=COLORS['text_dim'])
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, fontsize=_fs_tick)
    ax.tick_params(axis='y', labelsize=_fs_tick)

    # Legend
    ax.legend(loc='upper left', fontsize=_fs_legend, framealpha=0.3,
              edgecolor=COLORS['grid'])
    ax.grid(True, alpha=0.12, axis='y')

    # Stats bar at bottom
    alpha_val = perf_data.get('alpha', 0)
    win_rate = perf_data.get('win_rate', 0)
    n_trades = perf_data.get('n_trades', 0)
    alpha_color = COLORS['green'] if alpha_val >= 0 else COLORS['red']

    stats_text = (
        f"Alpha: {alpha_val:+.1f}% vs BTC  |  "
        f"Win Rate: {win_rate:.0f}%  |  "
        f"{n_trades} trades"
    )
    fig.text(0.5, 0.02, stats_text, fontsize=_fs_stats, color=alpha_color,
             ha='center', va='bottom', fontweight='bold')

    # Best/worst trade annotations
    best = perf_data.get('best_trade')
    worst = perf_data.get('worst_trade')
    if best and worst:
        detail_text = (
            f"Best: {best['coin']} {best['signal']} {best['pnl_pct']:+.1f}%  |  "
            f"Worst: {worst['coin']} {worst['signal']} {worst['pnl_pct']:+.1f}%"
        )
        fig.text(0.5, 0.06, detail_text, fontsize=_fs_tick, color=COLORS['text_dim'],
                 ha='center', va='bottom')

    # FORTIX watermark
    fig.text(0.99, 0.01, 'FORTIX', fontsize=_fs_watermark, color=COLORS['text_dim'],
             ha='right', va='bottom', alpha=0.5)

    plt.tight_layout(rect=[0, 0.08, 1, 1])

    if not output_path:
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(CHARTS_DIR / 'portfolio_performance.png')
    else:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    plt.savefig(output_path, dpi=dpi, bbox_inches='tight',
                facecolor=COLORS['bg'], edgecolor='none')
    plt.close()

    log.info(f"Portfolio performance chart saved: {output_path}")
    return output_path


def get_performance_summary(days: int = 30) -> str:
    """Return formatted text string for script injection.

    Example: "Over the last 30 days, following our signals would have returned
    +X% versus Bitcoin's Y%. That's Z% alpha. Win rate: W% across N trades."
    """
    perf = calculate_portfolio_performance(days)

    if perf['n_trades'] == 0:
        return "PORTFOLIO PERFORMANCE: Not enough evaluated trades for performance tracking yet."

    total = perf['total_return_pct']
    btc = perf['btc_return_pct']
    alpha = perf['alpha']
    wr = perf['win_rate']
    n = perf['n_trades']

    direction = "returned" if total >= 0 else "lost"
    alpha_word = "outperformance" if alpha >= 0 else "underperformance"

    summary = (
        f"Over the last {days} days, following our signals would have "
        f"{direction} {total:+.1f}% versus Bitcoin's {btc:+.1f}%. "
        f"That's {alpha:+.1f}% {alpha_word}. "
        f"Win rate: {wr:.0f}% across {n} trades."
    )

    return summary


def get_portfolio_context(days: int = 30) -> str:
    """Return portfolio performance context for script injection.

    Called from script_generator.py to add performance data to all video types.
    Returns a multi-line context block with detailed trade stats.
    """
    try:
        perf = calculate_portfolio_performance(days)
    except Exception as e:
        log.warning(f"Portfolio performance calculation failed: {e}")
        return "SIGNAL PORTFOLIO PERFORMANCE: Calculation unavailable."

    if perf['n_trades'] == 0:
        return "SIGNAL PORTFOLIO PERFORMANCE: Not enough evaluated trades yet."

    lines = [f"SIGNAL PORTFOLIO PERFORMANCE (last {days} days):"]
    lines.append(f"  Average return per trade: {perf['total_return_pct']:+.1f}%")
    lines.append(f"  BTC buy-and-hold: {perf['btc_return_pct']:+.1f}%")
    lines.append(f"  Alpha vs BTC: {perf['alpha']:+.1f}%")
    lines.append(f"  Win rate: {perf['win_rate']:.0f}% ({perf['n_trades']} trades)")

    best = perf.get('best_trade')
    worst = perf.get('worst_trade')
    if best:
        lines.append(f"  Best trade: {best['coin']} {best['signal']} → {best['pnl_pct']:+.1f}%")
    if worst:
        lines.append(f"  Worst trade: {worst['coin']} {worst['signal']} → {worst['pnl_pct']:+.1f}%")

    # Trade breakdown by signal type
    trades = perf.get('trades', [])
    buy_trades = [t for t in trades if t['is_buy']]
    sell_trades = [t for t in trades if not t['is_buy']]

    if buy_trades:
        buy_avg = sum(t['pnl_pct'] for t in buy_trades) / len(buy_trades)
        buy_wr = len([t for t in buy_trades if t['pnl_pct'] > 0]) / len(buy_trades) * 100
        lines.append(f"  BUY trades: {len(buy_trades)}, avg {buy_avg:+.1f}%, win rate {buy_wr:.0f}%")

    if sell_trades:
        sell_avg = sum(t['pnl_pct'] for t in sell_trades) / len(sell_trades)
        sell_wr = len([t for t in sell_trades if t['pnl_pct'] > 0]) / len(sell_trades) * 100
        lines.append(f"  SELL trades: {len(sell_trades)}, avg {sell_avg:+.1f}%, win rate {sell_wr:.0f}%")

    return '\n'.join(lines)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX — Portfolio Performance Tracker')
    parser.add_argument('--days', type=int, default=30, help='Lookback period in days')
    parser.add_argument('--chart', action='store_true', help='Generate performance chart')
    parser.add_argument('--vertical', action='store_true', help='Vertical (9:16) chart')

    args = parser.parse_args()

    log.info("=" * 60)
    log.info("FORTIX — Portfolio Performance Tracker")
    log.info("=" * 60)

    perf = calculate_portfolio_performance(args.days)

    print(f"\n{'=' * 60}")
    print(f"  FORTIX Signal Portfolio — {args.days} Day Performance")
    print(f"{'=' * 60}")
    print(f"  Avg return per trade: {perf['total_return_pct']:+.1f}%")
    print(f"  BTC buy-and-hold:     {perf['btc_return_pct']:+.1f}%")
    print(f"  Alpha vs BTC:         {perf['alpha']:+.1f}%")
    print(f"  Win rate:             {perf['win_rate']:.0f}%")
    print(f"  Total trades:         {perf['n_trades']}")

    if perf['best_trade']:
        b = perf['best_trade']
        print(f"  Best:  {b['coin']} {b['signal']} → {b['pnl_pct']:+.1f}% ({b['entry_date']})")
    if perf['worst_trade']:
        w = perf['worst_trade']
        print(f"  Worst: {w['coin']} {w['signal']} → {w['pnl_pct']:+.1f}% ({w['entry_date']})")

    print(f"\n  Equity curve: {len(perf['equity_curve'])} points")
    print(f"  BTC curve: {len(perf['btc_curve'])} points")

    # Print trades table
    if perf['trades']:
        print(f"\n  {'Coin':<8} {'Signal':<14} {'Entry':>10} {'Exit':>10} {'P&L':>8} {'Reason':<12}")
        print(f"  {'-' * 66}")
        for t in perf['trades'][:20]:
            ep = f"${t['entry_price']:,.0f}" if t['entry_price'] >= 1 else f"${t['entry_price']:.6f}"
            xp = f"${t['exit_price']:,.0f}" if t['exit_price'] >= 1 else f"${t['exit_price']:.6f}"
            print(f"  {t['coin']:<8} {t['signal']:<14} {ep:>10} {xp:>10} "
                  f"{t['pnl_pct']:>+7.1f}% {t['exit_reason']:<12}")
        if len(perf['trades']) > 20:
            print(f"  ... and {len(perf['trades']) - 20} more trades")

    print(f"\n  Summary: {get_performance_summary(args.days)}")
    print(f"{'=' * 60}\n")

    if args.chart:
        path = chart_portfolio_performance(perf, vertical=args.vertical)
        if path:
            log.info(f"Chart saved: {path}")
