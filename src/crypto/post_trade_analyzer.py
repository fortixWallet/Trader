"""
FORTIX Post-Trade Analyzer — Learn from Every Trade
====================================================

After each trade closes, analyzes WHY it won or lost.
Updates signal confidence scores based on real trading results.
Feeds back into the system to improve future trades.
"""

import sqlite3
import logging
import json
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
TRADES_DB = _FACTORY_DIR / 'data' / 'crypto' / 'trades.db'
MARKET_DB = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
LEARNING_FILE = _FACTORY_DIR / 'data' / 'crypto' / 'trade_learning.json'


def analyze_closed_trade(trade: dict) -> dict:
    """Analyze a single closed trade.

    Returns analysis with: why_won/lost, signal_quality, lessons.
    """
    coin = trade.get('coin', '')
    direction = trade.get('direction', '')
    pnl = trade.get('pnl_usdt', 0)
    entry = trade.get('entry_price', 0)
    exit_price = trade.get('exit_price', 0)
    signal_type = trade.get('signal_type', '')
    exit_reason = trade.get('exit_reason', '')
    duration = trade.get('duration_hours', 0)

    won = pnl > 0

    analysis = {
        'coin': coin,
        'direction': direction,
        'won': won,
        'pnl': pnl,
        'signal_type': signal_type,
        'exit_reason': exit_reason,
        'duration_hours': duration,
        'lessons': [],
    }

    # Lesson 1: Exit reason quality
    if exit_reason == 'HARD_STOP':
        analysis['lessons'].append(f"Stop hit — signal was wrong or entry timing was bad")
    elif exit_reason == 'TRAILING_STOP' and won:
        analysis['lessons'].append(f"Trailing stop locked profit — good trade management")
    elif exit_reason == 'TP1':
        analysis['lessons'].append(f"Took partial profit — position management working")
    elif exit_reason == 'TIME_EXIT':
        analysis['lessons'].append(f"Timed out after {duration:.0f}h — signal was too slow")
    elif exit_reason == 'FUNDING_DRAIN':
        analysis['lessons'].append(f"Funding fees exceeded profit — avoid holding against funding")
    elif exit_reason == 'REVERSAL':
        analysis['lessons'].append(f"Signal reversed — system detected regime change")

    # Lesson 2: Duration analysis
    if won and duration and duration < 4:
        analysis['lessons'].append(f"Quick win ({duration:.1f}h) — strong signal")
    elif not won and duration and duration > 24:
        analysis['lessons'].append(f"Slow loss ({duration:.1f}h) — should have exited earlier")

    return analysis


def update_signal_confidence(closed_trades: list):
    """Update signal type confidence based on real trade results."""
    signal_stats = {}

    for trade in closed_trades:
        sig = trade.get('signal_type', 'unknown')
        if sig not in signal_stats:
            signal_stats[sig] = {'wins': 0, 'losses': 0, 'total_pnl': 0}

        if trade.get('pnl_usdt', 0) > 0:
            signal_stats[sig]['wins'] += 1
        else:
            signal_stats[sig]['losses'] += 1
        signal_stats[sig]['total_pnl'] += trade.get('pnl_usdt', 0)

    # Save to learning file
    learning = {}
    if LEARNING_FILE.exists():
        try:
            learning = json.loads(LEARNING_FILE.read_text())
        except Exception:
            pass

    learning['signal_performance'] = signal_stats
    learning['last_updated'] = datetime.now(timezone.utc).isoformat()
    learning['total_trades'] = sum(s['wins'] + s['losses'] for s in signal_stats.values())

    LEARNING_FILE.write_text(json.dumps(learning, indent=2))
    logger.info(f"Updated signal confidence from {learning['total_trades']} trades")

    return signal_stats


def get_signal_confidence_adjustment(signal_type: str) -> float:
    """Get confidence adjustment for a signal type based on real trading.

    Returns multiplier: >1.0 = signal performs better than expected,
                         <1.0 = worse than expected.
    """
    if not LEARNING_FILE.exists():
        return 1.0

    try:
        learning = json.loads(LEARNING_FILE.read_text())
        stats = learning.get('signal_performance', {}).get(signal_type)
        if not stats:
            return 1.0

        total = stats['wins'] + stats['losses']
        if total < 5:  # need at least 5 trades to judge
            return 1.0

        win_rate = stats['wins'] / total
        # Expected ~65% for our signals
        if win_rate > 0.75:
            return 1.2  # performing great
        elif win_rate > 0.60:
            return 1.0  # as expected
        elif win_rate > 0.45:
            return 0.8  # underperforming
        else:
            return 0.5  # badly underperforming

    except Exception:
        return 1.0


def run_post_trade_analysis():
    """Analyze all closed trades and update learning."""
    try:
        conn = sqlite3.connect(str(TRADES_DB))
        trades = conn.execute(
            "SELECT * FROM trades WHERE status='closed' ORDER BY exit_time DESC"
        ).fetchall()

        if not trades:
            logger.info("No closed trades to analyze")
            return

        columns = [desc[0] for desc in conn.execute("SELECT * FROM trades LIMIT 0").description]
        trade_dicts = [dict(zip(columns, t)) for t in trades]

        analyses = []
        for trade in trade_dicts:
            analysis = analyze_closed_trade(trade)
            analyses.append(analysis)

        # Update confidence
        signal_stats = update_signal_confidence(trade_dicts)

        # Summary
        wins = sum(1 for a in analyses if a['won'])
        losses = len(analyses) - wins
        total_pnl = sum(a['pnl'] for a in analyses)

        logger.info(f"Post-trade analysis: {wins}W/{losses}L, PnL: ${total_pnl:+.2f}")
        for sig, stats in signal_stats.items():
            total = stats['wins'] + stats['losses']
            wr = stats['wins'] / total * 100 if total > 0 else 0
            logger.info(f"  {sig}: {wr:.0f}% win rate ({total} trades)")

        conn.close()
        return analyses

    except Exception as e:
        logger.error(f"Post-trade analysis failed: {e}")
        return []
