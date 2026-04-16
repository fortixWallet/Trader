"""
Profi Learner — Self-Improvement from Own Trades
==================================================
After each closed trade, Profi analyzes:
- What did I predict? What actually happened?
- Why was I right/wrong?
- What would I do differently?
- What pattern should I remember?

Lessons are saved and fed back into Profi's knowledge base.
Runs daily at 02:00 UTC alongside model retrain.
"""

import os
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

import anthropic

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
LESSONS_PATH = Path(__file__).parent / 'profi_lessons.md'
KNOWLEDGE_PATH = Path(__file__).parent / 'profi_knowledge.md'


def analyze_recent_trades(days: int = 1) -> str:
    """Analyze trades from last N days, generate lessons."""
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return ""

    conn = sqlite3.connect(str(DB_PATH))

    trades = conn.execute("""
        SELECT coin, direction, entry_price, exit_price, pnl_pct, pnl_usd,
               exit_reason, regime, reg_score, held_minutes, leverage,
               entry_time, exit_time
        FROM okx_trades
        WHERE entry_time > datetime('now', ?)
        ORDER BY entry_time
    """, (f'-{days} days',)).fetchall()

    if not trades:
        conn.close()
        logger.info("No recent trades to analyze")
        return ""

    # Build trade summaries
    trade_summaries = []
    for t in trades:
        coin, direction, entry, exit_p, pnl_pct, pnl_usd = t[0], t[1], t[2], t[3], t[4], t[5]
        reason, regime, score, mins, lev = t[6], t[7], t[8], t[9], t[10]

        # Get what happened AFTER exit (did we leave money on table?)
        if exit_p and entry:
            after = conn.execute("""
                SELECT MIN(close), MAX(close) FROM prices
                WHERE coin=? AND timeframe='4h'
                AND timestamp > strftime('%s', ?)
                AND timestamp < strftime('%s', ?, '+12 hours')
            """, (coin, t[12] or '2026-01-01', t[12] or '2026-01-01')).fetchone()

            after_low = after[0] if after and after[0] else exit_p
            after_high = after[1] if after and after[1] else exit_p

            if direction == 'LONG':
                missed = (after_high / exit_p - 1) * 100 if exit_p else 0
                worse = (after_low / exit_p - 1) * 100 if exit_p else 0
            else:
                missed = (exit_p / after_low - 1) * 100 if after_low else 0
                worse = (exit_p / after_high - 1) * 100 if after_high else 0
        else:
            missed = 0
            worse = 0

        summary = (
            f"{direction} {coin} {lev}x | Entry ${entry:.4f} → Exit ${exit_p:.4f} | "
            f"PnL: {pnl_pct*100 if pnl_pct else 0:+.2f}% (${pnl_usd or 0:+.2f}) | "
            f"Exit: {reason} after {mins or 0:.0f}min | Regime: {regime} | "
            f"After exit: could have gained {missed:+.1f}% more or lost {worse:.1f}% more"
        )
        trade_summaries.append(summary)

    conn.close()

    if not trade_summaries:
        return ""

    # Ask Profi to analyze its own trades
    client = anthropic.Anthropic(api_key=api_key, timeout=60.0, max_retries=2)

    prompt = f"""You are reviewing YOUR OWN recent trades. Be brutally honest.

TRADES ({len(trade_summaries)} total):
{chr(10).join(trade_summaries)}

For EACH trade, analyze:
1. Was the direction correct?
2. Was the entry timing good?
3. Was the exit reason optimal, or did we leave money on table / hold too long?
4. What specific pattern or signal was I right/wrong about?

Then write LESSONS — specific, actionable rules I should follow next time:
- "When [specific situation], do [specific action] because [specific reason]"
- Keep each lesson to 1-2 sentences
- Focus on MISTAKES more than successes — mistakes teach more
- Include exact numbers where possible (e.g., "RSI below 25 on SOL bounces 72% of time")

Format:
## Trade Review {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

### Wins:
[analysis]

### Losses:
[analysis]

### LESSONS LEARNED:
1. [lesson]
2. [lesson]
...
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        analysis = response.content[0].text if response.content else ""
        logger.info(f"Trade review: {len(trades)} trades analyzed, {len(analysis)} chars")
        return analysis
    except Exception as e:
        logger.error(f"Trade review failed: {e}")
        return ""


def update_lessons(new_analysis: str):
    """Append new lessons to lessons file, keep last 30 days."""
    existing = ""
    if LESSONS_PATH.exists():
        existing = LESSONS_PATH.read_text()

    # Keep only last 50KB (roughly 30 days of lessons)
    combined = new_analysis + "\n\n---\n\n" + existing
    if len(combined) > 50000:
        combined = combined[:50000]

    LESSONS_PATH.write_text(combined)
    logger.info(f"Lessons updated: {len(combined):,} chars")


def compile_all_lessons() -> str:
    """Read all lessons for Profi's knowledge base."""
    if LESSONS_PATH.exists():
        return LESSONS_PATH.read_text()
    return ""


def run_daily_review():
    """Run full daily trade review + lesson extraction."""
    # Load env
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    analysis = analyze_recent_trades(days=1)
    if analysis:
        update_lessons(analysis)
        logger.info("Daily trade review complete")
    else:
        logger.info("No trades to review today")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    run_daily_review()
    print(f"\nLessons file: {LESSONS_PATH}")
    if LESSONS_PATH.exists():
        print(LESSONS_PATH.read_text()[:2000])
