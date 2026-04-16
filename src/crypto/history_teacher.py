"""
History Teacher — Extract Trading Lessons from 2 Years of Market Data
=====================================================================
Instead of asking Profi to analyze 730 days (expensive),
we extract statistical lessons from data and ask Profi to
synthesize them into actionable rules (1 API call).

Lessons extracted:
1. What happened after major crashes (>10% drops)
2. What happened after major pumps (>10% gains)
3. Best/worst performing strategies by market condition
4. Regime transition patterns
5. Funding rate extreme outcomes
6. Volume spike → direction prediction accuracy
7. RSI extreme → bounce/continuation rates
8. Day-of-week profitability
9. Which coins lead, which lag
10. Biggest missed opportunities and biggest traps
"""

import os
import json
import sqlite3
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import anthropic

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
LESSONS_PATH = Path(__file__).parent / 'profi_history_lessons.md'

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO']


def extract_all_lessons():
    """Extract statistical lessons from 2+ years of data."""
    conn = sqlite3.connect(str(DB_PATH))
    sections = []

    sections.append(_crash_recovery_patterns(conn))
    sections.append(_pump_dump_patterns(conn))
    sections.append(_regime_performance(conn))
    sections.append(_funding_extreme_lessons(conn))
    sections.append(_rsi_actionable_levels(conn))
    sections.append(_volume_breakout_accuracy(conn))
    sections.append(_leader_lagger_analysis(conn))
    sections.append(_biggest_opportunities_missed(conn))
    sections.append(_weekend_vs_weekday(conn))
    sections.append(_consecutive_days_patterns(conn))

    conn.close()

    raw_data = "\n\n".join(s for s in sections if s)
    logger.info(f"Raw statistical lessons: {len(raw_data):,} chars")

    # Now ask Profi to synthesize into actionable rules
    lessons = _synthesize_with_claude(raw_data)
    if lessons:
        LESSONS_PATH.write_text(lessons)
        logger.info(f"History lessons saved: {len(lessons):,} chars → {LESSONS_PATH}")

    return lessons


def _crash_recovery_patterns(conn):
    """After BTC drops >5% in a day, what happens next 1/3/7 days?"""
    lines = ["## CRASH RECOVERY PATTERNS"]

    for coin in ['BTC', 'ETH', 'SOL']:
        rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 100:
            continue

        for thresh in [5, 10, 15]:
            recoveries_1d = []
            recoveries_3d = []
            recoveries_7d = []

            for i in range(1, len(rows) - 7):
                daily_ret = (rows[i][1] / rows[i-1][1] - 1) * 100
                if daily_ret < -thresh:
                    if i + 1 < len(rows):
                        recoveries_1d.append((rows[i+1][1] / rows[i][1] - 1) * 100)
                    if i + 3 < len(rows):
                        recoveries_3d.append((rows[i+3][1] / rows[i][1] - 1) * 100)
                    if i + 7 < len(rows):
                        recoveries_7d.append((rows[i+7][1] / rows[i][1] - 1) * 100)

            if recoveries_1d:
                lines.append(
                    f"{coin} after >{thresh}% daily drop ({len(recoveries_1d)} events): "
                    f"1d avg {np.mean(recoveries_1d):+.1f}% (up {sum(1 for r in recoveries_1d if r > 0)/len(recoveries_1d)*100:.0f}%), "
                    f"3d avg {np.mean(recoveries_3d):+.1f}%, "
                    f"7d avg {np.mean(recoveries_7d):+.1f}%"
                )

    return "\n".join(lines)


def _pump_dump_patterns(conn):
    """After a coin pumps >10% in a day, what happens?"""
    lines = ["## PUMP → DUMP PATTERNS"]

    for coin in COINS[:10]:
        rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 100:
            continue

        for thresh in [10, 20]:
            dumps_after = []
            for i in range(1, len(rows) - 3):
                daily_ret = (rows[i][1] / rows[i-1][1] - 1) * 100
                if daily_ret > thresh:
                    next_3d = (rows[min(i+3, len(rows)-1)][1] / rows[i][1] - 1) * 100
                    dumps_after.append(next_3d)

            if len(dumps_after) >= 3:
                dump_pct = sum(1 for d in dumps_after if d < 0) / len(dumps_after) * 100
                lines.append(
                    f"{coin} after +{thresh}%+ pump ({len(dumps_after)} events): "
                    f"dumped {dump_pct:.0f}% of time, avg 3d return {np.mean(dumps_after):+.1f}%"
                )

    return "\n".join(lines)


def _regime_performance(conn):
    """Performance of LONG vs SHORT in different regimes."""
    lines = ["## REGIME → STRATEGY PERFORMANCE"]

    btc = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()
    if len(btc) < 60:
        return ""

    # Define regimes
    regimes = {}
    for i in range(30, len(btc)):
        ret_30d = (btc[i][1] / btc[i-30][1] - 1) * 100
        date = datetime.utcfromtimestamp(btc[i][0]).strftime('%Y-%m-%d')
        if ret_30d > 10:
            regimes[date] = 'STRONG_BULL'
        elif ret_30d > 0:
            regimes[date] = 'BULL'
        elif ret_30d > -10:
            regimes[date] = 'BEAR'
        else:
            regimes[date] = 'STRONG_BEAR'

    # For each regime, what's the avg daily return of alts?
    for regime_name in ['STRONG_BULL', 'BULL', 'BEAR', 'STRONG_BEAR']:
        regime_dates = [d for d, r in regimes.items() if r == regime_name]
        if len(regime_dates) < 10:
            continue

        alt_returns = []
        for coin in ['ETH', 'SOL', 'DOGE', 'AVAX']:
            rows = conn.execute(
                "SELECT date(timestamp,'unixepoch'), close FROM prices "
                "WHERE coin=? AND timeframe='1d' ORDER BY timestamp",
                (coin,)
            ).fetchall()
            close_by_date = {r[0]: r[1] for r in rows}

            for i, d in enumerate(regime_dates[:-1]):
                next_d = regime_dates[i+1] if i+1 < len(regime_dates) else None
                if d in close_by_date and next_d and next_d in close_by_date:
                    ret = (close_by_date[next_d] / close_by_date[d] - 1) * 100
                    alt_returns.append(ret)

        if alt_returns:
            lines.append(
                f"{regime_name} ({len(regime_dates)} days): "
                f"avg alt daily return {np.mean(alt_returns):+.2f}%, "
                f"positive {sum(1 for r in alt_returns if r > 0)/len(alt_returns)*100:.0f}%"
            )

    return "\n".join(lines)


def _funding_extreme_lessons(conn):
    """What happens after extreme funding rates."""
    lines = ["## FUNDING RATE EXTREMES → OUTCOMES"]

    for coin in ['BTC', 'ETH', 'SOL', 'DOGE']:
        rates = conn.execute("""
            SELECT f.timestamp, f.rate,
                   (SELECT close FROM prices p WHERE p.coin=f.coin AND p.timeframe='4h'
                    AND p.timestamp <= f.timestamp ORDER BY p.timestamp DESC LIMIT 1) as price
            FROM funding_rates f WHERE f.coin=? AND f.rate IS NOT NULL
            ORDER BY f.timestamp
        """, (coin,)).fetchall()

        if len(rates) < 50:
            continue

        # Extreme positive (>0.05%)
        high_fr = [(r[0], r[1], r[2]) for r in rates if r[1] > 0.0005 and r[2]]
        if len(high_fr) >= 5:
            next_rets = []
            for ts, rate, price in high_fr:
                np_row = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "AND timestamp > ? ORDER BY timestamp LIMIT 1 OFFSET 6",
                    (coin, ts)
                ).fetchone()
                if np_row and np_row[0]:
                    next_rets.append((np_row[0] / price - 1) * 100)

            if next_rets:
                lines.append(
                    f"{coin} after funding >0.05% ({len(next_rets)} events): "
                    f"avg 24h return {np.mean(next_rets):+.1f}%, "
                    f"dropped {sum(1 for r in next_rets if r < 0)/len(next_rets)*100:.0f}%"
                )

        # Extreme negative (<-0.03%)
        low_fr = [(r[0], r[1], r[2]) for r in rates if r[1] < -0.0003 and r[2]]
        if len(low_fr) >= 5:
            next_rets = []
            for ts, rate, price in low_fr:
                np_row = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "AND timestamp > ? ORDER BY timestamp LIMIT 1 OFFSET 6",
                    (coin, ts)
                ).fetchone()
                if np_row and np_row[0]:
                    next_rets.append((np_row[0] / price - 1) * 100)

            if next_rets:
                lines.append(
                    f"{coin} after funding <-0.03% ({len(next_rets)} events): "
                    f"avg 24h return {np.mean(next_rets):+.1f}%, "
                    f"bounced {sum(1 for r in next_rets if r > 0)/len(next_rets)*100:.0f}%"
                )

    return "\n".join(lines)


def _rsi_actionable_levels(conn):
    """Exact RSI levels that produce best win rates."""
    lines = ["## RSI → BEST ENTRY LEVELS PER COIN"]

    for coin in COINS[:12]:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = np.array([r[0] for r in rows])
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_g = np.zeros(len(deltas))
        avg_l = np.zeros(len(deltas))
        if len(gains) >= 14:
            avg_g[13] = np.mean(gains[:14])
            avg_l[13] = np.mean(losses[:14])
            for i in range(14, len(deltas)):
                avg_g[i] = (avg_g[i-1]*13 + gains[i])/14
                avg_l[i] = (avg_l[i-1]*13 + losses[i])/14
        rsi = 100 - 100/(1 + avg_g/(avg_l + 1e-10))

        # Best LONG entry RSI
        best_long_rsi = None
        best_long_wr = 0
        for rsi_level in [15, 20, 25, 30]:
            entries = []
            for i in range(20, len(rsi) - 6):
                if rsi[i] < rsi_level and rsi[i-1] >= rsi_level:  # crosses below
                    ret = (closes[i+7] / closes[i+1] - 1) * 100
                    entries.append(ret)
            if len(entries) >= 5:
                wr = sum(1 for e in entries if e > 0) / len(entries) * 100
                if wr > best_long_wr:
                    best_long_wr = wr
                    best_long_rsi = rsi_level

        if best_long_rsi:
            lines.append(f"{coin}: LONG when RSI < {best_long_rsi} → {best_long_wr:.0f}% WR")

    return "\n".join(lines)


def _volume_breakout_accuracy(conn):
    """When volume spikes 3x+, does it predict direction?"""
    lines = ["## VOLUME SPIKE → DIRECTION PREDICTION"]

    for coin in COINS[:10]:
        rows = conn.execute(
            "SELECT close, volume FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = [r[0] for r in rows]
        volumes = [r[1] for r in rows]

        correct = 0
        total = 0
        for i in range(50, len(rows) - 1):
            avg_vol = np.mean(volumes[i-50:i])
            if avg_vol <= 0:
                continue
            ratio = volumes[i] / avg_vol
            if ratio >= 3.0:
                candle_dir = 1 if closes[i] > closes[i-1] else -1
                next_dir = 1 if closes[i+1] > closes[i] else -1
                if candle_dir == next_dir:
                    correct += 1
                total += 1

        if total >= 10:
            acc = correct / total * 100
            lines.append(f"{coin}: 3x volume spike → direction continues {acc:.0f}% ({total} events)")

    return "\n".join(lines)


def _leader_lagger_analysis(conn):
    """Which coins move first, which follow."""
    lines = ["## LEADER-LAGGER: WHO MOVES FIRST"]

    # BTC big move → which alt follows fastest?
    btc = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp"
    ).fetchall()

    for coin in ['ETH', 'SOL', 'DOGE', 'AVAX', 'LINK']:
        alt = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        alt_map = {r[0]: r[1] for r in alt}

        same_candle = []
        next_candle = []

        for i in range(1, len(btc) - 1):
            btc_ret = (btc[i][1] / btc[i-1][1] - 1) * 100
            ts = btc[i][0]
            ts_prev = btc[i-1][0]
            ts_next = btc[i+1][0] if i + 1 < len(btc) else None

            if abs(btc_ret) > 2 and ts in alt_map and ts_prev in alt_map:
                alt_same = (alt_map[ts] / alt_map[ts_prev] - 1) * 100
                same_candle.append((btc_ret, alt_same))

                if ts_next and ts_next in alt_map:
                    alt_next = (alt_map[ts_next] / alt_map[ts] - 1) * 100
                    next_candle.append((btc_ret, alt_next))

        if same_candle:
            avg_beta = np.mean([a/b for b, a in same_candle if b != 0])
            # Does alt continue moving next candle?
            if next_candle:
                cont = sum(1 for b, a in next_candle if (b > 0 and a > 0) or (b < 0 and a < 0))
                cont_pct = cont / len(next_candle) * 100
                lines.append(
                    f"BTC move → {coin}: beta {avg_beta:.1f}x same candle, "
                    f"continues {cont_pct:.0f}% next candle"
                )

    return "\n".join(lines)


def _biggest_opportunities_missed(conn):
    """Biggest moves that happened — what signals preceded them."""
    lines = ["## BIGGEST MOVES — WHAT SIGNALS CAME BEFORE"]

    for coin in COINS[:8]:
        rows = conn.execute(
            "SELECT timestamp, close, volume FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 100:
            continue

        # Find top 5 biggest 24h moves
        moves = []
        for i in range(6, len(rows)):
            ret = (rows[i][1] / rows[i-6][1] - 1) * 100
            if abs(ret) > 5:
                dt = datetime.utcfromtimestamp(rows[i][0])
                # What was RSI before?
                closes = [r[1] for r in rows[max(0,i-20):i]]
                if len(closes) >= 14:
                    d = np.diff(closes)
                    g = np.where(d > 0, d, 0)
                    l = np.where(d < 0, -d, 0)
                    ag = np.mean(g[-14:])
                    al = np.mean(l[-14:])
                    rsi = 100 - 100/(1 + ag/(al+1e-10))
                else:
                    rsi = 50

                vol_ratio = rows[i][2] / np.mean([r[2] for r in rows[max(0,i-50):i]]) if i > 50 else 1

                moves.append({
                    'date': dt.strftime('%Y-%m-%d'),
                    'ret': ret,
                    'rsi_before': rsi,
                    'vol_ratio': vol_ratio,
                })

        moves.sort(key=lambda x: abs(x['ret']), reverse=True)

        for m in moves[:3]:
            direction = "UP" if m['ret'] > 0 else "DOWN"
            lines.append(
                f"{coin} {m['date']}: {direction} {m['ret']:+.1f}% | "
                f"RSI before: {m['rsi_before']:.0f} | Volume: {m['vol_ratio']:.1f}x avg"
            )

    return "\n".join(lines)


def _weekend_vs_weekday(conn):
    """Weekend vs weekday performance."""
    lines = ["## WEEKEND vs WEEKDAY"]

    btc = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()

    weekend_rets = []
    weekday_rets = []
    for i in range(1, len(btc)):
        dt = datetime.utcfromtimestamp(btc[i][0])
        ret = (btc[i][1] / btc[i-1][1] - 1) * 100
        if dt.weekday() >= 5:
            weekend_rets.append(ret)
        else:
            weekday_rets.append(ret)

    if weekend_rets and weekday_rets:
        lines.append(f"BTC weekday: avg {np.mean(weekday_rets):+.3f}%, positive {sum(1 for r in weekday_rets if r > 0)/len(weekday_rets)*100:.0f}%")
        lines.append(f"BTC weekend: avg {np.mean(weekend_rets):+.3f}%, positive {sum(1 for r in weekend_rets if r > 0)/len(weekend_rets)*100:.0f}%")

    return "\n".join(lines)


def _consecutive_days_patterns(conn):
    """After N red/green days, what happens?"""
    lines = ["## CONSECUTIVE DAYS → REVERSAL PROBABILITY"]

    btc = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()
    closes = [r[0] for r in btc]

    for streak in [3, 5, 7]:
        # Red streak
        red_reversals = 0
        red_total = 0
        for i in range(streak, len(closes) - 1):
            all_red = all(closes[i-j] < closes[i-j-1] for j in range(streak))
            if all_red:
                red_total += 1
                if closes[i+1] > closes[i]:
                    red_reversals += 1

        if red_total >= 3:
            lines.append(f"BTC after {streak} red days: bounced {red_reversals/red_total*100:.0f}% ({red_total} events)")

        # Green streak
        green_reversals = 0
        green_total = 0
        for i in range(streak, len(closes) - 1):
            all_green = all(closes[i-j] > closes[i-j-1] for j in range(streak))
            if all_green:
                green_total += 1
                if closes[i+1] < closes[i]:
                    green_reversals += 1

        if green_total >= 3:
            lines.append(f"BTC after {streak} green days: pulled back {green_reversals/green_total*100:.0f}% ({green_total} events)")

    return "\n".join(lines)


def _synthesize_with_claude(raw_data: str) -> str:
    """Ask Claude to turn raw statistics into actionable trading rules."""
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return raw_data

    client = anthropic.Anthropic(api_key=api_key, timeout=120.0, max_retries=2)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            messages=[{
                "role": "user",
                "content": f"""You are learning from 2+ years of crypto market data. Below are raw statistical patterns.

Your job: turn these into ACTIONABLE TRADING RULES that you will follow.

Rules must be:
- Specific: "When BTC RSI < 20 AND funding negative, go LONG — bounced 72% of time"
- Quantified: include % win rate, avg return, number of events
- Prioritized: strongest edge first
- Honest: if a pattern is weak (<55% WR), say so

Also identify:
- TRAPS to avoid (patterns that look good but fail)
- BEST setups (highest WR + highest avg return)
- REGIME rules (what works in bull vs bear)

RAW DATA:
{raw_data}

Write as markdown with clear sections. These will be your PERMANENT trading rules."""
            }]
        )
        return response.content[0].text if response.content else raw_data
    except Exception as e:
        logger.error(f"Claude synthesis failed: {e}")
        return raw_data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    print("Extracting 2 years of lessons...")
    lessons = extract_all_lessons()
    if lessons:
        print(f"\nDone: {len(lessons):,} chars")
        print(lessons[:3000])
