"""
Advanced Knowledge Compiler — Deep Statistical Patterns
=========================================================
Extracts patterns that a human trader would learn after years:
- Per-coin: what setups actually work (RSI levels, BB positions)
- Regime transitions: how coins behave during BULL→BEAR shifts
- Monthly seasonality per coin
- Volume reversal patterns
- BTC→altcoin lag relationships
- Our trade history autopsy
"""

import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
OUTPUT_PATH = Path(__file__).parent / 'profi_advanced_knowledge.md'

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO',
         'BONK', 'PEPE', 'SHIB', 'FET', 'RAY', 'BOME', 'W', 'TNSR']


def compile():
    conn = sqlite3.connect(str(DB_PATH))
    sections = []

    sections.append("# PROFI — Advanced Statistical Knowledge (Auto-Generated)\n")
    sections.append("*Patterns extracted from 2+ years of 4H data. These are REAL statistics, not theory.*\n")

    sections.append(_rsi_reversal_stats(conn))
    sections.append(_bb_squeeze_breakout(conn))
    sections.append(_volume_spike_outcomes(conn))
    sections.append(_btc_alt_lag(conn))
    sections.append(_monthly_seasonality(conn))
    sections.append(_regime_transitions(conn))
    sections.append(_consecutive_candle_patterns(conn))
    sections.append(_trade_autopsy(conn))

    conn.close()

    text = "\n".join(s for s in sections if s)
    OUTPUT_PATH.write_text(text)
    print(f"Advanced knowledge: {len(text):,} chars → {OUTPUT_PATH}")
    return text


def _rsi_reversal_stats(conn):
    """What happens AFTER RSI hits extreme levels for each coin."""
    lines = ["\n## RSI Extreme → Next 24h Performance\n"]
    lines.append("*When RSI hits extreme, what's the probability of reversal?*\n")

    for coin in COINS[:15]:
        rows = conn.execute(
            "SELECT timestamp, open, high, low, close, volume FROM prices "
            "WHERE coin=? AND timeframe='4h' ORDER BY timestamp", (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = np.array([r[4] for r in rows])

        # Calculate RSI
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.zeros(len(deltas))
        avg_loss = np.zeros(len(deltas))
        if len(gains) >= 14:
            avg_gain[13] = np.mean(gains[:14])
            avg_loss[13] = np.mean(losses[:14])
            for i in range(14, len(deltas)):
                avg_gain[i] = (avg_gain[i-1] * 13 + gains[i]) / 14
                avg_loss[i] = (avg_loss[i-1] * 13 + losses[i]) / 14
        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - 100 / (1 + rs)

        # RSI < 25 (oversold)
        oversold_returns = []
        for i in range(20, len(rsi) - 6):
            if rsi[i] < 25:
                ret_24h = (closes[i+7] / closes[i+1] - 1) * 100  # +1 to avoid lookahead
                oversold_returns.append(ret_24h)

        # RSI > 75 (overbought)
        overbought_returns = []
        for i in range(20, len(rsi) - 6):
            if rsi[i] > 75:
                ret_24h = (closes[i+7] / closes[i+1] - 1) * 100
                overbought_returns.append(ret_24h)

        if oversold_returns or overbought_returns:
            parts = [f"**{coin}:**"]
            if oversold_returns:
                avg = np.mean(oversold_returns)
                bounce_pct = sum(1 for r in oversold_returns if r > 0) / len(oversold_returns) * 100
                parts.append(f"RSI<25 → {bounce_pct:.0f}% bounced (avg {avg:+.1f}%, {len(oversold_returns)} events)")
            if overbought_returns:
                avg = np.mean(overbought_returns)
                drop_pct = sum(1 for r in overbought_returns if r < 0) / len(overbought_returns) * 100
                parts.append(f"RSI>75 → {drop_pct:.0f}% dropped (avg {avg:+.1f}%, {len(overbought_returns)} events)")
            lines.append(" | ".join(parts))

    lines.append("")
    return "\n".join(lines)


def _bb_squeeze_breakout(conn):
    """BB squeeze → which direction does breakout go?"""
    lines = ["\n## Bollinger Band Squeeze → Breakout Direction\n"]
    lines.append("*When BB width is minimal (squeeze), which way does it break?*\n")

    for coin in COINS[:15]:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = np.array([r[0] for r in rows])

        # BB width
        bb_width = []
        for i in range(20, len(closes)):
            ma = np.mean(closes[i-20:i])
            std = np.std(closes[i-20:i])
            bb_width.append(std / ma if ma > 0 else 0)

        if not bb_width:
            continue

        bb_arr = np.array(bb_width)
        threshold = np.percentile(bb_arr, 10)  # bottom 10% = squeeze

        breakout_up = 0
        breakout_down = 0
        for i in range(len(bb_arr) - 6):
            if bb_arr[i] < threshold:
                # Check next 6 candles (24h)
                ret = (closes[i + 26] / closes[i + 20] - 1) * 100
                if ret > 0.5:
                    breakout_up += 1
                elif ret < -0.5:
                    breakout_down += 1

        total = breakout_up + breakout_down
        if total > 10:
            up_pct = breakout_up / total * 100
            lines.append(f"**{coin}:** BB squeeze → UP {up_pct:.0f}% / DOWN {100-up_pct:.0f}% ({total} events)")

    lines.append("")
    return "\n".join(lines)


def _volume_spike_outcomes(conn):
    """What happens after a volume spike (2x+ avg)?"""
    lines = ["\n## Volume Spike (2x+) → Next Candle Direction\n"]
    lines.append("*High volume often confirms direction. But does it predict continuation?*\n")

    for coin in COINS[:15]:
        rows = conn.execute(
            "SELECT close, volume FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = np.array([r[0] for r in rows])
        volumes = np.array([r[1] for r in rows])

        results = {'up_continue': 0, 'up_reverse': 0, 'down_continue': 0, 'down_reverse': 0}

        for i in range(50, len(rows) - 1):
            avg_vol = np.mean(volumes[i-50:i])
            if avg_vol <= 0:
                continue
            vol_ratio = volumes[i] / avg_vol

            if vol_ratio >= 2.0:
                candle_dir = 'up' if closes[i] > closes[i-1] else 'down'
                next_dir = 'up' if closes[i+1] > closes[i] else 'down'

                if candle_dir == 'up' and next_dir == 'up':
                    results['up_continue'] += 1
                elif candle_dir == 'up' and next_dir == 'down':
                    results['up_reverse'] += 1
                elif candle_dir == 'down' and next_dir == 'down':
                    results['down_continue'] += 1
                else:
                    results['down_reverse'] += 1

        total_up = results['up_continue'] + results['up_reverse']
        total_down = results['down_continue'] + results['down_reverse']

        if total_up > 5 and total_down > 5:
            up_cont_pct = results['up_continue'] / total_up * 100
            down_cont_pct = results['down_continue'] / total_down * 100
            lines.append(f"**{coin}:** Volume spike UP candle → continues UP {up_cont_pct:.0f}% | "
                        f"Volume spike DOWN candle → continues DOWN {down_cont_pct:.0f}%")

    lines.append("")
    return "\n".join(lines)


def _btc_alt_lag(conn):
    """When BTC moves, how long until alts follow?"""
    lines = ["\n## BTC Move → Altcoin Lag Pattern\n"]
    lines.append("*When BTC drops/pumps >2% in 4h, how do alts respond?*\n")

    btc_rows = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp"
    ).fetchall()
    if len(btc_rows) < 200:
        return ""

    btc_ts = {r[0]: r[1] for r in btc_rows}
    btc_timestamps = sorted(btc_ts.keys())

    for coin in ['ETH', 'SOL', 'DOGE', 'AVAX', 'LINK', 'ARB']:
        coin_rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        coin_ts = {r[0]: r[1] for r in coin_rows}

        same_candle_corr = []
        next_candle_corr = []

        for i in range(1, len(btc_timestamps) - 1):
            ts = btc_timestamps[i]
            ts_prev = btc_timestamps[i-1]
            ts_next = btc_timestamps[i+1] if i+1 < len(btc_timestamps) else None

            btc_ret = (btc_ts[ts] / btc_ts[ts_prev] - 1) * 100

            if abs(btc_ret) >= 2.0 and ts in coin_ts and ts_prev in coin_ts:
                coin_ret_same = (coin_ts[ts] / coin_ts[ts_prev] - 1) * 100
                same_candle_corr.append((btc_ret, coin_ret_same))

                if ts_next and ts_next in coin_ts:
                    coin_ret_next = (coin_ts[ts_next] / coin_ts[ts] - 1) * 100
                    next_candle_corr.append((btc_ret, coin_ret_next))

        if len(same_candle_corr) > 10:
            # When BTC dumps >2%, does alt dump MORE or LESS?
            btc_dumps = [(b, c) for b, c in same_candle_corr if b < -2]
            btc_pumps = [(b, c) for b, c in same_candle_corr if b > 2]

            parts = [f"**{coin}:**"]
            if btc_dumps:
                avg_alt = np.mean([c for _, c in btc_dumps])
                avg_btc = np.mean([b for b, _ in btc_dumps])
                beta = avg_alt / avg_btc if avg_btc != 0 else 1
                parts.append(f"BTC dump → {coin} {avg_alt:+.1f}% (beta {beta:.1f}x)")
            if btc_pumps:
                avg_alt = np.mean([c for _, c in btc_pumps])
                avg_btc = np.mean([b for b, _ in btc_pumps])
                beta = avg_alt / avg_btc if avg_btc != 0 else 1
                parts.append(f"BTC pump → {coin} {avg_alt:+.1f}% (beta {beta:.1f}x)")

            if next_candle_corr:
                # After BTC big move, does alt continue next candle?
                btc_big_down = [(b, c) for b, c in next_candle_corr if b < -2]
                if btc_big_down:
                    continued = sum(1 for _, c in btc_big_down if c < 0) / len(btc_big_down) * 100
                    parts.append(f"After BTC dump: {coin} continues down {continued:.0f}%")

            lines.append(" | ".join(parts))

    lines.append("")
    return "\n".join(lines)


def _monthly_seasonality(conn):
    """Month-by-month performance per coin."""
    lines = ["\n## Monthly Seasonality (2024-2026)\n"]
    lines.append("*Average monthly return per coin — reveals seasonal patterns.*\n")

    header = "| Coin | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |"
    lines.append(header)
    lines.append("|" + "------|" * 13)

    for coin in COINS[:15]:
        rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 100:
            continue

        monthly = defaultdict(list)
        for i in range(30, len(rows)):
            dt = datetime.utcfromtimestamp(rows[i][0])
            if dt.day <= 5:  # first week of month → calc prev month return
                prev_month = dt.month - 1 if dt.month > 1 else 12
                ret = (rows[i][1] / rows[i-30][1] - 1) * 100 if rows[i-30][1] > 0 else 0
                monthly[prev_month].append(ret)

        cells = [f"**{coin}**"]
        for m in range(1, 13):
            if monthly[m]:
                avg = np.mean(monthly[m])
                cells.append(f"{avg:+.0f}%")
            else:
                cells.append("—")
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("")
    return "\n".join(lines)


def _regime_transitions(conn):
    """What happens during regime transitions (using BTC as proxy)."""
    lines = ["\n## Regime Transitions — What Happens\n"]
    lines.append("*Based on BTC 30-day returns crossing zero.*\n")

    btc = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp"
    ).fetchall()
    if len(btc) < 60:
        return ""

    closes = np.array([r[1] for r in btc])

    # Define regime: 30d return > 0 = BULL, < 0 = BEAR
    transitions = []
    for i in range(30, len(closes) - 30):
        prev_regime = 'BULL' if closes[i] > closes[i-30] else 'BEAR'
        next_regime = 'BULL' if closes[i+1] > closes[i-29] else 'BEAR'
        if prev_regime != next_regime:
            # What happened in next 7 days?
            ret_7d = (closes[min(i+7, len(closes)-1)] / closes[i] - 1) * 100
            transitions.append({
                'from': prev_regime, 'to': next_regime,
                'date': datetime.utcfromtimestamp(btc[i][0]).strftime('%Y-%m-%d'),
                'ret_7d': ret_7d
            })

    bull_to_bear = [t for t in transitions if t['from'] == 'BULL' and t['to'] == 'BEAR']
    bear_to_bull = [t for t in transitions if t['from'] == 'BEAR' and t['to'] == 'BULL']

    if bull_to_bear:
        avg = np.mean([t['ret_7d'] for t in bull_to_bear])
        lines.append(f"**BULL→BEAR transition:** {len(bull_to_bear)} times, avg next 7d: {avg:+.1f}%")
        lines.append(f"  Recent: {', '.join(t['date'] for t in bull_to_bear[-5:])}")
        lines.append("  *Action: Close longs immediately, wait 2-3 days before shorting (dead cat bounce risk).*")

    if bear_to_bull:
        avg = np.mean([t['ret_7d'] for t in bear_to_bull])
        lines.append(f"\n**BEAR→BULL transition:** {len(bear_to_bull)} times, avg next 7d: {avg:+.1f}%")
        lines.append(f"  Recent: {', '.join(t['date'] for t in bear_to_bull[-5:])}")
        lines.append("  *Action: Start with small longs on strongest coins (ETH, SOL), add as confirmation builds.*")

    lines.append("")
    return "\n".join(lines)


def _consecutive_candle_patterns(conn):
    """After N red/green candles in a row, what happens?"""
    lines = ["\n## Consecutive Candle Patterns\n"]
    lines.append("*After N candles in same direction, what's the probability of reversal?*\n")

    for coin in ['BTC', 'ETH', 'SOL', 'DOGE']:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp",
            (coin,)
        ).fetchall()
        if len(rows) < 200:
            continue

        closes = [r[0] for r in rows]
        results = {}

        for streak_len in [3, 4, 5]:
            reversal_after_red = 0
            total_red_streaks = 0
            reversal_after_green = 0
            total_green_streaks = 0

            for i in range(streak_len, len(closes) - 1):
                # Check for red streak
                all_red = all(closes[i-j] < closes[i-j-1] for j in range(streak_len))
                if all_red:
                    total_red_streaks += 1
                    if closes[i+1] > closes[i]:  # next candle green
                        reversal_after_red += 1

                # Check for green streak
                all_green = all(closes[i-j] > closes[i-j-1] for j in range(streak_len))
                if all_green:
                    total_green_streaks += 1
                    if closes[i+1] < closes[i]:  # next candle red
                        reversal_after_green += 1

            if total_red_streaks > 5 and total_green_streaks > 5:
                red_rev = reversal_after_red / total_red_streaks * 100
                green_rev = reversal_after_green / total_green_streaks * 100
                results[streak_len] = (red_rev, total_red_streaks, green_rev, total_green_streaks)

        if results:
            parts = [f"**{coin}:**"]
            for n, (red_rev, red_n, green_rev, green_n) in sorted(results.items()):
                parts.append(f"{n} red → reversal {red_rev:.0f}% ({red_n} events), "
                           f"{n} green → reversal {green_rev:.0f}% ({green_n} events)")
            lines.append(" | ".join(parts))

    lines.append("")
    return "\n".join(lines)


def _trade_autopsy(conn):
    """Deep analysis of our 60 trades — what went wrong, what went right."""
    lines = ["\n## Our Trade Autopsy — 60 Trades Analyzed\n"]

    try:
        trades = conn.execute("""
            SELECT coin, direction, entry_price, exit_price, pnl_pct, pnl_usd,
                   exit_reason, regime, reg_score, held_minutes, leverage,
                   funding_rate, liq_bias
            FROM okx_trades WHERE pnl_pct IS NOT NULL ORDER BY entry_time
        """).fetchall()
    except Exception:
        return "\n## No trade history for autopsy.\n"

    if not trades:
        return "\n## No trade history for autopsy.\n"

    wins = [t for t in trades if t[4] > 0]
    losses = [t for t in trades if t[4] <= 0]

    # Winning patterns
    if wins:
        lines.append(f"### Winners ({len(wins)} trades)")
        avg_hold_w = np.mean([t[9] for t in wins if t[9]]) if any(t[9] for t in wins) else 0
        avg_score_w = np.mean([t[8] for t in wins if t[8]]) if any(t[8] for t in wins) else 0
        lines.append(f"  Avg hold: {avg_hold_w:.0f} min | Avg ML score: {avg_score_w:.4f}")

        # Exit reason distribution for wins
        win_reasons = defaultdict(int)
        for t in wins:
            win_reasons[t[6] or 'unknown'] += 1
        lines.append(f"  Exit reasons: {dict(win_reasons)}")

    # Losing patterns
    if losses:
        lines.append(f"\n### Losers ({len(losses)} trades)")
        avg_hold_l = np.mean([t[9] for t in losses if t[9]]) if any(t[9] for t in losses) else 0
        avg_score_l = np.mean([t[8] for t in losses if t[8]]) if any(t[8] for t in losses) else 0
        lines.append(f"  Avg hold: {avg_hold_l:.0f} min | Avg ML score: {avg_score_l:.4f}")

        loss_reasons = defaultdict(int)
        for t in losses:
            loss_reasons[t[6] or 'unknown'] += 1
        lines.append(f"  Exit reasons: {dict(loss_reasons)}")

    # Key insights
    lines.append("\n### Key Patterns Discovered:")

    # Score threshold analysis
    high_score = [t for t in trades if t[8] and abs(t[8]) > 0.005]
    low_score = [t for t in trades if t[8] and abs(t[8]) <= 0.005]
    if high_score and low_score:
        wr_high = sum(1 for t in high_score if t[4] > 0) / len(high_score) * 100
        wr_low = sum(1 for t in low_score if t[4] > 0) / len(low_score) * 100
        lines.append(f"- Strong ML score (|s|>0.005): WR {wr_high:.0f}% ({len(high_score)} trades)")
        lines.append(f"- Weak ML score (|s|<=0.005): WR {wr_low:.0f}% ({len(low_score)} trades)")

    # Direction analysis
    longs = [t for t in trades if t[1] == 'LONG']
    shorts = [t for t in trades if t[1] == 'SHORT']
    if longs:
        wr_l = sum(1 for t in longs if t[4] > 0) / len(longs) * 100
        lines.append(f"- LONG trades: WR {wr_l:.0f}% ({len(longs)} trades)")
    if shorts:
        wr_s = sum(1 for t in shorts if t[4] > 0) / len(shorts) * 100
        lines.append(f"- SHORT trades: WR {wr_s:.0f}% ({len(shorts)} trades)")

    # Hold time vs profitability
    quick = [t for t in trades if t[9] and t[9] < 60]
    medium = [t for t in trades if t[9] and 60 <= t[9] < 240]
    long_hold = [t for t in trades if t[9] and t[9] >= 240]
    for name, group in [("Quick <1h", quick), ("Medium 1-4h", medium), ("Long >4h", long_hold)]:
        if group:
            wr = sum(1 for t in group if t[4] > 0) / len(group) * 100
            lines.append(f"- {name}: WR {wr:.0f}% ({len(group)} trades)")

    lines.append("")
    return "\n".join(lines)


if __name__ == '__main__':
    text = compile()
    print(f"\nDone: {len(text):,} chars")
