"""
Knowledge Compiler — Extract EVERYTHING from DB for Profi
==========================================================
Analyzes 2+ years of data per coin and builds comprehensive knowledge:
- Per-coin behavior profiles (ATR, volatility patterns, best/worst hours)
- Historical support/resistance levels
- Funding rate patterns (when extremes → reversals)
- Liquidation cascade patterns
- Correlation matrix
- What worked in our trades vs what didn't
- Seasonal/time patterns
- Regime-specific behavior per coin
"""

import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
OUTPUT_PATH = Path(__file__).parent / 'profi_coin_knowledge.md'

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO',
         'BONK', 'PEPE', 'SHIB', 'FET', 'RAY', 'BOME', 'W', 'TNSR',
         'ORCA', 'JTO', 'DRIFT', 'MEW', 'POPCAT']


def compile_knowledge():
    conn = sqlite3.connect(str(DB_PATH))
    sections = []

    # ==========================================
    # 1. PER-COIN DEEP PROFILES
    # ==========================================
    sections.append("# PROFI — Per-Coin Deep Knowledge (Auto-Generated)\n")
    sections.append("Data: 2+ years of 4H candles, funding rates, liquidations, our trade history.\n")

    for coin in COINS:
        profile = _build_coin_profile(conn, coin)
        if profile:
            sections.append(profile)

    # ==========================================
    # 2. CROSS-COIN CORRELATION MATRIX
    # ==========================================
    sections.append(_build_correlations(conn))

    # ==========================================
    # 3. FUNDING RATE PATTERNS
    # ==========================================
    sections.append(_build_funding_patterns(conn))

    # ==========================================
    # 4. FEAR & GREED PATTERNS
    # ==========================================
    sections.append(_build_fear_greed_patterns(conn))

    # ==========================================
    # 5. TIME-OF-DAY PATTERNS
    # ==========================================
    sections.append(_build_time_patterns(conn))

    # ==========================================
    # 6. OUR TRADE HISTORY LESSONS
    # ==========================================
    sections.append(_build_trade_lessons(conn))

    # ==========================================
    # 7. RECENT MARKET CONTEXT (last 30 days)
    # ==========================================
    sections.append(_build_recent_context(conn))

    conn.close()

    text = "\n".join(sections)
    OUTPUT_PATH.write_text(text)
    print(f"Knowledge compiled: {len(text):,} chars → {OUTPUT_PATH}")
    return text


def _build_coin_profile(conn, coin):
    """Deep profile for one coin based on 2+ years of data."""
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='4h' ORDER BY timestamp", (coin,)
    ).fetchall()

    if len(rows) < 100:
        return ""

    closes = np.array([r[4] for r in rows])
    highs = np.array([r[2] for r in rows])
    lows = np.array([r[3] for r in rows])
    volumes = np.array([r[5] for r in rows])

    # Returns
    returns_4h = np.diff(closes) / closes[:-1]
    returns_1d = closes[6:] / closes[:-6] - 1  # ~daily (6 candles)

    # ATR
    tr = np.maximum(highs[1:] - lows[1:],
                    np.maximum(abs(highs[1:] - closes[:-1]), abs(lows[1:] - closes[:-1])))
    atr_14 = np.convolve(tr, np.ones(14)/14, mode='valid')
    min_len = min(len(atr_14), len(closes) - 15)
    atr_pct = atr_14[:min_len] / closes[15:15+min_len] * 100 if min_len > 0 else [0]

    # Current price and range
    current = closes[-1]
    high_52w = max(closes[-1300:]) if len(closes) > 1300 else max(closes)  # ~52 weeks
    low_52w = min(closes[-1300:]) if len(closes) > 1300 else min(closes)
    pct_from_high = (current / high_52w - 1) * 100
    pct_from_low = (current / low_52w - 1) * 100

    # Volatility buckets
    avg_atr = np.mean(atr_pct) if len(atr_pct) > 0 else 0
    max_atr = np.max(atr_pct) if len(atr_pct) > 0 else 0
    min_atr = np.min(atr_pct) if len(atr_pct) > 0 else 0

    # Win rate by direction (4h candles)
    up_candles = sum(1 for r in rows if r[4] > r[1])
    total = len(rows)
    up_pct = up_candles / total * 100

    # Best/worst hour patterns
    hour_returns = defaultdict(list)
    for i in range(1, len(rows)):
        dt = datetime.utcfromtimestamp(rows[i][0])
        ret = (rows[i][4] / rows[i-1][4] - 1) * 100
        hour_returns[dt.hour].append(ret)

    best_hour = max(hour_returns.items(), key=lambda x: np.mean(x[1]))
    worst_hour = min(hour_returns.items(), key=lambda x: np.mean(x[1]))

    # Day of week patterns
    dow_returns = defaultdict(list)
    for i in range(6, len(rows)):
        dt = datetime.utcfromtimestamp(rows[i][0])
        ret = (rows[i][4] / rows[i-6][4] - 1) * 100  # daily return
        dow_returns[dt.strftime('%A')].append(ret)

    best_day = max(dow_returns.items(), key=lambda x: np.mean(x[1]))
    worst_day = min(dow_returns.items(), key=lambda x: np.mean(x[1]))

    # Support/resistance from price distribution
    price_hist, bins = np.histogram(closes[-500:], bins=50)
    top_levels = np.argsort(price_hist)[-5:]
    sr_levels = sorted([(bins[i] + bins[i+1])/2 for i in top_levels])

    # Max drawdown and max rally (recent 6 months)
    recent = closes[-1000:]
    peak = recent[0]
    max_dd = 0
    for p in recent:
        peak = max(peak, p)
        dd = (peak - p) / peak
        max_dd = max(max_dd, dd)

    trough = recent[0]
    max_rally = 0
    for p in recent:
        trough = min(trough, p)
        rally = (p - trough) / trough if trough > 0 else 0
        max_rally = max(max_rally, rally)

    # Funding rate stats
    fr = conn.execute(
        "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 500",
        (coin,)
    ).fetchall()
    fr_vals = [r[0] for r in fr if r[0] is not None]
    avg_fr = np.mean(fr_vals) * 100 if fr_vals else 0
    max_fr = max(fr_vals) * 100 if fr_vals else 0
    min_fr = min(fr_vals) * 100 if fr_vals else 0

    # Biggest single 4h moves
    biggest_up = max(returns_4h) * 100
    biggest_down = min(returns_4h) * 100

    # Volume profile
    avg_vol = np.mean(volumes[-500:])
    vol_now = volumes[-1]
    vol_ratio = vol_now / avg_vol if avg_vol > 0 else 1

    lines = [
        f"\n## {coin} — Deep Profile",
        f"**Current:** ${current:.4f} | {pct_from_high:+.0f}% from 52w high | +{pct_from_low:.0f}% from 52w low",
        f"**52w range:** ${low_52w:.4f} — ${high_52w:.4f}",
        f"**ATR(4h):** avg {avg_atr:.2f}% | range {min_atr:.2f}%-{max_atr:.2f}%",
        f"**Volatility class:** {'HIGH' if avg_atr > 3 else 'MEDIUM' if avg_atr > 1.5 else 'LOW'}",
        f"**Up candles:** {up_pct:.0f}% (of {total} 4h candles)",
        f"**Max moves (4h):** +{biggest_up:.1f}% / {biggest_down:.1f}%",
        f"**Max drawdown (6mo):** -{max_dd*100:.0f}% | Max rally: +{max_rally*100:.0f}%",
        f"**Volume:** current {'HIGH' if vol_ratio > 1.5 else 'LOW' if vol_ratio < 0.5 else 'NORMAL'} ({vol_ratio:.1f}x avg)",
        f"**Funding:** avg {avg_fr:.4f}% | range [{min_fr:.3f}%, {max_fr:.3f}%]",
        f"**Best time:** {best_hour[0]:02d}:00 UTC (avg {np.mean(best_hour[1]):+.3f}%)",
        f"**Worst time:** {worst_hour[0]:02d}:00 UTC (avg {np.mean(worst_hour[1]):+.3f}%)",
        f"**Best day:** {best_day[0]} (avg {np.mean(best_day[1]):+.3f}%)",
        f"**Worst day:** {worst_day[0]} (avg {np.mean(worst_day[1]):+.3f}%)",
        f"**Key S/R levels:** {', '.join(f'${l:.4f}' for l in sr_levels)}",
        "",
    ]

    # Trading hints
    if avg_atr > 3:
        lines.append(f"*HINT: {coin} is highly volatile — wider TP/SL needed (1.5-2.5%), fast moves, don't hold too long.*")
    elif avg_atr < 1.5:
        lines.append(f"*HINT: {coin} is low volatility — tight TP (0.5-1%), patience needed, consider skipping in low-vol days.*")

    if abs(avg_fr) > 0.005:
        if avg_fr > 0:
            lines.append(f"*HINT: {coin} has persistent positive funding — shorts get paid, longs pay. Favor SHORT on neutral.*")
        else:
            lines.append(f"*HINT: {coin} has persistent negative funding — longs get paid. Favor LONG on neutral.*")

    if pct_from_high < -60:
        lines.append(f"*HINT: {coin} is {pct_from_high:.0f}% from ATH — deeply oversold territory, watch for capitulation bounce.*")

    return "\n".join(lines)


def _build_correlations(conn):
    """Cross-coin correlation matrix."""
    coin_returns = {}
    for coin in COINS[:15]:  # top 15
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' ORDER BY timestamp DESC LIMIT 500",
            (coin,)
        ).fetchall()
        if len(rows) > 100:
            closes = np.array([r[0] for r in rows[::-1]])
            coin_returns[coin] = np.diff(closes) / closes[:-1]

    lines = ["\n## Cross-Coin Correlations (4H returns, last 3 months)\n"]

    # Find most and least correlated pairs
    pairs = []
    coins_list = list(coin_returns.keys())
    for i in range(len(coins_list)):
        for j in range(i+1, len(coins_list)):
            c1, c2 = coins_list[i], coins_list[j]
            min_len = min(len(coin_returns[c1]), len(coin_returns[c2]))
            corr = np.corrcoef(coin_returns[c1][:min_len], coin_returns[c2][:min_len])[0, 1]
            pairs.append((c1, c2, corr))

    pairs.sort(key=lambda x: x[2], reverse=True)

    lines.append("**Most correlated (move together — don't hold both):**")
    for c1, c2, corr in pairs[:8]:
        lines.append(f"  {c1}-{c2}: {corr:.2f}")

    lines.append("\n**Least correlated (good for diversification):**")
    for c1, c2, corr in pairs[-5:]:
        lines.append(f"  {c1}-{c2}: {corr:.2f}")

    lines.append("")
    return "\n".join(lines)


def _build_funding_patterns(conn):
    """When extreme funding → what happened next."""
    lines = ["\n## Funding Rate Reversal Patterns\n"]
    lines.append("*When funding rate goes extreme, price often reverses within 4-12 hours.*\n")

    for coin in ['BTC', 'ETH', 'SOL', 'DOGE']:
        rows = conn.execute("""
            SELECT f.timestamp, f.rate, p.close
            FROM funding_rates f
            JOIN prices p ON p.coin = f.coin AND p.timeframe = '4h'
                AND p.timestamp = (SELECT MAX(p2.timestamp) FROM prices p2
                    WHERE p2.coin = f.coin AND p2.timeframe = '4h' AND p2.timestamp <= f.timestamp)
            WHERE f.coin = ? AND f.rate IS NOT NULL
            ORDER BY f.timestamp
        """, (coin,)).fetchall()

        if len(rows) < 100:
            continue

        extreme_positive = [(r[0], r[1], r[2]) for r in rows if r[1] > 0.0005]
        extreme_negative = [(r[0], r[1], r[2]) for r in rows if r[1] < -0.0003]

        # Check what happened 24h after extreme funding
        if extreme_positive:
            next_returns = []
            for ts, rate, price in extreme_positive:
                next_price = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "AND timestamp > ? ORDER BY timestamp LIMIT 1 OFFSET 5",
                    (coin, ts)
                ).fetchone()
                if next_price:
                    next_returns.append((next_price[0] / price - 1) * 100)

            if next_returns:
                avg_ret = np.mean(next_returns)
                down_pct = sum(1 for r in next_returns if r < 0) / len(next_returns) * 100
                lines.append(f"**{coin} after extreme POSITIVE funding (>{0.05}%):** "
                           f"avg 24h return {avg_ret:+.2f}%, dropped {down_pct:.0f}% of time "
                           f"({len(next_returns)} events)")

        if extreme_negative:
            next_returns = []
            for ts, rate, price in extreme_negative:
                next_price = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "AND timestamp > ? ORDER BY timestamp LIMIT 1 OFFSET 5",
                    (coin, ts)
                ).fetchone()
                if next_price:
                    next_returns.append((next_price[0] / price - 1) * 100)

            if next_returns:
                avg_ret = np.mean(next_returns)
                up_pct = sum(1 for r in next_returns if r > 0) / len(next_returns) * 100
                lines.append(f"**{coin} after extreme NEGATIVE funding (<-{0.03}%):** "
                           f"avg 24h return {avg_ret:+.2f}%, bounced {up_pct:.0f}% of time "
                           f"({len(next_returns)} events)")

    lines.append("")
    return "\n".join(lines)


def _build_fear_greed_patterns(conn):
    """What happens at different F&G levels."""
    lines = ["\n## Fear & Greed → BTC Performance\n"]

    fg_rows = conn.execute("""
        SELECT fg.date, fg.value, p.close
        FROM fear_greed fg
        JOIN prices p ON p.coin = 'BTC' AND p.timeframe = '1d'
            AND date(p.timestamp, 'unixepoch') = fg.date
        WHERE fg.value IS NOT NULL
        ORDER BY fg.date
    """).fetchall()

    if len(fg_rows) < 100:
        return ""

    buckets = {
        'Extreme Fear (0-20)': [], 'Fear (20-40)': [],
        'Neutral (40-60)': [], 'Greed (60-80)': [],
        'Extreme Greed (80-100)': []
    }

    for i in range(len(fg_rows) - 7):
        val = fg_rows[i][1]
        price_now = fg_rows[i][2]
        price_7d = fg_rows[i + 7][2] if i + 7 < len(fg_rows) else None
        if price_7d and price_now:
            ret = (price_7d / price_now - 1) * 100
            if val < 20: buckets['Extreme Fear (0-20)'].append(ret)
            elif val < 40: buckets['Fear (20-40)'].append(ret)
            elif val < 60: buckets['Neutral (40-60)'].append(ret)
            elif val < 80: buckets['Greed (60-80)'].append(ret)
            else: buckets['Extreme Greed (80-100)'].append(ret)

    for name, returns in buckets.items():
        if returns:
            avg = np.mean(returns)
            up = sum(1 for r in returns if r > 0) / len(returns) * 100
            lines.append(f"**{name}:** BTC avg 7d return {avg:+.2f}%, "
                        f"positive {up:.0f}% of time ({len(returns)} events)")

    lines.append("\n*Key insight: Extreme Fear is historically the BEST time to buy BTC (not sell).*")
    lines.append("")
    return "\n".join(lines)


def _build_time_patterns(conn):
    """Best/worst hours and days for trading."""
    lines = ["\n## Time-Based Patterns (All Coins Aggregated)\n"]

    all_hour_returns = defaultdict(list)
    for coin in COINS[:15]:
        rows = conn.execute(
            "SELECT timestamp, open, close FROM prices "
            "WHERE coin=? AND timeframe='4h' ORDER BY timestamp DESC LIMIT 1000",
            (coin,)
        ).fetchall()
        for r in rows:
            hour = datetime.utcfromtimestamp(r[0]).hour
            ret = (r[2] / r[1] - 1) * 100 if r[1] > 0 else 0
            all_hour_returns[hour].append(ret)

    lines.append("**4H candle performance by hour (UTC):**")
    for h in sorted(all_hour_returns.keys()):
        rets = all_hour_returns[h]
        avg = np.mean(rets)
        up = sum(1 for r in rets if r > 0) / len(rets) * 100
        bar = '+' * int(abs(avg) * 20) if avg > 0 else '-' * int(abs(avg) * 20)
        lines.append(f"  {h:02d}:00: avg {avg:+.3f}% | {up:.0f}% up | {bar}")

    lines.append("")
    return "\n".join(lines)


def _build_trade_lessons(conn):
    """Lessons from our actual trade history."""
    lines = ["\n## Our Trade History — Lessons Learned\n"]

    try:
        trades = conn.execute("""
            SELECT coin, direction, entry_price, exit_price, pnl_pct, pnl_usd,
                   exit_reason, regime, held_minutes
            FROM okx_trades ORDER BY entry_time
        """).fetchall()
    except Exception:
        return "\n## No trade history yet.\n"

    if not trades:
        return "\n## No trade history yet.\n"

    wins = [t for t in trades if t[4] and t[4] > 0]
    losses = [t for t in trades if t[4] and t[4] <= 0]

    lines.append(f"**Total trades:** {len(trades)} | Wins: {len(wins)} | Losses: {len(losses)}")
    if trades:
        wr = len(wins) / len(trades) * 100
        lines.append(f"**Win rate:** {wr:.0f}%")

    # Per coin performance
    coin_pnl = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0})
    for t in trades:
        coin_pnl[t[0]]['trades'] += 1
        if t[4] and t[4] > 0: coin_pnl[t[0]]['wins'] += 1
        coin_pnl[t[0]]['pnl'] += t[5] or 0

    lines.append("\n**Per-coin results:**")
    for coin, stats in sorted(coin_pnl.items(), key=lambda x: x[1]['pnl'], reverse=True):
        wr = stats['wins'] / stats['trades'] * 100 if stats['trades'] > 0 else 0
        lines.append(f"  {coin}: {stats['trades']} trades, WR {wr:.0f}%, ${stats['pnl']:+.2f}")

    # Per exit reason
    reason_stats = defaultdict(lambda: {'count': 0, 'pnl': 0})
    for t in trades:
        r = t[6] or 'unknown'
        reason_stats[r]['count'] += 1
        reason_stats[r]['pnl'] += t[5] or 0

    lines.append("\n**Per exit reason:**")
    for reason, stats in sorted(reason_stats.items(), key=lambda x: x[1]['pnl']):
        lines.append(f"  {reason}: {stats['count']} trades, ${stats['pnl']:+.2f}")

    # Average hold time
    hold_times = [t[8] for t in trades if t[8]]
    if hold_times:
        lines.append(f"\n**Avg hold:** {np.mean(hold_times):.0f} min ({np.mean(hold_times)/60:.1f}h)")

    lines.append("")
    return "\n".join(lines)


def _build_recent_context(conn):
    """Last 30 days market context."""
    lines = ["\n## Recent Market Context (Last 30 Days)\n"]

    # BTC trend
    btc = conn.execute(
        "SELECT date(timestamp,'unixepoch'), close FROM prices "
        "WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp DESC LIMIT 30"
    ).fetchall()

    if btc:
        btc_now = btc[0][1]
        btc_30d = btc[-1][1]
        btc_7d = btc[6][1] if len(btc) > 6 else btc_now
        lines.append(f"**BTC:** ${btc_now:,.0f} | 7d: {(btc_now/btc_7d-1)*100:+.1f}% | "
                    f"30d: {(btc_now/btc_30d-1)*100:+.1f}%")

    # Fear & Greed trend
    fg = conn.execute("SELECT date, value FROM fear_greed ORDER BY date DESC LIMIT 7").fetchall()
    if fg:
        fg_str = ', '.join(f"{r[1]}" for r in fg[:7])
        lines.append(f"**Fear & Greed (7d):** {fg_str}")
        avg_fg = np.mean([r[1] for r in fg])
        if avg_fg < 25:
            lines.append("*Market in EXTREME FEAR — historically best for buying, worst for shorting.*")
        elif avg_fg > 75:
            lines.append("*Market in GREED — historically risky for longs, watch for distribution.*")

    # Top/bottom performers (30d)
    perfs = []
    for coin in COINS:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp DESC LIMIT 30",
            (coin,)
        ).fetchall()
        if len(rows) >= 2:
            ret = (rows[0][0] / rows[-1][0] - 1) * 100
            perfs.append((coin, ret))

    perfs.sort(key=lambda x: x[1])
    if perfs:
        lines.append(f"\n**Worst 30d:** {', '.join(f'{c} {r:+.0f}%' for c, r in perfs[:5])}")
        lines.append(f"**Best 30d:** {', '.join(f'{c} {r:+.0f}%' for c, r in perfs[-5:])}")

    lines.append("")
    return "\n".join(lines)


if __name__ == '__main__':
    text = compile_knowledge()
    print(f"\nDone: {len(text):,} chars")
