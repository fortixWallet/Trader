"""
Pattern Matcher — Find Historical Twins
=========================================
For any coin's current chart shape, finds the most similar
historical moments across ALL coins and shows what happened next.

Uses DTW (Dynamic Time Warping) for shape matching —
doesn't care about price level, only about the SHAPE of movement.
"""

import sqlite3
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
         'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO',
         'ARB', 'OP', 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO']


def _normalize(series):
    """Normalize to 0-1 range (shape only, ignore price level)."""
    mn, mx = np.min(series), np.max(series)
    if mx - mn < 1e-10:
        return np.zeros_like(series)
    return (series - mn) / (mx - mn)


def _shape_distance(a, b):
    """Fast shape distance — correlation-based (negative = similar)."""
    if len(a) != len(b):
        b = np.interp(np.linspace(0, 1, len(a)), np.linspace(0, 1, len(b)), b)
    a_n = _normalize(a)
    b_n = _normalize(b)
    # Correlation: 1 = identical shape, -1 = mirror
    corr = np.corrcoef(a_n, b_n)[0, 1]
    # Also check euclidean on normalized
    euclid = np.sqrt(np.mean((a_n - b_n) ** 2))
    # Combined score (lower = more similar)
    return -corr + euclid * 0.5


def find_similar_patterns(coin: str, lookback: int = 30, top_n: int = 10,
                          future_candles: int = 12) -> list:
    """Find historical moments that look like current chart.

    Args:
        coin: current coin to match
        lookback: how many 4h candles to match (30 = 5 days)
        top_n: how many matches to return
        future_candles: how many candles to show what happened after (12 = 2 days)

    Returns:
        list of matches: [{
            'coin': str, 'date': str, 'distance': float,
            'pattern_closes': [...], 'future_closes': [...],
            'future_return': float (%), 'direction': 'UP'/'DOWN'
        }]
    """
    conn = sqlite3.connect(str(DB_PATH))

    # Get current pattern for target coin
    current = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='4h' "
        "ORDER BY timestamp DESC LIMIT ?", (coin, lookback)
    ).fetchall()

    if len(current) < lookback:
        conn.close()
        return []

    current = current[::-1]  # chronological
    current_closes = np.array([r[1] for r in current])
    current_norm = _normalize(current_closes)

    # Search across ALL coins, ALL time periods
    matches = []

    for search_coin in COINS:
        rows = conn.execute(
            "SELECT timestamp, close FROM prices WHERE coin=? AND timeframe='4h' "
            "ORDER BY timestamp", (search_coin,)
        ).fetchall()

        if len(rows) < lookback + future_candles + 10:
            continue

        closes = np.array([r[1] for r in rows])
        timestamps = [r[0] for r in rows]

        # Slide window across history (skip last 2 days to avoid matching current)
        skip_recent = future_candles + lookback if search_coin == coin else future_candles
        for i in range(lookback, len(closes) - skip_recent):
            window = closes[i - lookback:i]
            dist = _shape_distance(current_norm, _normalize(window))

            # Only keep good matches
            if dist < 0.5:
                # What happened AFTER this pattern?
                future = closes[i:i + future_candles]
                if len(future) >= future_candles // 2:
                    future_ret = (future[-1] / window[-1] - 1) * 100
                    dt = datetime.utcfromtimestamp(timestamps[i])

                    matches.append({
                        'coin': search_coin,
                        'date': dt.strftime('%Y-%m-%d %H:%M'),
                        'distance': round(dist, 3),
                        'pattern_closes': window.tolist(),
                        'future_closes': future.tolist(),
                        'future_return': round(future_ret, 2),
                        'direction': 'UP' if future_ret > 0.5 else 'DOWN' if future_ret < -0.5 else 'FLAT',
                        'max_up': round((max(future) / window[-1] - 1) * 100, 2),
                        'max_down': round((min(future) / window[-1] - 1) * 100, 2),
                    })

    conn.close()

    # Sort by similarity
    matches.sort(key=lambda x: x['distance'])
    top = matches[:top_n]

    # Summary stats
    if top:
        up = sum(1 for m in top if m['direction'] == 'UP')
        down = sum(1 for m in top if m['direction'] == 'DOWN')
        avg_ret = np.mean([m['future_return'] for m in top])
        avg_max_up = np.mean([m['max_up'] for m in top])
        avg_max_down = np.mean([m['max_down'] for m in top])

        logger.info(f"Pattern match {coin}: {len(top)} twins found | "
                   f"{up} UP / {down} DOWN | avg ret {avg_ret:+.1f}% | "
                   f"max up +{avg_max_up:.1f}% / max down {avg_max_down:.1f}%")

    return top


def format_for_profi(coin: str, matches: list) -> str:
    """Format pattern matches as text for Profi's analysis."""
    if not matches:
        return f"No historical pattern matches found for {coin}."

    up = sum(1 for m in matches if m['direction'] == 'UP')
    down = sum(1 for m in matches if m['direction'] == 'DOWN')
    flat = len(matches) - up - down
    avg_ret = np.mean([m['future_return'] for m in matches])
    avg_max_up = np.mean([m['max_up'] for m in matches])
    avg_max_down = np.mean([m['max_down'] for m in matches])

    lines = [
        f"HISTORICAL PATTERN ANALYSIS for {coin}:",
        f"Found {len(matches)} similar chart shapes in history.",
        f"",
        f"OUTCOME STATS (next 48h after similar patterns):",
        f"  Went UP: {up}/{len(matches)} ({up/len(matches)*100:.0f}%)",
        f"  Went DOWN: {down}/{len(matches)} ({down/len(matches)*100:.0f}%)",
        f"  Average return: {avg_ret:+.1f}%",
        f"  Average max upside: +{avg_max_up:.1f}%",
        f"  Average max downside: {avg_max_down:.1f}%",
        f"",
        f"TOP MATCHES:",
    ]

    for i, m in enumerate(matches[:5]):
        lines.append(
            f"  #{i+1}: {m['coin']} on {m['date']} (dist: {m['distance']:.2f}) "
            f"→ {m['direction']} {m['future_return']:+.1f}% "
            f"(peak +{m['max_up']:.1f}% / dip {m['max_down']:.1f}%)"
        )

    # Verdict
    if up > down * 2:
        lines.append(f"\nVERDICT: STRONGLY BULLISH — {up}/{len(matches)} historical twins went UP")
    elif down > up * 2:
        lines.append(f"\nVERDICT: STRONGLY BEARISH — {down}/{len(matches)} historical twins went DOWN")
    elif up > down:
        lines.append(f"\nVERDICT: SLIGHTLY BULLISH — {up}/{len(matches)} went up but not overwhelming")
    elif down > up:
        lines.append(f"\nVERDICT: SLIGHTLY BEARISH — {down}/{len(matches)} went down")
    else:
        lines.append(f"\nVERDICT: MIXED — no clear historical edge")

    return "\n".join(lines)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    import time
    start = time.time()

    # Test: find patterns similar to current ETH chart
    matches = find_similar_patterns('ETH', lookback=30, top_n=10)
    elapsed = time.time() - start

    print(f"\nSearch took {elapsed:.1f}s")
    print(f"\n{format_for_profi('ETH', matches)}")

    print("\n--- Individual matches ---")
    for m in matches[:5]:
        print(f"{m['coin']} {m['date']}: similarity {1-m['distance']:.0%} → "
              f"{m['direction']} {m['future_return']:+.1f}%")
