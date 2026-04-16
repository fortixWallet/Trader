"""
FORTIX Coin Risk Scorer — Volatility + Liquidity → Safe Leverage
================================================================

Each coin gets a risk score that determines:
  - Maximum leverage allowed
  - Position size adjustment
  - Stop-loss width
  - Whether to trade at all

No more TAO at 5x. System knows each coin's personality.
"""

import sqlite3
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'


def score_all_coins(conn=None) -> dict:
    """Score all coins by risk. Returns dict of coin → risk profile."""
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB_PATH))

    results = {}

    coins = [r[0] for r in conn.execute(
        "SELECT DISTINCT coin FROM prices WHERE timeframe='1d'"
    ).fetchall()]

    for coin in coins:
        results[coin] = score_coin(conn, coin)

    if own_conn:
        conn.close()

    return results


def score_coin(conn, coin: str) -> dict:
    """Score a single coin's risk profile."""
    # 1. Volatility (7d and 30d)
    rows = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp DESC LIMIT 31",
        (coin,)
    ).fetchall()

    if len(rows) < 14:
        return {'coin': coin, 'risk': 'unknown', 'max_leverage': 2, 'vol_daily': 0,
                'liquidity': 'unknown', 'stop_mult': 2.0}

    closes = [r[0] for r in rows]
    rets = [(closes[i] / closes[i+1] - 1) for i in range(min(30, len(closes)-1))]
    vol_7d = np.std(rets[:7]) * 100 if len(rets) >= 7 else 5.0
    vol_30d = np.std(rets) * 100

    # 2. Liquidity (average daily volume in USD)
    vol_rows = conn.execute(
        "SELECT close, volume FROM prices WHERE coin=? AND timeframe='1d' ORDER BY timestamp DESC LIMIT 14",
        (coin,)
    ).fetchall()
    if vol_rows:
        avg_volume_usd = np.mean([r[0] * r[1] for r in vol_rows])
    else:
        avg_volume_usd = 0

    # 3. Max drawdown in last 30d
    if len(closes) >= 2:
        peak = closes[-1]
        max_dd = 0
        for c in reversed(closes):
            peak = max(peak, c)
            dd = (peak - c) / peak * 100
            max_dd = max(max_dd, dd)
    else:
        max_dd = 0

    # 4. Risk classification
    if vol_7d > 6:
        risk_level = 'VERY_HIGH'
        max_leverage = 3
        stop_mult = 2.0
    elif vol_7d > 4:
        risk_level = 'HIGH'
        max_leverage = 5
        stop_mult = 1.5
    elif vol_7d > 2.5:
        risk_level = 'MEDIUM'
        max_leverage = 7
        stop_mult = 1.5
    elif vol_7d > 1.5:
        risk_level = 'LOW'
        max_leverage = 10
        stop_mult = 1.0
    else:
        risk_level = 'VERY_LOW'
        max_leverage = 10
        stop_mult = 1.0

    # Liquidity adjustment
    if avg_volume_usd < 1_000_000:  # < $1M daily volume
        max_leverage = min(max_leverage, 2)
        risk_level = 'VERY_HIGH'  # thin market
    elif avg_volume_usd < 10_000_000:  # < $10M
        max_leverage = min(max_leverage, 3)

    # Direction-specific leverage
    # LONG is riskier than SHORT in current market
    max_leverage_long = max(2, max_leverage - 2)
    max_leverage_short = max_leverage

    return {
        'coin': coin,
        'risk': risk_level,
        'vol_daily': round(vol_7d, 2),
        'vol_30d': round(vol_30d, 2),
        'max_drawdown_30d': round(max_dd, 1),
        'avg_volume_usd': avg_volume_usd,
        'liquidity': 'HIGH' if avg_volume_usd > 100_000_000 else 'MEDIUM' if avg_volume_usd > 10_000_000 else 'LOW',
        'max_leverage': max_leverage,
        'max_leverage_long': max_leverage_long,
        'max_leverage_short': max_leverage_short,
        'stop_mult': stop_mult,
    }


def get_safe_leverage(coin: str, direction: str, conn=None) -> int:
    """Get safe leverage for a coin and direction."""
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB_PATH))

    profile = score_coin(conn, coin)

    if own_conn:
        conn.close()

    if direction == 'LONG':
        return profile['max_leverage_long']
    return profile['max_leverage_short']


def get_stop_multiplier(coin: str, conn=None) -> float:
    """Get ATR stop multiplier for a coin (volatile coins get wider stops)."""
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB_PATH))

    profile = score_coin(conn, coin)

    if own_conn:
        conn.close()

    return profile['stop_mult']


if __name__ == '__main__':
    import sqlite3
    conn = sqlite3.connect(str(DB_PATH))
    results = score_all_coins(conn)

    print(f"{'Coin':>6s} {'Risk':>10s} {'Vol%':>6s} {'MaxDD':>6s} {'Volume':>12s} {'Liq':>6s} {'Lev_S':>6s} {'Lev_L':>6s}")
    print("-" * 70)

    for coin, r in sorted(results.items(), key=lambda x: -x[1]['vol_daily']):
        if r['risk'] != 'unknown':
            print(f"  {coin:>4s} {r['risk']:>10s} {r['vol_daily']:>5.1f}% {r['max_drawdown_30d']:>5.1f}% "
                  f"${r['avg_volume_usd']/1e6:>9.1f}M {r['liquidity']:>6s} "
                  f"{r['max_leverage_short']:>5d}x {r['max_leverage_long']:>5d}x")

    conn.close()
