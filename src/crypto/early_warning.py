"""
FORTIX Early Warning System (EWS)
=================================
Detects reversal risk BEFORE Profi scans.
Uses: OI divergence, CVD divergence, RSI extremes, funding velocity,
      long/short crowding, liquidation cascades.

Standalone module — does NOT affect live trading until integrated.
Call: get_reversal_risk() → dict with score (0-100) and components.

Score interpretation:
  0-30:  LOW risk — normal trading
  30-60: MODERATE — add caution to Profi prompt
  60-80: HIGH — reduce positions, tighten parameters
  80-100: DANGER — skip scan or close winners
"""

import sqlite3
import logging
import time
import numpy as np
from pathlib import Path

log = logging.getLogger('early_warning')

MARKET_DB = Path('data/crypto/market.db')


def _get_conn():
    conn = sqlite3.connect(str(MARKET_DB), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _calc_rsi(closes, period=14):
    """Calculate RSI from close prices (newest first → reversed)."""
    if len(closes) < period + 1:
        return 50.0
    prices = list(reversed(closes))
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        return 100.0
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _oi_price_divergence(conn, coin='BTC', hours=4):
    """Detect OI vs Price divergence.
    Price up + OI down = exhaustion (bearish).
    Price down + OI up = absorption (bullish reversal coming).
    Returns: score 0-20, direction hint, details.
    """
    now = int(time.time())
    lookback = now - hours * 3600

    prices = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "AND timestamp > ? ORDER BY timestamp ASC", (coin, lookback)
    ).fetchall()
    oi = conn.execute(
        "SELECT oi_usdt FROM open_interest WHERE coin=? "
        "AND timestamp > ? ORDER BY timestamp ASC", (coin, lookback)
    ).fetchall()

    if len(prices) < 3 or len(oi) < 3:
        return 0, 'neutral', {'error': 'insufficient data'}

    price_chg = (prices[-1][0] - prices[0][0]) / prices[0][0] * 100
    oi_chg = (oi[-1][0] - oi[0][0]) / oi[0][0] * 100

    score = 0
    direction = 'neutral'

    # Price up + OI down = bearish divergence (exhaustion)
    if price_chg > 0.3 and oi_chg < -1.0:
        score = min(20, int(abs(oi_chg) * 4))
        direction = 'bearish_exhaustion'
    # Price down + OI down = capitulation (bullish reversal)
    elif price_chg < -0.3 and oi_chg < -2.0:
        score = min(20, int(abs(oi_chg) * 3))
        direction = 'bullish_capitulation'
    # Price up + OI up strongly = healthy trend (low risk)
    elif price_chg > 0.3 and oi_chg > 2.0:
        score = 0
        direction = 'healthy_trend'
    # Price flat + OI surging = pressure building
    elif abs(price_chg) < 0.3 and oi_chg > 3.0:
        score = min(15, int(oi_chg * 2))
        direction = 'pressure_building'

    return score, direction, {
        'price_chg_pct': round(price_chg, 2),
        'oi_chg_pct': round(oi_chg, 2),
        'hours': hours
    }


def _cvd_divergence(conn, coin='BTC', hours=4):
    """Cumulative Volume Delta divergence.
    CVD = cumulative(buy_volume - sell_volume).
    Price up + CVD declining = sellers absorbing (bearish).
    Price down + CVD rising = buyers accumulating (bullish).
    Returns: score 0-20, direction, details.
    """
    now = int(time.time())
    lookback = now - hours * 3600

    taker = conn.execute(
        "SELECT buy_volume, sell_volume FROM taker_volume "
        "WHERE coin=? AND period='1h' AND timestamp > ? ORDER BY timestamp ASC",
        (coin, lookback)
    ).fetchall()

    prices = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "AND timestamp > ? ORDER BY timestamp ASC", (coin, lookback)
    ).fetchall()

    if len(taker) < 3 or len(prices) < 3:
        return 0, 'neutral', {'error': 'insufficient data'}

    deltas = [t[0] - t[1] for t in taker]
    cvd = np.cumsum(deltas)
    cvd_trend = cvd[-1] - cvd[0] if len(cvd) > 1 else 0
    price_chg = (prices[-1][0] - prices[0][0]) / prices[0][0] * 100

    total_vol = sum(t[0] + t[1] for t in taker)
    cvd_pct = (cvd_trend / total_vol * 100) if total_vol > 0 else 0

    score = 0
    direction = 'neutral'

    # Price up + CVD declining = bearish divergence
    if price_chg > 0.3 and cvd_pct < -5:
        score = min(20, int(abs(cvd_pct) * 1.5))
        direction = 'bearish_divergence'
    # Price down + CVD rising = bullish divergence
    elif price_chg < -0.3 and cvd_pct > 5:
        score = min(20, int(cvd_pct * 1.5))
        direction = 'bullish_divergence'

    return score, direction, {
        'price_chg_pct': round(price_chg, 2),
        'cvd_pct': round(cvd_pct, 2),
        'hours': hours
    }


def _rsi_extreme(conn, coin='BTC'):
    """BTC RSI extreme detection.
    RSI > 70 → block LONGs (7% WR historically).
    RSI < 35 → block SHORTs (14% WR historically).
    Returns: score 0-20, blocked direction, RSI value.
    """
    prices = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "ORDER BY timestamp DESC LIMIT 20", (coin,)
    ).fetchall()

    if len(prices) < 15:
        return 0, None, 50.0

    closes = [p[0] for p in prices]
    rsi = _calc_rsi(closes, 14)

    score = 0
    blocked = None

    if rsi > 75:
        score = min(20, int((rsi - 70) * 2))
        blocked = 'LONG'
    elif rsi > 70:
        score = min(15, int((rsi - 65) * 1.5))
        blocked = 'LONG'
    elif rsi < 30:
        score = min(20, int((35 - rsi) * 2))
        blocked = 'SHORT'
    elif rsi < 35:
        score = min(15, int((40 - rsi) * 1.5))
        blocked = 'SHORT'

    return score, blocked, round(rsi, 1)


def _funding_velocity(conn, coin='BTC', hours=24):
    """Funding rate velocity — not level, but SPEED of change.
    Rapid funding increase = crowded trade → squeeze imminent.
    Returns: score 0-15, direction, details.
    """
    now = int(time.time())
    lookback = now - hours * 3600

    rates = conn.execute(
        "SELECT rate, timestamp FROM funding_rates WHERE coin=? "
        "AND timestamp > ? ORDER BY timestamp ASC", (coin, lookback)
    ).fetchall()

    if len(rates) < 3:
        return 0, 'neutral', {'error': 'insufficient data'}

    current = rates[-1][0]
    earliest = rates[0][0]
    velocity = current - earliest

    score = 0
    direction = 'neutral'

    # Funding spiking positive → long squeeze risk
    if current > 0.0005 and velocity > 0.0002:
        score = min(15, int(abs(velocity) * 100 * 50))
        direction = 'long_squeeze_risk'
    # Funding spiking negative → short squeeze risk
    elif current < -0.0005 and velocity < -0.0002:
        score = min(15, int(abs(velocity) * 100 * 50))
        direction = 'short_squeeze_risk'
    # Extreme funding level even without velocity
    elif abs(current) > 0.001:
        score = min(10, int(abs(current) * 100 * 30))
        direction = 'extreme_funding'

    return score, direction, {
        'current_rate': round(current * 100, 4),
        'velocity_24h': round(velocity * 100, 4),
        'readings': len(rates)
    }


def _crowding_score(conn, coin='BTC'):
    """Long/Short ratio crowding.
    Ratio > 2.5 = too many longs (reversal risk for longs).
    Ratio < 0.4 = too many shorts (reversal risk for shorts).
    Returns: score 0-15, crowded direction, ratio.
    """
    row = conn.execute(
        "SELECT long_short_ratio FROM long_short_ratio "
        "WHERE coin=? AND period='1h' ORDER BY timestamp DESC LIMIT 1", (coin,)
    ).fetchone()

    if not row or not row[0]:
        return 0, None, 1.0

    ratio = row[0]
    score = 0
    crowded = None

    if ratio > 3.0:
        score = 15
        crowded = 'LONG'
    elif ratio > 2.5:
        score = 10
        crowded = 'LONG'
    elif ratio > 2.0:
        score = 5
        crowded = 'LONG'
    elif ratio < 0.33:
        score = 15
        crowded = 'SHORT'
    elif ratio < 0.4:
        score = 10
        crowded = 'SHORT'
    elif ratio < 0.5:
        score = 5
        crowded = 'SHORT'

    return score, crowded, round(ratio, 2)


def _recent_sl_cascade(conn, minutes=30):
    """Detect if multiple positions hit SL recently (correlation cascade).
    Returns: score 0-10, count of recent SLs.
    """
    now_str = time.strftime('%Y-%m-%d %H:%M:%S',
                            time.gmtime(time.time() - minutes * 60))

    sls = conn.execute(
        "SELECT COUNT(*) FROM fortix_trades "
        "WHERE exit_reason='STOP_LOSS' AND closed_at > ?", (now_str,)
    ).fetchone()

    count = sls[0] if sls else 0
    score = 0

    if count >= 4:
        score = 10
    elif count >= 3:
        score = 7
    elif count >= 2:
        score = 4

    return score, count


def _4h_trend_conflict(conn, coin='BTC'):
    """Check if 1H momentum conflicts with 4H trend.
    59% of SL trades conflicted with 4H trend.
    Returns: conflicting direction or None.
    """
    # 4H trend: EMA12 on 4H candles
    prices_4h = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
        "ORDER BY timestamp DESC LIMIT 15", (coin,)
    ).fetchall()

    prices_1h = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "ORDER BY timestamp DESC LIMIT 4", (coin,)
    ).fetchall()

    if len(prices_4h) < 13 or len(prices_1h) < 3:
        return None, 'unknown'

    closes_4h = list(reversed([p[0] for p in prices_4h]))
    multiplier = 2.0 / 13
    ema = closes_4h[0]
    for c in closes_4h[1:]:
        ema = c * multiplier + ema * (1 - multiplier)

    trend_4h = 'UP' if closes_4h[-1] > ema else 'DOWN'

    # 1H momentum
    mom_1h = (prices_1h[0][0] - prices_1h[-1][0]) / prices_1h[-1][0] * 100
    mom_dir = 'UP' if mom_1h > 0.1 else ('DOWN' if mom_1h < -0.1 else 'FLAT')

    conflict = None
    if trend_4h == 'UP' and mom_dir == 'DOWN':
        conflict = 'SHORT'  # shorting against 4H uptrend is risky
    elif trend_4h == 'DOWN' and mom_dir == 'UP':
        conflict = 'LONG'   # longing against 4H downtrend is risky

    return conflict, trend_4h


def get_reversal_risk(coin='BTC'):
    """Main function: compute overall reversal risk score.

    Returns dict:
    {
        'score': 0-100,
        'level': 'LOW'|'MODERATE'|'HIGH'|'DANGER',
        'blocked_direction': 'LONG'|'SHORT'|None,
        'components': {...},
        'recommendation': str
    }
    """
    conn = _get_conn()
    try:
        # 1. OI vs Price divergence (0-20)
        oi_score, oi_dir, oi_details = _oi_price_divergence(conn, coin, hours=4)

        # 2. CVD divergence (0-20)
        cvd_score, cvd_dir, cvd_details = _cvd_divergence(conn, coin, hours=4)

        # 3. RSI extremes (0-20)
        rsi_score, rsi_blocked, rsi_value = _rsi_extreme(conn, coin)

        # 4. Funding velocity (0-15)
        fund_score, fund_dir, fund_details = _funding_velocity(conn, coin)

        # 5. L/S crowding (0-15)
        crowd_score, crowd_dir, crowd_ratio = _crowding_score(conn, coin)

        # 6. Recent SL cascade (0-10)
        cascade_score, cascade_count = _recent_sl_cascade(conn)

        # Total score
        total = oi_score + cvd_score + rsi_score + fund_score + crowd_score + cascade_score
        total = min(100, total)

        # Determine level
        if total >= 80:
            level = 'DANGER'
        elif total >= 60:
            level = 'HIGH'
        elif total >= 30:
            level = 'MODERATE'
        else:
            level = 'LOW'

        # Determine blocked direction (consensus)
        blocked_votes = []
        if rsi_blocked:
            blocked_votes.append(rsi_blocked)
        if crowd_dir:
            blocked_votes.append(crowd_dir)
        if oi_dir == 'bearish_exhaustion':
            blocked_votes.append('LONG')
        elif oi_dir == 'bullish_capitulation':
            blocked_votes.append('SHORT')
        if cvd_dir == 'bearish_divergence':
            blocked_votes.append('LONG')
        elif cvd_dir == 'bullish_divergence':
            blocked_votes.append('SHORT')
        if fund_dir == 'long_squeeze_risk':
            blocked_votes.append('LONG')
        elif fund_dir == 'short_squeeze_risk':
            blocked_votes.append('SHORT')

        # 4H trend conflict
        conflict_dir, trend_4h = _4h_trend_conflict(conn, coin)

        blocked = None
        if blocked_votes:
            from collections import Counter
            counts = Counter(blocked_votes)
            most_common = counts.most_common(1)[0]
            if most_common[1] >= 2:
                blocked = most_common[0]
            elif most_common[1] == 1 and rsi_blocked:
                blocked = rsi_blocked  # RSI alone is strong enough

        # Build recommendation
        parts = []
        if rsi_score > 0:
            parts.append(f"RSI={rsi_value} {'overbought' if rsi_blocked == 'LONG' else 'oversold'}")
        if oi_score > 0:
            parts.append(f"OI divergence ({oi_dir})")
        if cvd_score > 0:
            parts.append(f"CVD divergence ({cvd_dir})")
        if fund_score > 0:
            parts.append(f"funding {fund_dir}")
        if crowd_score > 0:
            parts.append(f"L/S crowded {crowd_dir} ({crowd_ratio})")
        if cascade_score > 0:
            parts.append(f"{cascade_count} SLs in 30min")
        if conflict_dir:
            parts.append(f"4H trend={trend_4h}, {conflict_dir} risky")

        recommendation = "; ".join(parts) if parts else "No significant reversal signals"

        return {
            'score': total,
            'level': level,
            'blocked_direction': blocked,
            'trend_4h': trend_4h,
            'rsi': rsi_value,
            'components': {
                'oi_divergence': {'score': oi_score, 'direction': oi_dir, **oi_details},
                'cvd_divergence': {'score': cvd_score, 'direction': cvd_dir, **cvd_details},
                'rsi_extreme': {'score': rsi_score, 'blocked': rsi_blocked, 'rsi': rsi_value},
                'funding_velocity': {'score': fund_score, 'direction': fund_dir, **fund_details},
                'crowding': {'score': crowd_score, 'crowded': crowd_dir, 'ratio': crowd_ratio},
                'sl_cascade': {'score': cascade_score, 'count': cascade_count},
                'trend_conflict': {'conflict': conflict_dir, 'trend_4h': trend_4h},
            },
            'recommendation': recommendation
        }
    finally:
        conn.close()


def get_reversal_risk_all_coins(coins=None):
    """Get reversal risk for BTC (primary) + individual coin signals."""
    if coins is None:
        coins = ['BTC', 'ETH', 'SOL']

    btc_risk = get_reversal_risk('BTC')

    coin_risks = {}
    conn = _get_conn()
    try:
        for coin in coins:
            if coin == 'BTC':
                coin_risks[coin] = btc_risk
                continue
            rsi_score, rsi_blocked, rsi_val = _rsi_extreme(conn, coin)
            crowd_score, crowd_dir, crowd_ratio = _crowding_score(conn, coin)
            coin_risks[coin] = {
                'rsi': rsi_val,
                'rsi_blocked': rsi_blocked,
                'crowded': crowd_dir,
                'ls_ratio': crowd_ratio,
            }
    finally:
        conn.close()

    return {
        'btc_risk': btc_risk,
        'coin_risks': coin_risks
    }


if __name__ == '__main__':
    import json
    print("=" * 60)
    print("  FORTIX EARLY WARNING SYSTEM — LIVE CHECK")
    print("=" * 60)

    result = get_reversal_risk('BTC')

    print(f"\nOVERALL SCORE: {result['score']}/100 [{result['level']}]")
    print(f"BTC RSI(14): {result['rsi']}")
    print(f"4H Trend: {result['trend_4h']}")
    if result['blocked_direction']:
        print(f"BLOCKED DIRECTION: {result['blocked_direction']}")
    print(f"\nRecommendation: {result['recommendation']}")

    print("\nCOMPONENTS:")
    for name, comp in result['components'].items():
        score = comp.get('score', '-')
        print(f"  {name:20s} score={score:>3}")
        for k, v in comp.items():
            if k != 'score':
                print(f"    {k}: {v}")

    print("\n" + "=" * 60)
    print("  PER-COIN CHECK")
    print("=" * 60)

    all_coins = ['BTC', 'ETH', 'SOL', 'ADA', 'LINK', 'AVAX', 'XRP', 'BNB']
    full = get_reversal_risk_all_coins(all_coins)

    for coin, data in full['coin_risks'].items():
        if coin == 'BTC':
            continue
        rsi = data.get('rsi', '-')
        blocked = data.get('rsi_blocked', '')
        crowded = data.get('crowded', '')
        ratio = data.get('ls_ratio', '-')
        flags = []
        if blocked:
            flags.append(f"block {blocked}")
        if crowded:
            flags.append(f"crowded {crowded}")
        flag_str = " | ".join(flags) if flags else "OK"
        print(f"  {coin:6s} RSI={rsi:>5} L/S={ratio:>5} → {flag_str}")
