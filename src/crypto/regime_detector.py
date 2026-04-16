"""
FORTIX — Market Regime Detector
=======================================
Classifies current market as: strong_bull, mild_bull, ranging, mild_bear, strong_bear.
Uses BTC price action, Fear & Greed, volatility, and breadth indicators.

Training data shows:
- mild_bull: 78.5% accuracy (best edge)
- ranging: 64.8% (decent edge)
- mild_bear: 62.4% (weak edge)
- strong_bear: 50.0% (NO edge = coin flip)

The regime detector adjusts forecast behavior:
- strong_bear: suppress all signals (no edge)
- mild_bear: raise thresholds (be more selective)
- ranging: normal behavior
- mild_bull: lower thresholds (more signals, high accuracy)
- strong_bull: moderate thresholds (risk of reversal)

Usage:
    from src.crypto.regime_detector import detect_regime, get_regime_multipliers
"""

import sqlite3
import logging
import numpy as np
from pathlib import Path

log = logging.getLogger('regime_detector')

MARKET_DB = Path('data/crypto/market.db')


def detect_regime(conn=None):
    """Detect current market regime from BTC data + Fear & Greed.

    Returns dict with:
    - regime: str ('strong_bull', 'mild_bull', 'ranging', 'mild_bear', 'strong_bear')
    - confidence: float (0-1)
    - details: dict of component signals
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(MARKET_DB), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        close_conn = True

    try:
        # 1. BTC price trends (multiple timeframes)
        btc_prices = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 30"
        ).fetchall()

        if len(btc_prices) < 14:
            return {'regime': 'ranging', 'confidence': 0.0,
                    'details': {'error': 'insufficient BTC data'}}

        closes = [p[0] for p in btc_prices if p[0] is not None]
        if len(closes) < 14:
            return {'regime': 'ranging', 'confidence': 0.0,
                    'details': {'error': 'null prices'}}

        # Price changes at different horizons
        chg_7d = (closes[0] - closes[6]) / closes[6] * 100 if closes[6] else 0
        chg_14d = (closes[0] - closes[13]) / closes[13] * 100 if closes[13] else 0
        chg_30d = (closes[0] - closes[-1]) / closes[-1] * 100 if closes[-1] else 0

        # Recent volatility (7d std of daily returns)
        daily_returns = [(closes[i] - closes[i+1]) / closes[i+1]
                         for i in range(min(7, len(closes)-1)) if closes[i+1] != 0]
        volatility = float(np.std(daily_returns)) if daily_returns else 0.03

        # 2. Fear & Greed Index
        fg_row = conn.execute(
            "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1"
        ).fetchone()
        fg = float(fg_row[0]) if fg_row and fg_row[0] is not None else 50

        # 3. Market breadth: % of coins above their 7d price
        breadth_rows = conn.execute(
            """SELECT coin,
               (SELECT close FROM prices p2 WHERE p2.coin=p1.coin AND p2.timeframe='1d'
                ORDER BY timestamp DESC LIMIT 1) as current_price,
               (SELECT close FROM prices p3 WHERE p3.coin=p1.coin AND p3.timeframe='1d'
                ORDER BY timestamp DESC LIMIT 1 OFFSET 6) as price_7d_ago
            FROM (SELECT DISTINCT coin FROM prices WHERE timeframe='1d') p1"""
        ).fetchall()

        coins_up = 0
        coins_total = 0
        for row in breadth_rows:
            if row[1] and row[2] and row[2] > 0:
                coins_total += 1
                if row[1] > row[2]:
                    coins_up += 1
        breadth = coins_up / coins_total if coins_total > 0 else 0.5

        # 4. Classify regime using multi-signal voting
        score = 0.0  # -2 = strong_bear, +2 = strong_bull

        # BTC trend component (weight: 40%)
        if chg_7d > 10:
            score += 0.8
        elif chg_7d > 3:
            score += 0.4
        elif chg_7d < -10:
            score -= 0.8
        elif chg_7d < -3:
            score -= 0.4

        # 14d trend for confirmation
        if chg_14d > 15:
            score += 0.4
        elif chg_14d > 5:
            score += 0.2
        elif chg_14d < -15:
            score -= 0.4
        elif chg_14d < -5:
            score -= 0.2

        # Fear & Greed component (weight: 30%)
        if fg >= 75:
            score += 0.3  # extreme greed = bull
        elif fg >= 55:
            score += 0.15
        elif fg <= 25:
            score -= 0.3  # extreme fear = bear
        elif fg <= 40:
            score -= 0.15

        # Breadth component (weight: 20%)
        if breadth >= 0.7:
            score += 0.2  # most coins rising
        elif breadth >= 0.5:
            score += 0.1
        elif breadth <= 0.3:
            score -= 0.2  # most coins falling
        elif breadth <= 0.4:
            score -= 0.1

        # Volatility component (weight: 10%)
        if volatility > 0.06:
            # High volatility = uncertainty, push toward extreme
            score *= 1.2
        elif volatility < 0.02:
            # Low volatility = ranging
            score *= 0.5

        # Classify
        if score >= 1.0:
            regime = 'strong_bull'
        elif score >= 0.3:
            regime = 'mild_bull'
        elif score <= -1.0:
            regime = 'strong_bear'
        elif score <= -0.3:
            regime = 'mild_bear'
        else:
            regime = 'ranging'

        confidence = min(abs(score) / 1.5, 1.0)

        details = {
            'btc_7d': round(chg_7d, 2),
            'btc_14d': round(chg_14d, 2),
            'btc_30d': round(chg_30d, 2),
            'fear_greed': fg,
            'breadth': round(breadth, 3),
            'volatility': round(volatility, 4),
            'raw_score': round(score, 3),
        }

        return {'regime': regime, 'confidence': confidence, 'details': details}

    except Exception as e:
        log.warning(f"Regime detection failed: {e}")
        return {'regime': 'ranging', 'confidence': 0.0,
                'details': {'error': str(e)}}
    finally:
        if close_conn:
            conn.close()


def get_regime_multipliers(regime):
    """Get threshold multipliers for a given regime.

    Returns dict with:
    - buy_mult: multiplier for buy threshold (lower = more BUY signals)
    - sell_mult: multiplier for sell threshold (higher abs = fewer SELL)
    - blend_mult: multiplier for ML blend weight
    - suppress: bool (True = suppress ALL signals)
    """
    multipliers = {
        'strong_bull': {
            'buy_mult': 0.7,    # lower buy threshold → more BUY signals
            'sell_mult': 1.3,   # raise sell threshold → fewer false SELL
            'blend_mult': 1.0,
            'suppress': False,
        },
        'mild_bull': {
            'buy_mult': 0.8,    # slightly lower BUY threshold
            'sell_mult': 1.1,   # slightly higher SELL threshold
            'blend_mult': 1.0,
            'suppress': False,
        },
        'ranging': {
            'buy_mult': 1.0,    # normal thresholds
            'sell_mult': 1.0,
            'blend_mult': 1.0,
            'suppress': False,
        },
        'mild_bear': {
            'buy_mult': 1.3,    # raise BUY threshold → fewer (but more accurate) BUY
            'sell_mult': 0.8,   # lower SELL threshold → more SELL signals
            'blend_mult': 1.0,
            'suppress': False,
        },
        'strong_bear': {
            'buy_mult': 2.0,    # very high BUY threshold (almost no BUY)
            'sell_mult': 0.7,   # lower SELL threshold → more SELL
            'blend_mult': 0.5,  # reduce ML weight (50% = coin flip)
            'suppress': False,  # still give signals but reduced confidence
        },
    }
    return multipliers.get(regime, multipliers['ranging'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    result = detect_regime()
    print(f"Regime: {result['regime']} (confidence: {result['confidence']:.2f})")
    for k, v in result['details'].items():
        print(f"  {k}: {v}")
    print()
    mults = get_regime_multipliers(result['regime'])
    print(f"Multipliers: {mults}")
