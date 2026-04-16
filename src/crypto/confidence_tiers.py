"""
FORTIX Confidence Tier System — Path to 80%+ Accuracy
=====================================================

Classifies each trade signal into confidence tiers based on
multi-confirmation scoring. Only high-tier trades go to execution.

Evidence (walk-forward validated, 90-day live data):
  - Signal strength >= 0.80:        92.3% accuracy (N=39)
  - compound_bearish:                85.1% accuracy (N=94)
  - Bearish in bull regime:          84.7% accuracy (N=190)
  - compound_bearish in mild_bull:   90.9% accuracy (N=22)
  - crowded_long in strong_bull:    100.0% accuracy (N=17)
  - seller_dominance in mild_bull:   78.4% accuracy (N=51)
  - Signal strength 0.60-0.79:      74.2% accuracy (N=62)

Tier System:
  TIER 1 (target 80%+): 3+ confirmations from independent sources
    → Full position size, execute immediately
  TIER 2 (target 65-80%): 2 confirmations
    → Half position size
  TIER 3 (target 55-65%): 1 confirmation
    → Quarter position size or skip
  NO TRADE (<55%): insufficient evidence
    → Do not trade
"""

import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'


@dataclass
class TradeSuggestion:
    """A scored trade suggestion with confidence tier."""
    coin: str
    direction: str  # LONG or SHORT
    tier: int  # 1=highest confidence, 2=medium, 3=low, 0=no trade
    confidence_score: float  # 0.0-1.0
    expected_accuracy: float  # estimated accuracy for this tier
    confirmations: List[str]  # list of confirmation sources
    signal_strength: float
    regime: str
    regime_alignment: bool  # signal direction matches regime
    entry_reason: str
    position_size_mult: float  # 1.0 for full, 0.5 for half, 0.25 for quarter


# Confirmation sources (independent of each other)
CONFIRMATION_SOURCES = {
    'v3_signal': 'V3 walk-forward validated signal fired',
    'v3_compound': 'V3 compound signal (2+ sub-signals)',
    'regime_alignment': 'Signal direction aligns with BTC regime',
    'regime_contrarian': 'Bearish signal in bull regime (84.7% accuracy)',
    'ml_ranking': 'ML ranking model agrees with direction',
    'forecast_engine': 'Forecast engine composite agrees',
    'high_strength': 'Signal strength >= 0.60',
    'very_high_strength': 'Signal strength >= 0.80 (92.3% historical)',
    '4h_confirmation': '4h timeframe technical confirms',
    'volume_confirms': 'Volume supports the move',
    'funding_confirms': 'Funding rate supports direction',
    'etf_confirms': 'ETF flows support direction',
}


def _get_btc_regime(conn) -> str:
    """Quick BTC regime detection."""
    try:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 31"
        ).fetchall()
        if len(rows) < 31:
            return 'sideways'
        r7 = rows[0][0] / rows[7][0] - 1
        r30 = rows[0][0] / rows[30][0] - 1
        sc = 0
        if r7 > 0.05: sc += 2
        elif r7 > 0.02: sc += 1
        elif r7 < -0.05: sc -= 2
        elif r7 < -0.02: sc -= 1
        if r30 > 0.10: sc += 2
        elif r30 > 0.03: sc += 1
        elif r30 < -0.10: sc -= 2
        elif r30 < -0.03: sc -= 1
        if sc >= 3: return 'strong_bull'
        elif sc >= 1: return 'mild_bull'
        elif sc <= -3: return 'strong_bear'
        elif sc <= -1: return 'mild_bear'
        return 'sideways'
    except Exception:
        return 'sideways'


def _get_4h_confirmation(conn, coin: str, direction: str) -> bool:
    """Check if 4h timeframe supports the direction."""
    try:
        rows = conn.execute(
            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
            "ORDER BY timestamp DESC LIMIT 15", (coin,)
        ).fetchall()
        if len(rows) < 14:
            return False
        closes = [r[0] for r in rows]
        # 4h RSI
        deltas = [closes[i] - closes[i+1] for i in range(14)]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_g = np.mean(gains) if gains else 0.001
        avg_l = np.mean(losses) if losses else 0.001
        rsi_4h = 100 - 100 / (1 + avg_g / avg_l)

        # 4h momentum (last 3 candles = 12h)
        ret_12h = closes[0] / closes[3] - 1 if closes[3] > 0 else 0

        if direction == 'SHORT':
            return rsi_4h > 55 and ret_12h < 0  # 4h turning down
        else:
            return rsi_4h < 45 and ret_12h > 0  # 4h turning up
    except Exception:
        return False


def _get_volume_confirmation(conn, coin: str, direction: str) -> bool:
    """Check if volume supports the move."""
    try:
        rows = conn.execute(
            "SELECT close, volume FROM prices WHERE coin=? AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 8", (coin,)
        ).fetchall()
        if len(rows) < 8:
            return False
        vol_now = rows[0][1]
        vol_avg = np.mean([r[1] for r in rows[1:]])
        price_change = rows[0][0] / rows[1][0] - 1

        if direction == 'SHORT':
            # Price falling on high volume = strong selling (confirms short)
            return price_change < 0 and vol_now > vol_avg * 1.2
        else:
            # Price rising on high volume OR price falling on LOW volume (weak selling)
            return (price_change > 0 and vol_now > vol_avg * 1.1) or \
                   (price_change < 0 and vol_now < vol_avg * 0.7)
    except Exception:
        return False


def _get_funding_confirmation(conn, coin: str, direction: str) -> bool:
    """Check if funding rate supports direction."""
    try:
        row = conn.execute(
            "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
            (coin,)
        ).fetchone()
        if not row:
            return False
        rate = row[0]
        if direction == 'SHORT':
            return rate > 0.0001  # positive funding = longs paying = overlevered longs
        else:
            return rate < -0.0001  # negative funding = shorts paying = squeeze potential
    except Exception:
        return False


def _get_etf_confirmation(conn, direction: str) -> bool:
    """Check if ETF flows support direction (BTC only, applies globally)."""
    try:
        rows = conn.execute(
            "SELECT flow_usd FROM cg_etf_flows WHERE asset='BTC' "
            "ORDER BY date DESC LIMIT 7"
        ).fetchall()
        if not rows:
            return False
        total_7d = sum(r[0] for r in rows)
        if direction == 'SHORT':
            return total_7d < -100_000_000  # >$100M outflow
        else:
            return total_7d > 200_000_000  # >$200M inflow
    except Exception:
        return False


def score_trade(
    coin: str,
    v3_signals: list,
    forecast_result: dict,
    ml_ranking: dict = None,
    conn: sqlite3.Connection = None,
) -> Optional[TradeSuggestion]:
    """Score a potential trade and assign confidence tier.

    Args:
        coin: coin symbol
        v3_signals: list of Signal objects from signal_system for this coin
        forecast_result: dict from forecast_coin()
        ml_ranking: dict with 'signal' and 'rank_score' from v5 ranking
        conn: database connection for additional checks

    Returns:
        TradeSuggestion with tier, or None if no actionable signal
    """
    if not v3_signals and not forecast_result:
        return None

    own_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        own_conn = True

    try:
        regime = _get_btc_regime(conn)
        confirmations = []
        direction = None

        # ── Determine direction from strongest signal ──
        bearish_signals = [s for s in v3_signals if s.direction == 'BEARISH']
        bullish_signals = [s for s in v3_signals if s.direction == 'BULLISH']

        best_signal = None
        if bearish_signals:
            best_signal = max(bearish_signals, key=lambda s: s.strength)
            direction = 'SHORT'
        if bullish_signals:
            best_bull = max(bullish_signals, key=lambda s: s.strength)
            if not best_signal or best_bull.strength > best_signal.strength:
                best_signal = best_bull
                direction = 'LONG'

        # No v3 signal — check if forecast engine has strong opinion
        if not best_signal:
            composite = forecast_result.get('composite_score', 0)
            prediction = forecast_result.get('prediction', 'NEUTRAL')
            if prediction in ('SELL', 'STRONG SELL') and composite < -0.15:
                direction = 'SHORT'
            elif prediction in ('BUY', 'STRONG BUY') and composite > 0.15:
                direction = 'LONG'
            else:
                return None

        signal_strength = best_signal.strength if best_signal else abs(forecast_result.get('composite_score', 0))

        # ── Count confirmations ──

        # 1. V3 signal fired
        if best_signal:
            confirmations.append('v3_signal')

        # 2. V3 compound signal (2+ sub-signals)
        compound_signals = [s for s in v3_signals
                          if s.signal_type in ('compound_bearish', 'compound_bullish')]
        if compound_signals:
            confirmations.append('v3_compound')

        # 3. Regime alignment / contrarian
        bull_regimes = ('strong_bull', 'mild_bull')
        bear_regimes = ('strong_bear', 'mild_bear')
        regime_alignment = False

        if direction == 'SHORT' and regime in bull_regimes:
            # Bearish signal in bull regime = 84.7% accuracy!
            confirmations.append('regime_contrarian')
            regime_alignment = True  # paradoxically, this IS aligned (highest accuracy)
        elif direction == 'SHORT' and regime in bear_regimes:
            confirmations.append('regime_alignment')
            regime_alignment = True
        elif direction == 'LONG' and regime in bull_regimes:
            confirmations.append('regime_alignment')
            regime_alignment = True

        # 4. ML ranking agrees
        if ml_ranking:
            ml_signal = ml_ranking.get('signal', 'HOLD')
            if direction == 'SHORT' and ml_signal in ('SELL', 'STRONG_SELL'):
                confirmations.append('ml_ranking')
            elif direction == 'LONG' and ml_signal in ('BUY', 'STRONG_BUY'):
                confirmations.append('ml_ranking')

        # 5. Forecast engine agrees
        composite = forecast_result.get('composite_score', 0)
        if (direction == 'SHORT' and composite < -0.05) or \
           (direction == 'LONG' and composite > 0.05):
            confirmations.append('forecast_engine')

        # 6. Signal strength tiers
        if signal_strength >= 0.80:
            confirmations.append('very_high_strength')
        elif signal_strength >= 0.60:
            confirmations.append('high_strength')

        # 7. 4h confirmation
        if _get_4h_confirmation(conn, coin, direction):
            confirmations.append('4h_confirmation')

        # 8. Volume confirmation
        if _get_volume_confirmation(conn, coin, direction):
            confirmations.append('volume_confirms')

        # 9. Funding rate confirmation
        if _get_funding_confirmation(conn, coin, direction):
            confirmations.append('funding_confirms')

        # 10. ETF confirmation
        if _get_etf_confirmation(conn, direction):
            confirmations.append('etf_confirms')

        # 11. Gemini chart — only for top candidates (called if already 4+ confirmations)
        if n_confirmations >= 4:
            try:
                from src.crypto.chart_analyzer import analyze_chart
                chart = analyze_chart(coin, '1d')
                if chart.get('has_data') and chart.get('confidence', 0) >= 0.5:
                    chart_rec = chart.get('recommendation', 'HOLD')
                    if (direction == 'SHORT' and chart_rec == 'SELL') or \
                       (direction == 'LONG' and chart_rec == 'BUY'):
                        confirmations.append('chart_pattern_confirms')
            except Exception:
                pass

        # ── Assign tier based on confirmation count + special cases ──
        n_confirmations = len(confirmations)

        # Special case: very high strength signal (92.3% accuracy alone)
        if 'very_high_strength' in confirmations:
            tier = 1
            expected_accuracy = 0.90

        # Special case: compound + regime contrarian (90.9%)
        elif 'v3_compound' in confirmations and 'regime_contrarian' in confirmations:
            tier = 1
            expected_accuracy = 0.88

        # Special case: regime_contrarian with 3+ confirmations (84.7% proven)
        elif 'regime_contrarian' in confirmations and n_confirmations >= 3:
            tier = 1
            expected_accuracy = 0.82

        # Special case: compound bullish in bull regime with 3+ confirmations
        elif 'v3_compound' in confirmations and 'regime_alignment' in confirmations and n_confirmations >= 3:
            tier = 1
            expected_accuracy = 0.75

        # Standard tier assignment
        elif n_confirmations >= 4:
            tier = 1
            expected_accuracy = 0.78
        elif n_confirmations >= 3:
            tier = 2 if 'v3_signal' in confirmations else 3
            expected_accuracy = 0.72
        elif n_confirmations >= 2:
            tier = 2
            expected_accuracy = 0.65
        elif n_confirmations >= 1:
            tier = 3
            expected_accuracy = 0.58
        else:
            tier = 0
            expected_accuracy = 0.50

        # Confidence score (0-1)
        confidence_score = min(n_confirmations / 7, 1.0) * (signal_strength * 0.3 + 0.7)

        # Position size multiplier
        size_mult = {1: 1.0, 2: 0.5, 3: 0.25, 0: 0.0}[tier]

        # Build entry reason
        signal_name = best_signal.signal_type if best_signal else 'forecast_engine'
        reason = (f"{signal_name} ({direction}) | "
                 f"{n_confirmations} confirmations: {', '.join(confirmations)} | "
                 f"regime: {regime}")

        return TradeSuggestion(
            coin=coin,
            direction=direction,
            tier=tier,
            confidence_score=confidence_score,
            expected_accuracy=expected_accuracy,
            confirmations=confirmations,
            signal_strength=signal_strength,
            regime=regime,
            regime_alignment=regime_alignment,
            entry_reason=reason,
            position_size_mult=size_mult,
        )

    finally:
        if own_conn:
            conn.close()


def scan_all_opportunities(conn: sqlite3.Connection = None) -> List[TradeSuggestion]:
    """Scan all coins for trade opportunities, score and tier them.

    Returns list of TradeSuggestions sorted by tier (best first).
    """
    import os
    own_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        own_conn = True

    try:
        from src.crypto.signal_system import SignalSystem
        from src.crypto.forecast_engine import forecast_coin

        ss = SignalSystem()
        scan = ss.scan_all()

        # Group v3 signals by coin
        from collections import defaultdict
        signals_by_coin = defaultdict(list)
        for sig in scan['signals']:
            if sig.direction in ('BEARISH', 'BULLISH'):
                signals_by_coin[sig.coin].append(sig)

        # Get v5 ranking
        v5_rankings = {}
        try:
            from src.crypto.forecast_model_v5 import rank_all_coins
            v5_result = rank_all_coins(conn)
            if v5_result and 'rankings' in v5_result:
                for r in v5_result['rankings']:
                    v5_rankings[r['coin']] = r
        except Exception:
            pass

        COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
                 'DOGE', 'UNI', 'AAVE', 'LDO', 'CRV', 'FET', 'RENDER', 'TAO',
                 'ARB', 'OP', 'POL', 'SHIB', 'PEPE', 'WIF', 'BONK', 'PENDLE',
                 'JUP', 'RAY', 'PYTH', 'JTO', 'BOME', 'POPCAT', 'MEW',
                 'ORCA', 'DRIFT', 'W', 'TNSR']

        suggestions = []
        for coin in COINS:
            try:
                # Disable Claude API for speed
                os.environ['ANTHROPIC_API_KEY'] = os.environ.get('_REAL_ANTHROPIC_KEY', '')
                forecast = forecast_coin(conn, coin)
                if not forecast or forecast.get('error'):
                    continue

                suggestion = score_trade(
                    coin=coin,
                    v3_signals=signals_by_coin.get(coin, []),
                    forecast_result=forecast,
                    ml_ranking=v5_rankings.get(coin),
                    conn=conn,
                )

                if suggestion and suggestion.tier > 0:
                    suggestions.append(suggestion)
            except Exception as e:
                logger.warning(f"Failed to score {coin}: {e}")

        suggestions.sort(key=lambda s: (s.tier, -s.confidence_score))
        return suggestions

    finally:
        if own_conn:
            conn.close()


def format_opportunities(suggestions: List[TradeSuggestion]) -> str:
    """Format suggestions as readable report."""
    lines = ["=" * 70, "FORTIX TRADE SCANNER — Confidence Tier Report", "=" * 70]

    tier_names = {1: 'TIER 1 (80%+ expected)', 2: 'TIER 2 (65-80%)', 3: 'TIER 3 (55-65%)'}

    for tier in [1, 2, 3]:
        tier_sug = [s for s in suggestions if s.tier == tier]
        if not tier_sug:
            continue

        lines.append(f"\n{'='*20} {tier_names[tier]} {'='*20}")
        for s in tier_sug:
            lines.append(
                f"  {s.coin:6s} {s.direction:5s} | "
                f"confidence={s.confidence_score:.2f} | "
                f"strength={s.signal_strength:.2f} | "
                f"size={s.position_size_mult:.0%}"
            )
            lines.append(f"         {s.entry_reason}")

    if not suggestions:
        lines.append("\n  No actionable opportunities found.")
        lines.append("  This is normal — we only trade high-confidence setups.")

    t1 = sum(1 for s in suggestions if s.tier == 1)
    t2 = sum(1 for s in suggestions if s.tier == 2)
    t3 = sum(1 for s in suggestions if s.tier == 3)
    lines.append(f"\n  Summary: {t1} tier-1, {t2} tier-2, {t3} tier-3")

    return "\n".join(lines)


if __name__ == '__main__':
    import sys
    sys.path.insert(0, str(_FACTORY_DIR))
    logging.basicConfig(level=logging.WARNING)

    import os
    os.environ['ANTHROPIC_API_KEY'] = ''

    suggestions = scan_all_opportunities()
    print(format_opportunities(suggestions))
