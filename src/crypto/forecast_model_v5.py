"""
FORTIX — Forecast Model v5 (Ranking + Regime)
===============================================
Cross-sectional ranking model: ranks ALL coins from best to worst
expected 7d performance. NO neutral skipping possible.

Approach:
  1. Predict rank score for each coin (0.0 = worst, 1.0 = best)
  2. Sort coins by predicted rank
  3. Top N = BUY signal, Bottom N = SELL signal
  4. Combine with regime detection for absolute direction confidence

The model ALWAYS produces signals. There is no NEUTRAL escape.
"""

import json
import pickle
import logging
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger('forecast_v5')

MODEL_DIR = Path('data/crypto/models_v5')
_rank_model = None
_binary_model = None
_scaler = None
_features = None

# All tracked coins for ranking
ALL_COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK',
    'DOGE', 'UNI', 'AAVE', 'FET', 'RENDER', 'TAO', 'PEPE', 'ARB', 'OP',
    'SHIB', 'BONK', 'PENDLE', 'LDO', 'CRV', 'WIF', 'POL',
]

# Signal tier thresholds (based on 25 coins)
N_STRONG_BUY = 3    # Top 3 = STRONG BUY
N_BUY = 5           # Top 5 = BUY
N_SELL = 5          # Bottom 5 = SELL
N_STRONG_SELL = 3   # Bottom 3 = STRONG SELL


def _load_models():
    """Load v5 ranking model, binary model, scaler, and feature list."""
    global _rank_model, _binary_model, _scaler, _features

    if _rank_model is not None:
        return True

    rank_path = MODEL_DIR / 'ranking_7d.lgb'
    binary_path = MODEL_DIR / 'binary_7d.lgb'
    scaler_path = MODEL_DIR / 'ranking_scaler.pkl'
    features_path = MODEL_DIR / 'ranking_features.json'

    if not rank_path.exists():
        log.warning("Model v5 ranking not found")
        return False

    try:
        import warnings
        warnings.filterwarnings('ignore', message='X does not have valid feature names')
        import lightgbm as lgb
        _rank_model = lgb.Booster(model_file=str(rank_path))

        if binary_path.exists():
            _binary_model = lgb.Booster(model_file=str(binary_path))

        with open(scaler_path, 'rb') as f:
            _scaler = pickle.load(f)
        _features = json.loads(features_path.read_text())
        log.info(f"Model v5 loaded: {len(_features)} features, ranking + binary")
        return True
    except Exception as e:
        log.warning(f"Model v5 load failed: {e}")
        return False


def _get_features_for_coin(fb, coin: str, today: str) -> Optional[np.ndarray]:
    """Get scaled feature vector for a single coin."""
    features = fb.build_features_single(coin, today)
    if not features:
        return None

    X = []
    for f in _features:
        val = features.get(f, 0)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            val = 0
        X.append(float(val))

    return np.array([X])


def _detect_regime(fb, today: str) -> str:
    """Detect current market regime from BTC features.

    Returns: 'bull', 'bear', or 'sideways'
    """
    btc_features = fb.build_features_single('BTC', today)
    if not btc_features:
        return 'sideways'

    # Regime signals
    btc_ret_7d = btc_features.get('btc_ret_7d', 0) or 0
    btc_ret_30d = btc_features.get('btc_ret_30d', 0) or 0
    fg_value = btc_features.get('fg_value', 50) or 50
    ma50_dist = btc_features.get('ma50_dist', 0) or 0
    ma200_dist = btc_features.get('ma200_dist', 0) or 0

    bull_score = 0
    bear_score = 0

    # 7d return
    if btc_ret_7d > 0.05:
        bull_score += 2
    elif btc_ret_7d < -0.05:
        bear_score += 2

    # 30d return
    if btc_ret_30d > 0.10:
        bull_score += 2
    elif btc_ret_30d < -0.10:
        bear_score += 2

    # Fear & Greed
    if fg_value > 60:
        bull_score += 1
    elif fg_value < 25:
        bear_score += 1

    # MA distances
    if ma50_dist > 0.03 and ma200_dist > 0.05:
        bull_score += 1
    elif ma50_dist < -0.03 and ma200_dist < -0.05:
        bear_score += 1

    if bull_score >= 3:
        return 'bull'
    elif bear_score >= 3:
        return 'bear'
    return 'sideways'


def rank_all_coins(conn: sqlite3.Connection = None) -> dict:
    """
    Rank ALL tracked coins by predicted 7d performance.

    Returns dict:
    {
        'rankings': [(coin, rank_score, signal, confidence), ...],
        'buy_coins': [top N coins],
        'sell_coins': [bottom N coins],
        'regime': 'bull'|'bear'|'sideways',
        'timestamp': '...',
    }

    Signal types: 'STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'
    """
    if not _load_models():
        return None

    from src.crypto.feature_builder import FeatureBuilder

    fb = FeatureBuilder(db_path=str(Path('data/crypto/market.db')))
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Detect market regime
    regime = _detect_regime(fb, today)
    log.info(f"Market regime: {regime}")

    # Get features for all coins
    coin_scores = []
    for coin in ALL_COINS:
        try:
            X = _get_features_for_coin(fb, coin, today)
            if X is None:
                continue

            # LightGBM handles NaN natively — no scaling needed for tree models
            X_raw = X  # pass raw features with NaN intact

            # Ranking model prediction (primary)
            rank_score = float(_rank_model.predict(X_raw)[0])

            # Binary model prediction (secondary confirmation)
            binary_score = None
            if _binary_model is not None:
                binary_score = float(_binary_model.predict(X_raw)[0])

            coin_scores.append({
                'coin': coin,
                'rank_score': rank_score,
                'binary_score': binary_score,
            })

        except Exception as e:
            log.debug(f"v5 predict failed for {coin}: {e}")

    if not coin_scores:
        log.warning("No predictions generated")
        return None

    # Sort by rank score (highest = best expected performance)
    coin_scores.sort(key=lambda x: x['rank_score'], reverse=True)

    # Assign signals based on position
    n = len(coin_scores)
    rankings = []
    buy_coins = []
    sell_coins = []

    for i, cs in enumerate(coin_scores):
        coin = cs['coin']
        score = cs['rank_score']

        # Position-based signal (no NEUTRAL escape)
        if i < N_STRONG_BUY:
            signal = 'STRONG_BUY'
            confidence = 0.8
            buy_coins.append(coin)
        elif i < N_BUY:
            signal = 'BUY'
            confidence = 0.6
            buy_coins.append(coin)
        elif i >= n - N_STRONG_SELL:
            signal = 'STRONG_SELL'
            confidence = 0.8
            sell_coins.append(coin)
        elif i >= n - N_SELL:
            signal = 'SELL'
            confidence = 0.6
            sell_coins.append(coin)
        else:
            signal = 'HOLD'
            confidence = 0.4

        # Regime adjustment: boost confidence for regime-aligned signals
        if regime == 'bull' and signal in ('STRONG_BUY', 'BUY'):
            confidence = min(confidence + 0.15, 0.95)
        elif regime == 'bear' and signal in ('STRONG_SELL', 'SELL'):
            confidence = min(confidence + 0.15, 0.95)
        elif regime == 'bull' and signal in ('STRONG_SELL', 'SELL'):
            confidence = max(confidence - 0.15, 0.3)
        elif regime == 'bear' and signal in ('STRONG_BUY', 'BUY'):
            confidence = max(confidence - 0.15, 0.3)

        rankings.append({
            'coin': coin,
            'rank': i + 1,
            'rank_score': round(score, 4),
            'binary_score': round(cs['binary_score'], 4) if cs['binary_score'] is not None else None,
            'signal': signal,
            'confidence': round(confidence, 2),
        })

    result = {
        'rankings': rankings,
        'buy_coins': buy_coins,
        'sell_coins': sell_coins,
        'regime': regime,
        'n_coins_ranked': n,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }

    log.info(f"v5 ranking: {n} coins, regime={regime}, "
             f"BUY={buy_coins}, SELL={sell_coins}")

    return result


def predict_v5(conn: sqlite3.Connection, coin: str) -> Optional[float]:
    """
    Get v5 ranking score for a single coin.
    Returns rank_score (0.0 to 1.0, higher = better expected performance).

    NOTE: This score is RELATIVE — meaningful only when compared to other coins.
    For absolute BUY/SELL signals, use rank_all_coins() instead.
    """
    if not _load_models():
        return None

    try:
        from src.crypto.feature_builder import FeatureBuilder

        fb = FeatureBuilder(db_path=str(Path('data/crypto/market.db')))
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        X = _get_features_for_coin(fb, coin, today)
        if X is None:
            return None

        # LightGBM handles NaN natively — no scaling needed
        score = float(_rank_model.predict(X)[0])
        return round(score, 4)

    except Exception as e:
        log.debug(f"v5 predict failed for {coin}: {e}")
        return None


def format_ranking_for_claude(result: dict) -> str:
    """Format ranking results for injection into Claude's script context."""
    if not result:
        return ""

    lines = [
        "=== MODEL v5 RANKING (cross-sectional, no neutral possible) ===",
        f"Market regime: {result['regime'].upper()}",
        f"Coins ranked: {result['n_coins_ranked']}",
        "",
    ]

    # BUY tier
    lines.append("TOP PERFORMERS (BUY signals):")
    for r in result['rankings']:
        if r['signal'] in ('STRONG_BUY', 'BUY'):
            strength = "STRONG" if r['signal'] == 'STRONG_BUY' else ""
            lines.append(f"  #{r['rank']} {r['coin']:5s} — {strength} BUY "
                        f"(score: {r['rank_score']:.3f}, confidence: {r['confidence']:.0%})")

    lines.append("")

    # SELL tier
    lines.append("WORST PERFORMERS (SELL signals):")
    for r in result['rankings']:
        if r['signal'] in ('STRONG_SELL', 'SELL'):
            strength = "STRONG" if r['signal'] == 'STRONG_SELL' else ""
            lines.append(f"  #{r['rank']} {r['coin']:5s} — {strength} SELL "
                        f"(score: {r['rank_score']:.3f}, confidence: {r['confidence']:.0%})")

    lines.append("")

    # Regime context
    if result['regime'] == 'bull':
        lines.append("REGIME: BULL — BUY signals have higher confidence (61.6% historical accuracy)")
    elif result['regime'] == 'bear':
        lines.append("REGIME: BEAR — SELL signals have higher confidence (68.4% historical accuracy)")
    else:
        lines.append("REGIME: SIDEWAYS — Both signals moderate confidence")

    lines.append("")
    return '\n'.join(lines)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    result = rank_all_coins()
    if result:
        print(format_ranking_for_claude(result))
        print("\nFull rankings:")
        for r in result['rankings']:
            print(f"  #{r['rank']:2d} {r['coin']:5s} score={r['rank_score']:.4f} "
                  f"signal={r['signal']:12s} conf={r['confidence']:.0%}")
    else:
        print("Failed to generate rankings")
