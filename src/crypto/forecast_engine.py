"""
FORTIX — Forecast Engine v4
====================================
Combines all signal categories into a composite forecast score.

Signal categories (v19 — 50-period training):
  1. Technical (23%) — MA crossovers, RSI, Bollinger, MACD, volume
  2. Sentiment (2%) — F&G mechanical (51% = barely above random)
  3. On-chain (0%) — REMOVED (37.5% = harmful, worse than random)
  4. Macro (5%) — BTC dominance, DeFi TVL
  5. News (2%) — mechanical news scoring (fallback)
  6. Historical (16%) — vol-normalized pattern matching
  7. Learned (9%) — pattern database from training (69 patterns)
  8. Meta-analyst (12%) — Claude Sonnet contextual analysis
  9. News Claude (17%) — Claude Haiku news analysis
  10. CoinGlass (8%) — aggregated derivatives + ETF + options
  11. CryptoQuant (6%) — on-chain fundamentals

v19 improvements (from 50-period training at 58.8%):
  - Onchain removed (37.5% solo accuracy = actively harmful)
  - MA200 trend bias: above=91.7% accuracy → amplify, below=52.5% → dampen
  - Agreement filter: high agreement 72.2% vs low 56.4% → adaptive thresholds
  - Bull regime trend-following: add positive bias, not just counter-dampen
  - RSI oversold boost: 85.7% accuracy → trust oversold signals more

Output: composite score [-1.0 ... +1.0] + prediction + confidence
"""

import sys
import sqlite3
import logging
import json
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('forecast')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

# ── V5 ranking cache (refreshed once per forecast cycle) ──
_v5_ranking_cache = None
_v5_ranking_date = None

# Score thresholds — narrowed NEUTRAL band for actionable signals
THRESHOLDS = {
    'strong_buy': 0.4,
    'buy': 0.1,
    'neutral_low': -0.1,
    'sell': -0.4,
}

# Sector classification for rotation analysis
COIN_SECTORS = {
    'L1': ['BTC', 'ETH', 'SOL', 'BNB', 'ADA', 'AVAX', 'DOT'],
    'DeFi': ['UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV', 'LINK'],
    'L2': ['ARB', 'OP', 'STRK', 'ZK', 'POL'],
    'AI': ['FET', 'RENDER', 'AGIX', 'TAO'],
    'Meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}

# Coin groups for per-group weight optimization (Level 1 Phase A)
# Must match COIN_GROUPS in weight_optimizer.py
COIN_GROUPS = {
    'majors': ['BTC', 'ETH'],
    'l1_alts': ['SOL', 'BNB', 'ADA', 'AVAX', 'DOT', 'XRP'],
    'defi': ['AAVE', 'UNI', 'PENDLE', 'CRV', 'LDO', 'LINK'],
    'ai': ['FET', 'RENDER', 'TAO'],
    'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}

# Category weights — v20 config (regime-aware, OOS validated)
# Based on walk-forward temporal split: 76.8% test accuracy, -9.2pp gap
# Only categories with proven solo accuracy > 50% get significant weight:
#   technical: 55.4% solo → primary signal
#   learned: 56.0% solo → secondary signal
# Other categories contribute via ensemble effect, not solo accuracy
WEIGHTS = {
    'technical': 0.35,     # primary signal (55.4% solo, strongest validated)
    'learned': 0.20,       # pattern database (56.0% solo, second validated)
    'meta_analyst': 0.15,  # Claude Sonnet synthesis (adds context, not tested solo)
    'news_claude': 0.10,   # Claude Haiku news analysis
    'macro': 0.08,         # BTC dominance + DeFi TVL + DXY
    'coinglass': 0.05,     # aggregated derivatives (43.4% solo — low weight)
    'cryptoquant': 0.04,   # on-chain fundamentals (48.8% solo — low weight)
    'sentiment': 0.02,     # F&G mechanical (47.5% solo — minimal)
    'news': 0.01,          # mechanical news scoring (43.5% solo — minimal)
    'historical': 0.00,    # REMOVED — 48% solo accuracy, v20 uses 'learned' instead
    'onchain': 0.00,       # REMOVED — 37.5% solo accuracy = harmful signal
}

# Regime-specific weight adjustments
# In bull markets: boost technical, reduce learned (mean-reversion oriented)
# In bear markets: boost learned (pattern recognition), maintain technical
REGIME_WEIGHT_ADJUSTMENTS = {
    'strong_bull': {'technical': +0.05, 'meta_analyst': +0.05, 'learned': -0.05, 'coinglass': -0.05},
    'mild_bull':   {'technical': +0.03, 'meta_analyst': +0.02, 'learned': -0.03, 'coinglass': -0.02},
    'sideways':    {},  # default weights work best in sideways
    'mild_bear':   {'learned': +0.05, 'coinglass': +0.03, 'technical': -0.03, 'meta_analyst': -0.05},
    'strong_bear': {'learned': +0.08, 'coinglass': +0.05, 'technical': -0.05, 'meta_analyst': -0.08},
}

# Quality gate: minimum agreement ratio to emit BUY/SELL
# Training: high agreement (≥60%) = 78-92% accuracy, low = 50-57%
QUALITY_GATE = 0.0  # 0.0 = disabled (default)


def _load_optimized_config():
    """Load scipy-optimized config from JSON if available.

    v20: DISABLED weight override — optimizer produces overfitted weights
    (80% training → 51% live accuracy, 30pp gap). Hardcoded v20 weights
    have 76.8% OOS accuracy with only 9pp gap.
    Only thresholds are loaded from optimizer (buy/sell thresholds, quality gate).
    """
    config_path = _FACTORY_DIR / 'data' / 'crypto' / 'optimized_config.json'
    if not config_path.exists():
        return None
    try:
        config = json.loads(config_path.read_text())
        # v20: DO NOT load weights — they are overfitted
        # Only load thresholds which are less prone to overfitting
        config.pop('weights', None)
        config.pop('weights_by_group', None)
        log.info("Loaded optimized config (thresholds only, weights overridden by v20)")
        return config
    except Exception as e:
        log.warning(f"Failed to load optimized config: {e}")
        return None


def _get_coin_group(coin):
    """Get the optimization group for a coin.

    Returns group name (e.g. 'majors', 'defi') or None if coin is not in any group
    (fallback to overall weights).
    """
    for group, coins in COIN_GROUPS.items():
        if coin in coins:
            return group
    return None  # fallback to overall weights


# Auto-load optimized config if available (reloaded each forecast cycle, not just import)
_opt_config = _load_optimized_config()
_opt_config_mtime = 0

def _reload_config_if_changed():
    """Reload config if self_improver updated it since last load."""
    global _opt_config, _opt_config_mtime, WEIGHTS, THRESHOLDS
    config_path = _FACTORY_DIR / 'data' / 'crypto' / 'optimized_config.json'
    if config_path.exists():
        mtime = config_path.stat().st_mtime
        if mtime > _opt_config_mtime:
            _opt_config = _load_optimized_config()
            _opt_config_mtime = mtime
            if _opt_config:
                if _opt_config.get('weights'):
                    WEIGHTS = _opt_config['weights']
                THRESHOLDS['buy'] = _opt_config.get('buy_threshold', THRESHOLDS['buy'])
                THRESHOLDS['neutral_low'] = _opt_config.get('sell_threshold', THRESHOLDS['neutral_low'])
                log.info(f"Loaded optimized config: accuracy={_opt_config.get('training_accuracy', 0):.0%}, "
                         f"quality_gate={_opt_config.get('quality_gate', 0):.2f}")

if _opt_config:
    if _opt_config.get('weights'):
        WEIGHTS = _opt_config['weights']
    THRESHOLDS['buy'] = _opt_config.get('buy_threshold', THRESHOLDS['buy'])
    THRESHOLDS['neutral_low'] = _opt_config.get('sell_threshold', THRESHOLDS['neutral_low'])
    QUALITY_GATE = _opt_config.get('quality_gate', 0.0)
    log.info(f"Loaded optimized config: accuracy={_opt_config.get('training_accuracy', 0)*100:.0f}%, "
             f"quality_gate={QUALITY_GATE:.2f}")
    # Note: group-specific weights are applied per-coin in forecast_coin()
    if 'weights_by_group' in _opt_config:
        n_groups = len(_opt_config['weights_by_group'])
        log.info(f"  Group-specific weights available for {n_groups} groups")


# ════════════════════════════════════════════
# SENTIMENT SIGNAL
# ════════════════════════════════════════════

def score_sentiment(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on Fear & Greed + news sentiment.

    F&G contrarian is CONTEXT-AWARE: extreme fear is only bullish if
    price is above MA200 (fear overdone in uptrend). Below MA200, fear
    is justified and the contrarian signal is neutralized.
    Training: F&G extreme fear had 0% accuracy with blind contrarian.
    """
    signals = []

    # Check BTC MA200 trend for context-aware F&G interpretation
    # Training showed: blind contrarian on extreme fear = 0% accuracy
    # Fix: only contrarian BUY when trend is UP (above MA200)
    ma200_trend = None
    try:
        btc_prices = conn.execute(
            "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 200"
        ).fetchall()
        if len(btc_prices) >= 200:
            closes = [p[0] for p in btc_prices]
            current = closes[0]
            sma200 = sum(closes) / len(closes)
            ma200_trend = 'above' if current > sma200 else 'below'
    except Exception:
        pass

    # Fear & Greed Index
    fg = conn.execute(
        "SELECT value, classification FROM fear_greed ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if fg:
        value = fg[0]
        # Context-aware contrarian: F&G signal depends on MA200 trend
        # Above MA200 (uptrend): fear IS overdone → contrarian BUY works
        # Below MA200 (downtrend): fear is JUSTIFIED → neutralize contrarian
        if value <= 10:
            if ma200_trend == 'above':
                fg_score = 0.8
                reason = f'Extreme Fear ({value}) + uptrend — fear overdone, strong buy'
            else:
                fg_score = 0.0
                reason = f'Extreme Fear ({value}) + downtrend — fear justified, neutral'
        elif value <= 25:
            if ma200_trend == 'above':
                fg_score = 0.5
                reason = f'Fear ({value}) + uptrend — contrarian buy'
            else:
                fg_score = 0.1
                reason = f'Fear ({value}) + downtrend — weak contrarian'
        elif value <= 45:
            if ma200_trend == 'above':
                fg_score = 0.2
                reason = f'Mild fear ({value}) + uptrend — mild buy'
            else:
                fg_score = 0.05
                reason = f'Mild fear ({value}) + downtrend — near neutral'
        elif value >= 90:
            fg_score = -0.8
            reason = f'Extreme Greed ({value}) — historically strong sell signal'
        elif value >= 75:
            fg_score = -0.5
            reason = f'Greed ({value}) — contrarian sell signal'
        elif value >= 55:
            fg_score = -0.2
            reason = f'Greed ({value}) — mild sell signal'
        else:
            fg_score = 0.0
            reason = f'Neutral ({value})'

        signals.append({'name': 'fear_greed', 'score': fg_score, 'reason': reason})

    # News sentiment for this coin
    week_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
    news_rows = conn.execute(
        "SELECT sentiment FROM news WHERE timestamp > ? "
        "AND (coins_mentioned LIKE ? OR coins_mentioned IS NULL)",
        (week_ago, f'%{coin}%')
    ).fetchall()

    if news_rows:
        bullish = sum(1 for r in news_rows if r[0] == 'bullish')
        bearish = sum(1 for r in news_rows if r[0] == 'bearish')
        total = len(news_rows)

        if total > 0:
            sentiment_ratio = (bullish - bearish) / total
            news_score = np.clip(sentiment_ratio * 2, -0.6, 0.6)
            signals.append({
                'name': 'news_sentiment',
                'score': float(news_score),
                'reason': f'{bullish} bullish / {bearish} bearish / {total - bullish - bearish} neutral ({total} articles)'
            })

    # Social sentiment (Twitter/X)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    social = conn.execute(
        "SELECT score, volume FROM social_sentiment "
        "WHERE coin = ? AND date = ? AND source = 'twitter'",
        (coin, today)
    ).fetchone()

    if social and social[0] is not None:
        tw_score_raw = social[0]  # Already -1 to +1
        tw_volume = social[1] or 0
        # Weight by volume: more tweets = more reliable signal
        vol_weight = min(tw_volume / 100, 1.0) if tw_volume else 0.5
        tw_score = tw_score_raw * vol_weight * 0.6  # Cap at ±0.6
        signals.append({
            'name': 'twitter_sentiment',
            'score': float(np.clip(tw_score, -0.6, 0.6)),
            'reason': f'Twitter: score={tw_score_raw:+.2f}, {tw_volume} tweets analyzed'
        })

    if not signals:
        return {'score': 0.0, 'reason': 'No sentiment data available', 'details': [], 'has_data': False}

    avg_score = np.mean([s['score'] for s in signals])
    return {
        'score': float(avg_score),
        'reason': signals[0]['reason'] if len(signals) == 1 else f'{len(signals)} sentiment signals',
        'details': signals,
        'has_data': True,
    }


# ════════════════════════════════════════════
# ON-CHAIN SIGNAL
# ════════════════════════════════════════════

def score_onchain(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on funding rates + whale activity."""
    signals = []

    # Funding rate trend
    rates = conn.execute(
        "SELECT rate FROM funding_rates WHERE coin = ? "
        "ORDER BY timestamp DESC LIMIT 30", (coin,)
    ).fetchall()

    if rates:
        avg_rate = np.mean([r[0] for r in rates])
        recent_rate = rates[0][0]

        if avg_rate < -0.001:
            fr_score = 0.6
            reason = f'Negative funding rate ({avg_rate*100:.3f}%) — shorts overheated, squeeze likely'
        elif avg_rate < 0:
            fr_score = 0.3
            reason = f'Slightly negative funding ({avg_rate*100:.3f}%)'
        elif avg_rate > 0.003:
            fr_score = -0.6
            reason = f'High funding rate ({avg_rate*100:.3f}%) — longs overheated'
        elif avg_rate > 0.001:
            fr_score = -0.3
            reason = f'Elevated funding rate ({avg_rate*100:.3f}%)'
        else:
            fr_score = 0.0
            reason = f'Neutral funding rate ({avg_rate*100:.3f}%)'

        signals.append({'name': 'funding_rate', 'score': fr_score, 'reason': reason})

    # Whale activity (from whale_transactions)
    day_ago = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
    whales = conn.execute(
        "SELECT from_label, to_label, amount_usd FROM whale_transactions "
        "WHERE timestamp > ? AND (coin = ? OR coin = ?)",
        (day_ago, coin, coin.lower())
    ).fetchall()

    if whales:
        # Exchange inflow vs outflow
        to_exchange = sum(w[2] or 0 for w in whales if w[1] and 'exchange' in (w[1] or '').lower())
        from_exchange = sum(w[2] or 0 for w in whales if w[0] and 'exchange' in (w[0] or '').lower())

        net_flow = from_exchange - to_exchange  # Positive = outflow (bullish)

        if net_flow > 100_000_000:
            w_score = 0.7
            reason = f'Whales withdrawing from exchanges (net outflow ${net_flow/1e6:.0f}M) — accumulation'
        elif net_flow > 10_000_000:
            w_score = 0.3
            reason = f'Mild exchange outflow (${net_flow/1e6:.0f}M)'
        elif net_flow < -100_000_000:
            w_score = -0.7
            reason = f'Whales depositing to exchanges (net inflow ${abs(net_flow)/1e6:.0f}M) — sell pressure'
        elif net_flow < -10_000_000:
            w_score = -0.3
            reason = f'Mild exchange inflow (${abs(net_flow)/1e6:.0f}M)'
        else:
            w_score = 0.0
            reason = f'Balanced whale flows ({len(whales)} transactions)'

        signals.append({'name': 'whale_flow', 'score': w_score, 'reason': reason})

    # Open Interest trend
    oi_rows = conn.execute(
        "SELECT oi_usdt FROM open_interest WHERE coin = ? "
        "ORDER BY timestamp DESC LIMIT 7", (coin,)
    ).fetchall()

    if len(oi_rows) >= 2:
        current_oi = oi_rows[0][0]
        prev_oi = oi_rows[-1][0]
        if prev_oi and prev_oi > 0:
            oi_change = (current_oi - prev_oi) / prev_oi
            if oi_change > 0.1:  # OI up 10%+ = leverage building
                oi_score = -0.3  # Contrarian: high leverage = risky
                reason = f'OI surging +{oi_change*100:.1f}% — leverage building, liquidation risk'
            elif oi_change > 0.03:
                oi_score = 0.1
                reason = f'OI rising +{oi_change*100:.1f}% — growing interest'
            elif oi_change < -0.1:
                oi_score = 0.3  # Deleveraging = healthier
                reason = f'OI dropping {oi_change*100:.1f}% — deleveraging (healthier market)'
            else:
                oi_score = 0.0
                reason = f'OI stable ({oi_change*100:+.1f}%)'
            signals.append({'name': 'open_interest', 'score': oi_score, 'reason': reason})

    # L/S ratio and taker volume REMOVED — training showed 31% solo accuracy
    # (worse than random). These signals are noise, not alpha.
    # Kept: funding_rate, whale_flow, open_interest, liquidation_cascade

    # Liquidation cascade signal — contrarian reversal indicator
    ts_24h = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
    ts_4h = int((datetime.now(timezone.utc) - timedelta(hours=4)).timestamp())

    liq_24h = conn.execute(
        "SELECT side, COUNT(*) as cnt, SUM(notional_usd) as total "
        "FROM liquidations WHERE coin = ? AND timestamp > ? GROUP BY side",
        (coin, ts_24h)
    ).fetchall()

    if liq_24h:
        long_liq = sum(r[2] or 0 for r in liq_24h if r[0] == 'LONG')
        short_liq = sum(r[2] or 0 for r in liq_24h if r[0] == 'SHORT')
        total_liq = long_liq + short_liq

        if total_liq > 100:  # Minimum threshold to avoid noise
            # Spike detection: 4h volume > 40% of 24h = cascade happening now
            liq_4h = conn.execute(
                "SELECT side, SUM(notional_usd) as total "
                "FROM liquidations WHERE coin = ? AND timestamp > ? GROUP BY side",
                (coin, ts_4h)
            ).fetchall()
            total_4h = sum(r[1] or 0 for r in liq_4h)
            spike = total_4h > (total_liq * 0.4)

            net_bias = (long_liq - short_liq) / total_liq  # +1=all longs liq'd, -1=all shorts

            if net_bias > 0.6 and spike:
                liq_score = 0.6
                reason = f'Liquidation cascade: ${long_liq:,.0f} longs wiped (spike) — reversal likely'
            elif net_bias > 0.3:
                liq_score = 0.3
                reason = f'Long liquidations dominant (${long_liq:,.0f}) — contrarian buy'
            elif net_bias < -0.6 and spike:
                liq_score = -0.6
                reason = f'Short squeeze: ${short_liq:,.0f} shorts wiped (spike) — overextended'
            elif net_bias < -0.3:
                liq_score = -0.3
                reason = f'Short liquidations dominant (${short_liq:,.0f}) — overextended'
            else:
                liq_score = 0.0
                reason = f'Balanced liquidations (L: ${long_liq:,.0f} / S: ${short_liq:,.0f})'

            signals.append({'name': 'liquidation_cascade', 'score': liq_score, 'reason': reason})

    if not signals:
        return {'score': 0.0, 'reason': 'No on-chain data available', 'details': [], 'has_data': False}

    avg_score = np.mean([s['score'] for s in signals])
    return {
        'score': float(avg_score),
        'reason': signals[0]['reason'] if len(signals) == 1 else f'{len(signals)} on-chain signals',
        'details': signals,
        'has_data': True,
    }


# ════════════════════════════════════════════
# MACRO SIGNAL
# ════════════════════════════════════════════

def score_macro(conn: sqlite3.Connection) -> dict:
    """Score based on BTC dominance, DeFi TVL trend, and DXY (US Dollar Index)."""
    signals = []

    # BTC dominance trend
    metrics = conn.execute(
        "SELECT btc_dominance, total_market_cap FROM global_metrics ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if metrics:
        btc_dom = metrics[0]
        total_cap = metrics[1]

        if btc_dom:
            # Get previous entry for trend calculation
            prev_metrics = conn.execute(
                "SELECT btc_dominance FROM global_metrics ORDER BY date ASC LIMIT 1"
            ).fetchone()

            btc_dom_trend = 0.0
            trend_note = ''
            if prev_metrics and prev_metrics[0]:
                btc_dom_trend = btc_dom - prev_metrics[0]
                if abs(btc_dom_trend) > 0.1:
                    trend_note = f', {btc_dom_trend:+.1f}pp'

            # Absolute level score
            if btc_dom > 60:
                level_score = -0.2
            elif btc_dom < 45:
                level_score = 0.3
            else:
                level_score = 0.0

            # Trend direction (more important than absolute level)
            if btc_dom_trend > 2.0:
                trend_score = -0.3  # Dominance rising fast = risk-off for alts
            elif btc_dom_trend < -2.0:
                trend_score = 0.3   # Dominance falling fast = alt season
            else:
                trend_score = 0.0

            dom_score = float(np.clip(level_score + trend_score, -0.5, 0.5))
            signals.append({
                'name': 'btc_dominance',
                'score': dom_score,
                'reason': f'BTC dominance {btc_dom:.1f}%{trend_note}'
            })

    # DeFi TVL trend — compare latest vs 7 days ago
    tvl_latest = conn.execute(
        "SELECT SUM(tvl_usd), date FROM tvl WHERE protocol NOT LIKE '_chain_%' "
        "GROUP BY date ORDER BY date DESC LIMIT 1"
    ).fetchone()

    tvl_prev = conn.execute(
        "SELECT SUM(tvl_usd) FROM tvl WHERE protocol NOT LIKE '_chain_%' "
        "AND date <= date('now', '-5 days') GROUP BY date ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if tvl_latest and tvl_latest[0]:
        total_tvl = tvl_latest[0]
        if tvl_prev and tvl_prev[0] and tvl_prev[0] > 0:
            tvl_change = (total_tvl - tvl_prev[0]) / tvl_prev[0]
            if tvl_change > 0.05:
                tvl_score = 0.4
                reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B (+{tvl_change*100:.1f}% — capital inflow)'
            elif tvl_change > 0.01:
                tvl_score = 0.2
                reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B (+{tvl_change*100:.1f}%)'
            elif tvl_change < -0.05:
                tvl_score = -0.4
                reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B ({tvl_change*100:.1f}% — capital outflow)'
            elif tvl_change < -0.01:
                tvl_score = -0.2
                reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B ({tvl_change*100:.1f}%)'
            else:
                tvl_score = 0.0
                reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B (stable)'
        else:
            tvl_score = 0.0
            reason = f'DeFi TVL: ${total_tvl/1e9:.1f}B (no prior data for comparison)'
        signals.append({'name': 'defi_tvl', 'score': tvl_score, 'reason': reason})

    # DXY (US Dollar Index) — strong dollar = bearish for crypto
    try:
        dxy_latest = conn.execute(
            "SELECT dxy_value, date FROM dxy_rates ORDER BY date DESC LIMIT 1"
        ).fetchone()
        dxy_prev_7d = conn.execute(
            "SELECT dxy_value FROM dxy_rates "
            "WHERE date <= date('now', '-7 days') ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if dxy_latest and dxy_latest[0] and dxy_latest[0] > 0:
            dxy_now = dxy_latest[0]
            parts = []

            # Trend-based scoring (7-day momentum)
            trend_score = 0.0
            if dxy_prev_7d and dxy_prev_7d[0] and dxy_prev_7d[0] > 0:
                dxy_change_pct = (dxy_now - dxy_prev_7d[0]) / dxy_prev_7d[0] * 100
                if dxy_change_pct > 1.0:
                    trend_score = -0.3
                    parts.append(f'+{dxy_change_pct:.1f}% 7d (surging)')
                elif dxy_change_pct > 0.3:
                    trend_score = -0.15
                    parts.append(f'+{dxy_change_pct:.1f}% 7d (rising)')
                elif dxy_change_pct < -1.0:
                    trend_score = 0.3
                    parts.append(f'{dxy_change_pct:.1f}% 7d (falling)')
                elif dxy_change_pct < -0.3:
                    trend_score = 0.15
                    parts.append(f'{dxy_change_pct:.1f}% 7d (declining)')
                else:
                    parts.append(f'{dxy_change_pct:+.1f}% 7d (stable)')
            else:
                parts.append('no 7d trend data yet')

            # Level-based scoring (secondary)
            level_score = 0.0
            if dxy_now > 108:
                level_score = -0.1
                parts.append('above 108 (strong)')
            elif dxy_now < 98:
                level_score = 0.1
                parts.append('below 98 (weak)')

            dxy_score = float(np.clip(trend_score + level_score, -0.4, 0.4))
            signals.append({
                'name': 'dxy',
                'score': dxy_score,
                'reason': f'DXY {dxy_now:.1f} ({", ".join(parts)})'
            })
    except Exception:
        pass  # Table may not exist yet — graceful degradation

    if not signals:
        return {'score': 0.0, 'reason': 'No macro data', 'details': [], 'has_data': False}

    avg_score = np.mean([s['score'] for s in signals])
    return {
        'score': float(avg_score),
        'reason': f'{len(signals)} macro signals',
        'details': signals,
        'has_data': True,
    }


# ════════════════════════════════════════════
# NEWS SIGNAL
# ════════════════════════════════════════════

def score_news(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on recent news volume and sentiment."""
    day_ago = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
    week_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())

    # Recent news about this coin
    recent = conn.execute(
        "SELECT COUNT(*), "
        "SUM(CASE WHEN sentiment='bullish' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN sentiment='bearish' THEN 1 ELSE 0 END) "
        "FROM news WHERE timestamp > ? AND coins_mentioned LIKE ?",
        (week_ago, f'%{coin}%')
    ).fetchone()

    total, bullish, bearish = recent[0] or 0, recent[1] or 0, recent[2] or 0

    if total == 0:
        # Try general crypto news (not coin-specific)
        general = conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN sentiment='bullish' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN sentiment='bearish' THEN 1 ELSE 0 END) "
            "FROM news WHERE timestamp > ?", (week_ago,)
        ).fetchone()
        total, bullish, bearish = general[0] or 0, general[1] or 0, general[2] or 0
        specific = False
    else:
        specific = True

    if total == 0:
        return {'score': 0.0, 'reason': 'No recent news', 'details': [], 'has_data': False}

    ratio = (bullish - bearish) / total
    score = np.clip(ratio * 1.5, -0.6, 0.6)

    return {
        'score': float(score),
        'reason': f'{"Coin" if specific else "Market"} news: {bullish}B/{bearish}b/{total}t (ratio={ratio:+.2f})',
        'details': [{
            'total_articles': total,
            'bullish': bullish,
            'bearish': bearish,
            'neutral': total - bullish - bearish,
        }],
        'has_data': True,
    }


# ════════════════════════════════════════════
# HISTORICAL PATTERN SIGNAL
# ════════════════════════════════════════════

def score_historical(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on historical patterns with volatility normalization and recency weighting."""
    prices = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin = ? AND timeframe = '1d' "
        "ORDER BY timestamp ASC", (coin,)
    ).fetchall()

    if len(prices) < 90:
        return {'score': 0.0, 'reason': 'Insufficient historical data', 'details': [], 'has_data': False}

    closes = np.array([p[1] for p in prices])
    timestamps = np.array([p[0] for p in prices])

    # Daily returns for volatility calculation
    returns = np.diff(closes) / closes[:-1]
    if len(returns) < 30:
        return {'score': 0.0, 'reason': 'Insufficient return data', 'details': [], 'has_data': False}

    # Volatility normalization: 30-day rolling stdev of daily returns
    current_vol = float(np.std(returns[-30:]))
    if current_vol < 1e-8:
        current_vol = 0.01

    current_7d = (closes[-1] - closes[-7]) / closes[-7] * 100
    # Normalize by volatility * sqrt(7) to make cross-coin comparable
    vol_norm = current_vol * np.sqrt(7) * 100
    normalized_current = current_7d / vol_norm if vol_norm > 0 else 0

    similar_moves = []
    for i in range(30, len(closes) - 14):
        past_7d = (closes[i] - closes[i - 7]) / closes[i - 7] * 100

        # Local volatility for normalization
        local_returns = returns[max(0, i - 37):i - 1] if i > 1 else returns[:1]
        local_vol = float(np.std(local_returns)) if len(local_returns) >= 7 else current_vol
        local_vol_norm = local_vol * np.sqrt(7) * 100 if local_vol > 1e-8 else vol_norm

        normalized_past = past_7d / local_vol_norm if local_vol_norm > 0 else 0

        # Tighter similarity: within 1.5 normalized standard deviations
        if abs(normalized_past - normalized_current) < 1.5:
            future_7d = (closes[i + 7] - closes[i]) / closes[i] * 100

            # Recency weighting: exponential decay, ~90-day half-life
            days_ago = (timestamps[-1] - timestamps[i]) / 86400
            recency_weight = float(np.exp(-days_ago / 130))

            similar_moves.append((future_7d, recency_weight))

    if len(similar_moves) < 7:  # Require 7+ patterns for statistical meaning
        return {
            'score': 0.0,
            'reason': f'Only {len(similar_moves)} similar patterns found (need 7+)',
            'details': [],
            'has_data': False,
        }

    futures = np.array([m[0] for m in similar_moves])
    weights = np.array([m[1] for m in similar_moves])

    # Recency-weighted average and standard deviation
    avg_future = float(np.average(futures, weights=weights))
    std_future = float(np.sqrt(np.average((futures - avg_future) ** 2, weights=weights)))
    positive_pct = float(np.sum(weights[futures > 0]) / np.sum(weights) * 100)

    # Sharpe-like scoring: reward consistency, penalize variance
    # Old: score = avg/15 → BONK avg=+9%, std=20% → score=0.6 (max, unreliable!)
    # New: score = avg/std * scale → BONK 9/20*0.2=0.09 (low, correct!)
    # BTC avg=+2%, std=3% → 2/3*0.2=0.13 (decent, reliable signal)
    if std_future > 1.0:  # Normal variance
        sharpe = avg_future / std_future
        score = float(np.clip(sharpe * 0.50, -0.6, 0.6))
    else:  # Very low variance (rare) → trust the average
        score = float(np.clip(avg_future / 8, -0.6, 0.6))

    return {
        'score': score,
        'reason': f'{len(similar_moves)} similar patterns (vol-normalized, recency-weighted): avg {avg_future:+.1f}% ({positive_pct:.0f}% positive)',
        'details': [{
            'current_7d_change': float(current_7d),
            'similar_count': len(similar_moves),
            'avg_future_7d': avg_future,
            'positive_pct': positive_pct,
            'median_future_7d': float(np.median(futures)),
        }],
        'has_data': True,
    }


# ════════════════════════════════════════════
# COMPOSITE FORECAST
# ════════════════════════════════════════════

def calc_price_targets(current_price: float, signal: str, tech_data: dict) -> dict:
    """Calculate coin-specific price targets using Bollinger Bands + ATR + S/R."""
    if not current_price or current_price <= 0:
        return {
            'target_low': 0, 'target_high': 0, 'stop_loss': 0,
            'support': 0, 'resistance': 0, 'predicted_change': 'N/A',
            'support_levels': [], 'resistance_levels': [],
        }
    atr_pct = tech_data.get('atr_pct', 0.03)
    bb = tech_data.get('bollinger', {})
    sr = tech_data.get('support_resistance', {'support': [], 'resistance': []})

    bb_upper = bb.get('upper') or current_price * (1 + atr_pct * 2)
    bb_lower = bb.get('lower') or current_price * (1 - atr_pct * 2)

    # Closest S/R levels
    support = sr['support'][0] if sr['support'] else bb_lower
    resistance = sr['resistance'][0] if sr['resistance'] else bb_upper

    if signal in ('STRONG BUY', 'BUY'):
        target_low = current_price * (1 + atr_pct * 0.5)
        target_high = max(resistance, current_price * (1 + atr_pct * 1.5))
        stop_loss = max(support, current_price * (1 - atr_pct))
    elif signal in ('STRONG SELL', 'SELL'):
        target_low = min(support, current_price * (1 - atr_pct * 1.5))
        target_high = current_price * (1 - atr_pct * 0.5)
        stop_loss = min(resistance, current_price * (1 + atr_pct))
    else:  # NEUTRAL
        target_low = current_price * (1 - atr_pct)
        target_high = current_price * (1 + atr_pct)
        stop_loss = current_price * (1 - atr_pct * 1.5)

    # Format as predicted_change string
    pct_low = ((target_low / current_price) - 1) * 100
    pct_high = ((target_high / current_price) - 1) * 100

    # Smart rounding: use enough decimals for micro-priced coins
    import math
    if current_price >= 1:
        decimals = 2
    elif current_price >= 0.001:
        decimals = 4
    else:
        decimals = max(2, -int(math.floor(math.log10(abs(current_price)))) + 2)

    def sr_round(v):
        return round(v, decimals)

    return {
        'target_low': sr_round(target_low),
        'target_high': sr_round(target_high),
        'stop_loss': sr_round(stop_loss),
        'support': sr_round(support),
        'resistance': sr_round(resistance),
        'predicted_change': f'{pct_low:+.1f}% to {pct_high:+.1f}%',
        'support_levels': [sr_round(s) for s in sr['support']],
        'resistance_levels': [sr_round(r) for r in sr['resistance']],
    }


def score_learned(conn: sqlite3.Connection, coin: str) -> dict:
    """Score based on learned patterns from historical backtesting.

    Queries patterns.db for patterns matching current market conditions.
    Returns weighted average of matching pattern scores.
    """
    from src.crypto.technical_analyzer import get_prices

    pat_db = _FACTORY_DIR / 'data' / 'crypto' / 'patterns.db'
    if not pat_db.exists():
        return {'score': 0.0, 'reason': 'No pattern database', 'signals': [], 'has_data': False}

    # Gather current conditions
    conditions = {}
    try:
        prices = get_prices(conn, coin, '1d', 365)
        if len(prices) >= 14:
            closes = np.array([p[4] for p in prices])

            # RSI
            deltas = np.diff(closes)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            period = 14
            if len(gains) >= period:
                avg_gain = np.mean(gains[-period:])
                avg_loss = np.mean(losses[-period:])
                if avg_loss > 0:
                    rs = avg_gain / avg_loss
                    conditions['rsi'] = round(100 - (100 / (1 + rs)), 1)

            # MA200 trend
            if len(closes) >= 200:
                sma200 = np.mean(closes[-200:])
                conditions['ma200_trend'] = 'above' if closes[-1] > sma200 else 'below'

            # Volatility (30d)
            if len(closes) >= 31:
                returns = np.diff(closes[-31:]) / closes[-31:-1]
                conditions['volatility'] = round(float(np.std(returns)), 6)
    except Exception:
        pass

    # Fear & Greed
    try:
        fg = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
        if fg:
            conditions['fg'] = fg[0]
    except Exception:
        pass

    # Funding rate
    try:
        fr = conn.execute(
            "SELECT AVG(rate) FROM (SELECT rate FROM funding_rates "
            "WHERE coin = ? ORDER BY timestamp DESC LIMIT 30)", (coin,)
        ).fetchone()
        if fr and fr[0] is not None:
            conditions['funding_rate'] = float(fr[0])
    except Exception:
        pass

    if not conditions:
        return {'score': 0.0, 'reason': 'No conditions available', 'signals': [], 'has_data': False}

    # Find matching patterns
    try:
        from src.crypto.pattern_learner import PatternLearner
        learner = PatternLearner(str(pat_db))
        matches = learner.match_current_conditions(conditions)
        learner.close()
    except Exception:
        return {'score': 0.0, 'reason': 'Pattern matching failed', 'signals': [], 'has_data': False}

    if not matches:
        return {'score': 0.0, 'reason': 'No matching patterns', 'signals': [], 'has_data': False}

    # Weighted average: weight by confidence * sample_size
    total_weight = 0
    weighted_score = 0
    signals = []

    for pattern in matches:
        weight = pattern['confidence_score'] * min(pattern['sample_size'] / 20, 1.0)
        weighted_score += pattern['pattern_score'] * weight
        total_weight += weight
        signals.append({
            'name': f"pattern_{pattern['pattern_id']}",
            'score': pattern['pattern_score'],
            'reason': pattern['description'],
        })

    final_score = weighted_score / total_weight if total_weight > 0 else 0.0
    final_score = float(np.clip(final_score, -0.8, 0.8))

    return {
        'score': final_score,
        'reason': f'{len(matches)} learned patterns matched',
        'signals': signals,
        'has_data': True,
    }


def _get_meta_score(conn: sqlite3.Connection, coin: str, meta_result: dict) -> dict:
    """Extract per-coin meta-analyst score. Called by forecast_coin()."""
    if not meta_result or not meta_result.get('has_data'):
        return {'score': 0.0, 'weight': WEIGHTS['meta_analyst'],
                'details': {}, 'has_data': False}

    forecast = meta_result.get('coin_forecasts', {}).get(coin)
    if not forecast:
        return {'score': 0.0, 'weight': WEIGHTS['meta_analyst'],
                'details': {}, 'has_data': False}

    # Scale to [-0.8, 0.8] to match other signal ranges
    score = float(np.clip(forecast['score'] * 0.8, -0.8, 0.8))
    return {
        'score': score,
        'weight': WEIGHTS['meta_analyst'],
        'details': forecast,
        'has_data': True,
    }


# ════════════════════════════════════════════
# COINGLASS SIGNAL — aggregated derivatives + ETF + options
# ════════════════════════════════════════════

def score_coinglass(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on CoinGlass aggregated data (6 sub-signals).

    Provides cross-exchange derivatives signals that single-exchange
    (Binance) data cannot capture:
      1. Exchange flow — outflow = accumulation (bullish)
      2. Aggregated OI divergence — OI vs price direction
      3. Options max pain — gravitational price target (BTC/ETH only)
      4. ETF net flow — institutional capital direction (BTC, dampened for alts)
      5. Stablecoin supply trend — buying power entering/leaving crypto
      6. Multi-exchange liquidations — cascade detection
    """
    signals = []

    # ─── 1. Exchange flow: outflow = accumulation (bullish) ───
    try:
        balance_rows = conn.execute(
            "SELECT exchange, change_pct_7d, total_balance "
            "FROM cg_exchange_balance WHERE coin = ? "
            "ORDER BY timestamp DESC LIMIT 30", (coin,)
        ).fetchall()

        if balance_rows:
            # Weight by exchange size (total_balance)
            total_bal = sum(abs(r[2] or 0) for r in balance_rows)
            if total_bal > 0:
                weighted_change = sum(
                    (r[1] or 0) * (abs(r[2] or 0) / total_bal)
                    for r in balance_rows
                )
            else:
                weighted_change = np.mean([r[1] or 0 for r in balance_rows])

            # Negative change = outflow = bullish (accumulation)
            # Positive change = inflow = bearish (sell pressure)
            if weighted_change < -2.0:
                flow_score = 0.7
                reason = f'Strong exchange outflow ({weighted_change:+.1f}% 7d) — accumulation'
            elif weighted_change < -0.5:
                flow_score = 0.3
                reason = f'Exchange outflow ({weighted_change:+.1f}% 7d) — mild accumulation'
            elif weighted_change > 2.0:
                flow_score = -0.7
                reason = f'Strong exchange inflow ({weighted_change:+.1f}% 7d) — sell pressure'
            elif weighted_change > 0.5:
                flow_score = -0.3
                reason = f'Exchange inflow ({weighted_change:+.1f}% 7d) — mild sell pressure'
            else:
                flow_score = 0.0
                reason = f'Balanced exchange flow ({weighted_change:+.1f}% 7d)'

            signals.append({'name': 'exchange_flow', 'score': flow_score, 'reason': reason})
    except Exception as e:
        log.debug(f"  CoinGlass exchange flow error for {coin}: {e}")

    # ─── 2. Aggregated OI (multi-exchange) ───
    try:
        oi_row = conn.execute(
            "SELECT oi_usd, change_pct_1h, change_pct_4h, change_pct_24h "
            "FROM cg_aggregated_oi WHERE coin = ? AND exchange = 'All' "
            "ORDER BY timestamp DESC LIMIT 1", (coin,)
        ).fetchone()

        if oi_row:
            oi_24h = oi_row[3] or 0

            # Get price change for divergence detection
            price_change = 0
            try:
                price_rows = conn.execute(
                    "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                    "ORDER BY timestamp DESC LIMIT 2", (coin,)
                ).fetchall()
                if len(price_rows) >= 2 and price_rows[1][0] > 0:
                    price_change = ((price_rows[0][0] - price_rows[1][0]) / price_rows[1][0]) * 100
            except Exception:
                pass

            # OI vs price divergence signals
            if oi_24h > 5 and price_change < -2:
                # OI rising + price falling = short buildup, squeeze potential
                oi_score = 0.5
                reason = f'OI↑{oi_24h:+.1f}% + price↓{price_change:.1f}% — squeeze potential'
            elif oi_24h > 5 and price_change > 2:
                # OI rising + price rising = overleveraged longs
                oi_score = -0.3
                reason = f'OI↑{oi_24h:+.1f}% + price↑{price_change:.1f}% — overleveraged'
            elif oi_24h < -5 and price_change < -2:
                # OI falling + price falling = deleveraging (healthy)
                oi_score = 0.3
                reason = f'OI↓{oi_24h:.1f}% + price↓{price_change:.1f}% — deleveraging'
            elif oi_24h > 10:
                oi_score = -0.4
                reason = f'OI surging {oi_24h:+.1f}% — excessive leverage'
            elif oi_24h < -10:
                oi_score = 0.2
                reason = f'OI dropping {oi_24h:.1f}% — leverage reset'
            else:
                oi_score = 0.0
                reason = f'OI change {oi_24h:+.1f}% — normal range'

            signals.append({'name': 'aggregated_oi', 'score': oi_score, 'reason': reason})
    except Exception as e:
        log.debug(f"  CoinGlass OI error for {coin}: {e}")

    # ─── 3. Options max pain (BTC/ETH only) ───
    if coin in ('BTC', 'ETH'):
        try:
            # Get nearest expiry max pain
            mp_row = conn.execute(
                "SELECT max_pain_price, expiry_date, call_oi_notional, put_oi_notional "
                "FROM cg_options_max_pain WHERE coin = ? "
                "ORDER BY timestamp DESC, expiry_date ASC LIMIT 1", (coin,)
            ).fetchone()

            if mp_row and mp_row[0] and mp_row[0] > 0:
                max_pain = mp_row[0]
                call_notional = mp_row[2] or 0
                put_notional = mp_row[3] or 0

                # Get current price
                price_row = conn.execute(
                    "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                    "ORDER BY timestamp DESC LIMIT 1", (coin,)
                ).fetchone()

                if price_row and price_row[0] > 0:
                    current_price = price_row[0]
                    deviation = ((current_price - max_pain) / max_pain) * 100

                    # Put/call ratio for sentiment
                    pcr = put_notional / call_notional if call_notional > 0 else 1.0

                    # Price above max pain → gravitational pull down (bearish)
                    # Price below max pain → gravitational pull up (bullish)
                    if deviation > 5:
                        mp_score = -0.4
                        reason = f'Price {deviation:+.1f}% above max pain ${max_pain:,.0f} — pull down (PCR={pcr:.2f})'
                    elif deviation > 2:
                        mp_score = -0.2
                        reason = f'Price {deviation:+.1f}% above max pain ${max_pain:,.0f}'
                    elif deviation < -5:
                        mp_score = 0.4
                        reason = f'Price {deviation:+.1f}% below max pain ${max_pain:,.0f} — pull up (PCR={pcr:.2f})'
                    elif deviation < -2:
                        mp_score = 0.2
                        reason = f'Price {deviation:+.1f}% below max pain ${max_pain:,.0f}'
                    else:
                        mp_score = 0.0
                        reason = f'Price near max pain ${max_pain:,.0f} (dev={deviation:+.1f}%)'

                    signals.append({'name': 'options_max_pain', 'score': mp_score, 'reason': reason})
        except Exception as e:
            log.debug(f"  CoinGlass max pain error for {coin}: {e}")

    # ─── 4. ETF net flow (BTC, dampened for alts) ───
    try:
        etf_rows = conn.execute(
            "SELECT flow_usd FROM cg_etf_flows WHERE asset = 'BTC' "
            "ORDER BY date DESC LIMIT 7"
        ).fetchall()

        if etf_rows:
            # 7-day cumulative flow
            cumulative_flow = sum(r[0] or 0 for r in etf_rows)
            avg_daily = cumulative_flow / len(etf_rows)

            if cumulative_flow > 500_000_000:
                etf_score = 0.7
                reason = f'Strong ETF inflow ${cumulative_flow/1e6:,.0f}M (7d) — institutional buying'
            elif cumulative_flow > 100_000_000:
                etf_score = 0.3
                reason = f'ETF inflow ${cumulative_flow/1e6:,.0f}M (7d)'
            elif cumulative_flow < -500_000_000:
                etf_score = -0.7
                reason = f'Strong ETF outflow ${cumulative_flow/1e6:,.0f}M (7d) — institutional selling'
            elif cumulative_flow < -100_000_000:
                etf_score = -0.3
                reason = f'ETF outflow ${cumulative_flow/1e6:,.0f}M (7d)'
            else:
                etf_score = 0.0
                reason = f'ETF flow neutral ${cumulative_flow/1e6:,.0f}M (7d)'

            # Dampen for non-BTC coins (ETF flows primarily affect BTC directly)
            if coin != 'BTC':
                etf_score *= 0.4
                reason += f' (dampened for {coin})'

            signals.append({'name': 'etf_flow', 'score': etf_score, 'reason': reason})
    except Exception as e:
        log.debug(f"  CoinGlass ETF flow error: {e}")

    # ─── 5. Stablecoin supply trend ───
    try:
        stable_rows = conn.execute(
            "SELECT total_market_cap FROM cg_stablecoin_supply "
            "ORDER BY date DESC LIMIT 14"
        ).fetchall()

        if len(stable_rows) >= 7:
            recent = np.mean([r[0] or 0 for r in stable_rows[:7]])
            older = np.mean([r[0] or 0 for r in stable_rows[7:14]])

            if older > 0:
                change_pct = ((recent - older) / older) * 100

                if change_pct > 1.0:
                    stable_score = 0.5
                    reason = f'Stablecoin supply growing {change_pct:+.1f}% — buying power rising'
                elif change_pct > 0.2:
                    stable_score = 0.2
                    reason = f'Stablecoin supply up {change_pct:+.1f}%'
                elif change_pct < -1.0:
                    stable_score = -0.5
                    reason = f'Stablecoin supply shrinking {change_pct:+.1f}% — capital leaving'
                elif change_pct < -0.2:
                    stable_score = -0.2
                    reason = f'Stablecoin supply down {change_pct:+.1f}%'
                else:
                    stable_score = 0.0
                    reason = f'Stablecoin supply stable ({change_pct:+.1f}%)'

                signals.append({'name': 'stablecoin_supply', 'score': stable_score, 'reason': reason})
    except Exception as e:
        log.debug(f"  CoinGlass stablecoin error: {e}")

    # ─── 6. Multi-exchange liquidations ───
    try:
        liq_row = conn.execute(
            "SELECT liq_usd_24h, long_liq_usd_24h, short_liq_usd_24h, "
            "liq_usd_4h, long_liq_usd_4h, short_liq_usd_4h "
            "FROM cg_liquidations WHERE coin = ? "
            "ORDER BY timestamp DESC LIMIT 1", (coin,)
        ).fetchone()

        if liq_row:
            total_24h = liq_row[0] or 0
            long_24h = liq_row[1] or 0
            short_24h = liq_row[2] or 0
            total_4h = liq_row[3] or 0

            if total_24h > 0:
                net_bias = (long_24h - short_24h) / total_24h
                spike = total_4h > (total_24h * 0.4)

                if net_bias > 0.6 and spike:
                    liq_score = 0.6
                    reason = (f'Liquidation cascade: ${long_24h/1e6:.1f}M longs wiped '
                              f'(spike {total_4h/1e6:.1f}M/4h) — reversal likely')
                elif net_bias > 0.3:
                    liq_score = 0.3
                    reason = f'Long liquidations dominant ({net_bias:.0%}) — contrarian buy'
                elif net_bias < -0.6 and spike:
                    liq_score = -0.6
                    reason = (f'Short squeeze: ${short_24h/1e6:.1f}M shorts wiped '
                              f'(spike {total_4h/1e6:.1f}M/4h) — overextended')
                elif net_bias < -0.3:
                    liq_score = -0.3
                    reason = f'Short liquidations dominant ({abs(net_bias):.0%}) — overextended'
                else:
                    liq_score = 0.0
                    reason = f'Balanced liquidations (L: ${long_24h/1e6:.1f}M / S: ${short_24h/1e6:.1f}M)'

                signals.append({'name': 'cg_liquidations', 'score': liq_score, 'reason': reason})
    except Exception as e:
        log.debug(f"  CoinGlass liquidation error for {coin}: {e}")

    if not signals:
        return {'score': 0.0, 'reason': 'No CoinGlass data available', 'details': [], 'has_data': False}

    avg_score = float(np.mean([s['score'] for s in signals]))
    return {
        'score': avg_score,
        'reason': f'{len(signals)} CoinGlass signals',
        'details': signals,
        'has_data': True,
    }


# ════════════════════════════════════════════
# CRYPTOQUANT SIGNAL — on-chain fundamentals
# ════════════════════════════════════════════

def score_cryptoquant(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Score based on CryptoQuant on-chain data (6 sub-signals).

    Gold-standard on-chain indicators:
      1. SOPR — profit-taking vs capitulation
      2. NUPL — market cycle stage (euphoria/capitulation)
      3. MVRV — over/undervaluation
      4. Exchange netflow — accumulation vs sell pressure
      5. Coinbase Premium — US institutional demand
      6. Miner reserve — miner selling/accumulating behavior
    """
    signals = []

    # Coins that have CryptoQuant exchange flow data
    CQ_FLOW_COINS = {'BTC', 'ETH', 'LINK', 'UNI', 'AAVE', 'PENDLE', 'CRV', 'SHIB'}

    # ─── 1. SOPR (BTC only) ───
    if coin == 'BTC':
        try:
            sopr_row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = 'sopr' ORDER BY date DESC LIMIT 1"
            ).fetchone()

            if sopr_row and sopr_row[0] is not None:
                sopr = sopr_row[0]
                if sopr < 0.95:
                    s = 0.6
                    reason = f'SOPR {sopr:.3f} — deep capitulation (holders selling at loss)'
                elif sopr < 1.0:
                    s = 0.3
                    reason = f'SOPR {sopr:.3f} — mild capitulation'
                elif sopr > 1.05:
                    s = -0.5
                    reason = f'SOPR {sopr:.3f} — significant profit-taking'
                elif sopr > 1.02:
                    s = -0.2
                    reason = f'SOPR {sopr:.3f} — mild profit-taking'
                else:
                    s = 0.0
                    reason = f'SOPR {sopr:.3f} — neutral'
                signals.append({'name': 'sopr', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant SOPR error: {e}")

    # ─── 2. NUPL (BTC only) ───
    if coin == 'BTC':
        try:
            nupl_row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = 'nupl' ORDER BY date DESC LIMIT 1"
            ).fetchone()

            if nupl_row and nupl_row[0] is not None:
                nupl = nupl_row[0]
                if nupl > 0.75:
                    s = -0.7
                    reason = f'NUPL {nupl:.2f} — euphoria zone (cycle top risk)'
                elif nupl > 0.5:
                    s = -0.3
                    reason = f'NUPL {nupl:.2f} — belief/greed (elevated)'
                elif nupl < 0:
                    s = 0.7
                    reason = f'NUPL {nupl:.2f} — capitulation (historically bullish)'
                elif nupl < 0.25:
                    s = 0.3
                    reason = f'NUPL {nupl:.2f} — hope/fear (opportunity zone)'
                else:
                    s = 0.0
                    reason = f'NUPL {nupl:.2f} — optimism (neutral)'
                signals.append({'name': 'nupl', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant NUPL error: {e}")

    # ─── 3. MVRV (BTC only) ───
    if coin == 'BTC':
        try:
            mvrv_row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = 'mvrv' ORDER BY date DESC LIMIT 1"
            ).fetchone()

            if mvrv_row and mvrv_row[0] is not None:
                mvrv = mvrv_row[0]
                if mvrv > 3.5:
                    s = -0.7
                    reason = f'MVRV {mvrv:.2f} — extremely overvalued (>3.5x realized)'
                elif mvrv > 2.5:
                    s = -0.4
                    reason = f'MVRV {mvrv:.2f} — overvalued'
                elif mvrv < 1.0:
                    s = 0.7
                    reason = f'MVRV {mvrv:.2f} — undervalued (below realized value)'
                elif mvrv < 1.5:
                    s = 0.3
                    reason = f'MVRV {mvrv:.2f} — near fair value (opportunity)'
                else:
                    s = 0.0
                    reason = f'MVRV {mvrv:.2f} — fair value range'
                signals.append({'name': 'mvrv', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant MVRV error: {e}")

    # ─── 4. Exchange Netflow (BTC, ETH, ERC20) ───
    if coin in CQ_FLOW_COINS:
        try:
            # Get 7-day average netflow
            flow_rows = conn.execute(
                "SELECT netflow FROM cq_exchange_flows "
                "WHERE coin = ? ORDER BY date DESC LIMIT 7", (coin,)
            ).fetchall()

            if flow_rows:
                avg_flow = np.mean([r[0] or 0 for r in flow_rows])

                # Scale thresholds by coin (BTC flows are much larger)
                if coin == 'BTC':
                    high_thresh, low_thresh = 5000, 1000  # BTC units
                elif coin == 'ETH':
                    high_thresh, low_thresh = 50000, 10000  # ETH units
                else:
                    high_thresh, low_thresh = 1_000_000, 100_000  # ERC20 token units

                if avg_flow < -high_thresh:
                    s = 0.6
                    reason = f'{coin} strong exchange outflow ({avg_flow:+,.0f}/day avg) — accumulation'
                elif avg_flow < -low_thresh:
                    s = 0.3
                    reason = f'{coin} exchange outflow ({avg_flow:+,.0f}/day avg)'
                elif avg_flow > high_thresh:
                    s = -0.6
                    reason = f'{coin} strong exchange inflow ({avg_flow:+,.0f}/day avg) — sell pressure'
                elif avg_flow > low_thresh:
                    s = -0.3
                    reason = f'{coin} exchange inflow ({avg_flow:+,.0f}/day avg)'
                else:
                    s = 0.0
                    reason = f'{coin} balanced exchange flow ({avg_flow:+,.0f}/day avg)'

                signals.append({'name': 'cq_exchange_flow', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant exchange flow error for {coin}: {e}")

    # ─── 5. Coinbase Premium (BTC, dampened for alts) ───
    try:
        premium_rows = conn.execute(
            "SELECT premium_index FROM cq_coinbase_premium "
            "ORDER BY date DESC LIMIT 7"
        ).fetchall()

        if premium_rows:
            avg_premium = np.mean([r[0] or 0 for r in premium_rows])

            if avg_premium > 0.05:
                s = 0.5
                reason = f'Coinbase premium {avg_premium:+.3f} — strong US institutional demand'
            elif avg_premium > 0.02:
                s = 0.2
                reason = f'Coinbase premium {avg_premium:+.3f} — mild US demand'
            elif avg_premium < -0.05:
                s = -0.5
                reason = f'Coinbase premium {avg_premium:+.3f} — US selling pressure'
            elif avg_premium < -0.02:
                s = -0.2
                reason = f'Coinbase premium {avg_premium:+.3f} — mild US selling'
            else:
                s = 0.0
                reason = f'Coinbase premium {avg_premium:+.3f} — neutral'

            # Dampen for non-BTC (Coinbase premium directly reflects BTC demand)
            if coin != 'BTC':
                s *= 0.5
                reason += f' (dampened for {coin})'

            signals.append({'name': 'coinbase_premium', 'score': s, 'reason': reason})
    except Exception as e:
        log.debug(f"  CryptoQuant Coinbase premium error: {e}")

    # ─── 6. Miner Reserve trend (BTC only) ───
    if coin == 'BTC':
        try:
            miner_rows = conn.execute(
                "SELECT reserve FROM cq_miner_data ORDER BY date DESC LIMIT 14"
            ).fetchall()

            if len(miner_rows) >= 7:
                recent = np.mean([r[0] or 0 for r in miner_rows[:7]])
                older = np.mean([r[0] or 0 for r in miner_rows[7:14]])

                if older > 0:
                    change_pct = ((recent - older) / older) * 100

                    if change_pct < -1.0:
                        s = -0.4
                        reason = f'Miner reserve dropping {change_pct:+.2f}% — miners selling'
                    elif change_pct < -0.3:
                        s = -0.2
                        reason = f'Miner reserve down {change_pct:+.2f}%'
                    elif change_pct > 1.0:
                        s = 0.4
                        reason = f'Miner reserve rising {change_pct:+.2f}% — miners accumulating'
                    elif change_pct > 0.3:
                        s = 0.2
                        reason = f'Miner reserve up {change_pct:+.2f}%'
                    else:
                        s = 0.0
                        reason = f'Miner reserve stable ({change_pct:+.2f}%)'

                    signals.append({'name': 'miner_reserve', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant miner reserve error: {e}")

    # ─── 7. NVT Ratio (BTC only) ───
    if coin == 'BTC':
        try:
            nvt_row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = 'nvt' ORDER BY date DESC LIMIT 1"
            ).fetchone()

            if nvt_row and nvt_row[0] is not None:
                nvt = nvt_row[0]
                if nvt > 150:
                    s = -0.5
                    reason = f'NVT {nvt:.0f} — extremely overvalued (low tx volume vs market cap)'
                elif nvt > 80:
                    s = -0.2
                    reason = f'NVT {nvt:.0f} — elevated'
                elif nvt < 20:
                    s = 0.5
                    reason = f'NVT {nvt:.0f} — undervalued (high tx activity)'
                elif nvt < 40:
                    s = 0.2
                    reason = f'NVT {nvt:.0f} — healthy transaction activity'
                else:
                    s = 0.0
                    reason = f'NVT {nvt:.0f} — fair range'
                signals.append({'name': 'nvt', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant NVT error: {e}")

    # ─── 8. Puell Multiple (BTC only) ───
    if coin == 'BTC':
        try:
            puell_row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = 'puell_multiple' ORDER BY date DESC LIMIT 1"
            ).fetchone()

            if puell_row and puell_row[0] is not None:
                puell = puell_row[0]
                if puell > 4.0:
                    s = -0.6
                    reason = f'Puell {puell:.2f} — miners earning 4x+ avg (top risk)'
                elif puell > 2.0:
                    s = -0.3
                    reason = f'Puell {puell:.2f} — miners earning well above avg'
                elif puell < 0.5:
                    s = 0.6
                    reason = f'Puell {puell:.2f} — miner capitulation zone (historically bullish)'
                elif puell < 0.8:
                    s = 0.3
                    reason = f'Puell {puell:.2f} — miners under stress (accumulation zone)'
                else:
                    s = 0.0
                    reason = f'Puell {puell:.2f} — normal range'
                signals.append({'name': 'puell_multiple', 'score': s, 'reason': reason})
        except Exception as e:
            log.debug(f"  CryptoQuant Puell error: {e}")

    # ─── 9. Stablecoin Exchange Flow (market-wide) ───
    try:
        stable_rows = conn.execute(
            "SELECT netflow FROM cq_stablecoin_flows ORDER BY date DESC LIMIT 7"
        ).fetchall()

        if stable_rows:
            avg_flow = np.mean([r[0] or 0 for r in stable_rows])

            if avg_flow > 500_000_000:
                s = 0.5
                reason = f'Stablecoin inflow to exchanges ${avg_flow/1e6:+,.0f}M/day — buying power'
            elif avg_flow > 100_000_000:
                s = 0.2
                reason = f'Stablecoin mild inflow ${avg_flow/1e6:+,.0f}M/day'
            elif avg_flow < -500_000_000:
                s = -0.5
                reason = f'Stablecoin outflow from exchanges ${avg_flow/1e6:+,.0f}M/day — capital leaving'
            elif avg_flow < -100_000_000:
                s = -0.2
                reason = f'Stablecoin mild outflow ${avg_flow/1e6:+,.0f}M/day'
            else:
                s = 0.0
                reason = f'Stablecoin flow neutral ${avg_flow/1e6:+,.0f}M/day'

            signals.append({'name': 'stablecoin_flow', 'score': s, 'reason': reason})
    except Exception as e:
        log.debug(f"  CryptoQuant stablecoin flow error: {e}")

    if not signals:
        return {'score': 0.0, 'reason': 'No CryptoQuant data available', 'details': [], 'has_data': False}

    avg_score = float(np.mean([s['score'] for s in signals]))
    return {
        'score': avg_score,
        'reason': f'{len(signals)} CryptoQuant signals',
        'details': signals,
        'has_data': True,
    }


# ════════════════════════════════════════════
# MOMENTUM SIGNAL (fixes systematic bearish bias in bull markets)
# ════════════════════════════════════════════

def score_momentum(conn: sqlite3.Connection, coin: str) -> dict:
    """Pure price momentum signal — follows the trend, no contrarian logic.

    In bull markets, RSI/F&G/funding all say SELL (overbought/greedy/positive).
    Momentum captures what actually matters: is the price going up or down?

    Uses multi-timeframe: 7d (fast) + 14d (mid) + 30d (slow) for both BTC and coin.
    """
    score = 0.0
    has_data = False
    details = {}

    try:
        # Get BTC prices for market context
        btc_prices = conn.execute(
            "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 30"
        ).fetchall()

        # Get coin prices
        coin_prices = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 30", (coin,)
        ).fetchall()

        if len(coin_prices) < 7:
            return {'score': 0.0, 'has_data': False, 'details': {}}

        has_data = True
        current = coin_prices[0][0]

        # Coin momentum (multi-timeframe)
        coin_7d = ((current - coin_prices[6][0]) / coin_prices[6][0]) * 100
        coin_14d = ((current - coin_prices[13][0]) / coin_prices[13][0]) * 100 if len(coin_prices) >= 14 else None
        coin_30d = ((current - coin_prices[-1][0]) / coin_prices[-1][0]) * 100 if len(coin_prices) >= 25 else None

        details['coin_7d'] = round(coin_7d, 2)
        if coin_14d is not None:
            details['coin_14d'] = round(coin_14d, 2)
        if coin_30d is not None:
            details['coin_30d'] = round(coin_30d, 2)

        # BTC momentum (market context)
        btc_7d = 0
        btc_14d = None
        if len(btc_prices) >= 7:
            btc_7d = ((btc_prices[0][0] - btc_prices[6][0]) / btc_prices[6][0]) * 100
            details['btc_7d'] = round(btc_7d, 2)
        if len(btc_prices) >= 14:
            btc_14d = ((btc_prices[0][0] - btc_prices[13][0]) / btc_prices[13][0]) * 100
            details['btc_14d'] = round(btc_14d, 2)

        # Score calculation: normalize momentum to [-1, 1] signal
        # Coin's own momentum (60% weight) + BTC market momentum (40% weight)
        # Use tanh-like scaling: 10% change → ~0.7 signal, 20% → ~0.9

        def momentum_to_signal(pct_change):
            """Convert % change to [-1, 1] signal using tanh-like curve."""
            return float(np.tanh(pct_change / 12.0))  # 12% → 0.76, 6% → 0.46, 3% → 0.24

        # Coin momentum: weighted across timeframes (fast > slow)
        coin_mom = momentum_to_signal(coin_7d) * 0.50
        mom_w = 0.50
        if coin_14d is not None:
            coin_mom += momentum_to_signal(coin_14d) * 0.30
            mom_w += 0.30
        if coin_30d is not None:
            coin_mom += momentum_to_signal(coin_30d) * 0.20
            mom_w += 0.20
        coin_mom /= mom_w

        # BTC momentum
        btc_mom = momentum_to_signal(btc_7d)
        if btc_14d is not None:
            btc_mom = momentum_to_signal(btc_7d) * 0.6 + momentum_to_signal(btc_14d) * 0.4

        # Combined: coin 60% + BTC 40%
        score = coin_mom * 0.60 + btc_mom * 0.40

        # Exhaustion detection: when momentum is positive but RSI shows overbought,
        # or momentum is negative but RSI shows oversold → reduce signal
        # This catches turning points where momentum lags reality
        try:
            from src.crypto.technical_analyzer import analyze_coin
            tech = analyze_coin(conn, coin)
            rsi = tech.get('signals', {}).get('rsi', {}).get('value', 50)
            details['rsi'] = round(rsi, 1)

            if score > 0 and rsi > 70:
                # Bullish momentum but overbought → likely correction
                decay = max(0.2, 1.0 - (rsi - 70) / 30.0)  # RSI 70→1.0, RSI 85→0.5, RSI 100→0.2
                score *= decay
                details['exhaustion'] = f'overbought_decay_{decay:.2f}'
            elif score < 0 and rsi < 30:
                # Bearish momentum but oversold → likely bounce
                decay = max(0.2, 1.0 - (30 - rsi) / 30.0)  # RSI 30→1.0, RSI 15→0.5, RSI 0→0.2
                score *= decay
                details['exhaustion'] = f'oversold_decay_{decay:.2f}'

        except Exception:
            pass

        score = float(np.clip(score, -1.0, 1.0))

        details['coin_momentum'] = round(coin_mom, 3)
        details['btc_momentum'] = round(btc_mom, 3)

    except Exception as e:
        log.debug(f"  Momentum score error for {coin}: {e}")

    return {'score': round(score, 3), 'has_data': has_data, 'details': details}


def _get_v5_ranking_signal(conn: sqlite3.Connection, coin: str) -> dict:
    """
    Look up v5 ranking model signal for a coin.
    Caches rank_all_coins() result per day to avoid repeated computation.

    Returns: {'signal': str, 'confidence': float, 'rank': int, 'rank_score': float}
             or None if unavailable.
    """
    global _v5_ranking_cache, _v5_ranking_date
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if _v5_ranking_cache is None or _v5_ranking_date != today:
        try:
            from src.crypto.forecast_model_v5 import rank_all_coins
            result = rank_all_coins(conn)
            if result and result.get('rankings'):
                _v5_ranking_cache = {r['coin']: r for r in result['rankings']}
                _v5_ranking_date = today
                log.info(f"  [V5 RANKING] Cached {len(_v5_ranking_cache)} coins "
                         f"(regime={result.get('regime', '?')}, "
                         f"BUY={result.get('buy_coins', [])}, "
                         f"SELL={result.get('sell_coins', [])})")
            else:
                _v5_ranking_cache = {}
                _v5_ranking_date = today
        except Exception as e:
            log.debug(f"  v5 ranking unavailable: {e}")
            _v5_ranking_cache = {}
            _v5_ranking_date = today

    return _v5_ranking_cache.get(coin)


def _apply_v3_prediction(conn: sqlite3.Connection, coin: str, composite_fallback: float) -> dict:
    """
    Signal System v3: walk-forward validated signals override composite score.

    Returns:
        {'used': True, 'prediction': str, 'ml_score': float, 'confidence': float,
         'probabilities': dict, 'signal_type': str, 'signal_description': str}
         if a validated signal fires for this coin.
        {'used': False} if no signal meets threshold.
    """
    try:
        from src.crypto.signal_system import SignalSystem
    except ImportError:
        return {'used': False}

    global _signal_system_cache
    if '_signal_system_cache' not in globals() or _signal_system_cache is None:
        _signal_system_cache = SignalSystem(str(DB_PATH))

    system = _signal_system_cache
    data = system._get_coin_data(conn, coin)

    if not data:
        return {'used': False}

    # v3 detectors: compound first (strongest), then individual bearish, then capitulation
    # Check dynamically disabled signals (self_improver auto-disables below 50%)
    _disabled = set()
    try:
        disabled_rows = conn.execute(
            "SELECT signal_type FROM disabled_signals"
        ).fetchall()
        _disabled = {r[0] for r in disabled_rows}
    except Exception:
        pass  # Table may not exist yet

    best_signal = None

    # Compound bearish (60.7% hit rate, strongest signal)
    if 'compound_bearish' not in _disabled:
        compound = system.detect_compound_bearish(coin, data)
        if compound and compound.historical_hit_rate >= 0.55:
            best_signal = compound

    # Individual bearish signals (only if no compound)
    if not best_signal:
        detectors = [
            ('crowded_long', lambda: system.detect_crowded_long(coin, data)),
            ('seller_dominance', lambda: system.detect_seller_dominance(coin, data)),
            ('overbought', lambda: system.detect_overbought(coin, data)),
            ('oi_surge', lambda: system.detect_oi_surge(coin, data)),
            ('post_pump', lambda: system.detect_post_pump(coin, data)),
            ('capitulation', lambda: system.detect_volume_capitulation(coin, data) if hasattr(system, 'detect_volume_capitulation') else None),
        ]
        for sig_name, detector in detectors:
            if sig_name in _disabled:
                continue  # Skip dynamically disabled signals
            sig = detector()
            if sig and sig.historical_hit_rate >= 0.55 and sig.strength >= 0.3:
                if best_signal is None or sig.strength > best_signal.strength:
                    best_signal = sig

    if best_signal is None:
        return {'used': False}

    if best_signal.direction == 'BULLISH':
        prediction = 'BUY'
        ml_score = best_signal.strength * 0.5
    elif best_signal.direction == 'BEARISH':
        prediction = 'SELL'
        ml_score = -best_signal.strength * 0.5
    else:
        return {'used': False}

    # Regime-aware confidence: bearish signals weaker during bounce/recovery
    # March 2026: compound_bearish dropped 83.6% → 40.0% during bounce
    adjusted_hit_rate = best_signal.historical_hit_rate
    try:
        from src.crypto.regime_detector import detect_regime
        regime_info = detect_regime(conn)
        regime = regime_info.get('regime', 'ranging')
        if best_signal.direction == 'BEARISH' and regime in ('strong_bull', 'mild_bull'):
            # Bearish signals have ~40% accuracy during bounces/bull regimes
            adjusted_hit_rate = max(best_signal.historical_hit_rate - 0.15, 0.40)
            log.info(f"  [SIGNAL v3] Bearish signal in {regime} regime — "
                     f"confidence reduced {best_signal.historical_hit_rate:.0%} → {adjusted_hit_rate:.0%}")
    except Exception:
        pass

    log.info(f"  [SIGNAL v3] {coin}: {prediction} — {best_signal.signal_type} "
             f"(hit={adjusted_hit_rate:.0%}, "
             f"N_test={best_signal.historical_n_test})")

    return {
        'used': True,
        'prediction': prediction,
        'ml_score': ml_score,
        'confidence': adjusted_hit_rate,
        'probabilities': {
            'UP': best_signal.historical_hit_rate if best_signal.direction == 'BULLISH' else 1 - best_signal.historical_hit_rate,
            'DOWN': best_signal.historical_hit_rate if best_signal.direction == 'BEARISH' else 1 - best_signal.historical_hit_rate,
            'NEUTRAL': 0.0,
        },
        'signal_type': best_signal.signal_type,
        'signal_description': best_signal.description,
    }


_signal_system_cache = None


def forecast_coin(conn: sqlite3.Connection, coin: str, regime: dict = None,
                   meta_result: dict = None) -> dict:
    """Generate composite forecast for one coin.

    Args:
        regime: correlation regime from detect_correlation_regime()
               {'trend': 'dropping'|'rising'|'stable', 'delta': float}
        meta_result: pre-computed MetaAnalyst result (shared across all coins)
    """
    # Reload config if self_improver changed it
    _reload_config_if_changed()

    from src.crypto.technical_analyzer import analyze_coin

    # Technical signal
    tech = analyze_coin(conn, coin)
    tech_score = tech.get('score', 0)

    if tech.get('error'):
        return {
            'coin': coin, 'price': 0, 'composite_score': 0,
            'prediction': 'N/A', 'predicted_change': 'N/A',
            'confidence': 0, 'error': tech['error'],
            'categories': {}, 'price_targets': {},
        }

    # Other signals
    sentiment = score_sentiment(conn, coin)
    onchain = score_onchain(conn, coin)
    macro = score_macro(conn)
    news = score_news(conn, coin)
    historical = score_historical(conn, coin)
    learned = score_learned(conn, coin)
    coinglass = score_coinglass(conn, coin)
    cq = score_cryptoquant(conn, coin)

    # Claude news analysis (Level 2) — replaces mechanical sentiment+news
    news_claude_result = None
    try:
        from src.crypto.news_analyzer import score_news_claude
        news_claude_result = score_news_claude(conn, coin)
    except Exception as e:
        log.debug(f"  Claude news analysis unavailable: {e}")

    # Composite score
    # news_claude overrides sentiment+news when available (Claude understands context)
    nc_score = news_claude_result['score'] if news_claude_result and news_claude_result.get('has_data') else 0.0
    nc_has_data = bool(news_claude_result and news_claude_result.get('has_data'))

    hist_score = historical['score']

    # Use group-specific weights if available (Level 1 Phase A)
    # Fallback: overall WEIGHTS (backward compatible)
    coin_group = _get_coin_group(coin)
    group_config = None
    if _opt_config and coin_group and 'weights_by_group' in _opt_config:
        group_config = _opt_config['weights_by_group'].get(coin_group)

    # v20: regime-conditional weights
    # Detect BTC regime for weight adjustment
    _current_regime = 'sideways'
    try:
        _btc_p = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
            "ORDER BY timestamp DESC LIMIT 31"
        ).fetchall()
        if len(_btc_p) >= 31:
            _b7 = _btc_p[0][0] / _btc_p[7][0] - 1
            _b30 = _btc_p[0][0] / _btc_p[30][0] - 1
            _rs = 0
            if _b7 > 0.05: _rs += 2
            elif _b7 > 0.02: _rs += 1
            elif _b7 < -0.05: _rs -= 2
            elif _b7 < -0.02: _rs -= 1
            if _b30 > 0.10: _rs += 2
            elif _b30 > 0.03: _rs += 1
            elif _b30 < -0.10: _rs -= 2
            elif _b30 < -0.03: _rs -= 1
            if _rs >= 3: _current_regime = 'strong_bull'
            elif _rs >= 1: _current_regime = 'mild_bull'
            elif _rs <= -3: _current_regime = 'strong_bear'
            elif _rs <= -1: _current_regime = 'mild_bear'
    except Exception:
        pass

    # Apply regime-specific weight adjustments
    _w = dict(WEIGHTS)  # copy base weights
    adjustments = REGIME_WEIGHT_ADJUSTMENTS.get(_current_regime, {})
    for cat, delta in adjustments.items():
        if cat in _w:
            _w[cat] = max(0.0, _w[cat] + delta)
    # Renormalize to sum=1.0
    _w_sum = sum(_w.values())
    if _w_sum > 0:
        _w = {k: v / _w_sum for k, v in _w.items()}

    _buy_thr = THRESHOLDS['buy']
    _sell_thr = THRESHOLDS['neutral_low']
    _quality_gate = QUALITY_GATE
    _ma200_buy_mult = 1.5

    # Load thresholds from optimizer (if available, not weights)
    if _opt_config:
        _buy_thr = _opt_config.get('buy_threshold', _buy_thr)
        _sell_thr = _opt_config.get('sell_threshold', _sell_thr)
        _quality_gate = _opt_config.get('quality_gate', _quality_gate)
        _ma200_buy_mult = _opt_config.get('ma200_buy_mult', _ma200_buy_mult)

    categories = {
        'technical': {'score': tech_score, 'weight': _w.get('technical', WEIGHTS['technical']),
                      'details': tech.get('signals', {}), 'has_data': True},
        'sentiment': {'score': sentiment['score'], 'weight': _w.get('sentiment', WEIGHTS['sentiment']),
                      'details': sentiment, 'has_data': sentiment.get('has_data', False)},
        'onchain': {'score': onchain['score'], 'weight': _w.get('onchain', WEIGHTS['onchain']),
                    'details': onchain, 'has_data': onchain.get('has_data', False)},
        'macro': {'score': macro['score'], 'weight': _w.get('macro', WEIGHTS['macro']),
                  'details': macro, 'has_data': macro.get('has_data', False)},
        'news': {'score': news['score'], 'weight': _w.get('news', WEIGHTS['news']),
                 'details': news, 'has_data': news.get('has_data', False)},
        'news_claude': {'score': nc_score, 'weight': _w.get('news_claude', WEIGHTS['news_claude']),
                        'details': news_claude_result or {}, 'has_data': nc_has_data},
        'historical': {'score': hist_score, 'weight': _w.get('historical', WEIGHTS['historical']),
                       'details': historical, 'has_data': historical.get('has_data', False)},
        'learned': {'score': learned['score'], 'weight': _w.get('learned', WEIGHTS['learned']),
                    'details': learned, 'has_data': learned.get('has_data', False)},
        'meta_analyst': _get_meta_score(conn, coin, meta_result),
        'coinglass': {'score': coinglass['score'], 'weight': _w.get('coinglass', WEIGHTS['coinglass']),
                      'details': coinglass, 'has_data': coinglass.get('has_data', False)},
        'cryptoquant': {'score': cq['score'], 'weight': _w.get('cryptoquant', WEIGHTS['cryptoquant']),
                        'details': cq, 'has_data': cq.get('has_data', False)},
    }

    composite = sum(
        cat['score'] * cat['weight']
        for cat in categories.values()
        if not (np.isnan(cat['score']) or np.isnan(cat['weight']))
    )

    # Guard against NaN propagation from upstream scorers
    if np.isnan(composite):
        composite = 0.0

    # MA200 TREND CONTEXT (v19b — used as FILTER, not amplifier)
    # Training: above MA200 = 91.7% accuracy, below = 52.5%
    # Don't amplify composite (creates bad marginal predictions)
    # Instead, widen neutral band below MA200 → fewer but better predictions
    ma200_above = None
    try:
        coin_prices_200 = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 200", (coin,)
        ).fetchall()
        if len(coin_prices_200) >= 200:
            current_coin = coin_prices_200[0][0]
            sma200 = sum(p[0] for p in coin_prices_200) / len(coin_prices_200)
            ma200_above = current_coin > sma200
    except Exception:
        pass

    # BTC TREND REGIME ADJUSTMENT (v2 — asymmetric: dampen counter-trend only)
    btc_regime = 'ranging'
    try:
        btc_30d = conn.execute(
            "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 30"
        ).fetchall()
        if len(btc_30d) >= 25:
            current_btc = btc_30d[0][0]
            old_btc = btc_30d[-1][0]
            btc_30d_change = ((current_btc - old_btc) / old_btc) * 100

            if btc_30d_change > 15:
                btc_regime = 'strong_bull'
                if composite < 0:
                    composite *= 0.5
            elif btc_30d_change > 8:
                btc_regime = 'bull'
                if composite < 0:
                    composite *= 0.7
            elif btc_30d_change < -15:
                btc_regime = 'strong_bear'
                if composite > 0:
                    composite *= 0.5
            elif btc_30d_change < -8:
                btc_regime = 'bear'
                if composite > 0:
                    composite *= 0.7

            composite = float(np.clip(composite, -1.0, 1.0))
    except Exception:
        pass

    # Correlation regime adjustment (altcoins only)
    # Rising correlations = risk-off → dampen alt bullish signals
    # Dropping correlations = alt season → amplify alt bullish signals
    if coin != 'BTC' and regime:
        if regime.get('trend') == 'rising' and composite > 0:
            composite *= 0.8  # Risk-off: dampen alt bullish by 20%
        elif regime.get('trend') == 'dropping' and composite > 0:
            composite *= 1.15  # Alt season: amplify by 15%
        composite = float(np.clip(composite, -1.0, 1.0))

    # Level 2+4+6: ML correction (GBT wins → use GBT, else Elastic Net)
    # v20: SKIP groups where models have <50% accuracy (actively harmful)
    # Only apply ML correction for groups with proven > 50% test accuracy
    _ML_SAFE_GROUPS = {
        'meme': True,      # forest 66.0%, regression 64.4%
        'l1_alts': True,   # forest 64.3%, regression 57.8%
        'majors': True,    # regression 58.7% (but forest 45.9% — skip forest)
    }
    _ML_HARMFUL_GROUPS = {'defi', 'ai'}  # regression 35-42%, forest 39-42%

    try:
        from src.crypto.regression_model import (
            extract_features_live, predict_score, get_blend_weight, _get_coin_group as _reg_group
        )
        reg_group = _reg_group(coin)

        # v20: Skip ML correction for groups where models hurt accuracy
        if reg_group in _ML_HARMFUL_GROUPS:
            raise ValueError(f"ML correction disabled for {reg_group} (harmful)")

        # Try feature_builder features first (used by models trained with train_from_features)
        reg_features = None
        try:
            from src.crypto.feature_builder import FeatureBuilder
            _fb = FeatureBuilder(db_path=str(conn.execute("PRAGMA database_list").fetchone()[2]) if hasattr(conn, 'execute') else 'data/crypto/market.db')
            _feat_dict = _fb.build_features_single(coin, datetime.now(timezone.utc).strftime('%Y-%m-%d'))
            if _feat_dict:
                import json as _j
                _model_path = Path('data/crypto/regression_models') / f'{reg_group}.json'
                if _model_path.exists():
                    _m = _j.loads(_model_path.read_text())
                    _feat_names = _m.get('feature_names', [])
                    if _m.get('data_source') == 'feature_builder' and len(_feat_names) > 23:
                        _col_medians = _m.get('col_medians', [0.0] * len(_feat_names))
                        reg_features = np.array([
                            _feat_dict.get(f, _col_medians[i] if i < len(_col_medians) else 0.0)
                            for i, f in enumerate(_feat_names)
                        ], dtype=np.float64)
                        # Replace NaN with median (Elastic Net can't handle NaN)
                        nan_mask = np.isnan(reg_features)
                        for j in np.where(nan_mask)[0]:
                            reg_features[j] = _col_medians[j] if j < len(_col_medians) else 0.0
        except Exception:
            pass

        # Fallback to legacy features (23 features with category scores)
        if reg_features is None:
            cat_scores_dict = {name: cat['score'] for name, cat in categories.items()
                               if not np.isnan(cat.get('score', 0))}
            reg_features = extract_features_live(conn, coin, category_scores=cat_scores_dict)

        if reg_features is not None:
            ml_applied = False

            # Level 7: LightGBM v5 ranking model (83 features, event data)
            # v20: REDUCED from 35% to 10% blend — v5 has 46% direction accuracy
            # (worse than random). Keep for relative ranking only, not direction.
            try:
                from src.crypto.forecast_model_v5 import predict_v5
                v5_score = predict_v5(conn, coin)
                if v5_score is not None:
                    v5_signal = (v5_score - 0.5) * 2.0
                    v5_blend = 0.10  # v20: reduced from 0.35 (46% dir accuracy = harmful)
                    composite = composite * (1 - v5_blend) + v5_signal * v5_blend
                    composite = float(np.clip(composite, -1.0, 1.0))
                    ml_applied = True
            except Exception:
                pass

            # Level 6: Try GBT (if v5 didn't apply or as additional correction)
            # v20: Only for groups where forest model has >50% test accuracy
            if not ml_applied and reg_group not in ('majors',):  # forest/majors=45.9%
                try:
                    from src.crypto.ml_corrector import (
                        predict_score_forest, get_forest_blend_weight,
                    )
                    gbt_blend = get_forest_blend_weight(reg_group)
                    if gbt_blend > 0:
                        gbt_score = predict_score_forest(reg_features, reg_group)
                        if gbt_score is not None:
                            composite = composite * (1 - gbt_blend) + gbt_score * gbt_blend
                            composite = float(np.clip(composite, -1.0, 1.0))
                            ml_applied = True
                except Exception:
                    pass

            # Level 2+4: Elastic Net fallback (if nothing else applied)
            if not ml_applied:
                blend_w = get_blend_weight(reg_group)
                if blend_w > 0:
                    reg_score = predict_score(reg_features, reg_group)
                    if reg_score is not None:
                        composite = composite * (1 - blend_w) + reg_score * blend_w
                        composite = float(np.clip(composite, -1.0, 1.0))
    except Exception:
        pass  # Don't break forecasting if ML models unavailable

    # Dynamic NEUTRAL threshold based on coin volatility
    # High volatility = wider neutral band → fewer false predictions at turning points
    # Low volatility = standard band → normal sensitivity
    # Start from group-specific thresholds if available, else overall defaults
    buy_threshold = _buy_thr
    sell_threshold = _sell_thr
    try:
        atr_data = tech.get('signals', {}).get('atr', {})
        atr_val = atr_data.get('value', None)
        price = tech.get('price', 0)
        if atr_val and price > 0:
            atr_pct = atr_val / price * 100
            if atr_pct > 6:  # Very high volatility (>6% daily range)
                buy_threshold = 0.18
                sell_threshold = -0.18
            elif atr_pct > 4:  # High volatility
                buy_threshold = 0.14
                sell_threshold = -0.14
            # else: use defaults (0.1 / -0.1)
    except Exception:
        pass

    # MA200 FILTER (v19 — below MA200 = widen neutral band for BUY)
    # Training: above MA200 = 88-92% accuracy, below = 46-52%
    # Below MA200, require stronger bullish signal to avoid weak BUY predictions
    if ma200_above is False:
        buy_threshold *= _ma200_buy_mult  # Group-specific or default 1.5x

    # REGIME ADJUSTMENT: adapt thresholds to current market regime
    # mild_bull: lower buy threshold (more BUY), strong_bear: suppress signals
    regime_info = None
    try:
        from src.crypto.regime_detector import detect_regime, get_regime_multipliers
        regime_info = detect_regime(conn)
        regime_mults = get_regime_multipliers(regime_info['regime'])
        buy_threshold *= regime_mults['buy_mult']
        sell_threshold *= regime_mults['sell_mult']
    except Exception:
        pass

    # v22: BUY Detector — dedicated classifier for BUY opportunities
    # BUY detector disabled — 0/12 accuracy in production. Needs retraining.
    # Keeping code for logging/diagnostics only — does NOT override predictions.
    buy_detector_result = None
    try:
        from src.crypto.buy_detector import detect_buy_opportunity
        cat_scores = {name: cat['score'] for name, cat in categories.items()
                      if not np.isnan(cat.get('score', 0))}
        buy_detector_result = detect_buy_opportunity(conn, coin, composite, cat_scores, tech)
        if buy_detector_result and buy_detector_result.get('is_buy'):
            log.info(f"  [BUY Detector] {coin}: prob={buy_detector_result.get('probability', 0):.2f} "
                     f"(LOG ONLY — override disabled, 0/12 accuracy in production)")
    except Exception:
        pass

    # ── Signal System v3: walk-forward validated signals ──
    # Only issues BUY/SELL when a validated signal fires.
    # No signal = "DATA ONLY" (composite score shown as context, not as prediction).
    v3_result = None
    try:
        v3_result = _apply_v3_prediction(conn, coin, composite)
    except Exception as e:
        log.debug(f"  Signal system unavailable for {coin}: {e}")

    if v3_result and v3_result.get('used'):
        prediction = v3_result['prediction']
        composite = v3_result.get('ml_score', composite)
        composite = float(np.clip(composite, -1.0, 1.0))
    else:
        prediction = 'NEUTRAL'

    # ── V5 RANKING MODEL: secondary confirmation (v20: demoted) ──
    # v20: V5 has 46% direction accuracy — NEVER overrides v3 signals.
    # Used only as tiebreaker when v3 has no opinion (prediction=NEUTRAL).
    # v3 signals have 59-70% accuracy on 1000+ samples — they are king.
    v5_coin = _get_v5_ranking_signal(conn, coin)

    if v5_coin and prediction == 'NEUTRAL':
        v5_signal = v5_coin['signal']
        v5_confidence = v5_coin['confidence']
        v5_rank_score = v5_coin.get('rank_score', 0.5)

        # Only use v5 for STRONG signals (top/bottom 3) when nothing else fires
        if v5_signal == 'STRONG_BUY' and v5_confidence >= 0.75:
            prediction = 'BUY'
            composite = (v5_rank_score - 0.5) * 1.5  # dampened from 2.0
            composite = float(np.clip(composite, 0.05, 0.6))
            log.info(f"  [V5 BUY] {coin}: rank={v5_coin.get('rank')}, "
                     f"score={v5_rank_score:.4f} (tiebreaker, no v3 signal)")
        elif v5_signal == 'STRONG_SELL' and v5_confidence >= 0.75:
            prediction = 'SELL'
            composite = (v5_rank_score - 0.5) * 1.5
            composite = float(np.clip(composite, -0.6, -0.05))
            log.info(f"  [V5 SELL] {coin}: rank={v5_coin.get('rank')}, "
                     f"score={v5_rank_score:.4f} (tiebreaker, no v3 signal)")

    # Confidence (1-10): Hybrid formula calibrated against training accuracy
    # Training shows: volatility is strongest accuracy predictor (r=-0.17),
    # BTC stability is second (r=-0.14). Signal quality adds spread.
    # Spearman(confidence, accuracy) = +0.072 (positive = higher conf = more accurate)
    scores = [cat['score'] for cat in categories.values() if cat.get('has_data', True)]
    if not scores:
        scores = [0.0]
    n_positive = sum(1 for s in scores if s > 0)
    n_negative = sum(1 for s in scores if s < 0)
    agreement_ratio = max(n_positive, n_negative) / len(scores) if scores else 0
    avg_magnitude = float(np.mean([abs(s) for s in scores]))
    n_data_sources = sum(1 for cat in categories.values() if cat.get('has_data', False))

    confidence = 5  # default
    try:
        # Coin volatility (std of daily returns)
        _vol = 0.03
        try:
            _vol_prices = conn.execute(
                "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
                "ORDER BY timestamp DESC LIMIT 31", (coin,)
            ).fetchall()
            if len(_vol_prices) >= 7:
                _closes = [p[0] for p in _vol_prices if p[0]]
                _rets = [(c1 - c2) / c2 for c1, c2 in zip(_closes[:-1], _closes[1:]) if c2 != 0]
                if _rets:
                    _vol = float(np.std(_rets))
        except Exception:
            pass

        # BTC 7d change (market stability indicator)
        _btc_7d_abs = 0.0
        try:
            _btc_p = conn.execute(
                "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
                "ORDER BY timestamp DESC LIMIT 7"
            ).fetchall()
            if len(_btc_p) >= 7 and _btc_p[-1][0] and _btc_p[-1][0] > 0:
                _btc_7d_abs = abs((_btc_p[0][0] - _btc_p[-1][0]) / _btc_p[-1][0]) * 100
        except Exception:
            pass

        # Component 1: Inverse volatility (strongest accuracy predictor, 0-3 pts)
        _vol_score = max(0, 1 - _vol / 0.07) * 3.0

        # Component 2: BTC stability (second strongest predictor, 0-2 pts)
        _btc_stability = max(0, 1 - _btc_7d_abs / 15) * 2.0

        # Component 3: Signal quality — blend of strength + agreement (0-3 pts)
        _strength = min(abs(composite) / 0.20, 1.0)
        _agr_norm = min(max(agreement_ratio - 0.40, 0) / 0.40, 1.0)
        _signal_quality = _strength * 1.5 + _agr_norm * 1.5

        # Component 4: Data coverage (0-1 pt)
        _data_score = min(n_data_sources / 8, 1.0)

        # Combine: base(1) + vol(0-3) + btc(0-2) + signal(0-3) + data(0-1) = 1-10
        _raw_conf = 1.0 + _vol_score + _btc_stability + _signal_quality + _data_score
        confidence = int(np.clip(round(_raw_conf), 1, 10))
    except Exception:
        # Fallback: simple formula
        data_coverage = n_data_sources / len(WEIGHTS)
        raw_confidence = avg_magnitude * 8 + agreement_ratio * 4
        confidence = int(np.clip(raw_confidence * max(data_coverage, 0.5) + 1, 1, 10))

    # Level 5: Auto-adjust confidence based on rolling accuracy feedback
    try:
        rolling = conn.execute(
            "SELECT accuracy_30d, n_evaluated_30d FROM accuracy_rolling "
            "WHERE coin = ? ORDER BY date DESC LIMIT 1", (coin,)
        ).fetchone()
        if rolling and rolling[1] and rolling[1] >= 10:
            rolling_acc = rolling[0]
            if rolling_acc is not None:
                if rolling_acc < 0.40:
                    confidence = min(confidence, 3)
                elif rolling_acc > 0.70:
                    confidence = min(confidence + 1, 9)
    except Exception:
        pass

    # QUALITY GATE + REGIME SUPPRESSION: skip when v3 ML prediction is active
    # v3 already learned these patterns from data — double-filtering hurts accuracy
    _v3_active = v3_result and v3_result.get('used')

    # QUALITY GATE (v22): Symmetric filtering — same standard for BUY and SELL
    if not _v3_active and prediction != 'NEUTRAL' and _quality_gate > 0:
        symmetric_gate = max(_quality_gate, 0.35)
        min_agreeing_cats = 2
        if 'BUY' in prediction or 'STRONG BUY' in prediction:
            if agreement_ratio < symmetric_gate or n_positive < min_agreeing_cats:
                prediction = 'NEUTRAL'
        elif 'SELL' in prediction or 'STRONG SELL' in prediction:
            if agreement_ratio < symmetric_gate or n_negative < min_agreeing_cats:
                prediction = 'NEUTRAL'

    # REGIME SUPPRESSION (v22): Only suppress BUY in strong_bear, SELL in strong_bull
    if not _v3_active:
        try:
            if regime_info:
                regime = regime_info.get('regime', 'ranging')
                if 'BUY' in prediction and regime == 'strong_bear':
                    prediction = 'NEUTRAL'
                elif 'SELL' in prediction and regime == 'strong_bull':
                    prediction = 'NEUTRAL'
        except Exception:
            pass

    # Price targets from volatility + S/R
    current_price = tech.get('price', 0)
    price_targets = calc_price_targets(current_price, prediction, tech)

    result = {
        'coin': coin,
        'price': current_price,
        'composite_score': round(float(composite), 3),
        'prediction': prediction,
        'predicted_change': price_targets['predicted_change'],
        'target_high': price_targets.get('target_high', 0),
        'target_low': price_targets.get('target_low', 0),
        'confidence': confidence,
        'categories': categories,
        'price_targets': price_targets,
        'atr_pct': tech.get('atr_pct', 0),
        'support_resistance': tech.get('support_resistance', {'support': [], 'resistance': []}),
        'change_24h': tech.get('change_24h', 0),
        'change_7d': tech.get('change_7d', 0),
        'change_30d': tech.get('change_30d', 0),
        'btc_regime': btc_regime,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }

    # Add signal system v3 details (for video scripts)
    if v3_result:
        result['v3'] = {
            'used': v3_result.get('used', False),
            'confidence': v3_result.get('confidence', 0),
            'probabilities': v3_result.get('probabilities', {}),
            'direction': v3_result.get('prediction', ''),
            'signal_type': v3_result.get('signal_type', ''),
            'signal_description': v3_result.get('signal_description', ''),
        }

    return result


def compute_futures_signals(conn=None) -> dict:
    """Compute futures-specific analytical signals from derivatives data.

    Returns dict with:
      - leverage_environment: 'HIGH_RISK' | 'MODERATE' | 'LOW'
      - liquidation_clusters: [{coin, long_24h, short_24h, ratio, risk}]
      - options_context: [{coin, max_pain, distance_pct, put_call_ratio, sentiment}]
      - funding_regime: [{coin, rate, annualized, signal}]
      - oi_momentum: [{coin, change_24h, direction}]
    """
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")

    signals = {
        'leverage_environment': 'MODERATE',
        'liquidation_clusters': [],
        'options_context': [],
        'funding_regime': [],
        'oi_momentum': [],
    }

    # 1. Liquidation clusters — which coins have massive liquidation imbalance
    try:
        liq_rows = conn.execute(
            "SELECT coin, long_liq_usd_24h, short_liq_usd_24h, liq_usd_24h "
            "FROM cg_liquidations WHERE timestamp = ("
            "  SELECT MAX(timestamp) FROM cg_liquidations"
            ") ORDER BY liq_usd_24h DESC",
        ).fetchall()

        total_liq = 0
        for coin, long_liq, short_liq, total in liq_rows[:10]:
            if not total or total < 100000:
                continue
            total_liq += total
            ratio = long_liq / max(short_liq, 1)
            risk = 'HIGH' if total > 50_000_000 else 'MODERATE' if total > 10_000_000 else 'LOW'
            signals['liquidation_clusters'].append({
                'coin': coin,
                'long_24h': round(long_liq),
                'short_24h': round(short_liq),
                'total_24h': round(total),
                'long_short_ratio': round(ratio, 2),
                'risk': risk,
                'dominant': 'LONG LIQUIDATIONS' if ratio > 1.5 else 'SHORT LIQUIDATIONS' if ratio < 0.67 else 'BALANCED',
            })

        # Overall leverage environment
        if total_liq > 500_000_000:
            signals['leverage_environment'] = 'HIGH_RISK'
        elif total_liq > 100_000_000:
            signals['leverage_environment'] = 'MODERATE'
        else:
            signals['leverage_environment'] = 'LOW'
    except Exception as e:
        log.debug(f"Liquidation signals failed: {e}")

    # 2. Options max pain context — distance from current price
    try:
        for coin in ['BTC', 'ETH']:
            price_row = conn.execute(
                "SELECT price_usd FROM market_overview WHERE coin = ? ORDER BY timestamp DESC LIMIT 1",
                (coin,)
            ).fetchone()
            if not price_row:
                continue
            current_price = price_row[0]

            opt_rows = conn.execute(
                "SELECT expiry_date, max_pain_price, call_oi, put_oi "
                "FROM cg_options_max_pain WHERE coin = ? ORDER BY timestamp DESC LIMIT 6",
                (coin,)
            ).fetchall()

            for expiry, max_pain, call_oi, put_oi in opt_rows[:3]:
                if not max_pain or not current_price:
                    continue
                distance_pct = ((current_price - max_pain) / max_pain) * 100
                pc_ratio = put_oi / max(call_oi, 1) if call_oi else 0
                sentiment = 'BEARISH' if pc_ratio > 1.2 else 'BULLISH' if pc_ratio < 0.7 else 'NEUTRAL'

                signals['options_context'].append({
                    'coin': coin,
                    'expiry': expiry,
                    'max_pain': round(max_pain),
                    'current_price': round(current_price),
                    'distance_pct': round(distance_pct, 1),
                    'put_call_ratio': round(pc_ratio, 2),
                    'sentiment': sentiment,
                    'magnetic_pull': 'STRONG' if abs(distance_pct) < 3 else 'MODERATE' if abs(distance_pct) < 7 else 'WEAK',
                })
    except Exception as e:
        log.debug(f"Options signals failed: {e}")

    # 3. Funding rate regime — extreme funding = contrarian signal
    try:
        for coin in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE']:
            rate_row = conn.execute(
                "SELECT rate FROM funding_rates WHERE coin = ? ORDER BY timestamp DESC LIMIT 1",
                (coin,)
            ).fetchone()
            if rate_row and rate_row[0] is not None:
                rate = rate_row[0]
                annualized = rate * 3 * 365 * 100  # 8h rate → annual %
                if rate > 0.0005:
                    signal = 'LONGS_OVERLEVERAGED'
                elif rate < -0.0003:
                    signal = 'SHORTS_PAYING'
                else:
                    signal = 'NEUTRAL'
                signals['funding_regime'].append({
                    'coin': coin,
                    'rate': round(rate, 6),
                    'rate_pct': round(rate * 100, 4),
                    'annualized_pct': round(annualized, 1),
                    'signal': signal,
                })
    except Exception as e:
        log.debug(f"Funding signals failed: {e}")

    # 4. OI momentum — rising OI + falling price = leveraged short buildup
    try:
        for coin in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP']:
            oi_row = conn.execute(
                "SELECT SUM(oi_usd), AVG(change_pct_24h) FROM cg_aggregated_oi "
                "WHERE coin = ? AND timestamp > ? GROUP BY coin",
                (coin, int(datetime.now(timezone.utc).timestamp()) - 86400)
            ).fetchone()
            if oi_row and oi_row[0]:
                total_oi = oi_row[0]
                avg_change = oi_row[1] or 0
                direction = 'RISING' if avg_change > 2 else 'FALLING' if avg_change < -2 else 'STABLE'
                signals['oi_momentum'].append({
                    'coin': coin,
                    'total_oi_usd': round(total_oi),
                    'change_24h_pct': round(avg_change, 1),
                    'direction': direction,
                })
    except Exception as e:
        log.debug(f"OI momentum signals failed: {e}")

    return signals


def analyze_sector_rotation(forecasts: list) -> dict:
    """Analyze which sectors are leading/lagging based on forecast results.

    Returns dict of sector data with scores, changes, and top/worst coins.
    """
    sector_data = {}

    for sector, coins in COIN_SECTORS.items():
        sector_forecasts = [f for f in forecasts if f['coin'] in coins and not f.get('error')]

        if not sector_forecasts:
            sector_data[sector] = {
                'avg_score': 0.0, 'avg_change_7d': 0.0, 'avg_change_24h': 0.0,
                'signal': 'NO DATA', 'coins': coins, 'top_coin': None, 'worst_coin': None,
            }
            continue

        avg_score = np.mean([f['composite_score'] for f in sector_forecasts])
        avg_change_7d = np.mean([f.get('change_7d', 0) for f in sector_forecasts])
        avg_change_24h = np.mean([f.get('change_24h', 0) for f in sector_forecasts])

        # Best and worst coin in sector
        sorted_by_7d = sorted(sector_forecasts, key=lambda x: x.get('change_7d', 0), reverse=True)
        top_coin = sorted_by_7d[0] if sorted_by_7d else None
        worst_coin = sorted_by_7d[-1] if len(sorted_by_7d) > 1 else None

        # Sector signal
        if avg_score > THRESHOLDS['strong_buy']:
            signal = 'STRONG BUY'
        elif avg_score > THRESHOLDS['buy']:
            signal = 'BUY'
        elif avg_score > THRESHOLDS['neutral_low']:
            signal = 'NEUTRAL'
        elif avg_score > THRESHOLDS['sell']:
            signal = 'SELL'
        else:
            signal = 'STRONG SELL'

        sector_data[sector] = {
            'avg_score': round(float(avg_score), 3),
            'avg_change_7d': round(float(avg_change_7d), 1),
            'avg_change_24h': round(float(avg_change_24h), 1),
            'signal': signal,
            'coins': coins,
            'n_coins': len(sector_forecasts),
            'top_coin': {
                'coin': top_coin['coin'],
                'change_7d': round(top_coin.get('change_7d', 0), 1),
                'prediction': top_coin['prediction'],
            } if top_coin else None,
            'worst_coin': {
                'coin': worst_coin['coin'],
                'change_7d': round(worst_coin.get('change_7d', 0), 1),
                'prediction': worst_coin['prediction'],
            } if worst_coin else None,
        }

    # Detect rotation: sort sectors by 7d change
    sorted_sectors = sorted(sector_data.items(), key=lambda x: x[1]['avg_change_7d'], reverse=True)
    if len(sorted_sectors) >= 2:
        leader = sorted_sectors[0]
        laggard = sorted_sectors[-1]
        spread = leader[1]['avg_change_7d'] - laggard[1]['avg_change_7d']
        sector_data['_rotation'] = {
            'leader': leader[0],
            'leader_change': leader[1]['avg_change_7d'],
            'laggard': laggard[0],
            'laggard_change': laggard[1]['avg_change_7d'],
            'spread': round(float(spread), 1),
            'narrative': f"Capital rotating into {leader[0]} ({leader[1]['avg_change_7d']:+.1f}% 7d) "
                        f"from {laggard[0]} ({laggard[1]['avg_change_7d']:+.1f}% 7d)"
                        if spread > 5 else
                        f"Sectors relatively balanced (spread: {spread:.1f}%)"
        }

    return sector_data


# ════════════════════════════════════════════
# MULTI-TIMEFRAME FORECASTS (24h / 30d)
# ════════════════════════════════════════════

def forecast_coin_24h(conn: sqlite3.Connection, coin: str) -> dict:
    """Generate 24-hour horizon forecast for one coin.

    Uses shorter-term indicators optimized for 24h prediction:
      - 4h candles instead of 1d
      - RSI(7) instead of RSI(14)
      - MA20 instead of MA200 (short-term momentum)
      - Higher weight to funding rate and taker volume (short-term signals)
      - Lower weight to on-chain and macro (irrelevant for 24h)

    Returns dict with: prediction_24h, score_24h, confidence_24h, drivers_24h
    """
    from src.crypto.technical_analyzer import (
        get_prices, calc_rsi, calc_ema, calc_bollinger, calc_macd
    )

    # Use 4h candles for short-term view
    prices_4h = get_prices(conn, coin, '4h', 200)
    prices_1d = get_prices(conn, coin, '1d', 60)

    if len(prices_4h) < 20 and len(prices_1d) < 7:
        return {
            'prediction_24h': 'N/A',
            'score_24h': 0.0,
            'confidence_24h': 0,
            'drivers_24h': 'Insufficient data for 24h forecast',
        }

    signals = []
    drivers = []

    # 1. Short-term technical (RSI 7, MA20, Bollinger on 4h or 1d fallback)
    if len(prices_4h) >= 20:
        closes = np.array([p[4] for p in prices_4h])
    elif len(prices_1d) >= 20:
        closes = np.array([p[4] for p in prices_1d])
    else:
        closes = np.array([p[4] for p in prices_1d]) if prices_1d else np.array([])

    if len(closes) >= 10:
        # RSI(7) — faster, more responsive for 24h
        rsi_7 = calc_rsi(closes, period=7)
        current_rsi = rsi_7[-1] if not np.isnan(rsi_7[-1]) else 50

        if current_rsi < 20:
            rsi_score = 0.8
            drivers.append(f'RSI(7)={current_rsi:.0f} extreme oversold — bounce likely')
        elif current_rsi < 30:
            rsi_score = 0.5
            drivers.append(f'RSI(7)={current_rsi:.0f} oversold')
        elif current_rsi > 80:
            rsi_score = -0.8
            drivers.append(f'RSI(7)={current_rsi:.0f} extreme overbought — pullback likely')
        elif current_rsi > 70:
            rsi_score = -0.5
            drivers.append(f'RSI(7)={current_rsi:.0f} overbought')
        else:
            rsi_score = 0.0
        signals.append(('rsi7', rsi_score, 0.20))

        # EMA(9) vs EMA(21) — short-term momentum crossover
        if len(closes) >= 21:
            ema9 = calc_ema(closes, 9)
            ema21 = calc_ema(closes, 21)
            if not np.isnan(ema9[-1]) and not np.isnan(ema21[-1]):
                spread = (ema9[-1] - ema21[-1]) / ema21[-1]
                ma_score = float(np.clip(spread * 10, -0.6, 0.6))
                if ema9[-2] <= ema21[-2] and ema9[-1] > ema21[-1]:
                    ma_score = 0.7
                    drivers.append('EMA9 crossed above EMA21 — short-term bullish')
                elif ema9[-2] >= ema21[-2] and ema9[-1] < ema21[-1]:
                    ma_score = -0.7
                    drivers.append('EMA9 crossed below EMA21 — short-term bearish')
                signals.append(('ema_cross', ma_score, 0.15))

        # Bollinger on short timeframe
        if len(closes) >= 20:
            middle, upper, lower = calc_bollinger(closes, period=20)
            if not np.isnan(upper[-1]) and not np.isnan(lower[-1]):
                band_width = upper[-1] - lower[-1]
                if band_width > 0:
                    position = (closes[-1] - lower[-1]) / band_width
                    if position < 0.1:
                        bb_score = 0.6
                        drivers.append(f'At lower Bollinger ({position*100:.0f}%) — bounce zone')
                    elif position > 0.9:
                        bb_score = -0.6
                        drivers.append(f'At upper Bollinger ({position*100:.0f}%) — resistance')
                    else:
                        bb_score = 0.0
                    signals.append(('bollinger', bb_score, 0.10))

        # MACD short-term
        if len(closes) >= 30:
            macd_line, signal_line, histogram = calc_macd(closes, fast=8, slow=17, signal=9)
            if not np.isnan(histogram[-1]) and not np.isnan(histogram[-2]):
                if histogram[-2] <= 0 and histogram[-1] > 0:
                    macd_score = 0.6
                    drivers.append('MACD(8,17,9) bullish crossover')
                elif histogram[-2] >= 0 and histogram[-1] < 0:
                    macd_score = -0.6
                    drivers.append('MACD(8,17,9) bearish crossover')
                elif histogram[-1] > 0 and histogram[-1] > histogram[-2]:
                    macd_score = 0.3
                elif histogram[-1] < 0 and histogram[-1] < histogram[-2]:
                    macd_score = -0.3
                else:
                    macd_score = 0.0
                signals.append(('macd_short', macd_score, 0.10))

    # 2. Funding rate — HIGH weight for 24h (immediate squeeze/cost signal)
    try:
        rates = conn.execute(
            "SELECT rate FROM funding_rates WHERE coin = ? "
            "ORDER BY timestamp DESC LIMIT 6", (coin,)
        ).fetchall()

        if rates:
            recent_rates = [r[0] for r in rates]
            avg_rate = np.mean(recent_rates)
            if avg_rate < -0.001:
                fr_score = 0.7
                drivers.append(f'Funding {avg_rate*100:.3f}% (negative — short squeeze setup)')
            elif avg_rate < -0.0003:
                fr_score = 0.4
                drivers.append(f'Funding {avg_rate*100:.3f}% (slightly negative)')
            elif avg_rate > 0.003:
                fr_score = -0.7
                drivers.append(f'Funding {avg_rate*100:.3f}% (very high — long squeeze risk)')
            elif avg_rate > 0.001:
                fr_score = -0.4
                drivers.append(f'Funding {avg_rate*100:.3f}% (elevated)')
            else:
                fr_score = 0.0
            signals.append(('funding', fr_score, 0.20))
    except Exception:
        pass

    # 3. Taker buy/sell ratio — HIGH weight for 24h (immediate flow pressure)
    try:
        taker = conn.execute(
            "SELECT buy_sell_ratio FROM taker_volume WHERE coin = ? "
            "ORDER BY timestamp DESC LIMIT 1", (coin,)
        ).fetchone()

        if taker and taker[0]:
            ratio = taker[0]
            if ratio > 1.3:
                tk_score = 0.5
                drivers.append(f'Taker ratio {ratio:.2f} — aggressive buying')
            elif ratio < 0.7:
                tk_score = -0.5
                drivers.append(f'Taker ratio {ratio:.2f} — aggressive selling')
            else:
                tk_score = 0.0
            signals.append(('taker', tk_score, 0.15))
    except Exception:
        pass

    # 4. L/S ratio — short-term crowding signal
    try:
        ls = conn.execute(
            "SELECT long_pct FROM long_short_ratio WHERE coin = ? "
            "ORDER BY timestamp DESC LIMIT 1", (coin,)
        ).fetchone()

        if ls and ls[0]:
            long_pct = ls[0]
            if long_pct > 75:
                ls_score = -0.4
                drivers.append(f'Longs {long_pct:.0f}% — crowded, squeeze risk')
            elif long_pct < 30:
                ls_score = 0.4
                drivers.append(f'Longs {long_pct:.0f}% — shorts crowded, bounce setup')
            else:
                ls_score = 0.0
            signals.append(('long_short', ls_score, 0.10))
    except Exception:
        pass

    if not signals:
        return {
            'prediction_24h': 'N/A',
            'score_24h': 0.0,
            'confidence_24h': 0,
            'drivers_24h': 'No short-term data available',
        }

    # Weighted composite
    total_weight = sum(w for _, _, w in signals)
    composite = sum(s * w for _, s, w in signals) / total_weight if total_weight > 0 else 0
    composite = float(np.clip(composite, -1.0, 1.0))

    # Tighter thresholds for 24h (need stronger conviction for shorter horizon)
    if composite > 0.15:
        prediction = 'BUY'
    elif composite < -0.15:
        prediction = 'SELL'
    else:
        prediction = 'NEUTRAL'

    # Confidence
    scores_only = [s for _, s, _ in signals]
    n_agree = sum(1 for s in scores_only if (s > 0) == (composite > 0) and s != 0)
    agreement = n_agree / len(scores_only) if scores_only else 0
    confidence = int(np.clip(abs(composite) * 8 + agreement * 3 + 1, 1, 10))

    return {
        'prediction_24h': prediction,
        'score_24h': round(composite, 3),
        'confidence_24h': confidence,
        'drivers_24h': '; '.join(drivers) if drivers else 'Mixed signals',
    }


def forecast_coin_30d(conn: sqlite3.Connection, coin: str) -> dict:
    """Generate 30-day horizon forecast for one coin.

    Uses longer-term indicators optimized for monthly prediction:
      - Weekly candles (1d with 7-day grouping)
      - Higher weight to on-chain (NUPL, MVRV) and macro (DXY, F&G trend)
      - Lower weight to short-term technicals (noise on monthly timeframe)
      - MA200 trend as primary directional signal

    Returns dict with: prediction_30d, score_30d, confidence_30d, drivers_30d
    """
    signals = []
    drivers = []

    # 1. MA200 trend — PRIMARY signal for 30d (most reliable long-term indicator)
    try:
        prices_200 = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 200", (coin,)
        ).fetchall()

        if len(prices_200) >= 200:
            current = prices_200[0][0]
            sma200 = sum(p[0] for p in prices_200) / len(prices_200)
            distance_pct = ((current - sma200) / sma200) * 100

            if distance_pct > 20:
                ma_score = -0.3
                drivers.append(f'Price {distance_pct:.1f}% above MA200 — overextended')
            elif distance_pct > 5:
                ma_score = 0.4
                drivers.append(f'Price {distance_pct:.1f}% above MA200 — healthy uptrend')
            elif distance_pct > 0:
                ma_score = 0.2
                drivers.append(f'Price {distance_pct:.1f}% above MA200 — mild bullish')
            elif distance_pct > -10:
                ma_score = -0.2
                drivers.append(f'Price {distance_pct:.1f}% below MA200 — mild downtrend')
            else:
                ma_score = -0.5
                drivers.append(f'Price {distance_pct:.1f}% below MA200 — strong downtrend')

            signals.append(('ma200_trend', ma_score, 0.25))
    except Exception:
        pass

    # 2. On-chain: NUPL, MVRV — HIGH weight for 30d (cycle positioning)
    try:
        nupl = conn.execute(
            "SELECT nupl FROM cq_onchain WHERE metric_type = 'nupl' "
            "ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if nupl and nupl[0] is not None:
            val = nupl[0]
            if val < -0.25:
                oc_score = 0.8
                drivers.append(f'NUPL={val:.2f} capitulation zone — historically strong buy')
            elif val < 0:
                oc_score = 0.5
                drivers.append(f'NUPL={val:.2f} fear/capitulation — accumulation zone')
            elif val > 0.75:
                oc_score = -0.8
                drivers.append(f'NUPL={val:.2f} euphoria — historically strong sell')
            elif val > 0.5:
                oc_score = -0.4
                drivers.append(f'NUPL={val:.2f} greed zone')
            else:
                oc_score = 0.1
                drivers.append(f'NUPL={val:.2f} optimism zone')
            signals.append(('nupl', oc_score, 0.15))
    except Exception:
        pass

    try:
        mvrv = conn.execute(
            "SELECT mvrv_ratio FROM cq_onchain WHERE metric_type = 'mvrv' "
            "ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if mvrv and mvrv[0] is not None:
            val = mvrv[0]
            if val < 0.8:
                mv_score = 0.7
                drivers.append(f'MVRV={val:.2f} undervalued — strong accumulation zone')
            elif val < 1.0:
                mv_score = 0.4
                drivers.append(f'MVRV={val:.2f} below realized price — value zone')
            elif val > 3.5:
                mv_score = -0.8
                drivers.append(f'MVRV={val:.2f} extreme overvaluation')
            elif val > 2.5:
                mv_score = -0.5
                drivers.append(f'MVRV={val:.2f} overvalued territory')
            else:
                mv_score = 0.0
            signals.append(('mvrv', mv_score, 0.15))
    except Exception:
        pass

    # 3. Macro: DXY trend, BTC dominance — MEDIUM weight for 30d
    try:
        dxy_rows = conn.execute(
            "SELECT dxy_value FROM dxy_rates ORDER BY date DESC LIMIT 30"
        ).fetchall()

        if len(dxy_rows) >= 14:
            dxy_now = dxy_rows[0][0]
            dxy_14d = dxy_rows[13][0]
            if dxy_now and dxy_14d and dxy_14d > 0:
                dxy_change = (dxy_now - dxy_14d) / dxy_14d * 100
                if dxy_change > 2:
                    dxy_score = -0.4
                    drivers.append(f'DXY surging +{dxy_change:.1f}% (14d) — bearish for crypto')
                elif dxy_change < -2:
                    dxy_score = 0.4
                    drivers.append(f'DXY falling {dxy_change:.1f}% (14d) — bullish for crypto')
                else:
                    dxy_score = 0.0
                signals.append(('dxy', dxy_score, 0.10))
    except Exception:
        pass

    # 4. Fear & Greed trend (not spot value — 7d direction matters for 30d)
    try:
        fg_rows = conn.execute(
            "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 14"
        ).fetchall()

        if len(fg_rows) >= 7:
            recent_avg = np.mean([r[0] for r in fg_rows[:7]])
            older_avg = np.mean([r[0] for r in fg_rows[7:14]]) if len(fg_rows) >= 14 else recent_avg

            # Extreme values as contrarian
            if recent_avg < 20:
                fg_score = 0.6
                drivers.append(f'F&G avg={recent_avg:.0f} extreme fear — contrarian buy for 30d')
            elif recent_avg > 80:
                fg_score = -0.6
                drivers.append(f'F&G avg={recent_avg:.0f} extreme greed — contrarian sell for 30d')
            elif recent_avg < 35:
                fg_score = 0.3
            elif recent_avg > 65:
                fg_score = -0.3
            else:
                fg_score = 0.0

            # Trend direction adds conviction
            fg_trend = recent_avg - older_avg
            if abs(fg_trend) > 15:
                if fg_trend > 0:
                    fg_score -= 0.1  # Rising sentiment = contrarian risk
                else:
                    fg_score += 0.1  # Falling sentiment = contrarian opportunity

            signals.append(('fg_trend', float(np.clip(fg_score, -0.7, 0.7)), 0.10))
    except Exception:
        pass

    # 5. ETF flows trend (institutional conviction for 30d)
    try:
        etf_rows = conn.execute(
            "SELECT flow_usd FROM cg_etf_flows WHERE asset = 'BTC' "
            "ORDER BY date DESC LIMIT 14"
        ).fetchall()

        if etf_rows:
            total_14d = sum(r[0] or 0 for r in etf_rows)
            positive_days = sum(1 for r in etf_rows if r[0] and r[0] > 0)

            if total_14d > 1_000_000_000:
                etf_score = 0.6
                drivers.append(f'ETF +${total_14d/1e9:.1f}B (14d) — strong institutional accumulation')
            elif total_14d > 200_000_000:
                etf_score = 0.3
            elif total_14d < -1_000_000_000:
                etf_score = -0.6
                drivers.append(f'ETF ${total_14d/1e9:.1f}B (14d) — institutional distribution')
            elif total_14d < -200_000_000:
                etf_score = -0.3
            else:
                etf_score = 0.0

            # Dampen for non-BTC coins
            if coin != 'BTC':
                etf_score *= 0.4

            signals.append(('etf_trend', etf_score, 0.10))
    except Exception:
        pass

    # 6. Stablecoin supply (buying power) — good 30d signal
    try:
        stable_rows = conn.execute(
            "SELECT total_market_cap FROM cg_stablecoin_supply ORDER BY date DESC LIMIT 14"
        ).fetchall()

        if len(stable_rows) >= 7:
            recent_cap = stable_rows[0][0] or 0
            old_cap = stable_rows[-1][0] or 0
            if old_cap > 0:
                change = (recent_cap - old_cap) / old_cap * 100
                if change > 2:
                    sc_score = 0.4
                    drivers.append(f'Stablecoin supply +{change:.1f}% (14d) — fresh capital')
                elif change < -2:
                    sc_score = -0.4
                    drivers.append(f'Stablecoin supply {change:.1f}% (14d) — capital leaving')
                else:
                    sc_score = 0.0
                signals.append(('stablecoin', sc_score, 0.10))
    except Exception:
        pass

    # 7. Mild technical — weekly RSI(14) and price trend
    try:
        prices_60d = conn.execute(
            "SELECT close FROM prices WHERE coin = ? AND timeframe = '1d' "
            "ORDER BY timestamp DESC LIMIT 60", (coin,)
        ).fetchall()

        if len(prices_60d) >= 30:
            weekly_closes = np.array([p[0] for p in prices_60d[::7]])  # Sample weekly
            if len(weekly_closes) >= 7:
                from src.crypto.technical_analyzer import calc_rsi
                rsi_14 = calc_rsi(weekly_closes, period=7)  # Weekly RSI
                if not np.isnan(rsi_14[-1]):
                    wrsi = rsi_14[-1]
                    if wrsi < 30:
                        tech_score = 0.4
                        drivers.append(f'Weekly RSI={wrsi:.0f} oversold')
                    elif wrsi > 70:
                        tech_score = -0.4
                        drivers.append(f'Weekly RSI={wrsi:.0f} overbought')
                    else:
                        tech_score = 0.0
                    signals.append(('weekly_rsi', tech_score, 0.05))
    except Exception:
        pass

    if not signals:
        return {
            'prediction_30d': 'N/A',
            'score_30d': 0.0,
            'confidence_30d': 0,
            'drivers_30d': 'Insufficient long-term data',
        }

    # Weighted composite
    total_weight = sum(w for _, _, w in signals)
    composite = sum(s * w for _, s, w in signals) / total_weight if total_weight > 0 else 0
    composite = float(np.clip(composite, -1.0, 1.0))

    # Wider thresholds for 30d (more uncertainty)
    if composite > 0.08:
        prediction = 'BUY'
    elif composite < -0.08:
        prediction = 'SELL'
    else:
        prediction = 'NEUTRAL'

    # Confidence — lower baseline for 30d (inherently less certain)
    scores_only = [s for _, s, _ in signals]
    n_agree = sum(1 for s in scores_only if (s > 0) == (composite > 0) and s != 0)
    agreement = n_agree / len(scores_only) if scores_only else 0
    n_data = len(signals)
    confidence = int(np.clip(abs(composite) * 6 + agreement * 2 + min(n_data / 5, 1) + 1, 1, 8))

    return {
        'prediction_30d': prediction,
        'score_30d': round(composite, 3),
        'confidence_30d': confidence,
        'drivers_30d': '; '.join(drivers) if drivers else 'Mixed signals on monthly timeframe',
    }


def forecast_all(coins: list = None) -> list:
    """Generate forecasts for all tracked coins.

    Two-pass approach:
      Pass 1: Quantitative forecasts (no meta-analyst)
      Pass 2: Claude meta-analyst sees all quant results + news → final forecasts
    """
    from src.crypto.data_collector import TRACKED_COINS
    coins = coins or TRACKED_COINS

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    # Compute correlation regime once (used by all altcoin forecasts)
    regime = None
    try:
        from src.crypto.correlation_analyzer import CorrelationAnalyzer
        ca = CorrelationAnalyzer(conn)
        regime = ca.detect_correlation_regime(30, 90)
        log.info(f"  Correlation regime: {regime.get('trend', '?')} (delta={regime.get('delta', 0):.3f})")
    except Exception as e:
        log.warning(f"  Correlation regime detection failed: {e}")

    # PASS 1: Quantitative forecasts (meta_analyst=None → score=0)
    quant_forecasts = {}
    for coin in coins:
        try:
            forecast = forecast_coin(conn, coin, regime=regime, meta_result=None)
            quant_forecasts[coin] = forecast
        except Exception as e:
            log.warning(f"  {coin}: forecast failed — {e}")
            quant_forecasts[coin] = {
                'coin': coin, 'composite_score': 0, 'prediction': 'N/A',
                'confidence': 0, 'error': str(e),
            }

    # PASS 2: Claude meta-analyst (Level 3) — sees ALL quant data + news
    meta_result = None
    try:
        from src.crypto.news_analyzer import NewsAnalyzer
        from src.crypto.meta_analyst import MetaAnalyst

        news_analyzer = NewsAnalyzer(conn)
        news_analysis = news_analyzer.analyze()

        analyst = MetaAnalyst(conn)
        meta_result = analyst.analyze(quant_forecasts, news_analysis, coins, regime)
        log.info(f"  Meta-analyst: {meta_result.get('source', '?')}, "
                 f"{meta_result.get('n_coins', 0)} coin forecasts")
    except Exception as e:
        log.warning(f"  Meta-analyst failed: {e}")

    # PASS 3: Re-compute with meta-analyst scores if available
    forecasts = []
    if meta_result and meta_result.get('has_data'):
        for coin in coins:
            try:
                forecast = forecast_coin(conn, coin, regime=regime, meta_result=meta_result)
                forecasts.append(forecast)
            except Exception as e:
                forecasts.append(quant_forecasts.get(coin, {
                    'coin': coin, 'composite_score': 0, 'prediction': 'N/A',
                    'confidence': 0, 'error': str(e),
                }))
    else:
        # No meta-analyst — use pass 1 results
        forecasts = list(quant_forecasts.values())

    # PASS 4: Multi-timeframe enrichment (24h + 30d)
    # Add 24h and 30d predictions to each coin's forecast
    log.info("  Computing multi-timeframe forecasts (24h + 30d)...")
    for forecast in forecasts:
        coin = forecast.get('coin')
        if not coin or forecast.get('error'):
            continue
        try:
            f24h = forecast_coin_24h(conn, coin)
            forecast.update(f24h)
        except Exception as e:
            log.debug(f"  {coin}: 24h forecast failed — {e}")
            forecast['prediction_24h'] = 'N/A'
            forecast['score_24h'] = 0.0
            forecast['confidence_24h'] = 0
            forecast['drivers_24h'] = str(e)

        try:
            f30d = forecast_coin_30d(conn, coin)
            forecast.update(f30d)
        except Exception as e:
            log.debug(f"  {coin}: 30d forecast failed — {e}")
            forecast['prediction_30d'] = 'N/A'
            forecast['score_30d'] = 0.0
            forecast['confidence_30d'] = 0
            forecast['drivers_30d'] = str(e)

    n_24h = sum(1 for f in forecasts if f.get('prediction_24h') not in ('N/A', None))
    n_30d = sum(1 for f in forecasts if f.get('prediction_30d') not in ('N/A', None))
    log.info(f"  Multi-timeframe: {n_24h} coins with 24h, {n_30d} coins with 30d forecasts")

    conn.close()

    # Sort by score
    forecasts.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
    return forecasts


def save_predictions(forecasts: list, video_type: str = 'weekly_forecast'):
    """Save predictions to database for future scoring."""
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    now = datetime.now(timezone.utc)
    target = now + timedelta(days=7)

    for f in forecasts:
        if f.get('error') or f.get('prediction') == 'N/A':
            continue

        conn.execute(
            "INSERT INTO predictions "
            "(coin, created_at, prediction_date, target_date, signal_score, "
            "prediction, actual_price_at_prediction, video_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f['coin'],
                now.isoformat(),
                now.strftime('%Y-%m-%d'),
                target.strftime('%Y-%m-%d'),
                f['composite_score'],
                f['prediction'],
                f.get('price', 0),
                video_type,
            )
        )

    conn.commit()
    conn.close()
    log.info(f"  Saved {len(forecasts)} predictions (target: {target.strftime('%Y-%m-%d')})")


if __name__ == '__main__':
    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Forecast Engine")
    log.info("=" * 60)

    forecasts = forecast_all()

    print(f"\n{'Coin':<8} {'Price':>10} {'Score':>7} {'Conf':>5} {'Prediction':<14} {'Expected':>15}")
    print("=" * 68)

    for f in forecasts:
        if f.get('error'):
            print(f"{f['coin']:<8} {'N/A':>10} {'N/A':>7} {'':>5} {'ERROR':<14} {f['error'][:15]:>15}")
            continue

        print(f"{f['coin']:<8} ${f['price']:>9,.1f} {f['composite_score']:>+7.3f} "
              f"{f['confidence']:>4}/10 {f['prediction']:<14} {f['predicted_change']:>15}")

    # Multi-timeframe summary
    has_mtf = any(f.get('prediction_24h') and f['prediction_24h'] != 'N/A' for f in forecasts)
    if has_mtf:
        print(f"\n{'-'*75}")
        print(f"MULTI-TIMEFRAME FORECASTS:")
        print(f"{'Coin':<8} {'24h':>8} {'7d':>12} {'30d':>8}  {'24h Drivers':<40}")
        print("-" * 75)
        for f in forecasts:
            if f.get('error'):
                continue
            p24h = f.get('prediction_24h', 'N/A')
            p7d = f.get('prediction', 'N/A')
            p30d = f.get('prediction_30d', 'N/A')
            drv = f.get('drivers_24h', '')
            if len(drv) > 40:
                drv = drv[:37] + '...'
            print(f"{f['coin']:<8} {p24h:>8} {p7d:>12} {p30d:>8}  {drv:<40}")

    # Category breakdown for BTC
    btc = next((f for f in forecasts if f['coin'] == 'BTC'), None)
    if btc and 'categories' in btc:
        print(f"\n{'-'*60}")
        print(f"BTC Breakdown (score={btc['composite_score']:+.3f}, {btc['prediction']}):")
        for cat_name, cat in btc['categories'].items():
            weight_pct = int(cat['weight'] * 100)
            print(f"  {cat_name:<12} {cat['score']:>+6.3f} (×{weight_pct}%)")
            if isinstance(cat.get('details'), dict):
                for key, signal in cat['details'].items():
                    if isinstance(signal, dict) and 'reason' in signal:
                        print(f"    > {signal['reason']}")
        # Multi-timeframe for BTC
        if btc.get('prediction_24h') and btc['prediction_24h'] != 'N/A':
            print(f"\n  Multi-timeframe:")
            print(f"    24h: {btc['prediction_24h']} ({btc.get('score_24h', 0):+.3f}) — {btc.get('drivers_24h', '')}")
            print(f"    30d: {btc.get('prediction_30d', 'N/A')} ({btc.get('score_30d', 0):+.3f}) — {btc.get('drivers_30d', '')}")

    print()
