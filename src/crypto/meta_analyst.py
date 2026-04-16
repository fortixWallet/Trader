"""
FORTIX — Claude Meta-Analyst (Level 3)
===============================================
Sends ALL available data to Claude Sonnet for contextual forecast.

This is the highest-level forecasting layer. It receives:
  - Quantitative signals from all categories (technical, on-chain, macro)
  - Claude Haiku news analysis (Level 2)
  - Market state (prices, trends, regimes)
  - Learned patterns from training database
  - Historical performance context

Claude Sonnet synthesizes everything into a final directional forecast
per coin, with reasoning. This provides the "human analyst" perspective
that pure quantitative models miss.

Key advantages over mechanical scoring:
  - Understands narrative/regime shifts (e.g. "ETF rotation to alts")
  - Can weigh conflicting signals contextually
  - Catches non-linear interactions (e.g. high funding + low volume = divergence)
  - Adapts reasoning to current market phase

Cost: ~$0.03-0.08/call (Sonnet), called max 2x/day = ~$0.10/day
Cache TTL: 6 hours (forecasts don't change intra-day)

Usage:
    from src.crypto.meta_analyst import MetaAnalyst
    analyst = MetaAnalyst(conn)
    result = analyst.analyze(quantitative_signals, news_analysis, coins)
"""

import os
import sys
import sqlite3
import json
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger('meta_analyst')

ANTHROPIC_KEY = os.getenv('ANTHROPIC_API_KEY', '')
CACHE_FILE = Path('data/crypto/meta_analysis_cache.json')
CACHE_TTL_HOURS = 6

# Use Sonnet for deep reasoning
MODEL = 'claude-sonnet-4-5-20250929'


class MetaAnalyst:
    """Claude Sonnet meta-analyst for contextual crypto forecasting."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def analyze(self, quantitative: dict, news_analysis: dict,
                coins: list, regime: dict = None) -> dict:
        """Run Claude Sonnet meta-analysis on ALL available data.

        Args:
            quantitative: {coin: {category: {score, reason, ...}}} from forecast_coin
            news_analysis: result from NewsAnalyzer.analyze()
            coins: list of coins to forecast
            regime: correlation regime dict

        Returns:
            {
                'coin_forecasts': {
                    'BTC': {'direction': 'UP|DOWN|SIDEWAYS', 'confidence': 1-10,
                            'score': float, 'reasoning': str},
                    ...
                },
                'market_outlook': str,
                'has_data': bool,
                'source': 'claude' | 'cache' | 'fallback',
            }
        """
        # Check cache
        cached = self._load_cache()
        if cached:
            log.info("  Meta-analyst: using cached result")
            cached['source'] = 'cache'
            return cached

        if not ANTHROPIC_KEY:
            return self._empty_result('No API key')

        try:
            result = self._analyze_with_claude(quantitative, news_analysis, coins, regime)
            if result:
                self._save_cache(result)
                result['source'] = 'claude'
                return result
        except Exception as e:
            log.warning(f"  Claude meta-analysis failed: {e}")

        return self._empty_result('API call failed')

    def _build_quant_section(self, quantitative: dict, coins: list) -> str:
        """Build quantitative signals section for prompt."""
        lines = []
        for coin in coins:
            q = quantitative.get(coin, {})
            if not q:
                continue

            price = q.get('price', 0)
            score = q.get('composite_score', 0)
            pred = q.get('prediction', 'N/A')
            regime = q.get('btc_regime', '?')

            line = f"{coin}: ${price:,.2f} | quant_score={score:+.3f} | signal={pred} | regime={regime}"

            # Add key category scores
            cats = q.get('categories', {})
            cat_parts = []
            for cat_name in ['technical', 'onchain', 'historical', 'learned']:
                cat = cats.get(cat_name, {})
                if cat.get('has_data'):
                    cat_parts.append(f"{cat_name}={cat['score']:+.2f}")
            if cat_parts:
                line += f"\n  Signals: {', '.join(cat_parts)}"

            # Add price targets
            pt = q.get('price_targets', {})
            if pt.get('target_low') and pt.get('target_high'):
                line += f"\n  Targets: ${pt['target_low']:,.2f} — ${pt['target_high']:,.2f}"
                if pt.get('support'):
                    line += f" | S: ${pt['support']:,.2f}"
                if pt.get('resistance'):
                    line += f" | R: ${pt['resistance']:,.2f}"

            # Changes
            c24 = q.get('change_24h', 0)
            c7 = q.get('change_7d', 0)
            c30 = q.get('change_30d', 0)
            line += f"\n  Changes: 24h={c24:+.1f}% | 7d={c7:+.1f}% | 30d={c30:+.1f}%"

            lines.append(line)

        return '\n\n'.join(lines)

    def _build_news_section(self, news_analysis: dict) -> str:
        """Build news analysis section for prompt."""
        if not news_analysis or not news_analysis.get('has_data'):
            return "No news analysis available."

        parts = [f"Overall sentiment: {news_analysis.get('overall_score', 0):+.2f}"]

        if news_analysis.get('summary'):
            parts.append(f"Summary: {news_analysis['summary']}")

        events = news_analysis.get('key_events', [])
        if events:
            parts.append("Key Events:")
            for e in events[:5]:
                coins_str = ', '.join(e.get('coins', [])) if e.get('coins') else '?'
                parts.append(
                    f"  [{e.get('direction', '?')}] (impact {e.get('impact', '?')}/10) "
                    f"{e.get('title', '')} [{coins_str}]"
                )

        coin_scores = news_analysis.get('coin_scores', {})
        if coin_scores:
            scores_str = ', '.join(f"{c}={s:+.2f}" for c, s in sorted(coin_scores.items()))
            parts.append(f"Coin sentiment: {scores_str}")

        return '\n'.join(parts)

    def _build_patterns_section(self) -> str:
        """Build learned patterns section for prompt."""
        pat_db = Path('data/crypto/patterns.db')
        if not pat_db.exists():
            return "No learned patterns available."

        try:
            pconn = sqlite3.connect(str(pat_db))
            pconn.row_factory = sqlite3.Row
            patterns = pconn.execute(
                "SELECT description, direction_accuracy, avg_actual_change, "
                "confidence_score, sample_size, pattern_score "
                "FROM learned_patterns WHERE active = 1 "
                "ORDER BY confidence_score DESC LIMIT 10"
            ).fetchall()
            pconn.close()

            if not patterns:
                return "No patterns in database."

            lines = ["Top learned patterns (from 1750 historical predictions):"]
            for p in patterns:
                lines.append(
                    f"  {p['description']}: {p['direction_accuracy']*100:.0f}% accuracy "
                    f"(n={p['sample_size']}, avg_chg={p['avg_actual_change']:+.1f}%, "
                    f"score={p['pattern_score']:+.2f})"
                )
            return '\n'.join(lines)
        except Exception:
            return "Pattern database unavailable."

    def _build_market_state(self, regime: dict) -> str:
        """Build market state section."""
        parts = []

        # F&G
        try:
            fg = self.conn.execute(
                "SELECT value, classification FROM fear_greed ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if fg:
                parts.append(f"Fear & Greed: {fg[0]} ({fg[1]})")
        except Exception:
            pass

        # Correlation regime
        if regime:
            parts.append(
                f"Correlation regime: {regime.get('trend', '?')} "
                f"(delta={regime.get('delta', 0):.3f})"
            )

        # BTC dominance
        try:
            dom = self.conn.execute(
                "SELECT btc_dominance FROM global_metrics ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if dom:
                parts.append(f"BTC dominance: {dom[0]:.1f}%")
        except Exception:
            pass

        # Funding rate overview (BTC)
        try:
            fr = self.conn.execute(
                "SELECT AVG(rate) FROM (SELECT rate FROM funding_rates "
                "WHERE coin = 'BTC' ORDER BY timestamp DESC LIMIT 30)"
            ).fetchone()
            if fr and fr[0] is not None:
                parts.append(f"BTC avg funding rate (30): {fr[0]*100:.4f}%")
        except Exception:
            pass

        return '\n'.join(parts) if parts else "No market state data."

    def _analyze_with_claude(self, quantitative: dict, news_analysis: dict,
                             coins: list, regime: dict) -> Optional[dict]:
        """Send ALL data to Claude Sonnet for meta-analysis."""
        import anthropic

        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

        quant_text = self._build_quant_section(quantitative, coins)
        news_text = self._build_news_section(news_analysis)
        patterns_text = self._build_patterns_section()
        market_text = self._build_market_state(regime)

        coins_json = json.dumps(coins)

        prompt = f"""You are a professional crypto analyst with expertise in both technical and fundamental analysis. Your task is to synthesize ALL available data into 7-day directional forecasts.

IMPORTANT RULES:
- Be honest about uncertainty. If signals conflict, say SIDEWAYS with low confidence.
- Do NOT have a bullish or bearish bias. Let the data speak.
- Consider whether news is already priced in (24h+ old news usually is).
- In strong trends (30d change > 15%), trend-following beats contrarian.
- In ranging markets, mean-reversion/contrarian signals work better.

MARKET STATE:
{market_text}

QUANTITATIVE SIGNALS (per coin):
{quant_text}

NEWS ANALYSIS (Claude Haiku Level 2):
{news_text}

LEARNED PATTERNS (from historical backtesting):
{patterns_text}

COINS TO FORECAST: {coins_json}

For each coin, provide a 7-day directional forecast. Consider:
1. Does the quantitative signal align with news sentiment?
2. Is the current regime favorable for this coin's sector?
3. Are there any contradictions between signals that suggest caution?
4. What is the highest-conviction trade vs noise?

Return ONLY valid JSON (no markdown, no explanation):
{{
    "market_outlook": "<2-3 sentence overall market assessment>",
    "coin_forecasts": {{
        "BTC": {{
            "direction": "UP|DOWN|SIDEWAYS",
            "confidence": <1-10>,
            "expected_range_pct": [<low%>, <high%>],
            "reasoning": "<1-2 sentences>"
        }}
    }}
}}

Include ALL coins from the list: {coins_json}"""

        log.info(f"  Calling Claude Sonnet for meta-analysis ({len(coins)} coins)...")

        response = client.messages.create(
            model=MODEL,
            max_tokens=3000,
            messages=[{'role': 'user', 'content': prompt}],
        )

        text = response.content[0].text.strip()

        # Handle potential markdown code blocks
        if text.startswith('```'):
            text = text.split('\n', 1)[1]
            if text.endswith('```'):
                text = text[:-3]

        parsed = json.loads(text)

        # Convert to standardized format
        coin_forecasts = {}
        for coin, forecast in parsed.get('coin_forecasts', {}).items():
            coin = coin.upper()
            direction = forecast.get('direction', 'SIDEWAYS').upper()
            confidence = min(max(int(forecast.get('confidence', 5)), 1), 10)

            # Convert direction to score
            if direction == 'UP':
                score = confidence / 10.0  # 0.1 to 1.0
            elif direction == 'DOWN':
                score = -confidence / 10.0  # -0.1 to -1.0
            else:
                score = 0.0

            coin_forecasts[coin] = {
                'direction': direction,
                'confidence': confidence,
                'score': float(np.clip(score, -1.0, 1.0)),
                'expected_range': forecast.get('expected_range_pct', [-5, 5]),
                'reasoning': forecast.get('reasoning', ''),
            }

        result = {
            'coin_forecasts': coin_forecasts,
            'market_outlook': parsed.get('market_outlook', ''),
            'has_data': bool(coin_forecasts),
            'n_coins': len(coin_forecasts),
        }

        log.info(f"  Meta-analysis: {len(coin_forecasts)} forecasts, "
                 f"outlook: {result['market_outlook'][:80]}...")

        return result

    def _empty_result(self, reason: str) -> dict:
        return {
            'coin_forecasts': {},
            'market_outlook': reason,
            'has_data': False,
            'source': 'none',
        }

    def _load_cache(self) -> Optional[dict]:
        """Load cached analysis if fresh enough."""
        if not CACHE_FILE.exists():
            return None
        try:
            data = json.loads(CACHE_FILE.read_text())
            cached_at = datetime.fromisoformat(data.get('cached_at', ''))
            age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
            if age_hours < CACHE_TTL_HOURS:
                return data.get('result')
        except Exception:
            pass
        return None

    def _save_cache(self, result: dict):
        """Save analysis to cache."""
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cache = {
                'cached_at': datetime.now(timezone.utc).isoformat(),
                'result': result,
            }
            CACHE_FILE.write_text(json.dumps(cache, indent=2, default=str))
        except Exception as e:
            log.warning(f"  Meta-analysis cache save failed: {e}")


def score_meta_analyst(conn: sqlite3.Connection, coin: str,
                       meta_result: dict) -> dict:
    """Extract per-coin score from meta-analysis result.

    Called by forecast_coin() with pre-computed meta_result (shared across all coins).

    Returns dict with 'score', 'reason', 'details', 'has_data'.
    """
    if not meta_result or not meta_result.get('has_data'):
        return {
            'score': 0.0,
            'reason': 'Meta-analyst unavailable',
            'details': {},
            'has_data': False,
        }

    forecast = meta_result.get('coin_forecasts', {}).get(coin)
    if not forecast:
        return {
            'score': 0.0,
            'reason': f'No meta-analysis for {coin}',
            'details': {},
            'has_data': False,
        }

    # Scale to [-0.8, 0.8] to match other signal ranges
    score = float(np.clip(forecast['score'] * 0.8, -0.8, 0.8))

    return {
        'score': score,
        'reason': f"Meta ({forecast['direction']}, conf={forecast['confidence']}/10): "
                  f"{forecast.get('reasoning', '')}",
        'details': forecast,
        'has_data': True,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    from src.crypto.data_collector import TRACKED_COINS

    conn = sqlite3.connect('data/crypto/market.db', timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    # Build quantitative signals (simplified)
    from src.crypto.forecast_engine import forecast_coin
    quant = {}
    test_coins = ['BTC', 'ETH', 'SOL', 'DOGE', 'AAVE']
    for coin in test_coins:
        try:
            result = forecast_coin(conn, coin)
            quant[coin] = result
        except Exception as e:
            print(f"  {coin}: forecast failed -- {e}")

    # Get news analysis
    from src.crypto.news_analyzer import NewsAnalyzer
    news_analyzer = NewsAnalyzer(conn)
    news = news_analyzer.analyze()

    # Run meta-analysis
    analyst = MetaAnalyst(conn)
    result = analyst.analyze(quant, news, test_coins)

    print(f"\nSource: {result['source']}")
    print(f"Market outlook: {result.get('market_outlook', 'N/A')}")
    print(f"\nCoin Forecasts:")
    for coin, f in result.get('coin_forecasts', {}).items():
        print(f"  {coin}: {f['direction']} (conf={f['confidence']}/10, "
              f"score={f['score']:+.2f}) — {f.get('reasoning', '')}")

    conn.close()
