"""
FORTIX — Claude News Analyzer (Level 2)
================================================
Replaces mechanical sentiment scoring with Claude Haiku analysis.

Instead of naive F&G contrarian + keyword counting, this sends actual
news articles to Claude for contextual understanding.

Features:
  - Fetches last 24h news from DB (CryptoPanic RSS + Twitter)
  - Sends to Claude Haiku with market context (prices, F&G, trends)
  - Returns structured per-coin impact scores
  - 4-hour cache to avoid redundant API calls
  - Fallback to mechanical scoring if Claude API fails

Cost: ~$0.01-0.03/call (Haiku), called max 6x/day = ~$0.12/day

Usage:
    from src.crypto.news_analyzer import NewsAnalyzer
    analyzer = NewsAnalyzer(conn)
    result = analyzer.analyze()
    # result = {'overall': 0.3, 'coins': {'BTC': 0.5, 'ETH': -0.2, ...}, 'summary': '...'}
"""

import os
import sys
import sqlite3
import json
import logging
import hashlib
import time
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger('news_analyzer')

ANTHROPIC_KEY = os.getenv('ANTHROPIC_API_KEY', '')
CACHE_FILE = Path('data/crypto/news_analysis_cache.json')
CACHE_TTL_HOURS = 4

# Use Haiku for cost efficiency
MODEL = 'claude-haiku-4-5-20251001'


class NewsAnalyzer:
    """Claude-powered news analysis for crypto forecasting."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def analyze(self, coins: list = None) -> dict:
        """Analyze recent news with Claude Haiku.

        Returns:
            {
                'overall_score': float (-1 to +1),
                'coin_scores': {'BTC': float, 'ETH': float, ...},
                'summary': str,
                'key_events': [{'title': str, 'impact': int, 'direction': str}],
                'has_data': bool,
                'source': 'claude' | 'cache' | 'fallback',
            }
        """
        # Check cache first
        cached = self._load_cache()
        if cached:
            log.info("  News analysis: using cached result")
            cached['source'] = 'cache'
            return cached

        # Gather news data
        news_items = self._fetch_recent_news()
        social_data = self._fetch_social_sentiment()
        market_context = self._get_market_context()

        if not news_items and not social_data:
            return {
                'overall_score': 0.0,
                'coin_scores': {},
                'summary': 'No recent news data available',
                'key_events': [],
                'has_data': False,
                'source': 'none',
            }

        # Try Claude analysis
        if ANTHROPIC_KEY:
            try:
                result = self._analyze_with_claude(
                    news_items, social_data, market_context, coins
                )
                if result:
                    self._save_cache(result)
                    result['source'] = 'claude'
                    return result
            except Exception as e:
                log.warning(f"  Claude news analysis failed: {e}")

        # Fallback: mechanical scoring
        result = self._mechanical_fallback(news_items, social_data)
        result['source'] = 'fallback'
        return result

    def _fetch_recent_news(self, hours: int = 24) -> list:
        """Fetch news articles from last N hours."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

        rows = self.conn.execute(
            "SELECT title, source, sentiment, coins_mentioned, shock_score, timestamp "
            "FROM news WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 50",
            (cutoff,)
        ).fetchall()

        return [
            {
                'title': r[0],
                'source': r[1],
                'sentiment': r[2],
                'coins': r[3],
                'shock_score': r[4] or 0,
                'timestamp': r[5],
                'hours_ago': round((time.time() - r[5]) / 3600, 1),
            }
            for r in rows
        ]

    def _fetch_social_sentiment(self) -> list:
        """Fetch latest social sentiment data."""
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

        rows = self.conn.execute(
            "SELECT coin, source, score, volume, positive, negative "
            "FROM social_sentiment WHERE date IN (?, ?) "
            "ORDER BY date DESC",
            (today, yesterday)
        ).fetchall()

        return [
            {
                'coin': r[0],
                'source': r[1],
                'score': r[2],
                'volume': r[3],
                'positive': r[4],
                'negative': r[5],
            }
            for r in rows
        ]

    def _get_market_context(self) -> dict:
        """Get current market state for Claude context."""
        context = {}

        # BTC price and changes
        try:
            btc = self.conn.execute(
                "SELECT close FROM prices WHERE coin = 'BTC' AND timeframe = '1d' "
                "ORDER BY timestamp DESC LIMIT 30"
            ).fetchall()
            if btc:
                context['btc_price'] = btc[0][0]
                if len(btc) >= 7:
                    context['btc_7d_change'] = round(
                        ((btc[0][0] - btc[6][0]) / btc[6][0]) * 100, 1
                    )
                if len(btc) >= 30:
                    context['btc_30d_change'] = round(
                        ((btc[0][0] - btc[-1][0]) / btc[-1][0]) * 100, 1
                    )
        except Exception:
            pass

        # Fear & Greed
        try:
            fg = self.conn.execute(
                "SELECT value, classification FROM fear_greed ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if fg:
                context['fear_greed'] = fg[0]
                context['fg_classification'] = fg[1]
        except Exception:
            pass

        # ETH price
        try:
            eth = self.conn.execute(
                "SELECT close FROM prices WHERE coin = 'ETH' AND timeframe = '1d' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if eth:
                context['eth_price'] = eth[0]
        except Exception:
            pass

        return context

    def _analyze_with_claude(self, news_items: list, social_data: list,
                             market_context: dict, coins: list = None) -> Optional[dict]:
        """Send news to Claude Haiku for structured analysis."""
        import anthropic

        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

        # Build news section
        news_text = ""
        for i, item in enumerate(news_items[:30], 1):
            coins_tag = f" [{item['coins']}]" if item['coins'] else ""
            news_text += f"{i}. [{item['source']}] {item['title']}{coins_tag} ({item['hours_ago']}h ago)\n"

        if not news_text:
            news_text = "No news articles in the last 24 hours.\n"

        # Build social section
        social_text = ""
        for s in social_data:
            social_text += (
                f"  {s['coin']} ({s['source']}): score={s['score']:+.2f}, "
                f"vol={s['volume']}, pos={s['positive']:.0%}, neg={s['negative']:.0%}\n"
            )
        if not social_text:
            social_text = "  No social sentiment data available.\n"

        # Market context
        mc = market_context
        market_text = (
            f"BTC: ${mc.get('btc_price', 0):,.0f} "
            f"(7d: {mc.get('btc_7d_change', 0):+.1f}%, "
            f"30d: {mc.get('btc_30d_change', 0):+.1f}%)\n"
            f"ETH: ${mc.get('eth_price', 0):,.0f}\n"
            f"Fear & Greed: {mc.get('fear_greed', '?')} ({mc.get('fg_classification', '?')})\n"
        )

        prompt = f"""You are a crypto market analyst. Analyze these news items and social sentiment data.

CURRENT MARKET STATE:
{market_text}

NEWS (last 24 hours):
{news_text}

SOCIAL SENTIMENT:
{social_text}

INSTRUCTIONS:
1. Identify the 3-5 most impactful news events
2. For each, assess: market impact (1-10), direction (BULLISH/BEARISH/NEUTRAL), affected coins
3. Consider: Is the news already priced in? Is it a one-time event or trend shift?
4. Provide overall market sentiment score (-1.0 to +1.0)
5. Provide per-coin sentiment scores for major coins affected

Return ONLY valid JSON (no markdown, no explanation):
{{
    "overall_score": <float -1.0 to 1.0>,
    "summary": "<1-2 sentence market summary>",
    "key_events": [
        {{
            "title": "<event description>",
            "impact": <1-10>,
            "direction": "BULLISH|BEARISH|NEUTRAL",
            "coins": ["BTC", "ETH"],
            "reasoning": "<why this matters>"
        }}
    ],
    "coin_scores": {{
        "BTC": <float -1.0 to 1.0>,
        "ETH": <float -1.0 to 1.0>
    }}
}}"""

        log.info(f"  Calling Claude Haiku for news analysis ({len(news_items)} articles)...")

        response = client.messages.create(
            model=MODEL,
            max_tokens=1500,
            messages=[{'role': 'user', 'content': prompt}],
        )

        text = response.content[0].text.strip()

        # Parse JSON response
        # Handle potential markdown code blocks
        if text.startswith('```'):
            text = text.split('\n', 1)[1]
            if text.endswith('```'):
                text = text[:-3]

        parsed = json.loads(text)

        # Validate and sanitize
        overall = float(np.clip(parsed.get('overall_score', 0), -1.0, 1.0))
        coin_scores = {}
        for coin, score in parsed.get('coin_scores', {}).items():
            coin_scores[coin.upper()] = float(np.clip(score, -1.0, 1.0))

        result = {
            'overall_score': overall,
            'coin_scores': coin_scores,
            'summary': parsed.get('summary', ''),
            'key_events': parsed.get('key_events', [])[:5],
            'has_data': True,
            'n_articles': len(news_items),
            'n_social': len(social_data),
        }

        log.info(f"  Claude analysis: overall={overall:+.2f}, "
                 f"{len(coin_scores)} coin scores, {len(result['key_events'])} events")

        return result

    def _mechanical_fallback(self, news_items: list, social_data: list) -> dict:
        """Simple keyword-based fallback when Claude is unavailable."""
        bullish_words = ['bull', 'surge', 'rally', 'gain', 'up', 'buy', 'moon',
                         'accumulate', 'adoption', 'approve', 'etf']
        bearish_words = ['bear', 'crash', 'drop', 'fall', 'sell', 'hack', 'ban',
                         'regulation', 'scam', 'fraud', 'liquidat']

        bull_count = 0
        bear_count = 0
        for item in news_items:
            title_lower = item['title'].lower()
            for w in bullish_words:
                if w in title_lower:
                    bull_count += 1
                    break
            for w in bearish_words:
                if w in title_lower:
                    bear_count += 1
                    break

        total = bull_count + bear_count
        if total > 0:
            overall = (bull_count - bear_count) / total
        else:
            overall = 0.0

        return {
            'overall_score': float(np.clip(overall, -1.0, 1.0)),
            'coin_scores': {},
            'summary': f'Mechanical: {bull_count} bullish / {bear_count} bearish headlines',
            'key_events': [],
            'has_data': bool(news_items),
            'n_articles': len(news_items),
            'n_social': len(social_data),
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
        """Save analysis to cache (atomic write)."""
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cache = {
                'cached_at': datetime.now(timezone.utc).isoformat(),
                'result': result,
            }
            tmp = CACHE_FILE.with_suffix('.tmp')
            tmp.write_text(json.dumps(cache, indent=2, default=str))
            tmp.replace(CACHE_FILE)  # Atomic on most OS
        except Exception as e:
            log.warning(f"  Cache save failed: {e}")


def score_news_claude(conn: sqlite3.Connection, coin: str = 'BTC') -> dict:
    """Drop-in replacement for score_sentiment + score_news.

    Integrates Claude news analysis into the forecast engine scoring format.
    Returns dict with 'score', 'reason', 'details', 'has_data'.
    """
    analyzer = NewsAnalyzer(conn)
    analysis = analyzer.analyze()

    if not analysis.get('has_data'):
        return {
            'score': 0.0,
            'reason': 'No news data for Claude analysis',
            'details': analysis,
            'has_data': False,
        }

    # Get coin-specific score, fallback to overall
    coin_score = analysis.get('coin_scores', {}).get(coin)
    if coin_score is not None:
        score = coin_score
        reason = f"Claude ({analysis['source']}): {coin}={score:+.2f}"
    else:
        score = analysis['overall_score']
        reason = f"Claude ({analysis['source']}): overall={score:+.2f}"

    # Scale to [-0.8, 0.8] to match other signal ranges
    score = float(np.clip(score * 0.8, -0.8, 0.8))

    if analysis.get('summary'):
        reason += f" — {analysis['summary']}"

    return {
        'score': score,
        'reason': reason,
        'details': analysis,
        'has_data': True,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    conn = sqlite3.connect('data/crypto/market.db', timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    analyzer = NewsAnalyzer(conn)
    result = analyzer.analyze()

    print(f"\nOverall score: {result['overall_score']:+.2f}")
    print(f"Source: {result['source']}")
    print(f"Articles: {result.get('n_articles', 0)}")
    print(f"Summary: {result.get('summary', 'N/A')}")

    if result.get('key_events'):
        print(f"\nKey Events:")
        for e in result['key_events']:
            print(f"  [{e.get('direction','?')}] (impact {e.get('impact','?')}/10) {e.get('title','')}")
            if e.get('coins'):
                print(f"    Coins: {', '.join(e['coins'])}")

    if result.get('coin_scores'):
        print(f"\nCoin Scores:")
        for coin, score in sorted(result['coin_scores'].items()):
            print(f"  {coin}: {score:+.2f}")

    conn.close()
