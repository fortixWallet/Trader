"""
FORTIX News Reactor — Real-Time News Impact Trading
====================================================

Monitors news feeds every 2 minutes:
1. Fetches latest crypto news (CoinDesk, CoinTelegraph, Twitter, RSS)
2. Claude Haiku scores each news: impact (1-10), direction, affected coins
3. If impact >= 7 → position management:
   - Impact 7: close LOSING positions that contradict the news
   - Impact 8+: close ALL positions that contradict the news
   - Cancel contradicting pending limit orders (handled by trader_bybit)
4. If impact 5-6 → adjust confidence for next scan

Cost: ~$5-10/day in Claude API calls
"""

import os
import time
import json
import sqlite3
import logging
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

# News sources
COINDESK_RSS = "https://www.coindesk.com/arc/outboundfeeds/rss/"
COINTELEGRAPH_RSS = "https://cointelegraph.com/rss"

# Coins we trade
TRADING_COINS = {'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'DOGE',
                 'UNI', 'AAVE', 'LDO', 'CRV', 'RENDER', 'TAO', 'ARB', 'OP',
                 'POL', 'WIF', 'PENDLE', 'JUP', 'PYTH', 'JTO', 'BTC', 'ETH'}


class NewsItem:
    def __init__(self, title: str, source: str, timestamp: float):
        self.title = title
        self.source = source
        self.timestamp = timestamp
        self.hash = hashlib.md5(title.encode()).hexdigest()[:12]
        self.impact_score = 0
        self.direction = 'NEUTRAL'  # BULLISH, BEARISH, NEUTRAL
        self.affected_coins = []
        self.analyzed = False


class NewsReactor:
    """Monitors news and provides trading signals."""

    def __init__(self):
        self._api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        self._twitter_api_key = os.environ.get('TWITTER_API_KEY', '')
        self._twitter_api_url = os.environ.get('TWITTER_API_URL', 'https://api.twitterapi.io')
        self._seen_hashes = set()
        self._last_fetch = 0
        self._last_high_impact = None
        self._load_seen()

    def _load_seen(self):
        """Load already-seen news hashes to avoid re-processing."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""CREATE TABLE IF NOT EXISTS news_reactions (
                hash TEXT PRIMARY KEY, title TEXT, source TEXT,
                impact_score INTEGER, direction TEXT, affected_coins TEXT,
                timestamp TEXT, analyzed_at TEXT
            )""")
            rows = conn.execute(
                "SELECT hash FROM news_reactions WHERE analyzed_at > datetime('now', '-24 hours')"
            ).fetchall()
            self._seen_hashes = {r[0] for r in rows}
            conn.close()
        except Exception:
            pass

    def fetch_news(self) -> List[NewsItem]:
        """Fetch latest news from RSS feeds."""
        news = []

        # CoinDesk
        try:
            import xml.etree.ElementTree as ET
            r = requests.get(COINDESK_RSS, timeout=10)
            root = ET.fromstring(r.content)
            for item in root.findall('.//item')[:10]:
                title = item.find('title')
                if title is not None and title.text:
                    n = NewsItem(title.text, 'coindesk', time.time())
                    if n.hash not in self._seen_hashes:
                        news.append(n)
        except Exception as e:
            logger.debug(f"CoinDesk fetch: {e}")

        # CoinTelegraph
        try:
            r = requests.get(COINTELEGRAPH_RSS, timeout=10)
            root = ET.fromstring(r.content)
            for item in root.findall('.//item')[:10]:
                title = item.find('title')
                if title is not None and title.text:
                    n = NewsItem(title.text, 'cointelegraph', time.time())
                    if n.hash not in self._seen_hashes:
                        news.append(n)
        except Exception as e:
            logger.debug(f"CoinTelegraph fetch: {e}")

        # Twitter — crypto influencers & breaking news
        try:
            twitter_news = self._fetch_twitter()
            news.extend(twitter_news)
        except Exception as e:
            logger.debug(f"Twitter fetch: {e}")

        # Also check our DB for recent news
        try:
            conn = sqlite3.connect(str(DB_PATH))
            rows = conn.execute("""
                SELECT title, source FROM news
                WHERE timestamp > strftime('%s', 'now', '-30 minutes')
                ORDER BY timestamp DESC LIMIT 20
            """).fetchall()
            conn.close()
            for title, source in rows:
                n = NewsItem(title, source, time.time())
                if n.hash not in self._seen_hashes:
                    news.append(n)
        except Exception:
            pass

        return news

    # Curated breaking news accounts — only these are monitored
    TWITTER_SOURCES = [
        'WatcherGuru',      # "JUST IN:" crypto news alerts
        'unusual_whales',   # whale alerts, regulatory news
        'tier10k',          # fastest breaking news bot
        'DeItaone',         # Bloomberg terminal leaks
        'whale_alert',      # large on-chain transfers
        'BitcoinMagazine',  # BTC-specific major news
    ]

    def _fetch_twitter(self) -> List[NewsItem]:
        """Fetch breaking news from curated Twitter accounts only."""
        if not self._twitter_api_key:
            return []

        news = []
        try:
            headers = {'X-API-Key': self._twitter_api_key}
            # Single query: only from our trusted sources
            from_filter = ' OR '.join(f'from:{acc}' for acc in self.TWITTER_SOURCES)
            r = requests.get(
                f"{self._twitter_api_url}/twitter/tweet/advanced_search",
                params={'query': from_filter, 'queryType': 'Latest'},
                headers=headers, timeout=10
            )
            if r.status_code != 200:
                return []

            data = r.json()
            tweets = data.get('tweets', [])
            for tweet in tweets[:10]:
                text = tweet.get('text', '')
                author = tweet.get('author', {}).get('userName', '')

                # Skip retweets and replies
                if text.startswith('RT @') or text.startswith('@'):
                    continue

                title = f"@{author}: {text[:200]}"
                n = NewsItem(title, 'twitter', time.time())
                if n.hash not in self._seen_hashes:
                    news.append(n)

        except Exception as e:
            logger.debug(f"Twitter API error: {e}")

        return news

    def analyze_news(self, news_item: NewsItem) -> NewsItem:
        """Use Claude Haiku to analyze news impact.
        Uses Anthropic SDK with streaming for faster first-token response.
        """
        if not self._api_key:
            return news_item

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)

            # Streaming — get first tokens in ~300ms for urgency detection
            full_text = ""
            with client.messages.stream(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": f"""You score crypto news for TRADING impact. Be STRICT — most news is noise.

ONLY high impact (7+):
- ETF approvals/rejections/large flows ($500M+)
- Exchange hacks, insolvencies, halted withdrawals
- Government bans, SEC lawsuits, major regulatory actions
- Protocol exploits losing $50M+
- Surprise rate decisions, emergency Fed actions

NOT high impact (1-4):
- Price milestones ("BTC hits $X") — we already see the price
- Educational content, explainers, opinions, predictions
- Routine earnings, scheduled events (already priced in)
- Non-English content unless from official account
- Old news being re-shared or commented on

Reply ONLY JSON:
{{
  "impact": 1-10,
  "direction": "BULLISH" or "BEARISH" or "NEUTRAL",
  "coins": ["BTC"],
  "urgency": "IMMEDIATE" or "GRADUAL" or "NONE",
  "summary": "one sentence"
}}

News: "{news_item.title}"
"""
                }]
            ) as stream:
                for text in stream.text_stream:
                    full_text += text

            # Parse JSON
            start = full_text.find('{')
            end = full_text.rfind('}') + 1
            if start >= 0 and end > start:
                data = json.loads(full_text[start:end])
                news_item.impact_score = int(data.get('impact', 0))
                news_item.direction = data.get('direction', 'NEUTRAL')
                news_item.affected_coins = data.get('coins', [])
                news_item.analyzed = True
                self._save_analysis(news_item, data.get('summary', ''))

        except Exception as e:
            logger.debug(f"News analysis failed: {e}")

        return news_item

    def _save_analysis(self, news: NewsItem, summary: str):
        """Save analyzed news to DB."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""INSERT OR REPLACE INTO news_reactions
                (hash, title, source, impact_score, direction, affected_coins, timestamp, analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (news.hash, news.title, news.source, news.impact_score,
                 news.direction, json.dumps(news.affected_coins),
                 datetime.now(timezone.utc).isoformat(),
                 datetime.now(timezone.utc).isoformat()))
            conn.commit()
            conn.close()
            self._seen_hashes.add(news.hash)
        except Exception:
            pass

    def check_for_signals(self) -> Optional[Dict]:
        """Main method: fetch, analyze, return signal if high impact.

        Returns:
            None if no actionable news
            Dict with: direction, affected_coins, impact, urgency, reason
        """
        # Fetch new news
        news = self.fetch_news()
        if not news:
            return None

        # Analyze each (Claude Haiku ~$0.01 per call)
        high_impact = None
        for item in news[:5]:  # max 5 per check to limit API cost
            analyzed = self.analyze_news(item)

            if analyzed.impact_score >= 7 and analyzed.direction != 'NEUTRAL':
                logger.info(f"HIGH IMPACT NEWS: [{analyzed.impact_score}] {analyzed.direction} "
                           f"— {analyzed.title[:60]}")
                high_impact = {
                    'direction': analyzed.direction,
                    'affected_coins': [c for c in analyzed.affected_coins if c in TRADING_COINS],
                    'impact': analyzed.impact_score,
                    'title': analyzed.title,
                    'all_coins': analyzed.impact_score >= 8,  # 8+ = whole market, 7 = specific coins only
                }

            elif analyzed.impact_score >= 5:
                logger.info(f"MEDIUM IMPACT: [{analyzed.impact_score}] {analyzed.direction} "
                           f"— {analyzed.title[:60]}")

        if high_impact:
            self._last_high_impact = high_impact
            self._last_high_impact['timestamp'] = time.time()

        return high_impact

    def get_market_sentiment(self) -> str:
        """Get overall sentiment from recent news (last 4h)."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            rows = conn.execute("""
                SELECT direction, impact_score FROM news_reactions
                WHERE analyzed_at > datetime('now', '-4 hours')
                AND impact_score >= 5
            """).fetchall()
            conn.close()

            if not rows:
                return 'NEUTRAL'

            bull_weight = sum(r[1] for r in rows if r[0] == 'BULLISH')
            bear_weight = sum(r[1] for r in rows if r[0] == 'BEARISH')

            if bull_weight > bear_weight + 5:
                return 'BULLISH'
            elif bear_weight > bull_weight + 5:
                return 'BEARISH'
            return 'NEUTRAL'

        except Exception:
            return 'NEUTRAL'

    def should_close_positions(self, tracked_positions: dict,
                              price_getter=None) -> List[str]:
        """Check if positions contradict high-impact news.

        Impact 7: close only LOSING positions that contradict news
        Impact 8+: close ALL positions that contradict news

        Args:
            tracked_positions: {coin: TrackedPosition}
            price_getter: callable(coin) -> float (for PnL check on impact 7)

        Returns list of coins that should be closed immediately.
        """
        if not self._last_high_impact:
            return []

        if time.time() - self._last_high_impact.get('timestamp', 0) > 600:
            return []

        close_coins = []
        news_dir = self._last_high_impact['direction']
        impact = self._last_high_impact.get('impact', 0)
        affected = set(self._last_high_impact.get('affected_coins', []))
        all_market = self._last_high_impact.get('all_coins', False)

        for coin, tracked in tracked_positions.items():
            contradicts = False
            if all_market or coin in affected or 'BTC' in affected:
                if news_dir == 'BEARISH' and tracked.direction == 'LONG':
                    contradicts = True
                elif news_dir == 'BULLISH' and tracked.direction == 'SHORT':
                    contradicts = True

            if not contradicts:
                continue

            if impact >= 8:
                # Impact 8+: close ALL contradicting positions
                close_coins.append(coin)
            elif impact == 7 and price_getter:
                # Impact 7: close only if position is LOSING
                price = price_getter(coin)
                if price > 0:
                    if tracked.direction == 'SHORT':
                        pnl = (tracked.entry_price - price) / tracked.entry_price
                    else:
                        pnl = (price - tracked.entry_price) / tracked.entry_price
                    if pnl < 0:
                        close_coins.append(coin)
                        logger.info(f"Impact 7: closing LOSING {coin} ({pnl*100:.1f}%)")
            elif impact == 7:
                # No price getter — close all contradicting (safe default)
                close_coins.append(coin)

        return close_coins


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    # Load env
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    reactor = NewsReactor()
    print("Fetching news...")
    signal = reactor.check_for_signals()

    if signal:
        print(f"\n🚨 HIGH IMPACT: {signal['direction']}")
        print(f"   Coins: {signal['affected_coins']}")
        print(f"   Impact: {signal['impact']}/10")
        print(f"   News: {signal['title']}")
    else:
        print("No high-impact news right now")

    sentiment = reactor.get_market_sentiment()
    print(f"\nOverall sentiment: {sentiment}")
