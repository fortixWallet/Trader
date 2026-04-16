"""
Gemini Multi-Timeframe Analyzer
================================
Uses Gemini 2.5 Flash to analyze market from multiple perspectives:
- Weekly trend
- Daily pattern
- 4h entry timing
- Overall regime assessment

Cost: ~$0.01 per analysis, ~$2-5/day
"""

import os
import json
import logging
import requests
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

GEMINI_MODEL = 'gemini-2.5-flash'
GEMINI_URL = f'https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent'


class GeminiAnalyzer:
    def __init__(self):
        self._api_key = os.environ.get('GEMINI_API_KEY', '')
        self._last_analysis = {}
        self._last_time = 0

    def analyze_market(self) -> dict:
        """Full market analysis using price data (text-based, no images needed)."""
        if not self._api_key:
            return {'regime': 'UNKNOWN', 'confidence': 0}

        try:
            conn = sqlite3.connect(str(DB_PATH))

            # Gather multi-timeframe data
            # Weekly (last 4 weeks of daily)
            weekly = conn.execute("""
                SELECT date(timestamp,'unixepoch'), close FROM prices
                WHERE coin='BTC' AND timeframe='1d'
                ORDER BY timestamp DESC LIMIT 28
            """).fetchall()

            # Daily (last 7 days)
            daily = conn.execute("""
                SELECT date(timestamp,'unixepoch'), close FROM prices
                WHERE coin='BTC' AND timeframe='1d'
                ORDER BY timestamp DESC LIMIT 7
            """).fetchall()

            # 4h (last 24h = 6 candles)
            h4 = conn.execute("""
                SELECT datetime(timestamp,'unixepoch'), open, high, low, close
                FROM prices WHERE coin='BTC' AND timeframe='4h'
                ORDER BY timestamp DESC LIMIT 6
            """).fetchall()

            # Top movers last 24h
            movers = conn.execute("""
                SELECT p1.coin,
                       (p1.close - p2.close) / p2.close * 100 as change
                FROM prices p1
                JOIN prices p2 ON p1.coin = p2.coin
                WHERE p1.timeframe = '4h' AND p2.timeframe = '4h'
                AND p1.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h')
                AND p2.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h') - 86400
                ORDER BY change
            """).fetchall()

            # Fear & Greed
            fg = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
            fg_val = fg[0] if fg else 50

            conn.close()

            # Build prompt
            weekly_str = ', '.join([f"{r[0]}: ${r[1]:,.0f}" for r in weekly[:7]])
            daily_str = ', '.join([f"{r[0]}: ${r[1]:,.0f}" for r in daily])
            h4_str = '\n'.join([f"  {r[0]}: O=${r[1]:,.0f} H=${r[2]:,.0f} L=${r[3]:,.0f} C=${r[4]:,.0f}" for r in h4])
            top_gainers = ', '.join([f"{r[0]}({r[1]:+.1f}%)" for r in movers[-5:]])
            top_losers = ', '.join([f"{r[0]}({r[1]:+.1f}%)" for r in movers[:5]])

            prompt = f"""You are a crypto market analyst. Analyze this multi-timeframe data and provide trading regime.

WEEKLY BTC (last 7 days): {daily_str}
MONTHLY BTC (last 4 weeks): {weekly_str}

LAST 24H BTC (4h candles):
{h4_str}

Fear & Greed Index: {fg_val}
Top gainers 24h: {top_gainers}
Top losers 24h: {top_losers}

Reply ONLY with JSON:
{{
  "regime": "STRONG_BULL" or "BULL" or "NEUTRAL" or "BEAR" or "STRONG_BEAR",
  "confidence": 0.0-1.0,
  "trend_weekly": "UP" or "DOWN" or "SIDEWAYS",
  "trend_daily": "UP" or "DOWN" or "SIDEWAYS",
  "trend_4h": "UP" or "DOWN" or "SIDEWAYS",
  "key_levels": {{"support": price, "resistance": price}},
  "recommendation": "AGGRESSIVE_LONG" or "CAUTIOUS_LONG" or "WAIT" or "CAUTIOUS_SHORT" or "AGGRESSIVE_SHORT",
  "reason": "one sentence"
}}"""

            response = requests.post(
                f'{GEMINI_URL}?key={self._api_key}',
                json={'contents': [{'parts': [{'text': prompt}]}]},
                timeout=20
            )

            if response.status_code == 200:
                text = response.json()['candidates'][0]['content']['parts'][0]['text']
                start = text.find('{')
                end = text.rfind('}') + 1
                if start >= 0 and end > start:
                    result = json.loads(text[start:end])
                    self._last_analysis = result
                    self._last_time = datetime.now(timezone.utc).timestamp()
                    logger.info(f"Gemini: {result.get('regime')} ({result.get('confidence', 0):.0%}) — {result.get('reason', '')[:50]}")
                    return result

        except Exception as e:
            logger.debug(f"Gemini analysis: {e}")

        return self._last_analysis or {'regime': 'UNKNOWN', 'confidence': 0}

    def get_regime_signal(self) -> tuple:
        """Get regime and confidence from Gemini.
        Returns (regime: str, confidence: float)
        """
        analysis = self._last_analysis
        if not analysis or datetime.now(timezone.utc).timestamp() - self._last_time > 14400:
            analysis = self.analyze_market()

        regime_map = {
            'STRONG_BULL': 'BULL', 'BULL': 'BULL',
            'STRONG_BEAR': 'BEAR', 'BEAR': 'BEAR',
            'NEUTRAL': 'FLAT',
        }
        regime = regime_map.get(analysis.get('regime', 'NEUTRAL'), 'FLAT')
        confidence = float(analysis.get('confidence', 0.5))

        return regime, confidence


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    env_path = Path('/Users/williamstorm/Documents/Trading (OKX) 1h/.env')
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    analyzer = GeminiAnalyzer()
    result = analyzer.analyze_market()
    print(json.dumps(result, indent=2))
