"""
Gemini Scanner — Layer 1 (Fast Eyes)
=====================================
Uses Gemini 3.1 Pro to scan 30+ coins quickly with charts.
Returns TOP 5 candidates for Opus deep analysis.

Does NOT make trading decisions — only filters and ranks.
Gemini gets full 97K knowledge base via cached system instruction.
"""

import os
import json
import time
import base64
import logging
import sqlite3
import requests
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

GEMINI_MODELS = ['gemini-3.1-pro-preview', 'gemini-2.5-pro', 'gemini-2.5-flash']
GEMINI_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models'


class GeminiScanner:
    """Fast scanner using Gemini 3.1 Pro vision."""

    def __init__(self, api_key: str = '', knowledge_text: str = ''):
        self._api_key = api_key or os.environ.get('GEMINI_API_KEY', '')
        self._knowledge = knowledge_text
        self._call_count = 0

    def scan_all(self, coins: list, regime: str = '', sl_history: dict = None,
                 open_positions: str = '', on_found: callable = None) -> list:
        """Scan ALL coins with 4H charts, return TOP candidates.

        Args:
            coins: list of coin symbols to scan
            regime: current market regime (BEAR/BULL/FLAT)
            sl_history: {coin: timestamp} of recent SL hits
            open_positions: string describing current open positions
            on_found: callback(opp) called immediately for each find

        Returns:
            list of candidates: [{coin, direction, confidence, reason}]
        """
        if not self._api_key:
            logger.warning("No GEMINI_API_KEY")
            return []

        # Generate 4H chart for each coin (fast — 1 chart per coin)
        charts = []
        try:
            from src.crypto.chart_generator import generate_chart
            for coin in coins:
                b = generate_chart(coin, '4h', 14)
                if b:
                    charts.append({'coin': coin, 'b64': base64.b64encode(b).decode()})
        except Exception as e:
            logger.error(f"Chart generation failed: {e}")
            return []

        # For coins without DB data, try live chart
        charted_coins = {c['coin'] for c in charts}
        for coin in coins:
            if coin not in charted_coins:
                try:
                    b = self._generate_live_chart(coin)
                    if b:
                        charts.append({'coin': coin, 'b64': base64.b64encode(b).decode()})
                except Exception:
                    pass

        if not charts:
            return []

        # Build per-coin data (ATR, momentum)
        coin_data = self._get_coin_data([c['coin'] for c in charts])

        # Scan in batches of 6
        all_candidates = []
        batch_size = 6

        for i in range(0, len(charts), batch_size):
            batch = charts[i:i + batch_size]

            parts = []
            coin_names = []

            for item in batch:
                coin = item['coin']
                coin_names.append(coin)

                # Chart image
                parts.append({'inlineData': {'mimeType': 'image/png', 'data': item['b64']}})

                # Per-coin data: ATR + momentum + funding + whale + news
                data = coin_data.get(coin, {})
                info_parts = [
                    f"{coin}: ATR={data.get('atr', 0):.1f}%",
                    f"4h={data.get('mom_4h', 0):+.1f}%",
                    f"24h={data.get('mom_24h', 0):+.1f}%",
                    f"funding={data.get('funding', 0):+.3f}%",
                    f"L/S={data.get('ls_ratio', 1):.2f}",
                ]
                if data.get('whale_alert'):
                    info_parts.append("WHALE ALERT!")
                if data.get('news'):
                    info_parts.append(f"NEWS: {data['news']}")
                parts.append({'text': ' | '.join(info_parts)})

            # Build prompt
            sl_text = ""
            if sl_history:
                sl_coins = [f"{c} (SL {int((time.time()-t)/60)}min ago)"
                           for c, t in sl_history.items() if time.time() - t < 7200]
                if sl_coins:
                    sl_text = f"\nAVOID these coins (recent SL): {', '.join(sl_coins)}"

            pos_text = f"\nYour current positions: {open_positions}" if open_positions else ""

            # Get Fear & Greed from first coin's data
            fg_val = next((d.get('fear_greed', 50) for d in coin_data.values()), 50)

            parts.append({'text': f"""REGIME: {regime} | Fear & Greed: {fg_val}
{sl_text}{pos_text}

Consider ALL data per coin: chart + ATR + momentum + funding rate + L/S ratio + whale alerts + news.
Positive funding = longs overleveraged (SHORT signal). Negative funding = shorts overleveraged (LONG signal).
Whale alert = large money moving, expect volatility.
L/S > 1.5 = crowded long (bearish). L/S < 0.67 = crowded short (bullish).

Scan these {len(coin_names)} coins. For EACH:
- In BEAR regime: only SHORT setups. In BULL: only LONG.
- Need confidence >= 60% to include.

Reply JSON array of ONLY the best candidates (can be empty):
[{{"coin": "X", "direction": "SHORT", "confidence": 0.75, "reason": "why in 1 sentence"}}]

Coins: {', '.join(coin_names)}"""})

            # Call Gemini
            result = self._call_gemini(parts)
            if not result:
                continue

            # Parse response
            try:
                start = result.find('[')
                end = result.rfind(']') + 1
                if start >= 0 and end > start:
                    candidates = json.loads(result[start:end])
                    for c in candidates:
                        # Validate
                        direction = c.get('direction', '')
                        if direction not in ('LONG', 'SHORT'):
                            continue
                        conf = float(c.get('confidence', 0))
                        if conf > 1.0:
                            conf = conf / 100.0
                        if conf < 0.6:
                            continue

                        c['confidence'] = conf
                        c['source'] = 'gemini'
                        all_candidates.append(c)

                        logger.info(f"GEMINI FOUND: {direction} {c['coin']} "
                                   f"({conf:.0%}) — {c.get('reason', '')[:60]}")

                        if on_found:
                            try:
                                on_found(c)
                            except Exception:
                                pass
            except Exception as e:
                logger.debug(f"Gemini parse error: {e}")

        logger.info(f"GEMINI scan: {len(all_candidates)} candidates from {len(charts)} coins")
        return all_candidates

    def _call_gemini(self, parts: list) -> str:
        """Call Gemini 3.1 Pro API with images + text."""
        try:
            payload = {
                'systemInstruction': {
                    'parts': [{'text': (
                        "You are a crypto futures trader scanning charts for opportunities. "
                        "You analyze 4H candlestick charts with RSI, BB, MA20, MA50, volume. "
                        "Be SELECTIVE — only return coins with clear setups. "
                        "TP must be within 1× ATR. Never trade against the regime.\n\n"
                        f"YOUR KNOWLEDGE:\n{self._knowledge[:30000]}"
                    )}]
                },
                'contents': [{'parts': parts}],
                'generationConfig': {'maxOutputTokens': 1000}
            }

            # Try models in order: 3.1 Pro → 2.5 Pro → 2.5 Flash (fallback)
            for model in GEMINI_MODELS:
                url = f'{GEMINI_BASE_URL}/{model}:generateContent?key={self._api_key}'
                response = requests.post(url, json=payload, timeout=90)
                self._call_count += 1

                if response.status_code == 200:
                    data = response.json()
                    if 'candidates' in data and data['candidates']:
                        return data['candidates'][0]['content']['parts'][0]['text']
                elif response.status_code == 503:
                    logger.info(f"Gemini {model}: overloaded, trying next model...")
                    continue
                else:
                    logger.warning(f"Gemini {model}: {response.status_code}")
                    continue

                break  # success

        except Exception as e:
            logger.error(f"Gemini error: {e}")

        return ""

    def _get_coin_data(self, coins: list) -> dict:
        """Get ATR + momentum + funding + whale + news for each coin."""
        result = {}
        try:
            conn = sqlite3.connect(str(DB_PATH))

            # Global data (same for all coins)
            fg = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
            fear_greed = fg[0] if fg else 50

            # Recent whale activity (last 6h, >$5M)
            whale_coins = set()
            try:
                whales = conn.execute("""
                    SELECT symbol FROM whale_transactions
                    WHERE timestamp > strftime('%s', 'now', '-6 hours')
                    AND amount_usd > 5000000
                """).fetchall()
                whale_coins = {w[0] for w in whales if w[0]}
            except Exception:
                pass

            # Recent news sentiment
            news_sentiment = {}
            try:
                news = conn.execute("""
                    SELECT direction, affected_coins FROM news_reactions
                    WHERE analyzed_at > datetime('now', '-4 hours')
                    AND impact_score >= 7
                """).fetchall()
                for n in news:
                    try:
                        coins_affected = json.loads(n[1]) if n[1] else []
                        for c in coins_affected:
                            news_sentiment[c] = n[0]
                    except Exception:
                        pass
            except Exception:
                pass

            for coin in coins:
                rows = conn.execute(
                    "SELECT high, low, close FROM prices WHERE coin=? AND timeframe='4h' "
                    "ORDER BY timestamp DESC LIMIT 30", (coin,)
                ).fetchall()

                if len(rows) < 7:
                    continue

                atrs = [(r[0]-r[1])/r[2]*100 for r in rows if r[2] > 0]
                closes = [r[2] for r in rows]

                # Funding rate
                fr = conn.execute(
                    "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                    (coin,)
                ).fetchone()
                funding = fr[0] * 100 if fr and fr[0] else 0

                # Long/short ratio
                ls = conn.execute(
                    "SELECT long_ratio FROM long_short_ratio WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                    (coin,)
                ).fetchone()
                ls_ratio = float(ls[0]) if ls and ls[0] else 1.0

                result[coin] = {
                    'atr': np.mean(atrs) if atrs else 0,
                    'mom_4h': (closes[0]/closes[1] - 1) * 100 if len(closes) >= 2 else 0,
                    'mom_24h': (closes[0]/closes[6] - 1) * 100 if len(closes) >= 7 else 0,
                    'funding': funding,
                    'ls_ratio': ls_ratio,
                    'whale_alert': coin in whale_coins or coin.upper() in whale_coins,
                    'news': news_sentiment.get(coin, ''),
                    'fear_greed': fear_greed,
                }
            conn.close()
        except Exception:
            pass
        return result

    def _generate_live_chart(self, coin: str) -> bytes:
        """Generate chart from live Bybit data (for coins not in DB)."""
        try:
            import ccxt
            exchange = ccxt.bybit()
            ohlcv = exchange.fetch_ohlcv(f"{coin}/USDT:USDT", '4h', limit=50)
            if len(ohlcv) < 20:
                return b''

            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import io
            from datetime import datetime

            dates = [datetime.fromtimestamp(c[0]/1000) for c in ohlcv]
            opens = [c[1] for c in ohlcv]
            highs = [c[2] for c in ohlcv]
            lows = [c[3] for c in ohlcv]
            closes = [c[4] for c in ohlcv]
            volumes = [c[5] for c in ohlcv]

            fig, ax = plt.subplots(1, 1, figsize=(10, 4), facecolor='#0D1117')
            ax.set_facecolor('#0D1117')
            ax.tick_params(colors='#8B949E')
            ax.grid(True, color='#1C2333', alpha=0.5)

            for j in range(len(dates)):
                color = '#00FF88' if closes[j] >= opens[j] else '#FF4444'
                ax.plot([dates[j]]*2, [lows[j], highs[j]], color=color, linewidth=0.5)
                ax.plot([dates[j]]*2, [opens[j], closes[j]], color=color, linewidth=2)

            ax.set_title(f'{coin}/USDT 4H', color='#E6EDF3', fontsize=12)
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=80, facecolor='#0D1117')
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception:
            return b''


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    # Load knowledge
    knowledge = ""
    for f in ['profi_knowledge.md', 'profi_coin_knowledge.md']:
        p = Path(__file__).parent / f
        if p.exists():
            knowledge += p.read_text() + "\n"

    scanner = GeminiScanner(knowledge_text=knowledge)
    coins = ['ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'LINK', 'DOGE', 'AVAX']

    print(f"Scanning {len(coins)} coins...")
    results = scanner.scan_all(coins, regime='BEAR')
    print(f"\nResults: {len(results)} candidates")
    for r in results:
        print(f"  {r['direction']} {r['coin']} ({r['confidence']:.0%}) — {r.get('reason', '')[:60]}")
