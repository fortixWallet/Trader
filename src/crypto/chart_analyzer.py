"""
FORTIX Chart Analyzer — Visual Pattern Recognition via Gemini
=============================================================

Generates chart images and sends to Google Gemini for visual analysis.
Returns: pattern name, trend direction, key levels, confidence.

This is an INDEPENDENT signal source — not correlated with numerical features.
Ensemble of visual + numerical should outperform either alone.
"""

import os
import io
import base64
import sqlite3
import logging
import requests
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
CHARTS_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'charts'
CHARTS_DIR.mkdir(exist_ok=True)


def _get_api_key():
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        env_path = _FACTORY_DIR / '.env'
        if env_path.exists():
            for line in open(env_path):
                if line.startswith('GEMINI_API_KEY='):
                    key = line.strip().split('=', 1)[1]
    return key


def generate_chart(coin: str, timeframe: str = '1d', days: int = 60) -> bytes:
    """Generate a chart image as PNG bytes."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe=? ORDER BY timestamp DESC LIMIT ?",
        (coin, timeframe, days)
    ).fetchall()
    conn.close()

    if len(rows) < 20:
        return b''

    rows = rows[::-1]  # chronological
    dates = [datetime.fromtimestamp(r[0]) for r in rows]
    opens = [r[1] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    closes = [r[4] for r in rows]
    volumes = [r[5] for r in rows]

    # Compute indicators
    closes_arr = np.array(closes)
    ma20 = np.convolve(closes_arr, np.ones(20)/20, mode='valid')
    ma50 = np.convolve(closes_arr, np.ones(min(50, len(closes_arr)))/min(50, len(closes_arr)), mode='valid')

    # Bollinger Bands
    bb_period = 20
    bb_ma = np.array([np.mean(closes_arr[max(0,i-bb_period):i+1]) for i in range(len(closes_arr))])
    bb_std = np.array([np.std(closes_arr[max(0,i-bb_period):i+1]) for i in range(len(closes_arr))])
    bb_upper = bb_ma + 2 * bb_std
    bb_lower = bb_ma - 2 * bb_std

    # RSI
    deltas = np.diff(closes_arr)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.zeros_like(deltas)
    avg_loss = np.zeros_like(deltas)
    avg_gain[13] = np.mean(gains[:14])
    avg_loss[13] = np.mean(losses[:14])
    for i in range(14, len(deltas)):
        avg_gain[i] = (avg_gain[i-1] * 13 + gains[i]) / 14
        avg_loss[i] = (avg_loss[i-1] * 13 + losses[i]) / 14
    rs = avg_gain / (avg_loss + 1e-10)
    rsi = 100 - 100 / (1 + rs)

    # Create chart
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 8),
                                          gridspec_kw={'height_ratios': [3, 1, 1]},
                                          facecolor='#0D1117')

    for ax in [ax1, ax2, ax3]:
        ax.set_facecolor('#0D1117')
        ax.tick_params(colors='#8B949E')
        ax.spines['bottom'].set_color('#1C2333')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#1C2333')
        ax.grid(True, color='#1C2333', alpha=0.5)

    # Price + BB + MA
    ax1.plot(dates, closes, color='#E6EDF3', linewidth=1.5, label='Price')
    ax1.plot(dates, bb_upper, color='#3B82F6', linewidth=0.7, alpha=0.5)
    ax1.plot(dates, bb_lower, color='#3B82F6', linewidth=0.7, alpha=0.5)
    ax1.fill_between(dates, bb_upper, bb_lower, alpha=0.05, color='#3B82F6')

    if len(ma20) > 0:
        ax1.plot(dates[len(dates)-len(ma20):], ma20, color='#FFD700', linewidth=0.8, label='MA20')
    if len(ma50) > 0:
        ax1.plot(dates[len(dates)-len(ma50):], ma50, color='#FF4444', linewidth=0.8, label='MA50')

    # Color candles
    for i in range(len(dates)):
        color = '#00FF88' if closes[i] >= opens[i] else '#FF4444'
        ax1.plot([dates[i], dates[i]], [lows[i], highs[i]], color=color, linewidth=0.5)
        ax1.plot([dates[i], dates[i]], [opens[i], closes[i]], color=color, linewidth=2)

    ax1.set_title(f'{coin}/USDT {timeframe.upper()} — {dates[-1].strftime("%Y-%m-%d")}',
                  color='#E6EDF3', fontsize=14)
    ax1.legend(loc='upper left', fontsize=8, facecolor='#0D1117', edgecolor='#1C2333',
               labelcolor='#8B949E')

    # Volume
    colors = ['#00FF88' if closes[i] >= opens[i] else '#FF4444' for i in range(len(dates))]
    ax2.bar(dates, volumes, color=colors, alpha=0.6, width=0.8)
    ax2.set_ylabel('Volume', color='#8B949E', fontsize=8)

    # RSI
    rsi_dates = dates[1:]
    ax3.plot(rsi_dates, rsi, color='#9B59B6', linewidth=1)
    ax3.axhline(70, color='#FF4444', linewidth=0.5, linestyle='--', alpha=0.5)
    ax3.axhline(30, color='#00FF88', linewidth=0.5, linestyle='--', alpha=0.5)
    ax3.fill_between(rsi_dates, rsi, 70, where=rsi > 70, color='#FF4444', alpha=0.1)
    ax3.fill_between(rsi_dates, rsi, 30, where=rsi < 30, color='#00FF88', alpha=0.1)
    ax3.set_ylabel('RSI', color='#8B949E', fontsize=8)
    ax3.set_ylim(0, 100)

    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#0D1117')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def analyze_chart(coin: str, timeframe: str = '1d') -> dict:
    """Generate chart and analyze with Gemini Vision.

    Returns:
        {
            'coin': str,
            'pattern': str (e.g. 'ascending_triangle', 'head_and_shoulders'),
            'trend': str ('bullish', 'bearish', 'sideways'),
            'confidence': float (0-1),
            'key_levels': {'support': float, 'resistance': float},
            'recommendation': str ('BUY', 'SELL', 'HOLD'),
            'reasoning': str,
            'has_data': bool,
        }
    """
    api_key = _get_api_key()
    if not api_key:
        return {'coin': coin, 'has_data': False, 'error': 'No Gemini API key'}

    # Generate chart
    chart_bytes = generate_chart(coin, timeframe, days=60)
    if not chart_bytes:
        return {'coin': coin, 'has_data': False, 'error': 'Not enough price data'}

    # Save chart for debugging
    chart_path = CHARTS_DIR / f'{coin}_{timeframe}.png'
    chart_path.write_bytes(chart_bytes)

    # Send to Gemini
    chart_b64 = base64.b64encode(chart_bytes).decode()

    prompt = f"""Crypto chart {coin}/USDT {timeframe}. Green candles=up, red=down. Blue=Bollinger Bands. Yellow=MA20, Red=MA50. Bottom: Volume + RSI.

Reply ONLY with JSON, no markdown:
{{"pattern":"name","trend":"bullish/bearish/sideways","recommendation":"BUY/SELL/HOLD","confidence":1-10,"support":0,"resistance":0,"reasoning":"brief"}}"""

    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
            json={
                "contents": [{
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": "image/png", "data": chart_b64}}
                    ]
                }],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1000}
            },
            timeout=30
        )

        if resp.status_code != 200:
            return {'coin': coin, 'has_data': False, 'error': f'Gemini API {resp.status_code}'}

        data = resp.json()
        text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')

        # Parse JSON from response (Gemini wraps in ```json ... ```)
        import json, re
        # Strip markdown code blocks
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        json_start = text.find('{')
        json_end = text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            result = json.loads(text[json_start:json_end])
            return {
                'coin': coin,
                'pattern': result.get('pattern', 'unknown'),
                'trend': result.get('trend', 'sideways'),
                'trend_strength': result.get('trend_strength', 5),
                'confidence': result.get('confidence', 5) / 10,
                'key_levels': {
                    'support': result.get('support', 0),
                    'resistance': result.get('resistance', 0),
                },
                'recommendation': result.get('recommendation', 'HOLD'),
                'reasoning': result.get('reasoning', ''),
                'has_data': True,
            }

        # Fallback: try to extract key info from text
        trend = 'sideways'
        rec = 'HOLD'
        if 'bullish' in text.lower(): trend = 'bullish'
        elif 'bearish' in text.lower(): trend = 'bearish'
        if 'buy' in text.lower(): rec = 'BUY'
        elif 'sell' in text.lower(): rec = 'SELL'

        if trend != 'sideways' or rec != 'HOLD':
            return {
                'coin': coin, 'pattern': 'parsed_from_text', 'trend': trend,
                'trend_strength': 5, 'confidence': 0.5,
                'key_levels': {'support': 0, 'resistance': 0},
                'recommendation': rec, 'reasoning': text[:200],
                'has_data': True,
            }

        return {'coin': coin, 'has_data': False, 'error': 'Could not parse Gemini response'}

    except Exception as e:
        logger.error(f"Gemini analysis failed for {coin}: {e}")
        return {'coin': coin, 'has_data': False, 'error': str(e)}


def analyze_top_coins(coins: list = None, timeframe: str = '1d') -> list:
    """Analyze charts for multiple coins."""
    if coins is None:
        coins = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'DOT']

    results = []
    for coin in coins:
        result = analyze_chart(coin, timeframe)
        results.append(result)
        if result.get('has_data'):
            logger.info(f"Chart {coin}: {result['trend']} ({result['pattern']}) "
                       f"→ {result['recommendation']} (conf={result['confidence']:.0%})")

    return results


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    results = analyze_top_coins(['BTC', 'ETH', 'SOL'])
    for r in results:
        if r.get('has_data'):
            print(f"\n{r['coin']}: {r['trend']} — {r['pattern']}")
            print(f"  Recommendation: {r['recommendation']} (confidence: {r['confidence']:.0%})")
            print(f"  Support: ${r['key_levels']['support']}, Resistance: ${r['key_levels']['resistance']}")
            print(f"  Reasoning: {r['reasoning']}")
        else:
            print(f"\n{r['coin']}: {r.get('error', 'no data')}")
