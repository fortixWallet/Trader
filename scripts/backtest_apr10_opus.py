#!/usr/bin/env python3
"""
Backtest April 10, 2026 using REAL Opus API calls.
Scans every 2 hours (12 scans), simulates fills on 1h candles.
"""

import os
import sys
import json
import csv
import time
import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta

import anthropic

# Paths
BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / 'data' / 'crypto' / 'market.db'
OUT_DIR = BASE / 'data' / 'crypto' / 'backtest_apr10_opus'
OUT_DIR.mkdir(parents=True, exist_ok=True)

SRC = BASE / 'src' / 'crypto'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('bt_apr10')

# --- Config ---
START_EQUITY = 5000.0
LEVERAGE = 8
POS_SIZE_PCT = 0.10  # 10% of equity per position
FEE_ROUNDTRIP = 0.0004  # 0.04%
TIME_EXIT_BARS = 3  # exit after 3h if no SL/TP
MAX_PENDING = 8
CONF_THRESHOLD = 0.70
MODEL = "claude-opus-4-6"

# April 10 2026 UTC
DAY_START = int(datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc).timestamp())
DAY_END = int(datetime(2026, 4, 11, 0, 0, tzinfo=timezone.utc).timestamp())

TRACKED_COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'POL',
    'UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV',
    'ARB', 'OP',
    'FET', 'RENDER', 'TAO',
    'DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK',
]

# --- Load knowledge for system prompt ---
def load_knowledge():
    parts = []
    for fname in ['profi_knowledge.md', 'profi_coin_knowledge.md',
                   'profi_advanced_knowledge.md', 'profi_history_lessons.md',
                   'profi_lessons.md']:
        p = SRC / fname
        if p.exists():
            t = p.read_text()
            if t.strip():
                parts.append(t)
    return "\n\n".join(parts) if parts else "Expert crypto trader knowledge."

def build_system_prompt(knowledge):
    return [
        {
            "type": "text",
            "text": (
                "You are PROFI — a professional crypto futures trader who MAKES MONEY.\n"
                "You have 20 years experience and $1,000 capital on Bybit Demo.\n\n"
                "YOUR CORE BELIEF: There is ALWAYS a way to profit. Always.\n"
                "- Bear market? SHORT the weakest coins on dead cat bounces.\n"
                "- Bull market? LONG the leaders on pullbacks.\n"
                "- Sideways? Scalp the range edges. Mean reversion at BB bands.\n"
                "- Extreme fear? Buy the blood — oversold bounces are the easiest trades.\n"
                "- Extreme greed? Short the euphoria — overbought dumps are predictable.\n"
                "- Low volume? Tight scalps on coins that ARE moving.\n"
                "- One coin crashing? Short it. One coin pumping? Ride it.\n\n"
                "You are NOT a risk-averse analyst who says WAIT. You are a TRADER who finds the edge.\n"
                "Every scan, you MUST find at least 1 tradeable setup. If you can't find a great one,\n"
                "find a good one. If you can't find a good one, find a quick scalp.\n\n"
                "RULES:\n"
                "- Always set entry, TP, SL with R:R >= 1:1\n"
                "- Adjust leverage to match confidence (high conf = higher leverage)\n"
                "- Quick scalps (15-60min): 0.3-0.5% target, 10-15x leverage\n"
                "- Swing trades (4-24h): 1-3% target, 5-7x leverage\n"
                "- Use ALL tools available to confirm your thesis\n"
                "- You see charts — trust your pattern recognition\n\n"
                f"YOUR TRADING KNOWLEDGE:\n{knowledge}"
            ),
            "cache_control": {"type": "ephemeral"}
        }
    ]


# --- Data helpers ---
def get_1h_candles(conn, coin, ts_from, ts_to):
    """Get 1h candles for coin in [ts_from, ts_to)."""
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp < ? "
        "ORDER BY timestamp",
        (coin, ts_from, ts_to)
    ).fetchall()
    return [{'ts': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
             'close': r[4], 'volume': r[5]} for r in rows]


def get_4h_candles(conn, coin, ts_to, limit=42):
    """Get last N 4h candles ending at ts_to."""
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='4h' AND timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT ?",
        (coin, ts_to, limit)
    ).fetchall()
    return [{'ts': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
             'close': r[4], 'volume': r[5]} for r in rows]


def compute_atr_1h(candles_1h, n=14):
    """Compute ATR from 1h candles as fraction of price."""
    if len(candles_1h) < n + 1:
        return 0.02
    trs = []
    for i in range(1, len(candles_1h)):
        h, l, pc = candles_1h[i]['high'], candles_1h[i]['low'], candles_1h[i-1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    last_n = trs[-n:]
    price = candles_1h[-1]['close']
    return np.mean(last_n) / price if price > 0 else 0.02


def find_levels_from_candles(candles, current_price):
    """Compute S/R from candle list (already chronological)."""
    if len(candles) < 30:
        return [], []
    highs = np.array([c['high'] for c in candles])
    lows = np.array([c['low'] for c in candles])
    closes = np.array([c['close'] for c in candles])
    volumes = np.array([c['volume'] for c in candles])

    swing_levels = []
    window = 5
    for i in range(window, len(candles) - window):
        if highs[i] == max(highs[i-window:i+window+1]):
            swing_levels.append(('R', highs[i], volumes[i]))
        if lows[i] == min(lows[i-window:i+window+1]):
            swing_levels.append(('S', lows[i], volumes[i]))

    # Volume profile
    n_bins = 50
    price_range = max(highs) - min(lows)
    if price_range <= 0:
        price_range = current_price * 0.1
    bin_size = price_range / n_bins
    vol_profile = {}
    for i in range(len(candles)):
        idx = min(int((closes[i] - min(lows)) / bin_size), n_bins - 1)
        level = min(lows) + idx * bin_size + bin_size / 2
        vol_profile[level] = vol_profile.get(level, 0) + volumes[i]
    sorted_vp = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)

    level_scores = {}
    for t, price, vol in swing_levels:
        k = round(price, 6)
        if k not in level_scores:
            level_scores[k] = {'score': 0, 'type': t}
        level_scores[k]['score'] += 3
        if vol > np.mean(volumes):
            level_scores[k]['score'] += 1
    for price, vol in sorted_vp[:20]:
        k = round(price, 6)
        if k not in level_scores:
            t = 'R' if price > current_price else 'S'
            level_scores[k] = {'score': 0, 'type': t}
        level_scores[k]['score'] += 2

    # Merge nearby
    merged = {}
    for price in sorted(level_scores.keys()):
        found = False
        for ex in merged:
            if abs(price - ex) / ex < 0.003:
                merged[ex]['score'] += level_scores[price]['score']
                found = True
                break
        if not found:
            merged[price] = level_scores[price].copy()

    resistance = sorted([p for p in merged if p > current_price * 1.001],
                        key=lambda x: merged[x]['score'], reverse=True)[:5]
    support = sorted([p for p in merged if p < current_price * 0.999],
                     key=lambda x: merged[x]['score'], reverse=True)[:5]
    resistance.sort()
    support.sort(reverse=True)
    return resistance, support


def compute_regime(conn, scan_ts):
    """Compute regime at a specific point in time using _get_regime logic."""
    # BTC 12h momentum from 4h candles
    btc_4h = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 7",
        (scan_ts,)
    ).fetchall()
    btc_12h = (btc_4h[0][0] / btc_4h[3][0] - 1) * 100 if len(btc_4h) >= 4 else 0

    # Market breadth: latest 4h candle vs previous
    max_4h_ts = conn.execute(
        "SELECT MAX(timestamp) FROM prices WHERE timeframe='4h' AND timestamp <= ?",
        (scan_ts,)
    ).fetchone()[0]
    prev_4h_ts = max_4h_ts - 14400 if max_4h_ts else scan_ts - 14400

    breadth_rows = conn.execute(
        """SELECT p1.coin, p1.close, p2.close
           FROM prices p1 JOIN prices p2 ON p1.coin = p2.coin
           WHERE p1.timeframe='4h' AND p2.timeframe='4h'
           AND p1.timestamp = ? AND p2.timestamp = ?
           AND p1.coin != 'BTC'""",
        (max_4h_ts, prev_4h_ts)
    ).fetchall()

    if breadth_rows:
        up = sum(1 for r in breadth_rows if r[1] > r[2])
        total = len(breadth_rows)
        breadth = up / total if total > 0 else 0.5
    else:
        breadth = 0.5

    bull_score = 0
    bear_score = 0
    if breadth > 0.65: bull_score += 2
    elif breadth > 0.55: bull_score += 1
    if breadth < 0.35: bear_score += 2
    elif breadth < 0.45: bear_score += 1
    if btc_12h > 0.5: bull_score += 1
    if btc_12h < -0.5: bear_score += 1

    if bull_score >= 2:
        regime = 'BULL'
    elif bear_score >= 2:
        regime = 'BEAR'
    else:
        regime = 'FLAT'

    # Macro filter: BTC 7d
    btc_7d_rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 42",
        (scan_ts,)
    ).fetchall()
    btc_1d_rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 6",
        (scan_ts,)
    ).fetchall()

    if len(btc_7d_rows) >= 42 and len(btc_1d_rows) >= 6:
        macro_7d = (btc_7d_rows[0][0] / btc_7d_rows[-1][0] - 1) * 100
        change_1d = (btc_1d_rows[0][0] / btc_1d_rows[-1][0] - 1) * 100
        if macro_7d >= 3.0 and change_1d > -3.0 and regime == 'BEAR':
            regime = 'FLAT'
        elif macro_7d <= -3.0 and change_1d < 3.0 and regime == 'BULL':
            regime = 'FLAT'

    return regime, btc_12h, breadth


def get_macro_line(conn, scan_ts):
    """Build MACRO line from DB at scan_ts point in time."""
    parts = []

    # BTC 7d and 1d changes
    btc_7d = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 42", (scan_ts,)
    ).fetchall()
    btc_1d = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 6", (scan_ts,)
    ).fetchall()
    if len(btc_7d) >= 42:
        parts.append(f"BTC_7d={((btc_7d[0][0]/btc_7d[-1][0])-1)*100:+.1f}%")
    if len(btc_1d) >= 6:
        parts.append(f"BTC_1d={((btc_1d[0][0]/btc_1d[-1][0])-1)*100:+.1f}%")

    # F&G
    fg = conn.execute(
        "SELECT value FROM fear_greed WHERE date <= date(?, 'unixepoch') ORDER BY date DESC LIMIT 1",
        (scan_ts,)
    ).fetchone()
    if fg: parts.append(f"F&G={fg[0]}")

    # MVRV, SOPR, NUPL
    for metric in ['mvrv', 'sopr', 'nupl']:
        r = conn.execute(
            "SELECT value FROM cq_btc_onchain WHERE metric=? AND date <= date(?, 'unixepoch') "
            "ORDER BY date DESC LIMIT 1", (metric, scan_ts)
        ).fetchone()
        if r and r[0]:
            fmt = '.2f' if metric != 'sopr' else '.3f'
            parts.append(f"{metric.upper()}={r[0]:{fmt}}")

    # BTC funding
    fr = conn.execute(
        "SELECT rate FROM funding_rates WHERE coin='BTC' AND timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT 1", (scan_ts,)
    ).fetchone()
    if fr and fr[0] is not None:
        parts.append(f"BTC_funding={fr[0]*100:+.3f}%")

    # Coinbase premium
    cp = conn.execute(
        "SELECT premium_index FROM cq_coinbase_premium WHERE date <= date(?, 'unixepoch') "
        "ORDER BY date DESC LIMIT 1", (scan_ts,)
    ).fetchone()
    if cp and cp[0] is not None:
        parts.append(f"CB_premium={cp[0]:+.3f}%")

    return "MACRO: " + " | ".join(parts) if parts else ""


def get_btc_momentum(conn, scan_ts):
    """BTC momentum from recent 1h candles at scan_ts."""
    rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 4", (scan_ts,)
    ).fetchall()
    if len(rows) < 4:
        return "BTC FLAT: no data"
    now = rows[0][0]
    h1_ago = rows[3][0]
    h15m = rows[1][0]  # approximate 15m with prev hour
    chg_1h = (now - h1_ago) / h1_ago * 100
    chg_short = (now - h15m) / h15m * 100
    if chg_1h > 0.3:
        d = "RISING"
    elif chg_1h < -0.3:
        d = "FALLING"
    else:
        d = "FLAT"
    return f"BTC {d}: {chg_1h:+.1f}% (1h), {chg_short:+.1f}% (vs prev hr), ${now:.0f}"


def build_user_prompt(coins_data, regime, macro_line, btc_momentum, open_positions=""):
    """Build the user prompt exactly like find_level_setups."""
    parts = []

    if macro_line:
        parts.append(macro_line)

    parts.append(
        f"*** BTC MOMENTUM (85% correlated with alts): {btc_momentum} ***\n"
        f"RULE: When BTC RISING -> LONG BTC and alts. When BTC FALLING -> SHORT BTC and alts. "
        f"BTC is a TRADEABLE coin — include BTC setups! It has the best liquidity and lowest spread. "
        f"Verified on 1000+ data points: 85% accuracy."
    )

    for cd in coins_data:
        coin = cd['coin']
        r_str = ', '.join(f"${p:.4f}" for p in cd['resistance'][:3])
        s_str = ', '.join(f"${p:.4f}" for p in cd['support'][:3])
        data_parts = [f"LIVE=${cd['price']:.4f}", f"ATR_1h={cd['atr']*100:.2f}%",
                      f"R=[{r_str}]", f"S=[{s_str}]"]
        if cd.get('funding'):
            data_parts.append(f"funding={cd['funding']*100:.3f}%")
        parts.append(f"[{coin}: {' | '.join(data_parts)}]")

    parts.append(f"""REGIME: {regime}
{f"Open positions: {open_positions}" if open_positions else "No open positions."}

You are a 1-HOUR trader. Your job is to PREDICT the next move BEFORE it happens.

DATA YOU HAVE:
- ATR_1h = typical 1-hour price movement
- S/R levels from 1h candles
- Funding rate: crowded side will get squeezed

TWO ENTRY MODES:
1. AGGRESSIVE (momentum confirms direction): entry = LIVE price (fills immediately)
   Use when: BTC momentum strong. Price is MOVING — don't wait.
2. PATIENT (range, no clear direction): entry = nearest S/R level
   Use when: momentum flat. Wait for price to come to level.

SL RULES (from training):
  - SL = 0.8-1.0x ATR_1h (NEVER less). Each coin moves differently.
  - SL behind the S/R level where you entered. If level breaks = thesis wrong.

TP RULES (from training):
  - TP at next S/R level. MUST be reachable within 1-2 hours.
  - R:R target 1.5-1.8x (not higher — 2.0x+ rarely hits in time).

Leverage: 8-10x.

Reply JSON array (5-8 setups):
[{{
  "coin": "BNB",
  "direction": "LONG",
  "entry": 608.5,
  "sl": 606.7,
  "tp": 612.0,
  "leverage": 10,
  "confidence": 0.70,
  "reason": "near support, BTC rising, funding negative -> squeeze potential"
}}]""")

    return "\n\n".join(parts)


# --- Main backtest ---
def main():
    client = anthropic.Anthropic(
        api_key=os.environ['ANTHROPIC_API_KEY'],
        timeout=120.0,
        max_retries=3,
    )
    knowledge = load_knowledge()
    system = build_system_prompt(knowledge)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Pre-load ALL April 10 1h candles + lookback for S/R
    # Lookback: 100 bars before April 10 for level computation
    lookback_start = DAY_START - 100 * 3600  # 100 hours before
    all_candles = {}  # {coin: [candles]}
    for coin in TRACKED_COINS:
        all_candles[coin] = get_1h_candles(conn, coin, lookback_start, DAY_END)
        log.info(f"Loaded {len(all_candles[coin])} 1h candles for {coin}")

    # Get all April 10 candles for simulation
    apr10_candles = {}  # {coin: [candles on apr 10]}
    for coin in TRACKED_COINS:
        apr10_candles[coin] = get_1h_candles(conn, coin, DAY_START, DAY_END)

    # Track state
    equity = START_EQUITY
    trades = []
    pending_orders = []  # {coin, direction, entry, sl, tp, leverage, confidence, reason, scan_ts, expire_ts}
    open_positions = []  # {coin, direction, entry, sl, tp, leverage, size_usd, fill_ts, fill_bar_idx}
    total_input_tokens = 0
    total_output_tokens = 0
    api_calls = 0

    # Scan every 2 hours: 0, 2, 4, ..., 22 UTC
    scan_hours = list(range(0, 24, 2))

    for scan_hour in scan_hours:
        scan_ts = DAY_START + scan_hour * 3600
        scan_dt = datetime.fromtimestamp(scan_ts, tz=timezone.utc)
        log.info(f"\n{'='*60}")
        log.info(f"SCAN at {scan_dt.strftime('%Y-%m-%d %H:%M UTC')} | Equity: ${equity:.2f}")

        # Expire old pending orders (> 2h)
        pending_orders = [p for p in pending_orders if p['expire_ts'] > scan_ts]

        # Compute regime
        regime, btc_12h, breadth = compute_regime(conn, scan_ts)
        macro_line = get_macro_line(conn, scan_ts)
        btc_mom = get_btc_momentum(conn, scan_ts)
        log.info(f"Regime: {regime} | BTC 12h: {btc_12h:+.1f}% | Breadth: {breadth:.0%}")

        # Build coin data for prompt
        coins_data = []
        for coin in TRACKED_COINS:
            # Get candles up to scan_ts
            candles_up_to = [c for c in all_candles[coin] if c['ts'] <= scan_ts]
            if len(candles_up_to) < 30:
                continue
            current_price = candles_up_to[-1]['close']
            atr = compute_atr_1h(candles_up_to)
            resistance, support = find_levels_from_candles(candles_up_to[-100:], current_price)
            if not resistance and not support:
                continue

            # Funding rate
            fr = conn.execute(
                "SELECT rate FROM funding_rates WHERE coin=? AND timestamp <= ? "
                "ORDER BY timestamp DESC LIMIT 1", (coin, scan_ts)
            ).fetchone()
            funding = fr[0] if fr and fr[0] is not None else 0.0

            coins_data.append({
                'coin': coin, 'price': current_price, 'atr': atr,
                'resistance': resistance, 'support': support, 'funding': funding
            })

        if not coins_data:
            log.warning("No coin data available")
            continue

        # Open position context
        pos_str = ""
        if open_positions:
            parts = []
            for p in open_positions:
                c_candle = [c for c in apr10_candles[p['coin']] if c['ts'] == scan_ts]
                if c_candle:
                    px = c_candle[0]['close']
                    if p['direction'] == 'SHORT':
                        pnl = (p['entry'] - px) / p['entry'] * 100
                    else:
                        pnl = (px - p['entry']) / p['entry'] * 100
                    parts.append(f"{p['direction']} {p['coin']} ({pnl:+.1f}%)")
            pos_str = ", ".join(parts)

        # Skip coins already in open positions
        open_coins = {p['coin'] for p in open_positions}

        # Build prompt with up to 8 coins not in open positions
        prompt_coins = [cd for cd in coins_data if cd['coin'] not in open_coins][:8]
        if not prompt_coins:
            log.info("All coins in open positions, skipping scan")
            continue

        user_prompt = build_user_prompt(prompt_coins, regime, macro_line, btc_mom, pos_str)

        # Call Opus
        log.info(f"Calling Opus for {len(prompt_coins)} coins...")
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4000,
                system=system,
                messages=[{"role": "user", "content": user_prompt}],
            )
            api_calls += 1
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            cache_read = getattr(response.usage, 'cache_read_input_tokens', 0) or 0
            cache_create = getattr(response.usage, 'cache_creation_input_tokens', 0) or 0
            log.info(f"API call {api_calls}: in={response.usage.input_tokens} out={response.usage.output_tokens} "
                     f"cache_read={cache_read} cache_create={cache_create}")

            result_text = response.content[0].text if response.content else ""
        except Exception as e:
            log.error(f"API error: {e}")
            continue

        # Parse setups
        setups = []
        try:
            start_idx = result_text.find('[')
            end_idx = result_text.rfind(']') + 1
            if start_idx >= 0 and end_idx > start_idx:
                raw = json.loads(result_text[start_idx:end_idx])
                for s in raw:
                    direction = s.get('direction', '')
                    if direction not in ('LONG', 'SHORT'):
                        continue
                    conf = float(s.get('confidence', 0))
                    if conf > 1.0:
                        conf /= 100.0
                    if conf < CONF_THRESHOLD:
                        continue
                    s['confidence'] = conf
                    setups.append(s)
                    log.info(f"  SETUP: {direction} {s['coin']} entry=${s.get('entry',0):.4f} "
                             f"SL=${s.get('sl',0):.4f} TP=${s.get('tp',0):.4f} "
                             f"conf={conf:.0%} — {s.get('reason','')[:60]}")
            else:
                log.warning(f"No JSON array in response: {result_text[:200]}")
        except Exception as e:
            log.warning(f"Parse error: {e}")

        # Add to pending (respect MAX_PENDING)
        for s in setups:
            coin = s.get('coin', '')
            if coin not in [cd['coin'] for cd in coins_data]:
                continue
            if coin in open_coins:
                continue
            if len(pending_orders) >= MAX_PENDING:
                break
            pending_orders.append({
                'coin': coin,
                'direction': s['direction'],
                'entry': float(s.get('entry', 0)),
                'sl': float(s.get('sl', 0)),
                'tp': float(s.get('tp', 0)),
                'leverage': int(s.get('leverage', LEVERAGE)),
                'confidence': s['confidence'],
                'reason': s.get('reason', ''),
                'scan_ts': scan_ts,
                'expire_ts': scan_ts + 2 * 3600,  # expire in 2h
            })

        log.info(f"Pending orders: {len(pending_orders)} | Open positions: {len(open_positions)}")

        # --- Simulate fills and exits for the next 2 hours (until next scan) ---
        next_scan_ts = scan_ts + 2 * 3600
        sim_hours = [scan_ts + h * 3600 for h in range(0, 2)]

        for bar_ts in sim_hours:
            # Check pending orders for fills
            filled_indices = []
            for i, order in enumerate(pending_orders):
                coin = order['coin']
                candle = next((c for c in apr10_candles[coin] if c['ts'] == bar_ts), None)
                if not candle:
                    continue

                entry = order['entry']
                # Check if price reached entry during this candle
                hit = False
                if order['direction'] == 'LONG':
                    # Price must go down to entry (or entry is at/above open for aggressive)
                    if candle['low'] <= entry <= candle['high']:
                        hit = True
                    elif entry >= candle['open']:  # aggressive entry at market
                        hit = True
                        entry = candle['open']  # fill at open
                else:  # SHORT
                    if candle['low'] <= entry <= candle['high']:
                        hit = True
                    elif entry <= candle['open']:  # aggressive entry at market
                        hit = True
                        entry = candle['open']

                if hit and coin not in {p['coin'] for p in open_positions}:
                    size_usd = equity * POS_SIZE_PCT * order['leverage']
                    open_positions.append({
                        'coin': coin,
                        'direction': order['direction'],
                        'entry': entry,
                        'sl': order['sl'],
                        'tp': order['tp'],
                        'leverage': order['leverage'],
                        'confidence': order['confidence'],
                        'reason': order['reason'],
                        'size_usd': size_usd,
                        'fill_ts': bar_ts,
                        'bars_held': 0,
                    })
                    filled_indices.append(i)
                    log.info(f"  FILLED: {order['direction']} {coin} @ ${entry:.4f} "
                             f"size=${size_usd:.0f}")

            # Remove filled from pending
            for i in sorted(filled_indices, reverse=True):
                pending_orders.pop(i)

            # Check open positions for SL/TP/TIME_EXIT
            closed_indices = []
            for i, pos in enumerate(open_positions):
                coin = pos['coin']
                candle = next((c for c in apr10_candles[coin] if c['ts'] == bar_ts), None)
                if not candle:
                    continue

                pos['bars_held'] += 1
                exit_price = None
                exit_reason = None

                if pos['direction'] == 'LONG':
                    # Check SL first (worst case)
                    if candle['low'] <= pos['sl']:
                        exit_price = pos['sl']
                        exit_reason = 'SL'
                    elif candle['high'] >= pos['tp']:
                        exit_price = pos['tp']
                        exit_reason = 'TP'
                    elif pos['bars_held'] >= TIME_EXIT_BARS:
                        exit_price = candle['close']
                        exit_reason = 'TIME'
                else:  # SHORT
                    if candle['high'] >= pos['sl']:
                        exit_price = pos['sl']
                        exit_reason = 'SL'
                    elif candle['low'] <= pos['tp']:
                        exit_price = pos['tp']
                        exit_reason = 'TP'
                    elif pos['bars_held'] >= TIME_EXIT_BARS:
                        exit_price = candle['close']
                        exit_reason = 'TIME'

                if exit_price is not None:
                    if pos['direction'] == 'LONG':
                        pnl_pct = (exit_price - pos['entry']) / pos['entry']
                    else:
                        pnl_pct = (pos['entry'] - exit_price) / pos['entry']

                    pnl_pct_lev = pnl_pct * pos['leverage']
                    fee = FEE_ROUNDTRIP * pos['leverage']
                    net_pnl_pct = pnl_pct_lev - fee
                    pnl_usd = (equity * POS_SIZE_PCT) * net_pnl_pct

                    equity += pnl_usd

                    trade = {
                        'coin': coin,
                        'direction': pos['direction'],
                        'entry': pos['entry'],
                        'exit': exit_price,
                        'sl': pos['sl'],
                        'tp': pos['tp'],
                        'exit_reason': exit_reason,
                        'pnl_pct': round(pnl_pct * 100, 3),
                        'pnl_pct_lev': round(pnl_pct_lev * 100, 3),
                        'net_pnl_usd': round(pnl_usd, 2),
                        'equity_after': round(equity, 2),
                        'confidence': pos['confidence'],
                        'reason': pos['reason'],
                        'fill_time': datetime.fromtimestamp(pos['fill_ts'], tz=timezone.utc).strftime('%H:%M'),
                        'exit_time': datetime.fromtimestamp(bar_ts, tz=timezone.utc).strftime('%H:%M'),
                        'bars_held': pos['bars_held'],
                        'leverage': pos['leverage'],
                    }
                    trades.append(trade)
                    closed_indices.append(i)
                    log.info(f"  CLOSED: {pos['direction']} {coin} @ ${exit_price:.4f} "
                             f"({exit_reason}) PnL={net_pnl_pct*100:+.2f}% (${pnl_usd:+.2f}) "
                             f"Equity=${equity:.2f}")

            for i in sorted(closed_indices, reverse=True):
                open_positions.pop(i)

    # --- Force-close any remaining positions at end of day ---
    for pos in open_positions:
        coin = pos['coin']
        last_candle = apr10_candles[coin][-1] if apr10_candles[coin] else None
        if not last_candle:
            continue
        exit_price = last_candle['close']
        if pos['direction'] == 'LONG':
            pnl_pct = (exit_price - pos['entry']) / pos['entry']
        else:
            pnl_pct = (pos['entry'] - exit_price) / pos['entry']

        pnl_pct_lev = pnl_pct * pos['leverage']
        fee = FEE_ROUNDTRIP * pos['leverage']
        net_pnl_pct = pnl_pct_lev - fee
        pnl_usd = (equity * POS_SIZE_PCT) * net_pnl_pct
        equity += pnl_usd

        trade = {
            'coin': coin,
            'direction': pos['direction'],
            'entry': pos['entry'],
            'exit': exit_price,
            'sl': pos['sl'],
            'tp': pos['tp'],
            'exit_reason': 'EOD',
            'pnl_pct': round(pnl_pct * 100, 3),
            'pnl_pct_lev': round(pnl_pct_lev * 100, 3),
            'net_pnl_usd': round(pnl_usd, 2),
            'equity_after': round(equity, 2),
            'confidence': pos['confidence'],
            'reason': pos['reason'],
            'fill_time': datetime.fromtimestamp(pos['fill_ts'], tz=timezone.utc).strftime('%H:%M'),
            'exit_time': '23:00',
            'bars_held': pos['bars_held'],
            'leverage': pos['leverage'],
        }
        trades.append(trade)
        log.info(f"  EOD CLOSE: {pos['direction']} {coin} @ ${exit_price:.4f} "
                 f"PnL={net_pnl_pct*100:+.2f}% (${pnl_usd:+.2f})")

    conn.close()

    # --- Save results ---
    # trades.csv
    csv_path = OUT_DIR / 'trades.csv'
    if trades:
        with open(csv_path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=trades[0].keys())
            w.writeheader()
            w.writerows(trades)
        log.info(f"Saved {len(trades)} trades to {csv_path}")
    else:
        log.info("No trades executed")

    # summary.md
    total_pnl = sum(t['net_pnl_usd'] for t in trades)
    wins = [t for t in trades if t['net_pnl_usd'] > 0]
    losses = [t for t in trades if t['net_pnl_usd'] <= 0]
    wr = len(wins) / len(trades) * 100 if trades else 0

    by_coin = {}
    for t in trades:
        c = t['coin']
        if c not in by_coin:
            by_coin[c] = {'trades': 0, 'pnl': 0, 'wins': 0}
        by_coin[c]['trades'] += 1
        by_coin[c]['pnl'] += t['net_pnl_usd']
        if t['net_pnl_usd'] > 0:
            by_coin[c]['wins'] += 1

    by_exit = {}
    for t in trades:
        r = t['exit_reason']
        if r not in by_exit:
            by_exit[r] = {'count': 0, 'pnl': 0}
        by_exit[r]['count'] += 1
        by_exit[r]['pnl'] += t['net_pnl_usd']

    long_trades = [t for t in trades if t['direction'] == 'LONG']
    short_trades = [t for t in trades if t['direction'] == 'SHORT']
    long_wr = len([t for t in long_trades if t['net_pnl_usd'] > 0]) / len(long_trades) * 100 if long_trades else 0
    short_wr = len([t for t in short_trades if t['net_pnl_usd'] > 0]) / len(short_trades) * 100 if short_trades else 0

    best = max(trades, key=lambda t: t['net_pnl_usd']) if trades else None
    worst = min(trades, key=lambda t: t['net_pnl_usd']) if trades else None

    summary = f"""# Backtest: April 10, 2026 — Opus Live Decisions

## Overview
- **Start equity**: ${START_EQUITY:,.2f}
- **End equity**: ${equity:,.2f}
- **Net PnL**: ${total_pnl:+,.2f} ({total_pnl/START_EQUITY*100:+.2f}%)
- **Total trades**: {len(trades)}
- **Win rate**: {wr:.1f}% ({len(wins)}W / {len(losses)}L)
- **Leverage**: {LEVERAGE}x | Position size: {POS_SIZE_PCT*100:.0f}%
- **Scans**: {len(scan_hours)} (every 2h) | API calls: {api_calls}
- **Tokens**: {total_input_tokens:,} in / {total_output_tokens:,} out
- **Est. cost**: ${(total_input_tokens * 15 + total_output_tokens * 75) / 1e6:.2f}

## Direction Accuracy
| Direction | Trades | Win Rate | PnL |
|-----------|--------|----------|-----|
| LONG | {len(long_trades)} | {long_wr:.0f}% | ${sum(t['net_pnl_usd'] for t in long_trades):+.2f} |
| SHORT | {len(short_trades)} | {short_wr:.0f}% | ${sum(t['net_pnl_usd'] for t in short_trades):+.2f} |

## Exit Type Breakdown
| Exit | Count | PnL |
|------|-------|-----|
"""
    for r, d in sorted(by_exit.items()):
        summary += f"| {r} | {d['count']} | ${d['pnl']:+.2f} |\n"

    summary += "\n## Per-Coin Breakdown\n| Coin | Trades | Wins | PnL |\n|------|--------|------|-----|\n"
    for c, d in sorted(by_coin.items(), key=lambda x: x[1]['pnl'], reverse=True):
        summary += f"| {c} | {d['trades']} | {d['wins']} | ${d['pnl']:+.2f} |\n"

    if best:
        summary += f"\n## Best Trade\n{best['direction']} {best['coin']} — ${best['net_pnl_usd']:+.2f} "
        summary += f"({best['exit_reason']}) — \"{best['reason']}\"\n"
    if worst:
        summary += f"\n## Worst Trade\n{worst['direction']} {worst['coin']} — ${worst['net_pnl_usd']:+.2f} "
        summary += f"({worst['exit_reason']}) — \"{worst['reason']}\"\n"

    summary_path = OUT_DIR / 'summary.md'
    summary_path.write_text(summary)
    log.info(f"Saved summary to {summary_path}")

    # Print summary
    print("\n" + "=" * 60)
    print(summary)


if __name__ == '__main__':
    main()
