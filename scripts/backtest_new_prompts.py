#!/usr/bin/env python3
"""
Backtest NEW prompt system on April 17, 2026 using REAL Opus API calls.
Scans every 2 hours, simulates fills on 1h candles.
Compares with actual OKX results from fortix_trades.
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
from dotenv import load_dotenv

import anthropic

# Paths
BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / 'data' / 'crypto' / 'market.db'
OUT_DIR = BASE / 'data' / 'crypto' / 'new_prompt_backtest'
OUT_DIR.mkdir(parents=True, exist_ok=True)
SRC = BASE / 'src' / 'crypto'

# Load env
load_dotenv(BASE / '.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('bt_new_prompt')

# --- Config ---
START_EQUITY = 5000.0
LEVERAGE = 8
POS_SIZE_PCT = 0.10  # 10% of equity per position
FEE_ROUNDTRIP = 0.0004  # 0.04%
TIME_EXIT_BARS = 3  # exit after 3h
MODEL = "claude-opus-4-6"

# Apr 17 2026 UTC — data available 00:00-16:00
DAY_START = int(datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc).timestamp())
DAY_END = int(datetime(2026, 4, 17, 17, 0, tzinfo=timezone.utc).timestamp())  # last candle at 16:00

# Fixed TP/SL in ROI terms
SL_ROI = -0.065   # -6.5% ROI
TP_ROI = 0.13     # +13% ROI
TRAILING_ACTIVATE_ROI = 0.06   # trailing activates at +6% ROI
TRAILING_DROP_ROI = 0.02       # trailing triggers if drops 2% from peak

TRACKED_COINS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'POL',
    'UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV',
    'ARB', 'OP',
    'FET', 'RENDER', 'TAO',
    'DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK',
]


# --- Knowledge ---
def load_knowledge(max_chars=5000):
    parts = []
    for fname in ['profi_knowledge.md', 'profi_coin_knowledge.md',
                   'profi_advanced_knowledge.md', 'profi_history_lessons.md',
                   'profi_lessons.md']:
        p = SRC / fname
        if p.exists():
            t = p.read_text()[:max_chars]
            if t.strip():
                parts.append(t)
    return "\n\n".join(parts) if parts else "Expert crypto trader knowledge."


def build_system_prompt(knowledge):
    """NEW prompt system."""
    return [
        {
            "type": "text",
            "text": (
                "You are PROFI — a professional crypto futures trader.\n"
                "You make money by being RIGHT about direction, not by trading often.\n\n"
                "CORE PRINCIPLES:\n"
                "- SKIP is your best trade when uncertain. Empty array [] is a valid response.\n"
                "- Quality over quantity. 3 perfect setups > 8 mediocre ones.\n"
                "- MACRO drives direction. Per-coin data confirms entry.\n"
                "- OB shows WHERE walls are (for entry placement), NOT direction. Walls can be spoofed.\n"
                "- Past SL on a coin TODAY = avoid that coin and level.\n\n"
                f"YOUR TRADING KNOWLEDGE:\n{knowledge}"
            ),
            "cache_control": {"type": "ephemeral"}
        }
    ]


# --- Data helpers (reused from backtest_apr10_opus.py) ---
def get_1h_candles(conn, coin, ts_from, ts_to):
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close, volume FROM prices "
        "WHERE coin=? AND timeframe='1h' AND timestamp >= ? AND timestamp < ? "
        "ORDER BY timestamp",
        (coin, ts_from, ts_to)
    ).fetchall()
    return [{'ts': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
             'close': r[4], 'volume': r[5]} for r in rows]


def compute_atr_1h(candles_1h, n=14):
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
    btc_4h = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 7", (scan_ts,)
    ).fetchall()
    btc_12h = (btc_4h[0][0] / btc_4h[3][0] - 1) * 100 if len(btc_4h) >= 4 else 0

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

    btc_7d_rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 42", (scan_ts,)
    ).fetchall()
    btc_1d_rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 6", (scan_ts,)
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
    parts = []
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

    fg = conn.execute(
        "SELECT value FROM fear_greed WHERE date <= date(?, 'unixepoch') ORDER BY date DESC LIMIT 1",
        (scan_ts,)
    ).fetchone()
    if fg: parts.append(f"F&G={fg[0]}")

    for metric in ['mvrv', 'sopr', 'nupl']:
        r = conn.execute(
            "SELECT value FROM cq_btc_onchain WHERE metric=? AND date <= date(?, 'unixepoch') "
            "ORDER BY date DESC LIMIT 1", (metric, scan_ts)
        ).fetchone()
        if r and r[0]:
            fmt = '.2f' if metric != 'sopr' else '.3f'
            parts.append(f"{metric.upper()}={r[0]:{fmt}}")

    fr = conn.execute(
        "SELECT rate FROM funding_rates WHERE coin='BTC' AND timestamp <= ? "
        "ORDER BY timestamp DESC LIMIT 1", (scan_ts,)
    ).fetchone()
    if fr and fr[0] is not None:
        parts.append(f"BTC_funding={fr[0]*100:+.3f}%")

    cp = conn.execute(
        "SELECT premium_index FROM cq_coinbase_premium WHERE date <= date(?, 'unixepoch') "
        "ORDER BY date DESC LIMIT 1", (scan_ts,)
    ).fetchone()
    if cp and cp[0] is not None:
        parts.append(f"CB_premium={cp[0]:+.3f}%")

    return "MACRO: " + " | ".join(parts) if parts else ""


def get_btc_momentum(conn, scan_ts):
    rows = conn.execute(
        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 4", (scan_ts,)
    ).fetchall()
    if len(rows) < 4:
        return "BTC FLAT: no data"
    now = rows[0][0]
    h1_ago = rows[3][0]
    chg_1h = (now - h1_ago) / h1_ago * 100
    if chg_1h > 0.3:
        d = "RISING"
    elif chg_1h < -0.3:
        d = "FALLING"
    else:
        d = "FLAT"
    return f"BTC {d}: {chg_1h:+.1f}% (1h), ${now:.0f}"


def build_user_prompt(coins_data, regime, macro_line, btc_momentum, banned_coins, open_positions_str=""):
    """NEW scan prompt."""
    parts = []

    if macro_line:
        parts.append(macro_line)

    parts.append(f"BTC context: {btc_momentum}. Note: current momentum, not prediction.")

    for cd in coins_data:
        coin = cd['coin']
        r_str = ', '.join(f"${p:.4f}" for p in cd['resistance'][:3])
        s_str = ', '.join(f"${p:.4f}" for p in cd['support'][:3])
        data_parts = [f"LIVE=${cd['price']:.4f}", f"ATR_1h={cd['atr']*100:.2f}%",
                      f"R=[{r_str}]", f"S=[{s_str}]"]
        if cd.get('funding') is not None:
            data_parts.append(f"funding={cd['funding']*100:.3f}%")
        parts.append(f"[{coin}: {' | '.join(data_parts)}]")

    if banned_coins:
        parts.append(f"BANNED (SL'd 2+ times today): {', '.join(banned_coins)} — do NOT trade these.")

    parts.append(f"""REGIME: {regime}
{f"Open positions: {open_positions_str}" if open_positions_str else "No open positions."}

DECISION PROCESS (follow this order):
1. READ MACRO first. BTC_7d = trend. F&G = sentiment. Funding = crowd.
2. DECIDE direction from MACRO. If BTC moved >2% today -> exhaustion risk.
3. If uncertain -> SKIP. Return empty [].
4. Find 3-5 coins matching direction at S/R within 0.5%.

RULES:
- OB pressure for ENTRY PLACEMENT only, not direction.
- Fast approach to S/R (>0.5% in 1h) -> breakdown likely, SKIP.
- CITE at least one MACRO factor in each reason.
- Risk: SL -6.5% ROI, TP +13%, trailing +6%/-2%.

Reply JSON array (0-5 setups):
[{{
  "coin": "BNB",
  "direction": "LONG",
  "entry": 608.5,
  "sl": 606.7,
  "tp": 612.0,
  "leverage": 8,
  "confidence": 0.70,
  "reason": "BTC_7d +2.3% supports longs, near S1, funding negative -> squeeze potential"
}}]

If uncertain or no quality setups: return []""")

    return "\n\n".join(parts)


# --- Simulation with fixed ROI SL/TP + trailing ---
def compute_roi(direction, entry, current, leverage):
    """Compute ROI % as fraction."""
    if direction == 'LONG':
        return ((current - entry) / entry) * leverage
    else:
        return ((entry - current) / entry) * leverage


def simulate_position(pos, candles_after_fill, leverage):
    """
    Simulate position with fixed SL/TP in ROI terms + trailing stop.
    Returns (exit_price, exit_reason, bars_held).
    """
    entry = pos['entry']
    direction = pos['direction']
    peak_roi = 0.0
    trailing_active = False

    for bar_idx, candle in enumerate(candles_after_fill):
        # Check at multiple points: open, then high/low (worst first), then close
        check_prices = []
        if direction == 'LONG':
            check_prices = [candle['open'], candle['low'], candle['high'], candle['close']]
        else:
            check_prices = [candle['open'], candle['high'], candle['low'], candle['close']]

        for px in check_prices:
            roi = compute_roi(direction, entry, px, leverage)

            # SL check
            if roi <= SL_ROI:
                # Compute the exact SL price
                if direction == 'LONG':
                    sl_price = entry * (1 + SL_ROI / leverage)
                else:
                    sl_price = entry * (1 - SL_ROI / leverage)
                return sl_price, 'SL', bar_idx + 1

            # Update peak
            if roi > peak_roi:
                peak_roi = roi

            # Trailing activation
            if peak_roi >= TRAILING_ACTIVATE_ROI:
                trailing_active = True

            # Trailing stop check
            if trailing_active and (peak_roi - roi) >= TRAILING_DROP_ROI:
                return px, 'TRAILING', bar_idx + 1

            # TP check
            if roi >= TP_ROI:
                if direction == 'LONG':
                    tp_price = entry * (1 + TP_ROI / leverage)
                else:
                    tp_price = entry * (1 - TP_ROI / leverage)
                return tp_price, 'TP', bar_idx + 1

        # TIME_EXIT after 3 bars
        if bar_idx + 1 >= TIME_EXIT_BARS:
            return candle['close'], 'TIME', bar_idx + 1

    # If no more candles, exit at last close
    if candles_after_fill:
        return candles_after_fill[-1]['close'], 'EOD', len(candles_after_fill)
    return entry, 'NO_DATA', 0


# --- Load actual trades for comparison ---
def load_actual_trades(conn):
    """Load actual fortix_trades for Apr 17."""
    rows = conn.execute(
        "SELECT coin, direction, status, entry_type, fill_price, sl_price, tp_price, "
        "exit_price, regime, confidence, pnl_pct, pnl_usd, exit_reason, held_minutes, "
        "created_at, filled_at, closed_at "
        "FROM fortix_trades WHERE created_at LIKE '2026-04-17%' AND status = 'CLOSED' "
        "ORDER BY created_at"
    ).fetchall()
    actual = []
    for r in rows:
        actual.append({
            'coin': r[0], 'direction': r[1], 'status': r[2], 'entry_type': r[3],
            'fill_price': r[4], 'sl_price': r[5], 'tp_price': r[6], 'exit_price': r[7],
            'regime': r[8], 'confidence': r[9], 'pnl_pct': r[10], 'pnl_usd': r[11],
            'exit_reason': r[12], 'held_minutes': r[13], 'created_at': r[14],
            'filled_at': r[15], 'closed_at': r[16],
        })
    return actual


# --- Main ---
def main():
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        log.error("No ANTHROPIC_API_KEY found!")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key, timeout=120.0, max_retries=3)
    knowledge = load_knowledge()
    system = build_system_prompt(knowledge)

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Load actual trades for comparison
    actual_trades = load_actual_trades(conn)
    log.info(f"Loaded {len(actual_trades)} actual closed trades for Apr 17")

    # Pre-load candles
    lookback_start = DAY_START - 100 * 3600
    all_candles = {}
    for coin in TRACKED_COINS:
        all_candles[coin] = get_1h_candles(conn, coin, lookback_start, DAY_END)
        log.info(f"Loaded {len(all_candles[coin])} 1h candles for {coin}")

    apr17_candles = {}
    for coin in TRACKED_COINS:
        apr17_candles[coin] = get_1h_candles(conn, coin, DAY_START, DAY_END)

    # Track state
    equity = START_EQUITY
    trades = []
    pending_orders = []
    open_positions = []
    sl_counts = {}  # {coin: count} for daily SL ban
    banned_coins = set()
    total_input_tokens = 0
    total_output_tokens = 0
    api_calls = 0
    skipped_scans = 0
    macro_cited_count = 0
    total_setups = 0

    # Scan every 2 hours: 0, 2, 4, 6, 8, 10, 12, 14 (8 scans, data up to 16:00)
    scan_hours = list(range(0, 16, 2))

    for scan_hour in scan_hours:
        scan_ts = DAY_START + scan_hour * 3600
        scan_dt = datetime.fromtimestamp(scan_ts, tz=timezone.utc)
        log.info(f"\n{'='*60}")
        log.info(f"SCAN at {scan_dt.strftime('%Y-%m-%d %H:%M UTC')} | Equity: ${equity:.2f}")

        # Expire old pending orders (> 2h)
        pending_orders = [p for p in pending_orders if p['expire_ts'] > scan_ts]

        # Compute regime + macro
        regime, btc_12h, breadth = compute_regime(conn, scan_ts)
        macro_line = get_macro_line(conn, scan_ts)
        btc_mom = get_btc_momentum(conn, scan_ts)
        log.info(f"Regime: {regime} | BTC 12h: {btc_12h:+.1f}% | Breadth: {breadth:.0%}")
        log.info(f"{macro_line}")

        # Build coin data
        coins_data = []
        open_coins = {p['coin'] for p in open_positions}
        for coin in TRACKED_COINS:
            if coin in open_coins or coin in banned_coins:
                continue
            candles_up_to = [c for c in all_candles[coin] if c['ts'] <= scan_ts]
            if len(candles_up_to) < 30:
                continue
            current_price = candles_up_to[-1]['close']
            atr = compute_atr_1h(candles_up_to)
            resistance, support = find_levels_from_candles(candles_up_to[-100:], current_price)
            if not resistance and not support:
                continue
            fr = conn.execute(
                "SELECT rate FROM funding_rates WHERE coin=? AND timestamp <= ? "
                "ORDER BY timestamp DESC LIMIT 1", (coin, scan_ts)
            ).fetchone()
            funding = fr[0] if fr and fr[0] is not None else 0.0
            coins_data.append({
                'coin': coin, 'price': current_price, 'atr': atr,
                'resistance': resistance, 'support': support, 'funding': funding
            })

        # Limit to 8 coins for prompt
        prompt_coins = coins_data[:8]
        if not prompt_coins:
            log.info("No coins available for scan")
            continue

        user_prompt = build_user_prompt(
            prompt_coins, regime, macro_line, btc_mom, banned_coins
        )

        # Call Opus
        log.info(f"Calling Opus for {len(prompt_coins)} coins...")
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=3000,
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
            log.info(f"Response: {result_text[:300]}")
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
                    if conf < 0.65:
                        continue
                    s['confidence'] = conf
                    # Check banned
                    if s.get('coin', '') in banned_coins:
                        log.info(f"  SKIPPING banned coin: {s.get('coin')}")
                        continue
                    setups.append(s)
                    total_setups += 1
                    # Check if MACRO is cited in reason
                    reason = s.get('reason', '')
                    macro_keywords = ['BTC_7d', 'F&G', 'MVRV', 'funding', 'macro', 'sentiment',
                                     'fear', 'greed', 'SOPR', 'NUPL', 'premium', '7d', '1d']
                    if any(kw.lower() in reason.lower() for kw in macro_keywords):
                        macro_cited_count += 1
                    log.info(f"  SETUP: {direction} {s['coin']} entry=${s.get('entry',0):.4f} "
                             f"conf={conf:.0%} — {reason[:80]}")
            else:
                log.info("Empty response or no JSON array (model chose to SKIP)")
                skipped_scans += 1
        except Exception as e:
            log.warning(f"Parse error: {e}")

        if not setups:
            log.info(f"  >> SKIP (0 setups) at {scan_dt.strftime('%H:%M')}")
            skipped_scans += 1

        # Add to pending
        for s in setups:
            coin = s.get('coin', '')
            if coin not in [cd['coin'] for cd in coins_data]:
                continue
            if coin in open_coins or coin in banned_coins:
                continue
            pending_orders.append({
                'coin': coin,
                'direction': s['direction'],
                'entry': float(s.get('entry', 0)),
                'sl': float(s.get('sl', 0)),
                'tp': float(s.get('tp', 0)),
                'leverage': LEVERAGE,  # force 8x
                'confidence': s['confidence'],
                'reason': s.get('reason', ''),
                'scan_ts': scan_ts,
                'expire_ts': scan_ts + 2 * 3600,
            })

        log.info(f"Pending: {len(pending_orders)} | Open: {len(open_positions)} | Banned: {banned_coins}")

        # --- Simulate fills and exits for the next 2 hours ---
        next_scan_ts = scan_ts + 2 * 3600
        sim_hours = [scan_ts + h * 3600 for h in range(0, 2)]

        for bar_ts in sim_hours:
            # Check pending orders for fills
            filled_indices = []
            for i, order in enumerate(pending_orders):
                coin = order['coin']
                candle = next((c for c in apr17_candles[coin] if c['ts'] == bar_ts), None)
                if not candle:
                    continue
                entry = order['entry']
                hit = False
                if order['direction'] == 'LONG':
                    if candle['low'] <= entry <= candle['high']:
                        hit = True
                    elif entry >= candle['open']:
                        hit = True
                        entry = candle['open']
                else:
                    if candle['low'] <= entry <= candle['high']:
                        hit = True
                    elif entry <= candle['open']:
                        hit = True
                        entry = candle['open']

                if hit and coin not in {p['coin'] for p in open_positions}:
                    # Get candles after fill for simulation
                    candles_after = [c for c in apr17_candles[coin] if c['ts'] > bar_ts]
                    size_usd = equity * POS_SIZE_PCT * LEVERAGE

                    open_positions.append({
                        'coin': coin,
                        'direction': order['direction'],
                        'entry': entry,
                        'confidence': order['confidence'],
                        'reason': order['reason'],
                        'size_usd': size_usd,
                        'fill_ts': bar_ts,
                        'candles_after': candles_after,
                    })
                    filled_indices.append(i)
                    log.info(f"  FILLED: {order['direction']} {coin} @ ${entry:.4f}")

            for i in sorted(filled_indices, reverse=True):
                pending_orders.pop(i)

            # Check open positions
            closed_indices = []
            for i, pos in enumerate(open_positions):
                coin = pos['coin']
                # Get candles from fill to current bar
                bars_since_fill = [c for c in apr17_candles[coin]
                                   if c['ts'] > pos['fill_ts'] and c['ts'] <= bar_ts]
                if not bars_since_fill:
                    continue

                # Simulate with fixed ROI
                exit_price, exit_reason, bars_held = simulate_position(
                    pos, bars_since_fill, LEVERAGE
                )

                if exit_reason in ('SL', 'TP', 'TRAILING', 'TIME') or bar_ts >= next_scan_ts - 3600:
                    # Only close if actually triggered or approaching next scan
                    # Re-simulate on ALL available candles after fill up to now
                    all_bars = [c for c in apr17_candles[coin] if c['ts'] > pos['fill_ts'] and c['ts'] <= bar_ts]
                    exit_price, exit_reason, bars_held = simulate_position(pos, all_bars, LEVERAGE)

                    if exit_reason not in ('SL', 'TP', 'TRAILING', 'TIME', 'EOD'):
                        continue

                    pnl_pct = compute_roi(pos['direction'], pos['entry'], exit_price, LEVERAGE)
                    fee = FEE_ROUNDTRIP * LEVERAGE
                    net_pnl_pct = pnl_pct - fee
                    pnl_usd = (equity * POS_SIZE_PCT) * net_pnl_pct

                    equity += pnl_usd

                    # Track SL for ban
                    if exit_reason == 'SL':
                        sl_counts[coin] = sl_counts.get(coin, 0) + 1
                        if sl_counts[coin] >= 2:
                            banned_coins.add(coin)
                            log.info(f"  BANNED {coin} (2+ SL today)")

                    trade = {
                        'coin': coin,
                        'direction': pos['direction'],
                        'entry': pos['entry'],
                        'exit': exit_price,
                        'exit_reason': exit_reason,
                        'pnl_roi': round(pnl_pct * 100, 3),
                        'net_pnl_usd': round(pnl_usd, 2),
                        'equity_after': round(equity, 2),
                        'confidence': pos['confidence'],
                        'reason': pos['reason'],
                        'fill_time': datetime.fromtimestamp(pos['fill_ts'], tz=timezone.utc).strftime('%H:%M'),
                        'exit_time': datetime.fromtimestamp(bar_ts, tz=timezone.utc).strftime('%H:%M'),
                        'bars_held': bars_held,
                        'leverage': LEVERAGE,
                    }
                    trades.append(trade)
                    closed_indices.append(i)
                    log.info(f"  CLOSED: {pos['direction']} {coin} @ ${exit_price:.4f} "
                             f"({exit_reason}) ROI={pnl_pct*100:+.2f}% (${pnl_usd:+.2f}) Eq=${equity:.2f}")

            for i in sorted(closed_indices, reverse=True):
                open_positions.pop(i)

    # Force-close remaining
    for pos in open_positions:
        coin = pos['coin']
        last_candle = apr17_candles[coin][-1] if apr17_candles[coin] else None
        if not last_candle:
            continue
        exit_price = last_candle['close']
        pnl_pct = compute_roi(pos['direction'], pos['entry'], exit_price, LEVERAGE)
        fee = FEE_ROUNDTRIP * LEVERAGE
        net_pnl_pct = pnl_pct - fee
        pnl_usd = (equity * POS_SIZE_PCT) * net_pnl_pct
        equity += pnl_usd
        trade = {
            'coin': coin, 'direction': pos['direction'],
            'entry': pos['entry'], 'exit': exit_price,
            'exit_reason': 'EOD',
            'pnl_roi': round(pnl_pct * 100, 3),
            'net_pnl_usd': round(pnl_usd, 2),
            'equity_after': round(equity, 2),
            'confidence': pos['confidence'],
            'reason': pos['reason'],
            'fill_time': datetime.fromtimestamp(pos['fill_ts'], tz=timezone.utc).strftime('%H:%M'),
            'exit_time': '16:00',
            'bars_held': 0,
            'leverage': LEVERAGE,
        }
        trades.append(trade)

    conn.close()

    # --- Save trades.csv ---
    csv_path = OUT_DIR / 'trades.csv'
    if trades:
        with open(csv_path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=trades[0].keys())
            w.writeheader()
            w.writerows(trades)
        log.info(f"Saved {len(trades)} trades to {csv_path}")

    # --- Build comparison with actual trades ---
    actual_closed = actual_trades  # already filtered to CLOSED
    actual_total_pnl = sum(t['pnl_usd'] or 0 for t in actual_closed)
    actual_wins = sum(1 for t in actual_closed if (t['pnl_usd'] or 0) > 0)
    actual_wr = actual_wins / len(actual_closed) * 100 if actual_closed else 0
    actual_sl_count = sum(1 for t in actual_closed if t['exit_reason'] and 'STOP' in t['exit_reason'])

    # NEW results
    total_pnl = sum(t['net_pnl_usd'] for t in trades)
    wins = [t for t in trades if t['net_pnl_usd'] > 0]
    losses = [t for t in trades if t['net_pnl_usd'] <= 0]
    wr = len(wins) / len(trades) * 100 if trades else 0
    new_sl_count = sum(1 for t in trades if t['exit_reason'] == 'SL')

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

    # Actual per-coin
    actual_by_coin = {}
    for t in actual_closed:
        c = t['coin']
        if c not in actual_by_coin:
            actual_by_coin[c] = {'trades': 0, 'pnl': 0, 'wins': 0}
        actual_by_coin[c]['trades'] += 1
        actual_by_coin[c]['pnl'] += (t['pnl_usd'] or 0)
        if (t['pnl_usd'] or 0) > 0:
            actual_by_coin[c]['wins'] += 1

    # Macro citation rate
    macro_cite_pct = (macro_cited_count / total_setups * 100) if total_setups > 0 else 0

    # FLAT regime skip analysis
    flat_regimes = []  # we don't store regime per scan, but can note skipped scans

    summary = f"""# Backtest: April 17, 2026 — NEW Prompt System vs ACTUAL

## NEW vs ACTUAL Comparison

| Metric | NEW Prompt | ACTUAL (OKX) |
|--------|-----------|--------------|
| Total trades | {len(trades)} | {len(actual_closed)} |
| Win rate | {wr:.1f}% | {actual_wr:.1f}% |
| Net PnL | ${total_pnl:+,.2f} | ${actual_total_pnl:+,.2f} |
| SL hits | {new_sl_count} | {actual_sl_count} |
| Skipped scans | {skipped_scans}/{len(scan_hours)} | 0/{len(scan_hours)} |

## NEW Prompt Details
- **Start equity**: ${START_EQUITY:,.2f}
- **End equity**: ${equity:,.2f}
- **Net PnL**: ${total_pnl:+,.2f} ({total_pnl/START_EQUITY*100:+.2f}%)
- **Trades**: {len(trades)} ({len(wins)}W / {len(losses)}L)
- **Leverage**: {LEVERAGE}x | SL: {SL_ROI*100:.1f}% ROI | TP: {TP_ROI*100:.0f}% ROI
- **Trailing**: activate +{TRAILING_ACTIVATE_ROI*100:.0f}%, drop -{TRAILING_DROP_ROI*100:.0f}%
- **Scans**: {len(scan_hours)} (every 2h, 00:00-14:00 UTC) | API calls: {api_calls}
- **Tokens**: {total_input_tokens:,} in / {total_output_tokens:,} out
- **Est. cost**: ${(total_input_tokens * 15 + total_output_tokens * 75) / 1e6:.2f}
- **Banned coins** (2+ SL): {', '.join(banned_coins) if banned_coins else 'None'}

## MACRO Citation Analysis
- Setups citing MACRO: {macro_cited_count}/{total_setups} ({macro_cite_pct:.0f}%)
- Skipped scans (empty []): {skipped_scans}/{len(scan_hours)}

## Exit Type Breakdown (NEW)
| Exit | Count | PnL |
|------|-------|-----|
"""
    for r, d in sorted(by_exit.items()):
        summary += f"| {r} | {d['count']} | ${d['pnl']:+.2f} |\n"

    summary += "\n## Per-Coin Comparison\n| Coin | NEW trades | NEW PnL | ACTUAL trades | ACTUAL PnL |\n|------|-----------|---------|---------------|------------|\n"
    all_coins = sorted(set(list(by_coin.keys()) + list(actual_by_coin.keys())))
    for c in all_coins:
        n = by_coin.get(c, {'trades': 0, 'pnl': 0})
        a = actual_by_coin.get(c, {'trades': 0, 'pnl': 0})
        summary += f"| {c} | {n['trades']} | ${n['pnl']:+.2f} | {a['trades']} | ${a['pnl']:+.2f} |\n"

    # Regime analysis
    summary += f"""
## Key Questions

### Did NEW skip when it should (FLAT regime)?
Skipped {skipped_scans} out of {len(scan_hours)} scans. The actual system NEVER skipped and had {actual_sl_count} SL hits.
{'YES - NEW correctly skipped uncertain conditions.' if skipped_scans > 0 and actual_sl_count > 3 else 'Needs more data to assess.'}

### Did NEW cite MACRO in reasons?
{macro_cite_pct:.0f}% of setups cited at least one MACRO factor. Target: 100%.

### Was SL ban effective?
Banned coins: {', '.join(banned_coins) if banned_coins else 'None'}.
{'Prevented repeat losses on same coins.' if banned_coins else 'No coin hit 2+ SL (good discipline or few trades).'}

## Recommendation
"""
    if total_pnl > actual_total_pnl:
        summary += f"**DEPLOY** - NEW prompt outperformed ACTUAL by ${total_pnl - actual_total_pnl:+.2f}.\n"
        summary += f"Fewer trades ({len(trades)} vs {len(actual_closed)}), "
        summary += f"better selectivity. MACRO-driven decisions work.\n"
    elif total_pnl > 0:
        summary += f"**CAUTIOUS DEPLOY** - NEW prompt profitable (${total_pnl:+.2f}) but underperformed ACTUAL.\n"
        summary += f"Review if skipping was too aggressive.\n"
    else:
        summary += f"**DO NOT DEPLOY** - NEW prompt lost ${total_pnl:.2f}.\n"
        if actual_total_pnl > 0:
            summary += f"ACTUAL was profitable. The skip-heavy approach may not suit this market.\n"
        else:
            summary += f"Both lost money — market was tough. Review MACRO interpretation.\n"

    summary_path = OUT_DIR / 'summary.md'
    summary_path.write_text(summary)
    log.info(f"Saved summary to {summary_path}")

    print("\n" + "=" * 60)
    print(summary)


if __name__ == '__main__':
    main()
