"""
FORTIX — Signal Monitor v2
====================================
Monitors market conditions and auto-triggers breaking news shorts / signal alert videos.

Trigger categories:
  1. Price moves (all 28 tracked coins, per-group thresholds)
  2. Fear & Greed extremes and swings
  3. Whale transactions > $500M
  4. Derivatives (liquidations, funding, OI, L/S ratio — multi-coin)
  5. On-chain (SOPR, NUPL, MVRV, Puell, Coinbase premium, miner reserve)
  6. Market flows (taker volume, exchange flows, stablecoin flows)
  7. Structural (ETF, BTC levels, stablecoin supply, correlation, options max pain, exchange balance)
  8. Breaking news (Claude Haiku evaluation, impact >= 7)

Usage:
    python src/crypto/signal_monitor.py              # Check once
    python src/crypto/signal_monitor.py --daemon      # Run continuously (every 30 min)
    python src/crypto/signal_monitor.py --news-check  # Check only news triggers
    python src/crypto/signal_monitor.py --dry-run     # Check but don't trigger production
"""

import os
import sys
import json
import time
import hashlib
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('signal_monitor')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
ALERTS_LOG = _FACTORY_DIR / 'data' / 'crypto' / 'alerts.json'
CHECK_INTERVAL_MIN = 30
NEWS_CHECK_INTERVAL_MIN = 5
NEWS_IMPACT_THRESHOLD = 9  # Only major events (hack, ETF decision, regulation, crash)
NEWS_COOLDOWN_HOURS = 6
NEWS_MAX_AGE_HOURS = 2
MAX_ALERTS_PER_HOUR = 1           # Max 1 breaking news per hour
MAX_BREAKING_PER_DAY = 2          # Max 2 breaking news per day (total 4-5 vids with scheduled)
MAX_STANDALONE_PRODUCTIONS = 2


# ════════════════════════════════════════════
# COIN GROUPS & THRESHOLD CONFIGURATION
# ════════════════════════════════════════════

COIN_GROUPS = {
    'majors': ['BTC', 'ETH'],
    'top_alts': ['SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'POL'],
    'defi': ['UNI', 'AAVE', 'PENDLE', 'LDO', 'CRV'],
    'layer2': ['ARB', 'OP', 'STRK', 'ZK'],
    'ai': ['FET', 'RENDER', 'AGIX', 'TAO'],
    'meme': ['DOGE', 'SHIB', 'PEPE', 'WIF', 'BONK'],
}

# Build reverse lookup: coin -> group
_COIN_TO_GROUP = {}
for _grp, _coins in COIN_GROUPS.items():
    for _c in _coins:
        _COIN_TO_GROUP[_c] = _grp

# Price crash/pump thresholds (absolute % change in 24h)
# RAISED: only truly significant moves trigger breaking news
PRICE_THRESHOLDS = {
    'BTC': {'threshold': 8.0, 'cooldown': 24},
    'ETH': {'threshold': 12.0, 'cooldown': 24},
    'top_alts': {'threshold': 18.0, 'cooldown': 24},
    'defi': {'threshold': 22.0, 'cooldown': 24},
    'layer2': {'threshold': 22.0, 'cooldown': 24},
    'ai': {'threshold': 25.0, 'cooldown': 24},
    'meme': {'threshold': 30.0, 'cooldown': 24},
}

# Long/Short ratio extreme thresholds (stored as 0-100 percentage)
# RAISED: only extreme crowding triggers (not regular market noise)
LS_THRESHOLDS = {
    'majors': {'extreme_long': 80, 'extreme_short': 20, 'cooldown': 48},
    'top_alts': {'extreme_long': 82, 'extreme_short': 18, 'cooldown': 48},
    'defi': {'extreme_long': 78, 'extreme_short': 22, 'cooldown': 48},
    'layer2': {'extreme_long': 78, 'extreme_short': 22, 'cooldown': 48},
    'ai': {'extreme_long': 78, 'extreme_short': 22, 'cooldown': 48},
    'meme': {'extreme_long': 80, 'extreme_short': 20, 'cooldown': 48},
}

# Funding rate extreme thresholds (decimal fraction, e.g. 0.0005 = 0.05%)
# RAISED: only extreme funding (was triggering on normal market activity)
FUNDING_THRESHOLDS = {
    'majors': {'extreme': 0.002, 'flip_magnitude': 0.001, 'cooldown': 48},
    'top_alts': {'extreme': 0.004, 'flip_magnitude': 0.002, 'cooldown': 48},
    'defi': {'extreme': 0.006, 'flip_magnitude': 0.003, 'cooldown': 48},
    'layer2': {'extreme': 0.006, 'flip_magnitude': 0.003, 'cooldown': 48},
    'ai': {'extreme': 0.008, 'flip_magnitude': 0.004, 'cooldown': 48},
    'meme': {'extreme': 0.01, 'flip_magnitude': 0.005, 'cooldown': 48},
}

# Open Interest crash/surge thresholds (% change in 4h)
OI_THRESHOLDS = {
    'majors': {'crash': -10, 'surge': 15, 'cooldown': 8},
    'top_alts': {'crash': -15, 'surge': 25, 'cooldown': 8},
    'defi': {'crash': -20, 'surge': 30, 'cooldown': 8},
    'meme': {'crash': -20, 'surge': 30, 'cooldown': 6},
}

# Per-coin liquidation thresholds (USD in 4h / 24h)
# RAISED: only massive liquidation cascades (not routine clearing)
PER_COIN_LIQ_THRESHOLDS = {
    'BTC': {'liq_4h': 100_000_000, 'liq_24h': 400_000_000, 'cooldown': 24},
    'ETH': {'liq_4h': 60_000_000, 'liq_24h': 200_000_000, 'cooldown': 24},
    'SOL': {'liq_4h': 25_000_000, 'liq_24h': 80_000_000, 'cooldown': 24},
    'top_alts': {'liq_4h': 15_000_000, 'liq_24h': 50_000_000, 'cooldown': 24},
    'defi': {'liq_4h': 10_000_000, 'liq_24h': 25_000_000, 'cooldown': 24},
    'meme': {'liq_4h': 10_000_000, 'liq_24h': 25_000_000, 'cooldown': 24},
}

# Taker buy/sell ratio thresholds
TAKER_THRESHOLDS = {
    'majors': {'fomo': 1.5, 'panic': 0.55, 'cooldown': 12},
    'top_alts': {'fomo': 1.7, 'panic': 0.50, 'cooldown': 8},
    'defi': {'fomo': 1.8, 'panic': 0.45, 'cooldown': 8},
    'layer2': {'fomo': 1.8, 'panic': 0.45, 'cooldown': 8},
    'ai': {'fomo': 2.0, 'panic': 0.45, 'cooldown': 8},
    'meme': {'fomo': 1.5, 'panic': 0.35, 'cooldown': 6},
}

# BTC on-chain metric thresholds
ONCHAIN_THRESHOLDS = {
    'sopr_capitulation': 0.90,
    'sopr_euphoria': 1.05,
    'nupl_capitulation': -0.25,
    'nupl_euphoria': 0.75,
    'mvrv_undervalued': 0.8,
    'mvrv_overvalued': 2.7,
    'puell_bottom': 0.4,
    'puell_top': 4.0,
    'cooldown': 48,
}

# Coinbase premium thresholds (premium_index)
COINBASE_PREMIUM_THRESHOLDS = {
    'institutional_selling': -0.06,
    'institutional_fomo': 0.08,
    'extreme_selling': -0.15,
    'extreme_fomo': 0.15,
    'cooldown': 24,
}

# Miner reserve thresholds (% change over 7 days)
MINER_THRESHOLDS = {
    'capitulation_pct': -0.5,
    'accumulation_pct': 0.5,
    'cooldown': 72,
}

# Exchange flow thresholds (USD per day)
EXCHANGE_FLOW_THRESHOLDS = {
    'BTC': 500_000_000,
    'ETH': 200_000_000,
    'default': 50_000_000,
    'cooldown': 24,
}

# Stablecoin exchange flow thresholds (USD)
STABLECOIN_FLOW_THRESHOLDS = {
    'large_inflow': 1_500_000_000,
    'large_outflow': -1_000_000_000,
    'cooldown': 24,
}

# Options max pain thresholds
OPTIONS_THRESHOLDS = {
    'divergence_pct': 15,
    'cooldown': 48,
}

# Exchange balance thresholds (% change in 1 day)
EXCHANGE_BALANCE_THRESHOLDS = {
    'large_change_pct': 5.0,
    'cooldown': 24,
}

# ETF flow thresholds per asset
ETF_THRESHOLDS = {
    'BTC': 500_000_000,
    'ETH': 300_000_000,
    'cooldown': 24,
}


# ════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════

def get_coin_group(coin: str) -> str:
    """Return group name for a coin, or 'unknown'."""
    return _COIN_TO_GROUP.get(coin, 'unknown')


def _get_group_config(coin: str, thresholds: dict, default_key: str = 'top_alts') -> dict:
    """Get threshold config for a coin by its group."""
    group = get_coin_group(coin)
    if coin in thresholds:
        return thresholds[coin]
    if group in thresholds:
        return thresholds[group]
    return thresholds.get(default_key, thresholds.get('top_alts', {}))


def _fmt_usd(amount: float) -> str:
    """Format USD amount nicely."""
    if abs(amount) >= 1e9:
        return f"${abs(amount)/1e9:.1f}B"
    if abs(amount) >= 1e6:
        return f"${abs(amount)/1e6:.0f}M"
    return f"${abs(amount):,.0f}"


# ════════════════════════════════════════════
# ALERT HISTORY
# ════════════════════════════════════════════

def load_alert_history() -> dict:
    """Load history of triggered alerts to enforce cooldowns."""
    if ALERTS_LOG.exists():
        try:
            with open(ALERTS_LOG, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError, UnicodeDecodeError):
            try:
                with open(ALERTS_LOG, 'r', encoding='cp1252') as f:
                    return json.load(f)
            except Exception:
                pass
    return {'alerts': []}


def save_alert(alert: dict, history: dict):
    """Save a triggered alert to history."""
    history['alerts'].append(alert)
    history['alerts'] = history['alerts'][-200:]
    ALERTS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_LOG, 'w') as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def is_on_cooldown(trigger_name: str, history: dict, cooldown_hours: float) -> bool:
    """Check if a trigger is on cooldown."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=cooldown_hours)).isoformat()
    for alert in reversed(history['alerts']):
        if alert.get('trigger') == trigger_name and alert.get('timestamp', '') > cutoff:
            return True
    return False


def _count_recent_alerts(history: dict, hours: float = 1.0) -> int:
    """Count alerts fired in the last N hours (for rate limiting)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    return sum(1 for a in history['alerts'] if a.get('timestamp', '') > cutoff)


# ════════════════════════════════════════════
# 1. PRICE TRIGGERS (all 28 coins)
# ════════════════════════════════════════════

def check_price_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check price-based triggers for all tracked coins."""
    fired = []

    rows = conn.execute(
        "SELECT coin, change_24h, price_usd FROM market_overview "
        "WHERE timestamp > ? ORDER BY timestamp DESC",
        (int((datetime.now() - timedelta(hours=2)).timestamp()),)
    ).fetchall()

    # Deduplicate — keep latest per coin
    seen = set()
    prices = {}
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            prices[r[0]] = {'change_24h': r[1], 'price': r[2]}

    # Check every tracked coin against its group threshold
    for group_name, coins in COIN_GROUPS.items():
        for coin in coins:
            if coin not in prices or prices[coin]['change_24h'] is None:
                continue

            change = prices[coin]['change_24h']
            cfg = _get_group_config(coin, PRICE_THRESHOLDS)
            threshold = cfg['threshold']
            cooldown = cfg['cooldown']

            # Crash
            if change <= -threshold:
                trigger_id = f'price_crash_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} crashed {change:.1f}% in 24h",
                        'headline': f"{coin} Down {abs(change):.0f}% — What the Data Shows",
                        'coin': coin,
                        'category': 'price_crash',
                        'data': {'change': change, 'price': prices[coin]['price'], 'group': group_name},
                        'severity': 'high' if abs(change) > threshold * 1.5 else 'medium',
                    })

            # Pump
            elif change >= threshold:
                trigger_id = f'price_pump_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} surged {change:.1f}% in 24h",
                        'headline': f"{coin} Up {change:.0f}% — Here's What's Driving It",
                        'coin': coin,
                        'category': 'price_pump',
                        'data': {'change': change, 'price': prices[coin]['price'], 'group': group_name},
                        'severity': 'high' if change > threshold * 1.5 else 'medium',
                    })

    return fired


# ════════════════════════════════════════════
# 2. FEAR & GREED TRIGGERS
# ════════════════════════════════════════════

def check_fear_greed_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check Fear & Greed based triggers."""
    fired = []

    fg_rows = conn.execute(
        "SELECT date, value, classification FROM fear_greed ORDER BY date DESC LIMIT 2"
    ).fetchall()

    if not fg_rows:
        return fired

    current_value = fg_rows[0][1]

    # Extreme fear (≤ 10)
    if current_value <= 10:
        if not is_on_cooldown('fear_extreme', history, 24):
            fired.append({
                'trigger': 'fear_extreme',
                'trigger_type': 'breaking_news',
                'event': f"Fear & Greed hit {current_value} — extreme panic",
                'headline': f"Fear Index at {current_value} — Extreme Panic in Crypto",
                'category': 'fear_greed',
                'data': {'value': current_value, 'direction': 'fear'},
                'severity': 'high',
            })

    # Extreme greed (≥ 90)
    if current_value >= 90:
        if not is_on_cooldown('greed_extreme', history, 24):
            fired.append({
                'trigger': 'greed_extreme',
                'trigger_type': 'breaking_news',
                'event': f"Fear & Greed hit {current_value} — extreme euphoria",
                'headline': f"Greed Index at {current_value} — Market Euphoria Warning",
                'category': 'fear_greed',
                'data': {'value': current_value, 'direction': 'greed'},
                'severity': 'high',
            })

    # F&G swing (≥ 20 points in one day)
    if len(fg_rows) >= 2:
        prev_value = fg_rows[1][1]
        swing = abs(current_value - prev_value)
        if swing >= 20:
            if not is_on_cooldown('fg_swing', history, 24):
                fired.append({
                    'trigger': 'fg_swing',
                    'trigger_type': 'breaking_news',
                    'event': f"Fear & Greed swung {swing} points in 24h (from {prev_value} to {current_value})",
                    'headline': f"Fear Index Swung {swing} Points — Sentiment Reversal",
                    'category': 'fear_greed',
                    'data': {'swing': swing, 'prev': prev_value, 'current': current_value},
                    'severity': 'high' if swing > 30 else 'medium',
                })

    return fired


# ════════════════════════════════════════════
# 3. WHALE TRIGGERS
# ════════════════════════════════════════════

def check_whale_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check whale transaction triggers — only truly massive moves ($1B+)."""
    fired = []

    day_ago = int((datetime.now() - timedelta(hours=24)).timestamp())
    whales = conn.execute(
        "SELECT coin, amount_usd, from_label, to_label FROM whale_transactions "
        "WHERE timestamp > ? AND amount_usd >= ? ORDER BY amount_usd DESC LIMIT 3",
        (day_ago, 1_000_000_000)  # $1B minimum (was $500M — too sensitive)
    ).fetchall()

    for w in whales:
        coin = w[0] or 'Unknown'
        amount_usd = w[1]
        from_label = w[2] or 'unknown'
        to_label = w[3] or 'unknown'
        amount_str = _fmt_usd(amount_usd)

        trigger_id = f"whale_mega_{coin}_{int(amount_usd/1e6)}"
        if not is_on_cooldown(trigger_id, history, 24):  # 24h cooldown (was 6h)
            fired.append({
                'trigger': trigger_id,
                'trigger_type': 'breaking_news',
                'event': f"Whale moved {amount_str} of {coin}",
                'headline': f"{amount_str} {coin} Whale Move — Where Is It Going?",
                'coin': coin,
                'category': 'whale_move',
                'data': {'amount_usd': amount_usd, 'from': from_label, 'to': to_label},
                'severity': 'critical' if amount_usd > 2e9 else 'high',
            })

    return fired


# ════════════════════════════════════════════
# 4. DERIVATIVES TRIGGERS (multi-coin)
# ════════════════════════════════════════════

def check_derivatives_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check derivatives data for extreme conditions.

    Sub-checks:
      1. Liquidation cascade (aggregate): >$500M in 4h or >$1B in 24h
      2. Liquidation asymmetry: >90% one side
      3. Per-coin liquidations
      4. Funding rate extreme (multi-coin)
      5. Funding rate flip (multi-coin)
      6. Open Interest crash/surge (multi-coin)
      7. Long/Short ratio extreme (all tracked coins)
    """
    fired = []
    now_ts = int(datetime.now().timestamp())

    # ── 1. Aggregate liquidation cascade ──
    try:
        liq_row = conn.execute(
            "SELECT SUM(liq_usd_4h), SUM(liq_usd_24h), SUM(long_liq_usd_24h), SUM(short_liq_usd_24h) "
            "FROM cg_liquidations WHERE timestamp > ? "
            "AND timestamp = (SELECT MAX(timestamp) FROM cg_liquidations WHERE timestamp > ?)",
            (now_ts - 7200, now_ts - 7200)
        ).fetchone()

        if liq_row and liq_row[0]:
            liq_4h = liq_row[0] or 0
            liq_24h = liq_row[1] or 0
            long_24h = liq_row[2] or 0
            short_24h = liq_row[3] or 0

            if liq_4h > 500_000_000:
                trigger_id = 'liquidation_cascade_4h'
                if not is_on_cooldown(trigger_id, history, 8):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Liquidation cascade: {_fmt_usd(liq_4h)} wiped in 4 hours",
                        'headline': f"{_fmt_usd(liq_4h)} Liquidated in 4 Hours — Cascade Alert",
                        'category': 'liquidation_cascade',
                        'data': {'liq_4h': liq_4h, 'liq_24h': liq_24h},
                        'severity': 'critical' if liq_4h > 1e9 else 'high',
                    })
            elif liq_24h > 1_000_000_000:
                trigger_id = 'liquidation_cascade_24h'
                if not is_on_cooldown(trigger_id, history, 12):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Massive liquidations: {_fmt_usd(liq_24h)} wiped in 24 hours",
                        'headline': f"{_fmt_usd(liq_24h)} Liquidated in 24 Hours",
                        'category': 'liquidation_cascade',
                        'data': {'liq_24h': liq_24h},
                        'severity': 'high',
                    })

            # Asymmetry: >90% one side
            total_24h = long_24h + short_24h
            if total_24h > 100_000_000:
                long_pct = (long_24h / total_24h * 100) if total_24h > 0 else 50
                if long_pct > 90:
                    trigger_id = 'liquidation_asymmetry_longs'
                    if not is_on_cooldown(trigger_id, history, 12):
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{long_pct:.0f}% of liquidations are longs — massive long squeeze",
                            'headline': f"{long_pct:.0f}% Long Squeeze — Longs Getting Destroyed",
                            'category': 'liquidation_asymmetry',
                            'data': {'long_pct': long_pct, 'total_24h': total_24h},
                            'severity': 'high',
                        })
                elif long_pct < 10:
                    trigger_id = 'liquidation_asymmetry_shorts'
                    if not is_on_cooldown(trigger_id, history, 12):
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{100-long_pct:.0f}% of liquidations are shorts — massive short squeeze",
                            'headline': f"{100-long_pct:.0f}% Short Squeeze — Shorts Getting Crushed",
                            'category': 'liquidation_asymmetry',
                            'data': {'short_pct': 100 - long_pct, 'total_24h': total_24h},
                            'severity': 'high',
                        })
    except Exception as e:
        log.debug(f"Liquidation trigger check failed: {e}")

    # ── 2. Per-coin liquidations ──
    try:
        all_coins = set()
        for coins in COIN_GROUPS.values():
            all_coins.update(coins)

        coin_liqs = conn.execute(
            "SELECT coin, liq_usd_4h, liq_usd_24h, long_liq_usd_24h, short_liq_usd_24h "
            "FROM cg_liquidations WHERE timestamp > ? "
            "AND timestamp = (SELECT MAX(timestamp) FROM cg_liquidations WHERE timestamp > ?)",
            (now_ts - 7200, now_ts - 7200)
        ).fetchall()

        for row in coin_liqs:
            coin = row[0]
            if coin not in all_coins:
                continue
            liq_4h = row[1] or 0
            liq_24h = row[2] or 0
            long_liq = row[3] or 0
            short_liq = row[4] or 0

            cfg = _get_group_config(coin, PER_COIN_LIQ_THRESHOLDS)
            threshold_4h = cfg.get('liq_4h', 5_000_000)
            threshold_24h = cfg.get('liq_24h', 15_000_000)
            cooldown = cfg.get('cooldown', 8)

            triggered = False
            if liq_4h > threshold_4h:
                trigger_id = f'coin_liq_4h_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    total = long_liq + short_liq
                    side = 'longs' if total > 0 and long_liq > short_liq else 'shorts'
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} liquidations: {_fmt_usd(liq_4h)} in 4h — mostly {side}",
                        'headline': f"{_fmt_usd(liq_4h)} {coin} Liquidated in 4 Hours",
                        'coin': coin,
                        'category': 'coin_liquidation',
                        'data': {'liq_4h': liq_4h, 'long_liq': long_liq, 'short_liq': short_liq},
                        'severity': 'high' if liq_4h > threshold_4h * 2 else 'medium',
                    })
                    triggered = True

            if not triggered and liq_24h > threshold_24h:
                trigger_id = f'coin_liq_24h_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} liquidations: {_fmt_usd(liq_24h)} in 24h",
                        'headline': f"{_fmt_usd(liq_24h)} {coin} Liquidated in 24 Hours",
                        'coin': coin,
                        'category': 'coin_liquidation',
                        'data': {'liq_24h': liq_24h},
                        'severity': 'medium',
                    })
    except Exception as e:
        log.debug(f"Per-coin liquidation check failed: {e}")

    # ── 3. Funding rate extreme (multi-coin) ──
    try:
        # Check top coins with significant futures volume
        funding_coins = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'AVAX', 'LINK', 'ADA']
        for coin in funding_coins:
            funding_rows = conn.execute(
                "SELECT rate, timestamp FROM funding_rates "
                "WHERE coin = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT 10",
                (coin, now_ts - 28800)
            ).fetchall()

            if not funding_rows:
                continue

            latest_rate = funding_rows[0][0]
            if latest_rate is None:
                continue

            cfg = _get_group_config(coin, FUNDING_THRESHOLDS)
            extreme_threshold = cfg['extreme']
            flip_magnitude = cfg['flip_magnitude']
            cooldown = cfg['cooldown']

            # Extreme funding
            if abs(latest_rate) >= extreme_threshold:
                direction = 'positive' if latest_rate > 0 else 'negative'
                trigger_id = f'funding_extreme_{direction}_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    pct = latest_rate * 100
                    who_pays = 'longs paying heavy' if latest_rate > 0 else 'shorts paying heavy'
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} funding rate extreme: {pct:+.3f}% — {who_pays}",
                        'headline': f"{coin} Funding Rate at {pct:+.3f}% — Squeeze Setup",
                        'coin': coin,
                        'category': 'funding_extreme',
                        'data': {'rate': latest_rate, 'direction': direction},
                        'severity': 'high' if abs(latest_rate) >= extreme_threshold * 2 else 'medium',
                    })

            # Funding flip
            if len(funding_rows) >= 3:
                oldest_rate = funding_rows[-1][0]
                if oldest_rate and latest_rate:
                    if (oldest_rate > flip_magnitude and latest_rate < -flip_magnitude) or \
                       (oldest_rate < -flip_magnitude and latest_rate > flip_magnitude):
                        trigger_id = f'funding_flip_{coin}'
                        if not is_on_cooldown(trigger_id, history, cooldown):
                            old_pct = oldest_rate * 100
                            new_pct = latest_rate * 100
                            fired.append({
                                'trigger': trigger_id,
                                'trigger_type': 'breaking_news',
                                'event': f"{coin} funding flipped from {old_pct:+.3f}% to {new_pct:+.3f}% — sentiment reversal",
                                'headline': f"{coin} Funding Rate Flipped — Sentiment Reversal",
                                'coin': coin,
                                'category': 'funding_flip',
                                'data': {'old_rate': oldest_rate, 'new_rate': latest_rate},
                                'severity': 'high',
                            })
    except Exception as e:
        log.debug(f"Funding trigger check failed: {e}")

    # ── 4. Open Interest crash/surge (multi-coin) ──
    try:
        oi_rows = conn.execute(
            "SELECT coin, oi_usd, change_pct_4h FROM cg_aggregated_oi "
            "WHERE timestamp > ? "
            "GROUP BY coin HAVING MAX(timestamp)",
            (now_ts - 7200,)
        ).fetchall()

        for row in oi_rows:
            coin = row[0]
            if get_coin_group(coin) == 'unknown':
                continue
            oi_change_4h = row[2]
            if oi_change_4h is None:
                continue

            cfg = _get_group_config(coin, OI_THRESHOLDS)
            crash_threshold = cfg.get('crash', -15)
            surge_threshold = cfg.get('surge', 25)
            cooldown = cfg.get('cooldown', 8)

            if oi_change_4h <= crash_threshold:
                trigger_id = f'oi_crash_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} Open Interest crashed {oi_change_4h:.1f}% in 4h — mass deleveraging",
                        'headline': f"{coin} OI Crashed {abs(oi_change_4h):.0f}% — Deleveraging Event",
                        'coin': coin,
                        'category': 'oi_crash',
                        'data': {'change_4h': oi_change_4h, 'oi_usd': row[1]},
                        'severity': 'high' if oi_change_4h <= crash_threshold * 1.5 else 'medium',
                    })
            elif oi_change_4h >= surge_threshold:
                trigger_id = f'oi_surge_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} Open Interest surged {oi_change_4h:.1f}% in 4h — leverage spike",
                        'headline': f"{coin} OI Surged {oi_change_4h:.0f}% — Leverage Building Fast",
                        'coin': coin,
                        'category': 'oi_surge',
                        'data': {'change_4h': oi_change_4h, 'oi_usd': row[1]},
                        'severity': 'high' if oi_change_4h >= surge_threshold * 1.5 else 'medium',
                    })
    except Exception as e:
        log.debug(f"OI trigger check failed: {e}")

    # ── 5. Long/Short ratio extreme (all tracked coins) ──
    try:
        ls_rows = conn.execute(
            "SELECT coin, long_ratio FROM long_short_ratio "
            "WHERE timestamp > ? AND ratio_type = 'global' "
            "GROUP BY coin HAVING MAX(timestamp)",
            (now_ts - 7200,)
        ).fetchall()

        for row in ls_rows:
            coin, long_ratio = row[0], row[1]
            if long_ratio is None or get_coin_group(coin) == 'unknown':
                continue

            long_pct = long_ratio * 100 if long_ratio <= 1 else long_ratio
            group = get_coin_group(coin)
            cfg = LS_THRESHOLDS.get(group, LS_THRESHOLDS.get('top_alts'))
            cooldown = cfg['cooldown']

            if long_pct >= cfg['extreme_long']:
                trigger_id = f'ls_extreme_long_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} at {long_pct:.0f}% longs — extreme crowding, squeeze risk",
                        'headline': f"{coin} {long_pct:.0f}% Long — Squeeze Risk Alert",
                        'coin': coin,
                        'category': 'ls_extreme',
                        'data': {'long_pct': long_pct, 'group': group},
                        'severity': 'high',
                    })
            elif long_pct <= cfg['extreme_short']:
                trigger_id = f'ls_extreme_short_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} at {100-long_pct:.0f}% shorts — extreme bearish positioning",
                        'headline': f"{coin} {100-long_pct:.0f}% Short — Extreme Bear Setup",
                        'coin': coin,
                        'category': 'ls_extreme',
                        'data': {'short_pct': 100 - long_pct, 'group': group},
                        'severity': 'high',
                    })
    except Exception as e:
        log.debug(f"L/S ratio trigger check failed: {e}")

    return fired


# ════════════════════════════════════════════
# 5. ON-CHAIN TRIGGERS (BTC metrics + institutional)
# ════════════════════════════════════════════

def check_onchain_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check on-chain metrics: SOPR, NUPL, MVRV, Puell, Coinbase premium, miner reserve."""
    fired = []
    now_ts = int(datetime.now().timestamp())

    # ── 1. BTC on-chain extremes (cq_btc_onchain) ──
    try:
        metrics = {}
        for metric_name in ['sopr', 'nupl', 'mvrv', 'puell_multiple']:
            row = conn.execute(
                "SELECT value FROM cq_btc_onchain "
                "WHERE metric = ? ORDER BY date DESC LIMIT 1",
                (metric_name,)
            ).fetchone()
            if row and row[0] is not None:
                metrics[metric_name] = row[0]

        T = ONCHAIN_THRESHOLDS
        cooldown = T['cooldown']

        # SOPR
        if 'sopr' in metrics:
            val = metrics['sopr']
            if val <= T['sopr_capitulation']:
                trigger_id = 'btc_sopr_capitulation'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC SOPR at {val:.3f} — holders selling at a loss, capitulation signal",
                        'headline': f"BTC SOPR at {val:.3f} — Capitulation Signal",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'sopr', 'value': val},
                        'severity': 'high',
                    })
            elif val >= T['sopr_euphoria']:
                trigger_id = 'btc_sopr_euphoria'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC SOPR at {val:.3f} — heavy profit-taking underway",
                        'headline': f"BTC SOPR at {val:.3f} — Profit-Taking Warning",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'sopr', 'value': val},
                        'severity': 'high',
                    })

        # NUPL
        if 'nupl' in metrics:
            val = metrics['nupl']
            if val <= T['nupl_capitulation']:
                trigger_id = 'btc_nupl_capitulation'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC NUPL at {val:.3f} — network in net loss, capitulation zone",
                        'headline': f"BTC Network in Net Loss — NUPL Capitulation Zone",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'nupl', 'value': val},
                        'severity': 'high',
                    })
            elif val >= T['nupl_euphoria']:
                trigger_id = 'btc_nupl_euphoria'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC NUPL at {val:.3f} — euphoria zone, cycle top risk",
                        'headline': f"BTC NUPL in Euphoria Zone — Cycle Top Risk",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'nupl', 'value': val},
                        'severity': 'high',
                    })

        # MVRV
        if 'mvrv' in metrics:
            val = metrics['mvrv']
            if val <= T['mvrv_undervalued']:
                trigger_id = 'btc_mvrv_undervalued'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC MVRV at {val:.2f} — extreme undervaluation, historical buy zone",
                        'headline': f"BTC MVRV at {val:.2f} — Historical Buy Zone",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'mvrv', 'value': val},
                        'severity': 'high',
                    })
            elif val >= T['mvrv_overvalued']:
                trigger_id = 'btc_mvrv_overvalued'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC MVRV at {val:.2f} — extreme overvaluation, correction risk",
                        'headline': f"BTC MVRV at {val:.2f} — Overvaluation Warning",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'mvrv', 'value': val},
                        'severity': 'high',
                    })

        # Puell Multiple
        if 'puell_multiple' in metrics:
            val = metrics['puell_multiple']
            if val <= T['puell_bottom']:
                trigger_id = 'btc_puell_bottom'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC Puell Multiple at {val:.2f} — miner revenue depleted, cycle bottom zone",
                        'headline': f"BTC Puell at {val:.2f} — Cycle Bottom Signal",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'puell_multiple', 'value': val},
                        'severity': 'high',
                    })
            elif val >= T['puell_top']:
                trigger_id = 'btc_puell_top'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"BTC Puell Multiple at {val:.2f} — extreme miner revenue, cycle top zone",
                        'headline': f"BTC Puell at {val:.2f} — Cycle Top Warning",
                        'coin': 'BTC',
                        'category': 'onchain_extreme',
                        'data': {'metric': 'puell_multiple', 'value': val},
                        'severity': 'high',
                    })
    except Exception as e:
        log.debug(f"BTC on-chain trigger check failed: {e}")

    # ── 2. Coinbase premium ──
    try:
        row = conn.execute(
            "SELECT premium_index FROM cq_coinbase_premium ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if row and row[0] is not None:
            premium = row[0]
            T = COINBASE_PREMIUM_THRESHOLDS
            cooldown = T['cooldown']

            if premium <= T['extreme_selling']:
                trigger_id = 'coinbase_premium_extreme_sell'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Coinbase premium at {premium:.3f} — extreme US institutional selling",
                        'headline': f"Coinbase Premium Crashes to {premium:.3f} — Institutional Panic",
                        'coin': 'BTC',
                        'category': 'coinbase_premium',
                        'data': {'premium': premium},
                        'severity': 'high',
                    })
            elif premium <= T['institutional_selling']:
                trigger_id = 'coinbase_premium_selling'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Coinbase premium at {premium:.3f} — US institutional selling pressure",
                        'headline': f"Coinbase Premium Negative — Institutions Selling",
                        'coin': 'BTC',
                        'category': 'coinbase_premium',
                        'data': {'premium': premium},
                        'severity': 'medium',
                    })
            elif premium >= T['extreme_fomo']:
                trigger_id = 'coinbase_premium_extreme_buy'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Coinbase premium at {premium:.3f} — extreme US institutional FOMO",
                        'headline': f"Coinbase Premium Surges to {premium:.3f} — Institutional FOMO",
                        'coin': 'BTC',
                        'category': 'coinbase_premium',
                        'data': {'premium': premium},
                        'severity': 'high',
                    })
            elif premium >= T['institutional_fomo']:
                trigger_id = 'coinbase_premium_buying'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"Coinbase premium at {premium:.3f} — US institutional buying pressure",
                        'headline': f"Coinbase Premium Positive — Institutions Buying",
                        'coin': 'BTC',
                        'category': 'coinbase_premium',
                        'data': {'premium': premium},
                        'severity': 'medium',
                    })
    except Exception as e:
        log.debug(f"Coinbase premium trigger check failed: {e}")

    # ── 3. Miner reserve ──
    try:
        miner_rows = conn.execute(
            "SELECT reserve, date FROM cq_miner_data ORDER BY date DESC LIMIT 8"
        ).fetchall()

        if len(miner_rows) >= 2:
            latest_reserve = miner_rows[0][0]
            # Find ~7 day ago reserve
            oldest_reserve = miner_rows[-1][0]
            if latest_reserve and oldest_reserve and oldest_reserve > 0:
                pct_change = ((latest_reserve - oldest_reserve) / oldest_reserve) * 100
                T = MINER_THRESHOLDS
                cooldown = T['cooldown']

                if pct_change <= T['capitulation_pct']:
                    trigger_id = 'miner_capitulation'
                    if not is_on_cooldown(trigger_id, history, cooldown):
                        btc_lost = oldest_reserve - latest_reserve
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"Miner reserves dropped {pct_change:.2f}% in 7 days ({btc_lost:.0f} BTC) — miner capitulation",
                            'headline': f"Miners Selling {btc_lost:.0f} BTC — Capitulation Signal",
                            'coin': 'BTC',
                            'category': 'miner_capitulation',
                            'data': {'pct_change': pct_change, 'btc_change': -btc_lost},
                            'severity': 'high' if pct_change <= T['capitulation_pct'] * 2 else 'medium',
                        })
                elif pct_change >= T['accumulation_pct']:
                    trigger_id = 'miner_accumulation'
                    if not is_on_cooldown(trigger_id, history, cooldown):
                        btc_added = latest_reserve - oldest_reserve
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"Miner reserves grew {pct_change:.2f}% in 7 days (+{btc_added:.0f} BTC) — miners accumulating",
                            'headline': f"Miners Accumulating {btc_added:.0f} BTC — Bullish Signal",
                            'coin': 'BTC',
                            'category': 'miner_accumulation',
                            'data': {'pct_change': pct_change, 'btc_change': btc_added},
                            'severity': 'medium',
                        })
    except Exception as e:
        log.debug(f"Miner trigger check failed: {e}")

    return fired


# ════════════════════════════════════════════
# 6. MARKET FLOW TRIGGERS
# ════════════════════════════════════════════

def check_market_flow_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check market flow signals: taker volume, exchange flows, stablecoin flows."""
    fired = []
    now_ts = int(datetime.now().timestamp())

    # ── 1. Taker buy/sell ratio extreme ──
    try:
        taker_rows = conn.execute(
            "SELECT coin, buy_sell_ratio FROM taker_volume "
            "WHERE timestamp > ? "
            "GROUP BY coin HAVING MAX(timestamp)",
            (now_ts - 7200,)
        ).fetchall()

        for row in taker_rows:
            coin, ratio = row[0], row[1]
            if ratio is None or get_coin_group(coin) == 'unknown':
                continue

            cfg = _get_group_config(coin, TAKER_THRESHOLDS)
            fomo_threshold = cfg['fomo']
            panic_threshold = cfg['panic']
            cooldown = cfg['cooldown']

            if ratio >= fomo_threshold:
                trigger_id = f'taker_fomo_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} taker buy ratio at {ratio:.2f}x — extreme FOMO buying",
                        'headline': f"{coin} Buy Pressure at {ratio:.1f}x — FOMO Alert",
                        'coin': coin,
                        'category': 'taker_extreme',
                        'data': {'ratio': ratio, 'direction': 'fomo'},
                        'severity': 'high' if ratio >= fomo_threshold * 1.3 else 'medium',
                    })
            elif ratio <= panic_threshold:
                trigger_id = f'taker_panic_{coin}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    sell_ratio = 1 / ratio if ratio > 0 else 999
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{coin} taker sell ratio at {sell_ratio:.1f}x — panic selling",
                        'headline': f"{coin} Panic Selling — Sell Pressure at {sell_ratio:.1f}x",
                        'coin': coin,
                        'category': 'taker_extreme',
                        'data': {'ratio': ratio, 'direction': 'panic'},
                        'severity': 'high' if ratio <= panic_threshold * 0.7 else 'medium',
                    })
    except Exception as e:
        log.debug(f"Taker volume trigger check failed: {e}")

    # ── 2. Exchange flows (CryptoQuant) ──
    try:
        flow_rows = conn.execute(
            "SELECT coin, netflow, reserve, reserve_usd FROM cq_exchange_flows "
            "ORDER BY date DESC"
        ).fetchall()

        # Deduplicate — keep latest per coin
        seen = set()
        for row in flow_rows:
            coin = row[0]
            if coin in seen:
                continue
            seen.add(coin)

            netflow = row[1]
            reserve = row[2]
            reserve_usd = row[3]

            if netflow is None or reserve is None or reserve <= 0:
                continue

            # Estimate USD value of netflow
            price_per_unit = reserve_usd / reserve if reserve_usd and reserve > 0 else 0
            netflow_usd = abs(netflow * price_per_unit) if price_per_unit > 0 else 0

            threshold = EXCHANGE_FLOW_THRESHOLDS.get(coin, EXCHANGE_FLOW_THRESHOLDS['default'])
            cooldown = EXCHANGE_FLOW_THRESHOLDS['cooldown']

            if netflow_usd >= threshold:
                if netflow > 0:
                    trigger_id = f'exchange_inflow_{coin}'
                    if not is_on_cooldown(trigger_id, history, cooldown):
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{coin} exchange inflow: {_fmt_usd(netflow_usd)} — selling pressure rising",
                            'headline': f"{_fmt_usd(netflow_usd)} {coin} Flowing to Exchanges — Sell Pressure",
                            'coin': coin,
                            'category': 'exchange_flow',
                            'data': {'netflow_usd': netflow_usd, 'direction': 'inflow'},
                            'severity': 'high' if netflow_usd >= threshold * 2 else 'medium',
                        })
                else:
                    trigger_id = f'exchange_outflow_{coin}'
                    if not is_on_cooldown(trigger_id, history, cooldown):
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{coin} exchange outflow: {_fmt_usd(netflow_usd)} — accumulation signal",
                            'headline': f"{_fmt_usd(netflow_usd)} {coin} Leaving Exchanges — Accumulation",
                            'coin': coin,
                            'category': 'exchange_flow',
                            'data': {'netflow_usd': netflow_usd, 'direction': 'outflow'},
                            'severity': 'high' if netflow_usd >= threshold * 2 else 'medium',
                        })
    except Exception as e:
        log.debug(f"Exchange flow trigger check failed: {e}")

    # ── 3. Stablecoin exchange flows ──
    try:
        row = conn.execute(
            "SELECT netflow FROM cq_stablecoin_flows ORDER BY date DESC LIMIT 1"
        ).fetchone()

        if row and row[0] is not None:
            netflow = row[0]
            T = STABLECOIN_FLOW_THRESHOLDS
            cooldown = T['cooldown']

            if netflow >= T['large_inflow']:
                trigger_id = 'stablecoin_exchange_inflow'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{_fmt_usd(netflow)} stablecoins flowed to exchanges — fresh buying power arriving",
                        'headline': f"{_fmt_usd(netflow)} Stablecoins Hit Exchanges — Buy Pressure Coming",
                        'category': 'stablecoin_flow',
                        'data': {'netflow': netflow, 'direction': 'inflow'},
                        'severity': 'high' if netflow >= T['large_inflow'] * 2 else 'medium',
                    })
            elif netflow <= T['large_outflow']:
                trigger_id = 'stablecoin_exchange_outflow'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{_fmt_usd(abs(netflow))} stablecoins left exchanges — capital fleeing",
                        'headline': f"{_fmt_usd(abs(netflow))} Stablecoins Leaving Exchanges — Capital Flight",
                        'category': 'stablecoin_flow',
                        'data': {'netflow': netflow, 'direction': 'outflow'},
                        'severity': 'high',
                    })
    except Exception as e:
        log.debug(f"Stablecoin flow trigger check failed: {e}")

    return fired


# ════════════════════════════════════════════
# 7. STRUCTURAL TRIGGERS (market structure)
# ════════════════════════════════════════════

def check_structural_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check market structure signals.

    Sub-checks:
      1. ETF flow extreme (multi-asset)
      2. BTC crossing round psychological levels
      3. Stablecoin supply surge
      4. Correlation break (BTC vs alts)
      5. Options max pain divergence
      6. Exchange balance change
    """
    fired = []
    now_ts = int(datetime.now().timestamp())

    # ── 1. ETF flow extreme (multi-asset) ──
    try:
        etf_rows = conn.execute(
            "SELECT date, flow_usd, asset FROM cg_etf_flows "
            "WHERE asset IN ('BTC', 'ETH') "
            "ORDER BY date DESC LIMIT 5"
        ).fetchall()

        seen_assets = set()
        for etf_row in etf_rows:
            asset = etf_row[2]
            if asset in seen_assets:
                continue
            seen_assets.add(asset)

            flow = etf_row[1]
            if flow is None:
                continue

            threshold = ETF_THRESHOLDS.get(asset, 500_000_000)
            cooldown = ETF_THRESHOLDS['cooldown']

            if abs(flow) >= threshold:
                direction = 'inflow' if flow > 0 else 'outflow'
                trigger_id = f'etf_extreme_{direction}_{asset}'
                if not is_on_cooldown(trigger_id, history, cooldown):
                    name = 'Bitcoin' if asset == 'BTC' else 'Ethereum'
                    action = 'institutions buying' if flow > 0 else 'institutions selling'
                    fired.append({
                        'trigger': trigger_id,
                        'trigger_type': 'breaking_news',
                        'event': f"{name} ETF {direction}: {_fmt_usd(abs(flow))} in single day — {action}",
                        'headline': f"{name} ETF {direction.title()} {_fmt_usd(abs(flow))} — {action.title()}",
                        'coin': asset,
                        'category': 'etf_extreme',
                        'data': {'flow': flow, 'asset': asset},
                        'severity': 'high' if abs(flow) >= threshold * 2 else 'medium',
                    })
    except Exception as e:
        log.debug(f"ETF trigger check failed: {e}")

    # ── 2. BTC crossing round levels ──
    try:
        btc_prices = conn.execute(
            "SELECT price_usd, timestamp FROM market_overview "
            "WHERE coin = 'BTC' AND timestamp > ? ORDER BY timestamp DESC LIMIT 2",
            (now_ts - 7200,)
        ).fetchall()

        if len(btc_prices) >= 2:
            current_price = btc_prices[0][0]
            prev_price = btc_prices[1][0]

            if current_price and prev_price and current_price > 0 and prev_price > 0:
                round_levels = list(range(30000, 200001, 10000))

                for level in round_levels:
                    if prev_price < level <= current_price:
                        trigger_id = f'btc_cross_up_{level}'
                        if not is_on_cooldown(trigger_id, history, 48):
                            level_str = f"${level//1000}K"
                            fired.append({
                                'trigger': trigger_id,
                                'trigger_type': 'breaking_news',
                                'event': f"Bitcoin breaks above {level_str} — key psychological level reclaimed",
                                'headline': f"Bitcoin Breaks {level_str} — Key Level Reclaimed",
                                'coin': 'BTC',
                                'category': 'btc_level_cross',
                                'data': {'level': level, 'direction': 'up', 'price': current_price},
                                'severity': 'high',
                            })
                            break
                    elif prev_price > level >= current_price:
                        trigger_id = f'btc_cross_down_{level}'
                        if not is_on_cooldown(trigger_id, history, 48):
                            level_str = f"${level//1000}K"
                            fired.append({
                                'trigger': trigger_id,
                                'trigger_type': 'breaking_news',
                                'event': f"Bitcoin loses {level_str} — critical support broken",
                                'headline': f"Bitcoin Loses {level_str} — Support Broken",
                                'coin': 'BTC',
                                'category': 'btc_level_cross',
                                'data': {'level': level, 'direction': 'down', 'price': current_price},
                                'severity': 'high',
                            })
                            break
    except Exception as e:
        log.debug(f"BTC level trigger check failed: {e}")

    # ── 3. Stablecoin supply surge ──
    try:
        stable_rows = conn.execute(
            "SELECT date, total_market_cap FROM cg_stablecoin_supply "
            "ORDER BY date DESC LIMIT 2"
        ).fetchall()

        if len(stable_rows) >= 2:
            current_cap = stable_rows[0][1]
            prev_cap = stable_rows[1][1]
            if current_cap and prev_cap:
                change = current_cap - prev_cap
                if abs(change) >= 500_000_000:
                    direction = 'minted' if change > 0 else 'burned'
                    trigger_id = f'stablecoin_supply_{direction}'
                    if not is_on_cooldown(trigger_id, history, 24):
                        impact = 'fresh capital entering crypto' if change > 0 else 'capital leaving crypto'
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{_fmt_usd(abs(change))} stablecoins {direction} in one day — {impact}",
                            'headline': f"{_fmt_usd(abs(change))} Stablecoins {direction.title()} — {impact.title()}",
                            'category': 'stablecoin_supply',
                            'data': {'change': change, 'direction': direction},
                            'severity': 'high' if abs(change) >= 1e9 else 'medium',
                        })
    except Exception as e:
        log.debug(f"Stablecoin supply trigger check failed: {e}")

    # ── 4. Correlation break (BTC vs alts) ──
    try:
        changes = conn.execute(
            "SELECT coin, change_24h FROM market_overview "
            "WHERE timestamp > ? AND coin IN ('BTC','ETH','SOL','BNB','XRP','ADA','AVAX','DOGE','LINK','DOT') "
            "GROUP BY coin HAVING MAX(timestamp)",
            (now_ts - 7200,)
        ).fetchall()

        if len(changes) >= 4:
            change_map = {r[0]: r[1] for r in changes if r[1] is not None}
            btc_change = change_map.get('BTC')

            if btc_change is not None:
                alt_changes = [v for k, v in change_map.items() if k != 'BTC']
                if alt_changes:
                    avg_alt = sum(alt_changes) / len(alt_changes)
                    divergence = btc_change - avg_alt

                    if abs(divergence) >= 8:
                        if btc_change > 0 and avg_alt < -2:
                            trigger_id = 'correlation_break_btc_up'
                            desc = f"BTC up {btc_change:+.1f}% but alts down {avg_alt:.1f}% — capital rotating to Bitcoin"
                        elif btc_change < 0 and avg_alt > 2:
                            trigger_id = 'correlation_break_alts_up'
                            desc = f"BTC down {btc_change:.1f}% but alts up {avg_alt:+.1f}% — alt season signal"
                        elif btc_change > 0 and avg_alt > 0 and divergence > 8:
                            trigger_id = 'correlation_break_btc_leads'
                            desc = f"BTC outperforming alts by {divergence:.1f}% — dominance rising"
                        else:
                            trigger_id = 'correlation_break_alts_lead'
                            desc = f"Alts outperforming BTC by {abs(divergence):.1f}% — risk-on rotation"

                        if not is_on_cooldown(trigger_id, history, 24):
                            fired.append({
                                'trigger': trigger_id,
                                'trigger_type': 'breaking_news',
                                'event': desc,
                                'headline': desc.split(' — ')[1] if ' — ' in desc else desc[:50],
                                'coin': 'BTC',
                                'category': 'correlation_break',
                                'data': {'btc_change': btc_change, 'avg_alt': avg_alt, 'divergence': divergence},
                                'severity': 'high' if abs(divergence) >= 12 else 'medium',
                            })
    except Exception as e:
        log.debug(f"Correlation trigger check failed: {e}")

    # ── 5. Options max pain divergence ──
    try:
        for asset in ['BTC', 'ETH']:
            # Get current price
            price_row = conn.execute(
                "SELECT price_usd FROM market_overview "
                "WHERE coin = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT 1",
                (asset, now_ts - 7200)
            ).fetchone()

            if not price_row or not price_row[0]:
                continue
            current_price = price_row[0]

            # Get nearest major expiry max pain
            mp_row = conn.execute(
                "SELECT max_pain, expiry_date, call_oi, put_oi FROM cg_options_max_pain "
                "WHERE coin = ? AND expiry_date >= date('now') "
                "ORDER BY (call_oi + put_oi) DESC LIMIT 1",
                (asset,)
            ).fetchone()

            if not mp_row or not mp_row[0]:
                continue

            max_pain = mp_row[0]
            expiry = mp_row[1]

            if max_pain > 0 and current_price > 0:
                divergence_pct = ((current_price - max_pain) / max_pain) * 100

                if abs(divergence_pct) >= OPTIONS_THRESHOLDS['divergence_pct']:
                    direction = 'above' if divergence_pct > 0 else 'below'
                    trigger_id = f'options_maxpain_{direction}_{asset}'
                    if not is_on_cooldown(trigger_id, history, OPTIONS_THRESHOLDS['cooldown']):
                        name = 'Bitcoin' if asset == 'BTC' else 'Ethereum'
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{name} {abs(divergence_pct):.0f}% {direction} options max pain (${max_pain:,.0f}) — dealers will hedge",
                            'headline': f"{name} {abs(divergence_pct):.0f}% From Max Pain — Options Pressure",
                            'coin': asset,
                            'category': 'options_maxpain',
                            'data': {'price': current_price, 'max_pain': max_pain, 'divergence_pct': divergence_pct, 'expiry': expiry},
                            'severity': 'high' if abs(divergence_pct) >= OPTIONS_THRESHOLDS['divergence_pct'] * 1.5 else 'medium',
                        })
    except Exception as e:
        log.debug(f"Options max pain trigger check failed: {e}")

    # ── 6. Exchange balance change ──
    try:
        for asset in ['BTC', 'ETH']:
            bal_rows = conn.execute(
                "SELECT exchange, balance, change_pct_1d FROM cg_exchange_balance "
                "WHERE coin = ? AND timestamp > ? "
                "ORDER BY timestamp DESC",
                (asset, now_ts - 86400)
            ).fetchall()

            # Deduplicate by exchange
            seen_ex = set()
            for row in bal_rows:
                exchange = row[0]
                if exchange in seen_ex:
                    continue
                seen_ex.add(exchange)

                change_1d = row[2]
                if change_1d is None:
                    continue

                threshold = EXCHANGE_BALANCE_THRESHOLDS['large_change_pct']
                cooldown = EXCHANGE_BALANCE_THRESHOLDS['cooldown']

                if abs(change_1d) >= threshold:
                    direction = 'dropped' if change_1d < 0 else 'surged'
                    trigger_id = f'exchange_balance_{direction}_{asset}_{exchange}'
                    if not is_on_cooldown(trigger_id, history, cooldown):
                        name = 'Bitcoin' if asset == 'BTC' else 'Ethereum'
                        signal = 'whale accumulation' if change_1d < 0 else 'potential sell-off incoming'
                        fired.append({
                            'trigger': trigger_id,
                            'trigger_type': 'breaking_news',
                            'event': f"{name} balance on {exchange} {direction} {abs(change_1d):.1f}% in 24h — {signal}",
                            'headline': f"{name} on {exchange} {direction.title()} {abs(change_1d):.0f}% — {signal.title()}",
                            'coin': asset,
                            'category': 'exchange_balance',
                            'data': {'exchange': exchange, 'change_1d': change_1d, 'balance': row[1]},
                            'severity': 'high' if abs(change_1d) >= threshold * 2 else 'medium',
                        })
    except Exception as e:
        log.debug(f"Exchange balance trigger check failed: {e}")

    return fired


# ════════════════════════════════════════════
# 7B. CORRELATION BREAK TRIGGERS
# ════════════════════════════════════════════

# Correlation break thresholds
CORRELATION_BREAK_THRESHOLDS = {
    'drop_threshold': 0.3,     # Min correlation drop (30d→7d) to trigger
    'cooldown': 48,            # Hours between alerts for same coin
}


def check_correlation_break_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check for coins that have broken correlation with BTC.

    Uses CorrelationAnalyzer to compute 7d vs 30d rolling correlation
    for each coin vs BTC. A drop > 0.3 triggers an alert.

    This catches coins making independent moves — often precedes
    significant price action (rally or crash independent of BTC).
    """
    fired = []

    try:
        from src.crypto.correlation_analyzer import CorrelationAnalyzer

        analyzer = CorrelationAnalyzer(conn=conn)
        breaks = analyzer.detect_correlation_breaks(
            short_window=7,
            long_window=30,
            threshold=CORRELATION_BREAK_THRESHOLDS['drop_threshold'],
        )

        cooldown = CORRELATION_BREAK_THRESHOLDS['cooldown']

        for brk in breaks:
            coin = brk['coin']
            corr_7d = brk['corr_7d']
            corr_30d = brk['corr_30d']
            drop = brk['drop']

            trigger_id = f'correlation_break_{coin}'
            if is_on_cooldown(trigger_id, history, cooldown):
                continue

            # Build descriptive event string
            hist_ctx = brk.get('historical_context', '')
            event = (
                f"{coin} decoupled from BTC — correlation crashed from "
                f"{corr_30d:.2f} to {corr_7d:.2f} (drop: {abs(drop):.2f})"
            )
            if hist_ctx:
                event += f". {hist_ctx}"

            headline = f"{coin} Breaks Free From BTC — Correlation Crash Alert"

            severity = 'high' if abs(drop) > 0.5 else 'medium'

            fired.append({
                'trigger': trigger_id,
                'trigger_type': 'breaking_news',
                'event': event,
                'headline': headline,
                'coin': coin,
                'category': 'correlation_break_deep',
                'data': {
                    'corr_7d': corr_7d,
                    'corr_30d': corr_30d,
                    'drop': drop,
                    'historical_context': hist_ctx,
                },
                'severity': severity,
            })

    except Exception as e:
        log.debug(f"Correlation break trigger check failed: {e}")

    return fired


# ════════════════════════════════════════════
# 8. NEWS TRIGGERS (Claude Haiku evaluation)
# ════════════════════════════════════════════

def _init_news_evaluations_table(conn: sqlite3.Connection):
    """Create news_evaluations table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS news_evaluations (
            news_hash TEXT PRIMARY KEY,
            title TEXT,
            source TEXT,
            impact_score INTEGER,
            category TEXT,
            summary TEXT,
            urgency TEXT,
            evaluated_at TEXT,
            triggered INTEGER DEFAULT 0
        )
    """)
    conn.commit()


def _hash_news(title: str) -> str:
    """Create a dedup hash from news title (normalized)."""
    normalized = title.lower().strip()
    for prefix in ['breaking:', 'just in:', 'update:', 'report:']:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
    return hashlib.md5(normalized.encode()).hexdigest()[:16]


def check_news_triggers(conn: sqlite3.Connection, history: dict) -> list:
    """Check news for breaking events that warrant an immediate short.

    Uses Claude Haiku to evaluate recent unprocessed news articles.
    Triggers if impact_score >= NEWS_IMPACT_THRESHOLD (7).

    Categories: regulatory, hack_exploit, macro, depeg, institutional, black_swan
    """
    fired = []
    _init_news_evaluations_table(conn)

    cutoff_ts = int((datetime.now() - timedelta(hours=NEWS_MAX_AGE_HOURS)).timestamp())
    news_rows = conn.execute(
        "SELECT title, source, url, timestamp, sentiment, coins_mentioned "
        "FROM news WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 30",
        (cutoff_ts,)
    ).fetchall()

    if not news_rows:
        return fired

    unevaluated = []
    for row in news_rows:
        title = row[0]
        news_hash = _hash_news(title)
        existing = conn.execute(
            "SELECT 1 FROM news_evaluations WHERE news_hash = ?", (news_hash,)
        ).fetchone()
        if not existing:
            unevaluated.append({
                'title': title,
                'source': row[1],
                'url': row[2],
                'timestamp': row[3],
                'sentiment': row[4],
                'coins': row[5],
                'hash': news_hash,
            })

    if not unevaluated:
        log.debug("No new unprocessed news articles")
        return fired

    log.info(f"Evaluating {len(unevaluated)} new articles for breaking news triggers...")

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        log.warning("No ANTHROPIC_API_KEY — cannot evaluate news impact")
        return fired

    news_list = "\n".join(
        f"[{i+1}] ({n['source']}) {n['title']}"
        for i, n in enumerate(unevaluated)
    )

    # Get current market context for better evaluation
    market_context = ""
    try:
        btc_row = conn.execute(
            "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        fg_row = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
        liq_row = conn.execute(
            "SELECT SUM(notional_usd) FROM liquidations WHERE timestamp > ?",
            (int((datetime.now() - timedelta(hours=24)).timestamp()),)
        ).fetchone()
        market_context = f"""
CURRENT MARKET STATE:
- BTC price: ${btc_row[0]:,.0f}
- Fear & Greed: {fg_row[0] if fg_row else '?'}/100
- 24h liquidations: ${(liq_row[0] or 0)/1e6:.0f}M
"""
    except Exception:
        pass

    prompt = f"""You are a senior crypto market analyst at a trading desk. Your job: decide if a news headline warrants BREAKING COVERAGE on our YouTube channel.

We publish 3 scheduled shorts + 1 daily brief per day. Breaking news is RARE — max 1-2/day, ONLY for events that genuinely shake the market.
{market_context}
NEWS HEADLINES (last {NEWS_MAX_AGE_HOURS} hours):
{news_list}

For EACH headline, rate:
- impact_score: 1-10
- category: one of [regulatory, hack_exploit, macro, depeg, institutional, black_swan, market_event, none]
- urgency: high / medium / low

STRICT SCORING (most news is 2-4):
- 9-10: Exchange hack >$500M, stablecoin depeg, country-wide ban, ETF approval/rejection, >$1B liquidation cascade, Fed emergency rate change
- 7-8: SEC lawsuit against top-5 exchange, major protocol exploit >$50M, BTC ETF daily flow >$1B, Fortune 500 company buys/sells BTC
- 5-6: SEC investigation, network upgrade, major partnership, ETF flow $200-500M, exchange listing
- 3-4: Analyst predictions, routine updates, partnerships, price targets, opinions
- 1-2: Recycled stories, opinion pieces, "what if" articles, price commentary

CRITICAL: Score 9+ is EXTREMELY RARE (happens ~2-3 times per month). If you're unsure between 8 and 9, choose 8. We ONLY want to interrupt scheduled content for truly significant events.

Respond in JSON array ONLY:
[{{"index": 1, "impact_score": 3, "category": "none", "urgency": "low", "summary": "brief 10-word summary"}}]"""

    try:
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 2000,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        result_text = resp.json()['content'][0]['text'].strip()

        if result_text.startswith('```'):
            result_text = result_text.split('\n', 1)[1].rsplit('```', 1)[0].strip()

        evaluations = json.loads(result_text)

    except Exception as e:
        log.error(f"Claude news evaluation failed: {e}")
        for n in unevaluated:
            conn.execute(
                "INSERT OR IGNORE INTO news_evaluations (news_hash, title, source, impact_score, "
                "category, summary, urgency, evaluated_at) VALUES (?, ?, ?, 0, 'error', ?, 'low', ?)",
                (n['hash'], n['title'], n['source'], str(e),
                 datetime.now(timezone.utc).isoformat())
            )
        conn.commit()
        return fired

    for ev in evaluations:
        idx = ev.get('index', 0) - 1
        if idx < 0 or idx >= len(unevaluated):
            continue

        news = unevaluated[idx]
        impact = ev.get('impact_score', 0)
        category = ev.get('category', 'none')
        urgency = ev.get('urgency', 'low')
        summary = ev.get('summary', news['title'][:80])

        conn.execute(
            "INSERT OR IGNORE INTO news_evaluations "
            "(news_hash, title, source, impact_score, category, summary, urgency, evaluated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (news['hash'], news['title'], news['source'], impact, category,
             summary, urgency, datetime.now(timezone.utc).isoformat())
        )

        if impact >= NEWS_IMPACT_THRESHOLD and urgency == 'high':
            trigger_id = f"breaking_news_{category}"
            if not is_on_cooldown(trigger_id, history, NEWS_COOLDOWN_HOURS):
                conn.execute(
                    "UPDATE news_evaluations SET triggered = 1 WHERE news_hash = ?",
                    (news['hash'],)
                )
                fired.append({
                    'trigger': trigger_id,
                    'trigger_type': 'breaking_news',
                    'event': summary,
                    'headline': news['title'][:100],
                    'news_title': news['title'],
                    'news_source': news['source'],
                    'news_url': news.get('url', ''),
                    'impact_score': impact,
                    'category': category,
                    'coins': news.get('coins', ''),
                    'severity': 'critical' if impact >= 9 else 'high',
                })
                log.info(f"  BREAKING NEWS [{impact}/10]: {summary} ({category})")

    conn.commit()
    return fired


# ════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════

def _apply_rate_limit(fired: list, history: dict) -> list:
    """Apply strict rate limits: max 1/hour, max 2/day for breaking news.

    Only truly significant events should produce breaking news shorts.
    Regular content comes from scheduled micro_shorts (3/day) + daily_brief (1/day).
    """
    # Hourly limit
    recent_1h = _count_recent_alerts(history, hours=1.0)
    if recent_1h >= MAX_ALERTS_PER_HOUR:
        log.info(f"  Rate limit: {recent_1h} alerts in last hour (max {MAX_ALERTS_PER_HOUR})")
        return []

    # Daily limit (24h rolling window)
    recent_24h = _count_recent_alerts(history, hours=24.0)
    if recent_24h >= MAX_BREAKING_PER_DAY:
        log.info(f"  Daily limit: {recent_24h} breaking news in last 24h (max {MAX_BREAKING_PER_DAY})")
        return []

    remaining = min(MAX_ALERTS_PER_HOUR - recent_1h, MAX_BREAKING_PER_DAY - recent_24h)

    if not fired:
        return []

    # Only allow high/critical severity through (filter out medium)
    significant = [t for t in fired if t.get('severity') in ('critical', 'high')]
    if not significant:
        log.info(f"  {len(fired)} triggers fired but none are high/critical severity — skipping")
        return []

    log.info(f"  {len(significant)} significant triggers, {remaining} slots available")
    return significant[:remaining]


def check_all_triggers() -> list:
    """Run all trigger checks and return list of fired triggers."""
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    history = load_alert_history()

    all_fired = []

    # Price triggers (all 28 coins)
    price_alerts = check_price_triggers(conn, history)
    all_fired.extend(price_alerts)

    # Fear & Greed triggers
    fg_alerts = check_fear_greed_triggers(conn, history)
    all_fired.extend(fg_alerts)

    # Whale triggers
    whale_alerts = check_whale_triggers(conn, history)
    all_fired.extend(whale_alerts)

    # Derivatives triggers (liquidations, funding, OI, L/S — multi-coin)
    deriv_alerts = check_derivatives_triggers(conn, history)
    all_fired.extend(deriv_alerts)

    # On-chain triggers (SOPR, NUPL, MVRV, Puell, Coinbase premium, miner)
    onchain_alerts = check_onchain_triggers(conn, history)
    all_fired.extend(onchain_alerts)

    # Market flow triggers (taker volume, exchange flows, stablecoin flows)
    flow_alerts = check_market_flow_triggers(conn, history)
    all_fired.extend(flow_alerts)

    # Structural triggers (ETF, BTC levels, stablecoin supply, correlation, options, exchange balance)
    struct_alerts = check_structural_triggers(conn, history)
    all_fired.extend(struct_alerts)

    # Correlation break triggers (7d vs 30d rolling correlation — deep analysis)
    corr_break_alerts = check_correlation_break_triggers(conn, history)
    all_fired.extend(corr_break_alerts)

    # Breaking news triggers (Claude Haiku evaluation)
    news_alerts = check_news_triggers(conn, history)
    all_fired.extend(news_alerts)

    conn.close()

    # Sort by severity (critical > high > medium)
    severity_order = {'critical': 0, 'high': 1, 'medium': 2}
    all_fired.sort(key=lambda x: severity_order.get(x.get('severity', 'medium'), 2))

    # Apply rate limit
    all_fired = _apply_rate_limit(all_fired, history)

    return all_fired


def check_fast_triggers() -> list:
    """Run fast trigger checks (every 5 min): derivatives + on-chain + flows + structural + news.

    These are the time-sensitive triggers that produce immediate shorts.
    Price/F&G/whale checks run separately every 30 min.
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    history = load_alert_history()

    all_fired = []

    # Derivatives (quantitative — no API calls, instant)
    deriv_alerts = check_derivatives_triggers(conn, history)
    all_fired.extend(deriv_alerts)

    # On-chain (DB reads only, instant)
    onchain_alerts = check_onchain_triggers(conn, history)
    all_fired.extend(onchain_alerts)

    # Market flows (DB reads only, instant)
    flow_alerts = check_market_flow_triggers(conn, history)
    all_fired.extend(flow_alerts)

    # Structural (quantitative — no API calls, instant)
    struct_alerts = check_structural_triggers(conn, history)
    all_fired.extend(struct_alerts)

    # Correlation break triggers (DB reads only, instant)
    corr_break_alerts = check_correlation_break_triggers(conn, history)
    all_fired.extend(corr_break_alerts)

    # Breaking news (Claude Haiku — ~5sec API call)
    news_alerts = check_news_triggers(conn, history)
    all_fired.extend(news_alerts)

    conn.close()

    # Sort by severity
    severity_order = {'critical': 0, 'high': 1, 'medium': 2}
    all_fired.sort(key=lambda x: severity_order.get(x.get('severity', 'medium'), 2))

    # Apply rate limit
    all_fired = _apply_rate_limit(all_fired, history)

    return all_fired


def trigger_signal_alert(alert: dict, dry_run: bool = False):
    """Trigger a Signal Alert video production."""
    event = alert['event']
    coin = alert.get('coin')

    log.info(f"  TRIGGERING Signal Alert: {event}")

    if dry_run:
        log.info("  [DRY RUN] Would trigger: produce_crypto.py signal_alert")
        log.info(f"    --event=\"{event}\"")
        if coin:
            log.info(f"    --coin={coin}")
        return

    from src.crypto.produce_crypto import produce
    try:
        result = produce(
            video_type='signal_alert',
            event=event,
            coin=coin,
            skip_collect=False,
        )
        log.info(f"  Signal Alert produced: {result}")
    except Exception as e:
        log.error(f"  Signal Alert production failed: {e}")


def run_check(dry_run: bool = False) -> list:
    """Run a single check cycle."""
    log.info("Checking signal triggers...")

    if not dry_run:
        try:
            from src.crypto.data_collector import collect_all
            log.info("  Refreshing market data...")
            collect_all()
        except Exception as e:
            log.warning(f"  Data refresh failed: {e}")

    fired = check_all_triggers()

    if not fired:
        log.info("  No triggers fired. Market is calm.")
        return []

    log.info(f"  {len(fired)} trigger(s) fired!")
    history = load_alert_history()

    produced_count = 0
    for alert in fired:
        alert['timestamp'] = datetime.now(timezone.utc).isoformat()
        log.info(f"\n  ALERT [{alert.get('severity', '?').upper()}]: {alert['event']}")

        # Produce video for top alerts (up to MAX_STANDALONE_PRODUCTIONS)
        if produced_count < MAX_STANDALONE_PRODUCTIONS:
            trigger_signal_alert(alert, dry_run=dry_run)
            produced_count += 1
        else:
            log.info(f"  [SKIPPED] Production limit reached ({MAX_STANDALONE_PRODUCTIONS}). Logged only.")

        save_alert(alert, history)

    return fired


def run_daemon(dry_run: bool = False):
    """Run as a continuous daemon, checking every CHECK_INTERVAL_MIN minutes."""
    log.info(f"Signal Monitor daemon started (checking every {CHECK_INTERVAL_MIN} min)")
    log.info("Press Ctrl+C to stop\n")

    while True:
        try:
            run_check(dry_run=dry_run)
        except Exception as e:
            log.error(f"Check cycle failed: {e}")

        log.info(f"Next check in {CHECK_INTERVAL_MIN} minutes...")
        time.sleep(CHECK_INTERVAL_MIN * 60)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX — Signal Monitor v2')
    parser.add_argument('--daemon', action='store_true',
                        help=f'Run continuously (check every {CHECK_INTERVAL_MIN} min)')
    parser.add_argument('--news-check', action='store_true',
                        help='Fast triggers: derivatives + on-chain + flows + structural + news (5-min interval)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check triggers but don\'t produce videos')

    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Signal Monitor v2")
    log.info("=" * 60)

    if args.news_check:
        fired = check_fast_triggers()
        if fired:
            print(f"\n{len(fired)} fast trigger(s) fired!")
            for a in fired:
                severity = a.get('severity', '?').upper()
                category = a.get('category', 'unknown')
                print(f"  [{severity}] ({category}) {a['event']}")
                if not args.dry_run:
                    trigger_signal_alert(a, dry_run=False)
                    history = load_alert_history()
                    a['timestamp'] = datetime.now(timezone.utc).isoformat()
                    save_alert(a, history)
        else:
            print("No fast triggers detected.")
    elif args.daemon:
        run_daemon(dry_run=args.dry_run)
    else:
        fired = run_check(dry_run=args.dry_run)
        if fired:
            print(f"\n{len(fired)} alert(s) triggered!")
            for a in fired:
                print(f"  [{a.get('severity', '?').upper()}] {a['event']}")
        else:
            print("\nNo alerts triggered. Market conditions normal.")
