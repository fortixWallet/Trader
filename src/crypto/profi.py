"""
PROFI — Claude Trading Agent (v2)
==================================
Expert trader powered by Claude Sonnet with:
- Vision (chart analysis on every trade)
- Prompt Caching (96% cost savings on knowledge base)
- Adaptive Thinking (deep analysis before decisions)
- Tool Use (agentic data gathering)
- Structured Outputs (guaranteed JSON)

Knowledge: profi_knowledge.md (complete trading education)
"""

import os
import json
import time
import base64
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import anthropic

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
KNOWLEDGE_PATH = Path(__file__).parent / 'profi_knowledge.md'
COIN_KNOWLEDGE_PATH = Path(__file__).parent / 'profi_coin_knowledge.md'
ADVANCED_KNOWLEDGE_PATH = Path(__file__).parent / 'profi_advanced_knowledge.md'
LESSONS_PATH = Path(__file__).parent / 'profi_lessons.md'
HISTORY_LESSONS_PATH = Path(__file__).parent / 'profi_history_lessons.md'

MODEL = "claude-opus-4-6"  # Opus for all decisions — deepest thinking
MODEL_DAILY = "claude-opus-4-6"

# Tools that Profi can use to gather data
TRADING_TOOLS = [
    {
        "name": "get_price",
        "description": "Get current price and 24h change for a coin",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string", "description": "Coin symbol, e.g. 'SOL'"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_funding_rate",
        "description": "Get current funding rate. Positive = longs pay shorts, negative = shorts pay longs. Extreme (>0.05%) signals potential reversal.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_orderbook_imbalance",
        "description": "Get orderbook bid/ask imbalance. Positive = more bids (bullish pressure), negative = more asks (bearish pressure).",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_liquidations",
        "description": "Get recent liquidation data. Returns long_liq and short_liq USD amounts in last 4h.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_fear_greed",
        "description": "Get current Fear & Greed index (0-100). <20 = extreme fear, >80 = extreme greed.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_onchain",
        "description": "Get BTC on-chain metrics: MVRV (>3.7=overvalued, <1=undervalued), SOPR (<1=selling at loss=near bottom), NUPL (>0.75=euphoria, <0=capitulation), Puell Multiple, Coinbase premium.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_macro",
        "description": "Get macro context: DXY (USD strength — high=bearish for crypto), ETF flows (BTC+ETH — positive=institutional buying), stablecoin supply change, open interest change.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_whale_activity",
        "description": "Get recent whale transactions (>$1M). Large transfers TO exchange = potential sell pressure. FROM exchange = accumulation.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_options_data",
        "description": "Get options max pain for BTC and ETH. Price tends to gravitate toward max pain at expiry. Also returns put/call OI ratio.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_market_snapshot",
        "description": "Full market snapshot: BTC price+change, top gainers/losers 24h, total market breadth, current regime, our account P&L today.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_crowd_positioning",
        "description": "Get long/short ratio + taker buy/sell volume + open interest change for a coin. Shows if crowd is overleveraged in one direction (contrarian signal). High long_ratio + rising OI = crowded long = dump risk.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_exchange_flows",
        "description": "Get exchange balance and net flow for a coin. Coins flowing TO exchanges = sell pressure. Coins flowing FROM exchanges = accumulation. Also shows social sentiment if available.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_trading_calendar",
        "description": "Get current time context: UTC hour, day of week, upcoming macro events (FOMC, CPI, NFP), market sessions (Asia/Europe/US), holidays, options expiry. ALWAYS call this before opening a position.",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_historical_patterns",
        "description": "Find historical chart moments that look IDENTICAL to current chart shape for a coin. Searches across ALL coins and 2+ years of data. Returns what happened AFTER those similar patterns (went up or down, by how much). Use this to predict the most likely future move.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string", "description": "Coin to match current pattern for"}
            },
            "required": ["coin"]
        }
    },
]


class Profi:
    """Claude-powered expert trader with full API capabilities."""

    def __init__(self, exchange=None):
        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        self._client = anthropic.Anthropic(
            api_key=api_key,
            timeout=120.0,     # 120s for multi-image requests
            max_retries=3,
        ) if api_key else None
        self._exchange = exchange  # live exchange for real-time prices
        self._knowledge = self._load_knowledge()
        self._daily_strategy = None
        self._daily_strategy_time = 0
        self._call_count = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._cache_hits = 0

    def _load_knowledge(self) -> str:
        """Load ALL knowledge: theory + coins + stats + lessons from own mistakes."""
        parts = []
        for path in [KNOWLEDGE_PATH, COIN_KNOWLEDGE_PATH, ADVANCED_KNOWLEDGE_PATH,
                     HISTORY_LESSONS_PATH, LESSONS_PATH]:
            try:
                text = path.read_text()
                if text.strip():
                    parts.append(text)
            except Exception:
                pass
        if not parts:
            parts.append("Expert crypto trader knowledge.")
        return "\n\n".join(parts)

    def _system_with_cache(self) -> list:
        """System prompt with prompt caching on knowledge base.
        First call: writes to cache (+25% cost).
        All subsequent calls within 5 min: reads from cache (-90% cost).
        """
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
                    f"YOUR TRADING KNOWLEDGE:\n{self._knowledge}"
                ),
                "cache_control": {"type": "ephemeral"}
            }
        ]

    def _execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Execute a tool call from Claude and return the result."""
        try:
            conn = sqlite3.connect(str(DB_PATH))

            if tool_name == "get_price":
                coin = tool_input["coin"]
                # LIVE price from exchange (not stale DB)
                if self._exchange:
                    try:
                        ticker = self._exchange.get_ticker(coin)
                        price = ticker['price']
                        # 24h change from DB (ok to be slightly stale)
                        prev = conn.execute(
                            "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                            "ORDER BY timestamp DESC LIMIT 1 OFFSET 6", (coin,)
                        ).fetchone()
                        change_24h = ((price / prev[0]) - 1) * 100 if prev and prev[0] else 0
                        conn.close()
                        return json.dumps({"price": price, "change_24h": round(change_24h, 2),
                                         "source": "LIVE"})
                    except Exception:
                        pass
                # Fallback to DB
                row = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "ORDER BY timestamp DESC LIMIT 1", (coin,)
                ).fetchone()
                prev = conn.execute(
                    "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                    "ORDER BY timestamp DESC LIMIT 1 OFFSET 6", (coin,)
                ).fetchone()
                price = row[0] if row else 0
                change_24h = ((price / prev[0]) - 1) * 100 if prev and prev[0] else 0
                conn.close()
                return json.dumps({"price": price, "change_24h": round(change_24h, 2),
                                 "source": "DB_4h"})

            elif tool_name == "get_funding_rate":
                coin = tool_input["coin"]
                row = conn.execute(
                    "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                    (coin,)
                ).fetchone()
                conn.close()
                rate = row[0] if row and row[0] else 0
                return json.dumps({"funding_rate": rate, "annualized": round(rate * 3 * 365 * 100, 1)})

            elif tool_name == "get_orderbook_imbalance":
                coin = tool_input["coin"]
                row = conn.execute(
                    "SELECT imbalance_score FROM orderbook_imbalance WHERE coin=? "
                    "ORDER BY timestamp DESC LIMIT 1", (coin,)
                ).fetchone()
                conn.close()
                return json.dumps({"imbalance": row[0] if row and row[0] else 0})

            elif tool_name == "get_liquidations":
                coin = tool_input["coin"]
                row = conn.execute(
                    "SELECT long_liq_usd_4h, short_liq_usd_4h FROM cg_liquidations "
                    "WHERE coin=? ORDER BY timestamp DESC LIMIT 1", (coin,)
                ).fetchone()
                conn.close()
                if row and row[0] and row[1]:
                    return json.dumps({
                        "long_liquidations_4h": float(row[0]),
                        "short_liquidations_4h": float(row[1]),
                        "bias": "bearish" if float(row[0]) > float(row[1]) else "bullish"
                    })
                return json.dumps({"long_liquidations_4h": 0, "short_liquidations_4h": 0})

            elif tool_name == "get_fear_greed":
                row = conn.execute(
                    "SELECT value, date FROM fear_greed ORDER BY date DESC LIMIT 1"
                ).fetchone()
                # Also get 7-day trend
                trend = conn.execute(
                    "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 7"
                ).fetchall()
                conn.close()
                return json.dumps({
                    "value": row[0] if row else 50,
                    "date": row[1] if row else "",
                    "trend_7d": [r[0] for r in trend] if trend else [],
                    "interpretation": "Extreme Fear" if row and row[0] < 20 else
                                    "Fear" if row and row[0] < 40 else
                                    "Neutral" if row and row[0] < 60 else
                                    "Greed" if row and row[0] < 80 else "Extreme Greed"
                })

            elif tool_name == "get_onchain":
                results = {}
                for metric in ['mvrv', 'sopr', 'nupl', 'puell_multiple', 'realized_price']:
                    try:
                        row = conn.execute(
                            f"SELECT value FROM cq_btc_onchain WHERE metric=? ORDER BY date DESC LIMIT 1",
                            (metric,)
                        ).fetchone()
                        if row:
                            results[metric] = round(float(row[0]), 4)
                    except Exception:
                        pass
                # Coinbase premium
                try:
                    cb = conn.execute(
                        "SELECT value FROM cq_coinbase_premium ORDER BY date DESC LIMIT 1"
                    ).fetchone()
                    if cb:
                        results['coinbase_premium'] = round(float(cb[0]), 4)
                except Exception:
                    pass
                conn.close()
                return json.dumps(results)

            elif tool_name == "get_macro":
                results = {}
                # DXY
                try:
                    dxy = conn.execute("SELECT value FROM dxy ORDER BY date DESC LIMIT 1").fetchone()
                    if dxy:
                        results['dxy'] = round(float(dxy[0]), 2)
                        results['dxy_interpretation'] = "Strong USD (bearish crypto)" if dxy[0] > 105 else \
                            "Weak USD (bullish crypto)" if dxy[0] < 95 else "Neutral USD"
                except Exception:
                    pass
                # ETF flows
                try:
                    btc_etf = conn.execute(
                        "SELECT flow_usd FROM cg_etf_flows WHERE asset='BTC' ORDER BY date DESC LIMIT 1"
                    ).fetchone()
                    eth_etf = conn.execute(
                        "SELECT flow_usd FROM cg_etf_flows WHERE asset='ETH' ORDER BY date DESC LIMIT 1"
                    ).fetchone()
                    results['btc_etf_flow'] = float(btc_etf[0]) if btc_etf and btc_etf[0] else 0
                    results['eth_etf_flow'] = float(eth_etf[0]) if eth_etf and eth_etf[0] else 0
                except Exception:
                    pass
                # Stablecoin supply
                try:
                    stable = conn.execute(
                        "SELECT total_market_cap FROM cg_stablecoin_supply ORDER BY date DESC LIMIT 2"
                    ).fetchall()
                    if len(stable) >= 2 and stable[1][0]:
                        change = (stable[0][0] - stable[1][0]) / stable[1][0] * 100
                        results['stablecoin_supply_change'] = round(change, 3)
                        results['stablecoin_supply'] = round(stable[0][0] / 1e9, 1)
                except Exception:
                    pass
                # OI change
                try:
                    oi = conn.execute(
                        "SELECT oi_close FROM cg_oi_history WHERE coin='BTC' ORDER BY date DESC LIMIT 2"
                    ).fetchall()
                    if len(oi) >= 2 and oi[1][0]:
                        results['btc_oi_change_pct'] = round((oi[0][0] / oi[1][0] - 1) * 100, 2)
                except Exception:
                    pass
                conn.close()
                return json.dumps(results)

            elif tool_name == "get_whale_activity":
                try:
                    whales = conn.execute("""
                        SELECT symbol, amount_usd, from_owner, to_owner
                        FROM whale_transactions
                        WHERE timestamp > strftime('%s', 'now', '-6 hours')
                        AND amount_usd > 5000000
                        ORDER BY amount_usd DESC LIMIT 10
                    """).fetchall()
                    conn.close()
                    txs = []
                    to_exchange = 0
                    from_exchange = 0
                    for w in whales:
                        direction = "to_exchange" if w[3] and 'exchange' in str(w[3]).lower() else \
                                   "from_exchange" if w[2] and 'exchange' in str(w[2]).lower() else "unknown"
                        if direction == "to_exchange":
                            to_exchange += w[1]
                        elif direction == "from_exchange":
                            from_exchange += w[1]
                        txs.append({"coin": w[0], "amount_usd": w[1], "direction": direction})
                    return json.dumps({
                        "transactions": txs[:5],
                        "net_to_exchange_usd": to_exchange - from_exchange,
                        "interpretation": "Sell pressure" if to_exchange > from_exchange * 1.5 else
                                        "Accumulation" if from_exchange > to_exchange * 1.5 else "Neutral"
                    })
                except Exception:
                    conn.close()
                    return json.dumps({"transactions": [], "interpretation": "No data"})

            elif tool_name == "get_options_data":
                try:
                    rows = conn.execute(
                        "SELECT coin, expiry, max_pain, call_oi, put_oi FROM cg_options_max_pain "
                        "ORDER BY expiry LIMIT 4"
                    ).fetchall()
                    conn.close()
                    options = []
                    for r in rows:
                        pc_ratio = float(r[4]) / float(r[3]) if r[3] and float(r[3]) > 0 else 1
                        options.append({
                            "coin": r[0], "expiry": r[1], "max_pain": r[2],
                            "put_call_ratio": round(pc_ratio, 2),
                            "interpretation": "Bearish" if pc_ratio > 1.2 else
                                            "Bullish" if pc_ratio < 0.8 else "Neutral"
                        })
                    return json.dumps(options)
                except Exception:
                    conn.close()
                    return json.dumps([])

            elif tool_name == "get_market_snapshot":
                # BTC
                btc = conn.execute(
                    "SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 7"
                ).fetchall()
                btc_price = btc[0][0] if btc else 0
                btc_24h = (btc[0][0] / btc[6][0] - 1) * 100 if len(btc) >= 7 else 0

                # Breadth
                breadth = conn.execute("""
                    SELECT p1.coin, p1.close, p2.close
                    FROM prices p1 JOIN prices p2 ON p1.coin = p2.coin
                    WHERE p1.timeframe='4h' AND p2.timeframe='4h'
                    AND p1.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h')
                    AND p2.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h') - 14400
                    AND p1.coin != 'BTC'
                """).fetchall()
                up = sum(1 for r in breadth if r[1] > r[2])
                total = len(breadth)

                # Top movers
                movers = []
                for r in breadth:
                    if r[2] > 0:
                        movers.append((r[0], (r[1]/r[2]-1)*100))
                movers.sort(key=lambda x: x[1])

                conn.close()
                return json.dumps({
                    "btc_price": btc_price,
                    "btc_24h_change": round(btc_24h, 2),
                    "market_breadth": f"{up}/{total} coins up ({up/total*100:.0f}%)" if total else "N/A",
                    "top_gainers": [{"coin": c, "change": round(r, 1)} for c, r in movers[-3:]],
                    "top_losers": [{"coin": c, "change": round(r, 1)} for c, r in movers[:3]],
                    "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
                })

            elif tool_name == "get_crowd_positioning":
                coin = tool_input.get("coin", "BTC")
                result = {}
                # Long/short ratio
                try:
                    ls = conn.execute(
                        "SELECT long_ratio, timestamp FROM long_short_ratio "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 3", (coin,)
                    ).fetchall()
                    if ls:
                        result['long_ratio'] = round(float(ls[0][0]), 3)
                        if len(ls) >= 2:
                            result['long_ratio_change'] = round(float(ls[0][0]) - float(ls[1][0]), 3)
                        result['crowd'] = 'OVERLEVERAGED_LONG' if float(ls[0][0]) > 1.5 else \
                            'OVERLEVERAGED_SHORT' if float(ls[0][0]) < 0.67 else 'BALANCED'
                except Exception:
                    pass
                # Taker buy/sell
                try:
                    tv = conn.execute(
                        "SELECT buy_vol, sell_vol FROM taker_volume "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 1", (coin,)
                    ).fetchone()
                    if tv and tv[0] and tv[1]:
                        result['taker_buy_sell_ratio'] = round(float(tv[0]) / (float(tv[1]) + 1), 3)
                        result['aggressor'] = 'BUYERS' if float(tv[0]) > float(tv[1]) * 1.2 else \
                            'SELLERS' if float(tv[1]) > float(tv[0]) * 1.2 else 'BALANCED'
                except Exception:
                    pass
                # OI change
                try:
                    oi = conn.execute(
                        "SELECT value FROM open_interest WHERE coin=? ORDER BY timestamp DESC LIMIT 2",
                        (coin,)
                    ).fetchall()
                    if len(oi) >= 2 and oi[1][0]:
                        result['oi_change_pct'] = round((float(oi[0][0]) / float(oi[1][0]) - 1) * 100, 2)
                except Exception:
                    pass
                conn.close()
                return json.dumps(result)

            elif tool_name == "get_exchange_flows":
                coin = tool_input.get("coin", "BTC")
                result = {}
                # Exchange balance
                try:
                    eb = conn.execute(
                        "SELECT balance FROM cg_exchange_balance "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 2", (coin,)
                    ).fetchall()
                    if len(eb) >= 2 and eb[1][0]:
                        change = (float(eb[0][0]) - float(eb[1][0]))
                        result['exchange_balance_change'] = round(change, 2)
                        result['flow_direction'] = 'TO_EXCHANGE (sell pressure)' if change > 0 else \
                            'FROM_EXCHANGE (accumulation)'
                except Exception:
                    pass
                # Exchange flows (BTC/ETH)
                try:
                    ef = conn.execute(
                        "SELECT inflow, outflow FROM cq_exchange_flows "
                        "WHERE coin=? ORDER BY date DESC LIMIT 1", (coin,)
                    ).fetchone()
                    if ef:
                        result['exchange_inflow'] = float(ef[0]) if ef[0] else 0
                        result['exchange_outflow'] = float(ef[1]) if ef[1] else 0
                        result['net_flow'] = round(result['exchange_inflow'] - result['exchange_outflow'], 2)
                except Exception:
                    pass
                # Social sentiment
                try:
                    ss = conn.execute(
                        "SELECT sentiment_score FROM social_sentiment "
                        "WHERE coin=? ORDER BY timestamp DESC LIMIT 1", (coin,)
                    ).fetchone()
                    if ss:
                        result['social_sentiment'] = round(float(ss[0]), 2)
                except Exception:
                    pass
                conn.close()
                return json.dumps(result)

            elif tool_name == "get_trading_calendar":
                conn.close()
                now = datetime.now(timezone.utc)
                hour = now.hour
                dow = now.strftime('%A')
                date_str = now.strftime('%Y-%m-%d')

                # Market sessions
                if 0 <= hour < 8:
                    session = "ASIA (low volatility, range-bound, good for scalps)"
                elif 8 <= hour < 14:
                    session = "EUROPE (medium volatility, trend starts)"
                elif 14 <= hour < 22:
                    session = "US (HIGHEST volatility, biggest moves)"
                else:
                    session = "LATE US / pre-ASIA (declining volume)"

                # Weekend
                is_weekend = now.weekday() >= 5
                weekend_note = "WEEKEND — thinner orderbook = sharper moves. Opportunity for scalps on breakouts. But wider stops needed (gaps possible)." if is_weekend else ""

                # Upcoming macro events
                try:
                    conn2 = sqlite3.connect(str(DB_PATH))
                    upcoming = conn2.execute(
                        "SELECT date, event_type FROM macro_events WHERE date >= ? ORDER BY date LIMIT 5",
                        (date_str,)
                    ).fetchall()
                    conn2.close()
                    events = [f"{e[0]}: {e[1]}" for e in upcoming]
                except Exception:
                    events = []

                # Options expiry (last Friday of month)
                import calendar
                last_day = calendar.monthrange(now.year, now.month)[1]
                last_friday = last_day
                while datetime(now.year, now.month, last_friday).weekday() != 4:
                    last_friday -= 1
                days_to_expiry = last_friday - now.day
                expiry_note = f"Options expiry in {days_to_expiry} days" if 0 < days_to_expiry <= 3 else ""

                # Major holidays (hardcoded known ones)
                holidays = {
                    '01-01': 'New Year', '12-25': 'Christmas', '12-24': 'Christmas Eve',
                    '07-04': 'US Independence Day', '11-28': 'US Thanksgiving',
                }
                md = now.strftime('%m-%d')
                holiday = holidays.get(md, '')

                result = {
                    "utc_time": now.strftime('%Y-%m-%d %H:%M UTC'),
                    "day": dow,
                    "session": session,
                    "is_weekend": is_weekend,
                    "weekend_warning": weekend_note,
                    "holiday": holiday,
                    "options_expiry": expiry_note,
                    "upcoming_macro_events": events[:5],
                    "trading_tips": {
                        "best_hours": "14:00-20:00 UTC (US session overlap)",
                        "worst_hours": "22:00-02:00 UTC (dead zone)",
                        "best_days": "Tuesday-Thursday (cleanest signals)",
                        "worst_days": "Friday PM (profit-taking), Weekend (low liquidity)",
                        "avoid_30min_before_fomc": True,
                    }
                }
                return json.dumps(result)

            elif tool_name == "get_historical_patterns":
                coin = tool_input.get("coin", "BTC")
                conn.close()
                try:
                    from src.crypto.pattern_matcher import find_similar_patterns, format_for_profi
                    matches = find_similar_patterns(coin, lookback=30, top_n=10)
                    return format_for_profi(coin, matches)
                except Exception as e:
                    return json.dumps({"error": str(e)})

            conn.close()
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

        except Exception as e:
            return json.dumps({"error": str(e)})

    def _call_with_tools(self, messages: list, model: str = None,
                         max_tokens: int = 1000, use_thinking: bool = True) -> str:
        """Call Claude with tool use + adaptive thinking + prompt caching.
        Runs agentic loop until Claude stops calling tools.
        """
        if not self._client:
            return ""

        try:
            kwargs = {
                "model": model or MODEL,
                "max_tokens": max_tokens,
                "system": self._system_with_cache(),
                "messages": messages,
                "tools": TRADING_TOOLS,
            }

            # Adaptive thinking — Claude decides how much to think
            if use_thinking:
                kwargs["thinking"] = {"type": "adaptive"}
                kwargs["max_tokens"] = max_tokens + 2000  # thinking + output

            response = self._client.messages.create(**kwargs)

            # Track token usage + cache
            self._call_count += 1
            self._total_input_tokens += response.usage.input_tokens
            self._total_output_tokens += response.usage.output_tokens
            if hasattr(response.usage, 'cache_read_input_tokens') and response.usage.cache_read_input_tokens:
                self._cache_hits += 1

            # Agentic loop: process tool calls
            loop_count = 0
            while response.stop_reason == "tool_use" and loop_count < 15:
                loop_count += 1
                tool_results = []

                for block in response.content:
                    if block.type == "tool_use":
                        result = self._execute_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result
                        })

                # Continue conversation with tool results
                messages = messages + [
                    {"role": "assistant", "content": response.content},
                    {"role": "user", "content": tool_results}
                ]

                kwargs["messages"] = messages
                response = self._client.messages.create(**kwargs)
                self._total_input_tokens += response.usage.input_tokens
                self._total_output_tokens += response.usage.output_tokens

            # Extract text response (skip thinking blocks in adaptive mode)
            text_parts = []
            for block in response.content:
                if block.type == "text":
                    if block.text.strip():
                        text_parts.append(block.text)
                elif block.type == "thinking":
                    # In adaptive mode, thinking may contain the actual analysis
                    pass

            if text_parts:
                return "\n".join(text_parts)

            # No text found — log content types for debugging
            types = [(b.type, len(getattr(b, 'text', '') or '')) for b in response.content]
            logger.warning(f"Empty Opus response. Blocks: {types}. stop_reason={response.stop_reason}")
            return ""

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return ""

    def _call_simple(self, messages: list, model: str = None, max_tokens: int = 500) -> str:
        """Simple call without tools (for daily strategy etc)."""
        if not self._client:
            return ""

        try:
            response = self._client.messages.create(
                model=model or MODEL,
                max_tokens=max_tokens,
                system=self._system_with_cache(),
                messages=messages,
            )

            self._call_count += 1
            self._total_input_tokens += response.usage.input_tokens
            self._total_output_tokens += response.usage.output_tokens
            if hasattr(response.usage, 'cache_read_input_tokens') and response.usage.cache_read_input_tokens:
                self._cache_hits += 1

            return response.content[0].text if response.content else ""

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return ""

    def get_daily_strategy(self) -> dict:
        """Daily strategy call (cached 4h)."""
        if time.time() - self._daily_strategy_time < 14400 and self._daily_strategy:
            return self._daily_strategy

        conn = sqlite3.connect(str(DB_PATH))

        btc_weekly = conn.execute("""
            SELECT date(timestamp,'unixepoch'), close FROM prices
            WHERE coin='BTC' AND timeframe='1d' ORDER BY timestamp DESC LIMIT 30
        """).fetchall()

        fg = conn.execute("SELECT date, value FROM fear_greed ORDER BY date DESC LIMIT 7").fetchall()

        breadth_rows = conn.execute("""
            SELECT p1.coin, p1.close, p2.close
            FROM prices p1 JOIN prices p2 ON p1.coin = p2.coin
            WHERE p1.timeframe='4h' AND p2.timeframe='4h'
            AND p1.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h')
            AND p2.timestamp = (SELECT MAX(timestamp) FROM prices WHERE timeframe='4h') - 86400
            AND p1.coin != 'BTC'
        """).fetchall()

        up = sum(1 for r in breadth_rows if r[1] > r[2])
        total = len(breadth_rows)
        breadth = up / total * 100 if total > 0 else 50

        conn.close()

        btc_str = ', '.join([f"{r[0]}: ${r[1]:,.0f}" for r in btc_weekly[:14]])
        fg_str = ', '.join([f"{r[0]}: {r[1]}" for r in fg])

        prompt = f"""Current market data:

BTC last 14 days: {btc_str}
Fear & Greed (last 7 days): {fg_str}
Market breadth (24h): {breadth:.0f}% coins up ({up}/{total})

Give daily strategy. Reply JSON ONLY:
{{
  "regime": "BULL/BEAR/SIDEWAYS/UNCERTAIN",
  "should_trade": true/false,
  "preferred_direction": "LONG/SHORT/BOTH/NONE",
  "max_positions": 1-4,
  "sectors_to_avoid": ["defi", "meme", etc],
  "confidence": 0.0-1.0,
  "reason": "one paragraph"
}}"""

        result = self._call_simple(
            [{"role": "user", "content": prompt}],
            model=MODEL_DAILY, max_tokens=400
        )

        try:
            start = result.find('{')
            end = result.rfind('}') + 1
            if start >= 0:
                strategy = json.loads(result[start:end])
                self._daily_strategy = strategy
                self._daily_strategy_time = time.time()
                logger.info(f"PROFI daily: {strategy.get('regime')} "
                           f"({strategy.get('confidence', 0):.0%}) — {strategy.get('reason', '')[:60]}")
                return strategy
        except Exception:
            pass

        return {'regime': 'UNCERTAIN', 'should_trade': False, 'confidence': 0}

    def find_level_setups(self, coins: list, levels_data: dict, regime: str = '',
                          open_positions: str = '', sl_history: dict = None,
                          trade_feedback: str = '') -> list:
        """OPUS finds trade setups at S/R levels. Runs ONCE per hour.

        Args:
            coins: coins to analyze (max 10)
            levels_data: {coin: {resistance: [...], support: [...], atr: float, current_price: float}}
            regime: BEAR/BULL/FLAT
            open_positions: "SHORT BNB +1.2%, LONG ETH -0.5%"
            sl_history: {coin: timestamp} recent SL coins

        Returns:
            list of setups: [{coin, direction, entry, sl, tp, leverage, confidence, reason}]
        """
        if not self._client:
            return []

        content = []

        # Macro data — real numbers from DB, no interpretation
        try:
            _conn = sqlite3.connect(str(DB_PATH))
            parts = []
            r = _conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
            if r: parts.append(f"F&G={r[0]}")
            r = _conn.execute("SELECT value FROM cq_btc_onchain WHERE metric='mvrv' ORDER BY date DESC LIMIT 1").fetchone()
            if r and r[0]: parts.append(f"MVRV={r[0]:.2f}")
            r = _conn.execute("SELECT value FROM cq_btc_onchain WHERE metric='sopr' ORDER BY date DESC LIMIT 1").fetchone()
            if r and r[0]: parts.append(f"SOPR={r[0]:.3f}")
            r = _conn.execute("SELECT value FROM cq_btc_onchain WHERE metric='nupl' ORDER BY date DESC LIMIT 1").fetchone()
            if r and r[0]: parts.append(f"NUPL={r[0]:.2f}")
            r = _conn.execute("SELECT rate FROM funding_rates WHERE coin='BTC' ORDER BY timestamp DESC LIMIT 1").fetchone()
            if r and r[0] is not None: parts.append(f"BTC_funding={r[0]*100:+.3f}%")
            r = _conn.execute("SELECT long_ratio FROM long_short_ratio WHERE coin='BTC' ORDER BY timestamp DESC LIMIT 1").fetchone()
            if r and r[0]: parts.append(f"BTC_LS_ratio={r[0]:.2f}")
            r = _conn.execute("SELECT premium_index FROM cq_coinbase_premium ORDER BY date DESC LIMIT 1").fetchone()
            if r and r[0] is not None: parts.append(f"CB_premium={r[0]:+.3f}%")
            r = _conn.execute("SELECT liq_usd_24h, long_liq_usd_24h, short_liq_usd_24h FROM cg_liquidations WHERE coin='BTC' ORDER BY timestamp DESC LIMIT 1").fetchone()
            if r and r[0]: parts.append(f"BTC_liq_24h=${r[0]/1e6:.0f}M(L${r[1]/1e6:.0f}M/S${r[2]/1e6:.0f}M)")
            btc_7d = _conn.execute("SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 42").fetchall()
            btc_1d = _conn.execute("SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 6").fetchall()
            if len(btc_7d) >= 42:
                parts.insert(0, f"BTC_7d={((btc_7d[0][0]/btc_7d[-1][0])-1)*100:+.1f}%")
            if len(btc_1d) >= 6:
                parts.insert(1, f"BTC_1d={((btc_1d[0][0]/btc_1d[-1][0])-1)*100:+.1f}%")
            _conn.close()
            if parts:
                content.append({"type": "text", "text": f"MACRO: {' | '.join(parts)}"})
        except Exception:
            pass

        # BTC momentum — strongest predictor (85% correlation with alts)
        exchange = getattr(self, '_exchange', None)
        btc_momentum = ""
        if exchange:
            try:
                btc_ohlcv = exchange._exchange.fetch_ohlcv('BTC/USDT:USDT', '15m', limit=4)
                if btc_ohlcv and len(btc_ohlcv) >= 3:
                    btc_now = btc_ohlcv[-1][4]
                    btc_1h_ago = btc_ohlcv[0][1]
                    btc_15m_ago = btc_ohlcv[-2][4]
                    btc_1h_pct = (btc_now - btc_1h_ago) / btc_1h_ago * 100
                    btc_15m_pct = (btc_now - btc_15m_ago) / btc_15m_ago * 100
                    if btc_1h_pct > 0.3:
                        btc_dir = "RISING"
                    elif btc_1h_pct < -0.3:
                        btc_dir = "FALLING"
                    else:
                        btc_dir = "FLAT"
                    btc_momentum = (f"BTC {btc_dir}: {btc_1h_pct:+.1f}% (1h), {btc_15m_pct:+.1f}% (15m), "
                                   f"${btc_now:.0f}")
                    content.append({
                        "type": "text",
                        "text": f"*** BTC MOMENTUM (85% correlated with alts): {btc_momentum} ***\n"
                                f"RULE: When BTC RISING → LONG BTC and alts. When BTC FALLING → SHORT BTC and alts. "
                                f"BTC is a TRADEABLE coin — include BTC setups!"
                    })
            except Exception:
                pass

        # Generate LIVE charts: 1H (main) + 4H (context)
        for coin in coins[:8]:
            try:
                from src.crypto.chart_generator import generate_live_chart
                # 1H chart — PRIMARY decision chart
                b = generate_live_chart(coin, '1h', 48, exchange=exchange)
                if b:
                    content.append({"type": "text", "text": f"--- {coin}/USDT 1H LIVE ---"})
                    content.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/png",
                                   "data": base64.b64encode(b).decode()}
                    })

                # 4H chart — trend context
                b2 = generate_live_chart(coin, '4h', 30, exchange=exchange)
                if b2:
                    content.append({"type": "text", "text": f"--- {coin}/USDT 4H context ---"})
                    content.append({
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/png",
                                   "data": base64.b64encode(b2).decode()}
                    })
            except Exception:
                pass

            # ALL live data pre-fetched for this coin
            lvl = levels_data.get(coin, {})
            r_list = lvl.get('resistance', [])
            s_list = lvl.get('support', [])
            atr_4h = lvl.get('atr', 0.02)
            live_price = lvl.get('current_price', 0)
            funding = 0.0
            momentum_15m = 0.0
            atr_1h = atr_4h * 0.5  # approximate
            ob_text = ""

            if hasattr(self, '_exchange') and self._exchange:
                try:
                    t = self._exchange.get_ticker(coin)
                    if t and t.get('price', 0) > 0:
                        live_price = t['price']
                    funding = self._exchange.get_funding_rate(coin)

                    # 1H ATR (real, from candles)
                    ohlcv_1h = self._exchange._exchange.fetch_ohlcv(
                        self._exchange._symbol(coin), '1h', limit=14)
                    if ohlcv_1h and len(ohlcv_1h) >= 10:
                        import numpy as np
                        h = [c[2] for c in ohlcv_1h]
                        l = [c[3] for c in ohlcv_1h]
                        c = [c[4] for c in ohlcv_1h]
                        trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
                               for i in range(1, len(ohlcv_1h))]
                        atr_1h = np.mean(trs[-14:]) / live_price if live_price > 0 else 0.01

                    # 15m momentum
                    ohlcv_15m = self._exchange._exchange.fetch_ohlcv(
                        self._exchange._symbol(coin), '15m', limit=4)
                    if ohlcv_15m and len(ohlcv_15m) >= 2:
                        momentum_15m = (ohlcv_15m[-1][4] - ohlcv_15m[-2][1]) / ohlcv_15m[-2][1] * 100

                    # Live order book
                    ob = self._exchange._exchange.fetch_order_book(
                        self._exchange._symbol(coin), limit=20)
                    if ob:
                        bid_vol = sum(b[1] * b[0] for b in ob['bids'][:10])
                        ask_vol = sum(a[1] * a[0] for a in ob['asks'][:10])
                        total = bid_vol + ask_vol
                        if total > 0:
                            imbalance = (bid_vol - ask_vol) / total
                            # Find biggest wall
                            biggest_bid = max(ob['bids'][:10], key=lambda x: x[1]*x[0])
                            biggest_ask = max(ob['asks'][:10], key=lambda x: x[1]*x[0])
                            ob_text = (f"OB: {'BUY' if imbalance > 0.1 else 'SELL' if imbalance < -0.1 else 'NEUTRAL'} "
                                      f"pressure ({imbalance:+.0%}) | "
                                      f"buy_wall=${biggest_bid[0]:.4f}({biggest_bid[1]*biggest_bid[0]:.0f}$) | "
                                      f"sell_wall=${biggest_ask[0]:.4f}({biggest_ask[1]*biggest_ask[0]:.0f}$)")
                except Exception:
                    pass

            r_str = ', '.join(f'${p:.4f}' for p in r_list[:3])
            s_str = ', '.join(f'${p:.4f}' for p in s_list[:3])
            data_parts = [f"LIVE=${live_price:.4f}", f"ATR_1h={atr_1h*100:.2f}%",
                         f"R=[{r_str}]", f"S=[{s_str}]"]
            if funding: data_parts.append(f"funding={funding*100:.3f}%")
            if abs(momentum_15m) > 0.2: data_parts.append(f"15m_mom={momentum_15m:+.1f}%")
            if ob_text: data_parts.append(ob_text)
            content.append({
                "type": "text",
                "text": f"[{coin}: {' | '.join(data_parts)}]"
            })

        # SL history
        sl_text = ""
        if sl_history:
            sl_coins = [f"{c}" for c, t in sl_history.items()
                       if time.time() - t < 7200]
            if sl_coins:
                sl_text = f"\nAVOID: {', '.join(sl_coins)} (recent SL)"

        # Learning feedback from trade journal
        if trade_feedback:
            content.append({
                "type": "text",
                "text": f"YOUR PERFORMANCE (learn from this):\n{trade_feedback}"
            })

        content.append({
            "type": "text",
            "text": f"""REGIME: {regime}
{f"Open positions: {open_positions}" if open_positions else "No open positions."}
{sl_text}

You are a 1-HOUR trader. Your job is to PREDICT the next move BEFORE it happens.

DATA YOU HAVE:
- 1H chart (PRIMARY), 4H chart (context)
- ATR_1h = typical 1-hour price movement
- S/R levels from 1h candles
- LIVE order book: buy/sell pressure and walls
- 15m momentum: direction RIGHT NOW
- Funding rate: crowded side will get squeezed
- MACRO line: BTC_7d, F&G, MVRV, SOPR, funding, liquidations

HOW TO PREDICT:
- OB buy pressure >20% + 15m momentum positive → bullish signal
- OB sell pressure + momentum negative → bearish signal
- OB neutral + no momentum → price ranging → PATIENT limit at S/R level
- Extreme funding → trade AGAINST the crowd
- Use MACRO data (BTC_7d, F&G) to confirm your direction

TWO ENTRY MODES:
1. AGGRESSIVE: entry = LIVE price (fills immediately)
   Use when: strong conviction + momentum confirms.
2. PATIENT: entry = nearest S/R level WITHIN 0.5% of current price.
   Levels >0.5% away have <20% fill rate — order expires unfilled, you miss the move.
   If no S/R within 0.5% → use AGGRESSIVE or SKIP this coin.

YOU control TP and SL. Set them based on S/R levels AND ATR. Verified on 1500+ trades:

RISK MANAGEMENT (fixed, code-enforced):
  - SL = -6.5% ROI (0.81% price at 8x). Hard stop on exchange.
  - TP = +13% ROI (1.625% price). Target on exchange.
  - TRAILING STOP: activates at +6% ROI, closes if drops -2% from peak.
    Example: peak +10% ROI → drops to +8% → trailing closes. Avg exit +9% ROI.
  - R:R = 2.0 (13/6.5). With 51%+ WR = profitable.
  - Your SL/TP suggestions are used for entry analysis, but code enforces these limits.

YOUR ROLE: pick DIRECTION and ENTRY POINT. Risk management is automated.
Focus on: which coins will move 1.6%+ in your direction within 3 hours.

Leverage: 8x default.

Reply JSON array (5-8 setups):
[{{
  "coin": "BNB",
  "direction": "LONG",
  "entry": 608.5,
  "sl": 606.7,
  "tp": 612.0,
  "leverage": 10,
  "confidence": 0.70,
  "reason": "OB buy +35%, 15m mom +0.8%, AGGRESSIVE entry near live"
}}]"""
        })

        # Single API call — no tools, no thinking. All data pre-fetched above.
        # This takes ~15-30 seconds instead of 3-5 minutes with tools.
        result = self._call_simple(
            [{"role": "user", "content": content}],
            model=MODEL,
            max_tokens=4000
        )

        setups = []
        try:
            start = result.find('[')
            end = result.rfind(']') + 1
            if start >= 0 and end > start:
                raw = json.loads(result[start:end])
                for s in raw:
                    direction = s.get('direction', '')
                    if direction not in ('LONG', 'SHORT'):
                        continue
                    conf = float(s.get('confidence', 0))
                    if conf > 1.0:
                        conf = conf / 100.0
                    if conf < 0.65:  # Rule 31: min 65% — low conf trades lose money
                        continue
                    s['confidence'] = conf
                    setups.append(s)
                    logger.info(f"OPUS SETUP: {direction} {s['coin']} "
                               f"limit@${s.get('entry',0):.4f} "
                               f"SL=${s.get('sl',0):.4f} TP=${s.get('tp',0):.4f} "
                               f"({conf:.0%}) — {s.get('reason','')[:50]}")
            else:
                # No JSON array found — log raw response for debugging
                logger.warning(f"OPUS raw (no JSON array): {result[:200]}")
        except Exception as e:
            logger.debug(f"Setup parse error: {e}")

        logger.info(f"OPUS: {len(setups)} level setups from {len(coins)} coins")
        return setups

    def analyze_candidate(self, coin: str, direction: str, gemini_reason: str,
                          charts: dict, coin_atr: float = 0.02) -> dict:
        """OPUS deep analysis of a Gemini candidate. 5 charts + tools + thinking.

        Args:
            coin: e.g. 'BNB'
            direction: Gemini's recommendation ('LONG' or 'SHORT')
            gemini_reason: Gemini's explanation
            charts: {'3M': b64, '1M': b64, '5D': b64, '4H': b64, '1H': b64}
            coin_atr: ATR % for TP/SL capping

        Returns:
            {decision, entry, tp, sl, leverage, confidence, hold_estimate, reason}
        """
        content = []

        # Add all 5 timeframe charts
        for label in ['3M', '1M', '5D', '4H', '1H']:
            if label in charts and charts[label]:
                content.append({"type": "text", "text": f"--- {coin}/USDT {label} ---"})
                content.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/png",
                               "data": charts[label]}
                })

        content.append({
            "type": "text",
            "text": f"""Gemini scanner found: {direction} {coin}
Gemini's reason: "{gemini_reason}"
Coin ATR (4h): {coin_atr*100:.1f}% — TP must NOT exceed this.

You are OPUS — the final decision maker. Gemini found this candidate, now YOU confirm or reject.
Look at ALL 5 timeframes. Use tools to check funding, orderbook, liquidations, on-chain.

If you CONFIRM Gemini's recommendation:
- Set EXACT entry, TP, SL prices (TP ≤ {coin_atr*100:.1f}% = 1× ATR)
- Set leverage (3-15x based on confidence)
- Set hold estimate

If you REJECT — explain why Gemini was wrong.

Reply JSON:
{{
  "decision": "LONG" or "SHORT" or "REJECT",
  "entry": price,
  "tp": price,
  "sl": price,
  "leverage": int,
  "confidence": 0.0-1.0,
  "hold_estimate": "Xh" or "Xmin",
  "reason": "detailed explanation"
}}"""
        })

        result = self._call_with_tools(
            [{"role": "user", "content": content}],
            model=MODEL,
            max_tokens=1200,
            use_thinking=True
        )

        try:
            start = result.find('{')
            end = result.rfind('}') + 1
            if start >= 0:
                analysis = json.loads(result[start:end])
                decision = analysis.get('decision', 'REJECT')
                conf = float(analysis.get('confidence', 0))
                if conf > 1.0:
                    conf = conf / 100.0
                analysis['confidence'] = conf

                logger.info(f"OPUS {coin}: {decision} {conf:.0%} — "
                           f"{analysis.get('reason', '')[:60]}")
                return analysis
        except Exception:
            pass

        return {'decision': 'REJECT', 'confidence': 0, 'reason': 'analysis failed'}

    def analyze_trade(self, coin: str, direction: str, charts: dict,
                      features: dict = None, news: str = "") -> dict:
        """Analyze a specific trade opportunity with CHARTS + TOOLS + THINKING.

        Args:
            coin: e.g. 'CRV'
            direction: suggested 'LONG' or 'SHORT' (from ML models)
            charts: {'1d': base64_png, '4h': base64_png, '1h': base64_png}
            features: dict of numerical features
            news: recent relevant news text

        Returns:
            {decision, entry, tp, sl, confidence, reason}
        """
        content = []

        # Add charts as images — PROFI MUST SEE CHARTS
        charts_added = 0
        for tf in ['1d', '4h', '1h']:
            if tf in charts and charts[tf]:
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": charts[tf]
                    }
                })
                charts_added += 1

        # Build text prompt
        features_str = ""
        if features:
            features_str = f"""
Key indicators:
- RSI: {features.get('rsi', 'N/A')}
- BB position: {features.get('bb_position', 'N/A')}
- Volume ratio: {features.get('volume_ratio', 'N/A')}
- ATR: {features.get('atr_pct', 'N/A')}
- Funding rate: {features.get('funding_rate', 'N/A')}
"""

        news_str = f"\nRecent news: {news}" if news else ""

        chart_desc = f"Above are {charts_added} chart(s): " + ", ".join(
            tf.upper() for tf in ['1d', '4h', '1h'] if tf in charts and charts[tf]
        ) if charts_added > 0 else "No charts available — use tools to gather data."

        content.append({
            "type": "text",
            "text": f"""Analyze {coin}/USDT for potential {direction} trade.

{chart_desc}
Each chart shows: candlesticks, volume, RSI, Bollinger Bands, MA20, MA50.
{features_str}{news_str}

You are an INDEPENDENT expert. Do NOT assume {direction} is correct — form your OWN opinion.
Use the available tools to check: current price, funding rate, orderbook imbalance, liquidations, fear & greed.

After gathering all data and analyzing charts, reply JSON ONLY:
{{
  "decision": "LONG" or "SHORT" or "WAIT",
  "entry": price,
  "tp": price,
  "sl": price,
  "rr_ratio": float,
  "confidence": 0.0-1.0,
  "patterns_seen": ["pattern1", "pattern2"],
  "timeframe_alignment": "all_agree/mixed/conflicting",
  "reason": "detailed explanation of your analysis"
}}"""
        })

        result = self._call_with_tools(
            [{"role": "user", "content": content}],
            model=MODEL,
            max_tokens=800,
            use_thinking=True
        )

        try:
            start = result.find('{')
            end = result.rfind('}') + 1
            if start >= 0:
                analysis = json.loads(result[start:end])
                logger.info(f"PROFI {coin}: {analysis.get('decision')} "
                           f"({analysis.get('confidence', 0):.0%}) "
                           f"— {analysis.get('reason', '')[:60]}")
                return analysis
        except Exception:
            pass

        return {'decision': 'WAIT', 'confidence': 0, 'reason': 'analysis failed'}

    def _generate_live_chart(self, coin: str) -> bytes:
        """Generate chart from live exchange data (for coins not in our DB)."""
        try:
            import ccxt
            exchange = ccxt.bybit()
            ohlcv = exchange.fetch_ohlcv(f"{coin}/USDT:USDT", '4h', limit=50)
            if len(ohlcv) < 20:
                return b''

            # Quick chart with matplotlib
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import io

            dates = [datetime.fromtimestamp(c[0]/1000) for c in ohlcv]
            opens = [c[1] for c in ohlcv]
            highs = [c[2] for c in ohlcv]
            lows = [c[3] for c in ohlcv]
            closes = [c[4] for c in ohlcv]
            volumes = [c[5] for c in ohlcv]

            closes_arr = np.array(closes)

            # RSI
            deltas = np.diff(closes_arr)
            gains = np.where(deltas > 0, deltas, 0)
            losses_arr = np.where(deltas < 0, -deltas, 0)
            avg_g = np.zeros(len(deltas))
            avg_l = np.zeros(len(deltas))
            if len(gains) >= 14:
                avg_g[13] = np.mean(gains[:14])
                avg_l[13] = np.mean(losses_arr[:14])
                for i in range(14, len(deltas)):
                    avg_g[i] = (avg_g[i-1]*13 + gains[i])/14
                    avg_l[i] = (avg_l[i-1]*13 + losses_arr[i])/14
            rsi = 100 - 100/(1 + avg_g/(avg_l + 1e-10))

            # BB
            bb_ma = np.array([np.mean(closes_arr[max(0,i-20):i+1]) for i in range(len(closes_arr))])
            bb_std = np.array([np.std(closes_arr[max(0,i-20):i+1]) for i in range(len(closes_arr))])

            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 8),
                gridspec_kw={'height_ratios': [3, 1, 1]}, facecolor='#0D1117')
            for ax in [ax1, ax2, ax3]:
                ax.set_facecolor('#0D1117')
                ax.tick_params(colors='#8B949E')
                ax.grid(True, color='#1C2333', alpha=0.5)

            # Candles
            for i in range(len(dates)):
                color = '#00FF88' if closes[i] >= opens[i] else '#FF4444'
                ax1.plot([dates[i]]*2, [lows[i], highs[i]], color=color, linewidth=0.5)
                ax1.plot([dates[i]]*2, [opens[i], closes[i]], color=color, linewidth=2)
            ax1.plot(dates, bb_ma, color='#FFD700', linewidth=0.8)
            ax1.plot(dates, bb_ma + 2*bb_std, color='#3B82F6', linewidth=0.5)
            ax1.plot(dates, bb_ma - 2*bb_std, color='#3B82F6', linewidth=0.5)
            ax1.set_title(f'{coin}/USDT 4H (LIVE)', color='#E6EDF3', fontsize=14)

            colors = ['#00FF88' if closes[i] >= opens[i] else '#FF4444' for i in range(len(dates))]
            ax2.bar(dates, volumes, color=colors, alpha=0.6)

            ax3.plot(dates[1:], rsi, color='#9B59B6', linewidth=1)
            ax3.axhline(70, color='#FF4444', linewidth=0.5, linestyle='--')
            ax3.axhline(30, color='#00FF88', linewidth=0.5, linestyle='--')
            ax3.set_ylim(0, 100)

            plt.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=80, bbox_inches='tight', facecolor='#0D1117')
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            logger.debug(f"Live chart {coin}: {e}")
            return b''

    def _format_sl_history(self, sl_history):
        """Format recent SL coins for scan prompt."""
        if not sl_history:
            return ""
        lines = ["COINS WITH RECENT STOP-LOSSES (AVOID or be extra careful):"]
        for coin, ts in sl_history.items():
            mins = (time.time() - ts) / 60
            if mins < 120:
                lines.append(f"  {coin}: SL hit {mins:.0f}min ago — DO NOT trade this coin")
        return "\n".join(lines) if len(lines) > 1 else ""

    def discover_hot_coins(self, exchange) -> list:
        """Scan ALL Bybit futures for hot coins — biggest movers, volume spikes.
        Returns list of coin symbols worth analyzing (beyond our usual 23).
        """
        try:
            tickers = exchange._exchange.fetch_tickers()
            movers = []
            for symbol, t in tickers.items():
                if not symbol.endswith('/USDT:USDT'):
                    continue
                coin = symbol.split('/')[0]
                pct = t.get('percentage', 0) or 0
                vol = t.get('quoteVolume', 0) or 0

                # Filter: decent volume (>$2M/24h) and meaningful move (>2%)
                if vol > 2_000_000 and abs(pct) > 2:
                    movers.append({
                        'coin': coin,
                        'change_24h': round(pct, 1),
                        'volume_24h': round(vol / 1e6, 1),
                        'price': t.get('last', 0),
                    })

            # Sort by absolute change
            movers.sort(key=lambda x: abs(x['change_24h']), reverse=True)
            hot = movers[:20]  # top 20 movers

            if hot:
                hot_str = ', '.join(f"{m['coin']}({m['change_24h']:+.0f}%)" for m in hot[:5])
                logger.info(f"HOT COINS: {hot_str}")

            return hot

        except Exception as e:
            logger.debug(f"Hot coin discovery: {e}")
            return []

    def scan_opportunities(self, coins: list, existing_positions: list = None,
                           regime: str = '', daily_strategy: dict = None,
                           on_found: callable = None,
                           sl_history: dict = None,
                           open_positions_info: str = '') -> list:
        """PROFI scans ALL coins, looks at charts, finds trade opportunities.
        Calls on_found(opportunity) IMMEDIATELY when found — don't wait for full scan.

        Args:
            coins: list of coin symbols to scan
            existing_positions: list of coins already in portfolio
            regime: current market regime (BULL/BEAR/FLAT)
            daily_strategy: Profi's own daily strategy dict

        Returns:
            list of opportunities: [{coin, direction, confidence, entry, tp, sl, reason}]
        """
        if not self._client:
            return []

        existing = set(existing_positions or [])
        available = [c for c in coins if c not in existing]

        if not available:
            return []

        # Generate charts — from DB for core coins, from exchange API for hot coins
        charts_data = []
        tools_only_coins = []
        try:
            from src.crypto.chart_generator import generate_chart
            for coin in available:
                # Multi-timeframe: 1D (trend) + 4H (setup) + 1H (entry timing)
                coin_charts = {}
                # 5 timeframes: 3M trend → 1M setup → 5D detail → 4H pattern → 1H entry
                for label, tf, days in [
                    ('3M', '1d', 90),    # 3-month big picture
                    ('1M', '1d', 30),    # 1-month trend
                    ('5D', '4h', 5),     # 5-day detail (30 candles)
                    ('4H', '4h', 14),    # 2-week 4H patterns
                    ('1H', '1h', 3),     # 3-day hourly for entry
                ]:
                    b = generate_chart(coin, tf, days)
                    if b:
                        coin_charts[label] = base64.b64encode(b).decode()

                if coin_charts:
                    charts_data.append({
                        'coin': coin,
                        'charts': coin_charts,  # dict of timeframe -> b64
                    })
                else:
                    # Try live API chart for hot coins
                    chart_bytes = self._generate_live_chart(coin)
                    if chart_bytes:
                        charts_data.append({
                            'coin': coin,
                            'charts': {'4h': base64.b64encode(chart_bytes).decode()},
                        })
                    else:
                        tools_only_coins.append(coin)
        except Exception as e:
            logger.debug(f"Chart scan error: {e}")

        if not charts_data and not tools_only_coins:
            return []

        # Send ALL charts to Profi in one batch (up to 8 coins at a time)
        # to reduce API calls while letting Profi see real charts
        opportunities = []
        batch_size = 2  # 2 coins × 5 timeframes = 10 images per call (safe for API)

        for i in range(0, len(charts_data), batch_size):
            batch = charts_data[i:i + batch_size]

            content = []
            coin_names = []

            for item in batch:
                coin_charts = item.get('charts', {})
                # Add each timeframe chart (3M → 1M → 5D → 4H → 1H)
                for label in ['3M', '1M', '5D', '4H', '1H']:
                    if label in coin_charts:
                        content.append({
                            "type": "text",
                            "text": f"--- {item['coin']}/USDT {label} ---"
                        })
                        content.append({
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": coin_charts[label]
                            }
                        })
                coin_names.append(item['coin'])

                # === PER-COIN DATA BLOCK (ATR, momentum, RSI div, SL history) ===
                try:
                    import sqlite3 as _sql
                    _conn = _sql.connect(str(DB_PATH))
                    _coin = item['coin']

                    # ATR (Fix 4)
                    _rows = _conn.execute(
                        "SELECT high, low, close FROM prices WHERE coin=? AND timeframe='4h' "
                        "ORDER BY timestamp DESC LIMIT 30", (_coin,)
                    ).fetchall()
                    _atr = 0
                    if len(_rows) >= 10:
                        _atrs = [(r[0]-r[1])/r[2]*100 for r in _rows if r[2] > 0]
                        _atr = sum(_atrs)/len(_atrs) if _atrs else 0

                    # Momentum (Fix 7)
                    _closes = _conn.execute(
                        "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                        "ORDER BY timestamp DESC LIMIT 7", (_coin,)
                    ).fetchall()
                    _mom_4h = 0
                    _mom_24h = 0
                    if len(_closes) >= 2:
                        _mom_4h = (_closes[0][0]/_closes[1][0] - 1) * 100
                    if len(_closes) >= 7:
                        _mom_24h = (_closes[0][0]/_closes[6][0] - 1) * 100

                    _conn.close()

                    # Build info text
                    info_parts = [f"{_coin}: ATR={_atr:.1f}%"]
                    info_parts.append(f"4h mom={_mom_4h:+.1f}%")
                    info_parts.append(f"24h mom={_mom_24h:+.1f}%")
                    info_parts.append(f"max TP={_atr:.1f}%")

                    content.append({
                        "type": "text",
                        "text": f"[{' | '.join(info_parts)}]"
                    })

                    # RSI bullish divergence detector (57-64% WR)
                    _rows2 = [r[0] for r in _closes] if _closes else []
                    # Simplified check kept from before
                except Exception:
                    pass

                try:
                    import sqlite3 as _sql
                    _conn = _sql.connect(str(DB_PATH))
                    _rows = _conn.execute(
                        "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
                        "ORDER BY timestamp DESC LIMIT 30", (item['coin'],)
                    ).fetchall()
                    _conn.close()
                    if len(_rows) >= 20:
                        _c = [r[0] for r in _rows[::-1]]
                        _d = [_c[j]-_c[j-1] for j in range(1, len(_c))]
                        _g = [max(0, x) for x in _d]
                        _l = [max(0, -x) for x in _d]
                        if len(_g) >= 14:
                            _ag = sum(_g[-14:])/14
                            _al = sum(_l[-14:])/14
                            _rsi = 100 - 100/(1 + _ag/(_al+1e-10))
                            if (len(_c) > 12 and _c[-1] < _c[-7] and
                                _rsi > 100 - 100/(1 + sum(_g[-20:-14])/14 / (sum(_l[-20:-14])/14 + 1e-10)) and
                                _rsi < 40):
                                content.append({
                                    "type": "text",
                                    "text": f"*** ALERT: {item['coin']} has RSI BULLISH DIVERGENCE (price lower low but RSI higher low, RSI={_rsi:.0f}). Historically 57-64% bounce rate. ***"
                                })
                except Exception:
                    pass

            # Build regime context — only regime and direction, NOT confidence
            # (confidence was poisoning scan — making Profi too cautious)
            regime_label = regime or 'UNKNOWN'
            if daily_strategy:
                regime_label = daily_strategy.get('regime', regime_label)

            regime_ctx = f"""
CURRENT REGIME: {regime_label}
You decide independently based on what you SEE on the charts.
In BEAR: look for SHORT setups ONLY (rejections, breakdowns, dead cat bounce failures).
In BULL: look for LONG setups ONLY (pullbacks, breakouts, higher lows).
{f"YOUR OPEN POSITIONS: {open_positions_info}" if open_positions_info else "No open positions."}
{self._format_sl_history(sl_history) if sl_history else ""}
"""

            content.append({
                "type": "text",
                "text": f"""You are scanning {len(coin_names)} coins for trade opportunities.
For each coin you see 5 charts:
- 3M (3-month daily) — BIG PICTURE trend. NEVER trade against this.
- 1M (1-month daily) — Medium-term structure, S/R levels.
- 5D (5-day 4H) — Recent price action detail.
- 4H (2-week 4H) — Pattern recognition, setups.
- 1H (3-day hourly) — Entry timing precision.
RULE: ALL timeframes must agree. 3M DOWN + 1M DOWN = SHORT only. Any conflict = WAIT.
Each chart shows candles, RSI, Bollinger Bands, MA20, MA50, and volume.
{regime_ctx}
For EACH coin, decide: is there a tradeable setup RIGHT NOW?

In BEAR market, look for SHORT setups:
- Rejection from resistance / MA50 / upper BB
- Breakdown below support with volume
- Lower highs + lower lows continuation
- Dead cat bounce exhaustion (price failed to reclaim MA20)
- Bearish divergence on RSI

In BULL market, look for LONG setups:
- Bounce off support / MA20 / lower BB
- Breakout above resistance with volume
- Higher lows continuation
- Pullback to MA in uptrend

Also check: funding rate, orderbook imbalance via tools.

Reply JSON ONLY — array of opportunities (empty array [] if TRULY nothing):
[
  {{
    "coin": "SYMBOL",
    "direction": "LONG" or "SHORT",
    "confidence": 0.0-1.0,
    "entry": approximate_price,
    "tp": take_profit_price,
    "sl": stop_loss_price,
    "leverage": 3-15 (higher for stronger setups, lower for risky ones),
    "size_pct": 5-15 (% of capital to risk, higher for best setups),
    "hold_estimate": "minutes/hours/days",
    "pattern": "what you see on the chart",
    "reason": "detailed explanation"
  }}
]

THERE IS ALWAYS AN OPPORTUNITY. Your job is to FIND it:
- In BEAR: short resistance rejections, dead cat bounces, breakdowns
- In BULL: buy dips, breakout retests, higher low formations
- In RANGE: mean reversion at edges, BB bounces
- Quick scalp (15-60 min, 0.3-0.5% target, high leverage 10-15x)
- Swing trade (4-24h, 1-3% target, medium leverage 5-7x)
- Even in chaos: there's ALWAYS one coin doing something different

Use leverage WISELY: high confidence + tight SL = higher leverage is SAFE.
Low confidence = lower leverage, smaller size.
TP MUST be realistic — within 1× ATR of the coin's typical 4h range. Do NOT set targets the coin cannot reach in 8 hours.

Include ALL coins you'd trade — even 50% confidence is worth considering if R:R is good.
Coins: {', '.join(coin_names)}"""
            })

            result = self._call_with_tools(
                [{"role": "user", "content": content}],
                model=MODEL,
                max_tokens=1500,
                use_thinking=True
            )

            try:
                # Parse JSON array
                start = result.find('[')
                end = result.rfind(']') + 1
                if start >= 0 and end > start:
                    batch_opps = json.loads(result[start:end])
                    for opp in batch_opps:
                        # Validate direction and confidence
                        direction = opp.get('direction', '')
                        if direction not in ('LONG', 'SHORT'):
                            continue
                        conf = float(opp.get('confidence', 0))
                        if conf > 1.0:
                            conf = conf / 100.0  # fix 65 → 0.65
                        opp['confidence'] = conf
                        if conf < 0.6:
                            continue

                        opportunities.append(opp)
                        logger.info(f"PROFI FOUND: {direction} {opp['coin']} "
                                   f"({conf:.0%}) — {opp.get('reason', '')[:50]}")
                        if on_found:
                            try:
                                on_found(opp)
                            except Exception as e:
                                logger.debug(f"Immediate open error: {e}")
            except Exception as e:
                logger.debug(f"Scan parse error: {e}")

        # Scan tools-only coins (hot movers without chart data)
        if tools_only_coins:
            hot_text = "These are HOT MOVERS from Bybit (no chart available, use tools):\n"
            hot_text += ", ".join(tools_only_coins[:10])
            hot_text += "\n\nUse get_price and get_funding_rate tools to evaluate each."
            hot_text += " These coins are moving BIG right now — find the opportunity."
            hot_text += " Reply JSON array same format as above."

            result = self._call_with_tools(
                [{"role": "user", "content": hot_text}],
                model=MODEL, max_tokens=800, use_thinking=True
            )
            try:
                start = result.find('[')
                end = result.rfind(']') + 1
                if start >= 0 and end > start:
                    hot_opps = json.loads(result[start:end])
                    for opp in hot_opps:
                        # Validate: direction must be LONG or SHORT, confidence 0-1
                        direction = opp.get('direction', '')
                        if direction not in ('LONG', 'SHORT'):
                            continue
                        conf = float(opp.get('confidence', 0))
                        if conf > 1.0:
                            conf = conf / 100.0  # fix 65 → 0.65
                        opp['confidence'] = conf
                        if conf < 0.65:  # Rule 31: min 65% — low conf trades lose money
                            continue
                        opp['source'] = 'hot_discovery'
                        opportunities.append(opp)
                        logger.info(f"HOT FOUND: {direction} {opp['coin']} "
                                   f"({conf:.0%}) — {opp.get('reason', '')[:50]}")
                        if on_found:
                            try:
                                on_found(opp)
                            except Exception:
                                pass
            except Exception:
                pass

        logger.info(f"PROFI scan: {len(opportunities)} opportunities from {len(available)} coins "
                   f"({len(charts_data)} charts + {len(tools_only_coins)} hot)")
        return opportunities

    def get_stats(self) -> dict:
        """Return API usage statistics."""
        est_cost = (self._total_input_tokens * 3 + self._total_output_tokens * 15) / 1_000_000
        return {
            'calls': self._call_count,
            'input_tokens': self._total_input_tokens,
            'output_tokens': self._total_output_tokens,
            'cache_hits': self._cache_hits,
            'est_cost_usd': round(est_cost, 4),
        }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    profi = Profi()

    # Test daily strategy
    print("=== PROFI Daily Strategy ===")
    strategy = profi.get_daily_strategy()
    print(json.dumps(strategy, indent=2))

    # Test trade analysis with chart generation
    print("\n=== PROFI Trade Analysis (with charts) ===")
    charts = {}
    try:
        from src.crypto.chart_generator import generate_chart
        for tf, days in [('1d', 60), ('4h', 30)]:
            chart_bytes = generate_chart('CRV', tf, days)
            if chart_bytes:
                charts[tf] = base64.b64encode(chart_bytes).decode()
                print(f"  Chart {tf}: {len(chart_bytes)} bytes")
    except Exception as e:
        print(f"  Chart generation: {e}")

    analysis = profi.analyze_trade('CRV', 'SHORT', charts,
        features={'rsi': 65, 'bb_position': 0.8, 'volume_ratio': 1.5,
                  'atr_pct': 0.03, 'funding_rate': 0.01})
    print(json.dumps(analysis, indent=2))

    # Stats
    print(f"\n=== API Stats ===")
    print(json.dumps(profi.get_stats(), indent=2))
