"""
FORTIX — Opportunity Detector
==============================
Detects BULLISH opportunity patterns from market data.
Not predictions — factual observations with historical context.

These are shown FIRST in Claude's data context to counterbalance
the bearish-only signal system v3.
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger('opportunity')
DB_PATH = Path('data/crypto/market.db')


def detect_opportunities() -> list:
    """Scan all data sources for bullish opportunity indicators.

    Returns list of dicts: {type, description, strength, coins}
    Sorted by strength (strongest first).
    """
    opportunities = []

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")

        # ═══ 1. INSTITUTIONAL ACCUMULATION (ETF inflows) ═══
        try:
            etf = conn.execute(
                "SELECT date, flow_usd FROM cg_etf_flows WHERE asset='BTC' "
                "ORDER BY date DESC LIMIT 7"
            ).fetchall()
            if etf:
                total = sum(r[1] for r in etf if r[1])
                positive_days = sum(1 for r in etf if r[1] and r[1] > 0)
                if total > 100_000_000:
                    opportunities.append({
                        'type': 'INSTITUTIONAL ACCUMULATION',
                        'description': f"Bitcoin ETF inflows: +${total/1e6:.0f}M in 7 days ({positive_days}/7 days positive). Institutions are buying while retail panics.",
                        'strength': min(total / 100_000_000, 5),
                        'coins': ['BTC'],
                    })
        except Exception:
            pass

        # ═══ 2. EXCHANGE ACCUMULATION (coins leaving exchanges) ═══
        try:
            flows = conn.execute(
                "SELECT date, netflow FROM cq_exchange_flows WHERE coin='BTC' "
                "ORDER BY date DESC LIMIT 7"
            ).fetchall()
            if flows:
                outflow_days = sum(1 for r in flows if r[1] and r[1] < 0)
                if outflow_days >= 3:
                    opportunities.append({
                        'type': 'EXCHANGE ACCUMULATION',
                        'description': f"Bitcoin leaving exchanges {outflow_days}/7 days. Coins moving to cold storage = accumulation pattern.",
                        'strength': outflow_days * 0.7,
                        'coins': ['BTC'],
                    })
        except Exception:
            pass

        # ═══ 3. CAPITULATION ENDING (SOPR near 1.0) ═══
        try:
            sopr = conn.execute(
                "SELECT value FROM cq_btc_onchain WHERE metric='sopr' ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if sopr and sopr[0]:
                val = float(sopr[0])
                if 0.95 <= val <= 1.02:
                    opportunities.append({
                        'type': 'CAPITULATION ENDING',
                        'description': f"SOPR at {val:.3f} — sellers are running out of profit to take. Historically, SOPR near 1.0 marks the end of capitulation.",
                        'strength': 3.5,
                        'coins': ['BTC'],
                    })
        except Exception:
            pass

        # ═══ 4. DIVERGENCE (coins rising against market) ═══
        try:
            rising = conn.execute(
                "SELECT coin, change_24h, change_7d FROM market_overview "
                "WHERE timestamp = (SELECT MAX(timestamp) FROM market_overview) "
                "AND change_7d > 3 ORDER BY change_7d DESC LIMIT 3"
            ).fetchall()
            for r in rising:
                opportunities.append({
                    'type': 'DIVERGENCE',
                    'description': f"{r[0]} up {r[1]:+.1f}% (24h) and {r[2]:+.1f}% (7d) while market is in fear — someone is accumulating.",
                    'strength': r[2] * 0.5,
                    'coins': [r[0]],
                })
        except Exception:
            pass

        # ═══ 5. SQUEEZE SETUP (negative funding + support zone) ═══
        try:
            funding = conn.execute(
                "SELECT fr.coin, fr.rate FROM funding_rates fr "
                "WHERE fr.timestamp = (SELECT MAX(timestamp) FROM funding_rates) "
                "AND fr.rate < -0.005 ORDER BY fr.rate ASC LIMIT 3"
            ).fetchall()
            for f in funding:
                opportunities.append({
                    'type': 'SQUEEZE SETUP',
                    'description': f"{f[0]} funding rate at {f[1]:.4f}% — shorts are paying longs. Historically, extreme negative funding precedes bounces.",
                    'strength': abs(f[1]) * 200,
                    'coins': [f[0]],
                })
        except Exception:
            pass

        # ═══ 6. OVERSOLD BOUNCE CANDIDATES ═══
        try:
            oversold = conn.execute(
                "SELECT coin, change_7d, price_usd FROM market_overview "
                "WHERE timestamp = (SELECT MAX(timestamp) FROM market_overview) "
                "AND change_7d < -12 ORDER BY change_7d ASC LIMIT 3"
            ).fetchall()
            for o in oversold:
                opportunities.append({
                    'type': 'OVERSOLD BOUNCE',
                    'description': f"{o[0]} down {abs(o[1]):.1f}% in 7 days — deeply oversold. History shows coins at this level bounce 65%+ of the time.",
                    'strength': abs(o[1]) * 0.3,
                    'coins': [o[0]],
                })
        except Exception:
            pass

        # ═══ 7. STABLECOIN DRY POWDER ═══
        try:
            stable = conn.execute(
                "SELECT total_market_cap FROM cg_stablecoin_supply ORDER BY date DESC LIMIT 2"
            ).fetchall()
            if len(stable) >= 2 and stable[0][0] and stable[1][0]:
                change = stable[0][0] - stable[1][0]
                supply = stable[0][0]
                if change > 0 or supply > 150_000_000_000:
                    opportunities.append({
                        'type': 'DRY POWDER READY',
                        'description': f"Stablecoin supply at ${supply/1e9:.0f}B" +
                                       (f" (grew ${change/1e9:.1f}B)" if change > 0 else "") +
                                       " — capital is sitting on the sidelines ready to deploy.",
                        'strength': 2.0 + (change / 1e9 if change > 0 else 0),
                        'coins': [],
                    })
        except Exception:
            pass

        # ═══ 8. FEAR EXTREME (contrarian) ═══
        try:
            fg = conn.execute(
                "SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if fg and fg[0] and fg[0] <= 15:
                opportunities.append({
                    'type': 'EXTREME FEAR = OPPORTUNITY',
                    'description': f"Fear & Greed at {fg[0]}. In the last 5 years, buying at extreme fear (<15) returned positive in 80%+ of cases within 30 days.",
                    'strength': (20 - fg[0]) * 0.3,
                    'coins': ['BTC'],
                })
        except Exception:
            pass

        conn.close()

    except Exception as e:
        log.warning(f"Opportunity detection failed: {e}")

    # ═══ 9. MODEL V5 RANKING (cross-sectional, no NEUTRAL) ═══
    try:
        from src.crypto.forecast_model_v5 import rank_all_coins
        ranking = rank_all_coins()
        if ranking and ranking.get('rankings'):
            regime = ranking['regime']
            for r in ranking['rankings']:
                if r['signal'] in ('STRONG_BUY', 'BUY'):
                    strength_base = 5.0 if r['signal'] == 'STRONG_BUY' else 3.5
                    opportunities.append({
                        'type': f"MODEL RANKING #{r['rank']}",
                        'description': (
                            f"{r['coin']} ranked #{r['rank']}/{ranking['n_coins_ranked']} "
                            f"by our 90-feature model (score: {r['rank_score']:.3f}). "
                            f"Market regime: {regime}. "
                            f"Confidence: {r['confidence']:.0%}."
                        ),
                        'strength': strength_base * r['confidence'],
                        'coins': [r['coin']],
                    })
    except Exception as e:
        log.debug(f"v5 ranking failed: {e}")

    # Sort by strength
    opportunities.sort(key=lambda x: x['strength'], reverse=True)
    return opportunities


def format_opportunities_for_claude(opportunities: list) -> str:
    """Format opportunity indicators for injection into Claude's data context.

    This goes BEFORE any bearish data — Claude weights early data more.
    """
    if not opportunities:
        return ""

    lines = [
        "=== OPPORTUNITY SIGNALS (present these FIRST in your script) ===",
        "",
        ""
    ]

    for opp in opportunities[:6]:  # Max 6 to not overwhelm
        coins = ', '.join(opp['coins']) if opp['coins'] else 'Market-wide'
        lines.append(f"  {opp['type']}: {opp['description']}")

    lines.append("")
    lines.append("These are context signals — use them if relevant to your chosen topic.")
    lines.append("")

    return '\n'.join(lines)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    opps = detect_opportunities()
    print(f"Found {len(opps)} opportunity indicators:\n")
    for o in opps:
        coins = ', '.join(o['coins']) if o['coins'] else 'Market'
        print(f"  [{o['type']}] ({coins}) strength={o['strength']:.1f}")
        print(f"    {o['description']}")
        print()
    print("=== FORMATTED FOR CLAUDE ===")
    print(format_opportunities_for_claude(opps))
