"""
FORTIX — Political & Regulatory Monitor
=========================================
Monitors political events, regulatory decisions, and macro catalysts
that impact crypto prices.

Sources:
  - CryptoPanic API (regulation filter)
  - FRED macro events (FOMC, CPI dates)
  - SEC.gov (future: RSS feed)

Impact examples:
  - SEC vs Ripple ruling → XRP +300%
  - BTC ETF approval → BTC +40%
  - China crypto ban → Market -30%
  - Trump pro-crypto statement → Market +15%
  - FOMC rate decision → BTC ±3-5%
"""

import os
import json
import sqlite3
import logging
import calendar
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger('political_monitor')
_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'

# ── CryptoPanic rate-limit protection ──
# Cache results in memory so repeated calls within the cooldown window
# (e.g. orchestrator running every 10 min) don't hit the API again.
_CRYPTOPANIC_COOLDOWN_SEC = 30 * 60  # 30 minutes
_cryptopanic_cache: dict = {
    'last_fetch': None,   # datetime (UTC) of last successful API call
    'results': [],        # cached list of events
}

# FOMC scheduled meeting dates (public schedule, 2-day meetings)
# Source: federalreserve.gov/monetarypolicy/fomccalendars.htm
# Update annually when the Fed publishes the next year's schedule.
_FOMC_DATES_BY_YEAR = {
    2026: [
        '2026-01-28', '2026-01-29',  # Jan
        '2026-03-17', '2026-03-18',  # Mar
        '2026-05-05', '2026-05-06',  # May
        '2026-06-16', '2026-06-17',  # Jun
        '2026-07-28', '2026-07-29',  # Jul
        '2026-09-15', '2026-09-16',  # Sep
        '2026-10-27', '2026-10-28',  # Oct
        '2026-12-15', '2026-12-16',  # Dec
    ],
    2027: [
        '2027-01-26', '2027-01-27',  # Jan
        '2027-03-16', '2027-03-17',  # Mar
        '2027-04-27', '2027-04-28',  # Apr/May
        '2027-06-15', '2027-06-16',  # Jun
        '2027-07-27', '2027-07-28',  # Jul
        '2027-09-21', '2027-09-22',  # Sep
        '2027-11-02', '2027-11-03',  # Nov
        '2027-12-14', '2027-12-15',  # Dec
    ],
}


def _get_fomc_dates() -> list:
    """Return FOMC dates for the current and next year.

    Uses known schedules when available. For years without hardcoded dates,
    estimates based on the Fed's typical pattern (8 meetings per year,
    roughly every 6-7 weeks starting late January).
    """
    current_year = datetime.now(timezone.utc).year
    dates = []
    for year in (current_year, current_year + 1):
        if year in _FOMC_DATES_BY_YEAR:
            dates.extend(_FOMC_DATES_BY_YEAR[year])
        else:
            # Estimate: 8 meetings per year based on typical FOMC pattern
            # 3rd Tue-Wed of each meeting month. Approximate — real dates
            # may shift by a few days. Update _FOMC_DATES_BY_YEAR when the
            # Fed publishes the official schedule for the new year.
            estimated_months = [1, 3, 5, 6, 7, 9, 11, 12]
            for month in estimated_months:
                # Typically Tue-Wed in the 3rd or 4th week
                # Use 3rd Tuesday as a reasonable estimate
                first_day_weekday, days_in_month = calendar.monthrange(year, month)
                # Find first Tuesday (weekday=1)
                first_tue = (1 - first_day_weekday) % 7 + 1
                third_tue = first_tue + 14
                if third_tue > days_in_month:
                    third_tue -= 7
                wed = third_tue + 1
                dates.append(f'{year}-{month:02d}-{third_tue:02d}')
                dates.append(f'{year}-{month:02d}-{wed:02d}')
    return dates

# Keywords for political/regulatory crypto events
POLITICAL_KEYWORDS = [
    'sec', 'regulation', 'congress', 'senate', 'bill', 'law', 'ban',
    'legal', 'court', 'ruling', 'lawsuit', 'settlement', 'fine',
    'trump', 'biden', 'gensler', 'treasury', 'irs', 'tax',
    'etf', 'approval', 'rejected', 'stablecoin bill',
    'cbdc', 'digital dollar', 'executive order',
    'china', 'russia', 'sanctions', 'sanctions', 'geopolit',
    'fed', 'fomc', 'rate cut', 'rate hike', 'inflation', 'cpi',
    'blackrock', 'fidelity', 'grayscale', 'institutional',
]

# Coins most affected by regulation
REGULATION_COIN_MAP = {
    'xrp': ['XRP'],
    'ripple': ['XRP'],
    'ethereum': ['ETH'],
    'solana': ['SOL'],
    'binance': ['BNB'],
    'bnb': ['BNB'],
    'stablecoin': ['USDT', 'USDC'],
    'defi': ['UNI', 'AAVE', 'PENDLE'],
    'bitcoin': ['BTC'],
    'btc': ['BTC'],
    'cardano': ['ADA'],
    'dogecoin': ['DOGE'],
}


def _init_tables(conn):
    """Create political events table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS political_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            event TEXT NOT NULL,
            source TEXT,
            impact_score INTEGER DEFAULT 0,
            direction TEXT,
            coins_affected TEXT,
            category TEXT,
            url TEXT,
            analyzed_at TEXT,
            UNIQUE(date, event)
        )
    """)
    conn.commit()


def collect_regulatory_news() -> list:
    """Collect crypto regulatory/political news from CryptoPanic.

    Returns list of {event, impact, coins, category, url}.
    Uses an in-memory cache to avoid hitting CryptoPanic more than once
    per 30-minute window (prevents 429 rate-limit errors).
    """
    events = []

    # ── Rate-limit guard: return cached results if still fresh ──
    now = datetime.now(timezone.utc)
    last = _cryptopanic_cache['last_fetch']
    if last and (now - last).total_seconds() < _CRYPTOPANIC_COOLDOWN_SEC:
        age_min = (now - last).total_seconds() / 60
        log.debug(f"CryptoPanic cache hit ({age_min:.0f}m old, cooldown {_CRYPTOPANIC_COOLDOWN_SEC // 60}m)")
        return list(_cryptopanic_cache['results'])  # return a copy

    try:
        api_key = os.getenv('CRYPTOPANIC_API_KEY', '')
        if not api_key:
            log.warning("No CryptoPanic API key")
            return events

        # Fetch regulation-tagged news
        url = f"https://cryptopanic.com/api/developer/v2/posts/"
        params = {
            'auth_token': api_key,
            'filter': 'important',
            'kind': 'news',
            'regions': 'en',
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            log.warning(f"CryptoPanic: {resp.status_code}")
            return events

        articles = resp.json().get('results', [])

        for article in articles[:30]:
            title = article.get('title', '').lower()
            # Check if it's political/regulatory
            is_political = any(kw in title for kw in POLITICAL_KEYWORDS)
            if not is_political:
                continue

            # Determine affected coins
            coins = []
            for keyword, coin_list in REGULATION_COIN_MAP.items():
                if keyword in title:
                    coins.extend(coin_list)
            coins = list(set(coins)) or ['BTC']  # Default to BTC if no specific coin

            # Determine category
            if any(w in title for w in ['sec', 'regulation', 'court', 'ruling', 'lawsuit', 'ban']):
                category = 'regulatory'
            elif any(w in title for w in ['trump', 'biden', 'congress', 'senate', 'bill']):
                category = 'political'
            elif any(w in title for w in ['fomc', 'fed', 'rate', 'inflation', 'cpi']):
                category = 'macro'
            elif any(w in title for w in ['china', 'russia', 'sanctions', 'geopolit']):
                category = 'geopolitical'
            elif any(w in title for w in ['etf', 'blackrock', 'fidelity', 'institutional']):
                category = 'institutional'
            else:
                category = 'other'

            # Estimate impact based on keywords
            high_impact = ['ban', 'approval', 'ruling', 'lawsuit', 'etf', 'executive order', 'sanctions']
            medium_impact = ['regulation', 'bill', 'investigation', 'fine', 'trump', 'fed']

            impact = 5  # default medium
            if any(w in title for w in high_impact):
                impact = 8
            elif any(w in title for w in medium_impact):
                impact = 6

            events.append({
                'event': article.get('title', ''),
                'impact': impact,
                'coins': coins,
                'category': category,
                'url': article.get('url', ''),
                'published': article.get('published_at', '')[:10],
            })

    except Exception as e:
        log.warning(f"Political news collection failed: {e}")

    # ── Update cache on successful fetch ──
    _cryptopanic_cache['last_fetch'] = datetime.now(timezone.utc)
    _cryptopanic_cache['results'] = list(events)  # store a copy

    return events


def check_upcoming_macro_events() -> list:
    """Check for upcoming FOMC, CPI, and other macro events.

    Returns list of {event, days_until, impact, category}.
    """
    events = []
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    today_dt = datetime.now(timezone.utc)

    # FOMC meetings
    for fomc_date in _get_fomc_dates():
        fomc_dt = datetime.strptime(fomc_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        days_until = (fomc_dt - today_dt).days
        if 0 <= days_until <= 7:
            events.append({
                'event': f"FOMC Meeting {'TODAY' if days_until == 0 else f'in {days_until} days'} ({fomc_date})",
                'days_until': days_until,
                'impact': 9 if days_until <= 1 else 7,
                'category': 'macro',
                'coins': ['BTC', 'ETH'],
                'description': 'FOMC rate decisions historically move BTC 3-5%. Rate cut = bullish, hold/hike = bearish short-term.'
            })

    # Check FRED macro_events table for upcoming CPI/NFP
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        # Look for CPI dates coming up
        upcoming = conn.execute(
            "SELECT date, event_type, value FROM macro_events "
            "WHERE date >= ? AND event_type IN ('cpi', 'nfp', 'fomc') "
            "ORDER BY date LIMIT 5",
            (today,)
        ).fetchall()
        for u in upcoming:
            days = (datetime.strptime(u[0], '%Y-%m-%d').replace(tzinfo=timezone.utc) - today_dt).days
            if 0 <= days <= 7:
                name = {'cpi': 'CPI Data Release', 'nfp': 'Non-Farm Payrolls', 'fomc': 'FOMC Decision'}.get(u[1], u[1])
                events.append({
                    'event': f"{name} {'TODAY' if days == 0 else f'in {days} days'}",
                    'days_until': days,
                    'impact': 8 if days <= 1 else 6,
                    'category': 'macro',
                    'coins': ['BTC', 'ETH'],
                })
        conn.close()
    except Exception:
        pass

    return events


def store_political_events(events: list):
    """Store political/regulatory events in database."""
    if not events:
        return

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        _init_tables(conn)

        count = 0
        for ev in events:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO political_events "
                    "(date, event, source, impact_score, direction, coins_affected, category, url, analyzed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ev.get('published', datetime.now(timezone.utc).strftime('%Y-%m-%d')),
                        ev['event'],
                        'cryptopanic',
                        ev.get('impact', 5),
                        ev.get('direction', 'unknown'),
                        json.dumps(ev.get('coins', [])),
                        ev.get('category', 'other'),
                        ev.get('url', ''),
                        datetime.now(timezone.utc).isoformat(),
                    )
                )
                count += 1
            except Exception:
                pass

        conn.commit()
        conn.close()
        if count:
            log.info(f"Stored {count} political/regulatory events")

    except Exception as e:
        log.warning(f"Political event storage failed: {e}")


def get_political_context() -> str:
    """Build political/regulatory context string for Claude's data injection.

    Returns formatted string of recent political events + upcoming macro.
    """
    lines = []

    # Recent political events (last 48h)
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        _init_tables(conn)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime('%Y-%m-%d')
        recent = conn.execute(
            "SELECT event, impact_score, coins_affected, category FROM political_events "
            "WHERE date >= ? ORDER BY impact_score DESC LIMIT 5",
            (cutoff,)
        ).fetchall()
        conn.close()

        if recent:
            lines.append("=== POLITICAL & REGULATORY EVENTS (last 48h) ===")
            for r in recent:
                coins = json.loads(r[2]) if r[2] else []
                coins_str = ', '.join(coins) if coins else 'Market-wide'
                lines.append(f"  [{r[3].upper()}] Impact {r[1]}/10: {r[0]} (affects: {coins_str})")
            lines.append("")
    except Exception:
        pass

    # Upcoming macro events
    macro = check_upcoming_macro_events()
    if macro:
        lines.append("=== UPCOMING MACRO CATALYSTS ===")
        for m in macro:
            desc = m.get('description', '')
            lines.append(f"  {m['event']} — Impact {m['impact']}/10")
            if desc:
                lines.append(f"    {desc}")
        lines.append("")

    return '\n'.join(lines)


def run_political_scan() -> dict:
    """Run full political/regulatory scan. Called by orchestrator."""
    log.info("Running political & regulatory scan...")

    # Collect news
    events = collect_regulatory_news()
    if events:
        store_political_events(events)
        log.info(f"  Found {len(events)} political/regulatory events")

    # Check macro calendar
    macro = check_upcoming_macro_events()
    if macro:
        log.info(f"  Upcoming macro events: {len(macro)}")
        for m in macro:
            log.info(f"    {m['event']}")

    # Build context
    context = get_political_context()

    return {
        'political_events': len(events),
        'macro_events': len(macro),
        'context_length': len(context),
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    result = run_political_scan()
    print(f"\nResults: {result}")
    print(f"\n=== CONTEXT FOR CLAUDE ===")
    print(get_political_context())
