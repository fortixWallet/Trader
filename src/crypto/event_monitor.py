"""
FORTIX — Event Monitor (Token Unlocks + Hacks + Listings)
==========================================================
Monitors crypto-specific events that directly impact prices:
  - Token unlocks (sell pressure)
  - Hacks/exploits (instant dumps)
  - Major exchange listings (pumps)
"""

import json
import logging
import requests
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger('event_monitor')
DB_PATH = Path('data/crypto/market.db')

# Map DeFi Llama coin names to our tickers
COIN_NAME_MAP = {
    'bitcoin': 'BTC', 'ethereum': 'ETH', 'solana': 'SOL',
    'bnb': 'BNB', 'ripple': 'XRP', 'xrp': 'XRP',
    'cardano': 'ADA', 'avalanche': 'AVAX', 'polkadot': 'DOT',
    'chainlink': 'LINK', 'dogecoin': 'DOGE', 'uniswap': 'UNI',
    'aave': 'AAVE', 'pendle': 'PENDLE', 'lido': 'LDO',
    'curve': 'CRV', 'arbitrum': 'ARB', 'optimism': 'OP',
    'polygon': 'POL', 'fetch-ai': 'FET', 'render': 'RENDER',
    'bittensor': 'TAO', 'pepe': 'PEPE', 'bonk': 'BONK',
    'dogwifhat': 'WIF', 'shiba-inu': 'SHIB',
}


def _init_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS token_unlocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin TEXT NOT NULL,
            unlock_date TEXT NOT NULL,
            amount_usd REAL,
            percentage_supply REAL,
            description TEXT,
            collected_at TEXT,
            UNIQUE(coin, unlock_date)
        );
        CREATE TABLE IF NOT EXISTS crypto_hacks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            protocol TEXT NOT NULL,
            amount_usd REAL,
            chain TEXT,
            description TEXT,
            coins_affected TEXT,
            collected_at TEXT,
            UNIQUE(date, protocol)
        );
    """)
    conn.commit()


def collect_token_unlocks() -> int:
    """Collect upcoming token unlocks from DeFi Llama.

    Large unlock = predictable sell pressure.
    """
    log.info("Collecting token unlocks...")
    count = 0

    try:
        # Try multiple DeFi Llama endpoints (API changes frequently)
        resp = None
        for url in ['https://api.llama.fi/emissions/breakdown',
                     'https://api.llama.fi/unlocks',
                     'https://coins.llama.fi/unlocks']:
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code == 200:
                    break
            except Exception:
                continue
        if not resp or resp.status_code != 200:
            log.warning(f"DeFi Llama unlocks: no valid response")
            return 0

        data = resp.json()
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        _init_tables(conn)

        today = datetime.now(timezone.utc)
        week_ahead = (today + timedelta(days=14)).strftime('%Y-%m-%d')

        for protocol in data:
            name = protocol.get('name', '').lower()
            coin = COIN_NAME_MAP.get(name)
            if not coin:
                # Try matching symbol
                symbol = protocol.get('symbol', '').upper()
                if symbol in ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX',
                              'DOT', 'LINK', 'DOGE', 'ARB', 'OP', 'UNI', 'AAVE',
                              'FET', 'RENDER', 'TAO', 'PEPE']:
                    coin = symbol
                else:
                    continue

            events = protocol.get('events', [])
            for event in events:
                unlock_date = event.get('date', '')
                if isinstance(unlock_date, (int, float)):
                    unlock_date = datetime.fromtimestamp(unlock_date, tz=timezone.utc).strftime('%Y-%m-%d')
                elif len(str(unlock_date)) > 10:
                    unlock_date = str(unlock_date)[:10]

                # Only upcoming (next 14 days)
                if unlock_date > week_ahead:
                    continue

                amount = event.get('amount', 0)
                pct = event.get('percentage', 0)
                desc = event.get('description', f"{coin} token unlock")

                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO token_unlocks "
                        "(coin, unlock_date, amount_usd, percentage_supply, description, collected_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (coin, unlock_date, amount, pct, desc,
                         datetime.now(timezone.utc).isoformat())
                    )
                    count += 1
                except Exception:
                    pass

        conn.commit()
        conn.close()
        log.info(f"  Stored {count} upcoming token unlocks")

    except Exception as e:
        log.warning(f"Token unlock collection failed: {e}")

    return count


def collect_recent_hacks() -> int:
    """Collect recent crypto hacks/exploits from DeFi Llama.

    Hack > $10M → immediate market impact.
    """
    log.info("Collecting recent hacks...")
    count = 0

    try:
        resp = requests.get('https://api.llama.fi/hacks', timeout=15)
        if resp.status_code != 200:
            log.warning(f"DeFi Llama hacks: {resp.status_code}")
            return 0

        hacks = resp.json()
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        _init_tables(conn)

        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

        for hack in hacks:
            date = hack.get('date', '')
            if isinstance(date, (int, float)):
                date = datetime.fromtimestamp(date, tz=timezone.utc).strftime('%Y-%m-%d')
            elif len(str(date)) > 10:
                date = str(date)[:10]

            if date < cutoff:
                continue

            amount = hack.get('amount', 0) or 0
            if amount < 1_000_000:  # Skip < $1M
                continue

            protocol = hack.get('name', 'Unknown')
            chain = hack.get('chain', 'Unknown')

            # Map chain to our coins
            chain_map = {'Ethereum': 'ETH', 'Solana': 'SOL', 'BSC': 'BNB',
                         'Avalanche': 'AVAX', 'Arbitrum': 'ARB', 'Optimism': 'OP',
                         'Polygon': 'POL'}
            coins = [chain_map.get(chain, '')] if chain in chain_map else []

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO crypto_hacks "
                    "(date, protocol, amount_usd, chain, description, coins_affected, collected_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (date, protocol, amount, chain,
                     hack.get('technique', ''),
                     json.dumps(coins),
                     datetime.now(timezone.utc).isoformat())
                )
                count += 1
            except Exception:
                pass

        conn.commit()
        conn.close()
        log.info(f"  Stored {count} recent hacks")

    except Exception as e:
        log.warning(f"Hack collection failed: {e}")

    return count


def get_event_context() -> str:
    """Build event context string for Claude's data injection."""
    lines = []
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    week_ahead = (datetime.now(timezone.utc) + timedelta(days=7)).strftime('%Y-%m-%d')

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        _init_tables(conn)

        # Upcoming token unlocks (next 7 days)
        unlocks = conn.execute(
            "SELECT coin, unlock_date, amount_usd, percentage_supply, description "
            "FROM token_unlocks WHERE unlock_date BETWEEN ? AND ? "
            "ORDER BY amount_usd DESC LIMIT 5",
            (today, week_ahead)
        ).fetchall()

        if unlocks:
            lines.append("=== UPCOMING TOKEN UNLOCKS (sell pressure warning) ===")
            for u in unlocks:
                amt = f"${u[2]/1e6:.0f}M" if u[2] and u[2] > 1e6 else f"${u[2]:,.0f}" if u[2] else "Unknown"
                pct = f" ({u[3]:.1f}% of supply)" if u[3] else ""
                lines.append(f"  {u[0]} on {u[1]}: {amt}{pct} — {u[4] or 'token unlock'}")
            lines.append("")

        # Recent hacks (last 7 days)
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
        hacks = conn.execute(
            "SELECT date, protocol, amount_usd, chain FROM crypto_hacks "
            "WHERE date >= ? ORDER BY amount_usd DESC LIMIT 3",
            (week_ago,)
        ).fetchall()

        if hacks:
            lines.append("=== RECENT HACKS/EXPLOITS ===")
            for h in hacks:
                amt = f"${h[2]/1e6:.0f}M" if h[2] else "Unknown"
                lines.append(f"  {h[1]} ({h[3]}): {amt} exploited on {h[0]}")
            lines.append("")

        conn.close()

    except Exception as e:
        log.warning(f"Event context failed: {e}")

    return '\n'.join(lines)


def run_event_scan() -> dict:
    """Run full event scan. Called by orchestrator."""
    unlocks = collect_token_unlocks()
    hacks = collect_recent_hacks()
    context = get_event_context()

    return {
        'unlocks': unlocks,
        'hacks': hacks,
        'context_length': len(context),
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    result = run_event_scan()
    print(f"\nResults: {result}")
    print(f"\n=== CONTEXT ===")
    print(get_event_context())
