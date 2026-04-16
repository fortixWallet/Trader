"""
FORTIX — Arkham-style Whale Identification & History Tracker
=============================================================
Local whale identification system using known addresses/entities.
Arkham Intelligence does NOT have a free public API — their API requires
an enterprise subscription. This module provides the same functionality
locally using a curated database of known whale addresses and entities.

Features:
  - identify_whale(address_or_description) — match whale labels to known entities
  - get_whale_history(entity_name) — track historical accuracy (bought → what happened)
  - format_whale_context(whale_transactions) — rich context for script injection

Usage:
    from src.crypto.arkham_tracker import identify_whale, format_whale_context
    enriched = format_whale_context(conn)
"""

import re
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger('arkham_tracker')

DB_PATH = Path('data/crypto/market.db')


# ════════════════════════════════════════════
# KNOWN WHALE ENTITIES DATABASE
# ════════════════════════════════════════════

# Maps label fragments (lowercase) → entity info
# Source: public blockchain explorers, Arkham Intel labels, Whale Alert tags
KNOWN_ENTITIES = {
    # ── Institutional / Corporate ──
    'microstrategy': {
        'name': 'MicroStrategy',
        'type': 'corporate',
        'bias': 'perma-bull',
        'description': 'Largest corporate BTC holder (~214k BTC). CEO Michael Saylor. Never sold.',
        'coins': ['BTC'],
    },
    'marathon': {
        'name': 'Marathon Digital',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Largest public BTC miner. HODL strategy — rarely sells.',
        'coins': ['BTC'],
    },
    'marathon digital': {
        'name': 'Marathon Digital',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Largest public BTC miner. HODL strategy — rarely sells.',
        'coins': ['BTC'],
    },
    'grayscale': {
        'name': 'Grayscale',
        'type': 'fund',
        'bias': 'neutral',
        'description': 'Largest crypto fund manager. GBTC/ETHE ETFs. Outflows = sell pressure.',
        'coins': ['BTC', 'ETH'],
    },
    'galaxy digital': {
        'name': 'Galaxy Digital',
        'type': 'fund',
        'bias': 'bull',
        'description': 'Mike Novogratz crypto merchant bank. Active trader.',
        'coins': ['BTC', 'ETH'],
    },
    'galaxy': {
        'name': 'Galaxy Digital',
        'type': 'fund',
        'bias': 'bull',
        'description': 'Mike Novogratz crypto merchant bank. Active trader.',
        'coins': ['BTC', 'ETH'],
    },
    'tesla': {
        'name': 'Tesla',
        'type': 'corporate',
        'bias': 'holder',
        'description': 'Holds ~9,720 BTC. Sold 75% in Q2 2022. Remaining position stable.',
        'coins': ['BTC'],
    },
    'el salvador': {
        'name': 'El Salvador',
        'type': 'sovereign',
        'bias': 'accumulator',
        'description': 'Nation-state BTC buyer. Buys 1 BTC/day. ~6,000 BTC total.',
        'coins': ['BTC'],
    },
    'block.one': {
        'name': 'Block.one',
        'type': 'corporate',
        'bias': 'holder',
        'description': 'EOS creator. Holds ~164k BTC. Rarely moves.',
        'coins': ['BTC'],
    },
    'blockone': {
        'name': 'Block.one',
        'type': 'corporate',
        'bias': 'holder',
        'description': 'EOS creator. Holds ~164k BTC. Rarely moves.',
        'coins': ['BTC'],
    },
    'tether': {
        'name': 'Tether Treasury',
        'type': 'stablecoin',
        'bias': 'neutral',
        'description': 'USDT mints = new liquidity entering market. Burns = liquidity exit.',
        'coins': ['USDT'],
    },
    'circle': {
        'name': 'Circle (USDC)',
        'type': 'stablecoin',
        'bias': 'neutral',
        'description': 'USDC issuer. Mints/burns reflect institutional demand.',
        'coins': ['USDC'],
    },

    # ── Trading Firms / Market Makers ──
    'jump trading': {
        'name': 'Jump Trading',
        'type': 'market_maker',
        'bias': 'neutral',
        'description': 'Major market maker. Large moves often = repositioning, not directional bet.',
        'coins': ['BTC', 'ETH', 'SOL'],
    },
    'jump': {
        'name': 'Jump Trading',
        'type': 'market_maker',
        'bias': 'neutral',
        'description': 'Major market maker. Large moves often = repositioning, not directional bet.',
        'coins': ['BTC', 'ETH', 'SOL'],
    },
    'wintermute': {
        'name': 'Wintermute',
        'type': 'market_maker',
        'bias': 'neutral',
        'description': 'Top crypto market maker. Active on-chain. Moves = liquidity provision.',
        'coins': ['BTC', 'ETH', 'SOL'],
    },
    'cumberland': {
        'name': 'Cumberland (DRW)',
        'type': 'market_maker',
        'bias': 'neutral',
        'description': 'OTC desk of DRW. Large block trades for institutions.',
        'coins': ['BTC', 'ETH'],
    },
    'alameda': {
        'name': 'Alameda/FTX Estate',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'FTX bankruptcy estate. Selling assets to repay creditors. Moves = forced liquidation.',
        'coins': ['BTC', 'ETH', 'SOL'],
    },
    'ftx': {
        'name': 'Alameda/FTX Estate',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'FTX bankruptcy estate. Selling assets to repay creditors. Moves = forced liquidation.',
        'coins': ['BTC', 'ETH', 'SOL'],
    },
    'celsius': {
        'name': 'Celsius Estate',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'Celsius bankruptcy. Distributing remaining assets.',
        'coins': ['BTC', 'ETH'],
    },
    'genesis': {
        'name': 'Genesis Trading',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'Genesis bankruptcy estate. Liquidating positions.',
        'coins': ['BTC', 'ETH'],
    },

    # ── Exchange Wallets (when labeled) ──
    'binance cold': {
        'name': 'Binance Cold Wallet',
        'type': 'exchange_reserve',
        'bias': 'neutral',
        'description': 'Binance cold storage. Movements = internal treasury management.',
        'coins': ['BTC', 'ETH'],
    },
    'binance': {
        'name': 'Binance',
        'type': 'exchange',
        'bias': 'neutral',
        'description': 'Largest crypto exchange. Hot wallet moves = user deposits/withdrawals.',
        'coins': ['BTC', 'ETH'],
    },
    'coinbase prime': {
        'name': 'Coinbase Prime',
        'type': 'exchange_institutional',
        'bias': 'neutral',
        'description': 'Coinbase institutional custody. Moves often linked to ETF rebalancing.',
        'coins': ['BTC', 'ETH'],
    },
    'coinbase': {
        'name': 'Coinbase',
        'type': 'exchange',
        'bias': 'neutral',
        'description': 'US exchange. Large withdrawals = institutional accumulation.',
        'coins': ['BTC', 'ETH'],
    },
    'kraken': {
        'name': 'Kraken',
        'type': 'exchange',
        'bias': 'neutral',
        'description': 'Major exchange.',
        'coins': ['BTC', 'ETH'],
    },
    'bitfinex': {
        'name': 'Bitfinex',
        'type': 'exchange',
        'bias': 'neutral',
        'description': 'Tether-affiliated exchange. Large whale activity.',
        'coins': ['BTC', 'ETH'],
    },

    # ── ETF Custodians ──
    'blackrock': {
        'name': 'BlackRock (iShares)',
        'type': 'etf_custodian',
        'bias': 'institutional_flow',
        'description': 'IBIT ETF custodian. Inflows = institutional buying. Largest BTC ETF.',
        'coins': ['BTC', 'ETH'],
    },
    'fidelity': {
        'name': 'Fidelity (FBTC)',
        'type': 'etf_custodian',
        'bias': 'institutional_flow',
        'description': 'FBTC ETF custodian. Second largest spot BTC ETF.',
        'coins': ['BTC'],
    },
    'ark invest': {
        'name': 'ARK Invest (ARKB)',
        'type': 'etf_custodian',
        'bias': 'institutional_flow',
        'description': 'Cathie Wood ARKB ETF. Aggressive buyer in dips.',
        'coins': ['BTC'],
    },
    'ark': {
        'name': 'ARK Invest (ARKB)',
        'type': 'etf_custodian',
        'bias': 'institutional_flow',
        'description': 'Cathie Wood ARKB ETF. Aggressive buyer in dips.',
        'coins': ['BTC'],
    },
    'bitwise': {
        'name': 'Bitwise (BITB)',
        'type': 'etf_custodian',
        'bias': 'institutional_flow',
        'description': 'BITB ETF. Smaller but growing AUM.',
        'coins': ['BTC'],
    },

    # ── DeFi / Protocol Treasuries ──
    'ethereum foundation': {
        'name': 'Ethereum Foundation',
        'type': 'foundation',
        'bias': 'periodic_seller',
        'description': 'Sells ETH periodically for operational funding. Moves cause FUD.',
        'coins': ['ETH'],
    },
    'vitalik': {
        'name': 'Vitalik Buterin',
        'type': 'founder',
        'bias': 'holder',
        'description': 'Ethereum co-founder. Mostly donates, rarely sells for profit.',
        'coins': ['ETH'],
    },
    'justin sun': {
        'name': 'Justin Sun',
        'type': 'whale',
        'bias': 'active_trader',
        'description': 'TRON founder. Active DeFi whale. Frequent large moves.',
        'coins': ['ETH', 'TRX', 'BTC'],
    },
    'sun': {
        'name': 'Justin Sun',
        'type': 'whale',
        'bias': 'active_trader',
        'description': 'TRON founder. Active DeFi whale. Frequent large moves.',
        'coins': ['ETH', 'TRX', 'BTC'],
    },

    # ── Government / Seized ──
    'us government': {
        'name': 'US Government',
        'type': 'government',
        'bias': 'forced_seller',
        'description': 'DOJ/US Marshals seized BTC. Sells via Coinbase. Sales = price pressure.',
        'coins': ['BTC'],
    },
    'us marshal': {
        'name': 'US Government',
        'type': 'government',
        'bias': 'forced_seller',
        'description': 'DOJ/US Marshals seized BTC. Sells via Coinbase.',
        'coins': ['BTC'],
    },
    'german government': {
        'name': 'German Government (BKA)',
        'type': 'government',
        'bias': 'forced_seller',
        'description': 'Sold 50k BTC in July 2024. May have more seized assets.',
        'coins': ['BTC'],
    },
    'mt. gox': {
        'name': 'Mt. Gox Trustee',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'Distributing ~142k BTC to creditors. Waves of selling expected.',
        'coins': ['BTC'],
    },
    'mt.gox': {
        'name': 'Mt. Gox Trustee',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'Distributing ~142k BTC to creditors. Waves of selling expected.',
        'coins': ['BTC'],
    },
    'mtgox': {
        'name': 'Mt. Gox Trustee',
        'type': 'bankrupt_estate',
        'bias': 'forced_seller',
        'description': 'Distributing ~142k BTC to creditors. Waves of selling expected.',
        'coins': ['BTC'],
    },
    'silk road': {
        'name': 'Silk Road Seized (DOJ)',
        'type': 'government',
        'bias': 'forced_seller',
        'description': 'DOJ-seized BTC from Silk Road. Periodic auction/sales.',
        'coins': ['BTC'],
    },

    # ── Mining Pools ──
    'f2pool': {
        'name': 'F2Pool',
        'type': 'miner',
        'bias': 'periodic_seller',
        'description': 'Major mining pool. Sells regularly to cover operating costs.',
        'coins': ['BTC'],
    },
    'foundry': {
        'name': 'Foundry USA',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Largest US mining pool (DCG subsidiary). Tends to HODL.',
        'coins': ['BTC'],
    },
    'antpool': {
        'name': 'AntPool (Bitmain)',
        'type': 'miner',
        'bias': 'periodic_seller',
        'description': 'Bitmain mining pool. Mixed strategy — holds some, sells some.',
        'coins': ['BTC'],
    },
    'riot': {
        'name': 'Riot Platforms',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Public BTC miner. HODL strategy. One of largest public holders.',
        'coins': ['BTC'],
    },
    'cleanspark': {
        'name': 'CleanSpark',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Public BTC miner. Aggressive accumulation strategy.',
        'coins': ['BTC'],
    },
    'hut 8': {
        'name': 'Hut 8 Mining',
        'type': 'miner',
        'bias': 'accumulator',
        'description': 'Canadian BTC miner. HODL strategy.',
        'coins': ['BTC'],
    },
}


def identify_whale(label_or_description: str) -> Optional[dict]:
    """Identify a whale entity from a label or transaction description.

    Args:
        label_or_description: Whale Alert label, address tag, or description text.

    Returns:
        Entity dict with name, type, bias, description, coins — or None if unknown.
    """
    if not label_or_description:
        return None

    text = label_or_description.lower().strip()

    # Try exact match first
    if text in KNOWN_ENTITIES:
        return KNOWN_ENTITIES[text].copy()

    # Try substring match (longest match wins for specificity)
    best_match = None
    best_len = 0
    for key, entity in KNOWN_ENTITIES.items():
        if key in text and len(key) > best_len:
            best_match = entity
            best_len = len(key)

    if best_match:
        return best_match.copy()

    return None


def get_whale_history(entity_name: str, conn: sqlite3.Connection = None,
                      days: int = 90) -> dict:
    """Track an entity's historical whale moves and what happened to price after.

    Returns:
        {
            'entity': str,
            'total_moves': int,
            'moves': [{'date': str, 'coin': str, 'amount_usd': float,
                        'direction': str, 'price_then': float,
                        'price_7d_after': float, 'pct_change_7d': float}],
            'accuracy': float,  # % of times their direction predicted price move
            'avg_move_usd': float,
        }
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        close_conn = True

    try:
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

        # Find all transactions involving this entity
        entity_lower = entity_name.lower()
        rows = conn.execute(
            "SELECT coin, amount_usd, from_label, to_label, timestamp, blockchain "
            "FROM whale_transactions WHERE timestamp > ? AND amount_usd > 0 "
            "AND (LOWER(from_label) LIKE ? OR LOWER(to_label) LIKE ?) "
            "ORDER BY timestamp DESC",
            (cutoff, f'%{entity_lower}%', f'%{entity_lower}%')
        ).fetchall()

        if not rows:
            return {
                'entity': entity_name,
                'total_moves': 0,
                'moves': [],
                'accuracy': 0.0,
                'avg_move_usd': 0.0,
            }

        moves = []
        correct_calls = 0
        total_evaluated = 0

        for r in rows:
            coin, amount_usd, from_label, to_label, ts, chain = r

            # Determine direction (accumulation or distribution)
            fl = (from_label or '').lower()
            tl = (to_label or '').lower()
            if entity_lower in fl:
                direction = 'sent'  # entity is sender
            else:
                direction = 'received'  # entity is receiver

            # Classify as buy/sell signal
            from src.crypto.script_generator import _classify_whale_tx
            tx_type = _classify_whale_tx(from_label, to_label)

            if tx_type == 'to_exchange':
                signal = 'sell'
            elif tx_type == 'from_exchange':
                signal = 'buy'
            else:
                signal = 'move'

            # Get price at time of transaction
            move_date = datetime.fromtimestamp(ts, tz=timezone.utc)
            price_then = _get_price_at_time(conn, coin, ts)

            # Get price 7 days later
            ts_7d = ts + 7 * 86400
            price_7d = _get_price_at_time(conn, coin, ts_7d)

            pct_change = 0.0
            if price_then and price_7d and price_then > 0:
                pct_change = (price_7d - price_then) / price_then * 100

                # Evaluate: did the whale's move correctly predict direction?
                if signal == 'buy' and pct_change > 0:
                    correct_calls += 1
                    total_evaluated += 1
                elif signal == 'sell' and pct_change < 0:
                    correct_calls += 1
                    total_evaluated += 1
                elif signal in ('buy', 'sell'):
                    total_evaluated += 1

            moves.append({
                'date': move_date.strftime('%Y-%m-%d %H:%M'),
                'coin': coin,
                'amount_usd': amount_usd or 0,
                'direction': signal,
                'price_then': price_then,
                'price_7d_after': price_7d,
                'pct_change_7d': round(pct_change, 2),
            })

        accuracy = (correct_calls / total_evaluated * 100) if total_evaluated > 0 else 0.0
        avg_usd = sum(m['amount_usd'] for m in moves) / len(moves) if moves else 0.0

        return {
            'entity': entity_name,
            'total_moves': len(moves),
            'moves': moves[:20],  # Cap at 20 most recent
            'accuracy': round(accuracy, 1),
            'correct_calls': correct_calls,
            'total_evaluated': total_evaluated,
            'avg_move_usd': avg_usd,
        }
    finally:
        if close_conn:
            conn.close()


def _get_price_at_time(conn: sqlite3.Connection, coin: str, timestamp: int) -> Optional[float]:
    """Get the closest price to a given timestamp."""
    # Look within a 12-hour window
    window = 12 * 3600
    row = conn.execute(
        "SELECT close FROM prices WHERE coin = ? "
        "AND timestamp BETWEEN ? AND ? "
        "ORDER BY ABS(timestamp - ?) LIMIT 1",
        (coin, timestamp - window, timestamp + window, timestamp)
    ).fetchone()

    if row:
        return row[0]

    # Fallback: try market_overview
    row = conn.execute(
        "SELECT price_usd FROM market_overview WHERE coin = ? "
        "ORDER BY ABS(timestamp - ?) LIMIT 1",
        (coin, timestamp)
    ).fetchone()

    return row[0] if row else None


def format_whale_context(conn: sqlite3.Connection = None, limit: int = 15) -> str:
    """Build enriched whale transaction context with entity identification.

    This replaces the basic get_whale_context() — adds WHO the whale is,
    their historical accuracy, and narrative context for Claude.

    Args:
        conn: SQLite connection (will open/close if None).
        limit: Max transactions to include.

    Returns:
        Formatted string for injection into Claude prompt.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        close_conn = True

    try:
        day_ago = int((datetime.now(timezone.utc) - timedelta(hours=48)).timestamp())

        rows = conn.execute(
            "SELECT coin, amount, amount_usd, from_label, to_label, blockchain, "
            "datetime(timestamp, 'unixepoch') as dt "
            "FROM whale_transactions WHERE timestamp > ? AND amount_usd > 0 "
            "ORDER BY amount_usd DESC LIMIT ?",
            (day_ago, limit)
        ).fetchall()

        if not rows:
            return "WHALE TRANSACTIONS (48h): No whale data collected yet."

        # Track entities we've already looked up (avoid repeated DB queries)
        entity_cache = {}
        to_exch_usd = 0.0
        from_exch_usd = 0.0

        lines = [f"WHALE TRANSACTIONS — IDENTIFIED (last 48h, top {len(rows)}):"]

        for r in rows:
            coin = r[0] or '?'
            amount = r[1] or 0
            usd = r[2] or 0
            from_label = r[3] or 'unknown'
            to_label = r[4] or 'unknown'
            chain = r[5] or '?'

            # Classify transaction type
            from src.crypto.script_generator import _classify_whale_tx
            tx_type = _classify_whale_tx(from_label, to_label)

            if tx_type == 'to_exchange':
                to_exch_usd += usd
                signal = "SELL PRESSURE"
            elif tx_type == 'from_exchange':
                from_exch_usd += usd
                signal = "ACCUMULATION"
            elif tx_type in ('exchange_move', 'internal'):
                signal = "NOISE (inter-exchange)"
            else:
                signal = "WHALE MOVE"

            # Format USD
            if usd >= 1e9:
                usd_str = f"${usd/1e9:.1f}B"
            elif usd >= 1e6:
                usd_str = f"${usd/1e6:.1f}M"
            else:
                usd_str = f"${usd:,.0f}"

            # Identify the whale (check both from and to labels)
            entity_from = _lookup_entity(from_label, entity_cache)
            entity_to = _lookup_entity(to_label, entity_cache)

            # Build identification string
            id_parts = []
            if entity_from:
                name = entity_from['name']
                history = _get_cached_history(name, conn, entity_cache)
                if history and history['total_evaluated'] > 2:
                    id_parts.append(
                        f"FROM: {name} ({entity_from['type']}, "
                        f"{history['correct_calls']}/{history['total_evaluated']} correct calls, "
                        f"bias={entity_from['bias']})"
                    )
                else:
                    id_parts.append(
                        f"FROM: {name} ({entity_from['type']}, bias={entity_from['bias']})"
                    )
            if entity_to:
                name = entity_to['name']
                history = _get_cached_history(name, conn, entity_cache)
                if history and history['total_evaluated'] > 2:
                    id_parts.append(
                        f"TO: {name} ({entity_to['type']}, "
                        f"{history['correct_calls']}/{history['total_evaluated']} correct calls)"
                    )
                else:
                    id_parts.append(f"TO: {name} ({entity_to['type']})")

            id_str = ' | '.join(id_parts) if id_parts else ''

            line = (f"  - [{signal}] {coin}: {amount:,.0f} ({usd_str}) "
                    f"{from_label} -> {to_label} [{chain}]")
            if id_str:
                line += f"\n    ^^ IDENTIFIED: {id_str}"

            lines.append(line)

        # Summary
        net_flow = from_exch_usd - to_exch_usd
        flow_dir = "NET ACCUMULATION" if net_flow > 0 else "NET SELLING PRESSURE"
        lines.append(
            f"\n  WHALE FLOW SUMMARY: To exchanges: ${to_exch_usd/1e6:.1f}M | "
            f"From exchanges: ${from_exch_usd/1e6:.1f}M | {flow_dir} (net ${abs(net_flow)/1e6:.1f}M)"
        )

        # Add notable entity summary
        notable = _get_notable_entities(rows, entity_cache)
        if notable:
            lines.append(f"\n  NOTABLE ENTITIES ACTIVE:")
            for note in notable:
                lines.append(f"    - {note}")

        return '\n'.join(lines)
    finally:
        if close_conn:
            conn.close()


def _lookup_entity(label: str, cache: dict) -> Optional[dict]:
    """Lookup entity with caching."""
    if not label or label == 'unknown':
        return None
    key = label.lower().strip()
    if key not in cache:
        cache[key] = identify_whale(label)
    result = cache[key]
    # Return a copy or None, not the cache sentinel
    return result.copy() if isinstance(result, dict) else result


def _get_cached_history(entity_name: str, conn: sqlite3.Connection,
                        cache: dict) -> Optional[dict]:
    """Get whale history with caching to avoid repeated DB queries."""
    cache_key = f'_history_{entity_name}'
    if cache_key not in cache:
        try:
            cache[cache_key] = get_whale_history(entity_name, conn, days=90)
        except Exception:
            cache[cache_key] = None
    return cache[cache_key]


def _get_notable_entities(rows: list, entity_cache: dict) -> list:
    """Summarize which notable entities were active in the last 48h."""
    seen = {}
    for r in rows:
        usd = r[2] or 0
        from_label = r[3] or ''
        to_label = r[4] or ''

        for label in [from_label, to_label]:
            entity = _lookup_entity(label, entity_cache)
            if entity and entity['type'] not in ('exchange', 'exchange_reserve'):
                name = entity['name']
                if name not in seen:
                    seen[name] = {'total_usd': 0, 'count': 0, 'entity': entity}
                seen[name]['total_usd'] += usd
                seen[name]['count'] += 1

    notes = []
    for name, info in sorted(seen.items(), key=lambda x: x[1]['total_usd'], reverse=True):
        e = info['entity']
        total = info['total_usd']
        if total >= 1e9:
            total_str = f"${total/1e9:.1f}B"
        elif total >= 1e6:
            total_str = f"${total/1e6:.1f}M"
        else:
            total_str = f"${total:,.0f}"
        notes.append(f"{name}: {info['count']} moves totaling {total_str} — {e['description']}")

    return notes[:5]  # Top 5 most active


# ════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        label = ' '.join(sys.argv[1:])
        result = identify_whale(label)
        if result:
            print(f"Identified: {result['name']} ({result['type']})")
            print(f"  Bias: {result['bias']}")
            print(f"  Info: {result['description']}")
        else:
            print(f"Unknown entity: {label}")
    else:
        conn = sqlite3.connect(str(DB_PATH), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        print(format_whale_context(conn))
        conn.close()
