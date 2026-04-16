"""
CryptoQuant Historical Backfill
================================
One-time script to backfill CryptoQuant data to cover the full training period.

Current coverage: 93 days (2025-11-23 to 2026-02-23, limit=90)
Training period:  284 dates (2025-04-01 to 2026-02-15)
Target:           ~340 days to fully cover training + buffer

This script monkey-patches the collector's limit from 90 to 340.
Uses INSERT OR REPLACE so existing data won't be duplicated.

Usage:
    python src/crypto/backfill_cryptoquant.py           # run backfill
    python src/crypto/backfill_cryptoquant.py --dry-run  # show what would happen
"""

import sys
import sqlite3
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('backfill_cq')

MARKET_DB = Path('data/crypto/market.db')
BACKFILL_LIMIT = 340  # ~340 days covers 2025-04-01 to 2026-02-23


def get_current_coverage(conn):
    """Show current data coverage per table."""
    tables = [
        'cq_btc_onchain', 'cq_exchange_flows', 'cq_coinbase_premium',
        'cq_miner_data', 'cq_active_addresses', 'cq_stablecoin_flows'
    ]
    coverage = {}
    for t in tables:
        try:
            row = conn.execute(f'SELECT MIN(date), MAX(date), COUNT(*) FROM {t}').fetchone()
            coverage[t] = {'min': row[0], 'max': row[1], 'count': row[2]}
            log.info(f"  {t}: {row[0]} to {row[1]} ({row[2]} rows)")
        except Exception as e:
            coverage[t] = {'min': None, 'max': None, 'count': 0}
            log.info(f"  {t}: no data ({e})")
    return coverage


def run_backfill(dry_run=False):
    """Run CryptoQuant backfill with extended limit."""
    from src.crypto.cryptoquant_collector import (
        CryptoQuantCollector, CRYPTOQUANT_API_KEY, collect_all_cryptoquant
    )

    if not CRYPTOQUANT_API_KEY:
        log.error("CRYPTOQUANT_API_KEY not set in .env")
        return

    conn = sqlite3.connect(str(MARKET_DB), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    log.info("=" * 60)
    log.info("CRYPTOQUANT BACKFILL")
    log.info(f"  Target limit: {BACKFILL_LIMIT} days")
    log.info("=" * 60)

    # Show current coverage
    log.info("\nCurrent coverage:")
    before = get_current_coverage(conn)

    if dry_run:
        log.info("\n[DRY RUN] Would backfill all CryptoQuant tables to ~340 days")
        log.info(f"  Estimated API calls: ~22 endpoints x 1 request = ~22 calls")
        log.info(f"  Estimated time: ~22 x 3.5s = ~77 seconds")
        conn.close()
        return

    # Monkey-patch the collector's _get to use higher limit
    cq = CryptoQuantCollector()
    original_get = cq._get

    def patched_get(path, params=None):
        if params and 'limit' in params:
            params = dict(params)  # copy
            params['limit'] = BACKFILL_LIMIT
        return original_get(path, params)

    cq._get = patched_get

    # Run all collectors with patched limit
    total = 0
    collectors = [
        ('btc_onchain', cq.collect_btc_onchain),
        ('exchange_flows', cq.collect_exchange_flows),
        ('erc20_flows', cq.collect_erc20_flows),
        ('coinbase_premium', cq.collect_coinbase_premium),
        ('miner_data', cq.collect_miner_data),
        ('active_addresses', cq.collect_active_addresses),
        ('stablecoin_flows', cq.collect_stablecoin_flows),
    ]

    for name, fn in collectors:
        try:
            log.info(f"\n--- {name} (limit={BACKFILL_LIMIT}) ---")
            result = fn(conn)
            total += result
        except Exception as e:
            log.error(f"  {name} failed: {e}")

    # Show new coverage
    log.info("\n" + "=" * 60)
    log.info("AFTER BACKFILL:")
    after = get_current_coverage(conn)

    # Summary
    log.info("\nSUMMARY:")
    for t in before:
        b_count = before[t]['count']
        a_count = after.get(t, {}).get('count', 0)
        a_min = after.get(t, {}).get('min', '?')
        log.info(f"  {t}: {b_count} -> {a_count} rows (from {a_min})")

    log.info(f"\nTotal rows inserted/updated: {total}")
    conn.close()


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    run_backfill(dry_run=dry_run)
