#!/usr/bin/env python3
"""
Backfill stablecoin total supply history from DeFi Llama.

Source: https://stablecoins.llama.fi/stablecoincharts/all
- Free, no API key required
- Daily data from 2017-11-29 to present
- Returns totalCirculatingUSD.peggedUSD (market-cap-weighted supply)

Target: cg_stablecoin_supply(date TEXT PK, total_market_cap REAL)
"""

import sqlite3
import requests
import datetime
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'crypto', 'market.db')
DB_PATH = os.path.normpath(DB_PATH)

DEFILLAMA_URL = 'https://stablecoins.llama.fi/stablecoincharts/all'


def fetch_stablecoin_history() -> list[dict]:
    """Fetch all historical stablecoin supply data from DeFi Llama."""
    print(f"Fetching stablecoin supply history from DeFi Llama...")
    resp = requests.get(DEFILLAMA_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    print(f"  Got {len(data)} daily data points")
    return data


def backfill(db_path: str = DB_PATH):
    """Main backfill function."""
    data = fetch_stablecoin_history()
    if not data:
        print("ERROR: No data returned from DeFi Llama")
        return

    conn = sqlite3.connect(db_path)

    # Count before
    before = conn.execute("SELECT COUNT(*) FROM cg_stablecoin_supply").fetchone()[0]
    min_date_before = conn.execute("SELECT MIN(date) FROM cg_stablecoin_supply").fetchone()[0]
    max_date_before = conn.execute("SELECT MAX(date) FROM cg_stablecoin_supply").fetchone()[0]
    print(f"\nBefore: {before} rows, range {min_date_before} to {max_date_before}")

    # Insert data
    inserted = 0
    skipped = 0
    for entry in data:
        ts = int(entry['date'])
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        date_str = dt.strftime('%Y-%m-%d')

        # Use totalCirculatingUSD (market-cap weighted) as total_market_cap
        # This matches what CoinGecko reports
        total_usd = entry.get('totalCirculatingUSD', {}).get('peggedUSD', 0)
        if not total_usd:
            # Fallback to totalCirculating
            total_usd = entry.get('totalCirculating', {}).get('peggedUSD', 0)

        if not total_usd or total_usd <= 0:
            skipped += 1
            continue

        try:
            conn.execute(
                "INSERT OR IGNORE INTO cg_stablecoin_supply (date, total_market_cap) VALUES (?, ?)",
                (date_str, float(total_usd))
            )
            if conn.total_changes:
                inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()

    # Count after
    after = conn.execute("SELECT COUNT(*) FROM cg_stablecoin_supply").fetchone()[0]
    min_date_after = conn.execute("SELECT MIN(date) FROM cg_stablecoin_supply").fetchone()[0]
    max_date_after = conn.execute("SELECT MAX(date) FROM cg_stablecoin_supply").fetchone()[0]

    new_rows = after - before
    print(f"\nAfter:  {after} rows, range {min_date_after} to {max_date_after}")
    print(f"New rows inserted: {new_rows}")
    print(f"Skipped (already existed or zero): {skipped}")

    # Show sample of old and new data
    print("\n--- Earliest 5 rows ---")
    rows = conn.execute("SELECT * FROM cg_stablecoin_supply ORDER BY date ASC LIMIT 5").fetchall()
    for r in rows:
        print(f"  {r[0]}: ${r[1]:,.0f}")

    print("\n--- Latest 5 rows ---")
    rows = conn.execute("SELECT * FROM cg_stablecoin_supply ORDER BY date DESC LIMIT 5").fetchall()
    for r in rows:
        print(f"  {r[0]}: ${r[1]:,.0f}")

    # Show yearly breakdown
    print("\n--- Yearly row counts ---")
    yearly = conn.execute(
        "SELECT substr(date, 1, 4) as year, COUNT(*), "
        "MIN(total_market_cap), MAX(total_market_cap) "
        "FROM cg_stablecoin_supply GROUP BY year ORDER BY year"
    ).fetchall()
    for y in yearly:
        print(f"  {y[0]}: {y[1]} rows, ${y[2]:,.0f} - ${y[3]:,.0f}")

    conn.close()
    print(f"\nDone! Total rows: {before} -> {after} (+{new_rows})")


if __name__ == '__main__':
    backfill()
