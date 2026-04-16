#!/usr/bin/env python3
"""
Backfill whale transactions from Whale Alert API (Developer/free tier).

Limitations (Developer API):
- Max 30 days (2,592,000 seconds) historical lookback
- 10 requests/minute rate limit
- 100 transactions per request (use cursor for pagination)

Strategy:
- Walk backwards from now in 1-hour windows
- Skip windows where DB already has data (smart resume)
- Use cursor pagination within each window
- min_value=$1,000,000 to get significant whale moves
- INSERT OR IGNORE to skip duplicates
- Sleep between requests to respect rate limit (7s = safe for 10/min)
"""

import sqlite3
import requests
import datetime
import time
import sys
import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'data', 'crypto', 'market.db'))
WHALE_ALERT_KEY = os.getenv('WHALE_ALERT_API_KEY', '')
BASE_URL = 'https://api.whale-alert.io/v1/transactions'

# Free tier: max 30 days back
MAX_HISTORY_SECONDS = 2_592_000  # 30 days
WINDOW_SECONDS = 3600  # 1 hour per request window
MIN_VALUE_USD = 1_000_000
REQUEST_LIMIT = 100
RATE_LIMIT_SLEEP = 7  # seconds between requests (safe for 10/min limit)


def fetch_window(api_key: str, start_ts: int, end_ts: int) -> list[dict]:
    """Fetch all whale transactions in a time window, handling pagination."""
    all_txs = []
    cursor = None

    while True:
        params = {
            'api_key': api_key,
            'min_value': MIN_VALUE_USD,
            'start': start_ts,
            'end': end_ts,
            'limit': REQUEST_LIMIT,
        }
        if cursor:
            params['cursor'] = cursor

        try:
            resp = requests.get(BASE_URL, params=params, timeout=20)
        except requests.RequestException as e:
            print(f"\n    Request error: {e}")
            break

        if resp.status_code == 429:
            print("\n    Rate limited! Sleeping 60s...")
            time.sleep(60)
            continue

        if resp.status_code != 200:
            try:
                data = resp.json()
                msg = data.get('message', resp.text[:200])
            except Exception:
                msg = resp.text[:200]
            print(f"\n    API error {resp.status_code}: {msg}")
            break

        data = resp.json()
        if data.get('result') != 'success':
            print(f"\n    API result: {data.get('result')}: {data.get('message', '')}")
            break

        txs = data.get('transactions', [])
        all_txs.extend(txs)

        count = data.get('count', 0)
        if count == 0 or len(txs) == 0:
            break

        # If we got exactly the limit, there might be more (paginate)
        new_cursor = data.get('cursor')
        if new_cursor and new_cursor != cursor and len(txs) >= REQUEST_LIMIT:
            cursor = new_cursor
            time.sleep(RATE_LIMIT_SLEEP)
        else:
            break

    return all_txs


def store_transactions(conn: sqlite3.Connection, txs: list[dict]) -> int:
    """Store whale transactions, returning count of new rows."""
    before = conn.execute("SELECT COUNT(*) FROM whale_transactions").fetchone()[0]
    for tx in txs:
        tx_hash = tx.get('hash', f"wa_{tx.get('id', '')}")
        conn.execute(
            "INSERT OR IGNORE INTO whale_transactions "
            "(tx_hash, timestamp, blockchain, from_addr, from_label, "
            "to_addr, to_label, amount, amount_usd, coin) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tx_hash,
                tx.get('timestamp', 0),
                tx.get('blockchain', ''),
                tx.get('from', {}).get('address', ''),
                tx.get('from', {}).get('owner', ''),
                tx.get('to', {}).get('address', ''),
                tx.get('to', {}).get('owner', ''),
                tx.get('amount', 0),
                tx.get('amount_usd', 0),
                tx.get('symbol', '').upper(),
            )
        )
    after = conn.execute("SELECT COUNT(*) FROM whale_transactions").fetchone()[0]
    return after - before


def has_data_in_window(conn: sqlite3.Connection, start_ts: int, end_ts: int, min_count: int = 5) -> bool:
    """Check if we already have sufficient data in this time window."""
    count = conn.execute(
        "SELECT COUNT(*) FROM whale_transactions WHERE timestamp >= ? AND timestamp < ?",
        (start_ts, end_ts)
    ).fetchone()[0]
    return count >= min_count


def backfill(db_path: str = DB_PATH, days_back: int = 30):
    """Main backfill function. Walks backwards in 1-hour windows, skipping covered periods."""
    if not WHALE_ALERT_KEY:
        print("ERROR: WHALE_ALERT_API_KEY not set in .env")
        sys.exit(1)

    # Clamp to free tier max
    if days_back > 30:
        print(f"WARNING: Free tier max is 30 days, clamping from {days_back}")
        days_back = 30

    now = datetime.datetime.now(datetime.timezone.utc)
    end_ts = int(now.timestamp())
    start_limit = end_ts - (days_back * 86400)

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Stats before
    before = conn.execute("SELECT COUNT(*) FROM whale_transactions").fetchone()[0]
    existing_min = conn.execute("SELECT MIN(timestamp) FROM whale_transactions").fetchone()[0]
    existing_max = conn.execute("SELECT MAX(timestamp) FROM whale_transactions").fetchone()[0]

    print(f"Whale Alert backfill: {days_back} days, min_value=${MIN_VALUE_USD/1e6:.0f}M")
    print(f"Before: {before} rows", end="")
    if existing_min:
        dt_min = datetime.datetime.fromtimestamp(existing_min, tz=datetime.timezone.utc)
        dt_max = datetime.datetime.fromtimestamp(existing_max, tz=datetime.timezone.utc)
        print(f", range {dt_min.strftime('%Y-%m-%d')} to {dt_max.strftime('%Y-%m-%d')}")
    else:
        print()

    # Find gaps: check which hourly windows have no data
    total_windows = (end_ts - start_limit) // WINDOW_SECONDS
    gaps = []
    covered = 0
    for i in range(total_windows):
        w_start = start_limit + i * WINDOW_SECONDS
        w_end = w_start + WINDOW_SECONDS
        if has_data_in_window(conn, w_start, w_end, min_count=5):
            covered += 1
        else:
            gaps.append((w_start, w_end))

    print(f"Windows: {total_windows} total, {covered} already covered, {len(gaps)} gaps to fill")

    if not gaps:
        print("No gaps to fill!")
        conn.close()
        return

    total_new = 0
    total_fetched = 0
    api_calls = 0

    for idx, (w_start, w_end) in enumerate(gaps):
        dt_start = datetime.datetime.fromtimestamp(w_start, tz=datetime.timezone.utc)
        dt_end = datetime.datetime.fromtimestamp(w_end, tz=datetime.timezone.utc)

        progress = (idx + 1) / len(gaps) * 100
        sys.stdout.write(
            f"\r  [{progress:5.1f}%] {dt_start.strftime('%m-%d %H:%M')} -> {dt_end.strftime('%m-%d %H:%M')} "
            f"| API calls: {api_calls} | fetched: {total_fetched} | new: {total_new}   "
        )
        sys.stdout.flush()

        txs = fetch_window(WHALE_ALERT_KEY, w_start, w_end)
        api_calls += 1
        total_fetched += len(txs)

        if txs:
            for attempt in range(5):
                try:
                    new = store_transactions(conn, txs)
                    total_new += new
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if 'locked' in str(e) and attempt < 4:
                        wait = 15 * (attempt + 1)
                        print(f"\n    DB locked, retry {attempt+1}/5 in {wait}s...")
                        time.sleep(wait)
                        # Reconnect on repeated locks
                        if attempt >= 2:
                            try:
                                conn.close()
                            except Exception:
                                pass
                            conn = sqlite3.connect(db_path, timeout=60)
                            conn.execute("PRAGMA journal_mode=WAL")
                            conn.execute("PRAGMA busy_timeout=60000")
                            print("    Reconnected to DB")
                    else:
                        print(f"\n    DB locked after 5 attempts, skipping window...")
                        break

        time.sleep(RATE_LIMIT_SLEEP)

    print()  # newline after progress

    # Stats after
    after = conn.execute("SELECT COUNT(*) FROM whale_transactions").fetchone()[0]
    new_min = conn.execute("SELECT MIN(timestamp) FROM whale_transactions").fetchone()[0]
    new_max = conn.execute("SELECT MAX(timestamp) FROM whale_transactions").fetchone()[0]

    dt_min = datetime.datetime.fromtimestamp(new_min, tz=datetime.timezone.utc)
    dt_max = datetime.datetime.fromtimestamp(new_max, tz=datetime.timezone.utc)

    print(f"\nAfter:  {after} rows, range {dt_min.strftime('%Y-%m-%d')} to {dt_max.strftime('%Y-%m-%d')}")
    print(f"API calls: {api_calls}, transactions fetched: {total_fetched}")
    print(f"New rows inserted: {after - before}")

    # Distribution by day
    print("\n--- Daily transaction counts ---")
    daily = conn.execute(
        "SELECT date(timestamp, 'unixepoch') as d, COUNT(*), "
        "COALESCE(SUM(amount_usd), 0) as total_usd "
        "FROM whale_transactions "
        "GROUP BY d ORDER BY d"
    ).fetchall()
    for row in daily:
        usd = row[2] or 0
        bar = '#' * min(int(row[1] / 100), 40)
        print(f"  {row[0]}: {row[1]:>5} txs, ${usd/1e9:>6.2f}B {bar}")

    # Top coins
    print("\n--- Top coins by transaction count ---")
    coins = conn.execute(
        "SELECT coin, COUNT(*), COALESCE(SUM(amount_usd), 0) "
        "FROM whale_transactions "
        "GROUP BY coin ORDER BY COUNT(*) DESC LIMIT 10"
    ).fetchall()
    for c in coins:
        usd = c[2] or 0
        print(f"  {c[0]:>6}: {c[1]:>6} txs, ${usd/1e9:.2f}B total")

    conn.close()
    print(f"\nDone! Total rows: {before} -> {after} (+{after - before})")


if __name__ == '__main__':
    # Default: backfill full 30 days (free tier max)
    days = 30
    if len(sys.argv) > 1:
        days = int(sys.argv[1])
    backfill(days_back=days)
