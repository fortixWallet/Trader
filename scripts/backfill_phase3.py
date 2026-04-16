"""Phase 3: Backfill all new data sources for model retraining."""
import os, sys, json, time, requests, sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
load_dotenv()

DB = 'data/crypto/market.db'
conn = sqlite3.connect(DB, timeout=60)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA busy_timeout=30000')

fred_key = os.getenv('FRED_API_KEY')

print("=" * 60)
print("PHASE 3: DATA BACKFILL FOR MODEL RETRAINING")
print("=" * 60)

# ═══ 1. S&P500, NASDAQ, DXY from FRED ═══
print("\n1. MACRO DATA (FRED)...")
conn.execute("""CREATE TABLE IF NOT EXISTS macro_events (
    date TEXT, event_type TEXT, value REAL,
    PRIMARY KEY (date, event_type))""")

new_series = {
    'SP500': 'sp500',
    'NASDAQCOM': 'nasdaq',
    'DTWEXBGS': 'dxy',
}

for series_id, event_type in new_series.items():
    existing = conn.execute('SELECT COUNT(*) FROM macro_events WHERE event_type=?', (event_type,)).fetchone()[0]
    if existing > 500:
        print(f"  {event_type}: {existing} entries (already done)")
        continue

    print(f"  Fetching {series_id} ({event_type})...")
    r = requests.get('https://api.stlouisfed.org/fred/series/observations',
                    params={'series_id': series_id, 'api_key': fred_key,
                            'file_type': 'json', 'sort_order': 'asc',
                            'observation_start': '2019-01-01'},
                    timeout=30)
    if r.status_code == 200:
        count = 0
        for obs in r.json().get('observations', []):
            if obs['value'] != '.':
                conn.execute(
                    'INSERT OR REPLACE INTO macro_events (date, event_type, value) VALUES (?,?,?)',
                    (obs['date'], event_type, float(obs['value']))
                )
                count += 1
        conn.commit()
        print(f"    {count} entries")
    else:
        print(f"    FRED error: {r.status_code}")
    time.sleep(1)

# ═══ 2. Collect recent hacks from DeFi Llama ═══
print("\n2. HACKS (DeFi Llama)...")
conn.executescript("""
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

try:
    resp = requests.get('https://api.llama.fi/hacks', timeout=15)
    if resp.status_code == 200:
        hacks = resp.json()
        count = 0
        for hack in hacks:
            date = hack.get('date', '')
            if isinstance(date, (int, float)):
                date = datetime.fromtimestamp(date, tz=timezone.utc).strftime('%Y-%m-%d')
            elif isinstance(date, str) and len(date) > 10:
                date = date[:10]

            amount = hack.get('amount', 0) or 0
            if amount < 1_000_000:
                continue

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO crypto_hacks "
                    "(date, protocol, amount_usd, chain, description, coins_affected, collected_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (date, hack.get('name', 'Unknown'), amount,
                     str(hack.get('chain', ['Unknown'])),
                     hack.get('technique', ''),
                     json.dumps([]),
                     datetime.now(timezone.utc).isoformat())
                )
                count += 1
            except Exception:
                pass
        conn.commit()
        print(f"  {count} hacks stored")
    else:
        print(f"  DeFi Llama hacks: {resp.status_code}")
except Exception as e:
    print(f"  Hacks failed: {e}")

# ═══ 3. Political events table ═══
print("\n3. POLITICAL EVENTS TABLE...")
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
print("  Table ready")

# ═══ 4. Token unlocks table ═══
print("\n4. TOKEN UNLOCKS TABLE...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS token_unlocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin TEXT NOT NULL,
        unlock_date TEXT NOT NULL,
        amount_usd REAL,
        percentage_supply REAL,
        description TEXT,
        collected_at TEXT,
        UNIQUE(coin, unlock_date)
    )
""")
conn.commit()
print("  Table ready")

# ═══ 5. Verify all data ═══
print("\n" + "=" * 60)
print("DATA VERIFICATION")
print("=" * 60)

tables_to_check = [
    ('prices (1d)', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM prices WHERE timeframe='1d'"),
    ('funding_rates', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM funding_rates"),
    ('long_short_ratio', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM long_short_ratio"),
    ('taker_volume', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM taker_volume"),
    ('fear_greed', "SELECT 1, COUNT(*) FROM fear_greed"),
    ('whale_transactions', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM whale_transactions"),
    ('macro (sp500)', "SELECT 1, COUNT(*) FROM macro_events WHERE event_type='sp500'"),
    ('macro (nasdaq)', "SELECT 1, COUNT(*) FROM macro_events WHERE event_type='nasdaq'"),
    ('macro (dxy)', "SELECT 1, COUNT(*) FROM macro_events WHERE event_type='dxy'"),
    ('macro (vix)', "SELECT 1, COUNT(*) FROM macro_events WHERE event_type='vix'"),
    ('macro (yield_curve)', "SELECT 1, COUNT(*) FROM macro_events WHERE event_type='yield_curve'"),
    ('cg_etf_flows', "SELECT COUNT(DISTINCT asset), COUNT(*) FROM cg_etf_flows"),
    ('cq_exchange_flows', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM cq_exchange_flows"),
    ('cq_btc_onchain', "SELECT 1, COUNT(*) FROM cq_btc_onchain"),
    ('cq_active_addresses', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM cq_active_addresses"),
    ('crypto_hacks', "SELECT 1, COUNT(*) FROM crypto_hacks"),
    ('political_events', "SELECT 1, COUNT(*) FROM political_events"),
    ('social_sentiment', "SELECT COUNT(DISTINCT coin), COUNT(*) FROM social_sentiment"),
    ('news', "SELECT 1, COUNT(*) FROM news"),
    ('google_trends', "SELECT COUNT(DISTINCT keyword), COUNT(*) FROM google_trends"),
    ('defi_tvl_history', "SELECT 1, COUNT(*) FROM defi_tvl_history"),
]

for name, query in tables_to_check:
    try:
        row = conn.execute(query).fetchone()
        print(f"  {name:25}: {row[0]:>3} coins, {row[1]:>8} entries")
    except Exception as e:
        print(f"  {name:25}: ERROR ({str(e)[:40]})")

conn.close()
print("\nDone!")
