"""
FORTIX — Centralized Database Connection Helper
=================================================
Ensures every sqlite3 connection to market.db (and other DBs) uses:
  - WAL journal mode  (allows concurrent readers + one writer)
  - busy_timeout=60000 (wait up to 60s instead of failing immediately)
  - timeout=60         (Python-level lock wait)

Usage:
    from src.crypto.db_helper import get_connection

    conn = get_connection()                      # market.db (default)
    conn = get_connection('data/crypto/uploads.db')  # other DB
    conn = get_connection(DB_PATH)               # pathlib.Path works too

All existing `sqlite3.connect(...)` calls should be migrated to this.
"""

import sqlite3
import logging
from pathlib import Path

log = logging.getLogger('db_helper')

MARKET_DB = Path('data/crypto/market.db')


def get_connection(db_path=None, timeout: int = 60, readonly: bool = False) -> sqlite3.Connection:
    """Get a properly configured SQLite connection.

    Args:
        db_path: Path to the database file. Defaults to market.db.
        timeout: Python-level lock timeout in seconds (default 60).
        readonly: If True, opens in read-only mode (sqlite3 URI).

    Returns:
        sqlite3.Connection with WAL mode and busy_timeout configured.
    """
    if db_path is None:
        db_path = MARKET_DB

    db_path = str(db_path)

    if readonly and db_path != ':memory:':
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout)
    else:
        conn = sqlite3.connect(db_path, timeout=timeout)

    # WAL mode: allows concurrent readers while one writer is active.
    # This is the single most important fix for "database is locked" errors.
    if db_path != ':memory:':
        conn.execute("PRAGMA journal_mode=WAL")

    # busy_timeout: SQLite-level retry for up to 60 seconds when the DB is locked.
    # This is separate from Python's timeout parameter.
    conn.execute("PRAGMA busy_timeout=60000")

    return conn
