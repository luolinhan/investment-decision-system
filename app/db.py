"""
Shared SQLite connection helper.

Opens connections with a generous timeout and applies PRAGMAs for:
  - WAL journaling
  - synchronous = NORMAL
  - busy_timeout (5 seconds)
  - foreign_keys = ON

Usage:
    from app.db import get_sqlite_connection

    # Simple path-based connect
    conn = get_sqlite_connection("data/investment.db")

    # Context manager (recommended)
    with get_sqlite_connection("data/investment.db") as conn:
        conn.execute("SELECT 1")

    # Path-like object
    from pathlib import Path
    conn = get_sqlite_connection(Path("data/investment.db"))
"""
import sqlite3
from pathlib import Path
from typing import Union

DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_BUSY_TIMEOUT = 5000  # milliseconds


def get_sqlite_connection(
    db_path: Union[str, Path],
    timeout: int = DEFAULT_TIMEOUT,
    busy_timeout: int = DEFAULT_BUSY_TIMEOUT,
) -> sqlite3.Connection:
    """
    Open a SQLite connection with safe defaults for concurrent access.
    """
    path = str(db_path)
    conn = sqlite3.connect(path, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={busy_timeout}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
