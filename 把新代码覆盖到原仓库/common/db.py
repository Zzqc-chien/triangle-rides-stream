"""Small Postgres helper used by every consumer (works for local Postgres and Supabase)."""

import time
from pathlib import Path

import psycopg

from common.config import DATABASE_URL

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def connect(retries: int = 30) -> psycopg.Connection:
    """Open a connection, waiting for the database to come up (docker compose starts it in parallel).

    prepare_threshold=None turns off server-side prepared statements, which the Supabase
    transaction pooler (port 6543) does not support. It costs nothing on plain Postgres.
    """
    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(DATABASE_URL, autocommit=False, prepare_threshold=None)
        except psycopg.OperationalError as exc:
            if attempt == retries:
                raise
            print(f"database not ready ({exc.__class__.__name__}), retrying in 2s...")
            time.sleep(2)
    raise RuntimeError("unreachable")


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create every table if it does not exist yet. Safe when several consumers start at once."""
    with conn.cursor() as cur:
        # Transaction-level advisory lock: held until COMMIT, so a second consumer starting at the
        # same moment waits until the first one's tables are committed and visible to it.
        cur.execute("SELECT pg_advisory_xact_lock(706706)")
        cur.execute(SCHEMA_FILE.read_text())
    conn.commit()
