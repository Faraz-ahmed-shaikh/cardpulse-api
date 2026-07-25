# database.py
import os
import psycopg2
from fastapi import HTTPException


CONN_STR = os.environ.get("NEON_CONN_STR")


def get_conn():
    """Open and return a NeonDB connection."""
    if not CONN_STR:
        raise HTTPException(
            status_code=500,
            detail="Database connection not configured. Set NEON_CONN_STR env variable."
        )
    return psycopg2.connect(CONN_STR)


def rows_to_dicts(cur) -> list[dict]:
    """
    Convert psycopg2 cursor result to a list of dicts.
    Also converts timestamps to strings so JSON can serialize them.
    """
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    for row in rows:
        for col in ["transaction_timestamp", "updated_at",
                    "joining_date", "expiry_date", "snapshot_date"]:
            if row.get(col) is not None:
                row[col] = str(row[col])

    return rows