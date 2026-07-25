from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import psycopg2
import os

app = FastAPI(
    title="CardPulse Transactions API",
    description="Mock payment processor feed for CardPulse ELT pipeline",
    version="4.0.0"
)

CONN_STR = os.environ.get("NEON_CONN_STR")

def get_conn():
    if not CONN_STR:
        raise HTTPException(status_code=500, detail="Database connection not configured")
    return psycopg2.connect(CONN_STR)

def rows_to_dicts(cur):
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    for row in rows:
        if row.get("transaction_timestamp"):
            row["transaction_timestamp"] = str(row["transaction_timestamp"])
        if row.get("updated_at"):
            row["updated_at"] = str(row["updated_at"])
    return rows


@app.get("/")
def root():
    return {
        "project": "CardPulse Transactions API",
        "version": "4.0.0",
        "endpoints": {
            "health":      "GET /health",
            "incremental": "GET /transactions?updated_after=2024-10-01T00:00:00Z",
            "paginate":    "GET /transactions?updated_after=X&limit=5000&offset=0",
            "filter":      "GET /transactions?updated_after=X&status=Failed",
            "batch":       "GET /transactions/batch/{n}?size=1000",
        }
    }


@app.get("/health")
def health():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions")
    count = cur.fetchone()[0]
    conn.close()
    return {"status": "ok", "total_transactions": count}


@app.get("/transactions")
def get_transactions(
    updated_after: Optional[str] = Query(
        None,
        description="ISO 8601 timestamp — returns rows where updated_at > this value. "
                    "Example: 2024-10-01T00:00:00Z. "
                    "Omit for full load."
    ),
    status: Optional[str] = Query(
        None,
        description="Filter by status: Successful | Failed | Pending"
    ),
    limit:  int = Query(1000, ge=1, le=10000, description="Page size (max 10000)"),
    offset: int = Query(0,    ge=0,           description="Number of rows to skip"),
):
    """
    Primary ingestion endpoint.

    For incremental loading pass updated_after = last pipeline run timestamp.
    Pipeline stores this watermark in a metadata table and passes it on
    every run to fetch only new or updated rows.

    For full load omit updated_after entirely.
    """
    conn = get_conn()
    try:
        filters = []
        params  = {"limit": limit, "offset": offset}

        if updated_after:
            filters.append("updated_at > %(updated_after)s")
            params["updated_after"] = updated_after

        if status:
            filters.append("status = %(status)s")
            params["status"] = status

        where = f"WHERE {' AND '.join(filters)}" if filters else ""

        query = f"""
            SELECT *
            FROM transactions
            {where}
            ORDER BY updated_at ASC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """

        cur = conn.cursor()
        cur.execute(query, params)
        rows = rows_to_dicts(cur)

        return {
            "updated_after": updated_after or "none (full load)",
            "status_filter": status or "none",
            "limit":         limit,
            "offset":        offset,
            "count":         len(rows),
            "data":          rows
        }
    finally:
        conn.close()


@app.get("/transactions/batch/{batch_number}")
def get_batch(
    batch_number: int,
    size: int = Query(1000, ge=1, le=5000, description="Rows per batch")
):
    """
    Batch pagination endpoint.
    Useful for full historical load — iterate batch 0, 1, 2 ... until empty.
    """
    conn = get_conn()
    try:
        offset = batch_number * size
        query  = """
            SELECT * FROM transactions
            ORDER BY updated_at ASC
            LIMIT %(size)s
            OFFSET %(offset)s
        """
        cur = conn.cursor()
        cur.execute(query, {"size": size, "offset": offset})
        rows = rows_to_dicts(cur)
        return {
            "batch_number": batch_number,
            "size":         size,
            "count":        len(rows),
            "has_more":     len(rows) == size,
            "data":         rows
        }
    finally:
        conn.close()