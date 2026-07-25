from fastapi import FastAPI, Query
from typing import Optional
import psycopg2

app = FastAPI(
    title="CardPulse Transactions API",
    description="Mock payment processor feed for CardPulse ELT pipeline",
    version="3.0.0"
)

CONN_STR = "postgresql://neondb_owner:npg_WQD4UYhrXo8H@ep-withered-cell-ay6cmuw3-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def get_conn():
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
        "project":   "CardPulse Transactions API",
        "endpoints": ["/transactions", "/transactions/date/{date}",
                      "/transactions/batch/{batch_number}",
                      "/transactions/since", "/health"]
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
    limit:  int           = Query(1000, le=5000),
    offset: int           = Query(0),
    status: Optional[str] = Query(None),
):
    conn = get_conn()
    try:
        if status:
            query = "SELECT * FROM transactions WHERE status = %(status)s ORDER BY updated_at ASC LIMIT %(limit)s OFFSET %(offset)s"
            params = {"status": status, "limit": limit, "offset": offset}
        else:
            query = "SELECT * FROM transactions ORDER BY updated_at ASC LIMIT %(limit)s OFFSET %(offset)s"
            params = {"limit": limit, "offset": offset}

        cur = conn.cursor()
        cur.execute(query, params)
        rows = rows_to_dicts(cur)
        return {"limit": limit, "offset": offset, "count": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/transactions/since")
def get_since(
    timestamp: str = Query(..., description="ISO format: 2024-10-01T00:00:00"),
    limit:     int = Query(5000, le=10000),
    offset:    int = Query(0)
):
    """Incremental loading endpoint — returns rows where updated_at > timestamp."""
    conn = get_conn()
    try:
        query = """
            SELECT * FROM transactions
            WHERE updated_at > %(ts)s
            ORDER BY updated_at ASC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        cur = conn.cursor()
        cur.execute(query, {"ts": timestamp, "limit": limit, "offset": offset})
        rows = rows_to_dicts(cur)
        return {"since": timestamp, "count": len(rows), "limit": limit, "offset": offset, "data": rows}
    finally:
        conn.close()


@app.get("/transactions/date/{date}")
def get_by_date(
    date:   str,
    limit:  int = Query(5000, le=10000),
    offset: int = Query(0)
):
    conn = get_conn()
    try:
        query = """
            SELECT * FROM transactions
            WHERE DATE(transaction_timestamp) = %(date)s
            ORDER BY updated_at ASC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        cur = conn.cursor()
        cur.execute(query, {"date": date, "limit": limit, "offset": offset})
        rows = rows_to_dicts(cur)
        return {"date": date, "count": len(rows), "limit": limit, "offset": offset, "data": rows}
    finally:
        conn.close()


@app.get("/transactions/batch/{batch_number}")
def get_batch(
    batch_number: int,
    size:         int = Query(1000, le=5000)
):
    conn = get_conn()
    try:
        offset = batch_number * size
        query  = "SELECT * FROM transactions ORDER BY updated_at ASC LIMIT %(size)s OFFSET %(offset)s"
        cur    = conn.cursor()
        cur.execute(query, {"size": size, "offset": offset})
        rows = rows_to_dicts(cur)
        return {"batch_number": batch_number, "size": size, "count": len(rows), "data": rows}
    finally:
        conn.close()