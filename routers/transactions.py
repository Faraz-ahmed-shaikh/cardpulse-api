# routers/transactions.py
from fastapi import APIRouter, Query
from typing import Optional
from database import get_conn, rows_to_dicts


router = APIRouter(
    prefix="/transactions",
    tags=["Transactions"],
)


@router.get(
    "",
    summary="Fetch transactions",
    description="""
    **Primary ingestion endpoint for the Transactions Bronze pipeline.**

    - Omit `updated_after` for a **full load** (first pipeline run).
    - Pass `updated_after` for an **incremental load** — returns only rows
      where `updated_at` is strictly after the given timestamp.
    - Combine with `limit` and `offset` to paginate through large results.

    Timestamp format: `YYYY-MM-DDTHH:MM:SSZ` — e.g. `2024-10-01T00:00:00Z`
    """
)
def get_transactions(
    updated_after: Optional[str] = Query(
        None,
        description="ISO 8601 timestamp. Returns rows where updated_at > this value."
    ),
    status: Optional[str] = Query(
        None,
        description="Filter by status: Successful | Failed | Pending"
    ),
    limit:  int = Query(1000, ge=1, le=10000, description="Page size (max 10000)"),
    offset: int = Query(0,    ge=0,           description="Rows to skip"),
):
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
            SELECT * FROM transactions
            {where}
            ORDER BY updated_at ASC
            LIMIT %(limit)s OFFSET %(offset)s
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


@router.get(
    "/batch/{batch_number}",
    summary="Fetch transactions by batch number",
    description="""
    Pagination by batch number instead of offset.
    Easier for looping — keep incrementing batch_number until `has_more` is false.
    """
)
def get_batch(
    batch_number: int,
    size: int = Query(1000, ge=1, le=5000, description="Rows per batch")
):
    conn = get_conn()
    try:
        offset = batch_number * size
        query  = """
            SELECT * FROM transactions
            ORDER BY updated_at ASC
            LIMIT %(size)s OFFSET %(offset)s
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