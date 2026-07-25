# routers/customers.py
from fastapi import APIRouter, Query
from typing import Optional
from database import get_conn, rows_to_dicts


router = APIRouter(
    prefix="/customers",
    tags=["Customers"],
)


@router.get(
    "",
    summary="Fetch customers",
    description="""
    **Primary ingestion endpoint for the Customers Bronze pipeline.**

    - Omit `updated_after` for a **full load** (first pipeline run).
    - Pass `updated_after` for an **incremental load** — returns only rows
      where `updated_at` is strictly after the given timestamp.
    - Supports SCD Type 2 — multiple snapshots per customer are returned
      when they exist. Use `snapshot_date` to distinguish versions.

    Timestamp format: `YYYY-MM-DDTHH:MM:SSZ` — e.g. `2024-07-01T00:00:00Z`
    """
)
def get_customers(
    updated_after: Optional[str] = Query(
        None,
        description="ISO 8601 timestamp. Returns rows where updated_at > this value."
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

        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"""
            SELECT * FROM customers
            {where}
            ORDER BY updated_at ASC
            LIMIT %(limit)s OFFSET %(offset)s
        """

        cur = conn.cursor()
        cur.execute(query, params)
        rows = rows_to_dicts(cur)

        return {
            "updated_after": updated_after or "none (full load)",
            "limit":         limit,
            "offset":        offset,
            "count":         len(rows),
            "data":          rows
        }
    finally:
        conn.close()