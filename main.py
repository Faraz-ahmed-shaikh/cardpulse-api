# main.py
from fastapi import FastAPI
from routers import transactions, customers, cards

app = FastAPI(
    title="CardPulse API",
    description="""
    Mock payment processor REST API for the CardPulse ELT Lakehouse pipeline.

    Exposes three resources for Databricks Bronze ingestion:
    - **/transactions** — credit card transaction events
    - **/customers**    — customer dimension (supports SCD Type 2)
    - **/cards**        — card dimension

    All endpoints support incremental loading via `updated_after` parameter.
    """,
    version="4.0.0",
)

app.include_router(transactions.router)
app.include_router(customers.router)
app.include_router(cards.router)


@app.get("/", tags=["Root"])
def root():
    return {
        "project": "CardPulse Transactions API",
        "version": "4.0.0",
        "docs":    "/docs",
        "endpoints": {
            "transactions": "GET /transactions?updated_after=TIMESTAMP&limit=1000&offset=0",
            "customers":    "GET /customers?updated_after=TIMESTAMP&limit=1000&offset=0",
            "cards":        "GET /cards?updated_after=TIMESTAMP&limit=1000&offset=0",
            "health":       "GET /health",
            "batch":        "GET /transactions/batch/{n}?size=1000",
        }
    }


@app.get("/health", tags=["Health"])
def health():
    from database import get_conn
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions")
    txn_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM customers")
    cust_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM cards")
    card_count = cur.fetchone()[0]
    conn.close()
    return {
        "status":       "ok",
        "transactions": txn_count,
        "customers":    cust_count,
        "cards":        card_count,
    }