from fastapi import FastAPI, Query
from typing import Optional
import json, requests, io

app = FastAPI(
    title="CardPulse Transactions API",
    description="Mock payment processor feed for CardPulse ELT pipeline",
    version="1.0.0"
)

def load_from_drive(file_id: str):
    print("Downloading transactions from Google Drive...")
    url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    lines = response.content.decode("utf-8").splitlines()
    data  = [json.loads(line) for line in lines if line.strip()]
    print(f"Loaded {len(data):,} transactions")
    return data

# Replace with your actual file ID
FILE_ID = "1PH42kRgtSdr1dB3fS6TXEPx23CBfYTZi"
ALL_TRANSACTIONS = load_from_drive(FILE_ID)

# Load once on startup — stays in memory
print("Loading transactions...")
with open("transactions_all.jsonl", "r") as f:
    ALL_TRANSACTIONS = [json.loads(line) for line in f if line.strip()]
print(f"Loaded {len(ALL_TRANSACTIONS):,} transactions")


@app.get("/")
def root():
    return {
        "project": "CardPulse Transactions API",
        "total_transactions": len(ALL_TRANSACTIONS),
        "endpoints": ["/transactions", "/transactions/date/{date}", "/health"]
    }


@app.get("/health")
def health():
    return {"status": "ok", "records_loaded": len(ALL_TRANSACTIONS)}


@app.get("/transactions")
def get_transactions(
    limit:  int            = Query(1000, le=5000, description="Max rows to return"),
    offset: int            = Query(0,             description="Skip first N rows"),
    status: Optional[str]  = Query(None,          description="Filter by status: Successful / Failed / Pending"),
):
    filtered = ALL_TRANSACTIONS

    if status:
        filtered = [t for t in filtered if t.get("status") == status]

    return {
        "total_matched": len(filtered),
        "offset":        offset,
        "limit":         limit,
        "data":          filtered[offset: offset + limit]
    }


@app.get("/transactions/date/{date}")
def get_by_date(
    date:   str,
    limit:  int = Query(5000, le=10000),
    offset: int = Query(0)
):
    """
    Pull all transactions for a specific date.
    date format: YYYY-MM-DD  e.g. /transactions/date/2024-10-01
    """
    filtered = [
        t for t in ALL_TRANSACTIONS
        if t.get("transaction_timestamp", "").startswith(date)
    ]

    return {
        "date":          date,
        "total_matched": len(filtered),
        "offset":        offset,
        "limit":         limit,
        "data":          filtered[offset: offset + limit]
    }


@app.get("/transactions/batch/{batch_number}")
def get_batch(
    batch_number: int,
    size:         int = Query(1000, le=5000, description="Rows per batch")
):
    """
    Pull transactions by batch number.
    Useful for paginating the full dataset in your Databricks pipeline.
    batch 0 → rows 0-999, batch 1 → rows 1000-1999, etc.
    """
    start = batch_number * size
    end   = start + size
    total_batches = (len(ALL_TRANSACTIONS) + size - 1) // size

    return {
        "batch_number":  batch_number,
        "total_batches": total_batches,
        "size":          size,
        "data":          ALL_TRANSACTIONS[start:end]
    }