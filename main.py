from fastapi import FastAPI, Query
from typing import Optional
import json, requests

app = FastAPI(
    title="CardPulse Transactions API",
    description="Mock payment processor feed for CardPulse ELT pipeline",
    version="1.0.0"
)

# Direct download URL from Hugging Face — no confirmation pages, always works
HF_URL = "https://huggingface.co/datasets/farazahmed417/cardpulse-transactions/resolve/main/transactions_all.jsonl"

def load_data(url: str):
    print(f"Downloading transactions from Hugging Face...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    lines = response.content.decode("utf-8").splitlines()
    data  = [json.loads(line) for line in lines if line.strip()]
    print(f"Loaded {len(data):,} transactions")
    return data

ALL_TRANSACTIONS = load_data(HF_URL)

# --- all endpoints stay exactly the same, no changes needed ---

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
    limit:  int           = Query(1000, le=5000),
    offset: int           = Query(0),
    status: Optional[str] = Query(None),
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
    size:         int = Query(1000, le=5000)
):
    start         = batch_number * size
    total_batches = (len(ALL_TRANSACTIONS) + size - 1) // size
    return {
        "batch_number":  batch_number,
        "total_batches": total_batches,
        "size":          size,
        "data":          ALL_TRANSACTIONS[start: start + size]
    }