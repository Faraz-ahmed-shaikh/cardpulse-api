from fastapi import FastAPI, Query
from typing import Optional
import json, requests, io

app = FastAPI(
    title="CardPulse Transactions API",
    description="Mock payment processor feed for CardPulse ELT pipeline",
    version="1.0.0"
)

HF_URL = "https://huggingface.co/datasets/farazahmed417/cardpulse-transactions/resolve/main/transactions_all.jsonl"

def stream_transactions(date: str = None, status: str = None,
                         limit: int = 1000, offset: int = 0):
    """Stream JSONL from HF, filter on the fly — never loads full file into RAM."""
    response = requests.get(HF_URL, stream=True)
    response.raise_for_status()

    matched = []
    skipped = 0

    for raw_line in response.iter_lines():
        if not raw_line:
            continue
        try:
            txn = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        # Apply filters
        if date and not txn.get("transaction_timestamp", "").startswith(date):
            continue
        if status and txn.get("status") != status:
            continue

        # Apply offset
        if skipped < offset:
            skipped += 1
            continue

        matched.append(txn)

        if len(matched) >= limit:
            break

    return matched


@app.get("/")
def root():
    return {
        "project":   "CardPulse Transactions API",
        "note":      "Streaming mode — filters applied on the fly",
        "endpoints": ["/transactions", "/transactions/date/{date}",
                      "/transactions/batch/{batch_number}", "/health"]
    }


@app.get("/health")
def health():
    return {"status": "ok", "mode": "streaming"}


@app.get("/transactions")
def get_transactions(
    limit:  int           = Query(1000, le=5000),
    offset: int           = Query(0),
    status: Optional[str] = Query(None),
):
    data = stream_transactions(status=status, limit=limit, offset=offset)
    return {"limit": limit, "offset": offset, "count": len(data), "data": data}


@app.get("/transactions/date/{date}")
def get_by_date(
    date:   str,
    limit:  int = Query(5000, le=5000),
    offset: int = Query(0)
):
    data = stream_transactions(date=date, limit=limit, offset=offset)
    return {"date": date, "count": len(data), "data": data}


@app.get("/transactions/batch/{batch_number}")
def get_batch(
    batch_number: int,
    size:         int = Query(1000, le=5000)
):
    offset = batch_number * size
    data   = stream_transactions(limit=size, offset=offset)
    return {"batch_number": batch_number, "size": size,
            "count": len(data), "data": data}