"""
Streaming endpoints for simulating enterprise data pipelines.
Part of Phase 1: The Resilient Ingest (Sidecar) architecture.
"""
import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import AsyncGenerator

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

router = APIRouter(tags=["stream"])

VENDOR_NAMES = [
    "Acme Corp", "TechFlow Inc", "DataSync Solutions", "CloudBridge Ltd",
    "NetWorks Pro", "InfoStream Systems", "DigiPipe Industries", "FlowLogic LLC",
    "StreamForce Co", "PipeLink Technologies", "SyncMaster Corp", "DataPulse Inc"
]

PRODUCT_DESCRIPTIONS = [
    "Enterprise SaaS License", "API Integration Service", "Cloud Storage Tier",
    "Data Processing Credits", "Platform Subscription", "Support Package Premium",
    "Analytics Dashboard Pro", "Security Compliance Module", "Backup Service Annual",
    "Consulting Hours Bundle", "Training Certification", "Custom Development Sprint"
]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]

CHAOS_BAD_AMOUNTS = ["THREE", "FIFTY", "one hundred", "N/A", "", None, "€500", "$1,234.56"]


def generate_invoice_record() -> dict:
    """Generate a realistic MuleSoft invoice sync record."""
    invoice_date = datetime.now() - timedelta(days=random.randint(0, 90))
    due_date = invoice_date + timedelta(days=random.choice([15, 30, 45, 60]))
    
    line_items = []
    for _ in range(random.randint(1, 5)):
        qty = random.randint(1, 100)
        unit_price = round(random.uniform(10.0, 5000.0), 2)
        line_items.append({
            "description": random.choice(PRODUCT_DESCRIPTIONS),
            "quantity": qty,
            "unit_price": unit_price,
            "line_total": round(qty * unit_price, 2)
        })
    
    subtotal = sum(item["line_total"] for item in line_items)
    tax_rate = random.choice([0.0, 0.05, 0.07, 0.10, 0.20])
    tax_amount = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax_amount, 2)
    
    return {
        "record_type": "invoice",
        "source_system": "mulesoft_erp_sync",
        "invoice_id": f"INV-{random.randint(100000, 999999)}",
        "vendor": {
            "name": random.choice(VENDOR_NAMES),
            "vendor_id": f"V-{random.randint(1000, 9999)}",
            "tax_id": f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}"
        },
        "invoice_date": invoice_date.isoformat(),
        "due_date": due_date.isoformat(),
        "currency": random.choice(CURRENCIES),
        "line_items": line_items,
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax_amount": tax_amount,
        "total_amount": total,
        "payment_status": random.choice(["pending", "partial", "paid", "overdue"]),
        "po_number": f"PO-{random.randint(10000, 99999)}" if random.random() > 0.3 else None,
        "notes": random.choice([None, "Net 30 terms", "Recurring monthly", "One-time purchase"]),
        "sync_timestamp": datetime.now().isoformat(),
        "correlation_id": str(uuid.uuid4())
    }


def inject_chaos_malformed_json() -> str:
    """Return malformed JSON to break parsers."""
    chaos_variants = [
        '{"invoice_id": "INV-BROKEN", "amount": 100, "vendor":',
        '{"incomplete": true, "data": [1, 2, 3',
        'not even json at all!!!',
        '{"valid_start": 1, {"nested_broken": true}}',
        '{"amount": 500, "currency": "USD"',
        '',
        '\n\n',
        '{"invoice_id": "INV-999", total_amount: 500}',
    ]
    return random.choice(chaos_variants)


def inject_chaos_bad_types(record: dict) -> dict:
    """Inject bad types into an otherwise valid record."""
    chaos_record = record.copy()
    
    chaos_type = random.choice(["bad_amount", "bad_date", "bad_nested", "null_required"])
    
    if chaos_type == "bad_amount":
        chaos_record["total_amount"] = random.choice(CHAOS_BAD_AMOUNTS)
        chaos_record["subtotal"] = "INVALID"
    elif chaos_type == "bad_date":
        chaos_record["invoice_date"] = random.choice(["not-a-date", "yesterday", 12345, None])
        chaos_record["due_date"] = "ASAP"
    elif chaos_type == "bad_nested":
        chaos_record["vendor"] = "This should be an object"
        chaos_record["line_items"] = "Not an array"
    elif chaos_type == "null_required":
        chaos_record["invoice_id"] = None
        chaos_record["vendor"] = None
    
    chaos_record["_chaos_injected"] = chaos_type
    return chaos_record


async def generate_stream(
    speed: str = "normal",
    chaos: bool = False
) -> AsyncGenerator[str, None]:
    """Generate a continuous stream of invoice records."""
    
    if speed == "fast":
        delay = 0.01
    elif speed == "slow":
        delay = 1.0
    else:
        delay = 0.1
    
    record_count = 0
    chaos_count = 0
    
    while True:
        record_count += 1
        
        if chaos and random.random() < 0.10:
            chaos_count += 1
            chaos_type = random.choice(["malformed", "latency", "bad_types"])
            
            if chaos_type == "malformed":
                yield inject_chaos_malformed_json() + "\n"
                await asyncio.sleep(delay)
                continue
            
            elif chaos_type == "latency":
                yield json.dumps({
                    "_chaos": "latency_spike_starting",
                    "delay_seconds": 5,
                    "record_number": record_count
                }) + "\n"
                await asyncio.sleep(5)
                continue
            
            elif chaos_type == "bad_types":
                record = generate_invoice_record()
                chaos_record = inject_chaos_bad_types(record)
                yield json.dumps(chaos_record) + "\n"
                await asyncio.sleep(delay)
                continue
        
        record = generate_invoice_record()
        record["_stream_meta"] = {
            "record_number": record_count,
            "chaos_mode": chaos,
            "chaos_events_so_far": chaos_count
        }
        yield json.dumps(record) + "\n"
        await asyncio.sleep(delay)


@router.get("/api/stream/synthetic/mulesoft")
async def stream_mulesoft_invoices(
    speed: str = Query("normal", description="Stream speed: 'fast' (100/sec), 'normal' (10/sec), 'slow' (1/sec)"),
    chaos: bool = Query(False, description="Enable chaos mode: malformed JSON, latency spikes, bad types (~10% rate)")
):
    """
    Stream synthetic MuleSoft invoice data as NDJSON (newline-delimited JSON).
    
    This endpoint simulates a "MuleSoft Invoice Sync" enterprise data pipeline.
    Use it to test DCL's Ingest Sidecar resilience.
    
    **Chaos Mode Anomalies (when chaos=true):**
    - Malformed JSON: Half-written objects to break parsers
    - Latency Spikes: 5-second delays to test timeouts  
    - Bad Types: `"amount": "THREE"` instead of numbers
    
    **Usage:**
    ```bash
    curl -N "http://localhost:5000/api/stream/synthetic/mulesoft"
    curl -N "http://localhost:5000/api/stream/synthetic/mulesoft?speed=fast&chaos=true"
    ```
    """
    return StreamingResponse(
        generate_stream(speed=speed, chaos=chaos),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


@router.get("/api/stream/synthetic/mulesoft/sample")
async def sample_mulesoft_record(
    chaos: bool = Query(False, description="Include a chaos-injected sample")
):
    """
    Get a single sample invoice record (non-streaming).
    Useful for understanding the data structure before consuming the stream.
    """
    record = generate_invoice_record()
    
    if chaos:
        chaos_sample = inject_chaos_bad_types(generate_invoice_record())
        return {
            "normal_record": record,
            "chaos_record": chaos_sample,
            "malformed_example": inject_chaos_malformed_json()
        }
    
    return record
