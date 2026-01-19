"""
Streaming endpoints for simulating enterprise data pipelines.
Part of Phase 1: The Resilient Ingest (Sidecar) architecture.
Phase 3: Adds "Source of Truth" repair endpoint for Active Repair Agent.
"""
import asyncio
import hashlib
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Query, Path, HTTPException
from fastapi.responses import StreamingResponse

router = APIRouter(tags=["stream"])

VENDOR_NAMES = [
    "Acme Corp", "TechFlow Inc", "DataSync Solutions", "CloudBridge Ltd",
    "NetWorks Pro", "InfoStream Systems", "DigiPipe Industries", "FlowLogic LLC",
    "StreamForce Co", "PipeLink Technologies", "SyncMaster Corp", "DataPulse Inc"
]

BILLING_ADDRESSES = [
    {"street": "123 Enterprise Way", "city": "San Francisco", "state": "CA", "zip": "94105", "country": "USA"},
    {"street": "456 Tech Boulevard", "city": "Austin", "state": "TX", "zip": "78701", "country": "USA"},
    {"street": "789 Innovation Drive", "city": "Seattle", "state": "WA", "zip": "98101", "country": "USA"},
    {"street": "101 Cloud Street", "city": "New York", "state": "NY", "zip": "10001", "country": "USA"},
    {"street": "202 Data Center Lane", "city": "Denver", "state": "CO", "zip": "80202", "country": "USA"},
    {"street": "303 SaaS Avenue", "city": "Boston", "state": "MA", "zip": "02101", "country": "USA"},
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
        "billing_address": random.choice(BILLING_ADDRESSES),
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


def inject_chaos_drift(record: dict) -> dict:
    """
    Create a 'drifted' record - valid JSON but missing critical fields.
    This simulates data drift where the MuleSoft pipe drops fields.
    The Sidecar should detect this and call the repair endpoint.
    """
    drifted_record = record.copy()
    
    drift_type = random.choice([
        "missing_vendor_id",
        "missing_billing_address",
        "missing_vendor_completely",
        "missing_multiple_fields"
    ])
    
    if drift_type == "missing_vendor_id":
        if "vendor" in drifted_record and isinstance(drifted_record["vendor"], dict):
            drifted_record["vendor"] = {
                k: v for k, v in drifted_record["vendor"].items() 
                if k != "vendor_id"
            }
    elif drift_type == "missing_billing_address":
        drifted_record.pop("billing_address", None)
    elif drift_type == "missing_vendor_completely":
        drifted_record.pop("vendor", None)
    elif drift_type == "missing_multiple_fields":
        if "vendor" in drifted_record and isinstance(drifted_record["vendor"], dict):
            drifted_record["vendor"] = {
                k: v for k, v in drifted_record["vendor"].items() 
                if k != "vendor_id"
            }
        drifted_record.pop("billing_address", None)
        drifted_record.pop("po_number", None)
    
    drifted_record["_chaos_injected"] = "drift"
    drifted_record["_drift_type"] = drift_type
    return drifted_record


def generate_pristine_invoice(invoice_id: str) -> dict:
    """
    Generate a deterministic, complete 'Source of Truth' invoice record.
    Given the same invoice_id, always returns the same pristine record.
    This is used by the repair endpoint to provide missing fields.
    """
    seed = int(hashlib.md5(invoice_id.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    
    invoice_num = int(invoice_id.replace("INV-", "")) if invoice_id.startswith("INV-") else seed % 1000000
    
    base_date = datetime(2025, 1, 1)
    days_offset = seed % 365
    invoice_date = base_date + timedelta(days=days_offset)
    due_date = invoice_date + timedelta(days=rng.choice([15, 30, 45, 60]))
    
    line_items = []
    for _ in range(rng.randint(1, 5)):
        qty = rng.randint(1, 100)
        unit_price = round(rng.uniform(10.0, 5000.0), 2)
        line_items.append({
            "description": rng.choice(PRODUCT_DESCRIPTIONS),
            "quantity": qty,
            "unit_price": unit_price,
            "line_total": round(qty * unit_price, 2)
        })
    
    subtotal = sum(item["line_total"] for item in line_items)
    tax_rate = rng.choice([0.0, 0.05, 0.07, 0.10, 0.20])
    tax_amount = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax_amount, 2)
    
    vendor_idx = seed % len(VENDOR_NAMES)
    address_idx = seed % len(BILLING_ADDRESSES)
    
    return {
        "record_type": "invoice",
        "source_system": "salesforce_master",
        "invoice_id": invoice_id,
        "vendor": {
            "name": VENDOR_NAMES[vendor_idx],
            "vendor_id": f"V-{1000 + (seed % 9000)}",
            "tax_id": f"{10 + (seed % 90)}-{1000000 + (seed % 9000000)}"
        },
        "billing_address": BILLING_ADDRESSES[address_idx],
        "invoice_date": invoice_date.isoformat(),
        "due_date": due_date.isoformat(),
        "currency": rng.choice(CURRENCIES),
        "line_items": line_items,
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax_amount": tax_amount,
        "total_amount": total,
        "payment_status": rng.choice(["pending", "partial", "paid", "overdue"]),
        "po_number": f"PO-{10000 + (seed % 90000)}",
        "notes": rng.choice([None, "Net 30 terms", "Recurring monthly", "One-time purchase"]),
        "created_at": invoice_date.isoformat(),
        "updated_at": (invoice_date + timedelta(days=rng.randint(0, 30))).isoformat(),
        "is_pristine": True
    }


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
            chaos_type = random.choice(["malformed", "latency", "bad_types", "drift", "drift"])
            
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
            
            elif chaos_type == "drift":
                record = generate_invoice_record()
                drifted_record = inject_chaos_drift(record)
                drifted_record["_stream_meta"] = {
                    "record_number": record_count,
                    "chaos_mode": chaos,
                    "chaos_events_so_far": chaos_count
                }
                yield json.dumps(drifted_record) + "\n"
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
        drift_sample = inject_chaos_drift(generate_invoice_record())
        return {
            "normal_record": record,
            "chaos_record": chaos_sample,
            "drifted_record": drift_sample,
            "malformed_example": inject_chaos_malformed_json()
        }
    
    return record


@router.get("/api/source/salesforce/invoice/{invoice_id}")
async def get_pristine_invoice(
    invoice_id: str = Path(..., description="The invoice ID to look up (e.g., INV-123456)")
):
    """
    Source of Truth API - The Repair Shop.
    
    Returns the pristine, complete record for a given invoice ID.
    This simulates a Salesforce master database that always has the full data.
    
    **Purpose:** When the MuleSoft stream produces "drifted" records (missing fields),
    the DCL Ingest Sidecar can call this endpoint to fetch the missing data
    and repair the record before pushing to Redis.
    
    **Usage:**
    ```bash
    curl "http://localhost:5000/api/source/salesforce/invoice/INV-123456"
    ```
    
    **Returns:** Complete invoice with all fields including:
    - vendor.vendor_id (commonly missing in drift)
    - billing_address (commonly missing in drift)
    - All other standard invoice fields
    """
    if not invoice_id:
        raise HTTPException(status_code=400, detail="invoice_id is required")
    
    pristine_record = generate_pristine_invoice(invoice_id)
    
    return {
        "source": "salesforce_master",
        "lookup_timestamp": datetime.now().isoformat(),
        "invoice": pristine_record
    }
