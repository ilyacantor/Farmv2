"""
Scenario API endpoints for DCL/BLL/NLQ ground truth system.

Provides endpoints to generate scenarios and access ground truth metrics
for validating intent resolution and aggregation correctness.
"""
import asyncio
import hashlib
import json
import random
import time
from datetime import datetime
from enum import Enum
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, Query, Path, HTTPException, Body
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.models.scenarios import (
    ScaleEnum,
    ScenarioManifest,
    Invoice,
    RevenueMetric,
    RevenueMoMMetric,
    TopCustomersMetric,
    VendorSpendMetric,
    ResourceHealthMetric,
    InvoiceVerificationResult,
    TotalRevenueResponse,
)
from src.generators.scenarios import get_or_create_scenario, ScenarioGenerator

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])


class GenerateRequest(BaseModel):
    seed: Optional[int] = None
    scale: ScaleEnum = ScaleEnum.medium


class GenerateResponse(BaseModel):
    scenario_id: str
    manifest: ScenarioManifest


class InvoiceVerifyRequest(BaseModel):
    invoice_id: str
    customer_id: Optional[str] = None
    vendor_id: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    invoice_date: Optional[str] = None
    due_date: Optional[str] = None
    status: Optional[str] = None
    is_refund: Optional[bool] = None
    original_invoice_id: Optional[str] = None


_scenario_seeds: dict[str, tuple[int, ScaleEnum]] = {}


def _generate_scenario_id(seed: int, scale: str) -> str:
    """Generate deterministic scenario ID from seed and scale.
    
    The scenario_id is fully deterministic - same seed + scale always
    produces the same scenario_id.
    """
    hash_input = f"scenario:v1:{seed}:{scale}"
    return hashlib.md5(hash_input.encode()).hexdigest()[:12]


@router.post("/generate", response_model=GenerateResponse)
async def generate_scenario(request: GenerateRequest):
    """Generate a new scenario with deterministic data.
    
    If seed is not provided, one will be auto-generated based on current time.
    The same seed will always produce identical data.
    """
    seed = request.seed if request.seed is not None else int(time.time() * 1000) % 1000000
    
    scenario_id = _generate_scenario_id(seed, request.scale.value)
    
    _scenario_seeds[scenario_id] = (seed, request.scale)
    
    generator = get_or_create_scenario(scenario_id, seed, request.scale)
    manifest = generator.get_manifest()
    
    return GenerateResponse(
        scenario_id=scenario_id,
        manifest=manifest
    )


def _get_generator(scenario_id: str) -> ScenarioGenerator:
    """Get generator for scenario, raising 404 if not found."""
    if scenario_id not in _scenario_seeds:
        raise HTTPException(
            status_code=404,
            detail=f"Scenario {scenario_id} not found. Generate it first with POST /api/scenarios/generate"
        )
    
    seed, scale = _scenario_seeds[scenario_id]
    return get_or_create_scenario(scenario_id, seed, scale)


@router.get("/{scenario_id}/manifest", response_model=ScenarioManifest)
async def get_manifest(scenario_id: str = Path(..., description="Scenario ID")):
    """Get full manifest with entity counts and time range."""
    generator = _get_generator(scenario_id)
    return generator.get_manifest()


@router.get("/{scenario_id}/metrics/revenue", response_model=RevenueMetric)
async def get_revenue_metric(scenario_id: str = Path(..., description="Scenario ID")):
    """Get total revenue metric for the scenario period."""
    generator = _get_generator(scenario_id)
    return generator.get_revenue_metric()


@router.get("/{scenario_id}/metrics/revenue-mom", response_model=RevenueMoMMetric)
async def get_revenue_mom(scenario_id: str = Path(..., description="Scenario ID")):
    """Get month-over-month revenue breakdown."""
    generator = _get_generator(scenario_id)
    return generator.get_revenue_mom()


@router.get("/{scenario_id}/metrics/total-revenue", response_model=TotalRevenueResponse)
async def get_total_revenue(
    scenario_id: str = Path(..., description="Scenario ID"),
    time_window: Optional[str] = Query(
        None,
        description="Time filter: 'last_year', 'this_year', 'ytd', 'last_quarter', 'this_quarter', "
                    "'q1'-'q4', 'last_month', 'this_month', or a year like '2024'. "
                    "Leave empty for all-time total."
    )
):
    """Get total revenue with optional time filtering.

    Supports various time window filters for temporal revenue queries:
    - **Year-based**: `last_year`, `this_year`, `ytd` (year-to-date), or specific year (`2024`, `2025`)
    - **Quarter-based**: `last_quarter`, `this_quarter`, `q1`, `q2`, `q3`, `q4`
    - **Month-based**: `last_month`, `this_month`
    - **All-time**: Leave `time_window` empty or null

    Returns total revenue, transaction count, and the date range applied.
    """
    generator = _get_generator(scenario_id)
    return generator.get_total_revenue(time_window)


@router.get("/{scenario_id}/metrics/top-customers", response_model=TopCustomersMetric)
async def get_top_customers(
    scenario_id: str = Path(..., description="Scenario ID"),
    limit: int = Query(10, ge=1, le=100, description="Number of top customers to return"),
    time_window: Optional[str] = Query(
        None,
        description="Time filter: 'last_year', 'this_year', 'ytd', 'last_quarter', 'this_quarter', "
                    "'q1'-'q4', 'last_month', 'this_month', or a year like '2024'. "
                    "Leave empty for all-time totals."
    )
):
    """Get top customers by revenue with optional time filtering.

    Supports various time window filters for temporal queries:
    - **Year-based**: `last_year`, `this_year`, `ytd` (year-to-date), or specific year (`2024`, `2025`)
    - **Quarter-based**: `last_quarter`, `this_quarter`, `q1`, `q2`, `q3`, `q4`
    - **Month-based**: `last_month`, `this_month`
    - **All-time**: Leave `time_window` empty or null

    Returns top customers ranked by revenue in the specified time period.
    """
    generator = _get_generator(scenario_id)
    return generator.get_top_customers(limit, time_window)


@router.get("/{scenario_id}/metrics/vendor-spend", response_model=VendorSpendMetric)
async def get_vendor_spend(scenario_id: str = Path(..., description="Scenario ID")):
    """Get vendor spend breakdown."""
    generator = _get_generator(scenario_id)
    return generator.get_vendor_spend()


@router.get("/{scenario_id}/metrics/resource-health", response_model=ResourceHealthMetric)
async def get_resource_health(scenario_id: str = Path(..., description="Scenario ID")):
    """Get resource health metrics (active/zombie/orphan counts)."""
    generator = _get_generator(scenario_id)
    return generator.get_resource_health()


@router.get("/{scenario_id}/source/invoice/{invoice_id}", response_model=Invoice)
async def get_source_invoice(
    scenario_id: str = Path(..., description="Scenario ID"),
    invoice_id: str = Path(..., description="Invoice ID")
):
    """Get pristine invoice record (ground truth).
    
    This is the Source of Truth API for DCL/AAM to verify repairs.
    """
    generator = _get_generator(scenario_id)
    invoice = generator.get_invoice(invoice_id)
    
    if invoice is None:
        raise HTTPException(
            status_code=404,
            detail=f"Invoice {invoice_id} not found in scenario {scenario_id}"
        )
    
    return invoice


@router.post("/{scenario_id}/verify/invoice", response_model=InvoiceVerificationResult)
async def verify_invoice(
    scenario_id: str = Path(..., description="Scenario ID"),
    request: InvoiceVerifyRequest = Body(...)
):
    """Verify a repaired invoice record against ground truth.
    
    Submit the repaired invoice record and receive verification results
    showing any mismatches with the ground truth.
    """
    generator = _get_generator(scenario_id)
    
    submitted = request.model_dump(exclude_none=True)
    result = generator.verify_invoice(submitted)
    
    return InvoiceVerificationResult(**result)


class ScenarioChaosType(str, Enum):
    """DCL-specific chaos types for scenario toxic streams."""
    MISSING_FIELDS = "missing_fields"
    DUPLICATE_INVOICE = "duplicate_invoice"
    INCORRECT_CURRENCY = "incorrect_currency"
    STALE_TIMESTAMP = "stale_timestamp"
    ORPHANED_REFERENCE = "orphaned_reference"


async def _generate_scenario_stream(
    generator,
    chaos: bool = False,
    chaos_rate: float = 0.15,
    speed: str = "normal"
) -> AsyncGenerator[str, None]:
    """Generate toxic MuleSoft stream from scenario invoices."""
    
    speed_delays = {"fast": 0.01, "normal": 0.1, "slow": 1.0}
    delay = speed_delays.get(speed, 0.1)
    
    invoices = generator.get_invoices()
    rng = random.Random(generator.seed)
    
    chaos_counts = {ct: 0 for ct in ScenarioChaosType}
    record_number = 0
    
    for inv in invoices:
        record_number += 1
        record = {
            "record_type": "invoice",
            "source_system": "mulesoft_erp_sync",
            "invoice_id": inv.invoice_id,
            "customer_id": inv.customer_id,
            "vendor_id": inv.vendor_id,
            "amount": inv.amount,
            "currency": inv.currency.value,
            "invoice_date": inv.invoice_date,
            "due_date": inv.due_date,
            "status": inv.status.value,
            "is_refund": inv.is_refund,
            "original_invoice_id": inv.original_invoice_id,
            "sync_timestamp": datetime.now().isoformat(),
        }
        
        chaos_event = None
        if chaos and rng.random() < chaos_rate:
            chaos_type = rng.choice(list(ScenarioChaosType))
            chaos_counts[chaos_type] += 1
            
            if chaos_type == ScenarioChaosType.MISSING_FIELDS:
                for field in rng.sample(["vendor_id", "customer_id", "due_date", "status"], k=rng.randint(1, 2)):
                    record.pop(field, None)
                chaos_event = "missing_fields"
                
            elif chaos_type == ScenarioChaosType.DUPLICATE_INVOICE:
                chaos_event = "duplicate_invoice"
                
            elif chaos_type == ScenarioChaosType.INCORRECT_CURRENCY:
                wrong_currencies = ["XXX", "ZZZ", "ABC", "EURO", "DOLLAR"]
                record["currency"] = rng.choice(wrong_currencies)
                chaos_event = "incorrect_currency"
                
            elif chaos_type == ScenarioChaosType.STALE_TIMESTAMP:
                record["invoice_date"] = "2020-01-01"
                record["sync_timestamp"] = "2020-01-01T00:00:00"
                chaos_event = "stale_timestamp"
                
            elif chaos_type == ScenarioChaosType.ORPHANED_REFERENCE:
                record["customer_id"] = f"CUST-ORPHAN-{rng.randint(90000, 99999)}"
                record["vendor_id"] = f"VENDOR-ORPHAN-{rng.randint(90000, 99999)}"
                chaos_event = "orphaned_reference"
        
        record["_stream_meta"] = {
            "record_number": record_number,
            "chaos_mode": chaos,
            "chaos_event": chaos_event,
            "chaos_counts": {k.value: v for k, v in chaos_counts.items()},
            "total_chaos_events": sum(chaos_counts.values())
        }
        
        yield json.dumps(record) + "\n"
        
        if chaos_event == ScenarioChaosType.DUPLICATE_INVOICE:
            record_number += 1
            dup_record = record.copy()
            dup_record["_stream_meta"] = {
                "record_number": record_number,
                "chaos_mode": chaos,
                "chaos_event": "duplicate_invoice_copy",
                "chaos_counts": {k.value: v for k, v in chaos_counts.items()},
                "total_chaos_events": sum(chaos_counts.values())
            }
            yield json.dumps(dup_record) + "\n"
        
        await asyncio.sleep(delay)


@router.get("/{scenario_id}/stream/toxic")
async def stream_scenario_invoices(
    scenario_id: str = Path(..., description="Scenario ID"),
    chaos: bool = Query(True, description="Enable chaos injection (default: true)"),
    chaos_rate: float = Query(0.15, description="Probability of chaos per record (0.0-1.0)"),
    speed: str = Query("normal", description="Stream speed: 'fast' (100/sec), 'normal' (10/sec), 'slow' (1/sec)")
):
    """Stream scenario invoices as toxic MuleSoft data (NDJSON).
    
    This endpoint simulates a "MuleSoft Invoice Sync" enterprise data pipeline
    for the given scenario. Use it to test DCL's Ingest Sidecar resilience.
    
    **Chaos Types (when chaos=true):**
    - `missing_fields`: Random fields removed (vendor_id, customer_id, etc.)
    - `duplicate_invoice`: Same invoice emitted twice
    - `incorrect_currency`: Invalid currency codes (XXX, EURO, etc.)
    - `stale_timestamp`: Very old timestamps (2020-01-01)
    - `orphaned_reference`: Customer/vendor IDs that don't exist
    
    **Stream Metadata:**
    Each record includes `_stream_meta` with:
    - `chaos_event`: Type of chaos injected (or null)
    - `chaos_counts`: Running totals per chaos type
    - `total_chaos_events`: Total chaos events so far
    
    **Usage:**
    ```bash
    curl -N "http://localhost:5000/api/scenarios/{scenario_id}/stream/toxic"
    curl -N "http://localhost:5000/api/scenarios/{scenario_id}/stream/toxic?chaos_rate=0.3"
    ```
    """
    generator = _get_generator(scenario_id)
    
    return StreamingResponse(
        _generate_scenario_stream(generator, chaos=chaos, chaos_rate=chaos_rate, speed=speed),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


@router.get("/{scenario_id}/chaos-catalog")
async def get_scenario_chaos_catalog(
    scenario_id: str = Path(..., description="Scenario ID")
):
    """Get catalog of chaos types available for scenario toxic streams.
    
    Returns the DCL-specific chaos injection types with descriptions
    of what each one tests.
    """
    _get_generator(scenario_id)
    
    return {
        "scenario_id": scenario_id,
        "chaos_types": [
            {
                "type": "missing_fields",
                "description": "Random fields removed from invoice record",
                "affected_fields": ["vendor_id", "customer_id", "due_date", "status"],
                "dcl_test": "Tests repair via source API lookup"
            },
            {
                "type": "duplicate_invoice",
                "description": "Same invoice emitted twice in sequence",
                "affected_fields": ["invoice_id"],
                "dcl_test": "Tests deduplication logic"
            },
            {
                "type": "incorrect_currency",
                "description": "Invalid currency codes (XXX, EURO, etc.)",
                "affected_fields": ["currency"],
                "dcl_test": "Tests currency validation and normalization"
            },
            {
                "type": "stale_timestamp",
                "description": "Very old timestamps (2020-01-01)",
                "affected_fields": ["invoice_date", "sync_timestamp"],
                "dcl_test": "Tests stale data detection and handling"
            },
            {
                "type": "orphaned_reference",
                "description": "Customer/vendor IDs that don't exist in scenario",
                "affected_fields": ["customer_id", "vendor_id"],
                "dcl_test": "Tests referential integrity validation"
            }
        ],
        "default_chaos_rate": 0.15,
        "total_types": 5
    }


@router.get("/nlq/questions")
async def get_nlq_questions(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(100, ge=1, le=100, description="Number of questions to return")
):
    """
    Get NLQ test questions for DCL validation.
    
    Returns a dataset of natural language questions that can be validated
    against Farm's ground truth endpoints.
    """
    import os
    
    questions_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nlq_test_questions.json")
    
    try:
        with open(questions_path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NLQ questions dataset not found")
    
    questions = data.get("questions", [])
    
    if category:
        questions = [q for q in questions if q.get("category") == category]
    
    return {
        "metadata": data.get("metadata", {}),
        "categories": data.get("categories", {}),
        "questions": questions[:limit],
        "total_count": len(questions)
    }


@router.get("/nlq/categories")
async def get_nlq_categories():
    """
    Get available NLQ question categories.
    """
    import os
    
    questions_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nlq_test_questions.json")
    
    try:
        with open(questions_path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NLQ questions dataset not found")
    
    categories = data.get("categories", {})
    questions = data.get("questions", [])
    
    # Count questions per category
    category_counts = {}
    for q in questions:
        cat = q.get("category", "unknown")
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    return {
        "categories": [
            {"id": cat_id, "description": desc, "count": category_counts.get(cat_id, 0)}
            for cat_id, desc in categories.items()
        ],
        "total_categories": len(categories)
    }
