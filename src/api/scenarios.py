"""
Scenario API endpoints for DCL/BLL/NLQ ground truth system.

Provides endpoints to generate scenarios and access ground truth metrics
for validating intent resolution and aggregation correctness.
"""
import asyncio
import csv
import hashlib
import io
import json
import random
import time
from datetime import datetime
from enum import Enum
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, Query, Path, HTTPException, Body
from fastapi.responses import StreamingResponse, Response
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
from src.models.fabric import (
    IndustryVertical,
    FabricPlaneType,
    FabricPlaneConfig,
    IndustryProfile,
    INDUSTRY_VENDOR_WEIGHTS,
    generate_fabric_config,
    select_vendor_weighted,
)

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])
fabric_router = APIRouter(prefix="/api/fabric", tags=["fabric"])


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


@router.get("/{scenario_id}/export/invoices.csv")
async def export_invoices_csv(
    scenario_id: str = Path(..., description="Scenario ID"),
    limit: Optional[int] = Query(None, ge=1, le=10000, description="Limit number of invoices (default: all)")
):
    """Export invoices as CSV for local testing.

    Returns CSV with columns:
    - invoice_id, customer_id, customer_name, vendor_id, vendor_name
    - amount, currency, invoice_date, due_date, status, is_refund

    Data spans 2024-2025 for testing time_window queries.
    """
    generator = _get_generator(scenario_id)
    invoices = generator.get_invoices()
    customers = {c.customer_id: c.name for c in generator.get_customers()}
    vendors = {v.vendor_id: v.name for v in generator.get_vendors()}

    if limit:
        invoices = invoices[:limit]

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        "invoice_id", "customer_id", "customer_name", "vendor_id", "vendor_name",
        "amount", "currency", "invoice_date", "due_date", "status", "is_refund"
    ])

    # Data rows
    for inv in invoices:
        writer.writerow([
            inv.invoice_id,
            inv.customer_id,
            customers.get(inv.customer_id, "Unknown"),
            inv.vendor_id,
            vendors.get(inv.vendor_id, "Unknown"),
            inv.amount,
            inv.currency.value,
            inv.invoice_date[:10],  # Just the date part
            inv.due_date[:10],
            inv.status.value,
            inv.is_refund
        ])

    csv_content = output.getvalue()
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=invoices_{scenario_id}.csv"}
    )


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


@router.get("/nlq/invoices")
async def get_nlq_invoices(
    year: Optional[int] = Query(None, description="Filter by year (2024 or 2025)"),
    quarter: Optional[int] = Query(None, ge=1, le=4, description="Filter by quarter (1-4)"),
    customer_id: Optional[str] = Query(None, description="Filter by customer ID"),
    format: str = Query("json", description="Response format: json or csv")
):
    """
    Get invoice dataset for NLQ time-window query testing.
    
    Supports filtering by year, quarter, and customer.
    Returns data spanning 2024-2025 for time_window queries.
    """
    import os
    from datetime import datetime
    
    invoices_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nlq_invoices.json")
    
    try:
        with open(invoices_path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NLQ invoices dataset not found")
    
    invoices = data.get("invoices", [])
    
    # Apply filters
    if year:
        invoices = [inv for inv in invoices if inv["invoice_date"].startswith(str(year))]
    
    if quarter:
        quarter_months = {1: ["01", "02", "03"], 2: ["04", "05", "06"], 3: ["07", "08", "09"], 4: ["10", "11", "12"]}
        months = quarter_months[quarter]
        invoices = [inv for inv in invoices if inv["invoice_date"][5:7] in months]
    
    if customer_id:
        invoices = [inv for inv in invoices if inv["customer_id"] == customer_id]
    
    if format == "csv":
        from fastapi.responses import PlainTextResponse
        csv_lines = ["invoice_id,customer_id,customer_name,amount,invoice_date"]
        for inv in invoices:
            csv_lines.append(f'{inv["invoice_id"]},{inv["customer_id"]},{inv["customer_name"]},{inv["amount"]},{inv["invoice_date"]}')
        return PlainTextResponse("\n".join(csv_lines), media_type="text/csv")
    
    return {
        "metadata": data.get("metadata", {}),
        "invoices": invoices,
        "count": len(invoices),
        "ground_truth": data.get("ground_truth", {})
    }


@router.get("/nlq/invoices/ground-truth")
async def get_nlq_invoices_ground_truth(
    time_window: Optional[str] = Query(None, description="Time window: last_year, last_quarter, ytd, all_time"),
    customer_id: Optional[str] = Query(None, description="Filter by customer ID")
):
    """
    Get pre-computed ground truth for invoice queries.
    
    Useful for validating NLQ queries like:
    - "What is total revenue last year?"
    - "Who are the top customers?"
    - "What was Q3 2024 revenue?"
    """
    import os
    from datetime import datetime
    
    invoices_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "nlq_invoices.json")
    
    try:
        with open(invoices_path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NLQ invoices dataset not found")
    
    ground_truth = data.get("ground_truth", {})
    invoices = data.get("invoices", [])
    
    # Compute based on time window
    result = {
        "time_window": time_window or "all_time",
        "computed_at": datetime.utcnow().isoformat()
    }
    
    if customer_id:
        customer_data = ground_truth.get("customer_totals", {}).get(customer_id)
        if customer_data:
            result["customer"] = {
                "customer_id": customer_id,
                **customer_data
            }
        else:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        return result
    
    if time_window == "last_year" or time_window == "2024":
        result["total_revenue"] = ground_truth.get("total_revenue_2024", 0)
        result["quarters"] = {k: v for k, v in ground_truth.get("quarterly_revenue", {}).items() if k.startswith("2024")}
    elif time_window == "ytd" or time_window == "2025":
        result["total_revenue"] = ground_truth.get("total_revenue_2025", 0)
        result["quarters"] = {k: v for k, v in ground_truth.get("quarterly_revenue", {}).items() if k.startswith("2025")}
    elif time_window and time_window.startswith("Q"):
        parts = time_window.split("_")
        if len(parts) == 2:
            quarter_key = f"{parts[1]}_Q{parts[0][1]}"
            result["total_revenue"] = ground_truth.get("quarterly_revenue", {}).get(quarter_key, 0)
    else:
        result["total_revenue"] = ground_truth.get("total_revenue_all_time", 0)
        result["by_year"] = {
            "2024": ground_truth.get("total_revenue_2024", 0),
            "2025": ground_truth.get("total_revenue_2025", 0)
        }
        result["quarterly_revenue"] = ground_truth.get("quarterly_revenue", {})
    
    # Top customers
    customer_totals = ground_truth.get("customer_totals", {})
    sorted_customers = sorted(customer_totals.items(), key=lambda x: x[1]["total"], reverse=True)
    result["top_customers"] = [
        {"customer_id": cid, **cdata} for cid, cdata in sorted_customers[:5]
    ]
    
    return result


class FabricConfigRequest(BaseModel):
    industry: IndustryVertical = IndustryVertical.DEFAULT
    seed: Optional[int] = None


class FabricVendorWeight(BaseModel):
    vendor: str
    weight: float


class FabricPlaneWeights(BaseModel):
    plane_type: str
    vendors: list[FabricVendorWeight]


class IndustryWeightsResponse(BaseModel):
    industry: str
    industry_profile: dict
    planes: list[FabricPlaneWeights]


class FabricConfigResponse(BaseModel):
    industry: str
    seed: Optional[int]
    config: dict


@fabric_router.get("/industries")
async def list_industries():
    """List all available industry verticals with their profiles."""
    industries = []
    for industry in IndustryVertical:
        profile = IndustryProfile.for_industry(industry)
        industries.append({
            "id": industry.value,
            "name": profile.name,
            "description": profile.description,
            "primary_cloud": profile.primary_cloud,
            "compliance_focus": profile.compliance_focus,
            "typical_scale": profile.typical_scale,
        })
    return {"industries": industries}


@fabric_router.get("/weights/{industry}")
async def get_industry_weights(
    industry: IndustryVertical = Path(..., description="Industry vertical")
) -> IndustryWeightsResponse:
    """Get the weighted vendor distribution for an industry."""
    weights = INDUSTRY_VENDOR_WEIGHTS.get(industry, INDUSTRY_VENDOR_WEIGHTS[IndustryVertical.DEFAULT])
    profile = IndustryProfile.for_industry(industry)
    
    planes = []
    for plane_type in FabricPlaneType:
        plane_weights = weights.get(plane_type, {})
        vendors = [
            FabricVendorWeight(vendor=v, weight=w)
            for v, w in sorted(plane_weights.items(), key=lambda x: -x[1])
        ]
        planes.append(FabricPlaneWeights(
            plane_type=plane_type.value,
            vendors=vendors
        ))
    
    return IndustryWeightsResponse(
        industry=industry.value,
        industry_profile={
            "name": profile.name,
            "description": profile.description,
            "primary_cloud": profile.primary_cloud,
            "compliance_focus": profile.compliance_focus,
            "typical_scale": profile.typical_scale,
        },
        planes=planes
    )


@fabric_router.post("/generate")
async def generate_fabric(request: FabricConfigRequest) -> FabricConfigResponse:
    """
    Generate a fabric plane configuration for an enterprise.
    
    Uses industry-specific weighted vendor selection based on 2025/2026 market data.
    Deterministic when seed is provided - same seed always produces identical config.
    """
    config = generate_fabric_config(industry=request.industry, seed=request.seed)
    
    config_dict = {}
    for plane_type, plane_config in config.items():
        config_dict[plane_type.value] = {
            "vendor": plane_config.vendor,
            "endpoint": plane_config.endpoint,
            "is_healthy": plane_config.is_healthy,
            "latency_ms": plane_config.latency_ms,
        }
    
    return FabricConfigResponse(
        industry=request.industry.value,
        seed=request.seed,
        config=config_dict
    )


@fabric_router.get("/weights-matrix")
async def get_weights_matrix():
    """
    Get the complete vendor weight matrix across all industries.
    
    Returns a matrix showing how vendor selection probability varies
    by industry vertical for each fabric plane type.
    """
    matrix = {}
    
    for industry in IndustryVertical:
        weights = INDUSTRY_VENDOR_WEIGHTS.get(industry, {})
        industry_data = {}
        
        for plane_type in FabricPlaneType:
            plane_weights = weights.get(plane_type, {})
            industry_data[plane_type.value] = {
                v: round(w, 2) for v, w in 
                sorted(plane_weights.items(), key=lambda x: -x[1])
            }
        
        matrix[industry.value] = industry_data
    
    return {
        "description": "Vendor selection weights by industry and fabric plane (2025/2026 market data)",
        "matrix": matrix,
        "planes": [p.value for p in FabricPlaneType],
        "industries": [i.value for i in IndustryVertical],
    }
