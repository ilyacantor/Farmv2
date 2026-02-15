"""
API routes for business data generation and ground truth verification.

Exposes endpoints to trigger business data generation, push to DCL,
and retrieve ground truth manifests for verification.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.generators.business_data_orchestrator import (
    BusinessDataOrchestrator,
    TIER_1_GENERATORS,
    TIER_2_GENERATORS,
    TIER_3_GENERATORS,
)

logger = logging.getLogger("farm.api.business_data")

router = APIRouter(prefix="/api/business-data", tags=["business-data"])

# In-memory store for recent runs (production would use DB)
_run_store: Dict[str, Dict[str, Any]] = {}
_MAX_STORED_RUNS = 10


class GenerateRequest(BaseModel):
    """Request body for business data generation."""
    seed: int = Field(default=42, description="Random seed for deterministic generation")
    tiers: str = Field(
        default="1,2,3",
        description="Comma-separated tier numbers to generate (1=Salesforce/NetSuite/Chargebee, 2=Workday/Zendesk, 3=Jira/Datadog/AWS)",
    )
    base_revenue: float = Field(default=22.0, description="Base quarterly revenue in millions USD")
    growth_rate: float = Field(default=0.15, description="Year-over-year growth rate (0.15 = 15%)")
    num_quarters: int = Field(default=12, ge=1, le=20, description="Number of quarters to generate")
    push_to_dcl: bool = Field(default=False, description="Whether to push generated data to DCL")


class GenerateResponse(BaseModel):
    """Response from business data generation."""
    run_id: str
    status: str
    active_systems: list
    record_counts: dict
    quarters_covered: list
    manifest_valid: bool
    manifest_errors: list = []
    push_results: list = []


@router.post("/generate", response_model=GenerateResponse)
async def generate_business_data(request: GenerateRequest):
    """
    Generate business data for all configured source systems.

    This creates realistic CRM, ERP, Billing, HCM, Support, PM, Monitoring,
    and Cloud Cost data, computes a ground truth manifest, and optionally
    pushes to DCL for ingestion.
    """
    if not os.getenv("BUSINESS_DATA_ENABLED", "false").lower() in ("true", "1", "yes"):
        # Allow generation even without env var, just log a warning
        logger.warning("BUSINESS_DATA_ENABLED not set, proceeding anyway")

    # Parse tiers
    tier_nums = [t.strip() for t in request.tiers.split(",")]
    active = []
    if "1" in tier_nums:
        active.extend(TIER_1_GENERATORS)
    if "2" in tier_nums:
        active.extend(TIER_2_GENERATORS)
    if "3" in tier_nums:
        active.extend(TIER_3_GENERATORS)

    orchestrator = BusinessDataOrchestrator(
        seed=request.seed,
        tiers=active,
        base_revenue=request.base_revenue,
        growth_rate=request.growth_rate,
        num_quarters=request.num_quarters,
    )

    # Generate data
    summary = orchestrator.generate_all()

    # Store run data
    run_id = summary["run_id"]
    _store_run(run_id, orchestrator)

    # Optionally push to DCL
    push_results = []
    if request.push_to_dcl:
        push_results = await orchestrator.push_to_dcl()

    return GenerateResponse(
        run_id=run_id,
        status="completed",
        active_systems=summary["active_systems"],
        record_counts=summary["record_counts"],
        quarters_covered=summary["quarters_covered"],
        manifest_valid=summary["manifest_valid"],
        manifest_errors=summary["manifest_errors"],
        push_results=push_results,
    )


@router.get("/ground-truth/{run_id}")
async def get_ground_truth(run_id: str):
    """
    Retrieve the ground truth manifest for a specific generation run.

    DCL and test harnesses use this to verify their unified output against
    expected values.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(
            status_code=404,
            detail=f"Run {run_id} not found. Available runs: {list(_run_store.keys())}",
        )

    manifest = run_data.get("manifest")
    if not manifest:
        raise HTTPException(status_code=404, detail=f"No manifest for run {run_id}")

    return JSONResponse(content=manifest)


@router.get("/ground-truth/{run_id}/metric/{metric}")
async def get_ground_truth_metric(
    run_id: str,
    metric: str,
    quarter: Optional[str] = Query(None, description="Specific quarter like '2024-Q1'"),
):
    """
    Retrieve a specific metric from the ground truth manifest.

    Useful for targeted verification queries.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    manifest = run_data.get("manifest", {})
    ground_truth = manifest.get("ground_truth", {})

    if quarter:
        qt = ground_truth.get(quarter, {})
        if metric not in qt:
            raise HTTPException(
                status_code=404,
                detail=f"Metric '{metric}' not found in {quarter}",
            )
        return JSONResponse(content={"quarter": quarter, "metric": metric, **qt[metric]})

    # Return metric across all quarters
    results = {}
    for q_label, q_data in ground_truth.items():
        if isinstance(q_data, dict) and metric in q_data:
            results[q_label] = q_data[metric]

    if not results:
        raise HTTPException(
            status_code=404, detail=f"Metric '{metric}' not found in any quarter"
        )

    return JSONResponse(content={"metric": metric, "quarters": results})


@router.get("/ground-truth/{run_id}/dimensional/{dimension}")
async def get_dimensional_truth(run_id: str, dimension: str):
    """
    Retrieve a dimensional breakdown from the ground truth.

    Available dimensions: revenue_by_region, pipeline_by_stage, headcount_by_department
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    manifest = run_data.get("manifest", {})
    dimensional = manifest.get("ground_truth", {}).get("dimensional_truth", {})

    if dimension not in dimensional:
        raise HTTPException(
            status_code=404,
            detail=f"Dimension '{dimension}' not found. Available: {list(dimensional.keys())}",
        )

    return JSONResponse(content={"dimension": dimension, "data": dimensional[dimension]})


@router.get("/ground-truth/{run_id}/conflicts")
async def get_expected_conflicts(run_id: str):
    """
    Retrieve expected cross-system conflicts from the ground truth.

    DCL should detect these conflicts and flag them with matching root causes.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    manifest = run_data.get("manifest", {})
    conflicts = manifest.get("ground_truth", {}).get("expected_conflicts", [])

    return JSONResponse(content={"conflicts": conflicts, "count": len(conflicts)})


@router.get("/payload/{run_id}/{pipe_id}")
async def get_pipe_payload(run_id: str, pipe_id: str):
    """
    Retrieve a specific pipe's generated payload.

    Useful for debugging and inspection.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    orchestrator = run_data.get("orchestrator")
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Orchestrator data not available")

    payload = orchestrator.get_payload_for_pipe(pipe_id)
    if not payload:
        # List available pipes
        available = []
        for sys_name, pipes in orchestrator.get_payloads().items():
            for pipe_name, p in pipes.items():
                if isinstance(p, dict) and "meta" in p:
                    available.append(p["meta"].get("pipe_id", f"{sys_name}_{pipe_name}"))
        raise HTTPException(
            status_code=404,
            detail=f"Pipe '{pipe_id}' not found. Available: {available}",
        )

    return JSONResponse(content=payload)


@router.get("/runs")
async def list_runs():
    """List all stored generation runs."""
    runs = []
    for run_id, data in _run_store.items():
        manifest = data.get("manifest", {})
        runs.append({
            "run_id": run_id,
            "generated_at": manifest.get("generated_at"),
            "source_systems": manifest.get("source_systems", []),
            "record_counts": manifest.get("record_counts", {}),
        })
    return JSONResponse(content={"runs": runs})


@router.get("/profile/{run_id}")
async def get_business_profile(run_id: str):
    """
    Retrieve the business profile (truth spine) for a generation run.

    Shows the quarterly metrics trajectory that all generators derive from.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    orchestrator = run_data.get("orchestrator")
    if not orchestrator or not orchestrator.profile:
        raise HTTPException(status_code=404, detail="Profile data not available")

    profile = orchestrator.profile
    quarters = []
    for qm in profile.quarters:
        quarters.append({
            "quarter": qm.quarter,
            "is_forecast": qm.is_forecast,
            "revenue": qm.revenue,
            "arr": qm.arr,
            "mrr": qm.mrr,
            "pipeline": qm.pipeline,
            "win_rate": qm.win_rate,
            "customer_count": qm.customer_count,
            "headcount": qm.headcount,
            "nrr": qm.nrr,
            "gross_churn_pct": qm.gross_churn_pct,
            "gross_margin_pct": qm.gross_margin_pct,
            "support_tickets": qm.support_tickets,
            "csat": qm.csat,
            "sprint_velocity": qm.sprint_velocity,
            "cloud_spend": qm.cloud_spend,
            "incident_count": qm.incident_count,
        })

    return JSONResponse(content={
        "run_id": run_id,
        "seed": profile.seed,
        "base_revenue": profile.base_revenue,
        "yoy_growth_rate": profile.yoy_growth_rate,
        "quarters": quarters,
    })


def _store_run(run_id: str, orchestrator: BusinessDataOrchestrator):
    """Store run data for later retrieval. Evicts oldest if over limit."""
    _run_store[run_id] = {
        "orchestrator": orchestrator,
        "manifest": orchestrator.get_manifest(),
    }
    # Evict oldest runs if over limit
    while len(_run_store) > _MAX_STORED_RUNS:
        oldest = next(iter(_run_store))
        del _run_store[oldest]
