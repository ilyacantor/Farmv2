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
    manifest_version: str = "2.0"
    active_systems: list
    record_counts: dict
    quarters_covered: list
    manifest_valid: bool
    manifest_errors: list = []
    generation_errors: dict = {}
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

    # Collect any generation errors (generators that threw exceptions)
    generation_errors = {}
    for sys_name, pipes in orchestrator.get_payloads().items():
        if "_error" in pipes:
            generation_errors[sys_name] = pipes["_error"]

    # Optionally push to DCL
    push_results = []
    if request.push_to_dcl:
        push_results = await orchestrator.push_to_dcl()

    manifest = orchestrator.get_manifest() or {}

    return GenerateResponse(
        run_id=run_id,
        status="completed",
        manifest_version=manifest.get("manifest_version", "1.0"),
        active_systems=summary["active_systems"],
        record_counts=summary["record_counts"],
        quarters_covered=summary["quarters_covered"],
        manifest_valid=summary["manifest_valid"],
        manifest_errors=summary["manifest_errors"],
        generation_errors=generation_errors,
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
    When a financial model was used, includes full P&L, BS, CF, and SaaS metrics.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    orchestrator = run_data.get("orchestrator")
    if not orchestrator or not orchestrator.profile:
        raise HTTPException(status_code=404, detail="Profile data not available")

    # Use financial model quarters if available (richer data)
    if orchestrator.model_quarters:
        quarters = []
        for fmq in orchestrator.model_quarters:
            quarters.append({
                "quarter": fmq.quarter,
                "is_forecast": fmq.is_forecast,
                # ARR Waterfall
                "beginning_arr": round(fmq.beginning_arr, 2),
                "new_arr": round(fmq.new_arr, 2),
                "new_logo_arr": round(fmq.new_logo_arr, 2),
                "expansion_arr": round(fmq.expansion_arr, 2),
                "churned_arr": round(fmq.churned_arr, 2),
                "ending_arr": round(fmq.ending_arr, 2),
                "mrr": round(fmq.mrr, 4),
                # Revenue
                "revenue": round(fmq.revenue, 2),
                "new_logo_revenue": round(fmq.new_logo_revenue, 2),
                "expansion_revenue": round(fmq.expansion_revenue, 2),
                "renewal_revenue": round(fmq.renewal_revenue, 2),
                # P&L
                "cogs": round(fmq.cogs, 2),
                "gross_profit": round(fmq.gross_profit, 2),
                "gross_margin_pct": round(fmq.gross_margin_pct, 1),
                "sm_expense": round(fmq.sm_expense, 2),
                "rd_expense": round(fmq.rd_expense, 2),
                "ga_expense": round(fmq.ga_expense, 2),
                "total_opex": round(fmq.total_opex, 2),
                "ebitda": round(fmq.ebitda, 2),
                "ebitda_margin_pct": round(fmq.ebitda_margin_pct, 1),
                "net_income": round(fmq.net_income, 2),
                "net_margin_pct": round(fmq.net_margin_pct, 1),
                # Balance Sheet
                "cash": round(fmq.cash, 2),
                "ar": round(fmq.ar, 2),
                "total_assets": round(fmq.total_assets, 2),
                "deferred_revenue": round(fmq.deferred_revenue, 2),
                "total_liabilities": round(fmq.total_liabilities, 2),
                "stockholders_equity": round(fmq.stockholders_equity, 2),
                # Cash Flow
                "cfo": round(fmq.cfo, 2),
                "fcf": round(fmq.fcf, 2),
                # SaaS Metrics
                "nrr": round(fmq.nrr, 1),
                "gross_churn_pct": round(fmq.gross_churn_pct, 1),
                "ltv_cac_ratio": round(fmq.ltv_cac_ratio, 1),
                "magic_number": round(fmq.magic_number, 2),
                "burn_multiple": round(fmq.burn_multiple, 2),
                "rule_of_40": round(fmq.rule_of_40, 1),
                # Pipeline
                "pipeline": round(fmq.pipeline, 2),
                "win_rate": round(fmq.win_rate, 1),
                "avg_deal_size": round(fmq.avg_deal_size, 4),
                # Customer & People
                "customer_count": fmq.customer_count,
                "new_customers": fmq.new_customers,
                "churned_customers": fmq.churned_customers,
                "headcount": fmq.headcount,
                "hires": fmq.hires,
                "terminations": fmq.terminations,
                "attrition_rate": round(fmq.attrition_rate, 1),
                # Support & Engineering
                "support_tickets": fmq.support_tickets,
                "csat": round(fmq.csat, 2),
                "sprint_velocity": round(fmq.sprint_velocity, 1),
                "features_shipped": fmq.features_shipped,
                # Infrastructure
                "cloud_spend": round(fmq.cloud_spend, 2),
                "p1_incidents": fmq.p1_incidents,
                "p2_incidents": fmq.p2_incidents,
                "uptime_pct": round(fmq.uptime_pct, 2),
            })

        return JSONResponse(content={
            "run_id": run_id,
            "model_version": "2.0",
            "seed": orchestrator.seed,
            "quarters": quarters,
        })

    # Fallback: legacy BusinessProfile data
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
        "model_version": "1.0",
        "seed": profile.seed,
        "base_revenue": profile.base_revenue,
        "yoy_growth_rate": profile.yoy_growth_rate,
        "quarters": quarters,
    })


@router.get("/dcl-status")
async def dcl_status():
    """
    Check DCL connectivity status.

    Returns whether DCL_INGEST_URL is configured and reachable.
    """
    import httpx

    dcl_url = os.getenv("DCL_INGEST_URL", "")
    if not dcl_url:
        return JSONResponse(content={
            "connected": False,
            "status": "not_configured",
            "message": "DCL_INGEST_URL not set",
            "url": None,
        })

    base_url = dcl_url.rstrip("/")
    health_base = os.getenv("DCL_HEALTH_URL", "")
    if not health_base:
        health_base = base_url.split("/api/dcl")[0] if "/api/dcl" in base_url else base_url
    health_url = health_base.rstrip("/") + "/health"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(health_url)
            if 200 <= resp.status_code < 300:
                return JSONResponse(content={
                    "connected": True,
                    "status": "connected",
                    "message": f"DCL reachable (HTTP {resp.status_code})",
                    "url": base_url,
                })
            elif resp.status_code in (401, 403):
                return JSONResponse(content={
                    "connected": False,
                    "status": "auth_error",
                    "message": f"DCL requires authentication (HTTP {resp.status_code})",
                    "url": base_url,
                })
            else:
                return JSONResponse(content={
                    "connected": False,
                    "status": "error",
                    "message": f"DCL returned HTTP {resp.status_code}",
                    "url": base_url,
                })
    except httpx.TimeoutException:
        return JSONResponse(content={
            "connected": False,
            "status": "timeout",
            "message": "DCL connection timed out",
            "url": base_url,
        })
    except httpx.ConnectError:
        return JSONResponse(content={
            "connected": False,
            "status": "unreachable",
            "message": "Cannot connect to DCL (connection refused)",
            "url": base_url,
        })
    except Exception as e:
        return JSONResponse(content={
            "connected": False,
            "status": "unreachable",
            "message": f"Cannot reach DCL: {type(e).__name__}",
            "url": base_url,
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
