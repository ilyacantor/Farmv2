"""
API routes for business data generation and ground truth verification.

Exposes endpoints to trigger business data generation, push to DCL,
and retrieve ground truth manifests for verification.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.generators.business_data_orchestrator import (
    BusinessDataOrchestrator,
    TIER_1_GENERATORS,
    TIER_2_GENERATORS,
    TIER_3_GENERATORS,
)
from src.farm.db import save_ground_truth_manifest, load_ground_truth_manifest, list_ground_truth_runs, update_manifest_push_results

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
    await _store_run(run_id, orchestrator)

    # Collect any generation errors (generators that threw exceptions)
    generation_errors = {}
    for sys_name, pipes in orchestrator.get_payloads().items():
        if "_error" in pipes:
            generation_errors[sys_name] = pipes["_error"]

    # Optionally push to DCL
    push_results = []
    if request.push_to_dcl:
        push_results = await orchestrator.push_to_dcl()
        if push_results:
            try:
                await update_manifest_push_results(run_id, push_results)
                logger.info(f"DCL push results persisted for run {run_id}")
            except Exception as e:
                logger.error(f"Failed to persist push results for {run_id}: {e}")

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
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found in memory or database",
            )
        return JSONResponse(content=db_data["manifest"])

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
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
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
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
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
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
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
    """List all stored generation runs, merging in-memory and DB."""
    runs = []
    seen_ids = set()
    for run_id, data in _run_store.items():
        manifest = data.get("manifest", {})
        runs.append({
            "run_id": run_id,
            "generated_at": manifest.get("generated_at"),
            "source_systems": manifest.get("source_systems", []),
            "record_counts": manifest.get("record_counts", {}),
        })
        seen_ids.add(run_id)

    try:
        db_runs = await list_ground_truth_runs()
        for db_run in db_runs:
            if db_run["run_id"] not in seen_ids:
                runs.append({
                    "run_id": db_run["run_id"],
                    "generated_at": db_run["created_at"],
                    "source_systems": db_run["source_systems"],
                    "record_counts": db_run["record_counts"],
                })
    except Exception as e:
        logger.warning(f"Could not load runs from DB: {e}")

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


class VerifyRequest(BaseModel):
    """Request body for ground truth verification."""
    quarter: str = Field(..., description="Quarter to verify, e.g. '2024-Q1'")
    actuals: Dict[str, Any] = Field(..., description="Actual metric values to compare against ground truth")
    source: str = Field(default="manual", description="Source of actuals: 'manual', 'dcl_readback', etc.")


class MetricResult(BaseModel):
    metric: str
    expected: Any
    actual: Any
    delta: Optional[float] = None
    delta_pct: Optional[float] = None
    accuracy: Optional[float] = None
    unit: Optional[str] = None
    status: str


class VerifyResponse(BaseModel):
    run_id: str
    quarter: str
    source: str
    overall_accuracy: float
    metric_count: int
    pass_count: int
    warn_count: int
    fail_count: int
    missing_count: int
    results: List[Dict[str, Any]]
    verdict: str


@router.post("/verify/{run_id}")
async def verify_ground_truth(run_id: str, request: VerifyRequest):
    """
    Verify actual values against the persisted ground truth manifest.
    
    This is the server-side scoring engine for Path 4 (Farm ↔ DCL) verification.
    Computes per-metric accuracy and returns a structured verdict.
    """
    run_data = _run_store.get(run_id)
    if run_data:
        manifest = run_data.get("manifest", {})
    else:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.error(f"DB lookup failed for verification of run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "RUN_NOT_FOUND",
                    "message": f"Run {run_id} not found in memory or database",
                    "run_id": run_id,
                },
            )
        manifest = db_data["manifest"]

    ground_truth = manifest.get("ground_truth", {})
    quarter_data = ground_truth.get(request.quarter)
    if not quarter_data or not isinstance(quarter_data, dict):
        available = [k for k in ground_truth.keys() if k.startswith("20")]
        raise HTTPException(
            status_code=404,
            detail={
                "error": "QUARTER_NOT_FOUND",
                "message": f"Quarter '{request.quarter}' not found in ground truth",
                "available_quarters": available,
            },
        )

    results = []
    total_accuracy = 0.0
    metric_count = 0
    pass_count = 0
    warn_count = 0
    fail_count = 0
    missing_count = 0

    for metric_name, metric_data in quarter_data.items():
        if not isinstance(metric_data, dict) or "value" not in metric_data:
            continue

        expected = metric_data["value"]
        unit = metric_data.get("unit", "unknown")
        actual = request.actuals.get(metric_name)

        if actual is None:
            results.append({
                "metric": metric_name,
                "expected": expected,
                "actual": None,
                "delta": None,
                "delta_pct": None,
                "accuracy": None,
                "unit": unit,
                "status": "missing",
            })
            missing_count += 1
            continue

        try:
            actual_num = float(actual)
            expected_num = float(expected)
        except (ValueError, TypeError):
            results.append({
                "metric": metric_name,
                "expected": expected,
                "actual": actual,
                "delta": None,
                "delta_pct": None,
                "accuracy": None,
                "unit": unit,
                "status": "fail",
                "error": "non_numeric_comparison",
            })
            fail_count += 1
            continue

        delta = actual_num - expected_num
        if expected_num != 0:
            delta_pct = abs(delta) / abs(expected_num) * 100
        else:
            delta_pct = 0.0 if actual_num == 0 else 100.0

        accuracy = max(0.0, 100.0 - delta_pct)

        if accuracy >= 95:
            status = "pass"
            pass_count += 1
        elif accuracy >= 85:
            status = "warn"
            warn_count += 1
        else:
            status = "fail"
            fail_count += 1

        total_accuracy += accuracy
        metric_count += 1

        results.append({
            "metric": metric_name,
            "expected": expected_num,
            "actual": actual_num,
            "delta": round(delta, 4),
            "delta_pct": round(delta_pct, 2),
            "accuracy": round(accuracy, 2),
            "unit": unit,
            "status": status,
        })

    overall_accuracy = total_accuracy / metric_count if metric_count > 0 else 0.0

    if overall_accuracy >= 95:
        verdict = "PASS"
    elif overall_accuracy >= 85:
        verdict = "DEGRADED"
    else:
        verdict = "FAIL"

    return JSONResponse(content={
        "run_id": run_id,
        "quarter": request.quarter,
        "source": request.source,
        "overall_accuracy": round(overall_accuracy, 2),
        "metric_count": metric_count,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "missing_count": missing_count,
        "verdict": verdict,
        "results": sorted(results, key=lambda r: (r["status"] != "fail", r["status"] != "warn", r["status"] != "missing", r["metric"])),
    })


@router.post("/verify/{run_id}/dcl-readback")
async def verify_dcl_readback(run_id: str, quarter: Optional[str] = Query(None)):
    """
    Trigger automatic DCL readback and verification.
    
    Reads back unified data from DCL using the stored push correlation keys,
    then scores against the ground truth manifest.
    
    Requires: DCL readback endpoint contract (not yet available).
    """
    run_data = _run_store.get(run_id)
    manifest = None
    push_results = None

    if run_data:
        manifest = run_data.get("manifest", {})

    try:
        db_data = await load_ground_truth_manifest(run_id)
        if db_data:
            if not manifest:
                manifest = db_data["manifest"]
    except Exception as e:
        logger.warning(f"DB lookup for DCL readback failed: {e}")

    if not manifest:
        raise HTTPException(
            status_code=404,
            detail={"error": "RUN_NOT_FOUND", "message": f"Run {run_id} not found"},
        )

    raise HTTPException(
        status_code=501,
        detail={
            "error": "DCL_READBACK_NOT_IMPLEMENTED",
            "message": "DCL readback endpoint contract not yet configured. Use POST /verify/{run_id} with manual actuals, or provide the DCL readback contract.",
            "run_id": run_id,
            "manifest_available": bool(manifest),
        },
    )


async def _store_run(run_id: str, orchestrator: BusinessDataOrchestrator):
    """Store run data in memory and persist manifest to DB."""
    manifest = orchestrator.get_manifest()
    _run_store[run_id] = {
        "orchestrator": orchestrator,
        "manifest": manifest,
    }
    while len(_run_store) > _MAX_STORED_RUNS:
        oldest = next(iter(_run_store))
        del _run_store[oldest]

    if manifest:
        try:
            await save_ground_truth_manifest(
                run_id=run_id,
                seed=orchestrator.seed,
                created_at=manifest.get("generated_at", ""),
                manifest=manifest,
                source_systems=manifest.get("source_systems", []),
                record_counts=manifest.get("record_counts", {}),
            )
            logger.info(f"Ground truth manifest persisted to DB for run {run_id}")
        except Exception as e:
            logger.error(f"Failed to persist ground truth manifest for run {run_id}: {e}")
