"""
API routes for business data ground truth verification and NLQ dashboard.

Exposes endpoints to retrieve ground truth manifests, verify actuals against
ground truth, and list manifest-driven runs for the NLQ dashboard.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.farm.db import load_ground_truth_manifest, list_ground_truth_runs, update_manifest_push_results

logger = logging.getLogger("farm.api.business_data")

router = APIRouter(prefix="/api/business-data", tags=["business-data"])


@router.get("/ground-truth/{run_id}")
async def get_ground_truth(run_id: str):
    """
    Retrieve the ground truth manifest for a specific generation run.

    DCL and test harnesses use this to verify their unified output against
    expected values.
    """
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return JSONResponse(content=db_data["manifest"])


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
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    manifest = db_data["manifest"]
    ground_truth = manifest.get("ground_truth", {})

    if quarter:
        qt = ground_truth.get(quarter, {})
        if metric not in qt:
            raise HTTPException(
                status_code=404,
                detail=f"Metric '{metric}' not found in {quarter}",
            )
        return JSONResponse(content={"quarter": quarter, "metric": metric, **qt[metric]})

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
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    manifest = db_data["manifest"]
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
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    manifest = db_data["manifest"]
    conflicts = manifest.get("ground_truth", {}).get("expected_conflicts", [])

    return JSONResponse(content={"conflicts": conflicts, "count": len(conflicts)})


@router.get("/runs")
async def list_runs():
    """List all stored generation runs from the database."""
    try:
        db_runs = await list_ground_truth_runs()
    except Exception as e:
        logger.error(f"Failed to list runs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    runs = [
        {
            "run_id": db_run["run_id"],
            "generated_at": db_run["created_at"],
            "source_systems": db_run["source_systems"],
            "record_counts": db_run["record_counts"],
        }
        for db_run in db_runs
    ]
    return JSONResponse(content={"runs": runs})


@router.get("/manifest-runs")
async def list_manifest_runs(limit: int = Query(50, ge=1, le=200)):
    """List recent manifest-driven NLQ runs for the dashboard."""
    try:
        runs = await list_ground_truth_runs(limit)
    except Exception as e:
        logger.error(f"Failed to list runs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    return JSONResponse(content={"runs": runs, "count": len(runs)})


@router.get("/manifest-runs/{run_id}")
async def get_manifest_run_detail(run_id: str):
    """Get full detail for a manifest-driven run including provenance and push results."""
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return JSONResponse(content=db_data)


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
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.error(f"DB lookup failed for verification of run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    if not db_data:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "RUN_NOT_FOUND",
                "message": f"Run {run_id} not found in database",
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
    try:
        db_data = await load_ground_truth_manifest(run_id)
    except Exception as e:
        logger.warning(f"DB lookup for DCL readback failed: {e}")
        db_data = None

    manifest = db_data["manifest"] if db_data else None

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
