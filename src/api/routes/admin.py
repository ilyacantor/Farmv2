"""
Admin and diagnostic routes.

Endpoints:
- GET  /api/_diagnostics/blob-stats        Blob fetch statistics
- GET  /api/_diagnostics/storage-stats     Storage statistics
- GET  /api/audit/grading                  Run grading audit
- POST /api/audit/gradeability             Validate AOD response
- GET  /api/audit/gradeability/demo-failure Demo gradeability failures
- GET  /api/aod/run-status                 Check AOD run status
- GET  /api/jobs/{job_id}                  Get job status
- GET  /api/jobs                           List jobs
- POST /api/admin/migrate-stale-analyses   Clear stale analyses
- GET  /api/admin/analysis-version-stats   Analysis version statistics
"""
import json
import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from src.farm.db import connection as db_connection
from src.farm.jobs import JobStatus, job_manager
from src.farm.snapshot_utils import get_blob_fetch_count
from src.generators.enterprise import load_mock_policy_config
from src.models.planes import AODRunStatusEnum, AODRunStatusResponse
from src.services.aod_client import fetch_policy_config
from src.services.constants import CURRENT_ANALYSIS_VERSION
from src.services.grading_audit import audit_gradeability, run_full_audit

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.get("/api/_diagnostics/blob-stats")
async def get_blob_stats():
    """Return blob fetch statistics for monitoring. Lower blob_fetch_count is better."""
    return {
        "blob_fetch_count": get_blob_fetch_count(),
        "message": "This counter tracks blob fetches since server start. Normal UI flows should NOT increment this."
    }


@router.get("/api/_diagnostics/storage-stats")
async def get_storage_stats():
    """Return storage statistics comparing legacy vs new tables."""
    async with db_connection() as conn:
        legacy_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        meta_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots_meta")
        blob_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots_blob")
        cache_count = await conn.fetchval("SELECT COUNT(*) FROM reconciliation_analysis_cache")

        pending_backfill = await conn.fetchval("""
            SELECT COUNT(*) FROM snapshots s
            LEFT JOIN snapshots_meta m ON s.snapshot_id = m.snapshot_id
            WHERE m.snapshot_id IS NULL
        """)

        return {
            "legacy_snapshots_count": legacy_count,
            "snapshots_meta_count": meta_count,
            "snapshots_blob_count": blob_count,
            "reconciliation_cache_count": cache_count,
            "pending_backfill": pending_backfill,
            "backfill_complete": pending_backfill == 0,
        }


class AuditRequest(BaseModel):
    snapshot_id: str
    n_runs: int = 10
    finance_target_keys: Optional[list] = None
    activity_window_days: int = 90


@router.get("/api/audit/grading")
async def audit_grading(
    snapshot_id: str = Query(..., description="Snapshot ID to audit"),
    n_runs: int = Query(10, description="Number of determinism runs"),
    activity_window_days: int = Query(90, description="Activity window in days"),
):
    """Run the complete grading correctness audit suite on a snapshot.

    Audits:
    - Determinism: N runs produce identical results
    - Consistency: No contradictory flags, all implications hold
    - Finance traceability: HAS_ONGOING_FINANCE has evidence refs
    - Activity invariants: Timestamps coherent with classification

    Returns PASS, INVALID_SNAPSHOT, UPSTREAM_ERROR, or INVALID_INPUT_CONTRACT.
    """
    async with db_connection() as conn:
        row = await conn.fetchrow(
            "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
            snapshot_id
        )

        if not row:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")

        snapshot = json.loads(row["snapshot_json"])

    policy = None
    try:
        policy = await fetch_policy_config()
    except Exception:
        policy = load_mock_policy_config()

    report = await run_in_threadpool(
        run_full_audit,
        snapshot=snapshot,
        snapshot_id=snapshot_id,
        n_runs=n_runs,
        activity_window_days=activity_window_days,
        policy=policy,
    )

    return report.to_dict()


@router.post("/api/audit/gradeability")
async def audit_gradeability_endpoint(aod_response: dict):
    """Validate AOD response for grading requirements.

    Checks:
    - Response is JSON (not HTML)
    - Has required fields: shadows, zombies, actual_reason_codes

    Returns contract_status: PASS, UPSTREAM_ERROR, or INVALID_INPUT_CONTRACT.
    """
    result = audit_gradeability(aod_response)
    return result


@router.get("/api/audit/gradeability/demo-failure")
async def audit_gradeability_demo_failure(mode: str = Query("html", description="Failure mode: html, missing_fields, null")):
    """Demo endpoint to demonstrate gradeability enforcement failures.

    Modes:
    - html: Simulates AOD returning HTML instead of JSON
    - missing_fields: Simulates AOD missing required fields
    - null: Simulates null AOD response
    """
    if mode == "html":
        fake_response = "<!DOCTYPE html><html><head><title>Error</title></head><body>503 Service Unavailable</body></html>"
    elif mode == "missing_fields":
        fake_response = {"some_field": "value", "other_field": []}
    elif mode == "null":
        fake_response = None
    else:
        fake_response = {"invalid": True}

    result = audit_gradeability(fake_response)
    return {
        "demo_mode": mode,
        "simulated_response_type": type(fake_response).__name__ if fake_response else "None",
        "audit_result": result,
    }


@router.get("/api/aod/run-status", response_model=AODRunStatusResponse)
async def check_aod_run_status(
    snapshot_id: str = Query(..., description="Snapshot ID to check"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Check if AOD has processed a snapshot."""
    aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")

    if not aod_url:
        return AODRunStatusResponse(
            status=AODRunStatusEnum.AOD_ERROR,
            message="AOD_URL or AOD_BASE_URL not configured"
        )

    headers = {}
    if aod_secret:
        headers["X-API-Key"] = aod_secret

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            status_url = f"{aod_url}/api/runs/latest"
            params = {"snapshot_id": snapshot_id, "tenant_id": tenant_id}
            resp = await client.get(status_url, params=params, headers=headers)

            if resp.status_code == 404:
                return AODRunStatusResponse(status=AODRunStatusEnum.NOT_PROCESSED)
            elif resp.status_code == 200:
                data = resp.json()
                run_id = data.get("run_id")
                if run_id:
                    return AODRunStatusResponse(
                        status=AODRunStatusEnum.PROCESSED,
                        run_id=run_id
                    )
                else:
                    return AODRunStatusResponse(status=AODRunStatusEnum.NOT_PROCESSED)
            else:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.AOD_ERROR,
                    message=f"AOD returned {resp.status_code}"
                )

        except httpx.RequestError as e:
            return AODRunStatusResponse(
                status=AODRunStatusEnum.AOD_ERROR,
                message=f"Could not reach AOD: {str(e)}"
            )


@router.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get status and progress of a background job."""
    job = await job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@router.get("/api/jobs")
async def list_jobs(
    limit: int = Query(default=50, le=100),
    status: Optional[str] = Query(default=None),
):
    """List recent background jobs."""
    job_status = JobStatus(status) if status else None
    jobs = await job_manager.list_jobs(limit=limit, status=job_status)
    return [job.to_dict() for job in jobs]


@router.post("/api/admin/migrate-stale-analyses")
async def migrate_stale_analyses():
    """Clear stale cached analyses so they auto-recompute on next access.

    Clears analysis_json where:
    - analysis_version IS NULL (never versioned)
    - analysis_version < CURRENT_ANALYSIS_VERSION (outdated logic)

    This migration ensures old categorizations don't resurface.
    """
    async with db_connection() as conn:
        count_row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE analysis_version IS NULL AND analysis_json IS NOT NULL) as null_version,
                COUNT(*) FILTER (WHERE analysis_version IS NOT NULL AND analysis_version < $1 AND analysis_json IS NOT NULL) as outdated_version,
                COUNT(*) FILTER (WHERE analysis_version = $1) as current_version,
                COUNT(*) as total
            FROM reconciliations
        """, CURRENT_ANALYSIS_VERSION)

        null_version = count_row["null_version"]
        outdated_version = count_row["outdated_version"]
        current_version = count_row["current_version"]
        total = count_row["total"]

        stale_count = null_version + outdated_version

        if stale_count == 0:
            return {
                "message": "No stale analyses found",
                "current_analysis_version": CURRENT_ANALYSIS_VERSION,
                "stats": {
                    "null_version": null_version,
                    "outdated_version": outdated_version,
                    "current_version": current_version,
                    "total": total,
                    "cleared": 0,
                }
            }

        await conn.execute("""
            UPDATE reconciliations
            SET analysis_json = NULL, analysis_version = NULL, analysis_computed_at = NULL
            WHERE analysis_version IS NULL OR analysis_version < $1
        """, CURRENT_ANALYSIS_VERSION)

        return {
            "message": f"Cleared {stale_count} stale analyses",
            "current_analysis_version": CURRENT_ANALYSIS_VERSION,
            "stats": {
                "null_version": null_version,
                "outdated_version": outdated_version,
                "current_version": current_version,
                "total": total,
                "cleared": stale_count,
            }
        }


@router.get("/api/admin/analysis-version-stats")
async def get_analysis_version_stats():
    """Get statistics on analysis versions across reconciliations."""
    async with db_connection() as conn:
        stats_row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE analysis_json IS NULL) as no_analysis,
                COUNT(*) FILTER (WHERE analysis_version IS NULL AND analysis_json IS NOT NULL) as unversioned,
                COUNT(*) FILTER (WHERE analysis_version IS NOT NULL AND analysis_version < $1 AND analysis_json IS NOT NULL) as outdated,
                COUNT(*) FILTER (WHERE analysis_version = $1) as current,
                COUNT(*) as total
            FROM reconciliations
        """, CURRENT_ANALYSIS_VERSION)

        return {
            "current_analysis_version": CURRENT_ANALYSIS_VERSION,
            "stats": {
                "no_analysis": stats_row["no_analysis"],
                "unversioned": stats_row["unversioned"],
                "outdated": stats_row["outdated"],
                "current": stats_row["current"],
                "total": stats_row["total"],
                "stale_requiring_recompute": stats_row["unversioned"] + stats_row["outdated"],
            }
        }


@router.get("/api/_diagnostics/dcl-status")
async def dcl_status():
    """Check DCL connectivity. Replaces the removed /api/business-data/dcl-status."""
    dcl_url = os.environ.get("DCL_INGEST_URL", "")
    if not dcl_url:
        return {
            "connected": False,
            "status": "not_configured",
            "message": "DCL_INGEST_URL not set",
            "url": None,
        }

    base_url = dcl_url.rstrip("/")
    health_base = os.environ.get("DCL_HEALTH_URL", "")
    if not health_base:
        health_base = base_url.split("/api/dcl")[0] if "/api/dcl" in base_url else base_url
    health_url = health_base.rstrip("/") + "/health"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(health_url)
            if 200 <= resp.status_code < 300:
                return {"connected": True, "status": "connected", "message": f"DCL reachable (HTTP {resp.status_code})", "url": base_url}
            elif resp.status_code in (401, 403):
                return {"connected": False, "status": "auth_error", "message": f"DCL requires authentication (HTTP {resp.status_code})", "url": base_url}
            else:
                return {"connected": False, "status": "error", "message": f"DCL returned HTTP {resp.status_code}", "url": base_url}
    except httpx.TimeoutException:
        return {"connected": False, "status": "timeout", "message": "DCL connection timed out", "url": base_url}
    except httpx.ConnectError:
        return {"connected": False, "status": "unreachable", "message": "DCL connection refused", "url": base_url}
    except Exception as e:
        return {"connected": False, "status": "error", "message": str(e)[:200], "url": base_url}


# -- Mapping from DCL stats keys to human-readable category labels ----------
_DCL_RETENTION_CATEGORIES = [
    # (label,           actual_key,            limit_key)
    ("Ingest Runs",     "total_runs",          "max_runs"),
    ("Buffered Rows",   "total_rows_buffered", "max_rows"),
    ("Materialized Pts","materialized_points",  "max_materialized_points"),
    ("Drift Events",    "total_drift_events",  "max_drift_events"),
    ("Schema Entries",  "pipes_tracked",       "max_schema_entries"),
    ("Activity Log",    "activity_entries",     "max_activity"),
    ("Drop Log",        "total_drops",         "max_drops"),
    ("Dispatches",      "content_dispatches",  "max_content_dispatches"),
]


@router.get("/api/_diagnostics/dcl-retention")
async def dcl_retention():
    """Return DCL retention utilization for each capped category.

    Fetches GET {DCL_INGEST_URL}/stats, maps 8 categories with
    actual / limit / pct.  Skips any category where DCL has not yet
    exposed the limit (graceful forward-compatibility).  All transport
    errors surface as HTTPException (no silent fallbacks).
    """
    dcl_url = os.environ.get("DCL_INGEST_URL", "")
    if not dcl_url:
        raise HTTPException(
            status_code=503,
            detail="DCL_INGEST_URL environment variable is not configured -- cannot fetch retention stats",
        )

    stats_url = dcl_url.rstrip("/") + "/stats"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(stats_url)
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail=f"DCL timed out after 5 s -- GET {stats_url}",
        )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=503,
            detail=f"DCL unreachable (connection refused) -- GET {stats_url}",
        )
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"DCL request failed -- GET {stats_url} -- {type(exc).__name__}: {exc}",
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"DCL returned HTTP {resp.status_code} -- GET {stats_url}",
        )

    try:
        data = resp.json()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail=f"DCL returned non-JSON body -- GET {stats_url}",
        )

    categories = []
    for label, actual_key, limit_key in _DCL_RETENTION_CATEGORIES:
        limit = data.get(limit_key)
        actual = data.get(actual_key)
        if limit is None or actual is None:
            continue  # DCL hasn't exposed this field yet -- skip gracefully
        pct = round(actual / limit * 100, 1) if limit > 0 else 0.0
        categories.append({
            "label": label,
            "actual": actual,
            "limit": limit,
            "pct": pct,
        })

    return {"categories": categories, "dcl_url": stats_url}
