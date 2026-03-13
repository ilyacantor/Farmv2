"""
Maestra status endpoint for the Farm module.

Returns structured JSON about Farm's generation state for a given tenant_id.
Queries existing Farm state (manifest_runs, jobs, snapshots_meta) — does not
fabricate data.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

from src.farm.db import connection as db_connection, DBUnavailable, is_healthy

logger = logging.getLogger("farm.maestra")

router = APIRouter(tags=["maestra"])


@router.get("/api/maestra/status")
async def maestra_status(
    tenant_id: str = Query(..., description="Tenant ID to query status for"),
):
    """Return structured JSON about Farm generation state for a given tenant.

    Queries:
    - snapshots_meta: latest snapshot for the tenant (enterprise_profile, created_at)
    - manifest_runs: generation runs for the tenant (progress, quality flags)
    - jobs: any running background jobs for the tenant
    """
    db_healthy, db_msg = is_healthy()

    try:
        async with db_connection() as conn:
            # 1. Latest snapshot for this tenant — tells us the active profile
            latest_snapshot = await conn.fetchrow(
                """SELECT enterprise_profile, realism_profile, created_at,
                          total_assets, plane_counts, expected_summary
                   FROM snapshots_meta
                   WHERE tenant_id = $1
                   ORDER BY created_at DESC
                   LIMIT 1""",
                tenant_id,
            )

            # 2. Manifest runs for this tenant — tells us generation progress
            manifest_stats = await conn.fetchrow(
                """SELECT
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                       COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                       COUNT(*) FILTER (WHERE status = 'rejected_by_dcl') AS rejected,
                       MAX(created_at) AS last_run_at
                   FROM manifest_runs
                   WHERE tenant_id = $1""",
                tenant_id,
            )

            # 3. Data quality flags from manifest runs — schema drift, rejections
            quality_rows = await conn.fetch(
                """SELECT pipe_id, status, error_type, schema_drift
                   FROM manifest_runs
                   WHERE tenant_id = $1
                     AND (schema_drift = true OR status IN ('failed', 'rejected_by_dcl'))
                   ORDER BY created_at DESC
                   LIMIT 20""",
                tenant_id,
            )

            # 4. Active background jobs (any running job that references this tenant)
            running_jobs = await conn.fetch(
                """SELECT job_id, job_type, status, progress_json, created_at
                   FROM jobs
                   WHERE status IN ('pending', 'running')
                   ORDER BY created_at DESC
                   LIMIT 5""",
                tenant_id,
            )

    except DBUnavailable as e:
        raise  # Let the global handler return 503
    except Exception as e:
        logger.error(
            f"Maestra status query failed for tenant_id={tenant_id}: "
            f"{type(e).__name__}: {e}"
        )
        raise HTTPException(
            status_code=500,
            detail=(
                f"Farm could not query generation state for tenant_id={tenant_id} — "
                f"{type(e).__name__}: {e}"
            ),
        )

    # Build personas from enterprise_profile
    personas_active = []
    if latest_snapshot and latest_snapshot["enterprise_profile"]:
        profile = latest_snapshot["enterprise_profile"]
        # Enterprise profiles map to persona packs
        _PROFILE_PERSONAS = {
            "modern_saas": ["CRO", "CFO", "CTO"],
            "regulated_finance": ["CFO", "COO", "CRO"],
            "healthcare_provider": ["COO", "CFO", "CHRO"],
            "global_manufacturing": ["COO", "CFO", "CTO"],
        }
        personas_active = _PROFILE_PERSONAS.get(profile, [])

    # Determine generation progress
    total = manifest_stats["total"] if manifest_stats else 0
    completed = manifest_stats["completed"] if manifest_stats else 0
    failed = manifest_stats["failed"] if manifest_stats else 0

    if total == 0:
        gen_status = "idle"
        gen_percent = 0
    elif failed > 0 and completed == 0:
        gen_status = "error"
        gen_percent = 0
    else:
        # Check if any jobs are currently running
        has_running = any(
            j["status"] == "running" for j in running_jobs
        )
        if has_running:
            gen_status = "running"
            gen_percent = int((completed / max(total, 1)) * 100)
        else:
            gen_status = "complete" if failed == 0 else "complete"
            gen_percent = 100 if failed == 0 else int((completed / max(total, 1)) * 100)

    # Build data quality flags
    data_quality_flags = []
    for row in quality_rows:
        flag = {
            "pipe_id": row["pipe_id"],
            "issue": row["error_type"] or row["status"],
        }
        if row["schema_drift"]:
            flag["issue"] = "schema_drift"
        data_quality_flags.append(flag)

    # Determine last_generation_at — most recent of snapshot or manifest run
    last_generation_at = None
    if manifest_stats and manifest_stats["last_run_at"]:
        last_generation_at = manifest_stats["last_run_at"]
    elif latest_snapshot and latest_snapshot["created_at"]:
        last_generation_at = latest_snapshot["created_at"]

    # If it's a datetime, format it; if it's already a string, keep it
    if isinstance(last_generation_at, datetime):
        last_generation_at = last_generation_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "module": "farm",
        "tenant_id": tenant_id,
        "active_tenant": tenant_id if (latest_snapshot or total > 0) else None,
        "personas_active": personas_active,
        "generation_progress": {
            "percent": gen_percent,
            "status": gen_status,
        },
        "data_quality_flags": data_quality_flags,
        "last_generation_at": last_generation_at,
        "healthy": db_healthy,
    }
