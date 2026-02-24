"""
Run log API — full provenance visibility for manifest-driven executions.

Every manifest execution (success, failure, DCL rejection) is persisted in the
manifest_runs table. These endpoints expose that data for debugging, monitoring,
and tracing the AAM → Farm → DCL data flow.

Endpoints:
- GET  /api/runs                    List runs (filterable by tenant_id, status, pipe_id)
- GET  /api/runs/{farm_run_id}      Full run detail with push result
- GET  /api/runs/by-pipe/{pipe_id}  All runs for a specific pipe
- GET  /api/runs/by-aam-run/{aam_run_id} All Farm executions for an AAM batch run
- GET  /api/farm/status/{job_id}    Quick execution status + row counts for AAM polling
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.farm.db import get_manifest_run, list_manifest_runs, connection as db_connection

logger = logging.getLogger("farm.api.run_log")

router = APIRouter(tags=["run-log"])


@router.get("/api/runs")
async def list_runs(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    status: Optional[str] = Query(None, description="Filter by status: completed, failed, rejected_by_dcl"),
    pipe_id: Optional[str] = Query(None, description="Filter by pipe_id"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List manifest runs with full provenance. Ordered by created_at DESC."""
    if pipe_id:
        # Use dedicated pipe query for pipe_id filtering
        async with db_connection() as conn:
            rows = await conn.fetch(
                """SELECT farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
                          tenant_id, snapshot_name, source_system, category, generator_key,
                          status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
                          error_type, error_message, schema_drift,
                          created_at, elapsed_ms
                   FROM manifest_runs
                   WHERE pipe_id = $1
                   ORDER BY created_at DESC
                   LIMIT $2 OFFSET $3""",
                pipe_id, limit, offset,
            )
            return [dict(r) for r in rows]

    return await list_manifest_runs(
        tenant_id=tenant_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/api/runs/by-pipe/{pipe_id}")
async def get_runs_by_pipe(
    pipe_id: str,
    limit: int = Query(50, ge=1, le=500),
):
    """Find all runs for a specific pipe_id. Traces a pipe's execution history."""
    async with db_connection() as conn:
        rows = await conn.fetch(
            """SELECT farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
                      tenant_id, snapshot_name, source_system, category, generator_key,
                      status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
                      error_type, error_message, schema_drift,
                      created_at, elapsed_ms
               FROM manifest_runs
               WHERE pipe_id = $1
               ORDER BY created_at DESC
               LIMIT $2""",
            pipe_id, limit,
        )
        if not rows:
            return {"pipe_id": pipe_id, "runs": [], "message": f"No runs found for pipe_id={pipe_id}"}
        return {"pipe_id": pipe_id, "runs": [dict(r) for r in rows], "count": len(rows)}


@router.get("/api/runs/by-aam-run/{aam_run_id}")
async def get_runs_by_aam_run(aam_run_id: str):
    """Find all Farm executions triggered by a single AAM batch run.

    The aam_run_id is the batch-level correlation key that AAM/AOD assigns
    to a dispatch. All pipes in that batch share the same aam_run_id.
    Falls back to matching run_id for pre-migration data.
    """
    async with db_connection() as conn:
        # Try aam_run_id first; fall back to run_id for pre-migration rows
        rows = await conn.fetch(
            """SELECT farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
                      tenant_id, snapshot_name, source_system, category, generator_key,
                      status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
                      error_type, error_message, schema_drift,
                      created_at, elapsed_ms
               FROM manifest_runs
               WHERE aam_run_id = $1
               ORDER BY created_at DESC""",
            aam_run_id,
        )

        if not rows:
            # Fallback: pre-migration data only has run_id
            rows = await conn.fetch(
                """SELECT farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
                          tenant_id, snapshot_name, source_system, category, generator_key,
                          status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
                          error_type, error_message, schema_drift,
                          created_at, elapsed_ms
                   FROM manifest_runs
                   WHERE run_id = $1
                   ORDER BY created_at DESC""",
                aam_run_id,
            )

        # Status counts
        completed = sum(1 for r in rows if r["status"] == "completed")
        failed = sum(1 for r in rows if r["status"] == "failed")
        rejected = sum(1 for r in rows if r["status"] == "rejected_by_dcl")

        # Per-system breakdown
        per_system: dict[str, dict[str, int]] = {}
        for r in rows:
            sys_key = r["source_system"]
            if sys_key not in per_system:
                per_system[sys_key] = {"completed": 0, "failed": 0, "rejected_by_dcl": 0}
            per_system[sys_key][r["status"]] = per_system[sys_key].get(r["status"], 0) + 1

        # Error type breakdown
        error_types: dict[str, int] = {}
        for r in rows:
            if r["error_type"]:
                error_types[r["error_type"]] = error_types.get(r["error_type"], 0) + 1

        # Timing
        elapsed_values = [r["elapsed_ms"] for r in rows if r["elapsed_ms"] is not None]
        total_rows_generated = sum(r["rows_generated"] or 0 for r in rows)
        total_rows_pushed = sum(r["rows_pushed"] or 0 for r in rows)
        total_rows_accepted = sum(r["rows_accepted"] or 0 for r in rows if r["rows_accepted"] is not None)

        # Failed pipes detail (so operators can see at a glance what failed)
        failed_pipes = [
            {
                "pipe_id": r["pipe_id"],
                "source_system": r["source_system"],
                "error_type": r["error_type"],
                "error_message": r["error_message"],
                "dcl_status_code": r["dcl_status_code"],
            }
            for r in rows
            if r["status"] in ("failed", "rejected_by_dcl")
        ]

        summary = {
            "aam_run_id": aam_run_id,
            "total": len(rows),
            "completed": completed,
            "failed": failed,
            "rejected_by_dcl": rejected,
            "total_rows_generated": total_rows_generated,
            "total_rows_pushed": total_rows_pushed,
            "total_rows_accepted": total_rows_accepted,
            "per_system": per_system,
            "error_types": error_types if error_types else None,
            "failed_pipes": failed_pipes if failed_pipes else None,
            "timing": {
                "min_ms": min(elapsed_values) if elapsed_values else None,
                "max_ms": max(elapsed_values) if elapsed_values else None,
                "avg_ms": round(sum(elapsed_values) / len(elapsed_values)) if elapsed_values else None,
            },
        }
        return {"summary": summary, "runs": [dict(r) for r in rows]}


@router.get("/api/runs/{farm_run_id}")
async def get_run_detail(farm_run_id: str):
    """Full run detail including push_result_json for deep debugging."""
    run = await get_manifest_run(farm_run_id)
    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"No manifest run found with farm_run_id={farm_run_id}",
        )
    return run


@router.get("/api/farm/status/{job_id}")
async def get_farm_status(job_id: str):
    """Quick execution status for a single job — designed for AAM polling.

    Accepts either a farm_run_id or AAM's run_id (job_id). Tries
    farm_run_id first, then falls back to run_id. Returns the most
    recent run if multiple exist for the same run_id.
    """
    # Try farm_run_id match first
    run = await get_manifest_run(job_id)
    if not run:
        # Fall back to AAM run_id match (most recent)
        async with db_connection() as conn:
            row = await conn.fetchrow(
                """SELECT farm_run_id, run_id, status, rows_generated,
                          rows_pushed, rows_accepted, elapsed_ms, error_message
                   FROM manifest_runs
                   WHERE run_id = $1
                   ORDER BY created_at DESC
                   LIMIT 1""",
                job_id,
            )
            if row:
                run = dict(row)

    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"No manifest run found for job_id={job_id}",
        )

    return {
        "job_id": job_id,
        "farm_run_id": run.get("farm_run_id"),
        "status": run.get("status"),
        "rows_generated": run.get("rows_generated", 0),
        "rows_pushed": run.get("rows_pushed", 0),
        "rows_accepted": run.get("rows_accepted"),
        "elapsed_ms": run.get("elapsed_ms"),
        "error_message": run.get("error_message"),
    }
