"""
Run log API — full provenance visibility for manifest-driven executions.

Every manifest execution (success, failure, DCL rejection) is persisted in the
manifest_runs table. These endpoints expose that data for debugging, monitoring,
and tracing the AAM → Farm → DCL data flow.

Endpoints:
- GET  /api/runs                    List runs (filterable by tenant_id, status, pipe_id)
- GET  /api/runs/{farm_run_id}      Full run detail with push result
- GET  /api/runs/by-pipe/{pipe_id}  All runs for a specific pipe
- GET  /api/runs/by-aam-run/{run_id} All Farm executions for an AAM batch run
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
                """SELECT farm_run_id, run_id, pipe_id, dcl_run_id,
                          tenant_id, snapshot_name, source_system, category, generator_key,
                          status, rows_generated, rows_accepted, dcl_status_code,
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
            """SELECT farm_run_id, run_id, pipe_id, dcl_run_id,
                      tenant_id, snapshot_name, source_system, category, generator_key,
                      status, rows_generated, rows_accepted, dcl_status_code,
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


@router.get("/api/runs/by-aam-run/{run_id}")
async def get_runs_by_aam_run(run_id: str):
    """Find all Farm executions triggered by a single AAM run_id.

    An AAM batch dispatch produces one run_id across multiple pipes.
    This endpoint shows all Farm executions for that batch.
    """
    async with db_connection() as conn:
        rows = await conn.fetch(
            """SELECT farm_run_id, run_id, pipe_id, dcl_run_id,
                      tenant_id, snapshot_name, source_system, category, generator_key,
                      status, rows_generated, rows_accepted, dcl_status_code,
                      error_type, error_message, schema_drift,
                      created_at, elapsed_ms
               FROM manifest_runs
               WHERE run_id = $1
               ORDER BY created_at DESC""",
            run_id,
        )
        summary = {
            "run_id": run_id,
            "total": len(rows),
            "completed": sum(1 for r in rows if r["status"] == "completed"),
            "failed": sum(1 for r in rows if r["status"] == "failed"),
            "rejected_by_dcl": sum(1 for r in rows if r["status"] == "rejected_by_dcl"),
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
