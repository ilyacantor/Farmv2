from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from uuid import UUID
import logging

from src.models.run import Run, RunCreate, RunList, RunStatusResponse
from src.services.run_service import RunService

logger = logging.getLogger(__name__)
router = APIRouter()
run_service = RunService()


@router.post("/runs", response_model=Run, status_code=201)
async def create_run(run_create: RunCreate):
    """Start a new test run."""
    try:
        run = await run_service.create_run(
            scenario_id=run_create.scenario_id,
            config_overrides=run_create.config_overrides
        )
        return run
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs", response_model=RunList)
async def list_runs(
    scenario_id: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    module: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """List runs with optional filtering and pagination."""
    try:
        runs, total = await run_service.list_runs(
            scenario_id=scenario_id,
            run_type=type,
            module=module,
            status=status,
            limit=limit,
            offset=offset
        )
        return RunList(
            runs=runs,
            total=total,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        logger.error(f"Error listing runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}", response_model=Run)
async def get_run(run_id: UUID):
    """Get a specific run by ID."""
    try:
        run = await run_service.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        return run
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}/status", response_model=RunStatusResponse)
async def get_run_status(run_id: UUID):
    """Get the current status of a run."""
    try:
        status = await run_service.get_run_status(run_id)
        if not status:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run status {run_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}/metrics")
async def get_run_metrics(run_id: UUID):
    """Get metrics for a completed run."""
    try:
        metrics = await run_service.get_run_metrics(run_id)
        if metrics is None:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        return {"run_id": run_id, "metrics": metrics}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run metrics {run_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
