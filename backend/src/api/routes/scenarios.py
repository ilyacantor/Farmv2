from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging

from src.models.scenario import Scenario, ScenarioList
from src.services.scenario_service import ScenarioService

logger = logging.getLogger(__name__)
router = APIRouter()
scenario_service = ScenarioService()


@router.get("/scenarios", response_model=ScenarioList)
async def list_scenarios(
    type: Optional[str] = Query(None, description="Filter by scenario type (e2e, module)"),
    module: Optional[str] = Query(None, description="Filter by module (aam, dcl)"),
    tags: Optional[str] = Query(None, description="Comma-separated tags to filter by")
):
    """List all scenarios with optional filtering."""
    try:
        tag_list = tags.split(",") if tags else None
        scenarios = await scenario_service.list_scenarios(
            scenario_type=type,
            module=module,
            tags=tag_list
        )
        return ScenarioList(
            scenarios=scenarios,
            total=len(scenarios)
        )
    except Exception as e:
        logger.error(f"Error listing scenarios: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scenarios/{scenario_id}", response_model=Scenario)
async def get_scenario(scenario_id: str):
    """Get a specific scenario by ID."""
    try:
        scenario = await scenario_service.get_scenario(scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail=f"Scenario '{scenario_id}' not found")
        return scenario
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting scenario {scenario_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
