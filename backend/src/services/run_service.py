from typing import Optional, List, Tuple, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime
import logging

from src.models.run import Run, RunStatus, RunType, RunStatusResponse
from src.db.supabase import get_supabase
from src.services.scenario_service import ScenarioService

logger = logging.getLogger(__name__)


class RunService:
    """Service for managing test runs."""

    def __init__(self):
        self.supabase = get_supabase()
        self.scenario_service = ScenarioService()

    async def create_run(
        self,
        scenario_id: str,
        config_overrides: Optional[Dict[str, Any]] = None
    ) -> Run:
        """Create a new test run."""
        # Get scenario
        scenario = await self.scenario_service.get_scenario(scenario_id)
        if not scenario:
            raise ValueError(f"Scenario '{scenario_id}' not found")

        # Merge config with overrides
        config = {**scenario.config}
        if config_overrides:
            config.update(config_overrides)

        # Generate IDs
        run_id = uuid4()
        lab_tenant_id = uuid4()

        # Create run record
        run_data = {
            "id": str(run_id),
            "scenario_id": scenario_id,
            "run_type": scenario.scenario_type.value,
            "module": scenario.module.value if scenario.module else None,
            "lab_tenant_id": str(lab_tenant_id),
            "status": RunStatus.PENDING.value,
            "started_at": datetime.utcnow().isoformat(),
            "config": config,
            "metrics": {},
            "logs": []
        }

        result = self.supabase.table("farm_runs").insert(run_data).execute()

        if not result.data:
            raise Exception("Failed to create run")

        row = result.data[0]

        # TODO: Trigger async orchestration
        logger.info(f"Created run {run_id} for scenario {scenario_id}")

        return self._row_to_run(row)

    async def list_runs(
        self,
        scenario_id: Optional[str] = None,
        run_type: Optional[str] = None,
        module: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[Run], int]:
        """List runs with filtering and pagination."""
        # Build query
        query = self.supabase.table("farm_runs").select("*", count="exact")

        if scenario_id:
            query = query.eq("scenario_id", scenario_id)
        if run_type:
            query = query.eq("run_type", run_type)
        if module:
            query = query.eq("module", module)
        if status:
            query = query.eq("status", status)

        # Order by most recent first
        query = query.order("started_at", desc=True)
        query = query.range(offset, offset + limit - 1)

        result = query.execute()

        runs = [self._row_to_run(row) for row in result.data]
        total = result.count if hasattr(result, 'count') else len(runs)

        return runs, total

    async def get_run(self, run_id: UUID) -> Optional[Run]:
        """Get a specific run by ID."""
        result = self.supabase.table("farm_runs").select("*").eq("id", str(run_id)).execute()

        if not result.data:
            return None

        return self._row_to_run(result.data[0])

    async def get_run_status(self, run_id: UUID) -> Optional[RunStatusResponse]:
        """Get the current status of a run."""
        run = await self.get_run(run_id)
        if not run:
            return None

        elapsed = None
        if run.started_at:
            if run.completed_at:
                elapsed = int((run.completed_at - run.started_at).total_seconds())
            else:
                elapsed = int((datetime.utcnow() - run.started_at).total_seconds())

        return RunStatusResponse(
            run_id=run.id,
            status=run.status,
            current_stage=self._get_current_stage(run),
            progress=self._get_progress(run),
            started_at=run.started_at,
            elapsed_seconds=elapsed
        )

    async def get_run_metrics(self, run_id: UUID) -> Optional[Dict[str, Any]]:
        """Get metrics for a run."""
        run = await self.get_run(run_id)
        if not run:
            return None
        return run.metrics

    def _row_to_run(self, row: Dict[str, Any]) -> Run:
        """Convert database row to Run model."""
        return Run(
            id=UUID(row["id"]),
            scenario_id=row["scenario_id"],
            run_type=RunType(row["run_type"]),
            module=row.get("module"),
            lab_tenant_id=UUID(row["lab_tenant_id"]),
            status=RunStatus(row["status"]),
            started_at=datetime.fromisoformat(row["started_at"].replace('Z', '+00:00')),
            completed_at=datetime.fromisoformat(row["completed_at"].replace('Z', '+00:00')) if row.get("completed_at") else None,
            metrics=row.get("metrics", {}),
            config=row.get("config", {}),
            error_message=row.get("error_message"),
            logs=row.get("logs", []),
            created_at=datetime.fromisoformat(row["created_at"].replace('Z', '+00:00')),
            updated_at=datetime.fromisoformat(row["updated_at"].replace('Z', '+00:00'))
        )

    def _get_current_stage(self, run: Run) -> Optional[str]:
        """Determine current stage of run."""
        if run.status in [RunStatus.SUCCESS, RunStatus.FAILED]:
            return None

        # Check metrics to determine current stage
        metrics = run.metrics
        if not metrics.get("aod"):
            return "aod"
        if not metrics.get("aam"):
            return "aam"
        if not metrics.get("dcl"):
            return "dcl"
        if not metrics.get("agents"):
            return "agents"

        return None

    def _get_progress(self, run: Run) -> Optional[Dict[str, str]]:
        """Get progress of each stage."""
        if run.run_type == RunType.E2E:
            metrics = run.metrics
            return {
                "aod": "completed" if metrics.get("aod") else "pending",
                "aam": "completed" if metrics.get("aam") else "pending",
                "dcl": "completed" if metrics.get("dcl") else "pending",
                "agents": "completed" if metrics.get("agents") else "pending"
            }
        elif run.run_type == RunType.MODULE:
            return {
                run.module: run.status.value
            }

        return None
