import json
import os
from typing import Optional, List
from pathlib import Path
import logging

from src.models.scenario import Scenario, ScenarioType, ModuleType
from src.db.supabase import get_supabase

logger = logging.getLogger(__name__)


class ScenarioService:
    """Service for managing test scenarios."""

    def __init__(self):
        self.supabase = get_supabase()
        self.scenarios_path = Path(__file__).parent.parent.parent.parent / "scenarios"

    async def load_scenarios_from_files(self) -> List[Scenario]:
        """Load scenarios from JSON files in the scenarios directory."""
        scenarios = []

        for scenario_type in ["e2e", "aam", "dcl"]:
            type_dir = self.scenarios_path / scenario_type
            if not type_dir.exists():
                continue

            for json_file in type_dir.glob("*.json"):
                try:
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                        scenario = Scenario(**data)
                        scenarios.append(scenario)
                        logger.info(f"Loaded scenario: {scenario.id}")
                except Exception as e:
                    logger.error(f"Error loading scenario from {json_file}: {e}")

        return scenarios

    async def sync_scenarios_to_db(self):
        """Sync scenario files to the database."""
        scenarios = await self.load_scenarios_from_files()

        for scenario in scenarios:
            try:
                # Upsert scenario to database
                data = {
                    "id": scenario.id,
                    "name": scenario.name,
                    "description": scenario.description,
                    "scenario_type": scenario.scenario_type.value,
                    "module": scenario.module.value if scenario.module else None,
                    "tags": scenario.tags,
                    "config": scenario.config
                }

                result = self.supabase.table("farm_scenarios").upsert(data).execute()
                logger.info(f"Synced scenario {scenario.id} to database")
            except Exception as e:
                logger.error(f"Error syncing scenario {scenario.id}: {e}")

        logger.info(f"Synced {len(scenarios)} scenarios to database")

    async def list_scenarios(
        self,
        scenario_type: Optional[str] = None,
        module: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[Scenario]:
        """List scenarios with optional filtering."""
        query = self.supabase.table("farm_scenarios").select("*")

        if scenario_type:
            query = query.eq("scenario_type", scenario_type)

        if module:
            query = query.eq("module", module)

        if tags:
            # Filter by tags (contains any of the specified tags)
            query = query.overlaps("tags", tags)

        result = query.execute()

        scenarios = []
        for row in result.data:
            scenario = Scenario(
                id=row["id"],
                name=row["name"],
                description=row.get("description"),
                scenario_type=ScenarioType(row["scenario_type"]),
                module=ModuleType(row["module"]) if row.get("module") else None,
                tags=row.get("tags", []),
                config=row.get("config", {}),
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at")
            )
            scenarios.append(scenario)

        return scenarios

    async def get_scenario(self, scenario_id: str) -> Optional[Scenario]:
        """Get a specific scenario by ID."""
        result = self.supabase.table("farm_scenarios").select("*").eq("id", scenario_id).execute()

        if not result.data:
            return None

        row = result.data[0]
        return Scenario(
            id=row["id"],
            name=row["name"],
            description=row.get("description"),
            scenario_type=ScenarioType(row["scenario_type"]),
            module=ModuleType(row["module"]) if row.get("module") else None,
            tags=row.get("tags", []),
            config=row.get("config", {}),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at")
        )
