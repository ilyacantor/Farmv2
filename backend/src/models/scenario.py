from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from datetime import datetime
from enum import Enum


class ScenarioType(str, Enum):
    """Scenario type enumeration."""
    E2E = "e2e"
    MODULE = "module"


class ModuleType(str, Enum):
    """Module type enumeration."""
    AAM = "aam"
    DCL = "dcl"


class Scenario(BaseModel):
    """Scenario model."""
    id: str
    name: str
    description: Optional[str] = None
    scenario_type: ScenarioType
    module: Optional[ModuleType] = None
    tags: List[str] = Field(default_factory=list)
    config: Dict[str, Any]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "e2e-small-clean",
                "name": "Small Clean Enterprise",
                "description": "Baseline E2E test with clean data",
                "scenario_type": "e2e",
                "module": None,
                "tags": ["small", "clean", "baseline"],
                "config": {
                    "scale": {
                        "assets": {"applications": 50}
                    }
                }
            }
        }


class ScenarioList(BaseModel):
    """List of scenarios."""
    scenarios: List[Scenario]
    total: int
