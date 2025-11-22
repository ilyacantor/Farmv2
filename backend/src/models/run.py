from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
from uuid import UUID


class RunStatus(str, Enum):
    """Run status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class RunType(str, Enum):
    """Run type enumeration."""
    E2E = "e2e"
    MODULE = "module"


class Run(BaseModel):
    """Run model."""
    id: UUID
    scenario_id: str
    run_type: RunType
    module: Optional[str] = None
    lab_tenant_id: UUID
    status: RunStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    config: Dict[str, Any]
    error_message: Optional[str] = None
    logs: list[Dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class RunCreate(BaseModel):
    """Request model for creating a new run."""
    scenario_id: str
    config_overrides: Optional[Dict[str, Any]] = None


class RunList(BaseModel):
    """List of runs."""
    runs: list[Run]
    total: int
    limit: int
    offset: int


class RunStatusResponse(BaseModel):
    """Run status response."""
    run_id: UUID
    status: RunStatus
    current_stage: Optional[str] = None
    progress: Optional[Dict[str, str]] = None
    started_at: datetime
    elapsed_seconds: Optional[int] = None
