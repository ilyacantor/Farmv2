"""
Background job management for AOS Farm.

Implements async job pattern for long-running operations like Mega snapshot generation.
Returns 202 Accepted + job_id immediately, processes in background.
"""

import asyncio
import logging
import uuid
import json
from datetime import datetime
from typing import Optional, Any, Callable, Awaitable
from enum import Enum
from dataclasses import dataclass, field, asdict

from src.farm.db import connection, DBUnavailable

# Configure module-level logger
logger = logging.getLogger("farm.jobs")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobProgress:
    current_step: str = ""
    steps_completed: int = 0
    total_steps: int = 0
    percent: int = 0
    message: str = ""


@dataclass
class Job:
    job_id: str
    job_type: str
    status: JobStatus
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: JobProgress = field(default_factory=JobProgress)
    result: Optional[dict] = None
    error: Optional[str] = None
    input_params: Optional[dict] = None
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d['status'] = self.status.value
        return d


class JobManager:
    """Manages background jobs with database persistence."""
    
    _instance: Optional["JobManager"] = None
    _active_jobs: dict[str, asyncio.Task] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        pass
    
    @staticmethod
    async def ensure_table():
        """Create jobs table if not exists."""
        try:
            async with connection() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        job_type TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        progress_json JSONB DEFAULT '{}',
                        result_json JSONB,
                        error TEXT,
                        input_params_json JSONB
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC)")
        except DBUnavailable:
            logger.warning("DB unavailable, skipping table creation")
    
    async def create_job(self, job_type: str, input_params: Optional[dict] = None) -> str:
        """Create a new pending job and return job_id."""
        job_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat() + "Z"
        
        try:
            async with connection() as conn:
                await conn.execute("""
                    INSERT INTO jobs (job_id, job_type, status, created_at, input_params_json)
                    VALUES ($1, $2, 'pending', $3, $4)
                """, job_id, job_type, created_at, json.dumps(input_params) if input_params else None)
        except DBUnavailable as e:
            logger.debug("DB unavailable when persisting job creation for %s: %s", job_id, e)
        
        return job_id
    
    async def start_job(self, job_id: str) -> None:
        """Mark job as running."""
        started_at = datetime.utcnow().isoformat() + "Z"
        try:
            async with connection() as conn:
                await conn.execute("""
                    UPDATE jobs SET status = 'running', started_at = $1 WHERE job_id = $2
                """, started_at, job_id)
        except DBUnavailable as e:
            logger.debug("DB unavailable when marking job %s as started: %s", job_id, e)
    
    async def update_progress(
        self,
        job_id: str,
        current_step: str = "",
        steps_completed: int = 0,
        total_steps: int = 0,
        message: str = "",
    ) -> None:
        """Update job progress."""
        percent = int((steps_completed / total_steps) * 100) if total_steps > 0 else 0
        progress = {
            "current_step": current_step,
            "steps_completed": steps_completed,
            "total_steps": total_steps,
            "percent": percent,
            "message": message,
        }
        try:
            async with connection() as conn:
                await conn.execute("""
                    UPDATE jobs SET progress_json = $1 WHERE job_id = $2
                """, json.dumps(progress), job_id)
        except DBUnavailable as e:
            logger.debug("DB unavailable when updating progress for job %s: %s", job_id, e)
    
    async def complete_job(self, job_id: str, result: Optional[dict] = None) -> None:
        """Mark job as completed with optional result."""
        completed_at = datetime.utcnow().isoformat() + "Z"
        try:
            async with connection() as conn:
                await conn.execute("""
                    UPDATE jobs SET 
                        status = 'completed', 
                        completed_at = $1,
                        result_json = $2,
                        progress_json = '{"percent": 100, "current_step": "done", "message": "Complete"}'
                    WHERE job_id = $3
                """, completed_at, json.dumps(result) if result else None, job_id)
        except DBUnavailable as e:
            logger.debug("DB unavailable when marking job %s as completed: %s", job_id, e)
        
        if job_id in self._active_jobs:
            del self._active_jobs[job_id]
    
    async def fail_job(self, job_id: str, error: str) -> None:
        """Mark job as failed with error message."""
        completed_at = datetime.utcnow().isoformat() + "Z"
        try:
            async with connection() as conn:
                await conn.execute("""
                    UPDATE jobs SET 
                        status = 'failed', 
                        completed_at = $1,
                        error = $2
                    WHERE job_id = $3
                """, completed_at, error, job_id)
        except DBUnavailable as e:
            logger.debug("DB unavailable when marking job %s as failed: %s", job_id, e)
        
        if job_id in self._active_jobs:
            del self._active_jobs[job_id]
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        try:
            async with connection() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM jobs WHERE job_id = $1",
                    job_id
                )
                if not row:
                    return None
                
                progress_data = row.get("progress_json") or {}
                if isinstance(progress_data, str):
                    progress_data = json.loads(progress_data)
                
                result_data = row.get("result_json")
                if isinstance(result_data, str):
                    result_data = json.loads(result_data)
                
                input_params = row.get("input_params_json")
                if isinstance(input_params, str):
                    input_params = json.loads(input_params)
                
                return Job(
                    job_id=row["job_id"],
                    job_type=row["job_type"],
                    status=JobStatus(row["status"]),
                    created_at=row["created_at"],
                    started_at=row.get("started_at"),
                    completed_at=row.get("completed_at"),
                    progress=JobProgress(**progress_data) if progress_data else JobProgress(),
                    result=result_data,
                    error=row.get("error"),
                    input_params=input_params,
                )
        except DBUnavailable:
            return None
    
    async def list_jobs(self, limit: int = 50, status: Optional[JobStatus] = None) -> list[Job]:
        """List recent jobs."""
        try:
            async with connection() as conn:
                if status:
                    rows = await conn.fetch(
                        "SELECT * FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2",
                        status.value, limit
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1",
                        limit
                    )
                
                jobs = []
                for row in rows:
                    progress_data = row.get("progress_json") or {}
                    if isinstance(progress_data, str):
                        progress_data = json.loads(progress_data)
                    
                    result_data = row.get("result_json")
                    if isinstance(result_data, str):
                        result_data = json.loads(result_data)
                    
                    jobs.append(Job(
                        job_id=row["job_id"],
                        job_type=row["job_type"],
                        status=JobStatus(row["status"]),
                        created_at=row["created_at"],
                        started_at=row.get("started_at"),
                        completed_at=row.get("completed_at"),
                        progress=JobProgress(**progress_data) if progress_data else JobProgress(),
                        result=result_data,
                        error=row.get("error"),
                    ))
                return jobs
        except DBUnavailable:
            return []
    
    def run_in_background(
        self,
        job_id: str,
        coro: Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> asyncio.Task:
        """Run a coroutine in the background, tracking it by job_id."""
        async def wrapped():
            try:
                await self.start_job(job_id)
                await coro(job_id, *args, **kwargs)
            except Exception as e:
                await self.fail_job(job_id, str(e))
                raise
        
        task = asyncio.create_task(wrapped())
        self._active_jobs[job_id] = task
        return task
    
    async def cleanup_old_jobs(self, days: int = 7) -> int:
        """Delete jobs older than N days. Returns count deleted."""
        try:
            async with connection() as conn:
                result = await conn.execute("""
                    DELETE FROM jobs
                    WHERE created_at < NOW() - INTERVAL '%s days'
                    AND status IN ('completed', 'failed')
                """ % days)
                # asyncpg returns status string like "DELETE 42" - extract count safely
                return _parse_delete_count(result)
        except DBUnavailable:
            return 0


def _parse_delete_count(result: str) -> int:
    """Parse row count from asyncpg DELETE result string (e.g., 'DELETE 42')."""
    if not result:
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0].upper() == "DELETE":
            return int(parts[1])
        return 0
    except (ValueError, IndexError):
        logger.warning(f"Could not parse DELETE count from: {result}")
        return 0


job_manager = JobManager()
