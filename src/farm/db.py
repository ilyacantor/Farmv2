"""
Centralized database management for AOS Farm.

Features:
- Singleton asyncpg pool with conservative settings for Supabase pooler
- Exponential backoff on connection failures
- Circuit breaker to prevent connection storms
- Concurrency semaphore for DB-heavy operations
- Graceful degradation when DB is unavailable

Environment variables (all optional with sensible defaults):
- DB_POOL_MIN: Minimum pool size (default: 0)
- DB_POOL_MAX: Maximum pool size (default: 2)
- DB_CONNECT_TIMEOUT: Connection timeout in seconds (default: 10)
- DB_COMMAND_TIMEOUT: Command timeout in seconds (default: 15)
- DB_MAX_INACTIVE_LIFETIME: Max idle connection lifetime (default: 10)
- DB_BACKOFF_BASE: Initial backoff delay in seconds (default: 10)
- DB_BACKOFF_CAP: Maximum backoff delay in seconds (default: 120)
- DB_FAIL_THRESHOLD: Failures before circuit breaker trips (default: 8)
- DB_COOLDOWN_SECONDS: Circuit breaker cooldown period (default: 180)
- DB_CONCURRENCY: Max concurrent DB operations (default: 2)
- DB_SIMULATE_DOWN: Force DB failures for testing (default: false)
"""

import os
import asyncio
import logging
import time
from typing import Optional, TypeVar, Callable, Any
from contextlib import asynccontextmanager
from functools import wraps

import asyncpg

# Configure module-level logger
logger = logging.getLogger("farm.db")
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

T = TypeVar('T')

DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", "2"))
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "5"))
DB_CONNECT_TIMEOUT = float(os.environ.get("DB_CONNECT_TIMEOUT", "30"))
DB_COMMAND_TIMEOUT = float(os.environ.get("DB_COMMAND_TIMEOUT", "30"))
DB_STATEMENT_TIMEOUT = int(os.environ.get("DB_STATEMENT_TIMEOUT", "30"))
DB_MAX_INACTIVE_LIFETIME = float(os.environ.get("DB_MAX_INACTIVE_LIFETIME", "30"))
DB_BACKOFF_BASE = float(os.environ.get("DB_BACKOFF_BASE", "2"))
DB_BACKOFF_CAP = float(os.environ.get("DB_BACKOFF_CAP", "30"))
DB_FAIL_THRESHOLD = int(os.environ.get("DB_FAIL_THRESHOLD", "5"))
DB_COOLDOWN_SECONDS = float(os.environ.get("DB_COOLDOWN_SECONDS", "60"))
DB_CONCURRENCY = int(os.environ.get("DB_CONCURRENCY", "2"))
DB_BATCH_SIZE = int(os.environ.get("DB_BATCH_SIZE", "500"))
DB_MAX_RETRIES = int(os.environ.get("DB_MAX_RETRIES", "3"))
DB_SIMULATE_DOWN = os.environ.get("DB_SIMULATE_DOWN", "").lower() == "true"


class DBUnavailable(Exception):
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after


class CircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_seconds: float):
        self.fail_threshold = fail_threshold
        self.cooldown_seconds = cooldown_seconds
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.cooldown_until: Optional[float] = None
        self._lock = asyncio.Lock()
    
    def _log(self, msg: str):
        logger.debug(f"[CircuitBreaker] {msg}")
    
    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.fail_threshold and self.cooldown_until is None:
                self.cooldown_until = time.time() + self.cooldown_seconds
                self._log(f"COOLDOWN ENTERED: {self.cooldown_seconds}s after {self.failure_count} failures")
    
    async def record_success(self):
        async with self._lock:
            if self.failure_count > 0:
                self._log(f"SUCCESS: Resetting after {self.failure_count} failures")
            self.failure_count = 0
            self.last_failure_time = None
            self.cooldown_until = None
    
    async def check(self) -> tuple[bool, Optional[float]]:
        async with self._lock:
            if self.cooldown_until is not None:
                now = time.time()
                if now < self.cooldown_until:
                    remaining = self.cooldown_until - now
                    return False, remaining
                else:
                    self._log("COOLDOWN EXPIRED: Allowing retry")
                    self.cooldown_until = None
            return True, None
    
    def get_backoff_delay(self) -> float:
        delay = DB_BACKOFF_BASE * (2 ** min(self.failure_count, 10))
        return min(delay, DB_BACKOFF_CAP)


class DatabaseManager:
    _instance: Optional["DatabaseManager"] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        self._pool: Optional[asyncpg.Pool] = None
        self._pool_lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(DB_FAIL_THRESHOLD, DB_COOLDOWN_SECONDS)
        self._semaphore = asyncio.Semaphore(DB_CONCURRENCY)
        self._db_url: Optional[str] = None
        self._schema_initialized = False
        self._schema_lock = asyncio.Lock()
    
    def _log(self, msg: str):
        logger.info(msg)
    
    def _get_db_url(self) -> str:
        if self._db_url is not None:
            return self._db_url

        url = os.environ.get("SUPABASE_DB_URL", "").strip()
        if not url:
            raise RuntimeError(
                "FATAL: SUPABASE_DB_URL must be set. "
                "Farm requires a Supabase PostgreSQL connection string."
            )
        self._log("Using SUPABASE_DB_URL")

        self._db_url = url
        return url
    
    async def _create_pool(self) -> asyncpg.Pool:
        if DB_SIMULATE_DOWN:
            raise DBUnavailable("DB_SIMULATE_DOWN=true: Simulating database failure", retry_after=60)
        
        allowed, retry_after = await self._circuit_breaker.check()
        if not allowed:
            wait_time = int(retry_after) if retry_after else 60
            raise DBUnavailable(
                f"Circuit breaker open: DB unavailable. Try again in {wait_time}s",
                retry_after=retry_after or 60.0
            )
        
        db_url = self._get_db_url()
        
        try:
            pool = await asyncpg.create_pool(
                db_url,
                min_size=DB_POOL_MIN,
                max_size=DB_POOL_MAX,
                timeout=DB_CONNECT_TIMEOUT,
                command_timeout=DB_COMMAND_TIMEOUT,
                max_inactive_connection_lifetime=DB_MAX_INACTIVE_LIFETIME,
                statement_cache_size=0,
            )
            await self._circuit_breaker.record_success()
            self._log(f"Pool created (min={DB_POOL_MIN}, max={DB_POOL_MAX})")
            return pool
        except Exception as e:
            await self._circuit_breaker.record_failure()
            delay = self._circuit_breaker.get_backoff_delay()
            self._log(f"Pool creation failed: {e}. Next retry in {delay}s")
            raise DBUnavailable(
                f"Database connection failed: {str(e)[:100]}. Backing off.",
                retry_after=delay
            ) from e
    
    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is not None:
            return self._pool
        
        async with self._pool_lock:
            if self._pool is not None:
                return self._pool
            
            self._pool = await self._create_pool()
            return self._pool
    
    async def close(self):
        async with self._pool_lock:
            if self._pool is not None:
                try:
                    await self._pool.close()
                    self._log("Pool closed")
                except Exception as e:
                    self._log(f"Error closing pool: {e}")
                finally:
                    self._pool = None
                    self._schema_initialized = False
    
    @asynccontextmanager
    async def connection(self):
        if DB_SIMULATE_DOWN:
            raise DBUnavailable("DB_SIMULATE_DOWN=true: Simulating database failure", retry_after=60)
        
        allowed, retry_after = await self._circuit_breaker.check()
        if not allowed:
            wait_time = int(retry_after) if retry_after else 60
            raise DBUnavailable(
                f"Circuit breaker open: DB unavailable. Try again in {wait_time}s",
                retry_after=retry_after or 60.0
            )
        
        async with self._semaphore:
            try:
                pool = await self.get_pool()
                async with pool.acquire() as conn:
                    await self._circuit_breaker.record_success()
                    yield conn
            except asyncpg.exceptions.InternalServerError as e:
                if "MaxClientsInSessionMode" in str(e):
                    await self._circuit_breaker.record_failure()
                    delay = self._circuit_breaker.get_backoff_delay()
                    raise DBUnavailable(
                        f"Supabase pooler saturated. Try again in {int(delay)}s",
                        retry_after=delay
                    ) from e
                raise
            except asyncpg.exceptions.PostgresConnectionError as e:
                await self._circuit_breaker.record_failure()
                delay = self._circuit_breaker.get_backoff_delay()
                raise DBUnavailable(
                    f"Database connection error: {str(e)[:50]}. Backing off.",
                    retry_after=delay
                ) from e
            except TimeoutError as e:
                await self._circuit_breaker.record_failure()
                delay = self._circuit_breaker.get_backoff_delay()
                raise DBUnavailable(
                    f"Database connection timeout. Backing off {int(delay)}s.",
                    retry_after=delay
                ) from e
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if "timeout" in str(e).lower():
                    await self._circuit_breaker.record_failure()
                    delay = self._circuit_breaker.get_backoff_delay()
                    raise DBUnavailable(
                        f"Database timeout: {str(e)[:50]}. Backing off.",
                        retry_after=delay
                    ) from e
                raise
    
    async def with_connection(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        async with self.connection() as conn:
            return await fn(conn, *args, **kwargs)
    
    async def ensure_schema(self):
        if self._schema_initialized:
            return
        
        async with self._schema_lock:
            if self._schema_initialized:
                return
            
            async with self.connection() as conn:
                self._log("Creating runs table...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        run_fingerprint TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        seed INTEGER NOT NULL,
                        schema_version TEXT NOT NULL,
                        enterprise_profile TEXT NOT NULL,
                        realism_profile TEXT NOT NULL,
                        scale TEXT NOT NULL,
                        tenant_id TEXT NOT NULL
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES runs(run_id),
                        sequence INTEGER DEFAULT 0,
                        snapshot_fingerprint TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        seed INTEGER NOT NULL,
                        scale TEXT NOT NULL,
                        enterprise_profile TEXT NOT NULL,
                        realism_profile TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        snapshot_json TEXT NOT NULL
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS reconciliations (
                        reconciliation_id TEXT PRIMARY KEY,
                        snapshot_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        aod_run_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        aod_payload_json TEXT NOT NULL,
                        farm_expectations_json TEXT NOT NULL,
                        report_text TEXT NOT NULL,
                        status TEXT NOT NULL,
                        analysis_json TEXT,
                        assessment_md TEXT,
                        analysis_version TEXT,
                        analysis_computed_at TEXT
                    )
                """)
                
                # Migration: Add analysis_version and analysis_computed_at columns if missing
                try:
                    await conn.execute("ALTER TABLE reconciliations ADD COLUMN IF NOT EXISTS analysis_version TEXT")
                    await conn.execute("ALTER TABLE reconciliations ADD COLUMN IF NOT EXISTS analysis_computed_at TEXT")
                    self._log("Added analysis_version and analysis_computed_at columns")
                except Exception as e:
                    self._log(f"Analysis version columns already exist or migration skipped: {e}")
                
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_fingerprint ON snapshots(snapshot_fingerprint)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_tenant ON snapshots(tenant_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_created ON snapshots(created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_run ON snapshots(run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_fingerprint ON runs(run_fingerprint)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_tenant ON runs(tenant_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliations_snapshot ON reconciliations(snapshot_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliations_aod_run ON reconciliations(aod_run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliations_created ON reconciliations(created_at DESC)")
                
                # New tables for hot/cold storage split (Phase 2)
                self._log("Creating snapshots_meta table (hot path)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots_meta (
                        snapshot_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL REFERENCES runs(run_id),
                        snapshot_fingerprint TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        seed INTEGER NOT NULL,
                        scale TEXT NOT NULL,
                        enterprise_profile TEXT NOT NULL,
                        realism_profile TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        total_assets INTEGER NOT NULL DEFAULT 0,
                        plane_counts JSONB NOT NULL DEFAULT '{}',
                        expected_summary JSONB NOT NULL DEFAULT '{}',
                        blob_size_bytes INTEGER NOT NULL DEFAULT 0,
                        blob_hash TEXT NOT NULL DEFAULT '',
                        backfill_state TEXT NOT NULL DEFAULT 'pending'
                    )
                """)
                
                # Add fabric/SOR columns to snapshots_meta if they don't exist
                try:
                    await conn.execute("ALTER TABLE snapshots_meta ADD COLUMN IF NOT EXISTS fabric_planes JSONB DEFAULT '[]'")
                    await conn.execute("ALTER TABLE snapshots_meta ADD COLUMN IF NOT EXISTS sors JSONB DEFAULT '[]'")
                    await conn.execute("ALTER TABLE snapshots_meta ADD COLUMN IF NOT EXISTS industry TEXT DEFAULT 'default'")
                except Exception as e:
                    logger.error("Migration failed adding fabric/SOR columns to snapshots_meta: %s", e, exc_info=True)
                    raise
                
                self._log("Creating snapshots_blob table (cold storage)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots_blob (
                        snapshot_id TEXT PRIMARY KEY,
                        blob TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT ''
                    )
                """)
                
                self._log("Creating reconciliation_analysis_cache table...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS reconciliation_analysis_cache (
                        reconciliation_id TEXT PRIMARY KEY,
                        snapshot_id TEXT NOT NULL,
                        snapshot_hash TEXT NOT NULL DEFAULT '',
                        light_json JSONB,
                        heavy_json JSONB,
                        computed_at TEXT NOT NULL DEFAULT ''
                    )
                """)
                
                self._log("Creating jobs table...")
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
                
                self._log("Creating stress_test_runs table...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS stress_test_runs (
                        run_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        target_url TEXT NOT NULL,
                        scale TEXT NOT NULL,
                        workflow_count INTEGER NOT NULL,
                        chaos_rate REAL NOT NULL,
                        seed INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        fleet_summary JSONB NOT NULL DEFAULT '{}',
                        scenario_summary JSONB NOT NULL DEFAULT '{}',
                        expected JSONB NOT NULL DEFAULT '{}',
                        validation JSONB NOT NULL DEFAULT '{}',
                        execution_result JSONB,
                        duration_ms INTEGER,
                        -- AOA integration fields (added for FARM-AOA protocol)
                        aoa_verdict TEXT,
                        aoa_analysis JSONB,
                        aoa_validation JSONB,
                        comparative_analysis JSONB,
                        dashboard_metrics JSONB DEFAULT '[]'
                    )
                """)

                # Migration: Add AOA columns if missing (for existing databases)
                self._log("Running stress_test_runs schema migration...")
                for column, coltype, default in [
                    ("aoa_verdict", "TEXT", None),
                    ("aoa_analysis", "JSONB", None),
                    ("aoa_validation", "JSONB", None),
                    ("comparative_analysis", "JSONB", None),
                    ("dashboard_metrics", "JSONB", "'[]'"),
                ]:
                    try:
                        default_clause = f" DEFAULT {default}" if default else ""
                        await conn.execute(f"""
                            ALTER TABLE stress_test_runs
                            ADD COLUMN IF NOT EXISTS {column} {coltype}{default_clause}
                        """)
                    except Exception as e:
                        logger.debug("ADD COLUMN %s on stress_test_runs skipped (likely already exists): %s", column, e)
                
                self._log("Creating sim_agents table (AOA simulation)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS sim_agents (
                        agent_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        agent_type TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        capabilities JSONB NOT NULL DEFAULT '[]',
                        tools JSONB NOT NULL DEFAULT '[]',
                        policy_template TEXT NOT NULL DEFAULT 'standard',
                        created_at TEXT NOT NULL,
                        last_active_at TEXT
                    )
                """)
                
                self._log("Creating sim_agent_runs table (AOA simulation)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS sim_agent_runs (
                        run_id TEXT PRIMARY KEY,
                        agent_id TEXT NOT NULL,
                        workflow_id TEXT,
                        status TEXT NOT NULL DEFAULT 'pending',
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        duration_ms INTEGER,
                        cost_usd REAL DEFAULT 0.0,
                        budget_limit_usd REAL DEFAULT 1.0,
                        tasks_completed INTEGER DEFAULT 0,
                        tasks_total INTEGER DEFAULT 0,
                        error_message TEXT
                    )
                """)
                
                self._log("Creating sim_agent_approvals table (AOA simulation)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS sim_agent_approvals (
                        approval_id TEXT PRIMARY KEY,
                        agent_id TEXT NOT NULL,
                        run_id TEXT,
                        approval_type TEXT NOT NULL DEFAULT 'policy_gate',
                        is_approved BOOLEAN NOT NULL DEFAULT FALSE,
                        requested_at TEXT NOT NULL,
                        resolved_at TEXT,
                        resolver TEXT
                    )
                """)
                
                self._log("Creating sim_state table (simulation control)...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS sim_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)
                
                # Indexes for new tables
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_tenant_created ON snapshots_meta(tenant_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_fingerprint ON snapshots_meta(snapshot_fingerprint)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_run ON snapshots_meta(run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_recon_cache_snapshot ON reconciliation_analysis_cache(snapshot_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_stress_test_runs_created ON stress_test_runs(created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_stress_test_runs_status ON stress_test_runs(status)")
                
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_agents_active ON sim_agents(is_active)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_agent_runs_agent ON sim_agent_runs(agent_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_agent_runs_status ON sim_agent_runs(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_agent_runs_started ON sim_agent_runs(started_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_agent_approvals_agent ON sim_agent_approvals(agent_id)")
                
                self._log("Creating ground_truth_manifests table...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ground_truth_manifests (
                        run_id TEXT PRIMARY KEY,
                        seed INTEGER NOT NULL,
                        created_at TEXT NOT NULL,
                        manifest_json JSONB NOT NULL,
                        source_systems JSONB NOT NULL DEFAULT '[]',
                        record_counts JSONB NOT NULL DEFAULT '{}'
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_gt_manifests_created ON ground_truth_manifests(created_at DESC)")

                try:
                    await conn.execute("ALTER TABLE ground_truth_manifests ADD COLUMN IF NOT EXISTS dcl_push_results JSONB DEFAULT '[]'")
                except Exception as e:
                    logger.debug("ADD COLUMN dcl_push_results on ground_truth_manifests skipped (likely already exists): %s", e)

                self._log("Creating manifest_runs table...")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS manifest_runs (
                        farm_run_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        aam_run_id TEXT,
                        pipe_id TEXT NOT NULL,
                        dcl_run_id TEXT,
                        tenant_id TEXT NOT NULL,
                        snapshot_name TEXT NOT NULL,
                        source_system TEXT NOT NULL,
                        category TEXT,
                        generator_key TEXT NOT NULL,
                        status TEXT NOT NULL,
                        rows_generated INTEGER DEFAULT 0,
                        rows_pushed INTEGER DEFAULT 0,
                        rows_accepted INTEGER,
                        dcl_status_code INTEGER,
                        error_type TEXT,
                        error_message TEXT,
                        schema_drift BOOLEAN DEFAULT FALSE,
                        created_at TEXT NOT NULL,
                        elapsed_ms INTEGER,
                        push_result_json JSONB
                    )
                """)
                # Migration: add aam_run_id column if table predates it (must run before index creation)
                try:
                    await conn.execute("ALTER TABLE manifest_runs ADD COLUMN IF NOT EXISTS aam_run_id TEXT")
                except Exception as e:
                    logger.error("Migration failed adding aam_run_id to manifest_runs: %s", e, exc_info=True)
                    raise

                # Migration: add rows_pushed column (tracks post-truncation count, distinct from rows_generated)
                try:
                    await conn.execute("ALTER TABLE manifest_runs ADD COLUMN IF NOT EXISTS rows_pushed INTEGER DEFAULT 0")
                except Exception as e:
                    logger.error("Migration failed adding rows_pushed to manifest_runs: %s", e, exc_info=True)
                    raise

                await conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_runs_run_id ON manifest_runs(run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_runs_aam_run_id ON manifest_runs(aam_run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_runs_tenant_created ON manifest_runs(tenant_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_runs_status ON manifest_runs(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_runs_pipe_id ON manifest_runs(pipe_id)")

                self._schema_initialized = True
                self._log("Schema initialized")
    
    def is_healthy(self) -> tuple[bool, str]:
        if DB_SIMULATE_DOWN:
            return False, "DB_SIMULATE_DOWN=true"
        if self._pool is None:
            return False, "Pool not initialized"
        if self._circuit_breaker.cooldown_until is not None:
            remaining = self._circuit_breaker.cooldown_until - time.time()
            if remaining > 0:
                return False, f"Circuit breaker cooldown ({int(remaining)}s remaining)"
        # Check pool metrics — asyncpg pool is async so we can't execute a
        # test query from this sync method, but pool exhaustion is detectable.
        pool_size = self._pool.get_size()
        idle_size = self._pool.get_idle_size()
        max_size = self._pool.get_max_size()
        if pool_size >= max_size and idle_size == 0:
            return False, (
                f"DB pool exhausted: {pool_size}/{max_size} connections in use, "
                f"0 idle — all writes will block or fail"
            )
        return True, f"Healthy (pool: {pool_size}/{max_size}, idle: {idle_size})"

    async def is_healthy_async(self) -> tuple[bool, str]:
        """Async health check — executes a real test query against the database.

        Use this from async endpoints (e.g. /api/health) to verify actual DB
        connectivity, not just pool existence. Catches missing tables, rotated
        credentials, and exhausted connection pools.
        """
        if DB_SIMULATE_DOWN:
            return False, "DB_SIMULATE_DOWN=true"
        if self._pool is None:
            return False, "Pool not initialized"
        if self._circuit_breaker.cooldown_until is not None:
            remaining = self._circuit_breaker.cooldown_until - time.time()
            if remaining > 0:
                return False, f"Circuit breaker cooldown ({int(remaining)}s remaining)"
        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    return False, f"DB test query returned unexpected result: {result}"
        except Exception as e:
            return False, f"DB unreachable: {e}"
        pool_size = self._pool.get_size()
        max_size = self._pool.get_max_size()
        idle_size = self._pool.get_idle_size()
        return True, f"Healthy (pool: {pool_size}/{max_size}, idle: {idle_size})"


db = DatabaseManager()


async def get_pool() -> asyncpg.Pool:
    return await db.get_pool()


@asynccontextmanager
async def connection():
    async with db.connection() as conn:
        yield conn


async def with_connection(fn: Callable[..., Any], *args, **kwargs) -> Any:
    return await db.with_connection(fn, *args, **kwargs)


async def ensure_schema():
    await db.ensure_schema()


async def close_pool():
    await db.close()


def is_healthy() -> tuple[bool, str]:
    return db.is_healthy()


async def is_healthy_async() -> tuple[bool, str]:
    return await db.is_healthy_async()


import random as _random

def _jitter(delay: float, factor: float = 0.25) -> float:
    """Add random jitter to delay (0.75x to 1.25x)."""
    return delay * (1 + ((_random.random() - 0.5) * 2 * factor))


async def execute_with_retry(
    query: str,
    *args,
    max_retries: int = DB_MAX_RETRIES,
    statement_timeout_seconds: int = DB_STATEMENT_TIMEOUT,
) -> Any:
    """Execute a single query with retry and per-statement timeout."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            async with connection() as conn:
                await conn.execute(f"SET statement_timeout = '{statement_timeout_seconds}s'")
                result = await conn.execute(query, *args)
                return result
        except DBUnavailable as e:
            last_error = e
            if attempt < max_retries:
                delay = _jitter(DB_BACKOFF_BASE * (2 ** attempt))
                logger.warning(f"[Retry] Attempt {attempt + 1}/{max_retries + 1} failed, retrying in {delay:.1f}s: {e.message}")
                await asyncio.sleep(delay)
            else:
                raise
        except asyncio.TimeoutError as e:
            last_error = e
            delay = _jitter(DB_BACKOFF_BASE * (2 ** attempt))
            if attempt < max_retries:
                logger.warning(f"[Retry] Timeout on attempt {attempt + 1}/{max_retries + 1}, retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
            else:
                raise DBUnavailable(f"Query timeout after {max_retries + 1} attempts", retry_after=delay)
    raise last_error or DBUnavailable("Query failed after retries")


async def batch_insert(
    table: str,
    columns: list[str],
    rows: list[tuple],
    batch_size: int = DB_BATCH_SIZE,
    on_conflict: str = "",
    statement_timeout_seconds: int = DB_STATEMENT_TIMEOUT,
) -> int:
    """
    Insert rows in batches with commit per batch.
    Each batch uses its own connection and transaction to avoid holding pooler session.
    
    Returns total rows inserted.
    """
    if not rows:
        return 0
    
    total_inserted = 0
    num_batches = (len(rows) + batch_size - 1) // batch_size
    
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(rows))
        batch_rows = rows[start_idx:end_idx]
        
        # Retry logic for each batch
        last_error = None
        for attempt in range(DB_MAX_RETRIES + 1):
            try:
                async with connection() as conn:
                    await conn.execute(f"SET statement_timeout = '{statement_timeout_seconds}s'")
                    
                    # Use copy_records_to_table for bulk insert (fastest)
                    await conn.copy_records_to_table(
                        table,
                        records=batch_rows,
                        columns=columns,
                    )
                    total_inserted += len(batch_rows)
                    break  # Success
                    
            except DBUnavailable as e:
                last_error = e
                if attempt < DB_MAX_RETRIES:
                    delay = _jitter(DB_BACKOFF_BASE * (2 ** attempt))
                    logger.warning(f"[BatchInsert] Batch {batch_num + 1}/{num_batches} attempt {attempt + 1} failed, retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[BatchInsert] Batch {batch_num + 1}/{num_batches} failed after {DB_MAX_RETRIES + 1} attempts")
                    raise
                    
            except asyncio.TimeoutError:
                last_error = DBUnavailable("Batch insert timeout")
                if attempt < DB_MAX_RETRIES:
                    delay = _jitter(DB_BACKOFF_BASE * (2 ** attempt))
                    logger.warning(f"[BatchInsert] Batch {batch_num + 1}/{num_batches} timeout, retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                else:
                    raise DBUnavailable(f"Batch insert timeout after {DB_MAX_RETRIES + 1} attempts")
    
    return total_inserted


async def insert_single_row(
    table: str,
    columns: list[str],
    values: tuple,
    statement_timeout_seconds: int = DB_STATEMENT_TIMEOUT,
) -> None:
    """Insert a single row with retry and statement timeout."""
    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))
    col_str = ", ".join(columns)
    query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
    
    await execute_with_retry(query, *values, statement_timeout_seconds=statement_timeout_seconds)


async def save_ground_truth_manifest(
    run_id: str,
    seed: int,
    created_at: str,
    manifest: dict,
    source_systems: list,
    record_counts: dict,
) -> None:
    """Persist a ground truth manifest for later reconciliation."""
    import json
    async with connection() as conn:
        await conn.execute("""
            INSERT INTO ground_truth_manifests (run_id, seed, created_at, manifest_json, source_systems, record_counts)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb)
            ON CONFLICT (run_id) DO UPDATE SET
                manifest_json = EXCLUDED.manifest_json,
                source_systems = EXCLUDED.source_systems,
                record_counts = EXCLUDED.record_counts
        """, run_id, seed, created_at, json.dumps(manifest), json.dumps(source_systems), json.dumps(record_counts))


async def load_ground_truth_manifest(run_id: str) -> dict | None:
    """Load a persisted ground truth manifest by run_id. Returns None if not found."""
    import json
    async with connection() as conn:
        row = await conn.fetchrow(
            "SELECT manifest_json, seed, created_at, source_systems, record_counts FROM ground_truth_manifests WHERE run_id = $1",
            run_id,
        )
        if not row:
            return None
        return {
            "manifest": json.loads(row["manifest_json"]) if isinstance(row["manifest_json"], str) else row["manifest_json"],
            "seed": row["seed"],
            "created_at": row["created_at"],
            "source_systems": json.loads(row["source_systems"]) if isinstance(row["source_systems"], str) else row["source_systems"],
            "record_counts": json.loads(row["record_counts"]) if isinstance(row["record_counts"], str) else row["record_counts"],
        }


async def update_manifest_push_results(run_id: str, push_results: list) -> None:
    """Store DCL push correlation keys for a manifest run."""
    import json
    async with connection() as conn:
        await conn.execute(
            "UPDATE ground_truth_manifests SET dcl_push_results = $1::jsonb WHERE run_id = $2",
            json.dumps(push_results), run_id,
        )


async def list_ground_truth_runs(limit: int = 50) -> list[dict]:
    """List recent ground truth manifest runs."""
    import json
    async with connection() as conn:
        rows = await conn.fetch(
            "SELECT run_id, seed, created_at, source_systems, record_counts FROM ground_truth_manifests ORDER BY created_at DESC LIMIT $1",
            limit,
        )
        return [
            {
                "run_id": r["run_id"],
                "seed": r["seed"],
                "created_at": r["created_at"],
                "source_systems": json.loads(r["source_systems"]) if isinstance(r["source_systems"], str) else r["source_systems"],
                "record_counts": json.loads(r["record_counts"]) if isinstance(r["record_counts"], str) else r["record_counts"],
            }
            for r in rows
        ]


async def save_manifest_run(
    farm_run_id: str,
    run_id: str,
    pipe_id: str,
    tenant_id: str,
    snapshot_name: str,
    source_system: str,
    generator_key: str,
    status: str,
    created_at: str,
    category: str | None = None,
    aam_run_id: str | None = None,
    dcl_run_id: str | None = None,
    rows_generated: int = 0,
    rows_pushed: int = 0,
    rows_accepted: int | None = None,
    dcl_status_code: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    schema_drift: bool = False,
    elapsed_ms: int | None = None,
    push_result_json: dict | None = None,
) -> None:
    """Persist a manifest-driven execution run with full provenance."""
    import json
    # Lazy schema init — if DB was unavailable at boot, ensure table exists now
    await db.ensure_schema()
    async with connection() as conn:
        await conn.execute("""
            INSERT INTO manifest_runs (
                farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
                tenant_id, snapshot_name, source_system, category, generator_key,
                status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
                error_type, error_message, schema_drift,
                created_at, elapsed_ms, push_result_json
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15,
                $16, $17, $18,
                $19, $20, $21::jsonb
            )
            ON CONFLICT (farm_run_id) DO UPDATE SET
                status = EXCLUDED.status,
                aam_run_id = EXCLUDED.aam_run_id,
                dcl_run_id = EXCLUDED.dcl_run_id,
                rows_pushed = EXCLUDED.rows_pushed,
                rows_accepted = EXCLUDED.rows_accepted,
                dcl_status_code = EXCLUDED.dcl_status_code,
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                schema_drift = EXCLUDED.schema_drift,
                elapsed_ms = EXCLUDED.elapsed_ms,
                push_result_json = EXCLUDED.push_result_json
        """,
            farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
            tenant_id, snapshot_name, source_system, category, generator_key,
            status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
            error_type, error_message, schema_drift,
            created_at, elapsed_ms, json.dumps(push_result_json) if push_result_json else None,
        )


async def get_completed_run_for_pipe(run_id: str, pipe_id: str) -> dict | None:
    """Check if a completed run already exists for this (run_id, pipe_id) pair.

    Used as an idempotency guard: if AAM double-dispatches the same manifest
    (e.g. due to batch timeout fallback), Farm skips re-execution and returns
    the cached result instead of generating + pushing duplicate data to DCL.
    """
    await db.ensure_schema()
    async with connection() as conn:
        row = await conn.fetchrow(
            """SELECT farm_run_id, status, rows_accepted, dcl_status_code, elapsed_ms,
                      rows_generated, rows_pushed, source_system
               FROM manifest_runs
               WHERE run_id = $1 AND pipe_id = $2 AND status = 'completed'
               ORDER BY created_at DESC LIMIT 1""",
            run_id, pipe_id,
        )
        return dict(row) if row else None


async def get_manifest_run(farm_run_id: str) -> dict | None:
    """Load a single manifest run by farm_run_id."""
    import json
    async with connection() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM manifest_runs WHERE farm_run_id = $1",
            farm_run_id,
        )
        if not row:
            return None
        result = dict(row)
        if result.get("push_result_json") and isinstance(result["push_result_json"], str):
            result["push_result_json"] = json.loads(result["push_result_json"])
        return result


async def list_manifest_runs(
    tenant_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """List manifest runs with optional filters, ordered by created_at DESC."""
    import json
    await db.ensure_schema()
    conditions = []
    params: list = []
    idx = 1

    if tenant_id:
        conditions.append(f"tenant_id = ${idx}")
        params.append(tenant_id)
        idx += 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([limit, offset])

    query = f"""
        SELECT farm_run_id, run_id, aam_run_id, pipe_id, dcl_run_id,
               tenant_id, snapshot_name, source_system, category, generator_key,
               status, rows_generated, rows_pushed, rows_accepted, dcl_status_code,
               error_type, error_message, schema_drift,
               created_at, elapsed_ms
        FROM manifest_runs
        {where}
        ORDER BY created_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """

    async with connection() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(r) for r in rows]
