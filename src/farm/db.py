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
import time
from typing import Optional, TypeVar, Callable, Any
from contextlib import asynccontextmanager
from functools import wraps

import asyncpg

T = TypeVar('T')

DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", "0"))
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "2"))
DB_CONNECT_TIMEOUT = float(os.environ.get("DB_CONNECT_TIMEOUT", "30"))
DB_COMMAND_TIMEOUT = float(os.environ.get("DB_COMMAND_TIMEOUT", "15"))
DB_MAX_INACTIVE_LIFETIME = float(os.environ.get("DB_MAX_INACTIVE_LIFETIME", "10"))
DB_BACKOFF_BASE = float(os.environ.get("DB_BACKOFF_BASE", "10"))
DB_BACKOFF_CAP = float(os.environ.get("DB_BACKOFF_CAP", "120"))
DB_FAIL_THRESHOLD = int(os.environ.get("DB_FAIL_THRESHOLD", "8"))
DB_COOLDOWN_SECONDS = float(os.environ.get("DB_COOLDOWN_SECONDS", "180"))
DB_CONCURRENCY = int(os.environ.get("DB_CONCURRENCY", "2"))
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
        print(f"[DB:CircuitBreaker] {msg}")
    
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
        print(f"[DB] {msg}")
    
    def _get_db_url(self) -> str:
        if self._db_url is not None:
            return self._db_url
        
        ignore_replit = os.environ.get("IGNORE_REPLIT_DB", "").lower() == "true"
        supabase_url = os.environ.get("SUPABASE_DB_URL", "")
        database_url = os.environ.get("DATABASE_URL", "")
        
        url = None
        if supabase_url:
            url = supabase_url
            self._log("Using SUPABASE_DB_URL")
        elif database_url:
            if ignore_replit and "replit" in database_url.lower():
                raise RuntimeError(
                    "FATAL: IGNORE_REPLIT_DB=true but only Replit DATABASE_URL found. "
                    "Set SUPABASE_DB_URL or unset IGNORE_REPLIT_DB."
                )
            url = database_url
            self._log("Using DATABASE_URL")
        else:
            raise RuntimeError("FATAL: No database URL configured. Set SUPABASE_DB_URL or DATABASE_URL.")
        
        if "supabase" in url.lower() and "pgbouncer=true" not in url.lower():
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}pgbouncer=true"
            self._log("Added pgbouncer=true for transaction pooling")
        
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
                        assessment_md TEXT
                    )
                """)
                
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
                
                # Indexes for new tables
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_tenant_created ON snapshots_meta(tenant_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_fingerprint ON snapshots_meta(snapshot_fingerprint)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_meta_run ON snapshots_meta(run_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_recon_cache_snapshot ON reconciliation_analysis_cache(snapshot_id)")
                
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
        return True, "Healthy"


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
