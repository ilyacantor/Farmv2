"""
Shared test configuration.

Loads environment variables from .env file so tests can access
SUPABASE_DB_URL and other required config without python-dotenv.

Fixes asyncpg pool lifecycle for TestClient: Starlette's TestClient runs
ASGI requests synchronously via anyio.from_thread.run, which can leave
asyncpg connection protocol state unsettled between requests. Setting
min_size=0 forces fresh connections per acquire instead of reusing pooled
connections with potentially stale protocol state.
"""
import asyncio
import os
from pathlib import Path

import pytest


def _load_env_file():
    """Load only DB-related env vars from .env file.

    Only loads SUPABASE_DB_URL and DB_* vars needed for database connectivity.
    Other vars (AOD_URL, AOD_BASE_URL, etc.) are left unset so tests can
    control them via monkeypatch without interference.
    """
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return

    # Only load vars needed for DB connectivity
    db_prefixes = ("SUPABASE_DB_URL", "DB_", "DATABASE_URL")

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key and key not in os.environ and any(key.startswith(p) for p in db_prefixes):
                os.environ[key] = value


# Load before any test imports trigger DB connections
_load_env_file()

# Force min_size=0 for test pool so connections aren't reused with stale state
os.environ.setdefault("DB_POOL_MIN", "0")

# Known test tenant IDs used in test_farm.py — cleaned up before each session
_TEST_TENANTS = (
    "APICorp", "FetchCorp", "ListCorp", "FilterCorp", "LimitCorp",
    "DualTestCorp", "FingerprintCorp", "SchemaCorp",
    "ReconcileCorp", "AutoReconcileCorp",
)


def _cleanup_test_data():
    """Delete stale test data from Supabase so deduplication doesn't collide
    with data from previous test runs."""
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if not db_url:
        return

    async def _do_cleanup():
        import asyncpg
        conn = await asyncpg.connect(db_url, timeout=10, statement_cache_size=0)
        try:
            for tenant in _TEST_TENANTS:
                # Get snapshot_ids for this tenant
                rows = await conn.fetch(
                    "SELECT snapshot_id FROM snapshots WHERE tenant_id = $1", tenant
                )
                sids = [r["snapshot_id"] for r in rows]
                if not sids:
                    continue

                # Delete from dependent tables first
                for sid in sids:
                    await conn.execute("DELETE FROM snapshots_blob WHERE snapshot_id = $1", sid)
                    await conn.execute("DELETE FROM snapshots_meta WHERE snapshot_id = $1", sid)

                # Delete snapshots
                await conn.execute("DELETE FROM snapshots WHERE tenant_id = $1", tenant)

                # Delete orphaned runs
                run_ids = await conn.fetch(
                    "SELECT run_id FROM runs WHERE tenant_id = $1", tenant
                )
                for r in run_ids:
                    # Only delete runs that have no remaining snapshots
                    remaining = await conn.fetchval(
                        "SELECT count(*) FROM snapshots WHERE run_id = $1", r["run_id"]
                    )
                    if remaining == 0:
                        await conn.execute("DELETE FROM runs WHERE run_id = $1", r["run_id"])
            # Clean up manifest_runs from test executions
            # Test manifests use run_id "aam-run-001" and pipe_id "sf-crm-001-*"
            await conn.execute(
                "DELETE FROM manifest_runs WHERE run_id = $1", "aam-run-001"
            )
        finally:
            await conn.close()

    try:
        asyncio.run(_do_cleanup())
    except Exception:
        pass  # DB might be unreachable — tests will fail on their own


# Clean up stale test data once at session start
_cleanup_test_data()


@pytest.fixture(autouse=True)
def _reset_db_pool():
    """Reset the asyncpg pool between tests.

    The module-level `db` object in src.farm.db is the actual DatabaseManager
    used by all routes. We reset its pool between tests and pre-mark schema
    as initialized so lifespan doesn't create connections on the wrong loop.
    """
    try:
        from src.farm import db as db_module
        db_module.db._pool = None
        db_module.db._schema_initialized = True
    except ImportError:
        pass
    yield
    try:
        from src.farm import db as db_module
        db_module.db._pool = None
    except ImportError:
        pass
