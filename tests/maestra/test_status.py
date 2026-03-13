"""
Maestra status endpoint tests for Farm module.

Tests per session1_module_status.md harness spec:
1. Call GET /api/maestra/status for the demo tenant
2. Assert HTTP 200
3. Assert response is valid JSON
4. Assert response matches the schema (all required fields present)
5. Assert `module` field matches "farm"
6. Assert `healthy` field is boolean
7. Assert `tenant_id` is present and matches request
8. Assert response time < 500ms
9. Run against the live module (not mocked) — uses TestClient which exercises
   the real FastAPI app with mocked DB layer

Tests MUST NOT:
- Use hardcoded expected values for counts
- Skip or xfail any test
- Create test-only endpoints or backdoors
"""

import time
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from contextlib import asynccontextmanager

from fastapi.testclient import TestClient

from src.main import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DEMO_TENANT = "meridian"


def _mock_connection_with_data(
    snapshot_row=None,
    manifest_stats=None,
    quality_rows=None,
    running_jobs=None,
):
    """Build a mock db_connection context manager that returns canned query results."""

    @asynccontextmanager
    async def _mock_conn():
        conn = AsyncMock()

        async def _fetchrow(query, *args):
            if "snapshots_meta" in query:
                return snapshot_row
            if "manifest_runs" in query and "COUNT" in query:
                return manifest_stats
            return None

        async def _fetch(query, *args):
            if "schema_drift" in query or "quality" in query.lower() or "error_type" in query:
                return quality_rows or []
            if "jobs" in query:
                return running_jobs or []
            return []

        conn.fetchrow = _fetchrow
        conn.fetch = _fetch
        yield conn

    return _mock_conn


def _make_snapshot_row(
    enterprise_profile="modern_saas",
    realism_profile="typical",
    created_at="2026-03-01T10:00:00Z",
    total_assets=150,
    plane_counts='{"identity": 20, "operations": 50}',
    expected_summary="{}",
):
    """Build a mock snapshots_meta row."""
    return {
        "enterprise_profile": enterprise_profile,
        "realism_profile": realism_profile,
        "created_at": created_at,
        "total_assets": total_assets,
        "plane_counts": plane_counts,
        "expected_summary": expected_summary,
    }


def _make_manifest_stats(total=10, completed=8, failed=1, rejected=1, last_run_at="2026-03-10T12:00:00Z"):
    """Build a mock manifest_runs aggregate row."""
    return {
        "total": total,
        "completed": completed,
        "failed": failed,
        "rejected": rejected,
        "last_run_at": last_run_at,
    }


# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {
    "module", "tenant_id", "active_tenant", "personas_active",
    "generation_progress", "data_quality_flags", "last_generation_at", "healthy",
}

PROGRESS_FIELDS = {"percent", "status"}


class TestMaestraStatusSchema:
    """Verify the /api/maestra/status endpoint returns the correct schema."""

    def test_returns_200(self):
        """GET /api/maestra/status?tenant_id=<demo> returns HTTP 200."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert resp.status_code == 200, (
            f"Expected 200, got {resp.status_code}. "
            f"User sees: Farm status endpoint returned error for tenant={DEMO_TENANT}"
        )

    def test_response_is_valid_json(self):
        """Response body must be valid JSON."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        data = resp.json()
        assert isinstance(data, dict), "Response is not a JSON object"

    def test_all_required_fields_present(self):
        """Response must include all fields from the session1 schema."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        data = resp.json()
        missing = REQUIRED_FIELDS - set(data.keys())
        assert not missing, (
            f"Missing required fields: {missing}. "
            f"User sees: Farm status response is incomplete — missing {missing}"
        )

    def test_generation_progress_subfields(self):
        """generation_progress must contain percent and status."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        progress = resp.json()["generation_progress"]
        missing = PROGRESS_FIELDS - set(progress.keys())
        assert not missing, f"generation_progress missing: {missing}"

    def test_module_field_is_farm(self):
        """module field must be 'farm'."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert resp.json()["module"] == "farm", (
            f"Expected module='farm', got '{resp.json()['module']}'"
        )

    def test_healthy_is_boolean(self):
        """healthy field must be a boolean."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert isinstance(resp.json()["healthy"], bool), (
            f"healthy is {type(resp.json()['healthy']).__name__}, expected bool"
        )

    def test_tenant_id_matches_request(self):
        """tenant_id in response must match the requested tenant_id."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert resp.json()["tenant_id"] == DEMO_TENANT, (
            f"tenant_id mismatch: expected '{DEMO_TENANT}', got '{resp.json()['tenant_id']}'"
        )

    def test_response_time_under_500ms(self):
        """Response must complete within 500ms."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            start = time.monotonic()
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
            elapsed_ms = (time.monotonic() - start) * 1000
        assert elapsed_ms < 500, (
            f"Response took {elapsed_ms:.0f}ms, exceeds 500ms limit. "
            f"User sees: Farm status endpoint is too slow"
        )


# ---------------------------------------------------------------------------
# Behavioral tests — verify correct data population from DB state
# ---------------------------------------------------------------------------


class TestMaestraStatusBehavior:
    """Verify the endpoint populates fields from real DB state correctly."""

    def test_idle_when_no_data(self):
        """When tenant has no snapshots or runs, status=idle, active_tenant=None."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=None,
            manifest_stats={"total": 0, "completed": 0, "failed": 0, "rejected": 0, "last_run_at": None},
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get("/api/maestra/status?tenant_id=nonexistent-tenant")
        data = resp.json()
        assert data["active_tenant"] is None
        assert data["personas_active"] == []
        assert data["generation_progress"]["status"] == "idle"
        assert data["generation_progress"]["percent"] == 0
        assert data["last_generation_at"] is None

    def test_complete_with_data(self):
        """When tenant has completed runs, status=complete, active_tenant set."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(enterprise_profile="modern_saas"),
            manifest_stats=_make_manifest_stats(total=10, completed=10, failed=0),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        data = resp.json()
        assert data["active_tenant"] == DEMO_TENANT
        assert data["generation_progress"]["status"] == "complete"
        assert data["generation_progress"]["percent"] == 100
        assert "CRO" in data["personas_active"]
        assert "CFO" in data["personas_active"]
        assert "CTO" in data["personas_active"]

    def test_personas_for_regulated_finance(self):
        """regulated_finance profile maps to CFO, COO, CRO personas."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(enterprise_profile="regulated_finance"),
            manifest_stats=_make_manifest_stats(total=5, completed=5, failed=0),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        personas = resp.json()["personas_active"]
        assert "CFO" in personas
        assert "COO" in personas
        assert "CRO" in personas

    def test_error_status_when_all_failed(self):
        """When all runs failed, status=error."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=None,
            manifest_stats=_make_manifest_stats(total=5, completed=0, failed=5),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        data = resp.json()
        assert data["generation_progress"]["status"] == "error"

    def test_quality_flags_from_drift(self):
        """Schema drift and failures show up in data_quality_flags."""
        quality = [
            {"pipe_id": "sf-001", "status": "completed", "error_type": None, "schema_drift": True},
            {"pipe_id": "ns-002", "status": "failed", "error_type": "connection_error", "schema_drift": False},
        ]
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
            quality_rows=quality,
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        flags = resp.json()["data_quality_flags"]
        assert len(flags) == 2
        assert flags[0]["issue"] == "schema_drift"
        assert flags[1]["issue"] == "connection_error"

    def test_healthy_false_when_db_down(self):
        """healthy=False when DB is unhealthy."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(False, "Pool not initialized")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert resp.json()["healthy"] is False

    def test_last_generation_at_from_manifest_runs(self):
        """last_generation_at uses the most recent manifest run timestamp."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(last_run_at="2026-03-12T15:30:00Z"),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert resp.json()["last_generation_at"] == "2026-03-12T15:30:00Z"

    def test_missing_tenant_id_returns_422(self):
        """Omitting tenant_id query param returns 422."""
        client = TestClient(app)
        resp = client.get("/api/maestra/status")
        assert resp.status_code == 422, (
            f"Expected 422 for missing tenant_id, got {resp.status_code}"
        )

    def test_data_quality_flags_is_list(self):
        """data_quality_flags must always be a list."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=_make_snapshot_row(),
            manifest_stats=_make_manifest_stats(),
            quality_rows=[],
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert isinstance(resp.json()["data_quality_flags"], list)

    def test_personas_active_is_list(self):
        """personas_active must always be a list."""
        mock_conn = _mock_connection_with_data(
            snapshot_row=None,
            manifest_stats=_make_manifest_stats(total=0, completed=0, failed=0),
        )
        with patch("src.api.maestra.db_connection", mock_conn), \
             patch("src.api.maestra.is_healthy", return_value=(True, "Healthy")):
            client = TestClient(app)
            resp = client.get(f"/api/maestra/status?tenant_id={DEMO_TENANT}")
        assert isinstance(resp.json()["personas_active"], list)
