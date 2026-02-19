"""
Tests for the manifest-driven execution path (AAM → Farm → DCL).

Covers:
1. JobManifest Pydantic model validation
2. Manifest intake endpoint (POST /api/farm/manifest-intake)
3. DCL push with 422 NO_MATCHING_PIPE handling
4. DCL push with schema_drift handling
5. Correlation key completeness
6. Self-directed push enhancements in the orchestrator
"""

import json
import logging
import pytest
import pytest_asyncio
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from pydantic import ValidationError

from src.models.manifest import (
    JobManifest,
    SourceSpec,
    TargetSpec,
    TransformSpec,
    RunLimits,
    DCLPushResult,
    ManifestExecutionResult,
)
from src.api.manifest_intake import (
    _compute_schema_hash,
    _find_pipe_data,
    _push_to_dcl,
    _GENERATOR_REGISTRY,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_manifest(**overrides) -> JobManifest:
    """Build a valid JobManifest with sensible defaults, allowing overrides."""
    defaults = {
        "manifest_version": "1.0",
        "run_id": "aam-run-001",
        "farm_verification": False,
        "source": {
            "pipe_id": "sf-crm-001-opportunities",
            "system": "salesforce",
            "adapter": "rest_api",
            "endpoint_ref": {"pipe_name": "opportunities"},
        },
        "target": {
            "dcl_url": "http://localhost:8000/api/dcl/ingest",
            "tenant_id": "aos-demo",
            "snapshot_name": "test_snap",
        },
        "provenance": {
            "run_timestamp": "2026-02-16T10:00:00Z",
            "triggered_by": "test",
        },
        "limits": {
            "max_rows": 100000,
            "timeout_seconds": 30,
            "retry_count": 2,
        },
    }
    # Deep merge overrides
    for key, val in overrides.items():
        if isinstance(val, dict) and key in defaults and isinstance(defaults[key], dict):
            defaults[key] = {**defaults[key], **val}
        else:
            defaults[key] = val
    return JobManifest(**defaults)


def _make_dcl_success_response(pipe_id: str = "sf-crm-001-opportunities") -> dict:
    """Build a DCL success response (HTTP 200)."""
    return {
        "dcl_run_id": "dcl-uuid-001",
        "pipe_id": pipe_id,
        "rows_accepted": 20,
        "schema_drift": False,
        "drift_fields": [],
        "matched_schema": True,
        "schema_fields": ["Id", "Name", "Amount", "StageName"],
        "timestamp": "2026-02-16T10:00:01Z",
    }


def _make_dcl_422_response(pipe_id: str = "sf-crm-001-opportunities") -> dict:
    """Build a DCL NO_MATCHING_PIPE rejection (HTTP 422)."""
    return {
        "error": "NO_MATCHING_PIPE",
        "pipe_id": pipe_id,
        "message": f"No schema blueprint found for pipe_id: {pipe_id}.",
        "hint": "Ensure AAM has exported pipe definitions via /export-pipes",
        "available_pipes": ["ns-erp-001-invoices", "cb-billing-001-subscriptions"],
        "timestamp": "2026-02-16T10:00:01Z",
    }


def _make_dcl_drift_response(pipe_id: str = "sf-crm-001-opportunities") -> dict:
    """Build a DCL success response with schema_drift=True."""
    return {
        "dcl_run_id": "dcl-uuid-002",
        "pipe_id": pipe_id,
        "rows_accepted": 20,
        "schema_drift": True,
        "drift_fields": ["NewField__c", "RemovedField"],
        "matched_schema": True,
        "schema_fields": ["Id", "Name", "Amount"],
        "timestamp": "2026-02-16T10:00:01Z",
    }


# ===================================================================
# 1. JobManifest Model Validation Tests
# ===================================================================


class TestJobManifestModel:
    """Verify Pydantic validation for the JobManifest schema."""

    def test_valid_manifest(self):
        """A complete, well-formed manifest should parse without errors."""
        m = _make_manifest()
        assert m.run_id == "aam-run-001"
        assert m.source.pipe_id == "sf-crm-001-opportunities"
        assert m.source.system == "salesforce"
        assert m.target.dcl_url == "http://localhost:8000/api/dcl/ingest"
        assert m.farm_verification is False
        assert m.limits.max_rows == 100000

    def test_missing_run_id_fails(self):
        """run_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                source={"pipe_id": "x", "system": "salesforce"},
                target={"dcl_url": "http://localhost:8000", "tenant_id": "t", "snapshot_name": "s"},
            )
        assert "run_id" in str(exc_info.value)

    def test_missing_pipe_id_fails(self):
        """source.pipe_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                run_id="run-1",
                source={"system": "salesforce"},
                target={"dcl_url": "http://localhost:8000", "tenant_id": "t", "snapshot_name": "s"},
            )
        assert "pipe_id" in str(exc_info.value)

    def test_missing_system_fails(self):
        """source.system is required."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                run_id="run-1",
                source={"pipe_id": "x"},
                target={"dcl_url": "http://localhost:8000", "tenant_id": "t", "snapshot_name": "s"},
            )
        assert "system" in str(exc_info.value)

    def test_missing_dcl_url_fails(self):
        """target.dcl_url is required."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                run_id="run-1",
                source={"pipe_id": "x", "system": "salesforce"},
                target={"tenant_id": "t", "snapshot_name": "s"},
            )
        assert "dcl_url" in str(exc_info.value)

    def test_defaults_applied(self):
        """Defaults should fill in optional fields (non-target)."""
        m = JobManifest(
            run_id="run-1",
            source={"pipe_id": "x", "system": "salesforce"},
            target={"dcl_url": "http://localhost:8000", "tenant_id": "test-tenant", "snapshot_name": "snap-001"},
        )
        assert m.manifest_version == "1.0"
        assert m.farm_verification is False
        assert m.limits.max_rows == 100000
        assert m.limits.timeout_seconds == 300
        assert m.limits.retry_count == 2
        assert m.target.tenant_id == "test-tenant"
        assert m.target.snapshot_name == "snap-001"

    def test_missing_tenant_id_fails(self):
        """tenant_id is required — no default, no fallback."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                run_id="run-1",
                source={"pipe_id": "x", "system": "salesforce"},
                target={"dcl_url": "http://localhost:8000", "snapshot_name": "snap-001"},
            )
        assert "tenant_id" in str(exc_info.value)

    def test_missing_snapshot_name_fails(self):
        """snapshot_name is required — no default, no fallback."""
        with pytest.raises(ValidationError) as exc_info:
            JobManifest(
                run_id="run-1",
                source={"pipe_id": "x", "system": "salesforce"},
                target={"dcl_url": "http://localhost:8000", "tenant_id": "test-tenant"},
            )
        assert "snapshot_name" in str(exc_info.value)

    def test_transform_spec_optional(self):
        """transform is optional and defaults to None."""
        m = _make_manifest()
        assert m.transform is None

    def test_transform_spec_populated(self):
        """transform can be provided."""
        m = _make_manifest(transform={
            "schema_map": {"SourceField": {"target": "target_field"}},
            "grain": "quarter",
            "period_field": "CloseDate",
            "period_format": "YYYY-Qq",
        })
        assert m.transform is not None
        assert m.transform.grain == "quarter"
        assert "SourceField" in m.transform.schema_map

    def test_farm_verification_true(self):
        """farm_verification can be set to true."""
        m = _make_manifest(farm_verification=True)
        assert m.farm_verification is True


# ===================================================================
# 2. Helper Function Tests
# ===================================================================


class TestHelperFunctions:
    """Test utility functions used by the manifest intake endpoint."""

    def test_compute_schema_hash_deterministic(self):
        """Same rows should produce the same hash."""
        rows = [{"id": "1", "name": "test", "amount": 100.0}]
        h1 = _compute_schema_hash(rows)
        h2 = _compute_schema_hash(rows)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_compute_schema_hash_empty(self):
        """Empty rows should return 'empty'."""
        assert _compute_schema_hash([]) == "empty"

    def test_compute_schema_hash_different_schemas(self):
        """Different schemas should produce different hashes."""
        h1 = _compute_schema_hash([{"id": 1, "name": "x"}])
        h2 = _compute_schema_hash([{"id": 1, "amount": 5.0}])
        assert h1 != h2

    def test_find_pipe_data_exact_match(self):
        """Should find pipe by exact name."""
        data = {
            "opportunities": {"meta": {}, "schema": {}, "data": [{"Id": "1"}]},
            "accounts": {"meta": {}, "schema": {}, "data": [{"Id": "2"}]},
        }
        result = _find_pipe_data(data, "opportunities")
        assert result is not None
        assert result["data"][0]["Id"] == "1"

    def test_find_pipe_data_fuzzy_match(self):
        """Should find pipe by partial name match."""
        data = {
            "cost_line_items": {"meta": {}, "schema": {}, "data": [{"id": "1"}]},
        }
        result = _find_pipe_data(data, "cost_line")
        assert result is not None

    def test_find_pipe_data_no_match(self):
        """Should return None when pipe not found."""
        data = {
            "opportunities": {"meta": {}, "schema": {}, "data": [{"Id": "1"}]},
        }
        result = _find_pipe_data(data, "nonexistent")
        assert result is None

    def test_find_pipe_data_no_pipe_name_returns_first(self):
        """With no pipe_name, should return the first valid pipe."""
        data = {
            "_error": "something",
            "opportunities": {"meta": {}, "schema": {}, "data": [{"Id": "1"}]},
        }
        result = _find_pipe_data(data, None)
        assert result is not None
        assert result["data"][0]["Id"] == "1"

    def test_find_pipe_data_skips_errors(self):
        """Should skip entries starting with underscore."""
        data = {
            "_error": "something",
            "_meta": {"not": "data"},
        }
        result = _find_pipe_data(data, None)
        assert result is None


# ===================================================================
# 3. DCL Push Tests (manifest-driven, with mocked HTTP)
# ===================================================================


class TestManifestDrivenPush:
    """Test the _push_to_dcl function with various DCL responses."""

    @pytest.mark.asyncio
    async def test_push_success(self):
        """Successful DCL push should return status=success with correlation keys."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1", "Amount": 100}],
                farm_run_id="farm_test_001",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "success"
        assert result.status_code == 200
        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"
        assert result.dcl_run_id == "dcl-uuid-001"
        assert result.farm_run_id == "farm_test_001"
        assert result.rows_accepted == 20
        assert result.matched_schema is True
        assert result.schema_drift is False

    @pytest.mark.asyncio
    async def test_push_422_no_matching_pipe(self):
        """422 NO_MATCHING_PIPE should return status=rejected, error_type=NO_MATCHING_PIPE."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = _make_dcl_422_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_002",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "rejected"
        assert result.status_code == 422
        assert result.error_type == "NO_MATCHING_PIPE"
        assert result.dcl_run_id is None
        assert result.hint is not None
        # Correlation keys still present
        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"
        assert result.farm_run_id == "farm_test_002"

    @pytest.mark.asyncio
    async def test_push_schema_drift(self):
        """schema_drift=True should be captured with drift_fields."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_drift_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_003",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "success"
        assert result.schema_drift is True
        assert result.drift_fields == ["NewField__c", "RemovedField"]
        assert result.matched_schema is True

    @pytest.mark.asyncio
    async def test_push_timeout(self):
        """Timeout should return status=failed, error_type=timeout."""
        manifest = _make_manifest()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.TimeoutException("timed out")
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_004",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "failed"
        assert result.error_type == "timeout"
        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"

    @pytest.mark.asyncio
    async def test_push_connection_error(self):
        """Connection error should return status=failed, error_type=connection_error."""
        manifest = _make_manifest()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.ConnectError("refused")
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_005",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "failed"
        assert result.error_type == "connection_error"

    @pytest.mark.asyncio
    async def test_push_500_error(self):
        """HTTP 500 should return status=failed, error_type=http_error."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_006",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.status == "failed"
        assert result.status_code == 500
        assert result.error_type == "http_error"

    @pytest.mark.asyncio
    async def test_push_uses_manifest_pipe_id_not_generator_id(self):
        """The x-pipe-id header MUST use the manifest's pipe_id, not generator's."""
        manifest = _make_manifest(source={
            "pipe_id": "aam-assigned-pipe-id-999",
            "system": "salesforce",
            "endpoint_ref": {"pipe_name": "opportunities"},
        })
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response("aam-assigned-pipe-id-999")

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_test_007",
                source_system="salesforce",
                schema_hash="abc123",
            )

        # Verify the actual HTTP call used the manifest pipe_id
        call_kwargs = mock_client.post.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert headers["x-pipe-id"] == "aam-assigned-pipe-id-999"
        assert headers["x-run-id"] == "aam-run-001"
        assert result.pipe_id == "aam-assigned-pipe-id-999"


# ===================================================================
# 4. Correlation Key Completeness Tests
# ===================================================================


class TestCorrelationKeys:
    """Verify all 4 correlation keys are present in push results."""

    @pytest.mark.asyncio
    async def test_success_has_all_keys(self):
        """Successful push must include run_id, pipe_id, dcl_run_id, farm_run_id."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_corr_001",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.run_id is not None
        assert result.pipe_id is not None
        assert result.dcl_run_id is not None
        assert result.farm_run_id is not None
        # Specific values
        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"
        assert result.dcl_run_id == "dcl-uuid-001"
        assert result.farm_run_id == "farm_corr_001"

    @pytest.mark.asyncio
    async def test_rejection_has_correlation_keys(self):
        """Even on 422 rejection, run_id, pipe_id, farm_run_id must be present."""
        manifest = _make_manifest()
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = _make_dcl_422_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_corr_002",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"
        assert result.farm_run_id == "farm_corr_002"
        # dcl_run_id should be None on rejection (DCL didn't accept the data)
        assert result.dcl_run_id is None

    @pytest.mark.asyncio
    async def test_failure_has_correlation_keys(self):
        """Even on timeout/error, run_id, pipe_id, farm_run_id must be present."""
        manifest = _make_manifest()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.TimeoutException("timeout")
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            result = await _push_to_dcl(
                manifest=manifest,
                rows=[{"Id": "1"}],
                farm_run_id="farm_corr_003",
                source_system="salesforce",
                schema_hash="abc123",
            )

        assert result.run_id == "aam-run-001"
        assert result.pipe_id == "sf-crm-001-opportunities"
        assert result.farm_run_id == "farm_corr_003"


# ===================================================================
# 5. ManifestExecutionResult Model Tests
# ===================================================================


class TestManifestExecutionResult:
    """Verify the response model for manifest execution."""

    def test_completed_result(self):
        push = DCLPushResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            dcl_run_id="d1", status="success", rows_pushed=100, rows_accepted=100,
        )
        result = ManifestExecutionResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            status="completed", source_system="salesforce",
            rows_generated=100, push_result=push,
        )
        assert result.status == "completed"
        assert result.push_result.dcl_run_id == "d1"

    def test_rejected_result(self):
        push = DCLPushResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            status="rejected", error_type="NO_MATCHING_PIPE",
            error="No schema blueprint", rows_pushed=100,
        )
        result = ManifestExecutionResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            status="rejected_by_dcl", source_system="salesforce",
            rows_generated=100, push_result=push,
        )
        assert result.status == "rejected_by_dcl"
        assert result.push_result.error_type == "NO_MATCHING_PIPE"

    def test_serializable(self):
        push = DCLPushResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            dcl_run_id="d1", status="success", rows_pushed=50, rows_accepted=50,
            matched_schema=True, schema_fields=["a", "b"],
        )
        result = ManifestExecutionResult(
            run_id="r1", pipe_id="p1", farm_run_id="f1",
            status="completed", source_system="salesforce",
            rows_generated=50, push_result=push,
        )
        data = result.model_dump()
        json.dumps(data)  # Should not raise


# ===================================================================
# 6. Generator Registry Tests
# ===================================================================


class TestGeneratorRegistry:
    """Verify the generator registry covers all known systems."""

    def test_all_systems_registered(self):
        expected = {
            "salesforce", "netsuite", "chargebee", "workday",
            "zendesk", "jira", "datadog", "aws_cost_explorer",
        }
        assert set(_GENERATOR_REGISTRY.keys()) == expected

    def test_each_has_class_and_interface(self):
        for system, spec in _GENERATOR_REGISTRY.items():
            assert "class" in spec, f"{system} missing 'class'"
            assert "interface" in spec, f"{system} missing 'interface'"
            assert spec["interface"] in (
                "generate_profile", "init_profile", "generate_profile_only"
            ), f"{system} has invalid interface: {spec['interface']}"


# ===================================================================
# 7. Integration Test: Manifest → Generate → Push Result
# ===================================================================


class TestManifestIntegration:
    """Integration tests that exercise the full manifest → generate → push flow."""

    @pytest.mark.asyncio
    async def test_salesforce_manifest_generates_data(self):
        """A salesforce manifest should produce opportunity rows and push them."""
        manifest = _make_manifest(
            source={
                "pipe_id": "aam-sf-opps-001",
                "system": "salesforce",
                "endpoint_ref": {"pipe_name": "opportunities"},
            }
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response("aam-sf-opps-001")

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            # Import and call the endpoint logic directly
            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        assert result.status == "completed"
        assert result.source_system == "salesforce"
        assert result.rows_generated > 0
        assert result.pipe_id == "aam-sf-opps-001"
        assert result.push_result is not None
        assert result.push_result.status == "success"
        assert result.push_result.pipe_id == "aam-sf-opps-001"

    @pytest.mark.asyncio
    async def test_netsuite_manifest_generates_data(self):
        """A netsuite manifest with pipe_name=invoices should produce invoice rows."""
        manifest = _make_manifest(
            source={
                "pipe_id": "aam-ns-inv-001",
                "system": "netsuite",
                "endpoint_ref": {"pipe_name": "invoices"},
            }
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response("aam-ns-inv-001")

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        assert result.status == "completed"
        assert result.source_system == "netsuite"
        assert result.rows_generated > 0
        assert result.push_result.pipe_id == "aam-ns-inv-001"

    @pytest.mark.asyncio
    async def test_unknown_system_returns_422(self):
        """An unknown system with no category should raise HTTPException 422 NO_GENERATOR_ROUTE."""
        manifest = _make_manifest(
            source={
                "pipe_id": "unknown-pipe",
                "system": "unknown_vendor",
                "endpoint_ref": {},
            }
        )

        from fastapi import HTTPException
        from src.api.manifest_intake import manifest_intake

        with pytest.raises(HTTPException) as exc_info:
            await manifest_intake(manifest)

        assert exc_info.value.status_code == 422
        assert "NO_GENERATOR_ROUTE" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_dcl_rejection_sets_rejected_status(self):
        """When DCL returns 422 NO_MATCHING_PIPE, overall status=rejected_by_dcl."""
        manifest = _make_manifest()

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = _make_dcl_422_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        assert result.status == "rejected_by_dcl"
        assert result.push_result.error_type == "NO_MATCHING_PIPE"

    @pytest.mark.asyncio
    async def test_verification_flag_triggers_recon(self):
        """When farm_verification=True and push succeeds, recon_triggered=True."""
        manifest = _make_manifest(farm_verification=True)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        assert result.farm_verification_requested is True
        assert result.recon_triggered is True

    @pytest.mark.asyncio
    async def test_verification_not_triggered_on_failure(self):
        """When farm_verification=True but push fails, recon_triggered=False."""
        manifest = _make_manifest(farm_verification=True)

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = _make_dcl_422_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        assert result.farm_verification_requested is True
        assert result.recon_triggered is False

    @pytest.mark.asyncio
    async def test_max_rows_limit_applied(self):
        """Manifest limits.max_rows should truncate pushed rows."""
        manifest = _make_manifest(limits={"max_rows": 5, "timeout_seconds": 30, "retry_count": 2})

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _make_dcl_success_response()

        with patch("src.api.manifest_intake.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            from src.api.manifest_intake import manifest_intake
            result = await manifest_intake(manifest)

        # The push should have been called — verify rows were truncated
        call_kwargs = mock_client.post.call_args
        pushed_body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json", {})
        assert pushed_body["row_count"] <= 5
        assert len(pushed_body["rows"]) <= 5


