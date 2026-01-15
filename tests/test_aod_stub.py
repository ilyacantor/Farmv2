"""Unit tests for the enhanced AOD stub that reads snapshot CMDB/IdP planes."""
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.aod_client import stub_aod_explain_nonflag_from_snapshot
from src.services.key_normalization import extract_registered_domain

pytestmark = pytest.mark.asyncio(loop_scope="function")


class TestStubCMDBCorrelation:
    """Tests for CMDB correlation in stub mode."""

    @pytest.mark.asyncio
    async def test_stub_returns_has_cmdb_when_canonical_domain_matches(self):
        """When an asset key matches a CMDB canonical_domain, stub returns HAS_CMDB."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"canonical_domain": "slack.com", "name": "Slack Enterprise"},
                        {"canonical_domain": "zoom.us", "name": "Zoom Video"},
                    ]
                },
                "idp": {"objects": []}
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["slack.com", "unknown.com"],
                ask="shadow"
            )
        
        assert "HAS_CMDB" in result["slack.com"]["reason_codes"]
        assert "NO_IDP" in result["slack.com"]["reason_codes"]
        
        assert "NO_CMDB" in result["unknown.com"]["reason_codes"]
        assert "NO_IDP" in result["unknown.com"]["reason_codes"]

    @pytest.mark.asyncio
    async def test_stub_matches_subdomain_to_registered_domain(self):
        """When asset key is a subdomain, stub matches against registered domain."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"canonical_domain": "salesforce.com", "name": "Salesforce CRM"},
                    ]
                },
                "idp": {"objects": []}
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["app.salesforce.com", "login.salesforce.com"],
                ask="shadow"
            )
        
        assert "HAS_CMDB" in result["app.salesforce.com"]["reason_codes"]
        assert "HAS_CMDB" in result["login.salesforce.com"]["reason_codes"]


class TestStubIdPCorrelation:
    """Tests for IdP correlation in stub mode."""

    @pytest.mark.asyncio
    async def test_stub_returns_has_idp_when_canonical_domain_matches(self):
        """When an asset key matches an IdP canonical_domain, stub returns HAS_IDP."""
        snapshot = {
            "__planes__": {
                "cmdb": {"cis": []},
                "idp": {
                    "objects": [
                        {"canonical_domain": "github.com", "name": "GitHub Enterprise"},
                        {"canonical_domain": "atlassian.com", "name": "Atlassian Suite"},
                    ]
                }
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["github.com", "atlassian.com", "unknown.io"],
                ask="shadow"
            )
        
        assert "HAS_IDP" in result["github.com"]["reason_codes"]
        assert "HAS_IDP" in result["atlassian.com"]["reason_codes"]
        assert "NO_IDP" in result["unknown.io"]["reason_codes"]

    @pytest.mark.asyncio
    async def test_stub_returns_both_flags_when_in_cmdb_and_idp(self):
        """When asset is in both CMDB and IdP, stub returns both HAS_CMDB and HAS_IDP."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [{"canonical_domain": "okta.com", "name": "Okta Identity"}]
                },
                "idp": {
                    "objects": [{"canonical_domain": "okta.com", "name": "Okta SSO"}]
                }
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["okta.com"],
                ask="shadow"
            )
        
        assert "HAS_CMDB" in result["okta.com"]["reason_codes"]
        assert "HAS_IDP" in result["okta.com"]["reason_codes"]
        assert result["okta.com"]["decision"] == "ADMITTED_NOT_SHADOW"


class TestStubNoCrossTLDCorrelation:
    """Negative tests: stub must NOT do cross-TLD or fuzzy matching."""

    @pytest.mark.asyncio
    async def test_stub_does_not_match_different_tld(self):
        """example.com in CMDB must NOT match example.io - no cross-TLD correlation."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [{"canonical_domain": "example.com", "name": "Example Inc"}]
                },
                "idp": {"objects": []}
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["example.io", "example.net", "example.org"],
                ask="shadow"
            )
        
        assert "NO_CMDB" in result["example.io"]["reason_codes"]
        assert "NO_CMDB" in result["example.net"]["reason_codes"]
        assert "NO_CMDB" in result["example.org"]["reason_codes"]

    @pytest.mark.asyncio
    async def test_stub_does_not_do_fuzzy_name_matching(self):
        """slack.com in CMDB must NOT match slackapp.com - no fuzzy matching."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [{"canonical_domain": "slack.com", "name": "Slack"}]
                },
                "idp": {"objects": []}
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["slackapp.com", "slackhq.com", "myslack.com"],
                ask="shadow"
            )
        
        assert "NO_CMDB" in result["slackapp.com"]["reason_codes"]
        assert "NO_CMDB" in result["slackhq.com"]["reason_codes"]
        assert "NO_CMDB" in result["myslack.com"]["reason_codes"]


class TestStubModeFlag:
    """Tests that stub mode is properly indicated in responses."""

    @pytest.mark.asyncio
    async def test_stub_responses_include_stub_mode_flag(self):
        """All stub responses should include stub_mode: True."""
        snapshot = {
            "__planes__": {
                "cmdb": {"cis": []},
                "idp": {"objects": []}
            }
        }
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["any.com"],
                ask="shadow"
            )
        
        assert result["any.com"]["stub_mode"] is True


class TestStubHandlesMissingSnapshot:
    """Tests for graceful handling when snapshot is missing or malformed."""

    @pytest.mark.asyncio
    async def test_stub_handles_missing_snapshot(self):
        """When snapshot not found in DB, stub returns reasonable defaults."""
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="nonexistent-snapshot",
                asset_keys=["some.com"],
                ask="shadow"
            )
        
        assert "NO_CMDB" in result["some.com"]["reason_codes"]
        assert "NO_IDP" in result["some.com"]["reason_codes"]

    @pytest.mark.asyncio
    async def test_stub_handles_empty_planes(self):
        """When snapshot has empty planes, stub returns NO_CMDB/NO_IDP."""
        snapshot = {"__planes__": {"cmdb": {}, "idp": {}}}
        
        mock_row = {"snapshot_json": json.dumps(snapshot)}
        
        with patch("src.farm.db.connection") as mock_db:
            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=mock_row)
            mock_db.return_value.__aenter__.return_value = mock_conn
            
            result = await stub_aod_explain_nonflag_from_snapshot(
                snapshot_id="test-snapshot-123",
                asset_keys=["any.com"],
                ask="shadow"
            )
        
        assert "NO_CMDB" in result["any.com"]["reason_codes"]
        assert "NO_IDP" in result["any.com"]["reason_codes"]
