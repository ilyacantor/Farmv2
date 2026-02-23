"""Unit tests for Stub v2 - Two-tier correlation algorithm for AOD simulation."""
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.aod_client import (
    stub_aod_explain_nonflag_from_snapshot,
    _normalize_name_for_matching,
    _compute_word_overlap,
)
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


class TestStubV2TierOneAuthoritative:
    """Tests for Tier 1 (AUTHORITATIVE) correlation - direct domain matches."""

    @pytest.mark.asyncio
    async def test_tier1_matches_domains_array(self):
        """Tier 1: Match against CMDB domains[] array yields AUTHORITATIVE status."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {
                            "ci_id": "CI12345",
                            "canonical_domain": "salesforce.com",
                            "domains": ["force.com", "salesforce.eu", "lightning.force.com"],
                            "name": "Salesforce CRM"
                        }
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
                asset_keys=["force.com", "salesforce.eu"],
                ask="shadow"
            )
        
        assert result["force.com"]["cmdb_correlation"]["status"] == "AUTHORITATIVE"
        assert result["force.com"]["cmdb_correlation"]["method"] == "registered_domain"
        assert result["force.com"]["cmdb_correlation"]["matched_id"] == "CI12345"
        
        assert result["salesforce.eu"]["cmdb_correlation"]["status"] == "AUTHORITATIVE"

    @pytest.mark.asyncio
    async def test_tier1_canonical_domain_is_authoritative(self):
        """Tier 1: Match against canonical_domain yields AUTHORITATIVE status."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [{"ci_id": "CI999", "canonical_domain": "slack.com", "name": "Slack"}]
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
                asset_keys=["slack.com"],
                ask="shadow"
            )
        
        assert result["slack.com"]["cmdb_correlation"]["status"] == "AUTHORITATIVE"
        assert "HAS_CMDB" in result["slack.com"]["reason_codes"]
        assert "HAS_CMDB_WEAK" not in result["slack.com"]["reason_codes"]


class TestStubV2TierTwoWeak:
    """Tests for Tier 2 (WEAK) correlation - vendor/name matching."""

    @pytest.mark.asyncio
    async def test_tier2_vendor_in_domain_yields_weak(self):
        """Tier 2: Vendor name in domain yields WEAK correlation."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"ci_id": "CI001", "canonical_domain": "salesforce.com", "vendor": "salesforce", "name": "Salesforce CRM"}
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
                asset_keys=["salesforce-integration.io"],
                ask="shadow"
            )
        
        assert result["salesforce-integration.io"]["cmdb_correlation"]["status"] == "WEAK"
        assert result["salesforce-integration.io"]["cmdb_correlation"]["method"] == "vendor_in_domain"
        assert "HAS_CMDB_WEAK" in result["salesforce-integration.io"]["reason_codes"]

    @pytest.mark.asyncio
    async def test_tier2_name_word_overlap_yields_weak(self):
        """Tier 2: Name word overlap (>=2 words) yields WEAK correlation."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"ci_id": "CI002", "canonical_domain": "hubspot.com", "name": "HubSpot Marketing Platform"}
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
                asset_keys=["hubspot-marketing.io"],
                ask="shadow"
            )
        
        assert result["hubspot-marketing.io"]["cmdb_correlation"]["status"] == "WEAK"
        assert "name_overlap" in result["hubspot-marketing.io"]["cmdb_correlation"]["method"]

    @pytest.mark.asyncio
    async def test_tier2_single_word_overlap_is_not_weak(self):
        """Tier 2: Single word overlap (<2 words) does NOT yield WEAK correlation."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"ci_id": "CI003", "canonical_domain": "analytics.com", "name": "Analytics Dashboard"}
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
                asset_keys=["analytics.io"],
                ask="shadow"
            )
        
        assert result["analytics.io"]["cmdb_correlation"]["status"] == "NONE"
        assert "NO_CMDB" in result["analytics.io"]["reason_codes"]


class TestStubV2WeakDoesNotMerge:
    """Negative tests: WEAK correlation must NOT trigger identity merges."""

    @pytest.mark.asyncio
    async def test_weak_correlation_does_not_assert_has_cmdb(self):
        """WEAK correlation uses HAS_CMDB_WEAK, not HAS_CMDB."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [
                        {"ci_id": "CI100", "canonical_domain": "zendesk.com", "vendor": "zendesk", "name": "Zendesk Support"}
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
                asset_keys=["zendesk-integration.net"],
                ask="shadow"
            )
        
        assert result["zendesk-integration.net"]["cmdb_correlation"]["status"] == "WEAK"
        assert "HAS_CMDB" not in result["zendesk-integration.net"]["reason_codes"]
        assert "HAS_CMDB_WEAK" in result["zendesk-integration.net"]["reason_codes"]


class TestStubV2StructuredOutput:
    """Tests for structured correlation output."""

    @pytest.mark.asyncio
    async def test_correlation_output_includes_all_fields(self):
        """Structured output includes status, method, and matched_id."""
        snapshot = {
            "__planes__": {
                "cmdb": {
                    "cis": [{"ci_id": "CI789", "canonical_domain": "github.com", "name": "GitHub"}]
                },
                "idp": {
                    "objects": [{"idp_id": "IDP123", "canonical_domain": "github.com", "name": "GitHub SSO"}]
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
                asset_keys=["github.com"],
                ask="shadow"
            )
        
        cmdb_corr = result["github.com"]["cmdb_correlation"]
        idp_corr = result["github.com"]["idp_correlation"]
        
        assert cmdb_corr["status"] == "AUTHORITATIVE"
        assert cmdb_corr["method"] == "registered_domain"
        assert cmdb_corr["matched_id"] == "CI789"
        
        assert idp_corr["status"] == "AUTHORITATIVE"
        assert idp_corr["method"] == "registered_domain"
        assert idp_corr["matched_id"] == "IDP123"

    @pytest.mark.asyncio
    async def test_none_correlation_has_null_matched_id(self):
        """NONE correlation status has null matched_id."""
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
                asset_keys=["unknown-app.io"],
                ask="shadow"
            )
        
        cmdb_corr = result["unknown-app.io"]["cmdb_correlation"]
        idp_corr = result["unknown-app.io"]["idp_correlation"]
        
        assert cmdb_corr["status"] == "NONE"
        assert cmdb_corr["matched_id"] is None
        
        assert idp_corr["status"] == "NONE"
        assert idp_corr["matched_id"] is None


class TestNameNormalizationHelpers:
    """Tests for name normalization helper functions."""

    def test_normalize_name_extracts_meaningful_words(self):
        """Normalization extracts meaningful words, filters stopwords."""
        words = _normalize_name_for_matching("Salesforce Marketing Cloud Platform")
        assert "salesforce" in words
        assert "marketing" in words
        assert "cloud" not in words
        assert "platform" not in words

    def test_normalize_name_handles_dashes_and_underscores(self):
        """Normalization handles dashes and underscores as word separators."""
        words = _normalize_name_for_matching("hubspot-marketing_automation")
        assert "hubspot" in words
        assert "marketing" in words
        assert "automation" in words

    def test_normalize_name_filters_short_words(self):
        """Words with 2 or fewer chars are filtered."""
        words = _normalize_name_for_matching("My CRM App")
        assert "my" not in words
        assert "app" not in words
        assert "crm" in words

    def test_word_overlap_counts_shared_words(self):
        """Word overlap correctly counts shared non-stopwords."""
        words1 = {"salesforce", "marketing", "automation"}
        words2 = {"marketing", "automation", "platform"}
        assert _compute_word_overlap(words1, words2) == 2
