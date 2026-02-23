"""Unit tests for the services layer."""
import pytest
from datetime import datetime

from src.services.key_normalization import (
    normalize_name,
    extract_domain,
    extract_registered_domain,
    to_domain_key,
    is_external_domain,
)
from src.services.constants import (
    VENDOR_DOMAIN_SETS,
    DOMAIN_TO_VENDOR,
    get_domain_to_vendor_map,
    INFRASTRUCTURE_DOMAINS,
    EXTERNAL_DOMAIN_TLDS,
)
from src.services.reconciliation import (
    build_candidate_flags,
    propagate_vendor_governance,
    derive_reason_codes,
)


class TestExtractDomain:
    """Tests for extract_domain() function."""

    def test_extract_domain_from_https_url(self):
        assert extract_domain("https://example.com/path") == "example.com"

    def test_extract_domain_from_http_url(self):
        assert extract_domain("http://example.com/page") == "example.com"

    def test_extract_domain_simple(self):
        assert extract_domain("example.com") == "example.com"

    def test_extract_domain_with_subdomain(self):
        result = extract_domain("https://app.slack.com/path")
        assert result == "slack.com"

    def test_extract_domain_with_port(self):
        result = extract_domain("example.com:8080")
        assert result == "example.com:8080" or "example" in result

    def test_extract_domain_empty_string(self):
        assert extract_domain("") is None

    def test_extract_domain_none(self):
        assert extract_domain(None) is None

    def test_extract_domain_no_tld(self):
        assert extract_domain("localhost") is None

    def test_extract_domain_whitespace(self):
        assert extract_domain("   ") is None


class TestNormalizeName:
    """Tests for normalize_name() function."""

    def test_normalize_name_simple(self):
        assert normalize_name("Slack") == "slack"

    def test_normalize_name_with_spaces(self):
        assert normalize_name("Microsoft 365") == "microsoft365"

    def test_normalize_name_with_special_chars(self):
        assert normalize_name("Zoom-Video!") == "zoomvideo"

    def test_normalize_name_empty(self):
        assert normalize_name("") == ""

    def test_normalize_name_uppercase(self):
        assert normalize_name("SALESFORCE") == "salesforce"


class TestToDomainKey:
    """Tests for to_domain_key() function."""

    def test_to_domain_key_with_domain(self):
        assert to_domain_key("calendly.com") == "calendly.com"

    def test_to_domain_key_with_url(self):
        result = to_domain_key("https://example.com/path")
        assert result == "example.com"

    def test_to_domain_key_with_name(self):
        result = to_domain_key("Microsoft 365")
        assert result == "microsoft365"

    def test_to_domain_key_empty(self):
        assert to_domain_key("") == ""


class TestExtractRegisteredDomain:
    """Tests for extract_registered_domain() function."""

    def test_registered_domain_subdomain(self):
        assert extract_registered_domain("app.slack.com") == "slack.com"

    def test_registered_domain_simple(self):
        assert extract_registered_domain("slack.com") == "slack.com"

    def test_registered_domain_compound_tld(self):
        assert extract_registered_domain("example.co.uk") == "example.co.uk"

    def test_registered_domain_subdomain_compound_tld(self):
        assert extract_registered_domain("cdn.static.example.co.uk") == "example.co.uk"

    def test_registered_domain_empty(self):
        assert extract_registered_domain("") is None

    def test_registered_domain_none(self):
        assert extract_registered_domain(None) is None


class TestIsExternalDomain:
    """Tests for is_external_domain() function."""

    def test_is_external_domain_com(self):
        assert is_external_domain("example.com") is True

    def test_is_external_domain_io(self):
        assert is_external_domain("redis.io") is True

    def test_is_external_domain_internal(self):
        assert is_external_domain("authservice") is False

    def test_is_external_domain_internal_with_hyphen(self):
        assert is_external_domain("billing-api") is False

    def test_is_external_domain_co_uk(self):
        assert is_external_domain("example.co.uk") is True


class TestVendorDomainSets:
    """Tests for VENDOR_DOMAIN_SETS constant."""

    def test_microsoft_domains(self):
        assert "microsoft.com" in VENDOR_DOMAIN_SETS["microsoft"]
        assert "office365.com" in VENDOR_DOMAIN_SETS["microsoft"]
        assert "sharepoint.com" in VENDOR_DOMAIN_SETS["microsoft"]

    def test_google_domains(self):
        assert "google.com" in VENDOR_DOMAIN_SETS["google"]
        assert "gmail.com" in VENDOR_DOMAIN_SETS["google"]
        assert "youtube.com" in VENDOR_DOMAIN_SETS["google"]

    def test_salesforce_domains(self):
        assert "salesforce.com" in VENDOR_DOMAIN_SETS["salesforce"]
        assert "slack.com" in VENDOR_DOMAIN_SETS["salesforce"]

    def test_vendor_exists(self):
        assert "microsoft" in VENDOR_DOMAIN_SETS
        assert "google" in VENDOR_DOMAIN_SETS
        assert "aws" in VENDOR_DOMAIN_SETS
        assert "okta" in VENDOR_DOMAIN_SETS


class TestGetDomainToVendorMap:
    """Tests for get_domain_to_vendor_map() function."""

    def test_returns_dict(self):
        result = get_domain_to_vendor_map()
        assert isinstance(result, dict)

    def test_microsoft_domain_lookup(self):
        result = get_domain_to_vendor_map()
        assert result["microsoft.com"] == "microsoft"
        assert result["office365.com"] == "microsoft"

    def test_google_domain_lookup(self):
        result = get_domain_to_vendor_map()
        assert result["gmail.com"] == "google"

    def test_salesforce_includes_slack(self):
        result = get_domain_to_vendor_map()
        assert result["slack.com"] == "salesforce"

    def test_domain_to_vendor_constant(self):
        assert DOMAIN_TO_VENDOR["microsoft.com"] == "microsoft"
        assert DOMAIN_TO_VENDOR["slack.com"] == "salesforce"


class TestInfrastructureDomains:
    """Tests for INFRASTRUCTURE_DOMAINS constant."""

    def test_contains_databases(self):
        assert "postgresql.org" in INFRASTRUCTURE_DOMAINS
        assert "mysql.com" in INFRASTRUCTURE_DOMAINS
        assert "mongodb.com" in INFRASTRUCTURE_DOMAINS

    def test_contains_tools(self):
        assert "docker.com" in INFRASTRUCTURE_DOMAINS
        assert "kubernetes.io" in INFRASTRUCTURE_DOMAINS


class TestExternalDomainTlds:
    """Tests for EXTERNAL_DOMAIN_TLDS constant."""

    def test_contains_common_tlds(self):
        assert ".com" in EXTERNAL_DOMAIN_TLDS
        assert ".io" in EXTERNAL_DOMAIN_TLDS
        assert ".org" in EXTERNAL_DOMAIN_TLDS

    def test_contains_compound_tlds(self):
        assert ".co.uk" in EXTERNAL_DOMAIN_TLDS
        assert ".com.au" in EXTERNAL_DOMAIN_TLDS


class TestBuildCandidateFlags:
    """Tests for build_candidate_flags() function."""

    @pytest.fixture
    def simple_snapshot(self):
        return {
            "meta": {"created_at": "2025-01-01T00:00:00Z"},
            "planes": {
                "discovery": {
                    "observations": [
                        {
                            "domain": "slack.com",
                            "observed_name": "Slack",
                            "observed_at": "2024-12-01T00:00:00Z",
                            "source": "proxy"
                        }
                    ]
                },
                "idp": {"objects": []},
                "cmdb": {"cis": []},
                "cloud": {"resources": []},
                "finance": {"contracts": [], "transactions": []},
            }
        }

    @pytest.fixture
    def governed_snapshot(self):
        return {
            "meta": {"created_at": "2025-01-01T00:00:00Z"},
            "planes": {
                "discovery": {
                    "observations": [
                        {
                            "domain": "slack.com",
                            "observed_name": "Slack",
                            "observed_at": "2024-12-01T00:00:00Z",
                            "source": "proxy"
                        }
                    ]
                },
                "idp": {
                    "objects": [
                        {"name": "Slack", "external_ref": "slack.com", "last_login_at": "2024-12-15T00:00:00Z"}
                    ]
                },
                "cmdb": {"cis": []},
                "cloud": {"resources": []},
                "finance": {"contracts": [], "transactions": []},
            }
        }

    def test_build_flags_returns_dict(self, simple_snapshot):
        result = build_candidate_flags(simple_snapshot)
        assert isinstance(result, dict)

    def test_discovery_present_flag(self, simple_snapshot):
        result = build_candidate_flags(simple_snapshot)
        assert "slack.com" in result
        assert result["slack.com"]["discovery_present"] is True

    def test_activity_present_flag(self, simple_snapshot):
        result = build_candidate_flags(simple_snapshot)
        assert result["slack.com"]["activity_present"] is True

    def test_idp_not_present(self, simple_snapshot):
        result = build_candidate_flags(simple_snapshot)
        assert result["slack.com"]["idp_present"] is False

    def test_idp_present_flag(self, governed_snapshot):
        result = build_candidate_flags(governed_snapshot)
        assert result["slack.com"]["idp_present"] is True

    def test_empty_snapshot(self):
        empty_snapshot = {
            "meta": {},
            "planes": {
                "discovery": {"observations": []},
                "idp": {"objects": []},
                "cmdb": {"cis": []},
                "cloud": {"resources": []},
                "finance": {"contracts": [], "transactions": []},
            }
        }
        result = build_candidate_flags(empty_snapshot)
        assert len(result) == 0


class TestPropagateVendorGovernance:
    """Tests for propagate_vendor_governance() function."""

    def test_propagates_idp_across_vendor(self):
        candidates = {
            "microsoft.com": {"idp_present": True, "cmdb_present": False},
            "office365.com": {"idp_present": False, "cmdb_present": False},
        }
        result = propagate_vendor_governance(candidates)
        
        assert "microsoft.com" in result
        assert "office365.com" in result
        has_idp, has_cmdb, vendor = result["microsoft.com"]
        assert has_idp is True
        assert vendor == "microsoft"

    def test_propagates_cmdb_across_vendor(self):
        candidates = {
            "sharepoint.com": {"idp_present": False, "cmdb_present": True},
        }
        result = propagate_vendor_governance(candidates)
        
        has_idp, has_cmdb, vendor = result["sharepoint.com"]
        assert has_cmdb is True
        assert vendor == "microsoft"

    def test_returns_all_vendor_domains(self):
        candidates = {
            "google.com": {"idp_present": True, "cmdb_present": False},
        }
        result = propagate_vendor_governance(candidates)
        
        assert "gmail.com" in result
        has_idp, has_cmdb, vendor = result["gmail.com"]
        assert has_idp is True
        assert vendor == "google"

    def test_empty_candidates(self):
        result = propagate_vendor_governance({})
        assert isinstance(result, dict)
        assert len(result) > 0


class TestDeriveReasonCodes:
    """Tests for derive_reason_codes() function."""

    def test_discovery_present_code(self):
        cand = {"discovery_present": True, "idp_present": False, "cmdb_present": False}
        codes = derive_reason_codes(cand)
        assert "HAS_DISCOVERY" in codes
        assert "NO_IDP" in codes
        assert "NO_CMDB" in codes

    def test_idp_present_code(self):
        cand = {"discovery_present": False, "idp_present": True, "cmdb_present": False}
        codes = derive_reason_codes(cand)
        assert "HAS_IDP" in codes

    def test_cmdb_present_code(self):
        cand = {"discovery_present": False, "idp_present": False, "cmdb_present": True}
        codes = derive_reason_codes(cand)
        assert "HAS_CMDB" in codes

    def test_activity_present_code(self):
        cand = {"discovery_present": True, "idp_present": False, "cmdb_present": False, "activity_present": True}
        codes = derive_reason_codes(cand)
        assert "RECENT_ACTIVITY" in codes

    def test_stale_activity_code(self):
        cand = {"discovery_present": True, "idp_present": False, "cmdb_present": False, "activity_present": False, "stale_timestamps": ["2024-01-01"]}
        codes = derive_reason_codes(cand)
        assert "STALE_ACTIVITY" in codes


class TestClassificationLogic:
    """Tests for shadow/zombie classification logic."""

    @pytest.fixture
    def shadow_candidate_snapshot(self):
        """Snapshot that should produce a shadow: ungoverned + active + discovered."""
        return {
            "meta": {"created_at": "2025-01-01T00:00:00Z"},
            "planes": {
                "discovery": {
                    "observations": [
                        {
                            "domain": "dropbox.com",
                            "observed_name": "Dropbox",
                            "observed_at": "2024-12-01T00:00:00Z",
                            "source": "proxy"
                        },
                        {
                            "domain": "dropbox.com",
                            "observed_name": "Dropbox",
                            "observed_at": "2024-12-02T00:00:00Z",
                            "source": "dns"
                        }
                    ]
                },
                "idp": {"objects": []},
                "cmdb": {"cis": []},
                "cloud": {"resources": []},
                "finance": {"contracts": [], "transactions": []},
            }
        }

    def test_shadow_is_ungoverned_active_discovered(self, shadow_candidate_snapshot):
        candidates = build_candidate_flags(shadow_candidate_snapshot)
        
        cand = candidates.get("dropbox.com")
        assert cand is not None
        assert cand["discovery_present"] is True
        assert cand["activity_present"] is True
        assert cand["idp_present"] is False
        assert cand["cmdb_present"] is False

    def test_governed_not_shadow(self):
        snapshot = {
            "meta": {"created_at": "2025-01-01T00:00:00Z"},
            "planes": {
                "discovery": {
                    "observations": [
                        {
                            "domain": "slack.com",
                            "observed_name": "Slack",
                            "observed_at": "2024-12-01T00:00:00Z",
                            "source": "proxy"
                        }
                    ]
                },
                "idp": {
                    "objects": [{"name": "Slack", "external_ref": "slack.com"}]
                },
                "cmdb": {"cis": []},
                "cloud": {"resources": []},
                "finance": {"contracts": [], "transactions": []},
            }
        }
        candidates = build_candidate_flags(snapshot)
        cand = candidates.get("slack.com")
        
        assert cand["idp_present"] is True
