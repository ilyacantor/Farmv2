"""Tests for MissingPolicyError and policy fallback behavior.

These tests verify:
1. Missing policy hard-fails (MissingPolicyError raised)
2. Fallback policy with FARM_ALLOW_DEFAULT_POLICY=true behaves like AOD
3. Explicit policy enforces gates correctly
"""

import os
import pytest
from unittest.mock import patch

from src.models.policy import PolicyConfig, MissingPolicyError, SecondaryGatesConfig
from src.services.reconciliation import compute_expected_block, analyze_snapshot_for_expectations


@pytest.fixture
def minimal_snapshot():
    """A minimal snapshot with one IdP record (no SSO) and discovery evidence."""
    return {
        "meta": {
            "snapshot_id": "test-snapshot",
            "tenant_id": "test-tenant",
            "created_at": "2026-01-15T12:00:00Z",
        },
        "planes": {
            "discovery": {
                "observations": [
                    {
                        "observation_id": "obs-1",
                        "observed_at": "2026-01-10T12:00:00Z",
                        "source": "proxy",
                        "observed_name": "PagerDuty",
                        "domain": "pagerduty.com",
                        "vendor_hint": "PagerDuty",
                        "category_hint": "saas",
                        "environment_hint": "prod",
                    },
                    {
                        "observation_id": "obs-2",
                        "observed_at": "2026-01-10T12:00:00Z",
                        "source": "dns",
                        "observed_name": "PagerDuty",
                        "domain": "pagerduty.com",
                        "vendor_hint": "PagerDuty",
                        "category_hint": "saas",
                        "environment_hint": "prod",
                    },
                ]
            },
            "idp": {
                "objects": [
                    {
                        "idp_id": "idp-1",
                        "name": "PAGERDUTY",
                        "idp_type": "application",
                        "external_ref": "https://pagerduty.com",
                        "has_sso": False,  # No SSO - key test case
                        "has_scim": True,
                        "vendor": "PagerDuty",
                        "last_login_at": None,
                        "canonical_domain": "pagerduty.com",
                    }
                ]
            },
            "cmdb": {"cis": []},
            "cloud": {"resources": []},
            "endpoint": {"devices": [], "installed_apps": []},
            "network": {"dns": [], "proxy": [], "certs": [], "browser": []},
            "finance": {"vendors": [], "contracts": [], "transactions": []},
            "security": {"attestations": []},
        },
    }


class TestMissingPolicyHardFail:
    """Test Case 1: Missing policy hard-fails with MissingPolicyError."""

    def test_compute_expected_block_raises_without_policy(self, minimal_snapshot):
        """compute_expected_block should raise MissingPolicyError when policy is None."""
        with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": ""}, clear=False):
            with pytest.raises(MissingPolicyError) as exc_info:
                compute_expected_block(minimal_snapshot, policy=None)
            
            assert "requires policy snapshot from AOD" in str(exc_info.value)
            assert "FARM_ALLOW_DEFAULT_POLICY" in str(exc_info.value)

    def test_analyze_snapshot_raises_without_policy(self, minimal_snapshot):
        """analyze_snapshot_for_expectations should raise MissingPolicyError when policy is None."""
        with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": ""}, clear=False):
            with pytest.raises(MissingPolicyError) as exc_info:
                analyze_snapshot_for_expectations(minimal_snapshot, policy=None)
            
            assert "requires policy snapshot from AOD" in str(exc_info.value)

    def test_empty_env_var_still_fails(self, minimal_snapshot):
        """Empty string for FARM_ALLOW_DEFAULT_POLICY should still raise."""
        with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": ""}, clear=False):
            with pytest.raises(MissingPolicyError):
                compute_expected_block(minimal_snapshot, policy=None)

    def test_false_env_var_still_fails(self, minimal_snapshot):
        """FARM_ALLOW_DEFAULT_POLICY=false should still raise."""
        with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": "false"}, clear=False):
            with pytest.raises(MissingPolicyError):
                compute_expected_block(minimal_snapshot, policy=None)


class TestFallbackPolicyBehavesLikeAOD:
    """Test Case 2: With FARM_ALLOW_DEFAULT_POLICY=true, fallback matches AOD behavior."""

    def test_fallback_allows_idp_without_sso(self, minimal_snapshot):
        """When fallback is enabled, IdP without SSO should still yield HAS_IDP.
        
        This matches AOD's effective behavior where require_sso_for_idp=false.
        """
        with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": "true"}, clear=False):
            expected = compute_expected_block(minimal_snapshot, policy=None, mode="all")
            
            # pagerduty.com should have HAS_IDP in reason codes
            reasons = expected.get("expected_reasons", {})
            pagerduty_reasons = reasons.get("pagerduty.com", [])
            
            assert "HAS_IDP" in pagerduty_reasons, \
                f"IdP without SSO should yield HAS_IDP when using fallback policy. Got: {pagerduty_reasons}"
            assert "NO_IDP" not in pagerduty_reasons, \
                f"IdP without SSO should NOT yield NO_IDP when using fallback policy. Got: {pagerduty_reasons}"

    def test_fallback_policy_sso_gate_disabled(self):
        """Default fallback policy should have require_sso_for_idp=False."""
        policy = PolicyConfig.default_fallback()
        assert policy.secondary_gates.require_sso_for_idp == False, \
            "Fallback policy should have require_sso_for_idp=False to match AOD"

    def test_fallback_idp_passes_gates_without_sso(self):
        """With fallback policy, idp_passes_gates(False) should return True."""
        policy = PolicyConfig.default_fallback()
        assert policy.idp_passes_gates(has_sso=False) == True, \
            "Fallback policy should pass IdP without SSO"

    def test_env_var_case_insensitive(self, minimal_snapshot):
        """FARM_ALLOW_DEFAULT_POLICY should be case-insensitive."""
        for value in ["true", "TRUE", "True"]:
            with patch.dict(os.environ, {"FARM_ALLOW_DEFAULT_POLICY": value}, clear=False):
                # Should not raise
                expected = compute_expected_block(minimal_snapshot, policy=None, mode="all")
                assert "expected_reasons" in expected


class TestExplicitPolicyEnforcesGate:
    """Test Case 3: Explicit policy with require_sso_for_idp=True enforces the gate."""

    def test_explicit_policy_rejects_idp_without_sso(self, minimal_snapshot):
        """When policy has require_sso_for_idp=True, IdP without SSO yields NO_IDP."""
        strict_policy = PolicyConfig(
            secondary_gates=SecondaryGatesConfig(
                require_sso_for_idp=True,  # Explicitly enabled
            )
        )
        
        expected = compute_expected_block(minimal_snapshot, policy=strict_policy, mode="all")
        
        reasons = expected.get("expected_reasons", {})
        pagerduty_reasons = reasons.get("pagerduty.com", [])
        
        assert "NO_IDP" in pagerduty_reasons, \
            f"Explicit policy with require_sso_for_idp=True should yield NO_IDP. Got: {pagerduty_reasons}"
        assert "HAS_IDP" not in pagerduty_reasons, \
            f"Explicit policy with require_sso_for_idp=True should NOT yield HAS_IDP. Got: {pagerduty_reasons}"

    def test_explicit_policy_accepts_idp_with_sso(self, minimal_snapshot):
        """When policy has require_sso_for_idp=True but IdP has SSO, yields HAS_IDP."""
        # Modify the snapshot to have SSO enabled
        minimal_snapshot["planes"]["idp"]["objects"][0]["has_sso"] = True
        
        strict_policy = PolicyConfig(
            secondary_gates=SecondaryGatesConfig(
                require_sso_for_idp=True,
            )
        )
        
        expected = compute_expected_block(minimal_snapshot, policy=strict_policy, mode="all")
        
        reasons = expected.get("expected_reasons", {})
        pagerduty_reasons = reasons.get("pagerduty.com", [])
        
        assert "HAS_IDP" in pagerduty_reasons, \
            f"IdP with SSO should yield HAS_IDP even with strict policy. Got: {pagerduty_reasons}"

    def test_idp_passes_gates_with_sso_required_and_present(self):
        """idp_passes_gates should return True when SSO is required and present."""
        strict_policy = PolicyConfig(
            secondary_gates=SecondaryGatesConfig(
                require_sso_for_idp=True,
            )
        )
        assert strict_policy.idp_passes_gates(has_sso=True) == True

    def test_idp_passes_gates_with_sso_required_but_missing(self):
        """idp_passes_gates should return False when SSO is required but missing."""
        strict_policy = PolicyConfig(
            secondary_gates=SecondaryGatesConfig(
                require_sso_for_idp=True,
            )
        )
        assert strict_policy.idp_passes_gates(has_sso=False) == False


class TestPolicyMasterJsonAlignment:
    """Test that policy_master.json aligns with AOD's effective behavior."""

    def test_policy_master_sso_gate_disabled(self):
        """policy_master.json should have require_sso_for_idp=false."""
        policy = PolicyConfig.from_policy_master()
        assert policy.secondary_gates.require_sso_for_idp == False, \
            "policy_master.json should have require_sso_for_idp=false to match AOD"

    def test_policy_master_idp_passes_without_sso(self):
        """Policy from policy_master.json should pass IdP without SSO."""
        policy = PolicyConfig.from_policy_master()
        assert policy.idp_passes_gates(has_sso=False) == True, \
            "policy_master.json IdP gate should pass without SSO"
