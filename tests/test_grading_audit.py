"""
Tests for Farm Grading Correctness Audit Suite.

Includes:
1. Determinism tests - verify N runs produce identical results
2. Consistency tests - mutual exclusion, implications
3. Negative tests for contract violations (UPSTREAM_ERROR, INVALID_INPUT_CONTRACT, INVALID_SNAPSHOT)
"""
import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import patch

from src.services.grading_audit import (
    run_full_audit,
    audit_determinism,
    audit_consistency,
    audit_finance_traceability,
    audit_activity_invariants,
    audit_gradeability,
    _hash_expected_block,
    _extract_category_sets,
    _extract_reason_codes_map,
)
from src.services.reconciliation import compute_expected_block
from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import ScaleEnum, EnterpriseProfileEnum, RealismProfileEnum


def generate_snapshot(seed: int = 12345, scale: ScaleEnum = ScaleEnum.small, snapshot_time: datetime = None):
    """Helper to generate a snapshot for testing."""
    generator = EnterpriseGenerator(
        tenant_id=f"TestTenant-{seed}",
        seed=seed,
        scale=scale,
        enterprise_profile=EnterpriseProfileEnum.modern_saas,
        realism_profile=RealismProfileEnum.typical,
        snapshot_time=snapshot_time,
    )
    return generator.generate()


class TestDeterminismAudit:
    """Tests for determinism/idempotency audit."""
    
    def test_determinism_passes_for_valid_snapshot(self):
        """Determinism audit should pass for a properly generated snapshot."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        result = audit_determinism(snapshot_dict, n_runs=5)
        
        assert result['passed'] is True
        assert result['runs'] == 5
        assert result['unique_hashes'] == 1
        assert result['hash'] is not None
        assert 'diff' not in result
    
    def test_determinism_produces_consistent_counts(self):
        """Category counts should be consistent across runs."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=123, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        result = audit_determinism(snapshot_dict, n_runs=10)
        
        assert result['passed'] is True
        assert 'counts' in result
        assert 'shadow' in result['counts']
        assert 'zombie' in result['counts']
        assert 'clean' in result['counts']


class TestConsistencyAudit:
    """Tests for expected-block self-consistency audit."""
    
    def test_consistency_passes_for_valid_snapshot(self):
        """Consistency audit should pass for properly generated snapshot."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        result = audit_consistency(snapshot_dict)
        
        assert result['passed'] is True
        assert result['assets_checked'] > 0
        assert 'EXPECTED_BLOCK_CONSISTENCY' in result['checks_performed']
    
    def test_consistency_detects_mutual_exclusion_violation(self):
        """Audit should fail if RECENT_ACTIVITY and STALE_ACTIVITY both present."""
        fake_expected = {
            'expected_reasons': {
                'bad-asset.com': ['HAS_DISCOVERY', 'RECENT_ACTIVITY', 'STALE_ACTIVITY'],
            },
            'decision_traces': {
                'bad-asset.com': {'reason_codes': ['HAS_DISCOVERY', 'RECENT_ACTIVITY', 'STALE_ACTIVITY']},
            },
        }
        fake_snapshot = {'__expected__': fake_expected}
        
        result = audit_consistency(fake_snapshot, expected=fake_expected)
        
        assert result['passed'] is False
        assert any('MUTUAL_EXCLUSION' in e['rule'] for e in result['errors'])
    
    def test_consistency_detects_implication_violation(self):
        """Audit should fail if HAS_ONGOING_FINANCE without HAS_FINANCE."""
        fake_expected = {
            'expected_reasons': {
                'bad-asset.com': ['HAS_DISCOVERY', 'HAS_ONGOING_FINANCE'],
            },
            'decision_traces': {
                'bad-asset.com': {'reason_codes': ['HAS_DISCOVERY', 'HAS_ONGOING_FINANCE']},
            },
        }
        fake_snapshot = {'__expected__': fake_expected}
        
        result = audit_consistency(fake_snapshot, expected=fake_expected)
        
        assert result['passed'] is False
        assert any('IMPLICATION_VIOLATION' in e['rule'] for e in result['errors'])
    
    def test_consistency_detects_empty_reason_codes(self):
        """Audit should fail if any asset has empty reason codes."""
        fake_expected = {
            'expected_reasons': {
                'no-reasons.com': [],
            },
            'decision_traces': {
                'no-reasons.com': {'reason_codes': []},
            },
        }
        fake_snapshot = {'__expected__': fake_expected}
        
        result = audit_consistency(fake_snapshot, expected=fake_expected)
        
        assert result['passed'] is False
        assert any('NON_EMPTY_REASONS' in e['rule'] for e in result['errors'])


class TestGradeabilityAuditNegative:
    """Negative tests for gradeability enforcement - MUST fail with explicit contract statuses."""
    
    def test_html_response_returns_upstream_error(self):
        """
        NEGATIVE TEST 1: HTML response must return UPSTREAM_ERROR, not silent failure.
        
        Scenario: AOD returns HTML error page instead of JSON.
        Expected: contract_status = UPSTREAM_ERROR (not "no evidence" or empty result)
        """
        html_response = """<!DOCTYPE html>
<html>
<head><title>503 Service Unavailable</title></head>
<body><h1>503 Service Unavailable</h1></body>
</html>"""
        
        result = audit_gradeability(html_response)
        
        assert result['passed'] is False
        assert result['contract_status'] == 'UPSTREAM_ERROR'
        assert any('HTML' in e['message'] for e in result['errors'])
    
    def test_null_response_returns_upstream_error(self):
        """
        NEGATIVE TEST 2: Null response must return UPSTREAM_ERROR, not silent failure.
        
        Scenario: AOD returns null or connection timeout.
        Expected: contract_status = UPSTREAM_ERROR (not "no evidence")
        """
        result = audit_gradeability(None)
        
        assert result['passed'] is False
        assert result['contract_status'] == 'UPSTREAM_ERROR'
        assert any('null' in e['message'].lower() for e in result['errors'])
    
    def test_missing_required_fields_returns_invalid_contract(self):
        """
        NEGATIVE TEST 3: Missing required fields must return INVALID_INPUT_CONTRACT.
        
        Scenario: AOD returns JSON but missing shadows, zombies, or actual_reason_codes.
        Expected: contract_status = INVALID_INPUT_CONTRACT (not silent skip)
        """
        incomplete_response = {
            "some_field": "value",
            "other_field": [],
        }
        
        result = audit_gradeability(incomplete_response)
        
        assert result['passed'] is False
        assert result['contract_status'] == 'INVALID_INPUT_CONTRACT'
        assert any('missing' in e['message'].lower() for e in result['errors'])
    
    def test_invalid_json_string_returns_upstream_error(self):
        """
        NEGATIVE TEST 4: Invalid JSON string must return UPSTREAM_ERROR.
        
        Scenario: Response is a string that's not valid JSON.
        Expected: contract_status = UPSTREAM_ERROR
        """
        invalid_json = "{ broken json here, missing quotes }"
        
        result = audit_gradeability(invalid_json)
        
        assert result['passed'] is False
        assert result['contract_status'] == 'UPSTREAM_ERROR'
    
    def test_valid_aod_response_passes(self):
        """Valid AOD response with all required fields should pass."""
        valid_response = {
            "shadows": ["shadow1.com", "shadow2.com"],
            "zombies": ["zombie1.com"],
            "actual_reason_codes": {
                "shadow1.com": ["HAS_DISCOVERY", "NO_IDP"],
                "shadow2.com": ["HAS_DISCOVERY", "NO_CMDB"],
                "zombie1.com": ["HAS_DISCOVERY", "STALE_ACTIVITY"],
            },
        }
        
        result = audit_gradeability(valid_response)
        
        assert result['passed'] is True
        assert result['contract_status'] == 'PASS'


class TestFullAuditReport:
    """Tests for the complete audit report generation."""
    
    def test_full_audit_returns_report_structure(self):
        """Full audit should return proper report structure."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        report = run_full_audit(
            snapshot=snapshot_dict,
            snapshot_id="test-snapshot-123",
            n_runs=3,
        )
        
        assert report.snapshot_id == "test-snapshot-123"
        assert report.audit_timestamp is not None
        assert report.contract_status in ['PASS', 'INVALID_SNAPSHOT', 'UPSTREAM_ERROR', 'INVALID_INPUT_CONTRACT']
        assert 'determinism' in report.to_dict()
        assert 'consistency' in report.to_dict()
        assert 'finance_traceability' in report.to_dict()
        assert 'activity_invariants' in report.to_dict()
    
    def test_full_audit_fails_without_expected_block(self):
        """Full audit should fail with INVALID_SNAPSHOT if __expected__ block missing."""
        bad_snapshot = {
            'meta': {'created_at': '2025-01-15T12:00:00Z'},
            'planes': {},
        }
        
        report = run_full_audit(
            snapshot=bad_snapshot,
            snapshot_id="bad-snapshot",
            n_runs=3,
        )
        
        assert report.contract_status == 'INVALID_SNAPSHOT'
        assert any('EXPECTED_BLOCK' in e.check for e in report.errors)


class TestActivityInvariantsAudit:
    """Tests for activity as-of invariants audit."""
    
    def test_activity_audit_validates_golden_fixtures(self):
        """Activity audit should validate golden fixture test cases."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        result = audit_activity_invariants(snapshot_dict, activity_window_days=90)
        
        assert 'golden_results' in result
        assert result['golden_results']['test_count'] == 6
        assert result['golden_results']['passed'] is True
    
    def test_activity_audit_detects_future_timestamps(self):
        """Activity audit should detect and flag future timestamps."""
        future_date = (datetime.utcnow() + timedelta(days=60)).isoformat() + 'Z'
        
        fake_snapshot = {
            'meta': {'created_at': datetime.utcnow().isoformat() + 'Z'},
            'planes': {
                'discovery': {
                    'observations': [
                        {'domain': 'future.com', 'observed_at': future_date}
                    ]
                },
                'idp': {'objects': []},
            },
            '__expected__': {
                'expected_reasons': {},
                'decision_traces': {},
            },
        }
        
        result = audit_activity_invariants(fake_snapshot, activity_window_days=90)
        
        assert result['future_timestamp_count'] > 0


class TestFinanceTraceabilityAudit:
    """Tests for finance anchoring traceability audit."""
    
    def test_finance_audit_traces_evidence(self):
        """Finance audit should trace HAS_ONGOING_FINANCE to evidence refs."""
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot_dict = json.loads(snapshot.model_dump_json())
        
        result = audit_finance_traceability(
            snapshot_dict,
            target_keys=['zapier.com', 'slack.com']
        )
        
        assert 'traces' in result
        assert result['target_keys_checked'] == 2
        for trace in result['traces']:
            assert 'asset_key' in trace
            assert 'expected_status' in trace
            assert 'grounded' in trace


class TestHashFunctions:
    """Tests for internal hash and extraction functions."""
    
    def test_hash_is_deterministic(self):
        """Hash function should produce same result for same input."""
        expected = {
            'shadow_expected': [{'asset_key': 'a.com'}, {'asset_key': 'b.com'}],
            'zombie_expected': [{'asset_key': 'z.com'}],
            'expected_reasons': {'a.com': ['HAS_DISCOVERY']},
        }
        
        hash1 = _hash_expected_block(expected)
        hash2 = _hash_expected_block(expected)
        
        assert hash1 == hash2
    
    def test_hash_differs_for_different_input(self):
        """Hash function should produce different results for different input."""
        expected1 = {'shadow_expected': [{'asset_key': 'a.com'}]}
        expected2 = {'shadow_expected': [{'asset_key': 'b.com'}]}
        
        hash1 = _hash_expected_block(expected1)
        hash2 = _hash_expected_block(expected2)
        
        assert hash1 != hash2
    
    def test_category_extraction_handles_all_formats(self):
        """Category extraction should handle dict and string formats."""
        expected = {
            'shadow_expected': [{'asset_key': 'a.com'}, 'b.com'],
            'zombie_expected': [{'asset_key': 'z.com'}],
            'clean_expected': ['c.com'],
            'parked_expected': [],
            'expected_admission': {'rejected.com': 'rejected'},
        }
        
        result = _extract_category_sets(expected)
        
        assert 'a.com' in result['shadow']
        assert 'b.com' in result['shadow']
        assert 'z.com' in result['zombie']
        assert 'c.com' in result['clean']
        assert 'rejected.com' in result['rejected']
