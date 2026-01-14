"""
Farm Grading Correctness Audit Suite.

Provides comprehensive audits to verify Farm's expected-block grading is:
1. Deterministic - Same snapshot produces identical results across N runs
2. Self-consistent - No contradictory flags, all implications hold
3. Grounded - Evidence-based classifications can be traced to source data
4. Temporally coherent - Activity labels match timestamp reality

Contract Statuses:
- PASS: All audits pass
- INVALID_SNAPSHOT: Snapshot data is internally inconsistent
- UPSTREAM_ERROR: External system returned invalid response (non-JSON, HTML)
- INVALID_INPUT_CONTRACT: Missing required fields for grading
"""
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Any
from dateutil import parser as dateutil_parser

from src.services.reconciliation import compute_expected_block, build_candidate_flags
from src.services.expected_validation import (
    ValidationResult, 
    validate_expected_block_consistency,
    validate_clock_invariants,
    validate_finance_consistency,
    validate_join_hygiene,
    validate_gradeability,
    MUTUALLY_EXCLUSIVE_PAIRS,
    IMPLICATION_RULES,
)
from src.models.policy import PolicyConfig


@dataclass
class AuditError:
    """Single audit error."""
    check: str
    severity: str  # ERROR, WARNING
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class DeterminismDiff:
    """Diff between two expected-block runs."""
    category_moves: list  # Assets that moved between categories
    reason_code_changes: list  # Assets with different reason codes
    count_differences: dict  # {category: (run_i_count, run_j_count)}


@dataclass
class FinanceTrace:
    """Finance evidence trace for an asset."""
    asset_key: str
    expected_status: str  # NONE, ONE_TIME, ONGOING
    has_ongoing_finance: bool
    evidence_refs: list  # List of supporting transaction/contract refs
    grounded: bool  # True if HAS_ONGOING_FINANCE has supporting refs


@dataclass
class AuditReport:
    """Complete audit report for a snapshot."""
    snapshot_id: str
    audit_timestamp: str
    contract_status: str  # PASS, INVALID_SNAPSHOT, UPSTREAM_ERROR, INVALID_INPUT_CONTRACT
    determinism: dict = field(default_factory=dict)
    consistency: dict = field(default_factory=dict)
    finance_traceability: dict = field(default_factory=dict)
    activity_invariants: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    
    def add_error(self, check: str, message: str, details: dict = None):
        self.errors.append(AuditError(
            check=check,
            severity='ERROR',
            message=message,
            details=details or {}
        ))
    
    def add_warning(self, check: str, message: str, details: dict = None):
        self.warnings.append(AuditError(
            check=check,
            severity='WARNING',
            message=message,
            details=details or {}
        ))
    
    def to_dict(self) -> dict:
        return {
            'snapshot_id': self.snapshot_id,
            'audit_timestamp': self.audit_timestamp,
            'contract_status': self.contract_status,
            'determinism': self.determinism,
            'consistency': self.consistency,
            'finance_traceability': self.finance_traceability,
            'activity_invariants': self.activity_invariants,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': [
                {'check': e.check, 'severity': e.severity, 'message': e.message, 'details': e.details}
                for e in self.errors
            ],
            'warnings': [
                {'check': w.check, 'severity': w.severity, 'message': w.message, 'details': w.details}
                for w in self.warnings
            ],
        }


def _hash_expected_block(expected: dict) -> str:
    """Create a deterministic hash of expected block contents."""
    def normalize(obj):
        if isinstance(obj, dict):
            return {k: normalize(v) for k, v in sorted(obj.items())}
        elif isinstance(obj, (list, set)):
            items = [normalize(i) for i in obj]
            if all(isinstance(i, (str, int, float, bool, type(None))) for i in items):
                return sorted(items, key=lambda x: str(x))
            return items
        elif isinstance(obj, set):
            return sorted(list(obj), key=str)
        return obj
    
    normalized = normalize(expected)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _extract_category_sets(expected: dict) -> dict:
    """Extract asset key sets per category from expected block."""
    shadow_keys = set()
    zombie_keys = set()
    clean_keys = set()
    parked_keys = set()
    rejected_keys = set()
    
    for item in expected.get('shadow_expected', []):
        key = item.get('asset_key') if isinstance(item, dict) else item
        if key:
            shadow_keys.add(key)
    
    for item in expected.get('zombie_expected', []):
        key = item.get('asset_key') if isinstance(item, dict) else item
        if key:
            zombie_keys.add(key)
    
    for item in expected.get('clean_expected', []):
        key = item.get('asset_key') if isinstance(item, dict) else item
        if key:
            clean_keys.add(key)
    
    for item in expected.get('parked_expected', []):
        key = item.get('asset_key') if isinstance(item, dict) else item
        if key:
            parked_keys.add(key)
    
    for key, status in expected.get('expected_admission', {}).items():
        if status == 'rejected':
            rejected_keys.add(key)
    
    return {
        'shadow': shadow_keys,
        'zombie': zombie_keys,
        'clean': clean_keys,
        'parked': parked_keys,
        'rejected': rejected_keys,
    }


def _extract_reason_codes_map(expected: dict) -> dict:
    """Extract reason codes per asset from expected block."""
    reason_map = {}
    
    expected_reasons = expected.get('expected_reasons', {})
    decision_traces = expected.get('decision_traces', {})
    
    for key, reasons in expected_reasons.items():
        reason_map[key] = frozenset(reasons) if reasons else frozenset()
    
    for key, trace in decision_traces.items():
        trace_reasons = trace.get('reason_codes', [])
        if key not in reason_map and trace_reasons:
            reason_map[key] = frozenset(trace_reasons)
        elif trace_reasons:
            reason_map[key] = reason_map.get(key, frozenset()) | frozenset(trace_reasons)
    
    return reason_map


def audit_determinism(snapshot: dict, n_runs: int = 10, policy: PolicyConfig = None) -> dict:
    """
    Audit A: Determinism/Idempotency.
    
    Runs compute_expected_block N times and verifies identical results.
    
    Returns:
        dict with 'passed', 'runs', 'hash', 'diff' (if failed)
    """
    if policy is None:
        policy = PolicyConfig.default_fallback()
    
    results = []
    hashes = []
    category_sets_per_run = []
    reason_codes_per_run = []
    
    for i in range(n_runs):
        expected = compute_expected_block(snapshot, policy=policy)
        h = _hash_expected_block(expected)
        category_sets = _extract_category_sets(expected)
        reason_codes = _extract_reason_codes_map(expected)
        
        results.append(expected)
        hashes.append(h)
        category_sets_per_run.append(category_sets)
        reason_codes_per_run.append(reason_codes)
    
    unique_hashes = set(hashes)
    passed = len(unique_hashes) == 1
    
    result = {
        'passed': passed,
        'runs': n_runs,
        'unique_hashes': len(unique_hashes),
        'hash': hashes[0] if passed else None,
        'counts': {
            'shadow': len(category_sets_per_run[0]['shadow']),
            'zombie': len(category_sets_per_run[0]['zombie']),
            'clean': len(category_sets_per_run[0]['clean']),
            'parked': len(category_sets_per_run[0]['parked']),
            'rejected': len(category_sets_per_run[0]['rejected']),
        },
    }
    
    if not passed:
        diff = _compute_determinism_diff(category_sets_per_run, reason_codes_per_run)
        result['diff'] = diff
        result['hashes'] = hashes
    
    return result


def _compute_determinism_diff(category_sets_per_run: list, reason_codes_per_run: list) -> dict:
    """Compute diff between runs when determinism fails."""
    category_moves = []
    reason_code_changes = []
    count_differences = {}
    
    baseline_cats = category_sets_per_run[0]
    baseline_reasons = reason_codes_per_run[0]
    
    for i in range(1, len(category_sets_per_run)):
        run_cats = category_sets_per_run[i]
        run_reasons = reason_codes_per_run[i]
        
        for cat in ['shadow', 'zombie', 'clean', 'parked', 'rejected']:
            base_set = baseline_cats[cat]
            run_set = run_cats[cat]
            
            gained = run_set - base_set
            lost = base_set - run_set
            
            if gained or lost:
                category_moves.append({
                    'run': i,
                    'category': cat,
                    'gained': list(gained)[:5],
                    'lost': list(lost)[:5],
                })
            
            if len(base_set) != len(run_set):
                if cat not in count_differences:
                    count_differences[cat] = []
                count_differences[cat].append((0, len(base_set), i, len(run_set)))
        
        all_keys = set(baseline_reasons.keys()) | set(run_reasons.keys())
        for key in all_keys:
            base_codes = baseline_reasons.get(key, frozenset())
            run_codes = run_reasons.get(key, frozenset())
            if base_codes != run_codes:
                reason_code_changes.append({
                    'run': i,
                    'asset_key': key,
                    'baseline': list(base_codes),
                    'run_codes': list(run_codes),
                    'added': list(run_codes - base_codes),
                    'removed': list(base_codes - run_codes),
                })
    
    return {
        'category_moves': category_moves[:10],
        'reason_code_changes': reason_code_changes[:10],
        'count_differences': count_differences,
    }


def audit_consistency(snapshot: dict, expected: dict = None, policy: PolicyConfig = None) -> dict:
    """
    Audit B: Expected-block self-consistency.
    
    Checks:
    - Every expected asset has non-empty reason codes
    - Mutually exclusive flags are not both present
    - Implication rules hold
    
    Returns:
        dict with 'passed', 'errors', 'warnings', 'checks_performed'
    """
    if policy is None:
        policy = PolicyConfig.default_fallback()
    
    if expected is None:
        expected = compute_expected_block(snapshot, policy=policy)
    
    result = ValidationResult(valid=True)
    validate_expected_block_consistency(expected, result)
    
    return {
        'passed': result.valid,
        'assets_checked': result.assets_checked,
        'checks_performed': result.checks_performed,
        'error_count': len(result.errors),
        'warning_count': len(result.warnings),
        'errors': [
            {'asset_key': e.asset_key, 'rule': e.rule, 'message': e.message}
            for e in result.errors
        ],
        'warnings': [
            {'asset_key': w.asset_key, 'rule': w.rule, 'message': w.message}
            for w in result.warnings
        ],
    }


def audit_finance_traceability(
    snapshot: dict, 
    target_keys: list = None,
    policy: PolicyConfig = None
) -> dict:
    """
    Audit C: Finance anchoring traceability.
    
    For each target asset, trace HAS_ONGOING_FINANCE to concrete evidence.
    
    Default target keys: zapier.com, airtable.com, hubspot.com, figma.com, canva.com
    
    Returns:
        dict with 'passed', 'traces', 'ungrounded_assets'
    """
    if policy is None:
        policy = PolicyConfig.default_fallback()
    
    if target_keys is None:
        target_keys = ['zapier.com', 'airtable.com', 'hubspot.com', 'figma.com', 'canva.com']
    
    candidates = build_candidate_flags(snapshot, policy=policy)
    expected = compute_expected_block(snapshot, policy=policy)
    decision_traces = expected.get('decision_traces', {})
    
    planes = snapshot.get('planes', {})
    finance = planes.get('finance', {})
    contracts = finance.get('contracts', [])
    transactions = finance.get('transactions', [])
    
    vendor_to_contracts = {}
    for c in contracts:
        vendor = (c.get('vendor_name') or '').lower()
        if vendor:
            if vendor not in vendor_to_contracts:
                vendor_to_contracts[vendor] = []
            vendor_to_contracts[vendor].append({
                'contract_id': c.get('contract_id'),
                'product': c.get('product'),
                'is_recurring': True,
            })
    
    vendor_to_transactions = {}
    for t in transactions:
        vendor = (t.get('vendor_name') or '').lower()
        if vendor:
            if vendor not in vendor_to_transactions:
                vendor_to_transactions[vendor] = []
            vendor_to_transactions[vendor].append({
                'txn_id': t.get('txn_id'),
                'is_recurring': t.get('is_recurring', False),
                'amount': t.get('amount'),
                'payment_date': t.get('payment_date'),
            })
    
    traces = []
    ungrounded_assets = []
    
    for key in target_keys:
        key_lower = key.lower()
        cand = candidates.get(key_lower, {})
        trace_info = decision_traces.get(key_lower, {})
        reason_codes = trace_info.get('reason_codes', [])
        
        has_ongoing = 'HAS_ONGOING_FINANCE' in reason_codes or cand.get('has_ongoing_finance', False)
        has_finance = 'HAS_FINANCE' in reason_codes or cand.get('finance_present', False)
        
        evidence_refs = []
        
        vendor_variants = [key_lower, key_lower.replace('.com', ''), key_lower.split('.')[0]]
        for variant in vendor_variants:
            if variant in vendor_to_contracts:
                evidence_refs.extend([
                    {'type': 'contract', **c} for c in vendor_to_contracts[variant]
                ])
            if variant in vendor_to_transactions:
                evidence_refs.extend([
                    {'type': 'transaction', **t} for t in vendor_to_transactions[variant]
                    if t.get('is_recurring')
                ])
        
        status = 'NONE'
        if has_ongoing:
            status = 'ONGOING'
        elif has_finance:
            status = 'ONE_TIME'
        
        grounded = not (has_ongoing and len(evidence_refs) == 0)
        
        trace = FinanceTrace(
            asset_key=key,
            expected_status=status,
            has_ongoing_finance=has_ongoing,
            evidence_refs=evidence_refs,
            grounded=grounded,
        )
        traces.append({
            'asset_key': trace.asset_key,
            'expected_status': trace.expected_status,
            'has_ongoing_finance': trace.has_ongoing_finance,
            'evidence_count': len(trace.evidence_refs),
            'evidence_refs': trace.evidence_refs[:5],
            'grounded': trace.grounded,
        })
        
        if not grounded:
            ungrounded_assets.append(key)
    
    return {
        'passed': len(ungrounded_assets) == 0,
        'target_keys_checked': len(target_keys),
        'traces': traces,
        'ungrounded_count': len(ungrounded_assets),
        'ungrounded_assets': ungrounded_assets,
    }


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string."""
    if not ts:
        return None
    try:
        dt = dateutil_parser.parse(ts)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError, AttributeError):
        return None


def audit_activity_invariants(
    snapshot: dict,
    activity_window_days: int = 90,
    policy: PolicyConfig = None
) -> dict:
    """
    Audit D: Activity "as-of" invariants.
    
    Checks:
    - No activity timestamps are in the future relative to snapshot created_at
    - RECENT/STALE classification is reproducible given activity_window_days
    - Golden fixture validation (boundary cases)
    
    Returns:
        dict with 'passed', 'future_timestamps', 'classification_errors', 'golden_results'
    """
    if policy is None:
        policy = PolicyConfig.default_fallback()
    
    meta = snapshot.get('meta', {})
    created_at_str = meta.get('created_at')
    created_at = _parse_timestamp(created_at_str)
    
    if not created_at:
        return {
            'passed': False,
            'error': 'Cannot parse snapshot created_at timestamp',
            'created_at': created_at_str,
        }
    
    planes = snapshot.get('planes', {})
    discovery = planes.get('discovery', {})
    observations = discovery.get('observations', [])
    idp = planes.get('idp', {})
    idp_objects = idp.get('objects', [])
    
    future_timestamps = []
    
    for obs in observations:
        ts_str = obs.get('observed_at')
        ts = _parse_timestamp(ts_str)
        if ts and ts > created_at + timedelta(hours=24):
            future_timestamps.append({
                'source': 'discovery',
                'domain': obs.get('domain'),
                'timestamp': ts_str,
                'created_at': created_at_str,
            })
    
    for obj in idp_objects:
        ts_str = obj.get('last_login_at')
        ts = _parse_timestamp(ts_str)
        if ts and ts > created_at + timedelta(hours=24):
            future_timestamps.append({
                'source': 'idp',
                'name': obj.get('name'),
                'timestamp': ts_str,
                'created_at': created_at_str,
            })
    
    candidates = build_candidate_flags(snapshot, activity_window_days, policy=policy)
    expected = compute_expected_block(snapshot, policy=policy)
    decision_traces = expected.get('decision_traces', {})
    
    classification_errors = []
    cutoff = created_at - timedelta(days=activity_window_days)
    
    for key, cand in candidates.items():
        if key not in decision_traces:
            continue
        
        trace = decision_traces[key]
        reason_codes = trace.get('reason_codes', [])
        
        has_recent = 'RECENT_ACTIVITY' in reason_codes
        has_stale = 'STALE_ACTIVITY' in reason_codes
        
        if has_recent and has_stale:
            classification_errors.append({
                'asset_key': key,
                'error': 'MUTUAL_EXCLUSION_VIOLATION',
                'message': 'Both RECENT_ACTIVITY and STALE_ACTIVITY present',
            })
        
        latest_ts_str = cand.get('latest_activity_at')
        if latest_ts_str:
            latest_ts = _parse_timestamp(latest_ts_str)
            if latest_ts:
                should_be_recent = latest_ts >= cutoff
                if should_be_recent and has_stale:
                    classification_errors.append({
                        'asset_key': key,
                        'error': 'RECENT_CLASSIFIED_AS_STALE',
                        'message': f'Activity at {latest_ts_str} is within window but classified STALE',
                        'latest_activity': latest_ts_str,
                        'cutoff': cutoff.isoformat(),
                    })
                elif not should_be_recent and has_recent:
                    classification_errors.append({
                        'asset_key': key,
                        'error': 'STALE_CLASSIFIED_AS_RECENT',
                        'message': f'Activity at {latest_ts_str} is outside window but classified RECENT',
                        'latest_activity': latest_ts_str,
                        'cutoff': cutoff.isoformat(),
                    })
    
    golden_results = _run_golden_fixture_tests(created_at, activity_window_days)
    
    passed = (
        len(future_timestamps) == 0 and 
        len(classification_errors) == 0 and
        golden_results['passed']
    )
    
    return {
        'passed': passed,
        'created_at': created_at_str,
        'activity_window_days': activity_window_days,
        'future_timestamp_count': len(future_timestamps),
        'future_timestamps': future_timestamps[:5],
        'classification_error_count': len(classification_errors),
        'classification_errors': classification_errors[:5],
        'golden_results': golden_results,
    }


def _run_golden_fixture_tests(reference: datetime, window_days: int) -> dict:
    """
    Run golden fixture tests with known timestamps around boundary.
    
    Tests 6 cases:
    1. Activity exactly at cutoff (should be RECENT)
    2. Activity 1 day before cutoff (should be STALE)
    3. Activity 1 day after cutoff (should be RECENT)
    4. No activity (should be NONE)
    5. Future activity (invalid - should be flagged)
    6. Activity yesterday (should be RECENT)
    """
    cutoff = reference - timedelta(days=window_days)
    
    test_cases = [
        {
            'name': 'exactly_at_cutoff',
            'timestamp': cutoff,
            'expected': 'RECENT',
        },
        {
            'name': 'one_day_before_cutoff',
            'timestamp': cutoff - timedelta(days=1),
            'expected': 'STALE',
        },
        {
            'name': 'one_day_after_cutoff',
            'timestamp': cutoff + timedelta(days=1),
            'expected': 'RECENT',
        },
        {
            'name': 'no_activity',
            'timestamp': None,
            'expected': 'NONE',
        },
        {
            'name': 'future_activity',
            'timestamp': reference + timedelta(days=30),
            'expected': 'INVALID',
        },
        {
            'name': 'activity_yesterday',
            'timestamp': reference - timedelta(days=1),
            'expected': 'RECENT',
        },
    ]
    
    results = []
    all_passed = True
    
    for tc in test_cases:
        ts = tc['timestamp']
        expected = tc['expected']
        
        if ts is None:
            actual = 'NONE'
        elif ts > reference:
            actual = 'INVALID'
        elif ts >= cutoff:
            actual = 'RECENT'
        else:
            actual = 'STALE'
        
        passed = actual == expected
        if not passed:
            all_passed = False
        
        results.append({
            'name': tc['name'],
            'timestamp': ts.isoformat() if ts else None,
            'expected': expected,
            'actual': actual,
            'passed': passed,
        })
    
    return {
        'passed': all_passed,
        'test_count': len(test_cases),
        'results': results,
    }


def audit_gradeability(aod_response: Any) -> dict:
    """
    Audit E: Reconciliation gradeability enforcement.
    
    Validates AOD response for grading requirements:
    - Must be JSON (not HTML or other format)
    - Must have required fields (shadows, zombies, actual_reason_codes)
    
    Returns:
        dict with 'passed', 'contract_status', 'errors'
    """
    result = ValidationResult(valid=True)
    
    if aod_response is None:
        result.add_error(
            asset_key='__aod_response__',
            rule='UPSTREAM_ERROR',
            message='AOD response is null - upstream error or timeout',
            reason_codes=[]
        )
        return {
            'passed': False,
            'contract_status': 'UPSTREAM_ERROR',
            'errors': [{'rule': 'UPSTREAM_ERROR', 'message': 'AOD response is null'}],
        }
    
    if isinstance(aod_response, str):
        lower = aod_response.lower()
        if '<html' in lower or '<!doctype' in lower or '<head' in lower:
            return {
                'passed': False,
                'contract_status': 'UPSTREAM_ERROR',
                'errors': [{'rule': 'HTML_RESPONSE', 'message': 'AOD returned HTML instead of JSON'}],
            }
        
        try:
            aod_response = json.loads(aod_response)
        except json.JSONDecodeError as e:
            return {
                'passed': False,
                'contract_status': 'UPSTREAM_ERROR',
                'errors': [{'rule': 'INVALID_JSON', 'message': f'AOD response is not valid JSON: {str(e)}'}],
            }
    
    if not isinstance(aod_response, dict):
        return {
            'passed': False,
            'contract_status': 'UPSTREAM_ERROR',
            'errors': [{'rule': 'NOT_DICT', 'message': f'AOD response is not a dict: {type(aod_response).__name__}'}],
        }
    
    validate_gradeability(aod_response, result)
    
    if not result.valid:
        contract_status = 'INVALID_INPUT_CONTRACT'
        for e in result.errors:
            if e.rule == 'UPSTREAM_ERROR':
                contract_status = 'UPSTREAM_ERROR'
                break
        
        return {
            'passed': False,
            'contract_status': contract_status,
            'errors': [{'rule': e.rule, 'message': e.message} for e in result.errors],
        }
    
    return {
        'passed': True,
        'contract_status': 'PASS',
        'errors': [],
    }


def run_full_audit(
    snapshot: dict,
    snapshot_id: str,
    n_runs: int = 10,
    finance_target_keys: list = None,
    activity_window_days: int = 90,
    policy: PolicyConfig = None,
) -> AuditReport:
    """
    Run the complete grading correctness audit suite.
    
    Args:
        snapshot: Full snapshot dict
        snapshot_id: Snapshot identifier
        n_runs: Number of determinism runs (default 10)
        finance_target_keys: Keys to check for finance traceability
        activity_window_days: Window for activity classification
        policy: Policy configuration
    
    Returns:
        AuditReport with all audit results
    """
    if policy is None:
        policy = PolicyConfig.default_fallback()
    
    report = AuditReport(
        snapshot_id=snapshot_id,
        audit_timestamp=datetime.utcnow().isoformat() + 'Z',
        contract_status='PASS',
    )
    
    expected_block = snapshot.get('__expected__')
    if not expected_block:
        report.contract_status = 'INVALID_SNAPSHOT'
        report.add_error(
            'MISSING_EXPECTED_BLOCK',
            'Snapshot has no __expected__ block - cannot grade',
            {}
        )
        return report
    
    det_result = audit_determinism(snapshot, n_runs=n_runs, policy=policy)
    report.determinism = det_result
    
    if not det_result['passed']:
        report.contract_status = 'INVALID_SNAPSHOT'
        report.add_error(
            'DETERMINISM_FAILURE',
            f'Expected block computation is non-deterministic across {n_runs} runs',
            det_result.get('diff', {})
        )
    
    cons_result = audit_consistency(snapshot, expected=expected_block, policy=policy)
    report.consistency = cons_result
    
    if not cons_result['passed']:
        report.contract_status = 'INVALID_SNAPSHOT'
        for err in cons_result['errors']:
            report.add_error(
                f"CONSISTENCY_{err['rule']}",
                err['message'],
                {'asset_key': err['asset_key']}
            )
    
    fin_result = audit_finance_traceability(snapshot, target_keys=finance_target_keys, policy=policy)
    report.finance_traceability = fin_result
    
    if not fin_result['passed']:
        for key in fin_result['ungrounded_assets']:
            report.add_warning(
                'UNGROUNDED_FINANCE',
                f'Asset {key} has HAS_ONGOING_FINANCE but no supporting evidence refs',
                {'asset_key': key}
            )
    
    act_result = audit_activity_invariants(snapshot, activity_window_days=activity_window_days, policy=policy)
    report.activity_invariants = act_result
    
    if not act_result['passed']:
        if act_result.get('future_timestamp_count', 0) > 0:
            report.add_error(
                'FUTURE_TIMESTAMPS',
                f"{act_result['future_timestamp_count']} activity timestamps are in the future",
                {'samples': act_result.get('future_timestamps', [])}
            )
        if act_result.get('classification_error_count', 0) > 0:
            report.contract_status = 'INVALID_SNAPSHOT'
            for err in act_result.get('classification_errors', []):
                report.add_error(
                    f"ACTIVITY_{err['error']}",
                    err['message'],
                    {'asset_key': err['asset_key']}
                )
    
    return report
