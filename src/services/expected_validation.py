"""
Farm-level expected-block and snapshot self-consistency validation.

Runs on every generated snapshot (before any AOD call) to validate:
1. Expected-block self-consistency (reason codes, mutual exclusion, implication rules)
2. As-of clock invariants (timestamps within plausible bounds, RECENT/STALE deterministic)
3. Ongoing finance determinism + distribution sanity
4. Cross-plane join hygiene (no hidden join keys, only realistic correlation keys)
5. Gradeability prerequisites

Fail mode: Broken generation = grading can't be trusted.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional
from dateutil import parser as dateutil_parser


@dataclass
class ValidationError:
    """Single validation error."""
    asset_key: str
    rule: str
    message: str
    reason_codes: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Result of expected-block validation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    assets_checked: int = 0
    checks_performed: list[str] = field(default_factory=list)
    
    def add_error(self, asset_key: str, rule: str, message: str, reason_codes: list[str] = None):
        self.errors.append(ValidationError(
            asset_key=asset_key,
            rule=rule,
            message=message,
            reason_codes=reason_codes or []
        ))
        self.valid = False
    
    def add_warning(self, asset_key: str, rule: str, message: str, reason_codes: list[str] = None):
        self.warnings.append(ValidationError(
            asset_key=asset_key,
            rule=rule,
            message=message,
            reason_codes=reason_codes or []
        ))
    
    def to_dict(self) -> dict:
        return {
            'valid': self.valid,
            'assets_checked': self.assets_checked,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'checks_performed': self.checks_performed,
            'errors': [
                {
                    'asset_key': e.asset_key,
                    'rule': e.rule,
                    'message': e.message,
                    'reason_codes': e.reason_codes,
                }
                for e in self.errors
            ],
            'warnings': [
                {
                    'asset_key': w.asset_key,
                    'rule': w.rule,
                    'message': w.message,
                    'reason_codes': w.reason_codes,
                }
                for w in self.warnings
            ],
        }


MUTUALLY_EXCLUSIVE_PAIRS = [
    ('STALE_ACTIVITY', 'RECENT_ACTIVITY'),
    ('NO_IDP', 'HAS_IDP'),
    ('NO_CMDB', 'HAS_CMDB'),
]

IMPLICATION_RULES = [
    ('HAS_ONGOING_FINANCE', 'HAS_FINANCE'),
]

REALISTIC_CORRELATION_KEYS = {
    'domain', 'registered_domain', 'vendor_name', 'account_id', 'subscription_id',
    'ci_id', 'idp_object_id', 'cloud_account', 'tenant_id',
}

MAX_TIMESTAMP_FUTURE_HOURS = 24
MAX_TIMESTAMP_PAST_DAYS = 730


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string."""
    if not ts:
        return None
    try:
        dt = dateutil_parser.parse(ts)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError, AttributeError):
        return None


def validate_expected_block_consistency(expected_block: dict, result: ValidationResult) -> None:
    """Check 1: Expected-block self-consistency (reason codes, mutual exclusion, implications)."""
    result.checks_performed.append('EXPECTED_BLOCK_CONSISTENCY')
    
    expected_reasons = expected_block.get('expected_reasons', {})
    decision_traces = expected_block.get('decision_traces', {})
    
    if not expected_reasons and not decision_traces:
        result.add_error(
            asset_key='__expected_block__',
            rule='EMPTY_EXPECTED_BLOCK',
            message='Expected block has no reason codes or decision traces - grading impossible',
            reason_codes=[]
        )
        return
    
    if not expected_reasons:
        result.add_error(
            asset_key='__expected_reasons__',
            rule='MISSING_EXPECTED_REASONS',
            message='expected_reasons is empty - no reason code coverage for grading',
            reason_codes=[]
        )
    
    if not decision_traces:
        result.add_error(
            asset_key='__decision_traces__',
            rule='MISSING_DECISION_TRACES',
            message='decision_traces is empty - no audit trail for classification decisions',
            reason_codes=[]
        )
    
    all_asset_keys = set(expected_reasons.keys()) | set(decision_traces.keys())
    result.assets_checked = len(all_asset_keys)
    
    if len(all_asset_keys) == 0:
        result.add_error(
            asset_key='__expected_block__',
            rule='ZERO_ASSETS_CHECKED',
            message='No assets found in expected block - broken generation',
            reason_codes=[]
        )
        return
    
    for asset_key in all_asset_keys:
        reasons = expected_reasons.get(asset_key, [])
        trace_reasons = []
        if asset_key in decision_traces:
            trace_reasons = decision_traces[asset_key].get('reason_codes', [])
        
        effective_reasons = reasons if reasons else trace_reasons
        
        if not effective_reasons:
            result.add_error(
                asset_key=asset_key,
                rule='NON_EMPTY_REASONS',
                message=f"Asset has no reason codes - grading cannot determine classification basis",
                reason_codes=[]
            )
            continue
        
        reason_set = set(effective_reasons)
        
        for code_a, code_b in MUTUALLY_EXCLUSIVE_PAIRS:
            if code_a in reason_set and code_b in reason_set:
                result.add_error(
                    asset_key=asset_key,
                    rule='MUTUAL_EXCLUSION',
                    message=f"Contradictory flags: {code_a} and {code_b} cannot both be present",
                    reason_codes=effective_reasons
                )
        
        for antecedent, consequent in IMPLICATION_RULES:
            if antecedent in reason_set and consequent not in reason_set:
                result.add_error(
                    asset_key=asset_key,
                    rule='IMPLICATION_VIOLATION',
                    message=f"{antecedent} requires {consequent} (ongoing finance implies finance exists)",
                    reason_codes=effective_reasons
                )


def validate_clock_invariants(snapshot: dict, result: ValidationResult) -> None:
    """Check 2: As-of clock invariants - timestamps within plausible bounds."""
    result.checks_performed.append('CLOCK_INVARIANTS')
    
    meta = snapshot.get('meta', {})
    created_at_str = meta.get('created_at')
    
    if not created_at_str:
        result.add_error(
            asset_key='__meta__',
            rule='MISSING_CREATED_AT',
            message='Snapshot missing created_at timestamp - cannot validate recency',
            reason_codes=[]
        )
        return
    
    created_at = _parse_timestamp(created_at_str)
    if not created_at:
        result.add_error(
            asset_key='__meta__',
            rule='INVALID_CREATED_AT',
            message=f'Cannot parse created_at timestamp: {created_at_str}',
            reason_codes=[]
        )
        return
    
    now = datetime.utcnow()
    max_future = now + timedelta(hours=MAX_TIMESTAMP_FUTURE_HOURS)
    max_past = now - timedelta(days=MAX_TIMESTAMP_PAST_DAYS)
    
    if created_at > max_future:
        result.add_error(
            asset_key='__meta__',
            rule='FUTURE_TIMESTAMP',
            message=f'created_at is in the future: {created_at_str}',
            reason_codes=[]
        )
    
    planes = snapshot.get('planes', {})
    discovery = planes.get('discovery', {})
    observations = discovery.get('observations', [])
    
    future_count = 0
    extreme_past_count = 0
    
    for obs in observations:
        obs_ts_str = obs.get('observed_at')
        if not obs_ts_str:
            continue
        obs_ts = _parse_timestamp(obs_ts_str)
        if not obs_ts:
            continue
        
        if obs_ts > max_future:
            future_count += 1
        if obs_ts < max_past:
            extreme_past_count += 1
    
    if future_count > 0:
        result.add_error(
            asset_key='__discovery__',
            rule='FUTURE_OBSERVATION_TIMESTAMPS',
            message=f'{future_count} observations have future timestamps - clock invariant violated',
            reason_codes=[]
        )
    
    if extreme_past_count > len(observations) * 0.5 and len(observations) > 10:
        result.add_warning(
            asset_key='__discovery__',
            rule='EXTREME_PAST_TIMESTAMPS',
            message=f'{extreme_past_count}/{len(observations)} observations have timestamps >2 years old',
            reason_codes=[]
        )


def validate_finance_consistency(snapshot: dict, expected_block: dict, result: ValidationResult) -> None:
    """Check 3: Ongoing finance determinism + distribution sanity."""
    result.checks_performed.append('FINANCE_CONSISTENCY')
    
    planes = snapshot.get('planes', {})
    finance = planes.get('finance', {})
    transactions = finance.get('transactions', [])
    
    if not transactions:
        return
    
    vendor_has_ongoing = {}
    vendor_has_onetime = {}
    
    for tx in transactions:
        vendor = tx.get('vendor_name', '')
        is_recurring = tx.get('is_recurring', False)
        
        if not vendor:
            continue
        
        if is_recurring:
            vendor_has_ongoing[vendor] = True
        else:
            if vendor not in vendor_has_ongoing:
                vendor_has_onetime[vendor] = True
    
    decision_traces = expected_block.get('decision_traces', {})
    
    for asset_key, trace in decision_traces.items():
        reason_codes = trace.get('reason_codes', [])
        has_ongoing = 'HAS_ONGOING_FINANCE' in reason_codes
        has_finance = 'HAS_FINANCE' in reason_codes
        
        if has_ongoing and not has_finance:
            result.add_error(
                asset_key=asset_key,
                rule='FINANCE_IMPLICATION_VIOLATED',
                message='HAS_ONGOING_FINANCE without HAS_FINANCE - implication rule violated',
                reason_codes=reason_codes
            )
    
    meta = snapshot.get('meta', {})
    realism = meta.get('realism_profile', 'typical')
    
    total_vendors = len(vendor_has_ongoing) + len(vendor_has_onetime)
    ongoing_count = len(vendor_has_ongoing)
    
    if total_vendors > 10:
        ongoing_pct = ongoing_count / total_vendors
        
        if realism == 'messy' or realism == 'typical':
            if ongoing_pct > 0.95:
                result.add_warning(
                    asset_key='__finance__',
                    rule='FINANCE_DISTRIBUTION_EXTREME',
                    message=f'{ongoing_pct*100:.0f}% vendors have ongoing finance - extreme for {realism} profile',
                    reason_codes=[]
                )
            if ongoing_pct < 0.05 and realism != 'clean':
                result.add_warning(
                    asset_key='__finance__',
                    rule='FINANCE_DISTRIBUTION_EXTREME',
                    message=f'Only {ongoing_pct*100:.0f}% vendors have ongoing finance - unexpectedly low for {realism} profile',
                    reason_codes=[]
                )


def validate_join_hygiene(snapshot: dict, result: ValidationResult) -> None:
    """Check 4: Cross-plane join hygiene - no hidden join keys."""
    result.checks_performed.append('JOIN_HYGIENE')
    
    planes = snapshot.get('planes', {})
    
    plane_keys_by_field = {}
    
    discovery = planes.get('discovery', {})
    for obs in discovery.get('observations', []):
        for field_name in obs.keys():
            if field_name not in plane_keys_by_field:
                plane_keys_by_field[field_name] = set()
            plane_keys_by_field[field_name].add('discovery')
    
    idp = planes.get('idp', {})
    for entry in idp.get('entries', []):
        for field_name in entry.keys():
            if field_name not in plane_keys_by_field:
                plane_keys_by_field[field_name] = set()
            plane_keys_by_field[field_name].add('idp')
    
    cmdb = planes.get('cmdb', {})
    for ci in cmdb.get('configuration_items', []):
        for field_name in ci.keys():
            if field_name not in plane_keys_by_field:
                plane_keys_by_field[field_name] = set()
            plane_keys_by_field[field_name].add('cmdb')
    
    finance = planes.get('finance', {})
    for tx in finance.get('transactions', []):
        for field_name in tx.keys():
            if field_name not in plane_keys_by_field:
                plane_keys_by_field[field_name] = set()
            plane_keys_by_field[field_name].add('finance')
    
    cloud = planes.get('cloud', {})
    for resource in cloud.get('resources', []):
        for field_name in resource.keys():
            if field_name not in plane_keys_by_field:
                plane_keys_by_field[field_name] = set()
            plane_keys_by_field[field_name].add('cloud')
    
    ALLOWED_SHARED_FIELDS = {
        'domain', 'registered_domain', 'vendor_name', 'vendor_hint',
        'app_name', 'name', 'display_name', 'observed_name',
        'account_id', 'subscription_id', 'tenant_id',
        'ci_id', 'object_id',
        'timestamp', 'created_at', 'observed_at', 'last_seen_at',
        'source', 'type', 'status', 'environment', 'lifecycle',
        'region', 'provider', 'amount', 'currency',
        'category', 'category_hint', 'description', 'notes',
        'is_recurring', 'payment_type', 'payment_date',
        'hostname', 'observed_uri', 'signin_url', 'icon_url',
        'resource_type', 'service_name', 'cloud_provider',
        'ci_type', 'idp_type', 'owner', 'team', 'cost_center',
    }
    
    suspicious_shared = []
    for field_name, plane_set in plane_keys_by_field.items():
        if len(plane_set) > 1 and field_name not in ALLOWED_SHARED_FIELDS:
            if field_name.startswith('_') or field_name.endswith('_id'):
                continue
            if 'farm' in field_name.lower() or 'expected' in field_name.lower():
                suspicious_shared.append((field_name, list(plane_set)))
    
    if suspicious_shared:
        for field_name, planes_list in suspicious_shared[:3]:
            result.add_warning(
                asset_key='__planes__',
                rule='SUSPICIOUS_SHARED_FIELD',
                message=f"Field '{field_name}' appears in multiple planes {planes_list} - verify it's a realistic key",
                reason_codes=[]
            )


def validate_stress_test_coverage(expected_block: dict, result: ValidationResult) -> None:
    """Check 5: Verify stress test scenarios are reflected in expected block."""
    result.checks_performed.append('STRESS_TEST_COVERAGE')
    
    decision_traces = expected_block.get('decision_traces', {})
    shadow_expected = expected_block.get('shadow_expected', [])
    zombie_expected = expected_block.get('zombie_expected', [])
    
    stress_domains = {
        'splitbrain-app.io': 'Split Brain',
        'toxic-legacy.internal': 'Toxic Asset',
        'banned-by-policy.com': 'Banned Asset',
        'zombie-abandoned.corp': 'Zombie Asset',
    }
    
    all_keys = set(decision_traces.keys())
    shadow_keys = {s.get('asset_key', '') for s in shadow_expected}
    zombie_keys = {z.get('asset_key', '') for z in zombie_expected}
    
    for domain, scenario_name in stress_domains.items():
        if domain not in all_keys:
            result.add_warning(
                asset_key=domain,
                rule='STRESS_TEST_MISSING',
                message=f'{scenario_name} stress test domain not found in decision traces',
                reason_codes=[]
            )


def validate_expected_block(expected_block: dict) -> ValidationResult:
    """
    Validate the expected block for self-consistency.
    
    Args:
        expected_block: The __expected__ dict from a snapshot
        
    Returns:
        ValidationResult with errors/warnings if any
    """
    result = ValidationResult(valid=True)
    validate_expected_block_consistency(expected_block, result)
    validate_stress_test_coverage(expected_block, result)
    return result


def validate_snapshot_expected(snapshot: dict) -> ValidationResult:
    """
    Validate the __expected__ block and snapshot planes for consistency.
    
    Args:
        snapshot: Full snapshot dict containing __expected__ and planes
        
    Returns:
        ValidationResult
    """
    result = ValidationResult(valid=True)
    
    expected_block = snapshot.get('__expected__')
    if not expected_block:
        result.add_error(
            asset_key='__snapshot__',
            rule='EXPECTED_BLOCK_MISSING',
            message='Snapshot has no __expected__ block - cannot grade',
            reason_codes=[]
        )
        return result
    
    validate_expected_block_consistency(expected_block, result)
    validate_clock_invariants(snapshot, result)
    validate_finance_consistency(snapshot, expected_block, result)
    validate_join_hygiene(snapshot, result)
    validate_stress_test_coverage(expected_block, result)
    
    return result


def validate_gradeability(aod_output: dict, result: ValidationResult) -> None:
    """
    Check 5: Gradeability gate - validate AOD output has required fields.
    
    Sets INVALID_INPUT_CONTRACT if AOD output is missing required fields.
    """
    result.checks_performed.append('GRADEABILITY_GATE')
    
    if not aod_output:
        result.add_error(
            asset_key='__aod_output__',
            rule='INVALID_INPUT_CONTRACT',
            message='AOD output is empty or null - cannot grade',
            reason_codes=[]
        )
        return
    
    if isinstance(aod_output, str):
        if '<html' in aod_output.lower() or '<!doctype' in aod_output.lower():
            result.add_error(
                asset_key='__aod_output__',
                rule='UPSTREAM_ERROR',
                message='AOD returned HTML instead of JSON - upstream error',
                reason_codes=[]
            )
            return
    
    required_fields = ['shadows', 'zombies', 'actual_reason_codes']
    missing_fields = []
    
    for field in required_fields:
        if field not in aod_output or aod_output.get(field) is None:
            missing_fields.append(field)
    
    if missing_fields:
        result.add_error(
            asset_key='__aod_output__',
            rule='INVALID_INPUT_CONTRACT',
            message=f'AOD output missing required fields: {missing_fields}',
            reason_codes=[]
        )
    
    actual_reason_codes = aod_output.get('actual_reason_codes', {})
    shadows = aod_output.get('shadows', [])
    zombies = aod_output.get('zombies', [])
    
    classified_assets = set()
    for s in shadows:
        key = s.get('asset_key') or s.get('domain') or s.get('key')
        if key:
            classified_assets.add(key)
    for z in zombies:
        key = z.get('asset_key') or z.get('domain') or z.get('key')
        if key:
            classified_assets.add(key)
    
    assets_missing_reasons = []
    for asset_key in classified_assets:
        if asset_key not in actual_reason_codes or not actual_reason_codes.get(asset_key):
            assets_missing_reasons.append(asset_key)
    
    if assets_missing_reasons:
        result.add_error(
            asset_key='__aod_output__',
            rule='INVALID_INPUT_CONTRACT',
            message=f'{len(assets_missing_reasons)} classified assets have empty reason codes - cannot grade',
            reason_codes=assets_missing_reasons[:5]
        )
