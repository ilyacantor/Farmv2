"""
Farm-level expected-block self-consistency validation.

Runs on every generated snapshot (before any AOD call) to validate:
1. Non-empty reason codes for all assets
2. HAS_ONGOING_FINANCE => HAS_FINANCE (ongoing is subset of finance)
3. STALE_ACTIVITY and RECENT_ACTIVITY are mutually exclusive
4. NO_IDP and HAS_IDP are mutually exclusive
5. NO_CMDB and HAS_CMDB are mutually exclusive

Fail mode: Broken expected generation = grading can't be trusted.
"""
from dataclasses import dataclass, field
from typing import Optional


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


def validate_expected_block(expected_block: dict) -> ValidationResult:
    """
    Validate the expected block for self-consistency.
    
    Args:
        expected_block: The __expected__ dict from a snapshot
        
    Returns:
        ValidationResult with errors/warnings if any
    """
    result = ValidationResult(valid=True)
    
    expected_reasons = expected_block.get('expected_reasons', {})
    decision_traces = expected_block.get('decision_traces', {})
    expected_admission = expected_block.get('expected_admission', {})
    
    if not expected_reasons and not decision_traces:
        result.add_error(
            asset_key='__expected_block__',
            rule='EMPTY_EXPECTED_BLOCK',
            message='Expected block has no reason codes or decision traces - grading impossible',
            reason_codes=[]
        )
        return result
    
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
        return result
    
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
    
    return result


def validate_snapshot_expected(snapshot: dict) -> ValidationResult:
    """
    Validate the __expected__ block of a full snapshot.
    
    Args:
        snapshot: Full snapshot dict containing __expected__
        
    Returns:
        ValidationResult
    """
    expected_block = snapshot.get('__expected__')
    if not expected_block:
        result = ValidationResult(valid=False)
        result.add_error(
            asset_key='__snapshot__',
            rule='EXPECTED_BLOCK_MISSING',
            message='Snapshot has no __expected__ block - cannot grade',
            reason_codes=[]
        )
        return result
    
    return validate_expected_block(expected_block)
