"""
Reconciliation service for computing expected classifications and comparing with AOD.

================================================================================
FARM GOVERNANCE & CORRELATION CONTRACT
================================================================================

AUTHORITATIVE TRUTH SOURCES
---------------------------
Farm treats the following as authoritative sources of truth:
  - CMDB
  - IdP

No other signals may assert governance.

GOVERNANCE RULES (HARD REQUIREMENTS)
------------------------------------
An asset is GOVERNED if and only if there exists at least one authoritative
record (CMDB or IdP) that EXPLICITLY PASSES ALL GOVERNANCE GATES.

CMDB Governance:
  A CMDB record grants governance ONLY IF:
    - CI exists
    - CI type is valid (per policy.secondary_gates.valid_ci_types)
    - CI lifecycle is valid (per policy.secondary_gates.invalid_lifecycle_states)
  
  If a CMDB record exists BUT FAILS ANY GATE:
    -> Explicitly NOT governed (NO_CMDB)
  
  If no CMDB record exists:
    -> NOT governed

IdP Governance:
  An IdP record grants governance ONLY IF:
    - Explicit IdP linkage exists
    - Required SSO gate passes (if policy.secondary_gates.require_sso_for_idp)
  
  If an IdP record exists BUT FAILS ANY GATE:
    -> Explicitly NOT governed (NO_IDP)
  
  If no IdP record exists:
    -> NOT governed

CLASSIFICATION LOGIC
--------------------
  governed = cmdb_present OR idp_present

Where:
  - cmdb_present = True only if a CMDB record passes all gates
  - idp_present = True only if an IdP record passes all gates

Otherwise:
  - governed = False

HEURISTICS (STRICTLY NON-AUTHORITATIVE)
---------------------------------------
Heuristics may be used ONLY to:
  - Suggest possible relationships
  - Enrich context
  - Generate hypotheses
  - Drive follow-up discovery

Heuristics MUST NEVER:
  - Assert governance
  - Override CMDB or IdP gate outcomes
  - Flip classification states
  - Set cmdb_present or idp_present

Examples of non-authoritative heuristics:
  - Fuzzy name matching
  - Vendor inference
  - Token or contains matching
  - Cross-TLD similarity

These may exist as annotations but CANNOT affect classification.

DETERMINISM GUARANTEE
---------------------
Given identical inputs (evidence + policy):
  - Farm must always produce the same classification
  - No probabilistic or confidence-based logic may alter outcomes

FAILURE MODE DEFINITION
-----------------------
If Farm and AOD disagree on classification under the same evidence and policy:
  - One of them contains a bug
  - There is no "policy difference" or "interpretation" explanation

INVARIANT: CMDB and IdP assert truth. Heuristics suggest context.
           Classification is deterministic.
================================================================================
"""

import os
from datetime import datetime
from typing import Optional
from collections import defaultdict
from enum import Enum
from dateutil import parser as dateutil_parser

from src.models.planes import FarmExpectations, ReconcileStatusEnum


class ActivityStatus(str, Enum):
    """Activity status for asset classification."""
    RECENT = "RECENT"  # Has activity within the detection window
    STALE = "STALE"    # Has activity but older than detection window
    NONE = "NONE"      # No activity timestamps at all


from src.models.policy import PolicyConfig
from src.services.constants import (
    INFRASTRUCTURE_DOMAINS,
    VENDOR_DOMAIN_SETS,
    DOMAIN_TO_VENDOR,
)
from src.services.key_normalization import (
    normalize_name,
    extract_domain,
    extract_registered_domain,
    is_external_domain,
    is_valid_fqdn,
)
from src.services.logging import trace_log


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime using dateutil."""
    if not ts:
        return None
    try:
        dt = dateutil_parser.parse(ts)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError, AttributeError) as e:
        trace_log("reconciliation", "parse_timestamp_error", {
            "timestamp": ts,
            "error": str(e)
        })
        return None


def is_within_window(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days <= window_days


def is_stale(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days > window_days


def build_candidate_flags(snapshot: dict, window_days: int = 90, policy: PolicyConfig = None) -> dict:
    """Build candidate flags from snapshot planes. Returns {key: {flags...}}.
    
    When policy is provided, secondary gates are applied:
    - require_sso_for_idp: IdP objects without SSO are treated as NO_IDP
    - require_valid_ci_type: CMDB CIs with invalid types are treated as NO_CMDB
    - require_valid_lifecycle: CMDB CIs with invalid lifecycles are treated as NO_CMDB
    """
    meta = snapshot.get('meta', {})
    planes = snapshot.get('planes', {})
    reference = parse_timestamp(meta.get('created_at')) or datetime.utcnow()
    
    candidates = defaultdict(lambda: {
        'key': '',
        'names': set(),
        'domains': set(),
        'vendors': set(),
        'idp_present': False,
        'cmdb_present': False,
        'cmdb_matches': [],
        'cmdb_resolution_reason': 'NONE',
        'finance_present': False,
        'has_ongoing_finance': False,
        'cloud_present': False,
        'discovery_present': False,
        'activity_present': False,
        'stale_timestamps': [],
        'activity_timestamps': [],
        'latest_activity_at': None,
        'activity_source': 'none',
        'discovery_sources': set(),
        'activity_status': ActivityStatus.NONE,
        'anchored': False,
        'security_attested': False,
    })
    
    # TWO-PASS KEY SELECTION CONTRACT:
    # Pass 1: Collect all observations grouped by eTLD+1 domain
    # Pass 2: Apply alias collapse and select canonical key via lexicographic sort
    #
    # NOT ALLOWED: "first observation wins", list position, CMDB domains for keying
    
    from src.services.key_normalization import select_canonical_key
    
    # Get policy parameters for key selection
    banned_domains_set = set(policy.banned_domains) if policy else set()
    alias_collapse = policy.alias_domains_to_collapse if policy else {}
    
    # PASS 1: Group observations by initial eTLD+1 key
    observations = planes.get('discovery', {}).get('observations', [])
    proto_candidates = defaultdict(lambda: {
        'names': set(),
        'domains': set(),
        'vendors': set(),
        'observations': [],
    })
    
    for obs in observations:
        raw_domain = obs.get('domain') or extract_domain(obs.get('observed_uri') or obs.get('hostname') or '')
        name = obs.get('observed_name', '')
        initial_key = extract_registered_domain(raw_domain) if raw_domain else normalize_name(name)
        if not initial_key:
            continue
        
        proto_candidates[initial_key]['names'].add(name)
        if raw_domain:
            proto_candidates[initial_key]['domains'].add(raw_domain)
            proto_candidates[initial_key]['domains'].add(initial_key)
        vendor_hint = obs.get('vendor_hint')
        if vendor_hint:
            proto_candidates[initial_key]['vendors'].add(vendor_hint)
        proto_candidates[initial_key]['observations'].append(obs)
    
    # PASS 2: Select canonical key and create final candidates
    # Apply alias collapse to merge related domains, then lexicographic sort for tie-breaker
    for initial_key, proto in proto_candidates.items():
        # Select canonical key from all observed domains
        canonical_key, rejection_reason = select_canonical_key(
            proto['domains'] if proto['domains'] else {initial_key},
            banned_domains=banned_domains_set,
            alias_collapse=alias_collapse,
        )
        
        if not canonical_key:
            # Banned or no valid domains - skip this candidate
            continue
        
        key = canonical_key
        
        # Create/update candidate with canonical key
        candidates[key]['key'] = key
        candidates[key]['names'].update(proto['names'])
        candidates[key]['domains'].update(proto['domains'])
        candidates[key]['vendors'].update(proto['vendors'])
        candidates[key]['discovery_present'] = True
        
        # Process observation timestamps
        for obs in proto['observations']:
            ts = obs.get('observed_at')
            source = obs.get('source', 'unknown')
            if ts:
                candidates[key]['activity_timestamps'].append({'ts': ts, 'source': source})
                if is_within_window(ts, window_days, reference):
                    candidates[key]['activity_present'] = True
                    candidates[key]['discovery_sources'].add(source)
                    ts_dt = parse_timestamp(ts)
                    latest = parse_timestamp(candidates[key]['latest_activity_at'])
                    if ts_dt and (not latest or ts_dt > latest):
                        candidates[key]['latest_activity_at'] = ts
                        candidates[key]['activity_source'] = source
                elif is_stale(ts, window_days, reference):
                    candidates[key]['stale_timestamps'].append(ts)
    
    # PERFORMANCE: Build reverse indexes BEFORE processing governance planes
    # This enables O(1) lookups instead of O(N) scans for IdP, CMDB, and Finance matching
    key_to_normalized = {key: normalize_name(key) for key in candidates.keys()}
    normalized_to_keys = defaultdict(set)
    vendor_to_keys = defaultdict(set)
    name_to_keys = defaultdict(set)
    domain_to_keys = defaultdict(set)

    for key, cand in candidates.items():
        # Index by normalized key
        normalized_to_keys[key_to_normalized[key]].add(key)
        # Index by normalized vendors
        for vendor in cand['vendors']:
            vendor_to_keys[normalize_name(vendor)].add(key)
        # Index by normalized names
        for name in cand['names']:
            name_to_keys[normalize_name(name)].add(key)
        # Index by domains
        for domain in cand['domains']:
            domain_to_keys[domain].add(key)

    # Process IdP objects with O(N) complexity using precomputed indexes
    # POLICY: If asset is in IdP AND passes secondary gates, admit it
    #
    # KEY SELECTION CONTRACT: IdP domains are ENRICHMENT ONLY, not for key selection
    # - Use canonical_domain for correlation (matching to existing candidates)
    # - Do NOT use external_ref extracted domains for keying
    idp_objects = planes.get('idp', {}).get('objects', [])
    for obj in idp_objects:
        name = normalize_name(obj.get('name', ''))
        canonical_domain = obj.get('canonical_domain')
        # CONTRACT: external_ref is stored for reference but NOT used for key selection
        external_ref = obj.get('external_ref', '')
        # Only use canonical_domain for matching - NOT extracted from external_ref
        idp_registered = canonical_domain
        has_sso = obj.get('has_sso', False)
        matched_keys = set()
        
        # Check secondary gates: require_sso_for_idp
        # If gate is enabled and has_sso is False, treat as NO_IDP
        idp_passes_gate = True
        if policy and not policy.idp_passes_gates(has_sso):
            idp_passes_gate = False
        
        # CORRELATION: Use canonical_domain for matching to existing candidates
        # CONTRACT: Only match to candidates keyed by the IdP canonical_domain
        # Do NOT use domain_to_keys index - that's too aggressive and causes 
        # correlation drift with AOD (Farm finds matches AOD doesn't)
        if idp_registered:
            # Direct key match using canonical domain
            if idp_registered in candidates:
                matched_keys.add(idp_registered)
            # Also try registered domain extraction for normalized matching
            registered = extract_registered_domain(idp_registered)
            if registered and registered in candidates:
                matched_keys.add(registered)
        
        # Fallback: O(1) lookups by name if no domain match
        if not matched_keys and name:
            # Match by normalized key
            matched_keys.update(normalized_to_keys.get(name, set()))
            # Match by normalized names in candidates
            matched_keys.update(name_to_keys.get(name, set()))
        
        # CONTRACT: Do NOT use external_ref domain for matching
        # (removed: domain lookup from external_ref)
        
        # CONTRACT: Do NOT create IdP-only candidates
        # AOD requires discovery evidence for admission - IdP is enrichment only, not admission gate
        # If there's no discovery candidate for this domain, skip IdP correlation
        # (This prevents Farm from admitting assets that AOD would reject)
        
        # Mark matched candidates - only if IdP passes the gate
        for key in matched_keys:
            if idp_passes_gate:
                candidates[key]['idp_present'] = True
        
        ts = obj.get('last_login_at')
        if ts and matched_keys:
            for key in matched_keys:
                cand = candidates[key]
                cand['activity_timestamps'].append({'ts': ts, 'source': 'idp'})
                if is_within_window(ts, window_days, reference):
                    cand['activity_present'] = True
                    ts_dt = parse_timestamp(ts)
                    latest = parse_timestamp(cand['latest_activity_at'])
                    if ts_dt and (not latest or ts_dt > latest):
                        cand['latest_activity_at'] = ts
                        cand['activity_source'] = 'idp'
                elif is_stale(ts, window_days, reference):
                    cand['stale_timestamps'].append(ts)

    # Process CMDB CIs with O(N) complexity instead of O(N*M)
    # POLICY: If asset is in CMDB AND passes secondary gates, admit it
    #
    # KEY SELECTION CONTRACT: CMDB domains are ENRICHMENT ONLY, not for key selection
    # - Use canonical_domain for correlation (matching to existing candidates)
    # - Do NOT use external_ref extracted domains for keying
    # - Do NOT create new candidates from external_ref domains
    cmdb_cis = planes.get('cmdb', {}).get('cis', [])
    for ci in cmdb_cis:
        name = normalize_name(ci.get('name', ''))
        canonical_domain = ci.get('canonical_domain')
        # CONTRACT: external_ref is stored for reference but NOT used for key selection
        external_ref = ci.get('external_ref', '')
        # Only use canonical_domain for matching - NOT extracted from external_ref
        cmdb_registered = canonical_domain
        ci_vendor = normalize_name(ci.get('vendor', '') or '')
        ci_type = ci.get('ci_type')
        lifecycle = ci.get('lifecycle')

        # Check secondary gates: require_valid_ci_type, require_valid_lifecycle
        # If gates are enabled and CI fails them, treat as NO_CMDB
        cmdb_passes_gate = True
        if policy and not policy.cmdb_passes_gates(ci_type, lifecycle):
            cmdb_passes_gate = False

        matched_keys = set()
        matched_by_vendor = set()

        # CORRELATION: Use canonical_domain for matching to existing candidates
        # CONTRACT: Only match to candidates keyed by the CMDB canonical_domain
        # Do NOT use domain_to_keys index - that's too aggressive and causes 
        # correlation drift with AOD (Farm finds matches AOD doesn't)
        if cmdb_registered:
            # Direct key match using canonical domain
            if cmdb_registered in candidates:
                matched_keys.add(cmdb_registered)
            # Also try registered domain extraction for normalized matching
            registered = extract_registered_domain(cmdb_registered)
            if registered and registered in candidates:
                matched_keys.add(registered)
            
        # Fallback: Direct lookups by name if no domain match
        if not matched_keys and name:
            if name in key_to_normalized.values():
                matched_keys.update(k for k, norm_k in key_to_normalized.items() if norm_k == name)
            matched_keys.update(name_to_keys.get(name, set()))

        # CONTRACT: Do NOT use external_ref domain for matching
        # (removed: domain lookup from external_ref)

        if ci_vendor:
            vendor_keys = vendor_to_keys.get(ci_vendor, set())
            matched_by_vendor.update(vendor_keys - matched_keys)

        # CONTRACT: Do NOT create CMDB-only candidates
        # AOD requires discovery evidence for admission - CMDB is enrichment only, not admission gate
        # If there's no discovery candidate for this domain, skip CMDB correlation
        # (This prevents Farm from admitting assets that AOD would reject)

        # Update candidates with matches - only if CMDB passes the gate
        for key in matched_keys:
            if cmdb_passes_gate:
                candidates[key]['cmdb_present'] = True
            candidates[key]['cmdb_matches'].append({
                'ci_id': ci.get('ci_id'),
                'name': ci.get('name'),
                'lifecycle': lifecycle,
                'vendor': ci.get('vendor'),
                'ci_type': ci_type,
                'matched_via_vendor': False,
                'passes_gate': cmdb_passes_gate,
            })

        for key in matched_by_vendor:
            if cmdb_passes_gate:
                candidates[key]['cmdb_present'] = True
            candidates[key]['cmdb_matches'].append({
                'ci_id': ci.get('ci_id'),
                'name': ci.get('name'),
                'lifecycle': lifecycle,
                'vendor': ci.get('vendor'),
                'ci_type': ci_type,
                'matched_via_vendor': True,
                'passes_gate': cmdb_passes_gate,
            })
    
    # Process cloud resources with O(N) complexity
    cloud_resources = planes.get('cloud', {}).get('resources', [])
    for res in cloud_resources:
        name = normalize_name(res.get('name', ''))
        if name:
            # Direct lookup by name
            for key in name_to_keys.get(name, set()):
                candidates[key]['cloud_present'] = True
            # Substring matching (kept for compatibility but limited scope)
            for key, cand in candidates.items():
                normalized_names = {normalize_name(n) for n in cand['names']}
                if any(n in name or name in n for n in normalized_names):
                    cand['cloud_present'] = True

    # Process finance contracts with O(N) complexity
    contracts = planes.get('finance', {}).get('contracts', [])
    transactions = planes.get('finance', {}).get('transactions', [])

    for contract in contracts:
        vendor = normalize_name(contract.get('vendor_name', ''))
        product = normalize_name(contract.get('product', '') or '')

        matched_keys = set()
        # Direct vendor lookup
        if vendor:
            matched_keys.update(vendor_to_keys.get(vendor, set()))
            # Substring match on names (limited scope)
            for key in name_to_keys.keys():
                if vendor in key or key in vendor:
                    matched_keys.update(name_to_keys[key])

        # Product name matching
        if product:
            matched_keys.update(name_to_keys.get(product, set()))
            for key in name_to_keys.keys():
                if product in key or key in product:
                    matched_keys.update(name_to_keys[key])

        for key in matched_keys:
            candidates[key]['finance_present'] = True
            candidates[key]['has_ongoing_finance'] = True

    # Process transactions with O(N) complexity
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        is_recurring = txn.get('is_recurring', False)

        matched_keys = set()
        if vendor:
            matched_keys.update(vendor_to_keys.get(vendor, set()))
            # Substring match
            for key in name_to_keys.keys():
                if vendor in key or key in vendor:
                    matched_keys.update(name_to_keys[key])

        for key in matched_keys:
            candidates[key]['finance_present'] = True
            if is_recurring:
                candidates[key]['has_ongoing_finance'] = True
    
    for key, cand in candidates.items():
        cand['cmdb_resolution_reason'] = determine_cmdb_resolution_reason(cand['cmdb_matches'], cand['vendors'])
    
    security_plane = planes.get('security', {})
    attestations = security_plane.get('attestations', []) if security_plane else []
    
    security_domain_index = defaultdict(set)
    security_name_index = defaultdict(set)
    for i, att in enumerate(attestations):
        domain = att.get('domain')
        name = normalize_name(att.get('asset_name', ''))
        vendor = normalize_name(att.get('vendor', '') or '')
        
        if domain:
            reg_domain = extract_registered_domain(domain)
            if reg_domain:
                security_domain_index[reg_domain].add(i)
            security_domain_index[domain].add(i)
        if name:
            security_name_index[name].add(i)
        if vendor:
            security_name_index[vendor].add(i)
    
    for key, cand in candidates.items():
        matched_attestation_indices = set()
        
        for domain in cand.get('domains', set()):
            matched_attestation_indices.update(security_domain_index.get(domain, set()))
        
        matched_attestation_indices.update(security_domain_index.get(key, set()))
        
        for name in cand.get('names', set()):
            matched_attestation_indices.update(security_name_index.get(normalize_name(name), set()))
        
        for vendor in cand.get('vendors', set()):
            matched_attestation_indices.update(security_name_index.get(normalize_name(vendor), set()))
        
        if matched_attestation_indices:
            cand['security_attested'] = True
    
    for key, cand in candidates.items():
        if cand['activity_present']:
            cand['activity_status'] = ActivityStatus.RECENT
        elif len(cand['stale_timestamps']) > 0:
            cand['activity_status'] = ActivityStatus.STALE
        else:
            cand['activity_status'] = ActivityStatus.NONE
        
        cand['anchored'] = (
            cand['idp_present'] or 
            cand['cmdb_present'] or 
            cand['has_ongoing_finance'] or 
            cand['cloud_present']
        )
    
    discovery_count = sum(1 for c in candidates.values() if c.get('discovery_present'))
    idp_count = sum(1 for c in candidates.values() if c.get('idp_present'))
    cmdb_count = sum(1 for c in candidates.values() if c.get('cmdb_present'))
    finance_count = sum(1 for c in candidates.values() if c.get('finance_present'))
    anchored_count = sum(1 for c in candidates.values() if c.get('anchored'))
    recent_count = sum(1 for c in candidates.values() if c.get('activity_status') == ActivityStatus.RECENT)
    stale_count = sum(1 for c in candidates.values() if c.get('activity_status') == ActivityStatus.STALE)
    trace_log("reconciliation", "build_candidate_flags", {
        "total_candidates": len(candidates),
        "discovery_present": discovery_count,
        "idp_present": idp_count,
        "cmdb_present": cmdb_count,
        "finance_present": finance_count,
        "anchored": anchored_count,
        "activity_recent": recent_count,
        "activity_stale": stale_count,
    })
    
    return dict(candidates)

def determine_cmdb_resolution_reason(cmdb_matches: list, candidate_vendors: set) -> str:
    """Determine CMDB resolution reason based on matched CIs.
    
    Returns one of:
    - NONE: Single clear match or no matches
    - MULTI_ENV: Same app in dev/staging/prod CIs
    - LEGACY: Old/deprecated CI alongside current  
    - DUPLICATE: True duplicate records (same name, or multiple CIs without clear differentiation)
    - PARENT_VENDOR: Matched via parent vendor relationship (e.g., Slack matched to Salesforce CMDB entry)
    """
    if len(cmdb_matches) == 0:
        return 'NONE'
    
    has_vendor_match = any(m.get('matched_via_vendor') for m in cmdb_matches)
    
    if len(cmdb_matches) == 1:
        if has_vendor_match:
            return 'PARENT_VENDOR'
        return 'NONE'
    
    names = [m.get('name', '').lower() for m in cmdb_matches]
    lifecycles = [m.get('lifecycle', '').lower() for m in cmdb_matches]
    
    unique_names = set(names)
    unique_lifecycles = set(lc for lc in lifecycles if lc)
    
    deprecated_keywords = ['legacy', 'old', 'deprecated', 'archive', 'retired']
    has_deprecated = any(
        any(kw in (m.get('name', '') or '').lower() for kw in deprecated_keywords) or
        any(kw in (m.get('lifecycle', '') or '').lower() for kw in deprecated_keywords)
        for m in cmdb_matches
    )
    if has_deprecated:
        return 'LEGACY'
    
    if has_vendor_match:
        return 'PARENT_VENDOR'
    
    if len(unique_names) == 1 and len(unique_lifecycles) > 1:
        return 'MULTI_ENV'
    
    if len(cmdb_matches) > 1:
        return 'DUPLICATE'
    
    return 'NONE'


def derive_reason_codes(cand: dict, idp_present_direct: bool = None, cmdb_present_direct: bool = None, security_attested: bool = None, vendor_governed: bool = False) -> list[str]:
    """Derive canonical reason codes from candidate flags.
    
    SEMANTIC INVARIANT:
    - HAS_IDP/HAS_CMDB = Direct authoritative record match only
    - VENDOR_GOVERNED = Governance inherited from vendor family (separate signal)
    - is_governed includes vendor_governed for classification purposes
    
    This ensures reason codes clearly distinguish direct vs propagated governance,
    matching AOD's semantics for accurate reconciliation.
    """
    codes = []
    if cand.get('discovery_present'):
        codes.append('HAS_DISCOVERY')
    
    effective_idp_direct = idp_present_direct if idp_present_direct is not None else cand.get('idp_present')
    effective_cmdb_direct = cmdb_present_direct if cmdb_present_direct is not None else cand.get('cmdb_present')
    effective_security = security_attested if security_attested is not None else cand.get('security_attested')
    
    if effective_idp_direct:
        codes.append('HAS_IDP')
    else:
        codes.append('NO_IDP')
    if effective_cmdb_direct:
        codes.append('HAS_CMDB')
    else:
        codes.append('NO_CMDB')
    if vendor_governed:
        codes.append('VENDOR_GOVERNED')
    if effective_security:
        codes.append('HAS_SECURITY_ATTESTATION')
    else:
        codes.append('NO_SECURITY_ATTESTATION')
    
    is_governed = effective_idp_direct or effective_cmdb_direct or vendor_governed
    
    if is_governed:
        codes.append('GOVERNED')
    else:
        codes.append('UNGOVERNED')
    
    if cand.get('finance_present'):
        codes.append('HAS_FINANCE')
    if cand.get('has_ongoing_finance'):
        codes.append('HAS_ONGOING_FINANCE')
    if cand.get('cloud_present'):
        codes.append('HAS_CLOUD')
    if cand.get('activity_present'):
        codes.append('RECENT_ACTIVITY')
    elif cand.get('stale_timestamps'):
        codes.append('STALE_ACTIVITY')
    return codes


def derive_rca_hint(classification: str, cand: dict) -> Optional[str]:
    """Derive RCA hint for debugging."""
    if classification == 'shadow':
        if not cand.get('idp_present') and not cand.get('cmdb_present'):
            return 'UNGOVERNED_ACTIVE'
    elif classification == 'zombie':
        if cand.get('stale_timestamps'):
            return 'STALE_NO_RECENT_USE'
    return None


def propagate_vendor_governance(candidates: dict) -> dict:
    """Propagate governance across vendor domain sets.
    
    If any domain in a vendor's set is governed (IdP or CMDB), 
    all domains in that vendor's set are considered governed.
    
    Returns dict mapping domain -> (vendor_has_idp, vendor_has_cmdb, vendor_name)
    """
    vendor_governance = {}
    
    for vendor, domains in VENDOR_DOMAIN_SETS.items():
        has_idp = False
        has_cmdb = False
        for domain in domains:
            domain_lower = domain.lower()
            if domain_lower in candidates:
                cand = candidates[domain_lower]
                if cand.get('idp_present'):
                    has_idp = True
                if cand.get('cmdb_present'):
                    has_cmdb = True
        vendor_governance[vendor] = (has_idp, has_cmdb)
    
    domain_governance = {}
    propagated_keys = []
    for domain, vendor in DOMAIN_TO_VENDOR.items():
        has_idp, has_cmdb = vendor_governance.get(vendor, (False, False))
        domain_governance[domain] = (has_idp, has_cmdb, vendor)
        if has_idp or has_cmdb:
            propagated_keys.append(domain)
    
    trace_log("reconciliation", "propagate_vendor_governance", {
        "total_vendors": len(vendor_governance),
        "propagated_domains": len(propagated_keys),
        "sample_keys": propagated_keys[:5],
    })
    
    return domain_governance


def compute_expected_block(
    snapshot: dict, 
    window_days: Optional[int] = None, 
    mode: str = "sprawl",
    policy: Optional[PolicyConfig] = None
) -> dict:
    """Compute the __expected__ block with classifications, reasons, and RCA hints.
    
    Mode controls eligibility:
    - sprawl: Only external SaaS domains (shadow IT detection)
    - infra: Only internal services (infrastructure monitoring)
    - all: All assets
    
    Policy-driven thresholds:
    - noise_floor: minimum discovery sources for admission
    - zombie_window_days: inactivity threshold
    - infrastructure_seeds: domains to exclude (from AOD, not local)
    
    Raises:
        MissingPolicyError: If policy is None/empty and FARM_ALLOW_DEFAULT_POLICY is not set.
    """
    from src.models.policy import MissingPolicyError
    
    # Guard: policy must be explicitly provided (not None, not empty dict if passed as dict)
    policy_missing = policy is None
    if policy_missing:
        allow_default = os.environ.get("FARM_ALLOW_DEFAULT_POLICY", "").lower() == "true"
        if not allow_default:
            raise MissingPolicyError(
                "Expected classification requires policy snapshot from AOD. "
                "Set FARM_ALLOW_DEFAULT_POLICY=true for local testing, "
                "or fetch policy via fetch_policy_config()."
            )
        trace_log("reconciliation", "using_default_policy", {
            "reason": "FARM_ALLOW_DEFAULT_POLICY=true",
            "warning": "Production should always use policy from AOD"
        })
        policy = PolicyConfig.default_fallback()
    
    if window_days is None:
        window_days = policy.admission.zombie_window_days
    
    noise_floor = policy.admission.noise_floor
    
    candidates = build_candidate_flags(snapshot, window_days, policy)
    
    # POLICY: Only propagate vendor governance if enabled in policy
    if policy.admission.enable_vendor_propagation:
        vendor_governance = propagate_vendor_governance(candidates)
    else:
        vendor_governance = {}
        trace_log("reconciliation", "vendor_propagation_disabled", {
            "reason": "policy.admission.enable_vendor_propagation=false"
        })
    
    shadow_expected = []
    zombie_expected = []
    clean_expected = []
    parked_expected = []
    expected_reasons = {}
    expected_admission = {}
    expected_rca_hint = {}
    expected_cmdb_resolution = {}
    excluded_by_mode = []
    decision_traces = {}
    
    for key, cand in candidates.items():
        is_external = is_external_domain(key)
        is_excluded = policy.is_excluded(key) or policy.is_banned(key)
        is_fqdn = is_valid_fqdn(key)
        
        if mode == "sprawl" and not is_external:
            excluded_by_mode.append(key)
            continue
        elif mode == "infra" and is_external:
            excluded_by_mode.append(key)
            continue
        
        if not is_fqdn:
            expected_admission[key] = 'rejected'
            continue
        
        discovery_sources = cand.get('discovery_sources', set())
        discovery_sources_list = sorted(list(discovery_sources))
        discovery_sources_count = len(discovery_sources)
        
        idp_present_direct = cand['idp_present']
        cmdb_present_direct = cand['cmdb_present']
        security_attested = cand.get('security_attested', False)
        vendor_name = None
        
        key_lower = key.lower()
        vendor_governed = False
        if key_lower in vendor_governance:
            vendor_has_idp, vendor_has_cmdb, vendor_name = vendor_governance[key_lower]
            if (vendor_has_idp or vendor_has_cmdb) and not idp_present_direct and not cmdb_present_direct:
                vendor_governed = True
        
        reasons = derive_reason_codes(
            cand, 
            idp_present_direct=idp_present_direct, 
            cmdb_present_direct=cmdb_present_direct, 
            security_attested=security_attested,
            vendor_governed=vendor_governed
        )
        expected_reasons[key] = reasons
        
        is_governed = idp_present_direct or cmdb_present_direct or vendor_governed
        
        cmdb_resolution = cand.get('cmdb_resolution_reason', 'NONE')
        if cmdb_resolution != 'NONE':
            expected_cmdb_resolution[key] = {
                'reason': cmdb_resolution,
                'match_count': len(cand.get('cmdb_matches', [])),
                'matches': cand.get('cmdb_matches', []),
            }
        
        is_shadow = False
        is_zombie = False
        is_parked = False
        rejection_reason = None
        is_admitted = False
        
        activity_status = cand.get('activity_status', ActivityStatus.NONE)
        anchored = cand.get('anchored', False)
        
        has_visibility = cmdb_present_direct
        has_validation = security_attested
        has_control = idp_present_direct
        
        missing_governance = []
        if not idp_present_direct:
            missing_governance.append('NO_IDP')
        if not cmdb_present_direct:
            missing_governance.append('NO_CMDB')
        if vendor_governed:
            missing_governance.append('VENDOR_GOVERNED')
        
        if is_excluded:
            rejection_reason = 'EXCLUDED_BY_POLICY'
            reasons.append('POLICY_EXCLUDED')
        else:
            finance_spend = cand.get('finance_spend', 0)
            is_admitted, rejection_reason = policy.is_admitted(
                discovery_sources_count=discovery_sources_count,
                cloud_present=cand['cloud_present'],
                idp_present=idp_present_direct or vendor_governed,
                cmdb_present=cmdb_present_direct or vendor_governed,
                finance_spend=finance_spend,
            )
            
            if is_admitted:
                has_ongoing_finance = cand.get('has_ongoing_finance', False)
                is_shadow = is_external and activity_status == ActivityStatus.RECENT and not is_governed
                is_zombie = is_governed and activity_status == ActivityStatus.STALE and has_ongoing_finance
                is_parked = is_external and not is_governed and activity_status == ActivityStatus.STALE
        
        raw_domains = list(cand.get('domains', set()))[:10]
        
        # Collect all activity timestamps for debugging
        all_activity_ts = cand.get('activity_timestamps', [])
        stale_ts = cand.get('stale_timestamps', [])
        
        decision_traces[key] = {
            'asset_key_used': key,
            'registered_domain': extract_registered_domain(key),
            'raw_domains_seen': raw_domains,
            'is_external': is_external,
            'activity_status': activity_status.value if isinstance(activity_status, ActivityStatus) else activity_status,
            'anchored': anchored,
            'activity_window_days': window_days,
            'activity_source': cand.get('activity_source', 'none'),
            'latest_activity_at': cand.get('latest_activity_at'),
            'all_activity_timestamps': all_activity_ts[:10],
            'stale_timestamps': stale_ts[:5],
            'idp_present_direct': idp_present_direct,
            'cmdb_present_direct': cmdb_present_direct,
            'vendor_governed': vendor_governed,
            'security_attested': security_attested,
            'has_visibility': has_visibility,
            'has_validation': has_validation,
            'has_control': has_control,
            'is_governed': is_governed,
            'missing_governance': missing_governance,
            'vendor_name': vendor_name,
            'policy_excluded': is_excluded,
            'admitted': is_admitted,
            'discovery_sources_count': discovery_sources_count,
            'discovery_sources_list': discovery_sources_list,
            'rejection_reason': rejection_reason,
            'is_shadow': is_shadow,
            'is_zombie': is_zombie,
            'is_parked': is_parked,
            'reason_codes': reasons,
        }
        
        if is_excluded or not is_admitted:
            expected_admission[key] = 'rejected'
            continue
        
        if is_shadow:
            shadow_expected.append({'asset_key': key})
            expected_admission[key] = 'admitted'
            rca = derive_rca_hint('shadow', cand)
            if rca:
                expected_rca_hint[key] = rca
        elif is_zombie:
            zombie_expected.append({'asset_key': key})
            expected_admission[key] = 'admitted'
            rca = derive_rca_hint('zombie', cand)
            if rca:
                expected_rca_hint[key] = rca
        elif is_parked:
            parked_expected.append({'asset_key': key})
            expected_admission[key] = 'parked'
        else:
            if cand['discovery_present']:
                clean_expected.append({'asset_key': key})
                expected_admission[key] = 'admitted'
    
    rejected_count = sum(1 for v in expected_admission.values() if v == 'rejected')
    parked_count = sum(1 for v in expected_admission.values() if v == 'parked')
    trace_log("reconciliation", "compute_expected_block", {
        "mode": mode,
        "shadows": len(shadow_expected),
        "zombies": len(zombie_expected),
        "parked": len(parked_expected),
        "clean": len(clean_expected),
        "rejected": rejected_count,
        "excluded_by_mode": len(excluded_by_mode),
    })
    
    return {
        'shadow_expected': shadow_expected,
        'zombie_expected': zombie_expected,
        'parked_expected': parked_expected,
        'clean_expected': clean_expected,
        'expected_reasons': expected_reasons,
        'expected_admission': expected_admission,
        'expected_rca_hint': expected_rca_hint,
        'expected_cmdb_resolution': expected_cmdb_resolution,
        'excluded_by_mode': excluded_by_mode,
        'decision_traces': decision_traces,
        'reconciliation_mode': mode,
    }


def analyze_snapshot_for_expectations(
    snapshot: dict, 
    window_days: Optional[int] = None,
    policy: Optional[PolicyConfig] = None
) -> FarmExpectations:
    """Legacy function for backward compatibility.
    
    Raises:
        MissingPolicyError: If policy is None/empty and FARM_ALLOW_DEFAULT_POLICY is not set.
    """
    from src.models.policy import MissingPolicyError
    
    policy_missing = policy is None
    if policy_missing:
        allow_default = os.environ.get("FARM_ALLOW_DEFAULT_POLICY", "").lower() == "true"
        if not allow_default:
            raise MissingPolicyError(
                "Expected classification requires policy snapshot from AOD. "
                "Set FARM_ALLOW_DEFAULT_POLICY=true for local testing."
            )
        policy = PolicyConfig.default_fallback()
    if window_days is None:
        window_days = policy.admission.zombie_window_days
    
    noise_floor = policy.admission.noise_floor
    
    candidates = build_candidate_flags(snapshot, window_days, policy)
    
    # POLICY: Only propagate vendor governance if enabled in policy
    if policy.admission.enable_vendor_propagation:
        vendor_governance = propagate_vendor_governance(candidates)
    else:
        vendor_governance = {}
    
    shadow_keys = []
    zombie_keys = []
    
    for key, cand in candidates.items():
        is_excluded = policy.is_excluded(key) or policy.is_banned(key)
        is_external = is_external_domain(key)
        is_fqdn = is_valid_fqdn(key)
        
        if is_excluded or not is_fqdn:
            continue
        
        idp_present = cand['idp_present']
        cmdb_present = cand['cmdb_present']
        security_attested = cand.get('security_attested', False)
        key_lower = key.lower()
        if key_lower in vendor_governance:
            vendor_has_idp, vendor_has_cmdb, _ = vendor_governance[key_lower]
            idp_present = idp_present or vendor_has_idp
            cmdb_present = cmdb_present or vendor_has_cmdb
        
        discovery_sources = cand.get('discovery_sources', set())
        finance_spend = cand.get('finance_spend', 0)
        
        is_admitted, _ = policy.is_admitted(
            discovery_sources_count=len(discovery_sources),
            cloud_present=cand['cloud_present'],
            idp_present=idp_present,
            cmdb_present=cmdb_present,
            finance_spend=finance_spend,
        )
        
        if not is_admitted:
            continue
        
        activity_status = cand.get('activity_status', ActivityStatus.NONE)
        
        is_governed = idp_present or cmdb_present
        has_ongoing_finance = cand.get('has_ongoing_finance', False)
        
        if is_external and activity_status == ActivityStatus.RECENT and not is_governed:
            shadow_keys.append(key)
        elif is_governed and activity_status == ActivityStatus.STALE and has_ongoing_finance:
            zombie_keys.append(key)
    
    return FarmExpectations(
        expected_zombies=len(zombie_keys),
        expected_shadows=len(shadow_keys),
        zombie_keys=zombie_keys[:20],
        shadow_keys=shadow_keys[:20],
    )


def grade_count_check(name: str, aod_count: int, farm_count: int) -> tuple[str, ReconcileStatusEnum]:
    """Grade a single count check. Exceeds = WARN, Under = FAIL, Match = PASS."""
    if aod_count == farm_count:
        return f"{name}: PASS (AOD {aod_count} = Farm {farm_count})", ReconcileStatusEnum.PASS
    elif aod_count > farm_count:
        return f"{name}: WARN (AOD {aod_count} > Farm {farm_count})", ReconcileStatusEnum.WARN
    else:
        return f"{name}: FAIL (AOD {aod_count} < Farm {farm_count})", ReconcileStatusEnum.FAIL


def generate_reconcile_report(aod_summary, aod_lists, farm_expectations: FarmExpectations) -> tuple[str, ReconcileStatusEnum]:
    lines = []
    
    aod_zombies = aod_summary.zombie_count
    aod_shadows = aod_summary.shadow_count
    farm_zombies = farm_expectations.expected_zombies
    farm_shadows = farm_expectations.expected_shadows
    
    lines.append(f"Zombie count: AOD {aod_zombies} vs Farm {farm_zombies}")
    lines.append(f"Shadow count: AOD {aod_shadows} vs Farm {farm_shadows}")
    
    aod_zombie_set = set(normalize_name(k) for k in aod_lists.zombie_assets)
    aod_shadow_set = set(normalize_name(k) for k in aod_lists.shadow_assets)
    farm_zombie_set = set(normalize_name(k) for k in farm_expectations.zombie_keys)
    farm_shadow_set = set(normalize_name(k) for k in farm_expectations.shadow_keys)
    
    zombie_overlap = len(aod_zombie_set & farm_zombie_set)
    shadow_overlap = len(aod_shadow_set & farm_shadow_set)
    zombie_missed = farm_zombie_set - aod_zombie_set
    shadow_missed = farm_shadow_set - aod_shadow_set
    zombie_extra = aod_zombie_set - farm_zombie_set
    shadow_extra = aod_shadow_set - farm_shadow_set
    
    total_expected = farm_zombies + farm_shadows
    materiality = max(2, int(total_expected * 0.1))
    
    lines.append(f"Zombie overlap: {zombie_overlap}/{len(farm_zombie_set)} expected")
    lines.append(f"Shadow overlap: {shadow_overlap}/{len(farm_shadow_set)} expected")
    
    if zombie_missed:
        lines.append(f"Zombie missed by AOD: {list(zombie_missed)[:5]}")
    if shadow_missed:
        lines.append(f"Shadow missed by AOD: {list(shadow_missed)[:5]}")
    if zombie_extra:
        lines.append(f"Zombie extra from AOD: {list(zombie_extra)[:5]}")
    if shadow_extra:
        lines.append(f"Shadow extra from AOD: {list(shadow_extra)[:5]}")
    
    total_missed = len(zombie_missed) + len(shadow_missed)
    
    if total_missed <= materiality:
        status = ReconcileStatusEnum.PASS
        lines.append(f"OVERALL: PASS ({total_missed} missed <= {materiality} threshold)")
    elif total_missed <= materiality * 2:
        status = ReconcileStatusEnum.WARN
        lines.append(f"OVERALL: WARN ({total_missed} missed <= {materiality * 2} threshold)")
    else:
        status = ReconcileStatusEnum.FAIL
        lines.append(f"OVERALL: FAIL ({total_missed} missed > {materiality * 2} threshold)")
    
    report_text = "\n".join(lines[:15])
    return report_text, status
