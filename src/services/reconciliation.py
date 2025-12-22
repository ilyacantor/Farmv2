from datetime import datetime
from typing import Optional
from collections import defaultdict

from src.models.planes import FarmExpectations, ReconcileStatusEnum
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
)
from src.services.logging import trace_log


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        ts = ts.replace('Z', '+00:00')
        if '+' in ts:
            ts = ts.split('+')[0]
        return datetime.fromisoformat(ts)
    except:
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


def build_candidate_flags(snapshot: dict, window_days: int = 90) -> dict:
    """Build candidate flags from snapshot planes. Returns {key: {flags...}}."""
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
    })
    
    observations = planes.get('discovery', {}).get('observations', [])
    for obs in observations:
        domain = obs.get('domain') or extract_domain(obs.get('observed_uri') or obs.get('hostname') or '')
        name = obs.get('observed_name', '')
        key = domain if domain else normalize_name(name)
        if not key:
            continue
        
        candidates[key]['key'] = key
        candidates[key]['names'].add(name)
        candidates[key]['discovery_present'] = True
        if domain:
            candidates[key]['domains'].add(domain)
        vendor_hint = obs.get('vendor_hint')
        if vendor_hint:
            candidates[key]['vendors'].add(vendor_hint)
        
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
    
    idp_objects = planes.get('idp', {}).get('objects', [])
    for obj in idp_objects:
        name = normalize_name(obj.get('name', ''))
        domain = extract_domain(obj.get('external_ref', ''))
        matched_keys = set()
        
        for key, cand in candidates.items():
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                cand['idp_present'] = True
                matched_keys.add(key)
            if domain and (domain == key or domain in cand['domains']):
                cand['idp_present'] = True
                matched_keys.add(key)
        
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
    
    cmdb_cis = planes.get('cmdb', {}).get('cis', [])
    for ci in cmdb_cis:
        name = normalize_name(ci.get('name', ''))
        domain = extract_domain(ci.get('external_ref', ''))
        ci_vendor = normalize_name(ci.get('vendor', '') or '')
        
        for key, cand in candidates.items():
            matched_by_name_or_domain = False
            matched_by_vendor_only = False
            
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                matched_by_name_or_domain = True
            if domain and (domain == key or domain in cand['domains']):
                matched_by_name_or_domain = True
            if ci_vendor and any(ci_vendor == normalize_name(v) for v in cand['vendors']):
                if not matched_by_name_or_domain:
                    matched_by_vendor_only = True
            
            if matched_by_name_or_domain or matched_by_vendor_only:
                cand['cmdb_present'] = True
                cand['cmdb_matches'].append({
                    'ci_id': ci.get('ci_id'),
                    'name': ci.get('name'),
                    'lifecycle': ci.get('lifecycle'),
                    'vendor': ci.get('vendor'),
                    'ci_type': ci.get('ci_type'),
                    'matched_via_vendor': matched_by_vendor_only,
                })
    
    cloud_resources = planes.get('cloud', {}).get('resources', [])
    for res in cloud_resources:
        name = normalize_name(res.get('name', ''))
        for key, cand in candidates.items():
            if name and any(normalize_name(n) in name or name in normalize_name(n) for n in cand['names']):
                cand['cloud_present'] = True
    
    contracts = planes.get('finance', {}).get('contracts', [])
    transactions = planes.get('finance', {}).get('transactions', [])
    
    for contract in contracts:
        vendor = normalize_name(contract.get('vendor_name', ''))
        product = normalize_name(contract.get('product', '') or '')
        
        for key, cand in candidates.items():
            matched = False
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                matched = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                matched = True
            if product and any(product in normalize_name(n) or normalize_name(n) in product for n in cand['names']):
                matched = True
            if matched:
                cand['finance_present'] = True
                cand['has_ongoing_finance'] = True
    
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        is_recurring = txn.get('is_recurring', False)
        for key, cand in candidates.items():
            matched = False
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                matched = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                matched = True
            if matched:
                cand['finance_present'] = True
                if is_recurring:
                    cand['has_ongoing_finance'] = True
    
    for key, cand in candidates.items():
        cand['cmdb_resolution_reason'] = determine_cmdb_resolution_reason(cand['cmdb_matches'], cand['vendors'])
    
    discovery_count = sum(1 for c in candidates.values() if c.get('discovery_present'))
    idp_count = sum(1 for c in candidates.values() if c.get('idp_present'))
    cmdb_count = sum(1 for c in candidates.values() if c.get('cmdb_present'))
    finance_count = sum(1 for c in candidates.values() if c.get('finance_present'))
    trace_log("reconciliation", "build_candidate_flags", {
        "total_candidates": len(candidates),
        "discovery_present": discovery_count,
        "idp_present": idp_count,
        "cmdb_present": cmdb_count,
        "finance_present": finance_count,
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


def derive_reason_codes(cand: dict, idp_present: bool = None, cmdb_present: bool = None) -> list[str]:
    """Derive canonical reason codes from candidate flags.
    
    If idp_present/cmdb_present are provided, use those (for governance propagation).
    Otherwise, use the raw candidate values.
    """
    codes = []
    if cand.get('discovery_present'):
        codes.append('HAS_DISCOVERY')
    
    effective_idp = idp_present if idp_present is not None else cand.get('idp_present')
    effective_cmdb = cmdb_present if cmdb_present is not None else cand.get('cmdb_present')
    
    if effective_idp:
        codes.append('HAS_IDP')
    else:
        codes.append('NO_IDP')
    if effective_cmdb:
        codes.append('HAS_CMDB')
    else:
        codes.append('NO_CMDB')
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


def compute_expected_block(snapshot: dict, window_days: int = 90, mode: str = "sprawl") -> dict:
    """Compute the __expected__ block with classifications, reasons, and RCA hints.
    
    Mode controls eligibility:
    - sprawl: Only external SaaS domains (shadow IT detection)
    - infra: Only internal services (infrastructure monitoring)
    - all: All assets
    """
    candidates = build_candidate_flags(snapshot, window_days)
    
    vendor_governance = propagate_vendor_governance(candidates)
    
    shadow_expected = []
    zombie_expected = []
    clean_expected = []
    expected_reasons = {}
    expected_admission = {}
    expected_rca_hint = {}
    expected_cmdb_resolution = {}
    excluded_by_mode = []
    decision_traces = {}
    
    for key, cand in candidates.items():
        is_external = is_external_domain(key)
        is_infra_excluded = key in INFRASTRUCTURE_DOMAINS
        
        if mode == "sprawl" and not is_external:
            excluded_by_mode.append(key)
            continue
        elif mode == "infra" and is_external:
            excluded_by_mode.append(key)
            continue
        
        cmdb_resolution = cand.get('cmdb_resolution_reason', 'NONE')
        if cmdb_resolution != 'NONE':
            expected_cmdb_resolution[key] = {
                'reason': cmdb_resolution,
                'match_count': len(cand.get('cmdb_matches', [])),
                'matches': cand.get('cmdb_matches', []),
            }
        
        idp_present_direct = cand['idp_present']
        cmdb_present_direct = cand['cmdb_present']
        idp_present = idp_present_direct
        cmdb_present = cmdb_present_direct
        vendor_name = None
        governed_via_vendor = False
        
        key_lower = key.lower()
        if key_lower in vendor_governance:
            vendor_has_idp, vendor_has_cmdb, vendor_name = vendor_governance[key_lower]
            if vendor_has_idp and not idp_present_direct:
                idp_present = True
                governed_via_vendor = True
            if vendor_has_cmdb and not cmdb_present_direct:
                cmdb_present = True
                governed_via_vendor = True
        
        reasons = derive_reason_codes(cand, idp_present=idp_present, cmdb_present=cmdb_present)
        if governed_via_vendor:
            reasons.append('GOVERNED_VIA_VENDOR')
        expected_reasons[key] = reasons
        
        discovery_sources = cand.get('discovery_sources', set())
        discovery_sources_list = sorted(list(discovery_sources))
        discovery_sources_count = len(discovery_sources)
        
        is_admitted = (
            discovery_sources_count >= 2 or
            cand['cloud_present'] or
            idp_present or
            cmdb_present
        )
        
        is_shadow = False
        is_zombie = False
        rejection_reason = None
        
        if is_admitted:
            is_shadow = is_external and cand['activity_present'] and not idp_present and not cmdb_present and not is_infra_excluded
            is_zombie = (idp_present or cmdb_present) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0
        else:
            if discovery_sources_count == 1 and 'dns' in discovery_sources:
                rejection_reason = 'DNS-only'
            elif discovery_sources_count == 1:
                rejection_reason = f'Single source ({discovery_sources_list[0]})'
            elif discovery_sources_count == 0:
                rejection_reason = 'No discovery sources'
            else:
                rejection_reason = 'No admission criteria satisfied'
        
        raw_domains = list(cand.get('domains', set()))[:10]
        decision_traces[key] = {
            'asset_key_used': key,
            'registered_domain': extract_registered_domain(key),
            'raw_domains_seen': raw_domains,
            'is_external': is_external,
            'is_active': cand['activity_present'],
            'activity_window_days': window_days,
            'activity_source': cand.get('activity_source', 'none'),
            'latest_activity_at': cand.get('latest_activity_at'),
            'idp_present': idp_present,
            'idp_present_direct': idp_present_direct,
            'cmdb_present': cmdb_present,
            'cmdb_present_direct': cmdb_present_direct,
            'vendor_governance': vendor_name,
            'infra_excluded': is_infra_excluded,
            'admitted': is_admitted,
            'discovery_sources_count': discovery_sources_count,
            'discovery_sources_list': discovery_sources_list,
            'rejection_reason': rejection_reason,
            'is_shadow': is_shadow,
            'reason_codes': reasons,
        }
        
        if not is_admitted:
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
        else:
            if cand['discovery_present']:
                clean_expected.append({'asset_key': key})
                expected_admission[key] = 'admitted'
    
    rejected_count = sum(1 for v in expected_admission.values() if v == 'rejected')
    trace_log("reconciliation", "compute_expected_block", {
        "mode": mode,
        "shadows": len(shadow_expected),
        "zombies": len(zombie_expected),
        "clean": len(clean_expected),
        "rejected": rejected_count,
        "excluded_by_mode": len(excluded_by_mode),
    })
    
    return {
        'shadow_expected': shadow_expected,
        'zombie_expected': zombie_expected,
        'clean_expected': clean_expected,
        'expected_reasons': expected_reasons,
        'expected_admission': expected_admission,
        'expected_rca_hint': expected_rca_hint,
        'expected_cmdb_resolution': expected_cmdb_resolution,
        'excluded_by_mode': excluded_by_mode,
        'decision_traces': decision_traces,
        'reconciliation_mode': mode,
    }


def analyze_snapshot_for_expectations(snapshot: dict, window_days: int = 90) -> FarmExpectations:
    """Legacy function for backward compatibility."""
    candidates = build_candidate_flags(snapshot, window_days)
    vendor_governance = propagate_vendor_governance(candidates)
    
    shadow_keys = []
    zombie_keys = []
    
    for key, cand in candidates.items():
        is_infra_excluded = key in INFRASTRUCTURE_DOMAINS
        is_external = is_external_domain(key)
        
        idp_present = cand['idp_present']
        cmdb_present = cand['cmdb_present']
        key_lower = key.lower()
        if key_lower in vendor_governance:
            vendor_has_idp, vendor_has_cmdb, _ = vendor_governance[key_lower]
            idp_present = idp_present or vendor_has_idp
            cmdb_present = cmdb_present or vendor_has_cmdb
        
        discovery_sources = cand.get('discovery_sources', set())
        is_admitted = (
            len(discovery_sources) >= 2 or
            cand['cloud_present'] or
            idp_present or
            cmdb_present
        )
        
        if not is_admitted:
            continue
        
        if is_external and cand['activity_present'] and not idp_present and not cmdb_present and not is_infra_excluded:
            shadow_keys.append(key)
        elif (idp_present or cmdb_present) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0:
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
