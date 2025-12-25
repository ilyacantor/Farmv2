import re
from typing import Optional

from src.services.constants import VENDOR_DOMAIN_SETS, DOMAIN_TO_VENDOR
from src.services.key_normalization import (
    normalize_name,
    extract_domain,
    to_domain_key,
    roll_up_to_domains,
)
from src.services.reconciliation import compute_expected_block
from src.services.logging import trace_log, increment_mismatch_counter, reset_mismatch_counters


EXPLANATION_TEMPLATES = {
    'shadow_missed': {
        'default': "AOD failed to identify {key} as shadow IT.",
        'UNGOVERNED_ACTIVE': "AOD missed {key}: has recent activity but no governance record in IdP/CMDB. This is ungoverned app sprawl.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
    },
    'zombie_missed': {
        'default': "AOD failed to identify {key} as a zombie asset.",
        'STALE_NO_RECENT_USE': "AOD missed {key}: exists in IdP/CMDB but has no recent activity. License costs continue but nobody's using it.",
        'HAS_IDP+STALE_ACTIVITY': "AOD missed {key}: still provisioned in IdP but activity is stale (90+ days old). This app might be abandoned.",
        'HAS_CMDB+STALE_ACTIVITY': "AOD missed {key}: still in CMDB as managed asset but no recent usage detected. Potential cost savings by decommissioning.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
    },
    'false_positive_shadow': {
        'default': "AOD incorrectly flagged {key} as shadow IT, but Farm expected it to be clean.",
        'HAS_IDP': "AOD false positive on {key}: this app is actually governed - it appears in IdP. Not shadow IT.",
        'HAS_CMDB': "AOD false positive on {key}: this app is tracked in CMDB as a managed asset. Not shadow IT.",
        'HAS_IDP+HAS_CMDB': "AOD false positive on {key}: fully governed - appears in both IdP and CMDB. Definitely not shadow IT.",
    },
    'false_positive_zombie': {
        'default': "AOD incorrectly flagged {key} as zombie, but Farm expected it to be active.",
        'RECENT_ACTIVITY': "AOD false positive on {key}: this app has recent activity within the detection window. Users are actively using it.",
        'HAS_DISCOVERY+RECENT_ACTIVITY': "AOD false positive on {key}: we see recent discovery observations showing active usage. Not a zombie.",
    },
    'matched_shadow': {
        'default': "Both Farm and AOD agree {key} is shadow IT.",
        'UNGOVERNED_ACTIVE': "{key} is shadow IT: has recent activity ({farm_reasons}) but missing from IdP/CMDB governance.",
    },
    'matched_zombie': {
        'default': "Both Farm and AOD agree {key} is a zombie asset.",
        'STALE_NO_RECENT_USE': "{key} is zombie: registered in governance systems but no recent activity ({farm_reasons}).",
    },
}


def generate_asset_analysis(mismatch_type: str, key: str, farm_reasons: list, rca_hint: str = None, aod_reasons: list = None, aod_admission: str = None) -> dict:
    """Generate structured analysis with headline, Farm perspective, and AOD perspective."""
    aod_reasons = aod_reasons or []
    farm_reasons_str = ', '.join(farm_reasons[:4]) if farm_reasons else 'no evidence'
    aod_reasons_str = ', '.join(aod_reasons[:4]) if aod_reasons else 'no reason codes provided'
    
    asset_type = 'shadow' if 'shadow' in mismatch_type else 'zombie'
    
    if mismatch_type == 'shadow_missed':
        headline = f"AOD missed {key} as shadow IT"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif rca_hint == 'UNGOVERNED_ACTIVE':
            headline += " - active but missing from governance systems"
        farm_detail = f"Farm expected SHADOW because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as shadow"
        
    elif mismatch_type == 'zombie_missed':
        headline = f"AOD missed {key} as zombie"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif 'STALE_ACTIVITY' in farm_reasons:
            headline += " - registered but no recent usage"
        elif rca_hint == 'STALE_NO_RECENT_USE':
            headline += " - paying for something nobody's using"
        farm_detail = f"Farm expected ZOMBIE because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as zombie"
        
    elif mismatch_type == 'false_positive_shadow':
        headline = f"AOD incorrectly flagged {key} as shadow"
        if 'HAS_IDP' in farm_reasons or 'HAS_CMDB' in farm_reasons:
            headline += " - it's actually governed"
        farm_detail = f"Farm says CLEAN because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be clean/governed"
        aod_detail = f"AOD flagged as shadow because: {aod_reasons_str}"
        
    elif mismatch_type == 'false_positive_zombie':
        headline = f"AOD incorrectly flagged {key} as zombie"
        if 'RECENT_ACTIVITY' in farm_reasons:
            headline += " - it actually has recent usage"
        farm_detail = f"Farm says ACTIVE because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be active"
        aod_detail = f"AOD flagged as zombie because: {aod_reasons_str}"
        
    elif mismatch_type == 'matched_shadow':
        headline = f"{key} correctly identified as shadow IT"
        farm_detail = f"Farm expected SHADOW: {farm_reasons_str}"
        aod_detail = f"AOD found SHADOW: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"
        
    elif mismatch_type == 'matched_zombie':
        headline = f"{key} correctly identified as zombie"
        farm_detail = f"Farm expected ZOMBIE: {farm_reasons_str}"
        aod_detail = f"AOD found ZOMBIE: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"
        
    else:
        headline = f"Mismatch on {key}"
        farm_detail = f"Farm reasons: {farm_reasons_str}"
        aod_detail = f"AOD reasons: {aod_reasons_str}"
    
    return {
        'headline': headline,
        'farm_detail': farm_detail,
        'aod_detail': aod_detail,
        'rca_hint': rca_hint,
    }


def get_explanation(mismatch_type: str, key: str, farm_reasons: list, rca_hint: str = None, aod_reasons: list = None) -> str:
    """Generate plain English explanation (legacy compat)."""
    analysis = generate_asset_analysis(mismatch_type, key, farm_reasons, rca_hint, aod_reasons)
    return f"{analysis['headline']}. {analysis['farm_detail']}. {analysis['aod_detail']}."


def investigate_fp_shadow(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's shadow classification.
    Returns evidence that the asset is actually governed (not shadow IT).
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}
    
    def matches_key(name):
        if not name:
            return False
        name_lower = name.lower()
        name_core = re.sub(r'[^a-z0-9]', '', name_lower)
        return key_lower in name_lower or key_core in name_core or name_core in key_core
    
    idp = snapshot.get('idp', {}).get('users', []) + snapshot.get('idp', {}).get('apps', [])
    for entry in idp:
        app_name = entry.get('app_name') or entry.get('name') or entry.get('display_name', '')
        if matches_key(app_name):
            findings.append(f"Found in IdP: '{app_name}'")
            evidence['idp_entry'] = app_name
            break
    
    cmdb = snapshot.get('cmdb', {}).get('assets', [])
    for entry in cmdb:
        name = entry.get('name') or entry.get('app_name') or entry.get('asset_name', '')
        if matches_key(name):
            findings.append(f"Found in CMDB: '{name}'")
            evidence['cmdb_entry'] = name
            break
    
    if 'NO_IDP' in aod_reasons and 'idp_entry' in evidence:
        findings.append(f"AOD claims NO_IDP but Farm found IdP record")
    if 'NO_CMDB' in aod_reasons and 'cmdb_entry' in evidence:
        findings.append(f"AOD claims NO_CMDB but Farm found CMDB record")
    
    if not findings:
        findings.append("Farm found governance records that AOD may have missed or matched differently")
    
    return {
        'conclusion': "Asset is governed - not shadow IT" if evidence else "Farm disagrees with shadow classification",
        'findings': findings,
        'evidence': evidence,
    }


def investigate_fp_zombie(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's zombie classification.
    Returns evidence that the asset is actually active (not zombie).
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}
    
    def matches_key(name):
        if not name:
            return False
        name_lower = name.lower()
        name_core = re.sub(r'[^a-z0-9]', '', name_lower)
        return key_lower in name_lower or key_core in name_core or name_core in key_core
    
    discovery = snapshot.get('discovery', {}).get('observations', [])
    for obs in discovery:
        app_name = obs.get('app_name') or obs.get('name', '')
        if matches_key(app_name):
            last_seen = obs.get('last_seen') or obs.get('timestamp', '')
            findings.append(f"Found discovery observation: '{app_name}' last seen {last_seen[:10] if last_seen else 'recently'}")
            evidence['discovery_entry'] = app_name
            evidence['last_seen'] = last_seen
            break
    
    finance = snapshot.get('finance', {})
    for tx in finance.get('transactions', []):
        vendor = tx.get('vendor_name') or tx.get('vendor', '')
        if matches_key(vendor):
            if tx.get('is_recurring'):
                findings.append(f"Has active recurring subscription: '{vendor}'")
                evidence['recurring_spend'] = vendor
            break
    
    if 'STALE_ACTIVITY' in aod_reasons and 'discovery_entry' in evidence:
        findings.append("AOD claims STALE_ACTIVITY but Farm found recent observations")
    
    if not findings:
        findings.append("Farm found activity evidence that AOD may have missed")
    
    return {
        'conclusion': "Asset is active - not zombie" if evidence else "Farm disagrees with zombie classification",
        'findings': findings,
        'evidence': evidence,
    }


def extract_aod_evidence_domains(aod_payload: dict) -> set:
    """Extract all domains/URLs referenced in AOD's asset_summaries and evidence.
    
    Recursively traverses all nested structures to find domain references.
    Returns a set of lowercase domain strings.
    """
    domains = set()
    
    def extract_domains_from_string(s: str):
        """Extract potential domain from a string (URL or domain)."""
        s = str(s).lower().strip()
        if not s:
            return
        if '://' in s:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(s)
                if parsed.netloc:
                    domains.add(parsed.netloc)
            except:
                pass
        if '.' in s and not s.startswith('http'):
            domains.add(s)
    
    def traverse(obj):
        """Recursively traverse dict/list to find domain strings."""
        if isinstance(obj, str):
            extract_domains_from_string(obj)
        elif isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(key, str) and '.' in key:
                    domains.add(key.lower())
                traverse(val)
        elif isinstance(obj, list):
            for item in obj:
                traverse(item)
    
    aod_lists = aod_payload.get('aod_lists', {})
    asset_summaries = aod_lists.get('asset_summaries', {})
    
    if isinstance(asset_summaries, dict):
        for key, summary in asset_summaries.items():
            if isinstance(key, str):
                domains.add(key.lower())
            traverse(summary)
    
    for list_key in ['actual_reason_codes', 'reason_codes', 'evidence']:
        data = aod_lists.get(list_key)
        if data:
            traverse(data)
    
    return domains


def check_key_in_aod_evidence(key: str, aod_evidence_domains: set) -> bool:
    """Check if a Farm-expected key appears anywhere in AOD's evidence.
    
    Uses normalized matching to handle variations like:
    - notion.so vs techworks.notion.so
    - slack.com vs slack
    """
    if not key or not aod_evidence_domains:
        return False
        
    key_lower = key.lower().strip()
    key_core = re.sub(r'\.(com|io|so|app|net|org|co|ai)$', '', key_lower)
    key_norm = re.sub(r'[^a-z0-9]', '', key_lower)
    key_core_norm = re.sub(r'[^a-z0-9]', '', key_core)
    
    for domain in aod_evidence_domains:
        if not isinstance(domain, str):
            continue
        domain_lower = domain.lower().strip()
        domain_core = re.sub(r'\.(com|io|so|app|net|org|co|ai)$', '', domain_lower)
        domain_norm = re.sub(r'[^a-z0-9]', '', domain_lower)
        domain_core_norm = re.sub(r'[^a-z0-9]', '', domain_core)
        
        if key_lower == domain_lower:
            return True
        if key_lower in domain_lower or domain_lower.endswith('.' + key_lower):
            return True
        if key_core_norm == domain_core_norm and len(key_core_norm) >= 3:
            return True
        if len(key_norm) >= 5 and (key_norm in domain_norm or domain_norm in key_norm):
            return True
    
    return False


def build_reconciliation_analysis(snapshot: dict, aod_payload: dict, farm_exp: dict) -> tuple:
    """Build detailed reconciliation analysis comparing Farm expectations vs AOD results.
    
    Uses stored __expected__ block from snapshot if available (fast path).
    Only recomputes if __expected__ is missing (legacy snapshots).
    
    Returns: (analysis_dict, recomputed_expected_block_or_none)
        - If cached block was valid: returns (analysis, None)
        - If recomputed: returns (analysis, new_expected_block) so caller can persist
    """
    reset_mismatch_counters()
    
    cached = snapshot.get('__expected__')
    cached_mode = cached.get('reconciliation_mode') if cached else None
    
    recomputed_block = None
    if cached and cached_mode == 'all' and cached.get('shadow_expected') is not None:
        expected_block = cached
    else:
        expected_block = compute_expected_block(snapshot, mode="all")
        recomputed_block = expected_block
    
    farm_shadows = {a['asset_key'] for a in expected_block.get('shadow_expected', [])}
    farm_zombies = {a['asset_key'] for a in expected_block.get('zombie_expected', [])}
    farm_clean = {a['asset_key'] for a in expected_block.get('clean_expected', [])}
    expected_reasons = expected_block.get('expected_reasons', {})
    expected_rca = expected_block.get('expected_rca_hint', {})
    
    aod_lists = aod_payload.get('aod_lists', {})
    aod_summary = aod_payload.get('aod_summary', {})
    
    aod_evidence_domains = extract_aod_evidence_domains(aod_payload)
    
    asset_summaries = aod_lists.get('asset_summaries', {})
    if asset_summaries:
        aod_shadows = set()
        aod_zombies = set()
        for key, summary in asset_summaries.items():
            if isinstance(summary, dict):
                if summary.get('is_shadow'):
                    aod_shadows.add(key)
                if summary.get('is_zombie'):
                    aod_zombies.add(key)
    else:
        aod_shadows = set(
            aod_lists.get('shadow_asset_keys') or 
            aod_lists.get('shadow_asset_keys_sample') or 
            aod_lists.get('shadow_assets', [])
        )
        aod_zombies = set(
            aod_lists.get('zombie_asset_keys') or 
            aod_lists.get('zombie_asset_keys_sample') or 
            aod_lists.get('zombie_assets', [])
        )
    aod_reason_codes = (
        aod_lists.get('actual_reason_codes') or 
        aod_lists.get('reason_codes') or 
        aod_lists.get('aod_reason_codes') or 
        {}
    )
    aod_admission = (
        aod_lists.get('admission_actual') or 
        aod_lists.get('admission') or 
        {}
    )
    
    aod_shadow_domains = roll_up_to_domains(aod_shadows, aod_reason_codes)
    aod_zombie_domains = roll_up_to_domains(aod_zombies, aod_reason_codes)
    
    shadow_domain_variants = {dk: info['variants'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_variants = {dk: info['variants'] for dk, info in aod_zombie_domains.items()}
    
    shadow_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_zombie_domains.items()}
    
    aod_shadow_domain_keys = set(aod_shadow_domains.keys())
    aod_zombie_domain_keys = set(aod_zombie_domains.keys())
    
    shadow_count_reported = aod_summary.get('shadow_count', 0)
    shadow_keys_received = len(aod_shadows)
    zombie_count_reported = aod_summary.get('zombie_count', 0)
    zombie_keys_received = len(aod_zombies)
    
    payload_health = {
        'shadow_count_reported': shadow_count_reported,
        'shadow_keys_received': shadow_keys_received,
        'shadow_mismatch': shadow_count_reported != shadow_keys_received,
        'zombie_count_reported': zombie_count_reported,
        'zombie_keys_received': zombie_keys_received,
        'zombie_mismatch': zombie_count_reported != zombie_keys_received,
        'has_issues': (shadow_count_reported != shadow_keys_received) or (zombie_count_reported != zombie_keys_received),
    }
    
    def norm(s):
        """Normalize asset key for comparison - extract core name, remove suffixes."""
        s = s.lower().strip()
        s = re.sub(r'[^a-z0-9]', '', s)
        for suffix in ['com', 'io', 'app', 'net', 'org', 'co']:
            if s.endswith(suffix) and len(s) > len(suffix) + 2:
                s = s[:-len(suffix)]
        return s
    
    def find_match(key, target_set):
        """Find matching key in target set using normalized comparison."""
        nk = norm(key)
        for t in target_set:
            nt = norm(t)
            if nk == nt:
                return t
            if nk in nt or nt in nk:
                return t
        return None
    
    expected_admission = expected_block.get('expected_admission', {})
    gross_observations = len(snapshot.get('planes', {}).get('discovery', {}).get('observations', []))
    unique_assets = len(expected_admission)
    rejected_count = sum(1 for v in expected_admission.values() if v == 'rejected')
    admitted_count = unique_assets - rejected_count
    
    lifecycle_funnel = {
        'gross_observations': gross_observations,
        'unique_assets': unique_assets,
        'rejected_count': rejected_count,
        'admitted_count': admitted_count,
        'shadow_count': len(farm_shadows),
        'zombie_count': len(farm_zombies),
        'clean_count': len(farm_clean),
        'final_cataloged': admitted_count,
    }
    
    analysis = {
        'summary': {
            'farm_shadows': len(farm_shadows),
            'farm_zombies': len(farm_zombies),
            'farm_clean': len(farm_clean),
            'aod_shadows': len(aod_shadows),
            'aod_zombies': len(aod_zombies),
            'aod_shadow_domains': len(aod_shadow_domain_keys),
            'aod_zombie_domains': len(aod_zombie_domain_keys),
            'entity_level_shadow_count': len(aod_shadows),
            'domain_level_shadow_count': len(aod_shadow_domain_keys),
            'farm_expected_shadow_count': len(farm_shadows),
            'entity_level_zombie_count': len(aod_zombies),
            'domain_level_zombie_count': len(aod_zombie_domain_keys),
            'farm_expected_zombie_count': len(farm_zombies),
            'gross_observations': gross_observations,
            'cataloged': admitted_count,
            'rejected': rejected_count,
        },
        'lifecycle_funnel': lifecycle_funnel,
        'payload_health': payload_health,
        'domain_roll_up': {
            'shadow_variants': shadow_domain_variants,
            'zombie_variants': zombie_domain_variants,
        },
        'matched_shadows': [],
        'matched_zombies': [],
        'missed_shadows': [],
        'missed_zombies': [],
        'false_positive_shadows': [],
        'false_positive_zombies': [],
    }
    
    def get_aod_reasons(key):
        """Get AOD's reason codes for a key, checking normalized variants."""
        if key in aod_reason_codes:
            return aod_reason_codes[key]
        for aod_key in aod_reason_codes:
            if norm(aod_key) == norm(key):
                return aod_reason_codes[aod_key]
        return []
    
    def get_aod_admission(key):
        """Get AOD's admission status for a key, checking normalized variants."""
        if key in aod_admission:
            return aod_admission[key]
        for aod_key in aod_admission:
            if norm(aod_key) == norm(key):
                return aod_admission[aod_key]
        return None
    
    for key in farm_shadows:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match(farm_domain_key, aod_shadow_domain_keys)
        
        if aod_domain_matched:
            aod_key_reasons = shadow_domain_reasons.get(aod_domain_matched, [])
            variants = shadow_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_shadow', key, reasons, rca, aod_key_reasons)
            analysis['matched_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_shadow', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            effective_rca = 'KEY_NORMALIZATION_MISMATCH' if is_key_drift else rca
            asset_analysis = generate_asset_analysis('shadow_missed', key, reasons, effective_rca, [])
            analysis['missed_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': [],
                'aod_admission': None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('shadow_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_shadow')
            trace_log("analysis", "missed_shadow", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift})
    
    for key in farm_zombies:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match(farm_domain_key, aod_zombie_domain_keys)
        
        if aod_domain_matched:
            aod_key_reasons = zombie_domain_reasons.get(aod_domain_matched, [])
            variants = zombie_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_zombie', key, reasons, rca, aod_key_reasons)
            analysis['matched_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_zombie', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            effective_rca = 'KEY_NORMALIZATION_MISMATCH' if is_key_drift else rca
            asset_analysis = generate_asset_analysis('zombie_missed', key, reasons, effective_rca, [])
            analysis['missed_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': [],
                'aod_admission': None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('zombie_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_zombie')
            trace_log("analysis", "missed_zombie", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift})
    
    farm_shadow_domain_keys = {to_domain_key(k) for k in farm_shadows}
    farm_zombie_domain_keys = {to_domain_key(k) for k in farm_zombies}
    farm_clean_domain_keys = {to_domain_key(k) for k in farm_clean}
    
    decision_traces = expected_block.get('decision_traces', {})
    
    def norm_key(s):
        """Normalize key for comparison - extract core name, remove suffixes."""
        s = s.lower().strip()
        s = re.sub(r'[^a-z0-9]', '', s)
        for suffix in ['com', 'io', 'app', 'net', 'org', 'co']:
            if s.endswith(suffix) and len(s) > len(suffix) + 2:
                s = s[:-len(suffix)]
        return s
    
    norm_admission = {norm_key(k): v for k, v in expected_admission.items()}
    norm_traces = {norm_key(k): v for k, v in decision_traces.items()}
    
    def get_farm_classification(domain_key, rep_key):
        """Determine Farm's classification with not-admitted awareness."""
        if domain_key in farm_zombie_domain_keys:
            return 'zombie', None
        if domain_key in farm_clean_domain_keys:
            return 'clean', None
        
        norm_rep = norm_key(rep_key)
        norm_dom = norm_key(domain_key)
        admission_status = (
            expected_admission.get(rep_key) or 
            expected_admission.get(domain_key) or
            norm_admission.get(norm_rep) or
            norm_admission.get(norm_dom)
        )
        if admission_status == 'rejected':
            trace = (
                decision_traces.get(rep_key) or 
                decision_traces.get(domain_key) or
                norm_traces.get(norm_rep) or
                norm_traces.get(norm_dom)
            )
            rejection_reason = trace.get('rejection_reason') if trace else None
            return 'not-admitted', rejection_reason
        return 'unknown', None
    
    for domain_key, domain_info in aod_shadow_domains.items():
        if not find_match(domain_key, farm_shadow_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            farm_class, rejection_reason = get_farm_classification(domain_key, rep_key)
            asset_analysis = generate_asset_analysis('false_positive_shadow', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_shadow(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            fp_entry = {
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_shadow', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            }
            if rejection_reason:
                fp_entry['farm_rejection_reason'] = rejection_reason
            analysis['false_positive_shadows'].append(fp_entry)
            increment_mismatch_counter('false_positive_shadow')
            trace_log("analysis", "false_positive_shadow", {"key": domain_key, "farm_class": farm_class})
    
    for domain_key, domain_info in aod_zombie_domains.items():
        if not find_match(domain_key, farm_zombie_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            if domain_key in farm_shadow_domain_keys:
                farm_class, rejection_reason = 'shadow', None
            else:
                farm_class, rejection_reason = get_farm_classification(domain_key, rep_key)
            asset_analysis = generate_asset_analysis('false_positive_zombie', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_zombie(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            fp_entry = {
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_zombie', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            }
            if rejection_reason:
                fp_entry['farm_rejection_reason'] = rejection_reason
            analysis['false_positive_zombies'].append(fp_entry)
            increment_mismatch_counter('false_positive_zombie')
            trace_log("analysis", "false_positive_zombie", {"key": domain_key, "farm_class": farm_class})
    
    total_expected = len(farm_shadows) + len(farm_zombies)
    total_matched = len(analysis['matched_shadows']) + len(analysis['matched_zombies'])
    total_missed = len(analysis['missed_shadows']) + len(analysis['missed_zombies'])
    total_fp = len(analysis['false_positive_shadows']) + len(analysis['false_positive_zombies'])
    
    trace_log("analysis", "build_reconciliation_analysis", {
        "matched_shadows": len(analysis['matched_shadows']),
        "matched_zombies": len(analysis['matched_zombies']),
        "missed_shadows": len(analysis['missed_shadows']),
        "missed_zombies": len(analysis['missed_zombies']),
        "false_positive_shadows": len(analysis['false_positive_shadows']),
        "false_positive_zombies": len(analysis['false_positive_zombies']),
        "total_expected": total_expected,
        "total_matched": total_matched,
        "total_missed": total_missed,
        "total_fp": total_fp,
    })
    
    fp_by_class = {'not-admitted': 0, 'clean': 0, 'zombie': 0, 'shadow': 0, 'unknown': 0}
    for fp in analysis['false_positive_shadows'] + analysis['false_positive_zombies']:
        fc = fp.get('farm_classification', 'unknown')
        fp_by_class[fc] = fp_by_class.get(fc, 0) + 1
    
    analysis['summary']['false_positive_breakdown'] = fp_by_class
    analysis['summary']['total_aod_graded'] = len(aod_shadows) + len(aod_zombies)
    analysis['summary']['total_farm_graded'] = total_expected
    
    materiality = max(2, int(total_expected * 0.1))
    
    if total_expected == 0:
        verdict = "GREAT - No anomalies expected and none found."
    elif total_missed == 0 and total_fp == 0:
        verdict = "GREAT - AOD correctly identified all expected anomalies with no false positives."
    elif total_missed <= materiality:
        if total_fp == 0:
            verdict = f"GREAT - AOD matched {total_matched}/{total_expected} expected anomalies ({total_missed} within tolerance)."
        else:
            verdict = f"GREAT - AOD matched {total_matched}/{total_expected} anomalies with {total_fp} extra flags."
    elif total_missed <= materiality * 2:
        if total_fp == 0:
            verdict = f"SOME IMPROVEMENT NEEDED - AOD missed {total_missed} of {total_expected} expected anomalies."
        else:
            verdict = f"SOME IMPROVEMENT NEEDED - AOD missed {total_missed} anomalies and flagged {total_fp} extras."
    else:
        if total_fp == 0:
            verdict = f"NEEDS WORK - AOD missed {total_missed} of {total_expected} expected anomalies."
        else:
            verdict = f"NEEDS WORK - AOD missed {total_missed} expected anomalies and had {total_fp} false positives."
    
    has_asset_summaries = bool(asset_summaries)
    payload_version = aod_payload.get('payload_version') or aod_lists.get('payload_version')
    
    consistency_errors = []
    if has_asset_summaries:
        summaries_shadow_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_shadow'))
        summaries_zombie_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_zombie'))
        
        legacy_shadow_keys = aod_lists.get('shadow_asset_keys') or aod_lists.get('shadow_assets') or []
        legacy_zombie_keys = aod_lists.get('zombie_asset_keys') or aod_lists.get('zombie_assets') or []
        
        if legacy_shadow_keys and len(legacy_shadow_keys) != summaries_shadow_count:
            consistency_errors.append(f"Shadow count mismatch: legacy list has {len(legacy_shadow_keys)}, asset_summaries has {summaries_shadow_count}")
        if legacy_zombie_keys and len(legacy_zombie_keys) != summaries_zombie_count:
            consistency_errors.append(f"Zombie count mismatch: legacy list has {len(legacy_zombie_keys)}, asset_summaries has {summaries_zombie_count}")
    
    if not has_asset_summaries:
        analysis['contract_status'] = 'STALE_CONTRACT'
        analysis['gradeable'] = False
        analysis['contract_banner'] = 'This reconciliation uses a legacy payload without asset_summaries. Grading is disabled. Re-run AOD on this snapshot to generate accurate results.'
        analysis['verdict'] = 'NOT_GRADEABLE'
        analysis['accuracy'] = None
    elif consistency_errors:
        analysis['contract_status'] = 'INCONSISTENT_CONTRACT'
        analysis['gradeable'] = False
        analysis['consistency_errors'] = consistency_errors
        analysis['contract_banner'] = f"Payload inconsistency detected: {'; '.join(consistency_errors)}. Grading refused."
        analysis['verdict'] = 'NOT_GRADEABLE'
        analysis['accuracy'] = None
    else:
        analysis['contract_status'] = 'CURRENT'
        analysis['gradeable'] = True
        analysis['payload_version'] = payload_version
        analysis['verdict'] = verdict
        denominator = total_expected + total_fp
        analysis['accuracy'] = round(total_matched / denominator * 100, 1) if denominator > 0 else 100.0
    
    return (analysis, recomputed_block)
