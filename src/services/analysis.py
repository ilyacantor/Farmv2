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
        'CMDB_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected NO_CMDB but AOD found a CMDB correlation, classifying it as governed instead of shadow. AOD's CMDB correlation logic differs from Farm's.",
        'IDP_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected NO_IDP but AOD found an IdP correlation, classifying it as governed instead of shadow. AOD's IdP correlation logic differs from Farm's.",
    },
    'zombie_missed': {
        'default': "AOD failed to identify {key} as a zombie asset.",
        'STALE_NO_RECENT_USE': "AOD missed {key}: exists in IdP/CMDB but has no recent activity. License costs continue but nobody's using it.",
        'HAS_IDP+STALE_ACTIVITY': "AOD missed {key}: still provisioned in IdP but activity is stale (90+ days old). This app might be abandoned.",
        'HAS_CMDB+STALE_ACTIVITY': "AOD missed {key}: still in CMDB as managed asset but no recent usage detected. Potential cost savings by decommissioning.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
        'CMDB_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected different CMDB status but AOD found a correlation. AOD's CMDB correlation logic differs from Farm's.",
        'IDP_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected different IdP status but AOD found a correlation. AOD's IdP correlation logic differs from Farm's.",
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
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            headline += " - AOD found CMDB correlation that Farm didn't"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            headline += " - AOD found IdP correlation that Farm didn't"
        elif rca_hint == 'UNGOVERNED_ACTIVE':
            headline += " - active but missing from governance systems"
        farm_detail = f"Farm expected SHADOW because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            aod_detail = f"AOD correlated {key} to a CMDB CI that Farm did not correlate"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            aod_detail = f"AOD correlated {key} to an IdP object that Farm did not correlate"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as shadow"
        
    elif mismatch_type == 'zombie_missed':
        headline = f"AOD missed {key} as zombie"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            headline += " - AOD found different CMDB correlation"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            headline += " - AOD found different IdP correlation"
        elif 'STALE_ACTIVITY' in farm_reasons:
            headline += " - registered but no recent usage"
        elif rca_hint == 'STALE_NO_RECENT_USE':
            headline += " - paying for something nobody's using"
        farm_detail = f"Farm expected ZOMBIE because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        elif rca_hint in ('CMDB_CORRELATION_MISMATCH', 'IDP_CORRELATION_MISMATCH'):
            aod_detail = f"AOD found different governance correlation for {key}"
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
    
    recomputed_block = None
    cached_block = snapshot.get('__expected__')
    
    if cached_block and cached_block.get('shadow_expected') is not None:
        expected_block = cached_block
    else:
        expected_block = compute_expected_block(snapshot, mode="all")
        recomputed_block = expected_block
    
    farm_shadows = {a['asset_key'] for a in expected_block.get('shadow_expected', [])}
    farm_zombies = {a['asset_key'] for a in expected_block.get('zombie_expected', [])}
    farm_parked = {a['asset_key'] for a in expected_block.get('parked_expected', [])}
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
    
    expected_admission = expected_block.get('expected_admission', {})
    
    aod_admitted_set = None
    if not aod_admission and asset_summaries:
        aod_admitted_set = set(
            k for k, v in asset_summaries.items() 
            if isinstance(v, dict) and v.get('aod_decision') == 'admitted'
        )
        aod_admission = {k: 'admitted' for k in aod_admitted_set}
    
    # Assets AOD classified as clean/governed (admitted but not shadow/zombie)
    aod_all_admitted = set(k for k, v in aod_admission.items() if v == 'admitted')
    aod_clean = aod_all_admitted - aod_shadows - aod_zombies
    
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
    
    gross_observations = len(snapshot.get('planes', {}).get('discovery', {}).get('observations', []))
    unique_assets = len(expected_admission)
    rejected_count = sum(1 for v in expected_admission.values() if v == 'rejected')
    admitted_count = unique_assets - rejected_count
    
    parked_count = sum(1 for v in expected_admission.values() if v == 'parked')
    
    lifecycle_funnel = {
        'gross_observations': gross_observations,
        'unique_assets': unique_assets,
        'rejected_count': rejected_count,
        'admitted_count': admitted_count,
        'shadow_count': len(farm_shadows),
        'zombie_count': len(farm_zombies),
        'parked_count': len(farm_parked),
        'clean_count': len(farm_clean),
        'final_cataloged': admitted_count,
    }
    
    analysis = {
        'summary': {
            'farm_shadows': len(farm_shadows),
            'farm_zombies': len(farm_zombies),
            'farm_parked': len(farm_parked),
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
            'farm_expected_parked_count': len(farm_parked),
            'gross_observations': gross_observations,
            'cataloged': admitted_count,
            'rejected': rejected_count,
            'parked': parked_count,
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
    
    def detect_correlation_mismatch(key, farm_reasons, aod_data):
        """Detect if AOD found CMDB/IdP correlation that Farm didn't.
        
        Returns (mismatch_type, aod_reasons) where mismatch_type is one of:
        - 'CMDB_CORRELATION_MISMATCH': AOD found CMDB, Farm expected NO_CMDB
        - 'IDP_CORRELATION_MISMATCH': AOD found IdP, Farm expected NO_IDP
        - None: No correlation mismatch detected
        """
        farm_expects_no_cmdb = 'NO_CMDB' in farm_reasons
        farm_expects_no_idp = 'NO_IDP' in farm_reasons
        
        aod_reasons_for_key = aod_data.get(key, [])
        if not aod_reasons_for_key:
            for aod_key, reasons in aod_data.items():
                if norm(aod_key) == norm(key):
                    aod_reasons_for_key = reasons
                    break
        
        aod_has_cmdb = 'HAS_CMDB' in aod_reasons_for_key
        aod_has_idp = 'HAS_IDP' in aod_reasons_for_key
        
        if farm_expects_no_cmdb and aod_has_cmdb:
            return 'CMDB_CORRELATION_MISMATCH', aod_reasons_for_key
        if farm_expects_no_idp and aod_has_idp:
            return 'IDP_CORRELATION_MISMATCH', aod_reasons_for_key
        return None, aod_reasons_for_key
    
    aod_clean_reasons = {}
    for clean_key in aod_clean:
        aod_clean_reasons[clean_key] = get_aod_reasons(clean_key)
    
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
            corr_mismatch, aod_clean_codes = detect_correlation_mismatch(key, reasons, aod_clean_reasons)
            
            if corr_mismatch:
                effective_rca = corr_mismatch
                is_correlation_diff = True
            elif is_key_drift:
                effective_rca = 'KEY_NORMALIZATION_MISMATCH'
                is_correlation_diff = False
            else:
                effective_rca = rca
                is_correlation_diff = False
            
            asset_analysis = generate_asset_analysis('shadow_missed', key, reasons, effective_rca, aod_clean_codes if corr_mismatch else [])
            analysis['missed_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_clean_codes if corr_mismatch else [],
                'aod_admission': get_aod_admission(key) if corr_mismatch else None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'is_correlation_mismatch': is_correlation_diff,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('shadow_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_shadow')
            trace_log("analysis", "missed_shadow", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift, "is_correlation_mismatch": is_correlation_diff})
    
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
            corr_mismatch, aod_clean_codes = detect_correlation_mismatch(key, reasons, aod_clean_reasons)
            
            if corr_mismatch:
                effective_rca = corr_mismatch
                is_correlation_diff = True
            elif is_key_drift:
                effective_rca = 'KEY_NORMALIZATION_MISMATCH'
                is_correlation_diff = False
            else:
                effective_rca = rca
                is_correlation_diff = False
            
            asset_analysis = generate_asset_analysis('zombie_missed', key, reasons, effective_rca, aod_clean_codes if corr_mismatch else [])
            analysis['missed_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_clean_codes if corr_mismatch else [],
                'aod_admission': get_aod_admission(key) if corr_mismatch else None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'is_correlation_mismatch': is_correlation_diff,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('zombie_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_zombie')
            trace_log("analysis", "missed_zombie", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift, "is_correlation_mismatch": is_correlation_diff})
    
    farm_shadow_domain_keys = {to_domain_key(k) for k in farm_shadows}
    farm_zombie_domain_keys = {to_domain_key(k) for k in farm_zombies}
    farm_parked_domain_keys = {to_domain_key(k) for k in farm_parked}
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
        """Determine Farm's classification with not-admitted and parked awareness."""
        if domain_key in farm_zombie_domain_keys:
            return 'zombie', None
        if domain_key in farm_parked_domain_keys:
            return 'parked', None
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
        if admission_status == 'parked':
            return 'parked', None
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
    
    farm_admitted_keys = set(k for k, v in expected_admission.items() if v == 'admitted')
    farm_rejected_keys = set(k for k, v in expected_admission.items() if v == 'rejected')
    aod_admitted_keys = aod_admitted_set if aod_admitted_set else set(k for k, v in aod_admission.items() if v == 'admitted')
    aod_rejected_keys = set(expected_admission.keys()) - aod_admitted_keys
    
    cataloged_matched = farm_admitted_keys & aod_admitted_keys
    cataloged_missed = farm_admitted_keys - aod_admitted_keys
    cataloged_fp = aod_admitted_keys - farm_admitted_keys
    
    rejected_matched = farm_rejected_keys & aod_rejected_keys
    rejected_missed = farm_rejected_keys - aod_rejected_keys
    rejected_fp = aod_rejected_keys - farm_rejected_keys
    
    admission_cataloged_accuracy = round(len(cataloged_matched) / len(farm_admitted_keys) * 100, 1) if len(farm_admitted_keys) > 0 else 100.0
    admission_rejected_accuracy = round(len(rejected_matched) / len(farm_rejected_keys) * 100, 1) if len(farm_rejected_keys) > 0 else 100.0
    
    decision_traces = expected_block.get('decision_traces', {})
    expected_reasons = expected_block.get('expected_reasons', {})
    
    def build_admission_mismatch_entry(key: str, category: str, result: str, farm_expected: str, aod_decision: str) -> dict:
        """Build a detailed entry for an admission mismatch."""
        trace = decision_traces.get(key, {})
        reasons = expected_reasons.get(key, [])
        
        entry = {
            'asset_key': key,
            'category': category,
            'result': result,
            'farm_expected_admission': farm_expected,
            'aod_admission': aod_decision,
            'farm_reason_codes': reasons if isinstance(reasons, list) else [],
        }
        
        if isinstance(trace, dict):
            entry['discovery_sources'] = trace.get('discovery_sources_list', [])
            entry['discovery_count'] = trace.get('discovery_sources_count', 0)
            entry['is_external'] = trace.get('is_external', False)
            entry['is_active'] = trace.get('is_active', False)
            entry['idp_present'] = trace.get('idp_present', False)
            entry['cmdb_present'] = trace.get('cmdb_present', False)
            entry['vendor_governance'] = trace.get('vendor_governance')
            entry['rejection_reason'] = trace.get('rejection_reason')
            entry['farm_classification'] = 'shadow' if trace.get('is_shadow') else ('zombie' if trace.get('is_zombie') else 'clean')
            entry['raw_domains_seen'] = trace.get('raw_domains_seen', [])
            entry['latest_activity'] = trace.get('latest_activity_at')
        
        aod_summary = asset_summaries.get(key, {}) if asset_summaries else {}
        if isinstance(aod_summary, dict):
            entry['aod_reason_codes'] = aod_summary.get('reason_codes', [])
            entry['aod_is_shadow'] = aod_summary.get('is_shadow', False)
            entry['aod_is_zombie'] = aod_summary.get('is_zombie', False)
        
        return entry
    
    cataloged_missed_details = [
        build_admission_mismatch_entry(k, 'cataloged', 'missed_by_aod', 'admitted', 'rejected')
        for k in cataloged_missed
    ]
    cataloged_fp_details = [
        build_admission_mismatch_entry(k, 'cataloged', 'false_positive', 'rejected', 'admitted')
        for k in cataloged_fp
    ]
    rejected_missed_details = [
        build_admission_mismatch_entry(k, 'rejected', 'missed_by_aod', 'rejected', 'admitted')
        for k in rejected_missed
    ]
    rejected_fp_details = [
        build_admission_mismatch_entry(k, 'rejected', 'false_positive', 'admitted', 'rejected')
        for k in rejected_fp
    ]
    
    analysis['admission_reconciliation'] = {
        'cataloged': {
            'farm_expected': len(farm_admitted_keys),
            'aod_found': len(aod_admitted_keys),
            'matched': len(cataloged_matched),
            'missed': len(cataloged_missed),
            'false_positive': len(cataloged_fp),
            'matched_keys': list(cataloged_matched),
            'missed_keys': list(cataloged_missed),
            'fp_keys': list(cataloged_fp),
            'accuracy': admission_cataloged_accuracy,
            'missed_details': cataloged_missed_details,
            'fp_details': cataloged_fp_details,
        },
        'rejected': {
            'farm_expected': len(farm_rejected_keys),
            'aod_found': len(aod_rejected_keys),
            'matched': len(rejected_matched),
            'missed': len(rejected_missed),
            'false_positive': len(rejected_fp),
            'matched_keys': list(rejected_matched),
            'missed_keys': list(rejected_missed),
            'fp_keys': list(rejected_fp),
            'accuracy': admission_rejected_accuracy,
            'missed_details': rejected_missed_details,
            'fp_details': rejected_fp_details,
        }
    }
    
    # Correlation Bugs: Track discrepancies that indicate bugs in Farm or AOD
    # All discrepancies are bugs to be fixed - there are no "expected policy differences"
    # Farm and AOD share policy via the policy center, so disagreements = bugs
    correlation_bugs_governance = []
    correlation_bugs_key_normalization = []
    correlation_bugs_cmdb = []
    correlation_bugs_idp = []
    
    for entry in cataloged_missed_details:
        is_governed = entry.get('idp_present', False) or entry.get('cmdb_present', False)
        discovery_count = entry.get('discovery_count', 0)
        
        # Pattern 1: Governance correlation bug - Farm found governance but AOD didn't correlate
        if is_governed and discovery_count < 2:
            correlation_bugs_governance.append({
                'asset_key': entry.get('asset_key'),
                'idp_present': entry.get('idp_present', False),
                'cmdb_present': entry.get('cmdb_present', False),
                'vendor_governance': entry.get('vendor_governance'),
                'discovery_count': discovery_count,
                'discovery_sources': entry.get('discovery_sources', []),
                'farm_classification': entry.get('farm_classification', 'admitted'),
                'reason': 'GOVERNANCE_CORRELATION_BUG',
            })
    
    # Categorize missed assets by mismatch type
    for entry in analysis['missed_shadows'] + analysis['missed_zombies']:
        rca_hint = entry.get('rca_hint', '')
        asset_data = {
            'asset_key': entry.get('asset_key'),
            'farm_reason_codes': entry.get('farm_reason_codes', []),
            'aod_reason_codes': entry.get('aod_reason_codes', []),
            'reason': rca_hint,
        }
        
        if rca_hint == 'CMDB_CORRELATION_MISMATCH':
            correlation_bugs_cmdb.append(asset_data)
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            correlation_bugs_idp.append(asset_data)
        elif entry.get('is_key_drift', False) or rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            asset_data['reason'] = 'KEY_NORMALIZATION_MISMATCH'
            correlation_bugs_key_normalization.append(asset_data)
    
    analysis['correlation_bugs'] = {
        'governance_correlation': {
            'count': len(correlation_bugs_governance),
            'assets': correlation_bugs_governance,
            'explanation': (
                "BUG: Farm found governance (IdP/CMDB) for these assets but AOD did not correlate them. "
                "Since Farm and AOD share policy, this indicates a correlation bug in either system. "
                "These discrepancies require investigation and fixing."
            ),
        },
        'cmdb_correlation': {
            'count': len(correlation_bugs_cmdb),
            'assets': correlation_bugs_cmdb,
            'explanation': (
                "BUG: CMDB correlation mismatch between Farm and AOD. Farm and AOD should use "
                "identical correlation logic via shared policy. This discrepancy indicates a bug "
                "in correlation that needs to be fixed."
            ),
        },
        'idp_correlation': {
            'count': len(correlation_bugs_idp),
            'assets': correlation_bugs_idp,
            'explanation': (
                "BUG: IdP correlation mismatch between Farm and AOD. Farm and AOD should use "
                "identical correlation logic via shared policy. This discrepancy indicates a bug "
                "in correlation that needs to be fixed."
            ),
        },
        'key_normalization': {
            'count': len(correlation_bugs_key_normalization),
            'assets': correlation_bugs_key_normalization,
            'explanation': (
                "BUG: Domain key normalization difference between Farm and AOD. The domain exists "
                "in both systems but was normalized differently. This is a bug that needs fixing."
            ),
        },
        'total_bug_count': (
            len(correlation_bugs_governance) + 
            len(correlation_bugs_cmdb) + 
            len(correlation_bugs_idp) + 
            len(correlation_bugs_key_normalization)
        ),
    }
    
    classification_materiality = max(2, int(total_expected * 0.1))
    admission_total = len(farm_admitted_keys) + len(farm_rejected_keys)
    admission_matched = len(cataloged_matched) + len(rejected_matched)
    admission_missed = len(cataloged_missed) + len(rejected_missed)
    admission_fp = len(cataloged_fp) + len(rejected_fp)
    admission_materiality = max(5, int(admission_total * 0.15))
    
    classification_score = 'GREAT' if total_missed <= classification_materiality else ('SOME_ISSUES' if total_missed <= classification_materiality * 2 else 'NEEDS_WORK')
    admission_score = 'GREAT' if admission_missed <= admission_materiality else ('SOME_ISSUES' if admission_missed <= admission_materiality * 2 else 'NEEDS_WORK')
    
    classification_accuracy = round(total_matched / (total_expected + total_fp) * 100, 1) if (total_expected + total_fp) > 0 else 100.0
    admission_accuracy = round(admission_matched / admission_total * 100, 1) if admission_total > 0 else 100.0
    
    if classification_score == 'GREAT' and admission_score == 'GREAT':
        verdict = f"GREAT - Classification {total_matched}/{total_expected} ({classification_accuracy}%), Admission {admission_matched}/{admission_total} ({admission_accuracy}%)"
        overall_status = 'PASS'
    elif classification_score == 'NEEDS_WORK' or admission_score == 'NEEDS_WORK':
        issues = []
        if classification_score == 'NEEDS_WORK':
            issues.append(f"classification missed {total_missed}/{total_expected}")
        if admission_score == 'NEEDS_WORK':
            issues.append(f"admission drift {admission_missed} missed, {admission_fp} FP")
        verdict = f"NEEDS WORK - {'; '.join(issues)}"
        overall_status = 'FAIL'
    else:
        issues = []
        if classification_score == 'SOME_ISSUES':
            issues.append(f"classification {total_matched}/{total_expected}")
        if admission_score == 'SOME_ISSUES':
            issues.append(f"admission {admission_matched}/{admission_total}")
        verdict = f"SOME IMPROVEMENT NEEDED - {'; '.join(issues)}"
        overall_status = 'WARN'
    
    analysis['classification_metrics'] = {
        'expected': total_expected,
        'matched': total_matched,
        'missed': total_missed,
        'false_positives': total_fp,
        'accuracy': classification_accuracy,
        'status': classification_score,
    }
    analysis['admission_metrics'] = {
        'total': admission_total,
        'matched': admission_matched,
        'missed': admission_missed,
        'false_positives': admission_fp,
        'accuracy': admission_accuracy,
        'status': admission_score,
    }
    analysis['overall_status'] = overall_status
    
    # ANY mismatch in ANY category requires explanation
    has_any_discrepancy = (
        total_missed > 0 or 
        total_fp > 0 or 
        admission_missed > 0 or 
        admission_fp > 0
    )
    analysis['has_any_discrepancy'] = has_any_discrepancy
    
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
        combined_matched = total_matched + admission_matched
        combined_total = total_expected + total_fp + admission_total
        analysis['accuracy'] = round(combined_matched / combined_total * 100, 1) if combined_total > 0 else 100.0
        analysis['classification_accuracy'] = classification_accuracy
        analysis['admission_accuracy'] = admission_accuracy
    
    analysis['expected_block'] = expected_block
    
    return (analysis, recomputed_block)


def generate_assessment_markdown(
    reconciliation_id: str,
    aod_run_id: str,
    snapshot_id: str,
    tenant_id: str,
    created_at: str,
    analysis: dict,
    farm_expectations: dict,
    aod_payload: dict,
    analysis_version: str | None = None,
    analysis_computed_at: str | None = None
) -> str | None:
    """Generate detailed assessment markdown for a reconciliation.
    
    Returns None if:
    - The reconciliation is 100% perfect match
    - Analysis data is missing or invalid
    - No issues to report
    
    This function is defensive and will not raise on missing data.
    """
    if not analysis or not isinstance(analysis, dict):
        return None
    
    summary = analysis.get('summary') or {}
    classification_metrics = analysis.get('classification_metrics') or {}
    admission_metrics = analysis.get('admission_metrics') or {}
    
    matched_shadows = analysis.get('matched_shadows', [])
    matched_zombies = analysis.get('matched_zombies', [])
    missed_shadows = analysis.get('missed_shadows', [])
    missed_zombies = analysis.get('missed_zombies', [])
    false_positive_shadows = analysis.get('false_positive_shadows', [])
    false_positive_zombies = analysis.get('false_positive_zombies', [])
    
    total_expected = classification_metrics.get('expected', 0)
    total_matched = classification_metrics.get('matched', 0)
    total_missed = classification_metrics.get('missed', 0)
    total_fp = classification_metrics.get('false_positives', 0)
    
    is_perfect = (
        total_missed == 0 and 
        total_fp == 0 and 
        admission_metrics.get('missed', 0) == 0 and 
        admission_metrics.get('false_positives', 0) == 0
    )
    
    if is_perfect:
        return None
    
    lines = []
    
    lines.append(f"# Reconciliation Assessment Report")
    lines.append("")
    lines.append(f"**AOD Run:** `{aod_run_id}`")
    lines.append(f"**Reconciliation ID:** `{reconciliation_id}`")
    lines.append(f"**Snapshot ID:** `{snapshot_id}`")
    lines.append(f"**Tenant:** `{tenant_id}`")
    lines.append(f"**Generated:** {created_at}")
    
    # Analysis version transparency
    if analysis_version is not None:
        version_line = f"**Analysis v{analysis_version}**"
        if analysis_computed_at:
            version_line += f" computed at {analysis_computed_at}"
        lines.append(version_line)
    lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    
    overall_status = analysis.get('overall_status', 'UNKNOWN')
    verdict = analysis.get('verdict', 'N/A')
    accuracy = analysis.get('accuracy')
    
    status_emoji = {'PASS': 'PASS', 'WARN': 'WARN', 'FAIL': 'FAIL'}.get(overall_status, 'UNKNOWN')
    lines.append(f"**Overall Status:** {status_emoji}")
    lines.append(f"**Verdict:** {verdict}")
    if accuracy is not None:
        lines.append(f"**Combined Accuracy:** {accuracy}%")
    lines.append("")
    
    lines.append("### Summary Table")
    lines.append("")
    
    admission_analysis = analysis.get('admission_reconciliation', {})
    cataloged_data = admission_analysis.get('cataloged', {})
    rejected_data = admission_analysis.get('rejected', {})
    cataloged_matched = cataloged_data.get('matched_keys', [])
    cataloged_missed = cataloged_data.get('missed_keys', [])
    cataloged_fp = cataloged_data.get('fp_keys', [])
    rejected_matched = rejected_data.get('matched_keys', [])
    rejected_missed = rejected_data.get('missed_keys', [])
    rejected_fp = rejected_data.get('fp_keys', [])
    
    lines.append("| Category | Farm Expected | AOD Found | Matched | Missed | FP |")
    lines.append("|----------|---------------|-----------|---------|--------|-----|")
    
    farm_cataloged = len(cataloged_matched) + len(cataloged_missed)
    aod_cataloged = len(cataloged_matched) + len(cataloged_fp)
    lines.append(f"| **Cataloged** | {farm_cataloged} | {aod_cataloged} | {len(cataloged_matched)} | {len(cataloged_missed)} | {len(cataloged_fp)} |")
    
    farm_rejected = len(rejected_matched) + len(rejected_missed)
    aod_rejected = len(rejected_matched) + len(rejected_fp)
    lines.append(f"| **Rejected** | {farm_rejected} | {aod_rejected} | {len(rejected_matched)} | {len(rejected_missed)} | {len(rejected_fp)} |")
    
    farm_shadows = summary.get('farm_shadows', 0)
    aod_shadows = summary.get('aod_shadows', 0)
    shadow_matched = len(matched_shadows)
    shadow_missed = len(missed_shadows)
    shadow_fp = len(false_positive_shadows)
    lines.append(f"| Shadows | {farm_shadows} | {aod_shadows} | {shadow_matched} | {shadow_missed} | {shadow_fp} |")
    
    farm_zombies = summary.get('farm_zombies', 0)
    aod_zombies = summary.get('aod_zombies', 0)
    zombie_matched = len(matched_zombies)
    zombie_missed = len(missed_zombies)
    zombie_fp = len(false_positive_zombies)
    lines.append(f"| Zombies | {farm_zombies} | {aod_zombies} | {zombie_matched} | {zombie_missed} | {zombie_fp} |")
    
    lines.append("")
    
    lines.append("### Lifecycle Funnel")
    lines.append("")
    funnel = analysis.get('lifecycle_funnel', {})
    lines.append(f"- **Gross Observations:** {funnel.get('gross_observations', 0)}")
    lines.append(f"- **Unique Assets:** {funnel.get('unique_assets', 0)}")
    lines.append(f"- **Rejected (not admitted):** {funnel.get('rejected_count', 0)}")
    lines.append(f"- **Admitted:** {funnel.get('admitted_count', 0)}")
    lines.append(f"- **Cataloged (final):** {funnel.get('final_cataloged', 0)}")
    lines.append("")
    
    # Correlation Bugs Section
    corr_bugs = analysis.get('correlation_bugs', {})
    total_bugs = corr_bugs.get('total_bug_count', 0)
    
    if total_bugs > 0:
        lines.append("---")
        lines.append("")
        lines.append("## Correlation Bugs (Discrepancies Requiring Fix)")
        lines.append("")
        lines.append("> **IMPORTANT:** The following discrepancies are BUGS, not expected differences. Farm and AOD share policy via the policy center, so any disagreement indicates a bug that must be fixed.")
        lines.append("")
        
        governance_bugs = corr_bugs.get('governance_correlation', {})
        if governance_bugs.get('count', 0) > 0:
            lines.append("### Governance Correlation Bug")
            lines.append("")
            lines.append(f"**{governance_bugs.get('count', 0)} assets** - Farm found governance but AOD didn't correlate.")
            lines.append("")
            lines.append(f"> {governance_bugs.get('explanation', '')}")
            lines.append("")
            lines.append("| Asset | IdP | CMDB | Vendor | Discovery Count |")
            lines.append("|-------|-----|------|--------|-----------------|")
            for asset in governance_bugs.get('assets', [])[:20]:
                idp = 'Yes' if asset.get('idp_present') else 'No'
                cmdb = 'Yes' if asset.get('cmdb_present') else 'No'
                vendor = asset.get('vendor_governance') or '-'
                disc = asset.get('discovery_count', 0)
                lines.append(f"| {asset.get('asset_key', 'N/A')} | {idp} | {cmdb} | {vendor} | {disc} |")
            if governance_bugs.get('count', 0) > 20:
                lines.append(f"| ... and {governance_bugs.get('count', 0) - 20} more | | | | |")
            lines.append("")
        
        cmdb_bugs = corr_bugs.get('cmdb_correlation', {})
        if cmdb_bugs.get('count', 0) > 0:
            lines.append("### CMDB Correlation Bug")
            lines.append("")
            lines.append(f"**{cmdb_bugs.get('count', 0)} assets** - CMDB correlation mismatch between Farm and AOD.")
            lines.append("")
            lines.append(f"> {cmdb_bugs.get('explanation', '')}")
            lines.append("")
            lines.append("| Asset | Farm Reason Codes | AOD Reason Codes |")
            lines.append("|-------|-------------------|------------------|")
            for asset in cmdb_bugs.get('assets', [])[:20]:
                farm_codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
                aod_codes = ', '.join(asset.get('aod_reason_codes', [])[:3]) or '-'
                lines.append(f"| {asset.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} |")
            if cmdb_bugs.get('count', 0) > 20:
                lines.append(f"| ... and {cmdb_bugs.get('count', 0) - 20} more | | |")
            lines.append("")
        
        idp_bugs = corr_bugs.get('idp_correlation', {})
        if idp_bugs.get('count', 0) > 0:
            lines.append("### IdP Correlation Bug")
            lines.append("")
            lines.append(f"**{idp_bugs.get('count', 0)} assets** - IdP correlation mismatch between Farm and AOD.")
            lines.append("")
            lines.append(f"> {idp_bugs.get('explanation', '')}")
            lines.append("")
            lines.append("| Asset | Farm Reason Codes | AOD Reason Codes |")
            lines.append("|-------|-------------------|------------------|")
            for asset in idp_bugs.get('assets', [])[:20]:
                farm_codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
                aod_codes = ', '.join(asset.get('aod_reason_codes', [])[:3]) or '-'
                lines.append(f"| {asset.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} |")
            if idp_bugs.get('count', 0) > 20:
                lines.append(f"| ... and {idp_bugs.get('count', 0) - 20} more | | |")
            lines.append("")
        
        key_norm = corr_bugs.get('key_normalization', {})
        if key_norm.get('count', 0) > 0:
            lines.append("### Key Normalization Bug")
            lines.append("")
            lines.append(f"**{key_norm.get('count', 0)} assets** - Domain canonicalization bug.")
            lines.append("")
            lines.append(f"> {key_norm.get('explanation', '')}")
            lines.append("")
            lines.append("| Asset | Farm Reason Codes |")
            lines.append("|-------|-------------------|")
            for asset in key_norm.get('assets', [])[:20]:
                codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
                lines.append(f"| {asset.get('asset_key', 'N/A')} | {codes} |")
            if key_norm.get('count', 0) > 20:
                lines.append(f"| ... and {key_norm.get('count', 0) - 20} more | |")
            lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("## Classification Analysis")
    lines.append("")
    
    if matched_shadows:
        lines.append("### Matched Shadows (Correctly Identified)")
        lines.append("")
        lines.append(f"**{len(matched_shadows)} assets correctly identified as Shadow IT**")
        lines.append("")
        lines.append("| Asset | Farm Reason Codes | AOD Reason Codes | RCA Hint |")
        lines.append("|-------|-------------------|------------------|----------|")
        for item in matched_shadows:
            farm_codes = ', '.join(item.get('farm_reason_codes', [])[:4]) or '-'
            aod_codes = ', '.join(item.get('aod_reason_codes', [])[:4]) or '-'
            rca = item.get('rca_hint') or '-'
            lines.append(f"| {item.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} | {rca} |")
        lines.append("")
    
    if missed_shadows:
        lines.append("### Missed Shadows (False Negatives)")
        lines.append("")
        lines.append(f"**{len(missed_shadows)} assets missed by AOD - should have been Shadow IT**")
        lines.append("")
        for item in missed_shadows:
            asset_key = item.get('asset_key', 'N/A')
            lines.append(f"#### `{asset_key}`")
            lines.append("")
            lines.append(f"**Headline:** {item.get('headline', 'N/A')}")
            lines.append("")
            lines.append(f"- **Farm Detail:** {item.get('farm_detail', 'N/A')}")
            lines.append(f"- **AOD Detail:** {item.get('aod_detail', 'N/A')}")
            lines.append(f"- **RCA Hint:** `{item.get('rca_hint', 'N/A')}`")
            if item.get('is_correlation_mismatch'):
                aod_codes = item.get('aod_reason_codes', [])
                lines.append(f"- **Correlation Mismatch:** Yes - AOD found governance correlation that Farm didn't")
                if aod_codes:
                    lines.append(f"- **AOD Reason Codes:** `{', '.join(aod_codes)}`")
            elif item.get('is_key_drift'):
                lines.append(f"- **Key Drift:** Yes - domain exists in AOD evidence but not used as canonical key")
            lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
            lines.append("")
    
    if false_positive_shadows:
        lines.append("### False Positive Shadows")
        lines.append("")
        lines.append(f"**{len(false_positive_shadows)} assets incorrectly classified as Shadow IT by AOD**")
        lines.append("")
        
        fp_by_class = {}
        for fp in false_positive_shadows:
            farm_class = fp.get('farm_classification', 'unknown')
            if farm_class not in fp_by_class:
                fp_by_class[farm_class] = []
            fp_by_class[farm_class].append(fp)
        
        for farm_class, items in fp_by_class.items():
            lines.append(f"#### Farm Classification: `{farm_class}` ({len(items)} assets)")
            lines.append("")
            for item in items:
                asset_key = item.get('asset_key', 'N/A')
                lines.append(f"**`{asset_key}`**")
                lines.append("")
                lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
                lines.append(f"- **AOD Reason Codes:** `{', '.join(item.get('aod_reason_codes', []))}`")
                
                farm_codes = set(item.get('farm_reason_codes', []))
                aod_codes = set(item.get('aod_reason_codes', []))
                diff_in_farm = farm_codes - aod_codes
                diff_in_aod = aod_codes - farm_codes
                if diff_in_farm:
                    lines.append(f"- **In Farm only:** `{', '.join(diff_in_farm)}`")
                if diff_in_aod:
                    lines.append(f"- **In AOD only:** `{', '.join(diff_in_aod)}`")
                
                if item.get('farm_investigation'):
                    inv = item.get('farm_investigation', {})
                    if inv.get('root_cause'):
                        lines.append(f"- **Root Cause:** {inv.get('root_cause')}")
                lines.append("")
    
    if matched_zombies:
        lines.append("### Matched Zombies (Correctly Identified)")
        lines.append("")
        lines.append(f"**{len(matched_zombies)} assets correctly identified as Zombie**")
        lines.append("")
        lines.append("| Asset | Farm Reason Codes | AOD Reason Codes | RCA Hint |")
        lines.append("|-------|-------------------|------------------|----------|")
        for item in matched_zombies:
            farm_codes = ', '.join(item.get('farm_reason_codes', [])[:4]) or '-'
            aod_codes = ', '.join(item.get('aod_reason_codes', [])[:4]) or '-'
            rca = item.get('rca_hint') or '-'
            lines.append(f"| {item.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} | {rca} |")
        lines.append("")
    
    if missed_zombies:
        lines.append("### Missed Zombies (False Negatives)")
        lines.append("")
        lines.append(f"**{len(missed_zombies)} assets missed by AOD - should have been Zombie**")
        lines.append("")
        for item in missed_zombies:
            asset_key = item.get('asset_key', 'N/A')
            lines.append(f"#### `{asset_key}`")
            lines.append("")
            lines.append(f"**Headline:** {item.get('headline', 'N/A')}")
            lines.append("")
            lines.append(f"- **Farm Detail:** {item.get('farm_detail', 'N/A')}")
            lines.append(f"- **AOD Detail:** {item.get('aod_detail', 'N/A')}")
            lines.append(f"- **RCA Hint:** `{item.get('rca_hint', 'N/A')}`")
            if item.get('is_correlation_mismatch'):
                aod_codes = item.get('aod_reason_codes', [])
                lines.append(f"- **Correlation Mismatch:** Yes - AOD found governance correlation that Farm didn't")
                if aod_codes:
                    lines.append(f"- **AOD Reason Codes:** `{', '.join(aod_codes)}`")
            elif item.get('is_key_drift'):
                lines.append(f"- **Key Drift:** Yes - domain exists in AOD evidence but not used as canonical key")
            lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
            lines.append("")
    
    if false_positive_zombies:
        lines.append("### False Positive Zombies")
        lines.append("")
        lines.append(f"**{len(false_positive_zombies)} assets incorrectly classified as Zombie by AOD**")
        lines.append("")
        
        fp_by_class = {}
        for fp in false_positive_zombies:
            farm_class = fp.get('farm_classification', 'unknown')
            if farm_class not in fp_by_class:
                fp_by_class[farm_class] = []
            fp_by_class[farm_class].append(fp)
        
        for farm_class, items in fp_by_class.items():
            lines.append(f"#### Farm Classification: `{farm_class}` ({len(items)} assets)")
            lines.append("")
            for item in items:
                asset_key = item.get('asset_key', 'N/A')
                lines.append(f"**`{asset_key}`**")
                lines.append("")
                lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
                lines.append(f"- **AOD Reason Codes:** `{', '.join(item.get('aod_reason_codes', []))}`")
                lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("## Admission Analysis")
    lines.append("")
    
    admission_analysis = analysis.get('admission_reconciliation', {})
    cataloged_data = admission_analysis.get('cataloged', {})
    rejected_data = admission_analysis.get('rejected', {})
    cataloged_matched = cataloged_data.get('matched_keys', [])
    cataloged_missed = cataloged_data.get('missed_keys', [])
    cataloged_fp = cataloged_data.get('fp_keys', [])
    rejected_matched = rejected_data.get('matched_keys', [])
    rejected_missed = rejected_data.get('missed_keys', [])
    rejected_fp = rejected_data.get('fp_keys', [])
    
    lines.append("### Admission Metrics")
    lines.append("")
    lines.append(f"- **Total Assets:** {admission_metrics.get('total', 0)}")
    lines.append(f"- **Matched:** {admission_metrics.get('matched', 0)}")
    lines.append(f"- **Missed:** {admission_metrics.get('missed', 0)}")
    lines.append(f"- **False Positives:** {admission_metrics.get('false_positives', 0)}")
    lines.append(f"- **Accuracy:** {admission_metrics.get('accuracy', 0)}%")
    lines.append("")
    
    if cataloged_missed:
        lines.append("### Cataloged Missed by AOD")
        lines.append("")
        lines.append(f"**{len(cataloged_missed)} assets should have been cataloged but weren't**")
        lines.append("")
        lines.append("| Asset | Farm Classification |")
        lines.append("|-------|---------------------|")
        for key in cataloged_missed[:20]:
            lines.append(f"| {key} | admitted |")
        if len(cataloged_missed) > 20:
            lines.append(f"| ... | ({len(cataloged_missed) - 20} more) |")
        lines.append("")
    
    if rejected_missed:
        lines.append("### Rejected Missed by AOD")
        lines.append("")
        lines.append(f"**{len(rejected_missed)} assets should have been rejected but weren't**")
        lines.append("")
        for key in rejected_missed[:10]:
            lines.append(f"- `{key}`")
        if len(rejected_missed) > 10:
            lines.append(f"- ... ({len(rejected_missed) - 10} more)")
        lines.append("")
    
    cataloged_fp_details = cataloged_data.get('fp_details', [])
    if cataloged_fp_details or cataloged_fp:
        lines.append("### Admission False Positives (Cataloged)")
        lines.append("")
        fp_count = len(cataloged_fp_details) if cataloged_fp_details else len(cataloged_fp)
        lines.append(f"**{fp_count} assets AOD cataloged but Farm expected rejection**")
        lines.append("")
        lines.append("These assets should have been rejected (not admitted) based on Farm's admission policy.")
        lines.append("")
        lines.append("| Asset Key | Discovery Sources | Rejection Reason | Farm Reason Codes |")
        lines.append("|-----------|-------------------|------------------|-------------------|")
        
        if cataloged_fp_details:
            for item in cataloged_fp_details:
                asset_key = item.get('asset_key', 'N/A')
                discovery_count = item.get('discovery_count', 0)
                discovery_sources = ', '.join(item.get('discovery_sources', [])) or 'none'
                rejection_reason = item.get('rejection_reason', 'N/A')
                reason_codes = ', '.join(item.get('farm_reason_codes', [])[:5]) or 'N/A'
                if len(item.get('farm_reason_codes', [])) > 5:
                    reason_codes += '...'
                lines.append(f"| `{asset_key}` | {discovery_count} ({discovery_sources}) | {rejection_reason} | {reason_codes} |")
        else:
            for key in cataloged_fp[:50]:
                lines.append(f"| `{key}` | - | - | - |")
            if len(cataloged_fp) > 50:
                lines.append(f"| ... | | | ({len(cataloged_fp) - 50} more) |")
        lines.append("")
    
    rejected_fp_details = rejected_data.get('fp_details', [])
    if rejected_fp_details or rejected_fp:
        lines.append("### Admission False Positives (Rejected)")
        lines.append("")
        fp_count = len(rejected_fp_details) if rejected_fp_details else len(rejected_fp)
        lines.append(f"**{fp_count} assets AOD rejected but Farm expected admission**")
        lines.append("")
        lines.append("| Asset Key | Discovery Sources | Farm Reason Codes |")
        lines.append("|-----------|-------------------|-------------------|")
        
        if rejected_fp_details:
            for item in rejected_fp_details:
                asset_key = item.get('asset_key', 'N/A')
                discovery_count = item.get('discovery_count', 0)
                discovery_sources = ', '.join(item.get('discovery_sources', [])) or 'none'
                reason_codes = ', '.join(item.get('farm_reason_codes', [])[:5]) or 'N/A'
                if len(item.get('farm_reason_codes', [])) > 5:
                    reason_codes += '...'
                lines.append(f"| `{asset_key}` | {discovery_count} ({discovery_sources}) | {reason_codes} |")
        else:
            for key in rejected_fp[:20]:
                lines.append(f"| `{key}` | - | - |")
            if len(rejected_fp) > 20:
                lines.append(f"| ... | | ({len(rejected_fp) - 20} more) |")
        lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("## Root Cause Analysis Summary")
    lines.append("")
    
    rca_counts = {}
    for item in missed_shadows + missed_zombies:
        rca = item.get('rca_hint') or 'UNKNOWN'
        rca_counts[rca] = rca_counts.get(rca, 0) + 1
    for item in false_positive_shadows + false_positive_zombies:
        farm_class = item.get('farm_classification', 'unknown')
        rca = f"FP_FROM_{farm_class.upper()}"
        rca_counts[rca] = rca_counts.get(rca, 0) + 1
    
    if rca_counts:
        lines.append("| RCA Hint | Count |")
        lines.append("|----------|-------|")
        for rca, count in sorted(rca_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {rca} | {count} |")
        lines.append("")
    else:
        lines.append("No issues to analyze.")
        lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    
    recommendations = []
    
    if any(item.get('is_key_drift') for item in missed_shadows + missed_zombies):
        recommendations.append("- **Key Normalization:** AOD has evidence for some assets but is not using the expected canonical keys. Review key normalization logic.")
    
    fp_clean_count = sum(1 for fp in false_positive_shadows if fp.get('farm_classification') == 'clean')
    if fp_clean_count > 0:
        has_ongoing_finance_fps = [fp for fp in false_positive_shadows 
                                    if fp.get('farm_classification') == 'clean' 
                                    and 'HAS_ONGOING_FINANCE' in fp.get('farm_reason_codes', [])]
        if has_ongoing_finance_fps:
            recommendations.append(f"- **Finance Governance:** {len(has_ongoing_finance_fps)} assets have `HAS_ONGOING_FINANCE` but AOD classified as shadow. Consider treating ongoing finance as governance.")
    
    if len(missed_shadows) > 0:
        recommendations.append(f"- **Shadow Detection:** {len(missed_shadows)} expected shadows not found. Check shadow classification rules.")
    
    if len(missed_zombies) > 0:
        recommendations.append(f"- **Zombie Detection:** {len(missed_zombies)} expected zombies not found. Check zombie classification rules.")
    
    if recommendations:
        for rec in recommendations:
            lines.append(rec)
    else:
        lines.append("No specific recommendations at this time.")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Generated by AOS Farm Assessment Engine*")
    
    return '\n'.join(lines)
