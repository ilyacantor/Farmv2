"""
Evidence extraction and key matching utilities.

These functions extract domain references from AOD payloads and
check if Farm-expected keys appear in AOD evidence.
"""
import re
from urllib.parse import urlparse


def extract_aod_evidence_domains(aod_payload: dict) -> set:
    """Extract all domains/URLs referenced in AOD's asset_summaries and evidence.

    Recursively traverses all nested structures to find domain references.

    Args:
        aod_payload: The AOD response payload

    Returns:
        Set of lowercase domain strings found in the payload
    """
    domains = set()

    def extract_domains_from_string(s: str):
        """Extract potential domain from a string (URL or domain)."""
        s = str(s).lower().strip()
        if not s:
            return
        if '://' in s:
            try:
                parsed = urlparse(s)
                if parsed.netloc:
                    domains.add(parsed.netloc)
            except Exception:
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

    Args:
        key: The Farm-expected asset key
        aod_evidence_domains: Set of domains extracted from AOD evidence

    Returns:
        True if the key appears in AOD evidence
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

        # Exact match
        if key_lower == domain_lower:
            return True
        # Subdomain match (key.domain.com matches domain.com)
        if key_lower in domain_lower or domain_lower.endswith('.' + key_lower):
            return True
        # Core name match (slack matches slackcom)
        if key_core_norm == domain_core_norm and len(key_core_norm) >= 3:
            return True
        # Partial match for longer keys
        if len(key_norm) >= 5 and (key_norm in domain_norm or domain_norm in key_norm):
            return True

    return False


def normalize_key_for_comparison(s: str) -> str:
    """Normalize asset key for comparison - extract core name, remove suffixes.

    Args:
        s: Asset key to normalize

    Returns:
        Normalized key string
    """
    s = s.lower().strip()
    s = re.sub(r'[^a-z0-9]', '', s)
    for suffix in ['com', 'io', 'app', 'net', 'org', 'co']:
        if s.endswith(suffix) and len(s) > len(suffix) + 2:
            s = s[:-len(suffix)]
    return s


def find_match_in_set(key: str, target_set: set) -> str | None:
    """Find matching key in target set using normalized comparison.

    Args:
        key: Key to search for
        target_set: Set of keys to search in

    Returns:
        Matching key from target_set, or None if not found
    """
    nk = normalize_key_for_comparison(key)
    for t in target_set:
        nt = normalize_key_for_comparison(t)
        if nk == nt:
            return t
        if nk in nt or nt in nk:
            return t
    return None


def detect_correlation_mismatch(
    key: str,
    farm_reasons: list,
    aod_data: dict
) -> tuple[str | None, list]:
    """Detect if AOD found CMDB/IdP correlation that Farm didn't.

    Args:
        key: Asset key to check
        farm_reasons: Farm's reason codes for this asset
        aod_data: AOD's reason codes dict

    Returns:
        Tuple of (mismatch_type, aod_reasons) where mismatch_type is one of:
        - 'CMDB_CORRELATION_MISMATCH': AOD found CMDB, Farm expected NO_CMDB
        - 'IDP_CORRELATION_MISMATCH': AOD found IdP, Farm expected NO_IDP
        - None: No correlation mismatch detected
    """
    farm_expects_no_cmdb = 'NO_CMDB' in farm_reasons
    farm_expects_no_idp = 'NO_IDP' in farm_reasons

    aod_reasons_for_key = aod_data.get(key, [])
    if not aod_reasons_for_key:
        norm_key = normalize_key_for_comparison(key)
        for aod_key, reasons in aod_data.items():
            if normalize_key_for_comparison(aod_key) == norm_key:
                aod_reasons_for_key = reasons
                break

    aod_has_cmdb = 'HAS_CMDB' in aod_reasons_for_key
    aod_has_idp = 'HAS_IDP' in aod_reasons_for_key

    if farm_expects_no_cmdb and aod_has_cmdb:
        return 'CMDB_CORRELATION_MISMATCH', aod_reasons_for_key
    if farm_expects_no_idp and aod_has_idp:
        return 'IDP_CORRELATION_MISMATCH', aod_reasons_for_key
    return None, aod_reasons_for_key
