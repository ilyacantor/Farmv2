import re
from functools import lru_cache
from typing import Optional
from collections import defaultdict

import tldextract

from src.services.constants import EXTERNAL_DOMAIN_TLDS

# Infrastructure domains that should NOT be collapsed to parent
# Per AOD Stage 4 fix (Jan 2026): these are preserved as standalone keys
# NOTE: google.com is NOT included - it's a regular SaaS domain, not infrastructure
INFRASTRUCTURE_DOMAINS = frozenset({
    # Microsoft infrastructure (NOT microsoft.com - that's regular SaaS)
    'outlook.com',
    'office.com',
    'office365.com',
    'sharepoint.com',
    'microsoftonline.com',
    'live.com',
    'hotmail.com',
    # Google infrastructure (NOT google.com - that's regular SaaS)
    'googleapis.com',
    'gstatic.com',
    'googleusercontent.com',
    'googlevideo.com',
    # AWS infrastructure  
    'cloudfront.net',
    'awsstatic.com',
    'amazonaws.com',
    # Other CDN/infrastructure
    'akamaihd.net',
    'akamai.net',
    'fastly.net',
    'cloudflare.com',
})


@lru_cache(maxsize=2048)
def normalize_name(name: str) -> str:
    """Normalize name for matching - cached for performance.

    Cache size 2048 covers typical enterprise asset count per snapshot.
    """
    if not name:
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def extract_domain(text: str) -> Optional[str]:
    """Extract the registered domain (eTLD+1) from a URL or domain string.
    
    Uses tldextract to properly identify the registered domain, avoiding
    the creation of phantom domains like 'cdn.com' from 'static.cdn.cloudflare.com'.
    
    IMPORTANT: Infrastructure domains (googleapis.com, gstatic.com, office.com, etc.)
    are preserved as standalone keys and NOT collapsed to their parent domain.
    This matches AOD Stage 4 behavior (Jan 2026).
    
    Examples:
        https://app.slack.com/path -> slack.com
        static.cdn.cloudflare.com -> cloudflare.com
        api.example.co.uk -> example.co.uk
        calendly.com -> calendly.com
        api.googleapis.com -> googleapis.com (preserved, not google.com)
        login.microsoftonline.com -> microsoftonline.com (preserved)
    """
    if not text:
        return None
    text = text.lower().strip()
    text = re.sub(r'^https?://', '', text)
    text = re.sub(r'/.*$', '', text)
    text = text.split(':')[0]  # Remove port if present
    
    if '.' not in text:
        return None
    
    # Check if this is an infrastructure domain that should be preserved
    # Infrastructure domains are NOT collapsed to parent (e.g., googleapis.com stays as-is)
    for infra_domain in INFRASTRUCTURE_DOMAINS:
        if text == infra_domain or text.endswith('.' + infra_domain):
            return infra_domain
    
    # Use tldextract for proper eTLD+1 extraction
    return extract_registered_domain(text)


@lru_cache(maxsize=1024)
def extract_registered_domain(domain: str) -> Optional[str]:
    """Extract eTLD+1 (registered domain) using PSL via tldextract.

    Cached because tldextract parsing is expensive.

    Examples:
        app.slack.com -> slack.com
        cdn.static.example.co.uk -> example.co.uk
        slack.com -> slack.com
        www.redis.com -> redis.com
    """
    if not domain:
        return None
    domain = domain.lower().strip('.')

    ext = tldextract.extract(domain)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return domain


def to_domain_key(entity_key: str) -> str:
    """
    Convert an entity key to its domain key for roll-up.
    e.g. "Microsoft 365" -> "microsoft.com" (if domain present)
         "calendly.com" -> "calendly.com"
         "Slack" -> "slack" (normalized name)
    """
    if not entity_key:
        return ""
    
    if '.' in entity_key and ' ' not in entity_key:
        domain = extract_domain(entity_key)
        if domain:
            return domain.lower()
        return entity_key.lower()
    
    return normalize_name(entity_key)


def roll_up_to_domains(entity_keys: set, reason_codes: dict = None) -> dict:
    """
    Roll up entity-level keys to domain-level.
    Returns: {domain_key: {'variants': [original_keys], 'reason_codes': merged_codes}}
    
    Domain roll-up rule:
    - domain.has_X = OR(entities.has_X) for each flag
    - Variants are tracked for display
    """
    domains = defaultdict(lambda: {'variants': [], 'reason_codes': set()})
    
    for key in entity_keys:
        domain_key = to_domain_key(key)
        if not domain_key:
            continue
        
        domains[domain_key]['variants'].append(key)
        
        if reason_codes and key in reason_codes:
            codes = reason_codes[key]
            if isinstance(codes, list):
                domains[domain_key]['reason_codes'].update(codes)
    
    for dk in domains:
        domains[dk]['reason_codes'] = list(domains[dk]['reason_codes'])
    
    return dict(domains)


def is_external_domain(key: str) -> bool:
    """Check if key looks like an external SaaS domain (has a valid TLD).
    
    Internal service names (authservice, billing-api, etc.) are not external domains.
    Supports compound TLDs like .co.uk, .com.au.
    """
    key_lower = key.lower()
    if '.' not in key_lower:
        return False
    return any(key_lower.endswith(tld) for tld in EXTERNAL_DOMAIN_TLDS)


def is_valid_fqdn(key: str) -> bool:
    """Check if key is a valid FQDN with a real TLD suffix.
    
    Uses tldextract to validate against Public Suffix List.
    Internal hostnames like 'paymentgateway', 'auth-service', 'images694' 
    will return False because they lack a valid TLD.
    
    Examples:
        google.com -> True
        mail.google.com -> True  
        paymentgateway -> False
        auth-service -> False
        images694 -> False
    """
    if not key:
        return False
    
    ext = tldextract.extract(key)
    return bool(ext.suffix)


def select_canonical_key(
    observed_domains: set,
    banned_domains: set = None,
    alias_collapse: dict = None,
) -> tuple[Optional[str], Optional[str]]:
    """Select canonical key from observed domains using deterministic contract.
    
    CONTRACT (must match AOD implementation):
    1. Build observed_registered_domains = {eTLD+1(domain) for domain in observations}
    2. Remove banned domains → if empty after removal, return (None, "REJECTED_BANNED")
    3. Apply alias collapse for domains in alias_collapse mapping
    4. If multiple remain: choose by lexicographic sort (deterministic)
    
    NOT ALLOWED:
    - Do not use list position (domains[0])
    - Do not use "first observation wins"
    - Do not roll up to vendor roots unless in alias_collapse
    
    Args:
        observed_domains: Set of domains from discovery observations
        banned_domains: Set of domains to exclude (from policy)
        alias_collapse: Dict mapping domain aliases to canonical form
        
    Returns:
        (canonical_key, rejection_reason) - rejection_reason is None if key selected
    """
    if not observed_domains:
        return None, "NO_DOMAINS"
    
    banned_domains = banned_domains or set()
    alias_collapse = alias_collapse or {}
    
    # 1. Get eTLD+1 for all observed domains
    registered = set()
    for domain in observed_domains:
        if not domain:
            continue
        reg = extract_registered_domain(domain)
        if reg:
            registered.add(reg.lower())
    
    if not registered:
        return None, "NO_VALID_DOMAINS"
    
    # 2. Remove banned domains
    banned_lower = {d.lower() for d in banned_domains}
    after_ban = registered - banned_lower
    
    if not after_ban:
        return None, "REJECTED_BANNED"
    
    # 3. Apply alias collapse (only for domains in the mapping)
    collapsed = set()
    for domain in after_ban:
        canonical = alias_collapse.get(domain, domain)
        collapsed.add(canonical.lower())
    
    # 4. Lexicographic sort, pick first (deterministic tie-breaker)
    sorted_domains = sorted(collapsed)
    return sorted_domains[0], None
