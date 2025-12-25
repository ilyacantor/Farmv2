import re
from typing import Optional
from collections import defaultdict

import tldextract

from src.services.constants import EXTERNAL_DOMAIN_TLDS


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def extract_domain(text: str) -> Optional[str]:
    if not text:
        return None
    text = text.lower()
    text = re.sub(r'^https?://', '', text)
    text = re.sub(r'/.*$', '', text)
    text = re.sub(r'^[^.]+\.', '', text) if text.count('.') > 1 else text
    return text if '.' in text else None


def extract_registered_domain(domain: str) -> Optional[str]:
    """Extract eTLD+1 (registered domain) using PSL via tldextract.
    
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
