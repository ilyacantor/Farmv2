"""
Mismatch pattern analyzer for automated debugging.
Produces machine-readable diagnostics that AOD can consume.
"""

from typing import Any
import re


def analyze_mismatches(analysis: dict) -> dict:
    """
    Analyze reconciliation mismatches and categorize by pattern.
    Returns structured diagnostics for AOD consumption.
    """
    admission = analysis.get("admission_reconciliation", {})
    cataloged = admission.get("cataloged", {})
    rejected = admission.get("rejected", {})
    
    fp_shadows = analysis.get("false_positive_shadows", [])
    fp_zombies = analysis.get("false_positive_zombies", [])
    missed_shadows = analysis.get("missed_shadows", [])
    missed_zombies = analysis.get("missed_zombies", [])
    
    result = {
        "key_drift": [],
        "truncation_patterns": [],
        "infrastructure_leaks": [],
        "suffix_drift": [],
        "domain_alias_suggestions": {},
        "summary": {
            "total_issues": 0,
            "key_drift_count": 0,
            "truncation_count": 0,
            "infrastructure_leak_count": 0,
            "suffix_drift_count": 0,
        }
    }
    
    farm_keys = set(cataloged.get("missed_keys", []))
    aod_keys = set(cataloged.get("fp_keys", []))
    
    farm_keys.update(rejected.get("fp_keys", []))
    aod_keys.update(rejected.get("missed_keys", []))
    
    _detect_key_drift(farm_keys, aod_keys, result)
    _detect_truncation(aod_keys, farm_keys, result)
    _detect_infrastructure_leaks(aod_keys, result)
    _detect_suffix_drift(farm_keys, aod_keys, result)
    _generate_alias_suggestions(result)
    
    result["summary"]["total_issues"] = (
        result["summary"]["key_drift_count"] +
        result["summary"]["truncation_count"] +
        result["summary"]["infrastructure_leak_count"] +
        result["summary"]["suffix_drift_count"]
    )
    
    return result


def _detect_key_drift(farm_keys: set, aod_keys: set, result: dict):
    """Detect domain vs product name mismatches."""
    
    domain_to_product = {
        "hubspot.com": ["hubspot"],
        "salesforce.com": ["salesforce"],
        "zendesk.com": ["zendesk"],
        "workday.com": ["workday"],
        "splunk.com": ["splunk"],
        "pagerduty.com": ["pagerduty", "pagerdut"],
        "okta.com": ["okta", "okta-prod"],
        "microsoft.com": ["microsoft 365", "office 365", "microsoft365"],
        "google.com": ["google workspace", "gsuite", "g suite"],
        "servicenow.com": ["servicenow"],
        "tableau.com": ["tableau"],
        "twilio.com": ["twilio"],
        "sendgrid.com": ["sendgrid"],
        "mailchimp.com": ["mailchimp"],
        "intercom.com": ["intercom"],
        "segment.com": ["segment"],
        "amplitude.com": ["amplitude"],
        "mixpanel.com": ["mixpanel"],
    }
    
    for farm_key in farm_keys:
        if farm_key in domain_to_product:
            for product_name in domain_to_product[farm_key]:
                if product_name in aod_keys:
                    result["key_drift"].append({
                        "farm_key": farm_key,
                        "aod_key": product_name,
                        "pattern": "domain_vs_product",
                        "fix": f"Map '{product_name}' → '{farm_key}'"
                    })
                    result["summary"]["key_drift_count"] += 1
        
        if farm_key.endswith(".com") or farm_key.endswith(".io") or farm_key.endswith(".org"):
            base = farm_key.rsplit(".", 1)[0]
            if "." in base:
                base = base.rsplit(".", 1)[0]
            if base in aod_keys:
                result["key_drift"].append({
                    "farm_key": farm_key,
                    "aod_key": base,
                    "pattern": "missing_tld",
                    "fix": f"Append TLD to '{base}'"
                })
                result["summary"]["key_drift_count"] += 1


def _detect_truncation(aod_keys: set, farm_keys: set, result: dict):
    """Detect truncated keys (7-8 char cutoff pattern)."""
    
    for aod_key in aod_keys:
        if len(aod_key) <= 8 and not aod_key.endswith(".com"):
            for farm_key in farm_keys:
                if farm_key.startswith(aod_key) and len(farm_key) > len(aod_key):
                    result["truncation_patterns"].append({
                        "aod_key": aod_key,
                        "likely_full_key": farm_key,
                        "truncated_at": len(aod_key),
                        "fix": f"Expand '{aod_key}' → '{farm_key}'"
                    })
                    result["summary"]["truncation_count"] += 1
                    break


INFRASTRUCTURE_DOMAINS = {
    "postgresql.org", "mysql.com", "apache.org", "redis.io", "redis.com",
    "mongodb.com", "elastic.co", "elasticsearch.com", "kafka.apache.org",
    "nginx.org", "docker.com", "kubernetes.io", "linux.org", "gnu.org",
    "python.org", "nodejs.org", "golang.org", "rust-lang.org", "ruby-lang.org"
}


def _detect_infrastructure_leaks(aod_keys: set, result: dict):
    """Detect infrastructure domains that should be excluded."""
    
    for key in aod_keys:
        if key in INFRASTRUCTURE_DOMAINS:
            result["infrastructure_leaks"].append({
                "key": key,
                "reason": "infrastructure_domain",
                "fix": f"Add '{key}' to infrastructure_seeds"
            })
            result["summary"]["infrastructure_leak_count"] += 1


def _detect_suffix_drift(farm_keys: set, aod_keys: set, result: dict):
    """Detect prod/legacy suffix creating duplicate entities."""
    
    suffixes = ["prod", "legacy", "v1", "v2", "old", "new", "backup"]
    
    base_to_variants = {}
    
    for key in aod_keys:
        for suffix in suffixes:
            if key.endswith(suffix):
                base = key[:-len(suffix)]
                if base and len(base) > 3:
                    if base not in base_to_variants:
                        base_to_variants[base] = []
                    base_to_variants[base].append(key)
                    break
    
    for base, variants in base_to_variants.items():
        if len(variants) >= 1:
            canonical = None
            for farm_key in farm_keys:
                if farm_key == base or farm_key.startswith(base):
                    canonical = farm_key
                    break
            
            if canonical or base in farm_keys:
                result["suffix_drift"].append({
                    "base_key": base,
                    "canonical": canonical or base,
                    "variants": variants,
                    "fix": f"Normalize variants to '{canonical or base}'"
                })
                result["summary"]["suffix_drift_count"] += 1


def _generate_alias_suggestions(result: dict):
    """Generate domain alias mapping for AOD to consume."""
    
    for drift in result["key_drift"]:
        if drift["pattern"] in ("domain_vs_product", "missing_tld"):
            result["domain_alias_suggestions"][drift["aod_key"]] = drift["farm_key"]
    
    for trunc in result["truncation_patterns"]:
        result["domain_alias_suggestions"][trunc["aod_key"]] = trunc["likely_full_key"]
