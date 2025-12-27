# AOD Iron Dome Fix: Unified Admission Gate

> **Plain English:** This document describes a bug where AOD was incorrectly cataloging internal server names (like "cdn674" or "billing-api") as if they were real SaaS applications. The fix is simple: before admitting anything as an asset, check if it has a valid internet domain suffix (like .com or .io). If it doesn't, reject it. This "Iron Dome" filter blocks junk from polluting the asset catalog.

## Executive Summary

Farm validation run `de663d31abe9` (TechHub-HPC9, adversarial profile) reveals:

| Issue | Count | Impact |
|-------|-------|--------|
| Admission FPs | 456 | Internal hostnames admitted as assets |
| Rejected Missed | 287 | Same root cause as FPs |
| Classification Missed | 6 | High-value FQDNs not classified |

**Root Cause**: AOD lacks unified TLD validation at admission gate.

---

## Problem: Non-FQDN Keys Leaking Through

### Sample FP Keys (AOD admits, Farm rejects)

```
fonts214, cdn674, beacon365, email-sender (legacy)
lb464, img189, edge143, bugsnag300, iframe591
edge325, identity231, assets606, analytics313
crash220, edge408, crash201, iframe128, fonts556
billing-api (legacy), analytics30
```

These are internal hostnames without valid TLD suffixes. They should never be cataloged as assets.

---

## Fix: Unified Admission Function

### 1. Create `validate_key_integrity(key)`

```python
import tldextract

def validate_key_integrity(key: str) -> tuple[bool, str]:
    """
    Unified admission gate. Run on ALL keys before cataloging.
    
    Returns (is_valid, rejection_reason)
    """
    if not key or not key.strip():
        return False, "Empty key"
    
    key = key.strip().lower()
    
    # Check for invalid characters
    if ' ' in key or '\t' in key:
        return False, "Contains whitespace"
    
    # TLD validation using Public Suffix List
    ext = tldextract.extract(key)
    if not ext.suffix:
        return False, f"No valid TLD suffix: {key}"
    
    if not ext.domain:
        return False, f"No domain component: {key}"
    
    return True, None
```

### 2. Apply to ALL Ingestion Pipelines

**Discovery Pipeline:**
```python
def ingest_discovery_observation(obs):
    key = normalize_key(obs)
    valid, reason = validate_key_integrity(key)
    if not valid:
        log.debug(f"Rejected discovery key: {key} - {reason}")
        return None
    return create_asset(key, obs)
```

**CMDB Pipeline:**
```python
def sync_cmdb_ci(ci):
    key = resolve_domain(ci) or normalize_name(ci.name)
    valid, reason = validate_key_integrity(key)
    if not valid:
        log.debug(f"Rejected CMDB key: {key} - {reason}")
        return None
    return upsert_asset(key, ci)
```

**Finance Pipeline:**
```python
def process_finance_contract(contract):
    key = resolve_vendor_domain(contract.vendor_name)
    if not key:
        log.debug(f"No domain for vendor: {contract.vendor_name}")
        return None
    valid, reason = validate_key_integrity(key)
    if not valid:
        log.debug(f"Rejected finance key: {key} - {reason}")
        return None
    return link_finance(key, contract)
```

**Endpoint Pipeline:**
```python
def ingest_endpoint_telemetry(event):
    key = extract_domain(event.url or event.hostname)
    valid, reason = validate_key_integrity(key)
    if not valid:
        return None  # Silent drop for telemetry
    return update_asset_activity(key, event)
```

---

## Vendor Name Resolution

High-value vendors like "Okta", "Workday" need domain mapping:

```python
VENDOR_DOMAIN_MAP = {
    "okta": "okta.com",
    "workday": "workday.com",
    "zendesk": "zendesk.com",
    "splunk": "splunk.com",
    "pagerduty": "pagerduty.com",
    "salesforce": "salesforce.com",
    "servicenow": "servicenow.com",
    # ... extend as needed
}

def resolve_vendor_domain(vendor_name: str) -> str | None:
    """Map vendor name to canonical domain."""
    normalized = normalize_name(vendor_name)  # "Okta-prod" -> "oktaprod"
    
    for key, domain in VENDOR_DOMAIN_MAP.items():
        if key in normalized or normalized in key:
            return domain
    
    return None
```

---

## Classification Gap: 6 Missed FQDNs

These domains have strong discovery evidence but AOD never classified them:

| Domain | Observations | Sources | Expected Class |
|--------|-------------|---------|----------------|
| `okta.com` | 60 | 7 | shadow |
| `zendesk.com` | 80 | 7 | shadow |
| `workday.com` | 60 | 7 | shadow |
| `splunk.com` | 80 | 7 | shadow |
| `pagerduty.com` | 80 | 7 | shadow |
| `techhub-hpc9.com` | 341 | 2 | shadow |

**Investigation**: Check if these domains are being ingested but not classified, or not ingested at all.

---

## Validation Criteria

After implementing Iron Dome:

1. **Zero non-FQDN keys in catalog**
   ```sql
   SELECT asset_key FROM assets 
   WHERE asset_key NOT LIKE '%.%' 
      OR asset_key ~ '^[0-9]+$';
   -- Should return 0 rows
   ```

2. **All admitted keys have valid TLD**
   ```python
   for asset in get_all_assets():
       ext = tldextract.extract(asset.key)
       assert ext.suffix, f"Invalid: {asset.key}"
   ```

3. **Re-run Farm validation**
   - Admission FPs should drop from 456 to < 10
   - Rejected Missed should drop from 287 to < 10

---

## Timeline

| Phase | Action | Expected Outcome |
|-------|--------|------------------|
| 1 | Add `validate_key_integrity()` | Central gate function |
| 2 | Apply to Discovery pipeline | Biggest impact |
| 3 | Apply to CMDB/Finance/Endpoint | Complete coverage |
| 4 | Add vendor name resolution | Fix 6 missed classifications |
| 5 | Re-run Farm validation | Confirm < 10 FPs |

---

## Contact

Farm validation questions: See `docs/ADMISSION_POLICY.md`
