# AOS Farm: Discovery, Admission & Classification Policy

> **Plain English:** This document explains the three-step pipeline for identifying IT assets. Step 1 (Discovery): Collect raw signals from network logs, SSO, cloud inventories, etc. Step 2 (Admission): Filter out noise—only assets seen by 2+ sources or confirmed in IdP/CMDB make the cut. Step 3 (Classification): Label admitted assets as Shadow (unmanaged), Zombie (abandoned), or Clean (properly governed). Think of it as: Find → Filter → Label.

This document defines the complete policy for how AOS Farm determines which assets are cataloged and how they are classified.

---

## Overview

The pipeline has three stages:

```
Discovery → Admission Gate → Classification
   ↓              ↓                ↓
Raw signals   Catalog/Reject   Shadow/Zombie/Clean
```

---

## Stage 1: Discovery

### What is Discovery?
Discovery collects raw signals about assets from multiple sources. An asset must be "discovered" before it can be evaluated.

### Discovery Sources
- **DNS** - Domain resolution records
- **Proxy/Firewall logs** - Network traffic observations
- **SSO/OAuth flows** - Authentication integrations
- **Cloud inventory** - AWS, Azure, GCP resources
- **Browser extensions** - Client-side SaaS detection
- **API integrations** - Connected service catalogs

### Discovery Strength
`discovery_sources_count` = number of distinct sources that observed this asset.

| Count | Strength | Example |
|-------|----------|---------|
| 0 | None | No observations |
| 1 | Weak | DNS-only |
| 2+ | Strong | Proxy + SSO |

---

## Stage 2: Admission Gate

### Purpose
Filter noise and ensure only "real" assets enter the catalog. This prevents false classifications.

### Admission Criteria
An asset is **ADMITTED** if ANY of these are true:

| Criterion | Description |
|-----------|-------------|
| `discovery_sources_count >= 2` | Corroborated by multiple sources |
| `cloud_present = true` | Has cloud plane evidence |
| `idp_present = true` | Found in Identity Provider (direct or via vendor) |
| `cmdb_present = true` | Found in CMDB (direct or via vendor) |

### Rejection Reasons
If no admission criteria are met, the asset is **REJECTED** with a reason:

| Reason | Condition |
|--------|-----------|
| `DNS-only` | Single source = DNS |
| `Single source (X)` | Single source = X |
| `No discovery sources` | Zero discovery sources |
| `No admission criteria satisfied` | Other cases |

### Infrastructure Exclusions
The following infrastructure/OSS domains are **always excluded** from classification (even if admitted):

```
postgresql.org, mysql.com, apache.org, redis.io, redis.com,
mongodb.com, elastic.co, elasticsearch.com, kafka.apache.org,
nginx.org, docker.com, kubernetes.io, linux.org, gnu.org,
python.org, nodejs.org, golang.org, rust-lang.org, ruby-lang.org
```

These are tools, not SaaS applications.

---

## Stage 3: Classification

### Applies To
Only **admitted** assets are classified. Rejected assets have no classification.

### Governance Trinity Framework

Classification is based on the **Governance Trinity** - three criteria that ALL must be present for an asset to be considered "Governed":

| Criterion | Definition | Evidence Source |
|-----------|------------|-----------------|
| **Visibility** | Asset is documented in system of record | CMDB presence |
| **Validation** | Asset has security attestation | Security plane evidence |
| **Control** | Asset has identity governance | IdP presence |

**Trinity Logic:**
- **PASS** = Has ALL THREE (Visibility + Validation + Control)
- **FAIL** = Missing ANY of the three

### Classification Matrix

| Classification | Criteria |
|----------------|----------|
| **Shadow** | Trinity FAIL + Active (missing Visibility, Validation, OR Control) |
| **Zombie** | Trinity PASS + Stale Activity (governed but abandoned) |
| **Clean** | Trinity PASS + Active (fully governed and in use) |

### Key Definitions

**is_external**: Domain has a public TLD (.com, .io, .org, etc.)

**is_active / RECENT_ACTIVITY**: Activity observed within the window (default 90 days). Activity sources:
- Authentication events
- Network traffic
- API calls
- User sessions

**STALE_ACTIVITY**: No activity within the staleness window (default 90 days).

**idp_present (Control)**: Found in Identity Provider. Can be:
- Direct match on this domain
- Vendor propagation (e.g., `teams.microsoft.com` inherits from `microsoft.com`)

**cmdb_present (Visibility)**: Found in Configuration Management Database. Same propagation rules as IdP.

**security_attestation (Validation)**: Has security review, compliance check, or attestation evidence.

**is_infra_excluded**: Domain is in `INFRASTRUCTURE_DOMAINS` set.

### Classification Examples

| Asset | CMDB | IdP | Security | Activity | Trinity | Classification |
|-------|------|-----|----------|----------|---------|----------------|
| slack.com | NO | YES | NO | Active | FAIL | **Shadow** |
| okta.com | YES | YES | YES | Active | PASS | **Clean** |
| oldapp.com | YES | YES | YES | Stale | PASS | **Zombie** |
| notion.so | NO | NO | NO | Active | FAIL | **Shadow** |

---

## Vendor Governance Propagation

When a vendor's root domain is governed (has IdP or CMDB), governance propagates to all related domains:

```python
VENDOR_DOMAIN_SETS = {
    'microsoft': {'microsoft.com', 'office.com', 'sharepoint.com', 'teams.microsoft.com', 'github.com', ...},
    'google': {'google.com', 'gmail.com', 'youtube.com', ...},
    'salesforce': {'salesforce.com', 'slack.com', 'heroku.com', 'tableau.com', ...},
    ...
}
```

Example: If `microsoft.com` is in IdP, then `teams.microsoft.com` is also considered governed.

---

## Reconciliation Modes

| Mode | Scope | Use Case |
|------|-------|----------|
| `sprawl` (default) | External SaaS domains only | Shadow IT detection |
| `infra` | Internal services only | Infrastructure monitoring |
| `all` | Everything | Full reconciliation |

Mode filters which assets are included in `shadow_expected`, `zombie_expected`, `clean_expected`.

---

## Invariants

These rules must ALWAYS hold:

1. **Mutual exclusivity**: Every admitted asset is exactly ONE of: shadow, zombie, clean
2. **Exhaustive**: `admitted_count = shadow_count + zombie_count + clean_count`
3. **Partition**: `unique_assets = admitted_count + rejected_count`
4. **No classification for rejected**: Rejected assets have no shadow/zombie/clean label
5. **Shadow ≠ Zombie**: An asset cannot be both
6. **Admission before classification**: Classification only runs on admitted assets
7. **Infrastructure always excluded**: `INFRASTRUCTURE_DOMAINS` members never become shadow/zombie

---

## Decision Trace

Every asset has a `decision_trace` recording:

```json
{
  "asset_key_used": "slack.com",
  "is_external": true,
  "is_active": true,
  "discovery_sources_count": 3,
  "discovery_sources_list": ["proxy", "sso", "browser"],
  "idp_present": false,
  "idp_present_direct": false,
  "cmdb_present": false,
  "cmdb_present_direct": false,
  "vendor_governance": null,
  "infra_excluded": false,
  "admitted": true,
  "rejection_reason": null,
  "is_shadow": true,
  "reason_codes": ["HAS_DISCOVERY", "IS_ACTIVE", "NO_IDP", "NO_CMDB"]
}
```

This provides full auditability for why each asset was classified.

---

## Reason Codes

### Evidence Codes
| Code | Meaning |
|------|---------|
| `HAS_DISCOVERY` | Discovered by at least one source |
| `HAS_IDP` / `NO_IDP` | Found / Not found in Identity Provider |
| `HAS_CMDB` / `NO_CMDB` | Found / Not found in CMDB |
| `HAS_CLOUD` | Has cloud plane evidence |
| `HAS_FINANCE` / `HAS_ONGOING_FINANCE` | Has finance/recurring spend records |
| `HAS_ENDPOINT` | Detected on endpoints |
| `HAS_NETWORK` | Seen in network traffic |
| `GOVERNED_VIA_VENDOR` | Governance inherited from vendor parent |

### Governance Trinity Codes
| Code | Meaning |
|------|---------|
| `HAS_VISIBILITY` / `MISSING_VISIBILITY` | CMDB presence (Visibility criterion) |
| `HAS_VALIDATION` / `MISSING_VALIDATION` | Security attestation (Validation criterion) |
| `HAS_CONTROL` / `MISSING_CONTROL` | IdP presence (Control criterion) |
| `HAS_SECURITY_ATTESTATION` / `NO_SECURITY_ATTESTATION` | Security plane evidence |
| `GOVERNANCE_TRINITY_PASS` | All three criteria met |
| `GOVERNANCE_TRINITY_FAIL` | Missing at least one criterion |

### Activity Codes
| Code | Meaning |
|------|---------|
| `RECENT_ACTIVITY` | Activity within window (default 90 days) |
| `STALE_ACTIVITY` | No activity within staleness window |

### Classification Codes
| Code | Meaning |
|------|---------|
| `SHADOW_CLASSIFICATION` | Asset classified as Shadow IT |
| `ZOMBIE_CLASSIFICATION` | Asset classified as Zombie |
| `CLEAN_CLASSIFICATION` | Asset classified as Clean/Governed |

---

## Summary Flow

```
Asset Key
    ↓
[Discovery Sources Count]
    ↓
[Check Admission Criteria]
    ↓
┌─────────────────────────────────┐
│ discovery >= 2?                 │
│ OR cloud_present?               │──→ YES → ADMITTED
│ OR idp_present?                 │
│ OR cmdb_present?                │
└─────────────────────────────────┘
    ↓ NO
REJECTED (with reason)
    
IF ADMITTED:
    ↓
[Check Governance Trinity]
    ↓
┌─────────────────────────────────────────────────┐
│ Has Visibility (CMDB)?           □              │
│ Has Validation (Security)?       □              │
│ Has Control (IdP)?               □              │
│                                                 │
│ ALL THREE = Trinity PASS                        │
│ MISSING ANY = Trinity FAIL                      │
└─────────────────────────────────────────────────┘
    ↓
[Check Activity + Apply Classification]
    ↓
┌─────────────────────────────────────────────────┐
│ Trinity FAIL + Active?           → SHADOW       │
│ Trinity PASS + Stale?            → ZOMBIE       │
│ Trinity PASS + Active?           → CLEAN        │
└─────────────────────────────────────────────────┘
```
