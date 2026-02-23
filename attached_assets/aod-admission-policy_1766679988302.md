# AOD Discovery & Admission Policy

## Overview

AOD (AutonomOS Discover) processes raw enterprise evidence to produce a verified Asset Catalog. This document defines the complete policy for how entities are discovered, evaluated, admitted, and classified.

---

## 1. Discovery Pipeline

### 1.1 Evidence Sources (7 Data Planes)

| Plane | Description | Example Evidence |
|-------|-------------|------------------|
| **Discovery** | DNS, proxy logs, browser, network scans | Domain visits, DNS queries |
| **IdP** | Identity Provider | SSO apps, SCIM provisioning, service principals |
| **CMDB** | Configuration Management Database | CI records, system-of-record entries |
| **Cloud** | Cloud resource inventory | AWS/Azure/GCP resources, SaaS integrations |
| **Endpoint** | Device/agent data | Installed software, device telemetry |
| **Network** | Traffic/topology | Flow logs, firewall rules |
| **Finance** | Spend/billing | Contracts, invoices, recurring transactions |

### 1.2 Pipeline Stages

1. **ValidateSnapshot** - Schema validation, reject banned fields
2. **NormalizeObservations** - Normalize names/domains, derive candidate entities
3. **BuildPlaneIndexes** - Create indexes for IdP/CMDB/Cloud/Finance correlation
4. **CorrelateEntitiesToPlanes** - Multi-pass matching (domain → name → vendor)
5. **ArtifactHandling** - Filter out non-system objects (dashboards, reports)
6. **Admission** - Apply admission criteria (this document)
7. **FindingsEngine** - Generate explainable findings

---

## 2. Admission Policy

### 2.1 Admission Gates

An entity is **ADMITTED** as an asset if it satisfies **at least one** of these 5 gates:

| Gate | Criteria | Purpose |
|------|----------|---------|
| **IdP Gate** | IdP match AND (has_sso OR has_scim OR idp_type = service_principal) | Identity-governed apps |
| **CMDB Gate** | CMDB match AND ci_type ∈ {app, service, database, infra} AND lifecycle ∈ {prod, staging} | System-of-record assets |
| **Cloud Gate** | Cloud match AND resource_type indicates real system/resource | Cloud-provisioned resources |
| **Finance Gate** | Finance match AND (contract exists OR recurring vendor spend ≥$200/mo) | Financially-backed services |
| **Discovery Gate** | ≥2 distinct discovery sources AND activity within 90 days | Shadow IT candidates |

### 2.2 Rejection Rules

An entity is **REJECTED** if:

| Rule | Condition | Rationale |
|------|-----------|-----------|
| **Corporate Root Domain** | Domain is vendor's main website (google.com, hubspot.com, servicenow.com, etc.) | Not an enterprise instance |
| **No Gates Satisfied** | Fails all 5 admission gates above | Insufficient evidence for asset status |

### 2.3 Key Invariants

1. **Vendor governance alone NEVER causes admission** - Vendor match is metadata only, not an admission criterion
2. **Corporate/marketing root domains are ALWAYS rejected** - Regardless of other evidence
3. **vendor_hypothesis is NON-DECISIONABLE** - Used for display only, never for admission/classification logic
4. **Deterministic output** - Identical inputs always produce identical outputs

---

## 3. Cataloged Assets

### 3.1 Terminology

| Term | Definition |
|------|------------|
| **Admitted** | Entity passed admission criteria |
| **Cataloged** | Same as admitted (user-facing term) |
| **Rejected** | Entity failed admission criteria |

### 3.2 Asset Record Structure

Each cataloged asset includes:

| Field | Description |
|-------|-------------|
| `asset_id` | Deterministic UUID |
| `name` | Canonical name |
| `asset_type` | app, service, database, infra, unknown |
| `environment` | prod, staging, dev, unknown |
| `lens_status` | Match status per plane (matched/unmatched/ambiguous) |
| `lens_coverage` | Boolean flags for each plane's presence |
| `evidence_refs` | Links to source observations |
| `admission_reason` | Explanation of which gate(s) passed |
| `tags` | Derived classifications (discovery_only, etc.) |

---

## 4. Derived Classifications

After admission, assets receive derived classifications for risk analysis.

### 4.1 Shadow Asset

**Definition:** Discovered, active, but ungoverned

**Criteria:**
- Admitted via Discovery gate (not IdP/CMDB)
- No IdP match AND no CMDB match
- Activity within 90 days
- NOT infrastructure domain (in sprawl mode)

### 4.2 Zombie Asset

**Definition:** Governed but inactive

**Criteria:**
- Has IdP OR CMDB presence
- No activity in 90+ days (stale)
- NOT infrastructure domain (in sprawl mode)

### 4.3 Infrastructure Exclusion

Infrastructure domains are **excluded from shadow/zombie classification** in "sprawl" mode:

```
redis.io, postgresql.org, docker.com, kubernetes.io, nginx.org,
elasticsearch.org, mongodb.com, kafka.apache.org, grafana.com,
jenkins.io, terraform.io, hashicorp.com, prometheus.io, etc.
```

**Reconciliation Modes:**

| Mode | Behavior |
|------|----------|
| **Sprawl** (default) | External SaaS only - infrastructure excluded |
| **Infra** | All assets eligible - includes internal systems |

---

## 5. Findings Generation

Cataloged assets are evaluated for findings:

### 5.1 Security Risks (Actionable)

| Finding | Trigger | Priority Factors |
|---------|---------|------------------|
| **IDENTITY_GAP** | Admitted via CMDB/Cloud/Finance but no IdP match; requires strong activity (cloud/finance/multi-plane) | Confidence + Materiality |
| **FINANCE_GAP** | Finance evidence ≥$200/mo recurring but no corresponding governed asset | Spend threshold |
| **DATA_CONFLICT** | Plane evidence contradicts on security-relevant fields (owner, environment, data_classification) | Field criticality |

### 5.2 Governance (Hygiene)

| Finding | Trigger |
|---------|---------|
| **CMDB_GAP** | Admitted via IdP/Finance but no CMDB match |
| **GOVERNANCE_GAP** | No owner or system record |
| **DUPLICATION_RISK** | Multiple entities ambiguous-match same plane record |

---

## 6. Example Traces

### 6.1 Admitted: Discovery-Only (Shadow Candidate)

```
Entity: Plugin598
Observations: 2 (dns, proxy)
Correlation: IdP=unmatched, CMDB=unmatched, Cloud=unmatched, Finance=unmatched
Admission: PASS via Discovery Gate (5 sources, recent activity)
Result: Cataloged with tags=["discovery_only"]
Classification: Shadow candidate
```

### 6.2 Rejected: Corporate Root Domain

```
Entity: HubSpot
Domain: hubspot.com
Admission: FAIL - Corporate root domain
Result: Rejected
Reason: "Corporate root domain: hubspot.com"
```

### 6.3 Admitted: Multi-Gate

```
Entity: Salesforce
Correlation: IdP=matched (SSO), CMDB=matched (app, prod), Finance=matched (contract)
Admission: PASS via IdP Gate + CMDB Gate + Finance Gate
Result: Cataloged with full governance
Classification: Clean (governed, active)
```

---

## 7. Policy Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    ADMISSION DECISION TREE                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Is domain a corporate root domain?                         │
│    YES → REJECT                                              │
│    NO  ↓                                                     │
│                                                              │
│  Does entity satisfy ANY gate?                               │
│    • IdP: match + (SSO|SCIM|service_principal)              │
│    • CMDB: match + valid ci_type + valid lifecycle          │
│    • Cloud: match + real resource_type                      │
│    • Finance: match + (contract|recurring spend ≥$200)      │
│    • Discovery: ≥2 sources + activity ≤90 days              │
│                                                              │
│    YES → ADMIT (Cataloged)                                   │
│    NO  → REJECT                                              │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                  POST-ADMISSION CLASSIFICATION               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Is infrastructure domain? (sprawl mode)                     │
│    YES → Exclude from shadow/zombie                          │
│    NO  ↓                                                     │
│                                                              │
│  Has IdP OR CMDB?                                            │
│    NO  + Active    → SHADOW                                  │
│    YES + Inactive  → ZOMBIE                                  │
│    YES + Active    → CLEAN                                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```
