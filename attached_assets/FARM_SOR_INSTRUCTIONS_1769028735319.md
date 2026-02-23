# Farm Instructions: System of Record (SOR) Expectations

This document provides guidance for Farm (the test data generator) on how to generate SOR-related test data and expectations for AOD reconciliation.

## Overview

AOD now supports **System of Record (SOR) tagging** - identifying assets that are likely authoritative data sources for specific data domains (customer, employee, financial, product, identity, IT assets).

**Key principle:** SOR is **ORTHOGONAL** to Shadow/Zombie/Governed classifications. An asset can be:
- **Governed + SOR** (ideal state - managed authoritative system)
- **Shadow + SOR-candidate** (ungoverned CRM being used as source of truth - RED FLAG!)
- **Zombie + former-SOR** (abandoned authoritative system - needs decommission plan)

---

## Snapshot Contract Extensions

### New Fields in `__expected__` Section

Farm should include SOR expectations in the `__expected__` section of snapshots:

```json
{
  "__expected__": {
    "shadow_domains": ["...", "..."],
    "zombie_domains": ["...", "..."],
    "governed_domains": ["...", "..."],
    
    "sor_high_domains": ["salesforce.com", "workday.com"],
    "sor_medium_domains": ["hubspot.com"],
    "sor_low_domains": [],
    
    "sor_domain_mapping": {
      "salesforce.com": "customer",
      "workday.com": "employee",
      "hubspot.com": "customer"
    }
  }
}
```

### New Fields on Asset Records (Optional Enrichment)

CMDB records can include SOR indicators:

```json
{
  "cmdb_records": [
    {
      "ci_id": "CI-001",
      "name": "Salesforce",
      "domain": "salesforce.com",
      "ci_type": "SaaS",
      "lifecycle": "active",
      "is_system_of_record": true,
      "data_tier": "gold",
      "data_domain": "customer",
      "description": "Customer master data system"
    }
  ]
}
```

Finance records can include contract details:

```json
{
  "finance_records": [
    {
      "vendor": "Salesforce",
      "domain": "salesforce.com",
      "annual_spend": 150000,
      "contract_type": "enterprise",
      "contract_term_years": 3
    }
  ]
}
```

---

## Signal Weights (AOD's Scoring Logic)

AOD scores SOR likelihood using these weighted signals:

| Signal | Weight | Description |
|--------|--------|-------------|
| `cmdb_authoritative` | +40 | CMDB flags: `is_system_of_record`, `data_tier=gold`, `authoritative` |
| `known_sor_vendor` | +30 | Matches known SOR vendor patterns (Salesforce, Workday, etc.) |
| `middleware_exporter` | +25 | Asset appears as data SOURCE in middleware routes |
| `enterprise_sso_scim` | +20 | Both SSO and SCIM enabled (enterprise-wide deployment) |
| `enterprise_contract` | +15 | Annual spend >= $50K or contract_type = "enterprise" |
| `high_corroboration` | +10 | Corroborated across 4+ data sources |
| `edge_app_penalty` | -20 | Niche TLD (.io, .app) + single discovery source |

**Confidence thresholds:**
- **High**: confidence >= 0.75
- **Medium**: confidence >= 0.50
- **Low**: confidence > 0 but < 0.50
- **None**: confidence = 0

---

## Known SOR Vendors by Data Domain

Farm should generate test cases using these known SOR vendor patterns:

### Customer Data
- salesforce.com, hubspot.com, dynamics.com, dynamics365.com
- zoho.com, pipedrive.com, freshworks.com, zendesk.com

### Employee Data
- workday.com, adp.com, bamboohr.com, namely.com
- paylocity.com, paychex.com, gusto.com, rippling.com
- successfactors.com, ultipro.com, dayforce.com

### Financial Data
- netsuite.com, quickbooks.com, xero.com, sage.com
- intacct.com, freshbooks.com, oracle.com, sap.com

### Product Data
- sap.com, oracle.com, epicor.com, infor.com
- dynamics.com, netsuite.com

### Identity Data
- okta.com, onelogin.com, auth0.com, ping.com
- duo.com

### IT Assets
- servicenow.com, freshservice.com, manageengine.com

---

## Test Case Categories

### Category A: Clear SOR (High Confidence)

Assets that should score HIGH SOR likelihood:

```json
{
  "test_case": "A1_salesforce_enterprise",
  "discovery": {"domain": "salesforce.com", "sources": ["network", "endpoint", "proxy"]},
  "cmdb": {"is_system_of_record": true, "data_tier": "gold"},
  "idp": {"has_sso": true, "has_scim": true},
  "finance": {"annual_spend": 150000, "contract_type": "enterprise"},
  "expected_sor": {
    "likelihood": "high",
    "domain": "customer",
    "signals": ["cmdb_authoritative", "known_sor_vendor", "enterprise_sso_scim", "enterprise_contract", "high_corroboration"]
  }
}
```

### Category B: Possible SOR (Medium Confidence)

Assets that should score MEDIUM SOR likelihood:

```json
{
  "test_case": "B1_hubspot_partial",
  "discovery": {"domain": "hubspot.com", "sources": ["network", "endpoint"]},
  "cmdb": null,
  "idp": {"has_sso": true, "has_scim": false},
  "finance": {"annual_spend": 25000},
  "expected_sor": {
    "likelihood": "medium",
    "domain": "customer",
    "signals": ["known_sor_vendor", "enterprise_sso_scim"]
  }
}
```

### Category C: Unlikely SOR (Low Confidence)

Assets that should score LOW SOR likelihood:

```json
{
  "test_case": "C1_niche_app",
  "discovery": {"domain": "randomtool.io", "sources": ["network"]},
  "cmdb": null,
  "idp": {"has_sso": false},
  "finance": {"annual_spend": 5000},
  "expected_sor": {
    "likelihood": "low",
    "domain": null,
    "signals": ["edge_app_penalty"]
  }
}
```

### Category D: Shadow + SOR Candidate (Red Flag)

Critical test case: Ungoverned asset that looks like an SOR:

```json
{
  "test_case": "D1_shadow_crm",
  "discovery": {"domain": "pipedrive.com", "sources": ["network", "endpoint"]},
  "cmdb": null,
  "idp": null,
  "finance": {"annual_spend": 30000},
  "expected_classification": "shadow",
  "expected_sor": {
    "likelihood": "medium",
    "domain": "customer",
    "signals": ["known_sor_vendor"]
  },
  "risk_note": "Shadow IT acting as customer data SOR - immediate attention required"
}
```

### Category E: Zombie + Former SOR

Assets that were SORs but are now inactive:

```json
{
  "test_case": "E1_deprecated_hcm",
  "discovery": {"domain": "ultipro.com", "sources": ["network"], "last_activity": "2025-06-01"},
  "cmdb": {"is_system_of_record": true, "lifecycle": "deprecated"},
  "idp": {"has_sso": true, "last_login": "2025-05-15"},
  "finance": {"annual_spend": 80000, "contract_end": "2025-12-31"},
  "expected_classification": "zombie",
  "expected_sor": {
    "likelihood": "high",
    "domain": "employee",
    "signals": ["cmdb_authoritative", "known_sor_vendor", "enterprise_sso_scim", "enterprise_contract"]
  },
  "risk_note": "Former employee SOR marked for decommission - ensure data migration complete"
}
```

---

## Reconciliation Endpoint

AOD exposes SOR scoring via existing reconciliation endpoints:

### Asset Catalog Response

```json
{
  "assets": [
    {
      "asset_id": "...",
      "name": "Salesforce",
      "domain": "salesforce.com",
      "provisioning_status": "active",
      "sor_tagging": {
        "likelihood": "high",
        "confidence": 0.85,
        "domain": "customer",
        "evidence": [
          "CMDB indicates authoritative status: 'system_of_record'",
          "Matches known SOR vendor: salesforce.com",
          "Both SSO and SCIM enabled (enterprise-wide deployment)",
          "Enterprise contract: $150,000/year",
          "Corroborated across 5 data sources"
        ],
        "signals_matched": ["cmdb_authoritative", "known_sor_vendor", "enterprise_sso_scim", "enterprise_contract", "high_corroboration"]
      }
    }
  ]
}
```

---

## Middleware Topology Signal

If Farm provides middleware route data, AOD can detect data exporter patterns:

```json
{
  "middleware_routes": [
    {
      "platform": "mulesoft",
      "route_id": "sf-to-warehouse",
      "source": {
        "system": "Salesforce",
        "url": "https://company.salesforce.com/services/data"
      },
      "target": {
        "system": "Snowflake",
        "url": "https://company.snowflakecomputing.com"
      }
    }
  ]
}
```

Assets appearing as `source` in multiple routes get the `middleware_exporter` signal.

---

## Implementation Notes

1. **SOR is additive** - It doesn't change existing Shadow/Zombie/Governed logic
2. **SOR is informational** - Not a blocking finding for AAM connection
3. **SOR enables prioritization** - Helps customers focus on their most critical systems first
4. **SOR + Shadow = High Priority** - An ungoverned SOR-candidate is a critical risk

---

## Questions for Farm Team

1. Should `sor_expectations` be a separate section or merged into existing `__expected__`?
2. Do you want AOD to report SOR discrepancies in reconciliation results?
3. Should middleware route data be a new snapshot section or embedded in observations?
4. What test coverage percentage should we target for SOR edge cases?

---

## Contact

For questions about SOR implementation, refer to:
- `src/aod/pipeline/sor_scoring.py` - Core scoring engine
- `config/policy_master.json` - Policy configuration (sor_scoring section)
- `src/aod/models/output_contracts.py` - SORTagging model
