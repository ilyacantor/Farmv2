# AOD Fabric Routing Integration Handoff

**Date:** 2026-02-04
**From:** Farm Team
**To:** AOD Team
**Status:** Farm-side complete, awaiting AOD integration

---

## Summary

Farm now generates `integrates_via` and `fabric_vendor` fields on CMDB Configuration Items, linking enterprise SaaS assets to their integration fabric plane. AOD does not currently parse these fields.

AOD already has a `connected_via_plane` concept computed from evidence collection. The new CMDB fields provide **ground truth** for that detection — enabling Farm to grade whether AOD correctly identifies fabric plane relationships.

---

## What Farm Provides

### New CMDB Fields

Every `CMDBConfigItem` in the CMDB plane now includes:

```json
{
  "ci_id": "CI482910",
  "name": "Salesforce",
  "ci_type": "app",
  "lifecycle": "prod",
  "vendor": "Salesforce",
  "canonical_domain": "salesforce.com",
  "is_system_of_record": true,
  "data_tier": "gold",
  "data_domain": "crm",
  "integrates_via": "ipaas",
  "fabric_vendor": "mulesoft"
}
```

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `integrates_via` | `string \| null` | `ipaas`, `api_gateway`, `event_bus`, `data_warehouse` | Which fabric plane this CI routes through |
| `fabric_vendor` | `string \| null` | See vendor list below | Specific vendor providing the integration |

### Coverage Rates

| CI Category | Routing Probability | Typical Result |
|-------------|-------------------|----------------|
| System of Record apps | 95% | Nearly all SOR apps are routed |
| Standard SaaS apps | 75% | Most apps are routed |
| Internal services | 70% | Most services are routed |
| Datastores | 80% | Most datastores are routed |
| Zombie apps | 60% (SOR zombies only) | Legacy integrations may still exist |
| Zombie services | 0% | No routing (decommissioned) |

**Overall:** 60-85% of CMDB CIs will have `integrates_via` set. The remainder have `null` (not integrated through fabric).

### Routing Rules

Which fabric plane a CI routes through is determined by the app's category:

| Fabric Plane | App Categories |
|--------------|---------------|
| `ipaas` | CRM, ERP, HRIS, Finance, HR, Accounting, Marketing (e.g., Salesforce, Workday, NetSuite, SAP) |
| `api_gateway` | Developer, API, DevOps, Monitoring (e.g., GitHub, GitLab, Jenkins, Datadog) |
| `event_bus` | Analytics, Data, BI, Streaming (e.g., Kafka, Segment, Amplitude) |
| `data_warehouse` | Database, Storage, Datastore (e.g., Snowflake, Tableau, Looker, Power BI) |
| `ipaas` (default) | Unmatched apps default to 75% iPaaS / 25% API Gateway |

### Possible Vendor Values

**iPaaS:**
`workato`, `mulesoft`, `tray.io`, `boomi`, `celigo`, `sap_integration_suite`

**API Gateway:**
`kong`, `apigee`, `aws_api_gateway`, `azure_api_management`

**Event Bus:**
`kafka`, `confluent`, `eventbridge`, `azure_event_hubs`, `rabbitmq`, `pulsar`

**Data Warehouse:**
`snowflake`, `bigquery`, `redshift`, `databricks`, `synapse`

Vendor selection is weighted by industry profile (Finance favors MuleSoft, Tech/SaaS favors Workato, etc.).

---

## What AOD Needs To Do

### 1. Add fields to CMDB_CI_MAPPING in `farm_adapter.py`

```python
CMDB_CI_MAPPING = {
    "ci_id": "ci_id",
    "name": "name",
    "ci_type": "ci_type",
    "lifecycle": "lifecycle",
    "environment": "lifecycle",
    "owner": "owner",
    "vendor": "vendor",
    "domain": "canonical_domain",
    # NEW: Fabric routing fields
    "integrates_via": "integrates_via",
    "fabric_vendor": "fabric_vendor",
}
```

### 2. Add fields to CMDBConfigItem model in `input_contracts.py`

```python
class CMDBConfigItem(BaseModel):
    ci_id: str
    name: str
    ci_type: str
    lifecycle: Optional[str] = None
    owner: Optional[str] = None
    vendor: Optional[str] = None
    domain: Optional[str] = None
    # NEW: Fabric routing from Farm ground truth
    integrates_via: Optional[str] = None   # "ipaas", "api_gateway", "event_bus", "data_warehouse"
    fabric_vendor: Optional[str] = None    # Specific vendor name
```

### 3. Use in evidence collection / fabric plane detection

Option A — **Direct assertion:** If `integrates_via` is set, treat it as ground truth for fabric plane assignment. Compare against AOD's own `connected_via_plane` detection.

Option B — **Validation only:** Keep AOD's existing detection logic, but use `integrates_via` as the answer key when grading discovery accuracy.

---

## Verification

Generate a new snapshot via Farm and inspect the CMDB data:

```bash
curl -s "http://<farm-host>/api/snapshots" -X POST \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"ROUTING-TEST","seed":12345,"scale":"small","enterprise_profile":"modern_saas","realism_profile":"typical"}'
```

Then check the CMDB plane:

```bash
curl -s "http://<farm-host>/api/snapshots/<snapshot_id>" | \
  python3 -c "
import json, sys
snap = json.load(sys.stdin)
cis = snap['planes']['cmdb']['cis']
routed = [c for c in cis if c.get('integrates_via')]
print(f'Routed: {len(routed)}/{len(cis)} ({100*len(routed)/len(cis):.0f}%)')
for c in routed[:5]:
    print(f'  {c[\"name\"]:25} -> {c[\"integrates_via\"]} via {c[\"fabric_vendor\"]}')
"
```

Expected output:
```
Routed: 25/32 (78%)
  Workday                   -> ipaas via workato
  Notion                    -> ipaas via workato
  Okta                      -> api_gateway via kong
  DocuSign                  -> ipaas via workato
  GitHub                    -> ipaas via workato
```

---

## Important Notes

1. **Snapshots generated before 2026-02-04** do not have these fields. Only new snapshots include fabric routing.

2. **Deterministic:** Same seed + scale + enterprise_profile always produces identical routing assignments.

3. **Network traffic correlation:** Farm also generates 100-200+ fabric-related proxy records (e.g., `https://api.workato.com/v1/data`) that correlate with the CMDB routing assignments. AOD's existing network evidence collection should already be picking these up.

4. **Null means not integrated:** A CI with `integrates_via: null` is intentionally unrouted — it represents an app that bypasses the integration fabric (direct API calls, manual processes, etc.).
