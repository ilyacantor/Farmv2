# AOS Farm

Synthetic Enterprise Data Generator for AutonomOS AOD (Discover) module.

## Overview

AOS Farm generates IRL-plausible enterprise source-of-truth planes and raw observation streams for testing AOD. Farm outputs raw evidence streams, not conclusions.

## Project Structure

```
src/
├── main.py              # FastAPI entry point
├── api/
│   └── routes.py        # API endpoints
├── models/
│   └── planes.py        # Pydantic models for all data planes
└── generators/
    └── enterprise.py    # Deterministic data generators

templates/
└── index.html           # Farm Console UI

tests/
└── test_farm.py         # Test suite

data/                    # Generated snapshots (gitignored)
```

## Data Planes

Farm generates 7 independent planes:

1. **Discovery** - Raw observations from scanners/logs
2. **IdP** - Okta/Entra-like identity view
3. **CMDB** - ServiceNow-like IT inventory
4. **Cloud** - AWS/Azure/GCP resources
5. **Endpoint** - Devices and installed apps
6. **Network** - DNS, proxy logs, certificates
7. **Finance** - Vendors, contracts, transactions

## API Endpoints

### New Snapshot API (for AOD)
- `POST /api/snapshots` - Generate snapshot, returns `{snapshot_id, tenant_id, created_at, schema_version}`
- `GET /api/snapshots/{snapshot_id}` - Get full snapshot JSON
- `GET /api/snapshots?tenant_id=...&limit=...` - List snapshot metadata only (no blob)

### Reconciliation API (for AOD comparison)
- `POST /api/reconcile` - Compare AOD results against Farm expectations
  - Request: `{snapshot_id, aod_run_id, tenant_id, aod_summary: {assets_admitted, findings, zombies, shadows}, aod_lists: {zombie_assets, shadow_assets, top_findings}}`
  - Response: `{reconciliation_id, status: PASS/WARN/FAIL, report_text, farm_expectations}`
- `GET /api/reconcile?snapshot_id=...` - List reconciliation metadata
- `GET /api/reconcile/{id}` - Get full reconciliation report

### Legacy API
- `POST /api/snapshot` - Generate a new enterprise snapshot (full response)
- `GET /api/runs` - List all run history
- `GET /api/runs/{run_id}` - Download specific snapshot

## Schema Version

All snapshots include `meta.schema_version = "farm.v1"`

## Configuration

- **Scale**: small, medium, large, enterprise
- **Enterprise Profile**: modern_saas, regulated_finance, healthcare_provider, global_manufacturing
- **Realism Profile**: clean, typical, messy

## Running

```bash
python -m uvicorn src.main:app --host 0.0.0.0 --port 5000 --reload
```

## Testing

```bash
pytest tests/ -v
```

## Design Principles

- All planes are independent (different IDs, coverage, naming)
- Correlation only via realistic keys (names, domains, hostnames)
- No "conclusions" fields (no shadow flags, labels, or verdicts)
- Deterministic generation by seed
