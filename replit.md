# AOS Farm

Synthetic Enterprise Data Generator for AutonomOS AOD (Discover) module.

## Overview

AOS Farm generates IRL-plausible enterprise source-of-truth planes and raw observation streams for testing AOD. Farm outputs raw evidence streams, not conclusions.

## Non-negotiables

Guardrail: No “green-test theater” (Stop optimizing for “done without errors”) The anti-pattern we are eliminating

Agents frequently “solve” problems by making the system look clean:

“All tests pass” while the feature fails the first real eval

Papering over contract mismatches by making schemas permissive

Converting upstream errors into “not found” or “empty” results

Overwriting history / collapsing identity scopes to avoid collisions

Adding hidden shortcuts / labels / join keys that make synthetic demos work but are IRL-invalid

This is forbidden. The goal is not “no errors”; the goal is “correct semantics.”

Definition of “DONE” (must satisfy all)

A change is DONE only if it meets all 4:

Semantics preserved

The behavior matches the stated IRL meaning of the feature (not just the test).

If semantics changed, it must be explicitly called out as a breaking change.

No cheating

No overwrites to silence conflicts

No “optional everything” to dodge validation

No silent fallbacks that hide upstream failure

No ground-truth labels or shared join keys that wouldn’t exist IRL

Proof is real

Tests are not proof by themselves.

Provide one of:

a minimal reproduction showing failure-before / success-after, OR

a “before/after” output diff from a real run (Farm → AOD → UI) that demonstrates the user-visible behavior.

Negative test included

Add at least one test that ensures the cheat can’t come back:

upstream returns HTML/empty → must become UPSTREAM_ERROR (not “no evidence”)

missing required fields → INVALID_INPUT_CONTRACT (not silently defaulted)

re-run same snapshot twice → history preserved (no overwrite)

Required “Fix Proposal Format” (agents must follow)

For any fix, output exactly:

What broke (1–2 sentences)

Why it broke (root cause)

What I changed (one paragraph, no code dump)

Why this is IRL-correct (1–3 bullets)

What would have been the tempting cheat (1 bullet) and why we did not do it

How I proved it (tests + one real run / fixture)

If the agent cannot provide this format, it must stop and say “I can’t prove it yet.”

“All tests pass” is not an acceptable claim by itself

If the agent says “57 tests pass,” it must also include:

which test(s) prove the user-visible behavior

and at least one eval (a run output / API response / UI behavior) demonstrating success

If it can’t show that, “tests pass” is treated as noise.

Fail loudly on reality violations

When data is bad or missing, do NOT “handle” it by pretending it’s fine. Use explicit error statuses (e.g., UPSTREAM_ERROR, INVALID_SNAPSHOT, INVALID_INPUT_CONTRACT) and surface the reason.

Default strategy when unsure

Prefer:

Adapters (normalize into canonical contract) over weakening contracts

Run-scoped identities over overwrites

Explicit errors over silent fallbacks

Evidence-only derivations over labels

## Prompt shortcuts
**DCCE or dcce** - don't change code, explain

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

data/                    # SQLite database (farm.db)
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
- `POST /api/snapshots` - Generate snapshot, returns:
  - `snapshot_id`: Unique run ID (UUID, always unique per generation)
  - `snapshot_fingerprint`: Deterministic hash from params (same seed = same fingerprint)
  - `duplicate_of_snapshot_id`: If fingerprint exists, references first snapshot with that fingerprint
  - `tenant_id`, `created_at`, `schema_version`
- `GET /api/snapshots/{snapshot_id}` - Get full snapshot JSON
- `GET /api/snapshots?tenant_id=...&limit=...` - List snapshot metadata only (no blob)

### AOD Status API
- `GET /api/aod/run-status?snapshot_id=...&tenant_id=...` - Check if AOD has processed a snapshot
  - Returns: `{status: "PROCESSED", run_id: "..."}` if AOD has a run for this snapshot
  - Returns: `{status: "NOT_PROCESSED"}` if no matching run (AOD 404 or 200 without run_id)
  - Returns: `{status: "AOD_ERROR", message: "..."}` if AOD unreachable/misconfigured

### Reconciliation API (for AOD comparison)
- `POST /api/reconcile` - Compare AOD results against Farm expectations (manual)
  - Request: `{snapshot_id, aod_run_id, tenant_id, aod_summary: {assets_admitted, findings, zombies, shadows}, aod_lists: {zombie_assets, shadow_assets, top_findings}}`
  - Response: `{reconciliation_id, status: PASS/WARN/FAIL, report_text, farm_expectations}`
- `POST /api/reconcile/auto` - One-click auto-reconciliation (fetches from AOD)
  - Request: `{snapshot_id, tenant_id}`
  - Requires: `AOD_URL` env var (optional: `AOD_SHARED_SECRET`)
  - Behavior: Fetches latest AOD run, gets reconcile payload, creates reconciliation
  - Response: `{reconciliation_id, snapshot_id, tenant_id, aod_run_id, status, report_text}`
  - Errors: 400 (not configured), 404 (no AOD run), 502 (AOD unreachable)
- `GET /api/reconcile?snapshot_id=...` - List reconciliation metadata
- `GET /api/reconcile/{id}` - Get full reconciliation report

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
- Deterministic generation by seed (when `snapshot_time` is fixed)
- Timestamps anchor to snapshot creation time (not fixed dates)
- Activity timestamps follow realistic recency distribution:
  - 60% within last 7 days (active)
  - 25% within 8-30 days (recent)
  - 10% within 31-90 days (stale)
  - 5% within 91-365 days (zombie candidates)
