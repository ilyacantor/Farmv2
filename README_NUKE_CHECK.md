# Nuke Prevention Check

A fast, repeatable validation script that ensures Farm is working correctly and not outputting any "cheat" fields that would give AOD unfair hints.

## How to Run

```bash
python scripts/nuke_check.py
```

**Prerequisites:**
- Farm server must be running on `localhost:5000`
- Run from the project root directory

## What It Checks

### Farm Checks
1. **Snapshots API** - Verifies `GET /api/snapshots` returns valid JSON list
2. **Metadata Fields** - Each snapshot has `snapshot_id`, `tenant_id`, `created_at`, `schema_version`
3. **Snapshot Retrieval** - Fetches a full snapshot via `GET /api/snapshots/{id}`
4. **Schema Version** - Confirms `meta.schema_version == "farm.v1"`
5. **Required Planes** - All 7 planes present: discovery, idp, cmdb, cloud, endpoint, network, finance
6. **Discovery Observations** - Discovery plane has observations array
7. **No-Cheat Scan** - Ensures banned adjudication fields don't appear anywhere:
   - `is_shadow_it`, `in_cmdb`, `rules_triggered`
   - `conflict_types`, `source_presence`, `parked_reason`
   - `ground_truth`, `verdict`, `conclusion`, `label`
   - `classification`, `risk_score`, `compliance_status`

## Output Format

### On Success
```
NUKE CHECK: PASS
Project: FARM
Timestamp: 2024-01-15T12:00:00Z
Duration: 1.23s

Key results:
  - GET /api/snapshots returned 20 snapshots
  - Snapshot metadata contains required fields
  - meta.schema_version == 'farm.v1'
  - All required planes present
  - No banned adjudication fields found

All checks passed!
```

### On Failure
```
NUKE CHECK: FAIL
Project: FARM
Timestamp: 2024-01-15T12:00:00Z

Key results:
  - GET /api/snapshots returned 20 snapshots
  - ...

------------------------------------------------------------
What failed: Banned adjudication fields found
Likely cause: Fields: ['planes.discovery.observations[0].is_shadow_it']
What to do: Remove conclusion/verdict fields from generator
```

## Suggested Cadence

- **Daily**: Run as part of CI/CD pipeline
- **Before merge**: Run before merging any PR
- **Before deploy**: Run before publishing to production

## Exit Codes

- `0` - All checks passed
- `1` - One or more checks failed (or project unknown)
