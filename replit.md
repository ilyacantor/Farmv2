# AOS Farm

## Overview

AOS Farm is a synthetic enterprise data generator designed to create realistic source-of-truth data planes and raw observation streams. Its primary purpose is to provide robust testing data for the AutonomOS AOD (Discover) module, focusing on generating raw evidence rather than pre-concluded insights. The project aims to eliminate "green-test theater" by enforcing strict rules against superficial fixes and ensuring that all changes preserve real-world semantics, are provable with real-world output, and include negative tests.

The business vision is to provide high-fidelity, plausible enterprise data for rigorous testing of AOD's discovery capabilities, thereby improving the accuracy and reliability of anomaly detection in complex enterprise environments.

## User Preferences

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

## System Architecture

The project is structured around a FastAPI application, serving as the entry point, with dedicated modules for API routes, Pydantic models for data planes, and deterministic data generators.

**UI/UX Decisions:**
A simple Farm Console UI is provided via `templates/index.html`.

**Technical Implementations:**
- **Core Framework:** FastAPI for the web API.
- **Data Generation:** Deterministic data generators ensure reproducible results based on a given seed, scale, and enterprise/realism profiles.
- **Data Planes:** Generates 7 independent data planes: Discovery, IdP, CMDB, Cloud, Endpoint, Network, and Finance. These planes are designed to be independent, correlating only via realistic keys.
- **API Design:** RESTful API endpoints for managing snapshots, reconciling AOD results, and querying status.
- **Schema Versioning:** All snapshots include a `meta.schema_version = "farm.v1"` for consistency.
- **Design Principles:**
    - Independence of data planes.
    - No "conclusions" fields (e.g., no pre-computed shadow flags).
    - Deterministic generation based on seed and configuration.
    - Timestamps anchor to snapshot creation time with realistic recency distributions (active, recent, stale, zombie candidates).

**Feature Specifications:**
- **Snapshot Management:** API to generate, retrieve, list, and delete data snapshots. Each snapshot includes an `__expected__` block for grading metadata, which AOD should ignore.
- **Reconciliation System:**
    - Allows comparison of AOD results against Farm's expectations.
    - Supports manual and auto-reconciliation, fetching AOD results if `AOD_URL` is configured.
    - Reconciliations have a `contract_status` (`CURRENT`, `STALE_CONTRACT`, `INCONSISTENT_CONTRACT`) to indicate gradeability based on the presence and consistency of `asset_summaries`.
    - Grading logic prioritizes `asset_summaries` for deriving counts and accuracy metrics.
- **AOD Interaction:** Defines clear contracts for AOD output and an optional `explain-nonflag` endpoint for diagnostic purposes, ensuring AOD never consumes Farm's expected data directly.
- **Finance Evidence Rules:** Specific rules for classifying assets based on finance data, emphasizing `HAS_ONGOING_FINANCE` for shadow classification.

**System Design Choices:**
- **Ownership Boundaries:** AOD never consumes Farm's expected/RCA data; Farm owns the reconciliation UI; AOD owns structured actual output.
- **Error Handling:** Emphasizes failing loudly with explicit error statuses (e.g., `UPSTREAM_ERROR`, `INVALID_INPUT_CONTRACT`) instead of silent fallbacks.
- **Configuration:** Supports `Scale` (small, medium, large, enterprise), `Enterprise Profile` (modern_saas, regulated_finance, healthcare_provider, global_manufacturing), and `Realism Profile` (clean, typical, messy).

## External Dependencies

- **Database:** Supabase Postgres (exclusively).
    - Environment variables: `SUPABASE_DB_URL` (priority), `DATABASE_URL`.
    - Replit DB URLs are ignored if `IGNORE_REPLIT_DB=true`.
    - Schema includes `runs`, `snapshots`, and `reconciliations` tables.
- **AOD Module (AutonomOS Discover):**
    - Interacts via defined API contracts for snapshot generation, status checks, and reconciliation.
    - Requires `AOD_URL` and optionally `AOD_SHARED_SECRET` for auto-reconciliation.
    - `USE_AOD_EXPLAIN_STUB=true` enables a local stub for testing without a live AOD instance.

## Ground Truth Classification Definitions

Farm classifies assets into three buckets based on evidence flags:

### Shadow (Ungoverned with Operational Spend)
```
is_shadow = (has_ongoing_finance OR cloud_present) 
            AND activity_present 
            AND NOT idp_present 
            AND NOT cmdb_present
```
- Requires ongoing financial commitment (not just one-time payment)
- Must have recent activity
- Must lack governance (no IdP, no CMDB)

### Zombie (Governed but Stale)
```
is_zombie = (idp_present OR cmdb_present) 
            AND NOT activity_present 
            AND stale_timestamps > 0
```
- Has governance presence
- No recent activity
- Has stale timestamps (>90 days)

### Clean (Governed and Active)
```
is_clean = NOT is_shadow AND NOT is_zombie AND discovery_present
```
- Not shadow, not zombie
- Has discovery evidence

## CMDB Resolution Reason Codes

When Farm matches multiple CMDB configuration items to a single asset, it emits a `cmdb_resolution_reason` explaining the ambiguity:

| Code | Description |
|------|-------------|
| `NONE` | Single clear match or no matches |
| `MULTI_ENV` | Same app name appears in different lifecycle environments (dev/staging/prod) |
| `LEGACY` | Deprecated/legacy CI exists alongside current version |
| `DUPLICATE` | True duplicate records or multiple CIs without clear differentiation |
| `PARENT_VENDOR` | CMDB vendor is broader parent vendor, not specific product |

**CMDB Matching Rules (IRL-correct):**
- Farm matches CMDB by **name** OR **external_ref domain** ONLY
- Farm does NOT match CMDB by vendor (vendor-based matching is incorrect)
- AOD should use the same matching rules to align with Farm expectations

## Known Ground Truth Issue (BLOCKING)

**Problem:** CLEAN bucket is too broad. Assets can be marked CLEAN while having:
- `NO_IDP + NO_CMDB` (no governance)
- `HAS_DISCOVERY + HAS_FINANCE` (but not ongoing finance)

This creates incoherent ground truth - asset is ungoverned but classified as CLEAN.

**Root cause:** Shadow requires `has_ongoing_finance`, so assets with one-time finance payments fall through to CLEAN despite being ungoverned.

**Options to resolve:**
1. **Tighten CLEAN** - Require `HAS_IDP OR HAS_CMDB` for CLEAN classification
2. **Add UNGOVERNED bucket** - New category for discovered-but-ungoverned assets
3. **Accept current policy** - Document that "no ongoing spend = not actionable"

**Status:** Grading may produce training noise until ground truth policy is decided.