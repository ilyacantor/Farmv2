# AOS Farm

## Overview
AOS Farm is a synthetic enterprise data generator that creates realistic source-of-truth data planes and raw observation streams. Its primary purpose is to generate robust testing data for the AutonomOS AOD (Discover) module, focusing on raw evidence. The project aims to eliminate "green-test theater" by enforcing strict rules that ensure all changes preserve real-world semantics, are provable with real-world output, and include negative tests. The business vision is to provide high-fidelity, plausible enterprise data for rigorous testing of AOD's discovery capabilities, thereby improving the accuracy and reliability of anomaly detection in complex enterprise environments.

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

## Recent Changes (December 28, 2025)
- **Assessment Reports**: Automatic generation of detailed markdown assessment reports for non-perfect reconciliations. Reports include executive summary, classification analysis (shadows/zombies), RCA hints, and actionable recommendations. Download via `/api/reconcile/{id}/assessment` endpoint with backward-compatible 204 responses (`X-Assessment-Status: perfect-match` or `not-generated`).
- **Progressive Rendering**: UI now renders snapshots immediately (~0.5s) while reconciliations continue loading in background (~5s). Eliminates perceived "stuck on Loading..." behavior during slow API calls.
- **Production Hardening**: APIJSONErrorMiddleware guarantees JSON responses for all /api/* routes. Frontend `apiFetch()` validates content-type and response.ok before JSON parsing.
- **Error Handling**: Error panel with retry CTA for graceful failure handling when server restarts cause 502 proxy errors.

## System Architecture
The project is structured around a FastAPI application, featuring a simple Farm Console UI.

**Services Layer (src/services/):**
- Provides core logic for key normalization, reconciliation, analysis, and AOD client interaction. Includes domain constants and logging utilities.

**Frontend Architecture (templates/index.html):**
- Manages global state (`FarmState`), asynchronous requests (`RequestController`), and guided validation runs (`TourController`).

**Documentation (docs/):**
- `SYNTHETIC_GENERATION.md`: Comprehensive guide to synthetic data generation.

**Technical Implementations:**
- **Core Framework:** FastAPI.
- **Data Generation:** Deterministic generators for reproducible results based on seed, scale, and enterprise/realism profiles. Generates 7 independent data planes designed to correlate only via realistic keys.
- **API Design:** RESTful API for snapshot management, AOD reconciliation, and status queries.
- **Schema Versioning:** All snapshots include `meta.schema_version = "farm.v1"`.
- **Design Principles:** Independence of data planes, no "conclusions" fields, deterministic generation, and timestamps anchored to snapshot creation.

**Feature Specifications:**
- **Snapshot Management:** API for generating, retrieving, listing, and deleting data snapshots, each with an `__expected__` block for grading metadata.
- **Reconciliation System:** Compares AOD results against Farm's expectations, indicating gradeability via `contract_status`.
- **AOD Interaction:** Defines contracts for AOD output and an optional `explain-nonflag` endpoint.
- **Finance Evidence Rules:** Classifies assets based on finance data, emphasizing `HAS_ONGOING_FINANCE` for shadow classification.
- **Stress Test Scenarios:** Includes 4 deterministic stress test scenarios (Split Brain, Toxic Asset, Banned Asset, Zombie Asset) injected into every snapshot.

**System Design Choices:**
- **Ownership Boundaries:** AOD never consumes Farm's expected data; Farm owns the reconciliation UI; AOD owns structured actual output.
- **Error Handling:** Emphasizes explicit error statuses (e.g., `UPSTREAM_ERROR`, `INVALID_INPUT_CONTRACT`).
- **Configuration:** Supports `Scale` (small to enterprise), `Enterprise Profile` (e.g., modern_saas), and `Realism Profile` (clean, typical, messy).
- **Data Presets:** Provides 3-tier challenge levels (`clean_baseline`, `enterprise_mess`, `adversarial`) controlling various parameters like conflict rate and aliasing.
- **Canonical Key Rules:** Domain-first for assets with a domain; normalized name for internal services.
- **CMDB Resolution:** Handles multiple CMDB matches with `cmdb_resolution_reason` codes (`NONE`, `MULTI_ENV`, `LEGACY`, `DUPLICATE`, `PARENT_VENDOR`).
- **Reconciliation Modes:** Supports `sprawl`, `infra`, and `all` for varying testing scopes.
- **Admission Rules:** An entity is admitted for classification if it meets specific criteria (e.g., discovery strength ≥ 2 distinct sources, cloud evidence, IdP match, or CMDB match). Rejected entities are explicitly marked.
- **Ground Truth Classification:** Admitted assets are classified as Shadow, Zombie, or Clean based on evidence flags and governance propagation logic.

## External Dependencies
- **Database:** Supabase Postgres. Configured via `SUPABASE_DB_URL` or `DATABASE_URL`. Schema includes `runs`, `snapshots`, and `reconciliations` tables.
- **AOD Module (AutonomOS Discover):** Interacts via defined API contracts. Requires `AOD_URL` and optionally `AOD_SHARED_SECRET`. `USE_AOD_EXPLAIN_STUB=true` enables a local stub for testing.