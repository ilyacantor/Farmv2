# AOS Farm

## Overview
AOS Farm is a synthetic enterprise data generator designed to create realistic source-of-truth data planes and raw observation streams. Its primary purpose is to generate robust testing data for the AutonomOS AOD (Discover) module, specifically focusing on raw evidence to improve the accuracy and reliability of anomaly detection. The project aims to eliminate "green-test theater" by enforcing strict rules that ensure all changes preserve real-world semantics, are provable with real-world output, and include negative tests. The business vision is to provide high-fidelity, plausible enterprise data for rigorous testing of AOD's discovery capabilities in complex enterprise environments.

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
AOS Farm is built with a FastAPI backend, Uvicorn ASGI server, and a Supabase PostgreSQL database. The frontend is a Vanilla JavaScript single-page application with Tailwind CSS and Jinja2 templating.

**Core Principles & Features:**
- **Deterministic Data Generation:** Generates reproducible synthetic data based on seed, scale, and enterprise/realism profiles, yielding 7 independent data planes designed to correlate only via realistic keys.
- **Governance Framework:** Classifies assets based on governance status (IdP OR CMDB = governed). Shadow = ungoverned + active. Zombie = governed + stale + ongoing finance. Parked = ungoverned + stale.
- **Snapshot Management:** Provides APIs for generating, retrieving, listing, and deleting data snapshots, each with an `__expected__` block for grading metadata.
- **Reconciliation System:** Compares AOD results against Farm's expectations, indicating gradeability via `contract_status`.
- **Validation Suite:** Comprehensive validation checks on every snapshot and reconciliation, including expected block consistency, clock invariants, finance consistency, join hygiene, and gradeability gates.
- **Hot/Cold Storage Split:** Optimizes performance by separating snapshot metadata (hot path) from full snapshot blobs (cold storage).
- **Database Resilience:** Implements connection pooling, circuit breaker, exponential backoff, and a concurrency semaphore for robust database interactions.
- **Background Job System:** Mega/Enterprise scale snapshots use a background job pattern (202 + job_id) to avoid holding database pooler sessions. Jobs have progress tracking and are polled by the UI.
- **Supabase Pooler Optimized:** Uses transaction pooling with statement timeouts (30s), max 2 concurrent DB operations, and batch inserts with commit-per-batch to prevent pooler saturation.
- **Discrepancy-Based Assessment Triggers:** Automatic generation of detailed markdown assessment reports for non-perfect reconciliations, triggered by any classification or admission mismatch.
- **Policy Differences Tracking:** Reconciliation analysis identifies and explains expected discrepancies due to intentional design choices between Farm and AOD:
  - *Governance-Only Admission:* Farm admits assets based on governance presence alone (IdP/CMDB = system-of-record truth), while AOD requires discovery evidence (live observable surface area). These produce expected false negatives and are NOT defects.
  - *Key Normalization:* Domain canonicalization differences between Farm and AOD may result in missed matches even when both systems processed the same evidence.
- **Error Handling:** Emphasizes explicit error statuses (`UPSTREAM_ERROR`, `INVALID_INPUT_CONTRACT`) and guarantees JSON responses for API errors.

**Design Choices:**
- **Ownership Boundaries:** Farm owns the reconciliation UI, while AOD owns structured actual output, with AOD never consuming Farm's expected data.
- **Configuration:** Supports various `Scale`, `Enterprise Profile`, and `Realism Profile` settings.
- **Data Presets:** Includes `clean_baseline`, `enterprise_mess`, and `adversarial` challenge levels.
- **Canonical Key Rules:** Defines clear rules for asset identification.
- **CMDB Resolution:** Handles multiple CMDB matches with specific `cmdb_resolution_reason` codes.
- **Admission Rules:** If an asset is in CMDB or IdP, it is admitted regardless of discovery evidence. Assets without CMDB/IdP presence require 1+ discovery sources, cloud evidence, or sufficient finance spend to be admitted.
- **Ground Truth Classification:** Admitted assets are classified based on evidence flags and governance propagation logic.

## External Dependencies
- **Database:** Supabase PostgreSQL (managed Postgres with session pooling), configured via `SUPABASE_DB_URL` or `DATABASE_URL`.
- **AOD Module (AutonomOS Discover):** Interacts via defined API contracts, requiring `AOD_URL` and optionally `AOD_SHARED_SECRET`. A local stub can be enabled with `USE_AOD_EXPLAIN_STUB=true`.