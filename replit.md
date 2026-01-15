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
- **Governance Framework:** Classifies assets based on governance status. See Governance Contract below for authoritative rules.
- **Snapshot Management:** Provides APIs for generating, retrieving, listing, and deleting data snapshots, each with an `__expected__` block for grading metadata.
- **Reconciliation System:** Compares AOD results against Farm's expectations, indicating gradeability via `contract_status`.
- **Validation Suite:** Comprehensive validation checks on every snapshot and reconciliation, including expected block consistency, clock invariants, finance consistency, join hygiene, and gradeability gates.
- **Hot/Cold Storage Split:** Optimizes performance by separating snapshot metadata (hot path) from full snapshot blobs (cold storage).
- **Database Resilience:** Implements connection pooling, circuit breaker, exponential backoff, and a concurrency semaphore for robust database interactions.
- **Background Job System:** Mega/Enterprise scale snapshots use a background job pattern (202 + job_id) to avoid holding database pooler sessions. Jobs have progress tracking and are polled by the UI.
- **Supabase Pooler Optimized:** Uses transaction pooling with statement timeouts (30s), max 2 concurrent DB operations, and batch inserts with commit-per-batch to prevent pooler saturation.
- **Discrepancy-Based Assessment Triggers:** Automatic generation of detailed markdown assessment reports for non-perfect reconciliations, triggered by any classification or admission mismatch.
- **All Discrepancies Are Bugs:** Farm and AOD share policy via the policy center. Any disagreement between Farm and AOD is a BUG requiring investigation and fixing - there are no "expected policy differences" or "intentional discrepancies."
- **Canonical Domain Correlation (2026-01-14):** Added `canonical_domain` field to IdP and CMDB records. The generator populates this field with the original domain, and correlation logic uses it first before falling back to name matching. This improves CMDB/IdP correlation reliability despite name drift and optional `external_ref` fields.
- **Policy Alignment Fix (2026-01-15):** Fixed `is_excluded()` to NOT treat `corporate_root_domains` as exclusions. Major SaaS vendors (salesforce.com, slack.com, etc.) are legitimate applications that should be admitted and classified. Only explicit `exclusions` list and `infrastructure_seeds` (when `include_infra=false`) determine exclusions. Removed hardcoded banned_domains - these are now controlled entirely by AOD policy.
- **Policy Gate Handling (2026-01-15):** When AOD's policy has secondary gates enabled (e.g., `require_valid_ci_type=True`) but doesn't define the validation lists (e.g., `valid_ci_types=[]`), Farm:
  1. Logs a `POLICY_INCONSISTENCY` warning once per session with `upstream_fix_needed` message
  2. Accepts all values (matching AOD's actual behavior) since AOD is authoritative
  3. Does NOT invent defaults that would cause Farm/AOD disagreement
  This is transparent (logged, not hidden) and matches the "fail loudly" principle by surfacing the upstream policy gap rather than silently inventing behavior.
- **CI Type Vocabulary Alignment (2026-01-15):** Fixed CMDB gate failures caused by vocabulary mismatch between Farm's `CITypeEnum` (app, service, database, infra) and the policy's `valid_ci_types` (application, service, database, etc.). Updated policy defaults and `policy_master.json` to include both short forms (app, infra) and long forms (application) to ensure CMDB records pass the `require_valid_ci_type` gate. This fix increased CMDB matches from ~139 to ~314 and reduced false positive shadows significantly.
- **Analysis Versioning:** Prevents stale cached analyses from resurfacing as logic evolves:
  - `CURRENT_ANALYSIS_VERSION` in `src/services/constants.py` is **automatically computed** from source file hashes (analysis.py + reconciliation.py)
  - **No manual version bumping required** - any code change to analysis logic automatically invalidates all cached analyses
  - `analysis_version` and `analysis_computed_at` columns track when each analysis was computed
  - Auto-recompute on version mismatch: if cached version != CURRENT_ANALYSIS_VERSION, recompute automatically
  - Migration endpoint `/api/admin/migrate-stale-analyses` clears all stale cached analyses
  - Stats endpoint `/api/admin/analysis-version-stats` shows version distribution across reconciliations
  - Assessment reports show "Analysis vN computed at <time>" in header for transparency
- **Error Handling:** Emphasizes explicit error statuses (`UPSTREAM_ERROR`, `INVALID_INPUT_CONTRACT`) and guarantees JSON responses for API errors.

**Design Choices:**
- **Ownership Boundaries:** Farm owns the reconciliation UI, while AOD owns structured actual output, with AOD never consuming Farm's expected data.
- **Configuration:** Supports various `Scale`, `Enterprise Profile`, and `Realism Profile` settings.
- **Data Presets:** Includes `clean_baseline`, `enterprise_mess`, and `adversarial` challenge levels.
- **Canonical Key Rules:** Defines clear rules for asset identification.
- **CMDB Resolution:** Handles multiple CMDB matches with specific `cmdb_resolution_reason` codes.
- **Admission Rules:** If an asset is in CMDB or IdP, it is admitted regardless of discovery evidence. Assets without CMDB/IdP presence require 1+ discovery sources, cloud evidence, or sufficient finance spend to be admitted.
- **Ground Truth Classification:** Admitted assets are classified based on evidence flags and governance propagation logic.

## Governance Contract

**INVARIANT:** CMDB and IdP assert truth. Heuristics suggest context. Classification is deterministic.

### Authoritative Truth Sources
Farm treats CMDB and IdP as the only authoritative sources of governance. No other signals may assert governance.

### Governance Rules (Hard Requirements)
An asset is **governed** if and only if there exists at least one authoritative record (CMDB or IdP) that **explicitly passes all governance gates**.

**CMDB Governance:**
- CI must exist
- CI type must be valid (per `policy.secondary_gates.valid_ci_types`)
- CI lifecycle must be valid (per `policy.secondary_gates.invalid_lifecycle_states`)
- If record exists but fails any gate → Explicitly NOT governed (NO_CMDB)
- If no record exists → NOT governed

**IdP Governance:**
- Explicit IdP linkage must exist
- Required SSO gate must pass (if `policy.secondary_gates.require_sso_for_idp`)
- If record exists but fails any gate → Explicitly NOT governed (NO_IDP)
- If no record exists → NOT governed

### Classification Logic
```
governed = cmdb_present OR idp_present
```
Where `cmdb_present` and `idp_present` are True only if records pass all gates.

**Classifications:**
- **Shadow:** Ungoverned + Recent activity = Shadow IT
- **Zombie:** Governed + Stale activity + Ongoing finance = Deprovision candidate
- **Parked:** Ungoverned + Stale activity = Inactive, no action needed
- **Clean:** Governed + Recent activity = Healthy asset

### Heuristics (Non-Authoritative)
Heuristics may enrich context but **must never** assert governance, override gate outcomes, or flip classification states. Examples: fuzzy name matching, vendor inference, cross-TLD similarity.

### Determinism Guarantee
Given identical inputs (evidence + policy), Farm always produces the same classification. If Farm and AOD disagree under same evidence and policy, one contains a bug.

## Policy Architecture

**INVARIANT:** AOD owns policy. Farm consumes it for grading. Farm never overrides.

### Policy Ownership
- **AOD Policy Switchboard** is the single source of truth for all policy configuration
- **Farm is a test harness** that generates scenarios and grades AOD against expectations
- **Policies are configuration** that control deterministic logic - they are not negotiated or inferred by Farm

### Policy Flow
```
AOD Policy Switchboard → webhook notification → Farm clears cache
                      → fetch_policy_config() → Farm uses for grading
```

When Farm grades a reconciliation:
1. Farm fetches the current policy from AOD (or uses policy snapshot from AOD run)
2. Farm computes expected classifications using that exact policy
3. Farm compares AOD's actual results against expectations
4. Any discrepancy is a BUG (not an "expected difference")

### Policy Snapshot Per Run (Ideal Implementation)
For reproducible grading, each AOD run artifact should include:
- `policy_snapshot`: The exact policy JSON used for that run
- Or `policy_hash` + pointer to the exact policy blob in AOD DB

Farm reads that policy snapshot when grading, ensuring Farm and AOD used identical policy.

### What Farm Does NOT Do
- Farm does NOT maintain independent policy authority
- Farm does NOT override or modify AOD policies
- Farm does NOT have a separate `policy_master.json` for production (only fallback for local testing)
- Farm does NOT infer or negotiate policy values

### Fallback Behavior
`src/fixtures/policy_master.json` exists ONLY as a fallback when:
- `USE_AOD_EXPLAIN_STUB=true` (local testing mode)
- AOD is unreachable (should fail explicitly in production)

In production, if policy cannot be fetched from AOD, Farm should fail with `POLICY_UNAVAILABLE` rather than silently using defaults.

## External Dependencies
- **Database:** Supabase PostgreSQL (managed Postgres with session pooling), configured via `SUPABASE_DB_URL` or `DATABASE_URL`.
- **AOD Module (AutonomOS Discover):** Interacts via defined API contracts, requiring `AOD_URL` and optionally `AOD_SHARED_SECRET`. A local stub can be enabled with `USE_AOD_EXPLAIN_STUB=true`.