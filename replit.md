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
- **Governance Framework:** Classifies assets based on governance status. Farm treats CMDB and IdP as the only authoritative sources of governance, enforcing strict rules for what constitutes a "governed" asset.
- **Snapshot Management:** Provides APIs for generating, retrieving, listing, and deleting data snapshots, each with an `__expected__` block for grading metadata.
- **Reconciliation System:** Compares AOD results against Farm's expectations, indicating gradeability via `contract_status`. Automatically generates detailed markdown assessment reports for non-perfect reconciliations.
- **Validation Suite:** Comprehensive validation checks on every snapshot and reconciliation, including expected block consistency, clock invariants, finance consistency, join hygiene, and gradeability gates.
- **Hot/Cold Storage Split:** Optimizes performance by separating snapshot metadata (hot path) from full snapshot blobs (cold storage).
- **Database Resilience:** Implements connection pooling, circuit breaker, exponential backoff, and a concurrency semaphore for robust database interactions. Optimized for Supabase Pooler with transaction pooling and batch inserts.
- **Background Job System:** Mega/Enterprise scale snapshots use a background job pattern for generation, with progress tracking and UI polling.
- **Policy Alignment:** Farm consumes policy from AOD for grading and does not maintain independent policy authority. Discrepancies between Farm's expected classifications and AOD's actual results, given identical policy, indicate a bug. When AOD policy gates are enabled but lists are undefined, Farm logs `POLICY_INCONSISTENCY` and accepts all values, matching AOD's actual behavior without inventing defaults.
- **Analysis Versioning:** Prevents stale cached analyses by automatically computing `CURRENT_ANALYSIS_VERSION` from source file hashes. Cached analyses are recomputed if the version is mismatched.
- **Error Handling:** Emphasizes explicit error statuses (`UPSTREAM_ERROR`, `INVALID_INPUT_CONTRACT`) and guarantees JSON responses for API errors.

**Design Choices:**
- **Ownership Boundaries:** Farm owns the reconciliation UI, AOD owns structured actual output. AOD never consumes Farm's expected data.
- **Configuration:** Supports various `Scale`, `Enterprise Profile`, and `Realism Profile` settings.
- **Data Presets:** Includes `clean_baseline`, `enterprise_mess`, and `adversarial` challenge levels.
- **Canonical Key Rules:** Defines clear rules for asset identification.
- **CMDB Resolution:** Handles multiple CMDB matches with specific `cmdb_resolution_reason` codes.
- **Admission Rules:** Assets in CMDB or IdP are admitted regardless of discovery evidence. Otherwise, they require 1+ discovery sources, cloud evidence, or sufficient finance spend.
- **Ground Truth Classification:** Admitted assets are classified based on evidence flags and governance propagation logic.
- **Policy Invariant:** AOD owns policy. Farm consumes it for grading. Farm never overrides. Policies are configuration that control deterministic logic. Farm fails explicitly with `POLICY_UNAVAILABLE` if policy cannot be fetched from AOD in production.

## Recent AOD Alignment Fixes (Jan 2026)

**Infrastructure Domain Preservation:**
- Infrastructure domains (googleapis.com, gstatic.com, office.com, cloudfront.net, etc.) are preserved as standalone keys, NOT collapsed to parent domains
- This matches AOD Stage 4 behavior where infrastructure domains remain separate catalog entries
- Defined in `INFRASTRUCTURE_DOMAINS` frozenset in key_normalization.py

**Lifecycle Gates:**
- CMDB entries with lifecycle states `pending`, `draft`, `retired`, `decommissioned`, `deprecated`, `archived` fail governance gates
- Only `active`, `development`, `staging`, `production`, `maintenance` states grant governance

**Zombie Detection:**
- Activity = discovery + IdP timestamps ONLY (NOT finance)
- Zombie = governed + stale activity + ongoing finance (all three required)
- Ungoverned stale assets are PARKED, not zombies

**Key Normalization:**
- Uses tldextract for proper eTLD+1 extraction
- Junk domain suffixes (cdn.com, edge.com, global.com, etc.) excluded via policy

**Zombie Finance Generation:**
- Zombie apps now use canonical vendor/product names (no drift) for reliable finance correlation
- Stress-test zombie scenario includes vendor, contract, and recurring transaction entries
- All three conditions verified: governed + stale activity + ongoing finance

**Infrastructure Domain Exclusion:**
- PolicyConfig.from_aod_response now correctly loads `infrastructure_domains` from policy_master.json
- Domains like googleapis.com, gstatic.com, office.com, cloudfront.net are excluded from expected block when include_infra=false
- Corporate root domains (google.com, microsoft.com, amazon.com) remain as valid SaaS vendors per policy intent

**Enhanced AOD Stub Mode v2 (Jan 2026):**
- Stub mode enabled with `USE_AOD_EXPLAIN_STUB=true` reads snapshot CMDB/IdP planes to compute governance flags
- **Two-tier correlation algorithm:**
  - **Tier 1 AUTHORITATIVE:** registered_domain matching against canonical_domain and domains[] arrays -> returns HAS_CMDB/HAS_IDP
  - **Tier 2 WEAK:** Vendor name matching or name word overlap (>=2 non-stopwords) -> returns HAS_CMDB_WEAK/HAS_IDP_WEAK (does NOT trigger governance merge)
- Structured correlation output: Each response includes `cmdb_correlation` and `idp_correlation` with `status` (AUTHORITATIVE/WEAK/NONE), `method`, and `matched_id`
- Reports include "MODE: STUB" banner and stub correlation breakdown table showing FP counts by correlation status
- WEAK correlation does NOT assert governance - prevents false positives from loose name matching
- Tests: 21 unit tests covering Tier 1 matching, Tier 2 matching, negative cases, and structured output validation

**Known Alignment Gaps:**
- CMDB correlation mismatch: Farm and AOD may use different correlation logic for linking CMDB entries to discovery domains
- Key normalization for synthetic domains: Some Farm-generated domains show as KEY_NORMALIZATION_MISMATCH due to different canonical key selection

## External Dependencies
- **Database:** Supabase PostgreSQL (managed Postgres with session pooling), configured via `SUPABASE_DB_URL` or `DATABASE_URL`.
- **AOD Module (AutonomOS Discover):** Interacts via defined API contracts, requiring `AOD_URL` and optionally `AOD_SHARED_SECRET`. A local stub can be enabled with `USE_AOD_EXPLAIN_STUB=true`.