# AOS Farm

## Overview
AOS Farm is the **Test Oracle** for the AutonomOS platform. Its sole purpose is to generate synthetic test data, compute expected outcomes, and grade actual results. Farm is a verification/QA tool - it does NOT perform operational tasks, repairs, or infrastructure management.

**Architectural Role:** Farm is the "Verifier" in the AOS Self-Healing Mesh architecture. It generates ground truth and validates that other components (AOD, DCL, AAM, AOA) behave correctly.

## Architectural Boundaries (CRITICAL)

### What FARM Does (Test Oracle Functions)
- **Generate** synthetic enterprise data (snapshots, agent profiles, workflows)
- **Compute** expected outcomes (`__expected__` blocks with classifications)
- **Grade** actual results against expectations (reconciliation)
- **Provide** ground truth APIs for other systems to verify their repairs
- **Validate** data consistency and semantic correctness

### What FARM Does NOT Do (Forbidden - Belongs to Other Components)
- **NO Repair Logic** - Self-healing belongs to AAM (The Mesh)
- **NO Connector Provisioning** - Infrastructure belongs to AOA (The Orchestrator)
- **NO Raw Data Buffering** - Data handling belongs to DCL (The Brain)
- **NO Operational Execution** - Workflow execution belongs to AOA

### Platform Component Boundaries
| Component | Role | Owns |
|-----------|------|------|
| **FARM** | Test Oracle | Test data, expectations, grading |
| **AAM** | Self-Healing Mesh | Repair, reconnection, connector lifecycle |
| **DCL** | Metadata Brain | Metadata routing (no raw payloads) |
| **AOA** | Orchestrator | Execution, infrastructure, workflow runtime |
| **AOD** | Discover | Asset discovery, classification, policy |

### Constraint Enforcement
- Farm must treat infrastructure as a "Black Box" - it audits inputs/outputs but never manages containers or workers
- Farm's "Source of Truth" endpoints provide ground truth for DCL/AAM to verify their repairs, but Farm does not perform repairs itself
- Farm must be independently deployable as a watchdog, not a manager

---

## Legacy Overview (Historical Context)
AOS Farm was originally designed as a synthetic enterprise data generator. Its main purpose is to create realistic source-of-truth data planes and raw observation streams to generate robust testing data for the AutonomOS AOD (Discover) module. This data specifically focuses on raw evidence to improve the accuracy and reliability of anomaly detection. The project aims to eliminate "green-test theater" by enforcing strict rules that ensure all changes preserve real-world semantics, are provable with real-world output, and include negative tests.

## User Preferences
### Guardrail: No "Green-Test Theater"
The anti-pattern we are eliminating: Agents frequently "solve" problems by making the system look clean.

**Forbidden patterns:**
- "All tests pass" while the feature fails the first real eval
- Papering over contract mismatches by making schemas permissive
- Converting upstream errors into "not found" or "empty" results
- Overwriting history / collapsing identity scopes to avoid collisions
- Adding hidden shortcuts / labels / join keys that make synthetic demos work but are IRL-invalid

**The goal is not "no errors"; the goal is "correct semantics."**

### Definition of "DONE" (must satisfy all 4)

1. **Semantics preserved** - The behavior matches the stated IRL meaning of the feature
2. **No cheating** - No overwrites, no "optional everything", no silent fallbacks, no ground-truth labels
3. **Proof is real** - Tests + one real run showing failure-before / success-after
4. **Negative test included** - Ensure the cheat can't come back

### Fail Loudly on Reality Violations
When data is bad or missing, do NOT "handle" it by pretending it's fine. Use explicit error statuses:
- `UPSTREAM_ERROR` - External service returned invalid data
- `INVALID_SNAPSHOT` - Snapshot data is malformed
- `INVALID_INPUT_CONTRACT` - Required fields missing

## System Architecture
AOS Farm is built with FastAPI and Uvicorn for the backend, Supabase PostgreSQL for the database, and a Vanilla JavaScript SPA with Tailwind CSS and Jinja2 templating for the frontend.

**Core Features:**
-   **Deterministic Data Generation:** Reproducible synthetic data generated based on seed, scale, and profiles, with 7 independent data planes correlated via realistic keys.
-   **Governance Framework:** CMDB and IdP are authoritative sources, enforcing strict gates for "governed" status.
-   **Snapshot Management:** APIs for generating, retrieving, listing, and deleting snapshots, including `__expected__` grading metadata.
-   **Reconciliation System:** Compares AOD results against expectations and generates detailed markdown assessment reports.
-   **Validation Suite:** Checks consistency for expected blocks, clock invariants, finance, join hygiene, and gradeability gates.
-   **End-to-End Injection Tests:** Inject test payloads into the Fabric (iPaaS) and verify arrival at destination (Data Warehouse).
-   **Hot/Cold Storage:** Separates snapshot metadata (hot) from full blobs (cold) for performance optimization.
-   **Database Resilience:** Implements connection pooling, circuit breaker, exponential backoff, and a concurrency semaphore.
-   **Background Jobs:** Asynchronous job pattern with progress tracking for large-scale snapshots.
-   **Policy Alignment:** Farm consumes policy from AOD; discrepancies indicate bugs.
-   **Analysis Versioning:** Auto-computed version from source hashes; stale cached analyses are recomputed.

**Farm.Verifier Module (src/verifier/):**
The Verifier namespace contains the core Test Oracle logic:
-   `injection_tests.py`: End-to-end injection test framework
    -   `create_injection_payload()`: Generate fingerprinted test payloads
    -   `verify_payload_arrival()`: Compare expected vs actual payloads
    -   `run_e2e_injection_test()`: Full injection test workflow

**Verifier API Endpoints:**
-   `GET /api/verifier/health`: Verifier module health and capabilities
-   `GET /api/verifier/payload`: Generate test payload for manual injection
-   `POST /api/verifier/injection-test`: Run end-to-end injection test

**Design Principles:**
-   **Ownership Boundaries:** Farm manages the reconciliation UI, while AOD handles structured actual output.
-   **Policy Invariant:** AOD owns policy; Farm consumes it for grading without overriding.
-   **Configuration:** Supports customization through Scale, Enterprise Profile, Realism Profile, and Data Presets (`clean_baseline`, `enterprise_mess`, `adversarial`).

**Governance Logic:**
-   **CMDB Governance Gate:** A CMDB record grants governance only if the CI exists, the CI type is valid (per policy), and its lifecycle is one of `{"prod", "production", "staging", "stage", "live", "active"}`.
-   **IdP Governance Gate:** An IdP record grants governance only if explicit IdP linkage exists, SSO gate passes (if `require_sso_for_idp` is enabled), and the app name is canonical (not containing tokens like `(legacy)`, `deprecated`, `-prod`, `-dev`, `-staging`, `-test`, `-qa`).
-   **Classification Logic:** Classifies assets as **Shadow** (ungoverned + active), **Zombie** (governed + stale + ongoing finance), **Parked** (ungoverned + stale), or **Clean** (governed + active).
-   **Activity Detection:** Activity is determined by discovery and IdP timestamps only; finance data does not count as activity. Recent activity is defined as within `activity_window_days` (default 90).

**System of Record (SOR) Scoring:**
-   **SOR is Orthogonal:** SOR scoring is independent of Shadow/Zombie/Governed classifications. An asset can be Governed + SOR, Shadow + SOR-candidate (RED FLAG!), or Zombie + former-SOR.
-   **Signal Weights:** cmdb_authoritative (+40), known_sor_vendor (+30), middleware_exporter (+25), high_data_tier (+20), primary_domain_match (+20), multi_consumer (+15), data_domain_alignment (+10).
-   **Confidence Thresholds:** High >= 0.75, Medium >= 0.50, Low > 0 but < 0.50, None = 0.
-   **Known SOR Vendors by Domain:**
    -   Customer: salesforce.com, hubspot.com, dynamics.com, zendesk.com
    -   Employee: workday.com, adp.com, bamboohr.com, ultipro.com
    -   Financial: netsuite.com, quickbooks.com, xero.com, sage.com
    -   Product: sap.com, oracle.com, epicor.com, infor.com
    -   Identity: okta.com, onelogin.com, auth0.com, ping.com
    -   IT Assets: servicenow.com, freshservice.com, jira.atlassian.com
-   **Shadow + SOR Flag:** Assets classified as Shadow but with high/medium SOR likelihood are flagged as critical risk during validation.

**Key API Endpoints:**
-   `/api/snapshots`: For generating, listing, retrieving, and deleting snapshots.
-   `/api/reconciliations`: For initiating and listing reconciliation runs.
-   `/api/policy`: To retrieve current policy.
-   `/api/stream/synthetic/mulesoft`: Simulates a "toxic" MuleSoft data stream for testing DCL's Ingest Sidecar resilience, supporting `speed` and `chaos` parameters.
-   `/api/source/salesforce/invoice/{invoice_id}`: Provides pristine invoice records for repairing drifted data.
-   `/api/verify/salesforce/invoice`: Allows DCL to verify repaired records against ground truth.

## External Dependencies
-   **Supabase PostgreSQL:** Managed PostgreSQL database with session pooling.
-   **AOD (AutonomOS Discover):** The target system being tested by AOS Farm.
-   **tldextract:** Used for domain parsing and effective Top-Level Domain (eTLD)+1 extraction.
-   **FastAPI:** The web framework used for building the API.
-   **Uvicorn:** The ASGI server that runs the FastAPI application.

## Agent Orchestration Stress Testing

Farm generates synthetic agent profiles and workflow graphs for stress testing agentic orchestration platforms.

### Agent Fleet Generation

**Endpoint:** `GET /api/agents/fleet?scale=small|medium|large`

Scale presets:
- `small`: 10 agents (unit tests)
- `medium`: 50 agents (integration)
- `large`: 100 agents (load tests)

Agent distribution: 10% planners, 50% workers, 20% specialists, 15% reviewers, 5% approvers

Agent profiles include:
- Capabilities (task_decomposition, delegation, tool_invocation, etc.)
- Tools (email, jira, database_query, code_execute, etc.)
- Reliability profiles (rock_solid 99.9%, reliable 95%, flaky 80%, unreliable 60%)
- Cost profiles (free, cheap, standard, premium, enterprise)
- Policy templates (permissive, standard, restricted, audit_heavy)

### Workflow Generation

**Endpoint:** `GET /api/agents/workflow?workflow_type=dag&chaos_rate=0.2`

Workflow types: `linear`, `dag`, `parallel`, `saga`

Chaos injection types:
- `tool_timeout` - Tests retry logic
- `tool_failure` - Tests compensation/saga patterns
- `agent_conflict` - Tests adjudication
- `policy_violation` - Tests escalation
- `checkpoint_crash` - Tests durable execution replay
- `rate_limit` - Tests backoff strategies

### Complete Stress Scenario

**Endpoint:** `GET /api/agents/stress-scenario?scale=medium&workflow_count=10&chaos_rate=0.2`

Returns:
- Agent fleet with diverse profiles
- Batch of workflows with agent assignments
- `__expected__` blocks for validation

### Streaming Workloads

**Endpoint:** `GET /api/agents/stream?rate=50&chaos_rate=0.1`

Streams continuous workflow payloads (NDJSON) for load testing.

### Stress Test Run Persistence

**Endpoints:**
- `GET /api/agents/stress-test-runs?limit=50` - List recent stress test runs
- `GET /api/agents/stress-test-runs/{run_id}` - Get full details for a specific run

Stress test runs are persisted to the database with:
- Run metadata (scale, workflow_count, chaos_rate, seed)
- Fleet and scenario summaries
- Validation results and execution outcome
- Duration and status (completed, completed_with_failures, timeout, etc.)

The UI displays run history in the Agents tab with color-coded status badges.

### Full Documentation

See [`docs/agent-orchestration-guide.md`](docs/agent-orchestration-guide.md) for complete protocol specifications, data schemas, and integration patterns.