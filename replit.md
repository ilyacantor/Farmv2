# AOS Farm

## Overview
AOS Farm is the **Test Oracle** for the AutonomOS platform, responsible for generating synthetic test data, computing expected outcomes, and grading actual results. It acts as a verification and QA tool, ensuring other AutonomOS components (AOD, DCL, AAM, AOA) behave correctly. Farm is the "Verifier" in the AOS Fabric Plane Mesh architecture, accountable for ground truth validation, end-to-end injection tests, and accuracy measurement. Its core purpose is to eliminate "green-test theater" by enforcing strict rules that ensure all changes preserve real-world semantics, are provable with real-world output, and include negative tests.

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

**Core Functions & Architectural Boundaries:**
Farm's primary functions include generating synthetic enterprise data, computing expected outcomes, grading actual results, providing ground truth APIs, and validating data consistency. It explicitly avoids repair logic, connector provisioning, raw data buffering, and operational execution, which are responsibilities of other AutonomOS components (AAM, AOA, DCL). Farm treats infrastructure as a "Black Box" for auditing inputs/outputs without managing underlying resources.

**Fabric Plane Mesh Integration:**
Farm generates test scenarios for different enterprise presets (Scrappy, iPaaS-Centric, Platform-Oriented, Warehouse-Centric) to verify data flows through the 4 Fabric Planes (IPAAS, API_GATEWAY, EVENT_BUS, DATA_WAREHOUSE). This involves injecting canary records, verifying arrival at destination, measuring accuracy, and tracking latency.

**Key Features:**
-   **Deterministic Data Generation:** Reproducible synthetic data generation with 7 independent, correlated data planes.
-   **Snapshot Management:** APIs for generating, retrieving, listing, and deleting snapshots, including `__expected__` grading metadata.
-   **Reconciliation System:** Compares AOD results against expectations, generating detailed assessment reports.
-   **Validation Suite:** Checks consistency for expected blocks, clock invariants, finance, join hygiene, and gradeability gates.
-   **End-to-End Injection Tests:** Framework for testing data injection into the Fabric (iPaaS) and verification at the Data Warehouse.
-   **Governance Framework:** Enforces strict gates for "governed" status based on CMDB and IdP authoritative sources, classifying assets as Shadow, Zombie, Parked, or Clean.
-   **System of Record (SOR) Scoring:** Orthogonal to governance, scores assets as SOR candidates based on weighted signals, flagging Shadow + SOR assets as critical risks.
-   **Agent Orchestration Stress Testing:** Generates agent fleets and workflow graphs (linear, DAG, parallel, saga) with chaos injection (e.g., tool_timeout, agent_conflict) to stress test agentic orchestration platforms. It also provides streaming workloads and persists stress test run results.
-   **DCL/BLL/NLQ Scenario Testing:** Generates deterministic scenarios with finance, CRM, vendor, and asset data to provide ground truth for validating DCL, BLL, and NLQ systems. Includes metric-level ground truth APIs (e.g., revenue, top customers) and toxic data streams with controlled chaos (e.g., missing fields, duplicate invoices) for DCL integration testing.

**Farm.Verifier Module (`src/verifier/`):** Contains the core Test Oracle logic, including functions for creating injection payloads, verifying payload arrival, and running end-to-end injection tests.

**API Endpoints:**
-   `/api/snapshots`: For managing synthetic data snapshots.
-   `/api/reconciliations`: For initiating and listing reconciliation runs.
-   `/api/policy`: To retrieve current policy.
-   `/api/verifier/*`: Health checks, payload generation, and injection tests.
-   `/api/agents/*`: Agent fleet, workflow, stress scenario generation, and stress test run management.
-   `/api/scenarios/*`: Scenario generation, ground truth metrics, and toxic data streams.
-   `/api/stream/synthetic/mulesoft`: Simulates toxic data streams for DCL.
-   `/api/source/salesforce/invoice/{invoice_id}`: Provides pristine source data for repairs.
-   `/api/verify/salesforce/invoice`: Allows verification of repaired records.

## External Dependencies
-   **Supabase PostgreSQL:** Managed PostgreSQL database.
-   **AOD (AutonomOS Discover):** The target system for Farm's testing.
-   **tldextract:** Used for domain parsing.
-   **FastAPI:** Web framework.
-   **Uvicorn:** ASGI server.