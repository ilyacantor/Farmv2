# AOS Farm - Test Oracle Platform

## Overview

AOS Farm is a **Test Oracle** platform designed to generate test data with known correct answers. Its primary purpose is to provide ground truth for verifying the correctness of other systems, acting as an "answer key" for enterprise software testing. It supports four AutonomOS components: AOD (data discovery), AOA (AI agent orchestration), NLQ (natural language query validation), and DCL (data ingestion resilience). The project aims to eliminate "green-test theater" by ensuring tests accurately reflect real-world system behavior and fail loudly when data is bad.

Farm is **Accountable/Responsible** for:
- Ground Truth Validation
- Accuracy Measurement
- End-to-End Injection Tests
- Test Data Generation & Source of Truth Provision
- Agent Registry & Metadata Management

## User Preferences

### Fundamental Coding Behavior Only
No band-aids, no silent fallbacks, no "plausible-looking wrong answers." Every code path must be loud, structured, and diagnosable. If something is unknown, fail explicitly — never guess. Same philosophy as DCL's `NO_MATCHING_PIPE`: structured errors that tell the operator exactly what went wrong and who needs to fix it.

### Determinism is Critical
Same seed + scale = identical output. This enables:
- Reproducible bug reports
- Consistent CI/CD testing
- Cross-environment validation

### Module Attribution
- AOD = AutonomOS Discover (data discovery)
- AOA = AutonomOS Agents (agent orchestration)
- NLQ = Natural Language Query (query validation)
- DCL = Data Connectivity Layer (data ingestion)
- AAM = AutonomOS Action Mesh (the mesh / connector provisioning)

## System Architecture

The AOS Farm platform is built with a clear focus on modularity, deterministic data generation, and rigorous validation.

**UI/UX Decisions:**
The user interface is a single-page application (SPA) built with Vanilla JavaScript and Tailwind CSS, served via Jinja2 templating. It features dedicated module tabs for AOD, AOA, NLQ, and DCL, each with collapsible guide panels for user assistance. A live DCL connectivity indicator (green/red/grey) provides immediate feedback on integration status.

**Technical Implementations & Feature Specifications:**

-   **AOD Module (Discovery Testing):** Generates correlated synthetic enterprise data (7 data planes: accounts, contacts, deals, assets, vendors, invoices, tickets) with configurable scale and reproducibility via seeds. Includes intentional anomalies for discovery testing. Features reconciliation capabilities to grade AOD's findings against expected outcomes, providing precision/recall/accuracy scores.
-   **AOA Module (Agent Stress Testing):** Creates synthetic AI agent fleets (planners, workers, specialists, reviewers) and complex task workflows (linear, DAG, parallel, saga). Supports chaos injection (tool timeouts, agent conflicts, resource exhaustion, partial failures) to test platform resilience. Persists stress test runs for historical performance comparison.
-   **NLQ Module (Business Data Ground Truth):** Generates multi-source enterprise business data across 8 systems (Salesforce, NetSuite, Chargebee, Workday, Zendesk, Jira, Datadog, AWS Cost) anchored by a financial model engine. Computes and provides ground truth manifests (v2.0) with per-quarter breakdowns for metrics like revenue, CAGR, and customer counts. Allows optional direct pushing of generated payloads to DCL's ingest endpoint. Ground truth manifests are persisted to `ground_truth_manifests` DB table (survive restarts). Server-side verification endpoint `POST /api/business-data/verify/{run_id}` scores actuals against ground truth per-metric with PASS/DEGRADED/FAIL verdicts. DCL push correlation keys (dcl_run_id, dispatch_id, snapshot_name) are persisted for Path 4 readback. DCL readback endpoint stubbed at `POST /verify/{run_id}/dcl-readback` (501 until readback contract provided).
-   **DCL Module (Data Ingestion Testing):** Generates "toxic streams" with controlled data problems (missing fields, duplicates, incorrect currency, stale timestamps, orphaned references). Provides pristine source records for corrupted data and enables verification of repaired records against the source of truth. The integration flow involves ingesting toxic streams, detecting problems, fetching sources, repairing, and verifying.
-   **Fabric Plane Configuration:** Includes endpoints to manage and generate fabric configurations based on industry verticals, applying weighted vendor selections.
-   **Core Philosophy:** Adheres to "No 'Green-Test Theater'" by ensuring correct semantics, prohibiting silent fallbacks or optional-everything, requiring real proof of failure/success, and including negative tests. Emphasizes "Fail Loudly" with explicit error statuses (UPSTREAM_ERROR, INVALID_SNAPSHOT, INVALID_INPUT_CONTRACT).

**Performance Caps (capability over capacity):**
-   customer_count capped at 2,000 per quarter (both `_generate_trajectory` and `from_model_quarters`)
-   support_tickets capped at 5,000 per quarter
-   Zendesk org names use numeric suffix on collision (pool: 300 unique combos)
-   Standard preset: ~121K records in ~2s; Full preset: ~198K records in ~3s

**System Design Choices:**
The backend is a FastAPI application using Uvicorn, leveraging Pydantic for data models. Determinism is a core principle, ensuring identical output for the same seed and scale. The system includes extensive data generation logic (`generators/` directory) for various enterprise and business data types, a `verifier/` for test oracle logic, and `services/` for client interactions.

**Trifecta Architecture (AAM ↔ Farm ↔ DCL):**
Farm participates in the trifecta via four execution paths:
-   **Path 2 (AAM → Farm):** `POST /api/farm/manifest-intake` receives single JobManifest; `POST /api/farm/manifest-intake/batch` receives batch of manifests with concurrency control.
-   **Path 3 (Farm → DCL):** Farm generates data per manifest, pushes to DCL's `/ingest` using the manifest's `pipe_id` as `x-pipe-id` header. Handles DCL 422 `NO_MATCHING_PIPE` rejections as config errors (never retried).
-   **Path 4 (Farm ↔ DCL):** Verification/recon path — inject ground truth, read back, compare.
-   Manifest-driven mode uses AAM's `pipe_id` (production path); self-directed mode uses Farm's internal `pipe_id` (dev/demo path).
-   **Category-based generator routing:** `source.system` stays truthful (real vendor name). `source.category` (crm, erp, billing, hr, support, devops, observability, infrastructure) routes to the appropriate generator archetype in simulation mode. Resolution: direct system match → category routing → 422 NO_GENERATOR_ROUTE (no silent fallback). When real adapters arrive, category routing is bypassed.

**Project Structure Highlights:**
-   `src/api/`: Contains route handlers for AOD, AOA, Business Data, DCL, and Manifest Intake.
-   `src/api/manifest_intake.py`: Path 2/3 implementation — single + batch manifest intake, DCL push with correlation keys.
-   `src/generators/`: Houses all data generation logic, including enterprise data, agent fleets, workflows, financial models, and ground truth computation.
-   `src/models/`: Defines Pydantic data models for various entities.
-   `src/models/manifest.py`: JobManifest, DCLPushResult, ManifestExecutionResult, BatchManifestRequest/Response models.
-   `src/verifier/`: Core test oracle logic.

## External Dependencies

-   **Backend Framework:** FastAPI (Python)
-   **ASGI Server:** Uvicorn
-   **Database:** Supabase PostgreSQL
-   **Frontend:** Vanilla JavaScript, Tailwind CSS
-   **Templating Engine:** Jinja2
-   **HTTP Client:** httpx (for asynchronous DCL integration)