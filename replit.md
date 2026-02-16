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
-   **NLQ Module (Business Data Ground Truth):** Generates multi-source enterprise business data across 8 systems (Salesforce, NetSuite, Chargebee, Workday, Zendesk, Jira, Datadog, AWS Cost) anchored by a financial model engine. Computes and provides ground truth manifests (v2.0) with per-quarter breakdowns for metrics like revenue, CAGR, and customer counts. Allows optional direct pushing of generated payloads to DCL's ingest endpoint.
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

**Project Structure Highlights:**
-   `src/api/`: Contains route handlers for AOD, AOA, Business Data, and DCL.
-   `src/generators/`: Houses all data generation logic, including enterprise data, agent fleets, workflows, financial models, and ground truth computation.
-   `src/models/`: Defines Pydantic data models for various entities.
-   `src/verifier/`: Core test oracle logic.

## External Dependencies

-   **Backend Framework:** FastAPI (Python)
-   **ASGI Server:** Uvicorn
-   **Database:** Supabase PostgreSQL
-   **Frontend:** Vanilla JavaScript, Tailwind CSS
-   **Templating Engine:** Jinja2
-   **HTTP Client:** httpx (for asynchronous DCL integration)