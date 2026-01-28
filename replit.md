# AOS Farm - Test Oracle Platform

## What Is This?

AOS Farm is a **Test Oracle** - a platform that generates test data with known correct answers, so you can verify that other systems produce the right results. Think of it as the "answer key" for testing enterprise software.

Farm serves four AutonomOS components:
- **AOD** (AutonomOS Discover) - Tests data discovery and relationship detection
- **AOA** (AutonomOS Agents) - Stress tests AI agent orchestration platforms
- **NLQ** (Natural Language Query) - Validates query systems with ground truth answers
- **DCL** (Data Connectivity Layer) - Tests data ingestion resilience with toxic streams

---

## Module Capabilities

### AOD Module: Discovery Testing

**Purpose:** Generate realistic enterprise datasets to test how well AOD discovers relationships, anomalies, and data quality issues.

**What You Can Do:**
1. **Generate Enterprise Snapshots** - Create synthetic company data with accounts, contacts, deals, assets, vendors, invoices, and tickets. All 7 data planes are correlated (e.g., contacts belong to accounts, invoices link to deals).

2. **Set Scale & Reproducibility** - Choose small/medium/large datasets. Use seeds for deterministic generation - same seed always produces identical data.

3. **Grade Discovery Results** - Each snapshot includes expected outcomes. After AOD analyzes the data, compare its findings against the built-in answer key to measure accuracy.

4. **Run Reconciliations** - Submit AOD's actual results and get precision/recall/accuracy scores showing how well it performed.

**Key Insight:** Snapshots contain intentional anomalies (orphaned records, stale data, governance violations) that AOD should detect.

---

### AOA Module: Agent Stress Testing

**Purpose:** Generate synthetic AI agent configurations and workflows to stress test orchestration platforms.

**What You Can Do:**
1. **Generate Agent Fleets** - Create teams of AI agents with different roles:
   - Planners (break down complex tasks)
   - Workers (execute tasks)
   - Specialists (domain experts)
   - Reviewers/Approvers (validate work)

2. **Generate Workflows** - Create task graphs agents must complete:
   - Linear (step-by-step)
   - DAG (complex dependencies)
   - Parallel (many tasks at once)
   - Saga (tasks with rollback capability)

3. **Inject Chaos** - Add controlled failures to test resilience:
   - tool_timeout - Tools fail to respond
   - agent_conflict - Multiple agents claim same task
   - resource_exhaustion - Hit capacity limits
   - partial_failure - Some steps succeed, others fail

4. **Run Stress Tests** - Execute scenarios against your platform, capture results, and automatically validate against expected outcomes.

5. **Track Results Over Time** - All stress test runs are persisted. Compare performance across versions.

**Key Insight:** The chaos injection reveals how your platform handles real-world failures, not just happy-path scenarios.

---

### NLQ Module: Query Validation

**Purpose:** Provide ground truth datasets for testing Natural Language Query systems. When someone asks "What's our total revenue?", you need to know the correct answer to verify the system works.

**What You Can Do:**
1. **Generate Business Scenarios** - Create deterministic datasets with:
   - Customers with tiers (enterprise, mid-market, SMB)
   - Invoices with amounts, dates, statuses
   - Vendors with spend categories
   - Assets with ownership and lifecycle data

2. **Get Ground Truth Metrics** - Retrieve pre-computed correct answers:
   - Total revenue
   - Top customers by spend
   - Revenue by quarter
   - Outstanding vs paid invoices
   - Vendor spend breakdown
   - Asset counts by status

3. **Use the Question Bank** - 100 pre-built test questions across 23 categories:
   - Revenue queries
   - Customer analytics
   - Time-based analysis
   - Aggregations and comparisons
   - Multi-table joins

4. **Validate NLQ Output** - Compare your system's answers against ground truth. Track accuracy by question category.

**Key Insight:** Same seed + scale always produces identical scenario_id and ground truth values. This enables reproducible testing across environments.

---

### DCL Module: Data Ingestion Testing

**Purpose:** Test how your Data Connectivity Layer handles corrupted, malformed, or problematic data during ingestion.

**What You Can Do:**
1. **Generate Toxic Streams** - Create data feeds with controlled chaos:
   - missing_fields - Required data is absent
   - duplicate_invoice - Same record appears twice
   - incorrect_currency - Currency codes don't match amounts
   - stale_timestamp - Dates are unreasonably old
   - orphaned_reference - Foreign keys point to nothing

2. **Detect Chaos** - Each toxic record includes metadata describing what's wrong. Your DCL should detect these issues.

3. **Fetch Pristine Source** - Look up the original "clean" version of any corrupted record. This is what the data should look like after repair.

4. **Verify Repairs** - Submit your repaired records for field-by-field validation against the source of truth.

**Integration Flow:**
Ingest Toxic Stream -> Detect Problems -> Fetch Source -> Repair -> Verify

**Key Insight:** DCL testing proves your ingestion pipeline fails gracefully and can recover, rather than silently propagating bad data.

---

## Core Philosophy

### No "Green-Test Theater"

The anti-pattern we eliminate: Tests that pass while the feature fails in production.

**Forbidden Patterns:**
- "All tests pass" while real usage fails
- Making schemas permissive to avoid contract mismatches
- Converting errors into empty results
- Adding hidden shortcuts that work for demos but fail IRL

**The Goal:** Correct semantics, not cosmetically clean results.

### Definition of DONE

Every feature must satisfy all four:
1. **Semantics preserved** - Behavior matches the real-world meaning
2. **No cheating** - No silent fallbacks, no optional-everything
3. **Proof is real** - Actual run showing failure-before / success-after
4. **Negative test included** - Verify the cheat can't return

### Fail Loudly

When data is bad, don't pretend it's fine. Use explicit error statuses:
- UPSTREAM_ERROR - External service returned invalid data
- INVALID_SNAPSHOT - Snapshot data is malformed
- INVALID_INPUT_CONTRACT - Required fields missing

---

## Project Structure

src/
  main.py              - FastAPI application entry point
  api/                 - API route handlers
    snapshots.py       - AOD snapshot endpoints
    reconcile.py       - Reconciliation endpoints
    agents.py          - AOA agent/workflow endpoints
    scenarios.py       - NLQ/DCL scenario endpoints
  generators/          - Data generation logic
    enterprise.py      - 7-plane enterprise data
    agents.py          - Agent fleet generation
    workflows.py       - Workflow graph generation
    scenarios.py       - Business scenario generation
  models/              - Pydantic data models
  verifier/            - Test oracle core logic
  db.py                - Database operations

templates/
  index.html           - Single-page application UI

data/
  nlq_test_questions.json   - 100-question test bank
  nlq_invoices.json         - Invoice time-series data

docs/
  USER_GUIDE.md        - Operator guide for non-technical users

---

## API Reference

### AOD Endpoints
| Endpoint | Method | Purpose |
|----------|--------|---------|
| /api/snapshots | GET | List all snapshots |
| /api/snapshots | POST | Generate new snapshot |
| /api/snapshots/{id} | GET | Get snapshot details |
| /api/snapshots/{id} | DELETE | Delete snapshot |
| /api/reconcile | GET | List reconciliation runs |
| /api/reconcile | POST | Run reconciliation |

### AOA Endpoints
| Endpoint | Method | Purpose |
|----------|--------|---------|
| /api/agents/fleet | POST | Generate agent fleet |
| /api/agents/workflows | POST | Generate workflow graphs |
| /api/agents/stress-scenario | POST | Generate complete stress scenario |
| /api/agents/stress-test-runs | GET | List stress test runs |
| /api/agents/stress-test-runs | POST | Execute stress test |

### NLQ Endpoints
| Endpoint | Method | Purpose |
|----------|--------|---------|
| /api/scenarios/generate | POST | Generate business scenario |
| /api/scenarios/{id}/metrics/{metric} | GET | Get ground truth metric |
| /api/nlq/questions | GET | Get test question bank |
| /api/nlq/categories | GET | Get question categories |

### DCL Endpoints
| Endpoint | Method | Purpose |
|----------|--------|---------|
| /api/stream/synthetic/mulesoft | GET | Get toxic data stream |
| /api/source/salesforce/invoice/{id} | GET | Fetch pristine source record |
| /api/verify/salesforce/invoice | POST | Verify repaired record |

---

## Technical Stack

- Backend: FastAPI + Uvicorn (Python)
- Database: Supabase PostgreSQL
- Frontend: Vanilla JavaScript SPA with Tailwind CSS
- Templating: Jinja2

---

## User Preferences

### Determinism is Critical
Same seed + scale = identical output. This enables:
- Reproducible bug reports
- Consistent CI/CD testing
- Cross-environment validation

### Scenarios are Shared
Once generated, scenarios are available across NLQ and DCL tabs. Generate once, test from multiple angles.

### Module Attribution
- AOD = AutonomOS Discover (data discovery)
- AOA = AutonomOS Agents (agent orchestration)
- NLQ = Natural Language Query (query validation)
- DCL = Data Connectivity Layer (data ingestion)

---

## Recent Changes

- 2026-01-28: Added collapsible guide panels to all module tabs (AOD, AOA, NLQ, DCL)
- 2026-01-28: Fixed DCL naming (Data Connectivity Layer, not Data Contract Library)
- 2026-01-28: Updated NLQ tab header with clear description
- 2026-01-28: Split UI into dedicated module tabs with proper attribution
