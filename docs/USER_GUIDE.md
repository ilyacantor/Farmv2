# AOS Farm User Guide

AOS Farm is the **Test Oracle** for the AutonomOS platform. It generates synthetic test data, computes expected outcomes, and grades actual results to ensure other AOS components behave correctly.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Snapshot Management](#snapshot-management)
3. [Reconciliation & Grading](#reconciliation--grading)
4. [Agent Stress Testing](#agent-stress-testing)
5. [DCL/NLQ Testing](#dclnlq-testing)
6. [API Reference](#api-reference)

---

## Quick Start

### Base URL
```
https://your-farm-instance.replit.app
```

### Generate Your First Scenario
```bash
curl -X POST "https://your-farm-url/api/scenarios/generate" \
  -H "Content-Type: application/json" \
  -d '{"seed": 12345, "scale": "medium"}'
```

Response:
```json
{
  "scenario_id": "dfa0ae0d57c9",
  "manifest": {
    "entity_counts": {
      "invoices": 604,
      "customers": 133,
      "vendors": 25,
      "assets": 261
    }
  }
}
```

### Key Concepts

| Term | Description |
|------|-------------|
| **Scenario** | A deterministic dataset for testing DCL/NLQ systems |
| **Snapshot** | Enterprise data with 7 correlated data planes for AOD grading |
| **Reconciliation** | Comparing AOD results against expected outcomes |
| **Stress Test** | Agent fleet + workflow generation for AOA validation |

---

## Snapshot Management

Snapshots represent synthetic enterprise data used to test and grade AOD (AutonomOS Discover).

### Generate a Snapshot
```bash
curl -X POST "https://your-farm-url/api/snapshots" \
  -H "Content-Type: application/json" \
  -d '{
    "org_name": "TestCorp",
    "preset": "PRESET_3_PLATFORM_ORIENTED",
    "scale": "small",
    "seed": 42
  }'
```

### List Snapshots
```bash
curl "https://your-farm-url/api/snapshots"
```

### Retrieve a Snapshot
```bash
curl "https://your-farm-url/api/snapshots/{snapshot_id}"
```

### Delete a Snapshot
```bash
curl -X DELETE "https://your-farm-url/api/snapshots/{snapshot_id}"
```

### Enterprise Presets

| Preset | Description |
|--------|-------------|
| `PRESET_1_IPAAS_CENTRIC` | Heavy iPaaS usage (MuleSoft, Workato) |
| `PRESET_2_WAREHOUSE_CENTRIC` | Data warehouse as integration hub |
| `PRESET_3_PLATFORM_ORIENTED` | Balanced platform approach |
| `PRESET_4_API_GATEWAY` | API Gateway-centric architecture |
| `PRESET_5_EVENT_DRIVEN` | Event bus / streaming focus |
| `PRESET_6_SCRAPPY` | Direct SaaS connections (no fabric) |

### Data Planes (7 Correlated Layers)

Each snapshot contains:
1. **Applications** - SaaS and internal apps
2. **Integrations** - Connections between apps
3. **Data Flows** - Movement of data
4. **Users** - Identity and access
5. **Assets** - Infrastructure resources
6. **Vendors** - Third-party providers
7. **Finance** - Invoices and spend

---

## Reconciliation & Grading

Reconciliation compares AOD's discovered results against Farm's expected outcomes.

### Create a Reconciliation Run
```bash
curl -X POST "https://your-farm-url/api/reconciliations" \
  -H "Content-Type: application/json" \
  -d '{
    "snapshot_id": "snap_abc123",
    "aod_results": { ... }
  }'
```

### List Reconciliation Runs
```bash
curl "https://your-farm-url/api/reconciliations?limit=25"
```

### Get Reconciliation Details
```bash
curl "https://your-farm-url/api/reconciliations/{run_id}"
```

### Grading Metrics

| Metric | Description |
|--------|-------------|
| `accuracy` | % of correct matches |
| `precision` | True positives / (True positives + False positives) |
| `recall` | True positives / (True positives + False negatives) |
| `f1_score` | Harmonic mean of precision and recall |

---

## Agent Stress Testing

Generate synthetic agent fleets and workflow graphs to stress test AOA (AutonomOS Agents).

### Generate Agent Fleet
```bash
curl -X POST "https://your-farm-url/api/agents/fleet" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 50,
    "seed": 999,
    "chaos_rate": 0.1
  }'
```

### Generate Workflow Graph
```bash
curl -X POST "https://your-farm-url/api/agents/workflow" \
  -H "Content-Type: application/json" \
  -d '{
    "pattern": "dag",
    "node_count": 20,
    "seed": 123
  }'
```

Workflow Patterns:
- `linear` - Sequential steps
- `dag` - Directed Acyclic Graph
- `parallel` - Concurrent branches
- `saga` - Compensating transactions

### Generate Stress Scenario
```bash
curl -X POST "https://your-farm-url/api/agents/stress-scenario" \
  -H "Content-Type: application/json" \
  -d '{
    "agent_count": 100,
    "workflow_count": 50,
    "chaos_types": ["tool_timeout", "agent_conflict"],
    "seed": 555
  }'
```

### Chaos Injection Types

| Type | Description |
|------|-------------|
| `tool_timeout` | Simulated tool execution delays |
| `agent_conflict` | Multiple agents claiming same task |
| `memory_pressure` | High memory usage simulation |
| `network_partition` | Simulated network failures |
| `approval_delay` | Human approval bottlenecks |

### Run Stress Test
```bash
curl -X POST "https://your-farm-url/api/agents/stress-test/run" \
  -H "Content-Type: application/json" \
  -d '{
    "scenario_id": "stress_abc123",
    "target_url": "https://aoa-instance.example.com"
  }'
```

### List Stress Test Results
```bash
curl "https://your-farm-url/api/agents/stress-runs"
```

---

## DCL/NLQ Testing

Use Farm as ground truth for validating Data Contract Library (DCL), Business Logic Layer (BLL), and Natural Language Query (NLQ) systems.

### 1. Generate a Scenario

```bash
curl -X POST "https://your-farm-url/api/scenarios/generate" \
  -H "Content-Type: application/json" \
  -d '{"seed": 12345, "scale": "medium"}'
```

Save the `scenario_id` for subsequent calls.

### 2. Query Ground Truth Metrics

**Total Revenue:**
```bash
curl "https://your-farm-url/api/scenarios/{scenario_id}/metrics/revenue"
```

**Month-over-Month Trend:**
```bash
curl "https://your-farm-url/api/scenarios/{scenario_id}/metrics/revenue-mom"
```

**Top Customers:**
```bash
curl "https://your-farm-url/api/scenarios/{scenario_id}/metrics/top-customers?limit=5"
```

**Vendor Spend:**
```bash
curl "https://your-farm-url/api/scenarios/{scenario_id}/metrics/vendor-spend"
```

**Resource Health:**
```bash
curl "https://your-farm-url/api/scenarios/{scenario_id}/metrics/resource-health"
```

### 3. Test NLQ Validation Flow

```python
# 1. Generate deterministic scenario
scenario = requests.post(f"{FARM}/api/scenarios/generate", 
    json={"seed": 12345, "scale": "medium"}).json()
scenario_id = scenario["scenario_id"]

# 2. Get ground truth
ground_truth = requests.get(
    f"{FARM}/api/scenarios/{scenario_id}/metrics/top-customers?limit=5").json()

# 3. Execute NLQ query in DCL
dcl_result = dcl.execute_nlq("Who are our top 5 customers?")

# 4. Compare results
assert dcl_result == ground_truth["customers"]
```

### 4. Toxic Stream Testing

Test DCL's drift detection and repair capabilities:

```bash
# Start toxic data stream
curl "https://your-farm-url/api/scenarios/{scenario_id}/stream/toxic?chaos=true&chaos_rate=0.15"
```

Chaos Types:
| Type | Description |
|------|-------------|
| `missing_fields` | Required fields omitted |
| `duplicate_invoice` | Same invoice_id repeated |
| `incorrect_currency` | Invalid currency codes |
| `stale_timestamp` | Very old dates (2020) |
| `orphaned_reference` | Non-existent customer/vendor IDs |

### 5. Repair & Verify Flow

```bash
# Get pristine source data
curl "https://your-farm-url/api/source/salesforce/invoice/{invoice_id}"

# Verify repaired record
curl -X POST "https://your-farm-url/api/verify/salesforce/invoice" \
  -H "Content-Type: application/json" \
  -d '{"invoice_id": "INV-123", "amount": 50000, ...}'
```

### 6. NLQ Test Questions

Get pre-built NLQ test questions:

```bash
# All questions (100)
curl "https://your-farm-url/api/scenarios/nlq/questions"

# Filter by category
curl "https://your-farm-url/api/scenarios/nlq/questions?category=top_customers"

# List categories
curl "https://your-farm-url/api/scenarios/nlq/categories"
```

### 7. Invoice Dataset for Time-Window Queries

```bash
# Get full dataset
curl "https://your-farm-url/api/scenarios/nlq/invoices"

# Filter by year
curl "https://your-farm-url/api/scenarios/nlq/invoices?year=2024"

# Filter by quarter
curl "https://your-farm-url/api/scenarios/nlq/invoices?year=2024&quarter=4"

# CSV export
curl "https://your-farm-url/api/scenarios/nlq/invoices?format=csv"

# Ground truth metrics
curl "https://your-farm-url/api/scenarios/nlq/invoices/ground-truth?time_window=2024"
```

---

## API Reference

### Snapshots

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/snapshots` | Generate new snapshot |
| GET | `/api/snapshots` | List all snapshots |
| GET | `/api/snapshots/{id}` | Get snapshot details |
| DELETE | `/api/snapshots/{id}` | Delete snapshot |

### Reconciliations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/reconciliations` | Create reconciliation run |
| GET | `/api/reconciliations` | List reconciliation runs |
| GET | `/api/reconciliations/{id}` | Get run details |

### Scenarios (DCL/NLQ)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/scenarios/generate` | Generate scenario |
| GET | `/api/scenarios/{id}/metrics/revenue` | Total revenue |
| GET | `/api/scenarios/{id}/metrics/revenue-mom` | MoM trend |
| GET | `/api/scenarios/{id}/metrics/top-customers` | Top N customers |
| GET | `/api/scenarios/{id}/metrics/vendor-spend` | Vendor spending |
| GET | `/api/scenarios/{id}/metrics/resource-health` | Asset health |
| GET | `/api/scenarios/{id}/stream/toxic` | Toxic data stream |
| GET | `/api/scenarios/nlq/questions` | NLQ test questions |
| GET | `/api/scenarios/nlq/categories` | Question categories |
| GET | `/api/scenarios/nlq/invoices` | Invoice dataset |
| GET | `/api/scenarios/nlq/invoices/ground-truth` | Invoice metrics |

### Source & Verify (Repair Flow)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/source/salesforce/invoice/{id}` | Get pristine source |
| POST | `/api/verify/salesforce/invoice` | Verify repaired record |

### Agent Stress Testing

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/agents/fleet` | Generate agent fleet |
| POST | `/api/agents/workflow` | Generate workflow graph |
| POST | `/api/agents/stress-scenario` | Generate stress scenario |
| POST | `/api/agents/stress-test/run` | Execute stress test |
| GET | `/api/agents/stress-runs` | List stress test results |

### Verifier

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/verifier/health` | Health check |
| POST | `/api/verifier/payload` | Generate injection payload |
| POST | `/api/verifier/inject` | Run injection test |

---

## Best Practices

### Deterministic Testing
- Always use explicit `seed` values for reproducibility
- Same `seed` + `scale` = identical `scenario_id` and data
- Store seeds in CI/CD config for regression testing

### Ground Truth Validation
```python
# Pattern for NLQ testing
def test_nlq_query(query: str, expected_endpoint: str, expected_field: str):
    dcl_result = dcl.execute(query)
    farm_truth = requests.get(f"{FARM}{expected_endpoint}").json()
    assert dcl_result == farm_truth[expected_field]
```

### Toxic Stream Integration
```python
# DCL integration pattern
for record in farm.stream_toxic(scenario_id):
    if record.get("_stream_meta", {}).get("chaos_applied"):
        # Detect and repair
        pristine = farm.get_source(record["invoice_id"])
        repaired = dcl.repair(record, pristine)
        farm.verify(repaired)
```

### No Green-Test Theater
- Always include negative tests
- Verify error cases, not just happy paths
- Use `UPSTREAM_ERROR`, `INVALID_SNAPSHOT`, `INVALID_INPUT_CONTRACT` statuses
- Real proof: show failure-before / success-after

---

## Troubleshooting

### Scenario not found
Scenarios are stored in memory. Generate a new one if the server restarted.

### Metrics returning zeros
Ensure you're using the correct `scenario_id` from the generate response.

### Toxic stream not showing chaos
Check `chaos=true` and `chaos_rate` > 0 in query parameters.

### Verification failing
Ensure all required fields match the source exactly (case-sensitive).

---

## Support

For issues or feature requests, contact the AutonomOS platform team.
