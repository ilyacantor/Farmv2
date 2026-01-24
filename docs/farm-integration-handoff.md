# FARM Integration Handoff Document

## Overview

This document provides context for continuing FARM ↔ AOA integration development. AOA has been enhanced with simulation infrastructure that fully integrates with FARM's stress testing protocol.

---

## What Was Built in AOA

### 1. Simulation Harness (`app/agentic/simulation/`)

New module providing full FARM protocol support:

| File | Purpose |
|------|---------|
| `executor.py` | Main simulation executor with all 9 chaos types |
| `event_emitter.py` | Bridges to StreamManager for real-time UI events |
| `metrics_bridge.py` | Connects to Tracer, MetricsCollector, VitalsMonitor, BudgetEnforcer |

### 2. Dashboard Metrics API (`app/api/v1/aoa_dashboard.py`)

New endpoints for UI KPI visualization:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/aoa/dashboard` | GET | Complete dashboard with all KPIs |
| `/api/v1/aoa/metrics/timeseries` | GET | Time-series data for charts |
| `/api/v1/aoa/agents/summary` | GET | Registered agent statistics |
| `/api/v1/aoa/workflows/active` | GET | Currently running workflows |
| `/api/v1/aoa/approvals/pending` | GET | Pending HITL approvals |

### 3. Enhanced Stress Test API (`app/api/v1/stress_test.py`)

Updated to use SimulationExecutor:

- `POST /api/v1/stress-test/scenario` now returns FARM-compatible verdicts
- `GET /api/v1/stress-test/scenario/{id}` includes `verdict`, `analysis`, `validation`
- Full integration with AOA modules (registry, observability, governance)

### 4. All 9 Chaos Types Implemented

| Type | Recovery Behavior |
|------|-------------------|
| `tool_timeout` | Retry after delay |
| `tool_failure` | Retry if recoverable, fail otherwise |
| `agent_conflict` | Arbitrator resolution |
| `policy_violation` | Escalate for approval (auto-approved in sim) |
| `checkpoint_crash` | Replay from checkpoint |
| `memory_pressure` | Summarize and continue |
| `rate_limit` | Exponential backoff (2 retries) |
| `data_corruption` | Validate and repair |
| `network_partition` | Queue and retry (3 retries) |

---

## AOA Endpoint Contract

### Required by FARM

```
POST /api/v1/stress-test/fleet
- Input: AgentFleetRequest (agents, distribution, seed)
- Output: FleetIngestionResponse (status, agents_ingested)

POST /api/v1/stress-test/scenario
- Input: StressScenarioRequest (agents, workflows, assignments, __expected__)
- Output: ScenarioSubmissionResponse (status, scenario_id, workflows_queued)

GET /api/v1/stress-test/scenario/{scenario_id}
- Output: ScenarioResult with FARM validation fields:
  - verdict: "PASS" | "DEGRADED" | "FAIL" | "PENDING"
  - analysis: Operator-grade analysis object
  - validation: Per-check validation results
  - completion_rate: 0.0-1.0
  - chaos_recovery_rate: 0.0-1.0
```

### Response Format (FARM-Compatible)

```json
{
  "scenario_id": "...",
  "status": "completed",
  "verdict": "PASS",
  "completion_rate": 0.95,
  "chaos_recovery_rate": 0.88,
  "validation": {
    "completion_rate": {"expected": 0.8, "actual": 0.95, "passed": true},
    "chaos_recovery": {"expected": 0.8, "actual": 0.88, "passed": true},
    "task_completion": {"expected_tasks": 68, "actual_tasks": 68, "passed": true}
  },
  "analysis": {
    "verdict": "PASS",
    "title": "Stress Test PASSED",
    "summary": "Platform achieved 95% completion rate with 88% chaos recovery.",
    "sections": {
      "reliability": {"verdict": "PASS", "findings": [...]},
      "performance": {"verdict": "PASS", "findings": [...]},
      "resilience": {"verdict": "PASS", "findings": [...]}
    },
    "recommendations": [],
    "metrics": {
      "completion_rate": 0.95,
      "chaos_recovery_rate": 0.88,
      "throughput_tasks_per_sec": 2.5,
      "avg_latency_ms": 850
    }
  },
  "workflow_results": [...],
  "total_cost_usd": 0.15
}
```

---

## What FARM Needs To Do

### 1. Update Push Integration

The `POST /api/agents/run-stress-test` endpoint should:

```python
# After scenario execution completes
result = poll_for_completion(f"{target_url}/api/v1/stress-test/scenario/{scenario_id}")

# AOA now returns FARM-compatible validation
if result["verdict"] == "PASS":
    return {"status": "passed", "analysis": result["analysis"]}
elif result["verdict"] == "DEGRADED":
    return {"status": "degraded", "analysis": result["analysis"]}
else:
    return {"status": "failed", "analysis": result["analysis"]}
```

### 2. Validate Against AOA's Validation Block

AOA now returns structured validation that FARM can directly compare:

```python
def validate_aoa_response(expected: dict, actual: dict) -> dict:
    """Compare FARM __expected__ with AOA validation."""

    validation = actual.get("validation", {})

    checks = {
        "completion_rate": validation.get("completion_rate", {}).get("passed", False),
        "chaos_recovery": validation.get("chaos_recovery", {}).get("passed", False),
        "task_completion": validation.get("task_completion", {}).get("passed", False),
    }

    return {
        "all_passed": all(checks.values()),
        "checks": checks,
        "verdict": actual.get("verdict"),
        "aoa_analysis": actual.get("analysis"),
    }
```

### 3. Use AOA Dashboard for Live Metrics

During stress test execution, FARM can poll:

```bash
# Get live KPIs during test
curl -H "X-Tenant-ID: stress-test" \
  "https://aoa-platform/api/v1/aoa/dashboard"

# Response includes:
# - agents.active: Currently registered agents
# - workflows.active_workflows: Running workflows
# - chaos.recovery_rate: Real-time chaos recovery
# - costs.today_usd: Cost accumulation
```

### 4. Stream Consumption Endpoint

For continuous load testing, AOA supports receiving FARM's streaming workflows:

```python
# FARM streams workflows
for workflow in generate_stream(rate=50, chaos_rate=0.2):
    # POST each workflow to AOA
    response = requests.post(
        f"{target_url}/api/v1/stress-test/workflow",
        json=workflow,
        headers={"X-Tenant-ID": tenant_id}
    )

    # Track execution_id for later validation
    execution_ids.append(response.json()["execution_id"])
```

---

## Test the Integration

### Quick Smoke Test

```bash
# 1. Generate a small scenario from FARM
curl "http://farm:5000/api/agents/stress-scenario?scale=small&workflow_count=3&seed=42"

# 2. POST to AOA
curl -X POST "http://aoa:8000/api/v1/stress-test/scenario" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: test-tenant" \
  -d @scenario.json

# 3. Poll for results
curl "http://aoa:8000/api/v1/stress-test/scenario/{scenario_id}"

# 4. Check dashboard
curl "http://aoa:8000/api/v1/aoa/dashboard" -H "X-Tenant-ID: test-tenant"
```

### Verify All Chaos Types

```bash
# Generate high-chaos scenario
curl "http://farm:5000/api/agents/stress-scenario?scale=small&chaos_rate=0.5&seed=99"

# Submit and verify chaos_recovery_rate >= 0.8
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           FARM                                       │
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │ Fleet Gen   │  │ Workflow Gen│  │ Chaos Gen   │                 │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │
│         │                │                │                         │
│         └────────────────┴────────────────┘                         │
│                          │                                          │
│                          ▼                                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              POST /api/agents/run-stress-test                │   │
│  └──────────────────────────┬──────────────────────────────────┘   │
└─────────────────────────────┼───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           AOA                                        │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    stress_test.py API                        │   │
│  │   POST /fleet  │  POST /scenario  │  GET /scenario/{id}     │   │
│  └─────────────────────────┬───────────────────────────────────┘   │
│                            │                                        │
│                            ▼                                        │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              SimulationExecutor                              │   │
│  │                                                              │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │   │
│  │  │ Registry │  │  Tracer  │  │ Metrics  │  │ Budget   │    │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │   │
│  │                                                              │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │   │
│  │  │Arbitrator│  │ Policy   │  │ Approval │  │ Vitals   │    │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │   │
│  └─────────────────────────┬───────────────────────────────────┘   │
│                            │                                        │
│                            ▼                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐         │
│  │EventEmitter  │───►│StreamManager │───►│   UI/WS      │         │
│  └──────────────┘    └──────────────┘    └──────────────┘         │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              aoa_dashboard.py API                            │   │
│  │   GET /dashboard  │  GET /metrics/timeseries                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Files Changed/Created in AOA

### New Files

```
app/agentic/simulation/
├── __init__.py           # Module exports
├── executor.py           # SimulationExecutor with 9 chaos types
├── event_emitter.py      # StreamManager bridge
└── metrics_bridge.py     # Observability/governance bridge

app/api/v1/
└── aoa_dashboard.py      # Dashboard metrics API
```

### Modified Files

```
app/api/v1/stress_test.py
- Import SimulationExecutor
- Updated submit_scenario to use executor
- Enhanced get_scenario_result with verdict/analysis
- Added chaos_recovery_rate, verdict, analysis to ScenarioResult

app/agentic/checkpointer.py
- Made sqlalchemy imports lazy (unrelated fix for import errors)
```

---

## Next Steps for FARM

1. **Update run-stress-test endpoint** to use AOA's new validation format
2. **Add dashboard polling** during test execution for progress monitoring
3. **Implement streaming consumer** to receive AOA events via WebSocket
4. **Store AOA verdicts** in FARM's stress test run history
5. **Generate comparative analysis** between __expected__ and AOA validation

---

## Environment Notes

- AOA runs on Replit (configured deployment)
- No sqlalchemy required for stress testing (lazy imports)
- Redis optional for event streaming (falls back to in-memory)
- All 6 AOA RACI modules integrated (registry, lifecycle, approval, coordination, observability, governance)

---

## Contact

For questions about the AOA implementation:
- Review `app/agentic/simulation/executor.py` for execution logic
- Review `docs/aoa-stress-testing-reference.md` for FARM protocol spec
- Check `app/api/v1/stress_test.py` for API contract

---

*Last Updated: 2026-01-24*
