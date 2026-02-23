# AOA Stress Testing Reference

## Overview

AOS Farm provides comprehensive synthetic test data generation for stress testing **AOA (Agentic Orchestration Architecture)** platforms. Farm operates as a **Test Oracle** - it generates synthetic agent fleets, workflow graphs, and chaos scenarios, then validates execution results against expected outcomes.

Farm does NOT execute workflows - it generates test scenarios and validates that AOA handles them correctly.

---

## Architectural Role

```
┌─────────────────────────────────────────────────────────────────┐
│                    AOS Fabric Plane Mesh                        │
│                                                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────────┐  │
│  │  IPAAS  │    │   API   │    │  EVENT  │    │    DATA     │  │
│  │         │◄──►│ GATEWAY │◄──►│   BUS   │◄──►│  WAREHOUSE  │  │
│  └─────────┘    └─────────┘    └─────────┘    └─────────────┘  │
│       ▲              ▲              ▲               ▲          │
│       │              │              │               │          │
│       └──────────────┴──────────────┴───────────────┘          │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   AAM (The Mesh)                          │  │
│  │          Self-healing, connector lifecycle                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│       ┌──────────────────────┼──────────────────────┐          │
│       ▼                      ▼                      ▼          │
│  ┌─────────┐           ┌─────────┐           ┌─────────┐       │
│  │   DCL   │           │   AOA   │           │   AOD   │       │
│  │ (Brain) │◄─────────►│(Orch.)  │◄─────────►│(Discover)│      │
│  └─────────┘           └─────────┘           └─────────┘       │
│                              ▲                                  │
│                              │                                  │
│                    ┌─────────┴─────────┐                       │
│                    │      FARM         │                       │
│                    │  (Test Oracle)    │                       │
│                    │                   │                       │
│                    │ - Generate agents │                       │
│                    │ - Generate flows  │                       │
│                    │ - Inject chaos    │                       │
│                    │ - Validate results│                       │
│                    └───────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
```

**Farm's RACI:**
- **A/R**: Ground Truth Validation, End-to-End Injection Tests, Accuracy Measurement
- **C/I**: Infrastructure, Execution, Self-healing (belongs to AAM/AOA)

---

## Available Capabilities

### 1. Agent Fleet Generation

Generate synthetic agent profiles with realistic characteristics for testing AOA's agent management.

**Endpoint:** `GET /api/agents/fleet`

**Parameters:**
| Parameter | Type | Default | Options |
|-----------|------|---------|---------|
| `scale` | string | `small` | `small` (10), `medium` (50), `large` (100) |
| `seed` | int | 12345 | Any integer for reproducibility |

**Agent Types Generated:**

| Type | Distribution | Capabilities |
|------|-------------|--------------|
| `planner` | 10% | task_decomposition, delegation, priority_ranking, dependency_analysis, resource_estimation |
| `worker` | 50% | task_execution, tool_invocation, result_formatting, error_recovery, progress_reporting |
| `specialist` | 20% | domain_expertise, deep_analysis, recommendation, validation, optimization |
| `reviewer` | 15% | quality_check, compliance_verify, diff_analysis, feedback_generation, approval_recommendation |
| `approver` | 5% | policy_enforcement, risk_assessment, final_decision, escalation, audit_trail |
| `coordinator` | (on request) | conflict_resolution, load_balancing, state_management, checkpoint_control, rollback |

**Reliability Profiles:**

| Profile | Success Rate | Mean Latency | Timeout Rate | Crash Probability |
|---------|-------------|--------------|--------------|-------------------|
| `rock_solid` | 99.9% | 50ms | 0.1% | 0.01% |
| `reliable` | 95% | 200ms | 2% | 0.5% |
| `flaky` | 80% | 500ms | 10% | 2% |
| `unreliable` | 60% | 2000ms | 25% | 5% |

**Cost Profiles:**

| Tier | Per Invocation | Per Token | Monthly Cap |
|------|----------------|-----------|-------------|
| `free` | $0.00 | $0.00 | None |
| `cheap` | $0.0001 | $0.000001 | $10 |
| `standard` | $0.001 | $0.00001 | $100 |
| `premium` | $0.01 | $0.0001 | $1000 |
| `enterprise` | $0.05 | $0.0005 | None |

**Policy Templates:**

| Template | Max Concurrent | Approval Above | Audit | Data Access |
|----------|---------------|----------------|-------|-------------|
| `permissive` | 100 | $10,000 | No | public, internal, confidential |
| `standard` | 10 | $1,000 | No | public, internal |
| `restricted` | 3 | $100 | No | public only |
| `audit_heavy` | 5 | $0 | Yes | public, internal |

**Example Response:**
```json
{
  "fleet_id": "fleet-12345-medium",
  "seed": 12345,
  "scale": "medium",
  "total_agents": 50,
  "agents": [
    {
      "agent_id": "agent-plan-8f3a2b1c",
      "name": "Strategist-000",
      "type": "planner",
      "version": "2.3.45",
      "capabilities": ["task_decomposition", "delegation", "priority_ranking"],
      "tools": ["jira", "calendar", "email", "llm_call"],
      "policy": {
        "template": "standard",
        "requires_approval": ["payments", "data_delete"],
        "max_concurrent_tasks": 10,
        "rate_limits": {"api_calls_per_min": 100}
      },
      "reliability": {
        "profile": "reliable",
        "success_rate": 0.95,
        "mean_latency_ms": 200
      },
      "cost": {
        "tier": "standard",
        "per_invocation": 0.001
      },
      "memory": {
        "short_term_capacity": 50,
        "context_window_tokens": 32768
      }
    }
  ],
  "distribution": {
    "by_type": {"planner": 5, "worker": 25, "specialist": 10, "reviewer": 8, "approver": 2},
    "by_reliability": {"rock_solid": 5, "reliable": 30, "flaky": 12, "unreliable": 3},
    "by_cost": {"free": 10, "cheap": 15, "standard": 15, "premium": 8, "enterprise": 2}
  },
  "__expected__": {
    "total_agents": 50,
    "has_planners": true,
    "has_approvers": true,
    "can_form_delegation_chain": true
  }
}
```

---

### 2. Workflow Graph Generation

Generate synthetic task graphs with dependencies, parallel branches, and saga patterns.

**Endpoint:** `GET /api/agents/workflow`

**Parameters:**
| Parameter | Type | Default | Options |
|-----------|------|---------|---------|
| `workflow_type` | string | random | `linear`, `dag`, `parallel`, `saga`, `map_reduce`, `cyclic` |
| `num_tasks` | int | 6 | 2-20 |
| `chaos_rate` | float | 0.0 | 0.0-1.0 |
| `seed` | int | 12345 | Any integer |

**Workflow Types:**

| Type | Structure | Use Case |
|------|-----------|----------|
| `linear` | A → B → C → D | Simple sequential pipelines |
| `dag` | Complex dependencies | Multi-path execution |
| `parallel` | A → [B,C,D] → E | Fan-out/fan-in patterns |
| `saga` | With compensation handlers | Distributed transactions |
| `map_reduce` | Split → Process → Aggregate | Large-scale batch processing |
| `cyclic` | Contains retry loops | Resilient patterns |

**Task Types:**

| Type | Tools Required | Avg Duration | Token Usage |
|------|---------------|--------------|-------------|
| `compute` | code_execute, data_transform | 500ms | 1000 |
| `io_read` | file_read, database_query, api_fetch | 200ms | 500 |
| `io_write` | file_write, database_write | 300ms | 500 |
| `api_call` | api_fetch, external_api | 1000ms | 200 |
| `decision` | llm_call, classifier | 800ms | 2000 |
| `approval` | (human in loop) | 60000ms | 100 |
| `aggregation` | data_transform, summarizer | 400ms | 1500 |
| `notification` | email, slack, sms | 100ms | 200 |
| `checkpoint` | (system) | 50ms | 0 |

**Example Response (DAG):**
```json
{
  "workflow_id": "wf-dag-12345",
  "type": "dag",
  "name": "DAG Workflow 12345",
  "tasks": [
    {
      "task_id": "task-a1b2c3",
      "type": "io_read",
      "name": "io_read_0",
      "depends_on": [],
      "tools_required": ["file_read", "database_query"],
      "expected_duration_ms": 180,
      "token_budget": 500,
      "priority": "high",
      "retryable": true,
      "max_retries": 3,
      "idempotent": true
    },
    {
      "task_id": "task-d4e5f6",
      "type": "compute",
      "name": "compute_1",
      "depends_on": ["task-a1b2c3"],
      "tools_required": ["code_execute", "data_transform"],
      "expected_duration_ms": 650,
      "token_budget": 1000,
      "priority": "normal",
      "chaos_injection": {
        "type": "tool_timeout",
        "description": "Tool invocation times out",
        "recovery_action": "retry",
        "trigger_probability": 0.7
      }
    }
  ],
  "entry_point": "task-a1b2c3",
  "exit_point": "task-g7h8i9",
  "__expected__": {
    "expected_status": "success_with_retries",
    "expected_execution_order": ["task-a1b2c3", "task-d4e5f6", "task-g7h8i9"],
    "expected_duration_range_ms": [1500, 6000],
    "expected_token_usage": 3500,
    "requires_human_approval": false,
    "checkpoints_expected": 0,
    "chaos_events_expected": 2,
    "critical_path_length": 3,
    "parallelizable_tasks": 1,
    "min_agents_required": 2
  }
}
```

---

### 3. Chaos Injection Framework

Simulate real-world failures to test AOA resilience.

**Endpoint:** `GET /api/agents/chaos-catalog`

**Chaos Types (9 operational types):**

| Type | Description | Affected Tasks | Recovery Action |
|------|-------------|----------------|-----------------|
| `tool_timeout` | Tool invocation times out | api_call, io_read, io_write | retry |
| `tool_failure` | Tool returns error | compute, api_call | compensate_or_fail |
| `agent_conflict` | Multiple agents produce conflicting outputs | decision, compute | adjudicate |
| `policy_violation` | Action blocked by policy engine | io_write, api_call | escalate_for_approval |
| `checkpoint_crash` | Process crashes after checkpoint | checkpoint | replay_from_checkpoint |
| `memory_pressure` | Context window exceeded | aggregation, decision | summarize_and_continue |
| `rate_limit` | Rate limit hit on tool/API | api_call | backoff_and_retry |
| `data_corruption` | Input data is malformed or inconsistent | io_read, compute | validate_and_repair |
| `network_partition` | Network connectivity lost temporarily | api_call, notification | queue_and_retry |

**Chaos Injection Schema:**
```json
{
  "chaos_injection": {
    "type": "tool_timeout",
    "description": "Tool invocation times out",
    "recovery_action": "retry",
    "trigger_probability": 0.7
  }
}
```

---

### 4. Stress Scenario Generation

Generate complete stress test packages combining agents, workflows, and chaos.

**Endpoint:** `GET /api/agents/stress-scenario`

**Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scale` | string | `small` | Agent fleet size |
| `workflow_count` | int | 5 | Number of workflows (1-50) |
| `chaos_rate` | float | 0.2 | Probability of chaos per task |
| `seed` | int | 12345 | Reproducibility seed |

**Response Structure:**
```json
{
  "scenario_id": "stress-12345-medium",
  "seed": 12345,
  "scale": "medium",
  "agents": {
    "fleet_id": "fleet-12345-medium",
    "total_agents": 50,
    "agents": [...],
    "distribution": {...}
  },
  "workflows": {
    "batch_id": "batch-13345",
    "workflow_count": 10,
    "total_tasks": 68,
    "chaos_events_total": 14,
    "workflows": [...]
  },
  "summary": {
    "total_agents": 50,
    "total_workflows": 10,
    "total_tasks": 68,
    "chaos_events_expected": 14,
    "chaos_rate": 0.2
  },
  "__expected__": {
    "all_workflows_assigned": true,
    "planner_count": 5,
    "worker_count": 25,
    "can_execute_all": true,
    "chaos_recovery_possible": true
  }
}
```

---

### 5. Workflow Batch Generation

Generate multiple workflows at once for batch testing.

**Endpoint:** `GET /api/agents/workflow-batch`

**Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `count` | int | 10 | Number of workflows (1-100) |
| `chaos_rate` | float | 0.1 | Chaos probability per task |
| `seed` | int | 12345 | Reproducibility seed |

---

### 6. Continuous Streaming

Stream workflows continuously for load testing.

**Endpoint:** `GET /api/agents/stream`

**Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rate` | int | 10 | Workflows per second (1-100) |
| `chaos_rate` | float | 0.1 | Chaos probability |
| `seed` | int | 12345 | Starting seed |

**Response:** NDJSON stream (newline-delimited JSON)

```bash
curl -N "http://localhost:5000/api/agents/stream?rate=50&chaos_rate=0.2"
```

Each line is a complete workflow JSON with `__expected__` block.

---

### 7. Agent Team Generation

Generate coordinated teams with complementary capabilities.

**Endpoint:** `GET /api/agents/team`

**Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `size` | int | 5 | Team size (3-10) |
| `seed` | int | 12345 | Reproducibility seed |

**Response includes:**
- Guaranteed planner + worker combination
- Collective capabilities coverage
- Collective tool coverage
- Team hierarchy structure

---

### 8. Push Integration (Farm → AOA)

Push stress tests directly to an AOA platform.

**Endpoint:** `POST /api/agents/run-stress-test`

**Request Body:**
```json
{
  "target_url": "https://your-aoa-platform.example.com",
  "scale": "medium",
  "workflow_count": 10,
  "chaos_rate": 0.2,
  "seed": 12345,
  "wait_for_completion": true
}
```

**Workflow:**
1. Farm generates fleet → POSTs to `{target_url}/api/v1/stress-test/fleet`
2. Farm generates scenario → POSTs to `{target_url}/api/v1/stress-test/scenario`
3. Farm polls `{target_url}/api/v1/stress-test/scenario/{id}` for results
4. Farm validates results against `__expected__` blocks
5. Farm generates operator-grade analysis

**Required AOA Endpoints:**

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/stress-test/fleet` | POST | Ingest agent fleet definitions |
| `/api/v1/stress-test/scenario` | POST | Submit stress test scenario |
| `/api/v1/stress-test/scenario/{id}` | GET | Return execution results |

---

### 9. Stress Test Run History

View historical stress test results.

**List Runs:** `GET /api/agents/stress-test-runs?limit=50`

**Get Run Details:** `GET /api/agents/stress-test-runs/{run_id}`

**Response includes:**
- Run metadata (scale, workflow_count, chaos_rate, seed)
- Fleet and scenario summaries
- Validation results
- Execution outcome and duration
- Status (completed, completed_with_failures, timeout, etc.)

---

## Tool Catalog

Agents are assigned tools from these categories:

| Category | Tools |
|----------|-------|
| `communication` | email, slack, teams, sms, calendar |
| `data` | database_query, file_read, file_write, api_fetch, data_transform |
| `code` | code_execute, code_review, test_runner, linter, deployer |
| `business` | jira, salesforce, hubspot, stripe, quickbooks |
| `ai` | llm_call, embedding, classifier, summarizer, translator |
| `infrastructure` | cloud_provision, container_deploy, dns_update, ssl_cert, monitoring |
| `security` | secret_fetch, audit_log, access_check, encryption, vulnerability_scan |
| `rpa` | web_scrape, form_fill, screenshot, click_action, keyboard_input |

---

## Validation System

### `__expected__` Blocks

Every generated object includes ground truth for validation:

```json
{
  "__expected__": {
    "expected_status": "success_with_retries",
    "expected_execution_order": ["task-a", "task-b", "task-c"],
    "expected_duration_range_ms": [1500, 6000],
    "expected_token_usage": 3500,
    "requires_human_approval": false,
    "chaos_events_expected": 2,
    "critical_path_length": 3,
    "min_agents_required": 2,
    "expected_completion_rate": 0.85
  }
}
```

### Validation Checks

| Check | Expected | Failure Indicates |
|-------|----------|-------------------|
| `completion_rate` | ≥ 95% pass, ≥ 80% degraded | Systemic failure |
| `chaos_recovery` | ≥ 80% pass, ≥ 50% degraded | Missing resilience |
| `task_completion` | ≥ 90% of total_tasks | Dropped tasks |
| `all_workflows_assigned` | true | Assignment bug |
| `can_execute_all` | true | Capacity issue |

### Analysis Verdicts

| Verdict | Criteria |
|---------|----------|
| `PASS` | All checks pass, completion ≥ 95% |
| `DEGRADED` | Some checks fail, completion 80-95% |
| `FAIL` | Critical failures, completion < 80% |

---

## Operator Analysis

Farm generates operator-grade analysis for each stress test:

```json
{
  "analysis": {
    "verdict": "PASS",
    "title": "Stress Test PASSED",
    "summary": "Platform handled 10 workflows with 14 chaos events. 95% completion rate with successful chaos recovery.",
    "sections": {
      "reliability": {
        "verdict": "PASS",
        "findings": [
          "Completion rate 95% exceeds 95% threshold",
          "All 14 chaos events recovered successfully"
        ]
      },
      "performance": {
        "verdict": "PASS",
        "findings": [
          "Throughput: 2.5 tasks/sec (above 1.0 minimum)",
          "Average latency: 850ms (under 1000ms target)"
        ]
      },
      "resilience": {
        "verdict": "PASS",
        "findings": [
          "Chaos recovery rate: 100%",
          "No circuit breakers triggered"
        ]
      }
    },
    "recommendations": [],
    "metrics": {
      "completion_rate": 0.95,
      "chaos_recovery_rate": 1.0,
      "throughput_tasks_per_sec": 2.5,
      "avg_latency_ms": 850
    }
  }
}
```

---

## Integration Patterns

### Pattern 1: Local Development Testing
```bash
# Generate scenario locally
curl "http://localhost:5000/api/agents/stress-scenario?scale=small&workflow_count=3"

# Parse and validate in your AOA implementation
```

### Pattern 2: CI/CD Integration
```bash
# Run stress test against staging
curl -X POST "http://localhost:5000/api/agents/run-stress-test" \
  -H "Content-Type: application/json" \
  -d '{"target_url":"https://staging.aoa.example.com","scale":"medium","workflow_count":20}'

# Check verdict in response
if [ "$verdict" != "PASS" ]; then exit 1; fi
```

### Pattern 3: Continuous Load Testing
```python
import requests
import json

# Stream workflows at 50/sec
response = requests.get(
    "http://localhost:5000/api/agents/stream?rate=50&chaos_rate=0.2",
    stream=True
)

for line in response.iter_lines():
    if line:
        workflow = json.loads(line)
        # Submit to AOA
        # Validate against workflow["__expected__"]
```

### Pattern 4: Chaos Engineering
```bash
# High chaos rate for resilience testing
curl "http://localhost:5000/api/agents/workflow?chaos_rate=0.5&num_tasks=10"

# Verify your AOA handles all chaos types
```

---

## Best Practices

1. **Use Seeds**: Always specify seeds for reproducible scenarios
2. **Start Small**: Begin with `scale=small` and `workflow_count=3`
3. **Validate Expected**: Compare actual results against `__expected__` blocks
4. **Handle All Chaos**: Implement handlers for every chaos type
5. **Monitor Metrics**: Track completion rate, latency, throughput
6. **Log Agent Assignments**: Track which agent handled which task
7. **Test Edge Cases**: Use high chaos_rate (0.5+) for resilience testing

---

## Environment Configuration

| Variable | Purpose | Default |
|----------|---------|---------|
| `PLATFORM_URL` | Default target for push integration | (none) |
| `DATABASE_URL` | PostgreSQL for run history | (required) |

---

## Status Codes

| Status | Description |
|--------|-------------|
| `completed` | All validation checks passed |
| `completed_with_failures` | Execution finished but validation failed |
| `fleet_ingestion_timeout` | Timed out posting fleet |
| `fleet_ingestion_failed` | Platform rejected fleet |
| `scenario_submission_timeout` | Timed out posting scenario |
| `scenario_submission_failed` | Platform rejected scenario |
| `timeout` | Execution did not complete in time |
| `execution_error` | Error during execution |

---

## Thresholds

| Metric | Pass | Degraded | Fail |
|--------|------|----------|------|
| Completion Rate | ≥ 95% | 80-95% | < 80% |
| Chaos Recovery | ≥ 80% | 50-80% | < 50% |
| Error Rate | < 5% | 5-15% | > 15% |
| Latency (avg) | < 1000ms | 1000-2000ms | > 2000ms |
| Throughput | > 1 task/sec | 0.5-1 task/sec | < 0.5 task/sec |

---

## Version

Protocol Version: 1.0

Last Updated: 2026-01-23
