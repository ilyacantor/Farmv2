# AOS Farm: Agent Orchestration Stress Testing Guide

## Overview

AOS Farm generates synthetic agent profiles and workflow graphs for stress testing agentic orchestration platforms. This guide explains the data contracts, connection points, and expected behaviors for platforms consuming this data.

---

## Connection Points

### Base URL
```
https://<your-farm-instance>/api/agents
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/fleet` | GET | Generate agent fleet with profiles |
| `/workflow` | GET | Generate single workflow with tasks |
| `/stress-scenario` | GET | Complete test package (agents + workflows) |
| `/stream` | GET | Continuous NDJSON workflow stream |

---

## Agent Fleet Protocol

### Request
```
GET /api/agents/fleet?scale=medium&seed=12345
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scale` | string | `medium` | Fleet size: `small` (10), `medium` (50), `large` (100) |
| `seed` | int | random | Deterministic seed for reproducibility |

### Response Schema
```json
{
  "agents": [
    {
      "agent_id": "agent_planner_001",
      "type": "planner",
      "capabilities": ["task_decomposition", "delegation", "priority_assignment"],
      "tools": ["jira", "calendar", "email"],
      "reliability": {
        "profile": "reliable",
        "success_rate": 0.95,
        "mean_latency_ms": 150,
        "timeout_rate": 0.02
      },
      "cost": {
        "tier": "standard",
        "per_invocation": 0.01,
        "per_token": 0.00002
      },
      "policy": {
        "template": "standard",
        "max_concurrent_tasks": 5,
        "requires_approval_above": 1000,
        "audit_all_actions": false
      },
      "metadata": {
        "version": "1.0",
        "created_at": "2026-01-20T10:00:00Z"
      }
    }
  ],
  "total_agents": 50,
  "distribution": {
    "by_type": {"planner": 5, "worker": 25, "specialist": 10, "reviewer": 8, "approver": 2},
    "by_reliability": {"rock_solid": 5, "reliable": 30, "flaky": 12, "unreliable": 3},
    "by_cost": {"free": 10, "cheap": 15, "standard": 15, "premium": 8, "enterprise": 2}
  },
  "seed": 12345
}
```

### Agent Types

| Type | Distribution | Role |
|------|-------------|------|
| `planner` | 10% | Decomposes complex tasks, assigns to workers |
| `worker` | 50% | Executes individual tasks |
| `specialist` | 20% | Domain-specific expertise (code, data, security) |
| `reviewer` | 15% | Validates outputs, quality gates |
| `approver` | 5% | Final authorization for sensitive operations |

### Capabilities Matrix

| Capability | Description | Typical Agents |
|------------|-------------|----------------|
| `task_decomposition` | Break complex work into subtasks | planner |
| `delegation` | Assign tasks to other agents | planner, reviewer |
| `tool_invocation` | Call external tools/APIs | worker, specialist |
| `code_execution` | Run code in sandboxed environments | specialist |
| `data_analysis` | Process and analyze datasets | specialist |
| `human_escalation` | Escalate to human operators | reviewer, approver |
| `approval_authority` | Authorize high-value actions | approver |

### Reliability Profiles

| Profile | Success Rate | Latency (ms) | Timeout Rate |
|---------|-------------|--------------|--------------|
| `rock_solid` | 99.9% | 50 | 0.1% |
| `reliable` | 95% | 150 | 2% |
| `flaky` | 80% | 500 | 10% |
| `unreliable` | 60% | 2000 | 25% |

### Cost Tiers

| Tier | Per Invocation | Per Token |
|------|----------------|-----------|
| `free` | $0.00 | $0.00 |
| `cheap` | $0.001 | $0.000005 |
| `standard` | $0.01 | $0.00002 |
| `premium` | $0.05 | $0.0001 |
| `enterprise` | $0.10 | $0.0002 |

---

## Workflow Protocol

### Request
```
GET /api/agents/workflow?workflow_type=dag&num_tasks=8&chaos_rate=0.2
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workflow_type` | string | `dag` | Topology: `linear`, `dag`, `parallel`, `saga` |
| `num_tasks` | int | 6 | Number of tasks (2-20) |
| `chaos_rate` | float | 0.1 | Probability of chaos injection per task (0-1) |
| `seed` | int | random | Deterministic seed |

### Response Schema
```json
{
  "workflow_id": "wf_abc123",
  "type": "dag",
  "tasks": [
    {
      "task_id": "task_001",
      "name": "fetch_customer_data",
      "type": "data_retrieval",
      "dependencies": [],
      "assigned_agent_type": "worker",
      "tools_required": ["database_query"],
      "estimated_duration_ms": 500,
      "priority": "high",
      "retry_policy": {
        "max_attempts": 3,
        "backoff_multiplier": 2
      },
      "chaos_injection": null
    },
    {
      "task_id": "task_002",
      "name": "validate_permissions",
      "type": "validation",
      "dependencies": ["task_001"],
      "assigned_agent_type": "reviewer",
      "tools_required": ["policy_check"],
      "estimated_duration_ms": 200,
      "priority": "high",
      "chaos_injection": {
        "type": "tool_timeout",
        "trigger_probability": 0.3,
        "parameters": {"delay_ms": 5000}
      }
    }
  ],
  "__expected__": {
    "total_tasks": 8,
    "critical_path_length": 5,
    "parallelizable_tasks": 3,
    "chaos_events_expected": 2,
    "expected_chaos_types": ["tool_timeout", "agent_conflict"],
    "min_agents_required": 4,
    "estimated_total_duration_ms": 3500
  },
  "metadata": {
    "generated_at": "2026-01-20T10:00:00Z",
    "seed": 12345
  }
}
```

### Workflow Types

| Type | Description | Use Case |
|------|-------------|----------|
| `linear` | Sequential A→B→C chain | Simple pipelines |
| `dag` | Directed acyclic graph with branches | Complex dependencies |
| `parallel` | Fan-out/fan-in pattern | Batch processing |
| `saga` | Includes compensation tasks | Transactional workflows |

### Task Types

| Type | Description |
|------|-------------|
| `data_retrieval` | Fetch data from external sources |
| `transformation` | Process or transform data |
| `validation` | Check data quality or permissions |
| `notification` | Send alerts or messages |
| `approval` | Human or agent approval gate |
| `compensation` | Rollback/undo for saga patterns |

---

## Chaos Injection Protocol

Chaos events simulate real-world failures for resilience testing.

### Chaos Types

| Type | Description | Expected Handling |
|------|-------------|-------------------|
| `tool_timeout` | Tool call exceeds timeout | Retry with backoff |
| `tool_failure` | Tool returns error | Fallback or compensation |
| `agent_conflict` | Multiple agents claim same task | Conflict resolution |
| `policy_violation` | Action blocked by policy | Escalation path |
| `checkpoint_crash` | Mid-workflow failure | Durable execution replay |
| `memory_pressure` | Agent memory limits exceeded | Graceful degradation |
| `rate_limit` | API rate limit hit | Backoff strategy |
| `data_corruption` | Invalid data in pipeline | Validation and repair |
| `network_partition` | Connectivity loss | Retry or circuit breaker |

### Chaos Event Schema
```json
{
  "type": "tool_timeout",
  "trigger_probability": 0.3,
  "parameters": {
    "delay_ms": 5000,
    "recoverable": true
  }
}
```

---

## Stress Scenario Protocol

### Request
```
GET /api/agents/stress-scenario?scale=medium&workflow_count=10&chaos_rate=0.2
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scale` | string | `medium` | Agent fleet size |
| `workflow_count` | int | 10 | Number of workflows to generate |
| `chaos_rate` | float | 0.1 | Global chaos probability |
| `seed` | int | random | Deterministic seed |

### Response Schema
```json
{
  "agents": { /* Fleet object */ },
  "workflows": [ /* Array of workflow objects */ ],
  "agent_assignments": {
    "wf_001": {
      "task_001": "agent_worker_003",
      "task_002": "agent_reviewer_001"
    }
  },
  "__expected__": {
    "total_agents": 50,
    "total_workflows": 10,
    "total_tasks": 72,
    "total_chaos_events": 14,
    "expected_failures": {
      "tool_timeout": 4,
      "tool_failure": 3,
      "agent_conflict": 2,
      "policy_violation": 2,
      "checkpoint_crash": 1,
      "rate_limit": 2
    },
    "min_throughput_wf_per_sec": 5,
    "expected_completion_rate": 0.85
  },
  "summary": {
    "total_agents": 50,
    "total_workflows": 10,
    "total_tasks": 72,
    "total_chaos_events": 14
  },
  "metadata": {
    "generated_at": "2026-01-20T10:00:00Z",
    "seed": 12345
  }
}
```

---

## Streaming Protocol

### Request
```
GET /api/agents/stream?rate=50&chaos_rate=0.1
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rate` | int | 10 | Workflows per second (1-100) |
| `chaos_rate` | float | 0.1 | Chaos injection probability |

### Response Format
Newline-delimited JSON (NDJSON) stream:
```
{"workflow_id":"wf_001","type":"dag","tasks":[...],"__expected__":{...}}
{"workflow_id":"wf_002","type":"linear","tasks":[...],"__expected__":{...}}
{"workflow_id":"wf_003","type":"parallel","tasks":[...],"__expected__":{...}}
```

### Consumption Pattern
```python
import requests
import json

response = requests.get(
    "https://farm.example.com/api/agents/stream?rate=50",
    stream=True
)

for line in response.iter_lines():
    if line:
        workflow = json.loads(line)
        # Process workflow
        orchestrator.submit(workflow)
```

---

## Validation Protocol

### Expected Blocks (`__expected__`)

Every generated object includes an `__expected__` block containing ground truth for validation:

```json
{
  "__expected__": {
    "total_tasks": 8,
    "chaos_events_expected": 2,
    "expected_chaos_types": ["tool_timeout", "agent_conflict"],
    "min_agents_required": 4,
    "expected_completion_rate": 0.85
  }
}
```

### Validation Checklist

| Check | Expected | Failure Indicates |
|-------|----------|-------------------|
| All tasks completed | Match `total_tasks` | Dropped tasks |
| Chaos handled | Recovered from all | Missing retry logic |
| Agent utilization | ≥ `min_agents_required` | Assignment bug |
| Completion rate | ≥ `expected_completion_rate` | Systemic failure |
| Order preserved | Dependencies respected | DAG violation |

---

## Integration Patterns

### Pattern 1: Batch Testing
```
1. GET /api/agents/stress-scenario?scale=large&workflow_count=50
2. Parse agents and workflows
3. Initialize orchestrator with agent fleet
4. Submit all workflows
5. Compare results against __expected__ blocks
6. Generate test report
```

### Pattern 2: Continuous Load Testing
```
1. GET /api/agents/fleet?scale=medium
2. Initialize orchestrator with agents
3. Connect to /api/agents/stream?rate=100
4. Process workflows continuously
5. Monitor throughput and error rates
6. Validate against __expected__ blocks in real-time
```

### Pattern 3: Chaos Engineering
```
1. GET /api/agents/workflow?chaos_rate=0.5
2. Inject workflow into orchestrator
3. Observe failure handling
4. Verify all chaos events were:
   - Detected
   - Logged
   - Recovered or escalated
5. Compare actual vs expected chaos types
```

---

## Error Handling

### HTTP Status Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | Success | Process response |
| 400 | Invalid parameters | Check request |
| 429 | Rate limited | Back off and retry |
| 500 | Server error | Retry with backoff |

### Error Response Schema
```json
{
  "error": "INVALID_PARAMETER",
  "message": "scale must be one of: small, medium, large",
  "parameter": "scale",
  "received": "huge"
}
```

---

## Best Practices

1. **Use Seeds for Reproducibility**: Always specify a seed when debugging specific scenarios
2. **Start Small**: Begin with `scale=small` and `workflow_count=5` before scaling up
3. **Validate Expected Blocks**: Always compare actual results against `__expected__`
4. **Handle All Chaos Types**: Ensure your orchestrator handles every chaos type listed
5. **Monitor Streaming**: Implement backpressure when consuming the stream endpoint
6. **Log Agent Assignments**: Track which agent handled which task for debugging

---

## Versioning

Current protocol version: `1.0`

The `metadata.version` field in responses indicates the protocol version. Breaking changes will increment the major version.

---

## Support

For issues or feature requests, contact the AOS Farm team or file an issue in the repository.
