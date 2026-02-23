# AOA Monitoring UI Handoff - Receiving FARM Signals

## Overview

This document specifies what AOA needs to build to receive and display FARM stress test signals. FARM pushes test data to AOA endpoints; AOA needs a monitoring dashboard to visualize real-time test execution.

---

## What FARM Sends to AOA

### 1. Fleet Ingestion
```
POST /api/v1/stress-test/fleet
Content-Type: application/json
X-Tenant-ID: {tenant_id}

{
  "agents": [...],           // Array of agent profiles
  "total_agents": 50,
  "distribution": {
    "planner": 5,
    "worker": 25,
    "specialist": 10,
    "reviewer": 7,
    "approver": 3
  },
  "seed": 12345
}
```

### 2. Scenario Submission
```
POST /api/v1/stress-test/scenario
Content-Type: application/json
X-Tenant-ID: {tenant_id}

{
  "scenario_id": "stress-12345-medium",
  "agents": {...},
  "workflows": [...],        // Array of workflow DAGs
  "summary": {
    "total_agents": 50,
    "total_workflows": 5,
    "total_tasks": 68,
    "chaos_events_expected": 14,
    "chaos_rate": 0.2
  },
  "__expected__": {          // FARM's test oracle expectations
    "total_tasks": 68,
    "expected_completion_rate": 0.85,
    "chaos_recovery_possible": true,
    ...
  }
}
```

### 3. Individual Workflow Streaming
```
POST /api/v1/stress-test/workflow
Content-Type: application/json
X-Tenant-ID: {tenant_id}

{
  "workflow_id": "wf-abc123",
  "workflow_type": "dag",
  "tasks": [...],
  "chaos_injections": [...],
  "stream_sequence": 42       // For streaming tests
}
```

### 4. Dashboard Polling (FARM polls this)
```
GET /api/v1/aoa/dashboard
X-Tenant-ID: {tenant_id}

Response (AOA should return):
{
  "agents": {
    "active": 45,
    "total": 50,
    "by_type": {"planner": 5, "worker": 22, ...}
  },
  "workflows": {
    "active_workflows": 3,
    "completed": 12,
    "failed": 1,
    "pending": 2
  },
  "chaos": {
    "recovery_rate": 0.88,
    "events_triggered": 14,
    "events_recovered": 12
  },
  "costs": {
    "today_usd": 1.50,
    "session_usd": 0.35
  },
  "approvals": {
    "pending": 2,
    "approved_today": 15
  }
}
```

---

## AOA Monitoring Dashboard UI Specification

### Required UI Components

#### 1. Active Stress Test Monitor
Real-time display of incoming FARM stress tests.

```
┌─────────────────────────────────────────────────────────────────┐
│  FARM Stress Test Monitor                          [Connected] │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Current Test: stress-12345-medium                              │
│  Status: ● Running                                              │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Agents    │  │  Workflows  │  │   Tasks     │              │
│  │    45/50    │  │    3/5      │  │   42/68     │              │
│  │   active    │  │  running    │  │  completed  │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                 │
│  Chaos Recovery: ████████░░ 88%                                 │
│  Completion Rate: █████████░ 92%                                │
│                                                                 │
│  Recent Events:                                                 │
│  • 10:42:15 - Workflow wf-abc completed (12 tasks)              │
│  • 10:42:12 - Chaos: tool_timeout on task-xyz (recovered)       │
│  • 10:42:08 - Agent agent-007 started task-123                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 2. Fleet Ingestion Panel
Shows agents registered from FARM.

```
┌─────────────────────────────────────────────────────────────────┐
│  Fleet Status                                    Seed: 12345   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Distribution:                                                  │
│  ┌──────────┬────────┬─────────────────────────────────────┐   │
│  │ Type     │ Count  │ Status                              │   │
│  ├──────────┼────────┼─────────────────────────────────────┤   │
│  │ Planner  │   5    │ ●●●●● (all active)                  │   │
│  │ Worker   │  25    │ ●●●●●●●●●●●●●●●●●●●●●●○○○           │   │
│  │ Specialist│ 10    │ ●●●●●●●●●●                          │   │
│  │ Reviewer │   7    │ ●●●●●●●                             │   │
│  │ Approver │   3    │ ●●●                                 │   │
│  └──────────┴────────┴─────────────────────────────────────┘   │
│                                                                 │
│  Reliability Tiers:                                             │
│  rock_solid: 15  reliable: 25  flaky: 8  unreliable: 2         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 3. Workflow Execution Grid
Real-time workflow progress.

```
┌─────────────────────────────────────────────────────────────────┐
│  Workflow Execution                                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ wf-001 (dag)     ████████████████████ 100% ✓ Completed  │   │
│  │ 12 tasks, 0 chaos, 2.3s                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ wf-002 (saga)    ██████████████░░░░░░  72% ● Running    │   │
│  │ 15 tasks, 2 chaos (1 recovered), 4.1s                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ wf-003 (parallel) ████████░░░░░░░░░░░  45% ● Running    │   │
│  │ 8 tasks, 1 chaos (pending), 1.8s                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 4. Chaos Injection Monitor
Shows chaos events and recovery status.

```
┌─────────────────────────────────────────────────────────────────┐
│  Chaos Events                              Recovery: 88%        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┬─────────┬──────────┬──────────────────┐  │
│  │ Type             │ Count   │ Recovered│ Pending          │  │
│  ├──────────────────┼─────────┼──────────┼──────────────────┤  │
│  │ tool_timeout     │    5    │    5     │ -                │  │
│  │ tool_failure     │    3    │    2     │ 1 (retrying)     │  │
│  │ agent_conflict   │    2    │    2     │ -                │  │
│  │ policy_violation │    2    │    1     │ 1 (escalated)    │  │
│  │ rate_limit       │    2    │    2     │ -                │  │
│  └──────────────────┴─────────┴──────────┴──────────────────┘  │
│                                                                 │
│  Total: 14 injected, 12 recovered, 2 in progress                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 5. FARM Expectations Comparison
Shows FARM's `__expected__` vs actual results.

```
┌─────────────────────────────────────────────────────────────────┐
│  FARM Test Oracle Comparison                      Alignment: 95%│
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌────────────────────┬──────────┬──────────┬────────────────┐ │
│  │ Check              │ Expected │ Actual   │ Status         │ │
│  ├────────────────────┼──────────┼──────────┼────────────────┤ │
│  │ Completion Rate    │ ≥85%     │ 92%      │ ✓ PASS         │ │
│  │ Chaos Recovery     │ ≥80%     │ 88%      │ ✓ PASS         │ │
│  │ Task Completion    │ 68       │ 68       │ ✓ PASS         │ │
│  │ All Assigned       │ true     │ true     │ ✓ PASS         │ │
│  │ Can Execute All    │ true     │ true     │ ✓ PASS         │ │
│  └────────────────────┴──────────┴──────────┴────────────────┘ │
│                                                                 │
│  Discrepancies: None                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## AOA API Endpoints to Implement

### Required for FARM Integration

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/stress-test/fleet` | POST | Receive agent fleet from FARM |
| `/api/v1/stress-test/scenario` | POST | Receive full scenario with workflows |
| `/api/v1/stress-test/scenario/{id}` | GET | Return scenario result with verdict |
| `/api/v1/stress-test/workflow` | POST | Receive individual workflow (streaming) |
| `/api/v1/aoa/dashboard` | GET | Return current dashboard metrics |

### For UI Event Streaming

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/aoa/events` | GET (SSE) | Stream real-time events to UI |
| `/api/v1/aoa/metrics/timeseries` | GET | Historical metrics for charts |

---

## Event Stream Format

AOA should emit events via SSE for real-time UI updates.

### Event Types

```javascript
// Agent events
{ "type": "agent.registered", "agent_id": "...", "agent_type": "worker" }
{ "type": "agent.started_task", "agent_id": "...", "task_id": "..." }
{ "type": "agent.completed_task", "agent_id": "...", "task_id": "...", "duration_ms": 150 }

// Workflow events
{ "type": "workflow.started", "workflow_id": "...", "task_count": 12 }
{ "type": "workflow.progress", "workflow_id": "...", "completed": 8, "total": 12 }
{ "type": "workflow.completed", "workflow_id": "...", "status": "success", "duration_ms": 2300 }
{ "type": "workflow.failed", "workflow_id": "...", "error": "..." }

// Chaos events
{ "type": "chaos.injected", "chaos_type": "tool_timeout", "task_id": "...", "workflow_id": "..." }
{ "type": "chaos.recovered", "chaos_type": "tool_timeout", "task_id": "...", "recovery_action": "retry" }
{ "type": "chaos.failed", "chaos_type": "tool_failure", "task_id": "...", "error": "..." }

// Scenario events
{ "type": "scenario.started", "scenario_id": "...", "total_workflows": 5 }
{ "type": "scenario.progress", "scenario_id": "...", "completion_rate": 0.72 }
{ "type": "scenario.completed", "scenario_id": "...", "verdict": "PASS" }

// Approval events
{ "type": "approval.requested", "approval_id": "...", "agent_id": "...", "reason": "budget_exceeded" }
{ "type": "approval.granted", "approval_id": "...", "approver": "system" }
```

### SSE Connection Example

```javascript
// In AOA UI JavaScript
const eventSource = new EventSource('/api/v1/aoa/events');

eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);

    switch(data.type) {
        case 'workflow.progress':
            updateWorkflowProgress(data.workflow_id, data.completed, data.total);
            break;
        case 'chaos.injected':
            addChaosEvent(data);
            break;
        case 'chaos.recovered':
            markChaosRecovered(data.task_id);
            break;
        // ... handle other events
    }
};
```

---

## Scenario Result Format (AOA Returns to FARM)

When FARM polls `/api/v1/stress-test/scenario/{id}`, AOA should return:

```json
{
  "scenario_id": "stress-12345-medium",
  "status": "completed",
  "verdict": "PASS",
  "completion_rate": 0.92,
  "chaos_recovery_rate": 0.88,
  "validation": {
    "completion_rate": {"expected": 0.85, "actual": 0.92, "passed": true},
    "chaos_recovery": {"expected": 0.80, "actual": 0.88, "passed": true},
    "task_completion": {"expected_tasks": 68, "actual_tasks": 68, "passed": true}
  },
  "analysis": {
    "verdict": "PASS",
    "title": "Stress Test PASSED",
    "summary": "Platform achieved 92% completion rate with 88% chaos recovery.",
    "sections": {
      "reliability": {"verdict": "PASS", "findings": ["All retries succeeded"]},
      "performance": {"verdict": "PASS", "findings": ["Throughput: 2.5 tasks/sec"]},
      "resilience": {"verdict": "PASS", "findings": ["All chaos types handled"]}
    },
    "recommendations": [],
    "metrics": {
      "completion_rate": 0.92,
      "chaos_recovery_rate": 0.88,
      "throughput_tasks_per_sec": 2.5,
      "avg_latency_ms": 850
    }
  },
  "workflow_results": [
    {"workflow_id": "wf-001", "status": "completed", "tasks_completed": 12, "chaos_handled": 2},
    {"workflow_id": "wf-002", "status": "completed", "tasks_completed": 15, "chaos_handled": 3}
  ],
  "total_cost_usd": 0.15
}
```

---

## UI State Management

### Key State Variables for AOA Dashboard

```javascript
const dashboardState = {
    // Connection status
    connected: true,
    lastHeartbeat: Date.now(),

    // Current scenario
    currentScenario: {
        id: "stress-12345-medium",
        status: "running",
        startedAt: "2024-01-24T10:42:00Z",
        farmExpected: {...}     // FARM's __expected__ block
    },

    // Fleet status
    fleet: {
        total: 50,
        active: 45,
        byType: {planner: 5, worker: 25, ...},
        byReliability: {rock_solid: 15, reliable: 25, ...}
    },

    // Workflows
    workflows: {
        total: 5,
        completed: 2,
        running: 2,
        pending: 1,
        failed: 0,
        items: [...]           // Individual workflow states
    },

    // Tasks
    tasks: {
        total: 68,
        completed: 42,
        running: 5,
        pending: 21
    },

    // Chaos
    chaos: {
        expected: 14,
        injected: 14,
        recovered: 12,
        pending: 2,
        byType: {...},
        events: [...]          // Event log
    },

    // Metrics
    metrics: {
        completionRate: 0.62,
        chaosRecoveryRate: 0.86,
        throughput: 2.5,
        avgLatencyMs: 850
    },

    // Event log
    recentEvents: [...]        // Last N events for display
};
```

---

## Implementation Checklist for AOA

### Backend
- [ ] `POST /api/v1/stress-test/fleet` - Accept and store agent fleet
- [ ] `POST /api/v1/stress-test/scenario` - Accept scenario, start execution
- [ ] `GET /api/v1/stress-test/scenario/{id}` - Return result with verdict
- [ ] `POST /api/v1/stress-test/workflow` - Accept streaming workflows
- [ ] `GET /api/v1/aoa/dashboard` - Return current metrics
- [ ] `GET /api/v1/aoa/events` - SSE event stream
- [ ] Store FARM's `__expected__` block for comparison
- [ ] Calculate and return validation checks against expected

### UI Components
- [ ] Active Stress Test Monitor panel
- [ ] Fleet Ingestion panel with agent distribution
- [ ] Workflow Execution Grid with progress bars
- [ ] Chaos Injection Monitor with recovery tracking
- [ ] FARM Expectations Comparison table
- [ ] Real-time event log
- [ ] Connection status indicator
- [ ] Metrics summary cards

### Event Handling
- [ ] Connect to SSE endpoint
- [ ] Handle all event types
- [ ] Update UI state on events
- [ ] Show toast notifications for important events
- [ ] Auto-reconnect on disconnect

---

## Quick Test Commands

After implementing, test with:

```bash
# 1. Generate scenario from FARM
curl "http://farm:5000/api/agents/stress-scenario?scale=small&workflow_count=3"

# 2. POST to AOA (your new endpoints)
curl -X POST "http://aoa:8000/api/v1/stress-test/fleet" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: test" \
  -d @fleet.json

# 3. Watch events
curl -N "http://aoa:8000/api/v1/aoa/events" -H "X-Tenant-ID: test"

# 4. Check dashboard
curl "http://aoa:8000/api/v1/aoa/dashboard" -H "X-Tenant-ID: test"
```

---

*Last Updated: 2024-01-24*
