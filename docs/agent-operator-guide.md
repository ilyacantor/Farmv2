# Agent Stress Testing - Operator Guide

## What Is This?

The Agent Stress Testing module helps you test how well your orchestration platform handles complex multi-agent workloads. Think of it like a flight simulator for your AI agent system - we generate realistic scenarios with multiple agents, tasks, and intentional chaos to see how your platform responds.

## Core Concepts

### Agent Fleet
A **fleet** is a group of AI agents with different roles, like a team in a company:

| Role | What They Do | % of Fleet |
|------|-------------|------------|
| **Planners** | Break down complex tasks, make decisions, delegate work | 10% |
| **Workers** | Execute tasks like sending emails, querying databases, writing code | 50% |
| **Specialists** | Handle specific domains (finance, legal, technical) | 20% |
| **Reviewers** | Check work quality, approve outputs | 15% |
| **Approvers** | Final sign-off on critical actions | 5% |

Each agent also has:
- **Reliability** - How often they succeed (60% to 99.9%)
- **Cost** - From free to enterprise-tier pricing
- **Capabilities** - What they can do (use tools, delegate, plan, etc.)

### Workflows
A **workflow** is a series of tasks that agents execute together. Types include:

| Type | Description | Use Case |
|------|-------------|----------|
| **Linear** | Tasks run one after another | Simple approval chains |
| **DAG** | Tasks with dependencies (some parallel) | Complex pipelines |
| **Parallel** | Multiple tasks run at the same time | Batch processing |
| **Saga** | Long-running with compensation (undo) logic | Transactions that might fail |

### Chaos Injection
We intentionally inject problems to test your platform's resilience:

| Chaos Type | What It Tests |
|------------|---------------|
| **Tool Timeout** | Does your platform retry correctly? |
| **Tool Failure** | Does it handle errors gracefully? |
| **Agent Conflict** | Can it resolve competing agents? |
| **Policy Violation** | Does escalation work? |
| **Checkpoint Crash** | Can it resume from failures? |
| **Rate Limit** | Does backoff logic work? |

## How to Use the UI

### 1. Generate Fleet
Creates a set of agents for testing. Choose a scale:
- **Small** (10 agents) - Quick unit tests
- **Medium** (50 agents) - Integration testing
- **Large** (100 agents) - Load testing

The result shows you agent distribution, reliability tiers, and capabilities.

### 2. Generate Workflow
Creates a single workflow with tasks. Options:
- **Type** - Linear, DAG, Parallel, or Saga
- **Tasks** - How many steps in the workflow
- **Chaos %** - How many tasks will have injected failures

The result shows the task flow with chaos indicators (lightning bolt icons).

### 3. Generate Stress Scenario
Creates a complete test package: fleet + multiple workflows + expected outcomes. This is what you'd typically use for real testing.

Options:
- **Scale** - Agent count (small/medium/large)
- **Workflows** - Number of concurrent workflows to run
- **Chaos %** - Overall chaos injection rate

The result includes an `__expected__` block with validation criteria.

### 4. Run Against Platform
Actually executes a stress test against your orchestration platform.

**Before running:**
1. Set `PLATFORM_URL` in your environment secrets
2. Your platform needs to implement these endpoints (see below)

**What happens:**
1. Farm generates a fleet and scenario
2. POSTs fleet to your platform's `/api/v1/stress-test/fleet`
3. POSTs scenario to `/api/v1/stress-test/scenario`
4. Polls for results and validates against expected outcomes

## Platform Requirements

Your orchestration platform needs to implement these endpoints:

### POST /api/v1/stress-test/fleet
Receives the agent fleet definition.

**Request body:**
```json
{
  "total_agents": 50,
  "agents": [
    {
      "agent_id": "agent-abc123",
      "type": "worker",
      "name": "DataProcessor-7",
      "capabilities": ["tool_invocation", "data_processing"],
      "reliability": "reliable",
      "cost_tier": "standard"
    }
  ]
}
```

**Expected response:**
```json
{
  "status": "ingested",
  "agent_count": 50
}
```

### POST /api/v1/stress-test/scenario
Receives the workflow scenario to execute.

**Request body:**
```json
{
  "scenario_id": "stress-12345-medium",
  "workflows": [...],
  "summary": {
    "total_workflows": 10,
    "total_tasks": 45,
    "chaos_events_expected": 9
  }
}
```

**Expected response:**
```json
{
  "scenario_id": "stress-12345-medium",
  "status": "running"
}
```

### GET /api/v1/stress-test/scenario/{scenario_id}
Returns current execution status.

**Expected response (running):**
```json
{
  "scenario_id": "stress-12345-medium",
  "status": "running",
  "progress": 0.45
}
```

**Expected response (completed):**
```json
{
  "scenario_id": "stress-12345-medium",
  "status": "completed",
  "completion_rate": 0.92,
  "tasks_completed": 42,
  "chaos_events_recovered": 7,
  "all_workflows_assigned": true,
  "can_execute_all": true
}
```

## Understanding Results

### Status Codes
| Status | Meaning |
|--------|---------|
| **completed** | All validation checks passed |
| **completed_with_failures** | Test ran but some checks failed |
| **fleet_ingestion_timeout** | Platform didn't accept fleet in time |
| **fleet_ingestion_failed** | Platform rejected the fleet |
| **scenario_submission_timeout** | Platform didn't accept scenario in time |
| **scenario_submission_failed** | Platform rejected the scenario |
| **timeout** | Scenario didn't complete in time |
| **execution_error** | Something went wrong during execution |

### Validation Checks
Farm validates your platform's results against expectations:

- **completion_rate** - Did enough tasks complete? (target: 85%+ with low chaos)
- **chaos_recovery** - Did platform recover from injected failures?
- **task_completion** - Were all expected tasks processed?
- **all_workflows_assigned** - Were agents assigned to every workflow?
- **can_execute_all** - Does the fleet have the right capabilities?

## Streaming Mode

For continuous load testing, use the Stream feature. It generates workflows continuously at your specified rate.

**Use the streaming URL** (`/api/agents/stream`) with tools like:
```bash
curl -N "https://your-farm-url.com/api/agents/stream?rate=10&chaos_rate=0.1"
```

Each line is a complete workflow in NDJSON format.

## Tips for Success

1. **Start small** - Begin with small scale and low chaos, then increase
2. **Check your endpoints** - Make sure your platform implements all required APIs
3. **Monitor logs** - Watch your platform's logs during stress tests
4. **Use deterministic seeds** - Same seed = same test, good for debugging
5. **Validate incrementally** - Fix one failure type before adding more chaos

## Troubleshooting

| Problem | Likely Cause | Solution |
|---------|-------------|----------|
| Fleet Upload Failed (404) | Platform missing endpoint | Implement `/api/v1/stress-test/fleet` |
| Fleet Upload Failed (500) | Platform error processing | Check platform logs |
| Scenario timeout | Tasks taking too long | Increase timeout or reduce scale |
| Low completion rate | Too much chaos | Lower chaos rate |
| Validation failures | Platform not returning expected fields | Check API response format |
