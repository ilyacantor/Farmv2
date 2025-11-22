# Orchestrator Module

## Purpose

The Orchestrator is the **central coordinator** for test runs. It manages the entire lifecycle of E2E and module-specific test scenarios.

## Responsibilities

1. **Run Lifecycle Management**
   - Create new runs with unique `lab_tenant_id`
   - Track run status (pending → running → success/failed)
   - Handle timeouts and cancellations

2. **Flow Coordination**
   - **E2E Flow**: AOD → AAM → DCL → Agents
   - **AAM Module Flow**: AAM testing only
   - **DCL Module Flow**: DCL testing only

3. **Service Integration**
   - Call AOD, AAM, DCL, and Agent Orchestrator APIs
   - Handle retries and error recovery
   - Propagate tenant context to external services

4. **Metrics Collection**
   - Gather metrics from each pipeline stage
   - Aggregate and store in `farm_runs.metrics`
   - Calculate pass/fail based on expected outcomes

## Key Components

### RunOrchestrator

Main orchestration engine:

```typescript
class RunOrchestrator {
  async startRun(scenarioId: string, overrides?: any): Promise<Run>;
  async getRunStatus(runId: string): Promise<RunStatus>;
  async getRunMetrics(runId: string): Promise<RunMetrics>;
  async cancelRun(runId: string): Promise<void>;
}
```

### E2ERunner

Executes end-to-end scenarios:

```typescript
class E2ERunner {
  async execute(run: Run, scenario: Scenario): Promise<void> {
    // 1. Generate synthetic data
    await this.generateSyntheticData(run.lab_tenant_id, scenario.config);

    // 2. Run AOD discovery
    const aodMetrics = await this.runAOD(run.lab_tenant_id);

    // 3. Configure and run AAM
    const aamMetrics = await this.runAAM(run.lab_tenant_id, scenario.config);

    // 4. Run DCL mapping
    const dclMetrics = await this.runDCL(run.lab_tenant_id, scenario.config);

    // 5. Run Agents
    const agentsMetrics = await this.runAgents(run.lab_tenant_id);

    // 6. Aggregate metrics and determine success
    await this.finalizeRun(run.id, { aod, aam, dcl, agents });
  }
}
```

### ModuleRunner

Executes module-specific scenarios (AAM or DCL):

```typescript
class ModuleRunner {
  async executeAAM(run: Run, scenario: Scenario): Promise<void> {
    // 1. Generate synthetic upstream systems
    await this.generateSyntheticServices(run.lab_tenant_id, scenario.config);

    // 2. Register connectors in AAM
    await this.registerAAMConnectors(run.lab_tenant_id, scenario.config);

    // 3. Wait for AAM to process
    await this.waitForAAMCompletion(run.lab_tenant_id);

    // 4. Collect AAM metrics
    const metrics = await this.collectAAMMetrics(run.lab_tenant_id);

    // 5. Finalize run
    await this.finalizeRun(run.id, { aam: metrics });
  }

  async executeDCL(run: Run, scenario: Scenario): Promise<void> {
    // Similar flow for DCL
  }
}
```

### MetricsCollector

Collects metrics from external services:

```typescript
class MetricsCollector {
  async collectAODMetrics(tenantId: string): Promise<AODMetrics>;
  async collectAAMMetrics(tenantId: string): Promise<AAMMetrics>;
  async collectDCLMetrics(tenantId: string): Promise<DCLMetrics>;
  async collectAgentsMetrics(tenantId: string): Promise<AgentsMetrics>;
}
```

---

## Run Flow Examples

### E2E Run Flow

```
1. User requests E2E run
   ↓
2. Orchestrator creates farm_runs record (status: pending)
   ↓
3. Generate lab_tenant_id
   ↓
4. E2ERunner.execute()
   ├─→ SyntheticDataEngine.generate() [assets, customers, events]
   ├─→ SyntheticServices.start() [expose endpoints with chaos]
   ├─→ Call AOD API with tenant_id
   │   └─→ Poll for completion
   │   └─→ Collect AOD metrics
   ├─→ Call AAM API to register connectors
   │   └─→ AAM pulls from SyntheticServices
   │   └─→ Collect AAM metrics (retries, errors, drift)
   ├─→ Call DCL API to run mapping
   │   └─→ DCL processes multi-source data
   │   └─→ Collect DCL metrics (conflicts, coverage)
   └─→ Call Agents API
       └─→ Collect recommendations count
   ↓
5. Aggregate all metrics
   ↓
6. Evaluate against expected_outcomes
   ↓
7. Update farm_runs (status: success/failed, metrics, completed_at)
   ↓
8. Return results to UI
```

### AAM Module Run Flow

```
1. User requests AAM-only run
   ↓
2. Orchestrator creates farm_runs record (module: aam)
   ↓
3. ModuleRunner.executeAAM()
   ├─→ Generate synthetic upstream APIs (CRM, Billing, etc.)
   ├─→ Apply chaos configuration (latency, errors, schema drift)
   ├─→ Call AAM API to register connectors
   │   └─→ AAM polls synthetic endpoints
   ├─→ Monitor AAM behavior (retries, error handling)
   └─→ Collect AAM-specific metrics
   ↓
4. Update farm_runs with AAM metrics
   ↓
5. Return results
```

---

## Error Handling

### Retry Strategy

```typescript
async function callWithRetry(fn: () => Promise<any>, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i === maxRetries - 1) throw err;
      if (!isRetryable(err)) throw err;
      await delay(Math.pow(2, i) * 1000); // Exponential backoff
    }
  }
}

function isRetryable(err: Error): boolean {
  // Retry on network errors and 5xx
  return err.code === 'ECONNRESET' ||
         err.statusCode >= 500;
}
```

### Partial Failure Handling

If a stage fails mid-run:
- Mark run as `failed`
- Store partial metrics (what completed before failure)
- Record error in `farm_runs.error_message`
- Clean up resources (optional: delete synthetic data)

---

## Configuration

### Timeouts

```javascript
const config = {
  aod_timeout_ms: 300000,      // 5 minutes
  aam_timeout_ms: 600000,      // 10 minutes
  dcl_timeout_ms: 600000,      // 10 minutes
  agents_timeout_ms: 600000,   // 10 minutes
  total_run_timeout_ms: 1800000 // 30 minutes
};
```

### Concurrency

```javascript
const MAX_CONCURRENT_RUNS = 5;
```

If more than 5 runs are requested, queue them.

---

## Integration with Other Modules

### SyntheticDataEngine

```typescript
await syntheticDataEngine.generate(
  run.lab_tenant_id,
  scenario.config.scale,
  scenario.config.chaos
);
```

### SyntheticServices

```typescript
await syntheticServices.startForTenant(
  run.lab_tenant_id,
  scenario.config.synthetic_services
);
```

### External Service Clients

```typescript
const aodClient = new AODClient(process.env.AOD_BASE_URL);
const result = await aodClient.startDiscovery({
  tenant_id: run.lab_tenant_id,
  sources: syntheticServices.getEndpoints(run.lab_tenant_id)
});
```

---

## Testing

### Unit Tests

Test individual runner methods:

```typescript
test('E2ERunner generates synthetic data', async () => {
  const runner = new E2ERunner(mockEngine, mockServices);
  const run = { lab_tenant_id: 'test-123', ... };
  await runner.execute(run, scenario);
  expect(mockEngine.generate).toHaveBeenCalledWith('test-123', ...);
});
```

### Integration Tests

Test with real Supabase and mocked external services:

```typescript
test('E2E run completes successfully', async () => {
  const orchestrator = new RunOrchestrator();
  const run = await orchestrator.startRun('e2e-small-clean');

  // Wait for completion
  await waitForRunCompletion(run.id);

  const status = await orchestrator.getRunStatus(run.id);
  expect(status.status).toBe('success');
  expect(status.metrics.aod).toBeDefined();
});
```

---

## Logging

Log key events with correlation:

```typescript
logger.info('Starting E2E run', {
  runId: run.id,
  tenantId: run.lab_tenant_id,
  scenarioId: scenario.id
});

logger.info('AOD discovery started', {
  runId: run.id,
  discoveryId: aodResult.discovery_id
});

logger.error('AAM connector registration failed', {
  runId: run.id,
  error: err.message
});
```

---

## Future Enhancements

- **Parallel Stage Execution**: Run AAM and DCL in parallel if independent
- **Run Queuing**: Queue system for managing concurrent runs
- **Webhooks**: Notify external systems on run completion
- **Run Cancellation**: Allow users to cancel in-progress runs
- **Partial Retry**: Retry individual stages without re-running entire flow
