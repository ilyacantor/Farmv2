# AOS-Farm Integration Guide

## Overview

This guide explains how to integrate autonomOS services (AOD, AAM, DCL, Agent Orchestrator) with AOS-Farm for testing.

**Key Principle**: AOS-Farm communicates with all services **exclusively via HTTP APIs**. No shared databases, no direct code dependencies.

---

## Integration Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         AOS-Farm                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Orchestrator (HTTP Client)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│           │              │              │              │         │
│           ▼              ▼              ▼              ▼         │
│        HTTP           HTTP           HTTP           HTTP         │
└───────────┼──────────────┼──────────────┼──────────────┼─────────┘
            │              │              │              │
            ▼              ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐
     │   AOD    │   │   AAM    │   │   DCL    │   │    Agents    │
     └──────────┘   └──────────┘   └──────────┘   └──────────────┘
```

---

## Configuration

### Environment Variables

AOS-Farm requires base URLs for all services:

```bash
# autonomOS Service URLs
AOD_BASE_URL=http://localhost:8001
AAM_BASE_URL=http://localhost:8002
DCL_BASE_URL=http://localhost:8003
AGENT_ORCH_BASE_URL=http://localhost:8004

# Authentication (if needed)
AOD_API_KEY=your-key
AAM_API_KEY=your-key
DCL_API_KEY=your-key
AGENT_API_KEY=your-key
```

### Service Discovery

AOS-Farm uses simple base URL + endpoint pattern:

```javascript
const aodClient = new HttpClient(process.env.AOD_BASE_URL);
await aodClient.post('/api/discovery/run', payload);
```

---

## AOD Integration

### Required AOD Capabilities

1. **Discovery Trigger API**: Start discovery for a lab tenant
2. **Configuration API**: Point AOD to Farm's synthetic endpoints
3. **Metrics API**: Query discovery results

### Example Integration Flow

**1. Farm Initiates Discovery**

```http
POST {AOD_BASE_URL}/api/discovery/run
Content-Type: application/json

{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "sources": {
    "assets_api": "http://farm:3001/synthetic/assets",
    "events_api": "http://farm:3001/synthetic/events",
    "network_logs": "http://farm:3001/synthetic/logs"
  },
  "config": {
    "mode": "lab",
    "timeout_seconds": 300
  }
}
```

**Response**:
```json
{
  "discovery_id": "disc-12345",
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "started_at": "2025-01-22T14:30:00Z"
}
```

**2. Farm Polls for Status**

```http
GET {AOD_BASE_URL}/api/discovery/{discovery_id}/status
```

**Response**:
```json
{
  "discovery_id": "disc-12345",
  "status": "completed",
  "started_at": "2025-01-22T14:30:00Z",
  "completed_at": "2025-01-22T14:31:23Z"
}
```

**3. Farm Retrieves Metrics**

```http
GET {AOD_BASE_URL}/api/discovery/{discovery_id}/metrics
```

**Response**:
```json
{
  "assets_discovered": 98,
  "errors": 2,
  "duration_seconds": 83,
  "asset_breakdown": {
    "applications": 50,
    "services": 30,
    "databases": 18
  }
}
```

### AOD Implementation Requirements

**Minimum Required Endpoints**:
- `POST /api/discovery/run` - Start discovery
- `GET /api/discovery/{id}/status` - Check status
- `GET /api/discovery/{id}/metrics` - Get results

**Lab Mode Support**:
- Accept external source URLs (Farm's synthetic endpoints)
- Isolate runs by tenant_id
- Return structured metrics

**Alternative Pattern**:
If AOD doesn't expose a "run" API, Farm can:
1. Configure AOD to point at Farm's endpoints
2. Monitor AOD's internal metrics/database
3. Infer completion based on expected asset count

---

## AAM Integration

### Required AAM Capabilities

1. **Connector Registration API**: Register test connectors for a lab tenant
2. **Connector Status API**: Query connector health and metrics
3. **Lab Mode**: Isolate test runs from production

### Example Integration Flow

**1. Farm Registers Connectors**

```http
POST {AAM_BASE_URL}/api/connectors/register
Content-Type: application/json

{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "connectors": [
    {
      "id": "farm-crm",
      "type": "rest",
      "name": "Synthetic CRM",
      "url": "http://farm:3001/synthetic/crm",
      "auth": {
        "type": "header",
        "header": "X-Lab-Tenant-Id",
        "value": "lab-550e8400-e29b-41d4-a716-446655440000"
      },
      "polling_interval_seconds": 60
    },
    {
      "id": "farm-billing",
      "type": "rest",
      "name": "Synthetic Billing",
      "url": "http://farm:3001/synthetic/billing",
      "auth": {
        "type": "header",
        "header": "X-Lab-Tenant-Id",
        "value": "lab-550e8400-e29b-41d4-a716-446655440000"
      }
    }
  ]
}
```

**Response**:
```json
{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "connectors": [
    {
      "id": "farm-crm",
      "status": "registered",
      "registered_at": "2025-01-22T14:32:00Z"
    },
    {
      "id": "farm-billing",
      "status": "registered",
      "registered_at": "2025-01-22T14:32:00Z"
    }
  ]
}
```

**2. AAM Polls Synthetic Services**

AAM automatically polls the registered endpoints according to `polling_interval_seconds`.

Farm's synthetic services respond with data for the specified `lab_tenant_id`.

**3. Farm Queries AAM Metrics**

```http
GET {AAM_BASE_URL}/api/connectors/metrics?tenant_id=lab-550e8400-e29b-41d4-a716-446655440000
```

**Response**:
```json
{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "connectors": [
    {
      "id": "farm-crm",
      "requests_total": 234,
      "requests_success": 225,
      "requests_error": 9,
      "retries": 15,
      "avg_latency_ms": 187,
      "p95_latency_ms": 450,
      "last_poll_at": "2025-01-22T14:35:00Z",
      "availability_pct": 96.2
    },
    {
      "id": "farm-billing",
      "requests_total": 120,
      "requests_success": 118,
      "requests_error": 2,
      "retries": 3,
      "avg_latency_ms": 92,
      "availability_pct": 98.3
    }
  ]
}
```

**4. Farm Queries Schema Drift Events**

```http
GET {AAM_BASE_URL}/api/connectors/drift?tenant_id=lab-550e8400-e29b-41d4-a716-446655440000
```

**Response**:
```json
{
  "drift_events": [
    {
      "connector_id": "farm-crm",
      "detected_at": "2025-01-22T14:33:15Z",
      "change_type": "field_added",
      "field": "customer.loyalty_tier",
      "handled": true
    }
  ]
}
```

### AAM Implementation Requirements

**Minimum Required Endpoints**:
- `POST /api/connectors/register` - Register connectors
- `GET /api/connectors/metrics` - Get metrics
- `GET /api/connectors/drift` - Get schema drift events (optional)

**Lab Mode Support**:
- Accept arbitrary connector URLs (Farm's endpoints)
- Isolate by tenant_id
- Support custom auth headers (for tenant passing)

---

## DCL Integration

### Required DCL Capabilities

1. **Source Registration API**: Register data sources for a lab tenant
2. **Mapping Trigger API**: Run mapping/unification jobs
3. **Metrics API**: Query mapping results, conflicts, drift

### Example Integration Flow

**1. Farm Registers Sources**

```http
POST {DCL_BASE_URL}/api/sources/register
Content-Type: application/json

{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "sources": [
    {
      "id": "farm-crm-customers",
      "name": "CRM Customers",
      "type": "rest",
      "entity_type": "customer",
      "url": "http://farm:3001/synthetic/crm/customers",
      "auth": {
        "type": "header",
        "header": "X-Lab-Tenant-Id",
        "value": "lab-550e8400-e29b-41d4-a716-446655440000"
      }
    },
    {
      "id": "farm-billing-customers",
      "name": "Billing Customers",
      "type": "rest",
      "entity_type": "customer",
      "url": "http://farm:3001/synthetic/billing/customers",
      "auth": {
        "type": "header",
        "header": "X-Lab-Tenant-Id",
        "value": "lab-550e8400-e29b-41d4-a716-446655440000"
      }
    }
  ]
}
```

**Response**:
```json
{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "sources": [
    {
      "id": "farm-crm-customers",
      "status": "registered"
    },
    {
      "id": "farm-billing-customers",
      "status": "registered"
    }
  ]
}
```

**2. Farm Triggers Mapping**

```http
POST {DCL_BASE_URL}/api/mapping/run
Content-Type: application/json

{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "entity_types": ["customer"],
  "config": {
    "conflict_resolution": "most_recent",
    "mode": "lab"
  }
}
```

**Response**:
```json
{
  "mapping_job_id": "map-67890",
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "started_at": "2025-01-22T14:33:00Z"
}
```

**3. Farm Polls for Completion**

```http
GET {DCL_BASE_URL}/api/mapping/{job_id}/status
```

**Response**:
```json
{
  "mapping_job_id": "map-67890",
  "status": "completed",
  "completed_at": "2025-01-22T14:34:30Z"
}
```

**4. Farm Retrieves Metrics**

```http
GET {DCL_BASE_URL}/api/mapping/{job_id}/metrics
```

**Response**:
```json
{
  "mapping_job_id": "map-67890",
  "entities_processed": 200,
  "entities_mapped": 195,
  "conflicts_detected": 12,
  "conflicts_resolved": 10,
  "unmapped_fields": 8,
  "drift_events": 3,
  "mapping_coverage_pct": 97.5,
  "duration_seconds": 90
}
```

### DCL Implementation Requirements

**Minimum Required Endpoints**:
- `POST /api/sources/register` - Register sources
- `POST /api/mapping/run` - Start mapping job
- `GET /api/mapping/{id}/status` - Check status
- `GET /api/mapping/{id}/metrics` - Get results

**Lab Mode Support**:
- Accept external source URLs (Farm's endpoints)
- Isolate by tenant_id
- Return structured conflict and drift information

---

## Agent Orchestrator Integration

### Required Capabilities

1. **Agent Trigger API**: Run agents for a lab tenant
2. **Results API**: Query recommendations and actions

### Example Integration Flow

**1. Farm Triggers Agents**

```http
POST {AGENT_ORCH_BASE_URL}/api/agents/run
Content-Type: application/json

{
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "mode": "lab",
  "config": {
    "timeout_seconds": 600
  }
}
```

**Response**:
```json
{
  "agent_run_id": "agent-11111",
  "tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "started_at": "2025-01-22T14:35:00Z"
}
```

**2. Farm Polls for Completion**

```http
GET {AGENT_ORCH_BASE_URL}/api/agents/{run_id}/status
```

**Response**:
```json
{
  "agent_run_id": "agent-11111",
  "status": "completed",
  "completed_at": "2025-01-22T14:36:00Z"
}
```

**3. Farm Retrieves Summary**

```http
GET {AGENT_ORCH_BASE_URL}/api/agents/{run_id}/summary
```

**Response**:
```json
{
  "agent_run_id": "agent-11111",
  "recommendations": 12,
  "actions_proposed": 4,
  "errors": 0,
  "duration_seconds": 60,
  "top_recommendations": [
    {
      "type": "cost_optimization",
      "description": "Consolidate underutilized databases",
      "priority": "high"
    }
  ]
}
```

### Agent Orchestrator Implementation Requirements

**Minimum Required Endpoints**:
- `POST /api/agents/run` - Trigger agents
- `GET /api/agents/{id}/status` - Check status
- `GET /api/agents/{id}/summary` - Get results

**Lab Mode Support**:
- Use DCL-unified data for the given tenant_id
- Return high-level summary (detailed recommendations optional)

---

## Error Handling

### Retry Logic

AOS-Farm implements retries for transient failures:

```javascript
async function callWithRetry(fn, maxRetries = 3) {
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

function isRetryable(err) {
  return err.statusCode >= 500 || err.code === 'ECONNRESET';
}
```

### Timeout Handling

All HTTP calls have configurable timeouts:

```javascript
const response = await httpClient.post('/api/discovery/run', payload, {
  timeout: 30000 // 30 seconds
});
```

### Error Propagation

When an external service fails:
1. Farm logs the error with context
2. Marks the run as `failed`
3. Records partial metrics (what completed before failure)
4. Returns error details to the UI

---

## Testing Your Integration

### Step 1: Verify Service Endpoints

```bash
# Check if services are reachable
curl http://localhost:8001/health  # AOD
curl http://localhost:8002/health  # AAM
curl http://localhost:8003/health  # DCL
curl http://localhost:8004/health  # Agents
```

### Step 2: Test Manual Integration

**Example: Call AAM manually**

```bash
curl -X POST http://localhost:8002/api/connectors/register \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "test-123",
    "connectors": [{
      "id": "test-conn",
      "type": "rest",
      "url": "http://farm:3001/synthetic/crm"
    }]
  }'
```

### Step 3: Run Simple Scenario

1. Start AOS-Farm backend
2. Run a small, clean scenario
3. Check logs for HTTP calls
4. Verify metrics are collected

### Step 4: Debug Failed Calls

Enable debug logging:

```bash
export LOG_LEVEL=debug
export LOG_HTTP_REQUESTS=true
```

Review logs for:
- Request/response bodies
- Status codes
- Timing information

---

## Common Integration Issues

### Issue: Service Not Reachable

**Symptoms**: Connection refused, timeout

**Solutions**:
- Verify service is running
- Check network connectivity (Docker networks, firewall)
- Verify base URL is correct

### Issue: Authentication Failures

**Symptoms**: 401, 403 errors

**Solutions**:
- Check API keys in environment variables
- Verify auth headers are correct
- Test auth independently

### Issue: Tenant Isolation Not Working

**Symptoms**: Wrong data returned, cross-tenant leakage

**Solutions**:
- Verify `tenant_id` is passed correctly
- Check service filters by tenant_id
- Review service logs for tenant_id usage

### Issue: Metrics Not Collected

**Symptoms**: Missing or partial metrics

**Solutions**:
- Verify metrics endpoints return expected format
- Check for errors during metrics collection
- Review Farm logs for parsing errors

---

## Best Practices

1. **Version Your APIs**: Include version in path (`/api/v1/...`)
2. **Document Contracts**: Clear OpenAPI/Swagger specs
3. **Use Standard Formats**: JSON for all payloads
4. **Return Structured Errors**: Consistent error format
5. **Support Filtering**: Allow tenant_id filtering on all endpoints
6. **Idempotency**: Support retries safely
7. **Timeouts**: Set reasonable defaults
8. **Health Checks**: Expose `/health` endpoint
9. **Logging**: Log all Farm-initiated requests
10. **Monitoring**: Track integration health metrics

---

## Future Enhancements

- **Service Discovery**: Automatic endpoint discovery
- **Contract Testing**: Automated API contract validation
- **Mock Mode**: Farm can mock services for testing
- **Webhooks**: Services push updates to Farm
- **GraphQL Support**: Alternative to REST
- **gRPC Support**: For high-performance scenarios
