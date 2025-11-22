# AOS-Farm API Specification

## Overview

The AOS-Farm Orchestrator API provides REST endpoints for managing scenarios, orchestrating test runs, and querying results. All endpoints return JSON responses.

**Base URL**: `http://localhost:3001/api` (configurable via `FARM_PORT`)

**Authentication**: TBD (session-based or token-based)

---

## API Endpoints

### Scenarios

#### List All Scenarios

```
GET /api/scenarios
```

**Query Parameters**:
- `type` (optional): Filter by scenario type (`e2e`, `module`)
- `module` (optional): Filter by module (`aam`, `dcl`)
- `tags` (optional): Comma-separated tags to filter by

**Response** (200 OK):
```json
{
  "scenarios": [
    {
      "id": "e2e-small-clean",
      "name": "Small Clean Enterprise",
      "description": "Small enterprise with clean data, no chaos",
      "type": "e2e",
      "module": null,
      "tags": ["small", "clean"],
      "config": {
        "scale": "small",
        "chaos_profile": "none"
      }
    },
    {
      "id": "aam-high-latency",
      "name": "High Latency Connectors",
      "description": "Test AAM with slow, unreliable sources",
      "type": "module",
      "module": "aam",
      "tags": ["medium", "high-latency"],
      "config": {
        "scale": "medium",
        "chaos_profile": "latency"
      }
    }
  ]
}
```

---

#### Get Scenario Details

```
GET /api/scenarios/:id
```

**Path Parameters**:
- `id`: Scenario identifier

**Response** (200 OK):
```json
{
  "id": "e2e-small-clean",
  "name": "Small Clean Enterprise",
  "description": "Small enterprise with clean data, no chaos",
  "type": "e2e",
  "module": null,
  "tags": ["small", "clean"],
  "config": {
    "scale": {
      "assets": 100,
      "customers": 200,
      "events_per_day": 5000
    },
    "chaos": {
      "response_chaos": {
        "latency_ms": { "p50": 50, "p95": 100, "p99": 200 },
        "error_rate_pct": 0,
        "timeout_rate_pct": 0
      },
      "schema_chaos": {
        "field_add_pct": 0,
        "field_remove_pct": 0
      },
      "data_chaos": {
        "missing_values_pct": 0,
        "duplicates_pct": 0
      }
    },
    "seed": 12345
  },
  "created_at": "2025-01-15T10:00:00Z",
  "updated_at": "2025-01-15T10:00:00Z"
}
```

**Errors**:
- 404: Scenario not found

---

### Runs

#### Start a New Run

```
POST /api/runs
```

**Request Body**:
```json
{
  "scenario_id": "e2e-small-clean",
  "config_overrides": {
    "scale": "medium"
  }
}
```

**Response** (201 Created):
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "scenario_id": "e2e-small-clean",
  "run_type": "e2e",
  "module": null,
  "lab_tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "started_at": "2025-01-22T14:30:00Z",
  "config": { /* merged configuration */ }
}
```

**Errors**:
- 400: Invalid scenario_id or configuration
- 500: Failed to start run

---

#### List Runs

```
GET /api/runs
```

**Query Parameters**:
- `scenario_id` (optional): Filter by scenario
- `type` (optional): Filter by run type (`e2e`, `module`)
- `module` (optional): Filter by module (`aam`, `dcl`)
- `status` (optional): Filter by status (`pending`, `running`, `success`, `failed`)
- `limit` (optional, default: 50): Number of results
- `offset` (optional, default: 0): Pagination offset

**Response** (200 OK):
```json
{
  "runs": [
    {
      "run_id": "550e8400-e29b-41d4-a716-446655440000",
      "scenario_id": "e2e-small-clean",
      "run_type": "e2e",
      "module": null,
      "lab_tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
      "status": "success",
      "started_at": "2025-01-22T14:30:00Z",
      "completed_at": "2025-01-22T14:35:23Z",
      "duration_seconds": 323,
      "summary_metrics": {
        "aod_assets_found": 98,
        "aam_connectors_success": 3,
        "dcl_mapped_entities": 195,
        "agents_recommendations": 12
      }
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

---

#### Get Run Details

```
GET /api/runs/:id
```

**Path Parameters**:
- `id`: Run ID (UUID)

**Response** (200 OK):
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "scenario_id": "e2e-small-clean",
  "run_type": "e2e",
  "module": null,
  "lab_tenant_id": "lab-550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "started_at": "2025-01-22T14:30:00Z",
  "completed_at": "2025-01-22T14:35:23Z",
  "config": { /* full configuration */ },
  "metrics": {
    "aod": {
      "assets_discovered": 98,
      "errors": 0,
      "duration_seconds": 45
    },
    "aam": {
      "connectors": [
        {
          "id": "crm-connector",
          "requests": 234,
          "errors": 0,
          "retries": 0,
          "avg_latency_ms": 87
        }
      ],
      "duration_seconds": 120
    },
    "dcl": {
      "entities_mapped": 195,
      "conflicts": 0,
      "unmapped_fields": 3,
      "drift_events": 0,
      "duration_seconds": 98
    },
    "agents": {
      "recommendations": 12,
      "actions_proposed": 4,
      "errors": 0,
      "duration_seconds": 60
    }
  },
  "logs": [
    {
      "timestamp": "2025-01-22T14:30:00Z",
      "level": "INFO",
      "message": "Run started",
      "component": "orchestrator"
    },
    {
      "timestamp": "2025-01-22T14:30:05Z",
      "level": "INFO",
      "message": "Synthetic data generated: 98 assets, 200 customers",
      "component": "synthetic-data"
    }
  ]
}
```

**Errors**:
- 404: Run not found

---

#### Get Run Status

```
GET /api/runs/:id/status
```

**Path Parameters**:
- `id`: Run ID (UUID)

**Response** (200 OK):
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "current_stage": "dcl",
  "progress": {
    "aod": "completed",
    "aam": "completed",
    "dcl": "running",
    "agents": "pending"
  },
  "started_at": "2025-01-22T14:30:00Z",
  "elapsed_seconds": 180
}
```

**Errors**:
- 404: Run not found

---

#### Get Run Metrics

```
GET /api/runs/:id/metrics
```

**Path Parameters**:
- `id`: Run ID (UUID)

**Response** (200 OK):
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "metrics": {
    /* Same structure as metrics in run details */
  }
}
```

**Errors**:
- 404: Run not found

---

### Synthetic Data Services

These are **emulated external services** exposed by AOS-Farm for AAM/DCL to connect to during test runs.

**Base URL**: `http://localhost:3001/synthetic`

All synthetic endpoints require a `X-Lab-Tenant-Id` header to specify which lab tenant's data to serve.

---

#### CRM API

**List Customers**

```
GET /synthetic/crm/customers
```

**Headers**:
- `X-Lab-Tenant-Id`: Lab tenant identifier

**Query Parameters**:
- `limit` (optional, default: 100)
- `offset` (optional, default: 0)
- `filter` (optional): JSON filter expression

**Response** (200 OK):
```json
{
  "customers": [
    {
      "id": "cust-001",
      "name": "Acme Corp",
      "email": "contact@acme.example",
      "created_at": "2024-03-15T10:00:00Z",
      "status": "active"
    }
  ],
  "total": 200,
  "limit": 100,
  "offset": 0
}
```

**Chaos Behaviors**:
- Random latency injection
- Occasional 500/503 errors
- Schema drift (fields may appear/disappear between requests)

---

**Get Customer**

```
GET /synthetic/crm/customers/:id
```

**Headers**:
- `X-Lab-Tenant-Id`: Lab tenant identifier

**Response** (200 OK):
```json
{
  "id": "cust-001",
  "name": "Acme Corp",
  "email": "contact@acme.example",
  "created_at": "2024-03-15T10:00:00Z",
  "status": "active",
  "contacts": [...]
}
```

---

#### Billing API

**List Invoices**

```
GET /synthetic/billing/invoices
```

**Headers**:
- `X-Lab-Tenant-Id`: Lab tenant identifier

**Query Parameters**:
- `limit`, `offset`, `filter` (same as CRM)

**Response** (200 OK):
```json
{
  "invoices": [
    {
      "id": "inv-001",
      "customer_id": "cust-001",
      "amount": 15000,
      "currency": "USD",
      "status": "paid",
      "issued_at": "2025-01-01T00:00:00Z"
    }
  ],
  "total": 450,
  "limit": 100,
  "offset": 0
}
```

---

#### Assets API

**List Applications**

```
GET /synthetic/assets/applications
```

**Headers**:
- `X-Lab-Tenant-Id`: Lab tenant identifier

**Response** (200 OK):
```json
{
  "applications": [
    {
      "id": "app-001",
      "name": "Customer Portal",
      "type": "web-app",
      "environment": "production",
      "owner": "Platform Team",
      "risk_level": "high"
    }
  ]
}
```

---

#### Events API

**List Events**

```
GET /synthetic/events
```

**Headers**:
- `X-Lab-Tenant-Id`: Lab tenant identifier

**Query Parameters**:
- `type` (optional): Event type filter
- `start_time` (optional): ISO 8601 timestamp
- `end_time` (optional): ISO 8601 timestamp
- `limit`, `offset`

**Response** (200 OK):
```json
{
  "events": [
    {
      "id": "evt-001",
      "type": "auth.login",
      "timestamp": "2025-01-22T14:25:00Z",
      "user_id": "user-123",
      "metadata": { "ip": "192.168.1.1", "success": true }
    }
  ]
}
```

---

## Error Responses

All endpoints use standard HTTP status codes and return errors in this format:

```json
{
  "error": {
    "code": "INVALID_SCENARIO",
    "message": "Scenario 'invalid-id' not found",
    "details": {}
  }
}
```

**Common Error Codes**:
- `INVALID_SCENARIO`: Scenario not found or invalid
- `INVALID_CONFIG`: Configuration validation failed
- `RUN_NOT_FOUND`: Run ID not found
- `EXTERNAL_SERVICE_ERROR`: AOD/AAM/DCL/Agents returned an error
- `INTERNAL_ERROR`: Unexpected server error

---

## Webhooks (Future)

Support for webhook notifications when run status changes:

```
POST <configured-webhook-url>
{
  "event": "run.completed",
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "timestamp": "2025-01-22T14:35:23Z"
}
```

---

## Rate Limiting (Future)

- Default: 100 requests per minute per client
- Synthetic endpoints: Configurable per scenario (to test throttling)

---

## Versioning

API version is included in the base path:

- Current: `/api/v1/...`
- Future versions: `/api/v2/...`

For now, version can be omitted (`/api/...` defaults to v1).
