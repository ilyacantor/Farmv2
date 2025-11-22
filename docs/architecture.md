# AOS-Farm Architecture

## System Overview

AOS-Farm is a **synthetic environment orchestration system** designed to test the autonomOS platform end-to-end and at the module level. It maintains strict boundaries through HTTP-only communication and operates independently from the systems it tests.

## Architectural Principles

1. **HTTP-Only Integration**: All communication with AOD, AAM, DCL, and Agents is via REST APIs
2. **Single Database**: Supabase Postgres is the only database used by AOS-Farm
3. **Tenant Isolation**: Each test run uses a unique `lab_tenant_id` for data isolation
4. **Reproducibility**: Scenarios are deterministic when using the same seed and parameters
5. **Module Independence**: AAM and DCL can be tested standalone without other services

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Farm UI (Lab GUI)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  E2E Lab     │  │   AAM Lab    │  │   DCL Lab    │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP/REST
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Farm Backend (Orchestrator)                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  Orchestrator API                         │   │
│  │  • Scenario Management  • Run Coordination                │   │
│  │  • Metrics Collection   • Status Tracking                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Synthetic   │  │  Synthetic   │  │    Chaos     │          │
│  │ Data Engine  │  │   Services   │  │    Engine    │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Supabase     │
                    │    Postgres     │
                    └─────────────────┘

External Services (HTTP Only):
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐
│   AOD    │  │   AAM    │  │   DCL    │  │ Agent Orchestr.  │
└──────────┘  └──────────┘  └──────────┘  └──────────────────┘
```

## Core Components

### 1. Farm UI (Lab GUI)

**Purpose**: Web interface for scenario management and run monitoring

**Views**:
- **E2E Lab**: End-to-end scenario selection and execution
- **AAM Lab**: AAM-only test scenarios
- **DCL Lab**: DCL-only test scenarios
- **Run History**: Past runs with filtering and details

**Key Features**:
- Scenario listing with tags (scale, chaos profile)
- Run controls (scenario, scale, duration selection)
- Live run status and metrics
- Historical run analysis

**Technology**: Framework-agnostic (React, Vue, Svelte, vanilla JS all acceptable)

---

### 2. Orchestrator API (Backend)

**Purpose**: Coordinate test runs and manage scenarios

**Responsibilities**:
- Scenario lifecycle management
- Run orchestration (E2E and module-specific)
- Lab tenant allocation and isolation
- Coordination of synthetic data generation
- External service invocation (AOD/AAM/DCL/Agents)
- Metrics collection and aggregation
- Run persistence and querying

**Key Endpoints**:
```
GET  /api/scenarios              # List all scenarios
GET  /api/scenarios/:id          # Get scenario details
POST /api/runs                   # Start a new run
GET  /api/runs                   # List runs (with filters)
GET  /api/runs/:id               # Get run details
GET  /api/runs/:id/status        # Get run status
GET  /api/runs/:id/metrics       # Get run metrics
```

**Technology**: Framework-agnostic (Express, FastAPI, Flask, etc.)

---

### 3. Synthetic Data Engine

**Purpose**: Generate realistic enterprise data for test scenarios

**Generators**:

**Asset Landscape**:
- Applications (SaaS, custom, legacy)
- Services (microservices, APIs)
- Databases (SQL, NoSQL)
- Hosts (physical, VMs, containers)
- Relationships (dependencies, ownership)

**Business Entities**:
- Organizations and departments
- Customers and accounts
- Products and subscriptions
- Invoices and transactions
- Usage records

**Events & Time-Series**:
- Access logs
- Network flows
- Authentication events
- Usage metrics
- Error logs

**Key Capabilities**:
- **Determinism**: Reproducible output with seed control
- **Scaling**: Configurable volume (small/medium/large)
- **Variability**: Clean vs. messy/chaotic data
- **Relationships**: Maintains referential integrity
- **Tenant Isolation**: All data tagged with `lab_tenant_id`

**Configuration**:
```json
{
  "scale": {
    "assets": 500,
    "customers": 1000,
    "events_per_day": 50000
  },
  "chaos": {
    "missing_fields_pct": 5,
    "duplicate_records_pct": 2,
    "skewed_distribution": true
  },
  "seed": 12345
}
```

---

### 4. Synthetic HTTP Services

**Purpose**: Emulate external systems that AAM/DCL would connect to

**Service Types**:
- **CRM API**: Customer, contact, account data
- **ERP/Billing API**: Invoices, transactions, subscriptions
- **Logging API**: Access logs, events
- **Asset API**: Application inventory
- **Custom APIs**: Scenario-specific endpoints

**Features**:
- Standard REST patterns (pagination, filtering, sorting)
- Authentication simulation (API keys, OAuth)
- Tenant-aware (serves data for specific `lab_tenant_id`)
- Chaos injection (latency, errors, schema drift)

**Example Endpoints**:
```
GET  /api/v1/crm/customers
GET  /api/v1/crm/customers/:id
GET  /api/v1/billing/invoices
GET  /api/v1/assets/applications
```

**Chaos Application**:
- Response latency (configurable delays)
- Error injection (500, 429, timeout)
- Schema drift (field additions/removals/renames)
- Pagination issues
- Inconsistent data across requests

---

### 5. Chaos Engine

**Purpose**: Inject realistic failures and quality issues

**Chaos Types**:

**Response-Level Chaos**:
- Latency injection (fixed, random, p95-based)
- HTTP error codes (500, 503, 429, 404)
- Timeouts and dropped connections
- Response duplication

**Schema-Level Chaos**:
- Field additions (new optional fields)
- Field removals (missing expected fields)
- Field renames (breaking changes)
- Type changes (string → number, etc.)
- Nested structure changes

**Data-Level Chaos**:
- Missing required values (nulls, empty strings)
- Conflicting records (same ID, different data)
- Invalid formats (bad emails, dates, etc.)
- Referential integrity violations
- Skewed distributions (power law instead of uniform)
- Duplicate records with slight variations

**Configuration**:
```json
{
  "response_chaos": {
    "latency_ms": { "p50": 100, "p95": 500, "p99": 2000 },
    "error_rate_pct": 5,
    "timeout_rate_pct": 1
  },
  "schema_chaos": {
    "field_add_pct": 10,
    "field_remove_pct": 5,
    "field_rename_pct": 2
  },
  "data_chaos": {
    "missing_values_pct": 8,
    "duplicates_pct": 3,
    "conflicts_pct": 1
  }
}
```

---

## Data Model

### Database Schema (Supabase)

**farm_runs**
```sql
- id (uuid, pk)
- scenario_id (varchar)
- run_type (enum: 'e2e', 'module')
- module (varchar, nullable) -- 'aam', 'dcl', or null for e2e
- lab_tenant_id (uuid)
- status (enum: 'pending', 'running', 'success', 'failed')
- started_at (timestamptz)
- completed_at (timestamptz, nullable)
- metrics (jsonb)
- config (jsonb)
- created_at (timestamptz)
```

**farm_scenarios**
```sql
- id (varchar, pk)
- name (varchar)
- description (text)
- scenario_type (enum: 'e2e', 'module')
- module (varchar, nullable)
- tags (text[])
- config (jsonb)
- created_at (timestamptz)
- updated_at (timestamptz)
```

**Synthetic Data Tables** (examples):
```sql
-- Tenant-isolated asset data
synthetic_applications
- id, lab_tenant_id, name, type, environment, owner, ...

synthetic_customers
- id, lab_tenant_id, name, email, created_at, ...

synthetic_events
- id, lab_tenant_id, event_type, timestamp, payload, ...
```

All synthetic data tables include:
- `lab_tenant_id` for isolation
- Indexes on tenant_id for efficient queries

---

## Test Flow Patterns

### End-to-End (E2E) Flow

```
1. User: Select E2E scenario → Click Run
2. Orchestrator:
   a. Create lab_tenant_id
   b. Invoke Synthetic Data Engine
      → Generate assets, customers, events
   c. Configure Synthetic HTTP Services
      → Expose endpoints for tenant
      → Apply chaos configuration
3. Orchestrator → AOD:
   POST /api/discovery/run
   { "tenant_id": "<lab_tenant_id>", "target": "<farm_endpoints>" }
4. AOD discovers assets (Farm acts as data source)
5. Orchestrator → AAM:
   POST /api/connectors/register
   { "tenant_id": "<lab_tenant_id>", "sources": [...] }
6. AAM pulls from Synthetic Services
7. Orchestrator → DCL:
   POST /api/mapping/run
   { "tenant_id": "<lab_tenant_id>" }
8. DCL unifies data
9. Orchestrator → Agent Orchestrator:
   POST /api/agents/run
   { "tenant_id": "<lab_tenant_id>" }
10. Orchestrator collects metrics:
    - AOD: discovery coverage, errors
    - AAM: connector status, retries
    - DCL: mapping coverage, conflicts
    - Agents: recommendations count, errors
11. Store results in farm_runs
12. Return summary to UI
```

### Module-Only Flow (AAM Example)

```
1. User: Select AAM scenario → Click Run
2. Orchestrator:
   a. Create lab_tenant_id
   b. Generate synthetic source APIs
   c. Configure chaos for these APIs
3. Orchestrator → AAM:
   POST /api/lab/setup
   {
     "tenant_id": "<lab_tenant_id>",
     "connectors": [
       { "type": "crm", "url": "<farm_crm_url>", ... },
       { "type": "billing", "url": "<farm_billing_url>", ... }
     ]
   }
4. AAM runs connectors against Farm's APIs
5. Chaos Engine applies configured failures
6. Orchestrator polls AAM:
   GET /api/metrics?tenant_id=<lab_tenant_id>
7. Collect metrics:
   - Availability per connector
   - Error rates and retry behavior
   - Schema drift detection events
8. Store in farm_runs
9. Return to UI
```

---

## Integration Points

### AOD Integration

**Expected Capabilities**:
- API to trigger discovery for a tenant
- Configuration to point to Farm's synthetic endpoints
- Metrics endpoint for coverage and errors

**Example**:
```
POST /api/discovery/run
{
  "tenant_id": "lab-12345",
  "sources": {
    "asset_api": "http://farm:3001/synthetic/assets",
    "network_logs": "http://farm:3001/synthetic/logs"
  }
}

GET /api/discovery/status/:tenant_id
→ { "status": "complete", "assets_found": 487, "errors": 2 }
```

### AAM Integration

**Expected Capabilities**:
- API to register connectors for a tenant
- Metrics/status endpoint per connector
- Lab/test mode for isolated runs

**Example**:
```
POST /api/connectors/register
{
  "tenant_id": "lab-12345",
  "connectors": [
    {
      "id": "crm-connector",
      "type": "rest",
      "url": "http://farm:3001/synthetic/crm",
      "auth": { ... }
    }
  ]
}

GET /api/connectors/metrics?tenant_id=lab-12345
→ {
  "crm-connector": {
    "requests": 1234,
    "errors": 45,
    "avg_latency_ms": 120,
    "retries": 15
  }
}
```

### DCL Integration

**Expected Capabilities**:
- API to register sources for a tenant
- API to trigger mapping/unification
- Metrics endpoint for mapping coverage and conflicts

**Example**:
```
POST /api/sources/register
{
  "tenant_id": "lab-12345",
  "sources": [
    { "id": "crm", "type": "customers", "url": "..." },
    { "id": "billing", "type": "customers", "url": "..." }
  ]
}

POST /api/mapping/run
{ "tenant_id": "lab-12345" }

GET /api/mapping/metrics?tenant_id=lab-12345
→ {
  "mapped_entities": 980,
  "conflicts": 12,
  "unmapped_fields": 34,
  "drift_events": 5
}
```

### Agent Orchestrator Integration

**Expected Capabilities**:
- API to run agents for a tenant
- Summary endpoint for recommendations

**Example**:
```
POST /api/agents/run
{ "tenant_id": "lab-12345" }

GET /api/agents/summary?tenant_id=lab-12345
→ {
  "recommendations": 23,
  "actions_proposed": 8,
  "errors": 0
}
```

---

## Scalability Considerations

### Tenant Isolation

- Each run gets a unique `lab_tenant_id`
- All queries filtered by tenant_id (indexed)
- Cleanup strategy: archive or delete old tenant data

### Concurrent Runs

- Multiple runs can execute simultaneously
- Each run is independent (different tenant_id)
- Resource limits (max concurrent runs configurable)

### Data Volume

- Scenarios define scale (small/medium/large)
- Synthetic data generated on-demand or pre-seeded
- Cleanup after run completion (configurable retention)

### Metrics Storage

- Store aggregated metrics in farm_runs.metrics (JSONB)
- Optional: time-series metrics in separate table
- Retention policy for detailed metrics

---

## Security Considerations

### Tenant Isolation

- Strict filtering by lab_tenant_id in all queries
- RLS (Row-Level Security) in Supabase for extra safety
- No cross-tenant data leakage

### API Authentication

- Farm UI → Farm Backend: session/token-based auth
- Farm Backend → External Services: API keys in environment
- Synthetic Services: optional auth simulation (not real auth)

### Data Privacy

- All data is synthetic (no real customer data)
- Generated data should not resemble real entities
- No PII or sensitive data in scenarios

---

## Extensibility

### Adding New Entity Types

1. Create generator in `backend/src/synthetic-data/generators/<type>/`
2. Define schema in `database/schema/`
3. Add to scenario configuration options
4. Update documentation

### Adding New Chaos Patterns

1. Implement in `backend/src/chaos/<category>/`
2. Add configuration schema
3. Update scenario examples
4. Document behavior and use cases

### Adding New Scenarios

1. Create definition in `scenarios/<type>/<name>.json`
2. Specify scale, chaos, and data requirements
3. Test via UI
4. Add to documentation

### Supporting New Services

1. Define synthetic service in `backend/src/synthetic-services/<name>/`
2. Implement REST endpoints
3. Add chaos integration
4. Update integration guide

---

## Monitoring & Observability

### Run Monitoring

- Real-time status updates via API polling
- Status transitions: pending → running → success/failed
- Error capture and logging

### Metrics Collection

**Per-Module Metrics**:
- AOD: discovery coverage, error count
- AAM: availability, retries, schema drift events
- DCL: mapping coverage, conflicts, unmapped fields
- Agents: recommendation count, actions, errors

**Aggregate Metrics**:
- Total runs, success rate
- Average run duration per scenario type
- Chaos impact (correlation with failures)

### Logging

- Structured logs (JSON format)
- Log levels: DEBUG, INFO, WARN, ERROR
- Correlation IDs (run_id, tenant_id)
- Centralized logging (optional)

---

## Future Enhancements

### Phase 2 Possibilities

- Advanced dashboards (Grafana-style)
- Real-time metrics streaming (WebSocket)
- Scenario versioning and history
- A/B testing of scenarios
- Integration with external chaos tools (Chaos Mesh)
- Multi-region synthetic data
- Performance benchmarking mode
- Automated regression detection

### Extensibility Hooks

- Plugin system for custom generators
- Webhook notifications for run completion
- Custom metrics collectors
- Scenario DSL or visual editor

---

## Conclusion

AOS-Farm provides a **comprehensive, isolated testing environment** for the autonomOS platform. Its architecture emphasizes:

- **Clean boundaries** (HTTP-only integration)
- **Reproducibility** (deterministic scenarios)
- **Flexibility** (E2E and module testing)
- **Realism** (chaos injection)
- **Simplicity** (single database, straightforward UI)

This design enables thorough testing of the autonomOS pipeline while maintaining independence and extensibility.
