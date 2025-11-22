# AOS-Farm Database Schema - Entity Relationship Diagram

## Core Tables

```
┌─────────────────────────┐
│   farm_scenarios        │
├─────────────────────────┤
│ id (PK)                 │
│ name                    │
│ description             │
│ scenario_type           │ ← 'e2e' or 'module'
│ module                  │ ← 'aam', 'dcl', or NULL
│ tags[]                  │
│ config (JSONB)          │
│ created_at              │
│ updated_at              │
└─────────────────────────┘
           │
           │ 1:N
           ▼
┌─────────────────────────┐
│     farm_runs           │
├─────────────────────────┤
│ id (PK)                 │
│ scenario_id (FK)        │────┐
│ run_type                │    │
│ module                  │    │
│ lab_tenant_id           │◄───┼─────────┐
│ status                  │    │         │ Tenant Isolation
│ started_at              │    │         │ (All synthetic tables
│ completed_at            │    │         │  filtered by this)
│ metrics (JSONB)         │    │         │
│ config (JSONB)          │    │         │
│ error_message           │    │         │
│ logs (JSONB[])          │    │         │
│ created_at              │    │         │
│ updated_at              │    │         │
└─────────────────────────┘    │         │
                               │         │
                               │         │
```

## Synthetic Asset Tables

```
                    ┌──────────────────────────────┐
                    │  synthetic_applications      │
                    ├──────────────────────────────┤
        ┌───────────│ id, lab_tenant_id (PK)       │
        │           │ name                         │
        │           │ type                         │
        │           │ environment                  │
        │           │ owner, team                  │
        │           │ risk_level                   │
        │           │ tech_stack[]                 │
        │           │ metadata (JSONB)             │
        │           └──────────────────────────────┘
        │                    │
        │                    │ 1:N
        │                    ▼
        │           ┌──────────────────────────────┐
        │           │  synthetic_services          │
        │           ├──────────────────────────────┤
        │           │ id, lab_tenant_id (PK)       │
        │           │ application_id               │
        │           │ name, type                   │
        │           │ environment, owner           │
        │           │ endpoint, protocol           │
        │           │ metadata (JSONB)             │
        │           └──────────────────────────────┘
        │
        │
        │           ┌──────────────────────────────┐
        │           │  synthetic_databases         │
        │           ├──────────────────────────────┤
        └───────────│ id, lab_tenant_id (PK)       │
        │           │ name, type                   │
        │           │ environment, owner           │
        │           │ size_gb                      │
        │           │ connection_string            │
        │           │ metadata (JSONB)             │
        │           └──────────────────────────────┘
        │
        │
        │           ┌──────────────────────────────┐
        │           │  synthetic_hosts             │
        │           ├──────────────────────────────┤
        └───────────│ id, lab_tenant_id (PK)       │
                    │ name, type                   │
                    │ cloud_provider, region       │
                    │ instance_type, ip_address    │
                    │ status                       │
                    │ metadata (JSONB)             │
                    └──────────────────────────────┘

                    ┌──────────────────────────────────┐
                    │ synthetic_asset_relationships    │
                    ├──────────────────────────────────┤
                    │ id (PK)                          │
                    │ lab_tenant_id                    │
                    │ source_id, source_type           │
                    │ target_id, target_type           │
                    │ relationship_type                │
                    │ metadata (JSONB)                 │
                    └──────────────────────────────────┘
```

## Synthetic Business Tables

```
┌─────────────────────────────┐
│ synthetic_organizations     │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ name, industry              │
│ size, country, region       │
│ metadata (JSONB)            │
└─────────────────────────────┘
           │
           │ 1:N
           ▼
┌─────────────────────────────┐
│  synthetic_customers        │
├─────────────────────────────┤
│ id, lab_tenant_id,          │
│   source_system (PK)        │ ◄─── Multiple sources per tenant
│ name, email, phone          │      (for conflict testing in DCL)
│ organization_id             │
│ status, tier                │
│ metadata (JSONB)            │
└─────────────────────────────┘
           │
           │ 1:N
           ├────────────────────────────────┐
           │                                │
           ▼                                ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│ synthetic_subscriptions  │   │  synthetic_invoices      │
├──────────────────────────┤   ├──────────────────────────┤
│ id, lab_tenant_id,       │   │ id, lab_tenant_id,       │
│   source_system (PK)     │   │   source_system (PK)     │
│ customer_id              │   │ customer_id              │
│ plan, status             │   │ amount, currency         │
│ start_date, end_date     │   │ status                   │
│ mrr                      │   │ issued_at, due_at        │
│ metadata (JSONB)         │   │ paid_at                  │
└──────────────────────────┘   │ metadata (JSONB)         │
                               └──────────────────────────┘
                                          │
                                          │ 1:N
                                          ▼
                               ┌──────────────────────────┐
                               │ synthetic_transactions   │
                               ├──────────────────────────┤
                               │ id, lab_tenant_id,       │
                               │   source_system (PK)     │
                               │ customer_id              │
                               │ invoice_id               │
                               │ amount, currency         │
                               │ type, status             │
                               │ timestamp                │
                               │ metadata (JSONB)         │
                               └──────────────────────────┘

┌─────────────────────────────┐
│  synthetic_products         │
├─────────────────────────────┤
│ id, lab_tenant_id,          │
│   source_system (PK)        │
│ name, sku                   │
│ category, price             │
│ currency, status            │
│ metadata (JSONB)            │
└─────────────────────────────┘
```

## Synthetic Event Tables

```
┌─────────────────────────────┐
│  synthetic_events           │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ event_type                  │
│ timestamp                   │
│ user_id, application_id     │
│ service_id, severity        │
│ message                     │
│ metadata (JSONB)            │
└─────────────────────────────┘

┌─────────────────────────────┐
│  synthetic_auth_events      │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ timestamp, event_type       │
│ user_id, username           │
│ ip_address, user_agent      │
│ success, failure_reason     │
│ metadata (JSONB)            │
└─────────────────────────────┘

┌─────────────────────────────┐
│  synthetic_access_logs      │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ timestamp                   │
│ method, path                │
│ status_code                 │
│ response_time_ms            │
│ user_id, ip_address         │
│ service_id                  │
│ metadata (JSONB)            │
└─────────────────────────────┘

┌─────────────────────────────┐
│  synthetic_network_events   │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ timestamp                   │
│ source_ip, destination_ip   │
│ source_port, dest_port      │
│ protocol                    │
│ bytes_sent, bytes_received  │
│ status                      │
│ metadata (JSONB)            │
└─────────────────────────────┘

┌─────────────────────────────┐
│  synthetic_error_logs       │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ timestamp, severity         │
│ application_id, service_id  │
│ error_type, error_message   │
│ stack_trace                 │
│ user_id, request_id         │
│ metadata (JSONB)            │
└─────────────────────────────┘

┌─────────────────────────────┐
│  synthetic_usage_metrics    │
├─────────────────────────────┤
│ id, lab_tenant_id (PK)      │
│ timestamp                   │
│ metric_name, value, unit    │
│ application_id, service_id  │
│ host_id                     │
│ dimensions (JSONB)          │
└─────────────────────────────┘
```

## Key Relationships

### Tenant Isolation

All synthetic data tables use **composite primary keys** or **filtered indexes** with `lab_tenant_id`:

```sql
-- Composite PK approach
PRIMARY KEY (id, lab_tenant_id)

-- Indexed approach
CREATE INDEX idx_table_tenant ON table_name(lab_tenant_id);
```

Every query on synthetic data **MUST** filter by `lab_tenant_id`:

```sql
SELECT * FROM synthetic_customers
WHERE lab_tenant_id = 'lab-550e8400-e29b-41d4-a716-446655440000';
```

Row-Level Security (RLS) enforces this at the database level.

---

### Multi-Source Data (DCL Testing)

Tables like `synthetic_customers`, `synthetic_subscriptions`, etc. have **source_system** in their composite PK:

```sql
PRIMARY KEY (id, lab_tenant_id, source_system)
```

This allows the same customer to exist in multiple "source systems" (e.g., CRM and Billing) with potentially conflicting data:

```
Customer 'cust-001' in CRM:    { name: "Acme Corp", email: "info@acme.com" }
Customer 'cust-001' in Billing: { name: "ACME Corporation", email: "billing@acme.com" }
```

DCL must detect and resolve these conflicts.

---

### Asset Relationships

`synthetic_asset_relationships` is a **generic relationship table**:

```
source_type + source_id  →  target_type + target_id
   'service'   'svc-1'       'database'    'db-1'
                   (relationship: 'connects_to')
```

Examples:
- Service `svc-1` **connects_to** Database `db-1`
- Application `app-1` **runs_on** Host `host-1`
- Service `svc-2` **depends_on** Service `svc-1`

---

## Indexes Summary

### Critical Indexes (for performance)

All synthetic tables have:
1. `idx_<table>_tenant` on `lab_tenant_id` (for isolation)
2. `idx_<table>_timestamp` on `timestamp DESC` (for time-series queries)

Additional indexes based on common query patterns:
- `source_system` (multi-source tables)
- `status` (filtering by status)
- `user_id`, `customer_id` (joins and lookups)

---

## Data Flow

### 1. Scenario Definition

```
User defines scenario → Stored in farm_scenarios
```

### 2. Run Initiated

```
User starts run → farm_runs created with lab_tenant_id
```

### 3. Synthetic Data Generation

```
Orchestrator → Synthetic Data Engine → Populates synthetic_* tables
                                        (all tagged with lab_tenant_id)
```

### 4. Testing

```
AAM/DCL query synthetic HTTP endpoints → Farm serves data filtered by lab_tenant_id
```

### 5. Metrics Collection

```
Orchestrator collects metrics → Stored in farm_runs.metrics (JSONB)
```

### 6. Cleanup (Optional)

```
After run completes → Delete synthetic data for lab_tenant_id (retention policy)
```

---

## Query Examples

### Find all runs for a scenario

```sql
SELECT *
FROM farm_runs
WHERE scenario_id = 'e2e-small-clean'
ORDER BY started_at DESC;
```

### Get all customers for a lab tenant (across all source systems)

```sql
SELECT *
FROM synthetic_customers
WHERE lab_tenant_id = 'lab-550e8400-e29b-41d4-a716-446655440000';
```

### Find conflicting customer records (DCL testing)

```sql
SELECT customer_id, array_agg(DISTINCT email) as emails
FROM synthetic_customers
WHERE lab_tenant_id = 'lab-550e8400-e29b-41d4-a716-446655440000'
GROUP BY customer_id
HAVING COUNT(DISTINCT email) > 1;
```

### Get recent errors for an application

```sql
SELECT *
FROM synthetic_error_logs
WHERE lab_tenant_id = 'lab-550e8400-e29b-41d4-a716-446655440000'
  AND application_id = 'app-001'
  AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

---

## Design Principles

1. **Strict Tenant Isolation**: Every synthetic table includes `lab_tenant_id`
2. **Multi-Source Support**: Business tables support multiple source systems
3. **JSONB for Flexibility**: `metadata` and `config` fields allow extensibility
4. **Time-Series Optimization**: Indexes on `timestamp DESC` for event queries
5. **Referential Integrity**: Soft references (no FKs across tenants)
6. **Audit Trail**: `created_at` and `updated_at` on all tables
7. **Scalability**: Ready for partitioning on high-volume event tables
