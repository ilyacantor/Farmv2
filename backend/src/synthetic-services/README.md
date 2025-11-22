# Synthetic HTTP Services

## Purpose

Expose **fake HTTP endpoints** that emulate real external systems (CRM, ERP, Billing, etc.) for AAM and DCL to connect to during test runs.

## Responsibilities

1. **API Emulation**
   - CRM API (customers, contacts, accounts)
   - Billing/ERP API (invoices, transactions, subscriptions)
   - Asset API (applications, infrastructure)
   - Events API (logs, metrics)

2. **Tenant-Aware Serving**
   - Filter data by `X-Lab-Tenant-Id` header
   - Isolate data per test run

3. **Chaos Application**
   - Inject latency, errors, timeouts
   - Apply schema drift (add/remove/rename fields)
   - Simulate rate limiting and pagination issues

4. **Standard REST Patterns**
   - Pagination (offset/limit or cursor-based)
   - Filtering and sorting
   - Authentication simulation

## Key Components

### BaseService

Shared logic for all synthetic services:

```typescript
class BaseService {
  constructor(
    private db: SupabaseClient,
    private chaos: ChaosEngine
  ) {}

  async serve(
    req: Request,
    tableName: string,
    chaosConfig: ChaosConfig
  ): Promise<Response> {
    // 1. Extract tenant ID
    const tenantId = req.headers['x-lab-tenant-id'];
    if (!tenantId) {
      return { status: 400, error: 'Missing X-Lab-Tenant-Id header' };
    }

    // 2. Apply response chaos (latency, errors)
    const chaosResult = await this.chaos.applyResponseChaos(chaosConfig);
    if (chaosResult.shouldError) {
      return { status: chaosResult.errorCode, error: chaosResult.errorMessage };
    }

    // 3. Query data
    await this.delay(chaosResult.latencyMs);
    let data = await this.queryData(tableName, tenantId, req.query);

    // 4. Apply schema chaos
    data = this.chaos.applySchemaChaos(data, chaosConfig);

    // 5. Return response
    return { status: 200, data };
  }

  private async queryData(
    table: string,
    tenantId: string,
    query: any
  ): Promise<any[]> {
    const { limit = 100, offset = 0, filter } = query;

    let q = this.db
      .from(table)
      .select('*')
      .eq('lab_tenant_id', tenantId)
      .range(offset, offset + limit - 1);

    if (filter) {
      q = this.applyFilters(q, JSON.parse(filter));
    }

    const { data, error } = await q;
    if (error) throw error;

    return data;
  }
}
```

---

## Service Implementations

### CRMService

Emulates a CRM system with customers, contacts, accounts:

```typescript
class CRMService extends BaseService {
  setupRoutes(app: Express) {
    app.get('/synthetic/crm/customers', async (req, res) => {
      const result = await this.serve(req, 'synthetic_customers', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/crm/customers/:id', async (req, res) => {
      const tenantId = req.headers['x-lab-tenant-id'];
      const { data, error } = await this.db
        .from('synthetic_customers')
        .select('*')
        .eq('lab_tenant_id', tenantId)
        .eq('id', req.params.id)
        .single();

      if (error) return res.status(404).json({ error: 'Customer not found' });

      // Apply schema chaos
      const modified = this.chaos.applySchemaChaos([data], this.chaosConfig)[0];
      res.json(modified);
    });

    app.get('/synthetic/crm/organizations', async (req, res) => {
      const result = await this.serve(req, 'synthetic_organizations', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });
  }
}
```

**Endpoints**:
- `GET /synthetic/crm/customers` - List customers
- `GET /synthetic/crm/customers/:id` - Get customer by ID
- `GET /synthetic/crm/organizations` - List organizations

---

### BillingService

Emulates a billing/ERP system:

```typescript
class BillingService extends BaseService {
  setupRoutes(app: Express) {
    app.get('/synthetic/billing/customers', async (req, res) => {
      const result = await this.serve(req, 'synthetic_customers', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/billing/invoices', async (req, res) => {
      const result = await this.serve(req, 'synthetic_invoices', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/billing/subscriptions', async (req, res) => {
      const result = await this.serve(req, 'synthetic_subscriptions', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/billing/transactions', async (req, res) => {
      const result = await this.serve(req, 'synthetic_transactions', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });
  }
}
```

**Endpoints**:
- `GET /synthetic/billing/customers` - List customers (billing view)
- `GET /synthetic/billing/invoices` - List invoices
- `GET /synthetic/billing/subscriptions` - List subscriptions
- `GET /synthetic/billing/transactions` - List transactions

---

### AssetsService

Emulates an asset inventory API:

```typescript
class AssetsService extends BaseService {
  setupRoutes(app: Express) {
    app.get('/synthetic/assets/applications', async (req, res) => {
      const result = await this.serve(req, 'synthetic_applications', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/assets/services', async (req, res) => {
      const result = await this.serve(req, 'synthetic_services', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/assets/databases', async (req, res) => {
      const result = await this.serve(req, 'synthetic_databases', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/assets/hosts', async (req, res) => {
      const result = await this.serve(req, 'synthetic_hosts', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });
  }
}
```

**Endpoints**:
- `GET /synthetic/assets/applications`
- `GET /synthetic/assets/services`
- `GET /synthetic/assets/databases`
- `GET /synthetic/assets/hosts`

---

### EventsService

Emulates an events/logs API:

```typescript
class EventsService extends BaseService {
  setupRoutes(app: Express) {
    app.get('/synthetic/events', async (req, res) => {
      const result = await this.serve(req, 'synthetic_events', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/events/auth', async (req, res) => {
      const result = await this.serve(req, 'synthetic_auth_events', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/events/access', async (req, res) => {
      const result = await this.serve(req, 'synthetic_access_logs', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });

    app.get('/synthetic/events/errors', async (req, res) => {
      const result = await this.serve(req, 'synthetic_error_logs', this.chaosConfig);
      res.status(result.status).json(result.data || result.error);
    });
  }
}
```

**Endpoints**:
- `GET /synthetic/events` - Generic events
- `GET /synthetic/events/auth` - Auth events
- `GET /synthetic/events/access` - Access logs
- `GET /synthetic/events/errors` - Error logs

---

## Chaos Integration

### Response Chaos

```typescript
class ResponseChaos {
  apply(config: ResponseChaosConfig): ChaosResult {
    const result: ChaosResult = {
      latencyMs: this.calculateLatency(config.latency_ms),
      shouldError: Math.random() < (config.error_rate_pct / 100),
      errorCode: 500,
      errorMessage: 'Internal Server Error'
    };

    if (result.shouldError) {
      result.errorCode = this.chooseErrorCode(config);
      result.errorMessage = this.chooseErrorMessage(result.errorCode);
    }

    return result;
  }

  private calculateLatency(config: LatencyConfig): number {
    // Simulate realistic latency distribution
    const random = Math.random();
    if (random < 0.5) return config.p50 + Math.random() * 10;
    if (random < 0.95) return config.p95 + Math.random() * 50;
    return config.p99 + Math.random() * 1000;
  }

  private chooseErrorCode(config: ResponseChaosConfig): number {
    const codes = [500, 503, 429, 504];
    return codes[Math.floor(Math.random() * codes.length)];
  }
}
```

### Schema Chaos

```typescript
class SchemaChaos {
  apply(data: any[], config: SchemaChaosConfig): any[] {
    return data.map(record => {
      let modified = { ...record };

      // Add fields
      if (Math.random() < (config.field_add_pct / 100)) {
        modified.new_field = faker.lorem.word();
      }

      // Remove fields
      if (Math.random() < (config.field_remove_pct / 100)) {
        delete modified.email; // Remove a common field
      }

      // Rename fields
      if (Math.random() < (config.field_rename_pct / 100)) {
        modified.customer_name = modified.name;
        delete modified.name;
      }

      return modified;
    });
  }
}
```

---

## Pagination

### Offset-Based Pagination

```typescript
app.get('/synthetic/crm/customers', async (req, res) => {
  const { limit = 100, offset = 0 } = req.query;
  const tenantId = req.headers['x-lab-tenant-id'];

  const { data, count } = await db
    .from('synthetic_customers')
    .select('*', { count: 'exact' })
    .eq('lab_tenant_id', tenantId)
    .range(offset, offset + limit - 1);

  res.json({
    data,
    total: count,
    limit,
    offset
  });
});
```

### Cursor-Based Pagination (Optional)

```typescript
app.get('/synthetic/events', async (req, res) => {
  const { cursor, limit = 100 } = req.query;

  let query = db
    .from('synthetic_events')
    .select('*')
    .eq('lab_tenant_id', tenantId)
    .limit(limit);

  if (cursor) {
    query = query.gt('id', cursor);
  }

  const { data } = await query;

  res.json({
    data,
    next_cursor: data.length === limit ? data[data.length - 1].id : null
  });
});
```

---

## Authentication Simulation

Synthetic services can simulate different auth mechanisms:

### API Key

```typescript
app.use((req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey || apiKey !== 'synthetic-api-key') {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
});
```

### Bearer Token

```typescript
app.use((req, res, next) => {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
});
```

---

## Configuration

Each synthetic service can be configured per scenario:

```json
{
  "synthetic_services": [
    {
      "type": "crm",
      "base_path": "/synthetic/crm",
      "chaos": {
        "response_chaos": {
          "latency_ms": { "p50": 100, "p95": 500, "p99": 2000 },
          "error_rate_pct": 5
        },
        "schema_chaos": {
          "field_add_pct": 10,
          "field_remove_pct": 5
        }
      },
      "pagination": {
        "type": "offset",
        "default_limit": 100,
        "max_limit": 1000
      }
    }
  ]
}
```

---

## Testing

### Unit Tests

```typescript
test('CRMService returns customers for tenant', async () => {
  const service = new CRMService(mockDb, mockChaos);
  const req = {
    headers: { 'x-lab-tenant-id': 'test-123' },
    query: {}
  };

  const result = await service.serve(req, 'synthetic_customers', chaosConfig);
  expect(result.status).toBe(200);
  expect(mockDb.from).toHaveBeenCalledWith('synthetic_customers');
});
```

### Integration Tests

```typescript
test('GET /synthetic/crm/customers returns data', async () => {
  const response = await request(app)
    .get('/synthetic/crm/customers')
    .set('X-Lab-Tenant-Id', 'test-123');

  expect(response.status).toBe(200);
  expect(response.body.data).toBeInstanceOf(Array);
});
```

---

## Future Enhancements

- **GraphQL Support**: Synthetic GraphQL endpoints
- **WebSocket Support**: Real-time event streams
- **Custom Data Transformations**: Per-scenario data transforms
- **Rate Limiting Simulation**: Realistic 429 responses with retry-after headers
- **Circuit Breaker Simulation**: Temporarily fail all requests to test resilience
