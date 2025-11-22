# Chaos Engine

## Purpose

Inject **realistic failures and quality issues** into synthetic data and services to test autonomOS resilience, error handling, and adaptive capabilities.

## Chaos Types

### 1. Response-Level Chaos

HTTP behavior chaos applied to synthetic service responses:
- Latency injection
- Error codes (500, 503, 429, 504)
- Timeouts and dropped connections
- Response duplication

### 2. Schema-Level Chaos

Data structure chaos simulating API evolution:
- Field additions (new optional fields)
- Field removals (missing expected fields)
- Field renames (breaking changes)
- Type changes (string → number)
- Nesting changes (flat → nested, nested → flat)

### 3. Data-Level Chaos

Data quality issues:
- Missing/null values
- Duplicate records with variations
- Conflicting records across sources
- Invalid formats (bad emails, dates)
- Referential integrity violations
- Skewed distributions (power law instead of uniform)

---

## Components

### ChaosEngine

Main coordinator:

```typescript
class ChaosEngine {
  constructor(
    private responseChaos: ResponseChaos,
    private schemaChaos: SchemaChaos,
    private dataChaos: DataChaos
  ) {}

  applyResponseChaos(config: ResponseChaosConfig): ChaosResult {
    return this.responseChaos.apply(config);
  }

  applySchemaChaos(data: any[], config: SchemaChaosConfig): any[] {
    return this.schemaChaos.apply(data, config);
  }

  applyDataChaos(tenantId: string, config: DataChaosConfig): Promise<void> {
    return this.dataChaos.apply(tenantId, config);
  }
}
```

---

## Response Chaos

### ResponseChaos

```typescript
class ResponseChaos {
  apply(config: ResponseChaosConfig): ChaosResult {
    if (!config.enabled) {
      return { latencyMs: 0, shouldError: false };
    }

    const result: ChaosResult = {
      latencyMs: this.calculateLatency(config.latency_ms),
      shouldError: Math.random() < (config.error_rate_pct / 100),
      shouldTimeout: Math.random() < (config.timeout_rate_pct / 100),
      errorCode: 500,
      errorMessage: 'Internal Server Error'
    };

    if (result.shouldError) {
      result.errorCode = this.chooseErrorCode(config);
      result.errorMessage = this.getErrorMessage(result.errorCode);
    }

    return result;
  }

  private calculateLatency(config: LatencyConfig): number {
    const random = Math.random();

    // Percentile-based latency distribution
    if (random < 0.50) {
      return config.p50 + Math.random() * (config.p50 * 0.2);
    } else if (random < 0.95) {
      return config.p95 + Math.random() * (config.p95 * 0.1);
    } else {
      return config.p99 + Math.random() * (config.p99 * 0.3);
    }
  }

  private chooseErrorCode(config: ResponseChaosConfig): number {
    const weights = {
      500: 40,  // Internal Server Error
      503: 30,  // Service Unavailable
      429: 20,  // Too Many Requests
      504: 10   // Gateway Timeout
    };

    return this.weightedChoice(weights);
  }

  private getErrorMessage(code: number): string {
    const messages = {
      500: 'Internal Server Error',
      503: 'Service Temporarily Unavailable',
      429: 'Too Many Requests',
      504: 'Gateway Timeout'
    };
    return messages[code] || 'Unknown Error';
  }
}
```

### Usage in Synthetic Services

```typescript
app.get('/synthetic/crm/customers', async (req, res) => {
  const chaosResult = chaos.applyResponseChaos(config.response_chaos);

  // Apply latency
  if (chaosResult.latencyMs > 0) {
    await delay(chaosResult.latencyMs);
  }

  // Apply error
  if (chaosResult.shouldError) {
    return res.status(chaosResult.errorCode).json({
      error: chaosResult.errorMessage
    });
  }

  // Apply timeout (drop connection)
  if (chaosResult.shouldTimeout) {
    req.socket.destroy();
    return;
  }

  // Normal response
  const data = await queryData();
  res.json(data);
});
```

---

## Schema Chaos

### SchemaChaos

```typescript
class SchemaChaos {
  apply(data: any[], config: SchemaChaosConfig): any[] {
    if (!config.enabled) return data;

    return data.map(record => this.applyToRecord(record, config));
  }

  private applyToRecord(record: any, config: SchemaChaosConfig): any {
    let modified = { ...record };

    // Field additions
    if (Math.random() < (config.field_add_pct / 100)) {
      modified = this.addRandomField(modified);
    }

    // Field removals
    if (Math.random() < (config.field_remove_pct / 100)) {
      modified = this.removeRandomField(modified);
    }

    // Field renames
    if (Math.random() < (config.field_rename_pct / 100)) {
      modified = this.renameRandomField(modified);
    }

    // Type changes
    if (Math.random() < (config.type_change_pct / 100)) {
      modified = this.changeFieldType(modified);
    }

    return modified;
  }

  private addRandomField(record: any): any {
    const newFields = [
      'loyalty_tier',
      'referral_code',
      'last_login',
      'preferences',
      'metadata'
    ];

    const field = newFields[Math.floor(Math.random() * newFields.length)];
    return { ...record, [field]: faker.lorem.word() };
  }

  private removeRandomField(record: any): any {
    const fieldsToRemove = ['phone', 'email', 'address'];
    const field = fieldsToRemove[Math.floor(Math.random() * fieldsToRemove.length)];

    const { [field]: removed, ...rest } = record;
    return rest;
  }

  private renameRandomField(record: any): any {
    const renames = {
      'name': 'customer_name',
      'email': 'email_address',
      'phone': 'phone_number',
      'created_at': 'registration_date'
    };

    const [oldName, newName] = Object.entries(renames)[
      Math.floor(Math.random() * Object.entries(renames).length)
    ];

    if (record[oldName]) {
      const { [oldName]: value, ...rest } = record;
      return { ...rest, [newName]: value };
    }

    return record;
  }

  private changeFieldType(record: any): any {
    // Change string to number or vice versa
    const fields = Object.keys(record).filter(k =>
      typeof record[k] === 'string' && !isNaN(Number(record[k]))
    );

    if (fields.length > 0) {
      const field = fields[Math.floor(Math.random() * fields.length)];
      return { ...record, [field]: Number(record[field]) };
    }

    return record;
  }
}
```

---

## Data Chaos

### DataChaos

Applied during synthetic data generation:

```typescript
class DataChaos {
  async apply(tenantId: string, config: DataChaosConfig): Promise<void> {
    if (!config.enabled) return;

    if (config.missing_values_pct > 0) {
      await this.injectMissingValues(tenantId, config.missing_values_pct);
    }

    if (config.duplicates_pct > 0) {
      await this.createDuplicates(tenantId, config.duplicates_pct);
    }

    if (config.conflicts_pct > 0) {
      await this.createConflicts(tenantId, config.conflicts_pct);
    }

    if (config.invalid_formats_pct > 0) {
      await this.createInvalidFormats(tenantId, config.invalid_formats_pct);
    }
  }

  private async injectMissingValues(
    tenantId: string,
    pct: number
  ): Promise<void> {
    // Randomly null out email and phone fields
    await db.raw(`
      UPDATE synthetic_customers
      SET email = CASE
        WHEN random() < ? THEN NULL
        ELSE email
      END,
      phone = CASE
        WHEN random() < ? THEN NULL
        ELSE phone
      END
      WHERE lab_tenant_id = ?
    `, [pct / 100, pct / 100, tenantId]);
  }

  private async createDuplicates(
    tenantId: string,
    pct: number
  ): Promise<void> {
    // Duplicate X% of records with slight variations
    const { data: customers } = await db
      .from('synthetic_customers')
      .select('*')
      .eq('lab_tenant_id', tenantId)
      .limit(Math.ceil(pct));

    const duplicates = customers.map(c => ({
      ...c,
      id: c.id + '-dup',
      name: this.varySlightly(c.name),
      email: c.email ? this.varyEmail(c.email) : null,
      created_at: new Date()
    }));

    await db.insert('synthetic_customers', duplicates);
  }

  private async createConflicts(
    tenantId: string,
    pct: number
  ): Promise<void> {
    // Create conflicting records across source systems
    const { data: customers } = await db
      .from('synthetic_customers')
      .select('*')
      .eq('lab_tenant_id', tenantId)
      .eq('source_system', 'crm')
      .limit(Math.ceil(pct));

    const conflicts = customers.map(c => ({
      ...c,
      source_system: 'billing',
      name: c.name.toUpperCase(), // Conflicting name
      email: c.email ? c.email.replace('@', '+billing@') : null,
      phone: this.generateRandomPhone(), // Conflicting phone
      created_at: new Date()
    }));

    await db.insert('synthetic_customers', conflicts);
  }

  private async createInvalidFormats(
    tenantId: string,
    pct: number
  ): Promise<void> {
    // Create records with invalid email/phone formats
    await db.raw(`
      UPDATE synthetic_customers
      SET email = CASE
        WHEN random() < ? THEN 'invalid-email'
        ELSE email
      END,
      phone = CASE
        WHEN random() < ? THEN '123'
        ELSE phone
      END
      WHERE lab_tenant_id = ?
    `, [pct / 100, pct / 100, tenantId]);
  }

  private varySlightly(text: string): string {
    const variations = [
      text.toUpperCase(),
      text.toLowerCase(),
      text + ' Inc',
      text + ', LLC',
      text.replace(' ', '-')
    ];
    return variations[Math.floor(Math.random() * variations.length)];
  }

  private varyEmail(email: string): string {
    return email.replace('@', '+variant@');
  }
}
```

---

## Configuration Examples

### Low Chaos (Baseline + Small Issues)

```json
{
  "response_chaos": {
    "enabled": true,
    "latency_ms": { "p50": 50, "p95": 100, "p99": 200 },
    "error_rate_pct": 1,
    "timeout_rate_pct": 0
  },
  "schema_chaos": {
    "enabled": true,
    "field_add_pct": 2,
    "field_remove_pct": 1,
    "field_rename_pct": 0,
    "type_change_pct": 0
  },
  "data_chaos": {
    "enabled": true,
    "missing_values_pct": 2,
    "duplicates_pct": 1,
    "conflicts_pct": 0
  }
}
```

### High Chaos (Stress Test)

```json
{
  "response_chaos": {
    "enabled": true,
    "latency_ms": { "p50": 500, "p95": 2000, "p99": 5000 },
    "error_rate_pct": 15,
    "timeout_rate_pct": 5
  },
  "schema_chaos": {
    "enabled": true,
    "field_add_pct": 20,
    "field_remove_pct": 10,
    "field_rename_pct": 5,
    "type_change_pct": 3
  },
  "data_chaos": {
    "enabled": true,
    "missing_values_pct": 15,
    "duplicates_pct": 10,
    "conflicts_pct": 8,
    "invalid_formats_pct": 5
  }
}
```

---

## Testing

### Unit Tests

```typescript
test('ResponseChaos applies latency', () => {
  const chaos = new ResponseChaos();
  const result = chaos.apply({
    enabled: true,
    latency_ms: { p50: 100, p95: 500, p99: 2000 },
    error_rate_pct: 0
  });

  expect(result.latencyMs).toBeGreaterThan(0);
  expect(result.shouldError).toBe(false);
});

test('SchemaChaos adds fields', () => {
  const chaos = new SchemaChaos();
  const data = [{ id: '1', name: 'Test' }];
  const result = chaos.apply(data, {
    enabled: true,
    field_add_pct: 100 // Always add
  });

  expect(Object.keys(result[0]).length).toBeGreaterThan(2);
});
```

---

## Future Enhancements

- **Temporal Chaos**: Chaos that changes over time during a run
- **Cascading Failures**: One service failure triggers others
- **Circuit Breaker Simulation**: Service completely fails after threshold
- **Gradual Degradation**: Performance slowly degrades
- **Recovery Simulation**: Service recovers after chaos period
