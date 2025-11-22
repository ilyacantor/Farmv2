# Synthetic Data Engine

## Purpose

Generate realistic, configurable **synthetic enterprise data** for testing autonomOS. Data includes asset landscapes, business entities, and events/time-series.

## Responsibilities

1. **Asset Landscape Generation**
   - Applications, services, databases, hosts
   - Relationships and dependencies between assets

2. **Business Data Generation**
   - Organizations, customers, subscriptions
   - Invoices, transactions, products
   - Multi-source data for DCL conflict testing

3. **Event Generation**
   - Authentication events, access logs
   - Network events, error logs
   - Usage metrics and telemetry

4. **Chaos Integration**
   - Inject data quality issues (missing values, duplicates)
   - Create conflicting records across sources
   - Generate schema variations

## Key Components

### SyntheticDataEngine

Main engine coordinating all generators:

```typescript
class SyntheticDataEngine {
  async generate(
    tenantId: string,
    scaleConfig: ScaleConfig,
    chaosConfig: ChaosConfig
  ): Promise<GenerationResult> {
    // 1. Generate assets
    await this.assetGenerators.generateAll(tenantId, scaleConfig.assets);

    // 2. Generate business data
    await this.businessGenerators.generateAll(tenantId, scaleConfig.business);

    // 3. Generate events
    await this.eventGenerators.generateAll(tenantId, scaleConfig.events);

    // 4. Apply chaos if configured
    if (chaosConfig.data_chaos.enabled) {
      await this.chaos.applyDataChaos(tenantId, chaosConfig.data_chaos);
    }

    return {
      assets_generated: ...,
      entities_generated: ...,
      events_generated: ...
    };
  }
}
```

---

## Generators

### Asset Generators

#### ApplicationGenerator

```typescript
class ApplicationGenerator {
  async generate(tenantId: string, count: number): Promise<void> {
    const apps = [];
    for (let i = 0; i < count; i++) {
      apps.push({
        id: `app-${i + 1}`,
        lab_tenant_id: tenantId,
        name: faker.company.name() + ' Portal',
        type: randomChoice(['web-app', 'mobile-app', 'saas', 'legacy']),
        environment: randomChoice(['production', 'staging', 'dev']),
        owner: faker.person.fullName(),
        team: randomChoice(['Platform', 'Product', 'Engineering']),
        risk_level: randomChoice(['critical', 'high', 'medium', 'low']),
        tech_stack: randomChoice([
          ['react', 'node', 'postgres'],
          ['vue', 'python', 'mysql'],
          ['angular', 'java', 'oracle']
        ])
      });
    }

    await db.insert('synthetic_applications', apps);
  }
}
```

#### ServiceGenerator

Generates microservices and APIs, linked to applications.

#### DatabaseGenerator

Generates database inventory.

#### HostGenerator

Generates infrastructure (VMs, containers, etc.).

#### RelationshipGenerator

Creates dependencies between assets:
```typescript
- Service A depends_on Service B
- Application runs_on Host X
- Service connects_to Database Y
```

---

### Business Generators

#### CustomerGenerator

```typescript
class CustomerGenerator {
  async generate(
    tenantId: string,
    count: number,
    sourceSystems: string[]
  ): Promise<void> {
    // Generate base customers
    const baseCustomers = this.generateBaseCustomers(count);

    // Duplicate across source systems with variations
    for (const source of sourceSystems) {
      const customers = baseCustomers.map(base => ({
        id: base.id,
        lab_tenant_id: tenantId,
        source_system: source,
        name: this.varyName(base.name, source),
        email: this.varyEmail(base.email, source),
        phone: base.phone,
        organization_id: base.organization_id,
        status: 'active',
        tier: randomChoice(['free', 'basic', 'premium', 'enterprise'])
      }));

      await db.insert('synthetic_customers', customers);
    }
  }

  // Introduce variations for conflict testing
  varyName(baseName: string, source: string): string {
    if (Math.random() < 0.1) {
      return baseName.toUpperCase(); // 10% chance of uppercase
    }
    return baseName;
  }

  varyEmail(baseEmail: string, source: string): string {
    if (Math.random() < 0.05) {
      return baseEmail.replace('@', '+' + source + '@'); // 5% variation
    }
    return baseEmail;
  }
}
```

#### OrganizationGenerator

Generates parent organizations for customers.

#### SubscriptionGenerator

Generates subscriptions linked to customers.

#### InvoiceGenerator

Generates invoices with amounts, dates, statuses.

#### TransactionGenerator

Generates payment transactions.

#### ProductGenerator

Generates product catalog.

---

### Event Generators

#### EventGenerator

Generic event log generator:

```typescript
class EventGenerator {
  async generate(
    tenantId: string,
    eventsPerDay: number,
    durationDays: number
  ): Promise<void> {
    const totalEvents = eventsPerDay * durationDays;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - durationDays);

    const events = [];
    for (let i = 0; i < totalEvents; i++) {
      const timestamp = new Date(
        startDate.getTime() +
        Math.random() * durationDays * 24 * 60 * 60 * 1000
      );

      events.push({
        id: `evt-${i}`,
        lab_tenant_id: tenantId,
        event_type: randomChoice([
          'auth.login',
          'auth.logout',
          'api.request',
          'error.exception'
        ]),
        timestamp,
        severity: randomChoice(['debug', 'info', 'warning', 'error']),
        message: faker.lorem.sentence(),
        metadata: { /* additional context */ }
      });
    }

    await db.batchInsert('synthetic_events', events, 1000);
  }
}
```

#### AuthEventGenerator

Generates authentication events (login, logout, failures).

#### AccessLogGenerator

Generates API access logs with methods, paths, status codes.

#### NetworkEventGenerator

Generates network traffic events.

#### ErrorLogGenerator

Generates error logs with stack traces.

#### UsageMetricsGenerator

Generates time-series usage metrics.

---

## Chaos Data Injection

### DataChaos Module

```typescript
class DataChaos {
  async applyDataChaos(
    tenantId: string,
    config: DataChaosConfig
  ): Promise<void> {
    if (config.missing_values_pct > 0) {
      await this.injectMissingValues(tenantId, config.missing_values_pct);
    }

    if (config.duplicates_pct > 0) {
      await this.createDuplicates(tenantId, config.duplicates_pct);
    }

    if (config.conflicts_pct > 0) {
      await this.createConflicts(tenantId, config.conflicts_pct);
    }
  }

  async injectMissingValues(tenantId: string, pct: number): Promise<void> {
    // Randomly set email/phone to null in X% of customer records
    await db.raw(`
      UPDATE synthetic_customers
      SET email = NULL
      WHERE lab_tenant_id = ?
        AND random() < ?
    `, [tenantId, pct / 100]);
  }

  async createDuplicates(tenantId: string, pct: number): Promise<void> {
    // Duplicate X% of records with slight variations
    const customers = await db.select('*')
      .from('synthetic_customers')
      .where({ lab_tenant_id: tenantId })
      .limit(Math.floor(pct));

    const duplicates = customers.map(c => ({
      ...c,
      id: c.id + '-dup',
      name: c.name + ' Inc', // Slight variation
      created_at: new Date()
    }));

    await db.insert('synthetic_customers', duplicates);
  }
}
```

---

## Configuration

### Scale Configuration

```json
{
  "assets": {
    "applications": 100,
    "services": 150,
    "databases": 30,
    "hosts": 50
  },
  "business": {
    "organizations": 50,
    "customers": 500,
    "subscriptions": 300,
    "invoices": 1000
  },
  "events": {
    "per_day": 10000,
    "duration_days": 7,
    "types": ["auth", "access", "network", "error"]
  }
}
```

### Chaos Configuration

```json
{
  "data_chaos": {
    "enabled": true,
    "missing_values_pct": 5,
    "duplicates_pct": 3,
    "conflicts_pct": 2,
    "invalid_formats_pct": 1
  }
}
```

---

## Determinism and Seeds

All random generation uses a **seed** for reproducibility:

```typescript
const rng = seedrandom(scenario.config.seed || 12345);

function randomChoice(arr: any[]): any {
  return arr[Math.floor(rng() * arr.length)];
}
```

Same scenario + same seed → same data.

---

## Performance Optimization

### Batch Inserts

```typescript
await db.batchInsert('synthetic_customers', customers, 1000);
// Inserts 1000 records per transaction
```

### Parallel Generation

```typescript
await Promise.all([
  this.assetGenerators.generate(tenantId, scale.assets),
  this.businessGenerators.generate(tenantId, scale.business)
]);
```

### Progress Tracking

```typescript
logger.info('Generating applications', {
  tenantId,
  total: scale.assets.applications
});

for (let i = 0; i < scale.assets.applications; i += 100) {
  await generateBatch(i, Math.min(i + 100, scale.assets.applications));
  logger.debug('Progress', {
    completed: Math.min(i + 100, scale.assets.applications),
    total: scale.assets.applications
  });
}
```

---

## Testing

### Unit Tests

```typescript
test('ApplicationGenerator creates correct number of apps', async () => {
  const gen = new ApplicationGenerator(mockDb);
  await gen.generate('test-tenant', 50);
  expect(mockDb.insert).toHaveBeenCalledWith('synthetic_applications',
    expect.arrayContaining([expect.objectContaining({
      lab_tenant_id: 'test-tenant'
    })])
  );
  expect(mockDb.insert.mock.calls[0][1]).toHaveLength(50);
});
```

### Integration Tests

```typescript
test('E2E synthetic data generation', async () => {
  const engine = new SyntheticDataEngine(supabase);
  const result = await engine.generate('test-tenant', scaleConfig, chaosConfig);

  const apps = await supabase
    .from('synthetic_applications')
    .select('*')
    .eq('lab_tenant_id', 'test-tenant');

  expect(apps.data.length).toBe(scaleConfig.assets.applications);
});
```

---

## Future Enhancements

- **Template-based generation**: Define custom entity templates
- **Graph-based relationships**: More realistic dependency graphs
- **Time-series realism**: Seasonality, trends in metrics
- **Schema evolution**: Simulate schema changes over time
- **External data sources**: Import real (anonymized) datasets
