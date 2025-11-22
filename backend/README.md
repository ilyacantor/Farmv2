# AOS-Farm Backend

## Overview

The AOS-Farm backend is the **Orchestrator API** that coordinates test runs, manages synthetic data, and integrates with autonomOS services (AOD, AAM, DCL, Agents).

## Architecture

```
backend/
├── src/
│   ├── api/                 # REST API endpoints and routes
│   ├── orchestrator/        # Run coordination logic
│   ├── synthetic-data/      # Data generation engine
│   ├── synthetic-services/  # Fake HTTP systems (CRM, ERP, etc.)
│   ├── chaos/               # Chaos injection engine
│   ├── scenarios/           # Scenario management
│   ├── db/                  # Database client and helpers
│   └── utils/               # Shared utilities
├── config/                  # Configuration files
├── tests/                   # Unit and integration tests
└── package.json / requirements.txt
```

---

## Technology Stack

**Recommended Options**:

**Option 1: Node.js + TypeScript**
- Express or Fastify (API framework)
- @supabase/supabase-js (database client)
- Axios (HTTP client for external services)
- TypeScript for type safety

**Option 2: Python**
- FastAPI or Flask (API framework)
- supabase-py (database client)
- httpx or requests (HTTP client)
- Pydantic for data validation

Choose based on team preference and existing autonomOS stack.

---

## Setup

### Prerequisites

- Node.js 18+ (if Node.js) or Python 3.10+ (if Python)
- Supabase project with migrations applied
- Environment variables configured

### Installation

**Node.js**:
```bash
cd backend
npm install
```

**Python**:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

### Configuration

Copy the example environment file:
```bash
cp ../config/.env.example .env
```

Edit `.env` with your values:
```bash
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-key

# autonomOS Services
AOD_BASE_URL=http://localhost:8001
AAM_BASE_URL=http://localhost:8002
DCL_BASE_URL=http://localhost:8003
AGENT_ORCH_BASE_URL=http://localhost:8004

# AOS-Farm
FARM_PORT=3001
LOG_LEVEL=info
```

### Database Setup

Ensure Supabase migrations are applied:
```bash
cd ../database
supabase db push
```

---

## Running

### Development Mode

**Node.js**:
```bash
npm run dev
```

**Python**:
```bash
uvicorn src.main:app --reload --port 3001
# or
python src/main.py
```

### Production Mode

**Node.js**:
```bash
npm run build
npm start
```

**Python**:
```bash
gunicorn -w 4 -k uvicorn.workers.UvicornWorker src.main:app
```

---

## API Endpoints

### Health Check

```bash
curl http://localhost:3001/health
```

### Scenarios

```bash
# List all scenarios
curl http://localhost:3001/api/scenarios

# Get scenario details
curl http://localhost:3001/api/scenarios/e2e-small-clean
```

### Runs

```bash
# Start a new run
curl -X POST http://localhost:3001/api/runs \
  -H "Content-Type: application/json" \
  -d '{"scenario_id": "e2e-small-clean"}'

# Get run status
curl http://localhost:3001/api/runs/{run_id}/status

# List runs
curl http://localhost:3001/api/runs?status=success
```

### Synthetic Services

```bash
# Get synthetic customers (requires X-Lab-Tenant-Id header)
curl http://localhost:3001/synthetic/crm/customers \
  -H "X-Lab-Tenant-Id: lab-550e8400-e29b-41d4-a716-446655440000"

# Get synthetic invoices
curl http://localhost:3001/synthetic/billing/invoices \
  -H "X-Lab-Tenant-Id: lab-550e8400-e29b-41d4-a716-446655440000"
```

See [API Specification](../docs/api-spec.md) for full details.

---

## Project Structure

### src/api/

REST API routes and controllers:

```
api/
├── routes/
│   ├── scenarios.ts         # Scenario endpoints
│   ├── runs.ts              # Run endpoints
│   └── synthetic.ts         # Synthetic service endpoints
├── controllers/
│   ├── ScenariosController.ts
│   ├── RunsController.ts
│   └── SyntheticController.ts
└── middleware/
    ├── errorHandler.ts
    ├── logging.ts
    └── validation.ts
```

### src/orchestrator/

Run coordination logic:

```
orchestrator/
├── RunOrchestrator.ts       # Main orchestrator
├── E2ERunner.ts             # E2E flow execution
├── ModuleRunner.ts          # Module-only flow execution
└── MetricsCollector.ts      # Collect metrics from external services
```

### src/synthetic-data/

Data generation engine:

```
synthetic-data/
├── generators/
│   ├── assets/              # Asset generators (apps, services, DBs, hosts)
│   ├── business/            # Business entity generators (customers, invoices)
│   └── events/              # Event generators (logs, metrics)
├── SyntheticDataEngine.ts   # Main engine
└── config/
    └── defaults.ts          # Default generation parameters
```

### src/synthetic-services/

Fake HTTP systems:

```
synthetic-services/
├── CRMService.ts            # Fake CRM API
├── BillingService.ts        # Fake billing/ERP API
├── AssetsService.ts         # Fake asset inventory API
├── EventsService.ts         # Fake events/logs API
└── BaseService.ts           # Shared service logic
```

### src/chaos/

Chaos injection:

```
chaos/
├── ChaosEngine.ts           # Main chaos coordinator
├── ResponseChaos.ts         # HTTP response chaos (latency, errors)
├── SchemaChaos.ts           # Schema drift chaos
└── DataChaos.ts             # Data quality chaos
```

### src/scenarios/

Scenario management:

```
scenarios/
├── ScenarioManager.ts       # Load and manage scenarios
├── ScenarioValidator.ts     # Validate scenario configs
└── loader.ts                # Load scenarios from JSON files
```

### src/db/

Database client and utilities:

```
db/
├── supabase.ts              # Supabase client setup
├── repositories/
│   ├── RunsRepository.ts
│   ├── ScenariosRepository.ts
│   └── SyntheticDataRepository.ts
└── migrations.ts            # Migration helpers (if needed)
```

---

## Development Workflow

### 1. Adding a New API Endpoint

1. Create route in `src/api/routes/`
2. Create controller method in `src/api/controllers/`
3. Add validation middleware if needed
4. Test with curl or Postman
5. Document in `docs/api-spec.md`

### 2. Adding a New Synthetic Data Generator

1. Create generator in `src/synthetic-data/generators/<type>/`
2. Implement generator interface:
   ```typescript
   interface Generator {
     generate(config: GeneratorConfig, tenantId: string): Promise<void>;
   }
   ```
3. Register in `SyntheticDataEngine`
4. Add to scenario config schema
5. Test with a scenario

### 3. Adding a New Chaos Pattern

1. Create chaos module in `src/chaos/`
2. Implement chaos interface:
   ```typescript
   interface ChaosModule {
     apply(data: any, config: ChaosConfig): any;
   }
   ```
3. Register in `ChaosEngine`
4. Add to scenario chaos config options
5. Test with a chaotic scenario

### 4. Testing Integration with External Services

1. Start local instances of AOD/AAM/DCL (or point to dev env)
2. Create a test scenario
3. Run via API:
   ```bash
   curl -X POST http://localhost:3001/api/runs \
     -d '{"scenario_id": "test-integration"}'
   ```
4. Monitor logs for HTTP calls and responses
5. Verify metrics collection

---

## Testing

### Unit Tests

**Node.js**:
```bash
npm test
```

**Python**:
```bash
pytest
```

### Integration Tests

```bash
npm run test:integration
# or
pytest tests/integration/
```

### E2E Tests

Requires all services running:
```bash
npm run test:e2e
# or
pytest tests/e2e/
```

---

## Logging

Structured logging with correlation IDs:

```javascript
logger.info('Starting run', {
  runId: '550e8400-e29b-41d4-a716-446655440000',
  tenantId: 'lab-550e8400-e29b-41d4-a716-446655440000',
  scenarioId: 'e2e-small-clean'
});
```

Log levels: `debug`, `info`, `warn`, `error`

Configure via `LOG_LEVEL` environment variable.

---

## Error Handling

All errors follow this structure:

```json
{
  "error": {
    "code": "INVALID_SCENARIO",
    "message": "Scenario 'invalid-id' not found",
    "details": {}
  }
}
```

Common error codes:
- `INVALID_SCENARIO`
- `INVALID_CONFIG`
- `RUN_NOT_FOUND`
- `EXTERNAL_SERVICE_ERROR`
- `INTERNAL_ERROR`

---

## Performance Considerations

### Concurrent Runs

- Each run operates independently (different `lab_tenant_id`)
- Resource limits configurable via `MAX_CONCURRENT_RUNS`
- Queue system for run scheduling (optional)

### Database Queries

- All synthetic data queries include `lab_tenant_id` filter
- Indexes optimize tenant-specific queries
- Connection pooling configured for Supabase

### Synthetic Data Generation

- Large-scale generation is batched (e.g., 1000 records per insert)
- Async/parallel generation where possible
- Configurable timeouts

---

## Deployment

### Docker (Optional)

```dockerfile
FROM node:18-alpine
# or FROM python:3.10-slim

WORKDIR /app
COPY package.json .
RUN npm install --production
# or: COPY requirements.txt . && pip install -r requirements.txt

COPY . .
EXPOSE 3001
CMD ["npm", "start"]
# or: CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "3001"]
```

Build and run:
```bash
docker build -t aos-farm-backend .
docker run -p 3001:3001 --env-file .env aos-farm-backend
```

### Environment Variables in Production

- Use secret management (AWS Secrets Manager, HashiCorp Vault, etc.)
- Never commit `.env` to version control
- Validate all required env vars at startup

---

## Troubleshooting

### Issue: Cannot connect to Supabase

**Check**:
- `SUPABASE_URL` and `SUPABASE_KEY` are correct
- Network connectivity (firewall, VPN)
- Supabase project is running

### Issue: External service call fails

**Debug**:
- Enable HTTP request logging: `LOG_HTTP_REQUESTS=true`
- Check service base URLs
- Verify network connectivity
- Test service independently with curl

### Issue: Synthetic data not appearing

**Check**:
- `lab_tenant_id` is being passed correctly
- RLS policies are not blocking queries (use service key)
- Data generation completed successfully (check logs)

---

## Contributing

1. Follow existing code structure and naming conventions
2. Add unit tests for new functionality
3. Update API docs if adding endpoints
4. Use linting and formatting tools
5. Test integration with external services before committing

---

## Resources

- [API Specification](../docs/api-spec.md)
- [Architecture](../docs/architecture.md)
- [Scenario Authoring](../docs/scenarios.md)
- [Integration Guide](../docs/integration.md)
