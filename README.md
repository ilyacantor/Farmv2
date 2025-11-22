# AOS-Farm

**Synthetic Enterprise Environment Orchestration for autonomOS Testing**

## Overview

AOS-Farm is a standalone service that generates and orchestrates **synthetic enterprise environments** to test the autonomOS platform. It provides comprehensive testing capabilities for the canonical autonomOS flow:

> **AOD → AAM → DCL → Agents**

## Purpose

- **End-to-End Testing**: Simulate realistic synthetic enterprises through the complete autonomOS pipeline
- **Module Testing**: Test AAM and DCL independently with controlled synthetic scenarios
- **Chaos Engineering**: Inject realistic failures, latency, and data quality issues
- **Reproducibility**: Define and re-run named scenarios deterministically

## Key Features

- 🏭 **Synthetic Data Generation**: Generate realistic enterprise assets, business data, and events
- 🎭 **Scenario Orchestration**: Run end-to-end or module-specific test scenarios
- 🌪️ **Chaos Injection**: Configurable chaos for latency, errors, schema drift, and data quality
- 📊 **Metrics & Reporting**: Collect and report metrics from each pipeline stage
- 🖥️ **Web GUI**: Simple, intuitive interface for scenario management and execution
- 🔌 **HTTP-Only Integration**: Clean boundaries with AOD, AAM, DCL, and Agent Orchestrator

## Architecture

AOS-Farm consists of five core components:

1. **Farm UI (Lab GUI)**: Web interface for scenario management and monitoring
2. **Orchestrator API**: Backend service coordinating test runs
3. **Synthetic Data Engine**: Generates enterprise assets, business data, and events
4. **Synthetic HTTP Services**: Emulates external systems (CRM, ERP, etc.)
5. **Chaos Engine**: Injects realistic failures and data quality issues

### System Boundaries

- **Database**: Supabase Postgres (only database used)
- **Integration**: HTTP-only communication with:
  - AOD (discovery engine)
  - AAM (adaptive API mesh)
  - DCL (data connectivity layer)
  - Agent Orchestrator
- **Repository**: Standalone repository, no shared code assumptions

## Scenario Types

### 1. End-to-End (E2E) Scenarios

Exercise the complete autonomOS pipeline with synthetic enterprises:

- Generate synthetic environment (assets, data, events)
- AOD discovers the environment
- AAM connects to synthetic systems
- DCL unifies the data
- Agents process unified data
- Collect metrics from all stages

### 2. AAM-Only Scenarios

Test Adaptive API Mesh independently:

- Connector behavior and resilience
- Retry and throttling logic
- Schema drift handling
- Error recovery

### 3. DCL-Only Scenarios

Test Data Connectivity Layer independently:

- Mapping and unification logic
- Data quality handling
- Schema drift detection
- Conflict resolution

## Quick Start

### Prerequisites

- Supabase account and project
- Node.js 18+ or Python 3.10+ (depending on implementation)
- Access to AOD, AAM, DCL, and Agent Orchestrator endpoints

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd AOS-Farm
   ```

2. Configure environment:
   ```bash
   cp config/.env.example .env
   # Edit .env with your Supabase credentials and service URLs
   ```

3. Initialize database:
   ```bash
   # Run Supabase migrations (instructions in database/README.md)
   ```

4. Start the backend:
   ```bash
   cd backend
   # Follow backend/README.md for installation and startup
   ```

5. Start the frontend:
   ```bash
   cd frontend
   # Follow frontend/README.md for installation and startup
   ```

6. Access the Lab GUI:
   ```
   Open browser to http://localhost:3000 (or configured port)
   ```

## Project Structure

```
/
├── backend/              # Orchestrator API and core services
│   ├── src/
│   │   ├── api/          # REST API endpoints
│   │   ├── orchestrator/ # Run coordination logic
│   │   ├── synthetic-data/     # Data generation engine
│   │   ├── synthetic-services/ # Fake HTTP systems
│   │   ├── chaos/        # Chaos injection engine
│   │   └── scenarios/    # Scenario management
│   └── config/           # Backend configuration
├── frontend/             # Lab GUI web interface
│   ├── src/
│   │   ├── components/   # Reusable UI components
│   │   ├── views/        # Main views (E2E Lab, AAM Lab, DCL Lab)
│   │   └── api/          # API client for backend
│   └── public/           # Static assets
├── database/             # Supabase schema and migrations
│   ├── migrations/       # SQL migration files
│   └── schema/           # Schema documentation
├── scenarios/            # Scenario definitions
│   ├── e2e/              # End-to-end scenarios
│   ├── aam/              # AAM-only scenarios
│   └── dcl/              # DCL-only scenarios
├── docs/                 # Documentation
│   ├── architecture.md   # Detailed architecture
│   ├── api-spec.md       # API specifications
│   ├── scenarios.md      # Scenario authoring guide
│   └── integration.md    # Integration guide for AOD/AAM/DCL
└── config/               # Configuration templates
    ├── .env.example      # Environment variables template
    └── scenarios.example.json
```

## Configuration

AOS-Farm requires the following environment variables:

```bash
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key

# autonomOS Services
AOD_BASE_URL=http://localhost:8001
AAM_BASE_URL=http://localhost:8002
DCL_BASE_URL=http://localhost:8003
AGENT_ORCH_BASE_URL=http://localhost:8004

# AOS-Farm
FARM_PORT=3001
FARM_UI_PORT=3000
```

See `config/.env.example` for complete configuration options.

## Documentation

- [Architecture](docs/architecture.md) - Detailed system design
- [API Specification](docs/api-spec.md) - REST API documentation
- [Scenario Authoring](docs/scenarios.md) - How to create scenarios
- [Integration Guide](docs/integration.md) - Integrating with AOD/AAM/DCL

## Development

### Adding a New Scenario

1. Create scenario definition in `scenarios/<type>/`
2. Define synthetic data requirements
3. Configure chaos parameters
4. Test via the Lab GUI

See [docs/scenarios.md](docs/scenarios.md) for detailed guidance.

### Extending Synthetic Data

Add new entity generators in `backend/src/synthetic-data/generators/`:

```
generators/
├── assets/       # Asset landscape generators
├── business/     # Business entity generators
└── events/       # Event/time-series generators
```

### Adding Chaos Patterns

Extend chaos behaviors in `backend/src/chaos/`:

```
chaos/
├── response/     # HTTP response chaos
├── schema/       # Schema drift chaos
└── data/         # Data quality chaos
```

## Testing

```bash
# Run backend tests
cd backend
npm test  # or pytest, depending on implementation

# Run frontend tests
cd frontend
npm test
```

## Contributing

This is an internal autonomOS testing tool. For contributions:

1. Create a feature branch
2. Make your changes
3. Test with multiple scenario types
4. Submit a pull request with clear description

## License

Proprietary - Internal autonomOS tool

## Support

For issues or questions:
- Create an issue in this repository
- Contact the autonomOS platform team
