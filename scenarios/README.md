# AOS-Farm Scenarios

## Overview

This directory contains **predefined test scenarios** for the AOS-Farm system. Each scenario is a JSON file defining scale, chaos configuration, and expected outcomes.

## Directory Structure

```
scenarios/
├── e2e/                    # End-to-end scenarios
│   ├── small-clean.json
│   └── medium-chaotic.json
├── aam/                    # AAM-only scenarios
│   ├── high-latency.json
│   └── schema-drift.json
└── dcl/                    # DCL-only scenarios
    ├── conflict-resolution.json
    └── data-quality.json
```

---

## Scenario Types

### E2E Scenarios

Test the complete autonomOS pipeline:
- **AOD** discovers synthetic assets
- **AAM** connects to synthetic services
- **DCL** unifies multi-source data
- **Agents** process unified data

**Examples**:
- `small-clean.json` - Baseline happy path test
- `medium-chaotic.json` - Moderate chaos across all layers

### AAM Scenarios

Test Adaptive API Mesh independently:
- Connector resilience
- Retry and throttling behavior
- Schema drift detection and adaptation
- Error recovery

**Examples**:
- `high-latency.json` - Slow, unreliable upstream services
- `schema-drift.json` - Frequent API schema changes

### DCL Scenarios

Test Data Connectivity Layer independently:
- Mapping and unification logic
- Conflict detection and resolution
- Data quality handling
- Schema drift detection

**Examples**:
- `conflict-resolution.json` - Conflicting customer records
- `data-quality.json` - Missing values, invalid formats

---

## Scenario Schema

Each scenario file contains:

```json
{
  "id": "unique-scenario-id",
  "name": "Human-readable name",
  "description": "What this scenario tests",
  "type": "e2e | module",
  "module": "aam | dcl | null",
  "tags": ["small", "clean", "chaos", ...],
  "config": {
    "scale": { /* volume configuration */ },
    "chaos": { /* chaos configuration */ },
    "seed": 12345,
    "synthetic_services": [ /* services to expose */ ],
    "expected_outcomes": { /* pass/fail thresholds */ }
  }
}
```

---

## Scale Configuration

### Assets (for E2E and asset-focused scenarios)

```json
"scale": {
  "assets": {
    "applications": 100,
    "services": 150,
    "databases": 40,
    "hosts": 60
  }
}
```

### Business Data (for DCL scenarios)

```json
"scale": {
  "business": {
    "organizations": 50,
    "customers": 500,
    "subscriptions": 300,
    "invoices": 1000,
    "transactions": 3000,
    "products": 100
  }
}
```

### Events (for time-series data)

```json
"scale": {
  "events": {
    "per_day": 10000,
    "duration_days": 7,
    "types": ["auth", "access", "network", "error"]
  }
}
```

---

## Chaos Configuration

### Response Chaos (HTTP behavior)

```json
"chaos": {
  "response_chaos": {
    "enabled": true,
    "latency_ms": {
      "p50": 100,
      "p95": 500,
      "p99": 2000
    },
    "error_rate_pct": 5,
    "timeout_rate_pct": 2
  }
}
```

### Schema Chaos (API evolution)

```json
"chaos": {
  "schema_chaos": {
    "enabled": true,
    "field_add_pct": 10,
    "field_remove_pct": 5,
    "field_rename_pct": 2,
    "type_change_pct": 1
  }
}
```

### Data Chaos (data quality)

```json
"chaos": {
  "data_chaos": {
    "enabled": true,
    "missing_values_pct": 8,
    "duplicates_pct": 3,
    "conflicts_pct": 5,
    "invalid_formats_pct": 2
  }
}
```

---

## Expected Outcomes

Define pass/fail criteria:

```json
"expected_outcomes": {
  "aod": {
    "min_assets_discovered": 150,
    "max_errors": 5
  },
  "aam": {
    "min_connector_availability_pct": 95,
    "max_error_rate_pct": 3
  },
  "dcl": {
    "min_mapping_coverage_pct": 90,
    "max_conflicts": 20
  },
  "agents": {
    "min_recommendations": 5
  }
}
```

---

## Tags

Use tags for filtering and categorization:

**Scale Tags**:
- `small` - < 500 total entities
- `medium` - 500-5000 entities
- `large` - > 5000 entities

**Chaos Tags**:
- `clean` - No chaos
- `low-chaos` - < 5% chaos
- `medium-chaos` - 5-15% chaos
- `high-chaos` - > 15% chaos

**Focus Tags**:
- `baseline` - Happy path
- `resilience` - Error handling
- `drift` - Schema changes
- `conflicts` - Data quality
- `latency` - Performance

**Module Tags**:
- `aam-focus` - AAM-specific testing
- `dcl-focus` - DCL-specific testing
- `aod-focus` - AOD-specific testing

---

## Creating a New Scenario

1. **Copy an existing scenario** as a template
2. **Modify the configuration**:
   - Set unique `id` and `name`
   - Adjust `scale` for desired volume
   - Configure `chaos` to match test goals
   - Define `expected_outcomes`
3. **Add appropriate tags**
4. **Test the scenario** via the UI
5. **Document** what it tests and why

---

## Example: Creating a New Scenario

### Goal

Test DCL with extreme data quality issues.

### Steps

1. Copy `dcl/data-quality.json` to `dcl/extreme-data-quality.json`

2. Modify configuration:
```json
{
  "id": "dcl-extreme-data-quality",
  "name": "Extreme Data Quality Issues",
  "description": "Push DCL to its limits with severe data quality problems",
  "type": "module",
  "module": "dcl",
  "tags": ["medium", "data-quality", "stress-test", "dcl-focus"],
  "config": {
    "scale": {
      "sources": 3,
      "customers_per_source": 800
    },
    "chaos": {
      "data_chaos": {
        "enabled": true,
        "missing_values_pct": 30,
        "duplicates_pct": 20,
        "conflicts_pct": 15,
        "invalid_formats_pct": 25
      }
    },
    "expected_outcomes": {
      "dcl": {
        "min_mapping_coverage_pct": 50,
        "max_unmapped_fields": 200,
        "data_quality_score_min": 40
      }
    }
  }
}
```

3. Run via UI and validate behavior

4. Commit to repository

---

## Scenario Loading

Scenarios are loaded by the backend at startup:

```typescript
// Load all scenarios from JSON files
const scenariosPath = './scenarios';
const scenarios = [
  ...loadScenariosFromDir(`${scenariosPath}/e2e`),
  ...loadScenariosFromDir(`${scenariosPath}/aam`),
  ...loadScenariosFromDir(`${scenariosPath}/dcl`)
];

// Store in database or memory
await scenarioRepository.upsert(scenarios);
```

---

## Best Practices

1. **Descriptive Names**: Clear, self-explanatory scenario names
2. **Realistic Chaos**: Match real-world failure rates
3. **Incremental Complexity**: Start simple, add chaos gradually
4. **Document Intent**: Explain what the scenario validates
5. **Set Achievable Expectations**: Don't expect 100% success with high chaos
6. **Use Seeds**: Fixed seeds for reproducibility
7. **Tag Appropriately**: Makes filtering easier

---

## Scenario Versioning

As autonomOS evolves, scenarios may need updates:

1. **Don't modify existing scenarios** if they're in use
2. **Create new versions** (e.g., `scenario-v2.json`)
3. **Deprecate old scenarios** by adding `"deprecated": true`
4. **Document changes** in scenario description

---

## Future Enhancements

- **Parameterized Scenarios**: Variables that can be set at runtime
- **Scenario Composition**: Combine smaller scenarios
- **Scenario Suites**: Run multiple related scenarios sequentially
- **Dynamic Chaos**: Chaos that evolves during the run
- **A/B Testing**: Compare two scenario variants
