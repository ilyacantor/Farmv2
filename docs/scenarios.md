# AOS-Farm Scenario Authoring Guide

## Overview

Scenarios are the core building blocks of AOS-Farm testing. Each scenario defines:

- **Type**: End-to-end (E2E) or module-specific (AAM/DCL)
- **Scale**: Volume of synthetic data
- **Chaos Profile**: Types and intensity of failures
- **Data Characteristics**: Clean vs. messy data
- **Expected Behavior**: Pass/fail criteria

---

## Scenario Structure

Scenarios are defined as JSON files in the `scenarios/` directory:

```
scenarios/
├── e2e/
│   ├── small-clean.json
│   ├── medium-chaotic.json
│   └── large-drift.json
├── aam/
│   ├── high-latency.json
│   ├── schema-drift.json
│   └── error-recovery.json
└── dcl/
    ├── conflict-resolution.json
    ├── data-quality.json
    └── mapping-coverage.json
```

---

## Scenario Schema

### E2E Scenario Example

```json
{
  "id": "e2e-small-clean",
  "name": "Small Clean Enterprise",
  "description": "Small enterprise with clean data and no chaos. Baseline test for happy path.",
  "type": "e2e",
  "tags": ["small", "clean", "baseline"],
  "config": {
    "scale": {
      "assets": {
        "applications": 50,
        "services": 80,
        "databases": 20,
        "hosts": 30
      },
      "business": {
        "customers": 200,
        "invoices": 500,
        "subscriptions": 150
      },
      "events": {
        "per_day": 5000,
        "duration_days": 7
      }
    },
    "chaos": {
      "response_chaos": {
        "enabled": false
      },
      "schema_chaos": {
        "enabled": false
      },
      "data_chaos": {
        "enabled": false
      }
    },
    "seed": 12345,
    "synthetic_services": [
      {
        "type": "crm",
        "base_path": "/synthetic/crm"
      },
      {
        "type": "billing",
        "base_path": "/synthetic/billing"
      },
      {
        "type": "assets",
        "base_path": "/synthetic/assets"
      }
    ],
    "expected_outcomes": {
      "aod": {
        "min_assets_discovered": 150,
        "max_errors": 0
      },
      "aam": {
        "min_connector_availability_pct": 99,
        "max_error_rate_pct": 1
      },
      "dcl": {
        "min_mapping_coverage_pct": 95,
        "max_conflicts": 5
      },
      "agents": {
        "min_recommendations": 1
      }
    }
  }
}
```

---

### AAM Module Scenario Example

```json
{
  "id": "aam-high-latency",
  "name": "High Latency Connectors",
  "description": "Test AAM resilience with slow, unreliable upstream services.",
  "type": "module",
  "module": "aam",
  "tags": ["medium", "high-latency", "resilience"],
  "config": {
    "scale": {
      "sources": 5,
      "records_per_source": 1000
    },
    "chaos": {
      "response_chaos": {
        "enabled": true,
        "latency_ms": {
          "p50": 500,
          "p95": 2000,
          "p99": 5000
        },
        "error_rate_pct": 10,
        "timeout_rate_pct": 5
      },
      "schema_chaos": {
        "enabled": false
      },
      "data_chaos": {
        "enabled": false
      }
    },
    "seed": 23456,
    "synthetic_services": [
      {
        "type": "crm",
        "base_path": "/synthetic/crm"
      },
      {
        "type": "billing",
        "base_path": "/synthetic/billing"
      }
    ],
    "expected_outcomes": {
      "aam": {
        "min_connector_availability_pct": 80,
        "max_retry_count": 50,
        "min_eventual_success_pct": 90
      }
    }
  }
}
```

---

### DCL Module Scenario Example

```json
{
  "id": "dcl-conflict-resolution",
  "name": "Conflict Resolution Test",
  "description": "Test DCL's ability to handle conflicting customer records from multiple sources.",
  "type": "module",
  "module": "dcl",
  "tags": ["medium", "conflicts", "data-quality"],
  "config": {
    "scale": {
      "sources": 3,
      "customers_per_source": 500
    },
    "chaos": {
      "response_chaos": {
        "enabled": false
      },
      "schema_chaos": {
        "enabled": false
      },
      "data_chaos": {
        "enabled": true,
        "conflicts_pct": 15,
        "missing_values_pct": 5,
        "duplicates_pct": 8
      }
    },
    "seed": 34567,
    "data_overlaps": {
      "customer_overlap_pct": 40,
      "conflict_rate_pct": 30
    },
    "expected_outcomes": {
      "dcl": {
        "min_mapping_coverage_pct": 85,
        "max_unresolved_conflicts_pct": 10,
        "conflict_resolution_strategy": "most_recent"
      }
    }
  }
}
```

---

## Configuration Options

### Scale Configuration

**Assets**:
```json
"assets": {
  "applications": 50,      // Number of applications
  "services": 80,          // Number of services
  "databases": 20,         // Number of databases
  "hosts": 30,             // Number of hosts
  "relationships": "auto"  // Auto-generate dependencies
}
```

**Business Entities**:
```json
"business": {
  "customers": 200,
  "organizations": 50,
  "subscriptions": 150,
  "invoices": 500,
  "transactions": 2000
}
```

**Events**:
```json
"events": {
  "per_day": 5000,
  "duration_days": 7,
  "types": ["auth", "access", "network", "error"]
}
```

---

### Chaos Configuration

**Response Chaos** (HTTP behavior):
```json
"response_chaos": {
  "enabled": true,
  "latency_ms": {
    "p50": 100,
    "p95": 500,
    "p99": 2000
  },
  "error_rate_pct": 5,          // % of requests that return 500
  "timeout_rate_pct": 2,         // % of requests that timeout
  "rate_limit_pct": 3,           // % of requests that return 429
  "jitter_enabled": true         // Add random variance
}
```

**Schema Chaos** (data structure changes):
```json
"schema_chaos": {
  "enabled": true,
  "field_add_pct": 10,           // % of responses with new fields
  "field_remove_pct": 5,         // % of responses missing fields
  "field_rename_pct": 2,         // % of responses with renamed fields
  "type_change_pct": 1,          // % of responses with type changes
  "nesting_change_pct": 3        // % of responses with structure changes
}
```

**Data Chaos** (data quality issues):
```json
"data_chaos": {
  "enabled": true,
  "missing_values_pct": 8,       // % of records with null/empty values
  "duplicates_pct": 3,           // % of duplicate records
  "conflicts_pct": 5,            // % of conflicting records
  "invalid_formats_pct": 2,      // % of records with format errors
  "referential_violations_pct": 1 // % of broken references
}
```

---

### Synthetic Services Configuration

```json
"synthetic_services": [
  {
    "type": "crm",
    "base_path": "/synthetic/crm",
    "auth": {
      "type": "api_key",
      "header": "X-API-Key"
    },
    "pagination": {
      "type": "offset",
      "default_limit": 100,
      "max_limit": 1000
    }
  },
  {
    "type": "billing",
    "base_path": "/synthetic/billing",
    "auth": {
      "type": "bearer"
    }
  }
]
```

---

### Expected Outcomes

Define pass/fail criteria for the scenario:

```json
"expected_outcomes": {
  "aod": {
    "min_assets_discovered": 150,
    "max_errors": 5,
    "min_coverage_pct": 90
  },
  "aam": {
    "min_connector_availability_pct": 95,
    "max_error_rate_pct": 3,
    "max_avg_latency_ms": 500
  },
  "dcl": {
    "min_mapping_coverage_pct": 90,
    "max_conflicts": 20,
    "max_unmapped_fields": 10
  },
  "agents": {
    "min_recommendations": 5,
    "max_errors": 0
  }
}
```

---

## Scenario Tags

Use tags to categorize and filter scenarios:

**Scale Tags**:
- `small` (< 500 total entities)
- `medium` (500 - 5000 entities)
- `large` (> 5000 entities)

**Chaos Tags**:
- `clean` (no chaos)
- `low-chaos` (< 5% chaos)
- `medium-chaos` (5-15% chaos)
- `high-chaos` (> 15% chaos)

**Focus Tags**:
- `baseline` (happy path test)
- `resilience` (error handling)
- `performance` (high load)
- `drift` (schema changes)
- `conflicts` (data quality)
- `latency` (slow responses)

**Module Tags**:
- `aam-focus`
- `dcl-focus`
- `aod-focus`
- `agents-focus`

---

## Creating a New Scenario

### Step 1: Define the Goal

What are you testing?
- Happy path baseline?
- Error recovery?
- Schema drift handling?
- Data quality issues?
- Performance under load?

### Step 2: Choose Type and Module

- **E2E**: Test full pipeline
- **Module**: Test AAM or DCL independently

### Step 3: Set Scale

Start small, then increase:
- Small: 100-500 entities
- Medium: 1000-5000 entities
- Large: 10000+ entities

### Step 4: Configure Chaos

Match chaos to your goal:
- **Baseline**: All chaos disabled
- **Resilience**: High error rates, timeouts
- **Drift**: Schema changes enabled
- **Quality**: Data chaos enabled

### Step 5: Define Expected Outcomes

Set realistic thresholds:
- What's the minimum acceptable performance?
- What error rate is tolerable?
- What conflicts are expected?

### Step 6: Test and Iterate

1. Run the scenario via the UI
2. Review metrics and logs
3. Adjust configuration
4. Re-run and validate

---

## Example Scenarios

### 1. Baseline E2E

**Goal**: Verify happy path works end-to-end

**Configuration**:
- Small scale
- No chaos
- Clean data
- Expect: >95% success on all stages

---

### 2. AAM Error Recovery

**Goal**: Test AAM handles upstream failures gracefully

**Configuration**:
- Medium scale
- 15% error rate
- 5% timeout rate
- Expect: Retries work, eventual consistency

---

### 3. DCL Schema Drift

**Goal**: Test DCL detects and handles schema changes

**Configuration**:
- Medium scale
- 20% schema drift (new/missing fields)
- Clean data otherwise
- Expect: Drift detected, mapping updated

---

### 4. E2E High Chaos

**Goal**: Stress test entire pipeline

**Configuration**:
- Large scale
- High chaos on all levels
- Expect: System degrades gracefully, recovers

---

## Best Practices

1. **Start Simple**: Begin with clean, small scenarios
2. **One Variable**: Change one thing at a time (scale OR chaos)
3. **Realistic Chaos**: Don't exceed real-world failure rates
4. **Document Intent**: Clear description and expected outcomes
5. **Deterministic**: Use fixed seeds for reproducibility
6. **Tag Clearly**: Use descriptive tags for filtering
7. **Incremental**: Build complex scenarios from simpler ones
8. **Validate**: Run scenarios multiple times to ensure consistency

---

## Scenario Lifecycle

1. **Draft**: Create JSON file with configuration
2. **Test**: Run via UI, validate behavior
3. **Refine**: Adjust scale, chaos, expectations
4. **Document**: Add clear description and tags
5. **Commit**: Add to repository
6. **Monitor**: Track results over time
7. **Evolve**: Update as autonomOS changes

---

## Troubleshooting

**Scenario fails immediately**:
- Check configuration syntax
- Verify service URLs are correct
- Ensure scale is reasonable

**Chaos doesn't seem to apply**:
- Verify `enabled: true` in chaos config
- Check percentage values are reasonable
- Review logs for chaos engine activity

**Metrics don't match expectations**:
- Review expected_outcomes thresholds
- Check if chaos is too aggressive
- Validate synthetic data generation

**Run never completes**:
- Check for deadlocks in orchestration
- Verify external services are responding
- Review timeout configurations

---

## Future Enhancements

- **Visual Scenario Builder**: GUI for creating scenarios
- **Scenario Versioning**: Track changes over time
- **Scenario Templates**: Pre-built templates for common patterns
- **A/B Testing**: Compare two scenario variants
- **Scenario Composition**: Combine smaller scenarios
- **Dynamic Chaos**: Chaos that changes during run
