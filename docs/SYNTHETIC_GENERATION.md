# Synthetic Data Generation

## Overview

AOS Farm generates synthetic enterprise IT environments for testing Asset Observability & Discovery (AOD) systems. The generator creates realistic, conflicted datasets that simulate the complexity of real enterprise environments—not "happy path" demo data.

## Core Concepts

### Data Planes

Farm generates 7 independent data planes that correlate only via realistic keys:

| Plane | Description | Key Fields |
|-------|-------------|------------|
| **Discovery** | Raw observations from network sources | domain, source, timestamp |
| **IdP** | Identity provider records | email, app_name, last_login |
| **CMDB** | Configuration management database | name, domain, environment |
| **Cloud** | Cloud provider inventory | account_id, service, region |
| **Endpoint** | Device/agent telemetry | hostname, agent_version |
| **Network** | DNS/proxy/firewall logs | domain, action, bytes |
| **Finance** | Spend and billing data | vendor, amount, contract_status |

### Coupled Evidence Generation

Assets receive observations from multiple distinct sources to pass AOD's noise floor (default: 2 sources required for admission).

| Asset Tier | Source Count | Admission Rate |
|------------|--------------|----------------|
| **Core Stack** (first 25 SaaS) | 3+ distinct sources | 100% |
| **Departmental** (remaining SaaS) | 2 distinct sources | 100% |
| **Shadow apps** (40%) | 2 sources | Admitted as shadows |
| **Shadow apps** (60%) | 1 source | Rejected (noise floor) |
| **Zombie apps** | 2 sources (stale) | Admitted as zombies |
| **Junk/noise domains** | 1 source | Correctly rejected |
| **Near collisions** | 1 source | Correctly rejected |

This produces admission rates of 20-30%, matching industry benchmarks (Netskope, Zscaler, CASB audits).

## Configuration Parameters

### Scale

Controls the base volume of generated assets:

| Scale | SaaS Apps | Internal Services | Datastores |
|-------|-----------|-------------------|------------|
| small | 10 | 5 | 3 |
| medium | 25 | 15 | 8 |
| large | 50 | 30 | 15 |
| enterprise | 100 | 60 | 30 |

### Volume Multiplier

Scales all generation formulas by 1-50x for enterprise-scale testing:

| Multiplier | Observations | Unique Assets | Admitted |
|------------|--------------|---------------|----------|
| 1x | ~1,500 | ~300 | ~50 |
| 5x | ~7,900 | ~1,000 | ~216 |
| 10x | ~13,400 | ~1,550 | ~428 |
| 15x | ~20,000 | ~2,100 | ~684 |

When exceeding static app lists, synthetic assets are generated with realistic domains (e.g., `cloudify.io`, `smartbase.com`).

### Enterprise Profile

Simulates different industry IT landscapes:

| Profile | Characteristics |
|---------|-----------------|
| **modern_saas** | Cloud-heavy, API-first, minimal legacy |
| **regulated_finance** | Legacy systems, compliance overhead, on-prem |
| **hybrid_enterprise** | Mix of cloud and traditional infrastructure |

### Realism Profile

Controls data quality and conflict levels:

| Profile | Description |
|---------|-------------|
| **clean** | Minimal conflicts, consistent data |
| **typical** | Normal enterprise messiness |
| **messy** | Significant conflicts, partial telemetry, duplicates |

### Data Preset (Challenge Level)

Pre-configured difficulty levels:

| Preset | Domain Coverage | Conflict Rate | Junk Domains | Near Collisions | Aliasing |
|--------|-----------------|---------------|--------------|-----------------|----------|
| **clean_baseline** | High | Low | Few | None | None |
| **enterprise_mess** | Medium | Medium | Some | Some | Some |
| **adversarial** | Variable | High | Many | Many | High |

## Stress Test Scenarios

Every snapshot includes 4 deterministic stress tests:

### 1. Split Brain (Monday.com)
- **Setup**: Finance vendor (name-only) + Network DNS/Proxy (domain-based)
- **Tests**: AOD's merge logic for multi-signal assets

### 2. Toxic Asset (Trello)
- **Setup**: CMDB=yes, IdP=no
- **Tests**: Identity gap detection

### 3. Banned Asset (TikTok)
- **Setup**: Discovery observations for blocked domain
- **Tests**: Banned domain detection

### 4. Zombie Asset (Zoom Legacy)
- **Setup**: CMDB+IdP present but stale >90 days
- **Tests**: Staleness detection

## Ground Truth Classification

Admitted assets are classified based on evidence flags:

| Classification | Criteria |
|----------------|----------|
| **Shadow** | HAS_ONGOING_FINANCE=false, not governed by vendor |
| **Zombie** | All evidence stale (>90 days) |
| **Clean** | Properly governed, recent evidence |

### Vendor Governance Propagation

Domains within known vendor sets inherit governance from the parent vendor. For example, if `microsoft.com` is governed, then `outlook.com`, `azure.com`, `office365.com` are also considered governed.

## Expected Block

Each snapshot includes an `__expected__` block containing:

```json
{
  "expected_shadows": ["shadow-app.com", ...],
  "expected_zombies": ["stale-tool.com", ...],
  "expected_clean": ["governed-saas.com", ...],
  "expected_rejected": ["noise.example.com", ...],
  "expected_totals": {
    "shadow": 48,
    "zombie": 41,
    "clean": 127,
    "admitted": 216,
    "rejected": 843
  }
}
```

This provides ground truth for grading AOD's performance.

## API Usage

### Generate Snapshot

```bash
curl -X POST "http://localhost:5000/api/snapshots" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "TestCorp",
    "scale": "large",
    "realism_profile": "messy",
    "data_preset": "adversarial",
    "volume_multiplier": 10
  }'
```

### Response

```json
{
  "snapshot_id": "abc123...",
  "meta": {
    "schema_version": "farm.v1",
    "generated_at": "2025-12-27T12:00:00Z",
    "scale": "large",
    "volume_multiplier": 10
  }
}
```

## Design Principles

1. **No Conclusions**: Farm generates raw evidence, not pre-concluded classifications
2. **Deterministic**: Same seed + config = same output
3. **Plane Independence**: Each data plane is generated independently
4. **FQDN Validation**: Only valid fully-qualified domain names are admitted
5. **Fail Loudly**: Invalid data produces explicit errors, not silent fallbacks
