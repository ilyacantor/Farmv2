# Farm Refactoring Plan: PolicyConfig Integration

## Current State Analysis

### Hardcoded Thresholds (Must Be Parameterized)

| Location | Current Hardcode | Config Replacement |
|----------|-----------------|-------------------|
| `reconciliation.py:421` | `discovery_sources_count >= 2` | `config.admission.noise_floor` |
| `reconciliation.py:433` | `90 days` stale window | `config.admission.zombie_window_days` |
| `constants.py:1-21` | `INFRASTRUCTURE_DOMAINS` set | `config.exclusions` + `config.scope.include_infra` |
| `enterprise.py:339-345` | Shadow count by profile | Derive from `noise_floor` |
| `enterprise.py:476-494` | Shadow source count (implicit) | Must exceed `noise_floor` |
| Finance admission | **MISSING** | `config.admission.minimum_spend` |

### Generator Functions Needing Parameterization

```
src/generators/enterprise.py
├── _pick_shadow_and_zombie_targets() → line 339
│   └── Hardcoded shadow_count, zombie_count dicts
├── generate_discovery_plane() → line 425
│   └── Shadow apps get 2+ sources (implicit)
│   └── Need: ensure source_count > noise_floor
├── generate_finance_plane() → line 818
│   └── Shadow apps generate "hidden spend"
│   └── Need: parameterize spend thresholds
```

### Reconciliation Functions Needing Config

```
src/services/reconciliation.py
├── compute_expected_block() → line 353
│   └── Uses hardcoded admission logic
│   └── Uses hardcoded 90-day window
│   └── Uses static INFRASTRUCTURE_DOMAINS
```

---

## Implementation Plan

### Phase 1: PolicyConfig Schema (src/models/policy.py)

```python
from pydantic import BaseModel
from typing import Optional

class AdmissionConfig(BaseModel):
    minimum_spend: int = 200          # Finance gate threshold ($)
    noise_floor: int = 2              # Min sources for discovery gate
    zombie_window_days: int = 90      # Inactivity threshold

class ScopeConfig(BaseModel):
    include_infra: bool = False       # Include infra domains
    treat_directory_as_idp: bool = False  # Startup mode

class PolicyConfig(BaseModel):
    admission: AdmissionConfig = AdmissionConfig()
    scope: ScopeConfig = ScopeConfig()
    exclusions: list[str] = []        # Kill list domains

    @classmethod
    def from_aod_response(cls, data: dict) -> "PolicyConfig":
        """Parse AOD's /api/v1/policy/config response."""
        return cls(**data)
```

### Phase 2: AOD Client Extension (src/services/aod_client.py)

Add method to fetch policy config:

```python
async def fetch_policy_config(self) -> Optional[PolicyConfig]:
    """Fetch active policy configuration from AOD."""
    try:
        response = await self.client.get(f"{self.base_url}/api/v1/policy/config")
        if response.status_code == 200:
            return PolicyConfig.from_aod_response(response.json())
        return None
    except Exception:
        return None  # Fall back to defaults
```

### Phase 3: Generator Refactor

**Current signature:**
```python
class EnterpriseGenerator:
    def __init__(self, seed, scale, enterprise_profile, realism_profile, tenant_id):
```

**New signature:**
```python
class EnterpriseGenerator:
    def __init__(self, seed, scale, enterprise_profile, realism_profile, tenant_id, 
                 policy_config: Optional[PolicyConfig] = None):
        self.policy = policy_config or PolicyConfig.default_fallback()
```

**Key changes:**

1. **Shadow generation** - Ensure `source_count > policy.admission.noise_floor`
   ```python
   # line ~476-494
   sources_needed = self.policy.admission.noise_floor + 1
   for shadow_app in self._shadow_apps:
       sources = self.rng.sample(['dns', 'proxy', 'sso', 'browser', 'network'], 
                                  k=sources_needed)
       # ... generate observation per source
   ```

2. **Finance generation** - Parameterize spend thresholds
   ```python
   # For admitted shadow: spend >= minimum_spend
   admitted_spend = self.policy.admission.minimum_spend + self.rng.randint(1, 500)
   
   # For rejected noise: spend < minimum_spend
   rejected_spend = self.rng.randint(10, self.policy.admission.minimum_spend - 1)
   ```

3. **Kill list awareness** - Use AOD-provided seeds, NOT local constants
   ```python
   # CRITICAL: Do NOT use local INFRASTRUCTURE_DOMAINS constant
   # Use the list from PolicyConfig which comes from AOD
   def _is_excluded(self, domain: str) -> bool:
       return self.policy.is_excluded(domain)
   ```
   
   **This prevents "Constant Drift"** where Farm and AOD disagree on definitions.

### Phase 4: Reconciliation Refactor

**Current signature:**
```python
def compute_expected_block(snapshot: dict, window_days: int = 90, mode: str = "sprawl"):
```

**New signature:**
```python
def compute_expected_block(snapshot: dict, policy: PolicyConfig = None, mode: str = "sprawl"):
    policy = policy or PolicyConfig()
    window_days = policy.admission.zombie_window_days
    noise_floor = policy.admission.noise_floor
    minimum_spend = policy.admission.minimum_spend
```

**Key changes:**

1. **Admission logic** - Use config thresholds
   ```python
   is_admitted = (
       discovery_sources_count >= policy.admission.noise_floor or
       cand['cloud_present'] or
       idp_present or
       cmdb_present or
       (cand.get('finance_spend', 0) >= policy.admission.minimum_spend)
   )
   ```

2. **Kill list** - Use dynamic exclusions
   ```python
   is_excluded = (
       key in policy.exclusions or
       (not policy.scope.include_infra and key in INFRASTRUCTURE_DOMAINS)
   )
   ```

3. **Zombie window** - Use config value
   ```python
   stale_threshold = now - timedelta(days=policy.admission.zombie_window_days)
   ```

### Phase 5: API Integration

**New endpoint:** `POST /api/snapshots/generate`

Accept optional `policy_config` in request body:

```python
@router.post("/api/snapshots/generate")
async def generate_snapshot(
    seed: int = None,
    scale: str = "small",
    policy_config: Optional[PolicyConfig] = None
):
    # If no config provided, try fetching from AOD
    if policy_config is None:
        policy_config = await aod_client.fetch_policy_config()
    
    generator = EnterpriseGenerator(
        seed=seed,
        scale=scale,
        policy_config=policy_config
    )
    return generator.generate()
```

**Reconciliation endpoint update:**

```python
@router.post("/api/reconciliation/{snapshot_id}")
async def run_reconciliation(
    snapshot_id: str,
    policy_config: Optional[PolicyConfig] = None
):
    if policy_config is None:
        policy_config = await aod_client.fetch_policy_config()
    
    expected = compute_expected_block(snapshot, policy=policy_config)
    # ... grade against active config
```

---

## Migration Strategy (Strangler Pattern)

1. **Add PolicyConfig model** - No breaking changes
2. **Add optional policy param to generator** - Defaults preserve current behavior
3. **Add optional policy param to reconciliation** - Defaults preserve current behavior
4. **Add fetch_policy_config to AOD client** - Returns None if AOD unavailable
5. **Wire up API endpoints** - Accept policy_config in requests
6. **Update UI** - Show active config in Simulation & Proof tour

---

## Testing Strategy

### Unit Tests
- Generator produces correct source counts for various noise_floor values
- Reconciliation correctly admits/rejects at spend thresholds
- Kill list properly filters excluded domains

### Integration Tests
- Farm + AOD roundtrip with custom PolicyConfig
- Verify scorecard accuracy when thresholds change

### Negative Tests (Anti-Cheat)
- Config with noise_floor=5 → shadow with 4 sources must be REJECTED
- Config with minimum_spend=500 → $400 expense must be REJECTED
- Config with exclusions=["slack.com"] → slack.com must be REJECTED regardless of evidence

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/models/policy.py` | NEW - PolicyConfig schema |
| `src/services/aod_client.py` | Add `fetch_policy_config()` |
| `src/generators/enterprise.py` | Accept policy, parameterize thresholds |
| `src/services/reconciliation.py` | Accept policy, use dynamic thresholds |
| `src/api/routes.py` | Accept policy_config in generate/reconcile endpoints |
| `templates/index.html` | Display active config in tour |
| `tests/test_policy_driven.py` | NEW - Policy-driven test suite |
