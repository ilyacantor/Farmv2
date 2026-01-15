"""
AOD Client Service with caching and circuit breaker pattern.

Provides:
- fetch_policy_config: Get active PolicyConfig from AOD
- fetch_run_policy_snapshot: Get exact PolicyConfig used for a specific AOD run
- call_aod_explain_nonflag: Get decision traces for missed assets

Features:
- Per-run cache: Cache results by (snapshot_id, frozenset(asset_keys))
- Circuit breaker: After 3 consecutive failures, skip real calls for 60 seconds
- Better error logging
- Enhanced stub mode: Reads snapshot CMDB/IdP for accurate correlation simulation

## Policy Snapshot Contract (AOD → Farm)

Farm requires the exact policy that AOD used for each run to ensure reproducible grading.
AOD must implement this endpoint:

### GET /api/runs/{run_id}/policy

Request:
    GET /api/runs/{run_id}/policy
    Headers:
        X-Shared-Secret: <shared_secret>  (if configured)

Response (200 OK):
    {
        "run_id": "abc123",
        "policy_hash": "sha256:...",
        "captured_at": "2026-01-15T12:00:00Z",
        "policy": {
            "exclusions": [...],
            "infrastructure_seeds": [...],
            "corporate_root_domains": [...],
            "admission": {
                "noise_floor": 2,
                "minimum_spend": 100,
                "zombie_window_days": 90,
                ...
            },
            "secondary_gates": {
                "require_valid_ci_type": true,
                "valid_ci_types": ["app", "application", "service", ...],
                ...
            },
            ...
        }
    }

Response (404 Not Found):
    {"error": "Run not found", "run_id": "abc123"}

Response (410 Gone):
    {"error": "Policy snapshot expired", "run_id": "abc123", "expired_at": "..."}

### Why This Matters

Without a policy snapshot per run:
- Policy may change between AOD run and Farm grading
- Grading becomes non-reproducible
- "Farm and AOD disagree" becomes ambiguous (policy drift vs real bug)

With policy snapshot per run:
- Farm grades against the EXACT policy AOD used
- Grading is 100% reproducible
- Any discrepancy is definitively a bug

## Stub Mode (USE_AOD_EXPLAIN_STUB=true)

Enhanced stub mode reads snapshot CMDB/IdP planes to compute HAS_CMDB/HAS_IDP
deterministically using registered_domain matching against canonical_domain fields.

This allows testing without real AOD while maintaining accurate correlation logic:
- No fuzzy matching or cross-TLD correlation
- Only exact registered_domain matches grant governance
- Reports should include "MODE: STUB" banner
- Discrepancies marked as STUB_ARTIFACT, not bugs
"""

import copy
import json
import os
import time
from typing import Optional
import httpx
from src.services.logging import trace_log
from src.models.policy import PolicyConfig
from src.services.key_normalization import extract_registered_domain

# Cache: {(snapshot_id, frozenset(keys)): result_dict}
_explain_cache: dict[tuple, dict] = {}

# Circuit breaker state
_circuit_failures = 0
_circuit_open_until = 0.0
CIRCUIT_THRESHOLD = 3
CIRCUIT_TIMEOUT = 60


def _is_circuit_open() -> bool:
    """Check if circuit breaker is open (should skip real calls)."""
    global _circuit_open_until
    if _circuit_open_until > 0 and time.time() < _circuit_open_until:
        return True
    if _circuit_open_until > 0 and time.time() >= _circuit_open_until:
        trace_log("aod_client", "circuit_close", {"reason": "timeout_expired"})
        _circuit_open_until = 0.0
    return False


def _record_failure():
    """Record a failure and potentially open the circuit."""
    global _circuit_failures, _circuit_open_until
    _circuit_failures += 1
    trace_log("aod_client", "failure_recorded", {"consecutive_failures": _circuit_failures})
    
    if _circuit_failures >= CIRCUIT_THRESHOLD:
        _circuit_open_until = time.time() + CIRCUIT_TIMEOUT
        trace_log("aod_client", "circuit_open", {
            "threshold": CIRCUIT_THRESHOLD,
            "timeout_seconds": CIRCUIT_TIMEOUT
        })


def _record_success():
    """Record a success and reset failure counter."""
    global _circuit_failures
    if _circuit_failures > 0:
        trace_log("aod_client", "success_reset", {"previous_failures": _circuit_failures})
    _circuit_failures = 0


def _get_fallback_response(asset_keys: list[str], http_code: str | None = None) -> dict[str, dict]:
    """Generate fallback response for when we can't make real calls.
    
    Preserves original behavior: ["NO_EXPLAIN_ENDPOINT"] or ["NO_EXPLAIN_ENDPOINT", "HTTP_xxx"]
    """
    if http_code:
        return {key: {
            "present_in_aod": False,
            "decision": "UNKNOWN_KEY",
            "reason_codes": ["NO_EXPLAIN_ENDPOINT", http_code]
        } for key in asset_keys}
    else:
        return {key: {
            "present_in_aod": False,
            "decision": "UNKNOWN_KEY",
            "reason_codes": ["NO_EXPLAIN_ENDPOINT"]
        } for key in asset_keys}


async def create_aod_http_client(timeout: float = 30.0) -> tuple[httpx.AsyncClient, dict]:
    """Create configured HTTP client for AOD with auth headers."""
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")
    headers = {}
    if aod_secret:
        headers["Authorization"] = f"Bearer {aod_secret}"
    client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
    return client, headers


def clear_cache():
    """Clear the explain cache. Useful for testing or between runs."""
    global _explain_cache
    _explain_cache.clear()
    trace_log("aod_client", "cache_cleared", {"action": "manual_clear"})


def reset_circuit_breaker():
    """Reset circuit breaker state. Useful for testing."""
    global _circuit_failures, _circuit_open_until
    _circuit_failures = 0
    _circuit_open_until = 0.0
    trace_log("aod_client", "circuit_reset", {"action": "manual_reset"})


def stub_aod_explain_nonflag_legacy(asset_keys: list[str], ask: str = "both") -> dict[str, dict]:
    """
    LEGACY: Deterministic stub for testing explain-nonflag without real AOD.
    Returns each decision bucket at least once based on key patterns.
    
    DEPRECATED: Use stub_aod_explain_nonflag_from_snapshot for accurate CMDB/IdP correlation.
    """
    results = {}
    for i, key in enumerate(asset_keys):
        key_lower = key.lower()
        bucket = i % 4
        
        if bucket == 0 or "unknown" in key_lower:
            results[key] = {
                "present_in_aod": False,
                "decision": "UNKNOWN_KEY",
                "reason_codes": ["NO_CANDIDATE", "NO_EVIDENCE_INGESTED"]
            }
        elif bucket == 1 or "reject" in key_lower:
            results[key] = {
                "present_in_aod": True,
                "decision": "NOT_ADMITTED",
                "reason_codes": ["REJECTED_NO_GATE", "INSUFFICIENT_DISCOVERY_SOURCES"]
            }
        elif bucket == 2 or "govern" in key_lower or "idp" in key_lower:
            results[key] = {
                "present_in_aod": True,
                "decision": "ADMITTED_NOT_SHADOW" if ask in ["shadow", "both"] else "ADMITTED_NOT_ZOMBIE",
                "reason_codes": ["HAS_IDP", "HAS_CMDB"] if ask in ["shadow", "both"] else ["RECENT_ACTIVITY", "HAS_ACTIVE_USERS"]
            }
        else:
            results[key] = {
                "present_in_aod": True,
                "decision": "ADMITTED_NOT_ZOMBIE" if ask in ["zombie", "both"] else "ADMITTED_NOT_SHADOW",
                "reason_codes": ["RECENT_ACTIVITY", "HAS_ACTIVE_USERS"]
            }
    
    return results


async def stub_aod_explain_nonflag_from_snapshot(
    snapshot_id: str,
    asset_keys: list[str],
    ask: str = "both"
) -> dict[str, dict]:
    """
    Enhanced stub that reads snapshot CMDB/IdP planes to compute HAS_CMDB/HAS_IDP.
    
    Simulates AOD correlation logic:
    - Extracts canonical_domain from CMDB CIs and IdP objects
    - Builds domain indexes using registered_domain extraction
    - For each asset key, checks if its registered_domain matches any CMDB/IdP entry
    - NO fuzzy matching, NO cross-TLD correlation
    
    This provides accurate stub responses without requiring real AOD.
    
    Returns per-key: {present_in_aod, decision, reason_codes[], stub_mode: True}
    """
    from src.farm.db import connection as db_connection
    
    cmdb_domains = set()
    idp_domains = set()
    
    try:
        async with db_connection() as conn:
            row = await conn.fetchrow(
                "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
                snapshot_id
            )
            if row and row["snapshot_json"]:
                snapshot = json.loads(row["snapshot_json"])
                planes = snapshot.get("__planes__", snapshot)
                
                cmdb_cis = planes.get("cmdb", {}).get("cis", [])
                for ci in cmdb_cis:
                    canonical_domain = ci.get("canonical_domain")
                    if canonical_domain:
                        reg_domain = extract_registered_domain(canonical_domain)
                        if reg_domain:
                            cmdb_domains.add(reg_domain)
                
                idp_objects = planes.get("idp", {}).get("objects", [])
                for obj in idp_objects:
                    canonical_domain = obj.get("canonical_domain")
                    if canonical_domain:
                        reg_domain = extract_registered_domain(canonical_domain)
                        if reg_domain:
                            idp_domains.add(reg_domain)
                
                trace_log("aod_client", "stub_indexes_built", {
                    "snapshot_id": snapshot_id,
                    "cmdb_domains_count": len(cmdb_domains),
                    "idp_domains_count": len(idp_domains),
                })
    except Exception as e:
        trace_log("aod_client", "stub_snapshot_fetch_error", {
            "snapshot_id": snapshot_id,
            "error": str(e),
        })
    
    results = {}
    for key in asset_keys:
        key_domain = extract_registered_domain(key)
        
        has_cmdb = key_domain in cmdb_domains if key_domain else False
        has_idp = key_domain in idp_domains if key_domain else False
        has_governance = has_cmdb or has_idp
        
        reason_codes = []
        if has_cmdb:
            reason_codes.append("HAS_CMDB")
        else:
            reason_codes.append("NO_CMDB")
        
        if has_idp:
            reason_codes.append("HAS_IDP")
        else:
            reason_codes.append("NO_IDP")
        
        if has_governance:
            decision = "ADMITTED_NOT_SHADOW" if ask in ["shadow", "both"] else "ADMITTED_NOT_ZOMBIE"
            reason_codes.extend(["RECENT_ACTIVITY", "HAS_ACTIVE_USERS"])
        else:
            decision = "SHADOW_CANDIDATE" if ask in ["shadow", "both"] else "ZOMBIE_CANDIDATE"
        
        results[key] = {
            "present_in_aod": True,
            "decision": decision,
            "reason_codes": reason_codes,
            "stub_mode": True,
        }
    
    trace_log("aod_client", "stub_explain_computed", {
        "snapshot_id": snapshot_id,
        "keys_count": len(asset_keys),
        "governed_count": sum(1 for r in results.values() if "HAS_CMDB" in r["reason_codes"] or "HAS_IDP" in r["reason_codes"]),
    })
    
    return results


async def call_aod_explain_nonflag(
    snapshot_id: str,
    asset_keys: list[str],
    ask: str = "both"
) -> dict[str, dict]:
    """
    Call AOD explain-nonflag endpoint to get decision traces for missed assets.
    
    Features:
    - Caches results by (snapshot_id, frozenset(asset_keys))
    - Circuit breaker: After 3 failures, skips real calls for 60 seconds
    
    Returns per-key: {present_in_aod, decision, reason_codes[]}
    Fallback: decision="UNKNOWN_KEY", reason_codes=["NO_EXPLAIN_ENDPOINT"]
    """
    aod_url = (os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")).rstrip("/")
    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"
    
    if use_stub:
        trace_log("aod_client", "using_enhanced_stub", {
            "snapshot_id": snapshot_id,
            "keys_count": len(asset_keys),
        })
        return await stub_aod_explain_nonflag_from_snapshot(snapshot_id, asset_keys, ask)
    
    if not aod_url:
        return _get_fallback_response(asset_keys)
    
    # Check cache first - return deep copy to prevent mutation
    cache_key = (snapshot_id, frozenset(asset_keys), ask)
    if cache_key in _explain_cache:
        trace_log("aod_client", "cache_hit", {
            "snapshot_id": snapshot_id,
            "keys_count": len(asset_keys)
        })
        return copy.deepcopy(_explain_cache[cache_key])
    
    # Check circuit breaker
    if _is_circuit_open():
        trace_log("aod_client", "circuit_open_skip", {
            "snapshot_id": snapshot_id,
            "keys_count": len(asset_keys)
        })
        return _get_fallback_response(asset_keys)
    
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            shared_secret = os.environ.get("AOD_SHARED_SECRET", "")
            headers = {"X-Shared-Secret": shared_secret} if shared_secret else {}
            
            resp = await client.post(
                f"{aod_url}/reconcile/explain-nonflag",
                json={
                    "snapshot_id": snapshot_id,
                    "asset_keys": asset_keys,
                    "ask": ask
                },
                headers=headers
            )
            
            if resp.status_code == 200:
                result = resp.json()
                _record_success()
                # Cache successful result (store copy)
                _explain_cache[cache_key] = copy.deepcopy(result)
                trace_log("aod_client", "call_success", {
                    "snapshot_id": snapshot_id,
                    "keys_count": len(asset_keys),
                    "cached": True
                })
                return result
            else:
                trace_log("aod_client", "call_failed", {
                    "snapshot_id": snapshot_id,
                    "status_code": resp.status_code,
                    "keys_count": len(asset_keys)
                })
                _record_failure()
                return _get_fallback_response(asset_keys, f"HTTP_{resp.status_code}")
                
    except Exception as e:
        trace_log("aod_client", "call_error", {
            "snapshot_id": snapshot_id,
            "error": str(e),
            "keys_count": len(asset_keys)
        })
        _record_failure()
        return _get_fallback_response(asset_keys)


_policy_cache: Optional[PolicyConfig] = None
_policy_cache_time: float = 0.0
POLICY_CACHE_TTL = 300


async def fetch_policy_config(force_refresh: bool = False) -> PolicyConfig:
    """
    Fetch active PolicyConfig from AOD.
    
    Features:
    - Caches result for 5 minutes
    - Falls back to default config if AOD unavailable
    - Respects circuit breaker
    
    INVARIANT: Farm never defines policy locally.
    This function is the ONLY source of policy configuration.
    """
    global _policy_cache, _policy_cache_time
    
    if not force_refresh and _policy_cache is not None:
        if time.time() - _policy_cache_time < POLICY_CACHE_TTL:
            trace_log("aod_client", "policy_cache_hit", {
                "age_seconds": int(time.time() - _policy_cache_time)
            })
            return _policy_cache
    
    aod_url = (os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")).rstrip("/")
    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"
    
    if use_stub:
        trace_log("aod_client", "policy_stub", {"reason": "USE_AOD_EXPLAIN_STUB=true"})
        return PolicyConfig.from_policy_master()
    
    if not aod_url:
        trace_log("aod_client", "policy_fallback", {"reason": "no_aod_url"})
        return PolicyConfig.from_policy_master()
    
    if _is_circuit_open():
        trace_log("aod_client", "policy_circuit_open", {"reason": "circuit_breaker"})
        if _policy_cache is not None:
            return _policy_cache
        return PolicyConfig.from_policy_master()
    
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            shared_secret = os.environ.get("AOD_SHARED_SECRET", "")
            headers = {"X-Shared-Secret": shared_secret} if shared_secret else {}
            
            resp = await client.get(
                f"{aod_url}/api/v1/policy/config",
                headers=headers
            )
            
            if resp.status_code == 200:
                data = resp.json()
                
                raw_admission = data.get("admission", {})
                raw_noise_floor = raw_admission.get("noise_floor")
                trace_log("aod_client", "policy_raw_debug", {
                    "raw_noise_floor_value": raw_noise_floor,
                    "raw_noise_floor_type": type(raw_noise_floor).__name__,
                    "raw_admission_keys": list(raw_admission.keys()) if raw_admission else [],
                    "top_level_keys": list(data.keys()),
                })
                
                policy = PolicyConfig.from_aod_response(data)
                _record_success()
                
                _policy_cache = policy
                _policy_cache_time = time.time()
                
                trace_log("aod_client", "policy_fetch_success", {
                    "noise_floor": policy.admission.noise_floor,
                    "noise_floor_type": type(policy.admission.noise_floor).__name__,
                    "minimum_spend": policy.admission.minimum_spend,
                    "zombie_window_days": policy.admission.zombie_window_days,
                    "exclusions_count": len(policy.exclusions),
                    "infrastructure_seeds_count": len(policy.infrastructure_seeds),
                })
                return policy
            else:
                trace_log("aod_client", "policy_fetch_failed", {
                    "status_code": resp.status_code
                })
                _record_failure()
                if _policy_cache is not None:
                    return _policy_cache
                return PolicyConfig.from_policy_master()
                
    except Exception as e:
        trace_log("aod_client", "policy_fetch_error", {
            "error": str(e)
        })
        _record_failure()
        if _policy_cache is not None:
            return _policy_cache
        return PolicyConfig.from_policy_master()


def clear_policy_cache():
    """Clear the policy cache. Useful for testing."""
    global _policy_cache, _policy_cache_time
    _policy_cache = None
    _policy_cache_time = 0.0
    trace_log("aod_client", "policy_cache_cleared", {})


async def fetch_run_policy_snapshot(run_id: str) -> tuple[Optional[PolicyConfig], Optional[str]]:
    """
    Fetch the exact PolicyConfig that AOD used for a specific run.
    
    This is the preferred method for grading - it ensures Farm uses the
    EXACT same policy that AOD used, eliminating policy drift bugs.
    
    Args:
        run_id: The AOD run ID to fetch policy for
        
    Returns:
        Tuple of (PolicyConfig, policy_hash) if successful
        Tuple of (None, error_reason) if failed
        
    Error reasons:
        - "RUN_NOT_FOUND": Run ID doesn't exist
        - "POLICY_EXPIRED": Policy snapshot was garbage collected
        - "AOD_UNAVAILABLE": Can't reach AOD
        - "STUB_MODE": Using local stub (no run-specific policy available)
    """
    aod_url = (os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")).rstrip("/")
    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"
    
    if use_stub:
        trace_log("aod_client", "run_policy_stub", {
            "run_id": run_id,
            "reason": "USE_AOD_EXPLAIN_STUB=true"
        })
        return None, "STUB_MODE"
    
    if not aod_url:
        trace_log("aod_client", "run_policy_no_url", {
            "run_id": run_id,
            "reason": "no_aod_url"
        })
        return None, "AOD_UNAVAILABLE"
    
    if _is_circuit_open():
        trace_log("aod_client", "run_policy_circuit_open", {
            "run_id": run_id,
            "reason": "circuit_breaker"
        })
        return None, "AOD_UNAVAILABLE"
    
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            shared_secret = os.environ.get("AOD_SHARED_SECRET", "")
            headers = {"X-Shared-Secret": shared_secret} if shared_secret else {}
            
            resp = await client.get(
                f"{aod_url}/api/runs/{run_id}/policy",
                headers=headers
            )
            
            if resp.status_code == 200:
                data = resp.json()
                policy_data = data.get("policy", {})
                policy_hash = data.get("policy_hash")
                
                policy = PolicyConfig.from_aod_response(policy_data)
                _record_success()
                
                trace_log("aod_client", "run_policy_fetch_success", {
                    "run_id": run_id,
                    "policy_hash": policy_hash,
                    "captured_at": data.get("captured_at")
                })
                return policy, policy_hash
                
            elif resp.status_code == 404:
                trace_log("aod_client", "run_policy_not_found", {
                    "run_id": run_id
                })
                return None, "RUN_NOT_FOUND"
                
            elif resp.status_code == 410:
                trace_log("aod_client", "run_policy_expired", {
                    "run_id": run_id
                })
                return None, "POLICY_EXPIRED"
                
            else:
                trace_log("aod_client", "run_policy_fetch_failed", {
                    "run_id": run_id,
                    "status_code": resp.status_code
                })
                _record_failure()
                return None, "AOD_UNAVAILABLE"
                
    except Exception as e:
        trace_log("aod_client", "run_policy_fetch_error", {
            "run_id": run_id,
            "error": str(e)
        })
        _record_failure()
        return None, "AOD_UNAVAILABLE"


async def fetch_policy_for_grading(run_id: Optional[str] = None) -> PolicyConfig:
    """
    Fetch the appropriate policy for grading a reconciliation.
    
    This is the main entry point for getting policy when grading:
    1. If run_id provided → Try to get run-specific policy snapshot
    2. If run_id not provided or snapshot unavailable → Fall back to current policy
    
    Args:
        run_id: Optional AOD run ID. If provided, attempts to fetch
                the exact policy snapshot from that run.
                
    Returns:
        PolicyConfig to use for grading
        
    Note:
        When run_id is provided but snapshot is unavailable, this logs
        a warning and falls back to current policy. The grading result
        should note this fallback for transparency.
    """
    if run_id:
        policy, error = await fetch_run_policy_snapshot(run_id)
        if policy:
            trace_log("aod_client", "grading_policy_from_run", {
                "run_id": run_id,
                "source": "run_snapshot"
            })
            return policy
        else:
            trace_log("aod_client", "grading_policy_fallback", {
                "run_id": run_id,
                "fallback_reason": error,
                "source": "current_policy"
            })
    
    return await fetch_policy_config()
