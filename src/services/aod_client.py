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


STOPWORDS = frozenset({
    'the', 'a', 'an', 'and', 'or', 'of', 'for', 'to', 'in', 'on', 'at', 'by',
    'inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'co', 'software',
    'app', 'application', 'service', 'platform', 'cloud', 'solutions', 'systems',
    'technologies', 'tech', 'enterprise', 'pro', 'premium', 'plus', 'suite'
})


def _normalize_name_for_matching(name: str) -> set[str]:
    """Extract meaningful words from a name for word-overlap matching."""
    if not name:
        return set()
    words = set()
    for word in name.lower().replace('-', ' ').replace('_', ' ').split():
        word = ''.join(c for c in word if c.isalnum())
        if word and len(word) > 2 and word not in STOPWORDS:
            words.add(word)
    return words


def _compute_word_overlap(words1: set[str], words2: set[str]) -> int:
    """Count shared non-stopwords between two word sets."""
    return len(words1 & words2)


async def stub_aod_explain_nonflag_from_snapshot(
    snapshot_id: str,
    asset_keys: list[str],
    ask: str = "both"
) -> dict[str, dict]:
    """
    Stub v2: Two-tier correlation algorithm for accurate governance simulation.
    
    Tier 1 (AUTHORITATIVE):
      - Match asset registered_domain against CMDB/IdP canonical_domain
      - Match asset registered_domain against CMDB/IdP domains[] array (if present)
      - Direct domain match = authoritative governance assertion
    
    Tier 2 (WEAK):
      - Match by vendor_id (exact match)
      - Match by normalized product_name (exact match)
      - Match by name word overlap (>=2 shared non-stopwords AND same category)
      - WEAK matches set governance hints but do NOT assert identity merge
    
    Returns per-key: {
        present_in_aod, decision, reason_codes[], stub_mode: True,
        cmdb_correlation: {status: NONE|WEAK|AUTHORITATIVE, method: str, matched_id: str|null},
        idp_correlation: {status: NONE|WEAK|AUTHORITATIVE, method: str, matched_id: str|null}
    }
    """
    from src.farm.db import connection as db_connection
    
    cmdb_domain_index: dict[str, str] = {}
    cmdb_vendor_index: dict[str, list[str]] = {}
    cmdb_name_index: dict[str, list[tuple[str, set[str]]]] = {}
    cmdb_entries: dict[str, dict] = {}
    
    idp_domain_index: dict[str, str] = {}
    idp_vendor_index: dict[str, list[str]] = {}
    idp_name_index: dict[str, list[tuple[str, set[str]]]] = {}
    idp_entries: dict[str, dict] = {}
    
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
                    ci_id = ci.get("ci_id", "")
                    cmdb_entries[ci_id] = ci
                    
                    canonical_domain = ci.get("canonical_domain")
                    if canonical_domain:
                        reg_domain = extract_registered_domain(canonical_domain)
                        if reg_domain:
                            cmdb_domain_index[reg_domain] = ci_id
                    
                    domains = ci.get("domains", [])
                    for domain in domains:
                        reg_domain = extract_registered_domain(domain)
                        if reg_domain:
                            cmdb_domain_index[reg_domain] = ci_id
                    
                    vendor = ci.get("vendor", "")
                    if vendor:
                        vendor_lower = vendor.lower().strip()
                        if vendor_lower not in cmdb_vendor_index:
                            cmdb_vendor_index[vendor_lower] = []
                        cmdb_vendor_index[vendor_lower].append(ci_id)
                    
                    name = ci.get("name", "")
                    if name:
                        name_words = _normalize_name_for_matching(name)
                        if name_words:
                            name_lower = name.lower().strip()
                            if name_lower not in cmdb_name_index:
                                cmdb_name_index[name_lower] = []
                            cmdb_name_index[name_lower].append((ci_id, name_words))
                
                idp_objects = planes.get("idp", {}).get("objects", [])
                for obj in idp_objects:
                    idp_id = obj.get("idp_id", "")
                    idp_entries[idp_id] = obj
                    
                    canonical_domain = obj.get("canonical_domain")
                    if canonical_domain:
                        reg_domain = extract_registered_domain(canonical_domain)
                        if reg_domain:
                            idp_domain_index[reg_domain] = idp_id
                    
                    domains = obj.get("domains", [])
                    for domain in domains:
                        reg_domain = extract_registered_domain(domain)
                        if reg_domain:
                            idp_domain_index[reg_domain] = idp_id
                    
                    vendor = obj.get("vendor", "")
                    if vendor:
                        vendor_lower = vendor.lower().strip()
                        if vendor_lower not in idp_vendor_index:
                            idp_vendor_index[vendor_lower] = []
                        idp_vendor_index[vendor_lower].append(idp_id)
                    
                    name = obj.get("name", "")
                    if name:
                        name_words = _normalize_name_for_matching(name)
                        if name_words:
                            name_lower = name.lower().strip()
                            if name_lower not in idp_name_index:
                                idp_name_index[name_lower] = []
                            idp_name_index[name_lower].append((idp_id, name_words))
                
                trace_log("aod_client", "stub_v2_indexes_built", {
                    "snapshot_id": snapshot_id,
                    "cmdb_domain_count": len(cmdb_domain_index),
                    "cmdb_vendor_count": len(cmdb_vendor_index),
                    "cmdb_name_count": len(cmdb_name_index),
                    "idp_domain_count": len(idp_domain_index),
                    "idp_vendor_count": len(idp_vendor_index),
                    "idp_name_count": len(idp_name_index),
                })
    except Exception as e:
        trace_log("aod_client", "stub_snapshot_fetch_error", {
            "snapshot_id": snapshot_id,
            "error": str(e),
        })
    
    results = {}
    authoritative_count = 0
    weak_count = 0
    
    for key in asset_keys:
        key_domain = extract_registered_domain(key)
        key_words = _normalize_name_for_matching(key.replace('.', ' '))
        
        cmdb_correlation = {"status": "NONE", "method": None, "matched_id": None}
        idp_correlation = {"status": "NONE", "method": None, "matched_id": None}
        
        if key_domain and key_domain in cmdb_domain_index:
            cmdb_correlation = {
                "status": "AUTHORITATIVE",
                "method": "registered_domain",
                "matched_id": cmdb_domain_index[key_domain]
            }
        else:
            for vendor, ci_ids in cmdb_vendor_index.items():
                if vendor and key_domain and vendor in key_domain:
                    cmdb_correlation = {
                        "status": "WEAK",
                        "method": "vendor_in_domain",
                        "matched_id": ci_ids[0]
                    }
                    break
            
            if cmdb_correlation["status"] == "NONE" and key_words:
                for name_lower, entries in cmdb_name_index.items():
                    for ci_id, name_words in entries:
                        overlap = _compute_word_overlap(key_words, name_words)
                        if overlap >= 2:
                            cmdb_correlation = {
                                "status": "WEAK",
                                "method": f"name_overlap_{overlap}_words",
                                "matched_id": ci_id
                            }
                            break
                    if cmdb_correlation["status"] != "NONE":
                        break
        
        if key_domain and key_domain in idp_domain_index:
            idp_correlation = {
                "status": "AUTHORITATIVE",
                "method": "registered_domain",
                "matched_id": idp_domain_index[key_domain]
            }
        else:
            for vendor, idp_ids in idp_vendor_index.items():
                if vendor and key_domain and vendor in key_domain:
                    idp_correlation = {
                        "status": "WEAK",
                        "method": "vendor_in_domain",
                        "matched_id": idp_ids[0]
                    }
                    break
            
            if idp_correlation["status"] == "NONE" and key_words:
                for name_lower, entries in idp_name_index.items():
                    for idp_id, name_words in entries:
                        overlap = _compute_word_overlap(key_words, name_words)
                        if overlap >= 2:
                            idp_correlation = {
                                "status": "WEAK",
                                "method": f"name_overlap_{overlap}_words",
                                "matched_id": idp_id
                            }
                            break
                    if idp_correlation["status"] != "NONE":
                        break
        
        has_cmdb_authoritative = cmdb_correlation["status"] == "AUTHORITATIVE"
        has_cmdb_weak = cmdb_correlation["status"] == "WEAK"
        has_idp_authoritative = idp_correlation["status"] == "AUTHORITATIVE"
        has_idp_weak = idp_correlation["status"] == "WEAK"
        
        has_authoritative = has_cmdb_authoritative or has_idp_authoritative
        has_weak = has_cmdb_weak or has_idp_weak
        has_governance = has_authoritative or has_weak
        
        if has_authoritative:
            authoritative_count += 1
        elif has_weak:
            weak_count += 1
        
        reason_codes = []
        if has_cmdb_authoritative:
            reason_codes.append("HAS_CMDB")
        elif has_cmdb_weak:
            reason_codes.append("HAS_CMDB_WEAK")
        else:
            reason_codes.append("NO_CMDB")
        
        if has_idp_authoritative:
            reason_codes.append("HAS_IDP")
        elif has_idp_weak:
            reason_codes.append("HAS_IDP_WEAK")
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
            "cmdb_correlation": cmdb_correlation,
            "idp_correlation": idp_correlation,
        }
    
    trace_log("aod_client", "stub_v2_explain_computed", {
        "snapshot_id": snapshot_id,
        "keys_count": len(asset_keys),
        "authoritative_count": authoritative_count,
        "weak_count": weak_count,
        "ungoverned_count": len(asset_keys) - authoritative_count - weak_count,
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
