"""
AOD Client Service with caching and circuit breaker pattern.

Provides call_aod_explain_nonflag with:
- Per-run cache: Cache results by (snapshot_id, frozenset(asset_keys))
- Circuit breaker: After 3 consecutive failures, skip real calls for 60 seconds
- Better error logging
"""

import copy
import os
import time
import httpx
from src.services.logging import trace_log

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


def stub_aod_explain_nonflag(asset_keys: list[str], ask: str = "both") -> dict[str, dict]:
    """
    Deterministic stub for testing explain-nonflag without real AOD.
    Returns each decision bucket at least once based on key patterns.
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
    aod_url = os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")
    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"
    
    if use_stub:
        return stub_aod_explain_nonflag(asset_keys, ask)
    
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
        async with httpx.AsyncClient(timeout=10.0) as client:
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
