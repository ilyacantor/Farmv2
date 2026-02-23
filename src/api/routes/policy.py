"""
Policy configuration routes.

Endpoints:
- GET  /api/config          Frontend configuration
- GET  /api/policy          Get current policy config
- POST /api/policy/webhook  Receive policy update notifications from AOD
"""
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from src.services.aod_client import clear_policy_cache, fetch_policy_config
from src.services.logging import trace_log

logger = logging.getLogger(__name__)

router = APIRouter(tags=["policy"])


@router.get("/api/config")
async def get_config():
    """Return frontend configuration values."""
    aod_url = os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")
    aod_discover_url = os.environ.get("AOD_DISCOVER_URL", "https://aodv3-1.onrender.com")
    return {
        "aod_base_url": aod_url.rstrip("/") if aod_url else None,
        "aod_discover_url": aod_discover_url.rstrip("/") if aod_discover_url else None
    }


@router.get("/api/policy")
async def get_policy_config(refresh: bool = False):
    """Return the active PolicyConfig (from AOD or mock fallback).

    Query params:
        refresh: If true, bypass cache and fetch fresh from AOD
    """
    policy = await fetch_policy_config(force_refresh=refresh)
    return {
        "admission": {
            "minimum_spend": policy.admission.minimum_spend,
            "noise_floor": policy.admission.noise_floor,
            "zombie_window_days": policy.admission.zombie_window_days,
        },
        "scope": {
            "include_infra": policy.scope.include_infra,
            "treat_directory_as_idp": policy.scope.treat_directory_as_idp,
            "use_policy_engine": policy.scope.use_policy_engine,
        },
        "secondary_gates": {
            "require_sso_for_idp": policy.secondary_gates.require_sso_for_idp,
            "require_valid_ci_type": policy.secondary_gates.require_valid_ci_type,
            "require_valid_lifecycle": policy.secondary_gates.require_valid_lifecycle,
            "valid_ci_types": policy.secondary_gates.valid_ci_types,
            "valid_lifecycle_states": policy.secondary_gates.valid_lifecycle_states,
            "invalid_lifecycle_states": policy.secondary_gates.invalid_lifecycle_states,
        },
        "exclusions": policy.exclusions,
        "infrastructure_seeds": policy.infrastructure_seeds,
        "corporate_root_domains": policy.corporate_root_domains,
        "banned_domains": policy.banned_domains,
        "source": "aod" if os.environ.get("AOD_BASE_URL") or os.environ.get("AOD_URL") else "mock",
    }


class PolicyWebhookPayload(BaseModel):
    """Payload from AOD policy switchboard webhook notification."""
    policy: Optional[dict] = None
    event: str = "policy_updated"
    timestamp: Optional[str] = None


@router.post("/api/policy/webhook")
async def policy_webhook(payload: PolicyWebhookPayload):
    """Receive policy update notifications from AOD.

    When AOD's policy switchboard saves changes with auto_notify enabled,
    it POSTs here to notify Farm. Farm clears its policy cache so the next
    fetch gets the fresh policy.

    Webhook URL to configure in AOD: https://<farm-host>/api/policy/webhook
    """
    clear_policy_cache()

    trace_log("routes", "policy_webhook", {
        "event": payload.event,
        "timestamp": payload.timestamp or datetime.utcnow().isoformat(),
        "policy_received": payload.policy is not None,
    })

    return {
        "status": "ok",
        "message": "Policy cache cleared, will fetch fresh on next request",
        "received_at": datetime.utcnow().isoformat() + "Z",
    }
