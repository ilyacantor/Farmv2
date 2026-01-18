"""
Combined API router.

This module aggregates all domain-specific routers into a single router
that can be mounted by the FastAPI application.

Route modules:
- snapshots: Snapshot CRUD operations
- reconciliation: Reconciliation and analysis
- policy: Policy configuration
- admin: Administrative and diagnostic endpoints
"""
from fastapi import APIRouter

from .snapshots import router as snapshots_router
from .reconciliation import router as reconciliation_router
from .policy import router as policy_router
from .admin import router as admin_router

# Re-export compute_fingerprint for backwards compatibility with main.py seeding
from .common import compute_fingerprint

# Create combined router
router = APIRouter()

# Include all sub-routers
router.include_router(snapshots_router)
router.include_router(reconciliation_router)
router.include_router(policy_router)
router.include_router(admin_router)

__all__ = [
    "router",
    "compute_fingerprint",
    "snapshots_router",
    "reconciliation_router",
    "policy_router",
    "admin_router",
]
