"""
API endpoints for End-to-End Injection Tests.

ARCHITECTURAL BOUNDARY: These are VERIFICATION endpoints.

Farm.Verifier injects test payloads and validates they arrive correctly.
Farm does NOT perform repairs or manage infrastructure.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from src.verifier.injection_tests import (
    InjectionTestResult,
    InjectionTestStatus,
    create_injection_payload,
    run_e2e_injection_test,
)

logger = logging.getLogger("farm.api.injection")
router = APIRouter(tags=["verifier"])


class InjectionTestRequest(BaseModel):
    source_system: str = "ipaas"
    destination_system: str = "datawarehouse"
    payload_type: str = "invoice"
    timeout_seconds: float = 30.0
    chaos_mode: bool = False


class InjectionTestResponse(BaseModel):
    test_id: str
    status: str
    fingerprint: str
    injected_at: Optional[str] = None
    arrived_at: Optional[str] = None
    latency_ms: Optional[float] = None
    source_system: str
    destination_system: str
    discrepancies: list[str]
    passed: bool


@router.get("/api/verifier/payload")
async def generate_test_payload(
    source_system: str = Query("ipaas", description="Source system to simulate"),
    payload_type: str = Query("invoice", description="Type of payload (invoice, contact, order)"),
    chaos_mode: bool = Query(False, description="Inject intentional corruption"),
):
    """
    Generate a test payload for manual injection testing.
    
    Returns a fingerprinted payload that can be injected into the Fabric.
    Use the fingerprint to verify arrival at destination.
    """
    fingerprint, payload = create_injection_payload(
        source_system=source_system,
        payload_type=payload_type,
        chaos_mode=chaos_mode,
    )
    
    return {
        "fingerprint": fingerprint,
        "payload": payload,
        "instructions": {
            "step_1": f"Inject this payload into {source_system}",
            "step_2": "Wait for processing through the Fabric",
            "step_3": f"Call /api/verifier/verify with fingerprint={fingerprint}",
        }
    }


@router.post("/api/verifier/injection-test")
async def run_injection_test(request: InjectionTestRequest):
    """
    Run an end-to-end injection test.
    
    This is a VERIFICATION function:
    1. Creates a test payload with unique fingerprint
    2. Simulates injection into source system
    3. Verifies arrival at destination (mock in standalone mode)
    4. Reports discrepancies
    
    In standalone mode (no external systems connected), this runs a
    simulated test to validate the verification logic.
    """
    logger.info(f"Running injection test: {request.source_system} -> {request.destination_system}")
    
    result = await run_e2e_injection_test(
        source_system=request.source_system,
        destination_system=request.destination_system,
        payload_type=request.payload_type,
        timeout_seconds=request.timeout_seconds,
        chaos_mode=request.chaos_mode,
    )
    
    return result.to_dict()


@router.get("/api/verifier/health")
async def verifier_health():
    """
    Health check for the Farm.Verifier module.
    
    Returns verification capabilities and status.
    """
    return {
        "status": "healthy",
        "module": "Farm.Verifier",
        "role": "Test Oracle",
        "capabilities": [
            "snapshot_generation",
            "expected_block_computation",
            "reconciliation_grading",
            "injection_testing",
            "ground_truth_api",
        ],
        "architectural_boundary": {
            "does": [
                "Generate synthetic test data",
                "Compute expected outcomes",
                "Grade actual results",
                "Provide ground truth for verification",
            ],
            "does_not": [
                "Repair data (belongs to AAM)",
                "Provision connectors (belongs to AOA)",
                "Execute workflows (belongs to AOA)",
                "Buffer raw data (belongs to DCL)",
            ],
        }
    }
