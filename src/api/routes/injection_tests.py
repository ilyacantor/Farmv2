"""
API endpoints for End-to-End Injection Tests.

ARCHITECTURAL BOUNDARY: These are VERIFICATION endpoints.
RACI: FARM is A/R for Ground Truth Validation and Injection Testing.

Farm.Verifier injects test payloads and validates they arrive correctly
across Fabric Planes. Farm does NOT perform repairs or manage infrastructure.

Fabric Planes:
- IPAAS: Workato, MuleSoft
- API_GATEWAY: Kong, Apigee
- EVENT_BUS: Kafka, EventBridge
- DATA_WAREHOUSE: Snowflake, BigQuery

Enterprise Presets:
- PRESET_6_SCRAPPY: P2P allowed
- PRESET_8_IPAAS: All via iPaaS
- PRESET_9_PLATFORM: Event Bus
- PRESET_11_WAREHOUSE: Warehouse-centric
"""
import logging
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from src.verifier import (
    InjectionTestResult,
    InjectionTestStatus,
    InjectionTestHarness,
    create_injection_payload,
    create_canary_record,
    run_e2e_injection_test,
    FabricPlaneType,
    EnterprisePreset,
    PresetCharacteristics,
    STANDARD_TEST_PATHS,
)

logger = logging.getLogger("farm.api.injection")
router = APIRouter(tags=["verifier"])


class InjectionTestRequest(BaseModel):
    source_system: str = "ipaas"
    destination_system: str = "datawarehouse"
    payload_type: str = "invoice"
    timeout_seconds: float = 30.0
    chaos_mode: bool = False
    preset: Optional[str] = None


class InjectionTestResponse(BaseModel):
    test_id: str
    status: str
    fingerprint: str
    injected_at: Optional[str] = None
    arrived_at: Optional[str] = None
    latency_ms: Optional[float] = None
    source_system: str
    destination_system: str
    source_plane: Optional[str] = None
    destination_plane: Optional[str] = None
    preset: Optional[str] = None
    discrepancies: list[str]
    passed: bool


class HarnessTestRequest(BaseModel):
    preset: str = "preset_8_ipaas"
    run_standard_tests: bool = True
    canary_batch_count: int = 0


@router.get("/api/verifier/payload")
async def generate_test_payload(
    source_system: str = Query("ipaas", description="Source system (ipaas, kafka, snowflake, kong)"),
    payload_type: str = Query("invoice", description="Type of payload (invoice, contact, order, event)"),
    chaos_mode: bool = Query(False, description="Inject intentional corruption"),
    preset: Optional[str] = Query(None, description="Enterprise preset (preset_6_scrappy, preset_8_ipaas, etc.)"),
):
    """
    Generate a test payload for manual injection testing.
    
    Returns a fingerprinted payload that can be injected into the Fabric.
    Use the fingerprint to verify arrival at destination.
    """
    effective_preset = None
    if preset:
        try:
            effective_preset = EnterprisePreset(preset)
        except ValueError:
            pass
    
    fingerprint, payload = create_injection_payload(
        source_system=source_system,
        payload_type=payload_type,
        chaos_mode=chaos_mode,
        preset=effective_preset,
    )
    
    return {
        "fingerprint": fingerprint,
        "payload": payload,
        "instructions": {
            "step_1": f"Inject this payload into {source_system}",
            "step_2": "Wait for processing through the Fabric Mesh",
            "step_3": f"Call /api/verifier/verify with fingerprint={fingerprint}",
        }
    }


@router.post("/api/verifier/injection-test")
async def run_injection_test(request: InjectionTestRequest):
    """
    Run an end-to-end injection test across Fabric Planes.
    
    This is a VERIFICATION function:
    1. Creates a test payload with unique fingerprint
    2. Injects into source Fabric Plane
    3. Verifies arrival at destination Fabric Plane
    4. Reports discrepancies
    
    In standalone mode (no external systems connected), this runs a
    simulated test to validate the verification logic.
    """
    logger.info(f"Running injection test: {request.source_system} -> {request.destination_system}")
    
    effective_preset = None
    if request.preset:
        try:
            effective_preset = EnterprisePreset(request.preset)
        except ValueError:
            pass
    
    result = await run_e2e_injection_test(
        source_system=request.source_system,
        destination_system=request.destination_system,
        payload_type=request.payload_type,
        timeout_seconds=request.timeout_seconds,
        chaos_mode=request.chaos_mode,
        preset=effective_preset,
    )
    
    return result.to_dict()


@router.post("/api/verifier/harness")
async def run_harness_tests(request: HarnessTestRequest):
    """
    Run tests using the InjectionTestHarness.
    
    The harness provides:
    - Standard test paths for the specified preset
    - Canary batch testing for load/stress tests
    - Summary statistics
    """
    try:
        preset = EnterprisePreset(request.preset)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid preset: {request.preset}. Valid: preset_6_scrappy, preset_8_ipaas, preset_9_platform, preset_11_warehouse"
        )
    
    harness = InjectionTestHarness(preset=preset)
    
    results = []
    canary_results = []
    
    if request.run_standard_tests:
        test_results = await harness.run_standard_tests()
        results = [r.to_dict() for r in test_results]
    
    if request.canary_batch_count > 0:
        canaries = await harness.run_canary_batch(count=request.canary_batch_count)
        canary_results = [
            {
                "canary_id": c.canary_id,
                "fingerprint": c.fingerprint,
                "arrived": c.arrived,
                "arrival_time_ms": c.arrival_time_ms,
                "destination_plane": c.destination_plane.value,
                "payload_intact": c.payload_intact,
                "passed": c.passed,
            }
            for c in canaries
        ]
    
    return {
        "preset": preset.value,
        "summary": harness.get_summary(),
        "test_results": results,
        "canary_results": canary_results,
    }


@router.get("/api/verifier/presets")
async def list_presets():
    """
    List all Enterprise Presets and their characteristics.
    
    Presets determine how AAM connects to Fabric Planes.
    """
    presets = []
    for preset in EnterprisePreset:
        char = PresetCharacteristics.for_preset(preset)
        presets.append({
            "preset": preset.value,
            "name": char.name,
            "description": char.description,
            "primary_fabric_plane": char.primary_fabric_plane.value,
            "allows_direct_p2p": char.allows_direct_p2p,
            "typical_org_size": char.typical_org_size,
            "data_flow_pattern": char.data_flow_pattern,
        })
    
    return {
        "presets": presets,
        "fabric_planes": [p.value for p in FabricPlaneType],
    }


@router.get("/api/verifier/test-paths")
async def list_test_paths(
    preset: Optional[str] = Query(None, description="Filter by preset"),
):
    """
    List standard test paths for injection testing.
    
    Test paths define source -> destination flows through the Fabric Mesh.
    """
    paths = STANDARD_TEST_PATHS
    
    if preset:
        try:
            filter_preset = EnterprisePreset(preset)
            paths = [p for p in paths if p.preset == filter_preset]
        except ValueError:
            pass
    
    return {
        "test_paths": [
            {
                "source_plane": p.source_plane.value,
                "destination_plane": p.destination_plane.value,
                "preset": p.preset.value,
                "expected_latency_ms": p.expected_latency_ms,
                "expected_hops": p.expected_hops,
                "requires_transformation": p.requires_transformation,
                "description": p.description,
            }
            for p in paths
        ],
        "total_count": len(paths),
    }


@router.get("/api/verifier/canary")
async def generate_canary(
    source_plane: str = Query("ipaas", description="Source Fabric Plane"),
    destination_plane: str = Query("data_warehouse", description="Destination Fabric Plane"),
    preset: str = Query("preset_8_ipaas", description="Enterprise preset"),
    payload_type: str = Query("invoice", description="Payload type"),
):
    """
    Generate a canary record for injection testing.
    
    Canary records are fingerprinted payloads used to verify
    data flows correctly through the Fabric Mesh.
    """
    try:
        src = FabricPlaneType(source_plane)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid source_plane: {source_plane}")
    
    try:
        dst = FabricPlaneType(destination_plane)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid destination_plane: {destination_plane}")
    
    try:
        p = EnterprisePreset(preset)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid preset: {preset}")
    
    canary = create_canary_record(
        source_plane=src,
        destination_plane=dst,
        preset=p,
        payload_type=payload_type,
    )
    
    return {
        "canary_id": canary.canary_id,
        "fingerprint": canary.fingerprint,
        "injected_at": canary.injected_at,
        "source_plane": canary.source_plane.value,
        "destination_plane": canary.destination_plane.value,
        "preset": canary.preset.value,
        "expected_arrival_ms": canary.expected_arrival_ms,
        "payload": canary.payload,
    }


@router.get("/api/verifier/health")
async def verifier_health():
    """
    Health check for the Farm.Verifier module.
    
    Returns verification capabilities and Fabric Plane Mesh status.
    """
    return {
        "status": "healthy",
        "module": "Farm.Verifier",
        "role": "Test Oracle",
        "raci": "A/R for Ground Truth Validation, Injection Tests, Accuracy Measurement",
        "capabilities": [
            "snapshot_generation",
            "expected_block_computation",
            "reconciliation_grading",
            "injection_testing",
            "canary_records",
            "ground_truth_api",
        ],
        "fabric_planes": [p.value for p in FabricPlaneType],
        "enterprise_presets": [p.value for p in EnterprisePreset],
        "architectural_boundary": {
            "does": [
                "Generate synthetic test data",
                "Compute expected outcomes",
                "Grade actual results",
                "Inject canary records into Fabric",
                "Verify payload arrival at destination",
                "Provide ground truth for verification",
            ],
            "does_not": [
                "Repair data (belongs to AAM)",
                "Provision connectors (belongs to AOA)",
                "Execute workflows (belongs to AOA)",
                "Buffer raw data (belongs to DCL)",
                "Manage Fabric Plane connections (belongs to AAM)",
            ],
        }
    }
