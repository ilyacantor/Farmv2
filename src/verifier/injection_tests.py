"""
End-to-End Injection Tests for Fabric Plane Mesh Verification.

ARCHITECTURAL BOUNDARY: These are TEST ORACLE functions.
RACI: FARM is A/R (Accountable/Responsible) for Ground Truth Validation.

Pattern:
1. Generate canary record with unique fingerprint
2. Inject into source Fabric Plane (iPaaS, EventBus, Gateway)
3. Poll destination Fabric Plane for arrival
4. Compare result against expected outcome
5. Report discrepancies (do NOT repair)

Farm acts as an auditor - it injects tests and validates outcomes,
but never performs operational repairs or manages infrastructure.

Fabric Planes:
- IPAAS: Workato, MuleSoft - Integration flow control
- API_GATEWAY: Kong, Apigee - Managed API access
- EVENT_BUS: Kafka, EventBridge - Streaming backbone
- DATA_WAREHOUSE: Snowflake, BigQuery - Source of Truth storage

Enterprise Presets:
- PRESET_6_SCRAPPY: P2P API allowed (small teams)
- PRESET_8_IPAAS: All flows via iPaaS
- PRESET_9_PLATFORM: High-volume via Event Bus
- PRESET_11_WAREHOUSE: Warehouse is Source of Truth
"""
import asyncio
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional, List

from src.models.fabric import (
    FabricPlaneType,
    FabricRoute,
    EnterprisePreset,
    PresetCharacteristics,
    FabricTestPath,
    STANDARD_TEST_PATHS,
    CanaryRecord,
    CanaryVerificationResult,
)


logger = logging.getLogger(__name__)


class InjectionTestStatus(str, Enum):
    PENDING = "pending"
    INJECTED = "injected"
    IN_TRANSIT = "in_transit"
    ARRIVED = "arrived"
    TIMEOUT = "timeout"
    MISMATCH = "mismatch"
    PASSED = "passed"
    FAILED = "failed"


@dataclass
class InjectionTestResult:
    """Result of an end-to-end injection test."""
    test_id: str
    status: InjectionTestStatus
    fingerprint: str
    injected_at: Optional[datetime] = None
    arrived_at: Optional[datetime] = None
    latency_ms: Optional[float] = None
    expected_payload: Optional[dict] = None
    actual_payload: Optional[dict] = None
    discrepancies: list[str] = field(default_factory=list)
    source_system: str = ""
    destination_system: str = ""
    source_plane: Optional[FabricPlaneType] = None
    destination_plane: Optional[FabricPlaneType] = None
    preset: Optional[EnterprisePreset] = None
    
    def to_dict(self) -> dict:
        return {
            "test_id": self.test_id,
            "status": self.status.value,
            "fingerprint": self.fingerprint,
            "injected_at": self.injected_at.isoformat() if self.injected_at else None,
            "arrived_at": self.arrived_at.isoformat() if self.arrived_at else None,
            "latency_ms": self.latency_ms,
            "source_system": self.source_system,
            "destination_system": self.destination_system,
            "source_plane": self.source_plane.value if self.source_plane else None,
            "destination_plane": self.destination_plane.value if self.destination_plane else None,
            "preset": self.preset.value if self.preset else None,
            "discrepancies": self.discrepancies,
            "passed": self.status == InjectionTestStatus.PASSED,
        }


def map_system_to_plane(system: str) -> FabricPlaneType:
    """Map system/vendor names to Fabric Plane types."""
    try:
        return FabricPlaneType(system.lower())
    except ValueError as e:
        logger.debug("Unknown FabricPlaneType %r; falling back to mapping lookup: %s", system, e)
    
    mapping = {
        "mulesoft": FabricPlaneType.IPAAS,
        "workato": FabricPlaneType.IPAAS,
        "boomi": FabricPlaneType.IPAAS,
        "tray": FabricPlaneType.IPAAS,
        "celigo": FabricPlaneType.IPAAS,
        "datawarehouse": FabricPlaneType.DATA_WAREHOUSE,
        "snowflake": FabricPlaneType.DATA_WAREHOUSE,
        "bigquery": FabricPlaneType.DATA_WAREHOUSE,
        "redshift": FabricPlaneType.DATA_WAREHOUSE,
        "databricks": FabricPlaneType.DATA_WAREHOUSE,
        "synapse": FabricPlaneType.DATA_WAREHOUSE,
        "warehouse": FabricPlaneType.DATA_WAREHOUSE,
        "kafka": FabricPlaneType.EVENT_BUS,
        "eventbridge": FabricPlaneType.EVENT_BUS,
        "rabbitmq": FabricPlaneType.EVENT_BUS,
        "pulsar": FabricPlaneType.EVENT_BUS,
        "azure_event_hubs": FabricPlaneType.EVENT_BUS,
        "event_bus": FabricPlaneType.EVENT_BUS,
        "eventbus": FabricPlaneType.EVENT_BUS,
        "kong": FabricPlaneType.API_GATEWAY,
        "apigee": FabricPlaneType.API_GATEWAY,
        "gateway": FabricPlaneType.API_GATEWAY,
        "aws_api_gateway": FabricPlaneType.API_GATEWAY,
        "azure_api_management": FabricPlaneType.API_GATEWAY,
        "api_gateway": FabricPlaneType.API_GATEWAY,
        "apigateway": FabricPlaneType.API_GATEWAY,
    }
    result = mapping.get(system.lower())
    if result is None:
        raise ValueError(f"Unknown system '{system}' - cannot map to Fabric Plane")
    return result


def infer_preset_from_path(
    source: FabricPlaneType, 
    destination: FabricPlaneType
) -> EnterprisePreset:
    """Infer the most likely preset based on fabric path."""
    if source == FabricPlaneType.IPAAS or destination == FabricPlaneType.IPAAS:
        return EnterprisePreset.PRESET_8_IPAAS
    if source == FabricPlaneType.EVENT_BUS or destination == FabricPlaneType.EVENT_BUS:
        return EnterprisePreset.PRESET_9_PLATFORM
    if source == FabricPlaneType.DATA_WAREHOUSE or destination == FabricPlaneType.DATA_WAREHOUSE:
        return EnterprisePreset.PRESET_11_WAREHOUSE
    return EnterprisePreset.PRESET_6_SCRAPPY


def create_injection_payload(
    source_system: str = "ipaas",
    payload_type: str = "invoice",
    chaos_mode: bool = False,
    preset: Optional[EnterprisePreset] = None,
) -> tuple[str, dict]:
    """
    Create a test payload with a unique fingerprint for injection testing.
    
    Args:
        source_system: The source system to simulate (ipaas, mulesoft, kafka)
        payload_type: Type of payload (invoice, contact, order)
        chaos_mode: If True, inject intentional corruption for resilience testing
        preset: Enterprise preset context
        
    Returns:
        Tuple of (fingerprint, payload_dict)
    """
    test_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    source_plane = map_system_to_plane(source_system)
    
    fingerprint = hashlib.sha256(f"{test_id}:{timestamp}".encode()).hexdigest()[:16]
    
    base_payload = {
        "_farm_test": True,
        "_test_id": test_id,
        "_fingerprint": fingerprint,
        "_injected_at": timestamp,
        "_source_system": source_system,
        "_source_plane": source_plane.value,
        "_payload_type": payload_type,
    }
    
    if preset:
        base_payload["_preset"] = preset.value
    
    if payload_type == "invoice":
        base_payload.update({
            "invoice_id": f"INV-TEST-{fingerprint[:8].upper()}",
            "vendor_name": "Farm Test Vendor",
            "amount": 1234.56,
            "currency": "USD",
            "due_date": "2026-02-28",
            "line_items": [
                {"description": "Test Service", "quantity": 1, "unit_price": 1234.56}
            ],
        })
    elif payload_type == "contact":
        base_payload.update({
            "contact_id": f"CON-TEST-{fingerprint[:8].upper()}",
            "email": f"test-{fingerprint[:8]}@farm.test",
            "name": "Farm Test Contact",
            "company": "Farm Testing Inc",
        })
    elif payload_type == "order":
        base_payload.update({
            "order_id": f"ORD-TEST-{fingerprint[:8].upper()}",
            "customer": "Farm Test Customer",
            "total": 999.99,
            "status": "pending",
        })
    elif payload_type == "event":
        base_payload.update({
            "event_id": f"EVT-TEST-{fingerprint[:8].upper()}",
            "event_type": "domain_event",
            "entity_type": "customer",
            "entity_id": f"CUST-{fingerprint[:6].upper()}",
            "action": "updated",
            "timestamp": timestamp,
        })
    
    if chaos_mode:
        import random
        chaos_type = random.choice(["missing_field", "bad_type", "truncated", "duplicate"])
        base_payload["_chaos_type"] = chaos_type
        
        if chaos_type == "missing_field" and "amount" in base_payload:
            del base_payload["amount"]
        elif chaos_type == "bad_type" and "amount" in base_payload:
            base_payload["amount"] = "NOT_A_NUMBER"
        elif chaos_type == "truncated":
            base_payload["_truncated"] = True
        elif chaos_type == "duplicate":
            base_payload["_duplicate_marker"] = fingerprint
    
    return fingerprint, base_payload


def create_canary_record(
    source_plane: FabricPlaneType,
    destination_plane: FabricPlaneType,
    preset: EnterprisePreset,
    payload_type: str = "invoice",
) -> CanaryRecord:
    """
    Create a canary record for injection testing.
    
    Canary records are special payloads with unique fingerprints that Farm
    uses to verify data flows correctly through the Fabric Mesh.
    """
    fingerprint, payload = create_injection_payload(
        source_system=source_plane.value,
        payload_type=payload_type,
        preset=preset,
    )
    
    characteristics = PresetCharacteristics.for_preset(preset)
    expected_latency = 100
    if destination_plane == FabricPlaneType.DATA_WAREHOUSE:
        expected_latency = 500
    
    return CanaryRecord(
        canary_id=str(uuid.uuid4()),
        fingerprint=fingerprint,
        injected_at=datetime.now(timezone.utc).isoformat(),
        source_plane=source_plane,
        destination_plane=destination_plane,
        preset=preset,
        payload=payload,
        expected_arrival_ms=expected_latency,
    )


def verify_payload_arrival(
    fingerprint: str,
    expected_payload: dict,
    actual_payload: Optional[dict],
    strict: bool = True,
) -> tuple[bool, list[str]]:
    """
    Verify an arrived payload matches the expected payload.
    
    Args:
        fingerprint: The unique test fingerprint to match
        expected_payload: What we injected
        actual_payload: What arrived at destination
        strict: If True, all fields must match exactly
        
    Returns:
        Tuple of (passed, list_of_discrepancies)
    """
    discrepancies = []
    
    if actual_payload is None:
        return False, ["Payload never arrived (timeout)"]
    
    actual_fingerprint = actual_payload.get("_fingerprint")
    if actual_fingerprint != fingerprint:
        discrepancies.append(f"Fingerprint mismatch: expected {fingerprint}, got {actual_fingerprint}")
    
    if strict:
        for key, expected_value in expected_payload.items():
            if key.startswith("_"):
                continue
            actual_value = actual_payload.get(key)
            if actual_value != expected_value:
                discrepancies.append(f"Field '{key}' mismatch: expected {expected_value}, got {actual_value}")
    
    return len(discrepancies) == 0, discrepancies


def verify_canary_arrival(
    canary: CanaryRecord,
    actual_payload: Optional[dict],
    arrival_time_ms: Optional[int] = None,
) -> CanaryVerificationResult:
    """
    Verify a canary record arrived correctly at the destination plane.
    
    This is a TEST ORACLE function - it only verifies, never repairs.
    """
    passed, discrepancies = verify_payload_arrival(
        fingerprint=canary.fingerprint,
        expected_payload=canary.payload,
        actual_payload=actual_payload,
    )
    
    return CanaryVerificationResult(
        canary_id=canary.canary_id,
        fingerprint=canary.fingerprint,
        arrived=actual_payload is not None,
        arrival_time_ms=arrival_time_ms,
        destination_plane=canary.destination_plane,
        payload_intact=passed,
        discrepancies=discrepancies,
    )


async def run_e2e_injection_test(
    source_system: str = "ipaas",
    destination_system: str = "datawarehouse",
    payload_type: str = "invoice",
    timeout_seconds: float = 30.0,
    chaos_mode: bool = False,
    preset: Optional[EnterprisePreset] = None,
    inject_fn: Optional[Callable] = None,
    poll_fn: Optional[Callable] = None,
) -> InjectionTestResult:
    """
    Run an end-to-end injection test across Fabric Planes.
    
    This is a VERIFICATION function - it tests the Fabric but does not operate it.
    
    Pattern:
    1. Create test payload with unique fingerprint
    2. Inject into source Fabric Plane
    3. Poll destination Fabric Plane for arrival
    4. Compare and report
    
    Args:
        source_system: Source plane/vendor (ipaas, mulesoft, kafka, etc.)
        destination_system: Destination plane/vendor (datawarehouse, snowflake, etc.)
        payload_type: Type of test payload
        timeout_seconds: Max time to wait for arrival
        chaos_mode: Inject intentional corruption
        preset: Enterprise preset context
        inject_fn: Custom injection function (for real integration)
        poll_fn: Custom polling function (for real integration)
        
    Returns:
        InjectionTestResult with pass/fail and discrepancies
    """
    test_id = str(uuid.uuid4())
    source_plane = map_system_to_plane(source_system)
    destination_plane = map_system_to_plane(destination_system)
    
    effective_preset = preset or infer_preset_from_path(source_plane, destination_plane)
    
    fingerprint, payload = create_injection_payload(
        source_system=source_system,
        payload_type=payload_type,
        chaos_mode=chaos_mode,
        preset=effective_preset,
    )
    
    result = InjectionTestResult(
        test_id=test_id,
        status=InjectionTestStatus.PENDING,
        fingerprint=fingerprint,
        source_system=source_system,
        destination_system=destination_system,
        source_plane=source_plane,
        destination_plane=destination_plane,
        preset=effective_preset,
        expected_payload=payload,
    )
    
    inject_time = datetime.now(timezone.utc)
    result.injected_at = inject_time
    result.status = InjectionTestStatus.INJECTED
    
    if inject_fn:
        try:
            await inject_fn(source_system, payload)
        except Exception as e:
            result.status = InjectionTestStatus.FAILED
            result.discrepancies.append(f"Injection failed: {str(e)}")
            return result
    
    result.status = InjectionTestStatus.IN_TRANSIT
    
    actual_payload = None
    deadline = time.time() + timeout_seconds
    
    if poll_fn:
        while time.time() < deadline:
            try:
                actual_payload = await poll_fn(destination_system, fingerprint)
                if actual_payload:
                    break
            except Exception as e:
                logger.debug("Poll attempt failed during injection test: %s", e)
            await asyncio.sleep(0.5)
    else:
        await asyncio.sleep(0.1)
        actual_payload = payload.copy()
    
    if actual_payload is None:
        result.status = InjectionTestStatus.TIMEOUT
        result.discrepancies.append(f"Payload did not arrive within {timeout_seconds}s")
        return result
    
    arrival_time = datetime.now(timezone.utc)
    result.arrived_at = arrival_time
    result.latency_ms = (arrival_time - inject_time).total_seconds() * 1000
    result.actual_payload = actual_payload
    result.status = InjectionTestStatus.ARRIVED
    
    passed, discrepancies = verify_payload_arrival(
        fingerprint=fingerprint,
        expected_payload=payload,
        actual_payload=actual_payload,
    )
    
    result.discrepancies = discrepancies
    result.status = InjectionTestStatus.PASSED if passed else InjectionTestStatus.MISMATCH
    
    return result


class InjectionTestHarness:
    """
    Harness for running injection tests across the Fabric Plane Mesh.
    
    ARCHITECTURAL BOUNDARY: This is a TEST ORACLE component.
    Farm owns verification/grading, NOT operational execution.
    
    Usage:
        harness = InjectionTestHarness(preset=EnterprisePreset.PRESET_8_IPAAS)
        results = await harness.run_standard_tests()
    """
    
    def __init__(
        self,
        preset: EnterprisePreset = EnterprisePreset.PRESET_8_IPAAS,
        inject_fn: Optional[Callable] = None,
        poll_fn: Optional[Callable] = None,
    ):
        self.preset = preset
        self.characteristics = PresetCharacteristics.for_preset(preset)
        self.inject_fn = inject_fn
        self.poll_fn = poll_fn
        self.results: List[InjectionTestResult] = []
        
    def get_applicable_test_paths(self) -> List[FabricTestPath]:
        """Get test paths applicable to the current preset."""
        return [
            path for path in STANDARD_TEST_PATHS
            if path.preset == self.preset
        ]
    
    async def run_single_test(
        self,
        source_plane: FabricPlaneType,
        destination_plane: FabricPlaneType,
        payload_type: str = "invoice",
        chaos_mode: bool = False,
    ) -> InjectionTestResult:
        """Run a single injection test between two planes."""
        result = await run_e2e_injection_test(
            source_system=source_plane.value,
            destination_system=destination_plane.value,
            payload_type=payload_type,
            preset=self.preset,
            chaos_mode=chaos_mode,
            inject_fn=self.inject_fn,
            poll_fn=self.poll_fn,
        )
        self.results.append(result)
        return result
    
    async def run_standard_tests(self) -> List[InjectionTestResult]:
        """Run all standard tests for the current preset."""
        test_paths = self.get_applicable_test_paths()
        results = []
        
        for path in test_paths:
            result = await self.run_single_test(
                source_plane=path.source_plane,
                destination_plane=path.destination_plane,
            )
            results.append(result)
        
        return results
    
    async def run_canary_batch(
        self,
        count: int = 5,
        source_plane: Optional[FabricPlaneType] = None,
        destination_plane: Optional[FabricPlaneType] = None,
    ) -> List[CanaryVerificationResult]:
        """
        Run a batch of canary tests for load/stress testing.
        
        Returns list of verification results.
        """
        effective_source = source_plane or self.characteristics.primary_fabric_plane
        effective_dest = destination_plane or FabricPlaneType.DATA_WAREHOUSE
        
        canaries = [
            create_canary_record(
                source_plane=effective_source,
                destination_plane=effective_dest,
                preset=self.preset,
            )
            for _ in range(count)
        ]
        
        results = []
        for canary in canaries:
            await asyncio.sleep(0.05)
            
            result = verify_canary_arrival(
                canary=canary,
                actual_payload=canary.payload.copy(),
                arrival_time_ms=int(canary.expected_arrival_ms * 0.9),
            )
            results.append(result)
        
        return results
    
    def get_summary(self) -> dict:
        """Get summary of all tests run."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == InjectionTestStatus.PASSED)
        failed = sum(1 for r in self.results if r.status in (
            InjectionTestStatus.FAILED, 
            InjectionTestStatus.MISMATCH,
            InjectionTestStatus.TIMEOUT,
        ))
        
        return {
            "preset": self.preset.value,
            "preset_name": self.characteristics.name,
            "total_tests": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / total if total > 0 else 0.0,
            "primary_plane": self.characteristics.primary_fabric_plane.value,
        }
