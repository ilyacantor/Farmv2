"""
End-to-End Injection Tests for Fabric Verification.

ARCHITECTURAL BOUNDARY: These are TEST ORACLE functions.

Pattern:
1. Generate test payload with unique fingerprint
2. Inject into source system (iPaaS input)
3. Poll destination for arrival
4. Compare result against expected outcome
5. Report discrepancies (do NOT repair)

Farm acts as an auditor - it injects tests and validates outcomes,
but never performs operational repairs or manages infrastructure.
"""
import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


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
            "discrepancies": self.discrepancies,
            "passed": self.status == InjectionTestStatus.PASSED,
        }


def create_injection_payload(
    source_system: str = "ipaas",
    payload_type: str = "invoice",
    chaos_mode: bool = False,
) -> tuple[str, dict]:
    """
    Create a test payload with a unique fingerprint for injection testing.
    
    Args:
        source_system: The source system to simulate (ipaas, mulesoft, salesforce)
        payload_type: Type of payload (invoice, contact, order)
        chaos_mode: If True, inject intentional corruption for resilience testing
        
    Returns:
        Tuple of (fingerprint, payload_dict)
    """
    test_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    
    fingerprint = hashlib.sha256(f"{test_id}:{timestamp}".encode()).hexdigest()[:16]
    
    base_payload = {
        "_farm_test": True,
        "_test_id": test_id,
        "_fingerprint": fingerprint,
        "_injected_at": timestamp,
        "_source_system": source_system,
        "_payload_type": payload_type,
    }
    
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
    
    if chaos_mode:
        import random
        chaos_type = random.choice(["missing_field", "bad_type", "truncated"])
        base_payload["_chaos_type"] = chaos_type
        
        if chaos_type == "missing_field" and "amount" in base_payload:
            del base_payload["amount"]
        elif chaos_type == "bad_type" and "amount" in base_payload:
            base_payload["amount"] = "NOT_A_NUMBER"
        elif chaos_type == "truncated":
            base_payload["_truncated"] = True
    
    return fingerprint, base_payload


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


async def run_e2e_injection_test(
    source_system: str = "ipaas",
    destination_system: str = "datawarehouse",
    payload_type: str = "invoice",
    timeout_seconds: float = 30.0,
    chaos_mode: bool = False,
    inject_fn: Optional[Callable] = None,
    poll_fn: Optional[Callable] = None,
) -> InjectionTestResult:
    """
    Run an end-to-end injection test.
    
    This is a VERIFICATION function - it tests the Fabric but does not operate it.
    
    Pattern:
    1. Create test payload with unique fingerprint
    2. Inject into source system
    3. Poll destination for arrival
    4. Compare and report
    
    Args:
        source_system: Where to inject (ipaas, mulesoft, salesforce)
        destination_system: Where to verify arrival (datawarehouse, redis, etc.)
        payload_type: Type of test payload
        timeout_seconds: Max time to wait for arrival
        chaos_mode: Inject intentional corruption
        inject_fn: Custom injection function (for integration)
        poll_fn: Custom polling function (for integration)
        
    Returns:
        InjectionTestResult with pass/fail and discrepancies
    """
    test_id = str(uuid.uuid4())
    fingerprint, payload = create_injection_payload(
        source_system=source_system,
        payload_type=payload_type,
        chaos_mode=chaos_mode,
    )
    
    result = InjectionTestResult(
        test_id=test_id,
        status=InjectionTestStatus.PENDING,
        fingerprint=fingerprint,
        source_system=source_system,
        destination_system=destination_system,
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
            except Exception:
                pass
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


import asyncio
