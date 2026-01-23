"""
Farm.Verifier - Test Oracle Namespace

ARCHITECTURAL BOUNDARY: This module contains ONLY verification/grading logic.
Farm.Verifier is the "Ground Truth" auditor for the AOS Fabric.

Responsibilities:
- Generate synthetic test data with known expected outcomes
- Inject test payloads into the Fabric (iPaaS, DCL, AAM)
- Verify results arrive correctly at destinations (Data Warehouse, etc.)
- Grade actual results against expected outcomes
- Generate audit reports

What this module does NOT do:
- NO operational repairs (belongs to AAM)
- NO connector provisioning (belongs to AOA)
- NO workflow execution (belongs to AOA)
- NO raw data buffering (belongs to DCL)

End-to-End Injection Test Pattern:
1. Farm.Verifier generates a test payload with known fingerprint
2. Payload is injected into iPaaS input
3. Farm.Verifier polls destination (Data Warehouse) for result
4. Result is compared against expected outcome
5. Discrepancies are reported (but NOT repaired by Farm)
"""

from src.verifier.injection_tests import (
    InjectionTestResult,
    InjectionTestStatus,
    create_injection_payload,
    verify_payload_arrival,
    run_e2e_injection_test,
)

__all__ = [
    "InjectionTestResult",
    "InjectionTestStatus",
    "create_injection_payload",
    "verify_payload_arrival",
    "run_e2e_injection_test",
]
