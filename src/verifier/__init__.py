"""
Farm.Verifier - Test Oracle Namespace

ARCHITECTURAL BOUNDARY: This module contains ONLY verification/grading logic.
Farm.Verifier is the "Ground Truth" auditor for the AOS Fabric Plane Mesh.

RACI: FARM is A/R (Accountable/Responsible) for:
- Ground Truth Validation
- End-to-End Injection Tests
- Accuracy Measurement

Fabric Planes (AAM connects to these, not apps):
- IPAAS: Workato, MuleSoft - Integration flow control
- API_GATEWAY: Kong, Apigee - Managed API access
- EVENT_BUS: Kafka, EventBridge - Streaming backbone
- DATA_WAREHOUSE: Snowflake, BigQuery - Source of Truth storage

Enterprise Presets:
- PRESET_6_SCRAPPY: P2P API allowed (small teams)
- PRESET_8_IPAAS: All flows via iPaaS
- PRESET_9_PLATFORM: High-volume via Event Bus
- PRESET_11_WAREHOUSE: Warehouse is Source of Truth

What this module does NOT do:
- NO operational repairs (belongs to AAM)
- NO connector provisioning (belongs to AOA)
- NO workflow execution (belongs to AOA)
- NO raw data buffering (belongs to DCL)

End-to-End Injection Test Pattern:
1. Farm.Verifier generates canary record with known fingerprint
2. Canary is injected into source Fabric Plane (iPaaS/Gateway/Bus)
3. Farm.Verifier polls destination Fabric Plane for result
4. Result is compared against expected outcome
5. Discrepancies are reported (but NOT repaired by Farm)
"""

from src.verifier.injection_tests import (
    InjectionTestResult,
    InjectionTestStatus,
    InjectionTestHarness,
    create_injection_payload,
    create_canary_record,
    verify_payload_arrival,
    verify_canary_arrival,
    run_e2e_injection_test,
    map_system_to_plane,
    infer_preset_from_path,
)

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

__all__ = [
    "InjectionTestResult",
    "InjectionTestStatus",
    "InjectionTestHarness",
    "create_injection_payload",
    "create_canary_record",
    "verify_payload_arrival",
    "verify_canary_arrival",
    "run_e2e_injection_test",
    "map_system_to_plane",
    "infer_preset_from_path",
    "FabricPlaneType",
    "FabricRoute",
    "EnterprisePreset",
    "PresetCharacteristics",
    "FabricTestPath",
    "STANDARD_TEST_PATHS",
    "CanaryRecord",
    "CanaryVerificationResult",
]
