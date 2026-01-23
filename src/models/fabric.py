"""
Fabric Plane Mesh Architecture Types.

This module defines the core abstractions for the AOS Fabric Plane Mesh:
- 4 Fabric Planes: IPAAS, API_GATEWAY, EVENT_BUS, DATA_WAREHOUSE
- 4 Enterprise Presets: SCRAPPY, IPAAS_CENTRIC, PLATFORM_ORIENTED, WAREHOUSE_CENTRIC

CRITICAL CONSTRAINT: AAM (The Mesh) connects ONLY to Fabric Planes, not directly
to individual SaaS applications (except in Preset 6 Scrappy mode).

FARM uses these types to generate appropriate test scenarios for each preset.
"""
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class FabricPlaneType(str, Enum):
    """The 4 Fabric Planes that AAM connects to."""
    IPAAS = "ipaas"
    API_GATEWAY = "api_gateway"
    EVENT_BUS = "event_bus"
    DATA_WAREHOUSE = "data_warehouse"


class FabricRoute(str, Enum):
    """How data routes through the Fabric Mesh."""
    VIA_IPAAS = "via_ipaas"
    VIA_GATEWAY = "via_gateway"
    VIA_BUS = "via_bus"
    VIA_WAREHOUSE = "via_warehouse"
    VIA_DIRECT = "via_direct"


class EnterprisePreset(str, Enum):
    """
    Enterprise Preset patterns that determine connection strategy.
    
    - PRESET_6_SCRAPPY: P2P API connections allowed (startups, small teams)
    - PRESET_8_IPAAS: Integration via iPaaS (Workato, MuleSoft)
    - PRESET_9_PLATFORM: High-volume via Event Bus (Kafka, EventBridge)
    - PRESET_11_WAREHOUSE: Source of Truth is Data Warehouse (Snowflake, BigQuery)
    """
    PRESET_6_SCRAPPY = "preset_6_scrappy"
    PRESET_8_IPAAS = "preset_8_ipaas"
    PRESET_9_PLATFORM = "preset_9_platform"
    PRESET_11_WAREHOUSE = "preset_11_warehouse"


class FabricPlaneConfig(BaseModel):
    """Configuration for a Fabric Plane connection."""
    plane_type: FabricPlaneType
    vendor: str
    endpoint: Optional[str] = None
    is_healthy: bool = True
    latency_ms: Optional[int] = None
    
    
class FabricPlaneVendors:
    """Known vendors for each Fabric Plane type."""
    IPAAS = ["workato", "mulesoft", "boomi", "tray.io", "celigo"]
    API_GATEWAY = ["kong", "apigee", "aws_api_gateway", "azure_api_management"]
    EVENT_BUS = ["kafka", "eventbridge", "rabbitmq", "pulsar", "azure_event_hubs"]
    DATA_WAREHOUSE = ["snowflake", "bigquery", "redshift", "databricks", "synapse"]
    
    @classmethod
    def for_plane(cls, plane_type: FabricPlaneType) -> list[str]:
        return {
            FabricPlaneType.IPAAS: cls.IPAAS,
            FabricPlaneType.API_GATEWAY: cls.API_GATEWAY,
            FabricPlaneType.EVENT_BUS: cls.EVENT_BUS,
            FabricPlaneType.DATA_WAREHOUSE: cls.DATA_WAREHOUSE,
        }[plane_type]


class PresetCharacteristics(BaseModel):
    """Characteristics of each Enterprise Preset."""
    preset: EnterprisePreset
    name: str
    description: str
    primary_fabric_plane: FabricPlaneType
    allows_direct_p2p: bool = False
    typical_org_size: str
    data_flow_pattern: str
    
    @classmethod
    def for_preset(cls, preset: EnterprisePreset) -> "PresetCharacteristics":
        presets = {
            EnterprisePreset.PRESET_6_SCRAPPY: cls(
                preset=preset,
                name="Scrappy",
                description="P2P API connections allowed - startups/small teams",
                primary_fabric_plane=FabricPlaneType.API_GATEWAY,
                allows_direct_p2p=True,
                typical_org_size="1-50 employees",
                data_flow_pattern="Direct API calls to SaaS apps",
            ),
            EnterprisePreset.PRESET_8_IPAAS: cls(
                preset=preset,
                name="iPaaS-Centric",
                description="Integration logic flows via iPaaS platform",
                primary_fabric_plane=FabricPlaneType.IPAAS,
                allows_direct_p2p=False,
                typical_org_size="50-500 employees",
                data_flow_pattern="Workato/MuleSoft recipes orchestrate all flows",
            ),
            EnterprisePreset.PRESET_9_PLATFORM: cls(
                preset=preset,
                name="Platform-Oriented",
                description="High-volume data flows via Event Bus",
                primary_fabric_plane=FabricPlaneType.EVENT_BUS,
                allows_direct_p2p=False,
                typical_org_size="500-5000 employees",
                data_flow_pattern="Kafka topics for domain events",
            ),
            EnterprisePreset.PRESET_11_WAREHOUSE: cls(
                preset=preset,
                name="Warehouse-Centric",
                description="Data Warehouse is source of truth",
                primary_fabric_plane=FabricPlaneType.DATA_WAREHOUSE,
                allows_direct_p2p=False,
                typical_org_size="1000+ employees",
                data_flow_pattern="Snowflake/BigQuery as canonical store",
            ),
        }
        return presets[preset]


class FabricTestPath(BaseModel):
    """Defines a test path through the Fabric Mesh for injection testing."""
    source_plane: FabricPlaneType
    destination_plane: FabricPlaneType
    preset: EnterprisePreset
    expected_latency_ms: int = 100
    expected_hops: int = 1
    requires_transformation: bool = False
    description: str = ""


STANDARD_TEST_PATHS: list[FabricTestPath] = [
    FabricTestPath(
        source_plane=FabricPlaneType.IPAAS,
        destination_plane=FabricPlaneType.DATA_WAREHOUSE,
        preset=EnterprisePreset.PRESET_8_IPAAS,
        expected_latency_ms=500,
        expected_hops=1,
        requires_transformation=True,
        description="iPaaS recipe writes to warehouse (common ETL path)"
    ),
    FabricTestPath(
        source_plane=FabricPlaneType.EVENT_BUS,
        destination_plane=FabricPlaneType.DATA_WAREHOUSE,
        preset=EnterprisePreset.PRESET_9_PLATFORM,
        expected_latency_ms=200,
        expected_hops=1,
        requires_transformation=True,
        description="Kafka topic consumed by warehouse loader"
    ),
    FabricTestPath(
        source_plane=FabricPlaneType.API_GATEWAY,
        destination_plane=FabricPlaneType.IPAAS,
        preset=EnterprisePreset.PRESET_8_IPAAS,
        expected_latency_ms=150,
        expected_hops=1,
        requires_transformation=False,
        description="Gateway webhook triggers iPaaS flow"
    ),
    FabricTestPath(
        source_plane=FabricPlaneType.IPAAS,
        destination_plane=FabricPlaneType.EVENT_BUS,
        preset=EnterprisePreset.PRESET_9_PLATFORM,
        expected_latency_ms=100,
        expected_hops=1,
        requires_transformation=False,
        description="iPaaS publishes domain event to bus"
    ),
    FabricTestPath(
        source_plane=FabricPlaneType.DATA_WAREHOUSE,
        destination_plane=FabricPlaneType.API_GATEWAY,
        preset=EnterprisePreset.PRESET_11_WAREHOUSE,
        expected_latency_ms=300,
        expected_hops=2,
        requires_transformation=True,
        description="Warehouse-backed API (reverse ETL)"
    ),
]


class CanaryRecord(BaseModel):
    """
    A canary record for injection testing.
    
    Canary records have unique fingerprints that allow Farm to verify
    they flow correctly through the Fabric Mesh without modification.
    """
    canary_id: str
    fingerprint: str
    injected_at: str
    source_plane: FabricPlaneType
    destination_plane: FabricPlaneType
    preset: EnterprisePreset
    payload: dict
    expected_arrival_ms: int = 1000
    
    
class CanaryVerificationResult(BaseModel):
    """Result of verifying a canary record's arrival."""
    canary_id: str
    fingerprint: str
    arrived: bool
    arrival_time_ms: Optional[int] = None
    destination_plane: FabricPlaneType
    payload_intact: bool = True
    discrepancies: list[str] = Field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        return self.arrived and self.payload_intact and len(self.discrepancies) == 0
