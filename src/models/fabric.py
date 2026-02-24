"""
Fabric Plane Mesh Architecture Types.

This module defines the core abstractions for the AOS Fabric Plane Mesh:
- 4 Fabric Planes: IPAAS, API_GATEWAY, EVENT_BUS, DATA_WAREHOUSE
- 4 Enterprise Presets: SCRAPPY, IPAAS_CENTRIC, PLATFORM_ORIENTED, WAREHOUSE_CENTRIC
- Industry-specific vendor weights based on 2025/2026 market adoption

CRITICAL CONSTRAINT: AAM (The Mesh) connects ONLY to Fabric Planes, not directly
to individual SaaS applications (except in Preset 6 Scrappy mode).

FARM uses these types to generate appropriate test scenarios for each preset.
"""
from collections import defaultdict
from enum import Enum
from typing import Optional, List, Dict
from pydantic import BaseModel, Field
import random


class FabricPlaneType(str, Enum):
    """The 4 Fabric Planes that AAM connects to."""
    IPAAS = "ipaas"
    API_GATEWAY = "api_gateway"
    EVENT_BUS = "event_bus"
    DATA_WAREHOUSE = "data_warehouse"


class IndustryVertical(str, Enum):
    """Industry verticals with distinct fabric adoption patterns."""
    DEFAULT = "default"
    FINANCE = "finance"
    HEALTHCARE = "healthcare"
    MANUFACTURING = "manufacturing"
    LOGISTICS = "logistics"
    TECH_SAAS = "tech_saas"
    RETAIL = "retail"
    MEDIA = "media"
    GOVERNMENT = "government"


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
    
    
# Canonical source: farm_config.yaml → vendors.fabric_plane_vendors
# Reads YAML directly to avoid circular import through generators/__init__.py.
# Compiled fallback below matches the YAML defaults.
def _build_plane_vendor_lists() -> Dict[str, list]:
    from pathlib import Path
    try:
        import yaml
        candidate = Path(__file__).resolve().parent.parent.parent / "farm_config.yaml"
        if candidate.is_file():
            with open(candidate, encoding="utf-8") as f:
                raw = yaml.safe_load(f)
            if isinstance(raw, dict):
                fpv = (raw.get("vendors") or {}).get("fabric_plane_vendors")
                if fpv:
                    by_plane: Dict[str, list] = defaultdict(list)
                    for slug, info in fpv.items():
                        by_plane[info["plane"]].append(slug)
                    return dict(by_plane)
    except Exception:
        pass
    # Compiled fallback — matches farm_config.yaml defaults
    return {
        "ipaas": ["workato", "mulesoft", "boomi", "tray.io", "celigo", "sap_integration_suite"],
        "api_gateway": ["kong", "apigee", "aws_api_gateway", "azure_api_management"],
        "event_bus": ["kafka", "confluent", "eventbridge", "rabbitmq", "pulsar", "azure_event_hubs"],
        "data_warehouse": ["snowflake", "bigquery", "redshift", "databricks", "synapse"],
    }

_PLANE_VENDORS = _build_plane_vendor_lists()


class FabricPlaneVendors:
    """Known vendors for each Fabric Plane type."""
    IPAAS = _PLANE_VENDORS.get("ipaas", [])
    API_GATEWAY = _PLANE_VENDORS.get("api_gateway", [])
    EVENT_BUS = _PLANE_VENDORS.get("event_bus", [])
    DATA_WAREHOUSE = _PLANE_VENDORS.get("data_warehouse", [])

    @classmethod
    def for_plane(cls, plane_type: FabricPlaneType) -> list[str]:
        return {
            FabricPlaneType.IPAAS: cls.IPAAS,
            FabricPlaneType.API_GATEWAY: cls.API_GATEWAY,
            FabricPlaneType.EVENT_BUS: cls.EVENT_BUS,
            FabricPlaneType.DATA_WAREHOUSE: cls.DATA_WAREHOUSE,
        }[plane_type]


INDUSTRY_VENDOR_WEIGHTS: Dict[IndustryVertical, Dict[FabricPlaneType, Dict[str, float]]] = {
    IndustryVertical.DEFAULT: {
        FabricPlaneType.IPAAS: {
            "mulesoft": 0.35,
            "workato": 0.30,
            "boomi": 0.15,
            "tray.io": 0.10,
            "celigo": 0.10,
        },
        FabricPlaneType.API_GATEWAY: {
            "aws_api_gateway": 0.40,
            "apigee": 0.30,
            "kong": 0.15,
            "azure_api_management": 0.15,
        },
        FabricPlaneType.EVENT_BUS: {
            "eventbridge": 0.35,
            "confluent": 0.30,
            "kafka": 0.20,
            "azure_event_hubs": 0.10,
            "rabbitmq": 0.05,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "snowflake": 0.35,
            "databricks": 0.25,
            "bigquery": 0.20,
            "redshift": 0.15,
            "synapse": 0.05,
        },
    },
    IndustryVertical.FINANCE: {
        FabricPlaneType.IPAAS: {
            "mulesoft": 0.55,
            "boomi": 0.20,
            "workato": 0.15,
            "sap_integration_suite": 0.10,
        },
        FabricPlaneType.API_GATEWAY: {
            "apigee": 0.50,
            "kong": 0.25,
            "aws_api_gateway": 0.15,
            "azure_api_management": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "confluent": 0.45,
            "kafka": 0.30,
            "eventbridge": 0.15,
            "azure_event_hubs": 0.10,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "snowflake": 0.45,
            "databricks": 0.25,
            "synapse": 0.15,
            "redshift": 0.10,
            "bigquery": 0.05,
        },
    },
    IndustryVertical.HEALTHCARE: {
        FabricPlaneType.IPAAS: {
            "mulesoft": 0.50,
            "boomi": 0.25,
            "workato": 0.15,
            "celigo": 0.10,
        },
        FabricPlaneType.API_GATEWAY: {
            "apigee": 0.45,
            "aws_api_gateway": 0.25,
            "azure_api_management": 0.20,
            "kong": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "confluent": 0.40,
            "kafka": 0.25,
            "azure_event_hubs": 0.20,
            "eventbridge": 0.15,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "snowflake": 0.50,
            "databricks": 0.20,
            "synapse": 0.15,
            "redshift": 0.10,
            "bigquery": 0.05,
        },
    },
    IndustryVertical.MANUFACTURING: {
        FabricPlaneType.IPAAS: {
            "sap_integration_suite": 0.40,
            "boomi": 0.30,
            "mulesoft": 0.20,
            "workato": 0.10,
        },
        FabricPlaneType.API_GATEWAY: {
            "kong": 0.40,
            "aws_api_gateway": 0.25,
            "apigee": 0.20,
            "azure_api_management": 0.15,
        },
        FabricPlaneType.EVENT_BUS: {
            "eventbridge": 0.35,
            "rabbitmq": 0.25,
            "kafka": 0.20,
            "confluent": 0.15,
            "azure_event_hubs": 0.05,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "databricks": 0.35,
            "snowflake": 0.30,
            "redshift": 0.20,
            "synapse": 0.10,
            "bigquery": 0.05,
        },
    },
    IndustryVertical.LOGISTICS: {
        FabricPlaneType.IPAAS: {
            "sap_integration_suite": 0.35,
            "boomi": 0.30,
            "mulesoft": 0.20,
            "workato": 0.15,
        },
        FabricPlaneType.API_GATEWAY: {
            "kong": 0.35,
            "aws_api_gateway": 0.30,
            "apigee": 0.20,
            "azure_api_management": 0.15,
        },
        FabricPlaneType.EVENT_BUS: {
            "eventbridge": 0.40,
            "rabbitmq": 0.25,
            "kafka": 0.20,
            "confluent": 0.10,
            "azure_event_hubs": 0.05,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "databricks": 0.30,
            "snowflake": 0.30,
            "redshift": 0.25,
            "bigquery": 0.10,
            "synapse": 0.05,
        },
    },
    IndustryVertical.TECH_SAAS: {
        FabricPlaneType.IPAAS: {
            "workato": 0.40,
            "tray.io": 0.25,
            "mulesoft": 0.20,
            "celigo": 0.15,
        },
        FabricPlaneType.API_GATEWAY: {
            "aws_api_gateway": 0.45,
            "kong": 0.30,
            "apigee": 0.15,
            "azure_api_management": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "kafka": 0.35,
            "confluent": 0.30,
            "eventbridge": 0.25,
            "pulsar": 0.10,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "snowflake": 0.35,
            "databricks": 0.30,
            "bigquery": 0.25,
            "redshift": 0.10,
        },
    },
    IndustryVertical.RETAIL: {
        FabricPlaneType.IPAAS: {
            "workato": 0.35,
            "mulesoft": 0.30,
            "boomi": 0.20,
            "celigo": 0.15,
        },
        FabricPlaneType.API_GATEWAY: {
            "aws_api_gateway": 0.40,
            "apigee": 0.30,
            "kong": 0.20,
            "azure_api_management": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "eventbridge": 0.40,
            "kafka": 0.25,
            "confluent": 0.20,
            "rabbitmq": 0.15,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "snowflake": 0.40,
            "bigquery": 0.25,
            "databricks": 0.20,
            "redshift": 0.15,
        },
    },
    IndustryVertical.MEDIA: {
        FabricPlaneType.IPAAS: {
            "workato": 0.35,
            "tray.io": 0.30,
            "mulesoft": 0.20,
            "celigo": 0.15,
        },
        FabricPlaneType.API_GATEWAY: {
            "aws_api_gateway": 0.45,
            "kong": 0.25,
            "apigee": 0.20,
            "azure_api_management": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "kafka": 0.40,
            "confluent": 0.30,
            "eventbridge": 0.20,
            "pulsar": 0.10,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "bigquery": 0.35,
            "snowflake": 0.30,
            "databricks": 0.25,
            "redshift": 0.10,
        },
    },
    IndustryVertical.GOVERNMENT: {
        FabricPlaneType.IPAAS: {
            "mulesoft": 0.45,
            "boomi": 0.30,
            "sap_integration_suite": 0.15,
            "workato": 0.10,
        },
        FabricPlaneType.API_GATEWAY: {
            "apigee": 0.40,
            "azure_api_management": 0.30,
            "aws_api_gateway": 0.20,
            "kong": 0.10,
        },
        FabricPlaneType.EVENT_BUS: {
            "azure_event_hubs": 0.35,
            "confluent": 0.30,
            "kafka": 0.25,
            "eventbridge": 0.10,
        },
        FabricPlaneType.DATA_WAREHOUSE: {
            "synapse": 0.35,
            "snowflake": 0.30,
            "databricks": 0.20,
            "redshift": 0.15,
        },
    },
}


def select_vendor_weighted(
    plane_type: FabricPlaneType,
    industry: IndustryVertical = IndustryVertical.DEFAULT,
    rng: Optional[random.Random] = None
) -> str:
    """
    Select a vendor for a fabric plane using industry-specific weights.
    
    Uses weighted random selection based on 2025/2026 market adoption data.
    Same seed produces deterministic results for reproducible testing.
    """
    if rng is None:
        rng = random.Random()
    
    weights = INDUSTRY_VENDOR_WEIGHTS.get(industry, INDUSTRY_VENDOR_WEIGHTS[IndustryVertical.DEFAULT])
    plane_weights = weights.get(plane_type, {})
    
    if not plane_weights:
        vendors = FabricPlaneVendors.for_plane(plane_type)
        return rng.choice(vendors)
    
    vendors = list(plane_weights.keys())
    weight_values = list(plane_weights.values())
    
    return rng.choices(vendors, weights=weight_values, k=1)[0]


def generate_fabric_config(
    industry: IndustryVertical = IndustryVertical.DEFAULT,
    seed: Optional[int] = None
) -> Dict[FabricPlaneType, "FabricPlaneConfig"]:
    """
    Generate a complete fabric plane configuration for an enterprise.
    
    Uses industry-specific weighted vendor selection for realistic scenarios.
    Deterministic when seed is provided.
    """
    rng = random.Random(seed) if seed else random.Random()
    
    config = {}
    for plane_type in FabricPlaneType:
        vendor = select_vendor_weighted(plane_type, industry, rng)
        config[plane_type] = FabricPlaneConfig(
            plane_type=plane_type,
            vendor=vendor,
            endpoint=f"https://{vendor}.fabric.example.com/api/v1",
            is_healthy=rng.random() > 0.05,
            latency_ms=rng.randint(10, 200),
        )
    
    return config


class IndustryProfile(BaseModel):
    """Profile describing industry-specific fabric characteristics."""
    industry: IndustryVertical
    name: str
    description: str
    primary_cloud: str
    compliance_focus: List[str]
    typical_scale: str
    
    @classmethod
    def for_industry(cls, industry: IndustryVertical) -> "IndustryProfile":
        profiles = {
            IndustryVertical.DEFAULT: cls(
                industry=industry,
                name="Default Enterprise",
                description="General enterprise with hyperscaler bundle",
                primary_cloud="multi-cloud",
                compliance_focus=["SOC2", "GDPR"],
                typical_scale="500-5000 employees",
            ),
            IndustryVertical.FINANCE: cls(
                industry=industry,
                name="Regulated Finance",
                description="Banks, insurance, investment firms with strict compliance",
                primary_cloud="private/hybrid",
                compliance_focus=["SOX", "PCI-DSS", "FFIEC", "GDPR"],
                typical_scale="1000+ employees",
            ),
            IndustryVertical.HEALTHCARE: cls(
                industry=industry,
                name="Healthcare & Life Sciences",
                description="Hospitals, pharma, biotech with patient data protection",
                primary_cloud="private/hybrid",
                compliance_focus=["HIPAA", "HITRUST", "FDA 21 CFR Part 11"],
                typical_scale="500+ employees",
            ),
            IndustryVertical.MANUFACTURING: cls(
                industry=industry,
                name="Manufacturing & Industrial",
                description="Factories, supply chain with edge computing focus",
                primary_cloud="hybrid/edge",
                compliance_focus=["ISO 27001", "ITAR"],
                typical_scale="1000+ employees",
            ),
            IndustryVertical.LOGISTICS: cls(
                industry=industry,
                name="Logistics & Transportation",
                description="Shipping, fleet management, supply chain",
                primary_cloud="hybrid/edge",
                compliance_focus=["ISO 27001", "C-TPAT"],
                typical_scale="500+ employees",
            ),
            IndustryVertical.TECH_SAAS: cls(
                industry=industry,
                name="Tech & SaaS",
                description="Software companies, cloud-native startups",
                primary_cloud="public cloud",
                compliance_focus=["SOC2", "ISO 27001", "GDPR"],
                typical_scale="50-2000 employees",
            ),
            IndustryVertical.RETAIL: cls(
                industry=industry,
                name="Retail & E-commerce",
                description="Online and physical retail with omnichannel focus",
                primary_cloud="public cloud",
                compliance_focus=["PCI-DSS", "GDPR", "CCPA"],
                typical_scale="500+ employees",
            ),
            IndustryVertical.MEDIA: cls(
                industry=industry,
                name="Media & Entertainment",
                description="Streaming, publishing, gaming with high throughput",
                primary_cloud="public cloud",
                compliance_focus=["GDPR", "COPPA"],
                typical_scale="200+ employees",
            ),
            IndustryVertical.GOVERNMENT: cls(
                industry=industry,
                name="Government & Public Sector",
                description="Federal, state, local government with sovereign cloud",
                primary_cloud="sovereign/gov-cloud",
                compliance_focus=["FedRAMP", "FISMA", "StateRAMP"],
                typical_scale="1000+ employees",
            ),
        }
        return profiles.get(industry, profiles[IndustryVertical.DEFAULT])


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
