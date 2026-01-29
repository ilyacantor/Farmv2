from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class SourceEnum(str, Enum):
    browser = "browser"
    dns = "dns"
    proxy = "proxy"
    endpoint = "endpoint"
    cloud_api = "cloud_api"
    network_scan = "network_scan"
    saas_audit_log = "saas_audit_log"


class CategoryHintEnum(str, Enum):
    saas = "saas"
    service = "service"
    database = "database"
    infra = "infra"
    unknown = "unknown"


class EnvironmentHintEnum(str, Enum):
    prod = "prod"
    staging = "staging"
    dev = "dev"
    unknown = "unknown"


class IdPTypeEnum(str, Enum):
    application = "application"
    service_principal = "service_principal"


class CITypeEnum(str, Enum):
    app = "app"
    service = "service"
    database = "database"
    infra = "infra"


class LifecycleEnum(str, Enum):
    prod = "prod"
    staging = "staging"
    dev = "dev"


class CloudProviderEnum(str, Enum):
    aws = "aws"
    azure = "azure"
    gcp = "gcp"


class PaymentTypeEnum(str, Enum):
    invoice = "invoice"
    expense = "expense"
    card = "card"


class ScaleEnum(str, Enum):
    small = "small"
    medium = "medium"
    large = "large"
    enterprise = "enterprise"
    mega = "mega"


class EnterpriseProfileEnum(str, Enum):
    modern_saas = "modern_saas"
    regulated_finance = "regulated_finance"
    healthcare_provider = "healthcare_provider"
    global_manufacturing = "global_manufacturing"


class RealismProfileEnum(str, Enum):
    clean = "clean"
    typical = "typical"
    messy = "messy"


class DataPresetEnum(str, Enum):
    clean_baseline = "clean_baseline"
    enterprise_mess = "enterprise_mess"
    adversarial = "adversarial"


class ReconciliationModeEnum(str, Enum):
    sprawl = "sprawl"
    infra = "infra"
    all = "all"


class PresetConfig(BaseModel):
    """Configuration knobs for data generation presets."""
    name: str
    description: str
    domain_coverage: float = Field(ge=0.0, le=1.0, description="% of assets with domain present")
    conflict_rate: float = Field(ge=0.0, le=1.0, description="% cross-plane disagreement")
    junk_domain_count: int = Field(ge=0, description="Number of noise/junk domains")
    aliasing_rate: float = Field(ge=0.0, le=1.0, description="% multi-domain products")
    near_collision_count: int = Field(ge=0, description="Near-collision names for adversarial")
    
    @classmethod
    def from_preset(cls, preset: DataPresetEnum, scale: Optional["ScaleEnum"] = None) -> "PresetConfig":
        # Base configs
        configs = {
            DataPresetEnum.clean_baseline: cls(
                name="Clean Baseline",
                description="Heuristics baseline - minimal noise",
                domain_coverage=0.95,
                conflict_rate=0.05,
                junk_domain_count=0,
                aliasing_rate=0.0,
                near_collision_count=0,
            ),
            DataPresetEnum.enterprise_mess: cls(
                name="Enterprise Mess",
                description="Realistic enterprise chaos - moderate conflicts",
                domain_coverage=0.70,
                conflict_rate=0.20,
                junk_domain_count=200,
                aliasing_rate=0.10,
                near_collision_count=0,
            ),
            DataPresetEnum.adversarial: cls(
                name="Adversarial",
                description="LLM torture test - heavy ambiguity",
                domain_coverage=0.50,
                conflict_rate=0.35,
                junk_domain_count=750,
                aliasing_rate=0.25,
                near_collision_count=50,
            ),
        }
        config = configs[preset]
        
        # Scale down junk for large scales to avoid OOM
        if scale and preset == DataPresetEnum.adversarial:
            scale_factors = {
                "small": 1.0,
                "medium": 1.0,
                "large": 0.5,
                "enterprise": 0.2,
                "mega": 0.1,
            }
            factor = scale_factors.get(scale.value if hasattr(scale, 'value') else scale, 1.0)
            config = cls(
                name=config.name,
                description=config.description,
                domain_coverage=config.domain_coverage,
                conflict_rate=config.conflict_rate,
                junk_domain_count=int(config.junk_domain_count * factor),
                aliasing_rate=config.aliasing_rate,
                near_collision_count=int(config.near_collision_count * factor),
            )
        
        return config


class DiscoveryObservation(BaseModel):
    observation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    observed_at: str
    source: SourceEnum
    observed_name: str
    observed_uri: Optional[str] = None
    hostname: Optional[str] = None
    domain: Optional[str] = None
    vendor_hint: Optional[str] = None
    category_hint: CategoryHintEnum = CategoryHintEnum.unknown
    environment_hint: EnvironmentHintEnum = EnvironmentHintEnum.unknown
    raw: dict[str, Any] = Field(default_factory=dict)


class IdPObject(BaseModel):
    idp_id: str
    name: str
    idp_type: IdPTypeEnum
    external_ref: Optional[str] = None
    has_sso: bool = False
    has_scim: bool = False
    vendor: Optional[str] = None
    last_login_at: Optional[str] = None
    domain: Optional[str] = None
    canonical_domain: Optional[str] = None


class CMDBConfigItem(BaseModel):
    ci_id: str
    name: str
    ci_type: CITypeEnum
    lifecycle: LifecycleEnum = LifecycleEnum.prod
    owner: Optional[str] = None
    owner_email: Optional[str] = None
    vendor: Optional[str] = None
    external_ref: Optional[str] = None
    canonical_domain: Optional[str] = None
    is_system_of_record: bool = False
    data_tier: Optional[str] = None
    data_domain: Optional[str] = None
    description: Optional[str] = None


class CloudResource(BaseModel):
    cloud_id: str
    cloud_provider: CloudProviderEnum
    resource_type: str
    name: str
    uri: Optional[str] = None
    region: Optional[str] = None
    tags: dict[str, str] = Field(default_factory=dict)


class EndpointDevice(BaseModel):
    device_id: str
    device_type: str
    hostname: str
    os: str
    owner_email: Optional[str] = None
    last_seen_at: Optional[str] = None


class EndpointInstalledApp(BaseModel):
    install_id: str
    device_id: str
    app_name: str
    vendor: Optional[str] = None
    version: Optional[str] = None
    installed_at: Optional[str] = None


class NetworkDNS(BaseModel):
    dns_id: str
    queried_domain: str
    source_device: Optional[str] = None
    timestamp: str


class NetworkProxy(BaseModel):
    proxy_id: str
    url: str
    domain: str
    user_email: Optional[str] = None
    timestamp: str


class NetworkCert(BaseModel):
    cert_id: str
    domain: str
    issuer: Optional[str] = None
    not_after: Optional[str] = None


class FinanceVendor(BaseModel):
    vendor_id: str
    vendor_name: str
    domain: Optional[str] = None
    annual_spend: Optional[float] = None


class FinanceContract(BaseModel):
    contract_id: str
    vendor_name: str
    product: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    owner_email: Optional[str] = None
    domain: Optional[str] = None
    annual_value: Optional[float] = None
    contract_type: Optional[str] = None
    contract_term_years: Optional[int] = None


class FinanceTransaction(BaseModel):
    txn_id: str
    vendor_name: str
    amount: float
    currency: str = "USD"
    date: str
    payment_type: PaymentTypeEnum
    is_recurring: bool = False
    memo: Optional[str] = None


class DiscoveryPlane(BaseModel):
    observations: list[DiscoveryObservation] = Field(default_factory=list)


class IdPPlane(BaseModel):
    objects: list[IdPObject] = Field(default_factory=list)


class CMDBPlane(BaseModel):
    cis: list[CMDBConfigItem] = Field(default_factory=list)


class CloudPlane(BaseModel):
    resources: list[CloudResource] = Field(default_factory=list)


class EndpointPlane(BaseModel):
    devices: list[EndpointDevice] = Field(default_factory=list)
    installed_apps: list[EndpointInstalledApp] = Field(default_factory=list)


class NetworkPlane(BaseModel):
    dns: list[NetworkDNS] = Field(default_factory=list)
    proxy: list[NetworkProxy] = Field(default_factory=list)
    certs: list[NetworkCert] = Field(default_factory=list)


class FinancePlane(BaseModel):
    vendors: list[FinanceVendor] = Field(default_factory=list)
    contracts: list[FinanceContract] = Field(default_factory=list)
    transactions: list[FinanceTransaction] = Field(default_factory=list)


class SecurityAttestation(BaseModel):
    attestation_id: str
    asset_name: str
    domain: Optional[str] = None
    vendor: Optional[str] = None
    attestation_date: str
    attester_email: Optional[str] = None
    attestation_type: str = "security_review"
    valid_until: Optional[str] = None
    notes: Optional[str] = None


class SecurityPlane(BaseModel):
    attestations: list[SecurityAttestation] = Field(default_factory=list)


class AllPlanes(BaseModel):
    discovery: DiscoveryPlane
    idp: IdPPlane
    cmdb: CMDBPlane
    cloud: CloudPlane
    endpoint: EndpointPlane
    network: NetworkPlane
    finance: FinancePlane
    security: Optional[SecurityPlane] = None


SCHEMA_VERSION = "farm.v1"


class FabricPlaneInfo(BaseModel):
    """Summary of a fabric plane vendor selection."""
    plane_type: str
    vendor: str
    is_healthy: bool = True


class SORInfo(BaseModel):
    """System of Record information."""
    domain: str
    sor_name: str
    sor_type: str
    confidence: str = "high"


class SnapshotMeta(BaseModel):
    schema_version: str = SCHEMA_VERSION
    snapshot_id: str
    tenant_id: str
    seed: int
    scale: ScaleEnum
    enterprise_profile: EnterpriseProfileEnum
    realism_profile: RealismProfileEnum
    created_at: str
    counts: dict[str, int] = Field(default_factory=dict)
    fabric_planes: list[FabricPlaneInfo] = Field(default_factory=list)
    sors: list[SORInfo] = Field(default_factory=list)
    industry: Optional[str] = None
    
    @property
    def snapshot_as_of(self) -> str:
        return self.created_at
    
    def model_dump(self, **kwargs) -> dict:
        data = super().model_dump(**kwargs)
        data['snapshot_as_of'] = self.created_at
        return data


class SnapshotRequest(BaseModel):
    tenant_id: str
    seed: int = 12345
    scale: ScaleEnum = ScaleEnum.medium
    enterprise_profile: EnterpriseProfileEnum = EnterpriseProfileEnum.modern_saas
    realism_profile: RealismProfileEnum = RealismProfileEnum.typical
    data_preset: Optional[DataPresetEnum] = None
    force: bool = False


class SnapshotResponse(BaseModel):
    meta: SnapshotMeta
    planes: AllPlanes


class SnapshotCreateResponse(BaseModel):
    snapshot_id: str
    snapshot_fingerprint: str
    tenant_id: str
    created_at: str
    schema_version: str = SCHEMA_VERSION
    duplicate_of_snapshot_id: Optional[str] = None
    generation_time_seconds: Optional[float] = None
    validation_passed: Optional[bool] = None
    validation_error_count: Optional[int] = None
    backfill_state: Optional[str] = None


class SnapshotMetadata(BaseModel):
    snapshot_id: str
    snapshot_fingerprint: str
    tenant_id: str
    seed: int
    scale: str
    enterprise_profile: str
    realism_profile: str
    created_at: str
    schema_version: str = SCHEMA_VERSION
    fabric_planes: Optional[List[Dict[str, Any]]] = None
    sors: Optional[List[Dict[str, Any]]] = None
    industry: Optional[str] = None


class ReconcileStatusEnum(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class AODSummary(BaseModel):
    observations_in: int = 0
    candidates_out: int = 0
    assets_admitted: int = 0
    shadow_count: int = 0
    zombie_count: int = 0


class AODLists(BaseModel):
    zombie_assets: list[str] = Field(default_factory=list)
    shadow_assets: list[str] = Field(default_factory=list)
    high_severity_findings: list[str] = Field(default_factory=list)
    actual_reason_codes: dict[str, list[str]] = Field(default_factory=dict, alias="actual_reason_codes")
    admission_actual: dict[str, str] = Field(default_factory=dict, alias="admission_actual")
    reason_codes: dict[str, list[str]] = Field(default_factory=dict)
    admission: dict[str, str] = Field(default_factory=dict)
    aod_reason_codes: dict[str, list[str]] = Field(default_factory=dict)
    asset_summaries: dict[str, dict] = Field(default_factory=dict)
    
    def get_reason_codes(self) -> dict[str, list[str]]:
        """Get reason codes from whichever field AOD populated."""
        if self.actual_reason_codes:
            return self.actual_reason_codes
        if self.reason_codes:
            return self.reason_codes
        if self.aod_reason_codes:
            return self.aod_reason_codes
        return {}
    
    def get_admission(self) -> dict[str, str]:
        """Get admission data from whichever field AOD populated."""
        if self.admission_actual:
            return self.admission_actual
        if self.admission:
            return self.admission
        return {}


class ReconcileRequest(BaseModel):
    snapshot_id: str
    aod_run_id: str
    tenant_id: str
    aod_summary: AODSummary
    aod_lists: AODLists
    mode: ReconciliationModeEnum = ReconciliationModeEnum.sprawl


class FarmExpectations(BaseModel):
    expected_zombies: int = 0
    expected_shadows: int = 0
    zombie_keys: list[str] = Field(default_factory=list)
    shadow_keys: list[str] = Field(default_factory=list)


class ReconcileResponse(BaseModel):
    reconciliation_id: str
    snapshot_id: str
    tenant_id: str
    aod_run_id: str
    created_at: str
    status: ReconcileStatusEnum
    report_text: str
    aod_summary: AODSummary
    aod_lists: AODLists
    farm_expectations: FarmExpectations


class ReconcileMetadata(BaseModel):
    reconciliation_id: str
    snapshot_id: str
    tenant_id: str
    aod_run_id: str
    created_at: str
    status: str
    report_text: str = ""
    contract_status: str = "UNKNOWN"
    has_any_discrepancy: bool = False


class AutoReconcileRequest(BaseModel):
    snapshot_id: str
    tenant_id: str


class AutoReconcileResponse(BaseModel):
    reconciliation_id: str
    snapshot_id: str
    tenant_id: str
    aod_run_id: str
    status: ReconcileStatusEnum
    report_text: str


class AODRunStatusEnum(str, Enum):
    PROCESSED = "PROCESSED"
    NOT_PROCESSED = "NOT_PROCESSED"
    AOD_ERROR = "AOD_ERROR"


class AODRunStatusResponse(BaseModel):
    status: AODRunStatusEnum
    run_id: Optional[str] = None
    message: Optional[str] = None
