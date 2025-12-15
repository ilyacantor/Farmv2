from datetime import datetime
from enum import Enum
from typing import Any, Optional
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


class EnterpriseProfileEnum(str, Enum):
    modern_saas = "modern_saas"
    regulated_finance = "regulated_finance"
    healthcare_provider = "healthcare_provider"
    global_manufacturing = "global_manufacturing"


class RealismProfileEnum(str, Enum):
    clean = "clean"
    typical = "typical"
    messy = "messy"


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


class CMDBConfigItem(BaseModel):
    ci_id: str
    name: str
    ci_type: CITypeEnum
    lifecycle: LifecycleEnum = LifecycleEnum.prod
    owner: Optional[str] = None
    owner_email: Optional[str] = None
    vendor: Optional[str] = None
    external_ref: Optional[str] = None


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


class FinanceContract(BaseModel):
    contract_id: str
    vendor_name: str
    product: Optional[str] = None
    start_date: str
    end_date: Optional[str] = None
    owner_email: Optional[str] = None


class FinanceTransaction(BaseModel):
    txn_id: str
    vendor_name: str
    amount: float
    currency: str = "USD"
    date: str
    payment_type: PaymentTypeEnum
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


class AllPlanes(BaseModel):
    discovery: DiscoveryPlane
    idp: IdPPlane
    cmdb: CMDBPlane
    cloud: CloudPlane
    endpoint: EndpointPlane
    network: NetworkPlane
    finance: FinancePlane


SCHEMA_VERSION = "farm.v1"


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


class SnapshotRequest(BaseModel):
    tenant_id: str
    seed: int = 12345
    scale: ScaleEnum = ScaleEnum.medium
    enterprise_profile: EnterpriseProfileEnum = EnterpriseProfileEnum.modern_saas
    realism_profile: RealismProfileEnum = RealismProfileEnum.typical


class SnapshotResponse(BaseModel):
    meta: SnapshotMeta
    planes: AllPlanes


class RunRecord(BaseModel):
    run_id: str
    tenant_id: str
    seed: int
    scale: str
    enterprise_profile: str
    realism_profile: str
    generated_at: str
    counts: dict[str, int]
    file_path: Optional[str] = None


class SnapshotCreateResponse(BaseModel):
    snapshot_id: str
    snapshot_fingerprint: str
    tenant_id: str
    created_at: str
    schema_version: str = SCHEMA_VERSION
    duplicate_of_snapshot_id: Optional[str] = None


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


class ReconcileStatusEnum(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class AODSummary(BaseModel):
    assets_admitted: int = 0
    findings: int = 0
    zombies: int = 0
    shadows: int = 0


class AODAsset(BaseModel):
    vendor_key: str
    display_name: Optional[str] = None


class AODLists(BaseModel):
    zombie_assets: list[AODAsset] = Field(default_factory=list)
    shadow_assets: list[AODAsset] = Field(default_factory=list)
    top_findings: list[str] = Field(default_factory=list)


class ReconcileRequest(BaseModel):
    snapshot_id: str
    aod_run_id: str
    tenant_id: str
    aod_summary: AODSummary
    aod_lists: AODLists


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
    farm_expectations: FarmExpectations


class ReconcileMetadata(BaseModel):
    reconciliation_id: str
    snapshot_id: str
    tenant_id: str
    aod_run_id: str
    created_at: str
    status: str
    report_text: str = ""


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
