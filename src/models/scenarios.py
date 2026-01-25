"""
Scenario models for DCL/BLL/NLQ ground truth system.

These models define the structure for deterministic scenario generation
used to validate intent resolution and aggregation correctness.
"""
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class ScaleEnum(str, Enum):
    small = "small"
    medium = "medium"
    large = "large"


class InvoiceStatus(str, Enum):
    paid = "paid"
    pending = "pending"
    overdue = "overdue"


class AssetStatusEnum(str, Enum):
    active = "active"
    zombie = "zombie"
    orphan = "orphan"


class RegionEnum(str, Enum):
    NA = "NA"
    EMEA = "EMEA"
    APAC = "APAC"
    LATAM = "LATAM"


class VendorCategory(str, Enum):
    software = "Software"
    services = "Services"
    infrastructure = "Infrastructure"
    consulting = "Consulting"
    hardware = "Hardware"
    cloud = "Cloud"
    telecom = "Telecom"
    office = "Office"


class CurrencyEnum(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"


class TimeRange(BaseModel):
    start_date: str
    end_date: str


class EntityCounts(BaseModel):
    invoices: int
    customers: int
    vendors: int
    assets: int
    zombies: int
    orphans: int


class PathologyInfo(BaseModel):
    duplicate_customer_names: int = 0
    currency_variance_count: int = 0
    stale_timestamps: int = 0
    orphaned_references: int = 0
    refund_invoices: int = 0


class CurrencyRates(BaseModel):
    USD: float = 1.0
    EUR: float = 1.08
    GBP: float = 1.27
    JPY: float = 0.0067


class ScenarioManifest(BaseModel):
    scenario_id: str
    seed: int
    scale: ScaleEnum
    created_at: str
    time_range: TimeRange
    entity_counts: EntityCounts
    pathologies: PathologyInfo
    currency_rates: CurrencyRates


class Customer(BaseModel):
    customer_id: str
    name: str
    region: RegionEnum
    parent_customer_id: Optional[str] = None
    created_at: str


class Invoice(BaseModel):
    invoice_id: str
    customer_id: str
    vendor_id: str
    amount: float
    currency: CurrencyEnum
    invoice_date: str
    due_date: str
    status: InvoiceStatus
    is_refund: bool = False
    original_invoice_id: Optional[str] = None


class Vendor(BaseModel):
    vendor_id: str
    name: str
    category: VendorCategory


class AssetStatus(BaseModel):
    asset_id: str
    name: str
    status: AssetStatusEnum
    last_activity_at: str
    governed: bool


class RevenueMetric(BaseModel):
    total_revenue: float
    currency: str
    period_start: str
    period_end: str


class MonthlyRevenue(BaseModel):
    month: str
    revenue: float
    delta_pct: Optional[float] = None
    delta_abs: Optional[float] = None


class RevenueMoMMetric(BaseModel):
    months: list[MonthlyRevenue]


class CustomerRevenue(BaseModel):
    customer_id: str
    name: str
    revenue: float
    percent_of_total: float


class TopCustomersMetric(BaseModel):
    customers: list[CustomerRevenue]


class VendorSpendItem(BaseModel):
    vendor_id: str
    name: str
    total_spend: float
    invoice_count: int


class VendorSpendMetric(BaseModel):
    vendors: list[VendorSpendItem]


class ResourceHealthMetric(BaseModel):
    active_count: int
    zombie_count: int
    orphan_count: int
    zombie_ids: list[str]
    orphan_ids: list[str]


class InvoiceVerificationResult(BaseModel):
    is_valid: bool
    invoice_id: str
    mismatches: list[dict] = Field(default_factory=list)
    message: str
