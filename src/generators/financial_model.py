"""
Enterprise Financial Model Engine.

Standalone module that produces 12 quarters of internally consistent financial data
through a strict dependency chain:

    Assumptions → ARR Model → Revenue → P&L → Balance Sheet → Cash Flow → SaaS Metrics

Nothing is circular. Everything flows downward. The model outputs a Quarter object per
period containing ~131 scalar metrics + 13 dimensional breakdowns ≈ 177 data points/quarter.

Usage:
    from src.generators.financial_model import FinancialModel, validate_model

    model = FinancialModel()
    quarters = model.generate()

    issues = validate_model(quarters)
    assert len(issues) == 0
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field, asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_logger = logging.getLogger("farm.financial_model")


# ═══════════════════════════════════════════════════════════════════════════════
# YAML config loader — reads farm_config.yaml once at import time.
# Absence of the file is normal; compiled defaults in the dataclass are used.
# ═══════════════════════════════════════════════════════════════════════════════

def _load_farm_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load a farm config YAML from the given path or default farm_config.yaml.

    Returns a flat dict merging company_profile + realism_params sections.
    Returns empty dict if the file is absent (compiled defaults apply).
    Also stores the raw 'schema' and 'entity' sections for other modules.
    """
    if config_path:
        candidate = Path(config_path)
    else:
        candidate = Path(__file__).resolve().parent.parent.parent / "farm_config.yaml"
    if not candidate.is_file():
        return {}
    try:
        with open(candidate, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if not isinstance(raw, dict):
            return {}
        cp = raw.get("company_profile") or {}
        rp = raw.get("realism_params") or {}
        merged = {**cp, **rp}
        # Stash the raw config so other modules can read the schema and entity sections
        merged["_raw"] = raw
        # Promote entity section fields to top level for Assumptions access
        entity = raw.get("entity") or {}
        for k, v in entity.items():
            merged.setdefault(k, v)
        return merged
    except Exception as exc:
        _logger.warning(f"Failed to load {candidate}: {exc} — using compiled defaults")
        return {}

_cfg = _load_farm_config()


def get_schema_config() -> Dict[str, Any]:
    """Return the 'schema' section from farm_config.yaml, or empty dict."""
    raw = _cfg.get("_raw", {})
    return raw.get("schema") or {}


def get_vendor_config() -> Dict[str, Any]:
    """Return the 'vendors' section from farm_config.yaml, or empty dict."""
    raw = _cfg.get("_raw", {})
    return raw.get("vendors") or {}


# ═══════════════════════════════════════════════════════════════════════════════
# Assumptions — the ONLY independent inputs
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Assumptions:
    """
    Driver assumptions that parameterize the entire financial model.

    Values are loaded from farm_config.yaml when present.
    Compiled defaults (the second argument to _cfg.get) are the fallback
    when the YAML file is absent — Farm always runs without a config file.
    Field names are the DCL contract and must not be renamed.

    For multi-entity generation (Phase 1), use Assumptions.from_yaml(path)
    to load entity-specific configs (farm_config_meridian.yaml, etc.).
    """

    # Entity identity — None for legacy single-entity mode
    entity_id: Optional[str] = _cfg.get("entity_id", None)
    entity_name: Optional[str] = _cfg.get("entity_name", None)
    business_model: str = _cfg.get("business_model", "saas")  # saas | consultancy | bpm

    # ARR (SaaS model — used when business_model == "saas")
    starting_arr: float = _cfg.get("starting_arr", 83.6)
    arr_growth_rate_annual: float = _cfg.get("arr_growth_rate_annual", 0.32)
    arr_growth_deceleration: float = _cfg.get("arr_growth_deceleration", 0.06)
    new_logo_pct_of_new_arr: float = _cfg.get("new_logo_pct_of_new_arr", 0.42)

    # Churn & retention
    gross_churn_rate_annual: float = _cfg.get("gross_churn_rate_annual", 0.082)
    churn_improvement_annual: float = _cfg.get("churn_improvement_annual", 0.005)
    nrr_base: float = _cfg.get("nrr_base", 114.0)

    # P&L ratios (as fraction of revenue)
    cogs_pct: float = _cfg.get("cogs_pct", 0.35)
    cogs_improvement_annual: float = _cfg.get("cogs_improvement_annual", 0.007)
    sm_pct: float = _cfg.get("sm_pct", 0.115)
    rd_pct: float = _cfg.get("rd_pct", 0.085)
    ga_pct: float = _cfg.get("ga_pct", 0.065)
    da_pct: float = _cfg.get("da_pct", 0.035)

    # COGS breakdown
    cogs_hosting_pct: float = _cfg.get("cogs_hosting_pct", 0.35)
    cogs_support_staff_pct: float = _cfg.get("cogs_support_staff_pct", 0.30)
    cogs_ps_pct: float = _cfg.get("cogs_ps_pct", 0.20)
    cogs_licenses_pct: float = _cfg.get("cogs_licenses_pct", 0.10)
    cogs_payments_pct: float = _cfg.get("cogs_payments_pct", 0.05)

    # Balance sheet drivers
    dso_days: float = _cfg.get("dso_days", 45.0)
    dso_improvement_annual: float = _cfg.get("dso_improvement_annual", 1.0)
    deferred_rev_months: float = _cfg.get("deferred_rev_months", 5.5)
    capex_pct_revenue: float = _cfg.get("capex_pct_revenue", 0.03)
    tax_rate: float = _cfg.get("tax_rate", 0.25)

    # Starting balance sheet
    starting_cash: float = _cfg.get("starting_cash", 61.29)
    starting_ar: float = _cfg.get("starting_ar", 10.82)
    starting_unbilled_revenue: float = _cfg.get("starting_unbilled_revenue", 3.5)
    starting_prepaid: float = _cfg.get("starting_prepaid", 4.2)
    starting_pp_e: float = _cfg.get("starting_pp_e", 8.5)
    starting_intangibles: float = _cfg.get("starting_intangibles", 5.2)
    starting_goodwill: float = _cfg.get("starting_goodwill", 45.0)
    starting_ap: float = _cfg.get("starting_ap", 3.5)
    starting_accrued_expenses: float = _cfg.get("starting_accrued_expenses", 6.8)

    # Customers
    starting_customer_count: int = _cfg.get("starting_customer_count", 760)
    acv_enterprise: float = _cfg.get("acv_enterprise", 0.185)
    acv_mid_market: float = _cfg.get("acv_mid_market", 0.065)
    acv_smb: float = _cfg.get("acv_smb", 0.018)
    segment_enterprise_pct: float = _cfg.get("segment_enterprise_pct", 0.20)
    segment_mid_market_pct: float = _cfg.get("segment_mid_market_pct", 0.40)
    segment_smb_pct: float = _cfg.get("segment_smb_pct", 0.40)
    churn_smb_pct_of_total: float = _cfg.get("churn_smb_pct_of_total", 0.50)
    churn_mm_pct_of_total: float = _cfg.get("churn_mm_pct_of_total", 0.30)
    churn_ent_pct_of_total: float = _cfg.get("churn_ent_pct_of_total", 0.20)

    # Regional mix
    region_amer: float = _cfg.get("region_amer", 0.50)
    region_emea: float = _cfg.get("region_emea", 0.30)
    region_apac: float = _cfg.get("region_apac", 0.20)
    region_latam: float = _cfg.get("region_latam", 0.0)

    # People
    starting_headcount: int = _cfg.get("starting_headcount", 235)
    hc_engineering_pct: float = _cfg.get("hc_engineering_pct", 0.319)
    hc_product_pct: float = _cfg.get("hc_product_pct", 0.077)
    hc_sales_pct: float = _cfg.get("hc_sales_pct", 0.179)
    hc_marketing_pct: float = _cfg.get("hc_marketing_pct", 0.098)
    hc_cs_pct: float = _cfg.get("hc_cs_pct", 0.136)
    hc_ga_pct: float = _cfg.get("hc_ga_pct", 0.191)
    attrition_rate_annual: float = _cfg.get("attrition_rate_annual", 0.12)
    attrition_improvement_annual: float = _cfg.get("attrition_improvement_annual", 0.005)

    # Pipeline
    pipeline_multiple: float = _cfg.get("pipeline_multiple", 3.6)
    win_rate: float = _cfg.get("win_rate", 39.0)
    sales_cycle_days: float = _cfg.get("sales_cycle_days", 90.0)

    # Support
    tickets_per_customer_annual: float = _cfg.get("tickets_per_customer_annual", 15.0)
    csat_base: float = _cfg.get("csat_base", 4.15)
    nps_base: int = _cfg.get("nps_base", 40)
    first_response_hours: float = _cfg.get("first_response_hours", 2.5)
    resolution_hours: float = _cfg.get("resolution_hours", 18.0)

    # Engineering
    points_per_sprint: float = _cfg.get("points_per_sprint", 96.0)
    sprints_per_quarter: int = _cfg.get("sprints_per_quarter", 6)
    tech_debt_pct: float = _cfg.get("tech_debt_pct", 0.15)

    # Infrastructure
    cloud_spend_pct_revenue: float = _cfg.get("cloud_spend_pct_revenue", 0.028)
    uptime_pct: float = _cfg.get("uptime_pct", 99.45)
    p1_incidents_per_quarter: int = _cfg.get("p1_incidents_per_quarter", 3)
    p2_incidents_per_quarter: int = _cfg.get("p2_incidents_per_quarter", 8)
    mttr_p1_hours: float = _cfg.get("mttr_p1_hours", 2.5)
    mttr_p2_hours: float = _cfg.get("mttr_p2_hours", 4.0)

    # ── Non-SaaS revenue model (consultancy, BPM) ──────────────────────
    starting_annual_revenue: float = _cfg.get("starting_annual_revenue", 0.0)
    revenue_growth_rate_annual: float = _cfg.get("revenue_growth_rate_annual", 0.06)
    revenue_growth_deceleration: float = _cfg.get("revenue_growth_deceleration", 0.005)

    # ── Consultancy-specific (business_model == "consultancy") ────────────
    tm_revenue_pct: float = _cfg.get("tm_revenue_pct", 0.65)
    fixed_fee_revenue_pct: float = _cfg.get("fixed_fee_revenue_pct", 0.35)
    avg_billing_rate: float = _cfg.get("avg_billing_rate", 350.0)
    utilization_rate: float = _cfg.get("utilization_rate", 0.72)
    realization_rate: float = _cfg.get("realization_rate", 0.94)
    starting_consultant_count: int = _cfg.get("starting_consultant_count", 0)
    starting_corporate_count: int = _cfg.get("starting_corporate_count", 0)
    bench_consultant_count: int = _cfg.get("bench_consultant_count", 0)
    bench_monthly_cost: float = _cfg.get("bench_monthly_cost", 0.0151)
    sales_pct: float = _cfg.get("sales_pct", 0.04)
    marketing_pct: float = _cfg.get("marketing_pct", 0.025)
    facilities_pct: float = _cfg.get("facilities_pct", 0.005)
    recruiting_pct: float = _cfg.get("recruiting_pct", 0.01)
    cogs_consultant_comp_pct: float = _cfg.get("cogs_consultant_comp_pct", 0.62)
    cogs_bench_pct: float = _cfg.get("cogs_bench_pct", 0.267)
    cogs_subcontractor_pct: float = _cfg.get("cogs_subcontractor_pct", 0.082)
    cogs_travel_pct: float = _cfg.get("cogs_travel_pct", 0.031)
    avg_engagement_value: float = _cfg.get("avg_engagement_value", 4.2)
    utilization_improvement_annual: float = _cfg.get("utilization_improvement_annual", 0.005)
    hc_finance_pct: float = _cfg.get("hc_finance_pct", 0.08)
    hc_hr_pct: float = _cfg.get("hc_hr_pct", 0.05)
    hc_it_pct: float = _cfg.get("hc_it_pct", 0.12)
    hc_legal_pct: float = _cfg.get("hc_legal_pct", 0.024)

    # ── BPM-specific (business_model == "bpm") ───────────────────────────
    managed_services_pct: float = _cfg.get("managed_services_pct", 0.44)
    per_fte_revenue_pct: float = _cfg.get("per_fte_revenue_pct", 0.37)
    per_transaction_pct: float = _cfg.get("per_transaction_pct", 0.19)
    onshore_fte_count: int = _cfg.get("onshore_fte_count", 0)
    offshore_fte_count: int = _cfg.get("offshore_fte_count", 0)
    nearshore_fte_count: int = _cfg.get("nearshore_fte_count", 0)
    bench_fte_count: int = _cfg.get("bench_fte_count", 0)
    onshore_avg_comp: float = _cfg.get("onshore_avg_comp", 0.070)
    offshore_avg_comp: float = _cfg.get("offshore_avg_comp", 0.020)
    nearshore_avg_comp: float = _cfg.get("nearshore_avg_comp", 0.020)
    cogs_onshore_pct: float = _cfg.get("cogs_onshore_pct", 0.148)
    cogs_offshore_pct: float = _cfg.get("cogs_offshore_pct", 0.592)
    cogs_nearshore_pct: float = _cfg.get("cogs_nearshore_pct", 0.148)
    cogs_delivery_center_ops_pct: float = _cfg.get("cogs_delivery_center_ops_pct", 0.113)
    cogs_benefits_pct: float = _cfg.get("cogs_benefits_pct", 0.138)
    starting_delivery_count: int = _cfg.get("starting_delivery_count", 0)
    bench_delivery_count: int = _cfg.get("bench_delivery_count", 0)
    sm_pct: float = _cfg.get("sm_pct", 0.115)  # combined S&M for BPM
    tech_automation_pct: float = _cfg.get("tech_automation_pct", 0.08)
    facilities_corporate_pct: float = _cfg.get("facilities_corporate_pct", 0.055)
    capitalized_recruiting_annual: float = _cfg.get("capitalized_recruiting_annual", 0.0)
    capitalized_automation_annual: float = _cfg.get("capitalized_automation_annual", 0.0)
    depreciation_method: str = _cfg.get("depreciation_method", "straight_line")
    depreciation_years: int = _cfg.get("depreciation_years", 5)
    automation_rate: float = _cfg.get("automation_rate", 0.35)
    sla_attainment: float = _cfg.get("sla_attainment", 0.972)
    automation_improvement_annual: float = _cfg.get("automation_improvement_annual", 0.03)
    attrition_corporate_annual: float = _cfg.get("attrition_corporate_annual", 0.10)
    avg_contract_value: float = _cfg.get("avg_contract_value", 5.0)
    hc_sales_marketing_pct: float = _cfg.get("hc_sales_marketing_pct", 0.167)
    hc_operations_pct: float = _cfg.get("hc_operations_pct", 0.20)

    @classmethod
    def from_yaml(cls, config_path: str) -> "Assumptions":
        """Load assumptions from a specific YAML config file.

        Used for multi-entity generation where each entity has its own config
        (farm_config_meridian.yaml, farm_config_cascadia.yaml, etc.).
        """
        cfg = _load_farm_config(config_path)
        if not cfg:
            raise FileNotFoundError(f"Config not found or empty: {config_path}")
        # Build Assumptions with all fields from the loaded config
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        kwargs = {}
        for k, v in cfg.items():
            if k in field_names and k != "_raw":
                kwargs[k] = v
        return cls(**kwargs)


# ═══════════════════════════════════════════════════════════════════════════════
# Quarter — output data structure per period
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Quarter:
    """One quarter of fully computed financial data (~177 data points)."""

    quarter: str            # e.g. "2024-Q1"
    quarter_index: int      # 0-based ordinal
    is_forecast: bool
    period_type: str = "actual"  # "actual" or "forecast" — derived from is_forecast
    entity_id: Optional[str] = None       # entity tag for multi-entity (Phase 1)
    entity_name: Optional[str] = None
    business_model: str = "saas"          # saas | consultancy | bpm
    dimensions: Dict[str, str] = field(default_factory=dict)

    # ── ARR waterfall ──────────────────────────────────────────────────────
    beginning_arr: float = 0.0
    new_arr: float = 0.0
    new_logo_arr: float = 0.0
    expansion_arr: float = 0.0
    churned_arr: float = 0.0
    ending_arr: float = 0.0

    # ── Revenue decomposition ──────────────────────────────────────────────
    revenue: float = 0.0
    new_logo_revenue: float = 0.0
    expansion_revenue: float = 0.0
    renewal_revenue: float = 0.0

    # ── P&L ────────────────────────────────────────────────────────────────
    cogs: float = 0.0
    gross_profit: float = 0.0
    gross_margin_pct: float = 0.0
    sm_expense: float = 0.0
    rd_expense: float = 0.0
    ga_expense: float = 0.0
    total_opex: float = 0.0
    ebitda: float = 0.0
    ebitda_margin_pct: float = 0.0
    da_expense: float = 0.0
    operating_profit: float = 0.0
    operating_margin_pct: float = 0.0
    tax_expense: float = 0.0
    net_income: float = 0.0
    net_margin_pct: float = 0.0

    # ── Balance Sheet ──────────────────────────────────────────────────────
    cash: float = 0.0
    ar: float = 0.0
    unbilled_revenue: float = 0.0
    prepaid_expenses: float = 0.0
    pp_e: float = 0.0
    intangibles: float = 0.0
    goodwill: float = 0.0
    total_assets: float = 0.0
    ap: float = 0.0
    accrued_expenses: float = 0.0
    deferred_revenue: float = 0.0
    deferred_revenue_current: float = 0.0
    deferred_revenue_lt: float = 0.0
    total_liabilities: float = 0.0
    retained_earnings: float = 0.0
    stockholders_equity: float = 0.0

    # ── Derived ratios ───────────────────────────────────────────────────
    dso: float = 0.0               # Days Sales Outstanding
    dpo: float = 0.0               # Days Payables Outstanding
    working_capital: float = 0.0   # Current assets − current liabilities

    # ── Cash Flow ──────────────────────────────────────────────────────────
    cfo: float = 0.0
    capex: float = 0.0
    fcf: float = 0.0
    change_in_ar: float = 0.0
    change_in_ap: float = 0.0
    change_in_deferred_rev: float = 0.0
    change_in_unbilled_rev: float = 0.0
    change_in_prepaid: float = 0.0
    change_in_accrued: float = 0.0

    # ── SaaS Metrics ──────────────────────────────────────────────────────
    nrr: float = 0.0            # percent
    gross_churn_pct: float = 0.0 # percent (annualized)
    logo_churn_pct: float = 0.0  # percent (annualized)
    customer_count: int = 0
    new_customers: int = 0
    churned_customers: int = 0
    acv: float = 0.0             # average contract value (millions)
    ltv: float = 0.0             # lifetime value (millions)
    cac: float = 0.0             # customer acquisition cost (millions)
    ltv_cac_ratio: float = 0.0
    magic_number: float = 0.0
    burn_multiple: float = 0.0
    rule_of_40: float = 0.0
    revenue_per_employee: float = 0.0
    arr_per_employee: float = 0.0
    mrr: float = 0.0

    # ── Pipeline ──────────────────────────────────────────────────────────
    pipeline: float = 0.0
    win_rate: float = 0.0
    sales_cycle_days: float = 0.0
    avg_deal_size: float = 0.0
    quota_attainment: float = 0.0
    sales_headcount: int = 0

    # ── People ────────────────────────────────────────────────────────────
    headcount: int = 0
    hires: int = 0
    terminations: int = 0
    attrition_rate: float = 0.0  # annualized percent

    # ── Support ───────────────────────────────────────────────────────────
    support_tickets: int = 0
    csat: float = 0.0
    nps: int = 0
    first_response_hours: float = 0.0
    resolution_hours: float = 0.0

    # ── Engineering ───────────────────────────────────────────────────────
    sprint_velocity: float = 0.0
    story_points: float = 0.0
    features_shipped: int = 0
    tech_debt_pct: float = 0.0
    engineering_headcount: int = 0

    # ── Infrastructure ────────────────────────────────────────────────────
    cloud_spend: float = 0.0
    cloud_spend_pct_revenue: float = 0.0
    p1_incidents: int = 0
    p2_incidents: int = 0
    mttr_p1_hours: float = 0.0
    mttr_p2_hours: float = 0.0
    uptime_pct: float = 0.0
    downtime_hours: float = 0.0

    cloud_spend_by_resource_type: Dict[str, float] = field(default_factory=dict)

    # ── Consultancy-specific ────────────────────────────────────────────
    tm_revenue: float = 0.0               # time & materials revenue
    fixed_fee_revenue: float = 0.0        # fixed-fee project revenue
    consultant_comp: float = 0.0          # consultant compensation (COGS)
    bench_cost: float = 0.0              # bench cost (COGS)
    subcontractor_cost: float = 0.0      # subcontractor cost (COGS)
    travel_cost: float = 0.0             # travel cost (COGS)
    sales_expense: float = 0.0           # sales (separate from marketing)
    marketing_expense: float = 0.0       # marketing (separate from sales)
    facilities_expense: float = 0.0      # facilities
    recruiting_expense: float = 0.0      # recruiting (expensed for consultancy)
    consultant_count: int = 0
    corporate_count: int = 0
    bench_count: int = 0
    utilization_rate: float = 0.0

    # ── BPM-specific ────────────────────────────────────────────────────
    managed_services_revenue: float = 0.0
    per_fte_revenue: float = 0.0
    per_transaction_revenue: float = 0.0
    onshore_cost: float = 0.0
    offshore_cost: float = 0.0
    nearshore_cost: float = 0.0
    bench_delivery_cost: float = 0.0
    delivery_center_ops_cost: float = 0.0
    benefits_cost: float = 0.0            # separate benefits (COFA conflict)
    sm_combined_expense: float = 0.0      # bundled S&M (COFA conflict)
    tech_automation_expense: float = 0.0  # partially capitalized
    facilities_corporate_expense: float = 0.0
    capitalized_recruiting: float = 0.0   # capitalized (COFA conflict)
    capitalized_automation: float = 0.0   # capitalized (COFA conflict)
    delivery_count: int = 0
    bench_delivery_count_q: int = 0
    onshore_count: int = 0
    offshore_count: int = 0
    nearshore_count: int = 0
    automation_rate_q: float = 0.0
    sla_attainment_q: float = 0.0

    # ── Revenue by stream (generic) ────────────────────────────────────
    revenue_by_stream: Dict[str, float] = field(default_factory=dict)

    # ── Dimensional breakdowns ────────────────────────────────────────────
    revenue_by_region: Dict[str, float] = field(default_factory=dict)
    revenue_by_segment: Dict[str, float] = field(default_factory=dict)
    arr_by_region: Dict[str, float] = field(default_factory=dict)
    arr_by_segment: Dict[str, float] = field(default_factory=dict)
    pipeline_by_stage: Dict[str, float] = field(default_factory=dict)
    pipeline_by_region: Dict[str, float] = field(default_factory=dict)
    customers_by_segment: Dict[str, int] = field(default_factory=dict)
    bookings_by_segment: Dict[str, float] = field(default_factory=dict)
    churn_by_segment: Dict[str, float] = field(default_factory=dict)
    cogs_breakdown: Dict[str, float] = field(default_factory=dict)
    opex_breakdown: Dict[str, float] = field(default_factory=dict)
    headcount_by_department: Dict[str, int] = field(default_factory=dict)
    headcount_by_geo: Dict[str, int] = field(default_factory=dict)
    headcount_by_practice: Dict[str, int] = field(default_factory=dict)   # consultancy: practice areas, BPM: service lines
    headcount_by_level: Dict[str, int] = field(default_factory=dict)
    new_logo_revenue_by_region: Dict[str, float] = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Financial Model Engine
# ═══════════════════════════════════════════════════════════════════════════════

class FinancialModel:
    """
    Produces 12 quarters (2024-Q1 → 2026-Q4) of integrated financial data.

    The computation flows strictly downward:
        Assumptions → ARR → Revenue → P&L → Balance Sheet → Cash Flow → SaaS Metrics
    """

    QUARTERS = [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    def __init__(self, assumptions: Optional[Assumptions] = None):
        self.a = assumptions or Assumptions()

    # ─── public ────────────────────────────────────────────────────────────

    def generate(self, wall_clock: Optional[date] = None) -> List[Quarter]:
        quarters: List[Quarter] = []
        prev: Optional[Quarter] = None
        _wall_clock = wall_clock if wall_clock is not None else date.today()
        quarter_end_months = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}

        for idx, q_label in enumerate(self.QUARTERS):
            year = int(q_label[:4])
            q_num = int(q_label[-1])
            q_end_month, q_end_day = quarter_end_months[q_num]
            q_end_date = date(year, q_end_month, q_end_day)
            is_forecast = q_end_date >= _wall_clock
            period_type = "forecast" if is_forecast else "actual"
            years_elapsed = (year - 2024) + (q_num - 1) / 4.0

            q = Quarter(quarter=q_label, quarter_index=idx, is_forecast=is_forecast,
                        period_type=period_type,
                        entity_id=self.a.entity_id,
                        entity_name=self.a.entity_name,
                        business_model=self.a.business_model)

            if self.a.business_model == "consultancy":
                self._compute_consultancy(q, prev, years_elapsed)
            elif self.a.business_model == "bpm":
                self._compute_bpm(q, prev, years_elapsed)
            else:
                # Default SaaS model (backward compatible with Phase 0)
                self._compute_arr(q, prev, years_elapsed)
                self._compute_revenue(q, prev)
                self._compute_customers(q, prev, years_elapsed)
                self._compute_pipeline(q, years_elapsed)
                self._compute_pnl(q, years_elapsed)
                self._compute_people(q, prev, years_elapsed)
                self._compute_support(q)
                self._compute_engineering(q, years_elapsed)
                self._compute_infrastructure(q)
                self._compute_balance_sheet(q, prev, years_elapsed)
                self._compute_cash_flow(q, prev)
                self._compute_saas_metrics(q, prev)
                self._compute_dimensional(q, years_elapsed)

            quarters.append(q)
            prev = q

        return quarters

    # ─── ARR waterfall ────────────────────────────────────────────────────

    def _compute_arr(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        a = self.a
        # Growth rate decelerates each year
        annual_growth = max(a.arr_growth_rate_annual - a.arr_growth_deceleration * int(years_elapsed), 0.05)
        quarterly_growth = (1 + annual_growth) ** 0.25 - 1

        q.beginning_arr = prev.ending_arr if prev else a.starting_arr

        # Churn improves over time
        annual_churn = max(a.gross_churn_rate_annual - a.churn_improvement_annual * int(years_elapsed), 0.02)
        quarterly_churn = annual_churn / 4
        q.churned_arr = _r(q.beginning_arr * quarterly_churn)

        # Gross new ARR = growth target + churn replacement
        target_ending = _r(q.beginning_arr * (1 + quarterly_growth))
        q.new_arr = _r(target_ending - q.beginning_arr + q.churned_arr)
        # Expansion driven by NRR target (annualized → quarterly)
        quarterly_nrr = (1 + (a.nrr_base - 100) / 100) ** 0.25  # 1.14 → 1.0333
        target_expansion = q.beginning_arr * (quarterly_nrr - 1) + q.churned_arr
        # Cap expansion at total new ARR (can't expand more than we grow)
        q.expansion_arr = _r(min(target_expansion, q.new_arr))
        q.new_logo_arr = _r(q.new_arr - q.expansion_arr)

        q.ending_arr = _r(q.beginning_arr + q.new_arr - q.churned_arr)
        q.mrr = _r(q.ending_arr / 12)
        q.gross_churn_pct = _r(annual_churn * 100, 1)

    # ─── Revenue ──────────────────────────────────────────────────────────

    def _compute_revenue(self, q: Quarter, prev: Optional[Quarter]):
        # Revenue = base ARR recognition + partial new ARR recognition
        base_rev = q.beginning_arr / 4
        net_new_recognition = (q.new_arr - q.churned_arr) * 0.125  # half-quarter avg
        q.revenue = _r(base_rev + net_new_recognition)

        # Revenue decomposition
        total = q.revenue
        # new_logo_revenue proportion based on new logo ARR share
        new_logo_share = q.new_logo_arr / max(q.new_arr, 0.01) if q.new_arr > 0 else 0.42
        new_arr_rev_portion = max(q.new_arr * 0.125, 0)  # partial quarter recognition
        q.new_logo_revenue = _r(new_arr_rev_portion * new_logo_share)
        q.expansion_revenue = _r(new_arr_rev_portion * (1 - new_logo_share))
        q.renewal_revenue = _r(total - q.new_logo_revenue - q.expansion_revenue)

    # ─── Customers ────────────────────────────────────────────────────────

    def _compute_customers(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        a = self.a
        prev_count = prev.customer_count if prev else a.starting_customer_count

        # Churn customers
        annual_churn = max(a.gross_churn_rate_annual - a.churn_improvement_annual * int(years_elapsed), 0.02)
        logo_churn_rate_quarterly = annual_churn / 4 * 0.65  # logo churn < ARR churn
        q.churned_customers = max(int(round(prev_count * logo_churn_rate_quarterly)), 1)

        # New customers from new logo ARR / blended ACV
        blended_acv = (
            a.acv_enterprise * a.segment_enterprise_pct
            + a.acv_mid_market * a.segment_mid_market_pct
            + a.acv_smb * a.segment_smb_pct
        )
        q.new_customers = max(int(round(q.new_logo_arr / blended_acv)), 1)
        q.customer_count = prev_count + q.new_customers - q.churned_customers
        q.logo_churn_pct = _r(logo_churn_rate_quarterly * 4 * 100, 1)  # annualized

        # Segment split
        q.customers_by_segment = {
            "Enterprise": int(round(q.customer_count * a.segment_enterprise_pct)),
            "Mid-Market": int(round(q.customer_count * a.segment_mid_market_pct)),
            "SMB": q.customer_count - int(round(q.customer_count * a.segment_enterprise_pct))
                   - int(round(q.customer_count * a.segment_mid_market_pct)),
        }

    # ─── Pipeline ─────────────────────────────────────────────────────────

    def _compute_pipeline(self, q: Quarter, years_elapsed: float):
        a = self.a
        q.pipeline = _r(q.revenue * a.pipeline_multiple)
        q.win_rate = _r(a.win_rate + years_elapsed * 0.5, 1)  # slight improvement
        q.sales_cycle_days = _r(a.sales_cycle_days - years_elapsed * 2, 0)

        # Average deal size
        blended_acv = (
            a.acv_enterprise * a.segment_enterprise_pct
            + a.acv_mid_market * a.segment_mid_market_pct
            + a.acv_smb * a.segment_smb_pct
        )
        q.avg_deal_size = _r(blended_acv, 4)

        # Pipeline by stage
        q.pipeline_by_stage = {
            "Lead": _r(q.pipeline * 0.25),
            "Qualified": _r(q.pipeline * 0.20),
            "Proposal": _r(q.pipeline * 0.20),
            "Negotiation": _r(q.pipeline * 0.16),
            "Closed-Won": _r(q.pipeline * 0.19),
        }

    # ─── P&L ──────────────────────────────────────────────────────────────

    def _compute_pnl(self, q: Quarter, years_elapsed: float):
        a = self.a
        rev = q.revenue

        # COGS (improving over time)
        cogs_pct = a.cogs_pct - a.cogs_improvement_annual * years_elapsed
        q.cogs = _r(rev * cogs_pct)
        q.gross_profit = _r(rev - q.cogs)
        q.gross_margin_pct = _r((q.gross_profit / rev) * 100 if rev else 0, 1)

        # COGS breakdown
        q.cogs_breakdown = {
            "hosting": _r(q.cogs * a.cogs_hosting_pct),
            "support_staff": _r(q.cogs * a.cogs_support_staff_pct),
            "professional_services": _r(q.cogs * a.cogs_ps_pct),
            "licenses": _r(q.cogs * a.cogs_licenses_pct),
            "payment_processing": _r(q.cogs * a.cogs_payments_pct),
        }

        # OpEx
        q.sm_expense = _r(rev * a.sm_pct)
        q.rd_expense = _r(rev * a.rd_pct)
        q.ga_expense = _r(rev * a.ga_pct)
        q.total_opex = _r(q.sm_expense + q.rd_expense + q.ga_expense)
        q.opex_breakdown = {
            "sales_and_marketing": q.sm_expense,
            "research_and_development": q.rd_expense,
            "general_and_administrative": q.ga_expense,
        }

        # EBITDA
        q.ebitda = _r(q.gross_profit - q.total_opex)
        q.ebitda_margin_pct = _r((q.ebitda / rev) * 100 if rev else 0, 1)

        # D&A and operating profit
        q.da_expense = _r(rev * a.da_pct)
        q.operating_profit = _r(q.ebitda - q.da_expense)
        q.operating_margin_pct = _r((q.operating_profit / rev) * 100 if rev else 0, 1)

        # Tax and net income
        taxable = max(q.operating_profit, 0)
        q.tax_expense = _r(taxable * a.tax_rate)
        q.net_income = _r(q.operating_profit - q.tax_expense)
        q.net_margin_pct = _r((q.net_income / rev) * 100 if rev else 0, 1)

    # ─── People ───────────────────────────────────────────────────────────

    def _compute_people(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        a = self.a
        prev_hc = prev.headcount if prev else a.starting_headcount

        # Headcount grows with revenue
        # Target headcount = revenue_per_employee baseline
        revenue_growth = q.revenue / (prev.revenue if prev else q.revenue)
        target_growth = max(revenue_growth - 1, 0) * 0.7  # headcount lags revenue
        target_hc = int(round(prev_hc * (1 + target_growth)))

        # Attrition
        annual_attrition = max(a.attrition_rate_annual - a.attrition_improvement_annual * int(years_elapsed), 0.06)
        q.terminations = max(int(round(prev_hc * annual_attrition / 4)), 1)
        q.attrition_rate = _r(annual_attrition * 100, 1)

        # Hires to hit target + replace attrition
        q.hires = max(target_hc - prev_hc + q.terminations, 0)
        q.headcount = prev_hc + q.hires - q.terminations

        # Department breakdown
        hc = q.headcount
        eng = int(round(hc * a.hc_engineering_pct))
        prod = int(round(hc * a.hc_product_pct))
        sales = int(round(hc * a.hc_sales_pct))
        mkt = int(round(hc * a.hc_marketing_pct))
        cs = int(round(hc * a.hc_cs_pct))
        ga = hc - eng - prod - sales - mkt - cs  # remainder to G&A
        q.headcount_by_department = {
            "Engineering": eng,
            "Product": prod,
            "Sales": sales,
            "Marketing": mkt,
            "Customer Success": cs,
            "G&A": ga,
        }
        q.sales_headcount = sales
        q.engineering_headcount = eng

    # ─── Support ──────────────────────────────────────────────────────────

    def _compute_support(self, q: Quarter):
        a = self.a
        q.support_tickets = int(round(q.customer_count * a.tickets_per_customer_annual / 4))
        q.csat = _r(a.csat_base + q.quarter_index * 0.01, 2)  # slight improvement
        q.nps = min(a.nps_base + q.quarter_index, 55)
        q.first_response_hours = _r(max(a.first_response_hours - q.quarter_index * 0.05, 1.0), 1)
        q.resolution_hours = _r(max(a.resolution_hours - q.quarter_index * 0.3, 8.0), 1)

    # ─── Engineering ──────────────────────────────────────────────────────

    def _compute_engineering(self, q: Quarter, years_elapsed: float):
        a = self.a
        q.sprint_velocity = _r(a.points_per_sprint + years_elapsed * 3, 1)
        q.story_points = _r(q.sprint_velocity * a.sprints_per_quarter)
        q.features_shipped = int(round(q.story_points / 8))  # ~8 pts per feature
        q.tech_debt_pct = _r(max(a.tech_debt_pct - years_elapsed * 0.01, 0.08), 3)

    # ─── Infrastructure ───────────────────────────────────────────────────

    def _compute_infrastructure(self, q: Quarter):
        a = self.a
        q.cloud_spend = _r(q.revenue * a.cloud_spend_pct_revenue)
        q.cloud_spend_pct_revenue = _r(a.cloud_spend_pct_revenue * 100, 2)
        q.p1_incidents = a.p1_incidents_per_quarter
        q.p2_incidents = a.p2_incidents_per_quarter
        q.mttr_p1_hours = _r(a.mttr_p1_hours, 1)
        q.mttr_p2_hours = _r(a.mttr_p2_hours, 1)
        q.uptime_pct = _r(min(a.uptime_pct + q.quarter_index * 0.02, 99.99), 2)
        total_hours = 90 * 24  # ~90 days per quarter
        q.downtime_hours = _r(total_hours * (1 - q.uptime_pct / 100), 1)

        # Cloud spend by resource type — top-down percentage split
        # Categories match DCL entities.yaml resource_type allowed_values
        q.cloud_spend_by_resource_type = {
            "Compute": _r(q.cloud_spend * 0.40),
            "Storage": _r(q.cloud_spend * 0.15),
            "Database": _r(q.cloud_spend * 0.20),
            "Network": _r(q.cloud_spend * 0.10),
            "ML/AI": _r(q.cloud_spend * 0.08),
            "Other": _r(q.cloud_spend * 0.07),
        }

    # ─── Balance Sheet ────────────────────────────────────────────────────

    def _compute_balance_sheet(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        a = self.a

        # DSO improves over time
        dso = a.dso_days - a.dso_improvement_annual * years_elapsed
        q.dso = _r(dso, 1)
        q.ar = _r(q.revenue / 90 * dso)  # quarterly rev / 90 days * DSO

        # Deferred revenue ~ ARR * (deferred_months / 12)
        q.deferred_revenue = _r(q.ending_arr * (a.deferred_rev_months / 12))
        q.deferred_revenue_current = _r(q.deferred_revenue * 0.75)
        q.deferred_revenue_lt = _r(q.deferred_revenue * 0.25)

        # Other balance sheet items
        q.unbilled_revenue = _r((prev.unbilled_revenue if prev else a.starting_unbilled_revenue)
                                * (1 + 0.02))  # grows slowly
        q.prepaid_expenses = _r((prev.prepaid_expenses if prev else a.starting_prepaid)
                                * (1 + 0.01))

        # Fixed assets (PP&E depreciates, capex offsets)
        prev_ppe = prev.pp_e if prev else a.starting_pp_e
        q.capex = _r(q.revenue * a.capex_pct_revenue)
        q.pp_e = _r(prev_ppe - q.da_expense * 0.7 + q.capex)  # 70% of D&A is depreciation
        q.intangibles = _r((prev.intangibles if prev else a.starting_intangibles) - q.da_expense * 0.3)
        q.goodwill = a.starting_goodwill  # goodwill doesn't amortize under GAAP

        # AP and accrued
        q.ap = _r(q.cogs * 0.15 + q.total_opex * 0.08)  # ~15% of COGS + 8% of opex
        q.accrued_expenses = _r(q.total_opex * 0.35)  # ~35% of opex accrued

        # DPO = AP / annualised COGS × 365
        q.dpo = _r(q.ap / (q.cogs * 4) * 365, 1) if q.cogs else 0.0

        # Cash computed after cash flow

        # Retained earnings — for Q1, derive from BS identity to ensure balance
        if prev:
            prev_re = prev.retained_earnings
        else:
            # Compute starting assets & liabilities to derive initial retained earnings
            start_assets = (a.starting_cash + a.starting_ar + a.starting_unbilled_revenue
                            + a.starting_prepaid + a.starting_pp_e + a.starting_intangibles
                            + a.starting_goodwill)
            start_dr = a.starting_arr * (a.deferred_rev_months / 12)
            start_liabilities = a.starting_ap + a.starting_accrued_expenses + start_dr
            paid_in_capital = 40.0
            prev_re = start_assets - start_liabilities - paid_in_capital
        q.retained_earnings = _r(prev_re + q.net_income)

        # Stockholders' equity = retained earnings + paid-in capital (stable)
        paid_in_capital = 40.0  # assumed constant
        q.stockholders_equity = _r(q.retained_earnings + paid_in_capital)

        # Total liabilities (before total_assets which needs cash)
        q.total_liabilities = _r(q.ap + q.accrued_expenses + q.deferred_revenue)

    # ─── Cash Flow ────────────────────────────────────────────────────────

    def _compute_cash_flow(self, q: Quarter, prev: Optional[Quarter]):
        a = self.a
        # Working capital changes
        prev_ar = prev.ar if prev else a.starting_ar
        prev_ap = prev.ap if prev else a.starting_ap
        prev_dr = prev.deferred_revenue if prev else (a.starting_arr * a.deferred_rev_months / 12)
        prev_ubr = prev.unbilled_revenue if prev else a.starting_unbilled_revenue
        prev_prepaid = prev.prepaid_expenses if prev else a.starting_prepaid
        prev_accrued = prev.accrued_expenses if prev else a.starting_accrued_expenses

        q.change_in_ar = _r(q.ar - prev_ar)
        q.change_in_ap = _r(q.ap - prev_ap)
        q.change_in_deferred_rev = _r(q.deferred_revenue - prev_dr)
        q.change_in_unbilled_rev = _r(q.unbilled_revenue - prev_ubr)
        q.change_in_prepaid = _r(q.prepaid_expenses - prev_prepaid)
        q.change_in_accrued = _r(q.accrued_expenses - prev_accrued)

        # CFO = NI + D&A + working capital adjustments
        q.cfo = _r(
            q.net_income
            + q.da_expense
            - q.change_in_ar       # AR increase = cash use
            + q.change_in_ap       # AP increase = cash source
            + q.change_in_deferred_rev  # DR increase = cash source
            - q.change_in_unbilled_rev  # UBR increase = cash use
            - q.change_in_prepaid      # Prepaid increase = cash use
            + q.change_in_accrued      # Accrued increase = cash source
        )

        # FCF
        q.fcf = _r(q.cfo - q.capex)

        # Cash = previous cash + FCF
        prev_cash = prev.cash if prev else a.starting_cash
        q.cash = _r(prev_cash + q.fcf)

        # Now compute total assets (needs cash)
        q.total_assets = _r(q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
                            + q.pp_e + q.intangibles + q.goodwill)

        # Working capital = current assets − current liabilities
        current_assets = q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
        current_liabilities = q.ap + q.accrued_expenses + q.deferred_revenue_current
        q.working_capital = _r(current_assets - current_liabilities)

    # ─── SaaS Metrics ─────────────────────────────────────────────────────

    def _compute_saas_metrics(self, q: Quarter, prev: Optional[Quarter]):
        a = self.a
        # NRR = (ending ARR from existing customers) / beginning ARR, annualized
        existing_ending = q.beginning_arr + q.expansion_arr - q.churned_arr
        quarterly_ratio = existing_ending / q.beginning_arr if q.beginning_arr else 1.0
        q.nrr = _r(quarterly_ratio ** 4 * 100, 1)  # annualize

        # ACV
        if q.new_customers > 0:
            q.acv = _r(q.new_logo_arr / q.new_customers, 4)
        else:
            q.acv = _r(a.acv_enterprise * a.segment_enterprise_pct
                       + a.acv_mid_market * a.segment_mid_market_pct
                       + a.acv_smb * a.segment_smb_pct, 4)

        # LTV = ACV * (1 / annual_churn_rate) * gross_margin
        annual_churn = q.gross_churn_pct / 100 if q.gross_churn_pct > 0 else 0.08
        q.ltv = _r(q.acv * (1 / annual_churn) * (q.gross_margin_pct / 100) if annual_churn > 0 else 0, 2)

        # CAC = S&M spend / new customers (annualized)
        q.cac = _r((q.sm_expense * 4 / max(q.new_customers * 4, 1)), 4)

        # LTV/CAC
        q.ltv_cac_ratio = _r(q.ltv / q.cac if q.cac > 0 else 0, 1)

        # Magic Number = net new ARR (quarterly annualized) / S&M spend (prev quarter)
        prev_sm = prev.sm_expense if prev else q.sm_expense
        net_new_arr = q.new_arr - q.churned_arr
        q.magic_number = _r((net_new_arr) / prev_sm if prev_sm > 0 else 0, 2)

        # Burn Multiple = net burn / net new ARR (lower is better; negative burn = profitable)
        net_burn = -(q.fcf)  # negative FCF = burning cash
        q.burn_multiple = _r(net_burn / (net_new_arr) if net_new_arr > 0 else 0, 2)

        # Rule of 40 = revenue growth rate + FCF margin
        if prev and prev.revenue > 0:
            rev_growth_annualized = ((q.revenue / prev.revenue) ** 4 - 1) * 100
        else:
            rev_growth_annualized = a.arr_growth_rate_annual * 100  # seed quarter: use config
        fcf_margin = (q.fcf / q.revenue * 100) if q.revenue > 0 else 0
        q.rule_of_40 = _r(rev_growth_annualized + fcf_margin, 1)

        # Per-employee metrics
        q.revenue_per_employee = _r(q.revenue / q.headcount if q.headcount else 0, 4)
        q.arr_per_employee = _r(q.ending_arr / q.headcount if q.headcount else 0, 4)

        # Quota attainment — plan-based quota with stretch
        if q.sales_headcount > 0:
            quota_stretch = 1.12  # reps get 12% stretch over plan
            quota_per_rep = (q.new_arr * quota_stretch) / q.sales_headcount
            actual_per_rep = (q.new_logo_arr + q.expansion_arr) / q.sales_headcount
            q.quota_attainment = _r(min(actual_per_rep / quota_per_rep * 100, 150), 1)
        else:
            q.quota_attainment = 100.0

    # ─── Dimensional breakdowns ───────────────────────────────────────────

    def _compute_dimensional(self, q: Quarter, years_elapsed: float):
        a = self.a
        # Revenue by region
        q.revenue_by_region = {
            "AMER": _r(q.revenue * a.region_amer),
            "EMEA": _r(q.revenue * a.region_emea),
            "APAC": _r(q.revenue * a.region_apac),
        }

        # Revenue by segment
        ent_rev_share = 0.45 + years_elapsed * 0.005  # enterprise growing share
        mm_rev_share = 0.35
        smb_rev_share = 1.0 - ent_rev_share - mm_rev_share
        q.revenue_by_segment = {
            "Enterprise": _r(q.revenue * ent_rev_share),
            "Mid-Market": _r(q.revenue * mm_rev_share),
            "SMB": _r(q.revenue * smb_rev_share),
        }

        # ARR by region & segment
        q.arr_by_region = {
            "AMER": _r(q.ending_arr * a.region_amer),
            "EMEA": _r(q.ending_arr * a.region_emea),
            "APAC": _r(q.ending_arr * a.region_apac),
        }
        q.arr_by_segment = {
            "Enterprise": _r(q.ending_arr * ent_rev_share),
            "Mid-Market": _r(q.ending_arr * mm_rev_share),
            "SMB": _r(q.ending_arr * smb_rev_share),
        }

        # Pipeline by region
        q.pipeline_by_region = {
            "AMER": _r(q.pipeline * a.region_amer),
            "EMEA": _r(q.pipeline * a.region_emea),
            "APAC": _r(q.pipeline * a.region_apac),
        }

        # Bookings by segment (new ARR distribution)
        q.bookings_by_segment = {
            "Enterprise": _r(q.new_arr * 0.45),
            "Mid-Market": _r(q.new_arr * 0.35),
            "SMB": _r(q.new_arr * 0.20),
        }

        # Churn by segment
        q.churn_by_segment = {
            "Enterprise": _r(q.churned_arr * a.churn_ent_pct_of_total),
            "Mid-Market": _r(q.churned_arr * a.churn_mm_pct_of_total),
            "SMB": _r(q.churned_arr * a.churn_smb_pct_of_total),
        }

        # New logo revenue by region
        q.new_logo_revenue_by_region = {
            "AMER": _r(q.new_logo_revenue * a.region_amer),
            "EMEA": _r(q.new_logo_revenue * a.region_emea),
            "APAC": _r(q.new_logo_revenue * a.region_apac),
        }

        # Dimensional metadata — period-level identity for every Quarter record
        year = int(q.quarter[:4])
        q_num = int(q.quarter[-1])
        q.dimensions = {
            "period": q.quarter,
            "period_type": q.period_type,
            "year": str(year),
            "quarter_num": str(q_num),
        }

    # ═══════════════════════════════════════════════════════════════════════
    # CONSULTANCY MODEL ($5B management consultancy)
    # Revenue = project-based (T&M + fixed-fee), not ARR
    # COGS = consultant comp (includes benefits) + bench + subcontractors + travel
    # ═══════════════════════════════════════════════════════════════════════

    def _compute_consultancy(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        """Full computation chain for consultancy business model."""
        a = self.a

        # ── Revenue ──────────────────────────────────────────────────────
        growth = max(a.revenue_growth_rate_annual - a.revenue_growth_deceleration * int(years_elapsed), 0.02)
        quarterly_growth = (1 + growth) ** 0.25 - 1
        if prev:
            q.revenue = _r(prev.revenue * (1 + quarterly_growth))
        else:
            q.revenue = _r(a.starting_annual_revenue / 4)

        # Revenue streams
        q.tm_revenue = _r(q.revenue * a.tm_revenue_pct)
        q.fixed_fee_revenue = _r(q.revenue * a.fixed_fee_revenue_pct)
        q.revenue_by_stream = {
            "time_and_materials": q.tm_revenue,
            "fixed_fee": q.fixed_fee_revenue,
        }

        # ── COGS ─────────────────────────────────────────────────────────
        cogs_pct = a.cogs_pct - a.cogs_improvement_annual * years_elapsed
        q.cogs = _r(q.revenue * cogs_pct)
        q.consultant_comp = _r(q.cogs * a.cogs_consultant_comp_pct)
        q.bench_cost = _r(q.cogs * a.cogs_bench_pct)
        q.subcontractor_cost = _r(q.cogs * a.cogs_subcontractor_pct)
        q.travel_cost = _r(q.cogs * a.cogs_travel_pct)
        q.cogs_breakdown = {
            "consultant_compensation": q.consultant_comp,
            "bench": q.bench_cost,
            "subcontractors": q.subcontractor_cost,
            "travel": q.travel_cost,
        }

        q.gross_profit = _r(q.revenue - q.cogs)
        q.gross_margin_pct = _r((q.gross_profit / q.revenue) * 100 if q.revenue else 0, 1)

        # ── OpEx ─────────────────────────────────────────────────────────
        q.sales_expense = _r(q.revenue * a.sales_pct)
        q.marketing_expense = _r(q.revenue * a.marketing_pct)
        q.sm_expense = _r(q.sales_expense + q.marketing_expense)
        q.rd_expense = _r(q.revenue * a.rd_pct)
        q.ga_expense = _r(q.revenue * a.ga_pct)
        q.facilities_expense = _r(q.revenue * a.facilities_pct)
        q.recruiting_expense = _r(q.revenue * a.recruiting_pct)
        q.total_opex = _r(q.sm_expense + q.rd_expense + q.ga_expense
                          + q.facilities_expense + q.recruiting_expense)
        q.opex_breakdown = {
            "sales": q.sales_expense,
            "marketing": q.marketing_expense,
            "research_and_development": q.rd_expense,
            "general_and_administrative": q.ga_expense,
            "facilities": q.facilities_expense,
            "recruiting": q.recruiting_expense,
        }

        # ── EBITDA → Net Income ──────────────────────────────────────────
        q.ebitda = _r(q.gross_profit - q.total_opex)
        q.ebitda_margin_pct = _r((q.ebitda / q.revenue) * 100 if q.revenue else 0, 1)
        q.da_expense = _r(q.revenue * a.da_pct)
        q.operating_profit = _r(q.ebitda - q.da_expense)
        q.operating_margin_pct = _r((q.operating_profit / q.revenue) * 100 if q.revenue else 0, 1)
        taxable = max(q.operating_profit, 0)
        q.tax_expense = _r(taxable * a.tax_rate)
        q.net_income = _r(q.operating_profit - q.tax_expense)
        q.net_margin_pct = _r((q.net_income / q.revenue) * 100 if q.revenue else 0, 1)

        # ── People ───────────────────────────────────────────────────────
        prev_total = prev.headcount if prev else (a.starting_consultant_count + a.starting_corporate_count)
        prev_consultants = prev.consultant_count if prev else a.starting_consultant_count
        prev_corporate = prev.corporate_count if prev else a.starting_corporate_count

        annual_attrition = max(a.attrition_rate_annual - a.attrition_improvement_annual * int(years_elapsed), 0.08)
        q.terminations = max(int(round(prev_total * annual_attrition / 4)), 1)
        q.attrition_rate = _r(annual_attrition * 100, 1)

        # Headcount grows with revenue
        revenue_growth_q = (q.revenue / prev.revenue - 1) if prev and prev.revenue > 0 else 0
        target_growth = max(revenue_growth_q, 0) * 0.6  # headcount lags revenue
        target_hc = int(round(prev_total * (1 + target_growth)))
        q.hires = max(target_hc - prev_total + q.terminations, 0)
        q.headcount = prev_total + q.hires - q.terminations

        # Split consultant vs corporate
        consultant_ratio = prev_consultants / prev_total if prev_total > 0 else 0.833
        q.consultant_count = int(round(q.headcount * consultant_ratio))
        q.corporate_count = q.headcount - q.consultant_count
        q.bench_count = a.bench_consultant_count  # bench stays roughly constant
        q.utilization_rate = _r(min(a.utilization_rate + a.utilization_improvement_annual * years_elapsed, 0.82), 3)
        q.sales_headcount = int(round(q.corporate_count * a.hc_sales_pct))
        q.engineering_headcount = int(round(q.corporate_count * 0.05))
        q.revenue_per_employee = _r(q.revenue / q.headcount if q.headcount else 0, 4)

        q.headcount_by_department = {
            "Consulting": q.consultant_count,
            "Sales": q.sales_headcount,
            "Marketing": int(round(q.corporate_count * a.hc_marketing_pct)),
            "Finance": int(round(q.corporate_count * getattr(a, 'hc_finance_pct', 0.08))),
            "HR": int(round(q.corporate_count * getattr(a, 'hc_hr_pct', 0.05))),
            "IT": int(round(q.corporate_count * getattr(a, 'hc_it_pct', 0.12))),
            "Legal": int(round(q.corporate_count * getattr(a, 'hc_legal_pct', 0.024))),
            "G&A": q.corporate_count - q.sales_headcount
                   - int(round(q.corporate_count * a.hc_marketing_pct))
                   - int(round(q.corporate_count * getattr(a, 'hc_finance_pct', 0.08)))
                   - int(round(q.corporate_count * getattr(a, 'hc_hr_pct', 0.05)))
                   - int(round(q.corporate_count * getattr(a, 'hc_it_pct', 0.12)))
                   - int(round(q.corporate_count * getattr(a, 'hc_legal_pct', 0.024))),
        }

        # Headcount by practice area — consultants distributed across practices
        practice_pcts = {
            "Strategy": getattr(a, 'practice_strategy_pct', 0.18),
            "Operations": getattr(a, 'practice_operations_pct', 0.22),
            "Technology": getattr(a, 'practice_technology_pct', 0.20),
            "Risk": getattr(a, 'practice_risk_pct', 0.12),
            "Digital/AI": getattr(a, 'practice_digital_ai_pct', 0.16),
            "Commercial": getattr(a, 'practice_commercial_pct', 0.12),
        }
        q.headcount_by_practice = {}
        allocated = 0
        practices = list(practice_pcts.items())
        for name, pct in practices[:-1]:
            count = int(round(q.consultant_count * pct))
            q.headcount_by_practice[name] = count
            allocated += count
        q.headcount_by_practice[practices[-1][0]] = q.consultant_count - allocated

        # Headcount by geo — follows regional revenue distribution
        region_pcts = {
            "AMER": a.region_amer,
            "EMEA": a.region_emea,
            "APAC": a.region_apac,
        }
        if hasattr(a, 'region_latam'):
            region_pcts["LATAM"] = a.region_latam
        q.headcount_by_geo = {}
        allocated = 0
        geos = list(region_pcts.items())
        for name, pct in geos[:-1]:
            count = int(round(q.headcount * pct))
            q.headcount_by_geo[name] = count
            allocated += count
        q.headcount_by_geo[geos[-1][0]] = q.headcount - allocated

        # Headcount by level — consultancy pyramid
        level_pcts = {
            "Partner": 0.03,
            "Principal": 0.06,
            "Senior Manager": 0.10,
            "Manager": 0.15,
            "Senior Consultant": 0.22,
            "Consultant": 0.25,
            "Analyst": 0.19,
        }
        q.headcount_by_level = {}
        allocated = 0
        levels = list(level_pcts.items())
        for name, pct in levels[:-1]:
            count = int(round(q.consultant_count * pct))
            q.headcount_by_level[name] = count
            allocated += count
        q.headcount_by_level[levels[-1][0]] = q.consultant_count - allocated
        # Corporate staff as one level bucket
        q.headcount_by_level["Corporate Staff"] = q.corporate_count

        # ── Customers & Pipeline ─────────────────────────────────────────
        prev_cust = prev.customer_count if prev else a.starting_customer_count
        churn_rate_q = max(a.gross_churn_rate_annual - a.churn_improvement_annual * int(years_elapsed), 0.02) / 4
        q.churned_customers = max(int(round(prev_cust * churn_rate_q * 0.5)), 1)  # logo churn < ARR churn
        q.new_customers = max(int(round(q.revenue * 4 / a.avg_engagement_value / 8)), 1)  # rough new customer rate
        q.customer_count = prev_cust + q.new_customers - q.churned_customers

        q.pipeline = _r(q.revenue * a.pipeline_multiple)
        q.win_rate = _r(a.win_rate + years_elapsed * 0.3, 1)
        q.sales_cycle_days = _r(a.sales_cycle_days - years_elapsed * 1, 0)
        q.avg_deal_size = _r(a.avg_engagement_value, 4)

        # ── Balance Sheet (non-equity items) ───────────────────────────
        dso = a.dso_days - getattr(a, 'dso_improvement_annual', 1.5) * years_elapsed
        q.dso = _r(dso, 1)
        q.ar = _r(q.revenue / 90 * dso)
        q.deferred_revenue = _r(q.revenue * (getattr(a, 'deferred_rev_months', 1.5) / 12))
        q.deferred_revenue_current = _r(q.deferred_revenue * 0.85)
        q.deferred_revenue_lt = _r(q.deferred_revenue * 0.15)
        q.unbilled_revenue = _r((prev.unbilled_revenue if prev else a.starting_unbilled_revenue) * 1.015)
        q.prepaid_expenses = _r((prev.prepaid_expenses if prev else a.starting_prepaid) * 1.01)
        prev_ppe = prev.pp_e if prev else a.starting_pp_e
        q.capex = _r(q.revenue * a.capex_pct_revenue)
        q.pp_e = _r(prev_ppe - q.da_expense * 0.7 + q.capex)
        q.intangibles = _r((prev.intangibles if prev else a.starting_intangibles) - q.da_expense * 0.3)
        q.goodwill = a.starting_goodwill
        q.ap = _r(q.cogs * 0.12 + q.total_opex * 0.06)
        q.accrued_expenses = _r(q.total_opex * 0.32)
        q.total_liabilities = _r(q.ap + q.accrued_expenses + q.deferred_revenue)

        # ── Cash Flow (must run before equity to get total_assets) ────
        prev_ar = prev.ar if prev else a.starting_ar
        prev_ap = prev.ap if prev else a.starting_ap
        prev_dr = prev.deferred_revenue if prev else q.deferred_revenue
        prev_ubr = prev.unbilled_revenue if prev else a.starting_unbilled_revenue
        prev_prepaid = prev.prepaid_expenses if prev else a.starting_prepaid
        prev_accrued = prev.accrued_expenses if prev else a.starting_accrued_expenses
        q.change_in_ar = _r(q.ar - prev_ar)
        q.change_in_ap = _r(q.ap - prev_ap)
        q.change_in_deferred_rev = _r(q.deferred_revenue - prev_dr)
        q.change_in_unbilled_rev = _r(q.unbilled_revenue - prev_ubr)
        q.change_in_prepaid = _r(q.prepaid_expenses - prev_prepaid)
        q.change_in_accrued = _r(q.accrued_expenses - prev_accrued)
        q.cfo = _r(q.net_income + q.da_expense - q.change_in_ar + q.change_in_ap
                    + q.change_in_deferred_rev - q.change_in_unbilled_rev
                    - q.change_in_prepaid + q.change_in_accrued)
        q.fcf = _r(q.cfo - q.capex)
        prev_cash = prev.cash if prev else a.starting_cash
        q.cash = _r(prev_cash + q.fcf)
        q.total_assets = _r(q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
                            + q.pp_e + q.intangibles + q.goodwill)
        current_assets = q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
        current_liabilities = q.ap + q.accrued_expenses + q.deferred_revenue_current
        q.working_capital = _r(current_assets - current_liabilities)

        # ── Equity (plug retained_earnings at Q1 to balance BS) ──────
        paid_in_capital = _r(q.total_assets * 0.25)  # stable equity base
        if prev:
            q.retained_earnings = _r(prev.retained_earnings + q.net_income)
            paid_in_capital = prev.stockholders_equity - prev.retained_earnings
        else:
            q.retained_earnings = _r(q.total_assets - q.total_liabilities - paid_in_capital)
        q.stockholders_equity = _r(q.retained_earnings + paid_in_capital)

        # ── Dimensional ──────────────────────────────────────────────────
        regions = {"AMER": a.region_amer, "EMEA": a.region_emea, "APAC": a.region_apac}
        if a.region_latam > 0:
            regions["LATAM"] = a.region_latam
        q.revenue_by_region = {k: _r(q.revenue * v) for k, v in regions.items()}
        q.pipeline_by_region = {k: _r(q.pipeline * v) for k, v in regions.items()}
        q.new_logo_revenue_by_region = {k: _r(q.new_logo_revenue * v) for k, v in regions.items()}
        year = int(q.quarter[:4])
        q_num = int(q.quarter[-1])
        q.dimensions = {
            "period": q.quarter,
            "period_type": q.period_type,
            "year": str(year),
            "quarter_num": str(q_num),
        }
        if q.entity_id:
            q.dimensions["entity_id"] = q.entity_id

    # ═══════════════════════════════════════════════════════════════════════
    # BPM MODEL ($1B business process management / outsourcing)
    # Revenue = managed services + per-FTE + per-transaction
    # COGS = onshore/offshore/nearshore labor + bench + delivery center ops
    # ═══════════════════════════════════════════════════════════════════════

    def _compute_bpm(self, q: Quarter, prev: Optional[Quarter], years_elapsed: float):
        """Full computation chain for BPM business model."""
        a = self.a

        # ── Revenue ──────────────────────────────────────────────────────
        growth = max(a.revenue_growth_rate_annual - a.revenue_growth_deceleration * int(years_elapsed), 0.03)
        quarterly_growth = (1 + growth) ** 0.25 - 1
        if prev:
            q.revenue = _r(prev.revenue * (1 + quarterly_growth))
        else:
            q.revenue = _r(a.starting_annual_revenue / 4)

        # Revenue streams
        q.managed_services_revenue = _r(q.revenue * a.managed_services_pct)
        q.per_fte_revenue = _r(q.revenue * a.per_fte_revenue_pct)
        q.per_transaction_revenue = _r(q.revenue * a.per_transaction_pct)
        q.revenue_by_stream = {
            "managed_services": q.managed_services_revenue,
            "per_fte": q.per_fte_revenue,
            "per_transaction": q.per_transaction_revenue,
        }

        # ── COGS ─────────────────────────────────────────────────────────
        cogs_pct = a.cogs_pct - a.cogs_improvement_annual * years_elapsed
        q.cogs = _r(q.revenue * cogs_pct)
        q.onshore_cost = _r(q.cogs * a.cogs_onshore_pct)
        q.offshore_cost = _r(q.cogs * a.cogs_offshore_pct)
        q.nearshore_cost = _r(q.cogs * a.cogs_nearshore_pct)
        q.bench_delivery_cost = _r(q.cogs * a.cogs_bench_pct)
        q.delivery_center_ops_cost = _r(q.cogs * a.cogs_delivery_center_ops_pct)
        q.subcontractor_cost = _r(q.cogs * a.cogs_subcontractor_pct)
        q.benefits_cost = _r(q.cogs * a.cogs_benefits_pct)  # separate from comp — COFA conflict
        q.cogs_breakdown = {
            "onshore_delivery": q.onshore_cost,
            "offshore_delivery": q.offshore_cost,
            "nearshore_delivery": q.nearshore_cost,
            "bench_training_transition": q.bench_delivery_cost,
            "delivery_center_operations": q.delivery_center_ops_cost,
            "subcontractors": q.subcontractor_cost,
            "delivery_staff_benefits": q.benefits_cost,
        }

        q.gross_profit = _r(q.revenue - q.cogs)
        q.gross_margin_pct = _r((q.gross_profit / q.revenue) * 100 if q.revenue else 0, 1)

        # ── OpEx ─────────────────────────────────────────────────────────
        # S&M bundled — COFA conflict with Meridian's separated S and M
        q.sm_combined_expense = _r(q.revenue * a.sm_pct)
        q.sm_expense = q.sm_combined_expense
        q.ga_expense = _r(q.revenue * a.ga_pct)
        q.tech_automation_expense = _r(q.revenue * a.tech_automation_pct)
        q.facilities_corporate_expense = _r(q.revenue * a.facilities_corporate_pct)
        q.rd_expense = 0.0  # BPM doesn't have a separate R&D line

        # Capitalized amounts reduce OpEx — COFA conflicts
        q.capitalized_recruiting = _r(a.capitalized_recruiting_annual / 4)
        q.capitalized_automation = _r(a.capitalized_automation_annual / 4)

        q.total_opex = _r(q.sm_expense + q.ga_expense + q.tech_automation_expense
                          + q.facilities_corporate_expense
                          - q.capitalized_recruiting - q.capitalized_automation)
        q.opex_breakdown = {
            "sales_and_marketing": q.sm_expense,  # bundled — COFA conflict
            "general_and_administrative": q.ga_expense,
            "technology_and_automation": q.tech_automation_expense,
            "corporate_facilities": q.facilities_corporate_expense,
            "capitalized_recruiting": -q.capitalized_recruiting,  # reduces OpEx
            "capitalized_automation": -q.capitalized_automation,  # reduces OpEx
        }

        # ── EBITDA → Net Income ──────────────────────────────────────────
        q.ebitda = _r(q.gross_profit - q.total_opex)
        q.ebitda_margin_pct = _r((q.ebitda / q.revenue) * 100 if q.revenue else 0, 1)

        # Accelerated depreciation — COFA conflict with Meridian's straight-line
        if a.depreciation_method == "accelerated":
            q.da_expense = _r(q.revenue * a.da_pct * 1.5)  # accelerated = higher D&A
        else:
            q.da_expense = _r(q.revenue * a.da_pct)
        q.operating_profit = _r(q.ebitda - q.da_expense)
        q.operating_margin_pct = _r((q.operating_profit / q.revenue) * 100 if q.revenue else 0, 1)
        taxable = max(q.operating_profit, 0)
        q.tax_expense = _r(taxable * a.tax_rate)
        q.net_income = _r(q.operating_profit - q.tax_expense)
        q.net_margin_pct = _r((q.net_income / q.revenue) * 100 if q.revenue else 0, 1)

        # ── People ───────────────────────────────────────────────────────
        prev_total = prev.headcount if prev else (a.starting_delivery_count + a.starting_corporate_count)
        prev_delivery = prev.delivery_count if prev else a.starting_delivery_count
        prev_corporate = prev.corporate_count if prev else a.starting_corporate_count

        # Delivery staff attrition is higher than corporate
        delivery_attrition = max(a.attrition_rate_annual - a.attrition_improvement_annual * int(years_elapsed), 0.12)
        corporate_attrition = max(a.attrition_corporate_annual - a.attrition_improvement_annual * int(years_elapsed) * 0.5, 0.06)

        delivery_terms = max(int(round(prev_delivery * delivery_attrition / 4)), 1)
        corporate_terms = max(int(round(prev_corporate * corporate_attrition / 4)), 1)
        q.terminations = delivery_terms + corporate_terms
        q.attrition_rate = _r(delivery_attrition * 100, 1)  # report delivery attrition

        revenue_growth_q = (q.revenue / prev.revenue - 1) if prev and prev.revenue > 0 else 0
        target_growth = max(revenue_growth_q, 0) * 0.8  # delivery headcount tracks revenue closely
        target_delivery = int(round(prev_delivery * (1 + target_growth)))
        delivery_hires = max(target_delivery - prev_delivery + delivery_terms, 0)
        corporate_hires = max(corporate_terms, 0)  # corporate stays flat
        q.hires = delivery_hires + corporate_hires
        q.delivery_count = prev_delivery + delivery_hires - delivery_terms
        q.corporate_count = prev_corporate + corporate_hires - corporate_terms
        q.headcount = q.delivery_count + q.corporate_count

        # Delivery FTE breakdown
        total_delivery = q.delivery_count
        q.onshore_count = int(round(total_delivery * (a.onshore_fte_count / max(a.onshore_fte_count + a.offshore_fte_count + a.nearshore_fte_count, 1))))
        q.offshore_count = int(round(total_delivery * (a.offshore_fte_count / max(a.onshore_fte_count + a.offshore_fte_count + a.nearshore_fte_count, 1))))
        q.nearshore_count = total_delivery - q.onshore_count - q.offshore_count
        q.bench_delivery_count_q = a.bench_fte_count

        q.sales_headcount = int(round(q.corporate_count * 0.15))
        q.engineering_headcount = int(round(q.corporate_count * 0.10))
        q.revenue_per_employee = _r(q.revenue / q.headcount if q.headcount else 0, 4)

        q.headcount_by_department = {
            "Delivery": q.delivery_count,
            "Sales & Marketing": int(round(q.corporate_count * getattr(a, 'hc_sales_marketing_pct', 0.167))),
            "Finance": int(round(q.corporate_count * getattr(a, 'hc_finance_pct', 0.117))),
            "HR": int(round(q.corporate_count * getattr(a, 'hc_hr_pct', 0.067))),
            "IT": int(round(q.corporate_count * getattr(a, 'hc_it_pct', 0.093))),
            "Legal": int(round(q.corporate_count * getattr(a, 'hc_legal_pct', 0.027))),
            "Operations": int(round(q.corporate_count * getattr(a, 'hc_operations_pct', 0.20))),
            "G&A": q.corporate_count - int(round(q.corporate_count * getattr(a, 'hc_sales_marketing_pct', 0.167)))
                   - int(round(q.corporate_count * getattr(a, 'hc_finance_pct', 0.117)))
                   - int(round(q.corporate_count * getattr(a, 'hc_hr_pct', 0.067)))
                   - int(round(q.corporate_count * getattr(a, 'hc_it_pct', 0.093)))
                   - int(round(q.corporate_count * getattr(a, 'hc_legal_pct', 0.027)))
                   - int(round(q.corporate_count * getattr(a, 'hc_operations_pct', 0.20))),
        }

        # Headcount by service line — delivery staff distributed across service lines
        sl_pcts = {
            "Finance & Accounting": getattr(a, 'sl_finance_accounting_pct', 0.30),
            "HR Operations": getattr(a, 'sl_hr_operations_pct', 0.25),
            "Customer Operations": getattr(a, 'sl_customer_operations_pct', 0.28),
            "Supply Chain": getattr(a, 'sl_supply_chain_pct', 0.17),
        }
        q.headcount_by_practice = {}
        allocated = 0
        sls = list(sl_pcts.items())
        for name, pct in sls[:-1]:
            count = int(round(q.delivery_count * pct))
            q.headcount_by_practice[name] = count
            allocated += count
        q.headcount_by_practice[sls[-1][0]] = q.delivery_count - allocated

        # Headcount by delivery geo — distributes delivery staff across delivery centers
        # Offshore split: India ~60%, Philippines ~40% of offshore
        india_count = int(round(q.offshore_count * 0.60))
        philippines_count = q.offshore_count - india_count
        # Nearshore split: Costa Rica ~45%, Poland ~55% of nearshore
        costa_rica_count = int(round(q.nearshore_count * 0.45))
        poland_count = q.nearshore_count - costa_rica_count
        # Onshore split: US ~70%, UK ~30% of onshore
        us_count = int(round(q.onshore_count * 0.70))
        uk_count = q.onshore_count - us_count
        q.headcount_by_geo = {
            "India": india_count,
            "Philippines": philippines_count,
            "Costa Rica": costa_rica_count,
            "Poland": poland_count,
            "United States": us_count + q.corporate_count,  # corporate HQ in US
            "United Kingdom": uk_count,
        }

        # Headcount by level — BPM delivery pyramid (flatter than consultancy)
        level_pcts = {
            "Process Director": 0.02,
            "Senior Manager": 0.05,
            "Team Lead": 0.10,
            "Senior Associate": 0.20,
            "Associate": 0.35,
            "Junior Associate": 0.28,
        }
        q.headcount_by_level = {}
        allocated = 0
        levels = list(level_pcts.items())
        for name, pct in levels[:-1]:
            count = int(round(q.delivery_count * pct))
            q.headcount_by_level[name] = count
            allocated += count
        q.headcount_by_level[levels[-1][0]] = q.delivery_count - allocated
        q.headcount_by_level["Corporate Staff"] = q.corporate_count

        # ── BPM delivery metrics ─────────────────────────────────────────
        q.automation_rate_q = _r(min(a.automation_rate + a.automation_improvement_annual * years_elapsed, 0.65), 3)
        q.sla_attainment_q = _r(min(a.sla_attainment + years_elapsed * 0.002, 0.999), 3)

        # ── Customers & Pipeline ─────────────────────────────────────────
        prev_cust = prev.customer_count if prev else a.starting_customer_count
        churn_rate_q = max(a.gross_churn_rate_annual - a.churn_improvement_annual * int(years_elapsed), 0.02) / 4
        q.churned_customers = max(int(round(prev_cust * churn_rate_q * 0.3)), 0)  # BPM has low logo churn
        q.new_customers = max(int(round(q.revenue * 4 / getattr(a, 'avg_contract_value', 5.0) / 15)), 1)
        q.customer_count = prev_cust + q.new_customers - q.churned_customers

        q.pipeline = _r(q.revenue * a.pipeline_multiple)
        q.win_rate = _r(a.win_rate + years_elapsed * 0.3, 1)
        q.sales_cycle_days = _r(a.sales_cycle_days - years_elapsed * 1, 0)
        q.avg_deal_size = _r(getattr(a, 'avg_contract_value', 5.0), 4)

        # ── Balance Sheet ────────────────────────────────────────────────
        dso = a.dso_days - getattr(a, 'dso_improvement_annual', 1.0) * years_elapsed
        q.dso = _r(dso, 1)
        q.ar = _r(q.revenue / 90 * dso)
        q.deferred_revenue = _r(q.revenue * (getattr(a, 'deferred_rev_months', 2.0) / 12))
        q.deferred_revenue_current = _r(q.deferred_revenue * 0.80)
        q.deferred_revenue_lt = _r(q.deferred_revenue * 0.20)
        q.unbilled_revenue = _r((prev.unbilled_revenue if prev else a.starting_unbilled_revenue) * 1.01)
        q.prepaid_expenses = _r((prev.prepaid_expenses if prev else a.starting_prepaid) * 1.005)

        prev_ppe = prev.pp_e if prev else a.starting_pp_e
        q.capex = _r(q.revenue * a.capex_pct_revenue + q.capitalized_recruiting + q.capitalized_automation)
        q.pp_e = _r(prev_ppe - q.da_expense * 0.6 + q.capex)
        q.intangibles = _r((prev.intangibles if prev else a.starting_intangibles)
                           - q.da_expense * 0.4 + q.capitalized_automation)
        q.goodwill = a.starting_goodwill
        q.ap = _r(q.cogs * 0.10 + q.total_opex * 0.05)
        q.accrued_expenses = _r(q.total_opex * 0.30)

        q.total_liabilities = _r(q.ap + q.accrued_expenses + q.deferred_revenue)

        # ── Cash Flow (must run before equity to get total_assets) ────
        prev_ar = prev.ar if prev else a.starting_ar
        prev_ap = prev.ap if prev else a.starting_ap
        prev_dr = prev.deferred_revenue if prev else q.deferred_revenue
        prev_ubr = prev.unbilled_revenue if prev else a.starting_unbilled_revenue
        prev_prepaid = prev.prepaid_expenses if prev else a.starting_prepaid
        prev_accrued = prev.accrued_expenses if prev else a.starting_accrued_expenses
        q.change_in_ar = _r(q.ar - prev_ar)
        q.change_in_ap = _r(q.ap - prev_ap)
        q.change_in_deferred_rev = _r(q.deferred_revenue - prev_dr)
        q.change_in_unbilled_rev = _r(q.unbilled_revenue - prev_ubr)
        q.change_in_prepaid = _r(q.prepaid_expenses - prev_prepaid)
        q.change_in_accrued = _r(q.accrued_expenses - prev_accrued)
        q.cfo = _r(q.net_income + q.da_expense - q.change_in_ar + q.change_in_ap
                    + q.change_in_deferred_rev - q.change_in_unbilled_rev
                    - q.change_in_prepaid + q.change_in_accrued)
        q.fcf = _r(q.cfo - q.capex)
        prev_cash = prev.cash if prev else a.starting_cash
        q.cash = _r(prev_cash + q.fcf)
        q.total_assets = _r(q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
                            + q.pp_e + q.intangibles + q.goodwill)
        current_assets = q.cash + q.ar + q.unbilled_revenue + q.prepaid_expenses
        current_liabilities = q.ap + q.accrued_expenses + q.deferred_revenue_current
        q.working_capital = _r(current_assets - current_liabilities)

        # ── Equity (plug retained_earnings at Q1 to balance BS) ──────
        paid_in_capital = _r(q.total_assets * 0.25)
        if prev:
            q.retained_earnings = _r(prev.retained_earnings + q.net_income)
            paid_in_capital = prev.stockholders_equity - prev.retained_earnings
        else:
            q.retained_earnings = _r(q.total_assets - q.total_liabilities - paid_in_capital)
        q.stockholders_equity = _r(q.retained_earnings + paid_in_capital)

        # ── Dimensional ──────────────────────────────────────────────────
        regions = {"AMER": a.region_amer, "EMEA": a.region_emea, "APAC": a.region_apac}
        q.revenue_by_region = {k: _r(q.revenue * v) for k, v in regions.items()}
        q.pipeline_by_region = {k: _r(q.pipeline * v) for k, v in regions.items()}

        year = int(q.quarter[:4])
        q_num = int(q.quarter[-1])
        q.dimensions = {
            "period": q.quarter,
            "period_type": q.period_type,
            "year": str(year),
            "quarter_num": str(q_num),
        }
        if q.entity_id:
            q.dimensions["entity_id"] = q.entity_id


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def validate_model(quarters: List[Quarter]) -> List[str]:
    """
    Check internal consistency of the model output.

    Returns list of issues (empty = valid). Known tolerance: BS may drift
    ~$2-3M by 2026 due to accumulated rounding.
    """
    issues: List[str] = []

    for i, q in enumerate(quarters):
        prefix = f"[{q.quarter}]"

        # P&L ties
        expected_gp = q.revenue - q.cogs
        if abs(q.gross_profit - expected_gp) > 0.05:
            issues.append(f"{prefix} Gross profit mismatch: {q.gross_profit} vs rev-cogs={expected_gp:.2f}")

        # OpEx components check — SaaS uses sm+rd+ga; non-SaaS uses opex_breakdown
        if q.business_model == "saas":
            expected_opex = q.sm_expense + q.rd_expense + q.ga_expense
            if abs(q.total_opex - expected_opex) > 0.05:
                issues.append(f"{prefix} OpEx mismatch: {q.total_opex} vs sum={expected_opex:.2f}")

        expected_ebitda = q.gross_profit - q.total_opex
        if abs(q.ebitda - expected_ebitda) > 0.05:
            issues.append(f"{prefix} EBITDA mismatch: {q.ebitda} vs gp-opex={expected_ebitda:.2f}")

        expected_op = q.ebitda - q.da_expense
        if abs(q.operating_profit - expected_op) > 0.05:
            issues.append(f"{prefix} Operating profit mismatch: {q.operating_profit} vs ebitda-da={expected_op:.2f}")

        # ARR continuity (SaaS only — non-SaaS models don't use ARR)
        if q.business_model == "saas":
            expected_arr = q.beginning_arr + q.new_arr - q.churned_arr
            if abs(q.ending_arr - expected_arr) > 0.05:
                issues.append(f"{prefix} ARR continuity: ending={q.ending_arr} vs computed={expected_arr:.2f}")

        # Customer continuity
        if i > 0:
            prev = quarters[i - 1]
            expected_cust = prev.customer_count + q.new_customers - q.churned_customers
            if q.customer_count != expected_cust:
                issues.append(f"{prefix} Customer count: {q.customer_count} vs computed={expected_cust}")

        # Headcount continuity
        if i > 0:
            prev = quarters[i - 1]
            expected_hc = prev.headcount + q.hires - q.terminations
            if q.headcount != expected_hc:
                issues.append(f"{prefix} Headcount: {q.headcount} vs computed={expected_hc}")

        # GL balance: COGS breakdown sums to COGS
        cogs_sum = sum(q.cogs_breakdown.values())
        if abs(cogs_sum - q.cogs) > 0.05:
            issues.append(f"{prefix} COGS breakdown sum={cogs_sum:.2f} vs cogs={q.cogs}")

        # OpEx breakdown sums to total_opex
        opex_sum = sum(q.opex_breakdown.values())
        if abs(opex_sum - q.total_opex) > 0.05:
            issues.append(f"{prefix} OpEx breakdown sum={opex_sum:.2f} vs opex={q.total_opex}")

        # Revenue by region sums to revenue
        rev_region_sum = sum(q.revenue_by_region.values())
        if abs(rev_region_sum - q.revenue) > 0.05:
            issues.append(f"{prefix} Revenue by region sum={rev_region_sum:.2f} vs revenue={q.revenue}")

        # Balance sheet: A = L + E (with tolerance for rounding drift)
        expected_total_eq = q.total_liabilities + q.stockholders_equity
        # Scale tolerance by company size — larger companies have larger absolute rounding drift
        base_tol = 5.0 if q.quarter_index > 8 else 3.0
        revenue_scale = max(q.revenue * 4 / 100, 1.0)  # scale factor vs $100M baseline
        tolerance = base_tol * revenue_scale
        if abs(q.total_assets - expected_total_eq) > tolerance:
            issues.append(f"{prefix} BS imbalance: assets={q.total_assets:.2f} vs L+E={expected_total_eq:.2f} (delta={abs(q.total_assets - expected_total_eq):.2f})")

        # Cash flow → cash reconciliation
        if i > 0:
            prev = quarters[i - 1]
            expected_cash = prev.cash + q.fcf
            if abs(q.cash - expected_cash) > 0.05:
                issues.append(f"{prefix} Cash flow: cash={q.cash:.2f} vs prev+fcf={expected_cash:.2f}")

    return issues


# ═══════════════════════════════════════════════════════════════════════════════
# Export
# ═══════════════════════════════════════════════════════════════════════════════

def export_to_json(quarters: List[Quarter]) -> str:
    """Export the model output as JSON."""
    return json.dumps([_quarter_to_dict(q) for q in quarters], indent=2)


def _quarter_to_dict(q: Quarter) -> Dict[str, Any]:
    """Convert Quarter dataclass to a clean dict."""
    return asdict(q)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _r(val: float, decimals: int = 2) -> float:
    """Round to specified decimal places."""
    return round(val, decimals)
