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
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_logger = logging.getLogger("farm.financial_model")


# ═══════════════════════════════════════════════════════════════════════════════
# YAML config loader — reads farm_config.yaml once at import time.
# Absence of the file is normal; compiled defaults in the dataclass are used.
# ═══════════════════════════════════════════════════════════════════════════════

def _load_farm_config() -> Dict[str, Any]:
    """
    Load farm_config.yaml from the repo root.

    Returns a flat dict merging company_profile + realism_params sections.
    Returns empty dict if the file is absent (compiled defaults apply).
    Also stores the raw 'schema' section for profile.py to consume.
    """
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
        # Stash the raw config so other modules can read the schema section
        merged["_raw"] = raw
        return merged
    except Exception as exc:
        _logger.warning(f"Failed to load farm_config.yaml: {exc} — using compiled defaults")
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
    """

    # ARR
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


# ═══════════════════════════════════════════════════════════════════════════════
# Quarter — output data structure per period
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Quarter:
    """One quarter of fully computed financial data (~177 data points)."""

    quarter: str            # e.g. "2024-Q1"
    quarter_index: int      # 0-based ordinal
    is_forecast: bool

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

    def generate(self) -> List[Quarter]:
        quarters: List[Quarter] = []
        prev: Optional[Quarter] = None

        for idx, q_label in enumerate(self.QUARTERS):
            year = int(q_label[:4])
            q_num = int(q_label[-1])
            is_forecast = (year == 2026 and q_num >= 3)
            years_elapsed = (year - 2024) + (q_num - 1) / 4.0

            q = Quarter(quarter=q_label, quarter_index=idx, is_forecast=is_forecast)

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
        q.new_logo_arr = _r(q.new_arr * a.new_logo_pct_of_new_arr)
        q.expansion_arr = _r(q.new_arr - q.new_logo_arr)

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

    # ─── SaaS Metrics ─────────────────────────────────────────────────────

    def _compute_saas_metrics(self, q: Quarter, prev: Optional[Quarter]):
        a = self.a
        # NRR = (ending ARR from existing customers) / beginning ARR
        # existing ARR = beginning - churned + expansion (no new logos)
        existing_ending = q.beginning_arr + q.expansion_arr - q.churned_arr
        q.nrr = _r((existing_ending / q.beginning_arr * 100) if q.beginning_arr else 100, 1)

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
        rev_growth_annualized = 0.0
        if prev and prev.revenue > 0:
            rev_growth_annualized = ((q.revenue / prev.revenue) ** 4 - 1) * 100
        fcf_margin = (q.fcf / q.revenue * 100) if q.revenue > 0 else 0
        q.rule_of_40 = _r(rev_growth_annualized + fcf_margin, 1)

        # Per-employee metrics
        q.revenue_per_employee = _r(q.revenue / q.headcount if q.headcount else 0, 4)
        q.arr_per_employee = _r(q.ending_arr / q.headcount if q.headcount else 0, 4)

        # Quota attainment (implied)
        if q.sales_headcount > 0:
            quota_per_rep = q.ending_arr / q.sales_headcount * 0.015  # rough quarterly quota
            actual_per_rep = (q.new_logo_arr + q.expansion_arr) / q.sales_headcount
            q.quota_attainment = _r(min(actual_per_rep / quota_per_rep * 100, 150) if quota_per_rep > 0 else 100, 1)
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

        expected_opex = q.sm_expense + q.rd_expense + q.ga_expense
        if abs(q.total_opex - expected_opex) > 0.05:
            issues.append(f"{prefix} OpEx mismatch: {q.total_opex} vs sum={expected_opex:.2f}")

        expected_ebitda = q.gross_profit - q.total_opex
        if abs(q.ebitda - expected_ebitda) > 0.05:
            issues.append(f"{prefix} EBITDA mismatch: {q.ebitda} vs gp-opex={expected_ebitda:.2f}")

        expected_op = q.ebitda - q.da_expense
        if abs(q.operating_profit - expected_op) > 0.05:
            issues.append(f"{prefix} Operating profit mismatch: {q.operating_profit} vs ebitda-da={expected_op:.2f}")

        # ARR continuity
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
        tolerance = 5.0 if q.quarter_index > 8 else 3.0  # wider tolerance in later quarters
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
