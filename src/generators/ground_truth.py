"""
Ground truth manifest generator (v2.0).

After all source system data is generated, calculates the ground truth manifest
by aggregating across systems using primary source designations. This manifest
is the test oracle — it declares what correct aggregated answers should be.

v2.0 adds: full P&L, balance sheet, cash flow, SaaS metrics, ARR waterfall,
revenue decomposition, and 13 dimensional breakdowns from the financial model.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.generators.business_data.profile import (
    BusinessProfile,
    QuarterMetrics,
    REGION_WEIGHTS,
)


def _r(val: float, decimals: int = 2) -> float:
    """Round a value."""
    return round(val, decimals)


def compute_ground_truth(
    profile: BusinessProfile,
    run_id: str,
    generated_data: Dict[str, Dict[str, Any]],
    model_quarters: Optional[List] = None,
) -> Dict[str, Any]:
    """
    Compute the ground truth manifest from generated data and the business profile.

    When model_quarters (financial model Quarter objects) are provided, produces a
    v2.0 manifest with ~131 metrics per quarter. Otherwise falls back to v1.0.

    Args:
        profile: The business trajectory that generated data derives from.
        run_id: The generation run identifier.
        generated_data: Dict keyed by source_system containing generated payloads.
        model_quarters: Optional list of financial model Quarter objects for v2.0.

    Returns:
        Complete ground truth manifest dict.
    """
    source_systems = list(generated_data.keys())
    generated_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build per-quarter ground truth
    if model_quarters:
        quarterly_truth = _build_v2_quarterly_truth(model_quarters)
        dimensional_truth = _build_v2_dimensional_truth(model_quarters)
        expected_conflicts = _build_v2_expected_conflicts(model_quarters)
        manifest_version = "2.0"
    else:
        quarterly_truth = _build_v1_quarterly_truth(profile)
        dimensional_truth = _build_v1_dimensional_truth(profile)
        expected_conflicts = _build_v1_expected_conflicts(profile)
        manifest_version = "1.0"

    # Compute actual record counts from generated data
    record_counts = _compute_record_counts(generated_data)

    manifest = {
        "manifest_version": manifest_version,
        "run_id": run_id,
        "generated_at": generated_at,
        "source_systems": source_systems,
        "record_counts": record_counts,
        "ground_truth": {
            **quarterly_truth,
            "dimensional_truth": dimensional_truth,
            "expected_conflicts": expected_conflicts,
        },
    }

    return manifest


# ═══════════════════════════════════════════════════════════════════════════════
# v2.0 — Full financial model metrics
# ═══════════════════════════════════════════════════════════════════════════════

def _build_v2_quarterly_truth(model_quarters: List) -> Dict[str, Any]:
    """Build per-quarter ground truth from financial model Quarter objects."""
    quarterly_truth = {}

    for fmq in model_quarters:
        q = fmq.quarter
        quarterly_truth[q] = {
            # ── ARR Waterfall ─────────────────────────────────────────────
            "beginning_arr": {"value": _r(fmq.beginning_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "new_arr": {"value": _r(fmq.new_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "new_logo_arr": {"value": _r(fmq.new_logo_arr), "unit": "millions_usd", "primary_source": "salesforce+chargebee"},
            "expansion_arr": {"value": _r(fmq.expansion_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "churned_arr": {"value": _r(fmq.churned_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "arr": {"value": _r(fmq.ending_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "mrr": {"value": _r(fmq.mrr, 4), "unit": "millions_usd", "primary_source": "chargebee"},

            # ── Revenue Decomposition ─────────────────────────────────────
            "revenue": {"value": _r(fmq.revenue), "unit": "millions_usd", "primary_source": "netsuite", "corroborating_source": "salesforce"},
            "new_logo_revenue": {"value": _r(fmq.new_logo_revenue), "unit": "millions_usd", "primary_source": "salesforce"},
            "expansion_revenue": {"value": _r(fmq.expansion_revenue), "unit": "millions_usd", "primary_source": "chargebee"},
            "renewal_revenue": {"value": _r(fmq.renewal_revenue), "unit": "millions_usd", "primary_source": "chargebee"},

            # ── P&L ──────────────────────────────────────────────────────
            "cogs": {"value": _r(fmq.cogs), "unit": "millions_usd", "primary_source": "netsuite"},
            "gross_profit": {"value": _r(fmq.gross_profit), "unit": "millions_usd", "primary_source": "netsuite"},
            "gross_margin_pct": {"value": _r(fmq.gross_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "sm_expense": {"value": _r(fmq.sm_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "rd_expense": {"value": _r(fmq.rd_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "ga_expense": {"value": _r(fmq.ga_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "opex": {"value": _r(fmq.total_opex), "unit": "millions_usd", "primary_source": "netsuite"},
            "ebitda": {"value": _r(fmq.ebitda), "unit": "millions_usd", "primary_source": "netsuite"},
            "ebitda_margin_pct": {"value": _r(fmq.ebitda_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "da_expense": {"value": _r(fmq.da_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "operating_profit": {"value": _r(fmq.operating_profit), "unit": "millions_usd", "primary_source": "netsuite"},
            "operating_margin_pct": {"value": _r(fmq.operating_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "tax_expense": {"value": _r(fmq.tax_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "net_income": {"value": _r(fmq.net_income), "unit": "millions_usd", "primary_source": "netsuite"},
            "net_margin_pct": {"value": _r(fmq.net_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},

            # ── Balance Sheet ─────────────────────────────────────────────
            "cash": {"value": _r(fmq.cash), "unit": "millions_usd", "primary_source": "netsuite"},
            "ar": {"value": _r(fmq.ar), "unit": "millions_usd", "primary_source": "netsuite"},
            "unbilled_revenue": {"value": _r(fmq.unbilled_revenue), "unit": "millions_usd", "primary_source": "netsuite"},
            "prepaid_expenses": {"value": _r(fmq.prepaid_expenses), "unit": "millions_usd", "primary_source": "netsuite"},
            "pp_e": {"value": _r(fmq.pp_e), "unit": "millions_usd", "primary_source": "netsuite"},
            "intangibles": {"value": _r(fmq.intangibles), "unit": "millions_usd", "primary_source": "netsuite"},
            "goodwill": {"value": _r(fmq.goodwill), "unit": "millions_usd", "primary_source": "netsuite"},
            "total_assets": {"value": _r(fmq.total_assets), "unit": "millions_usd", "primary_source": "netsuite"},
            "ap": {"value": _r(fmq.ap), "unit": "millions_usd", "primary_source": "netsuite"},
            "accrued_expenses": {"value": _r(fmq.accrued_expenses), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue": {"value": _r(fmq.deferred_revenue), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue_current": {"value": _r(fmq.deferred_revenue_current), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue_lt": {"value": _r(fmq.deferred_revenue_lt), "unit": "millions_usd", "primary_source": "netsuite"},
            "total_liabilities": {"value": _r(fmq.total_liabilities), "unit": "millions_usd", "primary_source": "netsuite"},
            "retained_earnings": {"value": _r(fmq.retained_earnings), "unit": "millions_usd", "primary_source": "netsuite"},
            "stockholders_equity": {"value": _r(fmq.stockholders_equity), "unit": "millions_usd", "primary_source": "netsuite"},

            # ── Cash Flow ─────────────────────────────────────────────────
            "cfo": {"value": _r(fmq.cfo), "unit": "millions_usd", "primary_source": "netsuite"},
            "capex": {"value": _r(fmq.capex), "unit": "millions_usd", "primary_source": "netsuite"},
            "fcf": {"value": _r(fmq.fcf), "unit": "millions_usd", "primary_source": "netsuite"},

            # ── SaaS Metrics ──────────────────────────────────────────────
            "nrr": {"value": _r(fmq.nrr, 1), "unit": "percent", "primary_source": "chargebee"},
            "gross_churn_pct": {"value": _r(fmq.gross_churn_pct, 1), "unit": "percent", "primary_source": "chargebee"},
            "logo_churn_pct": {"value": _r(fmq.logo_churn_pct, 1), "unit": "percent", "primary_source": "salesforce"},
            "acv": {"value": _r(fmq.acv, 4), "unit": "millions_usd", "primary_source": "salesforce"},
            "ltv": {"value": _r(fmq.ltv), "unit": "millions_usd", "primary_source": "computed"},
            "cac": {"value": _r(fmq.cac, 4), "unit": "millions_usd", "primary_source": "computed"},
            "ltv_cac_ratio": {"value": _r(fmq.ltv_cac_ratio, 1), "unit": "ratio", "primary_source": "computed"},
            "magic_number": {"value": _r(fmq.magic_number), "unit": "ratio", "primary_source": "computed"},
            "burn_multiple": {"value": _r(fmq.burn_multiple), "unit": "ratio", "primary_source": "computed"},
            "rule_of_40": {"value": _r(fmq.rule_of_40, 1), "unit": "percent", "primary_source": "computed"},
            "revenue_per_employee": {"value": _r(fmq.revenue_per_employee, 4), "unit": "millions_usd", "primary_source": "computed"},
            "arr_per_employee": {"value": _r(fmq.arr_per_employee, 4), "unit": "millions_usd", "primary_source": "computed"},

            # ── Pipeline ──────────────────────────────────────────────────
            "pipeline": {"value": _r(fmq.pipeline), "unit": "millions_usd", "primary_source": "salesforce"},
            "win_rate": {"value": _r(fmq.win_rate, 1), "unit": "percent", "primary_source": "salesforce"},
            "sales_cycle_days": {"value": _r(fmq.sales_cycle_days, 0), "unit": "days", "primary_source": "salesforce"},
            "avg_deal_size": {"value": _r(fmq.avg_deal_size, 4), "unit": "millions_usd", "primary_source": "salesforce"},
            "quota_attainment": {"value": _r(fmq.quota_attainment, 1), "unit": "percent", "primary_source": "salesforce"},

            # ── Customer Metrics ──────────────────────────────────────────
            "customer_count": {"value": fmq.customer_count, "unit": "count", "primary_source": "salesforce"},
            "new_customers": {"value": fmq.new_customers, "unit": "count", "primary_source": "salesforce"},
            "churned_customers": {"value": fmq.churned_customers, "unit": "count", "primary_source": "chargebee"},

            # ── People ────────────────────────────────────────────────────
            "headcount": {"value": fmq.headcount, "unit": "count", "primary_source": "workday"},
            "new_hires": {"value": fmq.hires, "unit": "count", "primary_source": "workday"},
            "terminations": {"value": fmq.terminations, "unit": "count", "primary_source": "workday"},
            "attrition_rate": {"value": _r(fmq.attrition_rate, 1), "unit": "percent", "primary_source": "workday"},
            "engineering_headcount": {"value": fmq.engineering_headcount, "unit": "count", "primary_source": "workday"},
            "sales_headcount": {"value": fmq.sales_headcount, "unit": "count", "primary_source": "workday"},

            # ── Support ───────────────────────────────────────────────────
            "support_tickets": {"value": fmq.support_tickets, "unit": "count", "primary_source": "zendesk"},
            "csat": {"value": _r(fmq.csat, 2), "unit": "score_5", "primary_source": "zendesk"},
            "nps": {"value": fmq.nps, "unit": "score", "primary_source": "zendesk"},
            "first_response_hours": {"value": _r(fmq.first_response_hours, 1), "unit": "hours", "primary_source": "zendesk"},
            "resolution_hours": {"value": _r(fmq.resolution_hours, 1), "unit": "hours", "primary_source": "zendesk"},

            # ── Engineering ───────────────────────────────────────────────
            "sprint_velocity": {"value": _r(fmq.sprint_velocity, 1), "unit": "story_points", "primary_source": "jira"},
            "story_points": {"value": _r(fmq.story_points), "unit": "points", "primary_source": "jira"},
            "features_shipped": {"value": fmq.features_shipped, "unit": "count", "primary_source": "jira"},
            "tech_debt_pct": {"value": _r(fmq.tech_debt_pct, 3), "unit": "percent", "primary_source": "jira"},

            # ── Infrastructure ────────────────────────────────────────────
            "cloud_spend": {"value": _r(fmq.cloud_spend), "unit": "millions_usd", "primary_source": "aws_cost_explorer"},
            "cloud_spend_pct_revenue": {"value": _r(fmq.cloud_spend_pct_revenue, 2), "unit": "percent", "primary_source": "aws_cost_explorer"},
            "p1_incidents": {"value": fmq.p1_incidents, "unit": "count", "primary_source": "datadog"},
            "p2_incidents": {"value": fmq.p2_incidents, "unit": "count", "primary_source": "datadog"},
            "incident_count": {"value": fmq.p1_incidents + fmq.p2_incidents, "unit": "count", "primary_source": "datadog"},
            "mttr_p1_hours": {"value": _r(fmq.mttr_p1_hours, 1), "unit": "hours", "primary_source": "datadog"},
            "mttr_p2_hours": {"value": _r(fmq.mttr_p2_hours, 1), "unit": "hours", "primary_source": "datadog"},
            "uptime_pct": {"value": _r(fmq.uptime_pct, 2), "unit": "percent", "primary_source": "datadog"},
            "downtime_hours": {"value": _r(fmq.downtime_hours, 1), "unit": "hours", "primary_source": "datadog"},

            # ── Meta ──────────────────────────────────────────────────────
            "is_forecast": fmq.is_forecast,
        }

    return quarterly_truth


def _build_v2_dimensional_truth(model_quarters: List) -> Dict[str, Any]:
    """Build dimensional breakdowns from financial model Quarter objects."""
    dims: Dict[str, Dict[str, Any]] = {
        "revenue_by_region": {"source": "netsuite+salesforce"},
        "revenue_by_segment": {"source": "salesforce"},
        "arr_by_region": {"source": "chargebee"},
        "arr_by_segment": {"source": "chargebee"},
        "pipeline_by_stage": {"source": "salesforce"},
        "pipeline_by_region": {"source": "salesforce"},
        "customers_by_segment": {"source": "salesforce"},
        "bookings_by_segment": {"source": "salesforce+chargebee"},
        "churn_by_segment": {"source": "chargebee"},
        "cogs_breakdown": {"source": "netsuite"},
        "opex_breakdown": {"source": "netsuite"},
        "headcount_by_department": {"source": "workday"},
        "new_logo_revenue_by_region": {"source": "salesforce"},
    }

    for fmq in model_quarters:
        q = fmq.quarter
        dims["revenue_by_region"][q] = {k: _r(v) for k, v in fmq.revenue_by_region.items()}
        dims["revenue_by_segment"][q] = {k: _r(v) for k, v in fmq.revenue_by_segment.items()}
        dims["arr_by_region"][q] = {k: _r(v) for k, v in fmq.arr_by_region.items()}
        dims["arr_by_segment"][q] = {k: _r(v) for k, v in fmq.arr_by_segment.items()}
        dims["pipeline_by_stage"][q] = {k: _r(v) for k, v in fmq.pipeline_by_stage.items()}
        dims["pipeline_by_region"][q] = {k: _r(v) for k, v in fmq.pipeline_by_region.items()}
        dims["customers_by_segment"][q] = dict(fmq.customers_by_segment)
        dims["bookings_by_segment"][q] = {k: _r(v) for k, v in fmq.bookings_by_segment.items()}
        dims["churn_by_segment"][q] = {k: _r(v) for k, v in fmq.churn_by_segment.items()}
        dims["cogs_breakdown"][q] = {k: _r(v) for k, v in fmq.cogs_breakdown.items()}
        dims["opex_breakdown"][q] = {k: _r(v) for k, v in fmq.opex_breakdown.items()}
        dims["headcount_by_department"][q] = dict(fmq.headcount_by_department)
        dims["new_logo_revenue_by_region"][q] = {k: _r(v) for k, v in fmq.new_logo_revenue_by_region.items()}

    return dims


def _build_v2_expected_conflicts(model_quarters: List) -> List[Dict[str, Any]]:
    """
    Build the list of known cross-system conflicts from financial model.

    These are intentional discrepancies that DCL should detect and flag.
    """
    conflicts = []

    for fmq in model_quarters:
        q = fmq.quarter

        # Revenue conflict: Salesforce books on close date, NetSuite on rev rec schedule
        # Salesforce is ~3-8% higher than NetSuite for any given quarter
        sf_premium_pct = 0.05  # ~5% higher
        sf_revenue = _r(fmq.revenue * (1 + sf_premium_pct))
        delta_dollars = round((sf_revenue - fmq.revenue) * 1_000_000)
        conflicts.append({
            "metric": "revenue",
            "period": q,
            "salesforce_value": sf_revenue,
            "netsuite_value": _r(fmq.revenue),
            "delta_pct": _r(sf_premium_pct * 100, 1),
            "root_cause": "rev_rec_timing",
            "explanation": (
                f"Salesforce books on close date, NetSuite recognizes on rev rec "
                f"schedule start. ~${delta_dollars:,} in late-quarter deals "
                f"recognized in following quarter."
            ),
        })

        # Headcount conflict: Workday includes contingent workers
        contractor_count = 3
        conflicts.append({
            "metric": "headcount",
            "period": q,
            "workday_value": fmq.headcount + contractor_count,
            "reporting_value": fmq.headcount,
            "delta": contractor_count,
            "root_cause": "contractor_classification",
            "explanation": (
                f"Workday includes {contractor_count} contractors classified as "
                f"contingent workers. Standard reporting excludes them."
            ),
        })

        # CSAT conflict: Zendesk has 3-5% missing satisfaction ratings
        csat_missing_pct = 4.0
        conflicts.append({
            "metric": "csat",
            "period": q,
            "ground_truth_value": _r(fmq.csat, 2),
            "zendesk_reported_value": _r(fmq.csat * 0.98, 2),
            "delta_pct": _r(csat_missing_pct, 1),
            "root_cause": "missing_satisfaction_data",
            "explanation": (
                f"~{csat_missing_pct}% of solved tickets have no satisfaction "
                f"rating. Zendesk averages only rated responses, slightly "
                f"underreporting overall CSAT."
            ),
        })

    return conflicts


# ═══════════════════════════════════════════════════════════════════════════════
# v1.0 — Legacy (BusinessProfile-based) metrics
# ═══════════════════════════════════════════════════════════════════════════════

def _build_v1_quarterly_truth(profile: BusinessProfile) -> Dict[str, Any]:
    """Build per-quarter ground truth from BusinessProfile (v1.0 legacy)."""
    quarterly_truth = {}
    for qm in profile.quarters:
        q = qm.quarter
        quarterly_truth[q] = {
            "revenue": {"value": _r(qm.revenue), "unit": "millions_usd", "primary_source": "netsuite", "corroborating_source": "salesforce"},
            "arr": {"value": _r(qm.arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "pipeline": {"value": _r(qm.pipeline), "unit": "millions_usd", "primary_source": "salesforce"},
            "win_rate": {"value": qm.win_rate, "unit": "percent", "primary_source": "salesforce"},
            "customer_count": {"value": qm.customer_count, "unit": "count", "primary_source": "salesforce"},
            "headcount": {"value": qm.headcount, "unit": "count", "primary_source": "workday"},
            "attrition_rate": {"value": qm.attrition_rate, "unit": "percent", "primary_source": "workday"},
            "support_tickets": {"value": qm.support_tickets, "unit": "count", "primary_source": "zendesk"},
            "csat": {"value": qm.csat, "unit": "score_5", "primary_source": "zendesk"},
            "sprint_velocity": {"value": qm.sprint_velocity, "unit": "story_points", "primary_source": "jira"},
            "gross_margin_pct": {"value": qm.gross_margin_pct, "unit": "percent", "primary_source": "netsuite"},
            "nrr": {"value": qm.nrr, "unit": "percent", "primary_source": "chargebee"},
            "gross_churn_pct": {"value": qm.gross_churn_pct, "unit": "percent", "primary_source": "chargebee"},
            "cloud_spend": {"value": _r(qm.cloud_spend), "unit": "millions_usd", "primary_source": "aws_cost_explorer"},
            "incident_count": {"value": qm.incident_count, "unit": "count", "primary_source": "datadog"},
            "mttr_hours": {"value": qm.mttr_hours, "unit": "hours", "primary_source": "datadog"},
            "new_customers": {"value": qm.new_customers, "unit": "count", "primary_source": "salesforce"},
            "churned_customers": {"value": qm.churned_customers, "unit": "count", "primary_source": "chargebee"},
            "new_hires": {"value": qm.new_hires, "unit": "count", "primary_source": "workday"},
            "terminations": {"value": qm.terminations, "unit": "count", "primary_source": "workday"},
            "mrr": {"value": _r(qm.mrr, 4), "unit": "millions_usd", "primary_source": "chargebee"},
            "cogs": {"value": _r(qm.cogs), "unit": "millions_usd", "primary_source": "netsuite"},
            "opex": {"value": _r(qm.opex), "unit": "millions_usd", "primary_source": "netsuite"},
        }
    return quarterly_truth


def _build_v1_dimensional_truth(profile: BusinessProfile) -> Dict[str, Any]:
    """Build dimensional breakdowns from the profile (v1.0 legacy)."""
    revenue_by_region = {}
    pipeline_by_stage = {}
    headcount_by_dept = {}

    for qm in profile.quarters:
        q = qm.quarter
        revenue_by_region[q] = {
            region: round(val, 2) for region, val in qm.revenue_by_region.items()
        }
        pipeline_by_stage[q] = {
            stage: round(val, 2) for stage, val in qm.pipeline_by_stage.items()
        }
        headcount_by_dept[q] = dict(qm.headcount_by_dept)

    return {
        "revenue_by_region": {**revenue_by_region, "source": "netsuite+salesforce"},
        "pipeline_by_stage": {**pipeline_by_stage, "source": "salesforce"},
        "headcount_by_department": {**headcount_by_dept, "source": "workday"},
    }


def _build_v1_expected_conflicts(profile: BusinessProfile) -> List[Dict[str, Any]]:
    """Build the list of known cross-system conflicts (v1.0 legacy)."""
    conflicts = []
    for qm in profile.quarters:
        q = qm.quarter
        sf_revenue_premium = round(qm.revenue * 1.05, 2)
        conflicts.append({
            "metric": "revenue",
            "period": q,
            "salesforce_value": sf_revenue_premium,
            "netsuite_value": _r(qm.revenue),
            "root_cause": "rev_rec_timing",
            "explanation": (
                f"Salesforce books on close date, NetSuite recognizes on rev rec "
                f"schedule start. ~${round((sf_revenue_premium - qm.revenue) * 1_000_000):,} "
                f"in late-quarter deals recognized in following quarter."
            ),
        })
        if qm.headcount > 240:
            contractor_count = 3
            conflicts.append({
                "metric": "headcount",
                "period": q,
                "workday_value": qm.headcount + contractor_count,
                "reporting_value": qm.headcount,
                "root_cause": "contractor_classification",
                "explanation": (
                    f"Workday includes {contractor_count} contractors classified as "
                    f"contingent workers. Standard reporting excludes them."
                ),
            })
    return conflicts


# ═══════════════════════════════════════════════════════════════════════════════
# Shared utilities
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_record_counts(generated_data: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Extract record counts from generated DCL payloads."""
    counts = {}
    for source_system, pipes in generated_data.items():
        for pipe_name, payload in pipes.items():
            if isinstance(payload, dict) and "meta" in payload:
                pipe_id = payload["meta"].get("pipe_id", f"{source_system}_{pipe_name}")
                counts[pipe_id] = payload["meta"].get("record_count", 0)
            elif isinstance(payload, dict) and "data" in payload:
                counts[f"{source_system}_{pipe_name}"] = len(payload["data"])
    return counts


def validate_manifest_completeness(manifest: Dict[str, Any]) -> List[str]:
    """
    Validate that the ground truth manifest covers all required metrics and quarters.

    Returns a list of validation errors (empty list = valid).
    """
    errors = []

    required_quarters = [
        f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)
    ]

    # Core metrics required in both v1.0 and v2.0
    required_metrics = [
        "revenue", "arr", "pipeline", "win_rate", "customer_count",
        "headcount", "attrition_rate", "support_tickets", "csat",
        "sprint_velocity", "gross_margin_pct", "nrr", "gross_churn_pct",
    ]

    ground_truth = manifest.get("ground_truth", {})

    for q in required_quarters:
        if q not in ground_truth:
            errors.append(f"Missing quarter: {q}")
            continue
        for metric in required_metrics:
            if metric not in ground_truth[q]:
                errors.append(f"Missing metric {metric} in {q}")

    if "dimensional_truth" not in ground_truth:
        errors.append("Missing dimensional_truth block")
    else:
        dt = ground_truth["dimensional_truth"]
        for dim in ["revenue_by_region", "pipeline_by_stage", "headcount_by_department"]:
            if dim not in dt:
                errors.append(f"Missing dimensional breakdown: {dim}")

    if "expected_conflicts" not in ground_truth:
        errors.append("Missing expected_conflicts block")

    if not manifest.get("source_systems"):
        errors.append("Missing source_systems list")

    # v2.0 additional checks
    if manifest.get("manifest_version") == "2.0":
        v2_metrics = [
            "beginning_arr", "new_arr", "churned_arr", "gross_profit",
            "ebitda", "net_income", "cash", "total_assets", "fcf",
        ]
        for q in required_quarters:
            if q not in ground_truth:
                continue
            for metric in v2_metrics:
                if metric not in ground_truth[q]:
                    errors.append(f"Missing v2.0 metric {metric} in {q}")

    return errors
