"""
Financial Summary generator — pushes pre-computed metrics to DCL as a
dedicated pipe so that the materializer can extract margin percentages,
profitability line items, CRO ratios, CTO infrastructure metrics, and
CHRO people metrics.

The financial model (financial_model.py) computes a full set of ~177
data points per quarter, but that data only flows into the ground truth
manifest. The existing source-system generators (NetSuite, Salesforce,
etc.) produce transactional rows — invoices, GL entries, subscriptions —
from which DCL can derive revenue, AR, headcount, etc.  However, derived
metrics like gross_margin_pct, win_rate, uptime_pct, etc. require either
complex aggregation or a pre-computed summary. This generator provides
the latter: four rows per quarter (1 total + 3 regional) with all
pre-computed metrics. Regional rows carry AMER/EMEA/APAC splits.

Field names are chosen to match DCL extraction rule hints exactly:
- No `revenue` field (already extracted from invoices)
- No `opex` field (matches the `cost` concept's example_fields)
- Uses `operating_expenses` instead of `opex`/`total_opex`
"""

from typing import Any, Dict, List

from src.generators.business_data.base import BaseBusinessGenerator


SOURCE_SYSTEM = "oracle"

SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "date", "type": "date", "semantic_hint": "fiscal_period_end"},
    {"name": "quarter_label", "type": "string"},
    {"name": "region", "type": "string", "semantic_hint": "territory"},
    {"name": "segment", "type": "string", "semantic_hint": "practice_area"},
    # Total revenue (millions USD) — single source of truth for P&L consistency
    {"name": "total_revenue", "type": "number", "semantic_hint": "total_revenue"},
    # Margins (percentages)
    {"name": "gross_margin_pct", "type": "number", "semantic_hint": "gross_margin"},
    {"name": "operating_margin_pct", "type": "number", "semantic_hint": "operating_margin"},
    {"name": "net_margin_pct", "type": "number", "semantic_hint": "net_margin"},
    {"name": "ebitda_margin_pct", "type": "number", "semantic_hint": "ebitda_margin"},
    # P&L line items (millions USD)
    {"name": "cogs", "type": "number", "semantic_hint": "cost_of_goods_sold"},
    {"name": "gross_profit", "type": "number", "semantic_hint": "gross_profit"},
    {"name": "ebitda", "type": "number", "semantic_hint": "ebitda"},
    {"name": "operating_profit", "type": "number", "semantic_hint": "operating_income"},
    {"name": "net_income", "type": "number", "semantic_hint": "net_income"},
    {"name": "operating_expenses", "type": "number", "semantic_hint": "total_opex"},
    {"name": "sm_expense", "type": "number", "semantic_hint": "sales_marketing_expense"},
    {"name": "rd_expense", "type": "number", "semantic_hint": "rd_expense"},
    {"name": "ga_expense", "type": "number", "semantic_hint": "ga_expense"},
    {"name": "da_expense", "type": "number", "semantic_hint": "depreciation_amortization"},
    {"name": "tax_expense", "type": "number", "semantic_hint": "tax_expense"},
    # ARR (annualized recurring revenue — millions USD)
    {"name": "arr", "type": "number", "semantic_hint": "annual_recurring_revenue"},
    # Balance sheet (millions USD)
    {"name": "cash", "type": "number", "semantic_hint": "cash_and_equivalents"},
    {"name": "ar", "type": "number", "semantic_hint": "accounts_receivable"},
    {"name": "ap", "type": "number", "semantic_hint": "accounts_payable"},
    {"name": "unbilled_revenue", "type": "number", "semantic_hint": "unbilled_revenue"},
    {"name": "prepaid_expenses", "type": "number", "semantic_hint": "prepaid_expenses"},
    {"name": "pp_e", "type": "number", "semantic_hint": "property_plant_equipment"},
    {"name": "intangibles", "type": "number", "semantic_hint": "intangible_assets"},
    {"name": "goodwill", "type": "number", "semantic_hint": "goodwill"},
    {"name": "total_assets", "type": "number", "semantic_hint": "total_assets"},
    {"name": "accrued_expenses", "type": "number", "semantic_hint": "accrued_expenses"},
    {"name": "deferred_revenue", "type": "number", "semantic_hint": "deferred_revenue"},
    {"name": "total_liabilities", "type": "number", "semantic_hint": "total_liabilities"},
    {"name": "retained_earnings", "type": "number", "semantic_hint": "retained_earnings"},
    {"name": "stockholders_equity", "type": "number", "semantic_hint": "stockholders_equity"},
    # Cash flow (millions USD)
    {"name": "cfo", "type": "number", "semantic_hint": "cash_from_operations"},
    {"name": "capex", "type": "number", "semantic_hint": "capital_expenditures"},
    {"name": "fcf", "type": "number", "semantic_hint": "free_cash_flow"},
    {"name": "change_in_ar", "type": "number", "semantic_hint": "change_in_accounts_receivable"},
    {"name": "change_in_ap", "type": "number", "semantic_hint": "change_in_accounts_payable"},
    {"name": "change_in_deferred_rev", "type": "number", "semantic_hint": "change_in_deferred_revenue"},
    # CRO metrics
    {"name": "win_rate", "type": "number", "semantic_hint": "win_rate_percentage"},
    {"name": "churn_rate", "type": "number", "semantic_hint": "churn_rate_percentage"},
    {"name": "nrr", "type": "number", "semantic_hint": "net_revenue_retention"},
    {"name": "attainment_pct", "type": "number", "semantic_hint": "quota_attainment"},
    {"name": "sales_cycle_days", "type": "number", "semantic_hint": "sales_cycle"},
    # Sales pipeline (millions USD)
    {"name": "pipeline_value", "type": "number", "semantic_hint": "pipeline_total"},
    # CTO metrics
    {"name": "uptime", "type": "number", "semantic_hint": "uptime_percentage"},
    {"name": "time_to_resolve", "type": "number", "semantic_hint": "mttr_hours"},
    {"name": "p1_incidents", "type": "number", "semantic_hint": "p1_incident_count"},
    {"name": "deploy_count", "type": "number", "semantic_hint": "deployment_frequency"},
    # CHRO metrics
    {"name": "attrition_rate", "type": "number", "semantic_hint": "attrition_percentage"},
    {"name": "engagement_score", "type": "number", "semantic_hint": "employee_engagement"},
    {"name": "revenue_per_employee", "type": "number", "semantic_hint": "revenue_per_head"},
    {"name": "total_headcount", "type": "number", "semantic_hint": "total_employee_count"},
]

# Quarter end dates for period derivation
_QUARTER_END = {
    1: "-03-31",
    2: "-06-30",
    3: "-09-30",
    4: "-12-31",
}

_REGIONS = ["AMER", "EMEA", "APAC"]


class FinancialSummaryGenerator(BaseBusinessGenerator):
    """
    Generates four rows per quarter (1 total + 3 regional) containing
    pre-computed metrics from the financial model.  Designed for DCL's
    materializer to extract via strict_hint value_field_hint rules.
    Regional rows use AMER/EMEA/APAC splits from the financial model.
    """

    SOURCE_SYSTEM = SOURCE_SYSTEM
    PIPE_PREFIX = "finsummary"

    def __init__(self, model_quarters: List[Any], seed: int = 42, entity_id: str = ""):
        super().__init__(seed=seed)
        self._model_quarters = model_quarters
        self._entity_id = entity_id

    def generate(
        self,
        pipe_id: str = "",
        run_id: str = "run-finsummary-001",
        run_timestamp: str = "2026-01-15T00:00:00Z",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Produce a single 'pnl' pipe with four rows per quarter
        (1 total + 3 regional).

        Returns dict with key 'pnl' containing the DCL payload.
        """
        if not pipe_id:
            suffix = f"-{self._entity_id}" if self._entity_id else ""
            pipe_id = f"finsummary-pnl-001{suffix}"
        rows: List[Dict[str, Any]] = []

        for fmq in self._model_quarters:
            year = int(fmq.quarter[:4])
            q_num = int(fmq.quarter[-1])
            quarter_end = f"{year}{_QUARTER_END[q_num]}"

            # --- shared CRO / CTO / CHRO metrics (company-wide) ---
            cro_cto_chro = {
                "win_rate": round(fmq.win_rate, 1),
                "churn_rate": round(fmq.gross_churn_pct, 2),
                "nrr": round(fmq.nrr, 1),
                "attainment_pct": round(fmq.quota_attainment, 1),
                "sales_cycle_days": round(fmq.sales_cycle_days),
                "pipeline_value": round(fmq.pipeline, 2),
                "uptime": round(fmq.uptime_pct, 2),
                "time_to_resolve": round(fmq.mttr_p1_hours, 1),
                "p1_incidents": int(fmq.p1_incidents),
                "deploy_count": int(getattr(fmq, 'features_shipped', 0)),
                "attrition_rate": round(fmq.attrition_rate, 1),
                "engagement_score": round(getattr(fmq, 'csat', 3.8) * 20, 1),
                "revenue_per_employee": round(
                    fmq.revenue_per_employee * 1000, 1
                ) if fmq.revenue_per_employee else 0.0,
                "total_headcount": int(fmq.headcount),
            }

            # --- margin percentages (company-wide, same for all regions) ---
            margins = {
                "gross_margin_pct": round(fmq.gross_margin_pct, 1),
                "operating_margin_pct": round(fmq.operating_margin_pct, 1),
                "net_margin_pct": round(fmq.net_margin_pct, 1),
                "ebitda_margin_pct": round(fmq.ebitda_margin_pct, 1),
            }

            # 1. Total row (no region field — preserves flat queries)
            rows.append({
                "date": quarter_end,
                "quarter_label": fmq.quarter,
                "total_revenue": round(fmq.revenue, 2),
                **margins,
                "cogs": round(fmq.cogs, 2),
                "gross_profit": round(fmq.gross_profit, 2),
                "ebitda": round(fmq.ebitda, 2),
                "operating_profit": round(fmq.operating_profit, 2),
                "net_income": round(fmq.net_income, 2),
                "operating_expenses": round(fmq.total_opex + fmq.da_expense, 2),
                "sm_expense": round(fmq.sm_expense, 2),
                "rd_expense": round(fmq.rd_expense, 2),
                "ga_expense": round(fmq.ga_expense, 2),
                "da_expense": round(fmq.da_expense, 2),
                "tax_expense": round(fmq.tax_expense, 2),
                "arr": round(fmq.ending_arr, 2),
                "cash": round(fmq.cash, 2),
                # Balance sheet
                "ar": round(fmq.ar, 2),
                "ap": round(fmq.ap, 2),
                "unbilled_revenue": round(fmq.unbilled_revenue, 2),
                "prepaid_expenses": round(fmq.prepaid_expenses, 2),
                "pp_e": round(fmq.pp_e, 2),
                "intangibles": round(fmq.intangibles, 2),
                "goodwill": round(fmq.goodwill, 2),
                "total_assets": round(fmq.total_assets, 2),
                "accrued_expenses": round(fmq.accrued_expenses, 2),
                "deferred_revenue": round(fmq.deferred_revenue, 2),
                "total_liabilities": round(fmq.total_liabilities, 2),
                "retained_earnings": round(fmq.retained_earnings, 2),
                "stockholders_equity": round(fmq.stockholders_equity, 2),
                # Cash flow
                "cfo": round(fmq.cfo, 2),
                "capex": round(fmq.capex, 2),
                "fcf": round(fmq.fcf, 2),
                "change_in_ar": round(fmq.change_in_ar, 2),
                "change_in_ap": round(fmq.change_in_ap, 2),
                "change_in_deferred_rev": round(fmq.change_in_deferred_rev, 2),
                **cro_cto_chro,
            })

            # 2. Regional rows — use revenue_by_region keys (handles LATAM, etc.)
            for region in fmq.revenue_by_region:
                region_revenue = fmq.revenue_by_region[region]
                region_pct = region_revenue / fmq.revenue if fmq.revenue else 0.0
                region_arr = fmq.arr_by_region.get(region, 0.0)

                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "region": region,
                    "total_revenue": round(region_revenue, 2),
                    **margins,
                    "cogs": round(fmq.cogs * region_pct, 2),
                    "gross_profit": round(fmq.gross_profit * region_pct, 2),
                    "ebitda": round(fmq.ebitda * region_pct, 2),
                    "operating_profit": round(fmq.operating_profit * region_pct, 2),
                    "net_income": round(fmq.net_income * region_pct, 2),
                    "operating_expenses": round((fmq.total_opex + fmq.da_expense) * region_pct, 2),
                    "sm_expense": round(fmq.sm_expense * region_pct, 2),
                    "rd_expense": round(fmq.rd_expense * region_pct, 2),
                    "ga_expense": round(fmq.ga_expense * region_pct, 2),
                    "da_expense": round(fmq.da_expense * region_pct, 2),
                    "tax_expense": round(fmq.tax_expense * region_pct, 2),
                    "arr": round(region_arr, 2),
                    "cash": round(fmq.cash * region_pct, 2),
                    **cro_cto_chro,
                })

            # 3. Segment rows — practice area / service line P&L breakdowns
            for practice, practice_rev in getattr(fmq, 'revenue_by_practice', {}).items():
                seg_pct = practice_rev / fmq.revenue if fmq.revenue else 0.0
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "segment": practice,
                    "total_revenue": round(practice_rev, 2),
                    **margins,
                    "cogs": round(fmq.cogs * seg_pct, 2),
                    "gross_profit": round(fmq.gross_profit * seg_pct, 2),
                    "ebitda": round(fmq.ebitda * seg_pct, 2),
                    "operating_profit": round(fmq.operating_profit * seg_pct, 2),
                    "net_income": round(fmq.net_income * seg_pct, 2),
                    "operating_expenses": round((fmq.total_opex + fmq.da_expense) * seg_pct, 2),
                    "sm_expense": round(fmq.sm_expense * seg_pct, 2),
                    "rd_expense": round(fmq.rd_expense * seg_pct, 2),
                    "ga_expense": round(fmq.ga_expense * seg_pct, 2),
                    "da_expense": round(fmq.da_expense * seg_pct, 2),
                    "tax_expense": round(fmq.tax_expense * seg_pct, 2),
                    "arr": round(fmq.ending_arr * seg_pct, 2),
                    "cash": round(fmq.cash * seg_pct, 2),
                    **cro_cto_chro,
                })

            # 4. Headcount dimensional rows (department, geo, practice/service line, level)
            for dept, count in fmq.headcount_by_department.items():
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "metric": "headcount",
                    "dimension": "department",
                    "dimension_value": dept,
                    "value": count,
                    "total_headcount": int(fmq.headcount),
                })
            for geo, count in fmq.headcount_by_geo.items():
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "metric": "headcount",
                    "dimension": "geography",
                    "dimension_value": geo,
                    "value": count,
                    "total_headcount": int(fmq.headcount),
                })
            for practice, count in fmq.headcount_by_practice.items():
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "metric": "headcount",
                    "dimension": "practice_area",
                    "dimension_value": practice,
                    "value": count,
                    "total_headcount": int(fmq.headcount),
                })
            for level, count in fmq.headcount_by_level.items():
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "metric": "headcount",
                    "dimension": "level",
                    "dimension_value": level,
                    "value": count,
                    "total_headcount": int(fmq.headcount),
                })

            # 5. Revenue by customer rows (top 20) — Type A format for DCL materializer
            for customer, rev in fmq.revenue_by_customer.items():
                rows.append({
                    "date": quarter_end,
                    "quarter_label": fmq.quarter,
                    "customer": customer,
                    "total_revenue": rev,
                })

        return {
            "pnl": self.format_dcl_payload(
                pipe_id=pipe_id,
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=SCHEMA_FIELDS,
                data=rows,
            ),
        }
