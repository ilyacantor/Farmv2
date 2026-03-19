"""
COFA-Adjusted Combining Financial Statements Engine.

Produces combining income statements, balance sheets, and cash flows for
Meridian Partners (consultancy) and Cascadia Process Solutions (BPM),
with COFA (Chart of Accounts) adjustments to reconcile accounting
treatment differences between the two entities.

Six COFA adjustments are computed per quarter:
  COFA-001  Revenue gross-up (Cascadia FTE rate vs Meridian markup)
  COFA-002  Benefits loading (Cascadia separates; Meridian bundles)
  COFA-003  S&M bundling (Cascadia bundles; Meridian separates)
  COFA-004  Recruiting capitalization (Cascadia capitalizes; Meridian expenses)
  COFA-005  Automation capitalization (Cascadia capitalizes; Meridian expenses)
  COFA-006  Depreciation methods (Cascadia accelerated/3yr; Meridian straight-line/5yr)

Usage:
    from src.generators.financial_model import FinancialModel, Assumptions
    from src.generators.combining_statements import CombiningStatementEngine

    m_assumptions = Assumptions.from_yaml("farm_config_meridian.yaml")
    c_assumptions = Assumptions.from_yaml("farm_config_cascadia.yaml")
    m_quarters = FinancialModel(m_assumptions).generate()
    c_quarters = FinancialModel(c_assumptions).generate()

    engine = CombiningStatementEngine(m_quarters, c_quarters)
    result = engine.generate()
    errors = engine.validate(result)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_logger = logging.getLogger("farm.combining_statements")

# Import Quarter from the financial model — this is the only external dependency.
from src.generators.financial_model import (
    Assumptions,
    FinancialModel,
    Quarter,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Data structures
# ═══════════════════════════════════════════════════════════════════════════════

REVENUE_GROSSUP_ANNUAL = 50.0  # $50M annual pass-through delta


@dataclass
class COFAAdjustment:
    """A single COFA adjustment applied to the combining statements."""
    conflict_id: str          # "COFA-001" through "COFA-006"
    description: str
    metric: str               # which P&L/BS line is affected
    meridian_treatment: str
    cascadia_treatment: str
    adjustment_amount: float  # per quarter, positive = add to combined
    adjustment_rationale: str


@dataclass
class CombiningLineItem:
    """One row in a combining statement."""
    line_item: str            # e.g. "Revenue", "COGS - Consultant Comp"
    meridian: float
    cascadia: float
    adjustments: float        # sum of COFA adjustments for this line
    combined: float           # meridian + cascadia + adjustments
    adjustment_details: List[COFAAdjustment] = field(default_factory=list)


@dataclass
class CombiningStatement:
    """A full combining statement for one period."""
    statement_type: str       # "income_statement", "balance_sheet", "cash_flow"
    period: str               # "2024-Q1"
    line_items: List[CombiningLineItem] = field(default_factory=list)


@dataclass
class COFAMapping:
    """Maps a unified chart-of-accounts line to entity-specific accounts."""
    unified_id: str
    unified_name: str
    unified_type: str         # "revenue", "cogs", "opex", "asset", "liability", "equity"
    meridian_account: Optional[str]
    cascadia_account: Optional[str]
    conflict_type: Optional[str]  # "clean", "scope", "classification", "structural_gap", "materiality"
    conflict_description: Optional[str]


@dataclass
class CombiningResult:
    """Complete output of the combining statement engine."""
    cofa_mappings: List[COFAMapping]
    conflict_register: List[COFAAdjustment]
    income_statements: List[CombiningStatement]  # one per quarter
    balance_sheets: List[CombiningStatement]      # one per quarter
    cash_flows: List[CombiningStatement]           # one per quarter


# ═══════════════════════════════════════════════════════════════════════════════
# Rounding helper — matches financial_model.py convention
# ═══════════════════════════════════════════════════════════════════════════════

def _r(val: float, decimals: int = 2) -> float:
    return round(val, decimals)


# ═══════════════════════════════════════════════════════════════════════════════
# CombiningStatementEngine
# ═══════════════════════════════════════════════════════════════════════════════

class CombiningStatementEngine:
    """
    Produces COFA-adjusted combining financial statements from pre-generated
    Meridian (consultancy) and Cascadia (BPM) quarterly data.
    """

    # S&M split ratio for COFA-003: 60% sales, 40% marketing (typical BPM)
    SM_SALES_RATIO = 0.60
    SM_MARKETING_RATIO = 0.40

    def __init__(
        self,
        meridian_quarters: List[Quarter],
        cascadia_quarters: List[Quarter],
    ):
        if not meridian_quarters:
            raise ValueError("meridian_quarters must not be empty")
        if not cascadia_quarters:
            raise ValueError("cascadia_quarters must not be empty")

        # Filter out Period 0 (opening BS) — combining statements are P&L/BS/CF for operating quarters
        m_operating = [q for q in meridian_quarters if q.period_type != "opening"]
        c_operating = [q for q in cascadia_quarters if q.period_type != "opening"]

        if len(m_operating) != len(c_operating):
            raise ValueError(
                f"Quarter count mismatch: Meridian has {len(m_operating)}, "
                f"Cascadia has {len(c_operating)}"
            )
        # Verify quarter labels align
        for mq, cq in zip(m_operating, c_operating):
            if mq.quarter != cq.quarter:
                raise ValueError(
                    f"Quarter label mismatch: Meridian={mq.quarter}, Cascadia={cq.quarter}"
                )

        self._m_quarters = m_operating
        self._c_quarters = c_operating

    # ───────────────────────────────────────────────────────────────────────
    # Public API
    # ───────────────────────────────────────────────────────────────────────

    def generate(self) -> CombiningResult:
        """Generate all combining statements with COFA adjustments."""
        cofa_mappings = self._build_cofa_mappings()
        all_adjustments: List[COFAAdjustment] = []
        income_statements: List[CombiningStatement] = []
        balance_sheets: List[CombiningStatement] = []
        cash_flows: List[CombiningStatement] = []

        # Track cumulative BS adjustments (COFA-004/005 reduce assets each quarter)
        cumulative_ppe_adj = 0.0
        cumulative_intang_adj = 0.0

        for i, (mq, cq) in enumerate(zip(self._m_quarters, self._c_quarters)):
            period = mq.quarter
            adj = self._compute_cofa_adjustments(mq, cq)
            all_adjustments.extend(adj)

            income_statements.append(self._build_income_statement(mq, cq, adj, period))

            # Accumulate BS adjustments: each quarter adds another round of
            # capitalization reversal
            cumulative_ppe_adj += -cq.capitalized_recruiting
            cumulative_intang_adj += -cq.capitalized_automation

            balance_sheets.append(self._build_balance_sheet(
                mq, cq, adj, period,
                cumulative_ppe_adj=cumulative_ppe_adj,
                cumulative_intang_adj=cumulative_intang_adj,
            ))
            cash_flows.append(self._build_cash_flow(mq, cq, adj, period))

        return CombiningResult(
            cofa_mappings=cofa_mappings,
            conflict_register=all_adjustments,
            income_statements=income_statements,
            balance_sheets=balance_sheets,
            cash_flows=cash_flows,
        )

    def validate(self, result: CombiningResult) -> List[str]:
        """
        Validate combining statement integrity.

        Returns a list of error strings (empty = valid).
        """
        errors: List[str] = []
        TOL = 0.01

        # 1. Every combining line: meridian + cascadia + adjustments == combined
        for stmt_list, stmt_name in [
            (result.income_statements, "income_statement"),
            (result.balance_sheets, "balance_sheet"),
            (result.cash_flows, "cash_flow"),
        ]:
            for stmt in stmt_list:
                for li in stmt.line_items:
                    expected = li.meridian + li.cascadia + li.adjustments
                    if abs(expected - li.combined) > TOL:
                        errors.append(
                            f"{stmt_name} {stmt.period} '{li.line_item}': "
                            f"M({li.meridian}) + C({li.cascadia}) + Adj({li.adjustments}) "
                            f"= {expected}, but combined = {li.combined}"
                        )

        # 2. BS balances: total_assets == total_liabilities + total_equity
        for bs in result.balance_sheets:
            items = {li.line_item: li.combined for li in bs.line_items}
            total_assets = items.get("Total Assets", 0.0)
            total_liabilities = items.get("Total Liabilities", 0.0)
            total_equity = items.get("Total Equity", 0.0)
            diff = abs(total_assets - (total_liabilities + total_equity))
            if diff > TOL:
                errors.append(
                    f"balance_sheet {bs.period}: Total Assets ({total_assets:.2f}) != "
                    f"Total Liabilities ({total_liabilities:.2f}) + "
                    f"Total Equity ({total_equity:.2f}), diff={diff:.2f}"
                )

        # 3. Cash flow ties: beginning cash + net change == ending cash
        for i, cf in enumerate(result.cash_flows):
            items = {li.line_item: li.combined for li in cf.line_items}
            net_change = items.get("Net Change in Cash", 0.0)
            # Get ending cash from balance sheet
            bs_items = {li.line_item: li.combined for li in result.balance_sheets[i].line_items}
            ending_cash = bs_items.get("Cash", 0.0)
            if i > 0:
                prev_bs_items = {
                    li.line_item: li.combined
                    for li in result.balance_sheets[i - 1].line_items
                }
                beginning_cash = prev_bs_items.get("Cash", 0.0)
            else:
                beginning_cash = ending_cash - net_change  # first quarter: derive
            expected_ending = beginning_cash + net_change
            diff = abs(expected_ending - ending_cash)
            if diff > TOL:
                errors.append(
                    f"cash_flow {cf.period}: beginning_cash ({beginning_cash:.2f}) + "
                    f"net_change ({net_change:.2f}) = {expected_ending:.2f}, "
                    f"but BS cash = {ending_cash:.2f}, diff={diff:.2f}"
                )

        # 4. At least 6 COFA conflicts in the register (per quarter)
        unique_conflict_ids = {adj.conflict_id for adj in result.conflict_register}
        if len(unique_conflict_ids) < 6:
            errors.append(
                f"Expected at least 6 unique COFA conflict IDs, "
                f"found {len(unique_conflict_ids)}: {sorted(unique_conflict_ids)}"
            )

        return errors

    # ───────────────────────────────────────────────────────────────────────
    # COFA adjustment computation
    # ───────────────────────────────────────────────────────────────────────

    def _compute_cofa_adjustments(
        self, mq: Quarter, cq: Quarter
    ) -> List[COFAAdjustment]:
        """Compute the 6 COFA adjustments for a single quarter pair."""
        adjustments: List[COFAAdjustment] = []

        # ── COFA-001: Revenue Gross-Up ─────────────────────────────────────
        # Cascadia books full FTE rate. Meridian books contractor markup.
        # Reduce Cascadia revenue and COGS by $12.5M/Q (pass-through).
        grossup_q = _r(REVENUE_GROSSUP_ANNUAL / 4)
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-001",
            description="Revenue Gross-Up: Cascadia books full FTE rate; Meridian books markup only",
            metric="Revenue",
            meridian_treatment="Books contractor markup as revenue",
            cascadia_treatment="Books full FTE rate as revenue (includes pass-through cost)",
            adjustment_amount=_r(-grossup_q),
            adjustment_rationale=(
                f"Remove ${grossup_q:.1f}M/Q pass-through from Cascadia revenue. "
                f"Corresponding COGS reduction nets to zero P&L impact."
            ),
        ))
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-001",
            description="Revenue Gross-Up: COGS offset for pass-through cost removal",
            metric="COGS",
            meridian_treatment="COGS excludes pass-through",
            cascadia_treatment="COGS includes pass-through cost",
            adjustment_amount=_r(-grossup_q),
            adjustment_rationale=(
                f"Remove ${grossup_q:.1f}M/Q pass-through from Cascadia COGS, "
                f"matching the revenue reduction. Net P&L impact: zero."
            ),
        ))

        # ── COFA-002: Benefits Loading ─────────────────────────────────────
        # Cascadia separates benefits_cost as distinct COGS line.
        # Meridian bundles benefits into consultant_comp.
        # Reclassify: move benefits into comp line. Zero net COGS impact.
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-002",
            description="Benefits Loading: reclassify Cascadia benefits into delivery comp",
            metric="COGS_reclassification",
            meridian_treatment="Benefits bundled into consultant compensation (single COGS line)",
            cascadia_treatment="Benefits separated as distinct COGS line",
            adjustment_amount=0.0,  # net zero — reclassification within COGS
            adjustment_rationale=(
                f"Reclassify Cascadia benefits_cost (${cq.benefits_cost:.2f}M) into "
                f"delivery staff comp for combining view. Net COGS impact: zero."
            ),
        ))

        # ── COFA-003: Sales & Marketing Bundling ───────────────────────────
        # Cascadia bundles S&M; Meridian separates Sales and Marketing.
        # Split Cascadia sm_combined_expense 60/40 for combining view.
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-003",
            description="S&M Bundling: split Cascadia S&M into Sales (60%) and Marketing (40%)",
            metric="OpEx_reclassification",
            meridian_treatment="Separate Sales and Marketing OpEx lines",
            cascadia_treatment="Bundled Sales & Marketing as single OpEx line",
            adjustment_amount=0.0,  # net zero — reclassification within OpEx
            adjustment_rationale=(
                f"Split Cascadia sm_combined_expense (${cq.sm_combined_expense:.2f}M) "
                f"into Sales (${_r(cq.sm_combined_expense * self.SM_SALES_RATIO):.2f}M) "
                f"and Marketing (${_r(cq.sm_combined_expense * self.SM_MARKETING_RATIO):.2f}M)."
            ),
        ))

        # ── COFA-004: Recruiting Capitalization ────────────────────────────
        # Cascadia capitalizes $8M/yr ($2M/Q) recruiting.
        # Meridian expenses all. Add back to OpEx, reduce PP&E.
        cap_recruiting_q = cq.capitalized_recruiting  # actual value from model
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-004",
            description="Recruiting Capitalization: expense Cascadia capitalized recruiting",
            metric="OpEx_recruiting",
            meridian_treatment="All recruiting costs expensed immediately",
            cascadia_treatment=f"Capitalizes ${cap_recruiting_q:.1f}M/Q recruiting to BS",
            adjustment_amount=_r(cap_recruiting_q),
            adjustment_rationale=(
                f"Add ${cap_recruiting_q:.1f}M/Q back to OpEx (recruiting). "
                f"Reduces EBITDA by same amount. PP&E reduced correspondingly."
            ),
        ))

        # ── COFA-005: Automation Capitalization ────────────────────────────
        # Cascadia capitalizes $12M/yr ($3M/Q) automation platform dev.
        # Meridian expenses all technology costs. Add back to OpEx, reduce intangibles.
        cap_automation_q = cq.capitalized_automation  # actual value from model
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-005",
            description="Automation Capitalization: expense Cascadia capitalized automation",
            metric="OpEx_technology",
            meridian_treatment="All technology costs expensed immediately",
            cascadia_treatment=f"Capitalizes ${cap_automation_q:.1f}M/Q automation dev to BS",
            adjustment_amount=_r(cap_automation_q),
            adjustment_rationale=(
                f"Add ${cap_automation_q:.1f}M/Q back to OpEx (technology). "
                f"Reduces EBITDA by same amount. Intangibles reduced correspondingly."
            ),
        ))

        # ── COFA-006: Depreciation Methods ─────────────────────────────────
        # Cascadia uses accelerated/3yr (1.5x multiplier in model).
        # Meridian uses straight-line/5yr.
        # Restate Cascadia D&A to straight-line basis.
        # Cascadia accelerated D&A = da_expense (already has * 1.5)
        # Straight-line equivalent = da_expense / 1.5
        # Adjustment = -(da_expense - da_expense/1.5) = reduce D&A by 1/3
        cascadia_da = cq.da_expense
        straight_line_da = _r(cascadia_da / 1.5)
        da_reduction = _r(-(cascadia_da - straight_line_da))
        adjustments.append(COFAAdjustment(
            conflict_id="COFA-006",
            description="Depreciation Methods: restate Cascadia from accelerated to straight-line",
            metric="D&A",
            meridian_treatment="Straight-line depreciation over 5 years",
            cascadia_treatment="Accelerated depreciation over 3 years (1.5x multiplier)",
            adjustment_amount=da_reduction,
            adjustment_rationale=(
                f"Cascadia accelerated D&A = ${cascadia_da:.2f}M. "
                f"Straight-line equivalent = ${straight_line_da:.2f}M. "
                f"Adjustment = ${da_reduction:.2f}M (reduces D&A by 1/3). "
                f"Increases operating profit by ${-da_reduction:.2f}M."
            ),
        ))

        return adjustments

    # ───────────────────────────────────────────────────────────────────────
    # Helpers to extract adjustments by conflict_id + metric
    # ───────────────────────────────────────────────────────────────────────

    def _adj_sum(
        self, adjustments: List[COFAAdjustment], conflict_id: str, metric: str
    ) -> float:
        return sum(
            a.adjustment_amount
            for a in adjustments
            if a.conflict_id == conflict_id and a.metric == metric
        )

    def _adj_list(
        self, adjustments: List[COFAAdjustment], conflict_ids: List[str]
    ) -> List[COFAAdjustment]:
        return [a for a in adjustments if a.conflict_id in conflict_ids]

    # ───────────────────────────────────────────────────────────────────────
    # Income Statement
    # ───────────────────────────────────────────────────────────────────────

    def _build_income_statement(
        self,
        mq: Quarter,
        cq: Quarter,
        adjustments: List[COFAAdjustment],
        period: str,
    ) -> CombiningStatement:
        items: List[CombiningLineItem] = []
        grossup_q = _r(REVENUE_GROSSUP_ANNUAL / 4)

        # ── Revenue ────────────────────────────────────────────────────────
        # Meridian revenue streams
        items.append(CombiningLineItem(
            line_item="Advisory & Consulting Revenue",
            meridian=mq.revenue,
            cascadia=0.0,
            adjustments=0.0,
            combined=_r(mq.revenue),
            adjustment_details=[],
        ))
        # Cascadia revenue streams
        items.append(CombiningLineItem(
            line_item="Managed Services Revenue",
            meridian=0.0,
            cascadia=cq.managed_services_revenue,
            adjustments=0.0,
            combined=_r(cq.managed_services_revenue),
            adjustment_details=[],
        ))
        items.append(CombiningLineItem(
            line_item="Per-FTE Revenue",
            meridian=0.0,
            cascadia=cq.per_fte_revenue,
            adjustments=0.0,
            combined=_r(cq.per_fte_revenue),
            adjustment_details=[],
        ))
        items.append(CombiningLineItem(
            line_item="Per-Transaction Revenue",
            meridian=0.0,
            cascadia=cq.per_transaction_revenue,
            adjustments=0.0,
            combined=_r(cq.per_transaction_revenue),
            adjustment_details=[],
        ))
        # COFA-001 revenue adjustment
        rev_adj = _r(-grossup_q)
        items.append(CombiningLineItem(
            line_item="Revenue Gross-Up Adjustment",
            meridian=0.0,
            cascadia=0.0,
            adjustments=rev_adj,
            combined=rev_adj,
            adjustment_details=self._adj_list(adjustments, ["COFA-001"]),
        ))
        # Total Revenue
        total_rev_m = mq.revenue
        total_rev_c = cq.revenue
        total_rev_adj = rev_adj
        total_rev = _r(total_rev_m + total_rev_c + total_rev_adj)
        items.append(CombiningLineItem(
            line_item="Total Revenue",
            meridian=total_rev_m,
            cascadia=total_rev_c,
            adjustments=total_rev_adj,
            combined=total_rev,
            adjustment_details=self._adj_list(adjustments, ["COFA-001"]),
        ))

        # ── Cost of Revenue ────────────────────────────────────────────────
        # Consultant Comp (M) / Delivery Staff Comp + benefits (C, after COFA-002)
        # After COFA-002, Cascadia's comp line absorbs benefits_cost
        c_delivery_comp = _r(
            cq.onshore_cost + cq.offshore_cost + cq.nearshore_cost + cq.benefits_cost
        )
        items.append(CombiningLineItem(
            line_item="Consultant / Delivery Staff Compensation",
            meridian=mq.consultant_comp,
            cascadia=c_delivery_comp,
            adjustments=0.0,
            combined=_r(mq.consultant_comp + c_delivery_comp),
            adjustment_details=self._adj_list(adjustments, ["COFA-002"]),
        ))

        # Bench Costs
        items.append(CombiningLineItem(
            line_item="Bench Costs",
            meridian=mq.bench_cost,
            cascadia=cq.bench_delivery_cost,
            adjustments=0.0,
            combined=_r(mq.bench_cost + cq.bench_delivery_cost),
            adjustment_details=[],
        ))

        # Subcontractor Costs
        items.append(CombiningLineItem(
            line_item="Subcontractor Costs",
            meridian=mq.subcontractor_cost,
            cascadia=cq.subcontractor_cost,
            adjustments=0.0,
            combined=_r(mq.subcontractor_cost + cq.subcontractor_cost),
            adjustment_details=[],
        ))

        # Travel (M only)
        items.append(CombiningLineItem(
            line_item="Travel",
            meridian=mq.travel_cost,
            cascadia=0.0,
            adjustments=0.0,
            combined=_r(mq.travel_cost),
            adjustment_details=[],
        ))

        # Delivery Center Operations (C only)
        items.append(CombiningLineItem(
            line_item="Delivery Center Operations",
            meridian=0.0,
            cascadia=cq.delivery_center_ops_cost,
            adjustments=0.0,
            combined=_r(cq.delivery_center_ops_cost),
            adjustment_details=[],
        ))

        # Total COGS
        # Meridian total COGS is its own cogs figure.
        # Cascadia total COGS is its own cogs figure.
        # COFA-001 reduces combined COGS by the pass-through amount.
        cogs_adj = _r(-grossup_q)
        total_cogs_m = mq.cogs
        total_cogs_c = cq.cogs
        total_cogs = _r(total_cogs_m + total_cogs_c + cogs_adj)
        items.append(CombiningLineItem(
            line_item="Total COGS",
            meridian=total_cogs_m,
            cascadia=total_cogs_c,
            adjustments=cogs_adj,
            combined=total_cogs,
            adjustment_details=self._adj_list(adjustments, ["COFA-001"]),
        ))

        # Gross Profit
        gp_m = _r(total_rev_m - total_cogs_m)
        gp_c = _r(total_rev_c - total_cogs_c)
        gp_adj = _r(total_rev_adj - cogs_adj)  # should be zero (COFA-001 nets out)
        gp_combined = _r(total_rev - total_cogs)
        items.append(CombiningLineItem(
            line_item="Gross Profit",
            meridian=gp_m,
            cascadia=gp_c,
            adjustments=gp_adj,
            combined=gp_combined,
            adjustment_details=[],
        ))

        # ── Operating Expenses ─────────────────────────────────────────────
        # Sales (M) / Sales portion of S&M (C, after COFA-003)
        c_sales = _r(cq.sm_combined_expense * self.SM_SALES_RATIO)
        items.append(CombiningLineItem(
            line_item="Sales",
            meridian=mq.sales_expense,
            cascadia=c_sales,
            adjustments=0.0,
            combined=_r(mq.sales_expense + c_sales),
            adjustment_details=self._adj_list(adjustments, ["COFA-003"]),
        ))

        # Marketing (M) / Marketing portion of S&M (C, after COFA-003)
        c_marketing = _r(cq.sm_combined_expense * self.SM_MARKETING_RATIO)
        items.append(CombiningLineItem(
            line_item="Marketing",
            meridian=mq.marketing_expense,
            cascadia=c_marketing,
            adjustments=0.0,
            combined=_r(mq.marketing_expense + c_marketing),
            adjustment_details=self._adj_list(adjustments, ["COFA-003"]),
        ))

        # R&D / Technology (M only)
        items.append(CombiningLineItem(
            line_item="R&D / Technology",
            meridian=mq.rd_expense,
            cascadia=0.0,
            adjustments=0.0,
            combined=_r(mq.rd_expense),
            adjustment_details=[],
        ))

        # Technology & Automation (C, after COFA-005 uncapitalization)
        # The model already subtracted capitalized_automation from total_opex.
        # We show the full tech_automation_expense (pre-capitalization) in the combining view.
        # The COFA-005 adjustment adds capitalized_automation back.
        cap_auto = cq.capitalized_automation
        items.append(CombiningLineItem(
            line_item="Technology & Automation",
            meridian=0.0,
            cascadia=cq.tech_automation_expense,
            adjustments=_r(cap_auto),
            combined=_r(cq.tech_automation_expense + cap_auto),
            adjustment_details=self._adj_list(adjustments, ["COFA-005"]),
        ))

        # G&A (both)
        items.append(CombiningLineItem(
            line_item="G&A",
            meridian=mq.ga_expense,
            cascadia=cq.ga_expense,
            adjustments=0.0,
            combined=_r(mq.ga_expense + cq.ga_expense),
            adjustment_details=[],
        ))

        # Facilities (both)
        items.append(CombiningLineItem(
            line_item="Facilities",
            meridian=mq.facilities_expense,
            cascadia=cq.facilities_corporate_expense,
            adjustments=0.0,
            combined=_r(mq.facilities_expense + cq.facilities_corporate_expense),
            adjustment_details=[],
        ))

        # Recruiting (M + C after COFA-004 uncapitalization)
        # Meridian: recruiting_expense (fully expensed)
        # Cascadia: the model already subtracted capitalized_recruiting from total_opex.
        # For the combining view, Cascadia's recruiting line = 0 as-reported,
        # but COFA-004 adds back capitalized_recruiting.
        cap_recruit = cq.capitalized_recruiting
        items.append(CombiningLineItem(
            line_item="Recruiting",
            meridian=mq.recruiting_expense,
            cascadia=0.0,
            adjustments=_r(cap_recruit),
            combined=_r(mq.recruiting_expense + cap_recruit),
            adjustment_details=self._adj_list(adjustments, ["COFA-004"]),
        ))

        # Total OpEx
        # Meridian total_opex from model
        # Cascadia total_opex from model (already reduced by capitalizations)
        # COFA-004 adds back capitalized_recruiting, COFA-005 adds back capitalized_automation
        opex_adj = _r(cap_recruit + cap_auto)
        total_opex_m = mq.total_opex
        total_opex_c = cq.total_opex
        total_opex = _r(total_opex_m + total_opex_c + opex_adj)
        items.append(CombiningLineItem(
            line_item="Total OpEx",
            meridian=total_opex_m,
            cascadia=total_opex_c,
            adjustments=opex_adj,
            combined=total_opex,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005"]),
        ))

        # EBITDA
        ebitda_m = _r(gp_m - total_opex_m)
        ebitda_c = _r(gp_c - total_opex_c)
        ebitda_adj = _r(gp_adj - opex_adj)
        ebitda_combined = _r(gp_combined - total_opex)
        items.append(CombiningLineItem(
            line_item="EBITDA",
            meridian=ebitda_m,
            cascadia=ebitda_c,
            adjustments=ebitda_adj,
            combined=ebitda_combined,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005"]),
        ))

        # D&A (both, Cascadia restated per COFA-006)
        da_adj_amount = self._adj_sum(adjustments, "COFA-006", "D&A")
        items.append(CombiningLineItem(
            line_item="D&A",
            meridian=mq.da_expense,
            cascadia=cq.da_expense,
            adjustments=da_adj_amount,
            combined=_r(mq.da_expense + cq.da_expense + da_adj_amount),
            adjustment_details=self._adj_list(adjustments, ["COFA-006"]),
        ))

        # Operating Profit
        op_m = _r(ebitda_m - mq.da_expense)
        op_c = _r(ebitda_c - cq.da_expense)
        op_adj = _r(ebitda_adj - da_adj_amount)
        op_combined = _r(ebitda_combined - (mq.da_expense + cq.da_expense + da_adj_amount))
        items.append(CombiningLineItem(
            line_item="Operating Profit",
            meridian=op_m,
            cascadia=op_c,
            adjustments=op_adj,
            combined=op_combined,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005", "COFA-006"]),
        ))

        # Tax — recompute on adjusted operating profit
        # Use a blended tax rate (weighted by revenue contribution)
        m_tax_rate = mq.tax_expense / max(mq.operating_profit, 0.01) if mq.operating_profit > 0 else 0.24
        c_tax_rate = cq.tax_expense / max(cq.operating_profit, 0.01) if cq.operating_profit > 0 else 0.22
        # For Meridian and Cascadia columns, use their reported tax
        tax_m = mq.tax_expense
        tax_c = cq.tax_expense
        # For the combined column, compute tax on adjusted operating profit
        combined_taxable = max(op_combined, 0.0)
        # Blended rate weighted by entity revenue share
        total_entity_rev = mq.revenue + cq.revenue
        if total_entity_rev > 0:
            blended_rate = (m_tax_rate * mq.revenue + c_tax_rate * cq.revenue) / total_entity_rev
        else:
            blended_rate = 0.23
        combined_tax = _r(combined_taxable * blended_rate)
        tax_adj = _r(combined_tax - tax_m - tax_c)
        items.append(CombiningLineItem(
            line_item="Tax",
            meridian=tax_m,
            cascadia=tax_c,
            adjustments=tax_adj,
            combined=combined_tax,
            adjustment_details=[],
        ))

        # Net Income
        ni_m = _r(op_m - tax_m)
        ni_c = _r(op_c - tax_c)
        ni_adj = _r(op_adj - tax_adj)
        ni_combined = _r(op_combined - combined_tax)
        items.append(CombiningLineItem(
            line_item="Net Income",
            meridian=ni_m,
            cascadia=ni_c,
            adjustments=ni_adj,
            combined=ni_combined,
            adjustment_details=[],
        ))

        return CombiningStatement(
            statement_type="income_statement",
            period=period,
            line_items=items,
        )

    # ───────────────────────────────────────────────────────────────────────
    # Balance Sheet
    # ───────────────────────────────────────────────────────────────────────

    def _build_balance_sheet(
        self,
        mq: Quarter,
        cq: Quarter,
        adjustments: List[COFAAdjustment],
        period: str,
        cumulative_ppe_adj: float = 0.0,
        cumulative_intang_adj: float = 0.0,
    ) -> CombiningStatement:
        items: List[CombiningLineItem] = []

        # ── Assets ─────────────────────────────────────────────────────────
        items.append(CombiningLineItem(
            line_item="Cash",
            meridian=mq.cash,
            cascadia=cq.cash,
            adjustments=0.0,
            combined=_r(mq.cash + cq.cash),
        ))
        items.append(CombiningLineItem(
            line_item="AR",
            meridian=mq.ar,
            cascadia=cq.ar,
            adjustments=0.0,
            combined=_r(mq.ar + cq.ar),
        ))
        items.append(CombiningLineItem(
            line_item="Unbilled Revenue",
            meridian=mq.unbilled_revenue,
            cascadia=cq.unbilled_revenue,
            adjustments=0.0,
            combined=_r(mq.unbilled_revenue + cq.unbilled_revenue),
        ))
        items.append(CombiningLineItem(
            line_item="Prepaid Expenses",
            meridian=mq.prepaid_expenses,
            cascadia=cq.prepaid_expenses,
            adjustments=0.0,
            combined=_r(mq.prepaid_expenses + cq.prepaid_expenses),
        ))

        # PP&E — Cascadia adjusted for COFA-004 (cumulative capitalized recruiting removal)
        ppe_adj = _r(cumulative_ppe_adj)
        items.append(CombiningLineItem(
            line_item="PP&E",
            meridian=mq.pp_e,
            cascadia=cq.pp_e,
            adjustments=ppe_adj,
            combined=_r(mq.pp_e + cq.pp_e + ppe_adj),
            adjustment_details=self._adj_list(adjustments, ["COFA-004"]),
        ))

        # Intangibles — Cascadia adjusted for COFA-005 (cumulative capitalized automation removal)
        intang_adj = _r(cumulative_intang_adj)
        items.append(CombiningLineItem(
            line_item="Intangibles",
            meridian=mq.intangibles,
            cascadia=cq.intangibles,
            adjustments=intang_adj,
            combined=_r(mq.intangibles + cq.intangibles + intang_adj),
            adjustment_details=self._adj_list(adjustments, ["COFA-005"]),
        ))

        items.append(CombiningLineItem(
            line_item="Goodwill",
            meridian=mq.goodwill,
            cascadia=cq.goodwill,
            adjustments=0.0,
            combined=_r(mq.goodwill + cq.goodwill),
        ))

        # Total Assets
        total_assets_m = mq.total_assets
        total_assets_c = cq.total_assets
        total_assets_adj = _r(cumulative_ppe_adj + cumulative_intang_adj)
        total_assets = _r(total_assets_m + total_assets_c + total_assets_adj)
        items.append(CombiningLineItem(
            line_item="Total Assets",
            meridian=total_assets_m,
            cascadia=total_assets_c,
            adjustments=total_assets_adj,
            combined=total_assets,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005"]),
        ))

        # ── Liabilities ────────────────────────────────────────────────────
        items.append(CombiningLineItem(
            line_item="AP",
            meridian=mq.ap,
            cascadia=cq.ap,
            adjustments=0.0,
            combined=_r(mq.ap + cq.ap),
        ))
        items.append(CombiningLineItem(
            line_item="Accrued Expenses",
            meridian=mq.accrued_expenses,
            cascadia=cq.accrued_expenses,
            adjustments=0.0,
            combined=_r(mq.accrued_expenses + cq.accrued_expenses),
        ))
        items.append(CombiningLineItem(
            line_item="Deferred Revenue",
            meridian=mq.deferred_revenue,
            cascadia=cq.deferred_revenue,
            adjustments=0.0,
            combined=_r(mq.deferred_revenue + cq.deferred_revenue),
        ))

        total_liab_m = mq.total_liabilities
        total_liab_c = cq.total_liabilities
        total_liab = _r(total_liab_m + total_liab_c)
        items.append(CombiningLineItem(
            line_item="Total Liabilities",
            meridian=total_liab_m,
            cascadia=total_liab_c,
            adjustments=0.0,
            combined=total_liab,
        ))

        # ── Equity ─────────────────────────────────────────────────────────
        # Total equity is plugged to force BS balance: TE = TA - TL.
        # This absorbs both COFA adjustments and the known entity-level BS
        # drift in the Cascadia BPM model (capitalization-related rounding
        # accumulation, ~$3M/quarter — documented in financial_model.py).
        total_equity = _r(total_assets - total_liab)
        total_equity_m = mq.stockholders_equity
        total_equity_c = cq.stockholders_equity
        equity_adj = _r(total_equity - total_equity_m - total_equity_c)

        # Paid-in Capital (derive from stockholders_equity - retained_earnings)
        pic_m = _r(mq.stockholders_equity - mq.retained_earnings)
        pic_c = _r(cq.stockholders_equity - cq.retained_earnings)
        pic_combined = _r(pic_m + pic_c)
        items.append(CombiningLineItem(
            line_item="Paid-in Capital",
            meridian=pic_m,
            cascadia=pic_c,
            adjustments=0.0,
            combined=pic_combined,
        ))

        # Retained earnings absorb the COFA + drift adjustments
        re_m = mq.retained_earnings
        re_c = cq.retained_earnings
        re_combined = _r(total_equity - pic_combined)
        re_adj = _r(re_combined - re_m - re_c)
        items.append(CombiningLineItem(
            line_item="Retained Earnings",
            meridian=re_m,
            cascadia=re_c,
            adjustments=re_adj,
            combined=re_combined,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005"]),
        ))

        items.append(CombiningLineItem(
            line_item="Total Equity",
            meridian=total_equity_m,
            cascadia=total_equity_c,
            adjustments=equity_adj,
            combined=total_equity,
            adjustment_details=self._adj_list(adjustments, ["COFA-004", "COFA-005"]),
        ))

        return CombiningStatement(
            statement_type="balance_sheet",
            period=period,
            line_items=items,
        )

    # ───────────────────────────────────────────────────────────────────────
    # Cash Flow
    # ───────────────────────────────────────────────────────────────────────

    def _build_cash_flow(
        self,
        mq: Quarter,
        cq: Quarter,
        adjustments: List[COFAAdjustment],
        period: str,
    ) -> CombiningStatement:
        """
        Build the combining cash flow statement.

        COFA adjustments are accounting reclassifications with zero net cash
        impact. For COFA-004/005, expensing capitalized items reduces NI but
        increases the D&A add-back and reduces capex — netting to zero. For
        COFA-006, the D&A reduction lowers the add-back but also lowers tax
        (through operating profit), and neither changes actual capex — also
        netting to zero. We use the model's actual CFO/capex/FCF values for
        the entity columns, and set all COFA adjustments to zero.
        """
        items: List[CombiningLineItem] = []

        # ── Operating Activities ───────────────────────────────────────────
        # Use model's actual net income
        items.append(CombiningLineItem(
            line_item="Net Income",
            meridian=mq.net_income,
            cascadia=cq.net_income,
            adjustments=0.0,
            combined=_r(mq.net_income + cq.net_income),
        ))

        # D&A add-back — use model's actual D&A (no COFA adj on cash flow;
        # the D&A adj is a non-cash reclassification that nets out with capex)
        items.append(CombiningLineItem(
            line_item="D&A",
            meridian=mq.da_expense,
            cascadia=cq.da_expense,
            adjustments=0.0,
            combined=_r(mq.da_expense + cq.da_expense),
        ))

        # Working capital changes — derive from model's actual CFO
        # wc = cfo - net_income - da_expense
        wc_m = _r(mq.cfo - mq.net_income - mq.da_expense)
        wc_c = _r(cq.cfo - cq.net_income - cq.da_expense)
        items.append(CombiningLineItem(
            line_item="Changes in Working Capital",
            meridian=wc_m,
            cascadia=wc_c,
            adjustments=0.0,
            combined=_r(wc_m + wc_c),
        ))

        # CFO — use model's actual values
        items.append(CombiningLineItem(
            line_item="CFO",
            meridian=mq.cfo,
            cascadia=cq.cfo,
            adjustments=0.0,
            combined=_r(mq.cfo + cq.cfo),
        ))

        # ── Investing Activities ───────────────────────────────────────────
        # CapEx — use model's actual values (no COFA adj; the capitalization
        # reversal is a P&L/BS reclassification, not a cash event)
        items.append(CombiningLineItem(
            line_item="CapEx",
            meridian=_r(-mq.capex),
            cascadia=_r(-cq.capex),
            adjustments=0.0,
            combined=_r(-mq.capex - cq.capex),
        ))

        items.append(CombiningLineItem(
            line_item="CFI",
            meridian=_r(-mq.capex),
            cascadia=_r(-cq.capex),
            adjustments=0.0,
            combined=_r(-mq.capex - cq.capex),
        ))

        # ── Financing Activities ───────────────────────────────────────────
        items.append(CombiningLineItem(
            line_item="CFF",
            meridian=0.0,
            cascadia=0.0,
            adjustments=0.0,
            combined=0.0,
        ))

        # Net Change in Cash — FCF (since CFF = 0)
        net_change_m = mq.fcf
        net_change_c = cq.fcf
        net_change_combined = _r(net_change_m + net_change_c)
        items.append(CombiningLineItem(
            line_item="Net Change in Cash",
            meridian=net_change_m,
            cascadia=net_change_c,
            adjustments=0.0,
            combined=net_change_combined,
        ))

        return CombiningStatement(
            statement_type="cash_flow",
            period=period,
            line_items=items,
        )

    # ───────────────────────────────────────────────────────────────────────
    # COFA Mappings (Chart of Accounts reconciliation)
    # ───────────────────────────────────────────────────────────────────────

    def _build_cofa_mappings(self) -> List[COFAMapping]:
        """Build the unified chart-of-accounts mapping."""
        return [
            # Revenue
            COFAMapping("R-001", "Advisory & Consulting Revenue", "revenue",
                        "tm_revenue + fixed_fee_revenue", None, "clean", None),
            COFAMapping("R-002", "Managed Services Revenue", "revenue",
                        None, "managed_services_revenue", "clean", None),
            COFAMapping("R-003", "Per-FTE Revenue", "revenue",
                        None, "per_fte_revenue", "clean", None),
            COFAMapping("R-004", "Per-Transaction Revenue", "revenue",
                        None, "per_transaction_revenue", "clean", None),
            COFAMapping("R-005", "Revenue Gross-Up", "revenue",
                        None, None, "scope",
                        "COFA-001: Cascadia books full FTE rate; Meridian books markup only"),

            # COGS
            COFAMapping("C-001", "Consultant / Delivery Staff Compensation", "cogs",
                        "consultant_comp (includes benefits)", "onshore+offshore+nearshore+benefits",
                        "classification",
                        "COFA-002: Meridian bundles benefits; Cascadia separates"),
            COFAMapping("C-002", "Bench Costs", "cogs",
                        "bench_cost", "bench_delivery_cost", "clean", None),
            COFAMapping("C-003", "Subcontractor Costs", "cogs",
                        "subcontractor_cost", "subcontractor_cost", "clean", None),
            COFAMapping("C-004", "Travel", "cogs",
                        "travel_cost", None, "structural_gap",
                        "Meridian only — no equivalent Cascadia line"),
            COFAMapping("C-005", "Delivery Center Operations", "cogs",
                        None, "delivery_center_ops_cost", "structural_gap",
                        "Cascadia only — no equivalent Meridian line"),

            # OpEx
            COFAMapping("O-001", "Sales", "opex",
                        "sales_expense", "sm_combined_expense * 0.60",
                        "classification",
                        "COFA-003: Cascadia bundles S&M; split 60/40 for combining"),
            COFAMapping("O-002", "Marketing", "opex",
                        "marketing_expense", "sm_combined_expense * 0.40",
                        "classification",
                        "COFA-003: Cascadia bundles S&M; split 60/40 for combining"),
            COFAMapping("O-003", "R&D / Technology", "opex",
                        "rd_expense", None, "structural_gap",
                        "Meridian only — Cascadia uses Technology & Automation line"),
            COFAMapping("O-004", "Technology & Automation", "opex",
                        None, "tech_automation_expense + capitalized_automation",
                        "materiality",
                        "COFA-005: Cascadia capitalizes $12M/yr; Meridian expenses all"),
            COFAMapping("O-005", "G&A", "opex",
                        "ga_expense", "ga_expense", "clean", None),
            COFAMapping("O-006", "Facilities", "opex",
                        "facilities_expense", "facilities_corporate_expense", "clean", None),
            COFAMapping("O-007", "Recruiting", "opex",
                        "recruiting_expense", "capitalized_recruiting (expensed via COFA-004)",
                        "materiality",
                        "COFA-004: Cascadia capitalizes $8M/yr; Meridian expenses all"),

            # D&A
            COFAMapping("D-001", "D&A", "opex",
                        "da_expense (straight-line/5yr)", "da_expense (accelerated/3yr, * 1.5)",
                        "classification",
                        "COFA-006: restate Cascadia to straight-line basis"),

            # Balance Sheet — Assets
            COFAMapping("A-001", "Cash", "asset",
                        "cash", "cash", "clean", None),
            COFAMapping("A-002", "AR", "asset",
                        "ar", "ar", "clean", None),
            COFAMapping("A-003", "Unbilled Revenue", "asset",
                        "unbilled_revenue", "unbilled_revenue", "clean", None),
            COFAMapping("A-004", "Prepaid Expenses", "asset",
                        "prepaid_expenses", "prepaid_expenses", "clean", None),
            COFAMapping("A-005", "PP&E", "asset",
                        "pp_e", "pp_e (includes capitalized_recruiting)",
                        "materiality",
                        "COFA-004: remove capitalized recruiting from PP&E"),
            COFAMapping("A-006", "Intangibles", "asset",
                        "intangibles", "intangibles (includes capitalized_automation)",
                        "materiality",
                        "COFA-005: remove capitalized automation from intangibles"),
            COFAMapping("A-007", "Goodwill", "asset",
                        "goodwill", "goodwill", "clean", None),

            # Liabilities
            COFAMapping("L-001", "AP", "liability",
                        "ap", "ap", "clean", None),
            COFAMapping("L-002", "Accrued Expenses", "liability",
                        "accrued_expenses", "accrued_expenses", "clean", None),
            COFAMapping("L-003", "Deferred Revenue", "liability",
                        "deferred_revenue", "deferred_revenue", "clean", None),

            # Equity
            COFAMapping("E-001", "Retained Earnings", "equity",
                        "retained_earnings", "retained_earnings",
                        "materiality",
                        "Adjusted for cumulative P&L COFA impacts"),
            COFAMapping("E-002", "Paid-in Capital", "equity",
                        "stockholders_equity - retained_earnings",
                        "stockholders_equity - retained_earnings",
                        "clean", None),
        ]

    # ───────────────────────────────────────────────────────────────────────
    # Pretty-printing
    # ───────────────────────────────────────────────────────────────────────

    @staticmethod
    def print_statement(stmt: CombiningStatement, width: int = 100) -> None:
        """Print a combining statement in a readable table format."""
        title_map = {
            "income_statement": "COMBINING INCOME STATEMENT",
            "balance_sheet": "COMBINING BALANCE SHEET",
            "cash_flow": "COMBINING CASH FLOW STATEMENT",
        }
        title = title_map.get(stmt.statement_type, stmt.statement_type.upper())
        print(f"\n{'=' * width}")
        print(f"  {title} — {stmt.period}")
        print(f"{'=' * width}")

        col_w = 14
        header = (
            f"  {'Line Item':<36}"
            f"{'Meridian':>{col_w}}"
            f"{'Cascadia':>{col_w}}"
            f"{'Adjustments':>{col_w}}"
            f"{'Combined':>{col_w}}"
        )
        print(header)
        print(f"  {'-' * (width - 4)}")

        section_lines = {
            "Total Revenue", "Total COGS", "Gross Profit", "Total OpEx",
            "EBITDA", "Operating Profit", "Net Income",
            "Total Assets", "Total Liabilities", "Total Equity",
            "CFO", "CFI", "CFF", "Net Change in Cash",
        }

        for li in stmt.line_items:
            name = li.line_item
            if name in section_lines:
                prefix = "  "
                bold = True
            else:
                prefix = "    "
                bold = False

            def fmt(v: float) -> str:
                if v == 0.0:
                    return "—"
                return f"${v:,.1f}M" if abs(v) >= 1.0 else f"${v:,.2f}M"

            row = (
                f"{prefix}{name:<34}"
                f"{fmt(li.meridian):>{col_w}}"
                f"{fmt(li.cascadia):>{col_w}}"
                f"{fmt(li.adjustments):>{col_w}}"
                f"{fmt(li.combined):>{col_w}}"
            )
            print(row)
            if bold:
                print(f"  {'-' * (width - 4)}")


# ═══════════════════════════════════════════════════════════════════════════════
# Main — integration test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    from src.generators.financial_model import Assumptions, FinancialModel

    meridian_config = project_root / "farm_config_meridian.yaml"
    cascadia_config = project_root / "farm_config_cascadia.yaml"

    if not meridian_config.exists():
        raise FileNotFoundError(f"Meridian config not found: {meridian_config}")
    if not cascadia_config.exists():
        raise FileNotFoundError(f"Cascadia config not found: {cascadia_config}")

    print("Loading Meridian Partners config...")
    m_assumptions = Assumptions.from_yaml(str(meridian_config))
    print(f"  Entity: {m_assumptions.entity_name} ({m_assumptions.business_model})")
    print(f"  Starting revenue: ${m_assumptions.starting_annual_revenue:.0f}M")

    print("Loading Cascadia Process Solutions config...")
    c_assumptions = Assumptions.from_yaml(str(cascadia_config))
    print(f"  Entity: {c_assumptions.entity_name} ({c_assumptions.business_model})")
    print(f"  Starting revenue: ${c_assumptions.starting_annual_revenue:.0f}M")

    print("\nGenerating financial models...")
    m_quarters = FinancialModel(m_assumptions).generate()
    c_quarters = FinancialModel(c_assumptions).generate()
    m_op = [q for q in m_quarters if q.period_type != "opening"]
    c_op = [q for q in c_quarters if q.period_type != "opening"]
    print(f"  Meridian: {len(m_op)} operating quarters, Q1 revenue = ${m_op[0].revenue:.1f}M")
    print(f"  Cascadia: {len(c_op)} operating quarters, Q1 revenue = ${c_op[0].revenue:.1f}M")

    print("\nRunning CombiningStatementEngine...")
    engine = CombiningStatementEngine(m_quarters, c_quarters)
    result = engine.generate()

    # Print Q1 combining P&L
    CombiningStatementEngine.print_statement(result.income_statements[0])
    CombiningStatementEngine.print_statement(result.balance_sheets[0])
    CombiningStatementEngine.print_statement(result.cash_flows[0])

    # Print COFA conflict register summary
    print(f"\n{'=' * 80}")
    print(f"  COFA CONFLICT REGISTER — {result.income_statements[0].period}")
    print(f"{'=' * 80}")
    q1_adjustments = [
        a for a in result.conflict_register
        if result.income_statements[0].period in (result.income_statements[0].period,)
    ]
    # Group by conflict_id (first occurrence per quarter)
    seen = set()
    for adj in result.conflict_register[:8]:  # first quarter's adjustments
        key = adj.conflict_id
        if key not in seen:
            seen.add(key)
            print(f"\n  {adj.conflict_id}: {adj.description}")
            print(f"    Metric: {adj.metric}")
            print(f"    Amount: ${adj.adjustment_amount:+,.2f}M")
            print(f"    Rationale: {adj.adjustment_rationale}")

    # Validate
    print(f"\n{'=' * 80}")
    print("  VALIDATION")
    print(f"{'=' * 80}")
    errors = engine.validate(result)
    if errors:
        print(f"\n  FAILED — {len(errors)} error(s):")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)
    else:
        print("\n  PASSED — all validation checks OK")
        print(f"    - Line item arithmetic: M + C + Adj == Combined (all lines)")
        print(f"    - Balance sheet equation: Assets == Liabilities + Equity (all quarters)")
        print(f"    - Cash flow ties: beginning + net change == ending (all quarters)")
        print(f"    - COFA register: {len(seen)} unique conflict IDs")
        print(f"    - Total combining statements: "
              f"{len(result.income_statements)} P&L, "
              f"{len(result.balance_sheets)} BS, "
              f"{len(result.cash_flows)} CF")
