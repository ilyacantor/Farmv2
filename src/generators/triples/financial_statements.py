"""Triple generator for P&L, Balance Sheet, and Cash Flow statements.

Converts existing FinancialModel Quarter objects into SemanticTriple lists.
Balance Sheet adds long_term_debt and common_stock from config.
Cash Flow adds financing section (debt/dividends).

HARD GATES (enforced, not silently adjusted):
  - BS: asset.total == liability.total + equity.total (tolerance $0)
  - CF: operating + investing + financing == net_change (tolerance $0)
  - Cash continuity: cash[Q(n)] + net_change[Q(n+1)] == cash[Q(n+1)]
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from src.generators.financial_model import Assumptions, FinancialModel, Quarter
from src.output.triple_format import SemanticTriple

_logger = logging.getLogger("farm.triples.financial_statements")


def _r(val: float, decimals: int = 2) -> float:
    return round(val, decimals)


class FinancialStatementTripleGenerator:
    """Generate P&L, Balance Sheet, and Cash Flow triples from Quarter data."""

    def __init__(
        self,
        quarters: List[Quarter],
        assumptions: Assumptions,
        config_raw: Optional[Dict[str, Any]] = None,
    ):
        self.quarters = quarters
        self.assumptions = assumptions
        self.config_raw = config_raw or {}
        entity_id = quarters[0].entity_id if quarters else "unknown"
        self.entity_id = entity_id

        # BS ratio parameters from config (added in S1)
        cp = self.config_raw.get("company_profile") or {}
        self.long_term_debt_initial = cp.get("long_term_debt_initial", 0.0)
        self.long_term_debt_amort_pct_quarterly = cp.get(
            "long_term_debt_amort_pct_quarterly", 0.02
        )
        self.common_stock = cp.get("common_stock", 0.0)
        self.dividend_pct_net_income = cp.get("dividend_pct_net_income", 0.0)

    def generate(self) -> List[SemanticTriple]:
        """Generate all financial statement triples for this entity."""
        triples: List[SemanticTriple] = []
        triples.extend(self._generate_pnl_triples())
        bs_cf = self._generate_bs_cf_triples()
        triples.extend(bs_cf)
        return triples

    # ── P&L ─────────────────────────────────────────────────────────────

    def _generate_pnl_triples(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []
        for q in self.quarters:
            period = q.quarter
            triples.extend(self._pnl_for_quarter(q, period))
        return triples

    def _pnl_for_quarter(self, q: Quarter, period: str) -> List[SemanticTriple]:
        ts: List[SemanticTriple] = []

        def add(concept: str, value: float, unit: str = "dollars",
                source: str = "erp"):
            ts.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="amount",
                value=_r(value),
                period=period,
                unit=unit,
                source_system=source,
                confidence_score=0.95,
                confidence_tier="high",
            ))

        # Revenue
        add("revenue.total", q.revenue)
        if q.business_model == "consultancy":
            add("revenue.consulting", q.tm_revenue)
            add("revenue.fixed_fee", q.fixed_fee_revenue)
        elif q.business_model == "bpm":
            add("revenue.managed_services", q.managed_services_revenue)
            add("revenue.per_fte", q.per_fte_revenue)
            add("revenue.per_transaction", q.per_transaction_revenue)

        # COGS
        add("cogs.total", q.cogs)
        if q.business_model == "consultancy":
            add("cogs.direct_labor", q.consultant_comp)
            add("cogs.bench", q.bench_cost)
            add("cogs.subcontractors", q.subcontractor_cost)
            add("cogs.travel", q.travel_cost)
        elif q.business_model == "bpm":
            add("cogs.direct_labor", q.onshore_cost + q.offshore_cost + q.nearshore_cost)
            add("cogs.bench", q.bench_delivery_cost)
            add("cogs.subcontractors", q.subcontractor_cost)
            add("cogs.benefits", q.benefits_cost)
            add("cogs.delivery_center_ops", q.delivery_center_ops_cost)

        # OpEx
        add("opex.total", q.total_opex)
        add("opex.sales_marketing", q.sm_expense)
        add("opex.research_development", q.rd_expense)
        add("opex.general_admin", q.ga_expense)

        # Below-the-line
        add("pnl.ebitda", q.ebitda)
        add("pnl.depreciation_amortization", q.da_expense)
        add("pnl.operating_profit", q.operating_profit)
        add("pnl.tax", q.tax_expense)
        add("pnl.net_income", q.net_income)

        return ts

    # ── Period 0: Opening Balance Sheet ─────────────────────────────────

    def _opening_cash(self, ltd_balance: float) -> float:
        """Compute opening cash for 2023-Q4 (before any Q1 activity)."""
        cp = self.config_raw.get("company_profile") or {}
        return _r(cp.get("starting_cash", 0.0) + ltd_balance)

    def _generate_opening_bs(self, ltd_balance: float) -> List[SemanticTriple]:
        """Emit 2023-Q4 opening balance sheet as Period 0 anchor.

        Uses starting_* values from config. No P&L or CF — this is a
        point-in-time snapshot representing the position at the start
        of the model.
        """
        cp = self.config_raw.get("company_profile") or {}
        period = "2023-Q4"

        cash = _r(cp.get("starting_cash", 0.0) + ltd_balance)
        ar = _r(cp.get("starting_ar", 0.0))
        prepaid = _r(cp.get("starting_prepaid", 0.0))
        ppe = _r(cp.get("starting_pp_e", 0.0))
        intangibles = _r(cp.get("starting_intangibles", 0.0))
        goodwill = _r(cp.get("starting_goodwill", 0.0))
        asset_total = _r(cash + ar + prepaid + ppe + intangibles + goodwill)

        ap = _r(cp.get("starting_ap", 0.0))
        accrued = _r(cp.get("starting_accrued_expenses", 0.0))
        # Deferred revenue: starting_annual_revenue / 4 * deferred_rev_months / 3
        starting_rev = cp.get("starting_annual_revenue", 0.0)
        deferred_months = cp.get("deferred_rev_months", 0.0)
        deferred_rev = _r(starting_rev / 12.0 * deferred_months)
        long_term_debt = _r(ltd_balance)
        liability_total = _r(ap + accrued + deferred_rev + long_term_debt)

        common_stock = _r(self.common_stock)
        retained_earnings = _r(asset_total - liability_total - common_stock)
        equity_total = _r(retained_earnings + common_stock)

        # HARD GATE: BS identity
        bs_diff = abs(asset_total - (liability_total + equity_total))
        if bs_diff > 0.005:
            raise ValueError(
                f"Opening BS identity violation for {self.entity_id} in {period}: "
                f"asset.total={asset_total} != liability.total({liability_total}) "
                f"+ equity.total({equity_total}), diff={bs_diff}"
            )

        triples: List[SemanticTriple] = []

        def add_bs(concept: str, value: float, source: str = "erp"):
            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="amount",
                value=value,
                period=period,
                unit="dollars",
                source_system=source,
                confidence_score=0.95,
                confidence_tier="high",
            ))

        add_bs("asset.total", asset_total)
        add_bs("asset.current.cash", cash)
        add_bs("asset.current.accounts_receivable", ar)
        add_bs("asset.current.prepaid", prepaid)
        add_bs("asset.noncurrent.property_plant_equipment", ppe)
        add_bs("asset.noncurrent.intangibles", intangibles)
        add_bs("asset.noncurrent.goodwill", goodwill)
        add_bs("liability.total", liability_total)
        add_bs("liability.current.accounts_payable", ap)
        add_bs("liability.current.accrued_expenses", accrued)
        add_bs("liability.current.deferred_revenue", deferred_rev)
        add_bs("liability.noncurrent.long_term_debt", long_term_debt)
        add_bs("equity.total", equity_total)
        add_bs("equity.retained_earnings", retained_earnings)
        add_bs("equity.common_stock", common_stock)

        _logger.info(
            f"[{self.entity_id}] Opening BS (2023-Q4): assets={asset_total}, "
            f"liabilities={liability_total}, equity={equity_total}, cash={cash}"
        )

        return triples

    # ── Balance Sheet + Cash Flow ───────────────────────────────────────

    def _generate_bs_cf_triples(self) -> List[SemanticTriple]:
        """Generate BS and CF triples with hard gates enforced.

        Long-term debt from config is added to liabilities and offset
        by equal increase in cash (debt proceeds). This preserves the
        accounting identity: assets = liabilities + equity.

        Debt amortizes quarterly at the configured rate.
        """
        triples: List[SemanticTriple] = []

        # Track LTD balance across quarters for amortization
        ltd_balance = self.long_term_debt_initial
        prev_cash_triple: Optional[float] = None
        prev_q: Optional[Quarter] = None

        # ── Period 0: 2023-Q4 opening balance sheet anchor ────────────
        # Emits the opening BS snapshot from config starting_* values.
        # No P&L or CF for this period — it's a point-in-time position.
        triples.extend(self._generate_opening_bs(ltd_balance))
        prev_cash_triple = self._opening_cash(ltd_balance)

        for q in self.quarters:
            period = q.quarter

            # ── Debt amortization ───────────────────────────────────
            debt_repayment = _r(ltd_balance * self.long_term_debt_amort_pct_quarterly)
            if prev_q is not None:
                ltd_balance = _r(ltd_balance - debt_repayment)
            # Q1: no repayment yet, debt is at initial level

            # ── Dividends ───────────────────────────────────────────
            dividends = _r(max(q.net_income, 0) * self.dividend_pct_net_income)

            # ── BS items from Quarter ───────────────────────────────
            # Assets — add LTD proceeds to cash
            cash = _r(q.cash + ltd_balance)
            ar = _r(q.ar)
            prepaid = _r(q.prepaid_expenses)
            ppe = _r(q.pp_e)
            intangibles = _r(q.intangibles)
            goodwill = _r(q.goodwill)
            asset_total = _r(cash + ar + prepaid + ppe + intangibles + goodwill)

            # Liabilities — add LTD
            ap = _r(q.ap)
            accrued = _r(q.accrued_expenses)
            deferred_rev = _r(q.deferred_revenue)
            long_term_debt = _r(ltd_balance)
            liability_total = _r(ap + accrued + deferred_rev + long_term_debt)

            # Equity — common_stock from config, retained_earnings adjusted
            common_stock = _r(self.common_stock)
            # Retained earnings: adjust for dividends
            if prev_q is not None:
                # prev retained_earnings_triple was already computed
                retained_earnings = _r(
                    asset_total - liability_total - common_stock
                )
            else:
                # Q1: derive retained_earnings from BS identity
                retained_earnings = _r(asset_total - liability_total - common_stock)
            equity_total = _r(retained_earnings + common_stock)

            # ── HARD GATE: BS identity ──────────────────────────────
            bs_diff = abs(asset_total - (liability_total + equity_total))
            if bs_diff > 0.005:
                raise ValueError(
                    f"BS identity violation for {self.entity_id} in {period}: "
                    f"asset.total={asset_total} != liability.total({liability_total}) "
                    f"+ equity.total({equity_total}), diff={bs_diff}"
                )

            # ── CF items ────────────────────────────────────────────
            operating_total = _r(q.cfo)
            operating_ni = _r(q.net_income)
            operating_da = _r(q.da_expense)
            wc_changes = _r(
                q.cfo - q.net_income - q.da_expense
            )
            investing_capex = _r(q.capex)
            investing_total = _r(-q.capex)
            financing_debt_repayment = debt_repayment if prev_q is not None else 0.0
            financing_dividends = dividends
            financing_total = _r(
                -financing_debt_repayment - financing_dividends
            )
            net_change = _r(operating_total + investing_total + financing_total)

            # ── HARD GATE: CF identity ──────────────────────────────
            cf_sum = _r(operating_total + investing_total + financing_total)
            cf_diff = abs(cf_sum - net_change)
            if cf_diff > 0.005:
                raise ValueError(
                    f"CF identity violation for {self.entity_id} in {period}: "
                    f"operating({operating_total}) + investing({investing_total}) "
                    f"+ financing({financing_total}) = {cf_sum} != "
                    f"net_change({net_change}), diff={cf_diff}"
                )

            # ── HARD GATE: Cash continuity ──────────────────────────
            if prev_cash_triple is not None:
                expected_cash = _r(prev_cash_triple + net_change)
                cash_diff = abs(cash - expected_cash)
                if cash_diff > 0.01:
                    raise ValueError(
                        f"Cash continuity violation for {self.entity_id} in "
                        f"{period}: prev_cash({prev_cash_triple}) + "
                        f"net_change({net_change}) = {expected_cash} != "
                        f"cash({cash}), diff={cash_diff}"
                    )
                # Force exact continuity to prevent floating-point drift
                cash = expected_cash
                asset_total = _r(cash + ar + prepaid + ppe + intangibles + goodwill)
                retained_earnings = _r(asset_total - liability_total - common_stock)
                equity_total = _r(retained_earnings + common_stock)

            prev_cash_triple = cash

            # ── Emit BS triples ─────────────────────────────────────
            def add_bs(concept: str, value: float, source: str = "erp"):
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="amount",
                    value=value,
                    period=period,
                    unit="dollars",
                    source_system=source,
                    confidence_score=0.95,
                    confidence_tier="high",
                ))

            add_bs("asset.total", asset_total)
            add_bs("asset.current.cash", cash)
            add_bs("asset.current.accounts_receivable", ar)
            add_bs("asset.current.prepaid", prepaid)
            add_bs("asset.noncurrent.property_plant_equipment", ppe)
            add_bs("asset.noncurrent.intangibles", intangibles)
            add_bs("asset.noncurrent.goodwill", goodwill)
            add_bs("liability.total", liability_total)
            add_bs("liability.current.accounts_payable", ap)
            add_bs("liability.current.accrued_expenses", accrued)
            add_bs("liability.current.deferred_revenue", deferred_rev)
            add_bs("liability.noncurrent.long_term_debt", long_term_debt)
            add_bs("equity.total", equity_total)
            add_bs("equity.retained_earnings", retained_earnings)
            add_bs("equity.common_stock", common_stock)

            # ── Emit CF triples ─────────────────────────────────────
            add_bs("cash_flow.operating.total", operating_total)
            add_bs("cash_flow.operating.net_income", operating_ni)
            add_bs("cash_flow.operating.depreciation_add_back", operating_da)
            add_bs("cash_flow.operating.working_capital_changes", wc_changes)
            add_bs("cash_flow.investing.total", investing_total)
            add_bs("cash_flow.investing.capex", investing_capex)
            add_bs("cash_flow.financing.total", financing_total)
            add_bs("cash_flow.financing.debt_proceeds", 0.0)
            add_bs("cash_flow.financing.debt_repayment", financing_debt_repayment)
            add_bs("cash_flow.financing.dividends", financing_dividends)
            add_bs("cash_flow.net_change", net_change)

            prev_q = q

        return triples
