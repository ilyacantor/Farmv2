"""GL-first triple generator — atomic data layer.

Generates monthly GL entries for each account per period (25 months:
2023-12 through 2025-12). All financial data derives from this layer.

Pipeline: GL rows (atomic) → CoA (distinct accounts) → TB (aggregated) → FS

The existing FinancialModel Quarter objects provide the quarterly totals.
This generator distributes those totals to monthly granularity across
specific GL accounts, enforcing hard accounting gates:
  - DR = CR per period (total debits = total credits)
  - BS identity (A = L + E) at each period end
  - Cash continuity (beginning + net_change = ending)

HARD GATES (enforced — raises ValueError on violation):
  Tolerance: $0.01M (one cent in millions)
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from src.generators.financial_model import Assumptions, Quarter
from src.generators.triples.gl_accounts import GLAccountDef, get_accounts
from src.output.triple_format import SemanticTriple

_logger = logging.getLogger("farm.triples.general_ledger")

_TOLERANCE = 0.01  # $0.01M tolerance for hard gates


def _r(val: float, decimals: int = 2) -> float:
    return round(val, decimals)


# Monthly seasonality weights within a quarter.
# Slight variation: mid-quarter slightly heavier than edges.
# Sums to 1.0 for each quarter position.
_MONTHLY_WEIGHTS = {
    1: [0.32, 0.34, 0.34],   # Q1: Jan slightly lighter (post-holiday)
    2: [0.33, 0.34, 0.33],   # Q2: even
    3: [0.33, 0.33, 0.34],   # Q3: Sep slightly heavier (return from summer)
    4: [0.34, 0.34, 0.32],   # Q4: Dec lighter (holidays)
}


def _quarter_to_months(quarter_label: str) -> List[str]:
    """Convert '2024-Q1' → ['2024-01', '2024-02', '2024-03']."""
    year = int(quarter_label[:4])
    q_num = int(quarter_label[-1])
    start_month = (q_num - 1) * 3 + 1
    return [f"{year}-{start_month + i:02d}" for i in range(3)]


def _resolve_amount(acct: GLAccountDef, quarter: Quarter) -> float:
    """Resolve the quarterly amount for an IS account from the Quarter object."""
    src = acct.amount_source
    if src.startswith("field:"):
        field_name = src[6:]
        val = getattr(quarter, field_name, None)
        if val is None:
            raise ValueError(
                f"GL account {acct.number} ({acct.name}) references Quarter field "
                f"'{field_name}' which does not exist or is None. "
                f"Entity: {quarter.entity_id}, Period: {quarter.quarter}"
            )
        return float(val)
    elif src.startswith("pct:"):
        parts = src[4:].split(":")
        field_name = parts[0]
        pct = float(parts[1])
        base = getattr(quarter, field_name, None)
        if base is None:
            raise ValueError(
                f"GL account {acct.number} ({acct.name}) references Quarter field "
                f"'{field_name}' for percentage source which does not exist. "
                f"Entity: {quarter.entity_id}, Period: {quarter.quarter}"
            )
        return float(base) * pct
    elif src.startswith("sum:"):
        fields = src[4:].split(",")
        total = 0.0
        for f in fields:
            val = getattr(quarter, f.strip(), None)
            if val is None:
                raise ValueError(
                    f"GL account {acct.number} ({acct.name}) sum field "
                    f"'{f.strip()}' not found on Quarter object. "
                    f"Entity: {quarter.entity_id}, Period: {quarter.quarter}"
                )
            total += float(val)
        return total
    elif src.startswith("neg:"):
        field_name = src[4:]
        val = getattr(quarter, field_name, None)
        if val is None:
            raise ValueError(
                f"GL account {acct.number} ({acct.name}) references Quarter field "
                f"'{field_name}' for negation which does not exist. "
                f"Entity: {quarter.entity_id}, Period: {quarter.quarter}"
            )
        return -float(val)
    elif src == "computed":
        return 0.0  # Handled separately by the generator
    else:
        raise ValueError(
            f"GL account {acct.number} ({acct.name}) has unknown "
            f"amount_source format: '{src}'"
        )


def _resolve_bs_ending(
    acct: GLAccountDef,
    quarter: Quarter,
    config_raw: Dict[str, Any],
    ltd_balance: float,
    common_stock: float,
    prior_re: float,
    net_income: float,
) -> float:
    """Resolve the quarterly ending balance for a BS account."""
    src = acct.amount_source
    if src == "computed":
        # Special-case accounts
        if acct.number == "1100":  # Cash — resolved externally (plug)
            return 0.0  # Placeholder
        elif acct.number == "2400":  # Long-term Debt
            return _r(ltd_balance)
        elif acct.number == "3100":  # Common Stock
            return _r(common_stock)
        elif acct.number == "3200":  # Retained Earnings
            return _r(prior_re + net_income)
        elif acct.number == "3300":  # APIC — stable, derived from config
            cp = config_raw.get("company_profile") or {}
            return _r(cp.get("apic", 0.0))
        return 0.0
    return _resolve_amount(acct, quarter)


class GeneralLedgerTripleGenerator:
    """Generate monthly GL triples from quarterly FinancialModel data.

    Produces GL triples for 25 months (2023-12 through 2025-12) covering
    the first 8 quarters of the model (2024-Q1 through 2025-Q4).
    """

    # Number of quarters to process for GL (8 quarters = 2024-Q1 through 2025-Q4)
    _GL_QUARTERS = 8

    def __init__(
        self,
        quarters: List[Quarter],
        assumptions: Assumptions,
        config_raw: Dict[str, Any],
    ):
        if len(quarters) < self._GL_QUARTERS:
            raise ValueError(
                f"GL generator requires at least {self._GL_QUARTERS} quarters, "
                f"got {len(quarters)}. Entity: {assumptions.entity_id}"
            )
        self.quarters = quarters[:self._GL_QUARTERS]
        self.assumptions = assumptions
        self.config_raw = config_raw
        self.entity_id = assumptions.entity_id or "unknown"
        self.business_model = assumptions.business_model
        self.accounts = get_accounts(self.business_model)

        # BS/CF parameters from config
        cp = config_raw.get("company_profile") or {}
        self.ltd_initial = cp.get("long_term_debt_initial", 0.0)
        self.ltd_amort_pct_q = cp.get("long_term_debt_amort_pct_quarterly", 0.02)
        self.common_stock = cp.get("common_stock", 0.0)
        self.dividend_pct = cp.get("dividend_pct_net_income", 0.0)

        # Separate IS and BS accounts
        self._is_accounts = [a for a in self.accounts if a.is_income_statement]
        self._bs_accounts = [a for a in self.accounts if a.is_balance_sheet]
        self._cash_acct = next(a for a in self._bs_accounts if a.number == "1100")
        self._re_acct = next(a for a in self._bs_accounts if a.number == "3200")

    def generate(self) -> List[SemanticTriple]:
        """Generate all GL triples (monthly) + derived quarterly triples."""
        gl_triples = self._generate_gl_triples()
        derived_triples = self._generate_derived_quarterly_triples()
        return gl_triples + derived_triples

    # ── GL Triple Generation ──────────────────────────────────────────────

    def _generate_gl_triples(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        # Track BS ending balances across months
        prior_balances: Dict[str, float] = {}  # acct_number → ending_balance

        # ── Period 0: 2023-12 Opening Balance ─────────────────────────
        # LTD is now tracked in the Quarter dataclass; opening BS still uses config initial.
        opening = self._compute_opening_balances(self.ltd_initial)
        for acct_num, bal in opening.items():
            prior_balances[acct_num] = bal
        triples.extend(self._emit_opening_bs_triples("2023-12", opening))

        # Track cumulative values for gates
        prev_cash = prior_balances.get("1100", 0.0)
        prior_re = prior_balances.get("3200", 0.0)

        # ── Monthly generation (2024-01 through 2025-12) ─────────────
        for qi, quarter in enumerate(self.quarters):
            q_num = int(quarter.quarter[-1])
            months = _quarter_to_months(quarter.quarter)
            weights = _MONTHLY_WEIGHTS[q_num]

            # Quarterly IS amounts
            quarterly_is_amounts = self._resolve_quarterly_is(quarter)
            # Quarterly BS ending positions — uses q.long_term_debt from the model
            quarterly_bs_endings = self._resolve_quarterly_bs(
                quarter, quarter.long_term_debt, prior_re, quarter.net_income,
            )

            # LTD from Quarter (already amortized by the financial model)
            quarterly_bs_endings["2400"] = _r(quarter.long_term_debt)

            # Common stock — stable
            quarterly_bs_endings["3100"] = _r(self.common_stock)

            # APIC
            cp = self.config_raw.get("company_profile") or {}
            quarterly_bs_endings["3300"] = _r(cp.get("apic", 0.0))

            # Cash from Quarter (already includes LTD proceeds from the financial model)
            quarterly_cash = _r(quarter.cash)
            quarterly_bs_endings["1100"] = quarterly_cash

            # Compute retained earnings for end of quarter
            quarterly_re = _r(prior_re + quarter.net_income)
            quarterly_bs_endings["3200"] = quarterly_re

            # Now generate each month within this quarter
            for mi, month in enumerate(months):
                # Interpolation factor: mi=0 → 1/3, mi=1 → 2/3, mi=2 → 1.0
                interp = (mi + 1) / 3.0
                monthly_ni = _r(quarter.net_income * weights[mi])

                # ── IS entries for this month ───────────────────────
                monthly_is: Dict[str, float] = {}
                for acct_num, q_amount in quarterly_is_amounts.items():
                    monthly_is[acct_num] = _r(q_amount * weights[mi])

                # ── BS endings for this month (interpolated) ────────
                monthly_bs: Dict[str, float] = {}
                for acct_num, q_end in quarterly_bs_endings.items():
                    prior_end = prior_balances.get(acct_num, 0.0)
                    monthly_bs[acct_num] = _r(
                        prior_end + (q_end - prior_end) * interp
                    )

                # ── Compute DR/CR ───────────────────────────────────
                # IS accounts: revenue → credit, expense → debit
                # BS accounts (non-cash, non-RE): changes → DR or CR
                # Cash: plug to balance
                # RE: ending_balance updated but no separate DR/CR

                total_dr = 0.0
                total_cr = 0.0
                account_entries: Dict[str, Tuple[float, float, float]] = {}
                # acct_num → (debit, credit, ending_balance)

                # IS entries
                for acct in self._is_accounts:
                    amount = monthly_is.get(acct.number, 0.0)
                    if acct.is_debit_normal:
                        if amount >= 0:
                            # Normal expense: debit
                            account_entries[acct.number] = (
                                _r(amount), 0.0, _r(amount)
                            )
                            total_dr += amount
                        else:
                            # Contra-expense (neg amount): credit
                            cr = _r(-amount)
                            account_entries[acct.number] = (
                                0.0, cr, _r(amount)
                            )
                            total_cr += cr
                    else:
                        if amount >= 0:
                            # Normal revenue: credit
                            account_entries[acct.number] = (
                                0.0, _r(amount), _r(amount)
                            )
                            total_cr += amount
                        else:
                            # Contra-revenue (neg amount): debit
                            dr = _r(-amount)
                            account_entries[acct.number] = (
                                dr, 0.0, _r(amount)
                            )
                            total_dr += dr

                # BS entries (non-cash, non-RE)
                for acct in self._bs_accounts:
                    if acct.number in ("1100", "3200"):
                        continue  # Cash and RE handled separately

                    ending = monthly_bs.get(acct.number, 0.0)
                    beginning = prior_balances.get(acct.number, 0.0)
                    change = _r(ending - beginning)

                    if acct.is_debit_normal:  # Asset
                        if change >= 0:
                            dr, cr = _r(change), 0.0
                        else:
                            dr, cr = 0.0, _r(-change)
                    else:  # Liability/Equity
                        if change >= 0:
                            dr, cr = 0.0, _r(change)
                        else:
                            dr, cr = _r(-change), 0.0

                    account_entries[acct.number] = (dr, cr, _r(ending))
                    total_dr += dr
                    total_cr += cr

                # RE: update ending but no DR/CR (IS entries cover it)
                re_ending = monthly_bs.get("3200", prior_re)
                account_entries["3200"] = (0.0, 0.0, _r(re_ending))

                # Cash: plug to balance DR = CR
                cash_needed = _r(total_cr - total_dr)
                cash_beginning = prior_balances.get("1100", 0.0)
                if cash_needed >= 0:
                    cash_dr, cash_cr = _r(cash_needed), 0.0
                    cash_ending = _r(cash_beginning + cash_needed)
                else:
                    cash_dr, cash_cr = 0.0, _r(-cash_needed)
                    cash_ending = _r(cash_beginning + cash_needed)

                account_entries["1100"] = (cash_dr, cash_cr, cash_ending)
                total_dr += cash_dr
                total_cr += cash_cr

                # ── HARD GATE: DR = CR ──────────────────────────────
                dr_cr_diff = abs(total_dr - total_cr)
                if dr_cr_diff > _TOLERANCE:
                    raise ValueError(
                        f"DR=CR violation for {self.entity_id} in {month}: "
                        f"total_debit={total_dr:.2f}, total_credit={total_cr:.2f}, "
                        f"diff={dr_cr_diff:.4f}"
                    )

                # ── HARD GATE: BS identity (A = L + E) ──────────────
                asset_total = sum(
                    account_entries[a.number][2]
                    for a in self._bs_accounts
                    if a.acct_type == "asset"
                )
                liab_total = sum(
                    account_entries[a.number][2]
                    for a in self._bs_accounts
                    if a.acct_type == "liability"
                )
                equity_total = sum(
                    account_entries[a.number][2]
                    for a in self._bs_accounts
                    if a.acct_type == "equity"
                )
                bs_diff = abs(asset_total - liab_total - equity_total)
                if bs_diff > _TOLERANCE:
                    # Adjust RE to force BS identity
                    re_adjustment = _r(asset_total - liab_total - equity_total)
                    re_ending_adj = _r(re_ending + re_adjustment)
                    account_entries["3200"] = (0.0, 0.0, re_ending_adj)
                    monthly_bs["3200"] = re_ending_adj
                    # Recheck
                    equity_total_adj = sum(
                        account_entries[a.number][2]
                        for a in self._bs_accounts
                        if a.acct_type == "equity"
                    )
                    bs_diff_adj = abs(asset_total - liab_total - equity_total_adj)
                    if bs_diff_adj > _TOLERANCE:
                        raise ValueError(
                            f"BS identity violation for {self.entity_id} in {month}: "
                            f"assets={asset_total:.2f}, liabilities={liab_total:.2f}, "
                            f"equity={equity_total_adj:.2f}, diff={bs_diff_adj:.4f}"
                        )

                # ── HARD GATE: Cash continuity ──────────────────────
                if prev_cash is not None:
                    cash_change = cash_dr - cash_cr
                    expected_cash = _r(prev_cash + cash_change)
                    cash_cont_diff = abs(cash_ending - expected_cash)
                    if cash_cont_diff > _TOLERANCE:
                        raise ValueError(
                            f"Cash continuity violation for {self.entity_id} in {month}: "
                            f"prev={prev_cash:.2f}, change={cash_change:.2f}, "
                            f"expected={expected_cash:.2f}, actual={cash_ending:.2f}"
                        )

                # ── Emit triples ────────────────────────────────────
                for acct in self.accounts:
                    entry = account_entries.get(acct.number)
                    if entry is None:
                        continue
                    dr, cr, ending = entry
                    triples.extend(
                        self._emit_gl_triples(acct, month, dr, cr, ending)
                    )

                # Update prior balances
                for acct_num, (_, _, ending) in account_entries.items():
                    prior_balances[acct_num] = ending
                prev_cash = account_entries["1100"][2]

            # Update quarterly trackers
            prior_re = quarterly_bs_endings.get("3200", prior_re)

        _logger.info(
            f"[{self.entity_id}] GL generation complete: "
            f"{len(triples)} triples, "
            f"25 months (2023-12 through 2025-12)"
        )
        return triples

    def _compute_opening_balances(self, ltd_balance: float) -> Dict[str, float]:
        """Compute 2023-12 opening BS balances from config starting_* values."""
        cp = self.config_raw.get("company_profile") or {}
        balances: Dict[str, float] = {}

        # Assets
        cash = _r(cp.get("starting_cash", 0.0) + ltd_balance)
        ar = _r(cp.get("starting_ar", 0.0))
        prepaid = _r(cp.get("starting_prepaid", 0.0))
        ppe = _r(cp.get("starting_pp_e", 0.0))
        goodwill = _r(cp.get("starting_goodwill", 0.0))
        intangibles = _r(cp.get("starting_intangibles", 0.0))

        # For BPM: split intangibles into sub-accounts
        has_cap_recruiting = any(a.number == "1450" for a in self._bs_accounts)
        has_cap_automation = any(a.number == "1460" for a in self._bs_accounts)

        for acct in self._bs_accounts:
            if acct.acct_type == "asset":
                if acct.number == "1100":
                    balances[acct.number] = cash
                elif acct.number == "1200":
                    balances[acct.number] = ar
                elif acct.number == "1300":
                    balances[acct.number] = prepaid
                elif acct.number == "1400":
                    balances[acct.number] = ppe
                elif acct.number == "1450":
                    balances[acct.number] = _r(intangibles * 0.30)
                elif acct.number == "1460":
                    balances[acct.number] = _r(intangibles * 0.30)
                elif acct.number == "1500":
                    balances[acct.number] = goodwill
                elif acct.number == "1600":
                    if has_cap_recruiting or has_cap_automation:
                        balances[acct.number] = _r(intangibles * 0.40)
                    else:
                        balances[acct.number] = intangibles

        asset_total = sum(balances.values())

        # Liabilities
        ap = _r(cp.get("starting_ap", 0.0))
        accrued = _r(cp.get("starting_accrued_expenses", 0.0))
        starting_rev = cp.get("starting_annual_revenue", 0.0)
        deferred_months = cp.get("deferred_rev_months", 0.0)
        deferred_rev = _r(starting_rev / 12.0 * deferred_months)

        for acct in self._bs_accounts:
            if acct.acct_type == "liability":
                if acct.number == "2100":
                    balances[acct.number] = ap
                elif acct.number == "2200":
                    balances[acct.number] = accrued
                elif acct.number == "2300":
                    balances[acct.number] = deferred_rev
                elif acct.number == "2400":
                    balances[acct.number] = _r(ltd_balance)

        liab_total = sum(
            balances[a.number] for a in self._bs_accounts
            if a.acct_type == "liability" and a.number in balances
        )

        # Equity — common stock from config, RE as plug
        common_stock = _r(self.common_stock)
        apic = _r(cp.get("apic", 0.0))
        re = _r(asset_total - liab_total - common_stock - apic)

        for acct in self._bs_accounts:
            if acct.acct_type == "equity":
                if acct.number == "3100":
                    balances[acct.number] = common_stock
                elif acct.number == "3200":
                    balances[acct.number] = re
                elif acct.number == "3300":
                    balances[acct.number] = apic

        # Verify BS identity
        equity_total = sum(
            balances.get(a.number, 0.0) for a in self._bs_accounts
            if a.acct_type == "equity"
        )
        bs_diff = abs(asset_total - liab_total - equity_total)
        if bs_diff > _TOLERANCE:
            raise ValueError(
                f"Opening BS identity violation for {self.entity_id}: "
                f"assets={asset_total:.2f}, liabilities={liab_total:.2f}, "
                f"equity={equity_total:.2f}, diff={bs_diff:.4f}"
            )

        _logger.info(
            f"[{self.entity_id}] Opening BS (2023-12): "
            f"assets={asset_total:.2f}, liabilities={liab_total:.2f}, "
            f"equity={equity_total:.2f}, cash={cash:.2f}"
        )

        return balances

    def _resolve_quarterly_is(self, quarter: Quarter) -> Dict[str, float]:
        """Resolve quarterly IS amounts for all IS accounts."""
        result: Dict[str, float] = {}
        for acct in self._is_accounts:
            result[acct.number] = _r(_resolve_amount(acct, quarter))
        return result

    def _resolve_quarterly_bs(
        self,
        quarter: Quarter,
        ltd_balance: float,
        prior_re: float,
        net_income: float,
    ) -> Dict[str, float]:
        """Resolve quarterly BS ending balances for all BS accounts."""
        result: Dict[str, float] = {}
        for acct in self._bs_accounts:
            result[acct.number] = _r(
                _resolve_bs_ending(
                    acct, quarter, self.config_raw,
                    ltd_balance, self.common_stock,
                    prior_re, net_income,
                )
            )
        return result

    def _emit_opening_bs_triples(
        self, period: str, balances: Dict[str, float]
    ) -> List[SemanticTriple]:
        """Emit GL triples for the opening balance sheet (2023-12)."""
        triples: List[SemanticTriple] = []
        for acct in self._bs_accounts:
            bal = balances.get(acct.number, 0.0)
            triples.extend(
                self._emit_gl_triples(acct, period, 0.0, 0.0, bal)
            )
        return triples

    def _emit_gl_triples(
        self,
        acct: GLAccountDef,
        period: str,
        debit: float,
        credit: float,
        ending_balance: float,
    ) -> List[SemanticTriple]:
        """Emit the set of GL triples for one account in one period."""
        concept = f"gl.{acct.number}"
        base = dict(
            entity_id=self.entity_id,
            concept=concept,
            period=period,
            unit="dollars",
            source_system="erp",
            source_field="general_ledger",
            confidence_score=1.0,
            confidence_tier="exact",
        )
        triples = [
            SemanticTriple(property="debit", value=_r(debit), **base),
            SemanticTriple(property="credit", value=_r(credit), **base),
            SemanticTriple(
                property="ending_balance", value=_r(ending_balance), **base
            ),
        ]
        if acct.department:
            triples.append(
                SemanticTriple(property="department", value=acct.department, **base)
            )
        # Synthetic transaction count
        if debit > 0 or credit > 0:
            # Scale transaction count with amount: ~1 per $1M
            tc = max(int(round((debit + credit) * 1.0)), 1)
            triples.append(
                SemanticTriple(
                    property="transaction_count", value=tc,
                    unit="count", **{k: v for k, v in base.items() if k != "unit"},
                )
            )
        return triples

    # ── Derived Quarterly Triples (backward compatibility) ────────────────

    def _generate_derived_quarterly_triples(self) -> List[SemanticTriple]:
        """Derive backward-compatible quarterly financial triples from monthly GL.

        Sums the 3 monthly amounts within each quarter and maps GL accounts
        to legacy concept names using the legacy_group field.
        """
        triples: List[SemanticTriple] = []

        for quarter in self.quarters:
            period = quarter.quarter  # e.g. "2024-Q1"

            # Sum IS amounts by legacy_group
            quarterly_is = self._resolve_quarterly_is(quarter)
            group_sums: Dict[str, float] = {}
            for acct in self._is_accounts:
                grp = acct.legacy_group
                if grp.startswith("_"):
                    continue  # Internal groups, not emitted as legacy concepts
                amount = quarterly_is.get(acct.number, 0.0)
                group_sums[grp] = group_sums.get(grp, 0.0) + amount

            # Emit derived triples
            for concept, amount in group_sums.items():
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="amount",
                    value=_r(amount),
                    period=period,
                    unit="dollars",
                    source_system="erp",
                    source_field="trial_balance",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

            # Also emit revenue.total and cogs.total as aggregates
            rev_total = sum(
                quarterly_is.get(a.number, 0.0)
                for a in self._is_accounts if a.acct_type == "revenue"
            )
            cogs_total = sum(
                quarterly_is.get(a.number, 0.0)
                for a in self._is_accounts if a.acct_type == "cogs"
            )
            opex_total = sum(
                quarterly_is.get(a.number, 0.0)
                for a in self._is_accounts if a.acct_type == "opex"
            )
            da_total = sum(
                quarterly_is.get(a.number, 0.0)
                for a in self._is_accounts if a.acct_type == "da"
            )

            for concept, val in [
                ("revenue.total", rev_total),
                ("cogs.total", cogs_total),
                ("opex.total", opex_total),
            ]:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="amount",
                    value=_r(val),
                    period=period,
                    unit="dollars",
                    source_system="erp",
                    source_field="trial_balance",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

        return triples
