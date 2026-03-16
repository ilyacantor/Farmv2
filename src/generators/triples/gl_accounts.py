"""GL account definitions per business model.

Defines the chart of accounts for each entity type (consultancy, bpm).
Account structures are keyed by business_model — NOT by entity name —
to avoid hardcoded entity references.

Each account maps to:
- Quarterly data from the FinancialModel Quarter object (amount_source)
- A legacy financial concept for backward-compatible derived triples (legacy_group)
- CoA metadata (type, policies, hierarchy)

Account types:
  revenue, cogs, opex, other_income, da, tax  → income statement (flow)
  asset, liability, equity                     → balance sheet (stock)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class GLAccountDef:
    """One GL account definition."""

    number: str                # "4100", "5200"
    name: str                  # "Advisory Revenue"
    acct_type: str             # revenue, cogs, opex, other_income, da, tax, asset, liability, equity
    normal_balance: str        # "debit" or "credit"
    hierarchy_parent: str      # "revenue", "cogs", "opex", "other", "da", "asset", "liability", "equity"
    hierarchy_level: int       # 1=category, 2=detail
    legacy_group: str          # backward-compat concept: "revenue.consulting", "cogs.total", etc.
    department: str            # "consulting", "delivery", "corporate", "sales", "marketing", ""
    description: str

    # Amount source — how to extract the quarterly amount from a Quarter object.
    # For IS accounts: the P&L amount to distribute to 3 months.
    # For BS accounts: the quarterly ending balance to interpolate to months.
    #
    # Formats:
    #   "field:tm_revenue"         → getattr(quarter, "tm_revenue")
    #   "pct:revenue:0.35"         → quarter.revenue * 0.35
    #   "sum:ga_expense,facilities_expense"  → sum of multiple fields
    #   "config:starting_cash"     → from config (opening balance only)
    #   "computed"                 → special handling in the generator
    amount_source: str = ""

    # Policy metadata — entity-specific accounting choices (for CoA triples)
    recognition_method: str = ""          # "gross" or "net"
    cost_classification: str = ""         # "cogs" or "opex" for benefits
    capitalization_policy: str = ""       # "expense" or "capitalize"
    depreciation_method: str = ""         # "straight_line" or "accelerated"

    @property
    def is_income_statement(self) -> bool:
        return self.acct_type in ("revenue", "cogs", "opex", "other_income", "da", "tax")

    @property
    def is_balance_sheet(self) -> bool:
        return self.acct_type in ("asset", "liability", "equity")

    @property
    def is_debit_normal(self) -> bool:
        return self.normal_balance == "debit"


def get_accounts(business_model: str) -> List[GLAccountDef]:
    """Return GL account definitions for the given business model."""
    if business_model == "consultancy":
        return _consultancy_accounts()
    elif business_model == "bpm":
        return _bpm_accounts()
    raise ValueError(
        f"Unknown business model '{business_model}' — expected 'consultancy' or 'bpm'. "
        f"Cannot generate GL account definitions."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Consultancy GL Accounts (~40 accounts)
# Revenue: project-based (T&M + fixed-fee), split into 4 GL accounts
# COGS: consultant comp (includes benefits — COFA-002), bench, subcontractors, travel
# OpEx: sales and marketing SEPARATE (COFA-003), G&A, facilities, recruiting, R&D
# D&A: straight-line 5yr (COFA-006)
# ═══════════════════════════════════════════════════════════════════════════════

def _consultancy_accounts() -> List[GLAccountDef]:
    return [
        # ── Revenue (4000s) ─────────────────────────────────────────────
        # Total revenue split into 4 GL accounts. Sums of groups match
        # existing legacy concepts (revenue.consulting, revenue.fixed_fee).
        GLAccountDef(
            number="4100", name="Advisory Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.consulting",
            department="consulting",
            description="Strategic advisory engagements — T&M billing",
            amount_source="pct:revenue:0.35",
            recognition_method="gross",
        ),
        GLAccountDef(
            number="4200", name="Consulting Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.consulting",
            department="consulting",
            description="Implementation and delivery consulting — T&M billing",
            amount_source="pct:revenue:0.30",
            recognition_method="gross",
        ),
        GLAccountDef(
            number="4300", name="Per-FTE/Staffing Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.fixed_fee",
            department="consulting",
            description="Resource augmentation — fixed-fee arrangements",
            amount_source="pct:revenue:0.20",
        ),
        GLAccountDef(
            number="4400", name="Reimbursable Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.fixed_fee",
            department="consulting",
            description="Travel and expense pass-through to clients",
            amount_source="pct:revenue:0.15",
        ),

        # ── COGS (5000s) ───────────────────────────────────────────────
        # Maps directly to existing Quarter fields via cogs_breakdown.
        # Consultant comp INCLUDES benefits — this is COFA-002.
        GLAccountDef(
            number="5100", name="Consultant Compensation",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.direct_labor",
            department="consulting",
            description="Consultant salaries and benefits (benefits bundled — COFA-002)",
            amount_source="field:consultant_comp",
            cost_classification="cogs_with_benefits",
        ),
        GLAccountDef(
            number="5200", name="Subcontractor Costs",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.subcontractors",
            department="consulting",
            description="Third-party subcontractor fees",
            amount_source="field:subcontractor_cost",
        ),
        GLAccountDef(
            number="5300", name="Bench Costs — Consulting",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.bench",
            department="consulting",
            description="Undeployed consultant costs (between engagements)",
            amount_source="field:bench_cost",
        ),
        GLAccountDef(
            number="5400", name="Project Travel",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.travel",
            department="consulting",
            description="Client-site travel and expenses",
            amount_source="field:travel_cost",
        ),

        # ── OpEx (6000s) ───────────────────────────────────────────────
        # Sales and Marketing are SEPARATE — this is COFA-003.
        GLAccountDef(
            number="6100", name="Sales Compensation",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.sales_marketing",
            department="sales",
            description="Sales team base salaries",
            amount_source="pct:sales_expense:0.70",
        ),
        GLAccountDef(
            number="6110", name="Sales Commissions",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.sales_marketing",
            department="sales",
            description="Variable sales compensation",
            amount_source="pct:sales_expense:0.30",
        ),
        GLAccountDef(
            number="6120", name="Marketing Programs",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.sales_marketing",
            department="marketing",
            description="Campaign spend, events, brand",
            amount_source="pct:marketing_expense:0.60",
        ),
        GLAccountDef(
            number="6130", name="Marketing Personnel",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.sales_marketing",
            department="marketing",
            description="Marketing team compensation",
            amount_source="pct:marketing_expense:0.40",
        ),
        GLAccountDef(
            number="6200", name="G&A — Corporate Staff",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="Corporate headquarters staff",
            amount_source="pct:ga_expense:0.50",
        ),
        GLAccountDef(
            number="6210", name="G&A — Rent & Facilities",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_other",
            department="corporate",
            description="Office rent, utilities, maintenance ($25M — smaller facilities)",
            amount_source="field:facilities_expense",
        ),
        GLAccountDef(
            number="6220", name="G&A — Professional Fees",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="Audit, legal, tax advisory fees",
            amount_source="pct:ga_expense:0.30",
        ),
        GLAccountDef(
            number="6230", name="G&A — Insurance",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="E&O, D&O, general liability insurance",
            amount_source="pct:ga_expense:0.20",
        ),
        GLAccountDef(
            number="6300", name="Recruiting Expense",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_other",
            department="corporate",
            description="All recruiting expensed immediately (COFA-004: not capitalized)",
            amount_source="field:recruiting_expense",
            capitalization_policy="expense",
        ),
        GLAccountDef(
            number="6400", name="R&D / Automation Expense",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.research_development",
            department="corporate",
            description="Methodology and tools development (COFA-005: expensed, not capitalized)",
            amount_source="field:rd_expense",
            capitalization_policy="expense",
        ),

        # ── Other Income/Expense (7000s) ───────────────────────────────
        GLAccountDef(
            number="7100", name="Interest Income",
            acct_type="other_income", normal_balance="credit",
            hierarchy_parent="other", hierarchy_level=2,
            legacy_group="_other_income",
            department="corporate",
            description="Interest on cash balances",
            amount_source="pct:revenue:0.001",
        ),
        GLAccountDef(
            number="7200", name="Interest Expense",
            acct_type="other_income", normal_balance="debit",
            hierarchy_parent="other", hierarchy_level=2,
            legacy_group="_other_expense",
            department="corporate",
            description="Interest on long-term debt",
            amount_source="pct:revenue:0.002",
        ),
        GLAccountDef(
            number="7300", name="FX Gains/Losses",
            acct_type="other_income", normal_balance="debit",
            hierarchy_parent="other", hierarchy_level=2,
            legacy_group="_other_expense",
            department="corporate",
            description="Foreign exchange translation adjustments",
            amount_source="pct:revenue:0.0005",
        ),

        # ── D&A (8000s) ────────────────────────────────────────────────
        GLAccountDef(
            number="8100", name="Depreciation — PP&E",
            acct_type="da", normal_balance="debit",
            hierarchy_parent="da", hierarchy_level=2,
            legacy_group="pnl.depreciation_amortization",
            department="corporate",
            description="Straight-line depreciation over 5 years (COFA-006)",
            amount_source="pct:da_expense:0.70",
            depreciation_method="straight_line",
        ),
        GLAccountDef(
            number="8200", name="Amortization — Intangibles",
            acct_type="da", normal_balance="debit",
            hierarchy_parent="da", hierarchy_level=2,
            legacy_group="pnl.depreciation_amortization",
            department="corporate",
            description="Amortization of acquired intangible assets",
            amount_source="pct:da_expense:0.30",
        ),

        # ── Tax ────────────────────────────────────────────────────────
        GLAccountDef(
            number="9100", name="Income Tax Expense",
            acct_type="tax", normal_balance="debit",
            hierarchy_parent="tax", hierarchy_level=2,
            legacy_group="pnl.tax",
            department="corporate",
            description="Provision for income taxes",
            amount_source="field:tax_expense",
        ),

        # ── Assets (1000s) ─────────────────────────────────────────────
        GLAccountDef(
            number="1100", name="Cash & Equivalents",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.cash",
            department="",
            description="Cash, money market, short-term treasuries",
            amount_source="computed",  # Cash is the plug account
        ),
        GLAccountDef(
            number="1200", name="Accounts Receivable",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.accounts_receivable",
            department="",
            description="Trade receivables from client billings",
            amount_source="field:ar",
        ),
        GLAccountDef(
            number="1300", name="Prepaid Expenses",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.prepaid",
            department="",
            description="Prepaid rent, insurance, software licenses",
            amount_source="field:prepaid_expenses",
        ),
        GLAccountDef(
            number="1400", name="PP&E (net)",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.property_plant_equipment",
            department="",
            description="Office equipment, leasehold improvements (net of depreciation)",
            amount_source="field:pp_e",
        ),
        GLAccountDef(
            number="1500", name="Goodwill",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.goodwill",
            department="",
            description="Goodwill from prior acquisitions (not amortized under GAAP)",
            amount_source="field:goodwill",
        ),
        GLAccountDef(
            number="1600", name="Intangible Assets",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.intangibles",
            department="",
            description="Client relationships, trade names, acquired IP",
            amount_source="field:intangibles",
        ),

        # ── Liabilities (2000s) ────────────────────────────────────────
        GLAccountDef(
            number="2100", name="Accounts Payable",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.accounts_payable",
            department="",
            description="Trade payables to vendors and subcontractors",
            amount_source="field:ap",
        ),
        GLAccountDef(
            number="2200", name="Accrued Compensation",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.accrued_expenses",
            department="",
            description="Accrued salaries, bonuses, benefits payable",
            amount_source="field:accrued_expenses",
        ),
        GLAccountDef(
            number="2300", name="Deferred Revenue",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.deferred_revenue",
            department="",
            description="Billed but unearned engagement fees",
            amount_source="field:deferred_revenue",
        ),
        GLAccountDef(
            number="2400", name="Long-term Debt",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.noncurrent.long_term_debt",
            department="",
            description="Credit facility borrowings",
            amount_source="computed",  # Amortized from config
        ),

        # ── Equity (3000s) ─────────────────────────────────────────────
        GLAccountDef(
            number="3100", name="Common Stock",
            acct_type="equity", normal_balance="credit",
            hierarchy_parent="equity", hierarchy_level=2,
            legacy_group="equity.common_stock",
            department="",
            description="Par value of issued shares",
            amount_source="computed",  # From config, stable
        ),
        GLAccountDef(
            number="3200", name="Retained Earnings",
            acct_type="equity", normal_balance="credit",
            hierarchy_parent="equity", hierarchy_level=2,
            legacy_group="equity.retained_earnings",
            department="",
            description="Accumulated net income less dividends",
            amount_source="computed",  # Prior + NI
        ),
        GLAccountDef(
            number="3300", name="APIC",
            acct_type="equity", normal_balance="credit",
            hierarchy_parent="equity", hierarchy_level=2,
            legacy_group="equity.common_stock",
            department="",
            description="Additional paid-in capital above par",
            amount_source="computed",  # Stable, derived from config
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# BPM GL Accounts (~40 accounts)
# Revenue: managed services (net recognition — COFA-001) + per-FTE + per-transaction
# COGS: onshore/offshore/nearshore labor + bench + delivery ops + benefits (separate — COFA-002)
# OpEx: S&M bundled (COFA-003), G&A, R&D
# D&A: accelerated 3yr (COFA-006), includes capitalized recruiting (COFA-004)
#       and capitalized automation (COFA-005)
# ═══════════════════════════════════════════════════════════════════════════════

def _bpm_accounts() -> List[GLAccountDef]:
    return [
        # ── Revenue (4000s) ─────────────────────────────────────────────
        GLAccountDef(
            number="4100", name="Managed Services Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.managed_services",
            department="delivery",
            description="Fixed monthly managed services fees (net recognition — COFA-001)",
            amount_source="field:managed_services_revenue",
            recognition_method="net",
        ),
        GLAccountDef(
            number="4200", name="Transaction-Based Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.per_transaction",
            department="delivery",
            description="Volume-based transaction processing fees (net recognition)",
            amount_source="field:per_transaction_revenue",
            recognition_method="net",
        ),
        GLAccountDef(
            number="4300", name="Per-FTE/Staffing Revenue",
            acct_type="revenue", normal_balance="credit",
            hierarchy_parent="revenue", hierarchy_level=2,
            legacy_group="revenue.per_fte",
            department="delivery",
            description="FTE-based resource pricing",
            amount_source="field:per_fte_revenue",
        ),

        # ── COGS (5000s) ───────────────────────────────────────────────
        GLAccountDef(
            number="5110", name="Onshore Delivery Staff",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.direct_labor",
            department="delivery",
            description="US/UK-based delivery staff compensation",
            amount_source="field:onshore_cost",
        ),
        GLAccountDef(
            number="5120", name="Offshore Delivery Staff",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.direct_labor",
            department="delivery",
            description="India/Philippines delivery center staff",
            amount_source="field:offshore_cost",
        ),
        GLAccountDef(
            number="5130", name="Nearshore Delivery Staff",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.direct_labor",
            department="delivery",
            description="Costa Rica/Poland delivery center staff",
            amount_source="field:nearshore_cost",
        ),
        GLAccountDef(
            number="5200", name="Delivery Center Operations",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.delivery_center_ops",
            department="delivery",
            description="Delivery center rent, utilities, equipment ($80M — large facilities)",
            amount_source="field:delivery_center_ops_cost",
        ),
        GLAccountDef(
            number="5300", name="Subcontractor Costs",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.subcontractors",
            department="delivery",
            description="Third-party subcontractors for overflow capacity",
            amount_source="field:subcontractor_cost",
        ),
        GLAccountDef(
            number="5400", name="Bench Costs — Delivery",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.bench",
            department="delivery",
            description="Idle delivery staff between contracts",
            amount_source="field:bench_delivery_cost",
        ),
        GLAccountDef(
            number="5500", name="Employee Benefits",
            acct_type="cogs", normal_balance="debit",
            hierarchy_parent="cogs", hierarchy_level=2,
            legacy_group="cogs.benefits",
            department="delivery",
            description="Delivery staff benefits — separate from comp (COFA-002: in COGS, not OpEx)",
            amount_source="field:benefits_cost",
            cost_classification="cogs_separate_benefits",
        ),

        # ── OpEx (6000s) ───────────────────────────────────────────────
        # S&M bundled — COFA-003
        # total_opex = sm + ga + tech_automation + facilities - cap_recruiting - cap_automation
        GLAccountDef(
            number="6100", name="Sales & Marketing Combined",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.sales_marketing",
            department="corporate",
            description="Bundled sales and marketing ($90M — NOT separated, COFA-003)",
            amount_source="field:sm_expense",
        ),
        GLAccountDef(
            number="6200", name="G&A — Corporate Staff",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="Corporate headquarters and admin staff",
            amount_source="pct:ga_expense:0.50",
        ),
        GLAccountDef(
            number="6210", name="G&A — Rent & Facilities",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_other",
            department="corporate",
            description="Corporate office rent and facilities",
            amount_source="field:facilities_corporate_expense",
        ),
        GLAccountDef(
            number="6220", name="G&A — Professional Fees",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="Audit, legal, advisory",
            amount_source="pct:ga_expense:0.30",
        ),
        GLAccountDef(
            number="6230", name="G&A — Insurance",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="opex.general_admin",
            department="corporate",
            description="General liability and professional insurance",
            amount_source="pct:ga_expense:0.20",
        ),
        GLAccountDef(
            number="6300", name="Technology & Automation",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_other",
            department="corporate",
            description="Technology and process automation (net of capitalization — COFA-005)",
            amount_source="field:tech_automation_expense",
            capitalization_policy="partial_capitalize",
        ),
        # Capitalization contra-entries: these reduce total OpEx
        GLAccountDef(
            number="6810", name="Capitalized Recruiting (contra)",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_capitalization",
            department="corporate",
            description="Recruiting costs capitalized as intangible (COFA-004) — reduces OpEx",
            amount_source="neg:capitalized_recruiting",
            capitalization_policy="capitalize",
        ),
        GLAccountDef(
            number="6820", name="Capitalized Automation (contra)",
            acct_type="opex", normal_balance="debit",
            hierarchy_parent="opex", hierarchy_level=2,
            legacy_group="_opex_capitalization",
            department="corporate",
            description="Automation platform dev capitalized (COFA-005) — reduces OpEx",
            amount_source="neg:capitalized_automation",
            capitalization_policy="capitalize",
        ),

        # ── Other Income/Expense (7000s) ───────────────────────────────
        GLAccountDef(
            number="7100", name="Interest Income",
            acct_type="other_income", normal_balance="credit",
            hierarchy_parent="other", hierarchy_level=2,
            legacy_group="_other_income",
            department="corporate",
            description="Interest on cash balances",
            amount_source="pct:revenue:0.0008",
        ),
        GLAccountDef(
            number="7200", name="Interest Expense",
            acct_type="other_income", normal_balance="debit",
            hierarchy_parent="other", hierarchy_level=2,
            legacy_group="_other_expense",
            department="corporate",
            description="Interest on delivery center financing",
            amount_source="pct:revenue:0.003",
        ),

        # ── D&A (8000s) ────────────────────────────────────────────────
        GLAccountDef(
            number="8100", name="Depreciation — PP&E",
            acct_type="da", normal_balance="debit",
            hierarchy_parent="da", hierarchy_level=2,
            legacy_group="pnl.depreciation_amortization",
            department="corporate",
            description="Accelerated depreciation over 3 years (COFA-006)",
            amount_source="pct:da_expense:0.50",
            depreciation_method="accelerated",
        ),
        GLAccountDef(
            number="8200", name="Amortization — Capitalized Recruiting",
            acct_type="da", normal_balance="debit",
            hierarchy_parent="da", hierarchy_level=2,
            legacy_group="pnl.depreciation_amortization",
            department="corporate",
            description="Amortization of capitalized recruiting costs (COFA-004)",
            amount_source="pct:da_expense:0.25",
            capitalization_policy="capitalize",
        ),
        GLAccountDef(
            number="8300", name="Amortization — Capitalized Automation",
            acct_type="da", normal_balance="debit",
            hierarchy_parent="da", hierarchy_level=2,
            legacy_group="pnl.depreciation_amortization",
            department="corporate",
            description="Amortization of capitalized automation platform (COFA-005)",
            amount_source="pct:da_expense:0.25",
            capitalization_policy="capitalize",
        ),

        # ── Tax ────────────────────────────────────────────────────────
        GLAccountDef(
            number="9100", name="Income Tax Expense",
            acct_type="tax", normal_balance="debit",
            hierarchy_parent="tax", hierarchy_level=2,
            legacy_group="pnl.tax",
            department="corporate",
            description="Provision for income taxes",
            amount_source="field:tax_expense",
        ),

        # ── Assets (1000s) ─────────────────────────────────────────────
        GLAccountDef(
            number="1100", name="Cash & Equivalents",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.cash",
            department="",
            description="Cash and short-term investments",
            amount_source="computed",
        ),
        GLAccountDef(
            number="1200", name="Accounts Receivable",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.accounts_receivable",
            department="",
            description="Trade receivables from managed services clients",
            amount_source="field:ar",
        ),
        GLAccountDef(
            number="1300", name="Prepaid Expenses",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.current.prepaid",
            department="",
            description="Prepaid delivery center leases and software",
            amount_source="field:prepaid_expenses",
        ),
        GLAccountDef(
            number="1400", name="PP&E (net)",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.property_plant_equipment",
            department="",
            description="Delivery center equipment and buildouts (net of depreciation)",
            amount_source="field:pp_e",
        ),
        GLAccountDef(
            number="1450", name="Capitalized Recruiting Costs (net)",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.intangibles",
            department="",
            description="Capitalized recruiting tied to new contracts (COFA-004)",
            amount_source="pct:intangibles:0.30",
            capitalization_policy="capitalize",
        ),
        GLAccountDef(
            number="1460", name="Capitalized Automation Costs (net)",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.intangibles",
            department="",
            description="Capitalized automation platform development (COFA-005)",
            amount_source="pct:intangibles:0.30",
            capitalization_policy="capitalize",
        ),
        GLAccountDef(
            number="1500", name="Goodwill",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.goodwill",
            department="",
            description="Goodwill from acquired BPM operations",
            amount_source="field:goodwill",
        ),
        GLAccountDef(
            number="1600", name="Intangible Assets",
            acct_type="asset", normal_balance="debit",
            hierarchy_parent="asset", hierarchy_level=2,
            legacy_group="asset.noncurrent.intangibles",
            department="",
            description="Client contracts, process IP (excluding capitalized recruiting/automation)",
            amount_source="pct:intangibles:0.40",
        ),

        # ── Liabilities (2000s) ────────────────────────────────────────
        GLAccountDef(
            number="2100", name="Accounts Payable",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.accounts_payable",
            department="",
            description="Trade payables to vendors and delivery center suppliers",
            amount_source="field:ap",
        ),
        GLAccountDef(
            number="2200", name="Accrued Compensation",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.accrued_expenses",
            department="",
            description="Accrued delivery staff salaries and bonuses",
            amount_source="field:accrued_expenses",
        ),
        GLAccountDef(
            number="2300", name="Deferred Revenue",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.current.deferred_revenue",
            department="",
            description="Upfront billing on managed services contracts",
            amount_source="field:deferred_revenue",
        ),
        GLAccountDef(
            number="2400", name="Long-term Debt",
            acct_type="liability", normal_balance="credit",
            hierarchy_parent="liability", hierarchy_level=2,
            legacy_group="liability.noncurrent.long_term_debt",
            department="",
            description="Delivery center financing",
            amount_source="computed",
        ),

        # ── Equity (3000s) ─────────────────────────────────────────────
        GLAccountDef(
            number="3100", name="Common Stock",
            acct_type="equity", normal_balance="credit",
            hierarchy_parent="equity", hierarchy_level=2,
            legacy_group="equity.common_stock",
            department="",
            description="Par value of issued shares",
            amount_source="computed",
        ),
        GLAccountDef(
            number="3200", name="Retained Earnings",
            acct_type="equity", normal_balance="credit",
            hierarchy_parent="equity", hierarchy_level=2,
            legacy_group="equity.retained_earnings",
            department="",
            description="Accumulated net income less dividends",
            amount_source="computed",
        ),
    ]
