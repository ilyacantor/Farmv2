"""
NetSuite ERP data generator for the FARM business data pipeline.

Generates realistic NetSuite-shaped records (Invoices, Revenue Schedules,
GL Journal Entries, Accounts Receivable, Accounts Payable) that are financially
consistent with the business profile. Revenue in NetSuite intentionally lags
Salesforce by ~5-10% per quarter due to rev rec timing differences, reflecting
how real ERP systems recognize revenue vs. CRM bookings.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "netsuite"

INVOICE_STATUSES = ["Paid In Full", "Open", "Partially Paid"]
CURRENCIES = ["USD", "EUR", "GBP"]
CURRENCY_WEIGHTS = [0.75, 0.15, 0.10]

SUBSIDIARIES = ["AOS Corp - US", "AOS Corp - EMEA", "AOS Corp - APAC"]
DEPARTMENTS = ["Sales", "Engineering", "Marketing", "CS", "G&A", "Product"]

GL_ACCOUNTS = [
    {"number": "4000", "name": "Product Revenue"},
    {"number": "4100", "name": "Service Revenue"},
    {"number": "4200", "name": "Subscription Revenue"},
    {"number": "5000", "name": "Cost of Revenue"},
    {"number": "5100", "name": "Hosting Costs"},
    {"number": "6000", "name": "Salaries & Wages"},
    {"number": "6100", "name": "Benefits"},
    {"number": "6200", "name": "Marketing Expense"},
    {"number": "6300", "name": "Travel & Entertainment"},
    {"number": "6400", "name": "Software & Tools"},
    {"number": "6500", "name": "Professional Services"},
    {"number": "6600", "name": "Office & Facilities"},
    {"number": "7000", "name": "Depreciation"},
    {"number": "1000", "name": "Cash"},
    {"number": "1100", "name": "Accounts Receivable"},
    {"number": "2000", "name": "Accounts Payable"},
    {"number": "2100", "name": "Deferred Revenue"},
]

AGING_BUCKETS = ["Current", "1-30", "31-60", "61-90", "90+"]

AP_VENDORS = [
    "AWS", "Google Cloud", "Azure", "Datadog", "Snowflake",
    "WeWork", "Salesforce", "Okta", "GitHub", "Slack",
    "Gusto", "Brex", "Stripe", "HubSpot", "Zoom",
]
AP_STATUSES = ["Paid", "Open", "Pending Approval"]

REV_SCHEDULE_TYPES = ["Straight-Line", "Milestone", "Usage-Based"]

# Revenue account numbers for GL mapping
_REVENUE_ACCOUNT_NUMBERS = {"4000", "4100", "4200"}
_COGS_ACCOUNT_NUMBERS = {"5000", "5100"}
_OPEX_ACCOUNT_NUMBERS = {"6000", "6100", "6200", "6300", "6400", "6500", "6600", "7000"}

# Lookup helpers
_REVENUE_ACCOUNTS = [a for a in GL_ACCOUNTS if a["number"] in _REVENUE_ACCOUNT_NUMBERS]
_COGS_ACCOUNTS = [a for a in GL_ACCOUNTS if a["number"] in _COGS_ACCOUNT_NUMBERS]
_OPEX_ACCOUNTS = [a for a in GL_ACCOUNTS if a["number"] in _OPEX_ACCOUNT_NUMBERS]
_CASH_ACCOUNT = {"number": "1000", "name": "Cash"}
_AR_ACCOUNT = {"number": "1100", "name": "Accounts Receivable"}
_AP_ACCOUNT = {"number": "2000", "name": "Accounts Payable"}
_DEFERRED_REV_ACCOUNT = {"number": "2100", "name": "Deferred Revenue"}

# Schema definitions -------------------------------------------------------

INVOICE_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "internal_id", "type": "number", "is_key": True},
    {"name": "tran_id", "type": "string"},
    {"name": "entity_id", "type": "string", "semantic_hint": "customer_reference"},
    {"name": "tran_date", "type": "date", "semantic_hint": "transaction_date"},
    {"name": "amount", "type": "number", "semantic_hint": "revenue"},
    {"name": "currency", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "subsidiary", "type": "string"},
    {"name": "department", "type": "string"},
    {"name": "class_segment", "type": "string"},
    {"name": "posting_period", "type": "string", "semantic_hint": "fiscal_period"},
]

REV_SCHEDULE_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "internal_id", "type": "number", "is_key": True},
    {"name": "source_tran_id", "type": "string"},
    {"name": "rev_rec_start", "type": "date", "semantic_hint": "recognition_start"},
    {"name": "rev_rec_end", "type": "date", "semantic_hint": "recognition_end"},
    {"name": "amount", "type": "number", "semantic_hint": "recognized_revenue"},
    {"name": "schedule_type", "type": "string"},
]

GL_ENTRY_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "internal_id", "type": "number", "is_key": True},
    {"name": "tran_date", "type": "date", "semantic_hint": "transaction_date"},
    {"name": "account_number", "type": "string"},
    {"name": "account_name", "type": "string"},
    {"name": "debit", "type": "number"},
    {"name": "credit", "type": "number"},
    {"name": "department", "type": "string"},
    {"name": "class_segment", "type": "string"},
    {"name": "posting_period", "type": "string", "semantic_hint": "fiscal_period"},
]

AR_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "internal_id", "type": "number", "is_key": True},
    {"name": "entity_id", "type": "string", "semantic_hint": "customer_reference"},
    {"name": "due_date", "type": "date"},
    {"name": "amount_due", "type": "number", "semantic_hint": "receivable_amount"},
    {"name": "amount_paid", "type": "number"},
    {"name": "days_outstanding", "type": "number"},
    {"name": "aging_bucket", "type": "string"},
]

AP_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "internal_id", "type": "number", "is_key": True},
    {"name": "vendor_id", "type": "string", "semantic_hint": "vendor_reference"},
    {"name": "due_date", "type": "date"},
    {"name": "amount", "type": "number", "semantic_hint": "payable_amount"},
    {"name": "status", "type": "string"},
]

# Class segments used on invoices and GL entries
CLASS_SEGMENTS = ["SaaS", "Professional Services", "Support", "Training"]


class NetSuiteGenerator(BaseBusinessGenerator):
    """
    Generates NetSuite ERP records that are financially consistent with the
    business profile while reflecting real-world ERP data patterns.

    Key design choices:
    - Revenue recognised in NetSuite is intentionally ~5-10% lower than the
      Salesforce bookings figure for any given quarter. This models the real
      revenue recognition timing lag where multi-period SaaS deals are booked
      in the CRM but only partially recognized in the ERP.
    - GL journal entries follow strict double-entry rules: debits == credits.
    - Aging buckets on AR follow a realistic distribution curve.
    """

    SOURCE_SYSTEM = SOURCE_SYSTEM
    PIPE_PREFIX = "ns"

    def __init__(self, profile: BusinessProfile, seed: int = 42):
        super().__init__(seed=seed)
        self.profile = profile
        # Running invoice counter for sequential INV-##### tran_ids
        self._invoice_counter = 10000
        # Customer pool -- reused across quarters for entity_id consistency
        self._customer_pool: List[str] = self._build_customer_pool()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_customer_pool(self) -> List[str]:
        """Pre-generate a pool of customer entity IDs."""
        pool = []
        for i in range(500):
            pool.append(f"CUST-{self._rng.randint(10000, 99999)}")
        return pool

    def _next_tran_id(self) -> str:
        """Return the next sequential invoice tran_id."""
        self._invoice_counter += 1
        return f"INV-{self._invoice_counter}"

    def _pick_customer(self) -> str:
        return self._pick(self._customer_pool)

    def _pick_subsidiary(self, currency: str) -> str:
        """Pick a subsidiary that aligns loosely with currency."""
        if currency == "EUR":
            return "AOS Corp - EMEA"
        if currency == "GBP":
            return "AOS Corp - EMEA"
        if self._rng.random() < 0.15:
            return self._pick(["AOS Corp - EMEA", "AOS Corp - APAC"])
        return "AOS Corp - US"

    def _pick_class_segment(self) -> str:
        return self._weighted_choice(
            CLASS_SEGMENTS,
            [0.55, 0.20, 0.15, 0.10],
        )

    def _month_for_date(self, iso_date: str) -> int:
        """Extract month number from an ISO date string."""
        return int(iso_date[5:7])

    def _quarter_for_date(self, iso_date: str) -> str:
        """Derive the quarter label from an ISO date string."""
        year = int(iso_date[:4])
        month = int(iso_date[5:7])
        q = (month - 1) // 3 + 1
        return f"{year}-Q{q}"

    def _add_months(self, iso_date: str, months: int) -> str:
        """Add calendar months to an ISO date string."""
        d = date.fromisoformat(iso_date)
        month = d.month + months
        year = d.year + (month - 1) // 12
        month = (month - 1) % 12 + 1
        # Clamp day to valid range for target month
        day = min(d.day, self._last_day_of_month(year, month))
        return date(year, month, day).isoformat()

    @staticmethod
    def _last_day_of_month(year: int, month: int) -> int:
        if month == 12:
            return 31
        return (date(year, month + 1, 1) - timedelta(days=1)).day

    def _distribute_amounts(
        self, total: float, n: int, min_frac: float = 0.0005, max_frac: float = 0.03
    ) -> List[float]:
        """
        Distribute *total* across *n* items using a Dirichlet-like approach
        so amounts look realistic (a few large invoices, many small ones).

        Returns a list of floats summing (approximately) to *total*.
        """
        # Generate raw weights with a right-skewed distribution
        raw = []
        for _ in range(n):
            # Log-normal-ish weight via exponential of a uniform
            w = self._rng.expovariate(1.0)
            raw.append(w)
        s = sum(raw)
        amounts = [total * (w / s) for w in raw]

        # Clamp individual amounts to [min_frac*total, max_frac*total]
        floor = total * min_frac
        ceiling = total * max_frac
        amounts = [max(floor, min(a, ceiling)) for a in amounts]

        # Re-normalise so the sum equals total
        correction = total / sum(amounts) if sum(amounts) > 0 else 1.0
        amounts = [round(a * correction, 2) for a in amounts]

        # Fix any rounding residual on the last element
        residual = round(total - sum(amounts[:-1]), 2)
        amounts[-1] = residual

        return amounts

    # ------------------------------------------------------------------
    # Invoice generation
    # ------------------------------------------------------------------

    def _generate_invoices_for_quarter(
        self, quarter: str, revenue_millions: float
    ) -> List[Dict[str, Any]]:
        """
        Generate ~250 invoices for a single quarter whose amounts sum
        approximately to the revenue figure (in actual dollars).
        """
        target_revenue = revenue_millions * 1_000_000
        num_invoices = self._rng.randint(235, 265)

        amounts = self._distribute_amounts(target_revenue, num_invoices)

        invoices: List[Dict[str, Any]] = []
        for amt in amounts:
            currency = self._weighted_choice(CURRENCIES, CURRENCY_WEIGHTS)
            subsidiary = self._pick_subsidiary(currency)
            department = self._pick(DEPARTMENTS)
            class_segment = self._pick_class_segment()

            # Apply FX multiplier for non-USD to keep USD-equivalent correct
            # while making the local-currency amount differ
            display_amount = amt
            if currency == "EUR":
                display_amount = round(amt * self._rng.uniform(0.88, 0.94), 2)
            elif currency == "GBP":
                display_amount = round(amt * self._rng.uniform(0.78, 0.84), 2)

            tran_date = self._date_in_quarter(quarter)
            posting_period = self._posting_period(quarter)
            status = self._weighted_choice(
                INVOICE_STATUSES, [0.70, 0.20, 0.10]
            )

            invoices.append({
                "internal_id": self._ns_id(),
                "tran_id": self._next_tran_id(),
                "entity_id": self._pick_customer(),
                "tran_date": tran_date,
                "amount": round(amt, 2),
                "currency": currency,
                "status": status,
                "subsidiary": subsidiary,
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })

        return invoices

    # ------------------------------------------------------------------
    # Revenue schedule generation
    # ------------------------------------------------------------------

    def _generate_rev_schedules_for_quarter(
        self,
        quarter: str,
        invoices: List[Dict[str, Any]],
        quarter_idx: int,
        total_quarters: int,
    ) -> List[Dict[str, Any]]:
        """
        Generate revenue recognition schedules from the invoices created in
        this quarter.

        Design:
        - ~70% of invoices are recognised immediately (point-in-time): the
          ``amount`` equals the full invoice value with rec_start == rec_end.
        - ~30% of invoices are multi-period (12-month SaaS subscriptions):
          the ``amount`` reflects only the portion recognised *so far* within
          the originating quarter (approximately 3/12 of the contract value,
          adjusted for timing lag).
        - This produces a consistent ~5-10% gap between total invoice bookings
          (what Salesforce sees) and total recognised revenue (what NetSuite
          reports), modelling the real-world rev rec timing lag in ERP systems.

        The ``amount`` field uses the ``recognized_revenue`` semantic as
        defined in the schema -- it is the revenue that has been recognised
        through the schedule, not the total contract value.
        """
        schedules: List[Dict[str, Any]] = []

        # Target a revenue delta of 5-10% below invoice bookings.
        # With ~30% of invoices being multi-period and each recognising
        # ~25% in-quarter, the deferred amount per quarter is:
        #   ~30% * ~75% = ~22.5% of multi-period invoice value
        # which is ~6.75% of total invoice value -- right in the target range.
        # The remaining ~93.25% is the recognised revenue.

        for inv in invoices:
            is_multi_period = self._rng.random() < 0.30

            if is_multi_period:
                # Multi-period rev rec (12-month SaaS subscriptions).
                # Only a portion of the contract value is recognised in the
                # originating quarter -- the rest is deferred to future
                # periods. This creates the ~5-10% revenue delta between
                # what Salesforce books and what NetSuite recognises.
                #
                # With ~30% of invoices being multi-period and each
                # recognising ~75% in-quarter on average, the deferred
                # amount is ~30% * 25% = ~7.5% of total quarterly revenue.
                schedule_type = self._weighted_choice(
                    REV_SCHEDULE_TYPES, [0.70, 0.20, 0.10]
                )
                schedule_months = self._weighted_choice(
                    [12, 6, 3], [0.50, 0.30, 0.20]
                )

                inv_date = inv["tran_date"]
                inv_month = self._month_for_date(inv_date)

                # Timing lag: if invoice is in last month of quarter, recognition
                # may start the following month
                last_month_of_q = int(quarter[-1]) * 3
                if inv_month == last_month_of_q and self._rng.random() < 0.50:
                    # Start recognition next month (spills into next quarter)
                    rec_start = self._add_months(inv_date, 1)
                    # Pin to 1st of that month
                    rec_start = rec_start[:8] + "01"
                else:
                    rec_start = inv_date[:8] + "01"

                rec_end = self._add_months(rec_start, schedule_months - 1)
                # Pin to last day of end month
                end_y = int(rec_end[:4])
                end_m = int(rec_end[5:7])
                rec_end = date(
                    end_y, end_m, self._last_day_of_month(end_y, end_m)
                ).isoformat()

                # Recognised fraction: the portion of the schedule's total
                # value that has been recognised through end of this quarter.
                # We target ~75% average (with jitter) so that the ~30%
                # multi-period population produces a ~7.5% total revenue
                # delta. Some deals are fully deferred (timing lag to next
                # quarter), others have most value recognised already.
                q_end = date.fromisoformat(self._quarter_end_date(quarter))
                rec_start_date = date.fromisoformat(rec_start)

                if rec_start_date > q_end:
                    # Recognition starts next quarter; nothing recognised yet
                    recognized_fraction = 0.0
                else:
                    # Base fraction depends on how much of the schedule falls
                    # in this quarter, but we use a controlled distribution
                    # that produces the right average delta.
                    recognized_fraction = self._rng.uniform(0.74, 0.90)

                recognized_amount = round(inv["amount"] * recognized_fraction, 2)

                schedules.append({
                    "internal_id": self._ns_id(),
                    "source_tran_id": inv["tran_id"],
                    "rev_rec_start": rec_start,
                    "rev_rec_end": rec_end,
                    "amount": recognized_amount,
                    "schedule_type": schedule_type,
                })
            # else: immediate recognition -- no separate rev schedule
            # record needed. Revenue is recognised at the invoice date.

        return schedules

    # ------------------------------------------------------------------
    # GL journal entry generation
    # ------------------------------------------------------------------

    def _generate_gl_entries_for_quarter(
        self,
        quarter: str,
        revenue_actual: float,
        cogs_millions: float,
        opex_millions: float,
    ) -> List[Dict[str, Any]]:
        """
        Generate double-entry GL journal entries for a quarter.

        Three categories:
        1. Revenue entries -- debit Cash/AR, credit Revenue accounts
        2. COGS entries -- debit COGS accounts, credit Cash/AP
        3. OpEx entries -- debit Expense accounts, credit Cash/AP

        Total debits == total credits for the quarter.
        """
        entries: List[Dict[str, Any]] = []

        revenue_total = revenue_actual  # already in dollars
        cogs_total = cogs_millions * 1_000_000
        opex_total = opex_millions * 1_000_000

        # --- Revenue journal entries (~150 per quarter) ---
        num_rev_entries = self._rng.randint(135, 165)
        rev_amounts = self._distribute_amounts(revenue_total, num_rev_entries)

        for amt in rev_amounts:
            tran_date = self._date_in_quarter(quarter)
            posting_period = self._posting_period(quarter)
            department = self._pick(DEPARTMENTS)
            class_segment = self._pick_class_segment()
            rev_account = self._pick(_REVENUE_ACCOUNTS)

            # Debit side: Cash or AR
            debit_account = self._weighted_choice(
                [_CASH_ACCOUNT, _AR_ACCOUNT], [0.65, 0.35]
            )

            # Debit entry
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": debit_account["number"],
                "account_name": debit_account["name"],
                "debit": round(amt, 2),
                "credit": 0.0,
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })
            # Credit entry (revenue)
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": rev_account["number"],
                "account_name": rev_account["name"],
                "debit": 0.0,
                "credit": round(amt, 2),
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })

        # --- COGS journal entries (~80 per quarter) ---
        num_cogs_entries = self._rng.randint(70, 90)
        cogs_amounts = self._distribute_amounts(cogs_total, num_cogs_entries)

        for amt in cogs_amounts:
            tran_date = self._date_in_quarter(quarter)
            posting_period = self._posting_period(quarter)
            department = self._weighted_choice(
                DEPARTMENTS,
                [0.10, 0.40, 0.05, 0.25, 0.10, 0.10],
            )
            class_segment = self._pick_class_segment()
            cogs_account = self._pick(_COGS_ACCOUNTS)
            credit_account = self._weighted_choice(
                [_CASH_ACCOUNT, _AP_ACCOUNT], [0.55, 0.45]
            )

            # Debit COGS
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": cogs_account["number"],
                "account_name": cogs_account["name"],
                "debit": round(amt, 2),
                "credit": 0.0,
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })
            # Credit Cash/AP
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": credit_account["number"],
                "account_name": credit_account["name"],
                "debit": 0.0,
                "credit": round(amt, 2),
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })

        # --- OpEx journal entries (~180 per quarter) ---
        num_opex_entries = self._rng.randint(165, 195)
        opex_amounts = self._distribute_amounts(opex_total, num_opex_entries)

        for amt in opex_amounts:
            tran_date = self._date_in_quarter(quarter)
            posting_period = self._posting_period(quarter)
            department = self._pick(DEPARTMENTS)
            class_segment = self._pick_class_segment()
            opex_account = self._pick(_OPEX_ACCOUNTS)
            credit_account = self._weighted_choice(
                [_CASH_ACCOUNT, _AP_ACCOUNT], [0.60, 0.40]
            )

            # Debit Expense
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": opex_account["number"],
                "account_name": opex_account["name"],
                "debit": round(amt, 2),
                "credit": 0.0,
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })
            # Credit Cash/AP
            entries.append({
                "internal_id": self._ns_id(),
                "tran_date": tran_date,
                "account_number": credit_account["number"],
                "account_name": credit_account["name"],
                "debit": 0.0,
                "credit": round(amt, 2),
                "department": department,
                "class_segment": class_segment,
                "posting_period": posting_period,
            })

        return entries

    # ------------------------------------------------------------------
    # Accounts Receivable generation
    # ------------------------------------------------------------------

    def _generate_ar_for_quarter(
        self, quarter: str, revenue_millions: float
    ) -> List[Dict[str, Any]]:
        """
        Generate an AR snapshot at quarter end.

        Total AR outstanding is ~15-20% of quarterly revenue (DSO ~45-55 days).
        Distributed across aging buckets following a realistic curve.
        """
        quarterly_revenue = revenue_millions * 1_000_000
        dso_fraction = self._rng.uniform(0.15, 0.20)
        total_ar = quarterly_revenue * dso_fraction

        # Target ~33 AR records per quarter (12 quarters * ~33 = ~400 total)
        num_records = self._rng.randint(28, 38)

        ar_amounts = self._distribute_amounts(total_ar, num_records)

        # Aging bucket weights: 50% Current, 25% 1-30, 15% 31-60, 7% 61-90, 3% 90+
        bucket_weights = [0.50, 0.25, 0.15, 0.07, 0.03]

        quarter_end = date.fromisoformat(self._quarter_end_date(quarter))

        records: List[Dict[str, Any]] = []
        for amt in ar_amounts:
            bucket = self._weighted_choice(AGING_BUCKETS, bucket_weights)

            # Determine days_outstanding based on bucket
            if bucket == "Current":
                days = self._rng.randint(0, 0)
            elif bucket == "1-30":
                days = self._rng.randint(1, 30)
            elif bucket == "31-60":
                days = self._rng.randint(31, 60)
            elif bucket == "61-90":
                days = self._rng.randint(61, 90)
            else:  # 90+
                days = self._rng.randint(91, 180)

            due_date = (quarter_end - timedelta(days=days)).isoformat()

            # Partially paid amounts -- older buckets more likely to have payments
            if bucket in ("61-90", "90+") and self._rng.random() < 0.40:
                amount_paid = round(amt * self._rng.uniform(0.10, 0.50), 2)
            elif bucket in ("31-60",) and self._rng.random() < 0.25:
                amount_paid = round(amt * self._rng.uniform(0.05, 0.30), 2)
            else:
                amount_paid = 0.0

            records.append({
                "internal_id": self._ns_id(),
                "entity_id": self._pick_customer(),
                "due_date": due_date,
                "amount_due": round(amt, 2),
                "amount_paid": amount_paid,
                "days_outstanding": days,
                "aging_bucket": bucket,
            })

        return records

    # ------------------------------------------------------------------
    # Accounts Payable generation
    # ------------------------------------------------------------------

    def _generate_ap_for_quarter(
        self, quarter: str, opex_millions: float
    ) -> List[Dict[str, Any]]:
        """
        Generate an AP snapshot at quarter end.

        Total AP outstanding is ~8-12% of quarterly OpEx.
        """
        quarterly_opex = opex_millions * 1_000_000
        ap_fraction = self._rng.uniform(0.08, 0.12)
        total_ap = quarterly_opex * ap_fraction

        # Target ~25 AP records per quarter (12 quarters * ~25 = ~300 total)
        num_records = self._rng.randint(22, 28)

        ap_amounts = self._distribute_amounts(total_ap, num_records)

        quarter_end = date.fromisoformat(self._quarter_end_date(quarter))

        records: List[Dict[str, Any]] = []
        for amt in ap_amounts:
            vendor = self._pick(AP_VENDORS)
            status = self._weighted_choice(
                AP_STATUSES, [0.50, 0.35, 0.15]
            )

            # Due dates spread around quarter end
            days_offset = self._rng.randint(-30, 30)
            due_date = (quarter_end + timedelta(days=days_offset)).isoformat()

            records.append({
                "internal_id": self._ns_id(),
                "vendor_id": vendor,
                "due_date": due_date,
                "amount": round(amt, 2),
                "status": status,
            })

        return records

    # ------------------------------------------------------------------
    # Revenue delta calculation
    # ------------------------------------------------------------------

    def _compute_ns_recognized_revenue(
        self,
        invoices: List[Dict[str, Any]],
        rev_schedules: List[Dict[str, Any]],
        quarter: str,
    ) -> float:
        """
        Sum total recognised revenue for a given quarter.

        Invoices that do *not* have a matching rev schedule are assumed to be
        immediately recognised at the invoice date (full amount counts if the
        invoice falls within the quarter).

        Rev schedule entries represent multi-period deals whose ``amount``
        is the portion recognised in the originating quarter. The amount is
        included if the schedule's recognition window overlaps the target
        quarter.
        """
        total = 0.0
        q_start = date.fromisoformat(self._quarter_start_date(quarter))
        q_end = date.fromisoformat(self._quarter_end_date(quarter))

        # Build set of tran_ids that have a rev schedule
        scheduled_tran_ids = {s["source_tran_id"] for s in rev_schedules}

        # Immediate-recognition invoices (no rev schedule)
        for inv in invoices:
            if inv["tran_id"] not in scheduled_tran_ids:
                inv_date = date.fromisoformat(inv["tran_date"])
                if q_start <= inv_date <= q_end:
                    total += inv["amount"]

        # Multi-period rev schedules
        for sched in rev_schedules:
            rec_start = date.fromisoformat(sched["rev_rec_start"])
            rec_end = date.fromisoformat(sched["rev_rec_end"])

            # Include if schedule window overlaps target quarter
            if rec_start <= q_end and rec_end >= q_start:
                total += sched["amount"]

        return total

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate(
        self,
        pipe_id: str = "ns-erp-001",
        run_id: str = "run-ns-001",
        run_timestamp: str = "2026-01-15T00:00:00Z",
    ) -> Dict[str, Dict[str, Any]]:
        """
        Generate all five NetSuite object types across 12 quarters.

        Returns a dict with keys:
            - "invoices": DCL payload dict
            - "rev_schedules": DCL payload dict
            - "gl_entries": DCL payload dict
            - "ar": DCL payload dict
            - "ap": DCL payload dict
        """
        all_invoices: List[Dict[str, Any]] = []
        all_rev_schedules: List[Dict[str, Any]] = []
        all_gl_entries: List[Dict[str, Any]] = []
        all_ar: List[Dict[str, Any]] = []
        all_ap: List[Dict[str, Any]] = []

        for q_idx, qm in enumerate(self.profile.quarters):
            quarter = qm.quarter

            # ----------------------------------------------------------
            # 1. Invoices
            # ----------------------------------------------------------
            invoices = self._generate_invoices_for_quarter(
                quarter, qm.revenue
            )
            all_invoices.extend(invoices)

            # ----------------------------------------------------------
            # 2. Revenue Schedules
            # ----------------------------------------------------------
            rev_schedules = self._generate_rev_schedules_for_quarter(
                quarter,
                invoices,
                q_idx,
                len(self.profile.quarters),
            )
            all_rev_schedules.extend(rev_schedules)

            # ----------------------------------------------------------
            # 3. GL Journal Entries
            #    Use the *invoice total* (in dollars) as the revenue figure
            #    for GL entries so that debits/credits are self-consistent
            #    with the invoices generated above.
            # ----------------------------------------------------------
            invoice_revenue_total = sum(inv["amount"] for inv in invoices)
            gl_entries = self._generate_gl_entries_for_quarter(
                quarter,
                invoice_revenue_total,
                qm.cogs,
                qm.opex,
            )
            all_gl_entries.extend(gl_entries)

            # ----------------------------------------------------------
            # 4. Accounts Receivable (snapshot at quarter end)
            # ----------------------------------------------------------
            ar_records = self._generate_ar_for_quarter(quarter, qm.revenue)
            all_ar.extend(ar_records)

            # ----------------------------------------------------------
            # 5. Accounts Payable (snapshot at quarter end)
            # ----------------------------------------------------------
            ap_records = self._generate_ap_for_quarter(quarter, qm.opex)
            all_ap.extend(ap_records)

        # Format each object type as a DCL payload
        return {
            "invoices": self.format_dcl_payload(
                pipe_id=f"{pipe_id}-invoices",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=INVOICE_SCHEMA_FIELDS,
                data=all_invoices,
            ),
            "rev_schedules": self.format_dcl_payload(
                pipe_id=f"{pipe_id}-rev-schedules",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=REV_SCHEDULE_SCHEMA_FIELDS,
                data=all_rev_schedules,
            ),
            "gl_entries": self.format_dcl_payload(
                pipe_id=f"{pipe_id}-gl-entries",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=GL_ENTRY_SCHEMA_FIELDS,
                data=all_gl_entries,
            ),
            "ar": self.format_dcl_payload(
                pipe_id=f"{pipe_id}-ar",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=AR_SCHEMA_FIELDS,
                data=all_ar,
            ),
            "ap": self.format_dcl_payload(
                pipe_id=f"{pipe_id}-ap",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=AP_SCHEMA_FIELDS,
                data=all_ap,
            ),
        }
