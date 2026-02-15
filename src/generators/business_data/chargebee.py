"""
Chargebee subscription/billing data generator.

Generates realistic Chargebee-shaped records (Subscriptions, Invoices) that are
financially consistent with the business profile. Active subscription MRR x 12
must approximate ARR from the profile, and churn/retention metrics must align.

All monetary values in the profile (arr, mrr) are in MILLIONS of USD. This
generator converts them to actual dollar amounts for individual records.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile, QuarterMetrics


SOURCE_SYSTEM = "chargebee"

PLAN_TIERS = [
    {"id": "starter-monthly", "name": "Starter Monthly", "amount": 299, "interval": "month"},
    {"id": "starter-annual", "name": "Starter Annual", "amount": 2990, "interval": "year"},
    {"id": "professional-monthly", "name": "Professional Monthly", "amount": 999, "interval": "month"},
    {"id": "professional-annual", "name": "Professional Annual", "amount": 9990, "interval": "year"},
    {"id": "enterprise-monthly", "name": "Enterprise Monthly", "amount": 4999, "interval": "month"},
    {"id": "enterprise-annual", "name": "Enterprise Annual", "amount": 49990, "interval": "year"},
    {"id": "enterprise-custom", "name": "Enterprise Custom", "amount": 0, "interval": "year"},
]

# Distribution weights for plan selection (roughly matching revenue distribution)
PLAN_WEIGHTS = [0.10, 0.15, 0.15, 0.25, 0.05, 0.15, 0.15]

SUBSCRIPTION_STATUSES = ["active", "cancelled", "non_renewing", "paused"]
INVOICE_STATUSES = ["paid", "payment_due", "voided", "not_paid"]
CURRENCIES = ["USD", "EUR", "GBP"]
CURRENCY_WEIGHTS = [0.75, 0.15, 0.10]

SUBSCRIPTION_SCHEMA = [
    {"name": "id", "type": "string", "is_key": True},
    {"name": "customer_id", "type": "string", "semantic_hint": "customer_reference"},
    {"name": "plan_id", "type": "string", "semantic_hint": "plan_reference"},
    {"name": "plan_amount", "type": "number", "semantic_hint": "plan_price"},
    {"name": "currency", "type": "string"},
    {"name": "status", "type": "string", "semantic_hint": "subscription_status"},
    {"name": "started_at", "type": "datetime", "semantic_hint": "subscription_start"},
    {"name": "current_term_start", "type": "datetime"},
    {"name": "current_term_end", "type": "datetime"},
    {"name": "mrr", "type": "number", "semantic_hint": "monthly_recurring_revenue"},
    {"name": "cancelled_at", "type": "datetime"},
]

INVOICE_SCHEMA = [
    {"name": "id", "type": "string", "is_key": True},
    {"name": "subscription_id", "type": "string"},
    {"name": "customer_id", "type": "string", "semantic_hint": "customer_reference"},
    {"name": "date", "type": "date", "semantic_hint": "invoice_date"},
    {"name": "total", "type": "number", "semantic_hint": "invoice_total"},
    {"name": "amount_paid", "type": "number"},
    {"name": "status", "type": "string"},
    {"name": "line_items", "type": "array"},
]


def _plan_monthly_amount(plan: Dict[str, Any]) -> int:
    """Return the effective monthly amount for a plan tier (in cents-level dollars)."""
    if plan["interval"] == "month":
        return plan["amount"]
    # Annual plans: divide by 12 for monthly equivalent
    return plan["amount"] // 12


def _months_in_quarter(quarter: str) -> List[Tuple[int, int]]:
    """Return list of (year, month) tuples for a quarter label like '2024-Q1'."""
    year = int(quarter[:4])
    q = int(quarter[-1])
    start_month = (q - 1) * 3 + 1
    return [(year, start_month + offset) for offset in range(3)]


def _month_start_date(year: int, month: int) -> date:
    """First day of a given month."""
    return date(year, month, 1)


def _month_end_date(year: int, month: int) -> date:
    """Last day of a given month."""
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


class ChargebeeGenerator(BaseBusinessGenerator):
    """
    Generates Chargebee subscription and invoice records aligned with the
    business profile trajectory.

    The generator maintains a customer roster across quarters, tracking
    subscriptions from creation through potential cancellation. Enterprise
    custom plans are used as a balancing mechanism to ensure total MRR
    matches the profile's ARR / 12.
    """

    SOURCE_SYSTEM = "chargebee"
    PIPE_PREFIX = "cb"

    def __init__(self, profile: BusinessProfile, seed: int = 42):
        super().__init__(seed=seed)
        self.profile = profile

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        pipe_id: Optional[str] = None,
        run_id: Optional[str] = None,
        run_timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate all Chargebee data (subscriptions and invoices) and return
        them as DCL-formatted payloads.

        Returns a dict with keys ``"subscriptions"`` and ``"invoices"``,
        each containing a DCL payload dict.
        """
        pipe_id = pipe_id or f"{self.PIPE_PREFIX}_main"
        run_id = run_id or self._cb_id("run")
        run_timestamp = run_timestamp or "2026-12-31T23:59:59Z"

        subscriptions, invoices = self._build_all_records()

        return {
            "subscriptions": self.format_dcl_payload(
                pipe_id=f"{pipe_id}_subscriptions",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=SUBSCRIPTION_SCHEMA,
                data=subscriptions,
            ),
            "invoices": self.format_dcl_payload(
                pipe_id=f"{pipe_id}_invoices",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=INVOICE_SCHEMA,
                data=invoices,
            ),
        }

    # ------------------------------------------------------------------
    # Core record building
    # ------------------------------------------------------------------

    def _build_all_records(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Walk through all quarters, maintain a customer roster, and produce
        subscription + invoice records.

        Returns (subscriptions_list, invoices_list).
        """
        all_subscriptions: List[Dict[str, Any]] = []
        all_invoices: List[Dict[str, Any]] = []

        # Persistent customer roster: customer_id -> subscription dict
        # We track every customer that has ever existed.
        roster: Dict[str, Dict[str, Any]] = {}

        # Pre-generate stable IDs so the same customer keeps the same sub
        # across quarters.
        customer_ids: List[str] = []
        sub_ids: List[str] = []

        # We need enough IDs for the maximum possible customer count across
        # all quarters, plus some buffer for churn and re-entry.
        max_customers = max(q.customer_count for q in self.profile.quarters)
        total_churned = sum(q.churned_customers for q in self.profile.quarters)
        id_pool_size = max_customers + total_churned + 200

        for _ in range(id_pool_size):
            customer_ids.append(self._cb_id("cus"))
            sub_ids.append(self._cb_id("sub"))

        next_id_index = 0

        # --- Seed the initial customer base ---
        # The first quarter's customer_count reflects the count AFTER Q1
        # churn and new adds. To arrive at that count through the normal
        # quarterly loop logic (which applies churn then adds new), we
        # need to seed: customer_count - new_customers + churned_customers.
        first_qm = self.profile.quarters[0]
        initial_existing = (
            first_qm.customer_count - first_qm.new_customers + first_qm.churned_customers
        )
        first_quarter = first_qm.quarter

        for _ in range(initial_existing):
            cid = customer_ids[next_id_index]
            sid = sub_ids[next_id_index]
            next_id_index += 1

            plan = self._weighted_choice(PLAN_TIERS, PLAN_WEIGHTS)
            currency = self._weighted_choice(CURRENCIES, CURRENCY_WEIGHTS)

            # Historical start date: spread across the 2 years before
            # our observation window (2022-01-01 to 2023-12-31)
            days_back = self._rng.randint(1, 730)
            historical_start = date(2024, 1, 1) - timedelta(days=days_back)
            hour = self._rng.randint(0, 23)
            minute = self._rng.randint(0, 59)
            second = self._rng.randint(0, 59)
            started_at = (
                f"{historical_start.isoformat()}"
                f"T{hour:02d}:{minute:02d}:{second:02d}Z"
            )

            sub = self._make_subscription(
                sub_id=sid,
                customer_id=cid,
                plan=plan,
                currency=currency,
                started_at=started_at,
                quarter=first_quarter,
            )
            roster[cid] = sub

        for qi, qm in enumerate(self.profile.quarters):
            quarter = qm.quarter

            # --- Determine who is active, new, and churning this quarter ---
            active_ids = [
                cid for cid, sub in roster.items() if sub["status"] == "active"
            ]
            non_renewing_ids = [
                cid for cid, sub in roster.items() if sub["status"] == "non_renewing"
            ]

            # Churn: first churn the non_renewing from last quarter
            churned_this_quarter: List[str] = []
            for cid in non_renewing_ids:
                roster[cid]["status"] = "cancelled"
                roster[cid]["cancelled_at"] = self._timestamp_in_quarter(quarter)
                churned_this_quarter.append(cid)
                if cid in active_ids:
                    active_ids.remove(cid)

            # If we still need more churn to match the profile, cancel actives
            remaining_churn = max(0, qm.churned_customers - len(churned_this_quarter))
            if remaining_churn > 0 and active_ids:
                churn_candidates = list(active_ids)
                self._rng.shuffle(churn_candidates)
                for cid in churn_candidates[:remaining_churn]:
                    roster[cid]["status"] = "cancelled"
                    roster[cid]["cancelled_at"] = self._timestamp_in_quarter(quarter)
                    churned_this_quarter.append(cid)
                    active_ids.remove(cid)

            # Add new customers
            new_customers_this_q: List[str] = []
            for _ in range(qm.new_customers):
                if next_id_index >= len(customer_ids):
                    # Extend pool if needed
                    customer_ids.append(self._cb_id("cus"))
                    sub_ids.append(self._cb_id("sub"))
                cid = customer_ids[next_id_index]
                sid = sub_ids[next_id_index]
                next_id_index += 1

                plan = self._weighted_choice(PLAN_TIERS, PLAN_WEIGHTS)
                currency = self._weighted_choice(CURRENCIES, CURRENCY_WEIGHTS)
                started_at = self._timestamp_in_quarter(quarter)

                sub = self._make_subscription(
                    sub_id=sid,
                    customer_id=cid,
                    plan=plan,
                    currency=currency,
                    started_at=started_at,
                    quarter=quarter,
                )
                roster[cid] = sub
                new_customers_this_q.append(cid)
                active_ids.append(cid)

            # Mark a fraction of active subs as non_renewing (they will churn
            # next quarter). Use ~15-25% of next quarter's expected churn.
            if qi + 1 < len(self.profile.quarters):
                next_qm = self.profile.quarters[qi + 1]
                non_renewing_target = max(
                    1, int(next_qm.churned_customers * self._rng.uniform(0.15, 0.30))
                )
                # Only mark long-standing actives, not brand-new ones
                eligible = [
                    cid for cid in active_ids if cid not in new_customers_this_q
                ]
                self._rng.shuffle(eligible)
                for cid in eligible[:non_renewing_target]:
                    roster[cid]["status"] = "non_renewing"

            # --- MRR balancing ---
            # Target MRR in actual dollars (profile mrr is in millions)
            target_mrr = qm.mrr * 1_000_000

            # Sum MRR of currently active subscriptions
            current_active_ids = [
                cid for cid, sub in roster.items()
                if sub["status"] in ("active", "non_renewing")
            ]
            current_mrr = sum(roster[cid]["mrr"] for cid in current_active_ids)

            # Identify enterprise-custom subs for balancing
            custom_subs = [
                cid for cid in current_active_ids
                if roster[cid]["plan_id"] == "enterprise-custom"
            ]

            if custom_subs and current_mrr > 0:
                self._balance_mrr(roster, custom_subs, current_mrr, target_mrr)

            # Update term dates for continuing subscriptions
            for cid in current_active_ids:
                sub = roster[cid]
                # Refresh current_term_start/end to reflect this quarter
                q_start = self._quarter_start_date(quarter)
                q_end = self._quarter_end_date(quarter)
                sub["current_term_start"] = f"{q_start}T00:00:00Z"
                sub["current_term_end"] = f"{q_end}T23:59:59Z"

            # --- Generate invoices for this quarter ---
            quarter_invoices = self._generate_quarter_invoices(
                roster, current_active_ids, quarter
            )
            all_invoices.extend(quarter_invoices)

        # Collect final subscription snapshots
        all_subscriptions = list(roster.values())

        return all_subscriptions, all_invoices

    # ------------------------------------------------------------------
    # Subscription construction
    # ------------------------------------------------------------------

    def _make_subscription(
        self,
        sub_id: str,
        customer_id: str,
        plan: Dict[str, Any],
        currency: str,
        started_at: str,
        quarter: str,
    ) -> Dict[str, Any]:
        """Create a new subscription record."""
        plan_amount = plan["amount"]

        if plan["id"] == "enterprise-custom":
            # Custom enterprise amounts range from $8,000 to $120,000/year
            plan_amount = self._rng.randint(8000, 120000)

        # MRR calculation
        if plan["interval"] == "month":
            mrr = plan_amount
        else:
            mrr = round(plan_amount / 12)

        # Occasionally apply a small discount or add-on variance to MRR
        if self._rng.random() < 0.20:
            variance_factor = self._rng.uniform(0.85, 1.15)
            mrr = round(mrr * variance_factor)

        q_start = self._quarter_start_date(quarter)
        q_end = self._quarter_end_date(quarter)

        return {
            "id": sub_id,
            "customer_id": customer_id,
            "plan_id": plan["id"],
            "plan_amount": plan_amount,
            "currency": currency,
            "status": "active",
            "started_at": started_at,
            "current_term_start": f"{q_start}T00:00:00Z",
            "current_term_end": f"{q_end}T23:59:59Z",
            "mrr": mrr,
            "cancelled_at": None,
        }

    # ------------------------------------------------------------------
    # MRR balancing
    # ------------------------------------------------------------------

    def _balance_mrr(
        self,
        roster: Dict[str, Dict[str, Any]],
        custom_sub_ids: List[str],
        current_mrr: float,
        target_mrr: float,
    ) -> None:
        """
        Adjust enterprise-custom subscription MRR values so that total
        active MRR approximates the profile target (within +/-2%).

        The gap between current MRR (from fixed-tier plans) and target MRR
        is distributed proportionally across enterprise-custom subscriptions.
        """
        if not custom_sub_ids:
            return

        gap = target_mrr - current_mrr
        # Sum current MRR from custom subs only
        custom_mrr_total = sum(roster[cid]["mrr"] for cid in custom_sub_ids)

        if custom_mrr_total <= 0:
            # All custom subs have zero MRR; distribute gap evenly
            per_sub = max(round(gap / len(custom_sub_ids)), 500)
            for cid in custom_sub_ids:
                roster[cid]["mrr"] = per_sub
                roster[cid]["plan_amount"] = per_sub * 12
            return

        # Desired total custom MRR = existing custom MRR + gap
        desired_custom_mrr = custom_mrr_total + gap

        if desired_custom_mrr <= 0:
            # Target is lower than what fixed-tier plans already produce.
            # Set custom subs to a minimal amount.
            for cid in custom_sub_ids:
                roster[cid]["mrr"] = 500
                roster[cid]["plan_amount"] = 6000
            return

        # Scale each custom sub proportionally
        scale_factor = desired_custom_mrr / custom_mrr_total
        for cid in custom_sub_ids:
            old_mrr = roster[cid]["mrr"]
            new_mrr = max(round(old_mrr * scale_factor), 500)
            roster[cid]["mrr"] = new_mrr
            roster[cid]["plan_amount"] = new_mrr * 12

    # ------------------------------------------------------------------
    # Invoice generation
    # ------------------------------------------------------------------

    def _generate_quarter_invoices(
        self,
        roster: Dict[str, Dict[str, Any]],
        active_ids: List[str],
        quarter: str,
    ) -> List[Dict[str, Any]]:
        """
        Generate invoices for all active subscriptions in a quarter.

        Monthly subscriptions get one invoice per month (3 per quarter).
        Annual subscriptions get one invoice per quarter.
        """
        invoices: List[Dict[str, Any]] = []
        months = _months_in_quarter(quarter)

        for cid in active_ids:
            sub = roster[cid]
            plan_tier = self._find_plan(sub["plan_id"])
            is_annual = plan_tier is not None and plan_tier["interval"] == "year"

            if is_annual:
                # One invoice per quarter for annual plans
                inv_month = self._pick(months)
                invoice = self._make_invoice(sub, inv_month[0], inv_month[1])
                invoices.append(invoice)
            else:
                # One invoice per month for monthly plans
                for year, month in months:
                    invoice = self._make_invoice(sub, year, month)
                    invoices.append(invoice)

        return invoices

    def _make_invoice(
        self,
        sub: Dict[str, Any],
        year: int,
        month: int,
    ) -> Dict[str, Any]:
        """Create a single invoice record for a subscription in a given month."""
        plan_tier = self._find_plan(sub["plan_id"])

        # Invoice total matches the subscription's billing amount
        if plan_tier and plan_tier["interval"] == "year":
            # Annual billing: invoice for the quarterly portion (plan_amount / 4)
            # or full annual amount once a year. We use quarterly installments.
            total = round(sub["plan_amount"] / 4)
        else:
            # Monthly billing: invoice = mrr
            total = sub["mrr"]

        # Invoice status distribution: ~85% paid, ~10% payment_due, ~3% not_paid, ~2% voided
        status = self._weighted_choice(
            INVOICE_STATUSES,
            [0.85, 0.10, 0.03, 0.02],
        )

        if status == "paid":
            amount_paid = total
        elif status == "payment_due":
            amount_paid = 0
        elif status == "voided":
            amount_paid = 0
            total = 0
        else:
            # not_paid
            amount_paid = 0

        # Invoice date: random day within the billing month
        month_end = _month_end_date(year, month)
        day = self._rng.randint(1, month_end.day)
        inv_date = date(year, month, day).isoformat()

        # Build line items
        line_items = self._build_line_items(sub, plan_tier, total)

        return {
            "id": self._cb_id("inv"),
            "subscription_id": sub["id"],
            "customer_id": sub["customer_id"],
            "date": inv_date,
            "total": total,
            "amount_paid": amount_paid,
            "status": status,
            "line_items": line_items,
        }

    def _build_line_items(
        self,
        sub: Dict[str, Any],
        plan_tier: Optional[Dict[str, Any]],
        invoice_total: int,
    ) -> List[Dict[str, Any]]:
        """
        Build the line_items array for an invoice.

        Always includes the base plan charge. Occasionally includes add-on
        items (support, extra seats, API access) that explain the variance
        between plan_amount and actual MRR.
        """
        plan_name = plan_tier["name"] if plan_tier else "Enterprise Custom"

        items: List[Dict[str, Any]] = []

        # Primary plan line item
        if plan_tier and plan_tier["interval"] == "year":
            base_amount = round(sub["plan_amount"] / 4)
        else:
            base_amount = sub["mrr"]

        # If there is a difference between base_amount and invoice_total,
        # the remainder comes from add-ons
        addon_amount = invoice_total - base_amount

        items.append({
            "id": self._cb_id("li"),
            "description": plan_name,
            "amount": base_amount,
            "quantity": 1,
            "type": "plan",
        })

        # Generate add-on line items if there is a positive addon amount
        if addon_amount > 0:
            addon_options = [
                ("Premium Support", "addon"),
                ("Additional Seats", "addon"),
                ("API Access Pack", "addon"),
                ("Data Export Module", "addon"),
            ]
            # Split addon amount across 1-2 add-ons
            num_addons = self._rng.randint(1, min(2, len(addon_options)))
            selected = self._rng.sample(addon_options, num_addons)

            remaining = addon_amount
            for idx, (name, item_type) in enumerate(selected):
                if idx == len(selected) - 1:
                    amt = remaining
                else:
                    amt = self._rng.randint(1, max(1, remaining - 1))
                    remaining -= amt

                items.append({
                    "id": self._cb_id("li"),
                    "description": name,
                    "amount": max(amt, 0),
                    "quantity": 1,
                    "type": item_type,
                })
        elif self._rng.random() < 0.10:
            # ~10% chance of a small add-on even when amounts align
            addon_name = self._pick([
                "Premium Support",
                "Additional Seats",
                "API Access Pack",
            ])
            addon_amt = self._rng.choice([49, 99, 149, 199, 299])
            items.append({
                "id": self._cb_id("li"),
                "description": addon_name,
                "amount": addon_amt,
                "quantity": 1,
                "type": "addon",
            })

        return items

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_plan(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """Look up a plan tier by its ID."""
        for plan in PLAN_TIERS:
            if plan["id"] == plan_id:
                return plan
        return None
