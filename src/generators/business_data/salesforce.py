"""
Salesforce CRM data generator.

Generates realistic Salesforce-shaped records (Accounts, Users/Sales Reps,
Opportunities) that are financially consistent with the business profile.
Uses Salesforce-style PascalCase field naming with ``__c`` suffix for custom
fields.  Opportunity Amounts across each quarter sum to approximately the
profile's quarterly revenue (converted from millions to actual dollars).
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile, QuarterMetrics, REGIONS, REGION_WEIGHT_LIST

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "salesforce"

# Standard Salesforce opportunity stages in pipeline order
STAGES = [
    "Prospecting",
    "Qualification",
    "Needs Analysis",
    "Value Proposition",
    "Id. Decision Makers",
    "Perception Analysis",
    "Proposal/Price Quote",
    "Negotiation/Review",
    "Closed Won",
    "Closed Lost",
]

OPEN_STAGES = [s for s in STAGES if s not in ("Closed Won", "Closed Lost")]

FORECAST_CATEGORIES: Dict[str, str] = {
    "Prospecting": "Pipeline",
    "Qualification": "Pipeline",
    "Needs Analysis": "Best Case",
    "Value Proposition": "Best Case",
    "Id. Decision Makers": "Best Case",
    "Perception Analysis": "Commit",
    "Proposal/Price Quote": "Commit",
    "Negotiation/Review": "Commit",
    "Closed Won": "Closed",
    "Closed Lost": "Omitted",
}

SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]
SEGMENT_WEIGHTS = [0.30, 0.45, 0.25]
SEGMENT_AVG_DEAL: Dict[str, float] = {
    "Enterprise": 150_000,
    "Mid-Market": 45_000,
    "SMB": 12_000,
}

INDUSTRIES = [
    "Technology",
    "Financial Services",
    "Healthcare",
    "Manufacturing",
    "Retail",
    "Media",
    "Education",
    "Professional Services",
    "Energy",
    "Government",
]

# Regions that map profile regions to billing-country choices
REGION_COUNTRIES: Dict[str, List[str]] = {
    "AMER": ["United States", "United States", "United States", "Canada", "Brazil"],
    "EMEA": ["United Kingdom", "Germany", "France", "Netherlands", "Sweden"],
    "APAC": ["Australia", "Japan", "Singapore", "India", "South Korea"],
    "LATAM": ["Brazil", "Mexico", "Colombia", "Argentina", "Chile"],
}

# Typical account employee ranges by segment
SEGMENT_EMPLOYEE_RANGE: Dict[str, Tuple[int, int]] = {
    "Enterprise": (2_000, 120_000),
    "Mid-Market": (200, 2_000),
    "SMB": (10, 200),
}

# Typical annual-revenue ranges by segment (USD)
SEGMENT_REVENUE_RANGE: Dict[str, Tuple[float, float]] = {
    "Enterprise": (500_000_000, 50_000_000_000),
    "Mid-Market": (20_000_000, 500_000_000),
    "SMB": (1_000_000, 20_000_000),
}

ACCOUNT_TYPES = ["Customer", "Prospect", "Partner"]
ACCOUNT_TYPE_WEIGHTS = [0.55, 0.35, 0.10]

# Sales-rep first/last names for realistic User records
FIRST_NAMES = [
    "James", "Maria", "David", "Sarah", "Michael", "Jennifer", "Robert",
    "Linda", "William", "Elizabeth", "Carlos", "Priya", "Ahmed", "Yuki",
    "Thomas", "Anna", "Daniel", "Jessica", "Christopher", "Nicole",
    "Raj", "Mei", "Patrick", "Fatima", "Kevin", "Rachel", "Liam",
    "Sofia", "Nathan", "Olivia", "Andre", "Sakura", "Marcus", "Hannah",
    "Diego", "Emma", "Sean", "Anya", "Ryan", "Nadia", "Peter", "Chloe",
    "Ivan", "Zara", "Lucas", "Aisha", "Ethan", "Mira", "Owen", "Tanya",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Martinez", "Anderson", "Taylor", "Thomas", "Jackson", "White",
    "Harris", "Martin", "Thompson", "Robinson", "Clark", "Lewis",
    "Patel", "Kim", "Nakamura", "Chen", "Muller", "Dubois", "Johansson",
    "Santos", "Kowalski", "O'Brien", "Ivanov", "Singh", "Tanaka", "Park",
    "Fernandez", "Schmidt", "Bergstrom", "Rossi", "Van der Berg", "Hassan",
    "Yamamoto", "Lee", "Nguyen", "Bhat", "Reeves", "Fischer", "Olsen",
    "Moreau", "Larsson", "De Luca",
]

# Company name building blocks (~100+ unique combinations)
COMPANY_PREFIXES = [
    "Apex", "Quantum", "Stellar", "Vanguard", "Pinnacle", "Nexus", "Horizon",
    "Centric", "Summit", "Atlas", "Cipher", "Forge", "Helix", "Prism",
    "Vector", "Zenith", "Cobalt", "Ignite", "Lumen", "Nova", "Orbit",
    "Pulse", "Ridge", "Sable", "Titan", "Unity", "Vertex", "Axiom",
    "Beacon", "Crest", "Delta", "Echo", "Flux", "Glyph", "Haven",
    "Ionic", "Jade", "Kite", "Lyric", "Mosaic", "Nimbus", "Onyx",
    "Paragon", "Quill", "Rune", "Spark", "Tidal", "Umbra", "Vivid",
    "Wren", "Xeno", "Yonder", "Zephyr", "Aether", "Bolt", "Coral",
    "Drift", "Ember", "Fable", "Grove", "Halo", "Indigo", "Kinetic",
    "Lark", "Maple", "Nectar", "Opal", "Pike", "Quartz", "Reed",
    "Sage", "Terra", "Upstream", "Vale", "Willow", "Alpine", "Birch",
    "Cedar", "Dune", "Elm", "Frost", "Glen", "Heath", "Iris",
]

COMPANY_SUFFIXES = [
    "Systems", "Technologies", "Solutions", "Group", "Labs", "Industries",
    "Corp", "Inc", "Global", "Networks", "Analytics", "Dynamics",
    "Enterprises", "Innovations", "Partners", "Digital", "Software",
    "Ventures", "Capital", "Holdings", "Services", "Consulting",
]

COMPANY_MIDDLES = [
    "Data", "Cloud", "AI", "Cyber", "Bio", "Fin", "Health", "Med",
    "Auto", "Aero", "Nano", "Green", "Smart", "Edge", "Core",
    "", "", "", "", "",  # blanks so many names are two-word
]

# Sales rep roles
REP_ROLES = [
    "Account Executive",
    "Account Executive",
    "Account Executive",
    "Senior Account Executive",
    "Senior Account Executive",
    "Enterprise Account Executive",
    "Regional Sales Manager",
    "Business Development Rep",
]

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

OPPORTUNITY_SCHEMA: List[Dict[str, Any]] = [
    {"name": "Id", "type": "string", "is_key": True},
    {"name": "Name", "type": "string"},
    {"name": "AccountId", "type": "string", "semantic_hint": "account_reference"},
    {"name": "Amount", "type": "number", "semantic_hint": "deal_value"},
    {"name": "StageName", "type": "string", "semantic_hint": "pipeline_stage"},
    {"name": "CloseDate", "type": "date", "semantic_hint": "close_date"},
    {"name": "OwnerId", "type": "string", "semantic_hint": "sales_rep"},
    {"name": "Region__c", "type": "string", "semantic_hint": "region"},
    {"name": "Segment__c", "type": "string", "semantic_hint": "segment"},
    {"name": "ForecastCategory", "type": "string", "semantic_hint": "forecast_category"},
    {"name": "IsClosed", "type": "boolean"},
    {"name": "IsWon", "type": "boolean"},
    {"name": "CreatedDate", "type": "datetime", "semantic_hint": "created_date"},
]

ACCOUNT_SCHEMA: List[Dict[str, Any]] = [
    {"name": "Id", "type": "string", "is_key": True},
    {"name": "Name", "type": "string", "semantic_hint": "account_name"},
    {"name": "Industry", "type": "string", "semantic_hint": "industry"},
    {"name": "AnnualRevenue", "type": "number", "semantic_hint": "annual_revenue"},
    {"name": "NumberOfEmployees", "type": "number"},
    {"name": "BillingCountry", "type": "string", "semantic_hint": "country"},
    {"name": "Type", "type": "string"},
    {"name": "OwnerId", "type": "string"},
]

USER_SCHEMA: List[Dict[str, Any]] = [
    {"name": "Id", "type": "string", "is_key": True},
    {"name": "Name", "type": "string"},
    {"name": "Role", "type": "string"},
    {"name": "Region__c", "type": "string", "semantic_hint": "region"},
    {"name": "IsActive", "type": "boolean"},
    {"name": "HireDate", "type": "date"},
]


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class SalesforceGenerator(BaseBusinessGenerator):
    """Generates Salesforce CRM data aligned to a :class:`BusinessProfile`."""

    SOURCE_SYSTEM = SOURCE_SYSTEM
    PIPE_PREFIX = "sf"

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def generate(
        self,
        profile: BusinessProfile,
        run_id: str,
        run_timestamp: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Return ``{"opportunities": …, "accounts": …, "users": …}`` DCL payloads.

        Each value is a fully-formatted DCL ingest payload dict produced by
        :meth:`format_dcl_payload`.
        """
        quarters = profile.quarters

        # 1. Users (sales reps) -- stable pool generated once
        users = self._generate_users(quarters)

        # 2. Accounts -- grow over time aligned to customer_count trajectory
        accounts = self._generate_accounts(quarters, users)

        # 3. Opportunities -- financially consistent with profile revenue
        opportunities = self._generate_opportunities(quarters, accounts, users)

        return {
            "users": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}_users",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=USER_SCHEMA,
                data=users,
            ),
            "accounts": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}_accounts",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=ACCOUNT_SCHEMA,
                data=accounts,
            ),
            "opportunities": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}_opportunities",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=OPPORTUNITY_SCHEMA,
                data=opportunities,
            ),
        }

    # ------------------------------------------------------------------ #
    # User (Sales Rep) generation
    # ------------------------------------------------------------------ #

    def _generate_users(self, quarters: List[QuarterMetrics]) -> List[Dict[str, Any]]:
        """Generate ~42 sales reps distributed across regions."""
        total_reps = 42
        region_alloc = self._distribute_across_regions(total_reps)

        used_names: set[str] = set()
        users: List[Dict[str, Any]] = []

        for region, count in region_alloc.items():
            for _ in range(count):
                name = self._unique_person_name(used_names)
                # Hire dates spread across a realistic range before and during
                # the profile window.
                earliest_hire = date(2019, 1, 1)
                latest_hire = date(2025, 12, 31)
                days_span = (latest_hire - earliest_hire).days
                hire_date = earliest_hire + timedelta(
                    days=self._rng.randint(0, days_span)
                )
                is_active = True if self._rng.random() > 0.05 else False
                users.append({
                    "Id": self._sf_id("005"),
                    "Name": name,
                    "Role": self._pick(REP_ROLES),
                    "Region__c": region,
                    "IsActive": is_active,
                    "HireDate": hire_date.isoformat(),
                })

        return users

    # ------------------------------------------------------------------ #
    # Account generation
    # ------------------------------------------------------------------ #

    def _generate_accounts(
        self,
        quarters: List[QuarterMetrics],
        users: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate one Account per customer, growing across quarters.

        The final quarter's ``customer_count`` determines the total number of
        accounts that should exist.  Accounts are created progressively across
        quarters so that each quarter ends with approximately the correct
        customer count.
        """
        peak_customers = max(q.customer_count for q in quarters)
        used_names: set[str] = set()
        accounts: List[Dict[str, Any]] = []

        active_user_ids_by_region: Dict[str, List[str]] = {}
        for u in users:
            if u["IsActive"]:
                active_user_ids_by_region.setdefault(u["Region__c"], []).append(u["Id"])
        all_active_ids = [u["Id"] for u in users if u["IsActive"]]

        for _ in range(peak_customers):
            segment = self._weighted_choice(SEGMENTS, SEGMENT_WEIGHTS)
            region = self._weighted_choice(
                REGIONS,
                REGION_WEIGHT_LIST,
            )
            industry = self._pick(INDUSTRIES)
            country = self._pick(REGION_COUNTRIES[region])

            emp_lo, emp_hi = SEGMENT_EMPLOYEE_RANGE[segment]
            employees = self._rng.randint(emp_lo, emp_hi)

            rev_lo, rev_hi = SEGMENT_REVENUE_RANGE[segment]
            annual_revenue = round(
                self._rng.uniform(rev_lo, rev_hi), -3  # round to nearest thousand
            )

            acct_type = self._weighted_choice(ACCOUNT_TYPES, ACCOUNT_TYPE_WEIGHTS)

            # Owner: prefer reps in the matching region
            region_reps = active_user_ids_by_region.get(region, all_active_ids)
            owner_id = self._pick(region_reps) if region_reps else self._pick(all_active_ids)

            name = self._unique_company_name(used_names)

            accounts.append({
                "Id": self._sf_id("001"),
                "Name": name,
                "Industry": self._maybe_null(industry, 0.04),
                "AnnualRevenue": self._maybe_null(annual_revenue, 0.08),
                "NumberOfEmployees": self._maybe_null(employees, 0.06),
                "BillingCountry": self._maybe_null(country, 0.02),
                "Type": acct_type,
                "OwnerId": owner_id,
                "_segment": segment,   # internal, stripped before payload
                "_region": region,     # internal, stripped before payload
            })

        return accounts

    # ------------------------------------------------------------------ #
    # Opportunity generation
    # ------------------------------------------------------------------ #

    def _generate_opportunities(
        self,
        quarters: List[QuarterMetrics],
        accounts: List[Dict[str, Any]],
        users: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate opportunities that are financially consistent with the profile.

        For each quarter:
        * Closed Won amounts sum to approximately ``quarter.revenue * 1_000_000``.
        * Closed Lost deals are generated from the gap implied by the win rate.
        * Open pipeline deals are created for non-historical quarters.
        """
        active_user_ids = [u["Id"] for u in users if u["IsActive"]]
        user_ids_by_region: Dict[str, List[str]] = {}
        for u in users:
            if u["IsActive"]:
                user_ids_by_region.setdefault(u["Region__c"], []).append(u["Id"])

        # Pre-index accounts by region and segment for fast lookup
        accounts_by_region: Dict[str, List[Dict[str, Any]]] = {}
        for a in accounts:
            accounts_by_region.setdefault(a["_region"], []).append(a)

        # Track which accounts have been used per quarter for variety
        opportunities: List[Dict[str, Any]] = []

        # Determine the "current" quarter boundary for deciding open vs closed
        # In this dataset we treat the last two quarters as forecast (is_forecast)
        # but all quarters get closed-won deals to match the profile revenue.
        for qm in quarters:
            quarter = qm.quarter
            revenue_target_dollars = qm.revenue * 1_000_000
            win_rate = qm.win_rate / 100.0  # convert from percent

            # --- Closed Won deals ---
            won_deals = self._create_closed_won_deals(
                quarter=quarter,
                revenue_target=revenue_target_dollars,
                revenue_by_region=qm.revenue_by_region,
                accounts=accounts,
                accounts_by_region=accounts_by_region,
                user_ids_by_region=user_ids_by_region,
                active_user_ids=active_user_ids,
            )
            opportunities.extend(won_deals)

            # --- Closed Lost deals ---
            # Total closed deals = won / win_rate.  Lost = total - won.
            num_won = len(won_deals)
            total_closed = max(int(round(num_won / win_rate)), num_won + 1)
            num_lost = total_closed - num_won
            lost_deals = self._create_closed_lost_deals(
                quarter=quarter,
                count=num_lost,
                accounts=accounts,
                accounts_by_region=accounts_by_region,
                user_ids_by_region=user_ids_by_region,
                active_user_ids=active_user_ids,
            )
            opportunities.extend(lost_deals)

            # --- Open pipeline deals (for the current/future quarters) ---
            if qm.is_forecast or quarter == quarters[-3].quarter if len(quarters) >= 3 else False:
                open_pipeline_dollars = qm.pipeline * 1_000_000
                open_deals = self._create_open_pipeline_deals(
                    quarter=quarter,
                    pipeline_target=open_pipeline_dollars,
                    pipeline_by_stage=qm.pipeline_by_stage,
                    accounts=accounts,
                    accounts_by_region=accounts_by_region,
                    user_ids_by_region=user_ids_by_region,
                    active_user_ids=active_user_ids,
                )
                opportunities.extend(open_deals)

        # Strip internal keys from accounts before returning opps
        # (accounts list is mutated in-place for all consumers)
        for a in accounts:
            a.pop("_segment", None)
            a.pop("_region", None)

        return opportunities

    # ------------------------------------------------------------------ #
    # Closed-Won deal generation
    # ------------------------------------------------------------------ #

    def _create_closed_won_deals(
        self,
        quarter: str,
        revenue_target: float,
        revenue_by_region: Dict[str, float],
        accounts: List[Dict[str, Any]],
        accounts_by_region: Dict[str, List[Dict[str, Any]]],
        user_ids_by_region: Dict[str, List[str]],
        active_user_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Generate Closed Won opportunities summing to ``revenue_target``."""
        deals: List[Dict[str, Any]] = []

        for region in REGIONS:
            region_target = revenue_by_region.get(region, 0.0) * 1_000_000
            if region_target <= 0:
                continue

            region_deals = self._fill_revenue_bucket(
                target=region_target,
                region=region,
            )

            region_accounts = accounts_by_region.get(region, accounts[:50])
            region_reps = user_ids_by_region.get(region, active_user_ids)

            for amount, segment in region_deals:
                account = self._pick(region_accounts)
                owner_id = self._pick(region_reps)
                close_date = self._date_in_quarter(quarter)
                created_date = self._created_date_before(close_date, quarter)
                opp_name = self._opportunity_name(account["Name"], segment)

                deals.append({
                    "Id": self._sf_id("006"),
                    "Name": opp_name,
                    "AccountId": account["Id"],
                    "Amount": round(amount, 2),
                    "StageName": "Closed Won",
                    "CloseDate": close_date,
                    "OwnerId": owner_id,
                    "Region__c": region,
                    "Segment__c": segment,
                    "ForecastCategory": FORECAST_CATEGORIES["Closed Won"],
                    "IsClosed": True,
                    "IsWon": True,
                    "CreatedDate": created_date,
                })

        return deals

    # ------------------------------------------------------------------ #
    # Closed-Lost deal generation
    # ------------------------------------------------------------------ #

    def _create_closed_lost_deals(
        self,
        quarter: str,
        count: int,
        accounts: List[Dict[str, Any]],
        accounts_by_region: Dict[str, List[Dict[str, Any]]],
        user_ids_by_region: Dict[str, List[str]],
        active_user_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Generate ``count`` Closed Lost opportunities for a quarter."""
        deals: List[Dict[str, Any]] = []
        for _ in range(count):
            segment = self._weighted_choice(SEGMENTS, SEGMENT_WEIGHTS)
            region = self._weighted_choice(REGIONS, REGION_WEIGHT_LIST)
            avg_deal = SEGMENT_AVG_DEAL[segment]
            amount = round(
                avg_deal * self._rng.uniform(0.3, 2.2), 2
            )

            region_accounts = accounts_by_region.get(region, accounts[:50])
            region_reps = user_ids_by_region.get(region, active_user_ids)
            account = self._pick(region_accounts)
            owner_id = self._pick(region_reps)

            close_date = self._date_in_quarter(quarter)
            created_date = self._created_date_before(close_date, quarter)
            opp_name = self._opportunity_name(account["Name"], segment)

            # Lost deals sometimes have null Amount (data quality gap)
            deal_amount: Optional[float] = self._maybe_null(amount, 0.10)

            deals.append({
                "Id": self._sf_id("006"),
                "Name": opp_name,
                "AccountId": account["Id"],
                "Amount": deal_amount,
                "StageName": "Closed Lost",
                "CloseDate": close_date,
                "OwnerId": owner_id,
                "Region__c": region,
                "Segment__c": segment,
                "ForecastCategory": FORECAST_CATEGORIES["Closed Lost"],
                "IsClosed": True,
                "IsWon": False,
                "CreatedDate": created_date,
            })

        return deals

    # ------------------------------------------------------------------ #
    # Open pipeline deal generation
    # ------------------------------------------------------------------ #

    def _create_open_pipeline_deals(
        self,
        quarter: str,
        pipeline_target: float,
        pipeline_by_stage: Dict[str, float],
        accounts: List[Dict[str, Any]],
        accounts_by_region: Dict[str, List[Dict[str, Any]]],
        user_ids_by_region: Dict[str, List[str]],
        active_user_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Generate open-pipeline opportunities for a forecast quarter.

        Distributes the pipeline target across Salesforce stages, mapping from
        the profile's abstract stages (Lead, Qualified, Proposal, Negotiation)
        to concrete Salesforce stages.
        """
        # Map profile pipeline stages to Salesforce stages
        profile_to_sf_stages: Dict[str, List[str]] = {
            "Lead": ["Prospecting", "Qualification"],
            "Qualified": ["Needs Analysis", "Value Proposition"],
            "Proposal": ["Id. Decision Makers", "Perception Analysis", "Proposal/Price Quote"],
            "Negotiation": ["Negotiation/Review"],
        }

        deals: List[Dict[str, Any]] = []

        for profile_stage, stage_value_m in pipeline_by_stage.items():
            # Skip Closed-Won bucket in pipeline; those are separate
            if profile_stage == "Closed-Won":
                continue

            sf_stages = profile_to_sf_stages.get(profile_stage)
            if not sf_stages:
                continue

            stage_target = stage_value_m * 1_000_000
            if stage_target <= 0:
                continue

            # Divide stage target among the mapped SF stages
            per_sf_stage = stage_target / len(sf_stages)

            for sf_stage in sf_stages:
                remaining = per_sf_stage
                while remaining > 0:
                    segment = self._weighted_choice(SEGMENTS, SEGMENT_WEIGHTS)
                    avg_deal = SEGMENT_AVG_DEAL[segment]
                    amount = round(avg_deal * self._rng.uniform(0.5, 2.0), 2)
                    amount = min(amount, remaining)  # don't overshoot
                    remaining -= amount

                    region = self._weighted_choice(REGIONS, REGION_WEIGHT_LIST)
                    region_accounts = accounts_by_region.get(region, accounts[:50])
                    region_reps = user_ids_by_region.get(region, active_user_ids)
                    account = self._pick(region_accounts)
                    owner_id = self._pick(region_reps)

                    close_date = self._date_in_quarter(quarter)
                    created_date = self._created_date_before(close_date, quarter)
                    opp_name = self._opportunity_name(account["Name"], segment)

                    deals.append({
                        "Id": self._sf_id("006"),
                        "Name": opp_name,
                        "AccountId": account["Id"],
                        "Amount": self._maybe_null(amount, 0.03),
                        "StageName": sf_stage,
                        "CloseDate": close_date,
                        "OwnerId": owner_id,
                        "Region__c": region,
                        "Segment__c": segment,
                        "ForecastCategory": FORECAST_CATEGORIES[sf_stage],
                        "IsClosed": False,
                        "IsWon": False,
                        "CreatedDate": created_date,
                    })

                    # Safety: stop generating if amount rounded to zero
                    if amount <= 0:
                        break

        return deals

    # ------------------------------------------------------------------ #
    # Revenue-filling helper
    # ------------------------------------------------------------------ #

    def _fill_revenue_bucket(
        self,
        target: float,
        region: str,
    ) -> List[Tuple[float, str]]:
        """Create a list of ``(amount, segment)`` tuples summing to ~``target``.

        Uses segment weights and average deal sizes to produce a realistic deal
        mix.  The last deal is adjusted so the total hits the target exactly.
        """
        deals: List[Tuple[float, str]] = []
        accumulated = 0.0

        while accumulated < target:
            segment = self._weighted_choice(SEGMENTS, SEGMENT_WEIGHTS)
            avg_deal = SEGMENT_AVG_DEAL[segment]

            # Add realistic variance: 0.4x to 2.5x average deal size
            amount = avg_deal * self._rng.uniform(0.4, 2.5)

            gap = target - accumulated
            if gap < avg_deal * 0.3:
                # Remaining gap is small -- create one final deal to close it
                deals.append((round(gap, 2), segment))
                accumulated += gap
                break

            # Don't overshoot by too much
            if amount > gap:
                amount = gap * self._rng.uniform(0.85, 1.0)

            amount = round(amount, 2)
            if amount <= 0:
                break

            deals.append((amount, segment))
            accumulated += amount

        return deals

    # ------------------------------------------------------------------ #
    # Naming helpers
    # ------------------------------------------------------------------ #

    def _unique_company_name(self, used: set[str]) -> str:
        """Generate a unique company name not already in ``used``."""
        for _ in range(500):
            name = self._random_company_name()
            if name not in used:
                used.add(name)
                return name
        # Extremely unlikely fallback -- append a numeric suffix
        base = self._random_company_name()
        suffix = self._rng.randint(100, 9999)
        name = f"{base} {suffix}"
        used.add(name)
        return name

    def _random_company_name(self) -> str:
        """Build a company name from prefix + optional middle + suffix."""
        prefix = self._pick(COMPANY_PREFIXES)
        middle = self._pick(COMPANY_MIDDLES)
        suffix = self._pick(COMPANY_SUFFIXES)
        if middle:
            return f"{prefix} {middle} {suffix}"
        return f"{prefix} {suffix}"

    def _unique_person_name(self, used: set[str]) -> str:
        """Generate a unique full name."""
        for _ in range(200):
            first = self._pick(FIRST_NAMES)
            last = self._pick(LAST_NAMES)
            name = f"{first} {last}"
            if name not in used:
                used.add(name)
                return name
        # Fallback: append initial
        first = self._pick(FIRST_NAMES)
        last = self._pick(LAST_NAMES)
        initial = self._pick(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        name = f"{first} {initial}. {last}"
        used.add(name)
        return name

    def _opportunity_name(self, account_name: str, segment: str) -> str:
        """Create a realistic opportunity name."""
        descriptors = [
            "Platform License", "Expansion", "Renewal", "New Business",
            "Add-On", "Upgrade", "Professional Services", "Implementation",
            "Enterprise License", "Annual Subscription", "Multi-Year Deal",
            "POC", "Pilot Program", "Migration", "Consolidation",
        ]
        descriptor = self._pick(descriptors)
        return f"{account_name} - {descriptor}"

    # ------------------------------------------------------------------ #
    # Date helpers
    # ------------------------------------------------------------------ #

    def _created_date_before(self, close_date_str: str, quarter: str) -> str:
        """Generate a CreatedDate timestamp before the close date.

        Deals are typically created 30-180 days before close.
        """
        close_date = date.fromisoformat(close_date_str)
        lead_time_days = self._rng.randint(14, 180)
        created_date = close_date - timedelta(days=lead_time_days)

        # Don't go earlier than 2023-06-01 for realism
        floor = date(2023, 6, 1)
        if created_date < floor:
            created_date = floor + timedelta(days=self._rng.randint(0, 60))

        hour = self._rng.randint(7, 19)
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return f"{created_date.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}Z"

    # ------------------------------------------------------------------ #
    # Region distribution helper
    # ------------------------------------------------------------------ #

    def _distribute_across_regions(self, total: int) -> Dict[str, int]:
        """Distribute ``total`` items across schema-defined regions.

        Returns a dict mapping region -> count, ensuring they sum to ``total``.
        """
        raw = [int(total * w) for w in REGION_WEIGHT_LIST]
        remainder = total - sum(raw)
        # Distribute remainder one at a time to the largest regions
        for i in range(remainder):
            raw[i % len(raw)] += 1
        return dict(zip(REGIONS, raw))
