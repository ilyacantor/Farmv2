"""
Business profile and quarterly metrics trajectory.

The "truth spine" that all source-system generators derive from. Defines the
company's financial trajectory across 12 quarters (2024-Q1 through 2026-Q4),
ensuring cross-system consistency while allowing realistic variance.
"""

import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.generators.financial_model import Assumptions, get_schema_config

# ── Canonical assumptions instance ────────────────────────────────────────
# All BusinessProfile defaults below derive from this single source.
# If you need to change a company assumption, change it in farm_config.yaml
# (or in Assumptions compiled defaults if YAML is absent).
_A = Assumptions()
_schema = get_schema_config()


@dataclass
class QuarterMetrics:
    """Aggregated business metrics for a single quarter."""

    quarter: str  # "2024-Q1"
    is_forecast: bool  # True for 2026-Q3 and Q4

    # Revenue & ARR
    revenue: float  # millions USD (quarterly)
    arr: float  # millions USD (annualized)
    mrr: float  # millions USD (arr/12)

    # Pipeline
    pipeline: float  # millions USD (total open pipeline)
    new_pipeline: float  # millions USD (new pipeline created this quarter)
    win_rate: float  # percent (0-100)

    # Customers
    customer_count: int
    new_customers: int
    churned_customers: int
    nrr: float  # net revenue retention percent
    gross_churn_pct: float  # percent

    # People
    headcount: int
    new_hires: int
    terminations: int
    attrition_rate: float  # percent

    # Support
    support_tickets: int
    csat: float  # score out of 5

    # Engineering
    sprint_velocity: float  # avg story points per sprint
    sprints_in_quarter: int  # typically 6 (2-week sprints)

    # Financials
    gross_margin_pct: float  # percent
    cogs: float  # millions USD
    opex: float  # millions USD

    # Infrastructure
    cloud_spend: float  # millions USD
    incident_count: int
    mttr_hours: float  # mean time to resolve

    # Regional breakdown (millions USD)
    revenue_by_region: Dict[str, float] = field(default_factory=dict)

    # Department headcount
    headcount_by_dept: Dict[str, int] = field(default_factory=dict)

    # Dimensional headcount breakdowns
    headcount_by_geo: Dict[str, int] = field(default_factory=dict)
    headcount_by_practice: Dict[str, int] = field(default_factory=dict)  # practice area or service line
    headcount_by_level: Dict[str, int] = field(default_factory=dict)

    # Pipeline by stage (millions USD)
    pipeline_by_stage: Dict[str, float] = field(default_factory=dict)

    # Entity identification (multi-entity support)
    entity_id: Optional[str] = None
    entity_name: Optional[str] = None
    business_model: str = "saas"


REGIONS = _schema.get("regions", ["AMER", "EMEA", "APAC"])
REGION_WEIGHTS = {"AMER": _A.region_amer, "EMEA": _A.region_emea, "APAC": _A.region_apac}

DEPARTMENTS = _schema.get("departments", ["Engineering", "Product", "Marketing", "CS", "G&A", "Sales"])
DEPT_WEIGHTS = {
    "Engineering": _A.hc_engineering_pct,
    "Product": _A.hc_product_pct,
    "Marketing": _A.hc_marketing_pct,
    "CS": _A.hc_cs_pct,
    "G&A": _A.hc_ga_pct,
    "Sales": _A.hc_sales_pct,
}

PIPELINE_STAGES = _schema.get("pipeline_stages", ["Lead", "Qualified", "Proposal", "Negotiation", "Closed-Won"])
_default_stage_weights = {"Lead": 0.25, "Qualified": 0.20, "Proposal": 0.20, "Negotiation": 0.16, "Closed-Won": 0.19}
STAGE_WEIGHTS = _schema.get("stage_weights", _default_stage_weights)


@dataclass
class BusinessProfile:
    """
    Generates a coherent 12-quarter business trajectory.

    All source-system generators derive their records from this profile to ensure
    cross-system financial consistency.
    """

    seed: int = 42
    entity_id: Optional[str] = None
    entity_name: Optional[str] = None
    business_model: str = "saas"
    regions: List[str] = field(default_factory=lambda: ["AMER", "EMEA", "APAC"])
    base_revenue: float = _A.starting_arr / 4
    yoy_growth_rate: float = _A.arr_growth_rate_annual
    base_arr: float = _A.starting_arr
    base_customer_count: int = _A.starting_customer_count
    base_headcount: int = _A.starting_headcount
    base_win_rate: float = _A.win_rate
    base_nrr: float = _A.nrr_base
    base_gross_churn: float = _A.gross_churn_rate_annual * 100
    base_gross_margin: float = (1 - _A.cogs_pct) * 100
    base_csat: float = _A.csat_base
    base_sprint_velocity: float = _A.points_per_sprint
    base_support_tickets: int = int(_A.starting_customer_count * _A.tickets_per_customer_annual / 4)
    base_cloud_spend: float = _A.starting_arr / 4 * _A.cloud_spend_pct_revenue
    base_incident_count: int = _A.p1_incidents_per_quarter + _A.p2_incidents_per_quarter
    num_quarters: int = 12

    def __post_init__(self):
        self._rng = random.Random(self.seed)
        self._quarters: Optional[List[QuarterMetrics]] = None

    @property
    def quarters(self) -> List[QuarterMetrics]:
        if self._quarters is None:
            self._quarters = self._generate_trajectory()
        return self._quarters

    def get_quarter(self, quarter_label: str) -> QuarterMetrics:
        """Get metrics for a specific quarter like '2024-Q1'."""
        for q in self.quarters:
            if q.quarter == quarter_label:
                return q
        raise ValueError(f"Quarter {quarter_label} not found in profile")

    @property
    def quarter_labels(self) -> List[str]:
        """All quarter labels in order."""
        return [q.quarter for q in self.quarters]

    def _quarter_label(self, index: int) -> str:
        """Convert quarter index (0-based from 2024-Q1) to label."""
        year = 2024 + index // 4
        q = (index % 4) + 1
        return f"{year}-Q{q}"

    def _qoq_growth(self) -> float:
        """Quarterly compound growth rate derived from YoY."""
        return (1 + self.yoy_growth_rate) ** 0.25 - 1

    def _jitter(self, base: float, pct: float = 0.02) -> float:
        """Add small random jitter to a value."""
        return base * (1 + self._rng.uniform(-pct, pct))

    def _generate_trajectory(self) -> List[QuarterMetrics]:
        """Generate the full 12-quarter business trajectory."""
        quarters = []
        qoq = self._qoq_growth()

        prev_customers = self.base_customer_count
        prev_headcount = self.base_headcount
        prev_arr = self.base_arr

        for i in range(self.num_quarters):
            label = self._quarter_label(i)
            is_forecast = i >= 10  # 2026-Q3 and Q4

            # Revenue growth with slight quarterly variance
            growth_factor = (1 + qoq) ** i
            revenue = self._jitter(self.base_revenue * growth_factor, 0.015)
            arr = self._jitter(prev_arr * (1 + qoq), 0.01)
            mrr = arr / 12

            # Pipeline: ~2.8-3.2x coverage of next quarter revenue
            pipeline_coverage = self._jitter(3.0, 0.05)
            next_q_revenue = self.base_revenue * (1 + qoq) ** (i + 1)
            pipeline = next_q_revenue * pipeline_coverage
            new_pipeline = self._jitter(pipeline * 0.45, 0.05)

            # Win rate: stable with minor fluctuation
            win_rate = self._jitter(self.base_win_rate, 0.03)

            # Customer metrics
            customer_growth_rate = self._jitter(0.06, 0.3)  # ~5-8% per quarter
            new_customers = max(
                int(prev_customers * customer_growth_rate), 10
            )
            churned_customers = max(
                int(prev_customers * self.base_gross_churn / 100 / 4), 2
            )
            customer_count = prev_customers + new_customers - churned_customers
            customer_count = min(customer_count, 2000)

            # Churn and retention
            gross_churn_pct = self._jitter(self.base_gross_churn, 0.05)
            nrr = self._jitter(self.base_nrr, 0.02)

            # Headcount
            hc_growth_rate = self._jitter(0.04, 0.3)
            new_hires = max(int(prev_headcount * hc_growth_rate) + self._rng.randint(1, 4), 5)
            terminations = max(int(prev_headcount * 0.02) + self._rng.randint(-1, 2), 1)
            headcount = prev_headcount + new_hires - terminations
            attrition_rate = (terminations / prev_headcount) * 100 if prev_headcount > 0 else 0

            # Support
            tickets_growth = 1 + (customer_count - self.base_customer_count) / self.base_customer_count * 0.8
            support_tickets = int(self._jitter(self.base_support_tickets * tickets_growth, 0.05))
            support_tickets = min(support_tickets, 5000)
            csat = self._jitter(self.base_csat, 0.02)
            csat = min(max(csat, 3.5), 4.8)

            # Engineering
            velocity_trend = 1 + i * 0.005  # slight improvement over time
            sprint_velocity = self._jitter(self.base_sprint_velocity * velocity_trend, 0.04)
            sprints_in_quarter = 6  # 2-week sprints

            # Financials
            gross_margin_pct = self._jitter(self.base_gross_margin, 0.01)
            # Slight margin improvement over time
            gross_margin_pct = min(gross_margin_pct + i * 0.15, 72.0)
            cogs = revenue * (1 - gross_margin_pct / 100)
            opex = revenue * self._jitter(0.85, 0.03)

            # Infrastructure
            cloud_growth = (1 + qoq * 0.8) ** i  # grows slightly slower than revenue
            cloud_spend = self._jitter(self.base_cloud_spend * cloud_growth, 0.04)
            incident_count = max(
                int(self._jitter(self.base_incident_count * (1 - i * 0.01), 0.1)), 5
            )
            mttr_hours = self._jitter(4.5 * (1 - i * 0.02), 0.1)
            mttr_hours = max(mttr_hours, 1.5)

            # Regional breakdown
            revenue_by_region = {}
            remaining = revenue
            for region in REGIONS[:-1]:
                val = round(revenue * self._jitter(REGION_WEIGHTS[region], 0.05), 2)
                revenue_by_region[region] = val
                remaining -= val
            revenue_by_region[REGIONS[-1]] = round(remaining, 2)

            # Department headcount
            headcount_by_dept = {}
            remaining_hc = headcount
            for dept in DEPARTMENTS[:-1]:
                val = max(int(headcount * self._jitter(DEPT_WEIGHTS[dept], 0.03)), 1)
                headcount_by_dept[dept] = val
                remaining_hc -= val
            headcount_by_dept[DEPARTMENTS[-1]] = max(remaining_hc, 1)

            # Pipeline by stage
            pipeline_by_stage = {}
            remaining_pipe = pipeline
            for stage in PIPELINE_STAGES[:-1]:
                val = round(pipeline * self._jitter(STAGE_WEIGHTS[stage], 0.04), 2)
                pipeline_by_stage[stage] = val
                remaining_pipe -= val
            pipeline_by_stage[PIPELINE_STAGES[-1]] = round(remaining_pipe, 2)

            quarter = QuarterMetrics(
                quarter=label,
                is_forecast=is_forecast,
                revenue=round(revenue, 2),
                arr=round(arr, 2),
                mrr=round(mrr, 4),
                pipeline=round(pipeline, 2),
                new_pipeline=round(new_pipeline, 2),
                win_rate=round(win_rate, 1),
                customer_count=customer_count,
                new_customers=new_customers,
                churned_customers=churned_customers,
                nrr=round(nrr, 1),
                gross_churn_pct=round(gross_churn_pct, 1),
                headcount=headcount,
                new_hires=new_hires,
                terminations=terminations,
                attrition_rate=round(attrition_rate, 1),
                support_tickets=support_tickets,
                csat=round(csat, 2),
                sprint_velocity=round(sprint_velocity, 1),
                sprints_in_quarter=sprints_in_quarter,
                gross_margin_pct=round(gross_margin_pct, 1),
                cogs=round(cogs, 2),
                opex=round(opex, 2),
                cloud_spend=round(cloud_spend, 2),
                incident_count=incident_count,
                mttr_hours=round(mttr_hours, 1),
                revenue_by_region=revenue_by_region,
                headcount_by_dept=headcount_by_dept,
                pipeline_by_stage=pipeline_by_stage,
            )

            quarters.append(quarter)
            prev_customers = customer_count
            prev_headcount = headcount
            prev_arr = arr

        return quarters

    @classmethod
    def from_model_quarters(cls, model_quarters, seed: int = 42):
        """
        Create a BusinessProfile from financial model Quarter objects.

        This adapter lets existing generators consume the richer financial model
        output through the same QuarterMetrics interface they already know.
        """
        profile = cls.__new__(cls)
        profile.seed = seed
        profile.base_revenue = model_quarters[0].revenue if model_quarters else 22.0
        profile.yoy_growth_rate = _A.arr_growth_rate_annual
        profile.num_quarters = len(model_quarters)
        profile.base_arr = model_quarters[0].beginning_arr if model_quarters else 83.6
        profile.base_customer_count = model_quarters[0].customer_count if model_quarters else 760
        profile.base_headcount = model_quarters[0].headcount if model_quarters else 235

        # Entity identification from source Quarter objects
        profile.entity_id = getattr(model_quarters[0], "entity_id", None) if model_quarters else None
        profile.entity_name = getattr(model_quarters[0], "entity_name", None) if model_quarters else None
        profile.business_model = getattr(model_quarters[0], "business_model", "saas") if model_quarters else "saas"

        # Derive regions from the Quarter objects' revenue_by_region keys
        if model_quarters and hasattr(model_quarters[0], "revenue_by_region") and model_quarters[0].revenue_by_region:
            profile.regions = list(model_quarters[0].revenue_by_region.keys())
        else:
            profile.regions = ["AMER", "EMEA", "APAC"]

        converted = []
        for fmq in model_quarters:
            qm = QuarterMetrics(
                quarter=fmq.quarter,
                is_forecast=fmq.is_forecast,
                revenue=round(fmq.revenue, 2),
                arr=round(fmq.ending_arr, 2),
                mrr=round(fmq.mrr, 4),
                pipeline=round(fmq.pipeline, 2),
                new_pipeline=round(fmq.pipeline * 0.3, 2),
                win_rate=round(fmq.win_rate, 1),
                customer_count=fmq.customer_count,
                new_customers=fmq.new_customers,
                churned_customers=fmq.churned_customers,
                nrr=round(fmq.nrr, 1),
                gross_churn_pct=round(fmq.gross_churn_pct, 1),
                headcount=fmq.headcount,
                new_hires=fmq.hires,
                terminations=fmq.terminations,
                attrition_rate=round(fmq.attrition_rate, 1),
                support_tickets=fmq.support_tickets,
                csat=round(fmq.csat, 2),
                sprint_velocity=round(fmq.sprint_velocity, 1),
                sprints_in_quarter=6,
                gross_margin_pct=round(fmq.gross_margin_pct, 1),
                cogs=round(fmq.cogs, 2),
                opex=round(fmq.total_opex, 2),
                cloud_spend=round(fmq.cloud_spend, 2),
                incident_count=fmq.p1_incidents + fmq.p2_incidents,
                mttr_hours=round((fmq.mttr_p1_hours * fmq.p1_incidents + fmq.mttr_p2_hours * fmq.p2_incidents) / max(fmq.p1_incidents + fmq.p2_incidents, 1), 1),
                revenue_by_region=dict(fmq.revenue_by_region),
                headcount_by_dept=dict(fmq.headcount_by_department),
                headcount_by_geo=dict(fmq.headcount_by_geo) if fmq.headcount_by_geo else {},
                headcount_by_practice=dict(fmq.headcount_by_practice) if fmq.headcount_by_practice else {},
                headcount_by_level=dict(fmq.headcount_by_level) if fmq.headcount_by_level else {},
                pipeline_by_stage=dict(fmq.pipeline_by_stage),
                entity_id=getattr(fmq, "entity_id", None),
                entity_name=getattr(fmq, "entity_name", None),
                business_model=getattr(fmq, "business_model", "saas"),
            )
            converted.append(qm)

        profile._quarters = converted
        profile._rng = None
        return profile

    @property
    def quarter_labels(self) -> List[str]:
        """All quarter labels in order."""
        return [q.quarter for q in self.quarters]
