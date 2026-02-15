"""
Workday HCM data generator.

Generates realistic Workday-shaped records (Workers, Positions, Time Off)
that are consistent with a BusinessProfile's people metrics. Uses Workday-style
PascalCase_With_Underscores field naming and Workday-style IDs.

Output object types:
  - Workers:   one record per worker (active + terminated), keyed by Worker_ID
  - Positions: one record per position (filled + open), keyed by Position_ID
  - Time Off:  ~2 leave requests per active worker per quarter

The generator maintains cross-quarter consistency: workers hired in Q1 remain
in subsequent quarters until terminated, and terminated workers keep their
Termination_Date in future snapshots.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile, QuarterMetrics

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "workday"

MANAGEMENT_LEVELS = [
    "Individual Contributor", "Individual Contributor", "Individual Contributor",
    "Team Lead", "Team Lead", "Manager", "Senior Manager", "Director", "VP", "C-Suite",
]

LOCATIONS = [
    "San Francisco, CA", "New York, NY", "Austin, TX", "Seattle, WA",
    "Denver, CO", "Chicago, IL", "Boston, MA", "Atlanta, GA",
    "London, UK", "Berlin, DE", "Dublin, IE",
    "Sydney, AU", "Singapore, SG", "Tokyo, JP",
]
LOCATION_WEIGHTS = [
    0.15, 0.12, 0.10, 0.08, 0.06, 0.05, 0.05, 0.04,
    0.08, 0.05, 0.04, 0.06, 0.06, 0.06,
]

COST_CENTERS: Dict[str, List[str]] = {
    "Engineering": ["CC-ENG-100", "CC-ENG-200", "CC-ENG-300"],
    "Product": ["CC-PRD-100"],
    "Marketing": ["CC-MKT-100", "CC-MKT-200"],
    "CS": ["CC-CS-100", "CC-CS-200"],
    "G&A": ["CC-GA-100", "CC-GA-200", "CC-GA-300"],
    "Sales": ["CC-SAL-100", "CC-SAL-200"],
}

JOB_FAMILIES: Dict[str, List[str]] = {
    "Engineering": ["Software Engineering", "DevOps", "QA", "Data Engineering", "Security"],
    "Product": ["Product Management", "Product Design", "UX Research"],
    "Marketing": ["Growth Marketing", "Content", "Demand Gen", "Brand"],
    "CS": ["Customer Success", "Technical Support", "Solutions Engineering", "Onboarding"],
    "G&A": ["Finance", "HR", "Legal", "IT", "Operations", "Recruiting"],
    "Sales": ["Account Executive", "SDR/BDR", "Sales Engineering", "Sales Operations"],
}

LEAVE_TYPES = ["Vacation", "Sick", "Personal", "Parental", "Bereavement", "Jury Duty"]
LEAVE_TYPE_WEIGHTS = [0.60, 0.20, 0.10, 0.05, 0.03, 0.02]

LEAVE_STATUS = ["Approved", "Completed", "Pending", "Denied"]
LEAVE_STATUS_WEIGHTS = [0.35, 0.45, 0.12, 0.08]

PAY_CURRENCIES = ["USD", "EUR", "GBP", "AUD", "SGD", "JPY"]

# Map locations to their natural currency.
_LOCATION_CURRENCY: Dict[str, str] = {
    "San Francisco, CA": "USD",
    "New York, NY": "USD",
    "Austin, TX": "USD",
    "Seattle, WA": "USD",
    "Denver, CO": "USD",
    "Chicago, IL": "USD",
    "Boston, MA": "USD",
    "Atlanta, GA": "USD",
    "London, UK": "GBP",
    "Berlin, DE": "EUR",
    "Dublin, IE": "EUR",
    "Sydney, AU": "AUD",
    "Singapore, SG": "SGD",
    "Tokyo, JP": "JPY",
}

# Pay ranges (USD) by management level: (low, high).
_PAY_RANGES: Dict[str, Tuple[int, int]] = {
    "Individual Contributor": (80_000, 180_000),
    "Team Lead": (120_000, 200_000),
    "Manager": (140_000, 250_000),
    "Senior Manager": (140_000, 250_000),
    "Director": (180_000, 350_000),
    "VP": (250_000, 450_000),
    "C-Suite": (350_000, 600_000),
}

# Currency conversion factors relative to USD (approximate, for realistic data).
_CURRENCY_FACTORS: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.79,
    "AUD": 1.54,
    "SGD": 1.35,
    "JPY": 150.0,
}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Wei", "Priya", "Hiroshi", "Fatima", "Carlos", "Ananya", "Yuki", "Olga",
    "Raj", "Mei", "Ahmed", "Sonia", "Kenji", "Lena", "Mateo", "Ingrid",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Chen", "Wang", "Kim", "Patel", "Nakamura", "Singh", "Tanaka", "Müller",
    "Gupta", "Zhang", "Yamamoto", "Kumar", "Suzuki", "Fernandez", "Johansson",
]

# Business titles by department and level for realistic combinations.
_TITLE_TEMPLATES: Dict[str, Dict[str, List[str]]] = {
    "Engineering": {
        "Individual Contributor": [
            "Software Engineer", "Senior Software Engineer", "Staff Engineer",
            "Backend Engineer", "Frontend Engineer", "Data Engineer",
            "DevOps Engineer", "Security Engineer", "QA Engineer",
        ],
        "Team Lead": ["Engineering Lead", "Tech Lead", "Principal Engineer"],
        "Manager": ["Engineering Manager"],
        "Senior Manager": ["Senior Engineering Manager"],
        "Director": ["Director of Engineering"],
        "VP": ["VP of Engineering"],
        "C-Suite": ["CTO"],
    },
    "Product": {
        "Individual Contributor": [
            "Product Manager", "Senior Product Manager", "Product Designer",
            "UX Researcher", "UX Designer",
        ],
        "Team Lead": ["Lead Product Manager", "Lead Designer"],
        "Manager": ["Group Product Manager"],
        "Senior Manager": ["Senior Group Product Manager"],
        "Director": ["Director of Product"],
        "VP": ["VP of Product"],
        "C-Suite": ["CPO"],
    },
    "Marketing": {
        "Individual Contributor": [
            "Marketing Manager", "Content Strategist", "Growth Marketer",
            "Demand Gen Specialist", "Brand Manager", "Marketing Analyst",
        ],
        "Team Lead": ["Senior Marketing Manager", "Marketing Lead"],
        "Manager": ["Marketing Manager, Team"],
        "Senior Manager": ["Senior Marketing Manager, Team"],
        "Director": ["Director of Marketing"],
        "VP": ["VP of Marketing"],
        "C-Suite": ["CMO"],
    },
    "CS": {
        "Individual Contributor": [
            "Customer Success Manager", "Technical Support Engineer",
            "Solutions Engineer", "Onboarding Specialist",
            "Customer Success Associate",
        ],
        "Team Lead": ["Senior CSM", "Support Lead"],
        "Manager": ["CS Manager"],
        "Senior Manager": ["Senior CS Manager"],
        "Director": ["Director of Customer Success"],
        "VP": ["VP of Customer Success"],
        "C-Suite": ["CCO"],
    },
    "G&A": {
        "Individual Contributor": [
            "Financial Analyst", "HR Generalist", "Recruiter",
            "Paralegal", "IT Specialist", "Operations Analyst",
            "Accountant", "Executive Assistant",
        ],
        "Team Lead": ["Senior Recruiter", "Senior Analyst", "IT Lead"],
        "Manager": ["Finance Manager", "HR Manager", "IT Manager"],
        "Senior Manager": ["Senior Finance Manager", "Senior HR Manager"],
        "Director": ["Director of Finance", "Director of People", "Director of IT"],
        "VP": ["VP of Finance", "VP of People"],
        "C-Suite": ["CFO", "CHRO"],
    },
    "Sales": {
        "Individual Contributor": [
            "Account Executive", "SDR", "BDR",
            "Sales Engineer", "Sales Operations Analyst",
        ],
        "Team Lead": ["Senior Account Executive", "SDR Team Lead"],
        "Manager": ["Sales Manager"],
        "Senior Manager": ["Regional Sales Manager"],
        "Director": ["Director of Sales"],
        "VP": ["VP of Sales"],
        "C-Suite": ["CRO"],
    },
}

# Schema definitions -----------------------------------------------------------

WORKER_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "Worker_ID", "type": "string", "is_key": True},
    {"name": "Legal_Name", "type": "string"},
    {"name": "Business_Title", "type": "string"},
    {"name": "Supervisory_Organization", "type": "string", "semantic_hint": "department"},
    {"name": "Hire_Date", "type": "date", "semantic_hint": "hire_date"},
    {"name": "Termination_Date", "type": "date"},
    {"name": "Worker_Status", "type": "string", "semantic_hint": "employment_status"},
    {"name": "Management_Level", "type": "string"},
    {"name": "Location", "type": "string", "semantic_hint": "location"},
    {"name": "Cost_Center", "type": "string"},
    {"name": "Annual_Base_Pay", "type": "number", "semantic_hint": "compensation"},
    {"name": "Pay_Currency", "type": "string"},
]

POSITION_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "Position_ID", "type": "string", "is_key": True},
    {"name": "Position_Title", "type": "string"},
    {"name": "Job_Family", "type": "string"},
    {"name": "Job_Profile", "type": "string"},
    {"name": "Supervisory_Organization", "type": "string", "semantic_hint": "department"},
    {"name": "Worker_Count", "type": "number"},
    {"name": "Is_Filled", "type": "boolean"},
]

TIME_OFF_SCHEMA_FIELDS: List[Dict[str, Any]] = [
    {"name": "Worker_ID", "type": "string"},
    {"name": "Leave_Type", "type": "string"},
    {"name": "Start_Date", "type": "date"},
    {"name": "End_Date", "type": "date"},
    {"name": "Status", "type": "string"},
]


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------

class _WorkerRecord:
    """Mutable worker state tracked across quarters."""

    __slots__ = (
        "worker_id", "legal_name", "business_title", "department",
        "hire_date", "termination_date", "worker_status", "management_level",
        "location", "cost_center", "annual_base_pay", "pay_currency",
        "is_contingent",
    )

    def __init__(
        self,
        worker_id: str,
        legal_name: str,
        business_title: str,
        department: str,
        hire_date: str,
        management_level: str,
        location: str,
        cost_center: str,
        annual_base_pay: float,
        pay_currency: str,
        is_contingent: bool = False,
    ) -> None:
        self.worker_id = worker_id
        self.legal_name = legal_name
        self.business_title = business_title
        self.department = department
        self.hire_date = hire_date
        self.termination_date: Optional[str] = None
        self.worker_status = "Active"
        self.management_level = management_level
        self.location = location
        self.cost_center = cost_center
        self.annual_base_pay = annual_base_pay
        self.pay_currency = pay_currency
        self.is_contingent = is_contingent

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Worker_ID": self.worker_id,
            "Legal_Name": self.legal_name,
            "Business_Title": self.business_title,
            "Supervisory_Organization": self.department,
            "Hire_Date": self.hire_date,
            "Termination_Date": self.termination_date,
            "Worker_Status": self.worker_status,
            "Management_Level": self.management_level,
            "Location": self.location,
            "Cost_Center": self.cost_center,
            "Annual_Base_Pay": self.annual_base_pay,
            "Pay_Currency": self.pay_currency,
        }


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class WorkdayGenerator(BaseBusinessGenerator):
    """
    Generates Workday HCM data aligned to a BusinessProfile.

    Produces three datasets:
      * Workers  -- every person (active + terminated) across 12 quarters
      * Positions -- slightly more positions than workers (some unfilled)
      * Time Off  -- ~2 leave records per active worker per quarter

    Active-worker count matches ``profile.headcount`` each quarter; terminated
    count matches ``profile.terminations``. ~3 contingent workers are included
    to replicate the well-known headcount discrepancy between Workday and
    standard reporting (Workday shows 237/248 vs. 235/245 standard).
    """

    SOURCE_SYSTEM = SOURCE_SYSTEM
    PIPE_PREFIX = "wd"

    # Number of contingent workers to inject (creates headcount discrepancy).
    _CONTINGENT_COUNT = 3

    def __init__(self, profile: BusinessProfile, seed: int = 42) -> None:
        super().__init__(seed=seed)
        self.profile = profile

        # Accumulated state across quarters.
        self._workers: List[_WorkerRecord] = []
        self._worker_ids_seen: set = set()
        self._generated = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        pipe_id: Optional[str] = None,
        run_id: Optional[str] = None,
        run_timestamp: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Generate all Workday datasets.

        Returns a dict with keys ``"workers"``, ``"positions"``, ``"time_off"``,
        each containing a DCL-formatted payload.
        """
        pipe_id = pipe_id or f"{self.PIPE_PREFIX}-hcm-001"
        run_id = run_id or self._uuid()
        run_timestamp = run_timestamp or "2026-02-15T08:00:00Z"

        self._build_workforce()

        workers_data = [w.to_dict() for w in self._workers]
        positions_data = self._build_positions()
        time_off_data = self._build_time_off()

        return {
            "workers": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}-workers-001",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=WORKER_SCHEMA_FIELDS,
                data=workers_data,
            ),
            "positions": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}-positions-001",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=POSITION_SCHEMA_FIELDS,
                data=positions_data,
            ),
            "time_off": self.format_dcl_payload(
                pipe_id=f"{self.PIPE_PREFIX}-timeoff-001",
                run_id=run_id,
                run_timestamp=run_timestamp,
                schema_fields=TIME_OFF_SCHEMA_FIELDS,
                data=time_off_data,
            ),
        }

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def _build_workforce(self) -> None:
        """
        Simulate workforce across all quarters.

        For the first quarter, seed the workforce with enough workers to match
        the profile headcount. For each subsequent quarter, hire ``new_hires``
        and terminate ``terminations`` to track the profile.
        """
        if self._generated:
            return
        self._generated = True

        quarters = self.profile.quarters

        # --- Q1: seed the initial workforce ---------------------------------
        first_q = quarters[0]
        initial_headcount = first_q.headcount
        dept_counts = dict(first_q.headcount_by_dept)

        # Generate initial workers per department.
        for dept, count in dept_counts.items():
            for _ in range(count):
                worker = self._create_worker(
                    department=dept,
                    hire_quarter=None,  # pre-existing employee
                    quarter_label=first_q.quarter,
                )
                self._workers.append(worker)

        # Add contingent workers (contractors) -- these create the known
        # headcount discrepancy (Workday shows ~3 more than standard reporting).
        for _ in range(self._CONTINGENT_COUNT):
            dept = self._pick(list(dept_counts.keys()))
            contractor = self._create_worker(
                department=dept,
                hire_quarter=None,
                quarter_label=first_q.quarter,
                is_contingent=True,
            )
            self._workers.append(contractor)

        # --- Subsequent quarters: hire and terminate ------------------------
        for qi in range(1, len(quarters)):
            qm = quarters[qi]
            self._process_quarter(qm)

    def _process_quarter(self, qm: QuarterMetrics) -> None:
        """Apply hires and terminations for a single quarter."""
        quarter = qm.quarter

        # --- Terminations ---------------------------------------------------
        active_workers = [
            w for w in self._workers
            if w.worker_status == "Active" and not w.is_contingent
        ]
        terminations_needed = qm.terminations

        if terminations_needed > 0 and active_workers:
            # Prefer terminating from departments proportional to their size,
            # but avoid terminating everyone from a small department.
            term_candidates = list(active_workers)
            self._rng.shuffle(term_candidates)
            terminated = 0
            for worker in term_candidates:
                if terminated >= terminations_needed:
                    break
                worker.worker_status = "Terminated"
                worker.termination_date = self._date_in_quarter(quarter)
                terminated += 1

        # --- New hires ------------------------------------------------------
        new_hires_needed = qm.new_hires
        dept_counts = dict(qm.headcount_by_dept)

        # Distribute new hires roughly proportional to department target sizes.
        dept_list = list(dept_counts.keys())
        dept_weights = [float(dept_counts[d]) for d in dept_list]
        total_weight = sum(dept_weights)
        if total_weight == 0:
            dept_weights = [1.0] * len(dept_list)
            total_weight = float(len(dept_list))

        hires_remaining = new_hires_needed
        for i, dept in enumerate(dept_list):
            if i == len(dept_list) - 1:
                # Last department gets the remainder to avoid rounding errors.
                dept_hires = hires_remaining
            else:
                dept_hires = max(
                    int(round(new_hires_needed * dept_weights[i] / total_weight)), 0
                )
                dept_hires = min(dept_hires, hires_remaining)
            hires_remaining -= dept_hires

            for _ in range(dept_hires):
                worker = self._create_worker(
                    department=dept,
                    hire_quarter=quarter,
                    quarter_label=quarter,
                )
                self._workers.append(worker)

    # ------------------------------------------------------------------
    # Worker creation helpers
    # ------------------------------------------------------------------

    def _create_worker(
        self,
        department: str,
        hire_quarter: Optional[str],
        quarter_label: str,
        is_contingent: bool = False,
    ) -> _WorkerRecord:
        """Create a single worker record with realistic attributes."""
        worker_id = self._generate_unique_worker_id()
        legal_name = self._generate_name()
        management_level = self._assign_management_level()
        location = self._weighted_choice(LOCATIONS, LOCATION_WEIGHTS)
        cost_center = self._pick(COST_CENTERS.get(department, ["CC-GA-100"]))
        business_title = self._assign_title(department, management_level)
        pay_currency = _LOCATION_CURRENCY.get(location, "USD")
        annual_base_pay = self._generate_pay(management_level, department, pay_currency)

        # Hire date: if pre-existing, random date in the 3 years before Q1 2024.
        # If hired in a specific quarter, date within that quarter.
        if hire_quarter is None:
            # Pre-existing employee: hire date between 2021-01-01 and 2023-12-31
            days_offset = self._rng.randint(0, 1095)  # ~3 years
            hire_date = (date(2021, 1, 1) + timedelta(days=days_offset)).isoformat()
        else:
            hire_date = self._date_in_quarter(hire_quarter)

        return _WorkerRecord(
            worker_id=worker_id,
            legal_name=legal_name,
            business_title=business_title,
            department=department,
            hire_date=hire_date,
            management_level=management_level,
            location=location,
            cost_center=cost_center,
            annual_base_pay=annual_base_pay,
            pay_currency=pay_currency,
            is_contingent=is_contingent,
        )

    def _generate_unique_worker_id(self) -> str:
        """Generate a worker ID that hasn't been used before."""
        while True:
            wid = self._wd_id("WRK")
            if wid not in self._worker_ids_seen:
                self._worker_ids_seen.add(wid)
                return wid

    def _generate_name(self) -> str:
        """Generate a realistic full name."""
        first = self._pick(FIRST_NAMES)
        last = self._pick(LAST_NAMES)
        return f"{first} {last}"

    def _assign_management_level(self) -> str:
        """
        Pick a management level with realistic distribution.

        The MANAGEMENT_LEVELS list is weighted (3x IC, 2x TL, 1x everything
        else) so a simple uniform pick produces the right pyramid shape.
        """
        return self._pick(MANAGEMENT_LEVELS)

    def _assign_title(self, department: str, level: str) -> str:
        """Pick a business title appropriate for department and level."""
        dept_titles = _TITLE_TEMPLATES.get(department)
        if dept_titles is None:
            return f"{level} - {department}"
        level_titles = dept_titles.get(level)
        if not level_titles:
            # Fall back to IC titles if the specific level isn't mapped.
            level_titles = dept_titles.get("Individual Contributor", [f"{level}"])
        return self._pick(level_titles)

    def _generate_pay(
        self, level: str, department: str, currency: str
    ) -> float:
        """
        Generate a realistic annual base pay for the given level.

        Pay ranges are defined in USD; the value is converted to the worker's
        local currency using approximate exchange rates. Engineering and Sales
        departments get a ~5-10% premium at equivalent levels.
        """
        low, high = _PAY_RANGES.get(level, (80_000, 180_000))

        # Department premium for Engineering and Sales.
        if department in ("Engineering", "Sales"):
            low = int(low * 1.05)
            high = int(high * 1.10)

        base_usd = self._rng.randint(low, high)

        # Round to nearest $1,000 in USD equivalent.
        base_usd = round(base_usd / 1000) * 1000

        # Convert to local currency.
        factor = _CURRENCY_FACTORS.get(currency, 1.0)
        local_pay = base_usd * factor

        # For JPY, round to nearest 10,000; for others round to nearest 1,000.
        if currency == "JPY":
            local_pay = round(local_pay / 10_000) * 10_000
        else:
            local_pay = round(local_pay / 1_000) * 1_000

        return local_pay

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def _build_positions(self) -> List[Dict[str, Any]]:
        """
        Build position records.

        Creates one position per worker plus a number of unfilled positions
        (~5-8% of total) to model open requisitions.
        """
        positions: List[Dict[str, Any]] = []
        position_ids_seen: set = set()

        def _unique_pos_id() -> str:
            while True:
                pid = self._wd_id("POS")
                if pid not in position_ids_seen:
                    position_ids_seen.add(pid)
                    return pid

        # One position per worker (filled).
        for worker in self._workers:
            dept = worker.department
            job_family = self._pick(JOB_FAMILIES.get(dept, ["General"]))
            job_profile = self._derive_job_profile(job_family, worker.management_level)

            positions.append({
                "Position_ID": _unique_pos_id(),
                "Position_Title": worker.business_title,
                "Job_Family": job_family,
                "Job_Profile": job_profile,
                "Supervisory_Organization": dept,
                "Worker_Count": 1,
                "Is_Filled": True,
            })

        # Unfilled positions (open reqs): ~5-8% of workforce.
        total_workers = len(self._workers)
        unfilled_count = max(
            int(total_workers * self._rng.uniform(0.05, 0.08)), 3
        )

        departments = list(JOB_FAMILIES.keys())
        for _ in range(unfilled_count):
            dept = self._pick(departments)
            job_family = self._pick(JOB_FAMILIES[dept])
            level = self._assign_management_level()
            title = self._assign_title(dept, level)
            job_profile = self._derive_job_profile(job_family, level)

            positions.append({
                "Position_ID": _unique_pos_id(),
                "Position_Title": title,
                "Job_Family": job_family,
                "Job_Profile": job_profile,
                "Supervisory_Organization": dept,
                "Worker_Count": 0,
                "Is_Filled": False,
            })

        return positions

    def _derive_job_profile(self, job_family: str, level: str) -> str:
        """
        Derive a Workday Job_Profile from job family and level.

        Workday job profiles are typically more specific than job families.
        We synthesize them as ``"<level> - <family>"``.
        """
        # Simplify the level label for the profile string.
        level_short = {
            "Individual Contributor": "IC",
            "Team Lead": "TL",
            "Manager": "MGR",
            "Senior Manager": "SR-MGR",
            "Director": "DIR",
            "VP": "VP",
            "C-Suite": "EXEC",
        }.get(level, "IC")
        return f"{level_short} - {job_family}"

    # ------------------------------------------------------------------
    # Time Off
    # ------------------------------------------------------------------

    def _build_time_off(self) -> List[Dict[str, Any]]:
        """
        Build time-off records across all quarters.

        Generates ~2 leave requests per active worker per quarter, yielding
        roughly 1,500+ records across 12 quarters.
        """
        time_off_records: List[Dict[str, Any]] = []

        for qm in self.profile.quarters:
            quarter = qm.quarter

            # Collect workers who were active at some point during this quarter.
            active_in_quarter = self._workers_active_in_quarter(quarter)

            for worker in active_in_quarter:
                # Average ~2 requests per worker per quarter.
                # Use a Poisson-like distribution: 0, 1, 2, or 3 requests.
                num_requests = self._rng.choices(
                    [0, 1, 2, 3, 4],
                    weights=[0.10, 0.25, 0.35, 0.20, 0.10],
                    k=1,
                )[0]

                for _ in range(num_requests):
                    record = self._create_time_off_record(worker, quarter)
                    time_off_records.append(record)

        return time_off_records

    def _workers_active_in_quarter(self, quarter: str) -> List[_WorkerRecord]:
        """
        Return workers who were active at any point during the given quarter.

        A worker is active in a quarter if:
          - They were hired on or before the last day of the quarter, AND
          - They were not terminated before the first day of the quarter.
        Contingent workers are excluded from time-off generation.
        """
        q_start = self._quarter_start_date(quarter)
        q_end = self._quarter_end_date(quarter)

        result = []
        for w in self._workers:
            if w.is_contingent:
                continue
            # Worker must have been hired by end of quarter.
            if w.hire_date > q_end:
                continue
            # If terminated, the termination must be on or after the quarter start.
            if w.termination_date is not None and w.termination_date < q_start:
                continue
            result.append(w)
        return result

    def _create_time_off_record(
        self, worker: _WorkerRecord, quarter: str
    ) -> Dict[str, Any]:
        """Create a single time-off request for a worker in a quarter."""
        leave_type = self._weighted_choice(LEAVE_TYPES, LEAVE_TYPE_WEIGHTS)
        status = self._weighted_choice(LEAVE_STATUS, LEAVE_STATUS_WEIGHTS)

        # Start date within the quarter.
        start_date_str = self._date_in_quarter(quarter)
        start_date = date.fromisoformat(start_date_str)

        # Duration in business days varies by leave type.
        duration_days = self._leave_duration(leave_type)
        end_date = self._add_business_days(start_date, duration_days)

        return {
            "Worker_ID": worker.worker_id,
            "Leave_Type": leave_type,
            "Start_Date": start_date.isoformat(),
            "End_Date": end_date.isoformat(),
            "Status": status,
        }

    def _leave_duration(self, leave_type: str) -> int:
        """
        Return a realistic duration in business days for the leave type.

        - Vacation: 1-10 days (mean ~4)
        - Sick: 1-5 days (mean ~2)
        - Personal: 1-3 days (mean ~1)
        - Parental: 40-60 days (8-12 weeks)
        - Bereavement: 3-5 days
        - Jury Duty: 2-10 days
        """
        if leave_type == "Vacation":
            return self._rng.choices(
                range(1, 11),
                weights=[5, 8, 10, 12, 15, 12, 10, 8, 5, 3],
                k=1,
            )[0]
        elif leave_type == "Sick":
            return self._rng.choices(
                range(1, 6),
                weights=[30, 25, 20, 15, 10],
                k=1,
            )[0]
        elif leave_type == "Personal":
            return self._rng.choices(
                [1, 2, 3],
                weights=[50, 35, 15],
                k=1,
            )[0]
        elif leave_type == "Parental":
            return self._rng.randint(40, 60)
        elif leave_type == "Bereavement":
            return self._rng.randint(3, 5)
        elif leave_type == "Jury Duty":
            return self._rng.randint(2, 10)
        else:
            return self._rng.randint(1, 5)

    @staticmethod
    def _add_business_days(start: date, business_days: int) -> date:
        """Add business days (Mon-Fri) to a start date."""
        current = start
        added = 0
        while added < business_days:
            current += timedelta(days=1)
            # weekday(): Monday=0 ... Friday=4
            if current.weekday() < 5:
                added += 1
        return current
