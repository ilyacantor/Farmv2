"""
Customer profile generator for cross-sell scoring between Meridian and Cascadia.

Produces enriched customer records with behavioral signal fields that feed into
cross-sell scoring models:
  - C->M cross-sell: Can Cascadia sell BPM services to Meridian's consulting clients?
  - M->C cross-sell: Can Meridian sell advisory services to Cascadia's BPM clients?

Deterministic output via seed=42.  Total: 1200 Meridian + 200 Cascadia customers.
34 overlap customers appear in BOTH lists (marked with is_overlap=True).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Overlap constants (copied from entity_overlap.py to avoid circular deps)
# =============================================================================

# (name, industry, meridian_rev_M, cascadia_rev_M)
_EXACT_MATCHES: List[Tuple[str, str, float, float]] = [
    ("JPMorgan Chase", "Financial Services", 85.0, 12.0),
    ("Amazon", "Technology", 42.0, 28.0),
    ("UnitedHealth Group", "Healthcare", 65.0, 18.0),
    ("Walmart", "Retail", 38.0, 22.0),
    ("AT&T", "Telecommunications", 55.0, 15.0),
    ("Pfizer", "Pharmaceuticals", 48.0, 8.0),
    ("Bank of America", "Financial Services", 72.0, 14.0),
    ("General Motors", "Automotive", 35.0, 11.0),
    ("Procter & Gamble", "Consumer Goods", 28.0, 9.0),
    ("Citigroup", "Financial Services", 62.0, 16.0),
    ("Johnson & Johnson", "Healthcare", 45.0, 7.0),
    ("Verizon", "Telecommunications", 52.0, 13.0),
    ("Intel", "Technology", 30.0, 6.0),
    ("Honeywell", "Industrial", 25.0, 8.0),
]

# (meridian_name, cascadia_name, canonical_name, industry, meridian_rev_M, cascadia_rev_M)
_FUZZY_MATCHES: List[Tuple[str, str, str, str, float, float]] = [
    ("Goldman Sachs Group Inc", "Goldman Sachs & Co", "Goldman Sachs", "Financial Services", 92.0, 11.0),
    ("Microsoft Corporation", "Microsoft Corp", "Microsoft", "Technology", 78.0, 20.0),
    ("The Boeing Company", "Boeing Co", "Boeing", "Aerospace", 45.0, 12.0),
    ("Deloitte LLP", "Deloitte Touche Tohmatsu", "Deloitte", "Professional Services", 15.0, 5.0),
    ("General Electric Co", "GE Healthcare", "General Electric", "Industrial", 55.0, 14.0),
    ("Chevron Corporation", "Chevron USA Inc", "Chevron", "Energy", 40.0, 9.0),
    ("FedEx Corporation", "FedEx Ground", "FedEx", "Logistics", 22.0, 7.0),
    ("MetLife Inc", "MetLife Insurance", "MetLife", "Insurance", 35.0, 8.0),
    ("Raytheon Technologies", "RTX Corporation", "Raytheon/RTX", "Defense", 28.0, 6.0),
    ("Coca-Cola Company", "Coca-Cola Enterprises", "Coca-Cola", "Consumer Goods", 18.0, 5.0),
    ("Accenture PLC", "Accenture LLP", "Accenture", "Professional Services", 12.0, 4.0),
]

# (meridian_name, cascadia_name, canonical_name, industry, meridian_rev_M, cascadia_rev_M, notes)
_HARD_MATCHES: List[Tuple[str, str, str, str, float, float, str]] = [
    ("GlobalBank Corp", "GlobalBank International", "GlobalBank (parent)", "Financial Services", 120.0, 25.0,
     "Different subsidiary names in each CRM."),
    ("Apex Industries LLC", "Pinnacle Manufacturing", "Apex/Pinnacle (DBA)", "Manufacturing", 42.0, 15.0,
     "DBA vs legal name."),
    ("TechVision Inc", "Digital Dynamics Corp", "TechVision/Digital Dynamics (M&A)", "Technology", 85.0, 35.0,
     "TechVision acquired Digital Dynamics in 2023."),
    ("National Insurance Group", "NIG Underwriters", "National Insurance Group", "Insurance", 58.0, 18.0,
     "Subsidiary trading name in Cascadia."),
    ("Pacific Rim Holdings", "PacRim Solutions", "Pacific Rim Holdings", "Conglomerate", 75.0, 22.0,
     "Shortened name in Cascadia CRM."),
    ("First American Financial", "First American Title", "First American", "Financial Services", 32.0, 9.0,
     "Different division names."),
    ("United Technologies", "Carrier Global", "UTC/Carrier (spinoff)", "Industrial", 48.0, 14.0,
     "Carrier was spun off from UTC."),
    ("Berkshire Health Systems", "BHS Medical Group", "Berkshire Health", "Healthcare", 22.0, 8.0,
     "Abbreviated name in Cascadia."),
    ("MegaCorp Global Industries", "MegaCorp Process Services", "MegaCorp Global", "Conglomerate", 200.0, 110.0,
     "Largest shared client. Combined $310M."),
]


# =============================================================================
# Industry pools with weighted distributions
# =============================================================================

MERIDIAN_INDUSTRIES: List[Tuple[str, float]] = [
    ("Financial Services", 0.25),
    ("Technology", 0.20),
    ("Healthcare", 0.15),
    ("Manufacturing", 0.10),
    ("Retail", 0.08),
    ("Energy", 0.07),
    ("Telecommunications", 0.05),
    ("Aerospace", 0.03),
    ("Pharma", 0.04),
    ("Other", 0.03),
]

CASCADIA_INDUSTRIES: List[Tuple[str, float]] = [
    ("Financial Services", 0.30),
    ("Healthcare", 0.20),
    ("Insurance", 0.15),
    ("Technology", 0.10),
    ("Retail", 0.08),
    ("Telecommunications", 0.07),
    ("Manufacturing", 0.05),
    ("Other", 0.05),
]

# =============================================================================
# Service catalogs
# =============================================================================

MERIDIAN_SERVICES = [
    "Strategy Consulting",
    "Operations Advisory",
    "Technology Transformation",
    "Risk & Compliance",
    "Digital/AI Advisory",
    "Commercial Strategy",
    "M&A Integration",
    "Supply Chain Advisory",
    "Talent Strategy",
]

CASCADIA_SERVICES = [
    "F&A Outsourcing",
    "CX Management",
    "Data & Analytics BPO",
    "Industry Process Solutions",
    "HR Outsourcing",
    "Procurement BPO",
    "IT Service Management",
]

# =============================================================================
# Segment definitions
# =============================================================================

SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]

MERIDIAN_SEGMENT_WEIGHTS = [0.20, 0.45, 0.35]  # Enterprise, Mid-Market, SMB
CASCADIA_SEGMENT_WEIGHTS = [0.30, 0.50, 0.20]

REGIONS = ["NA", "EMEA", "APAC"]
MERIDIAN_REGION_WEIGHTS = [0.50, 0.30, 0.20]
CASCADIA_REGION_WEIGHTS = [0.55, 0.25, 0.20]

CONTRACT_TYPES = ["MSA", "SOW", "T&M"]

# Revenue / size ranges per segment: (revenue_min_M, revenue_max_M, emp_min, emp_max, eng_min_M, eng_max_M)
SEGMENT_RANGES = {
    "Enterprise":  (1000.0, 50000.0, 5000, 200000, 2.0, 50.0),
    "Mid-Market":  (100.0,  1000.0,  500,  5000,   0.2, 2.0),
    "SMB":         (10.0,   100.0,   50,   500,    0.05, 0.2),
}

# Industries where manual processes are high (for Meridian C->M scoring)
HIGH_MANUAL_INDUSTRIES = {"Manufacturing", "Retail", "Healthcare", "Industrial", "Consumer Goods"}

# Industries with high process complexity (for Cascadia M->C scoring)
HIGH_COMPLEXITY_INDUSTRIES = {"Financial Services", "Healthcare", "Insurance"}

# Industries with high regulatory burden
HIGH_REGULATORY_INDUSTRIES = {
    "Financial Services": (4.0, 5.0),
    "Healthcare": (3.0, 5.0),
    "Insurance": (3.0, 5.0),
}


# =============================================================================
# Company name pools for non-overlap customers
# =============================================================================

# Fictional company names so overlap names stay unique
_MERIDIAN_NAME_POOL = [
    "Northstar Analytics", "Veridian Capital", "Apex Strategic Group",
    "Cascabel Technologies", "Orion Consulting", "Summit Financial Corp",
    "BluePeak Industries", "CrestWave Solutions", "Falcon Digital",
    "Prism Healthcare", "TerraFirma Manufacturing", "Nexus Telecom",
    "Polaris Retail Group", "Keystone Energy", "Stratos Aerospace",
    "Halcyon Pharma", "Redwood Services", "Atlas Data Corp",
    "Equinox Holdings", "Sapphire Tech", "Silverline Manufacturing",
    "Quantum Analytics", "PinnacleTech Solutions", "Aegis Financial",
    "Cobalt Industries", "Zenith Health Systems", "Ironwood Consulting",
    "Vanguard Digital", "Magellan Corp", "Trident Services",
    "Obsidian Group", "Catalyst Partners", "Helix Bio",
    "Montague Capital", "Paladin Tech", "Rubicon Industries",
    "Solaris Energy", "TerraVerde Holdings", "Axiom Consulting",
    "Borealis Financial", "CedarPoint Group", "Delphi Analytics",
    "Ember Technologies", "Fortis Healthcare", "Granite Manufacturing",
    "HorizonLine Corp", "Indigo Systems", "Javelin Financial",
    "Kestrel Communications", "Lodestar Retail", "Meridius Corp",
    "Navigator Group", "Osprey Digital", "Paragon Services",
    "Quasar Tech", "Riviera Holdings", "Sterling Industries",
    "Titanium Health", "Ultramar Energy", "Vertex Solutions",
    "Wavecrest Analytics", "Xenon Consulting", "Yosemite Financial",
    "Zephyr Technologies", "Aldrin Corp", "Benchmark Digital",
    "Caspian Systems", "Denali Group", "Eclipse Partners",
    "Firelight Solutions", "Greenfield Capital", "Hawthorne Industries",
    "Irongate Financial", "Jupiter Analytics", "Keybridge Tech",
    "Luminary Health", "Magnolia Consulting", "Northbridge Corp",
    "Olympus Group", "Pathfinder Digital", "Quartzite Industries",
    "Ridgeline Financial", "Sequoia Tech", "Thunderbolt Corp",
    "Uplift Healthcare", "Velocitas Group", "Whitecap Solutions",
    "Xerxes Capital", "Yellowstone Industries", "Zinnia Partners",
    "Altair Services", "Bridgewater Tech", "Corinthian Group",
    "Daybreak Holdings", "Evergreen Capital", "Foxglove Analytics",
    "Glacier Financial", "Harbinger Systems", "IvoryTower Corp",
    "Juniper Digital", "Kinetic Industries", "Lighthouse Health",
    "Momentum Partners", "Nighthawk Tech", "Onyx Solutions",
]

_CASCADIA_NAME_POOL = [
    "Clearwater BPO", "Pacific Process Group", "Emerald Outsourcing",
    "Columbia Managed Services", "Rainier Solutions", "SoundView Processing",
    "Olympic Process Corp", "Cascade Financial Services", "Tidewater BPM",
    "Whidbey Group", "San Juan Analytics", "Puget Systems",
    "Snoqualmie Services", "Hood River Processing", "Astoria Solutions",
    "Bend Operations", "Corvallis Group", "Deschutes Corp",
    "Enumclaw Services", "Friday Harbor Tech", "Gresham Processing",
    "Hillsboro Solutions", "Issaquah Corp", "Juneau Partners",
    "Klamath Group", "Longview Services", "Mukilteo Systems",
    "Newport Processing", "Oregon City Corp", "Pendleton Group",
    "Quincy Solutions", "Richland Services", "Sequim Corp",
    "Tacoma Processing", "Umpqua Group", "Vancouver Solutions",
    "Walla Walla Corp", "Yakima Services", "Anacortes Group",
    "Bainbridge Processing", "Centralia Solutions", "Dupont Services",
    "Ellensburg Corp", "Ferndale Group", "Goldendale Systems",
]


# =============================================================================
# Data class
# =============================================================================

@dataclass
class CustomerProfile:
    customer_id: str
    customer_name: str
    entity_id: str
    industry: str
    segment: str
    annual_revenue_M: float
    employees: int
    region: str
    engagement_value_M: float
    years_as_client: int
    contract_type: str
    is_overlap: bool
    overlap_canonical_name: str | None

    # Behavioral signals for C->M cross-sell (BPM services to consulting clients)
    manual_process_count: int
    outsourcing_readiness: float
    transformation_maturity: float
    engagement_recency: float
    expressed_interest: float

    # Behavioral signals for M->C cross-sell (advisory to BPM clients)
    process_complexity: float
    regulatory_burden: float
    recent_ma: float
    growth_rate: float
    escalation_history: float

    # Service history
    active_services: list[str]
    completed_projects: int
    last_project_end: str | None


# =============================================================================
# Generator
# =============================================================================

class CustomerProfileGenerator:
    """Generates enriched customer profiles for Meridian and Cascadia entities."""

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)
        self.meridian: List[CustomerProfile] = []
        self.cascadia: List[CustomerProfile] = []
        self._generate()

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _weighted_choice(self, items: List[Tuple[str, float]]) -> str:
        """Pick from (value, weight) pairs using the seeded RNG."""
        values = [v for v, _ in items]
        weights = [w for _, w in items]
        return self._rng.choices(values, weights=weights, k=1)[0]

    def _weighted_choice_list(self, items: List[str], weights: List[float]) -> str:
        return self._rng.choices(items, weights=weights, k=1)[0]

    def _uniform(self, lo: float, hi: float) -> float:
        return self._rng.uniform(lo, hi)

    def _gauss_clipped(self, mu: float, sigma: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, self._rng.gauss(mu, sigma)))

    def _log_uniform(self, lo: float, hi: float) -> float:
        """Log-uniform distribution for revenue/size (produces more small values)."""
        return math.exp(self._uniform(math.log(lo), math.log(hi)))

    # -------------------------------------------------------------------------
    # Build overlap lookup
    # -------------------------------------------------------------------------

    def _build_overlap_records(self) -> Tuple[
        Dict[str, Tuple[str, str, float, float]],  # meridian name -> (canonical, industry, m_rev, c_rev)
        Dict[str, Tuple[str, str, float, float]],  # cascadia name -> (canonical, industry, m_rev, c_rev)
    ]:
        """Return dicts mapping entity-specific names to overlap metadata."""
        m_overlap: Dict[str, Tuple[str, str, float, float]] = {}
        c_overlap: Dict[str, Tuple[str, str, float, float]] = {}

        # Exact matches: same name in both CRMs
        for name, industry, m_rev, c_rev in _EXACT_MATCHES:
            m_overlap[name] = (name, industry, m_rev, c_rev)
            c_overlap[name] = (name, industry, m_rev, c_rev)

        # Fuzzy matches: different names per entity
        for m_name, c_name, canonical, industry, m_rev, c_rev in _FUZZY_MATCHES:
            m_overlap[m_name] = (canonical, industry, m_rev, c_rev)
            c_overlap[c_name] = (canonical, industry, m_rev, c_rev)

        # Hard matches: different names + notes
        for m_name, c_name, canonical, industry, m_rev, c_rev, _notes in _HARD_MATCHES:
            m_overlap[m_name] = (canonical, industry, m_rev, c_rev)
            c_overlap[c_name] = (canonical, industry, m_rev, c_rev)

        return m_overlap, c_overlap

    # -------------------------------------------------------------------------
    # Segment / size generation
    # -------------------------------------------------------------------------

    def _gen_segment(self, weights: List[float]) -> str:
        return self._weighted_choice_list(SEGMENTS, weights)

    def _gen_size(self, segment: str) -> Tuple[float, int, float]:
        """Return (annual_revenue_M, employees, engagement_value_M) for a segment."""
        rev_lo, rev_hi, emp_lo, emp_hi, eng_lo, eng_hi = SEGMENT_RANGES[segment]
        revenue = round(self._log_uniform(rev_lo, rev_hi), 1)
        employees = int(self._log_uniform(emp_lo, emp_hi))
        engagement = round(self._log_uniform(eng_lo, eng_hi), 3)
        return revenue, employees, engagement

    # -------------------------------------------------------------------------
    # Behavioral signal generation
    # -------------------------------------------------------------------------

    def _gen_meridian_signals(
        self, industry: str, segment: str, years: int, completed: int
    ) -> Tuple[int, float, float, float, float]:
        """Generate C->M cross-sell signals for a Meridian customer.

        Most consulting clients do NOT need BPM.  ~95% should have low
        signals (few manual processes, low outsourcing readiness).  Only
        ~5% are genuine BPM candidates with high signals.
        """
        # Decide if this customer is a genuine BPM candidate (~5%)
        is_bpm_candidate = self._rng.random() < 0.05

        if is_bpm_candidate:
            # High signals: many manual processes, high readiness
            if segment == "Enterprise":
                base_lo, base_hi = 28, 48
            elif segment == "Mid-Market":
                base_lo, base_hi = 22, 38
            else:
                base_lo, base_hi = 15, 28
            if industry in HIGH_MANUAL_INDUSTRIES:
                base_lo = min(base_lo + 5, 50)
                base_hi = min(base_hi + 5, 50)
        else:
            # Low signals: few manual processes (consulting clients have
            # sophisticated processes already)
            if segment == "Enterprise":
                base_lo, base_hi = 2, 12
            elif segment == "Mid-Market":
                base_lo, base_hi = 1, 8
            else:
                base_lo, base_hi = 0, 5

        manual_process_count = self._rng.randint(base_lo, base_hi)

        # transformation_maturity: candidates score higher
        if is_bpm_candidate:
            tm = self._gauss_clipped(4.0, 0.5, 3.0, 5.0)
        elif industry == "Technology":
            tm = self._gauss_clipped(2.5, 0.8, 0.0, 4.0)
        else:
            tm = self._gauss_clipped(1.5, 0.8, 0.0, 3.5)
        transformation_maturity = round(tm, 2)

        # outsourcing_readiness: only BPM candidates have meaningful readiness
        if is_bpm_candidate:
            base_readiness = 3.5 + (manual_process_count / 50.0) * 1.5
            outsourcing_readiness = round(
                self._gauss_clipped(base_readiness, 0.4, 3.0, 5.0), 2
            )
        else:
            outsourcing_readiness = round(
                self._gauss_clipped(0.8, 0.5, 0.0, 2.0), 2
            )

        # engagement_recency: BPM candidates have recent active engagement
        if is_bpm_candidate:
            recency_base = 4.2
        elif completed > 0 and years <= 2:
            recency_base = 3.0
        elif completed > 3:
            recency_base = 2.5
        else:
            recency_base = 1.5
        engagement_recency = round(
            self._gauss_clipped(recency_base, 0.6, 0.0, 5.0), 2
        )

        # expressed_interest: only BPM candidates with high readiness express interest
        if is_bpm_candidate and outsourcing_readiness >= 3.5 and self._rng.random() < 0.5:
            expressed_interest = round(self._uniform(3.5, 5.0), 2)
        else:
            expressed_interest = round(self._gauss_clipped(0.3, 0.4, 0.0, 1.5), 2)

        return (
            manual_process_count,
            outsourcing_readiness,
            transformation_maturity,
            engagement_recency,
            expressed_interest,
        )

    def _gen_cascadia_signals(
        self, industry: str, segment: str, years: int, engagement_recency: float
    ) -> Tuple[float, float, float, float, float]:
        """Generate M->C cross-sell signals for a Cascadia customer.

        About 35-40% of BPM clients should be strong advisory candidates.
        Complexity and regulatory burden are the key discriminators.
        """
        # ~40% are strong advisory candidates (BPM clients with genuine complexity)
        is_advisory_candidate = self._rng.random() < 0.40

        # process_complexity: 0-10
        if is_advisory_candidate:
            if industry in HIGH_COMPLEXITY_INDUSTRIES:
                pc_base = 7.5
            else:
                pc_base = 6.0
            if segment == "Enterprise":
                pc_base += 1.0
            process_complexity = round(self._gauss_clipped(pc_base, 1.0, 5.0, 10.0), 2)
        else:
            pc_base = 3.0
            if segment == "Enterprise":
                pc_base += 0.5
            process_complexity = round(self._gauss_clipped(pc_base, 1.2, 0.0, 5.5), 2)

        # regulatory_burden: candidates in regulated industries score high
        if is_advisory_candidate and industry in HIGH_REGULATORY_INDUSTRIES:
            lo, hi = HIGH_REGULATORY_INDUSTRIES[industry]
            regulatory_burden = round(self._uniform(lo, hi), 2)
        elif industry in HIGH_REGULATORY_INDUSTRIES:
            lo, hi = HIGH_REGULATORY_INDUSTRIES[industry]
            regulatory_burden = round(self._uniform(max(0, lo - 1), lo + 0.5), 2)
        else:
            regulatory_burden = round(self._gauss_clipped(1.0, 0.6, 0.0, 2.5), 2)

        # recent_ma: ~15% have recent M&A (strong trigger for advisory)
        if self._rng.random() < 0.15:
            recent_ma = round(self._uniform(3.0, 5.0), 2)
        else:
            recent_ma = round(self._gauss_clipped(0.5, 0.5, 0.0, 2.0), 2)

        # growth_rate: candidates have higher growth
        if is_advisory_candidate:
            growth_rate = round(self._gauss_clipped(3.5, 0.8, 2.0, 5.0), 2)
        else:
            growth_rate = round(self._gauss_clipped(1.8, 0.8, 0.0, 3.5), 2)

        # escalation_history: inversely correlates with engagement_recency
        esc_base = max(0.0, 3.5 - engagement_recency)
        escalation_history = round(self._gauss_clipped(esc_base, 0.7, 0.0, 5.0), 2)

        return (
            process_complexity,
            regulatory_burden,
            recent_ma,
            growth_rate,
            escalation_history,
        )

    # -------------------------------------------------------------------------
    # Service history
    # -------------------------------------------------------------------------

    def _gen_services(self, catalog: List[str], segment: str) -> Tuple[List[str], int, str | None]:
        """Generate active_services, completed_projects, last_project_end."""
        if segment == "Enterprise":
            n_services = self._rng.randint(2, min(5, len(catalog)))
            completed = self._rng.randint(3, 20)
        elif segment == "Mid-Market":
            n_services = self._rng.randint(1, min(3, len(catalog)))
            completed = self._rng.randint(1, 10)
        else:
            n_services = self._rng.randint(1, min(2, len(catalog)))
            completed = self._rng.randint(0, 5)

        active = self._rng.sample(catalog, n_services)

        if completed > 0:
            year = self._rng.choice([2024, 2025, 2026])
            quarter = self._rng.choice(["Q1", "Q2", "Q3", "Q4"])
            last_end = f"{year}-{quarter}"
        else:
            last_end = None

        return active, completed, last_end

    # -------------------------------------------------------------------------
    # Single profile builder
    # -------------------------------------------------------------------------

    def _build_profile(
        self,
        customer_id: str,
        name: str,
        entity_id: str,
        industry: str,
        segment: str,
        region: str,
        engagement_value_M: float | None,
        is_overlap: bool,
        canonical_name: str | None,
        catalog: List[str],
    ) -> CustomerProfile:
        """Build a single CustomerProfile with all signals."""

        revenue, employees, gen_engagement = self._gen_size(segment)
        if engagement_value_M is None:
            engagement_value_M = gen_engagement

        years = self._rng.randint(1, 15)
        contract_type = self._rng.choice(CONTRACT_TYPES)
        active_services, completed, last_end = self._gen_services(catalog, segment)

        # C->M signals (always generated; meaningful for Meridian customers)
        (
            manual_process_count,
            outsourcing_readiness,
            transformation_maturity,
            engagement_recency,
            expressed_interest,
        ) = self._gen_meridian_signals(industry, segment, years, completed)

        # M->C signals (always generated; meaningful for Cascadia customers)
        (
            process_complexity,
            regulatory_burden,
            recent_ma,
            growth_rate,
            escalation_history,
        ) = self._gen_cascadia_signals(industry, segment, years, engagement_recency)

        return CustomerProfile(
            customer_id=customer_id,
            customer_name=name,
            entity_id=entity_id,
            industry=industry,
            segment=segment,
            annual_revenue_M=revenue,
            employees=employees,
            region=region,
            engagement_value_M=round(engagement_value_M, 3),
            years_as_client=years,
            contract_type=contract_type,
            is_overlap=is_overlap,
            overlap_canonical_name=canonical_name,
            manual_process_count=manual_process_count,
            outsourcing_readiness=outsourcing_readiness,
            transformation_maturity=transformation_maturity,
            engagement_recency=engagement_recency,
            expressed_interest=expressed_interest,
            process_complexity=process_complexity,
            regulatory_burden=regulatory_burden,
            recent_ma=recent_ma,
            growth_rate=growth_rate,
            escalation_history=escalation_history,
            active_services=active_services,
            completed_projects=completed,
            last_project_end=last_end,
        )

    # -------------------------------------------------------------------------
    # Name generation
    # -------------------------------------------------------------------------

    def _generate_name(self, pool: List[str], index: int, industry: str) -> str:
        """Generate a unique customer name from the pool, recycling with suffixes."""
        if index < len(pool):
            return pool[index]
        # Beyond the pool: append a numeric suffix
        base = pool[index % len(pool)]
        suffix = (index // len(pool)) + 1
        return f"{base} {suffix}"

    # -------------------------------------------------------------------------
    # Main generation
    # -------------------------------------------------------------------------

    def _generate(self) -> None:
        m_overlap, c_overlap = self._build_overlap_records()

        # Track overlap names used, to avoid duplicate customer names
        m_overlap_names = set(m_overlap.keys())
        c_overlap_names = set(c_overlap.keys())

        meridian_id = 1
        cascadia_id = 1

        # --- Generate Meridian overlap customers first ---
        for m_name, (canonical, industry, m_rev, _c_rev) in m_overlap.items():
            cid = f"M-CUST-{meridian_id:04d}"
            meridian_id += 1

            # Overlap customers are generally Enterprise or Mid-Market (large companies)
            segment = self._weighted_choice_list(
                ["Enterprise", "Mid-Market"], [0.65, 0.35]
            )
            region = self._weighted_choice_list(REGIONS, MERIDIAN_REGION_WEIGHTS)

            profile = self._build_profile(
                customer_id=cid,
                name=m_name,
                entity_id="meridian",
                industry=industry,
                segment=segment,
                region=region,
                engagement_value_M=m_rev,
                is_overlap=True,
                canonical_name=canonical,
                catalog=MERIDIAN_SERVICES,
            )
            self.meridian.append(profile)

        # --- Generate Cascadia overlap customers ---
        for c_name, (canonical, industry, _m_rev, c_rev) in c_overlap.items():
            cid = f"C-CUST-{cascadia_id:04d}"
            cascadia_id += 1

            segment = self._weighted_choice_list(
                ["Enterprise", "Mid-Market"], [0.65, 0.35]
            )
            region = self._weighted_choice_list(REGIONS, CASCADIA_REGION_WEIGHTS)

            profile = self._build_profile(
                customer_id=cid,
                name=c_name,
                entity_id="cascadia",
                industry=industry,
                segment=segment,
                region=region,
                engagement_value_M=c_rev,
                is_overlap=True,
                canonical_name=canonical,
                catalog=CASCADIA_SERVICES,
            )
            self.cascadia.append(profile)

        # --- Fill remaining Meridian customers (1200 total) ---
        remaining_m = 1200 - len(self.meridian)
        name_idx = 0
        for i in range(remaining_m):
            cid = f"M-CUST-{meridian_id:04d}"
            meridian_id += 1

            industry = self._weighted_choice(MERIDIAN_INDUSTRIES)
            segment = self._gen_segment(MERIDIAN_SEGMENT_WEIGHTS)
            region = self._weighted_choice_list(REGIONS, MERIDIAN_REGION_WEIGHTS)
            name = self._generate_name(_MERIDIAN_NAME_POOL, name_idx, industry)
            name_idx += 1

            profile = self._build_profile(
                customer_id=cid,
                name=name,
                entity_id="meridian",
                industry=industry,
                segment=segment,
                region=region,
                engagement_value_M=None,
                is_overlap=False,
                canonical_name=None,
                catalog=MERIDIAN_SERVICES,
            )
            self.meridian.append(profile)

        # --- Fill remaining Cascadia customers (200 total) ---
        remaining_c = 200 - len(self.cascadia)
        name_idx = 0
        for i in range(remaining_c):
            cid = f"C-CUST-{cascadia_id:04d}"
            cascadia_id += 1

            industry = self._weighted_choice(CASCADIA_INDUSTRIES)
            segment = self._gen_segment(CASCADIA_SEGMENT_WEIGHTS)
            region = self._weighted_choice_list(REGIONS, CASCADIA_REGION_WEIGHTS)
            name = self._generate_name(_CASCADIA_NAME_POOL, name_idx, industry)
            name_idx += 1

            profile = self._build_profile(
                customer_id=cid,
                name=name,
                entity_id="cascadia",
                industry=industry,
                segment=segment,
                region=region,
                engagement_value_M=None,
                is_overlap=False,
                canonical_name=None,
                catalog=CASCADIA_SERVICES,
            )
            self.cascadia.append(profile)

    # -------------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------------

    def _segment_counts(self, profiles: List[CustomerProfile]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for p in profiles:
            counts[p.segment] = counts.get(p.segment, 0) + 1
        return counts

    def to_dict(self) -> dict:
        """Serialize all profiles to a dict for JSON output."""
        return {
            "meridian_customers": [p.__dict__ for p in self.meridian],
            "cascadia_customers": [p.__dict__ for p in self.cascadia],
            "summary": {
                "meridian_count": len(self.meridian),
                "cascadia_count": len(self.cascadia),
                "overlap_count": sum(1 for c in self.meridian if c.is_overlap),
                "meridian_segments": self._segment_counts(self.meridian),
                "cascadia_segments": self._segment_counts(self.cascadia),
            },
        }


# =============================================================================
# CLI entry point for quick validation
# =============================================================================

if __name__ == "__main__":
    import json

    gen = CustomerProfileGenerator(seed=42)
    data = gen.to_dict()

    print(f"Meridian customers: {data['summary']['meridian_count']}")
    print(f"Cascadia customers: {data['summary']['cascadia_count']}")
    print(f"Overlap count (Meridian side): {data['summary']['overlap_count']}")
    print(f"Meridian segments: {data['summary']['meridian_segments']}")
    print(f"Cascadia segments: {data['summary']['cascadia_segments']}")

    # Show a few overlap examples
    overlaps = [c for c in gen.meridian if c.is_overlap][:3]
    print("\nSample overlap profiles (Meridian side):")
    for o in overlaps:
        print(f"  {o.customer_id} | {o.customer_name} | canonical={o.overlap_canonical_name} "
              f"| engagement=${o.engagement_value_M}M | outsourcing_readiness={o.outsourcing_readiness}")

    # Show a few non-overlap
    non_overlaps = [c for c in gen.meridian if not c.is_overlap][:3]
    print("\nSample non-overlap profiles (Meridian side):")
    for o in non_overlaps:
        print(f"  {o.customer_id} | {o.customer_name} | {o.industry} | {o.segment} "
              f"| manual_procs={o.manual_process_count} | outsourcing={o.outsourcing_readiness}")
