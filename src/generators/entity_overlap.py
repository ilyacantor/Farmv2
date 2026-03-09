"""
Entity overlap generator for dual-entity (MEI/Convergence) scenarios.

Generates intentional overlaps between Meridian Consulting Group and Cascadia
Logistics for entity resolution testing. Produces customer, vendor, and people
overlaps with varying match difficulty (exact, fuzzy, hard).

The overlap data feeds into ground truth manifests so that entity resolution
accuracy can be measured against known answers.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════════
# Data structures
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CustomerOverlap:
    meridian_name: str           # name in Meridian's CRM
    cascadia_name: str           # name in Cascadia's CRM
    canonical_name: str          # actual company name
    match_type: str              # "exact", "fuzzy", "hard"
    confidence: float            # 0.0-1.0
    meridian_revenue: float      # annual revenue from Meridian ($M)
    cascadia_revenue: float      # annual revenue from Cascadia ($M)
    combined_revenue: float      # sum
    combined_pct_of_total: float  # combined / (5000 + 1000)
    concentration_flag: bool     # True if combined > 5% of $6B
    industry: str
    notes: str                   # e.g. "subsidiary vs parent name"
    engagement_detail: List["CustomerEngagementDetail"] = field(default_factory=list)


@dataclass
class VendorOverlap:
    meridian_name: str
    cascadia_name: str
    canonical_name: str
    match_type: str
    category: str                # "cloud_infra", "collaboration", "staffing", etc.
    meridian_spend: float        # annual ($M)
    cascadia_spend: float
    combined_spend: float
    consolidation_opportunity: bool  # True if combined spend > $5M
    consolidation_detail: Optional["VendorConsolidationDetail"] = None


@dataclass
class PeopleOverlap:
    function: str                # "Finance", "HR", "IT", "Legal"
    meridian_headcount: int
    cascadia_headcount: int
    combined_headcount: int
    role_overlap_examples: List[str]
    definitional_note: str       # e.g. "M includes contractors, C is W-2 only"
    role_detail: List["RoleDetail"] = field(default_factory=list)


@dataclass
class CustomerEngagementDetail:
    entity: str                    # "meridian" or "cascadia"
    service_types: List[str]       # e.g. ["Strategy Consulting", "Digital Transformation"]
    contract_type: str             # "MSA", "SOW", "T&M"
    annual_value_M: float
    relationship_start_year: int
    primary_contact_role: str      # e.g. "VP Operations", "CTO"


@dataclass
class VendorConsolidationDetail:
    meridian_contract_type: str     # "enterprise", "department", "ad-hoc"
    cascadia_contract_type: str
    meridian_contract_end: str      # "2025-Q4" format
    cascadia_contract_end: str
    estimated_savings_pct: float    # 10-20% for consolidation candidates
    estimated_savings_M: float
    savings_rationale: str
    service_subcategories: List[str]


@dataclass
class RoleDetail:
    title: str
    meridian_count: int
    cascadia_count: int
    combined_count: int
    consolidation_action: str      # "retain_both", "consolidate", "evaluate"
    reporting_line: str


@dataclass
class OverlapData:
    customers: List[CustomerOverlap]
    vendors: List[VendorOverlap]
    people: List[PeopleOverlap]

    # Summary stats
    customer_overlap_pct: float   # 15-20% of Cascadia's base (the smaller base)
    vendor_overlap_pct: float     # 20-25% of combined vendor base
    customers_creating_new_threshold: List[str]  # names crossing 5% combined

    def to_ground_truth_dict(self) -> Dict[str, Any]:
        """Produce the structure expected by the ground truth manifest."""
        meridian_accounts = 1200
        cascadia_accounts = 200
        combined_accounts = meridian_accounts + cascadia_accounts
        total_overlapping_customers = len(self.customers)

        customer_matches = []
        for c in self.customers:
            customer_matches.append({
                "meridian_name": c.meridian_name,
                "cascadia_name": c.cascadia_name,
                "canonical_name": c.canonical_name,
                "match_type": c.match_type,
                "confidence": c.confidence,
                "meridian_revenue_M": c.meridian_revenue,
                "cascadia_revenue_M": c.cascadia_revenue,
                "combined_revenue_M": c.combined_revenue,
                "combined_pct_of_total": c.combined_pct_of_total,
                "concentration_flag": c.concentration_flag,
                "industry": c.industry,
                "notes": c.notes,
                "engagement_detail": [
                    {
                        "entity": ed.entity,
                        "service_types": ed.service_types,
                        "contract_type": ed.contract_type,
                        "annual_value_M": ed.annual_value_M,
                        "relationship_start_year": ed.relationship_start_year,
                        "primary_contact_role": ed.primary_contact_role,
                    }
                    for ed in c.engagement_detail
                ],
            })

        concentration_crossings = [
            c.canonical_name for c in self.customers if c.concentration_flag
        ]

        vendor_matches = []
        for v in self.vendors:
            vendor_matches.append({
                "meridian_name": v.meridian_name,
                "cascadia_name": v.cascadia_name,
                "canonical_name": v.canonical_name,
                "match_type": v.match_type,
                "category": v.category,
                "meridian_spend_M": v.meridian_spend,
                "cascadia_spend_M": v.cascadia_spend,
                "combined_spend_M": v.combined_spend,
                "consolidation_opportunity": v.consolidation_opportunity,
                "consolidation_detail": {
                    "meridian_contract_type": cd.meridian_contract_type,
                    "cascadia_contract_type": cd.cascadia_contract_type,
                    "meridian_contract_end": cd.meridian_contract_end,
                    "cascadia_contract_end": cd.cascadia_contract_end,
                    "estimated_savings_pct": cd.estimated_savings_pct,
                    "estimated_savings_M": cd.estimated_savings_M,
                    "savings_rationale": cd.savings_rationale,
                    "service_subcategories": cd.service_subcategories,
                } if (cd := v.consolidation_detail) else None,
            })

        people_entries = []
        for p in self.people:
            people_entries.append({
                "function": p.function,
                "meridian_headcount": p.meridian_headcount,
                "cascadia_headcount": p.cascadia_headcount,
                "combined_headcount": p.combined_headcount,
                "role_overlap_examples": p.role_overlap_examples,
                "definitional_note": p.definitional_note,
                "role_detail": [
                    {
                        "title": rd.title,
                        "meridian_count": rd.meridian_count,
                        "cascadia_count": rd.cascadia_count,
                        "combined_count": rd.combined_count,
                        "consolidation_action": rd.consolidation_action,
                        "reporting_line": rd.reporting_line,
                    }
                    for rd in p.role_detail
                ],
            })

        meridian_vendors = 2000
        cascadia_vendors = 800
        total_overlapping_vendors = len(self.vendors)

        return {
            "customer_overlap": {
                "total_overlapping": total_overlapping_customers,
                "overlap_pct_of_meridian": round(
                    total_overlapping_customers / meridian_accounts * 100, 1
                ),
                "overlap_pct_of_cascadia": round(
                    total_overlapping_customers / cascadia_accounts * 100, 1
                ),
                "overlap_pct_of_combined": round(
                    total_overlapping_customers / combined_accounts * 100, 1
                ),
                "matches": customer_matches,
                "concentration_threshold_crossings": concentration_crossings,
            },
            "vendor_overlap": {
                "total_overlapping": total_overlapping_vendors,
                "overlap_pct_of_meridian": round(
                    total_overlapping_vendors / meridian_vendors * 100, 1
                ),
                "overlap_pct_of_cascadia": round(
                    total_overlapping_vendors / cascadia_vendors * 100, 1
                ),
                "overlap_pct_of_combined": round(
                    total_overlapping_vendors / (meridian_vendors + cascadia_vendors) * 100, 1
                ),
                "matches": vendor_matches,
            },
            "people_overlap": {
                "functions": people_entries,
                "total_meridian_corporate": sum(p.meridian_headcount for p in self.people),
                "total_cascadia_corporate": sum(p.cascadia_headcount for p in self.people),
                "total_combined_corporate": sum(p.combined_headcount for p in self.people),
            },
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Constants — named customer overlaps
# ═══════════════════════════════════════════════════════════════════════════════

# Combined revenue base: Meridian $5B + Cascadia $1B = $6B
_COMBINED_REVENUE_BASE = 6000.0  # $M
_CONCENTRATION_THRESHOLD = 0.05  # 5%

# ---- Exact matches: same name in both CRMs ----
# (name, industry, meridian_revenue_M, cascadia_revenue_M)
EXACT_MATCHES: List[Tuple[str, str, float, float]] = [
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

# ---- Fuzzy matches: name variants (Inc vs Corp, abbreviations, etc.) ----
# (meridian_name, cascadia_name, canonical_name, industry, meridian_rev_M, cascadia_rev_M)
FUZZY_MATCHES: List[Tuple[str, str, str, str, float, float]] = [
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

# ---- Hard matches: parent/subsidiary, DBA, M&A, completely different names ----
# (meridian_name, cascadia_name, canonical_name, industry, meridian_rev_M, cascadia_rev_M, notes)
HARD_MATCHES: List[Tuple[str, str, str, str, float, float, str]] = [
    ("GlobalBank Corp", "GlobalBank International", "GlobalBank (parent)", "Financial Services", 120.0, 25.0,
     "Different subsidiary names in each CRM. Combined = $145M = 2.4% — close to threshold."),
    ("Apex Industries LLC", "Pinnacle Manufacturing", "Apex/Pinnacle (DBA)", "Manufacturing", 42.0, 15.0,
     "DBA vs legal name. Pinnacle is Apex's trade name for outsourcing contracts."),
    ("TechVision Inc", "Digital Dynamics Corp", "TechVision/Digital Dynamics (M&A)", "Technology", 85.0, 35.0,
     "TechVision acquired Digital Dynamics in 2023. Still separate in both CRMs. Combined = $120M."),
    ("National Insurance Group", "NIG Underwriters", "National Insurance Group", "Insurance", 58.0, 18.0,
     "Subsidiary trading name in Cascadia."),
    ("Pacific Rim Holdings", "PacRim Solutions", "Pacific Rim Holdings", "Conglomerate", 75.0, 22.0,
     "Shortened name in Cascadia CRM. Combined = $97M."),
    ("First American Financial", "First American Title", "First American", "Financial Services", 32.0, 9.0,
     "Different division names."),
    ("United Technologies", "Carrier Global", "UTC/Carrier (spinoff)", "Industrial", 48.0, 14.0,
     "Carrier was spun off from UTC. Both entities still have contracts."),
    ("Berkshire Health Systems", "BHS Medical Group", "Berkshire Health", "Healthcare", 22.0, 8.0,
     "Abbreviated name in Cascadia."),
    ("MegaCorp Global Industries", "MegaCorp Process Services", "MegaCorp Global", "Conglomerate", 200.0, 110.0,
     "Largest shared client. Combined revenue $310M = 5.2% of combined entity — triggers concentration threshold."),
]


# ═══════════════════════════════════════════════════════════════════════════════
# Constants — shared vendors
# ═══════════════════════════════════════════════════════════════════════════════

# (vendor_name, meridian_spend_M, cascadia_spend_M)
SHARED_VENDORS: Dict[str, List[Tuple[str, float, float]]] = {
    "cloud_infrastructure": [
        ("Amazon Web Services", 45.0, 12.0),
        ("Microsoft Azure", 38.0, 8.0),
        ("Google Cloud Platform", 15.0, 4.0),
    ],
    "collaboration": [
        ("Slack Technologies", 2.5, 0.8),
        ("Zoom Video Communications", 1.8, 0.6),
        ("Atlassian", 3.2, 1.1),
    ],
    "professional_services": [
        ("KPMG", 8.0, 3.0),
        ("PwC", 12.0, 4.5),
        ("EY", 6.0, 2.0),
    ],
    "staffing": [
        ("Robert Half International", 15.0, 5.0),
        ("Randstad", 12.0, 8.0),
        ("ManpowerGroup", 8.0, 6.0),
        ("Hays", 5.0, 3.0),
    ],
    "technology": [
        ("Salesforce", 4.5, 1.2),
        ("ServiceNow", 3.8, 1.5),
        ("Oracle", 0.0, 6.0),
        ("SAP", 8.0, 0.0),
    ],
    "insurance": [
        ("Aon", 4.0, 1.5),
        ("Marsh McLennan", 3.5, 1.2),
    ],
    "travel": [
        ("American Express GBT", 22.0, 3.0),
        ("BCD Travel", 8.0, 2.0),
    ],
    "facilities": [
        ("CBRE Group", 12.0, 5.0),
        ("JLL", 8.0, 4.0),
        ("Cushman & Wakefield", 5.0, 3.0),
    ],
    "telecom": [
        ("AT&T Business", 3.0, 2.0),
        ("Verizon Business", 2.5, 1.5),
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# Non-overlapping vendor name pools for procedural generation
# ═══════════════════════════════════════════════════════════════════════════════

_MERIDIAN_VENDOR_PREFIXES = [
    "Summit", "Apex", "Vanguard", "Pinnacle", "Catalyst", "Beacon", "Nexus",
    "Vertex", "Fusion", "Atlas", "Horizon", "Sterling", "Keystone", "Crest",
    "Bridgepoint", "Ironclad", "Elevate", "Clearpath", "Trident", "Silverline",
    "Northstar", "Redwood", "Granite", "Cobalt", "Emerald", "Sapphire",
    "Titan", "Osprey", "Falcon", "Meridian", "Compass", "Anchor", "Prism",
    "Sequoia", "Cypress", "Alpine", "Harbor", "Pacific", "Atlantic", "Eagle",
]

_CASCADIA_VENDOR_PREFIXES = [
    "Coastal", "Cascade", "Evergreen", "Rainier", "Olympic", "Puget",
    "Columbia", "Willamette", "Skyline", "Tidewater", "Glacier", "Timber",
    "Orca", "Salmon", "Cedar", "Fern", "Douglas", "Hemlock", "Spruce",
    "Alder", "Birch", "Maple", "Aspen", "River", "Creek", "Bay",
    "Inlet", "Ridge", "Valley", "Peak", "Bluff", "Shore", "Cove",
    "Harbor", "Wharf", "Landing", "Point", "Mesa", "Plateau", "Basin",
]

_MERIDIAN_VENDOR_SUFFIXES = [
    "Staffing Solutions", "Travel Management", "Training Group", "Research Associates",
    "Legal Services", "Real Estate Partners", "Consulting Group", "Advisory LLC",
    "Capital Partners", "Analytics Inc", "Technologies Corp", "Global Services",
    "Management LLC", "Solutions Inc", "Partners LP", "Holdings Corp",
    "Professional Services", "Financial Advisors", "Marketing Group", "Security Services",
    "Logistics LLC", "Supply Chain Inc", "Data Services", "Cloud Solutions",
    "Network Partners", "Infrastructure Corp", "Engineering LLC", "Design Group",
    "Communications Inc", "Media Partners", "Talent Solutions", "Benefits Group",
    "Payroll Services", "Insurance Brokers", "Compliance Corp", "Audit Partners",
    "Tax Advisors", "Wealth Management", "Risk Solutions", "Operations Group",
]

_CASCADIA_VENDOR_SUFFIXES = [
    "Facility Management", "Telecom Services", "BPO Solutions", "IT Services",
    "Managed Services", "Outsourcing Group", "Infrastructure LLC", "Tech Partners",
    "Digital Solutions", "Process Corp", "Automation Inc", "Systems Group",
    "Integration Partners", "Platform Services", "Cloud Ops LLC", "DevOps Corp",
    "Security Solutions", "Compliance Services", "Monitoring Inc", "Support Group",
    "Maintenance Corp", "Logistics Partners", "Warehouse Solutions", "Fleet Services",
    "Transport LLC", "Distribution Corp", "Fulfillment Inc", "Packaging Group",
    "Materials Corp", "Equipment Leasing", "Asset Management", "Procurement Services",
    "Vendor Management", "Contract Services", "Sourcing Partners", "Quality Corp",
    "Safety Solutions", "Environmental LLC", "Sustainability Group", "Energy Services",
]

# Categories for non-overlapping vendor generation
_MERIDIAN_VENDOR_CATEGORIES = [
    "staffing", "travel", "training", "research", "legal",
    "real_estate", "consulting", "analytics", "marketing", "security",
]

_CASCADIA_VENDOR_CATEGORIES = [
    "facility_management", "offshore_telecom", "bpo_tools", "it_services",
    "managed_services", "logistics", "warehouse", "fleet", "maintenance", "procurement",
]


# ═══════════════════════════════════════════════════════════════════════════════
# People overlap constants
# ═══════════════════════════════════════════════════════════════════════════════

_PEOPLE_OVERLAPS_DATA: List[Dict[str, Any]] = [
    {
        "function": "Finance",
        "meridian_headcount": 400,
        "cascadia_headcount": 350,
        "combined_headcount": 750,
        "role_overlap_examples": ["CFO", "VP Finance", "Controller", "FP&A Director", "Treasury Manager"],
        "definitional_note": "Meridian includes finance contractors in headcount. Cascadia counts W-2 only.",
    },
    {
        "function": "HR",
        "meridian_headcount": 250,
        "cascadia_headcount": 200,
        "combined_headcount": 450,
        "role_overlap_examples": ["CHRO", "VP People", "Talent Acquisition Director", "Benefits Manager"],
        "definitional_note": "Meridian HR manages both W-2 and contractor onboarding. Cascadia HR is W-2 only.",
    },
    {
        "function": "IT",
        "meridian_headcount": 600,
        "cascadia_headcount": 280,
        "combined_headcount": 880,
        "role_overlap_examples": ["CTO", "VP Infrastructure", "CISO", "Enterprise Architect", "DevOps Lead"],
        "definitional_note": "Meridian IT includes managed service provider staff. Cascadia IT is internal only.",
    },
    {
        "function": "Legal",
        "meridian_headcount": 120,
        "cascadia_headcount": 80,
        "combined_headcount": 200,
        "role_overlap_examples": ["General Counsel", "VP Legal", "Corporate Secretary", "Compliance Director"],
        "definitional_note": "Both count only internal legal staff. Neither includes outside counsel.",
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Detail generation constants
# ═══════════════════════════════════════════════════════════════════════════════

_INDUSTRY_SERVICE_TYPES: Dict[str, List[str]] = {
    "Financial Services": [
        "Risk Advisory", "Regulatory Compliance", "Core Banking Transformation",
        "Fraud Analytics", "Treasury Optimization",
    ],
    "Technology": [
        "Cloud Migration", "DevOps Consulting", "Platform Engineering",
        "Data Analytics", "Security Assessment",
    ],
    "Healthcare": [
        "EHR Implementation", "Revenue Cycle Optimization", "Clinical Workflow",
        "Compliance Advisory", "Telehealth Platform",
    ],
    "Pharmaceuticals": [
        "R&D Optimization", "Supply Chain Analytics", "Quality Management",
        "Regulatory Submissions", "Manufacturing Automation",
    ],
    "Retail": [
        "Supply Chain Optimization", "POS Integration", "E-commerce Platform",
        "Inventory Analytics", "Customer Experience",
    ],
    "Telecommunications": [
        "Network Optimization", "Customer Billing", "5G Infrastructure",
        "Network Security", "Digital Experience",
    ],
}

_DEFAULT_SERVICE_TYPES: List[str] = [
    "Strategy Consulting", "Digital Transformation", "Process Optimization",
    "Change Management", "Technology Advisory",
]

_CONTRACT_TYPE_WEIGHTS: List[Tuple[str, float]] = [
    ("MSA", 0.50),
    ("SOW", 0.30),
    ("T&M", 0.20),
]

_PRIMARY_CONTACT_ROLES: List[str] = [
    "VP Operations", "CTO", "CFO", "VP Engineering", "VP Strategy",
    "Chief Digital Officer", "SVP Technology", "VP Procurement",
    "Director of IT", "VP Supply Chain",
]

_VENDOR_SAVINGS_RATIONALE: Dict[str, str] = {
    "cloud_infrastructure": "Volume discount on consolidated compute and storage commitment",
    "collaboration": "Enterprise license consolidation across unified user base",
    "professional_services": "Rate consolidation and preferred-vendor tiering",
    "staffing": "Rate consolidation and volume-based markup reduction",
    "technology": "Enterprise license agreement consolidation",
    "insurance": "Combined risk pool reduces premium rates",
    "travel": "Volume-based negotiation on corporate travel program",
    "facilities": "Portfolio-level master service agreement",
    "telecom": "Unified communications contract consolidation",
    "consulting": "Rate consolidation and preferred-vendor tiering",
    "training": "Enterprise learning platform consolidation",
    "legal": "Rate consolidation under single outside-counsel guidelines",
    "marketing": "Consolidated agency-of-record engagement",
    "office_supplies": "Volume discount on consolidated procurement",
}

_VENDOR_SERVICE_SUBCATEGORIES: Dict[str, List[str]] = {
    "cloud_infrastructure": [
        "Compute (EC2/VMs)", "Object Storage", "Managed Databases",
        "Container Services", "CDN/Edge", "Serverless",
    ],
    "collaboration": [
        "Messaging", "Video Conferencing", "Project Tracking",
        "Document Collaboration", "Workflow Automation",
    ],
    "professional_services": [
        "Audit", "Tax Advisory", "Management Consulting",
        "Transaction Advisory", "Risk Consulting",
    ],
    "staffing": [
        "IT Contract Staffing", "Finance & Accounting Temp",
        "Executive Search", "RPO Services", "Managed Staffing Programs",
    ],
    "technology": [
        "CRM Platform", "ITSM", "ERP Modules",
        "Analytics Platform", "Security Tools",
    ],
    "insurance": [
        "Property & Casualty", "D&O Liability", "Cyber Insurance",
        "Workers Compensation", "Benefits Brokerage",
    ],
    "travel": [
        "Air Travel Management", "Hotel Program", "Ground Transportation",
        "Travel Policy Enforcement", "Expense Management",
    ],
    "facilities": [
        "Property Management", "Lease Advisory", "Workplace Strategy",
        "Project Management", "Facilities Maintenance",
    ],
    "telecom": [
        "Voice Services", "Data Networking", "Mobile Fleet",
        "Unified Communications", "SD-WAN",
    ],
    "consulting": [
        "Strategy Advisory", "Operations Improvement",
        "Technology Consulting", "Organizational Design",
    ],
    "training": [
        "Leadership Development", "Technical Training",
        "Compliance Training", "Onboarding Programs",
    ],
    "legal": [
        "Corporate Law", "Litigation Support",
        "IP/Patent Services", "Regulatory Advisory",
    ],
    "marketing": [
        "Brand Strategy", "Digital Marketing",
        "Media Buying", "Content Production",
    ],
    "office_supplies": [
        "Office Supplies", "Furniture", "Print Services", "Janitorial",
    ],
}

_CONTRACT_END_QUARTERS: List[str] = [
    "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4",
    "2026-Q1", "2026-Q2", "2026-Q3", "2026-Q4",
]

_REPORTING_LINES: Dict[str, str] = {
    "CFO": "Board of Directors",
    "CHRO": "CEO",
    "CTO": "CEO",
    "General Counsel": "CEO",
    "VP Finance": "CFO",
    "VP People": "CHRO",
    "VP Infrastructure": "CTO",
    "VP Legal": "General Counsel",
    "Controller": "CFO",
    "FP&A Director": "VP Finance",
    "Treasury Manager": "VP Finance",
    "Talent Acquisition Director": "VP People",
    "Benefits Manager": "VP People",
    "CISO": "CTO",
    "Enterprise Architect": "CTO",
    "DevOps Lead": "VP Infrastructure",
    "Corporate Secretary": "General Counsel",
    "Compliance Director": "General Counsel",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Generator
# ═══════════════════════════════════════════════════════════════════════════════

class EntityOverlapGenerator:
    """Generates intentional overlaps between Meridian and Cascadia entities.

    Produces customer, vendor, and people overlaps at realistic ratios:
    - Customer overlap: 15-20% of Cascadia's 200 accounts (~35 overlapping)
    - Vendor overlap: 20-25% of Cascadia's 800 vendors (~160-200 overlapping)
    - People overlap: corporate function overlap only (no delivery staff)
    """

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)

    def generate(self) -> OverlapData:
        """Generate all overlap data and return a populated OverlapData."""
        customers = self._generate_customer_overlaps()
        vendors = self._generate_vendor_overlaps()
        people = self._generate_people_overlaps()

        # Populate drill-through detail for each overlap
        for customer in customers:
            customer.engagement_detail = self._generate_customer_engagement_detail(customer)
        for i, vendor in enumerate(vendors):
            vendor.consolidation_detail = self._generate_vendor_consolidation_detail(vendor, i)
        for p in people:
            p.role_detail = self._generate_people_role_detail(p)

        # Cascadia has ~200 accounts; overlap pct is relative to the smaller base
        cascadia_accounts = 200
        customer_overlap_pct = round(len(customers) / cascadia_accounts * 100, 1)

        # Vendor overlap: Cascadia has ~800 vendors
        cascadia_vendors = 800
        vendor_overlap_pct = round(len(vendors) / cascadia_vendors * 100, 1)

        threshold_names = [
            c.canonical_name for c in customers if c.concentration_flag
        ]

        return OverlapData(
            customers=customers,
            vendors=vendors,
            people=people,
            customer_overlap_pct=customer_overlap_pct,
            vendor_overlap_pct=vendor_overlap_pct,
            customers_creating_new_threshold=threshold_names,
        )

    # ------------------------------------------------------------------ #
    # Customer overlaps
    # ------------------------------------------------------------------ #

    def _generate_customer_overlaps(self) -> List[CustomerOverlap]:
        """Generate ~35 overlapping customers across exact/fuzzy/hard categories."""
        customers: List[CustomerOverlap] = []

        # Exact matches (~40%)
        for name, industry, m_rev, c_rev in EXACT_MATCHES:
            combined = m_rev + c_rev
            pct = round(combined / _COMBINED_REVENUE_BASE * 100, 2)
            customers.append(CustomerOverlap(
                meridian_name=name,
                cascadia_name=name,
                canonical_name=name,
                match_type="exact",
                confidence=1.0,
                meridian_revenue=m_rev,
                cascadia_revenue=c_rev,
                combined_revenue=combined,
                combined_pct_of_total=pct,
                concentration_flag=combined > _COMBINED_REVENUE_BASE * _CONCENTRATION_THRESHOLD,
                industry=industry,
                notes="Exact name match in both CRMs.",
            ))

        # Fuzzy matches (~35%)
        for m_name, c_name, canonical, industry, m_rev, c_rev in FUZZY_MATCHES:
            combined = m_rev + c_rev
            pct = round(combined / _COMBINED_REVENUE_BASE * 100, 2)
            # Confidence varies by how different the names are
            confidence = self.rng.uniform(0.75, 0.95)
            customers.append(CustomerOverlap(
                meridian_name=m_name,
                cascadia_name=c_name,
                canonical_name=canonical,
                match_type="fuzzy",
                confidence=round(confidence, 2),
                meridian_revenue=m_rev,
                cascadia_revenue=c_rev,
                combined_revenue=combined,
                combined_pct_of_total=pct,
                concentration_flag=combined > _COMBINED_REVENUE_BASE * _CONCENTRATION_THRESHOLD,
                industry=industry,
                notes="Name variant — Inc/Corp/LLC suffix or abbreviation difference.",
            ))

        # Hard matches (~25%)
        for m_name, c_name, canonical, industry, m_rev, c_rev, notes in HARD_MATCHES:
            combined = m_rev + c_rev
            pct = round(combined / _COMBINED_REVENUE_BASE * 100, 2)
            confidence = self.rng.uniform(0.40, 0.70)
            customers.append(CustomerOverlap(
                meridian_name=m_name,
                cascadia_name=c_name,
                canonical_name=canonical,
                match_type="hard",
                confidence=round(confidence, 2),
                meridian_revenue=m_rev,
                cascadia_revenue=c_rev,
                combined_revenue=combined,
                combined_pct_of_total=pct,
                concentration_flag=combined > _COMBINED_REVENUE_BASE * _CONCENTRATION_THRESHOLD,
                industry=industry,
                notes=notes,
            ))

        return customers

    # ------------------------------------------------------------------ #
    # Vendor overlaps
    # ------------------------------------------------------------------ #

    def _generate_vendor_overlaps(self) -> List[VendorOverlap]:
        """Generate overlapping vendors from named list + procedural generation.

        Target: 160-200 overlapping vendors (20-25% of Cascadia's 800).
        Named vendors from SHARED_VENDORS constant (~30), plus ~150 procedurally
        generated overlapping vendors.
        """
        vendors: List[VendorOverlap] = []

        # Named shared vendors
        for category, vendor_list in SHARED_VENDORS.items():
            for vendor_name, m_spend, c_spend in vendor_list:
                combined = m_spend + c_spend
                # Skip vendor-specific exclusives (spend=0 on one side) from overlap
                # but still include them — they represent single-vendor contracts
                # that are candidates for consolidation
                vendors.append(VendorOverlap(
                    meridian_name=vendor_name,
                    cascadia_name=vendor_name,
                    canonical_name=vendor_name,
                    match_type="exact",
                    category=category,
                    meridian_spend=m_spend,
                    cascadia_spend=c_spend,
                    combined_spend=combined,
                    consolidation_opportunity=combined > 5.0,
                ))

        # Procedurally generate additional overlapping vendors to reach target
        target_total = self.rng.randint(160, 200)
        remaining = target_total - len(vendors)

        # Categories and their spend distributions for generated vendors
        gen_categories = [
            ("staffing", (0.5, 4.0), (0.2, 2.0)),
            ("consulting", (0.3, 3.0), (0.1, 1.5)),
            ("technology", (0.2, 2.5), (0.1, 1.0)),
            ("facilities", (0.3, 2.0), (0.1, 1.0)),
            ("travel", (0.2, 1.5), (0.1, 0.8)),
            ("training", (0.1, 1.0), (0.05, 0.5)),
            ("legal", (0.2, 2.0), (0.1, 0.8)),
            ("marketing", (0.1, 1.5), (0.05, 0.6)),
            ("insurance", (0.2, 1.5), (0.1, 0.5)),
            ("office_supplies", (0.05, 0.5), (0.02, 0.3)),
        ]

        used_names: set = set()
        # Common vendor name patterns for overlapping vendors
        _overlap_prefixes = [
            "National", "American", "United", "Premier", "Global", "Pacific",
            "Continental", "Integrated", "Advanced", "Strategic", "Core",
            "Central", "Allied", "Standard", "Dynamic", "Progressive",
            "Modern", "Elite", "Prime", "Select", "Preferred", "Reliable",
            "Trusted", "Certified", "Quality", "Superior", "Precision",
            "Express", "Direct", "Metro", "Regional", "Interstate",
            "Consolidated", "Universal", "General", "Efficient", "Optimal",
            "ProTech", "DataCore", "InfoSys", "CompuServe", "NetWorks",
            "SysCorp", "TeleCom", "BioMed", "HealthFirst", "FinServ",
        ]
        _overlap_suffixes = [
            "Services", "Solutions", "Group", "Partners", "Corp",
            "Inc", "LLC", "Associates", "International", "Enterprises",
            "Industries", "Systems", "Technologies", "Management", "Resources",
            "Consulting", "Professionals", "Network", "Alliance", "Holdings",
        ]

        for i in range(remaining):
            cat_info = gen_categories[i % len(gen_categories)]
            category = cat_info[0]
            m_range = cat_info[1]
            c_range = cat_info[2]

            # Generate a unique vendor name
            for _ in range(50):
                prefix = self.rng.choice(_overlap_prefixes)
                suffix = self.rng.choice(_overlap_suffixes)
                name = f"{prefix} {suffix}"
                if name not in used_names:
                    used_names.add(name)
                    break

            m_spend = round(self.rng.uniform(*m_range), 2)
            c_spend = round(self.rng.uniform(*c_range), 2)
            combined = round(m_spend + c_spend, 2)

            # Determine match type: ~60% exact, ~30% fuzzy, ~10% hard
            roll = self.rng.random()
            if roll < 0.60:
                match_type = "exact"
                m_name = name
                c_name = name
            elif roll < 0.90:
                match_type = "fuzzy"
                m_name = name
                # Create a slight variant for cascadia
                variant_roll = self.rng.random()
                if variant_roll < 0.33:
                    c_name = name.replace("Inc", "LLC").replace("Corp", "Inc").replace("LLC", "Corp")
                    if c_name == name:
                        c_name = name + " Inc"
                elif variant_roll < 0.66:
                    c_name = "The " + name
                else:
                    c_name = name.replace("Services", "Svcs").replace("Solutions", "Solutns")
                    if c_name == name:
                        c_name = name + " Co"
            else:
                match_type = "hard"
                m_name = name
                # Completely different name for the hard match
                alt_prefix = self.rng.choice(_overlap_prefixes)
                alt_suffix = self.rng.choice(_overlap_suffixes)
                c_name = f"{alt_prefix} {alt_suffix}"

            vendors.append(VendorOverlap(
                meridian_name=m_name,
                cascadia_name=c_name,
                canonical_name=name,
                match_type=match_type,
                category=category,
                meridian_spend=m_spend,
                cascadia_spend=c_spend,
                combined_spend=combined,
                consolidation_opportunity=combined > 5.0,
            ))

        return vendors

    # ------------------------------------------------------------------ #
    # People overlaps
    # ------------------------------------------------------------------ #

    def _generate_people_overlaps(self) -> List[PeopleOverlap]:
        """Generate corporate function overlap data."""
        return [
            PeopleOverlap(
                function=d["function"],
                meridian_headcount=d["meridian_headcount"],
                cascadia_headcount=d["cascadia_headcount"],
                combined_headcount=d["combined_headcount"],
                role_overlap_examples=d["role_overlap_examples"],
                definitional_note=d["definitional_note"],
            )
            for d in _PEOPLE_OVERLAPS_DATA
        ]

    # ------------------------------------------------------------------ #
    # Detail generation
    # ------------------------------------------------------------------ #

    def _weighted_contract_type(self) -> str:
        """Pick a contract type using weighted random: MSA 50%, SOW 30%, T&M 20%."""
        roll = self.rng.random()
        cumulative = 0.0
        for ct, weight in _CONTRACT_TYPE_WEIGHTS:
            cumulative += weight
            if roll < cumulative:
                return ct
        return _CONTRACT_TYPE_WEIGHTS[-1][0]

    def _generate_customer_engagement_detail(
        self, customer: CustomerOverlap
    ) -> List[CustomerEngagementDetail]:
        """Generate 2 engagement details (one per entity) for a customer overlap."""
        service_pool = _INDUSTRY_SERVICE_TYPES.get(
            customer.industry, _DEFAULT_SERVICE_TYPES
        )
        details: List[CustomerEngagementDetail] = []
        for entity, revenue in [
            ("meridian", customer.meridian_revenue),
            ("cascadia", customer.cascadia_revenue),
        ]:
            num_services = self.rng.randint(1, min(3, len(service_pool)))
            service_types = self.rng.sample(service_pool, num_services)
            details.append(CustomerEngagementDetail(
                entity=entity,
                service_types=service_types,
                contract_type=self._weighted_contract_type(),
                annual_value_M=revenue,
                relationship_start_year=self.rng.randint(2015, 2024),
                primary_contact_role=self.rng.choice(_PRIMARY_CONTACT_ROLES),
            ))
        return details

    def _spend_to_contract_type(self, spend: float) -> str:
        """Map spend level to contract type classification."""
        if spend > 10.0:
            return "enterprise"
        elif spend >= 2.0:
            return "department"
        else:
            return "ad-hoc"

    def _generate_vendor_consolidation_detail(
        self, vendor: VendorOverlap, index: int
    ) -> VendorConsolidationDetail:
        """Generate consolidation detail for a vendor overlap."""
        # Contract end dates spread deterministically across quarters
        m_end = _CONTRACT_END_QUARTERS[index % len(_CONTRACT_END_QUARTERS)]
        c_end = _CONTRACT_END_QUARTERS[(index + 3) % len(_CONTRACT_END_QUARTERS)]

        # Savings percentage based on consolidation opportunity
        if vendor.consolidation_opportunity:
            savings_pct = round(self.rng.uniform(12.0, 18.0), 1)
        else:
            savings_pct = round(self.rng.uniform(3.0, 5.0), 1)

        savings_M = round(vendor.combined_spend * savings_pct / 100.0, 2)

        rationale = _VENDOR_SAVINGS_RATIONALE.get(
            vendor.category,
            "Consolidated procurement and volume-based discount",
        )

        # Service subcategories: 2-4 from the category pool
        subcategory_pool = _VENDOR_SERVICE_SUBCATEGORIES.get(
            vendor.category,
            ["General Services", "Support", "Consulting", "Implementation"],
        )
        num_subcats = min(self.rng.randint(2, 4), len(subcategory_pool))
        service_subcategories = self.rng.sample(subcategory_pool, num_subcats)

        return VendorConsolidationDetail(
            meridian_contract_type=self._spend_to_contract_type(vendor.meridian_spend),
            cascadia_contract_type=self._spend_to_contract_type(vendor.cascadia_spend),
            meridian_contract_end=m_end,
            cascadia_contract_end=c_end,
            estimated_savings_pct=savings_pct,
            estimated_savings_M=savings_M,
            savings_rationale=rationale,
            service_subcategories=service_subcategories,
        )

    def _generate_people_role_detail(
        self, people: PeopleOverlap
    ) -> List[RoleDetail]:
        """Expand role_overlap_examples into full RoleDetail objects."""
        roles = people.role_overlap_examples
        if not roles:
            return []

        details: List[RoleDetail] = []
        # Top role (C-suite) gets 1 count each; remaining headcount distributed
        # proportionally among the other roles.
        remaining_m = max(people.meridian_headcount - 1, 0)
        remaining_c = max(people.cascadia_headcount - 1, 0)
        other_roles = roles[1:] if len(roles) > 1 else []

        for i, title in enumerate(roles):
            if i == 0:
                # C-suite role: 1 each
                m_count = 1
                c_count = 1
            elif i < len(roles) - 1:
                # Distribute proportionally among non-top, non-last roles
                share = 1.0 / max(len(other_roles), 1)
                m_count = max(1, round(remaining_m * share))
                c_count = max(1, round(remaining_c * share))
            else:
                # Last role gets whatever remains
                allocated_m = sum(d.meridian_count for d in details)
                allocated_c = sum(d.cascadia_count for d in details)
                m_count = max(1, people.meridian_headcount - allocated_m)
                c_count = max(1, people.cascadia_headcount - allocated_c)

            combined_count = m_count + c_count

            # Consolidation action based on role level
            c_suite_titles = {"CFO", "CHRO", "CTO", "CISO", "General Counsel"}
            if title in c_suite_titles:
                consolidation_action = "retain_both"
            elif "VP" in title or "Director" in title:
                consolidation_action = "consolidate"
            else:
                consolidation_action = "evaluate"

            reporting_line = _REPORTING_LINES.get(title, "Department Head")

            details.append(RoleDetail(
                title=title,
                meridian_count=m_count,
                cascadia_count=c_count,
                combined_count=combined_count,
                consolidation_action=consolidation_action,
                reporting_line=reporting_line,
            ))

        return details

    # ------------------------------------------------------------------ #
    # Non-overlapping vendor generation (for filling out totals)
    # ------------------------------------------------------------------ #

    def generate_non_overlapping_vendors(
        self,
    ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """Generate non-overlapping vendors for each entity to fill totals.

        Returns:
            (meridian_unique_vendors, cascadia_unique_vendors) — each is a list
            of dicts with 'name', 'category', 'annual_spend_M'.

        Meridian target: ~2000 total vendors. With ~180 overlapping, need ~1820 unique.
        Cascadia target: ~800 total vendors. With ~180 overlapping, need ~620 unique.
        """
        meridian_vendors: List[Dict[str, str]] = []
        cascadia_vendors: List[Dict[str, str]] = []

        # Meridian unique vendors (~1820)
        used_m: set = set()
        for i in range(1820):
            prefix = self.rng.choice(_MERIDIAN_VENDOR_PREFIXES)
            suffix = self.rng.choice(_MERIDIAN_VENDOR_SUFFIXES)
            name = f"{prefix} {suffix}"
            # Ensure uniqueness by appending a number if needed
            if name in used_m:
                name = f"{name} {i}"
            used_m.add(name)

            category = self.rng.choice(_MERIDIAN_VENDOR_CATEGORIES)
            spend = round(self.rng.uniform(0.01, 3.0), 2)

            meridian_vendors.append({
                "name": name,
                "category": category,
                "annual_spend_M": str(spend),
            })

        # Cascadia unique vendors (~620)
        used_c: set = set()
        for i in range(620):
            prefix = self.rng.choice(_CASCADIA_VENDOR_PREFIXES)
            suffix = self.rng.choice(_CASCADIA_VENDOR_SUFFIXES)
            name = f"{prefix} {suffix}"
            if name in used_c:
                name = f"{name} {i}"
            used_c.add(name)

            category = self.rng.choice(_CASCADIA_VENDOR_CATEGORIES)
            spend = round(self.rng.uniform(0.01, 1.5), 2)

            cascadia_vendors.append({
                "name": name,
                "category": category,
                "annual_spend_M": str(spend),
            })

        return meridian_vendors, cascadia_vendors


# ═══════════════════════════════════════════════════════════════════════════════
# CLI entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    gen = EntityOverlapGenerator(seed=42)
    data = gen.generate()

    print("=" * 70)
    print("ENTITY OVERLAP SUMMARY")
    print("=" * 70)

    # Customer summary
    print(f"\nCUSTOMER OVERLAPS: {len(data.customers)} overlapping accounts")
    print(f"  Overlap as % of Cascadia (200 accounts): {data.customer_overlap_pct}%")

    exact = [c for c in data.customers if c.match_type == "exact"]
    fuzzy = [c for c in data.customers if c.match_type == "fuzzy"]
    hard = [c for c in data.customers if c.match_type == "hard"]
    print(f"  Exact matches: {len(exact)} ({len(exact)/len(data.customers)*100:.0f}%)")
    print(f"  Fuzzy matches: {len(fuzzy)} ({len(fuzzy)/len(data.customers)*100:.0f}%)")
    print(f"  Hard matches:  {len(hard)} ({len(hard)/len(data.customers)*100:.0f}%)")

    total_combined_rev = sum(c.combined_revenue for c in data.customers)
    print(f"  Total combined revenue from overlapping: ${total_combined_rev:.1f}M")

    if data.customers_creating_new_threshold:
        print(f"\n  CONCENTRATION THRESHOLD CROSSINGS (>5% of $6B = $300M):")
        for name in data.customers_creating_new_threshold:
            crossing = next(c for c in data.customers if c.canonical_name == name)
            print(f"    - {name}: ${crossing.combined_revenue:.0f}M = {crossing.combined_pct_of_total}%")

    # Vendor summary
    print(f"\nVENDOR OVERLAPS: {len(data.vendors)} overlapping vendors")
    print(f"  Overlap as % of Cascadia (800 vendors): {data.vendor_overlap_pct}%")

    named_vendors = [v for v in data.vendors if v.canonical_name in
                     {name for vendors in SHARED_VENDORS.values() for name, _, _ in vendors}]
    print(f"  Named/strategic vendors: {len(named_vendors)}")
    print(f"  Procedurally generated: {len(data.vendors) - len(named_vendors)}")

    consolidation = [v for v in data.vendors if v.consolidation_opportunity]
    print(f"  Consolidation opportunities (>$5M combined): {len(consolidation)}")

    total_vendor_spend = sum(v.combined_spend for v in data.vendors)
    print(f"  Total combined vendor spend from overlapping: ${total_vendor_spend:.1f}M")

    # People summary
    print(f"\nPEOPLE OVERLAPS: {len(data.people)} corporate functions")
    for p in data.people:
        print(f"  {p.function}: {p.meridian_headcount} M + {p.cascadia_headcount} C = {p.combined_headcount}")
        print(f"    Note: {p.definitional_note}")

    total_corp = sum(p.combined_headcount for p in data.people)
    print(f"  Total combined corporate headcount: {total_corp}")

    # Non-overlapping vendor counts
    m_unique, c_unique = gen.generate_non_overlapping_vendors()
    print(f"\nNON-OVERLAPPING VENDORS:")
    print(f"  Meridian unique: {len(m_unique)}")
    print(f"  Cascadia unique: {len(c_unique)}")
    print(f"  Meridian total: {len(m_unique) + len(data.vendors)}")
    print(f"  Cascadia total: {len(c_unique) + len(data.vendors)}")

    print("\n" + "=" * 70)
    print("GROUND TRUTH DICT PREVIEW (top-level keys):")
    gt = data.to_ground_truth_dict()
    for key, val in gt.items():
        if isinstance(val, dict):
            print(f"  {key}:")
            for k2, v2 in val.items():
                if isinstance(v2, list):
                    print(f"    {k2}: [{len(v2)} entries]")
                else:
                    print(f"    {k2}: {v2}")
